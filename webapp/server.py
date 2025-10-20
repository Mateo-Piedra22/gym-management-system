import os
import sys
import csv
import json
import secrets
from pathlib import Path
from typing import Optional, Dict, Any, Callable
from datetime import datetime, timezone

from fastapi import FastAPI, Request, HTTPException, Depends
import logging
from fastapi.responses import JSONResponse, RedirectResponse, Response
from fastapi.templating import Jinja2Templates
from starlette.middleware.sessions import SessionMiddleware
from starlette.middleware.trustedhost import TrustedHostMiddleware
from starlette.middleware.httpsredirect import HTTPSRedirectMiddleware
from starlette.staticfiles import StaticFiles
from starlette.middleware.base import BaseHTTPMiddleware
import uuid
import threading
import psutil
import psycopg2
import psycopg2.extras
import time
# HTTP client para probes (con fallback si no está disponible)
try:
    import requests  # type: ignore
except Exception:
    requests = None  # type: ignore

# Importar gestor de base de datos SIN modificar el programa principal
try:
    from database import DatabaseManager  # type: ignore
except Exception:
    DatabaseManager = None  # type: ignore

# Importar dataclasses de modelos para payloads del API
try:
    from models import Usuario, Pago, MetodoPago, ConceptoPago  # type: ignore
except Exception:
    Usuario = None  # type: ignore
    Pago = None  # type: ignore
    MetodoPago = None  # type: ignore
    ConceptoPago = None  # type: ignore

# Importar utilidades del proyecto principal para branding y contraseña de desarrollador
try:
    from utils import get_gym_name  # type: ignore
except Exception:
    def get_gym_name(default: str = "Gimnasio") -> str:  # type: ignore
        try:
            path = Path("gym_data.txt")
            if path.exists():
                for line in path.read_text(encoding="utf-8").splitlines():
                    if line.startswith("gym_name="):
                        return line.split("=", 1)[1].strip()
        except Exception:
            pass
        return default

try:
    from managers import DEV_PASSWORD  # type: ignore
except Exception:
    DEV_PASSWORD = None  # type: ignore

try:
    from payment_manager import PaymentManager
except Exception:
    PaymentManager = None

# Base URL pública (Railway) sin túneles
try:
    from utils import get_webapp_base_url  # type: ignore
except Exception:
    def get_webapp_base_url(default: str = "https://gym-ms-zrk.up.railway.app") -> str:  # type: ignore
        import os as _os
        return _os.getenv("WEBAPP_BASE_URL", default).strip()

# Utilidad para cerrar túneles públicos de forma segura
try:
    from utils import terminate_tunnel_processes  # type: ignore
except Exception:
    def terminate_tunnel_processes() -> None:  # type: ignore
        try:
            for p in psutil.process_iter(attrs=["pid","name","cmdline"]):
                try:
                    cmd = " ".join(p.info.get("cmdline") or [])
                    if ("ssh" in (p.info.get("name") or "")) or ("node" in (p.info.get("name") or "")):
                        p.terminate()
                except Exception:
                    pass
        except Exception:
            pass

from .qss_to_css import generate_css_from_qss, read_theme_vars

_db: Optional[DatabaseManager] = None
# Bloqueo para inicialización segura de la instancia global de DB y evitar condiciones de carrera
_db_lock = threading.RLock()
_db_initializing = False

# Gestor de pagos (se instancia perezosamente sobre la DB)
_pm: Optional[PaymentManager] = None

def _get_pm() -> Optional[PaymentManager]:
    global _pm
    try:
        if _pm is not None:
            return _pm
        db = _get_db()
        if db is None or PaymentManager is None:
            return None
        _pm = PaymentManager(db)
        return _pm
    except Exception:
        return None

# Ajuste de stdout/stderr para ejecutables sin consola en Windows (runw.exe)
# Evita fallos de configuración de logging en Uvicorn cuando sys.stdout/sys.stderr es None.
try:
    if os.name == "nt":
        import os as _os
        import io as _io
        # Asegurar stdout/stderr válidos aunque estén cerrados en ejecutables sin consola
        _sout = getattr(sys, "stdout", None)
        try:
            if _sout is None or getattr(_sout, "closed", False):
                sys.stdout = open(_os.devnull, "w", encoding="utf-8")
            elif hasattr(_sout, "buffer") and not isinstance(_sout, _io.TextIOWrapper):
                sys.stdout = _io.TextIOWrapper(_sout.buffer, encoding="utf-8", errors="replace")
        except Exception:
            pass
        _serr = getattr(sys, "stderr", None)
        try:
            if _serr is None or getattr(_serr, "closed", False):
                sys.stderr = open(_os.devnull, "w", encoding="utf-8")
            elif hasattr(_serr, "buffer") and not isinstance(_serr, _io.TextIOWrapper):
                sys.stderr = _io.TextIOWrapper(_serr.buffer, encoding="utf-8", errors="replace")
        except Exception:
            pass
        # Forzar política de event loop compatible en Windows a nivel global
        try:
            import asyncio as _asyncio
            _asyncio.set_event_loop_policy(_asyncio.WindowsSelectorEventLoopPolicy())
        except Exception:
            pass
except Exception:
    pass

# Fallback defensivo: rebind local de print a versión segura que no falle
# si stdout/stderr está cerrado en ejecutables sin consola.
try:
    import builtins as _builtins
    def _safe_print(*args, **kwargs):
        try:
            _builtins.print(*args, **kwargs)
        except Exception:
            try:
                import logging as _logging
                # Registrar el mensaje concatenado para no perder diagnóstico
                _logging.info(" ".join(str(a) for a in args))
            except Exception:
                # No romper si logging aún no está configurado
                pass
    print = _safe_print  # type: ignore
except Exception:
    pass

def _compute_base_dir() -> Path:
    """Determina la carpeta base desde la cual resolver recursos.
    - En ejecutable PyInstaller (onedir): junto al exe.
    - En modo onefile: carpeta temporal _MEIPASS.
    - En desarrollo: raíz del proyecto.
    """
    try:
        if getattr(sys, "frozen", False):
            exe_dir = Path(sys.executable).resolve().parent
            meipass = getattr(sys, "_MEIPASS", None)
            if meipass:
                return Path(meipass)
            return exe_dir
    except Exception:
        pass
    # Desarrollo: carpeta del proyecto
    try:
        # server.py está en webapp/, subimos un nivel
        return Path(__file__).resolve().parent.parent
    except Exception:
        return Path('.')

def _resolve_existing_dir(*parts: str) -> Path:
    """Devuelve el primer directorio existente entre varias ubicaciones candidatas.
    Prioriza BASE_DIR, luego el directorio del ejecutable (onedir) y por último el proyecto.
    """
    candidates = []
    try:
        candidates.append(BASE_DIR.joinpath(*parts))
    except Exception:
        pass
    try:
        exe_dir = Path(sys.executable).resolve().parent
        candidates.append(exe_dir.joinpath(*parts))
    except Exception:
        pass
    try:
        proj_root = Path(__file__).resolve().parent.parent
        candidates.append(proj_root.joinpath(*parts))
    except Exception:
        pass
    for c in candidates:
        try:
            if c.exists():
                return c
        except Exception:
            continue
    # Fallback: primera opción aunque no exista
    return candidates[0] if candidates else Path(*parts)

# Helper para secreto de sesion estable

def _get_session_secret() -> str:
    try:
        env = os.getenv("WEBAPP_SECRET_KEY", "").strip()
        if env:
            return env
    except Exception:
        pass
    # Intentar leer/persistir en config/config.json
    try:
        from utils import resource_path  # type: ignore
        cfg_path = resource_path("config/config.json")
        cfg = {}
        if os.path.exists(cfg_path):
            import json as _json
            try:
                with open(cfg_path, "r", encoding="utf-8") as f:
                    cfg = _json.load(f) or {}
                if not isinstance(cfg, dict):
                    cfg = {}
            except Exception:
                cfg = {}
        secret = str(cfg.get("webapp_session_secret") or cfg.get("session_secret") or "").strip()
        if not secret:
            import secrets as _secrets
            secret = _secrets.token_urlsafe(32)
            cfg["webapp_session_secret"] = secret
            try:
                with open(cfg_path, "w", encoding="utf-8") as f:
                    _json.dump(cfg, f, ensure_ascii=False, indent=2)
            except Exception:
                # Si no podemos persistir, devolver el generado
                pass
        return secret
    except Exception:
        pass
    # Fallback: secreto efimero
    import secrets as _secrets
    return _secrets.token_urlsafe(32)

# Inicialización de la app web
app = FastAPI(
    title="GymMS WebApp",
    version="2.0",
    # Permite servir detrás de reverse proxy con subpath
    root_path=os.getenv("ROOT_PATH", "").strip(),
)
app.add_middleware(SessionMiddleware, secret_key=_get_session_secret())

# Middleware de Request ID para trazabilidad en logs
class RequestIDMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        rid = request.headers.get("X-Request-ID") or uuid.uuid4().hex[:12]
        try:
            request.state.request_id = rid
        except Exception:
            pass
        response = await call_next(request)
        try:
            response.headers["X-Request-ID"] = rid
        except Exception:
            pass
        return response

try:
    app.add_middleware(RequestIDMiddleware)
except Exception:
    pass

# Middleware de latencia y estado del circuito para observabilidad
class TimingAndCircuitMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        import time
        start = time.perf_counter()
        rid = getattr(getattr(request, 'state', object()), 'request_id', '-')
        path = request.url.path
        method = request.method
        circuit_state = None
        try:
            db = _get_db()
            if db and hasattr(db, 'get_circuit_state'):
                circuit_state = db.get_circuit_state()  # type: ignore
        except Exception:
            circuit_state = None
        response = await call_next(request)
        dur_ms = int((time.perf_counter() - start) * 1000)
        try:
            logging.info(f"req {method} {path} rid={rid} duration_ms={dur_ms} circuit={circuit_state}")
        except Exception:
            pass
        return response

try:
    app.add_middleware(TimingAndCircuitMiddleware)
except Exception:
    pass

# Middlewares de producción (opcionales via ENV, cambios mínimos)
try:
    # Restringir hosts confiables. Si no hay ENV, añadir dominio Railway por defecto
    th = os.getenv("TRUSTED_HOSTS", "").strip()
    hosts = [h.strip() for h in th.split(",") if h.strip()] if th else []
    if not hosts:
        hosts = ["gym-ms-zrk.up.railway.app", "localhost", "127.0.0.1", "*.loca.lt"]
    app.add_middleware(TrustedHostMiddleware, allowed_hosts=hosts)
    # Forzar HTTPS en producción si se indica
    if (os.getenv("FORCE_HTTPS", "0").strip() in ("1", "true", "yes")):
        app.add_middleware(HTTPSRedirectMiddleware)
    # Nota: Gestión de cabeceras de proxy delegada a Uvicorn (proxy_headers=True)
    if (os.getenv("PROXY_HEADERS_ENABLED", "1").strip() in ("1", "true", "yes")):
        logging.info("Cabeceras de proxy gestionadas por Uvicorn (proxy_headers=True)")
except Exception:
    pass

# Static y assets basados en BASE_DIR
BASE_DIR = _compute_base_dir()
static_dir = _resolve_existing_dir("webapp", "static")
static_dir.mkdir(parents=True, exist_ok=True)
templates_dir = _resolve_existing_dir("webapp", "templates")
templates = Jinja2Templates(directory=str(templates_dir))

try:
    app.mount("/static", StaticFiles(directory=str(static_dir)), name="static")
except Exception:
    pass
# Preferir assets del proyecto raíz; fallback a assets dentro de webapp
try:
    app.mount("/assets", StaticFiles(directory=str(_resolve_existing_dir("assets"))), name="assets")
except Exception:
    try:
        app.mount("/assets", StaticFiles(directory=str(_resolve_existing_dir("webapp", "assets"))), name="assets")
    except Exception:
        pass

# --- Utilidad legacy de sincronización eliminada ---
# Nota: La replicación lógica de PostgreSQL reemplaza el sistema de sync vía HTTP.

# Endpoint de salud ligero para probes y monitores
@app.get("/healthz")
async def healthz():
    """Devuelve estado 200 si la app responde. Incluye mínimos detalles."""
    try:
        details: Dict[str, Any] = {
            "status": "ok",
            "time": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        }
        # Comprobación opcional de DB (no bloqueante)
        try:
            db = _get_db()
            if db is not None:
                details["db"] = "ok"
        except Exception:
            details["db"] = "error"
        return JSONResponse(details)
    except Exception:
        # Fallback defensivo: responder 200 para evitar cascadas de reinicios
        return JSONResponse({"status": "ok"})

# Endpoint para exponer la URL base pública (Railway)
@app.get("/webapp/base_url")
async def webapp_base_url():
    try:
        url = get_webapp_base_url()
        return JSONResponse({"base_url": url})
    except Exception:
        return JSONResponse({"base_url": "https://gym-ms-zrk.up.railway.app"})

# Endpoints de sincronización para integración con proxy local
@app.post("/api/sync/upload")
async def api_sync_upload(request: Request):
    rid = getattr(getattr(request, 'state', object()), 'request_id', '-')
    db = _get_db()
    guard = _circuit_guard_json(db, "/api/sync/upload") if db else None
    if guard:
        return guard
    # Validar JSON
    try:
        data = await request.json()
    except Exception:
        return JSONResponse({"error": "JSON inválido"}, status_code=400)
    ops = data.get("ops")
    if not isinstance(ops, list):
        return JSONResponse({"error": "Formato inválido: 'ops' debe ser lista"}, status_code=400)
    # Autenticación por token (solo desde ENV)
    expected = os.getenv("SYNC_UPLOAD_TOKEN", "").strip()
    if expected:
        auth = str(request.headers.get("Authorization", "")).strip()
        x_token = str(request.headers.get("X-Upload-Token", "")).strip()
        token = ""
        if auth.startswith("Bearer "):
            token = auth.split(" ", 1)[1].strip()
        elif x_token:
            token = x_token
        if not token:
            return JSONResponse({"error": "Falta token"}, status_code=401)
        if token != expected:
            return JSONResponse({"error": "Token inválido"}, status_code=401)
    # Registrar en inbox para observabilidad
    try:
        base_dir = _compute_base_dir()
        inbox_dir = base_dir / "config"
        inbox_dir.mkdir(parents=True, exist_ok=True)
        inbox_path = inbox_dir / "sync_inbox.jsonl"
        with open(inbox_path, "a", encoding="utf-8") as f:
            for op in ops:
                try:
                    f.write(json.dumps(op, ensure_ascii=False) + "\n")
                except Exception:
                    # No bloquear el lote por un op malformado
                    pass
    except Exception as e:
        try:
            logging.debug(f"/api/sync/upload: fallo escribiendo inbox rid={rid} err={e}")
        except Exception:
            pass
    # Construir ack por dedup_key
    dedup_keys = []
    try:
        for op in ops:
            k = None
            try:
                k = op.get("dedup_key")
            except Exception:
                k = None
            if k:
                dedup_keys.append(str(k))
    except Exception:
        dedup_keys = []
    try:
        logging.info(f"/api/sync/upload: recibido ops={len(ops)} acked={len(dedup_keys)} rid={rid}")
    except Exception:
        pass
    return JSONResponse({"acked": dedup_keys})

# Endpoint para aplicar cambios del outbox local de forma idempotente
@app.post("/api/sync/upload_outbox")
async def api_sync_upload_outbox(request: Request):
    rid = getattr(getattr(request, 'state', object()), 'request_id', '-')
    db = _get_db()
    guard = _circuit_guard_json(db, "/api/sync/upload_outbox") if db else None
    if guard:
        return guard
    # Validar JSON
    try:
        payload = await request.json()
    except Exception:
        return JSONResponse({"error": "JSON inválido"}, status_code=400)
    changes = payload.get("changes")
    if not isinstance(changes, list):
        return JSONResponse({"error": "Formato inválido: 'changes' debe ser lista"}, status_code=400)
    # Autenticación por token (solo desde ENV)
    expected = os.getenv("SYNC_UPLOAD_TOKEN", "").strip()
    if expected:
        auth = str(request.headers.get("Authorization", "")).strip()
        x_token = str(request.headers.get("X-Upload-Token", "")).strip()
        token = ""
        if auth.startswith("Bearer "):
            token = auth.split(" ", 1)[1].strip()
        elif x_token:
            token = x_token
        if not token:
            return JSONResponse({"error": "Falta token"}, status_code=401)
        if token != expected:
            return JSONResponse({"error": "Token inválido"}, status_code=401)

    acked: list = []
    errors: list = []
    try:
        with db.get_connection_context() as conn:  # type: ignore
            conn.autocommit = False
            for ch in changes:
                try:
                    schema = str(ch.get("schema") or "public")
                    table = str(ch.get("table") or "")
                    op = str(ch.get("op") or "").upper()
                    pk = ch.get("pk") or {}
                    data = ch.get("data")
                    dedup_key = str(ch.get("dedup_key") or "")
                    if not table or op not in ("INSERT", "UPDATE", "DELETE") or not isinstance(pk, dict):
                        errors.append({"dedup_key": dedup_key, "error": "change inválido"})
                        continue
                    applied = _apply_change_idempotent(conn, schema, table, op, pk, data)
                    if applied and dedup_key:
                        acked.append(dedup_key)
                except Exception as e:
                    try:
                        errors.append({"dedup_key": str(ch.get("dedup_key") or ""), "error": str(e)})
                    except Exception:
                        pass
            conn.commit()
    except Exception as e:
        try:
            logging.exception(f"/api/sync/upload_outbox: fallo aplicando cambios rid={rid} err={e}")
        except Exception:
            pass
        return JSONResponse({"error": "Fallo aplicando cambios"}, status_code=500)
    try:
        logging.info(f"/api/sync/upload_outbox: changes={len(changes)} acked={len(acked)} errors={len(errors)} rid={rid}")
    except Exception:
        pass
    return JSONResponse({"acked": acked, "errors": errors})

# Helpers para aplicar cambios de forma segura e idempotente

def _get_pk_columns_conn(conn, schema: str, table: str):
    cols = []
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT a.attname
            FROM pg_index i
            JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
            WHERE i.indrelid = %s::regclass AND i.indisprimary
            """,
            (f"{schema}.{table}",)
        )
        rows = cur.fetchall() or []
        cols = [r[0] for r in rows]
    except Exception:
        cols = []
    return cols


def _filter_existing_columns(conn, schema: str, table: str, data: dict) -> dict:
    if not isinstance(data, dict):
        return {}
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = %s AND table_name = %s
            """,
            (schema, table)
        )
        allowed = {r[0] for r in (cur.fetchall() or [])}
        return {k: v for k, v in data.items() if k in allowed}
    except Exception:
        return {}


def _apply_change_idempotent(conn, schema: str, table: str, op: str, pk: dict, data: dict) -> bool:
    from psycopg2 import sql as _sql
    try:
        pk_cols = _get_pk_columns_conn(conn, schema, table)
        if op == 'DELETE':
            # Borrar aunque no exista: idempotente
            where_parts = []
            params = []
            for c in pk_cols:
                where_parts.append(_sql.SQL("{} = %s").format(_sql.Identifier(c)))
                params.append(pk.get(c))
            stmt = _sql.SQL("DELETE FROM {}.{} WHERE ")\
                .format(_sql.Identifier(schema), _sql.Identifier(table)) + _sql.SQL(" AND ").join(where_parts)
            cur = conn.cursor()
            cur.execute(stmt, params)
            return True
        elif op == 'UPDATE':
            updates = _filter_existing_columns(conn, schema, table, data or {})
            if not updates:
                return True
            set_parts = []
            params = []
            for k, v in updates.items():
                set_parts.append(_sql.SQL("{} = %s").format(_sql.Identifier(str(k))))
                params.append(v)
            where_parts = []
            for c in pk_cols:
                where_parts.append(_sql.SQL("{} = %s").format(_sql.Identifier(c)))
                params.append(pk.get(c))
            stmt = _sql.SQL("UPDATE {}.{} SET ")\
                .format(_sql.Identifier(schema), _sql.Identifier(table)) + _sql.SQL(", ").join(set_parts) + _sql.SQL(" WHERE ") + _sql.SQL(" AND ").join(where_parts)
            cur = conn.cursor()
            cur.execute(stmt, params)
            return True
        elif op == 'INSERT':
            values = _filter_existing_columns(conn, schema, table, data or {})
            if not values:
                return False
            cols = list(values.keys())
            params = [values[c] for c in cols]
            placeholders = [_sql.Placeholder() for _ in cols]
            # ON CONFLICT por PK si existe
            cur = conn.cursor()
            if pk_cols:
                set_cols = [c for c in cols if c not in pk_cols]
                on_conf = _sql.SQL(', ').join([_sql.Composed([_sql.Identifier(c), _sql.SQL(' = EXCLUDED.'), _sql.Identifier(c)]) for c in set_cols])
                stmt = _sql.SQL("INSERT INTO {}.{} ({}) VALUES ({}) ON CONFLICT ({}) DO UPDATE SET ")\
                    .format(
                        _sql.Identifier(schema), _sql.Identifier(table),
                        _sql.SQL(', ').join(map(_sql.Identifier, cols)),
                        _sql.SQL(', ').join(placeholders),
                        _sql.SQL(', ').join(map(_sql.Identifier, pk_cols))
                    ) + on_conf
            else:
                stmt = _sql.SQL("INSERT INTO {}.{} ({}) VALUES ({})")\
                    .format(
                        _sql.Identifier(schema), _sql.Identifier(table),
                        _sql.SQL(', ').join(map(_sql.Identifier, cols)),
                        _sql.SQL(', ').join(placeholders)
                    )
            cur.execute(stmt, params)
            return True
    except Exception:
        return False




async def admin_sync_migrate(request: Request):
    return JSONResponse({"detail": "Legacy sync removed. Use PostgreSQL logical replication."}, status_code=410)

async def api_sync_download(request: Request):
    return JSONResponse({"detail": "Legacy sync removed. Use PostgreSQL logical replication."}, status_code=410)


# Evitar 404 de clientes de Vite durante desarrollo: devolver stub vacío
@app.get("/@vite/client")
async def vite_client_stub():
    return Response("// Vite client stub (deshabilitado en esta app)", media_type="application/javascript")

# Endpoint de estado de replicación externa retirado; usamos replicación lógica nativa de PostgreSQL.

# Generar CSS desde QSS de forma automática evitando sobrescritura
try:
    css_path = static_dir / "style.css"
    # Solo regenerar si no existe o si se fuerza explícitamente por ENV
    if (not css_path.exists()) or (os.getenv("REGENERATE_STYLE_CSS", "0").strip() == "1"):
        qss_path = _resolve_existing_dir("styles", "style.qss")
        generate_css_from_qss(qss_path, css_path)
except Exception:
    # Fallback silencioso; la UI seguirá mostrando colores por defecto
    pass


def _get_password() -> str:
    # Leer directamente desde la base de datos para sincronización inmediata
    try:
        db = _get_db()
        if db and hasattr(db, 'obtener_configuracion'):
            pwd = db.obtener_configuracion('owner_password', timeout_ms=700)  # type: ignore
            if isinstance(pwd, str) and pwd.strip():
                return pwd.strip()
    except Exception:
        pass
    # Fallback: contraseña de desarrollador si existe (solo si DB no devuelve valor)
    try:
        if DEV_PASSWORD:
            return str(DEV_PASSWORD).strip()
    except Exception:
        pass
    # Último recurso
    return "admin"


def _resolve_logo_url() -> str:
    # Resuelve el logo desde assets del proyecto principal
    candidates = [
        _resolve_existing_dir("assets") / "gym_logo.png",
        _resolve_existing_dir("assets") / "logo.svg",
        _resolve_existing_dir("webapp", "assets") / "logo.svg",
    ]
    for p in candidates:
        try:
            if p.exists():
                return "/assets/" + p.name
        except Exception:
            continue
    return "/assets/logo.svg"


def _get_db() -> Optional[DatabaseManager]:
    global _db, _db_initializing
    # Fast path
    if _db is not None:
        return _db
    if DatabaseManager is None:
        try:
            logging.error("_get_db: DatabaseManager no disponible (import falló)")
        except Exception:
            pass
        return None
    # Inicialización con bloqueo para evitar carreras entre hilos
    with _db_lock:
        if _db is not None:
            return _db
        _db_initializing = True
        try:
            logging.debug("_get_db: inicializando DatabaseManager (lazy, locked)")
            _db = DatabaseManager()
            # Opcional: crear índices de rendimiento de forma diferida y no bloqueante
            try:
                if hasattr(_db, 'ensure_indexes'):
                    import threading
                    def _defer_ensure_indexes():
                        try:
                            import time, random, logging as _logging
                            # Jitter para evitar competir con otras tareas de arranque
                            time.sleep(random.uniform(1.5, 4.0))
                            try:
                                _db.ensure_indexes()  # type: ignore
                            except Exception as ie:
                                try:
                                    _logging.exception(f"ensure_indexes diferido falló: {ie}")
                                except Exception:
                                    pass
                        except Exception:
                            # No bloquear la inicialización por errores aquí
                            pass
                    threading.Thread(target=_defer_ensure_indexes, daemon=True).start()
            except Exception:
                # No bloquear la inicialización por errores al programar el hilo
                pass
            try:
                # Verificación ligera para asegurar que la conexión está saludable (con timeouts de lectura)
                with _db.get_connection_context() as conn:  # type: ignore
                    cur = conn.cursor()
                    try:
                        if hasattr(_db, '_apply_readonly_timeouts'):
                            _db._apply_readonly_timeouts(cur, lock_ms=500, statement_ms=1200, idle_s=2)  # type: ignore
                    except Exception:
                        pass
                    try:
                        cur.execute("SELECT 1")
                        _ = cur.fetchone()
                    except Exception:
                        try:
                            conn.rollback()
                        except Exception:
                            pass
                logging.debug("_get_db: verificación SELECT 1 OK")
            except Exception as e:
                logging.exception(f"_get_db: verificación de conexión falló tras init: {e}")
                # Invalidar si la verificación falla para permitir reintentos controlados
                _db = None
            # Prefetch asíncrono de credenciales del dueño para respuesta más rápida
            try:
                if _db is not None and hasattr(_db, 'prefetch_owner_credentials_async'):
                    _db.prefetch_owner_credentials_async(ttl_seconds=600)  # type: ignore
            except Exception:
                pass
            if _db is not None:
                try:
                    logging.info("_get_db: DatabaseManager inicializado")
                except Exception:
                    pass
                # Seed inicial: si hay WEBAPP_OWNER_PASSWORD y la DB no tiene valor, guardarlo en configuración
                try:
                    env_pwd = os.getenv("WEBAPP_OWNER_PASSWORD", "").strip() or os.getenv("OWNER_PASSWORD", "").strip()
                    if env_pwd and hasattr(_db, 'obtener_configuracion') and hasattr(_db, 'actualizar_configuracion'):
                        try:
                            current = _db.obtener_configuracion('owner_password')  # type: ignore
                        except Exception:
                            current = None
                        if not current:
                            try:
                                ok = _db.actualizar_configuracion('owner_password', env_pwd)  # type: ignore
                                if ok:
                                    try:
                                        logging.info("_get_db: owner_password sembrada desde variable de entorno (solo inicial)")
                                    except Exception:
                                        pass
                            except Exception:
                                # No bloquear si falla el seed
                                pass
                except Exception:
                    pass
        except Exception as e:
            try:
                logging.exception(f"_get_db: error inicializando DatabaseManager: {e}")
            except Exception:
                pass
            _db = None
        finally:
            _db_initializing = False
    return _db


# Guard sencillo para responder 503 cuando el Circuit Breaker esté abierto
def _circuit_guard_json(db: DatabaseManager, endpoint: str = "") -> Optional[JSONResponse]:
    try:
        if hasattr(db, "is_circuit_open") and callable(getattr(db, "is_circuit_open")):
            if db.is_circuit_open():  # type: ignore
                state = {}
                try:
                    if hasattr(db, "get_circuit_state") and callable(getattr(db, "get_circuit_state")):
                        state = db.get_circuit_state()  # type: ignore
                except Exception:
                    state = {"open": True}
                try:
                    logging.warning(f"{endpoint or '[endpoint]'}: circuito abierto -> 503; state={state}")
                except Exception:
                    pass
                return JSONResponse({
                    "error": "Servicio temporalmente no disponible",
                    "circuit": state,
                }, status_code=503)
    except Exception as e:
        try:
            logging.exception(f"{endpoint or '[endpoint]'}: error comprobando circuito: {e}")
        except Exception:
            pass
    return None


def _force_db_init() -> Optional[DatabaseManager]:
    """Inicializa la instancia global de DB y valida una consulta ligera.

    Se usa en el arranque y como reintento defensivo antes de devolver 500.
    """
    global _db, _db_initializing
    # Log de parámetros de conexión (sin exponer credenciales)
    def _redact_dsn(dsn: str) -> str:
        try:
            import re as _re
            return _re.sub(r"//([^:@]+):([^@]+)@", lambda m: f"//{m.group(1)}:***@", dsn)
        except Exception:
            return "<redacted>"
    try:
        dsn = os.getenv("DATABASE_URL", "").strip()
        db_host = os.getenv("DB_HOST", "").strip()
        db_port = os.getenv("DB_PORT", "").strip()
        db_name = os.getenv("DB_NAME", "").strip()
        try:
            logging.info(f"_force_db_init: intentando iniciar DB host={db_host or '-'} port={db_port or '-'} name={db_name or '-'} url={'present' if dsn else 'missing'} dsn_masked={_redact_dsn(dsn) if dsn else ''}")
        except Exception:
            pass
    except Exception:
        pass
    with _db_lock:
        try:
            # Si ya está inicializado, devolverlo
            if _db is not None:
                return _db
            _db_initializing = True
            _db = DatabaseManager()
            try:
                if hasattr(_db, 'ensure_indexes'):
                    _db.ensure_indexes()  # type: ignore
            except Exception:
                pass
            # Verificación ligera de conexión para evitar estados a medio inicializar
            try:
                with _db.get_connection_context() as conn:  # type: ignore
                    cur = conn.cursor()
                    cur.execute("SELECT 1")
                    _ = cur.fetchone()
                    try:
                        logging.debug("_force_db_init: verificación SELECT 1 OK")
                    except Exception:
                        pass
            except Exception as e:
                try:
                    logging.exception(f"Verificación de DB tras inicializar falló: {e}")
                except Exception:
                    pass
                # Invalidar para que el caller pueda manejar el fallo
                _db = None
            if _db is not None:
                try:
                    logging.info("DatabaseManager inicializado correctamente en startup")
                except Exception:
                    pass
                return _db
        except Exception as e:
            try:
                logging.error(f"Error inicializando DatabaseManager: {e}")
            except Exception:
                pass
            _db = None
            return None
        finally:
            _db_initializing = False


# Inicializar DB en el arranque del servidor para evitar 500 intermitentes
@app.on_event("startup")
async def _startup_init_db():
    try:
        if _get_db() is None:
            _force_db_init()
        pm = _get_pm()
        if pm is not None:
            try:
                pm.asegurar_concepto_cuota_mensual()
            except Exception:
                # No bloquear el arranque si falla la autocreación
                pass
    except Exception:
        # No bloquear el arranque; los endpoints intentarán reintentar
        pass

# Inicio/apagado de motores externos de replicación retirado.
# La replicación se administra desde el servidor PostgreSQL (publications/subscriptions).


def require_owner(request: Request):
    if not request.session.get("logged_in"):
        raise HTTPException(status_code=401, detail="Acceso restringido al dueño")
    return True


def require_gestion_access(request: Request):
    if request.session.get("logged_in") or request.session.get("gestion_profesor_id"):
        return True
    raise HTTPException(status_code=401, detail="Acceso restringido a Gestión")


def start_web_server(db_manager: Optional[DatabaseManager] = None, host: str = "127.0.0.1", port: int = 8003, log_level: str = "info") -> None:
    """Arranca el servidor web en un hilo daemon para no bloquear la UI.

    Si se pasa una instancia de DatabaseManager, se inyecta para que la web la reutilice.
    """
    try:
        global _db
        if db_manager is not None:
            _db = db_manager

        def _run():
            import time as _t
            while True:
                try:
                    # Liberar/limpiar el puerto si está ocupado por otro proceso
                    try:
                        for conn in psutil.net_connections(kind='inet'):
                            laddr = getattr(conn, 'laddr', None)
                            if not laddr:
                                continue
                            if int(getattr(laddr, 'port', 0)) == int(port):
                                pid = conn.pid
                                if pid and pid != os.getpid():
                                    try:
                                        p = psutil.Process(pid)
                                        p.terminate()
                                        p.wait(timeout=2)
                                    except Exception:
                                        try:
                                            p.kill()
                                        except Exception:
                                            pass
                        # pequeña espera para liberar el puerto completamente
                        _t.sleep(0.5)
                    except Exception:
                        pass
                    import uvicorn
                    # Respetar PORT/HOST de entorno en plataformas de hosting
                    env_port = os.getenv("PORT")
                    try:
                        port = int(env_port) if env_port else port
                    except Exception:
                        pass
                    host = os.getenv("HOST", host)
                    config = uvicorn.Config(
                        app,
                        host=host,
                        port=port,
                        log_level=log_level,
                        log_config=None,  # Evita errores de logging sin TTY en ejecutables sin consola
                        loop="asyncio",
                        http="h11",
                        lifespan="off",
                        proxy_headers=(os.getenv("PROXY_HEADERS_ENABLED", "1").strip() in ("1", "true", "yes")),
                    )
                    server = uvicorn.Server(config=config)
                    # Desactivar instalación de manejadores de señales (no permitidos fuera del hilo principal)
                    try:
                        server.install_signal_handlers = lambda: None
                    except Exception:
                        pass
                    # Crear explícitamente un event loop en este hilo y servir
                    try:
                        import asyncio as _asyncio
                        loop = _asyncio.new_event_loop()
                        _asyncio.set_event_loop(loop)
                        loop.run_until_complete(server.serve())
                    except Exception:
                        # Fallback al modo síncrono
                        server.run()
                except Exception:
                    pass
                # Si el servidor se detiene o falla, esperar y reintentar mientras el programa siga abierto
                _t.sleep(1)

        # En ejecutables congelados en Windows, preferir proceso separado para robustez
        try:
            # Importante: en ejecutables congelados en Windows, evitar multiprocessing.
            # Usar siempre hilo dedicado para prevenir re-ejecución del binario (bucle de relanzamiento).
            t = threading.Thread(target=_run, daemon=True)
            t.start()
        except Exception:
            # Fallback defensivo: no bloquear la UI si el hilo falla
            try:
                t = threading.Thread(target=_run, daemon=True)
                t.start()
            except Exception:
                pass
    except Exception:
        pass


# Callback global de reconexión del túnel público (LocalTunnel por defecto)
_public_tunnel_on_reconnect_cb: Optional[Callable[[str], None]] = None

def set_public_tunnel_reconnect_callback(cb: Optional[Callable[[str], None]]):
    """Registra callback de reconexión del túnel público."""
    global _public_tunnel_on_reconnect_cb
    _public_tunnel_on_reconnect_cb = cb

def set_serveo_reconnect_callback(cb: Optional[Callable[[str], None]]):
    """Alias legado: redirige a set_public_tunnel_reconnect_callback."""
    set_public_tunnel_reconnect_callback(cb)

def start_public_tunnel(subdomain: str = "gym-ms-zrk", local_port: int = 8000, on_reconnect: Optional[Callable[[str], None]] = None) -> Optional[str]:
    """No-op de túnel público: devuelve la URL Railway configurada."""
    try:
        url = get_webapp_base_url()
        return url
    except Exception:
        pass
    try:
        import shutil, subprocess, os, threading, re, webbrowser, time, socket

        # Selección de proveedor centrado en LocalTunnel (default 'localtunnel').
        provider = str(os.getenv("TUNNEL_PROVIDER", "localtunnel")).strip().lower()

        # Localizar binario de SSH
        def _find_ssh() -> Optional[str]:
            b = shutil.which("ssh")
            if not b and os.name == "nt":
                cand = r"C:\\Windows\\System32\\OpenSSH\\ssh.exe"
                if os.path.exists(cand):
                    b = cand
            return b

        # Chequear conectividad TCP simple
        def _tcp_open(host: str, port: int, timeout: float = 2.0) -> bool:
            try:
                s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                s.settimeout(timeout)
                res = (s.connect_ex((host, int(port))) == 0)
                try:
                    s.close()
                except Exception:
                    pass
                return res
            except Exception:
                return False

        ssh_bin = _find_ssh()
        if not ssh_bin and provider != "localtunnel":
            print("Cliente SSH no encontrado; no se puede iniciar túnel SSH")
            return None

        # Esperar a que el servicio local esté escuchando
        try:
            deadline = time.time() + 15
            while time.time() < deadline:
                s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                s.settimeout(1.5)
                try:
                    if s.connect_ex(("127.0.0.1", int(local_port))) == 0:
                        break
                finally:
                    try:
                        s.close()
                    except Exception:
                        pass
                time.sleep(0.5)
        except Exception:
            # Si falla la comprobación, continuamos igualmente
            pass

        # Host y puerto SSH configurables para proveedores SSH genéricos
        ssh_host = os.getenv("PUBLIC_TUNNEL_SSH_HOST", "localhost.run").strip()
        ssh_port_env = os.getenv("PUBLIC_TUNNEL_SSH_PORT")
        ssh_port: int
        try:
            ssh_port = int(ssh_port_env) if ssh_port_env else 22
        except Exception:
            ssh_port = 22

        # Si no especificado por ENV, probar conectividad y usar 443 como fallback
        if ssh_port_env is None:
            if not _tcp_open(ssh_host, 22, 1.5):
                # Intento rápido de 443
                if _tcp_open(ssh_host, 443, 1.5):
                    ssh_port = 443
                else:
                    # Intento de flush DNS en Windows y re-test
                    try:
                        if os.name == "nt":
                            subprocess.run(["ipconfig", "/flushdns"], timeout=5)
                    except Exception:
                        pass
                    if _tcp_open(ssh_host, 22, 1.5):
                        ssh_port = 22
                    elif _tcp_open(ssh_host, 443, 1.5):
                        ssh_port = 443
                    else:
                        # No hay conectividad saliente; continuar igualmente para que el supervisor reintente
                        ssh_port = 22

        # Intento de permitir tráfico saliente de ssh.exe en Windows (no crítico, puede requerir admin)
        _startupinfo = None
        try:
            if os.name == "nt":
                _startupinfo = subprocess.STARTUPINFO()
                _startupinfo.dwFlags |= getattr(subprocess, "STARTF_USESHOWWINDOW", 0)
                try:
                    # SW_HIDE para asegurar que la ventana esté oculta
                    _startupinfo.wShowWindow = 0
                except Exception:
                    pass
        except Exception:
            _startupinfo = None
        try:
            if os.name == "nt" and ssh_bin:
                subprocess.run([
                    "netsh", "advfirewall", "firewall", "add", "rule",
                    "name=GymMS SSH Tunnel",
                    "dir=out",
                    f"program={ssh_bin}",
                    "action=allow",
                    "enable=yes"
                ], timeout=4)
        except Exception:
            pass

        # Selección de proveedor centrado en LocalTunnel (default 'localtunnel'). Sin fallback automático.
        # Bandera de verbosidad SSH configurable
        ssh_verbose = str(os.getenv("PUBLIC_TUNNEL_SSH_VERBOSE", "0")).strip().lower() in ("1", "true", "yes")

        # Helper para construir el comando SSH según proveedor y puerto actual
        def _build_cmd() -> list:
            try:
                if provider == "localtunnel":
                    # LocalTunnel: requiere Node. Intentar usar binario 'lt' o 'npx localtunnel'.
                    lt_bin = shutil.which("lt")
                    npx_bin = shutil.which("npx")
                    if lt_bin:
                        base = [lt_bin, "--port", str(local_port), "--subdomain", subdomain]
                    elif npx_bin:
                        base = [npx_bin, "localtunnel", "--port", str(local_port), "--subdomain", subdomain]
                    else:
                        logging.warning("[Tunnel] LocalTunnel no disponible (no se encontró 'lt' ni 'npx').")
                        return []
                    try:
                        logging.info(f"[Tunnel] provider=localtunnel subdomain={subdomain} port={local_port}")
                    except Exception:
                        pass
                    return base
                if provider == "localhost.run":
                    # localhost.run: túnel HTTP gratuito (dominio aleatorio) con usuario opcional
                    lhr_user = str(os.getenv("LHR_SSH_USER", "nokey")).strip()
                    remote_spec = f"80:localhost:{local_port}"
                    host_spec = f"{lhr_user}@localhost.run" if lhr_user else "localhost.run"
                    base = [
                        ssh_bin,
                        "-o", "StrictHostKeyChecking=no",
                        "-o", "ExitOnForwardFailure=yes",
                        "-o", "ServerAliveInterval=60",
                        "-o", "ServerAliveCountMax=3",
                        "-o", "ConnectTimeout=10",
                        "-o", "ConnectionAttempts=3",
                        "-N",
                        "-T",
                        "-R", remote_spec,
                        host_spec,
                    ]
                    if ssh_verbose:
                        base.insert(1, "-vvv")
                    try:
                        logging.info(f"[Tunnel] provider=localhost.run remote={remote_spec} host={host_spec} port=22")
                    except Exception:
                        pass
                    return base
                else:
                    # SSH genérico: sin soporte de subdominio explícito
                    try:
                        logging.info(f"[Tunnel] provider=ssh remote=80:localhost:{local_port} host={ssh_host} port={ssh_port}")
                    except Exception:
                        pass
                    return []
            except Exception:
                # Fallback seguro en caso de error construyendo comando
                return []

        # Comando inicial según proveedor seleccionado
        cmd = _build_cmd()

        # Log del comando inicial
        try:
            logging.debug(f"[PublicTunnel] comando inicial={' '.join(cmd)}")
        except Exception:
            pass

        attempt = 0

        # Última URL pública detectada desde la salida del proveedor
        last_public_url: Optional[str] = None
        # Contador de fallas para proveedores SSH
        ssh_failures = 0

        def _run():
            nonlocal ssh_port, attempt, provider, last_public_url, ssh_failures
            # Supervisor con backoff exponencial para reconectar si el proceso termina
            backoff = 2.0
            max_backoff = 300.0  # 5 minutos máximo
            # Estado para notificaciones de reconexión con anti-spam
            first_start = True
            last_notify_ts = 0.0
            min_notify_interval = 90.0  # segundos
            while True:
                try:
                    attempt += 1
                    had_banner_refused = False
                    # Reconstruir comando por intento para reflejar cambios de puerto o proveedor
                    cmd = _build_cmd()
                    try:
                        logging.info(f"[PublicTunnel] intento={attempt} lanzando proceso provider={provider} puerto={ssh_port}")
                        logging.debug(f"[PublicTunnel] comando={' '.join(cmd)}")
                    except Exception:
                        pass
                    p = subprocess.Popen(
                        cmd,
                        stdout=subprocess.PIPE,
                        stderr=subprocess.STDOUT,
                        text=True,
                        # En Windows, solo CREATE_NO_WINDOW para evitar consola; quitar flags que pueden provocar ventanas.
                        creationflags=(
                            getattr(subprocess, "CREATE_NO_WINDOW", 0)
                        ),
                        startupinfo=_startupinfo,
                        stdin=subprocess.DEVNULL,
                    )
                    try:
                        logging.info(f"[PublicTunnel] proceso lanzado pid={getattr(p, 'pid', None)}")
                    except Exception:
                        pass
                    # Si el primer arranque falla de inmediato y el puerto era 22, probar 443 en siguiente intento
                    try:
                        start_ts = time.time()
                    except Exception:
                        start_ts = 0.0
                    # Notificar reconexión (si no es el primer arranque) con anti-spam
                    cb = on_reconnect or _public_tunnel_on_reconnect_cb
                    if cb and not first_start:
                        now = time.time()
                        if (now - last_notify_ts) >= min_notify_interval:
                            last_notify_ts = now
                            try:
                                cb(f"https://{subdomain}.loca.lt/")
                                logging.info("[PublicTunnel] notificación de reconexión enviada")
                            except Exception:
                                pass
                    # Parsear salida del proceso para capturar URL pública (LocalTunnel)
                    if p.stdout:
                        refused_pattern = re.compile(r"banner exchange:.*Connection refused", re.IGNORECASE)
                        url_pattern = re.compile(r"https://[^\s]+", re.IGNORECASE)
                        try:
                            for line in p.stdout:
                                try:
                                    ln = (line or "").strip()
                                    if ln:
                                        logging.info(f"[PublicTunnel][stdout] {ln}")
                                        if refused_pattern.search(ln):
                                            had_banner_refused = True
                                        # Intentar capturar la URL pública impresa por el proveedor
                                        murl = url_pattern.search(ln)
                                        if murl:
                                            last_public_url = murl.group(0)
                                except Exception:
                                    pass
                        except Exception:
                            # Continuar aunque el parsing de salida falle
                            try:
                                logging.warning("[PublicTunnel] fallo leyendo stdout del proceso (continuando)")
                            except Exception:
                                pass
                            pass
                    # Esperar a que el proceso termine (por desconexión o error)
                    try:
                        p.wait()
                        try:
                            logging.info(f"[PublicTunnel] proceso terminado rc={getattr(p, 'returncode', None)}")
                        except Exception:
                            pass
                    except Exception:
                        pass
                except Exception:
                    # Fallo al lanzar SSH, continuar con backoff
                    try:
                        logging.error("[PublicTunnel] excepción al lanzar proceso", exc_info=True)
                    except Exception:
                        pass
                    pass
                # Marcar que siguientes lanzamientos serán reconexiones
                try:
                    first_start = False
                except Exception:
                    pass
                # Si el proceso terminó muy rápido y estábamos en 22, probar 443 como siguiente intento (solo serveo)
                # Sin alternancia de puertos ni fallback: centrado en LocalTunnel
                try:
                    pass
                except Exception:
                    pass
                # Dormir y aumentar backoff antes de reintentar
                try:
                    time.sleep(backoff)
                except Exception:
                    pass
                backoff = min(backoff * 2.0, max_backoff)

        threading.Thread(target=_run, daemon=True).start()
        # Retornar última URL pública detectada si existe; de lo contrario, URL por defecto según proveedor
        try:
            if last_public_url:
                return last_public_url
            if provider == "localtunnel":
                return f"https://{subdomain}.loca.lt/"
            else:
                return None
        except Exception:
            try:
                if provider == "localtunnel":
                    return f"https://{subdomain}.loca.lt/"
                else:
                    return None
            except Exception:
                return None
    except Exception:
        return None


@app.get("/")
async def root_selector(request: Request):
    """Página de selección: Dashboard (lleva a login) o Check-in."""
    theme_vars = read_theme_vars(static_dir / "style.css")
    ctx = {
        "request": request,
        "theme": theme_vars,
        "gym_name": get_gym_name("Gimnasio"),
        "logo_url": _resolve_logo_url(),
    }
    return templates.TemplateResponse("index.html", ctx)

@app.get("/tunnel/password")
async def tunnel_password():
    """Endpoint legado deshabilitado.

    En configuración Railway no hay contraseña de túnel.
    """
    return JSONResponse({"password": None, "ok": False})

@app.get("/login")
async def login_page_get(request: Request):
    """Muestra el formulario de login del dueño."""
    theme_vars = read_theme_vars(static_dir / "style.css")
    ctx = {
        "request": request,
        "theme": theme_vars,
        "error": request.query_params.get("error"),
        "gym_name": get_gym_name("Gimnasio"),
        "logo_url": _resolve_logo_url(),
    }
    return templates.TemplateResponse("login.html", ctx)


@app.post("/login")
async def do_login(request: Request):
    content_type = request.headers.get("content-type", "")
    if content_type.startswith("application/x-www-form-urlencoded"):
        data = await request.form()
    elif content_type.startswith("application/json"):
        data = await request.json()
    else:
        data = {}
    password = str(data.get("password", "")).strip()
    if not password:
        return RedirectResponse(url="/login?error=Ingrese%20la%20contrase%C3%B1a", status_code=303)
    ok = False
    # Contraseña única: obtenida por _get_password() (BD -> ENV [solo seed] -> DEV como último recurso)
    if password == _get_password():
        ok = True
    # Nota: Se ha eliminado el uso del PIN del dueño para autenticación web
    if ok:
        request.session["logged_in"] = True
        request.session["role"] = "dueño"
        return RedirectResponse(url="/dashboard", status_code=303)
    return RedirectResponse(url="/login?error=Credenciales%20inv%C3%A1lidas", status_code=303)


@app.post("/logout")
async def do_logout(request: Request, _=Depends(require_owner)):
    # Limpiar sesión y redirigir al login para evitar quedarse en una página JSON
    request.session.clear()
    return RedirectResponse(url="/", status_code=303)

@app.get("/logout")
async def logout_get(request: Request):
    # Soporta enlaces directos GET al logout y redirige al login
    request.session.clear()
    return RedirectResponse(url="/", status_code=303)

# --- Endpoint admin para actualizar la contraseña del dueño ---
@app.post("/api/admin/owner-password")
async def set_owner_password(request: Request):
    """
    Actualiza la contraseña del dueño en la base de datos.
    Acceso restringido exclusivamente mediante DEV_PASSWORD.
    """
    try:
        content_type = request.headers.get("content-type", "")
        if content_type.startswith("application/json"):
            data = await request.json()
        else:
            # Aceptar x-www-form-urlencoded como fallback
            data = await request.form()
    except Exception:
        data = {}

    dev_pwd = str(data.get("dev_password", "")).strip()
    new_pwd = str(data.get("new_password", "")).strip()

    if not dev_pwd or not new_pwd:
        return JSONResponse({"success": False, "message": "Parámetros inválidos"}, status_code=400)
    if len(new_pwd) < 4:
        return JSONResponse({"success": False, "message": "La nueva contraseña debe tener al menos 4 caracteres"}, status_code=400)

    # Resolver DEV_PASSWORD real
    real_dev = None
    try:
        if DEV_PASSWORD:
            real_dev = str(DEV_PASSWORD).strip()
    except Exception:
        real_dev = None
    if not real_dev:
        try:
            from managers import DeveloperManager  # type: ignore
            real_dev = str(getattr(DeveloperManager, "DEV_PASSWORD", "") or "").strip()
        except Exception:
            real_dev = None
    if not real_dev:
        # Último intento: variable de entorno
        real_dev = os.getenv("DEV_PASSWORD", "").strip()

    if not real_dev or dev_pwd != real_dev:
        return JSONResponse({"success": False, "message": "No autorizado"}, status_code=401)

    db = _get_db()
    if not db:
        return JSONResponse({"success": False, "message": "DatabaseManager no disponible"}, status_code=500)

    try:
        ok = False
        if hasattr(db, 'actualizar_configuracion'):
            ok = bool(db.actualizar_configuracion('owner_password', new_pwd))  # type: ignore
        else:
            # Fallback SQL directo si el método no existe
            with db.get_connection_context() as conn:  # type: ignore
                cur = conn.cursor()
                # Crear tabla/config si fuera necesario (idempotente)
                try:
                    cur.execute("""
                        CREATE TABLE IF NOT EXISTS configuracion (
                            clave TEXT PRIMARY KEY,
                            valor TEXT
                        )
                    """)
                except Exception:
                    pass
                cur.execute(
                    """
                    INSERT INTO configuracion (clave, valor)
                    VALUES ('owner_password', %s)
                    ON CONFLICT (clave) DO UPDATE SET valor = EXCLUDED.valor
                    """,
                    (new_pwd,),
                )
                conn.commit()
                ok = True
        if ok:
            # Refrescar caché si existe
            try:
                if hasattr(db, 'prefetch_owner_credentials_async'):
                    db.prefetch_owner_credentials_async(ttl_seconds=0)  # type: ignore
            except Exception:
                pass
            return JSONResponse({"success": True, "message": "Contraseña actualizada"})
        return JSONResponse({"success": False, "message": "No se pudo actualizar la contraseña"}, status_code=500)
    except Exception as e:
        try:
            logging.exception("Error en /api/admin/owner-password")
        except Exception:
            pass
        return JSONResponse({"success": False, "message": str(e)}, status_code=500)


@app.get("/dashboard")
async def dashboard(request: Request):
    # Redirigir a login si no hay sesión activa
    if not request.session.get("logged_in"):
        return RedirectResponse(url="/", status_code=303)
    theme_vars = read_theme_vars(static_dir / "style.css")
    ctx = {
        "request": request,
        "theme": theme_vars,
        "gym_name": get_gym_name("Gimnasio"),
        "logo_url": _resolve_logo_url(),
    }
    return templates.TemplateResponse("dashboard.html", ctx)

@app.get("/gestion")
async def gestion(request: Request):
    # Redirigir a login de Gestión si no hay sesión activa (dueño o profesor)
    if not (request.session.get("logged_in") or request.session.get("gestion_profesor_id")):
        return RedirectResponse(url="/gestion/login", status_code=303)
    theme_vars = read_theme_vars(static_dir / "style.css")
    ctx = {
        "request": request,
        "theme": theme_vars,
        "gym_name": get_gym_name("Gimnasio"),
        "logo_url": _resolve_logo_url(),
    }
    return templates.TemplateResponse("gestion.html", ctx)

# --- API Usuarios (CRUD) ---
@app.get("/api/usuarios")
async def api_usuarios_list(q: Optional[str] = None, limit: int = 50, offset: int = 0, _=Depends(require_gestion_access)):
    db = _get_db()
    if db is None:
        return []
    guard = _circuit_guard_json(db, "/api/usuarios")
    if guard:
        return guard
    try:
        with db.get_connection_context() as conn:  # type: ignore
            cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            sql = """
                SELECT id, nombre, dni, telefono, rol, tipo_cuota, activo, fecha_registro
                FROM usuarios
                WHERE TRUE
            """
            params: list = []
            if q:
                q_like = f"%{q.strip()}%"
                sql += " AND (LOWER(nombre) LIKE LOWER(%s) OR CAST(dni AS TEXT) LIKE %s OR CAST(telefono AS TEXT) LIKE %s)"
                params.extend([q_like, q_like, q_like])
            sql += " ORDER BY nombre ASC LIMIT %s OFFSET %s"
            params.extend([limit, offset])
            cur.execute(sql, params)
            rows = cur.fetchall() or []
            for r in rows:
                r["nombre"] = (r.get("nombre") or "").strip()
                r["rol"] = (r.get("rol") or "").strip().lower()
            return rows
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)

@app.get("/api/usuarios/{usuario_id}")
async def api_usuario_get(usuario_id: int, _=Depends(require_gestion_access)):
    db = _get_db()
    if db is None:
        raise HTTPException(status_code=503, detail="DB no disponible")
    guard = _circuit_guard_json(db, "/api/usuarios/{id}")
    if guard:
        return guard
    try:
        u = db.obtener_usuario_por_id(usuario_id)  # type: ignore
        if not u:
            raise HTTPException(status_code=404, detail="Usuario no encontrado")
        return {
            "id": u.id,
            "nombre": u.nombre,
            "dni": u.dni,
            "telefono": u.telefono,
            "pin": u.pin,
            "rol": u.rol,
            "activo": bool(u.activo),
            "tipo_cuota": u.tipo_cuota,
            "notas": u.notas,
            "fecha_registro": u.fecha_registro,
            "fecha_proximo_vencimiento": u.fecha_proximo_vencimiento,
            "cuotas_vencidas": u.cuotas_vencidas,
            "ultimo_pago": u.ultimo_pago,
        }
    except HTTPException:
        raise
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)

@app.post("/api/usuarios")
async def api_usuario_create(request: Request, _=Depends(require_gestion_access)):
    db = _get_db()
    if db is None:
        raise HTTPException(status_code=503, detail="DB no disponible")
    guard = _circuit_guard_json(db, "/api/usuarios")
    if guard:
        return guard
    payload = await request.json()
    try:
        nombre = (payload.get("nombre") or "").strip()
        dni = str(payload.get("dni") or "").strip()
        telefono = str(payload.get("telefono") or "").strip() or None
        pin = str(payload.get("pin") or "").strip() or None
        rol = (payload.get("rol") or "socio").strip().lower()
        activo = bool(payload.get("activo", True))
        tipo_cuota = payload.get("tipo_cuota")
        notas = payload.get("notas")
        if not nombre or not dni:
            raise HTTPException(status_code=400, detail="'nombre' y 'dni' son obligatorios")
        if db.dni_existe(dni):  # type: ignore
            raise HTTPException(status_code=400, detail="DNI ya existe")
        usuario = Usuario(  # type: ignore
            id=None,
            nombre=nombre,
            dni=dni,
            telefono=telefono,
            pin=pin,
            rol=rol,
            notas=notas,
            fecha_registro=datetime.now(timezone.utc).isoformat(),
            activo=activo,
            tipo_cuota=tipo_cuota,
            fecha_proximo_vencimiento=None,
            cuotas_vencidas=0,
            ultimo_pago=None,
        )
        new_id = db.crear_usuario(usuario)  # type: ignore
        return {"id": new_id}
    except HTTPException:
        raise
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)

@app.put("/api/usuarios/{usuario_id}")
async def api_usuario_update(usuario_id: int, request: Request, _=Depends(require_gestion_access)):
    db = _get_db()
    if db is None:
        raise HTTPException(status_code=503, detail="DB no disponible")
    guard = _circuit_guard_json(db, "/api/usuarios/{id}")
    if guard:
        return guard
    payload = await request.json()
    try:
        if not db.usuario_id_existe(usuario_id):  # type: ignore
            raise HTTPException(status_code=404, detail="Usuario no encontrado")
        nombre = (payload.get("nombre") or "").strip()
        dni = str(payload.get("dni") or "").strip()
        telefono = str(payload.get("telefono") or "").strip() or None
        pin = str(payload.get("pin") or "").strip() or None
        rol = (payload.get("rol") or "socio").strip().lower()
        activo = bool(payload.get("activo", True))
        tipo_cuota = payload.get("tipo_cuota")
        notas = payload.get("notas")
        if not nombre or not dni:
            raise HTTPException(status_code=400, detail="'nombre' y 'dni' son obligatorios")
        if db.dni_existe(dni, usuario_id):  # type: ignore
            raise HTTPException(status_code=400, detail="DNI ya existe")
        usuario = Usuario(  # type: ignore
            id=usuario_id,
            nombre=nombre,
            dni=dni,
            telefono=telefono,
            pin=pin,
            rol=rol,
            notas=notas,
            fecha_registro=None,
            activo=activo,
            tipo_cuota=tipo_cuota,
            fecha_proximo_vencimiento=None,
            cuotas_vencidas=None,
            ultimo_pago=None,
        )
        db.actualizar_usuario(usuario)  # type: ignore
        return {"ok": True}
    except HTTPException:
        raise
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)

@app.delete("/api/usuarios/{usuario_id}")
async def api_usuario_delete(usuario_id: int, _=Depends(require_gestion_access)):
    db = _get_db()
    if db is None:
        raise HTTPException(status_code=503, detail="DB no disponible")
    guard = _circuit_guard_json(db, "/api/usuarios/{id}")
    if guard:
        return guard
    try:
        db.eliminar_usuario(usuario_id)  # type: ignore
        return {"ok": True}
    except PermissionError as e:
        raise HTTPException(status_code=403, detail=str(e))
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)

# --- API Metadatos de pago ---
@app.get("/api/metodos_pago")
async def api_metodos_pago(_=Depends(require_gestion_access)):
    db = _get_db()
    if db is None:
        return []
    guard = _circuit_guard_json(db, "/api/metodos_pago")
    if guard:
        return guard
    try:
        with db.get_connection_context() as conn:  # type: ignore
            cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            cur.execute("SELECT id, nombre, activo, color, comision, icono FROM metodos_pago ORDER BY nombre")
            return cur.fetchall() or []
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)

@app.post("/api/metodos_pago")
async def api_metodos_pago_create(request: Request, _=Depends(require_gestion_access)):
    pm = _get_pm()
    db = _get_db()
    if db is None:
        raise HTTPException(status_code=503, detail="DB no disponible")
    guard = _circuit_guard_json(db, "/api/metodos_pago[POST]")
    if guard:
        return guard
    if pm is None or MetodoPago is None:
        raise HTTPException(status_code=503, detail="PaymentManager o modelo MetodoPago no disponible")
    payload = await request.json()
    try:
        nombre = (payload.get("nombre") or "").strip()
        if not nombre:
            raise HTTPException(status_code=400, detail="'nombre' es obligatorio")
        icono = payload.get("icono")
        color = (payload.get("color") or "#3498db").strip() or "#3498db"
        comision_raw = payload.get("comision")
        comision = float(comision_raw) if comision_raw is not None else 0.0
        if comision < 0 or comision > 100:
            raise HTTPException(status_code=400, detail="'comision' debe estar entre 0 y 100")
        activo = bool(payload.get("activo", True))
        descripcion = payload.get("descripcion")
        metodo = MetodoPago(nombre=nombre, icono=icono, color=color, comision=comision, activo=activo, descripcion=descripcion)  # type: ignore
        new_id = pm.crear_metodo_pago(metodo)
        return {"ok": True, "id": int(new_id)}
    except ValueError as ve:
        raise HTTPException(status_code=400, detail=str(ve))
    except HTTPException:
        raise
    except Exception as e:
        import traceback
        traceback.print_exc()
        return JSONResponse({"error": str(e)}, status_code=500)

@app.put("/api/metodos_pago/{metodo_id}")
async def api_metodos_pago_update(metodo_id: int, request: Request, _=Depends(require_gestion_access)):
    pm = _get_pm()
    db = _get_db()
    if db is None:
        raise HTTPException(status_code=503, detail="DB no disponible")
    guard = _circuit_guard_json(db, f"/api/metodos_pago/{metodo_id}[PUT]")
    if guard:
        return guard
    if pm is None or MetodoPago is None:
        raise HTTPException(status_code=503, detail="PaymentManager o modelo MetodoPago no disponible")
    payload = await request.json()
    try:
        existing = pm.obtener_metodo_pago(int(metodo_id))
        if not existing:
            raise HTTPException(status_code=404, detail="Método de pago no encontrado")
        nombre = (payload.get("nombre") or existing.nombre or "").strip() or existing.nombre
        icono = payload.get("icono") if ("icono" in payload) else existing.icono
        color = (payload.get("color") or existing.color or "#3498db").strip() or existing.color
        comision = float(payload.get("comision")) if (payload.get("comision") is not None) else float(existing.comision or 0.0)
        if comision < 0 or comision > 100:
            raise HTTPException(status_code=400, detail="'comision' debe estar entre 0 y 100")
        activo = bool(payload.get("activo")) if ("activo" in payload) else bool(existing.activo)
        descripcion = payload.get("descripcion") if ("descripcion" in payload) else existing.descripcion
        metodo = MetodoPago(id=int(metodo_id), nombre=nombre, icono=icono, color=color, comision=comision, activo=activo, descripcion=descripcion)  # type: ignore
        updated = pm.actualizar_metodo_pago(metodo)
        if not updated:
            raise HTTPException(status_code=404, detail="No se pudo actualizar el método de pago")
        return {"ok": True, "id": int(metodo_id)}
    except ValueError as ve:
        raise HTTPException(status_code=400, detail=str(ve))
    except HTTPException:
        raise
    except Exception as e:
        import traceback
        traceback.print_exc()
        return JSONResponse({"error": str(e)}, status_code=500)

@app.delete("/api/metodos_pago/{metodo_id}")
async def api_metodos_pago_delete(metodo_id: int, _=Depends(require_gestion_access)):
    pm = _get_pm()
    db = _get_db()
    if db is None:
        raise HTTPException(status_code=503, detail="DB no disponible")
    guard = _circuit_guard_json(db, f"/api/metodos_pago/{metodo_id}[DELETE]")
    if guard:
        return guard
    if pm is None:
        raise HTTPException(status_code=503, detail="PaymentManager no disponible")
    try:
        deleted = pm.eliminar_metodo_pago(int(metodo_id))
        if not deleted:
            raise HTTPException(status_code=404, detail="No se pudo eliminar el método de pago")
        return {"ok": True}
    except ValueError as ve:
        raise HTTPException(status_code=400, detail=str(ve))
    except HTTPException:
        raise
    except Exception as e:
        import traceback
        traceback.print_exc()
        return JSONResponse({"error": str(e)}, status_code=500)

@app.get("/api/conceptos_pago")
async def api_conceptos_pago(_=Depends(require_gestion_access)):
    db = _get_db()
    if db is None:
        return []
    guard = _circuit_guard_json(db, "/api/conceptos_pago")
    if guard:
        return guard
    try:
        with db.get_connection_context() as conn:  # type: ignore
            cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            cur.execute("SELECT id, nombre, descripcion, precio_base, tipo, activo FROM conceptos_pago ORDER BY nombre")
            return cur.fetchall() or []
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)

@app.post("/api/conceptos_pago")
async def api_conceptos_pago_create(request: Request, _=Depends(require_gestion_access)):
    pm = _get_pm()
    db = _get_db()
    if db is None:
        raise HTTPException(status_code=503, detail="DB no disponible")
    guard = _circuit_guard_json(db, "/api/conceptos_pago[POST]")
    if guard:
        return guard
    if pm is None or ConceptoPago is None:
        raise HTTPException(status_code=503, detail="PaymentManager o modelo ConceptoPago no disponible")
    payload = await request.json()
    try:
        nombre = (payload.get("nombre") or "").strip()
        if not nombre:
            raise HTTPException(status_code=400, detail="'nombre' es obligatorio")
        descripcion = payload.get("descripcion")
        precio_base_raw = payload.get("precio_base")
        precio_base = float(precio_base_raw) if precio_base_raw is not None else 0.0
        if precio_base < 0:
            raise HTTPException(status_code=400, detail="'precio_base' no puede ser negativo")
        tipo = (payload.get("tipo") or "fijo").strip().lower()
        if tipo not in ["fijo", "variable"]:
            raise HTTPException(status_code=400, detail="'tipo' debe ser 'fijo' o 'variable'")
        activo = bool(payload.get("activo", True))
        concepto = ConceptoPago(nombre=nombre, descripcion=descripcion, precio_base=precio_base, tipo=tipo, activo=activo)  # type: ignore
        new_id = pm.crear_concepto_pago(concepto)
        return {"ok": True, "id": int(new_id)}
    except ValueError as ve:
        raise HTTPException(status_code=400, detail=str(ve))
    except HTTPException:
        raise
    except Exception as e:
        import traceback
        traceback.print_exc()
        return JSONResponse({"error": str(e)}, status_code=500)

@app.put("/api/conceptos_pago/{concepto_id}")
async def api_conceptos_pago_update(concepto_id: int, request: Request, _=Depends(require_gestion_access)):
    pm = _get_pm()
    db = _get_db()
    if db is None:
        raise HTTPException(status_code=503, detail="DB no disponible")
    guard = _circuit_guard_json(db, f"/api/conceptos_pago/{concepto_id}[PUT]")
    if guard:
        return guard
    if pm is None or ConceptoPago is None:
        raise HTTPException(status_code=503, detail="PaymentManager o modelo ConceptoPago no disponible")
    payload = await request.json()
    try:
        # Como no hay método para obtener un concepto individual, consultamos SQL para merge
        with db.get_connection_context() as conn:  # type: ignore
            cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            cur.execute(
                "SELECT id, nombre, descripcion, precio_base, tipo, activo FROM conceptos_pago WHERE id = %s",
                (int(concepto_id),)
            )
            row = cur.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Concepto de pago no encontrado")
        nombre = (payload.get("nombre") or row.get("nombre") or "").strip() or row.get("nombre")
        descripcion = payload.get("descripcion") if ("descripcion" in payload) else row.get("descripcion")
        precio_base = float(payload.get("precio_base")) if (payload.get("precio_base") is not None) else float(row.get("precio_base") or 0.0)
        if precio_base < 0:
            raise HTTPException(status_code=400, detail="'precio_base' no puede ser negativo")
        tipo = (payload.get("tipo") or row.get("tipo") or "fijo").strip().lower()
        if tipo not in ["fijo", "variable"]:
            raise HTTPException(status_code=400, detail="'tipo' debe ser 'fijo' o 'variable'")
        activo = bool(payload.get("activo")) if ("activo" in payload) else bool(row.get("activo"))
        categoria = (payload.get("categoria") or row.get("categoria") or "general").strip().lower()
        concepto = ConceptoPago(id=int(concepto_id), nombre=nombre, descripcion=descripcion, precio_base=precio_base, tipo=tipo, activo=activo, categoria=categoria)  # type: ignore
        updated = pm.actualizar_concepto_pago(concepto)
        if not updated:
            raise HTTPException(status_code=404, detail="No se pudo actualizar el concepto de pago")
        return {"ok": True, "id": int(concepto_id)}
    except ValueError as ve:
        raise HTTPException(status_code=400, detail=str(ve))
    except HTTPException:
        raise
    except Exception as e:
        import traceback
        traceback.print_exc()
        return JSONResponse({"error": str(e)}, status_code=500)

@app.delete("/api/conceptos_pago/{concepto_id}")
async def api_conceptos_pago_delete(concepto_id: int, _=Depends(require_gestion_access)):
    pm = _get_pm()
    db = _get_db()
    if db is None:
        raise HTTPException(status_code=503, detail="DB no disponible")
    guard = _circuit_guard_json(db, f"/api/conceptos_pago/{concepto_id}[DELETE]")
    if guard:
        return guard
    if pm is None:
        raise HTTPException(status_code=503, detail="PaymentManager no disponible")
    try:
        deleted = pm.eliminar_concepto_pago(int(concepto_id))
        if not deleted:
            raise HTTPException(status_code=404, detail="No se pudo eliminar el concepto de pago")
        return {"ok": True}
    except ValueError as ve:
        raise HTTPException(status_code=400, detail=str(ve))
    except HTTPException:
        raise
    except Exception as e:
        import traceback
        traceback.print_exc()
        return JSONResponse({"error": str(e)}, status_code=500)

@app.get("/api/tipos_cuota_activos")
async def api_tipos_cuota_activos(_=Depends(require_gestion_access)):
    db = _get_db()
    if db is None:
        return []
    guard = _circuit_guard_json(db, "/api/tipos_cuota_activos")
    if guard:
        return guard
    try:
        # Preferir SQL directo para robustez
        with db.get_connection_context() as conn:  # type: ignore
            cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            cur.execute("SELECT id, nombre, precio, duracion_dias, activo FROM tipos_cuota WHERE activo = true ORDER BY precio ASC, nombre ASC")
            rows = cur.fetchall() or []
            for r in rows:
                r["nombre"] = (r.get("nombre") or "").strip()
            return rows
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)

# --- CRUD Tipos de Cuota ---
@app.get("/api/tipos_cuota_catalogo")
async def api_tipos_cuota_catalogo(_=Depends(require_gestion_access)):
    db = _get_db()
    if db is None:
        return []
    guard = _circuit_guard_json(db, "/api/tipos_cuota_catalogo")
    if guard:
        return guard
    try:
        with db.get_connection_context() as conn:  # type: ignore
            cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            cur.execute("SELECT id, nombre, precio, duracion_dias, activo, descripcion, icono_path FROM tipos_cuota ORDER BY activo DESC, precio ASC, nombre ASC")
            rows = cur.fetchall() or []
            for r in rows:
                r["nombre"] = (r.get("nombre") or "").strip()
            return rows
    except Exception as e:
        import traceback
        traceback.print_exc()
        return JSONResponse({"error": str(e)}, status_code=500)

@app.post("/api/tipos_cuota")
async def api_tipos_cuota_create(request: Request, _=Depends(require_gestion_access)):
    db = _get_db()
    if db is None:
        raise HTTPException(status_code=503, detail="DB no disponible")
    guard = _circuit_guard_json(db, "/api/tipos_cuota[POST]")
    if guard:
        return guard
    payload = await request.json()
    try:
        nombre = (payload.get("nombre") or "").strip()
        if not nombre:
            raise HTTPException(status_code=400, detail="'nombre' es obligatorio")
        precio_raw = payload.get("precio")
        precio = float(precio_raw) if precio_raw is not None else 0.0
        if precio < 0:
            raise HTTPException(status_code=400, detail="'precio' no puede ser negativo")
        duracion_raw = payload.get("duracion_dias")
        duracion_dias = int(duracion_raw) if duracion_raw is not None else 30
        if duracion_dias <= 0:
            raise HTTPException(status_code=400, detail="'duracion_dias' debe ser > 0")
        activo = bool(payload.get("activo", True))
        descripcion = payload.get("descripcion")
        icono_path = payload.get("icono_path")
        with db.get_connection_context() as conn:  # type: ignore
            from psycopg2 import sql as _sql
            data = {
                "nombre": nombre,
                "precio": precio,
                "duracion_dias": duracion_dias,
                "activo": activo,
                "descripcion": descripcion,
                "icono_path": icono_path,
            }
            filtered = _filter_existing_columns(conn, "public", "tipos_cuota", data)
            if not filtered:
                raise HTTPException(status_code=400, detail="No hay columnas válidas para insertar")
            cols = list(filtered.keys())
            stmt = _sql.SQL("INSERT INTO {}.{} ({}) VALUES ({}) RETURNING id").format(
                _sql.Identifier("public"),
                _sql.Identifier("tipos_cuota"),
                _sql.SQL(", ").join([_sql.Identifier(c) for c in cols]),
                _sql.SQL(", ").join([_sql.Placeholder() for _ in cols]),
            )
            cur = conn.cursor()
            cur.execute(stmt, [filtered[c] for c in cols])
            new_id_row = cur.fetchone()
            new_id = int(new_id_row[0]) if new_id_row else None
            if new_id is None:
                raise HTTPException(status_code=500, detail="No se pudo crear el tipo de cuota")
            return {"ok": True, "id": new_id}
    except ValueError as ve:
        raise HTTPException(status_code=400, detail=str(ve))
    except HTTPException:
        raise
    except Exception as e:
        import traceback
        traceback.print_exc()
        return JSONResponse({"error": str(e)}, status_code=500)

@app.put("/api/tipos_cuota/{tipo_id}")
async def api_tipos_cuota_update(tipo_id: int, request: Request, _=Depends(require_gestion_access)):
    db = _get_db()
    if db is None:
        raise HTTPException(status_code=503, detail="DB no disponible")
    guard = _circuit_guard_json(db, f"/api/tipos_cuota/{tipo_id}[PUT]")
    if guard:
        return guard
    payload = await request.json()
    try:
        with db.get_connection_context() as conn:  # type: ignore
            cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            cur.execute("SELECT id, nombre, precio, duracion_dias, activo, descripcion, icono_path FROM tipos_cuota WHERE id = %s", (int(tipo_id),))
            row = cur.fetchone()
            if not row:
                raise HTTPException(status_code=404, detail="Tipo de cuota no encontrado")
        nombre = (payload.get("nombre") or row.get("nombre") or "").strip() or row.get("nombre")
        precio = float(payload.get("precio")) if (payload.get("precio") is not None) else float(row.get("precio") or 0.0)
        if precio < 0:
            raise HTTPException(status_code=400, detail="'precio' no puede ser negativo")
        duracion_dias = int(payload.get("duracion_dias")) if (payload.get("duracion_dias") is not None) else int(row.get("duracion_dias") or 30)
        if duracion_dias <= 0:
            raise HTTPException(status_code=400, detail="'duracion_dias' debe ser > 0")
        activo = bool(payload.get("activo")) if ("activo" in payload) else bool(row.get("activo"))
        descripcion = payload.get("descripcion") if ("descripcion" in payload) else row.get("descripcion")
        icono_path = payload.get("icono_path") if ("icono_path" in payload) else row.get("icono_path")
        updates = {
            "nombre": nombre,
            "precio": precio,
            "duracion_dias": duracion_dias,
            "activo": activo,
            "descripcion": descripcion,
            "icono_path": icono_path,
        }
        with db.get_connection_context() as conn:  # type: ignore
            ok = _apply_change_idempotent(conn, "public", "tipos_cuota", "UPDATE", {"id": int(tipo_id)}, updates)
            if not ok:
                raise HTTPException(status_code=500, detail="No se pudo actualizar el tipo de cuota")
            return {"ok": True, "id": int(tipo_id)}
    except ValueError as ve:
        raise HTTPException(status_code=400, detail=str(ve))
    except HTTPException:
        raise
    except Exception as e:
        import traceback
        traceback.print_exc()
        return JSONResponse({"error": str(e)}, status_code=500)

@app.delete("/api/tipos_cuota/{tipo_id}")
async def api_tipos_cuota_delete(tipo_id: int, _=Depends(require_gestion_access)):
    db = _get_db()
    if db is None:
        raise HTTPException(status_code=503, detail="DB no disponible")
    guard = _circuit_guard_json(db, f"/api/tipos_cuota/{tipo_id}[DELETE]")
    if guard:
        return guard
    try:
        with db.get_connection_context() as conn:  # type: ignore
            ok = _apply_change_idempotent(conn, "public", "tipos_cuota", "DELETE", {"id": int(tipo_id)}, {})
            if not ok:
                raise HTTPException(status_code=500, detail="No se pudo eliminar el tipo de cuota")
            return {"ok": True}
    except HTTPException:
        raise
    except Exception as e:
        import traceback
        traceback.print_exc()
        return JSONResponse({"error": str(e)}, status_code=500)

@app.get("/gestion/login")
async def gestion_login_get(request: Request):
    theme_vars = read_theme_vars(static_dir / "style.css")
    ctx = {
        "request": request,
        "theme": theme_vars,
        "error": request.query_params.get("error"),
        "gym_name": get_gym_name("Gimnasio"),
        "logo_url": _resolve_logo_url(),
    }
    return templates.TemplateResponse("gestion_login.html", ctx)

@app.get("/api/profesores_basico")
async def api_profesores_basico():
    db = _get_db()
    if db is None:
        return []
    try:
        with db.get_connection_context() as conn:  # type: ignore
            cur = conn.cursor()
            cur.execute(
                """
                SELECT p.id AS profesor_id,
                       u.id AS usuario_id,
                       COALESCE(u.nombre,'') AS nombre
                FROM profesores p
                JOIN usuarios u ON u.id = p.usuario_id
                ORDER BY p.id
                """
            )
            res = []
            for r in cur.fetchall():
                res.append({
                    "profesor_id": int(r[0] or 0),
                    "usuario_id": int(r[1] or 0),
                    "nombre": r[2] or ""
                })
            return res
    except Exception:
        try:
            logging.exception("Error en /api/profesores_basico")
        except Exception:
            pass
        return []

@app.post("/gestion/auth")
async def gestion_auth(request: Request):
    try:
        content_type = request.headers.get("content-type", "")
        if content_type.startswith("application/json"):
            data = await request.json()
        else:
            data = await request.form()
    except Exception:
        data = {}
    usuario_id_raw = data.get("usuario_id")
    owner_password = str(data.get("owner_password", "")).strip()
    pin_raw = data.get("pin")

    db = _get_db()
    if db is None:
        return JSONResponse({"success": False, "message": "DatabaseManager no disponible"}, status_code=500)

    # Modo Dueño: usuario_id == "__OWNER__" y contraseña
    if isinstance(usuario_id_raw, str) and usuario_id_raw == "__OWNER__":
        if not owner_password:
            return RedirectResponse(url="/gestion/login?error=Ingrese%20la%20contrase%C3%B1a", status_code=303)
        if owner_password == _get_password():
            request.session.clear()
            request.session["logged_in"] = True
            request.session["role"] = "dueño"
            # Dueño también puede entrar a Gestión
            return RedirectResponse(url="/gestion", status_code=303)
        return RedirectResponse(url="/gestion/login?error=Credenciales%20inv%C3%A1lidas", status_code=303)

    # Modo Profesor: usuario_id numérico y PIN
    try:
        usuario_id = int(usuario_id_raw) if usuario_id_raw is not None else None
    except Exception:
        usuario_id = None
    pin = str(pin_raw or "").strip()
    if not usuario_id or not pin:
        return RedirectResponse(url="/gestion/login?error=Par%C3%A1metros%20inv%C3%A1lidos", status_code=303)

    ok = False
    try:
        ok = bool(db.verificar_pin_usuario(usuario_id, pin))  # type: ignore
    except Exception:
        ok = False
    if not ok:
        return RedirectResponse(url="/gestion/login?error=PIN%20inv%C3%A1lido", status_code=303)

    profesor_id = None
    try:
        prof = db.obtener_profesor_por_usuario_id(usuario_id)  # type: ignore
        if prof and hasattr(prof, 'profesor_id'):
            profesor_id = getattr(prof, 'profesor_id', None)
    except Exception:
        profesor_id = None

    request.session["gestion_profesor_user_id"] = usuario_id
    if profesor_id:
        request.session["gestion_profesor_id"] = int(profesor_id)
    return RedirectResponse(url="/gestion", status_code=303)


@app.get("/api/theme")
async def api_theme(_=Depends(require_owner)):
    try:
        theme_vars = read_theme_vars(static_dir / "style.css")
        return JSONResponse(theme_vars)
    except Exception as e:
        logging.exception("Error in /api/theme")
        return JSONResponse({"error": str(e)}, status_code=500)


@app.get("/api/kpis")
async def api_kpis(_=Depends(require_owner)):
    db = _get_db()
    if db is None:
        return {"kpis": {"total_activos": 0, "nuevos_30_dias": 0, "ingresos_mes_actual": 0.0, "asistencias_hoy": 0}, "arpu": 0.0, "morosos": 0}
    guard = _circuit_guard_json(db, "/api/kpis")
    if guard:
        return guard
    try:
        data = db.obtener_kpis_generales()
        arpu, morosos = db.obtener_arpu_y_morosos_mes_actual()
        return {"kpis": data, "arpu": arpu, "morosos": morosos}
    except Exception as e:
        logging.exception("Error in /api/kpis")
        return JSONResponse({"error": str(e)}, status_code=500)


@app.get("/api/ingresos12m")
async def api_ingresos12m(request: Request, _=Depends(require_owner)):
    db = _get_db()
    result: Dict[str, float] = {}
    # Sin DB no podemos calcular; devolvemos estructura vacía
    if db is None:
        return {"ingresos": result}
    guard = _circuit_guard_json(db, "/api/ingresos12m")
    if guard:
        return guard
    try:
        # Calcular ingresos por mes con SQL directo respetando rango si se provee
        start = request.query_params.get("start")
        end = request.query_params.get("end")
        with db.get_connection_context() as conn:  # type: ignore
            cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            if start and end:
                cur.execute(
                    """
                    WITH bounds AS (
                      SELECT date_trunc('month', %s::date) AS s, date_trunc('month', %s::date) AS e
                    )
                    SELECT to_char(date_trunc('month', COALESCE(p.fecha_pago::timestamp, make_timestamp(p.año, p.mes, 1, 0, 0, 0))), 'YYYY-MM') AS ym,
                           COALESCE(SUM(p.monto), 0) AS total
                    FROM pagos p, bounds b
                    WHERE date_trunc('month', COALESCE(p.fecha_pago::timestamp, make_timestamp(p.año, p.mes, 1, 0, 0, 0))) BETWEEN b.s AND b.e
                    GROUP BY 1
                    ORDER BY 1
                    """,
                    (start, end)
                )
            else:
                cur.execute(
                    """
                    SELECT to_char(date_trunc('month', COALESCE(p.fecha_pago::timestamp, make_timestamp(p.año, p.mes, 1, 0, 0, 0))), 'YYYY-MM') AS ym,
                           COALESCE(SUM(p.monto), 0) AS total
                    FROM pagos p
                    WHERE date_trunc('month', COALESCE(p.fecha_pago::timestamp, make_timestamp(p.año, p.mes, 1, 0, 0, 0))) 
                          >= date_trunc('month', CURRENT_DATE) - INTERVAL '11 months'
                    GROUP BY 1
                    ORDER BY 1
                    """
                )
            rows = cur.fetchall() or []

        # Construir base del rango solicitado o últimos 12 meses y completar faltantes
        try:
            from datetime import datetime as dt, date, timedelta
            base: Dict[str, float] = {}
            if start and end:
                s = dt.strptime(start, "%Y-%m-%d").date().replace(day=1)
                e = dt.strptime(end, "%Y-%m-%d").date().replace(day=1)
                curm = s
                while curm <= e:
                    base[curm.strftime("%Y-%m")] = 0.0
                    curm = (curm.replace(day=28) + timedelta(days=4)).replace(day=1)
            else:
                hoy = date.today().replace(day=1)
                curm = hoy
                for _ in range(12):
                    base[curm.strftime("%Y-%m")] = 0.0
                    curm = (curm.replace(day=28) + timedelta(days=4)).replace(day=1)
            for r in rows:
                ym = str(r.get('ym'))
                base[ym] = float(r.get('total') or 0.0)
            result = dict(sorted(base.items()))
        except Exception:
            # Si algo falla en el relleno, devolvemos la agregación tal cual
            result = {str(r.get('ym')): float(r.get('total') or 0.0) for r in rows}
        return {"ingresos": result}
    except Exception as e:
        logging.exception("Error en /api/ingresos12m")
        return JSONResponse({"error": str(e)}, status_code=500)


@app.get("/api/nuevos12m")
async def api_nuevos12m(request: Request, _=Depends(require_owner)):
    db = _get_db()
    result: Dict[str, int] = {}
    if db is None:
        return {"nuevos": result}
    guard = _circuit_guard_json(db, "/api/nuevos12m")
    if guard:
        return guard
    try:
        # Delegar completamente al backend de database.py para mantener consistencia
        start = request.query_params.get("start")
        end = request.query_params.get("end")
        from datetime import datetime
        if start and end:
            try:
                inicio = datetime.strptime(start, "%Y-%m-%d").date()
                fin = datetime.strptime(end, "%Y-%m-%d").date()
                result = db.obtener_nuevos_usuarios_por_mes_rango(inicio, fin)  # type: ignore
            except Exception:
                # Si el rango no es válido, usar últimos 12 meses
                result = db.obtener_nuevos_usuarios_por_mes_ultimos_12()  # type: ignore
        else:
            result = db.obtener_nuevos_usuarios_por_mes_ultimos_12()  # type: ignore
        # Rellenar meses faltantes con 0 para evitar gráficos vacíos
        try:
            from datetime import date, timedelta
            base: Dict[str, int] = {}
            hoy = date.today().replace(day=1)
            for i in range(11, -1, -1):
                m = (hoy - timedelta(days=30 * i)).replace(day=1)
                clave = m.strftime("%Y-%m")
                base[clave] = 0
            base.update(result or {})
            result = dict(sorted(base.items()))
        except Exception:
            pass
        return {"nuevos": result}
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


@app.get("/api/arpu12m")
async def api_arpu12m(request: Request, _=Depends(require_owner)):
    db = _get_db()
    result: Dict[str, float] = {}
    if db is None:
        return {"arpu": result}
    guard = _circuit_guard_json(db, "/api/arpu12m")
    if guard:
        return guard
    try:
        start = request.query_params.get("start")
        end = request.query_params.get("end")
        with db.get_connection_context() as conn:  # type: ignore
            cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            if start and end:
                cur.execute(
                    """
                    WITH bounds AS (
                      SELECT date_trunc('month', %s::date) AS s, date_trunc('month', %s::date) AS e
                    )
                    SELECT to_char(date_trunc('month', COALESCE(p.fecha_pago::timestamp, make_timestamp(p.año, p.mes, 1, 0, 0, 0))), 'YYYY-MM') AS ym,
                           COALESCE(SUM(p.monto), 0) AS ingresos,
                           COUNT(DISTINCT p.usuario_id) AS pagadores
                    FROM pagos p, bounds b
                    WHERE date_trunc('month', COALESCE(p.fecha_pago::timestamp, make_timestamp(p.año, p.mes, 1, 0, 0, 0))) BETWEEN b.s AND b.e
                    GROUP BY 1
                    ORDER BY 1
                    """,
                    (start, end)
                )
            else:
                cur.execute(
                    """
                    SELECT to_char(date_trunc('month', COALESCE(p.fecha_pago::timestamp, make_timestamp(p.año, p.mes, 1, 0, 0, 0))), 'YYYY-MM') AS ym,
                           COALESCE(SUM(p.monto), 0) AS ingresos,
                           COUNT(DISTINCT p.usuario_id) AS pagadores
                    FROM pagos p
                    WHERE date_trunc('month', COALESCE(p.fecha_pago::timestamp, make_timestamp(p.año, p.mes, 1, 0, 0, 0))) 
                          >= date_trunc('month', CURRENT_DATE) - INTERVAL '11 months'
                    GROUP BY 1
                    ORDER BY 1
                    """
                )
            rows = cur.fetchall() or []

        # Construir base del rango solicitado o últimos 12 meses y completar faltantes
        try:
            from datetime import datetime as dt, date, timedelta
            base: Dict[str, float] = {}
            if start and end:
                s = dt.strptime(start, "%Y-%m-%d").date().replace(day=1)
                e = dt.strptime(end, "%Y-%m-%d").date().replace(day=1)
                curm = s
                while curm <= e:
                    base[curm.strftime("%Y-%m")] = 0.0
                    curm = (curm.replace(day=28) + timedelta(days=4)).replace(day=1)
            else:
                hoy = date.today().replace(day=1)
                for i in range(11, -1, -1):
                    m = (hoy - timedelta(days=30 * i)).replace(day=1)
                    base[m.strftime("%Y-%m")] = 0.0
            for r in rows:
                ym = str(r.get('ym'))
                ingresos = float(r.get('ingresos') or 0.0)
                pagadores = int(r.get('pagadores') or 0)
                base[ym] = (ingresos / pagadores) if pagadores > 0 else 0.0
            result = dict(sorted(base.items()))
        except Exception:
            # Si falla el relleno, calcular ARPU directo de filas
            result = {}
            for r in rows:
                ym = str(r.get('ym'))
                ingresos = float(r.get('ingresos') or 0.0)
                pagadores = int(r.get('pagadores') or 0)
                result[ym] = (ingresos / pagadores) if pagadores > 0 else 0.0
        return {"arpu": result}
    except Exception as e:
        import traceback
        traceback.print_exc()
        return JSONResponse({"error": str(e)}, status_code=500)


# --- Check-in inverso por QR (público para socios) ---
@app.get("/checkin")
async def checkin_page(request: Request):
    """Página pública de check-in para socios: autenticación por DNI+teléfono y lector de QR."""
    theme_vars = read_theme_vars(static_dir / "style.css")
    autenticado = bool(request.session.get("checkin_user_id"))
    socio_info = {}
    if autenticado:
        try:
            db = _get_db()
            if db:
                user_id = int(request.session.get("checkin_user_id"))
                with db.get_connection_context() as conn:  # type: ignore
                    cur = conn.cursor()
                    cur.execute(
                        """
                        SELECT id, COALESCE(nombre,'') AS nombre, COALESCE(dni,'') AS dni, COALESCE(telefono,'') AS telefono
                        FROM usuarios WHERE id = %s LIMIT 1
                        """,
                        (user_id,)
                    )
                    row = cur.fetchone()
                    if row:
                        import re as _re
                        user_id_db = int(row[0]) if row[0] is not None else user_id
                        nombre = str(row[1] or "")
                        dni = str(row[2] or "")
                        telefono = str(row[3] or "")
                        digits = _re.sub(r"\D+", "", telefono)
                        masked = ("*" * max(len(digits) - 4, 4)) + (digits[-4:] if len(digits) >= 4 else digits)
                        socio_info = {
                            "id": user_id_db,
                            "nombre": nombre,
                            "dni": dni,
                            "telefono_mask": masked,
                        }
        except Exception:
            pass
    ctx = {
        "request": request,
        "theme": theme_vars,
        "gym_name": get_gym_name("Gimnasio"),
        "logo_url": _resolve_logo_url(),
        "autenticado": autenticado,
        "socio": socio_info,
    }
    return templates.TemplateResponse("checkin.html", ctx)

@app.get("/checkin/logout")
async def checkin_logout(request: Request):
    # Logout específico del flujo de check-in: limpia solo la sesión de socio y vuelve a /checkin
    try:
        if "checkin_user_id" in request.session:
            request.session.pop("checkin_user_id", None)
    except Exception:
        request.session.clear()
    return RedirectResponse(url="/checkin", status_code=303)


@app.post("/checkin/auth")
async def checkin_auth(request: Request):
    """Autentica al socio por DNI y teléfono (ambos numéricos) y guarda la sesión de check-in."""
    rid = getattr(getattr(request, 'state', object()), 'request_id', '-')
    db = _get_db()
    if db is None:
        try:
            logging.error(f"/checkin/auth: DB=None, intentando _force_db_init rid={rid}")
        except Exception:
            pass
        db = _force_db_init()
        if db is None:
            try:
                logging.error(f"/checkin/auth: _force_db_init falló rid={rid}")
            except Exception:
                pass
            return JSONResponse({"success": False, "message": "Base de datos no disponible"}, status_code=500)
    # Obtener datos del cuerpo (form o JSON)
    content_type = request.headers.get("content-type", "")
    try:
        logging.info(f"/checkin/auth: content_type={content_type or '-'} rid={rid}")
    except Exception:
        pass
    if content_type.startswith("application/x-www-form-urlencoded"):
        data = await request.form()
    else:
        try:
            data = await request.json()
        except Exception:
            data = {}
    dni = str(data.get("dni", "")).strip()
    telefono = str(data.get("telefono", "")).strip()
    # Normalizar: solo dígitos
    import re
    dni_num = re.sub(r"\D+", "", dni)
    tel_num = re.sub(r"\D+", "", telefono)
    if not dni_num or not tel_num:
        try:
            logging.info(f"/checkin/auth: input inválido rid={rid} dni_len={len(dni)} tel_len={len(telefono)}")
        except Exception:
            pass
        return JSONResponse({"success": False, "message": "Ingrese DNI y teléfono válidos"}, status_code=400)
    try:
        with db.get_connection_context() as conn:  # type: ignore
            cur = conn.cursor()
            # Preparar variantes del teléfono para permitir coincidencias flexibles
            tel_like = f"%{tel_num}"
            tel_last10 = tel_num[-10:] if len(tel_num) >= 10 else tel_num
            try:
                logging.debug(f"/checkin/auth: consultando usuario por dni={dni_num} tel={tel_num} tel_last10={tel_last10} rid={rid}")
            except Exception:
                pass
            # Comparar por equivalencia de dígitos para soportar formatos (+54, 0-prefijo, espacios, guiones)
            cur.execute(
                """
                SELECT id FROM usuarios
                WHERE activo = TRUE
                  AND regexp_replace(CAST(dni AS TEXT), '\\D+', '', 'g') = %s
                  AND (
                    regexp_replace(CAST(telefono AS TEXT), '\\D+', '', 'g') = %s
                    OR regexp_replace(CAST(telefono AS TEXT), '\\D+', '', 'g') = '54' || %s
                    OR regexp_replace(CAST(telefono AS TEXT), '\\D+', '', 'g') = '0' || %s
                    OR regexp_replace(CAST(telefono AS TEXT), '\\D+', '', 'g') LIKE %s
                    OR right(regexp_replace(CAST(telefono AS TEXT), '\\D+', '', 'g'), 10) = %s
                  )
                ORDER BY id ASC
                LIMIT 1
                """,
                (dni_num, tel_num, tel_num, tel_num, tel_like, tel_last10)
            )
            row = cur.fetchone()
            if not row:
                # Evitar 404 para no confundir con ruta inexistente
                try:
                    logging.info(f"/checkin/auth: credenciales inválidas rid={rid}")
                except Exception:
                    pass
                return JSONResponse({"success": False, "message": "Credenciales inválidas"}, status_code=200)
            user_id = int(row[0])
            request.session["checkin_user_id"] = user_id
            try:
                logging.info(f"/checkin/auth: autenticado usuario_id={user_id} rid={rid}")
            except Exception:
                pass
            return JSONResponse({"success": True, "message": "Autenticado", "usuario_id": user_id})
    except Exception as e:
        try:
            logging.exception(f"Error en /checkin/auth rid={rid}")
        except Exception:
            pass
        return JSONResponse({"success": False, "message": str(e)}, status_code=500)


@app.post("/api/checkin/validate")
async def api_checkin_validate(request: Request):
    """Valida el token escaneado y registra asistencia si corresponde."""
    rid = getattr(getattr(request, 'state', object()), 'request_id', '-')
    db = _get_db()
    if db is None:
        try:
            logging.error(f"/api/checkin/validate: DB=None, intentando _force_db_init rid={rid}")
        except Exception:
            pass
        db = _force_db_init()
        if db is None:
            return JSONResponse({"success": False, "message": "Base de datos no disponible"}, status_code=500)
    guard = _circuit_guard_json(db, "/api/checkin/validate")
    if guard:
        return guard
    try:
        data = await request.json()
        token = str(data.get("token", "")).strip()
        socio_id = request.session.get("checkin_user_id")
        try:
            masked_token = ("***" + token[-4:]) if token else ""
            logging.info(f"/api/checkin/validate: recibido token={masked_token} socio_id={socio_id} rid={rid}")
        except Exception:
            pass
        if not socio_id:
            return JSONResponse({"success": False, "message": "Sesión de socio no encontrada"}, status_code=401)
        # Orden de parámetros: (token, socio_id)
        ok, msg = db.validar_token_y_registrar_asistencia(token, int(socio_id))  # type: ignore
        status = 200 if ok else 400
        # Señal explícita: marcar 'used' en checkin_pending para robustecer el polling del escritorio
        if ok:
            try:
                with db.get_connection_context() as conn:  # type: ignore
                    cur = conn.cursor()
                    cur.execute("UPDATE checkin_pending SET used = TRUE WHERE token = %s", (token,))
                    conn.commit()
            except Exception:
                pass
        try:
            logging.info(f"/api/checkin/validate: resultado ok={ok} msg='{msg}' rid={rid}")
        except Exception:
            pass
        return JSONResponse({"success": ok, "message": msg}, status_code=status)
    except Exception as e:
        try:
            logging.exception(f"Error en /api/checkin/validate rid={rid}")
        except Exception:
            pass
        return JSONResponse({"success": False, "message": str(e)}, status_code=500)


@app.get("/api/checkin/token_status")
async def api_checkin_token_status(request: Request):
    """Consulta el estado de un token: { exists, used, expired }.

    Criterio de 'used': se considera usado si el flag en checkin_pending es TRUE
    o si ya existe una asistencia para el usuario en la fecha actual.
    Esto hace el polling del escritorio más robusto ante posibles desincronizaciones.
    """
    rid = getattr(getattr(request, 'state', object()), 'request_id', '-')
    db = _get_db()
    if db is None:
        try:
            logging.error(f"/api/checkin/token_status: DB=None, intentando _force_db_init rid={rid}")
        except Exception:
            pass
        db = _force_db_init()
        if db is None:
            return JSONResponse({"exists": False, "used": False, "expired": True}, status_code=200)
    import datetime as _dt
    token = str(request.query_params.get("token", "")).strip()
    try:
        masked_token = ("***" + token[-4:]) if token else ""
        logging.info(f"/api/checkin/token_status: token={masked_token} rid={rid}")
    except Exception:
        pass
    if not token:
        return JSONResponse({"exists": False, "used": False, "expired": True}, status_code=200)
    try:
        with db.get_connection_context() as conn:  # type: ignore
            cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            # Obtener también usuario_id para verificar asistencia del día
            cur.execute("SELECT usuario_id, used, expires_at FROM checkin_pending WHERE token = %s LIMIT 1", (token,))
            row = cur.fetchone()
            if not row:
                return JSONResponse({"exists": False, "used": False, "expired": True}, status_code=200)

            used_flag = bool(row.get("used") or False)
            expires_at = row.get("expires_at")
            now = _dt.datetime.utcnow()
            expired = bool(expires_at and expires_at < now)

            usuario_id = row.get("usuario_id")
            attended_today = False
            try:
                if usuario_id is not None:
                    cur2 = conn.cursor()
                    cur2.execute(
                        "SELECT 1 FROM asistencias WHERE usuario_id = %s AND fecha::date = CURRENT_DATE LIMIT 1",
                        (int(usuario_id),)
                    )
                    attended_today = cur2.fetchone() is not None
            except Exception:
                attended_today = False

            used = bool(used_flag or attended_today)

            # Log a nivel INFO para que aparezca en los logs por defecto
            try:
                logging.info(
                    f"/api/checkin/token_status: usuario_id={usuario_id} used_flag={used_flag} attended_today={attended_today} expired={expired} rid={rid}"
                )
            except Exception:
                pass

            return JSONResponse({"exists": True, "used": used, "expired": expired}, status_code=200)
    except Exception as e:
        try:
            logging.exception(f"Error en /api/checkin/token_status rid={rid}")
        except Exception:
            pass
        return JSONResponse({"exists": False, "used": False, "expired": True, "error": str(e)}, status_code=200)

@app.post("/api/checkin/create_token")
async def api_checkin_create_token(request: Request, _=Depends(require_gestion_access)):
    rid = getattr(getattr(request,'state',object()), 'request_id', '-')
    db = _get_db()
    if db is None:
        try:
            logging.error(f"/api/checkin/create_token: DB=None rid={rid}")
        except Exception:
            pass
        db = _force_db_init()
        if db is None:
            raise HTTPException(status_code=500, detail="DB no disponible")
    payload = await request.json()
    usuario_id = int(payload.get("usuario_id") or 0)
    expires_minutes = int(payload.get("expires_minutes") or 5)
    if not usuario_id:
        raise HTTPException(status_code=400, detail="usuario_id es requerido")
    token = secrets.token_urlsafe(12)
    try:
        db.crear_checkin_token(usuario_id, token, expires_minutes)  # type: ignore
        try:
            logging.info(f"/api/checkin/create_token: usuario_id={usuario_id} token=***{token[-4:]} expires={expires_minutes}m rid={rid}")
        except Exception:
            pass
        return JSONResponse({"success": True, "token": token, "expires_minutes": expires_minutes}, status_code=200)
    except PermissionError as e:
        raise HTTPException(status_code=403, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.post("/api/asistencias/registrar")
async def api_asistencias_registrar(request: Request, _=Depends(require_gestion_access)):
    rid = getattr(getattr(request,'state',object()), 'request_id', '-')
    db = _get_db()
    if db is None:
        db = _force_db_init()
        if db is None:
            raise HTTPException(status_code=500, detail="DB no disponible")
    payload = await request.json()
    usuario_id = int(payload.get("usuario_id") or 0)
    fecha_str = str(payload.get("fecha") or "").strip()
    if not usuario_id:
        raise HTTPException(status_code=400, detail="usuario_id es requerido")
    from datetime import date
    fecha = None
    try:
        if fecha_str:
            parts = fecha_str.split("-")
            if len(parts) == 3:
                fecha = date(int(parts[0]), int(parts[1]), int(parts[2]))
    except Exception:
        fecha = None
    try:
        asistencia_id = db.registrar_asistencia(usuario_id, fecha)  # type: ignore
        try:
            logging.info(f"/api/asistencias/registrar: usuario_id={usuario_id} fecha={fecha} rid={rid}")
        except Exception:
            pass
        return JSONResponse({"success": True, "asistencia_id": asistencia_id}, status_code=200)
    except PermissionError as e:
        raise HTTPException(status_code=403, detail=str(e))
    except ValueError as e:
        try:
            logging.info(f"/api/asistencias/registrar: ya existía asistencia usuario_id={usuario_id} rid={rid}")
        except Exception:
            pass
        return JSONResponse({"success": True, "message": str(e)}, status_code=200)
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.get("/api/asistencia_30d")
async def api_asistencia_30d(request: Request, _=Depends(require_owner)):
    db = _get_db()
    series: Dict[str, int] = {}
    if db is None:
        return series
    guard = _circuit_guard_json(db, "/api/asistencia_30d")
    if guard:
        return guard
    try:
        start = request.query_params.get("start")
        end = request.query_params.get("end")
        # Delegar a backend: nueva función agregada para consistencia de esquema
        if start and end:
            data = db.obtener_asistencias_por_rango_diario(start, end)  # type: ignore
        else:
            data = db.obtener_asistencias_por_dia(30)  # type: ignore
        for d, c in (data or []):
            series[str(d)] = int(c or 0)
        # Rellenar días faltantes con 0 para últimos 30 días
        try:
            from datetime import date, timedelta
            base: Dict[str, int] = {}
            hoy = date.today()
            for i in range(29, -1, -1):
                dia = hoy - timedelta(days=i)
                clave = dia.strftime("%Y-%m-%d")
                base[clave] = 0
            base.update(series or {})
            series = dict(sorted(base.items()))
        except Exception:
            pass
        return series
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


@app.get("/api/asistencia_por_hora_30d")
async def api_asistencia_por_hora_30d(request: Request, _=Depends(require_owner)):
    db = _get_db()
    series: Dict[str, int] = {}
    if db is None:
        return series
    try:
        start = request.query_params.get("start")
        end = request.query_params.get("end")
        # Delegar a backend: función agregada que agrupa por hora usando hora_registro
        if start and end:
            data = db.obtener_asistencias_por_hora_rango(start, end)  # type: ignore
        else:
            data = db.obtener_asistencias_por_hora(30)  # type: ignore
        for h, c in (data or []):
            label = f"{int(h):02d}:00" if isinstance(h, (int, float)) else str(h)
            series[label] = int(c or 0)
        # Completar horas 00..23 con 0 si faltan
        try:
            base: Dict[str, int] = {f"{i:02d}:00": 0 for i in range(0, 24)}
            base.update(series or {})
            series = dict(sorted(base.items()))
        except Exception:
            pass
        return series
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


@app.get("/api/activos_inactivos")
async def api_activos_inactivos(_=Depends(require_owner)):
    db = _get_db()
    if db is None:
        return {"Activos": 0, "Inactivos": 0}
    guard = _circuit_guard_json(db, "/api/activos_inactivos")
    if guard:
        return guard
    try:
        # Delegar al backend validado para conteo
        return db.obtener_conteo_activos_inactivos()  # type: ignore
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


@app.get("/api/tipos_cuota")
async def api_tipos_cuota(_=Depends(require_owner)):
    db = _get_db()
    dist: Dict[str, int] = {}
    if db is None:
        return dist
    try:
        # Contar usuarios activos por tipo de cuota (roles socio/miembro) de forma robusta:
        # - Soporta que usuarios.tipo_cuota sea nombre o id (distintos esquemas)
        # - Evita errores de tipo usando CAST a texto para comparar con id
        with db.get_connection_context() as conn:  # type: ignore
            cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            cur.execute(
                """
                SELECT COALESCE(tc.nombre, CAST(u.tipo_cuota AS TEXT), 'Sin tipo') AS tipo,
                       COUNT(*) AS cantidad
                FROM usuarios u
                LEFT JOIN tipos_cuota tc 
                  ON (CAST(u.tipo_cuota AS TEXT) = tc.nombre) 
                     OR (CAST(u.tipo_cuota AS TEXT) = CAST(tc.id AS TEXT))
                WHERE u.activo = true
                  AND LOWER(COALESCE(u.rol,'')) IN ('socio','profesor')
                GROUP BY COALESCE(tc.nombre, CAST(u.tipo_cuota AS TEXT), 'Sin tipo')
                ORDER BY 1
                """
            )
            def normalize_label(label: str) -> str:
                s = (label or 'Sin tipo').strip().lower()
                replacements = {
                    'estandar': 'Estándar', 'estándar': 'Estándar', 'standard': 'Estándar',
                    'sin tipo': 'Sin tipo',
                    'mensualidad': 'Mensual', 'mensual': 'Mensual',
                }
                return replacements.get(s, s.title())
            for r in cur.fetchall() or []:
                raw = (r.get('tipo') or 'Sin tipo')
                nombre = normalize_label(str(raw))
                # Sumar en la etiqueta normalizada para unificar sin duplicados
                dist[nombre] = dist.get(nombre, 0) + int(r.get('cantidad') or 0)
        return dist
    except Exception as e:
        import traceback
        traceback.print_exc()
        return JSONResponse({"error": str(e)}, status_code=500)


@app.get("/api/kpis_avanzados")
async def api_kpis_avanzados(_=Depends(require_owner)):
    db = _get_db()
    kpis: Dict[str, Any] = {
        'retention_rate': 0.0,
        'churn_rate': 0.0,
        'avg_attendance': 0.0,
        'peak_hour': 'N/A',
        'revenue_growth': 0.0,
        'weekly_active_users': 0,
        'payment_rate': 0.0,
        'ltv_12m': 0.0,
        'arpa': 0.0,
        'mrr': 0.0,
    }
    if db is None:
        return kpis
    guard = _circuit_guard_json(db, "/api/kpis_avanzados")
    if guard:
        return guard
    try:
        # Fechas clave y métricas utilizando métodos existentes del backend
        from datetime import timedelta
        hoy = datetime.now().date()
        primer_dia_mes = hoy.replace(day=1)
        fin_mes_anterior = primer_dia_mes - timedelta(days=1)
        primer_dia_mes_anterior = fin_mes_anterior.replace(day=1)

        # Ingresos del mes actual y anterior
        ing_act = float(db.calcular_ingresos_totales(primer_dia_mes, hoy) or 0.0)  # type: ignore
        ing_ant = float(db.calcular_ingresos_totales(primer_dia_mes_anterior, fin_mes_anterior) or 0.0)  # type: ignore
        kpis['revenue_growth'] = ((ing_act - ing_ant) / ing_ant * 100.0) if ing_ant > 0 else 0.0

        # Pagadores en el mes actual (roles socio/profesor, robusto a fecha/año-mes)
        with db.get_connection_context() as conn:  # type: ignore
            cur = conn.cursor()
            cur.execute(
                """
                SELECT COUNT(DISTINCT u.id) AS pagadores
                FROM usuarios u
                JOIN pagos p ON p.usuario_id = u.id
                WHERE u.activo = TRUE
                  AND LOWER(COALESCE(u.rol,'')) IN ('socio','profesor')
                  AND date_trunc('month', COALESCE(p.fecha_pago, make_date(p.año, p.mes, 1))) = date_trunc('month', CURRENT_DATE)
                """
            )
            row = cur.fetchone()
            pagadores_mes = int(row[0] if row and row[0] is not None else 0)

        # Lista de pagos del mes actual para métricas de retención
        pagos_mes = db.obtener_pagos_por_fecha(primer_dia_mes, hoy) or []  # type: ignore

        # Usuarios activos totales desde KPIs generales
        kpis_generales = db.obtener_kpis_generales() or {}
        activos_total = int(kpis_generales.get('total_activos', 0) or 0)
        kpis['payment_rate'] = (pagadores_mes / activos_total * 100.0) if activos_total > 0 else 0.0

        # ARPA y MRR
        kpis['mrr'] = ing_act
        kpis['arpa'] = (ing_act / pagadores_mes) if pagadores_mes > 0 else 0.0

        # LTV 12 meses
        inicio_12m = hoy - timedelta(days=365)
        pagos_12m = db.obtener_pagos_por_fecha(inicio_12m, hoy) or []  # type: ignore
        total_12m = sum(float(p.get('monto', 0) or 0.0) for p in pagos_12m if isinstance(p, dict))
        usuarios_12m = len({p.get('usuario_id') for p in pagos_12m if isinstance(p, dict)})
        kpis['ltv_12m'] = (total_12m / usuarios_12m) if usuarios_12m > 0 else 0.0

        # Retención vs mes anterior
        pagos_mes_ant = db.obtener_pagos_por_fecha(primer_dia_mes_anterior, fin_mes_anterior) or []  # type: ignore
        set_act = {p.get('usuario_id') for p in pagos_mes if isinstance(p, dict)}
        set_ant = {p.get('usuario_id') for p in pagos_mes_ant if isinstance(p, dict)}
        kpis['retention_rate'] = (len(set_act & set_ant) / len(set_ant) * 100.0) if len(set_ant) > 0 else 0.0
        kpis['churn_rate'] = 100.0 - kpis['retention_rate']

        # Asistencias últimos 30 días para promedio y hora pico
        asist_30 = db.obtener_asistencias_por_fecha_limite(hoy - timedelta(days=30)) or []  # type: ignore
        from collections import Counter
        dias_conteo = Counter()
        horas_conteo = Counter()
        for a in asist_30:
            f = a.get('fecha')
            h = a.get('hora_registro')
            if hasattr(f, 'date'):
                f = f.date()
            dias_conteo[f] += 1
            if h is not None:
                try:
                    hh = h.hour if hasattr(h, 'hour') else int(str(h).split(':')[0])
                    horas_conteo[hh] += 1
                except Exception:
                    pass
        kpis['avg_attendance'] = (sum(dias_conteo.values()) / len(dias_conteo)) if len(dias_conteo) > 0 else 0.0
        if len(horas_conteo) > 0:
            peak_h = max(horas_conteo.items(), key=lambda kv: kv[1])[0]
            kpis['peak_hour'] = f"{int(peak_h):02d}:00"
        else:
            kpis['peak_hour'] = 'N/A'

        # Usuarios activos semanales por asistencias últimos 7 días
        asist_7 = db.obtener_asistencias_por_fecha_limite(hoy - timedelta(days=7)) or []  # type: ignore
        kpis['weekly_active_users'] = len({a.get('usuario_id') for a in asist_7})

        return kpis
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


@app.get("/api/cohort_retencion_6m")
async def api_cohort_retencion_6m(_=Depends(require_owner)):
    """Retención por cohorte de registro (últimos 6 meses): % del cohorte que pagó este mes."""
    db = _get_db()
    result: Dict[str, float] = {}
    if db is None:
        return result
    try:
        with db.get_connection_context() as conn:  # type: ignore
            cur = conn.cursor()
            cur.execute(
                """
                SELECT to_char(date_trunc('month', u.fecha_registro), 'YYYY-MM') AS cohorte,
                       COUNT(*) AS tam,
                       SUM(CASE WHEN EXISTS (
                           SELECT 1 FROM pagos p
                           WHERE p.usuario_id = u.id
                             AND date_trunc('month', COALESCE(p.fecha_pago, make_date(p.año, p.mes, 1))) = date_trunc('month', CURRENT_DATE)
                       ) THEN 1 ELSE 0 END) AS retenidos
                FROM usuarios u
                WHERE u.fecha_registro IS NOT NULL
                  AND date_trunc('month', u.fecha_registro) >= date_trunc('month', CURRENT_DATE) - INTERVAL '5 months'
                  AND u.activo = true
                  AND LOWER(COALESCE(u.rol,'')) NOT IN ('dueño','dueno','owner','administrador','admin')
                GROUP BY 1
                ORDER BY 1
                """
            )
            for cohorte, tam, retenidos in cur.fetchall():
                tam = int(tam or 0); retenidos = int(retenidos or 0)
                result[str(cohorte)] = (retenidos / tam * 100.0) if tam > 0 else 0.0
        return result
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


@app.get("/api/cohort_retencion_heatmap")
async def api_cohort_retencion_heatmap(months: int = 6, _=Depends(require_owner)):
    """Matriz de retención por cohorte (registro) vs mes de pago.
    Devuelve { months: [...], cohorts: [...], matrix: [[%...], ...] } para últimos N meses.
    """
    db = _get_db()
    res: Dict[str, Any] = { 'months': [], 'cohorts': [], 'matrix': [] }
    if db is None:
        return res
    months = max(1, min(months, 24))
    try:
        with db.get_connection_context() as conn:  # type: ignore
            cur = conn.cursor()
            # Lista de meses (YYYY-MM) para los últimos N meses
            cur.execute("SELECT to_char(date_trunc('month', CURRENT_DATE) - s * INTERVAL '1 month', 'YYYY-MM') FROM generate_series(0,%s) s ORDER BY 1", (months-1,))
            months_list = [row[0] for row in cur.fetchall()]
            cohorts = months_list
            matrix: list[list[float]] = []
            for coh in cohorts:
                # Tamaño de cohorte
                cur.execute("""
                    SELECT COUNT(*) FROM usuarios u
                    WHERE u.activo = true AND u.fecha_registro IS NOT NULL
                      AND LOWER(COALESCE(u.rol,'')) NOT IN ('dueño','dueno','owner','administrador','admin')
                      AND to_char(date_trunc('month', u.fecha_registro), 'YYYY-MM') = %s
                """, (coh,))
                cohort_size = int(cur.fetchone()[0] or 0)
                row: list[float] = []
                for m in months_list:
                    # Usuarios del cohorte que pagaron en mes m
                    cur.execute("""
                        SELECT COUNT(*) FROM usuarios u
                        WHERE u.activo = true AND u.fecha_registro IS NOT NULL
                          AND LOWER(COALESCE(u.rol,'')) NOT IN ('dueño','dueno','owner','administrador','admin')
                          AND to_char(date_trunc('month', u.fecha_registro), 'YYYY-MM') = %s
                          AND EXISTS (
                            SELECT 1 FROM pagos p
                            WHERE p.usuario_id = u.id
                              AND to_char(date_trunc('month', COALESCE(p.fecha_pago, make_date(p.año, p.mes, 1))), 'YYYY-MM') = %s
                          )
                    """, (coh, m))
                    retained = int(cur.fetchone()[0] or 0)
                    pct = (retained / cohort_size * 100.0) if cohort_size > 0 else 0.0
                    row.append(pct)
                matrix.append(row)
            return {"months": months_list, "cohorts": cohorts, "matrix": matrix}
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


@app.get("/api/arpa_por_tipo_cuota")
async def api_arpa_por_tipo_cuota(_=Depends(require_owner)):
    """ARPA por tipo de cuota para el mes actual (monto total / pagadores del tipo)."""
    db = _get_db()
    dist: Dict[str, float] = {}
    if db is None:
        return dist
    guard = _circuit_guard_json(db, "/api/arpa_por_tipo_cuota")
    if guard:
        return guard
    try:
        with db.get_connection_context() as conn:  # type: ignore
            cur = conn.cursor()
            def norm(label: str) -> str:
                s = (label or '—').strip().lower()
                replacements = {
                    'estandar': 'Estándar', 'estándar': 'Estándar', 'standard': 'Estándar',
                    'estudiante': 'Estudiante', 'student': 'Estudiante',
                    'funcional': 'Funcional', 'functional': 'Funcional',
                }
                return replacements.get(s, s.title())
            cur.execute(
                """
                SELECT COALESCE(tc.nombre, CAST(u.tipo_cuota AS TEXT), '—') AS tipo,
                       COALESCE(SUM(CASE WHEN date_trunc('month',
                           CASE
                               WHEN p.fecha_pago IS NOT NULL THEN p.fecha_pago
                               WHEN p.año IS NOT NULL AND p.mes IS NOT NULL THEN make_date(p.año, p.mes, 1)
                               ELSE NULL
                           END
                       ) = date_trunc('month', CURRENT_DATE) THEN p.monto END), 0) AS monto_mes,
                       COUNT(DISTINCT CASE WHEN date_trunc('month',
                           CASE
                               WHEN p.fecha_pago IS NOT NULL THEN p.fecha_pago
                               WHEN p.año IS NOT NULL AND p.mes IS NOT NULL THEN make_date(p.año, p.mes, 1)
                               ELSE NULL
                           END
                       ) = date_trunc('month', CURRENT_DATE) THEN u.id END) AS pagadores_mes
                FROM usuarios u
                LEFT JOIN pagos p ON p.usuario_id = u.id
                -- Emparejar robustamente por nombre o id como texto
                LEFT JOIN tipos_cuota tc 
                  ON (CAST(u.tipo_cuota AS TEXT) = tc.nombre)
                  OR (CAST(u.tipo_cuota AS TEXT) = CAST(tc.id AS TEXT))
                WHERE u.activo = true
                  AND LOWER(COALESCE(u.rol,'')) NOT IN ('dueño','dueno','owner','administrador','admin')
                GROUP BY 1
                """
            )
            for tipo, monto_mes, pagadores_mes in cur.fetchall():
                key = norm(str(tipo))
                # Excluir SOLO "estandar" (sin acento)
                if key.strip().lower() == 'estandar':
                    continue
                pagadores = int(pagadores_mes or 0)
                total = float(monto_mes or 0.0)
                dist[key] = (total / pagadores) if pagadores > 0 else 0.0
        return dist
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


@app.get("/api/payment_status_dist")
async def api_payment_status_dist(_=Depends(require_owner)):
    db = _get_db()
    dist = {"no_payments": 0, "up_to_date": 0, "pending": 0, "overdue": 0}
    if db is None:
        return dist
    guard = _circuit_guard_json(db, "/api/payment_status_dist")
    if guard:
        return guard
    try:
        with db.get_connection_context() as conn:  # type: ignore
            cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            cur.execute(
                """
                WITH last_pay AS (
                  SELECT u.id AS usuario_id,
                         MAX(p.fecha_pago::timestamp) AS last_date
                  FROM usuarios u
                  LEFT JOIN pagos p ON p.usuario_id = u.id
                  WHERE u.activo = true
                    AND LOWER(COALESCE(u.rol,'')) IN ('socio','profesor')
                  GROUP BY u.id
                )
                SELECT
                  SUM(CASE WHEN last_date IS NULL THEN 1 ELSE 0 END) AS no_payments,
                  SUM(CASE WHEN last_date IS NOT NULL AND date_trunc('month', last_date) = date_trunc('month', CURRENT_DATE::timestamp) THEN 1 ELSE 0 END) AS up_to_date,
                  SUM(CASE WHEN last_date IS NOT NULL AND date_trunc('month', last_date) < date_trunc('month', CURRENT_DATE::timestamp) AND (CURRENT_DATE - last_date::date) <= 30 THEN 1 ELSE 0 END) AS pending,
                  SUM(CASE WHEN last_date IS NOT NULL AND (CURRENT_DATE - last_date::date) > 30 THEN 1 ELSE 0 END) AS overdue
                FROM last_pay
                """
            )
            row = cur.fetchone() or {}
            dist["no_payments"] = int(row.get("no_payments") or 0)
            dist["up_to_date"] = int(row.get("up_to_date") or 0)
            dist["pending"] = int(row.get("pending") or 0)
            dist["overdue"] = int(row.get("overdue") or 0)
        return dist
    except Exception as e:
        import traceback
        traceback.print_exc()
        return JSONResponse({"error": str(e)}, status_code=500)


# --- Tablas detalladas para subpestañas ---

@app.get("/api/pagos_detalle")
async def api_pagos_detalle(request: Request, _=Depends(require_gestion_access)):
    """Listado detallado de pagos con filtros por fecha, búsqueda y paginación.

    - Filtros: start (YYYY-MM-DD), end (YYYY-MM-DD), q (nombre/dni/metodo/concepto)
    - Paginación: limit, offset
    """
    db = _get_db()
    if db is None:
        return {"count": 0, "items": []}
    guard = _circuit_guard_json(db, "/api/pagos_detalle")
    if guard:
        return guard
    try:
        start = request.query_params.get("start")
        end = request.query_params.get("end")
        q = request.query_params.get("q")
        limit_q = request.query_params.get("limit")
        offset_q = request.query_params.get("offset")
        try:
            limit = int(limit_q) if (limit_q and str(limit_q).isdigit()) else 50
        except Exception:
            limit = 50
        try:
            offset = int(offset_q) if (offset_q and str(offset_q).isdigit()) else 0
        except Exception:
            offset = 0
        # Normalizar vacíos
        if start and isinstance(start, str) and start.strip() == "":
            start = None
        if end and isinstance(end, str) and end.strip() == "":
            end = None

        rows = db.obtener_pagos_por_fecha(start, end)  # type: ignore
        items = rows
        if q and isinstance(q, str) and q.strip() != "":
            ql = q.lower()
            def _match(r: Dict[str, Any]) -> bool:
                try:
                    nombre = str(r.get("usuario_nombre") or r.get("nombre") or "").lower()
                    dni = str(r.get("dni") or "").lower()
                    metodo = str(r.get("metodo_pago") or r.get("metodo") or "").lower()
                    concepto = str(r.get("concepto_pago") or r.get("concepto") or "").lower()
                    return (ql in nombre) or (ql in dni) or (ql in metodo) or (ql in concepto)
                except Exception:
                    return False
            items = [r for r in rows if _match(r)]
        total = len(items)
        sliced = items[offset:offset+limit]
        return {"count": total, "items": sliced}
    except Exception as e:
        import traceback
        traceback.print_exc()
        return JSONResponse({"error": str(e)}, status_code=500)

# --- Detalle de pago por ID ---
@app.get("/api/pagos/{pago_id}")
async def api_pago_resumen(pago_id: int, _=Depends(require_gestion_access)):
    db = _get_db()
    if db is None:
        raise HTTPException(status_code=503, detail="DB no disponible")
    try:
        with db.get_connection_context() as conn:  # type: ignore
            cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            cur.execute(
                """
                SELECT p.*, u.nombre AS usuario_nombre, u.dni, u.id AS usuario_id
                FROM pagos p
                JOIN usuarios u ON u.id = p.usuario_id
                WHERE p.id = %s
                """,
                (pago_id,)
            )
            pago = cur.fetchone()
            if not pago:
                raise HTTPException(status_code=404, detail="Pago no encontrado")
            cur.execute(
                """
                SELECT 
                    pd.id, pd.pago_id,
                    COALESCE(cp.nombre, pd.descripcion) AS concepto_nombre,
                    COALESCE(pd.cantidad, 1) AS cantidad,
                    COALESCE(pd.precio_unitario, 0) AS precio_unitario,
                    COALESCE(pd.subtotal, COALESCE(pd.cantidad,1) * COALESCE(pd.precio_unitario,0)) AS subtotal
                FROM pago_detalles pd
                LEFT JOIN conceptos_pago cp ON cp.id = pd.concepto_id
                WHERE pd.pago_id = %s
                ORDER BY pd.id
                """,
                (pago_id,)
            )
            detalles = cur.fetchall() or []
        total_detalles = sum(float(d.get("subtotal") or 0) for d in detalles)
        return {"pago": pago, "detalles": detalles, "total_detalles": total_detalles}
    except HTTPException:
        raise
    except Exception as e:
        import traceback
        traceback.print_exc()
        return JSONResponse({"error": str(e)}, status_code=500)

# --- Crear/registrar pago (básico) ---
@app.post("/api/pagos")
async def api_pagos_create(request: Request, _=Depends(require_gestion_access)):
    pm = _get_pm()
    db = _get_db()
    if db is None:
        raise HTTPException(status_code=503, detail="DB no disponible")
    guard = _circuit_guard_json(db, "/api/pagos[POST]")
    if guard:
        return guard
    if pm is None:
        raise HTTPException(status_code=503, detail="PaymentManager no disponible")
    payload = await request.json()
    try:
        usuario_id_raw = payload.get("usuario_id")
        monto_raw = payload.get("monto")
        mes_raw = payload.get("mes")
        año_raw = payload.get("año")
        metodo_pago_id = payload.get("metodo_pago_id")

        if usuario_id_raw is None or monto_raw is None or mes_raw is None or año_raw is None:
            raise HTTPException(status_code=400, detail="'usuario_id', 'monto', 'mes' y 'año' son obligatorios")
        try:
            usuario_id = int(usuario_id_raw)
            monto = float(monto_raw)
            mes = int(mes_raw)
            año = int(año_raw)
            metodo_pago_id_int = int(metodo_pago_id) if metodo_pago_id is not None else None
        except Exception:
            raise HTTPException(status_code=400, detail="Tipos inválidos en payload")

        if not (1 <= mes <= 12):
            raise HTTPException(status_code=400, detail="'mes' debe estar entre 1 y 12")
        if monto <= 0:
            raise HTTPException(status_code=400, detail="'monto' debe ser mayor a 0")

        try:
            pago_id = pm.registrar_pago(usuario_id, monto, mes, año, metodo_pago_id_int)
        except ValueError as ve:
            raise HTTPException(status_code=400, detail=str(ve))
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))

        return {"ok": True, "id": int(pago_id)}
    except HTTPException:
        raise
    except Exception as e:
        import traceback
        traceback.print_exc()
        return JSONResponse({"error": str(e)}, status_code=500)

# --- Modificar pago existente ---
@app.put("/api/pagos/{pago_id}")
async def api_pagos_update(pago_id: int, request: Request, _=Depends(require_gestion_access)):
    pm = _get_pm()
    db = _get_db()
    if db is None:
        raise HTTPException(status_code=503, detail="DB no disponible")
    guard = _circuit_guard_json(db, f"/api/pagos/{pago_id}[PUT]")
    if guard:
        return guard
    if pm is None or Pago is None:
        raise HTTPException(status_code=503, detail="PaymentManager o modelo Pago no disponible")
    payload = await request.json()
    try:
        usuario_id_raw = payload.get("usuario_id")
        monto_raw = payload.get("monto")
        fecha_raw = payload.get("fecha_pago")
        mes_raw = payload.get("mes")
        año_raw = payload.get("año")
        metodo_pago_id = payload.get("metodo_pago_id")

        if usuario_id_raw is None or monto_raw is None:
            raise HTTPException(status_code=400, detail="'usuario_id' y 'monto' son obligatorios")
        try:
            usuario_id = int(usuario_id_raw)
            monto = float(monto_raw)
            metodo_pago_id_int = int(metodo_pago_id) if metodo_pago_id is not None else None
        except Exception:
            raise HTTPException(status_code=400, detail="Tipos inválidos en payload")

        # Resolver fecha_pago
        fecha_dt = None
        if fecha_raw is not None:
            try:
                if isinstance(fecha_raw, str):
                    fecha_dt = datetime.fromisoformat(fecha_raw)
                else:
                    # Si llega ya como objeto serializado raro, intentar fallback
                    raise ValueError("fecha_pago debe ser string ISO")
            except Exception:
                raise HTTPException(status_code=400, detail="'fecha_pago' inválida, use ISO 8601 (YYYY-MM-DD)")
        else:
            if mes_raw is not None and año_raw is not None:
                try:
                    mes = int(mes_raw)
                    año = int(año_raw)
                    if not (1 <= mes <= 12):
                        raise HTTPException(status_code=400, detail="'mes' debe estar entre 1 y 12")
                    fecha_dt = datetime(año, mes, 1)
                except HTTPException:
                    raise
                except Exception:
                    raise HTTPException(status_code=400, detail="'mes'/'año' inválidos")
            else:
                # Intentar recuperar el pago para preservar su fecha
                try:
                    existing = pm.obtener_pago(pago_id)
                    if existing and getattr(existing, 'fecha_pago', None):
                        fecha_dt = existing.fecha_pago if not isinstance(existing.fecha_pago, str) else datetime.fromisoformat(existing.fecha_pago)
                    else:
                        fecha_dt = datetime.now()
                except Exception:
                    fecha_dt = datetime.now()

        # Resolver mes/año finales (usados en sync)
        if mes_raw is not None and año_raw is not None:
            try:
                mes = int(mes_raw)
                año = int(año_raw)
            except Exception:
                mes = fecha_dt.month
                año = fecha_dt.year
        else:
            mes = fecha_dt.month
            año = fecha_dt.year

        if monto <= 0:
            raise HTTPException(status_code=400, detail="'monto' debe ser mayor a 0")

        pago = Pago(id=int(pago_id), usuario_id=usuario_id, monto=monto, mes=mes, año=año, fecha_pago=fecha_dt, metodo_pago_id=metodo_pago_id_int)
        try:
            pm.modificar_pago(pago)
        except ValueError as ve:
            raise HTTPException(status_code=400, detail=str(ve))
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))
        return {"ok": True, "id": int(pago_id)}
    except HTTPException:
        raise
    except Exception as e:
        import traceback
        traceback.print_exc()
        return JSONResponse({"error": str(e)}, status_code=500)

# --- Eliminar pago ---
@app.delete("/api/pagos/{pago_id}")
async def api_pagos_delete(pago_id: int, _=Depends(require_gestion_access)):
    pm = _get_pm()
    db = _get_db()
    if db is None:
        raise HTTPException(status_code=503, detail="DB no disponible")
    guard = _circuit_guard_json(db, f"/api/pagos/{pago_id}[DELETE]")
    if guard:
        return guard
    if pm is None:
        raise HTTPException(status_code=503, detail="PaymentManager no disponible")
    try:
        pm.eliminar_pago(int(pago_id))
        return {"ok": True}
    except ValueError as ve:
        raise HTTPException(status_code=400, detail=str(ve))
    except Exception as e:
        import traceback
        traceback.print_exc()
        return JSONResponse({"error": str(e)}, status_code=500)

# --- Historial de pagos por usuario (gestión) ---
@app.get("/api/usuarios/{usuario_id}/pagos")
async def api_usuario_historial(usuario_id: int, request: Request, _=Depends(require_gestion_access)):
    db = _get_db()
    if db is None:
        return []
    try:
        limit_q = request.query_params.get("limit")
        offset_q = request.query_params.get("offset")
        lim = int(limit_q) if (limit_q and str(limit_q).isdigit()) else 50
        off = int(offset_q) if (offset_q and str(offset_q).isdigit()) else 0
        with db.get_connection_context() as conn:  # type: ignore
            cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            cur.execute(
                """
                SELECT 
                    p.id, p.fecha_pago::date AS fecha, p.monto,
                    COALESCE(mp.nombre,'') AS metodo, COALESCE(cp.nombre,'') AS concepto
                FROM pagos p
                LEFT JOIN metodos_pago mp ON mp.id = p.metodo_pago_id
                LEFT JOIN conceptos_pago cp ON cp.id = p.concepto_id
                WHERE p.usuario_id = %s
                ORDER BY p.fecha_pago DESC
                LIMIT %s OFFSET %s
                """,
                (usuario_id, lim, off)
            )
            rows = cur.fetchall() or []
        return rows
    except Exception as e:
        import traceback
        traceback.print_exc()
        return JSONResponse({"error": str(e)}, status_code=500)

@app.get("/api/usuarios_detalle")
async def api_usuarios_detalle(_=Depends(require_owner)):
    """Detalle de usuarios con métricas reales: pagos y asistencias."""
    db = _get_db()
    if db is None:
        return []
    guard = _circuit_guard_json(db, "/api/usuarios_detalle")
    if guard:
        return guard
    try:
        with db.get_connection_context() as conn:  # type: ignore
            cur = conn.cursor()
            cur.execute(
                """
                SELECT u.id,
                       COALESCE(u.nombre,'') AS nombre,
                       COALESCE(u.rol,'') AS rol,
                       COALESCE(u.dni,'') AS dni,
                       COALESCE(u.telefono,'') AS telefono,
                       COALESCE(u.tipo_cuota,'') AS tipo_cuota,
                       COALESCE(u.activo,false) AS activo,
                       COALESCE(u.fecha_registro::date, CURRENT_DATE) AS fecha_registro,
                       COALESCE(p.pagos_count,0) AS pagos_count,
                       p.last_pago_fecha::date AS last_pago_fecha,
                       COALESCE(a.asistencias_count,0) AS asistencias_count
                FROM usuarios u
                LEFT JOIN (
                    SELECT usuario_id, COUNT(*) AS pagos_count, MAX(fecha_pago) AS last_pago_fecha
                    FROM pagos
                    GROUP BY usuario_id
                ) p ON p.usuario_id = u.id
                LEFT JOIN (
                    SELECT usuario_id, COUNT(*) AS asistencias_count
                    FROM asistencias
                    GROUP BY usuario_id
                ) a ON a.usuario_id = u.id
                ORDER BY u.id
                """
            )
            rows = []
            for r in cur.fetchall():
                rows.append({
                    "id": r[0],
                    "nombre": r[1],
                    "rol": r[2],
                    "dni": r[3],
                    "telefono": r[4],
                    "tipo_cuota": r[5],
                    "activo": bool(r[6]),
                    "fecha_registro": str(r[7]) if r[7] is not None else None,
                    "pagos_count": int(r[8] or 0),
                    "last_pago_fecha": str(r[9]) if r[9] is not None else None,
                    "asistencias_count": int(r[10] or 0),
                })
        return rows
    except Exception as e:
        import traceback
        traceback.print_exc()
        print("Error in api_profesores_detalle:", repr(e))
        return JSONResponse({"error": str(e)}, status_code=500)


@app.get("/api/profesores_detalle")
async def api_profesores_detalle(request: Request, _=Depends(require_owner)):
    """Detalle de profesores con contacto y horarios/sesiones reales.

    Devuelve por profesor:
    - Datos de contacto (nombre, email, teléfono)
    - Cantidad de horarios de disponibilidad (activos)
    - Resumen de horarios (día, hora_inicio, hora_fin)
    - Sesiones del mes actual y horas trabajadas reales
    """
    print("DEBUG: top-level entry in /api/profesores_detalle")
    db = _get_db()
    if db is None:
        return []
    try:
        with db.get_connection_context() as conn:  # type: ignore
            cur = conn.cursor()
            # Mes/año actuales para métricas de sesiones reales
            # Rango opcional (start/end) para sesiones/horas; si no se envía, usa mes actual
            start = request.query_params.get("start")
            end = request.query_params.get("end")
            # Normalizar parámetros vacíos a None para evitar BETWEEN con cadenas vacías
            if not start or (isinstance(start, str) and start.strip() == ""):
                start = None
            if not end or (isinstance(end, str) and end.strip() == ""):
                end = None
            from datetime import datetime
            now = datetime.now()
            mes_actual = now.month
            anio_actual = now.year

            # Base: profesores + usuario info (sin 'email' para evitar UndefinedColumn en esquemas sin esa columna)
            try:
                cur.execute(
                    """
                    SELECT p.id,
                           COALESCE(u.nombre,'') AS nombre,
                           COALESCE(u.telefono,'') AS telefono
                    FROM profesores p
                    JOIN usuarios u ON u.id = p.usuario_id
                    ORDER BY p.id
                    """
                )
                base_rows = cur.fetchall()
                print(f"DEBUG: profesores base_rows count = {len(base_rows)}")
            except Exception as e:
                import traceback
                traceback.print_exc()
                print("Error base SELECT in profesores_detalle:", repr(e))
                try:
                    conn.rollback()
                except Exception:
                    pass
                base_rows = []

            # Sessions aggregation (controlled reintroduction)
            try:
                # Parsear fechas si vienen como strings
                from datetime import datetime as _dt
                start_date = None
                end_date = None
                try:
                    if start:
                        start_date = _dt.strptime(start, "%Y-%m-%d").date()
                    if end:
                        end_date = _dt.strptime(end, "%Y-%m-%d").date()
                except Exception:
                    start_date = None
                    end_date = None

                if start_date and end_date:
                    cur.execute(
                        """
                        SELECT profesor_id,
                               COUNT(*) AS sesiones_mes,
                               COALESCE(SUM(minutos_totales) / 60.0, 0) AS horas_mes
                        FROM profesor_horas_trabajadas
                        WHERE hora_fin IS NOT NULL AND fecha BETWEEN %s AND %s
                        GROUP BY profesor_id
                        """,
                        (start_date, end_date)
                    )
                else:
                    cur.execute(
                        """
                        SELECT profesor_id,
                               COUNT(*) AS sesiones_mes,
                               COALESCE(SUM(minutos_totales) / 60.0, 0) AS horas_mes
                        FROM profesor_horas_trabajadas
                        WHERE hora_fin IS NOT NULL AND EXTRACT(MONTH FROM fecha) = %s AND EXTRACT(YEAR FROM fecha) = %s
                        GROUP BY profesor_id
                        """,
                        (mes_actual, anio_actual)
                    )
                s_map = {row[0]: (int(row[1] or 0), float(row[2] or 0)) for row in cur.fetchall()}
            except Exception as e:
                logging.exception("Error sessions aggregation in profesores_detalle")
                try:
                    conn.rollback()
                except Exception:
                    pass
                s_map = {}

            # Base-only response plus schedules (controlled reintroduction)
            rows = []
            # Construir respuesta: como 'email' puede no existir en la BD, devolvemos ''
            for pid, nombre, telefono in base_rows:
                # Obtener horarios de disponibilidad del profesor desde database.py
                try:
                    disp = db.obtener_horarios_disponibilidad_profesor(pid)  # type: ignore
                    horarios_list = [
                        {
                            "dia": h.get("dia_semana"),
                            "inicio": str(h.get("hora_inicio")) if h.get("hora_inicio") is not None else None,
                            "fin": str(h.get("hora_fin")) if h.get("hora_fin") is not None else None,
                        }
                        for h in (disp or [])
                    ]
                except Exception as e:
                    logging.exception(f"Error disponibilidad para profesor {pid} en profesores_detalle")
                    try:
                        conn.rollback()
                    except Exception:
                        pass
                    horarios_list = []
                rows.append({
                    "id": pid,
                    "nombre": nombre,
                    "email": "",
                    "telefono": telefono,
                    "horarios_count": len(horarios_list),
                    "horarios": horarios_list,
                    "sesiones_mes": s_map.get(pid, (0, 0.0))[0],
                    "horas_mes": s_map.get(pid, (0, 0.0))[1],
                })
            logging.debug("profesores response rows count = %d", len(rows))
        return rows
    except Exception as e:
        logging.exception("Error final in /api/profesores_detalle")
        return []

# --- Endpoints de detalle para pagos y asistencias ---

@app.get("/api/profesor_sesiones")
async def api_profesor_sesiones(request: Request, _=Depends(require_owner)):
    """Lista de sesiones trabajadas por un profesor, opcionalmente por rango.

    Devuelve datos reales y completos por sesión obtenidos directamente del backend
    (database.py) sin cálculos en la web: fecha, inicio, fin, minutos, horas,
    clase (si existe) y tipo de actividad.
    """
    db = _get_db()
    if db is None:
        return []
    guard = _circuit_guard_json(db, "/api/profesor_sesiones")
    if guard:
        return guard
    try:
        pid = request.query_params.get("profesor_id")
        if not pid:
            return []
        try:
            profesor_id = int(pid)
        except Exception:
            return []
        start = request.query_params.get("start")
        end = request.query_params.get("end")
        # Delegar completamente al backend (database.py)
        from datetime import datetime
        fecha_inicio = None
        fecha_fin = None
        try:
            if start:
                fecha_inicio = datetime.strptime(start, "%Y-%m-%d").date()
            if end:
                fecha_fin = datetime.strptime(end, "%Y-%m-%d").date()
        except Exception:
            # Si las fechas no son válidas, se ignoran y se usa mes actual dentro del backend
            fecha_inicio = None
            fecha_fin = None

        try:
            sesiones = db.obtener_horas_trabajadas_profesor(profesor_id, fecha_inicio, fecha_fin)  # type: ignore
        except Exception:
            sesiones = []

        # Normalizar y filtrar: solo sesiones cerradas; permitimos minutos 0 para mostrar registro
        out = []
        for s in sesiones or []:
            minutos_val = s.get("minutos_totales")
            fin_val = s.get("hora_fin")
            # filtrar sin fin
            try:
                minutos_num = int(minutos_val) if minutos_val is not None else 0
            except Exception:
                minutos_num = 0
            if fin_val is None:
                continue
            # formato seguro para fecha y hora (HH:MM)
            def _fmt_date(d):
                try:
                    return str(d)[:10] if d is not None else ""
                except Exception:
                    return str(d) if d is not None else ""
            def _fmt_time(t):
                try:
                    sstr = str(t) if t is not None else ""
                    return sstr[:5] if len(sstr) >= 5 else sstr
                except Exception:
                    return ""
            out.append({
                "fecha": _fmt_date(s.get("fecha")),
                "inicio": _fmt_time(s.get("hora_inicio")),
                "fin": _fmt_time(fin_val),
                "minutos": minutos_num,
                "horas": round(minutos_num / 60.0, 2),
                "tipo": s.get("tipo_actividad") or ""
            })
        return out
    except Exception as e:
        logging.exception("Error en /api/profesor_sesiones")
        return []

# Ruta duplicada /api/profesor_resumen eliminada para evitar respuestas inconsistentes

@app.get("/api/usuario_pagos")
async def api_usuario_pagos(request: Request, _=Depends(require_owner)):
    """Lista de pagos reales de un usuario con soporte de búsqueda y paginación."""
    db = _get_db()
    if db is None:
        return []
    try:
        usuario_id = request.query_params.get("usuario_id")
        q = request.query_params.get("q")
        limit = request.query_params.get("limit")
        offset = request.query_params.get("offset")
        if not usuario_id:
            return []
        lim = int(limit) if limit and limit.isdigit() else 50
        off = int(offset) if offset and offset.isdigit() else 0
        with db.get_connection_context() as conn:  # type: ignore
            cur = conn.cursor()
            if q:
                cur.execute(
                    """
                    SELECT p.fecha_pago::date, p.monto, COALESCE(u.tipo_cuota,'')
                    FROM pagos p
                    JOIN usuarios u ON u.id = p.usuario_id
                    WHERE p.usuario_id = %s AND (u.tipo_cuota ILIKE %s)
                    ORDER BY p.fecha_pago DESC
                    LIMIT %s OFFSET %s
                    """,
                    (int(usuario_id), f"%{q}%", lim, off)
                )
            else:
                cur.execute(
                    """
                    SELECT p.fecha_pago::date, p.monto, COALESCE(u.tipo_cuota,'')
                    FROM pagos p
                    JOIN usuarios u ON u.id = p.usuario_id
                    WHERE p.usuario_id = %s
                    ORDER BY p.fecha_pago DESC
                    LIMIT %s OFFSET %s
                    """,
                    (int(usuario_id), lim, off)
                )
            rows = []
            for r in cur.fetchall():
                rows.append({
                    "fecha": str(r[0]) if r[0] is not None else None,
                    "monto": float(r[1] or 0),
                    "tipo_cuota": r[2],
                })
        return rows
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)

@app.get("/api/usuario_asistencias")
async def api_usuario_asistencias(request: Request, _=Depends(require_owner)):
    """Lista de asistencias reales de un usuario con soporte de búsqueda y paginación."""
    db = _get_db()
    if db is None:
        return []
    guard = _circuit_guard_json(db, "/api/usuario_asistencias")
    if guard:
        return guard
    try:
        usuario_id = request.query_params.get("usuario_id")
        q = request.query_params.get("q")
        limit = request.query_params.get("limit")
        offset = request.query_params.get("offset")
        if not usuario_id:
            return []
        lim = int(limit) if limit and limit.isdigit() else 50
        off = int(offset) if offset and offset.isdigit() else 0
        with db.get_connection_context() as conn:  # type: ignore
            cur = conn.cursor()
            if q:
                cur.execute(
                    """
                    SELECT fecha::date, hora_registro
                    FROM asistencias
                    WHERE usuario_id = %s AND (CAST(hora_registro AS TEXT) ILIKE %s)
                    ORDER BY fecha DESC
                    LIMIT %s OFFSET %s
                    """,
                    (int(usuario_id), f"%{q}%", lim, off)
                )
            else:
                cur.execute(
                    """
                    SELECT fecha::date, hora_registro
                    FROM asistencias
                    WHERE usuario_id = %s
                    ORDER BY fecha DESC
                    LIMIT %s OFFSET %s
                    """,
                    (int(usuario_id), lim, off)
                )
            rows = []
            for r in cur.fetchall():
                rows.append({
                    "fecha": str(r[0]) if r[0] is not None else None,
                    "hora": str(r[1]) if r[1] is not None else None,
                })
        return rows
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)

@app.get("/api/asistencias_detalle")
async def api_asistencias_detalle(request: Request, _=Depends(require_owner)):
    """Listado de asistencias con nombre del usuario para un rango de fechas (por defecto últimos 30 días), con búsqueda y paginación."""
    db = _get_db()
    if db is None:
        return []
    guard = _circuit_guard_json(db, "/api/asistencias_detalle")
    if guard:
        return guard
    try:
        start = request.query_params.get("start")
        end = request.query_params.get("end")
        q = request.query_params.get("q")
        limit = request.query_params.get("limit")
        offset = request.query_params.get("offset")
        lim = int(limit) if limit and limit.isdigit() else 500
        off = int(offset) if offset and offset.isdigit() else 0
        with db.get_connection_context() as conn:  # type: ignore
            cur = conn.cursor()
            if start and end:
                if q:
                    cur.execute(
                        """
                        SELECT a.fecha::date, a.hora_registro, u.nombre
                        FROM asistencias a
                        JOIN usuarios u ON u.id = a.usuario_id
                        WHERE a.fecha BETWEEN %s AND %s AND (u.nombre ILIKE %s)
                        ORDER BY a.fecha DESC, a.hora_registro DESC
                        LIMIT %s OFFSET %s
                        """,
                        (start, end, f"%{q}%", lim, off)
                    )
                else:
                    cur.execute(
                        """
                        SELECT a.fecha::date, a.hora_registro, u.nombre
                        FROM asistencias a
                        JOIN usuarios u ON u.id = a.usuario_id
                        WHERE a.fecha BETWEEN %s AND %s
                        ORDER BY a.fecha DESC, a.hora_registro DESC
                        LIMIT %s OFFSET %s
                        """,
                        (start, end, lim, off)
                    )
            else:
                if q:
                    cur.execute(
                        """
                        SELECT a.fecha::date, a.hora_registro, u.nombre
                        FROM asistencias a
                        JOIN usuarios u ON u.id = a.usuario_id
                        WHERE a.fecha >= CURRENT_DATE - INTERVAL '30 days' AND (u.nombre ILIKE %s)
                        ORDER BY a.fecha DESC, a.hora_registro DESC
                        LIMIT %s OFFSET %s
                        """,
                        (f"%{q}%", lim, off)
                    )
                else:
                    cur.execute(
                        """
                        SELECT a.fecha::date, a.hora_registro, u.nombre
                        FROM asistencias a
                        JOIN usuarios u ON u.id = a.usuario_id
                        WHERE a.fecha >= CURRENT_DATE - INTERVAL '30 days'
                        ORDER BY a.fecha DESC, a.hora_registro DESC
                        LIMIT %s OFFSET %s
                        """,
                        (lim, off)
                    )
        rows = []
        for r in cur.fetchall():
            rows.append({
                "fecha": str(r[0]) if r[0] is not None else None,
                "hora": str(r[1]) if r[1] is not None else None,
                "usuario": r[2] or ''
            })
        return rows
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


@app.get("/api/export")
async def api_export(_=Depends(require_owner)):
    # Exporta CSV de KPIs visibles en el dashboard
    rows: list[list[Any]] = [["Métrica", "Valor"]]
    try:
        kpis = await api_kpis()  # type: ignore
        if isinstance(kpis, dict) and 'kpis' in kpis:
            rows.append(["Socios activos", kpis['kpis'].get('total_activos', 0)])
            rows.append(["Ingresos del mes", kpis['kpis'].get('ingresos_mes_actual', 0.0)])
            rows.append(["Asistencias hoy", kpis['kpis'].get('asistencias_hoy', 0)])
            rows.append(["Nuevos (30d)", kpis['kpis'].get('nuevos_30_dias', 0)])
            rows.append(["ARPU (mes)", kpis.get('arpu', 0.0)])
            rows.append(["Morosos", kpis.get('morosos', 0)])
    except Exception:
        pass

    # Generar CSV en memoria y devolver como texto
    from io import StringIO
    buf = StringIO()
    w = csv.writer(buf)
    w.writerows(rows)
    buf.seek(0)
    return JSONResponse({"csv": buf.getvalue()})

# --- Exportación completa en ZIP con múltiples CSVs ---
@app.get("/api/export_csv")
async def api_export_csv(request: Request, _=Depends(require_owner)):
    """Genera un ZIP con múltiples CSVs del dashboard aplicando filtros actuales.

    Incluye:
    - kpis.csv: KPIs generales
    - usuarios.csv: detalle de usuarios con conteos
    - asistencias.csv: asistencias por rango (start/end)
    - pagos.csv: pagos por rango (start/end)
    - profesores.csv: detalle de profesores con sesiones/horas del rango
    - profesor_sesiones.csv: sesiones trabajadas por profesores en el rango
    """
    db = _get_db()
    if db is None:
        return JSONResponse({"error": "DB no disponible"}, status_code=500)

    start = request.query_params.get("start")
    end = request.query_params.get("end")
    prof_start = request.query_params.get("prof_start") or start
    prof_end = request.query_params.get("prof_end") or end

    from io import StringIO, BytesIO
    import zipfile

    def write_csv_to_zip(zf: zipfile.ZipFile, fname: str, headers: list[str], rows: list[list]):
        buf = StringIO()
        w = csv.writer(buf)
        if headers:
            w.writerow(headers)
        for r in rows:
            w.writerow(r)
        zf.writestr(fname, buf.getvalue())

    zip_buf = BytesIO()
    zf = zipfile.ZipFile(zip_buf, mode="w", compression=zipfile.ZIP_DEFLATED)

    # 1) KPIs
    try:
        k = await api_kpis()  # type: ignore
        kpis_rows = [["Métrica", "Valor"]]
        if isinstance(k, dict) and 'kpis' in k:
            kpis_rows += [
                ["Socios activos", k['kpis'].get('total_activos', 0)],
                ["Ingresos del mes", k['kpis'].get('ingresos_mes_actual', 0.0)],
                ["Asistencias hoy", k['kpis'].get('asistencias_hoy', 0)],
                ["Nuevos (30d)", k['kpis'].get('nuevos_30_dias', 0)],
                ["ARPU (mes)", k.get('arpu', 0.0)],
                ["Morosos", k.get('morosos', 0)],
            ]
        write_csv_to_zip(zf, "kpis.csv", [], kpis_rows)
    except Exception:
        write_csv_to_zip(zf, "kpis.csv", [], [["Error obteniendo KPIs"]])

    # 2) Usuarios
    try:
        with db.get_connection_context() as conn:  # type: ignore
            cur = conn.cursor()
            cur.execute(
                """
                SELECT u.id,
                       COALESCE(u.nombre,'') AS nombre,
                       COALESCE(u.rol,'') AS rol,
                       COALESCE(u.dni,'') AS dni,
                       COALESCE(u.telefono,'') AS telefono,
                       COALESCE(u.tipo_cuota,'') AS tipo_cuota,
                       COALESCE(u.activo,false) AS activo,
                       COALESCE(u.fecha_registro::date, CURRENT_DATE) AS fecha_registro,
                       COALESCE(p.pagos_count,0) AS pagos_count,
                       p.last_pago_fecha::date AS last_pago_fecha,
                       COALESCE(a.asistencias_count,0) AS asistencias_count
                FROM usuarios u
                LEFT JOIN (
                    SELECT usuario_id, COUNT(*) AS pagos_count, MAX(fecha_pago) AS last_pago_fecha
                    FROM pagos
                    GROUP BY usuario_id
                ) p ON p.usuario_id = u.id
                LEFT JOIN (
                    SELECT usuario_id, COUNT(*) AS asistencias_count
                    FROM asistencias
                    GROUP BY usuario_id
                ) a ON a.usuario_id = u.id
                ORDER BY u.id
                """
            )
            urows = []
            for r in cur.fetchall():
                urows.append([
                    r[0], r[1], r[2], r[3], r[4], r[5], bool(r[6]),
                    str(r[7]) if r[7] is not None else '',
                    int(r[8] or 0),
                    str(r[9]) if r[9] is not None else '',
                    int(r[10] or 0),
                ])
        write_csv_to_zip(zf, "usuarios.csv", [
            "id","nombre","rol","dni","telefono","tipo_cuota","activo","fecha_registro","pagos_count","last_pago_fecha","asistencias_count"
        ], urows)
    except Exception:
        write_csv_to_zip(zf, "usuarios.csv", [], [["Error exportando usuarios"]])

    # 3) Asistencias por rango
    try:
        with db.get_connection_context() as conn:  # type: ignore
            cur = conn.cursor()
            if start and end:
                cur.execute(
                    """
                    SELECT a.fecha::date, a.hora_registro, u.nombre
                    FROM asistencias a
                    JOIN usuarios u ON u.id = a.usuario_id
                    WHERE a.fecha BETWEEN %s AND %s
                    ORDER BY a.fecha DESC, a.hora_registro DESC
                    """,
                    (start, end)
                )
            else:
                # Últimos 30 días por defecto
                cur.execute(
                    """
                    SELECT a.fecha::date, a.hora_registro, u.nombre
                    FROM asistencias a
                    JOIN usuarios u ON u.id = a.usuario_id
                    WHERE a.fecha >= CURRENT_DATE - INTERVAL '30 day'
                    ORDER BY a.fecha DESC, a.hora_registro DESC
                    """
                )
            arows = [[str(r[0]) if r[0] is not None else '', str(r[1]) if r[1] is not None else '', r[2] or ''] for r in cur.fetchall()]
        write_csv_to_zip(zf, "asistencias.csv", ["fecha","hora","usuario"], arows)
    except Exception:
        write_csv_to_zip(zf, "asistencias.csv", [], [["Error exportando asistencias"]])

    # 4) Pagos por rango
    try:
        with db.get_connection_context() as conn:  # type: ignore
            cur = conn.cursor()
            if start and end:
                cur.execute(
                    """
                    SELECT p.usuario_id, u.nombre, p.fecha_pago::date, p.monto, COALESCE(u.tipo_cuota,'')
                    FROM pagos p
                    JOIN usuarios u ON u.id = p.usuario_id
                    WHERE p.fecha_pago BETWEEN %s AND %s
                    ORDER BY p.fecha_pago DESC
                    """,
                    (start, end)
                )
            else:
                cur.execute(
                    """
                    SELECT p.usuario_id, u.nombre, p.fecha_pago::date, p.monto, COALESCE(u.tipo_cuota,'')
                    FROM pagos p
                    JOIN usuarios u ON u.id = p.usuario_id
                    WHERE p.fecha_pago >= CURRENT_DATE - INTERVAL '30 day'
                    ORDER BY p.fecha_pago DESC
                    """
                )
            prows = []
            for r in cur.fetchall():
                prows.append([
                    int(r[0] or 0), r[1] or '', str(r[2]) if r[2] is not None else '', float(r[3] or 0.0), r[4] or ''
                ])
        write_csv_to_zip(zf, "pagos.csv", ["usuario_id","usuario","fecha_pago","monto","tipo_cuota"], prows)
    except Exception:
        write_csv_to_zip(zf, "pagos.csv", [], [["Error exportando pagos"]])

    # 5) Profesores detalle
    try:
        prof_rows = []
        detalle = await api_profesores_detalle(request)  # type: ignore
        if isinstance(detalle, list):
            for p in detalle:
                # Resumen de horarios como texto
                horarios = p.get("horarios") or []
                resumen = "; ".join([
                    f"{h.get('dia')}: {h.get('inicio')}-{h.get('fin')}" for h in horarios if isinstance(h, dict)
                ])
                prof_rows.append([
                    p.get("id"), p.get("nombre"), p.get("telefono"), int(p.get("horarios_count", 0)), resumen,
                    int(p.get("sesiones_mes", 0)), float(p.get("horas_mes", 0.0))
                ])
        write_csv_to_zip(zf, "profesores.csv", ["id","nombre","telefono","horarios_count","horarios_resumen","sesiones_rango","horas_rango"], prof_rows)
    except Exception:
        write_csv_to_zip(zf, "profesores.csv", [], [["Error exportando profesores"]])

    # 6) Sesiones de profesores por rango
    try:
        with db.get_connection_context() as conn:  # type: ignore
            cur = conn.cursor()
            if prof_start and prof_end:
                cur.execute(
                    """
                    SELECT profesor_id,
                           fecha::date,
                           hora_inicio,
                           hora_fin,
                           COALESCE(minutos_totales,0) AS minutos,
                           COALESCE(horas_totales,0) AS horas,
                           COALESCE(tipo_actividad,'') AS tipo
                    FROM profesor_horas_trabajadas
                    WHERE hora_fin IS NOT NULL AND fecha BETWEEN %s AND %s
                    ORDER BY fecha DESC, profesor_id
                    """,
                    (prof_start, prof_end)
                )
            else:
                # Mes actual por defecto
                from datetime import datetime, date, timedelta
                now = datetime.now()
                start_date = date(now.year, now.month, 1)
                if now.month == 12:
                    end_date = date(now.year, 12, 31)
                else:
                    end_date = date(now.year, now.month + 1, 1) - timedelta(days=1)
                cur.execute(
                    """
                    SELECT profesor_id,
                           fecha::date,
                           hora_inicio,
                           hora_fin,
                           COALESCE(minutos_totales,0) AS minutos,
                           COALESCE(horas_totales,0) AS horas,
                           COALESCE(tipo_actividad,'') AS tipo
                    FROM profesor_horas_trabajadas
                    WHERE hora_fin IS NOT NULL AND fecha BETWEEN %s AND %s
                    ORDER BY fecha DESC, profesor_id
                    """,
                    (start_date, end_date)
                )
            srows = []
            for r in cur.fetchall():
                srows.append([
                    int(r[0] or 0), str(r[1]) if r[1] is not None else '', str(r[2]) if r[2] is not None else '', str(r[3]) if r[3] is not None else '',
                    int(r[4] or 0), float(r[5] or 0.0), r[6] or ''
                ])
        write_csv_to_zip(zf, "profesor_sesiones.csv", ["profesor_id","fecha","hora_inicio","hora_fin","minutos","horas","tipo"], srows)
    except Exception:
        write_csv_to_zip(zf, "profesor_sesiones.csv", [], [["Error exportando sesiones de profesores"]])

    # Finalizar ZIP y devolver
    zf.close()
    zip_buf.seek(0)
    return Response(content=zip_buf.getvalue(), media_type="application/zip", headers={
        "Content-Disposition": "attachment; filename=dashboard_export.zip"
    })
@app.get("/api/profesor_resumen")
async def api_profesor_resumen(request: Request, _=Depends(require_owner)):
    """Resumen de horas trabajadas, proyectadas y extras para un profesor.

    - Trabajadas: suma de `minutos_totales` de sesiones cerradas en el rango.
    - Proyectadas: suma de minutos desde `horarios_profesores` (disponibilidades activas)
      por cada día del rango, contando ocurrencias por día de semana. No depende de clases.
    - Extras: trabajadas - proyectadas.
    """
    db = _get_db()
    if db is None:
        return {
            "total_sesiones": 0,
            "minutos_trabajados": 0,
            "horas_trabajadas": 0.0,
            "minutos_proyectados": 0,
            "horas_proyectadas": 0.0,
            "minutos_extras": 0,
            "horas_extras": 0.0,
        }
    try:
        pid = request.query_params.get("profesor_id")
        if not pid:
            return {
                "total_sesiones": 0,
                "minutos_trabajados": 0,
                "horas_trabajadas": 0.0,
                "minutos_proyectados": 0,
                "horas_proyectadas": 0.0,
                "minutos_extras": 0,
                "horas_extras": 0.0,
            }
        try:
            profesor_id = int(pid)
        except Exception:
            return {
                "total_sesiones": 0,
                "minutos_trabajados": 0,
                "horas_trabajadas": 0.0,
                "minutos_proyectados": 0,
                "horas_proyectadas": 0.0,
                "minutos_extras": 0,
                "horas_extras": 0.0,
            }

        start = request.query_params.get("start")
        end = request.query_params.get("end")
        from datetime import datetime, date, timedelta
        now = datetime.now()
        if start and end:
            try:
                start_date = datetime.strptime(start, "%Y-%m-%d").date()
                end_date = datetime.strptime(end, "%Y-%m-%d").date()
            except Exception:
                start_date = date(now.year, now.month, 1)
                # último día del mes
                if now.month == 12:
                    end_date = date(now.year, 12, 31)
                else:
                    end_date = date(now.year, now.month + 1, 1) - timedelta(days=1)
        else:
            start_date = date(now.year, now.month, 1)
            if now.month == 12:
                end_date = date(now.year, 12, 31)
            else:
                end_date = date(now.year, now.month + 1, 1) - timedelta(days=1)

        with db.get_connection_context() as conn:  # type: ignore
            cur = conn.cursor()

            # 1) Minutos trabajados y cantidad de sesiones cerradas
            cur.execute(
                """
                SELECT COALESCE(SUM(pht.minutos_totales),0) AS min_trab,
                       COUNT(*) AS total_ses
                FROM profesor_horas_trabajadas pht
                WHERE pht.profesor_id = %s
                  AND pht.hora_fin IS NOT NULL
                  AND pht.fecha BETWEEN %s AND %s
                """,
                (profesor_id, start_date, end_date)
            )
            r = cur.fetchone() or (0, 0)
            min_trabajados = int(r[0] or 0)
            total_sesiones = int(r[1] or 0)

        # 2) Minutos proyectados: cálculo centralizado en database.py (no depende de clases)
        min_proyectados = 0
        try:
            if hasattr(db, 'obtener_minutos_proyectados_profesor_rango'):
                res = db.obtener_minutos_proyectados_profesor_rango(  # type: ignore
                    profesor_id,
                    start_date.isoformat(),
                    end_date.isoformat()
                )
                if isinstance(res, dict) and res.get('success'):
                    min_proyectados = int(res.get('minutos_proyectados', 0) or 0)
        except Exception:
            min_proyectados = 0

        return {
            "total_sesiones": total_sesiones,
            "minutos_trabajados": min_trabajados,
            "horas_trabajadas": round(min_trabajados / 60.0, 2),
            "minutos_proyectados": int(min_proyectados),
            "horas_proyectadas": round(min_proyectados / 60.0, 2),
            "minutos_extras": int(min_trabajados - min_proyectados),
            "horas_extras": round((min_trabajados - min_proyectados) / 60.0, 2),
        }
    except Exception as e:
        import traceback
        traceback.print_exc()
        return {
            "total_sesiones": 0,
            "minutos_trabajados": 0,
            "horas_trabajadas": 0.0,
            "minutos_proyectados": 0,
            "horas_proyectadas": 0.0,
            "minutos_extras": 0,
            "horas_extras": 0.0,
        }
