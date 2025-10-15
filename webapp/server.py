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

# Inicialización de la app web
app = FastAPI(
    title="GymMS WebApp",
    version="2.0",
    # Permite servir detrás de reverse proxy con subpath
    root_path=os.getenv("ROOT_PATH", "").strip(),
)
app.add_middleware(SessionMiddleware, secret_key=os.getenv("WEBAPP_SECRET_KEY", secrets.token_urlsafe(32)))

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
        hosts = ["gym-ms-zrk.up.railway.app", "localhost", "127.0.0.1"]
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

# --- Utilidad: asegurar esquema mínimo para sincronización ---
def _ensure_sync_schema() -> Dict[str, Any]:
    """Crea columnas y tablas necesarias para el sistema de sincronización si faltan.

    Idempotente y segura para ejecutarse múltiples veces.
    """
    db = _get_db()
    if db is None:
        return {"success": False, "message": "DB no disponible"}
    created: Dict[str, Any] = {"tables": [], "columns": []}
    try:
        with db.get_connection_context() as conn:  # type: ignore
            cur = conn.cursor()
            # Tablas auxiliares de sync
            try:
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS sync_applied_ops (
                        op_id TEXT,
                        device_id TEXT,
                        applied_at TIMESTAMPTZ DEFAULT NOW(),
                        PRIMARY KEY (op_id, device_id)
                    )
                    """
                )
                created["tables"].append("sync_applied_ops")
            except Exception:
                pass
            try:
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS sync_deletes (
                        entity TEXT,
                        key JSONB,
                        updated_at TIMESTAMPTZ DEFAULT NOW(),
                        device_id TEXT
                    )
                    """
                )
                created["tables"].append("sync_deletes")
            except Exception:
                pass

            # Columnas requeridas en entidades principales
            tables = [
                "usuarios",
                "pagos",
                "asistencias",
                "usuario_notas",
                "rutinas",
                "ejercicios",
                "etiquetas",
                "usuario_etiquetas",
            ]
            for t in tables:
                try:
                    cur.execute(f"ALTER TABLE {t} ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ DEFAULT NOW()")
                    created["columns"].append(f"{t}.updated_at")
                except Exception:
                    pass
                try:
                    cur.execute(f"ALTER TABLE {t} ADD COLUMN IF NOT EXISTS updated_by_device TEXT")
                    created["columns"].append(f"{t}.updated_by_device")
                except Exception:
                    pass
            # Defaults seguros adicionales para WhatsApp en esquema base
            try:
                cur.execute("ALTER TABLE IF EXISTS whatsapp_messages ALTER COLUMN status SET DEFAULT 'sent'")
                cur.execute("ALTER TABLE IF EXISTS whatsapp_messages ALTER COLUMN message_content SET DEFAULT ''")
                cur.execute("UPDATE whatsapp_messages SET status = 'sent' WHERE status IS NULL")
                cur.execute("UPDATE whatsapp_messages SET message_content = '' WHERE message_content IS NULL")
                cur.execute("ALTER TABLE IF EXISTS whatsapp_templates ALTER COLUMN header_text SET DEFAULT ''")
                cur.execute("ALTER TABLE IF EXISTS whatsapp_templates ALTER COLUMN variables SET DEFAULT '{}'::jsonb")
                cur.execute("UPDATE whatsapp_templates SET header_text = '' WHERE header_text IS NULL")
                cur.execute("UPDATE whatsapp_templates SET variables = '{}'::jsonb WHERE variables IS NULL")
                cur.execute("ALTER TABLE IF EXISTS whatsapp_config ALTER COLUMN access_token SET DEFAULT ''")
                cur.execute("ALTER TABLE IF EXISTS whatsapp_config ALTER COLUMN phone_id SET DEFAULT ''")
                cur.execute("ALTER TABLE IF EXISTS whatsapp_config ALTER COLUMN waba_id SET DEFAULT ''")
                cur.execute("UPDATE whatsapp_config SET access_token = '' WHERE access_token IS NULL")
                cur.execute("UPDATE whatsapp_config SET phone_id = '' WHERE phone_id IS NULL")
                cur.execute("UPDATE whatsapp_config SET waba_id = '' WHERE waba_id IS NULL")
            except Exception:
                pass
            conn.commit()
        return {"success": True, "created": created}
    except Exception as e:
        try:
            logging.exception("Error asegurando esquema de sync")
        except Exception:
            pass
        return {"success": False, "message": str(e), "created": created}

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
""" Legacy sync removed: api_sync_upload disabled. Use SymmetricDS. """
# @app.post("/api/sync/upload")  # disabled: legacy sync removed
async def api_sync_upload(request: Request):
    """Recibe operaciones de sincronización en lote desde clientes y las aplica.

    Formato esperado:
      { "operations": [ { "type": "user.add|user.update|user.delete", "payload": { ... }, "op_id": "uuid", "source": {"device_id": "..."} }, ... ] }

    Responde 202 si el payload es válido y 200 si además informa conteos de aplicadas/omitidas.
    """
    return JSONResponse(
        {"detail": "Legacy sync removed. Use SymmetricDS."},
        status_code=410,
    )
    ''' Legacy sync body disabled; see SymmetricDS setup.
    try:
        # Autenticación opcional por token
        try:
            expected = os.getenv("SYNC_API_TOKEN", "").strip()
            if expected:
                auth = request.headers.get("Authorization") or ""
                if not isinstance(auth, str) or not auth.strip().startswith("Bearer ") or auth.strip()[7:] != expected:
                    return JSONResponse({"success": False, "message": "No autorizado"}, status_code=401)
        except Exception:
            pass
        try:
            payload = await request.json()
        except Exception:
            payload = {}
        if not isinstance(payload, dict):
            return JSONResponse({"success": False, "message": "JSON inválido"}, status_code=400)
        ops = payload.get("operations")
        if not isinstance(ops, list) or not ops:
            return JSONResponse({"success": False, "message": "operations vacío"}, status_code=400)

        # Trazabilidad mínima en logs con tamaño acotado
        try:
            rid = getattr(getattr(request, 'state', object()), 'request_id', str(uuid.uuid4()))
            logging.info(f"sync upload rid={rid} count={len(ops)} from={request.client.host if request.client else '-'}")
        except Exception:
            pass

        db = _get_db()
        if db is None:
            return JSONResponse({"success": False, "message": "DB no disponible"}, status_code=503)

        # Circuit breaker: si está abierto, responder 503 temprano
        try:
            guard = _circuit_guard_json(db, endpoint="/api/sync/upload")  # type: ignore
            if guard is not None:
                return guard
        except Exception:
            pass

        # Asegurar esquema de sincronización de forma defensiva (idempotente)
        try:
            _ensure_sync_schema()
        except Exception:
            pass

        applied = 0
        skipped = 0
        failed = 0
        errors: Dict[str, str] = {}

        try:
            _conn_attempts = 0
            while True:
                try:
                    # Ejecutar migraciones en una sesión dedicada en autocommit
                    with db.autocommit_session() as conn:  # type: ignore
                        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:  # type: ignore
                            # Tablas auxiliares para idempotencia y propagación de deletes
                            try:
                                cur.execute(
                                    """
                                    CREATE TABLE IF NOT EXISTS sync_applied_ops (
                                        op_id TEXT,
                                        device_id TEXT,
                                        applied_at TIMESTAMPTZ DEFAULT NOW(),
                                        PRIMARY KEY (op_id, device_id)
                                    )
                                    """
                                )
                                cur.execute(
                                    """
                                    CREATE TABLE IF NOT EXISTS sync_deletes (
                                        id BIGSERIAL PRIMARY KEY,
                                        entity TEXT NOT NULL,
                                        key JSONB NOT NULL,
                                        updated_at TIMESTAMPTZ DEFAULT NOW()
                                    )
                                    """
                                )
                                # Permitir registrar device_id en deletes para evitar eco
                                try:
                                    cur.execute("ALTER TABLE IF EXISTS sync_deletes ADD COLUMN IF NOT EXISTS device_id TEXT")
                                except Exception:
                                    pass
                                # Asegurar columnas de tracking para sync en tablas clave
                                cur.execute("ALTER TABLE IF EXISTS pagos ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ DEFAULT NOW()")
                                cur.execute("ALTER TABLE IF EXISTS pagos ADD COLUMN IF NOT EXISTS updated_by_device TEXT")
                                cur.execute("ALTER TABLE IF EXISTS asistencias ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ DEFAULT NOW()")
                                cur.execute("ALTER TABLE IF EXISTS asistencias ADD COLUMN IF NOT EXISTS updated_by_device TEXT")
                                cur.execute("ALTER TABLE IF EXISTS clase_asistencia_historial ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ DEFAULT NOW()")
                                cur.execute("ALTER TABLE IF EXISTS clase_asistencia_historial ADD COLUMN IF NOT EXISTS updated_by_device TEXT")
                                # Extensiones de sincronización nuevas
                                cur.execute("ALTER TABLE IF EXISTS horarios_profesores ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ DEFAULT NOW()")
                                cur.execute("ALTER TABLE IF EXISTS horarios_profesores ADD COLUMN IF NOT EXISTS updated_by_device TEXT")
                                cur.execute("ALTER TABLE IF EXISTS profesor_suplencias ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ DEFAULT NOW()")
                                cur.execute("ALTER TABLE IF EXISTS profesor_suplencias ADD COLUMN IF NOT EXISTS updated_by_device TEXT")
                                # Clases y derivados
                                cur.execute("ALTER TABLE IF EXISTS clases ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ DEFAULT NOW()")
                                cur.execute("ALTER TABLE IF EXISTS clases ADD COLUMN IF NOT EXISTS updated_by_device TEXT")
                                cur.execute("ALTER TABLE IF EXISTS clases_horarios ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ DEFAULT NOW()")
                                cur.execute("ALTER TABLE IF EXISTS clases_horarios ADD COLUMN IF NOT EXISTS updated_by_device TEXT")
                                cur.execute("ALTER TABLE IF EXISTS clase_usuarios ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ DEFAULT NOW()")
                                cur.execute("ALTER TABLE IF EXISTS clase_usuarios ADD COLUMN IF NOT EXISTS updated_by_device TEXT")
                                # Clases y derivados
                                cur.execute("ALTER TABLE IF EXISTS clases ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ DEFAULT NOW()")
                                cur.execute("ALTER TABLE IF EXISTS clases ADD COLUMN IF NOT EXISTS updated_by_device TEXT")
                                cur.execute("ALTER TABLE IF EXISTS clases_horarios ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ DEFAULT NOW()")
                                cur.execute("ALTER TABLE IF EXISTS clases_horarios ADD COLUMN IF NOT EXISTS updated_by_device TEXT")
                                cur.execute("ALTER TABLE IF EXISTS clase_usuarios ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ DEFAULT NOW()")
                                cur.execute("ALTER TABLE IF EXISTS clase_usuarios ADD COLUMN IF NOT EXISTS updated_by_device TEXT")
                                # Rutinas y ejercicios
                                cur.execute("ALTER TABLE IF EXISTS rutinas ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ DEFAULT NOW()")
                                cur.execute("ALTER TABLE IF EXISTS rutinas ADD COLUMN IF NOT EXISTS updated_by_device TEXT")
                                cur.execute("ALTER TABLE IF EXISTS rutinas_ejercicios ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ DEFAULT NOW()")
                                cur.execute("ALTER TABLE IF EXISTS rutinas_ejercicios ADD COLUMN IF NOT EXISTS updated_by_device TEXT")
                                cur.execute("ALTER TABLE IF EXISTS ejercicios ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ DEFAULT NOW()")
                                cur.execute("ALTER TABLE IF EXISTS ejercicios ADD COLUMN IF NOT EXISTS updated_by_device TEXT")
                                # Tablas adicionales operativas no cubiertas
                                cur.execute("ALTER TABLE IF EXISTS rutina_ejercicios ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ DEFAULT NOW()")
                                cur.execute("ALTER TABLE IF EXISTS rutina_ejercicios ADD COLUMN IF NOT EXISTS updated_by_device TEXT")
                                cur.execute("ALTER TABLE IF EXISTS clase_ejercicios ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ DEFAULT NOW()")
                                cur.execute("ALTER TABLE IF EXISTS clase_ejercicios ADD COLUMN IF NOT EXISTS updated_by_device TEXT")
                                cur.execute("ALTER TABLE IF EXISTS ejercicio_grupos ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ DEFAULT NOW()")
                                cur.execute("ALTER TABLE IF EXISTS ejercicio_grupos ADD COLUMN IF NOT EXISTS updated_by_device TEXT")
                                cur.execute("ALTER TABLE IF EXISTS ejercicio_grupo_items ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ DEFAULT NOW()")
                                cur.execute("ALTER TABLE IF EXISTS ejercicio_grupo_items ADD COLUMN IF NOT EXISTS updated_by_device TEXT")
                                cur.execute("ALTER TABLE IF EXISTS profesores ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ DEFAULT NOW()")
                                cur.execute("ALTER TABLE IF EXISTS profesores ADD COLUMN IF NOT EXISTS updated_by_device TEXT")
                                cur.execute("ALTER TABLE IF EXISTS profesores_horarios_disponibilidad ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ DEFAULT NOW()")
                                cur.execute("ALTER TABLE IF EXISTS profesores_horarios_disponibilidad ADD COLUMN IF NOT EXISTS updated_by_device TEXT")
                                cur.execute("ALTER TABLE IF EXISTS profesor_evaluaciones ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ DEFAULT NOW()")
                                cur.execute("ALTER TABLE IF EXISTS profesor_evaluaciones ADD COLUMN IF NOT EXISTS updated_by_device TEXT")
                                cur.execute("ALTER TABLE IF EXISTS profesor_disponibilidad ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ DEFAULT NOW()")
                                cur.execute("ALTER TABLE IF EXISTS profesor_disponibilidad ADD COLUMN IF NOT EXISTS updated_by_device TEXT")
                                cur.execute("ALTER TABLE IF EXISTS profesor_suplencias_generales ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ DEFAULT NOW()")
                                cur.execute("ALTER TABLE IF EXISTS profesor_suplencias_generales ADD COLUMN IF NOT EXISTS updated_by_device TEXT")
                                cur.execute("ALTER TABLE IF EXISTS tipos_cuota ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ DEFAULT NOW()")
                                cur.execute("ALTER TABLE IF EXISTS tipos_cuota ADD COLUMN IF NOT EXISTS updated_by_device TEXT")
                                cur.execute("ALTER TABLE IF EXISTS numeracion_comprobantes ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ DEFAULT NOW()")
                                cur.execute("ALTER TABLE IF EXISTS numeracion_comprobantes ADD COLUMN IF NOT EXISTS updated_by_device TEXT")
                                cur.execute("ALTER TABLE IF EXISTS comprobantes_pago ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ DEFAULT NOW()")
                                cur.execute("ALTER TABLE IF EXISTS comprobantes_pago ADD COLUMN IF NOT EXISTS updated_by_device TEXT")
                                cur.execute("ALTER TABLE IF EXISTS pago_detalles ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ DEFAULT NOW()")
                                cur.execute("ALTER TABLE IF EXISTS pago_detalles ADD COLUMN IF NOT EXISTS updated_by_device TEXT")
                                cur.execute("ALTER TABLE IF EXISTS historial_estados ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ DEFAULT NOW()")
                                cur.execute("ALTER TABLE IF EXISTS historial_estados ADD COLUMN IF NOT EXISTS updated_by_device TEXT")
                                # Configuración de comprobantes
                                cur.execute("ALTER TABLE IF EXISTS configuracion_comprobantes ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ DEFAULT NOW()")
                                cur.execute("ALTER TABLE IF EXISTS configuracion_comprobantes ADD COLUMN IF NOT EXISTS updated_by_device TEXT")
                                # WhatsApp: tracking y defaults/sanitización
                                cur.execute("ALTER TABLE IF EXISTS whatsapp_messages ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ DEFAULT NOW()")
                                cur.execute("ALTER TABLE IF EXISTS whatsapp_messages ADD COLUMN IF NOT EXISTS updated_by_device TEXT")
                                cur.execute("ALTER TABLE IF EXISTS whatsapp_templates ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ DEFAULT NOW()")
                                cur.execute("ALTER TABLE IF EXISTS whatsapp_templates ADD COLUMN IF NOT EXISTS updated_by_device TEXT")
                                cur.execute("ALTER TABLE IF EXISTS whatsapp_config ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ DEFAULT NOW()")
                                cur.execute("ALTER TABLE IF EXISTS whatsapp_config ADD COLUMN IF NOT EXISTS updated_by_device TEXT")
                                # Defaults seguros para WhatsApp
                                cur.execute("ALTER TABLE IF EXISTS whatsapp_messages ALTER COLUMN status SET DEFAULT 'sent'")
                                cur.execute("ALTER TABLE IF EXISTS whatsapp_messages ALTER COLUMN message_content SET DEFAULT ''")
                                cur.execute("UPDATE whatsapp_messages SET status = 'sent' WHERE status IS NULL")
                                cur.execute("UPDATE whatsapp_messages SET message_content = '' WHERE message_content IS NULL")
                                cur.execute("ALTER TABLE IF EXISTS whatsapp_templates ALTER COLUMN header_text SET DEFAULT ''")
                                cur.execute("ALTER TABLE IF EXISTS whatsapp_templates ALTER COLUMN variables SET DEFAULT '{}'::jsonb")
                                cur.execute("UPDATE whatsapp_templates SET header_text = '' WHERE header_text IS NULL")
                                cur.execute("UPDATE whatsapp_templates SET variables = '{}'::jsonb WHERE variables IS NULL")
                                cur.execute("ALTER TABLE IF EXISTS whatsapp_config ALTER COLUMN access_token SET DEFAULT ''")
                                cur.execute("ALTER TABLE IF EXISTS whatsapp_config ALTER COLUMN phone_id SET DEFAULT ''")
                                cur.execute("ALTER TABLE IF EXISTS whatsapp_config ALTER COLUMN waba_id SET DEFAULT ''")
                                cur.execute("UPDATE whatsapp_config SET access_token = '' WHERE access_token IS NULL")
                                cur.execute("UPDATE whatsapp_config SET phone_id = '' WHERE phone_id IS NULL")
                                cur.execute("UPDATE whatsapp_config SET waba_id = '' WHERE waba_id IS NULL")
                                cur.execute("ALTER TABLE IF EXISTS especialidades ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ DEFAULT NOW()")
                                cur.execute("ALTER TABLE IF EXISTS especialidades ADD COLUMN IF NOT EXISTS updated_by_device TEXT")
                                cur.execute("ALTER TABLE IF EXISTS profesor_especialidades ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ DEFAULT NOW()")
                                cur.execute("ALTER TABLE IF EXISTS profesor_especialidades ADD COLUMN IF NOT EXISTS updated_by_device TEXT")
                                cur.execute("ALTER TABLE IF EXISTS profesor_certificaciones ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ DEFAULT NOW()")
                                cur.execute("ALTER TABLE IF EXISTS profesor_certificaciones ADD COLUMN IF NOT EXISTS updated_by_device TEXT")
                                cur.execute("ALTER TABLE IF EXISTS profesor_horas_trabajadas ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ DEFAULT NOW()")
                                cur.execute("ALTER TABLE IF EXISTS profesor_horas_trabajadas ADD COLUMN IF NOT EXISTS updated_by_device TEXT")
                                cur.execute("ALTER TABLE IF EXISTS schedule_conflicts ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ DEFAULT NOW()")
                                cur.execute("ALTER TABLE IF EXISTS schedule_conflicts ADD COLUMN IF NOT EXISTS updated_by_device TEXT")
                                cur.execute("ALTER TABLE IF EXISTS configuracion_comprobantes ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ DEFAULT NOW()")
                                cur.execute("ALTER TABLE IF EXISTS configuracion_comprobantes ADD COLUMN IF NOT EXISTS updated_by_device TEXT")

                                # WhatsApp operational tables tracking
                                cur.execute("ALTER TABLE IF EXISTS whatsapp_messages ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ DEFAULT NOW()")
                                cur.execute("ALTER TABLE IF EXISTS whatsapp_messages ADD COLUMN IF NOT EXISTS updated_by_device TEXT")
                                cur.execute("ALTER TABLE IF EXISTS whatsapp_templates ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ DEFAULT NOW()")
                                cur.execute("ALTER TABLE IF EXISTS whatsapp_templates ADD COLUMN IF NOT EXISTS updated_by_device TEXT")
                                cur.execute("ALTER TABLE IF EXISTS whatsapp_config ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ DEFAULT NOW()")
                                cur.execute("ALTER TABLE IF EXISTS whatsapp_config ADD COLUMN IF NOT EXISTS updated_by_device TEXT")
                                # Disponibilidad de profesores del widget
                                try:
                                    cur.execute("ALTER TABLE IF EXISTS professor_availability ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ DEFAULT NOW()")
                                    cur.execute("ALTER TABLE IF EXISTS professor_availability ADD COLUMN IF NOT EXISTS updated_by_device TEXT")
                                    cur.execute("ALTER TABLE IF EXISTS professor_availability ALTER COLUMN status SET DEFAULT 'disponible'")
                                    cur.execute("ALTER TABLE IF EXISTS professor_availability ALTER COLUMN notes SET DEFAULT ''")
                                    cur.execute("UPDATE professor_availability SET status = 'disponible' WHERE status IS NULL")
                                    cur.execute("UPDATE professor_availability SET notes = '' WHERE notes IS NULL")
                                except Exception:
                                    pass
                                # Etiquetas, usuario_etiquetas y usuario_notas
                                cur.execute("ALTER TABLE IF EXISTS etiquetas ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ DEFAULT NOW()")
                                cur.execute("ALTER TABLE IF EXISTS etiquetas ADD COLUMN IF NOT EXISTS updated_by_device TEXT")
                                cur.execute("ALTER TABLE IF EXISTS usuario_etiquetas ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ DEFAULT NOW()")
                                cur.execute("ALTER TABLE IF EXISTS usuario_etiquetas ADD COLUMN IF NOT EXISTS updated_by_device TEXT")
                                cur.execute("ALTER TABLE IF EXISTS usuario_notas ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ DEFAULT NOW()")
                                cur.execute("ALTER TABLE IF EXISTS usuario_notas ADD COLUMN IF NOT EXISTS updated_by_device TEXT")
                                # Defaults y saneamiento de datos existentes para usuario_notas
                                try:
                                    cur.execute("ALTER TABLE IF EXISTS usuario_notas ALTER COLUMN categoria SET DEFAULT 'general'")
                                except Exception:
                                    pass
                                try:
                                    cur.execute("ALTER TABLE IF EXISTS usuario_notas ALTER COLUMN importancia SET DEFAULT 'normal'")
                                except Exception:
                                    pass
                                # Asegurar defaults también para cadenas requeridas
                                try:
                                    cur.execute("ALTER TABLE IF EXISTS usuario_notas ALTER COLUMN titulo SET DEFAULT ''")
                                except Exception:
                                    pass
                                try:
                                    cur.execute("ALTER TABLE IF EXISTS usuario_notas ALTER COLUMN contenido SET DEFAULT ''")
                                except Exception:
                                    pass
                                try:
                                    cur.execute("UPDATE usuario_notas SET categoria = 'general' WHERE categoria IS NULL")
                                except Exception:
                                    pass
                                try:
                                    cur.execute("UPDATE usuario_notas SET importancia = 'normal' WHERE importancia IS NULL")
                                except Exception:
                                    pass
                                try:
                                    cur.execute("UPDATE usuario_notas SET titulo = '' WHERE titulo IS NULL")
                                except Exception:
                                    pass
                                try:
                                    cur.execute("UPDATE usuario_notas SET contenido = '' WHERE contenido IS NULL")
                                except Exception:
                                    pass
                                cur.execute("ALTER TABLE IF EXISTS usuario_notas ALTER COLUMN categoria SET DEFAULT 'general'")
                                cur.execute("ALTER TABLE IF EXISTS usuario_notas ALTER COLUMN importancia SET DEFAULT 'normal'")
                            except Exception:
                                # No bloquear si fallan migraciones ligeras
                                pass


                    # Migraciones ligeras adicionales: saneo de booleanos y defaults transversales
                    try:
                        stmts = [
                            # Usuarios: defaults seguros
                            "ALTER TABLE IF EXISTS usuarios ALTER COLUMN activo SET DEFAULT TRUE",
                            "UPDATE usuarios SET activo = TRUE WHERE activo IS NULL AND rol IS DISTINCT FROM 'dueño'",
                            "ALTER TABLE IF EXISTS usuarios ALTER COLUMN telefono SET DEFAULT ''",
                            "UPDATE usuarios SET telefono = '' WHERE telefono IS NULL AND rol IS DISTINCT FROM 'dueño'",
                            "ALTER TABLE IF EXISTS usuarios ALTER COLUMN rol SET DEFAULT 'socio'",
                            "UPDATE usuarios SET rol = 'socio' WHERE rol IS NULL",
                            # Clases y horarios
                            "ALTER TABLE IF EXISTS clases ALTER COLUMN activa SET DEFAULT TRUE",
                            "UPDATE clases SET activa = TRUE WHERE activa IS NULL",
                            "ALTER TABLE IF EXISTS clases_horarios ALTER COLUMN activo SET DEFAULT TRUE",
                            "UPDATE clases_horarios SET activo = TRUE WHERE activo IS NULL",
                            # Rutinas
                            "ALTER TABLE IF EXISTS rutinas ALTER COLUMN activa SET DEFAULT TRUE",
                            "UPDATE rutinas SET activa = TRUE WHERE activa IS NULL",
                            # Etiquetas
                            "ALTER TABLE IF EXISTS etiquetas ALTER COLUMN activo SET DEFAULT TRUE",
                            "UPDATE etiquetas SET activo = TRUE WHERE activo IS NULL",
                            # Métodos y conceptos de pago
                            "ALTER TABLE IF EXISTS metodos_pago ALTER COLUMN activo SET DEFAULT TRUE",
                            "UPDATE metodos_pago SET activo = TRUE WHERE activo IS NULL",
                            "ALTER TABLE IF EXISTS conceptos_pago ALTER COLUMN activo SET DEFAULT TRUE",
                            "UPDATE conceptos_pago SET activo = TRUE WHERE activo IS NULL",
                            # Estados de usuario y relaciones
                            "ALTER TABLE IF EXISTS usuario_estados ALTER COLUMN activo SET DEFAULT TRUE",
                            "UPDATE usuario_estados SET activo = TRUE WHERE activo IS NULL",
                            "ALTER TABLE IF EXISTS clase_lista_espera ALTER COLUMN activo SET DEFAULT TRUE",
                            "UPDATE clase_lista_espera SET activo = TRUE WHERE activo IS NULL",
                            "ALTER TABLE IF EXISTS profesor_clase_asignaciones ALTER COLUMN activa SET DEFAULT TRUE",
                            "UPDATE profesor_clase_asignaciones SET activa = TRUE WHERE activa IS NULL",
                            # Temas y programación
                            "ALTER TABLE IF EXISTS custom_themes ALTER COLUMN activo SET DEFAULT TRUE",
                            "UPDATE custom_themes SET activo = TRUE WHERE activo IS NULL",
                            "ALTER TABLE IF EXISTS theme_schedules ALTER COLUMN activo SET DEFAULT TRUE",
                            "UPDATE theme_schedules SET activo = TRUE WHERE activo IS NULL",
                            "ALTER TABLE IF EXISTS theme_schedules ALTER COLUMN is_active SET DEFAULT TRUE",
                            "UPDATE theme_schedules SET is_active = TRUE WHERE is_active IS NULL",
                            # Notificaciones de cupos
                            "ALTER TABLE IF EXISTS notificaciones_cupos ALTER COLUMN activo SET DEFAULT TRUE",
                            "UPDATE notificaciones_cupos SET activo = TRUE WHERE activo IS NULL",
                            "ALTER TABLE IF EXISTS notificaciones_cupos ALTER COLUMN leida SET DEFAULT FALSE",
                            "UPDATE notificaciones_cupos SET leida = FALSE WHERE leida IS NULL",
                            # Asistencias: defaults seguros de fecha/hora
                            "ALTER TABLE IF EXISTS asistencias ALTER COLUMN fecha SET DEFAULT NOW()",
                            "UPDATE asistencias SET fecha = NOW() WHERE fecha IS NULL",
                            # hora_registro puede ser TIME; usar TRY/CATCH externo por si el tipo difiere
                            "ALTER TABLE IF EXISTS asistencias ALTER COLUMN hora_registro SET DEFAULT CAST(NOW() AS time)",
                            "UPDATE asistencias SET hora_registro = CAST(NOW() AS time) WHERE hora_registro IS NULL",
                        ]
                        for _stmt in stmts:
                            try:
                                cur.execute(_stmt)
                            except Exception:
                                pass
                        # Verificación ligera: log de NULLs restantes en columnas saneadas
                        try:
                            checks = [
                                ("usuarios", "telefono"),
                                ("usuarios", "rol"),
                                ("usuarios", "activo"),
                                ("etiquetas", "activo"),
                                ("clases", "activa"),
                                ("clases_horarios", "activo"),
                                ("rutinas", "activa"),
                                ("asistencias", "fecha"),
                                ("asistencias", "hora_registro"),
                                ("usuario_notas", "titulo"),
                                ("usuario_notas", "contenido"),
                            ]
                            null_report = []
                            for tbl, col in checks:
                                try:
                                    cur.execute(f"SELECT COUNT(*) AS c FROM {tbl} WHERE {col} IS NULL")
                                    r = cur.fetchone() or {}
                                    c = r.get("c") if isinstance(r, dict) else (r[0] if r else 0)
                                    if c:
                                        null_report.append(f"{tbl}.{col}={c}")
                                except Exception:
                                    pass
                            if null_report:
                                print("[migraciones-lite] Columnas aún con NULL:", ", ".join(null_report))
                        except Exception:
                            pass
                    except Exception:
                        pass

                    # Abrir una conexión normal para procesar operaciones
                    with db.get_connection_context() as conn:  # type: ignore
                        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:  # type: ignore
                            sp_idx = 0
                            for raw in ops:
                                try:
                                    op = raw or {}
                                    name = op.get("type") or op.get("name") or ""
                                    payload = op.get("payload") or op.get("data") or {}
                                    op_id = op.get("op_id") or op.get("id")
                                    source = op.get("source") or {}
                                    device_id = None
                                    try:
                                        if isinstance(source, dict):
                                            device_id = source.get("device_id")
                                    except Exception:
                                        device_id = None

                                    # Idempotencia: si hay op_id, registrar y evitar duplicados
                                    already_applied = False
                                    if op_id:
                                        try:
                                            cur.execute(
                                                "INSERT INTO sync_applied_ops(op_id, device_id) VALUES (%s, %s) ON CONFLICT DO NOTHING RETURNING op_id",
                                                (str(op_id), str(device_id) if device_id is not None else None),
                                            )
                                            inserted = cur.fetchone()
                                            if inserted is None:
                                                skipped += 1
                                                already_applied = True
                                        except Exception:
                                            # Si falla la tabla, seguimos sin idempotencia persistente
                                            pass
                                    if already_applied:
                                        continue

                                except Exception:
                                    # Error al preparar la operación; continuar con siguiente sin abortar el lote
                                    pass

                            # Aislar cada operación para evitar que un error invalide las siguientes
                            sp_idx += 1
                            sp_name = None
                            try:
                                sp_name = f"sp_{sp_idx}"
                                cur.execute(f"SAVEPOINT {sp_name}")
                            except Exception:
                                sp_name = None

                                if name in ("user.create", "user.add"):
                                    nombre = payload.get("name") or payload.get("nombre")
                                    telefono = (payload.get("phone") or payload.get("telefono") or "")
                                    tipo = (payload.get("membership_type") or payload.get("tipo_cuota") or "estandar")
                                    dni = payload.get("dni")
                                    activo = payload.get("active")
                                    rol = payload.get("role") or payload.get("rol") or 'socio'
                                # Valores por defecto sensatos
                                if not nombre:
                                    raise ValueError("user.add requiere 'name'")
                                if dni:
                                    # UPSERT por DNI para evitar duplicados y reconciliar IDs
                                    cur.execute(
                                        """
                                        INSERT INTO usuarios (dni, nombre, telefono, tipo_cuota, activo, rol, updated_at, updated_by_device)
                                        VALUES (%s, %s, %s, %s, COALESCE(%s, TRUE), %s, NOW(), %s)
                                        ON CONFLICT (dni) DO UPDATE SET
                                            nombre = COALESCE(EXCLUDED.nombre, usuarios.nombre),
                                            telefono = COALESCE(EXCLUDED.telefono, usuarios.telefono),
                                            tipo_cuota = COALESCE(EXCLUDED.tipo_cuota, usuarios.tipo_cuota),
                                            activo = COALESCE(EXCLUDED.activo, usuarios.activo),
                                            updated_at = NOW(),
                                            updated_by_device = EXCLUDED.updated_by_device
                                        RETURNING id
                                        """,
                                        (dni, nombre, telefono, tipo, True if activo is None else bool(activo), rol, device_id),
                                    )
                                    _ = cur.fetchone()
                                else:
                                    # Fallback sin DNI: inserción simple por id autoincremental
                                    cur.execute(
                                        """
                                        INSERT INTO usuarios (nombre, telefono, tipo_cuota, activo, rol, updated_at, updated_by_device)
                                        VALUES (%s, %s, %s, TRUE, %s, NOW(), %s)
                                        RETURNING id
                                        """,
                                        (nombre, telefono, tipo, rol, device_id),
                                    )
                                    _ = cur.fetchone()
                                applied += 1
                                # Liberar savepoint tras éxito de la operación
                                try:
                                    if sp_name:
                                        cur.execute(f"RELEASE SAVEPOINT {sp_name}")
                                except Exception:
                                    pass
                            elif name == "user.update":
                                uid = payload.get("user_id") or payload.get("id")
                                dni = payload.get("dni")
                                nombre = payload.get("name") or payload.get("nombre")
                                telefono = payload.get("phone") or payload.get("telefono")
                                tipo = payload.get("membership_type") or payload.get("tipo_cuota")
                                active = payload.get("active")
                                if dni:
                                    cur.execute(
                                        """
                                        UPDATE usuarios
                                        SET nombre = COALESCE(%s, nombre),
                                            telefono = COALESCE(%s, telefono),
                                            tipo_cuota = COALESCE(%s, tipo_cuota),
                                            activo = COALESCE(%s, activo),
                                            updated_at = NOW(),
                                            updated_by_device = %s
                                        WHERE dni = %s
                                        """,
                                        (nombre, telefono, tipo, None if active is None else bool(active), device_id, dni),
                                    )
                                elif uid:
                                    cur.execute(
                                        """
                                        UPDATE usuarios
                                        SET nombre = COALESCE(%s, nombre),
                                            telefono = COALESCE(%s, telefono),
                                            tipo_cuota = COALESCE(%s, tipo_cuota),
                                            updated_at = NOW(),
                                            updated_by_device = %s
                                        WHERE id = %s
                                        """,
                                        (nombre, telefono, tipo, device_id, uid),
                                    )
                                else:
                                    raise ValueError("user.update requiere 'dni' o 'id'")
                                applied += 1
                                try:
                                    if sp_name:
                                        cur.execute(f"RELEASE SAVEPOINT {sp_name}")
                                except Exception:
                                    pass
                            elif name == "user.delete":
                                uid = payload.get("user_id") or payload.get("id")
                                dni = payload.get("dni")
                                if dni:
                                    cur.execute(
                                        "UPDATE usuarios SET activo = FALSE, updated_at = NOW(), updated_by_device = %s WHERE dni = %s",
                                        (device_id, dni),
                                    )
                                elif uid:
                                    cur.execute(
                                        "UPDATE usuarios SET activo = FALSE, updated_at = NOW(), updated_by_device = %s WHERE id = %s",
                                        (device_id, uid),
                                    )
                                else:
                                    raise ValueError("user.delete requiere 'dni' o 'id'")
                                applied += 1
                                try:
                                    if sp_name:
                                        cur.execute(f"RELEASE SAVEPOINT {sp_name}")
                                except Exception:
                                    pass
                            # Rutinas
                            elif name in ("routine.add", "routine.create"):
                                row_id = payload.get("id")
                                usuario_id = payload.get("usuario_id")
                                nombre_rutina = payload.get("nombre_rutina") or payload.get("nombre")
                                descripcion = payload.get("descripcion")
                                dias_semana = payload.get("dias_semana")
                                categoria = payload.get("categoria")
                                activa = payload.get("activa")
                                if not nombre_rutina:
                                    raise ValueError("routine.add requiere 'nombre_rutina'")
                                if row_id:
                                    cur.execute(
                                        """
                                        INSERT INTO rutinas (id, usuario_id, nombre_rutina, descripcion, dias_semana, categoria, activa, updated_at, updated_by_device)
                                        VALUES (%s, %s, %s, %s, %s, %s, COALESCE(%s, TRUE), NOW(), %s)
                                        ON CONFLICT (id) DO UPDATE SET
                                            usuario_id = COALESCE(EXCLUDED.usuario_id, rutinas.usuario_id),
                                            nombre_rutina = COALESCE(EXCLUDED.nombre_rutina, rutinas.nombre_rutina),
                                            descripcion = COALESCE(EXCLUDED.descripcion, rutinas.descripcion),
                                            dias_semana = COALESCE(EXCLUDED.dias_semana, rutinas.dias_semana),
                                            categoria = COALESCE(EXCLUDED.categoria, rutinas.categoria),
                                            activa = COALESCE(EXCLUDED.activa, rutinas.activa),
                                            updated_at = NOW(),
                                            updated_by_device = EXCLUDED.updated_by_device
                                        """,
                                        (row_id, usuario_id, nombre_rutina, descripcion, dias_semana, categoria, activa, device_id),
                                    )
                                else:
                                    cur.execute(
                                        """
                                        INSERT INTO rutinas (usuario_id, nombre_rutina, descripcion, dias_semana, categoria, activa, updated_at, updated_by_device)
                                        VALUES (%s, %s, %s, %s, %s, COALESCE(%s, TRUE), NOW(), %s)
                                        RETURNING id
                                        """,
                                        (usuario_id, nombre_rutina, descripcion, dias_semana, categoria, activa, device_id),
                                    )
                                    _ = cur.fetchone()
                                applied += 1
                                try:
                                    if sp_name:
                                        cur.execute(f"RELEASE SAVEPOINT {sp_name}")
                                except Exception:
                                    pass
                            elif name == "routine.update":
                                row_id = payload.get("id") or payload.get("rutina_id")
                                if not row_id:
                                    raise ValueError("routine.update requiere 'id'")
                                usuario_id = payload.get("usuario_id")
                                nombre_rutina = payload.get("nombre_rutina") or payload.get("nombre")
                                descripcion = payload.get("descripcion")
                                dias_semana = payload.get("dias_semana")
                                categoria = payload.get("categoria")
                                activa = payload.get("activa")
                                cur.execute(
                                    """
                                    UPDATE rutinas
                                    SET usuario_id = COALESCE(%s, usuario_id),
                                        nombre_rutina = COALESCE(%s, nombre_rutina),
                                        descripcion = COALESCE(%s, descripcion),
                                        dias_semana = COALESCE(%s, dias_semana),
                                        categoria = COALESCE(%s, categoria),
                                        activa = COALESCE(%s, activa),
                                        updated_at = NOW(),
                                        updated_by_device = %s
                                    WHERE id = %s
                                    """,
                                    (usuario_id, nombre_rutina, descripcion, dias_semana, categoria, activa, device_id, row_id),
                                )
                                applied += 1
                                try:
                                    if sp_name:
                                        cur.execute(f"RELEASE SAVEPOINT {sp_name}")
                                except Exception:
                                    pass
                            elif name == "routine.delete":
                                row_id = payload.get("id") or payload.get("rutina_id")
                                if not row_id:
                                    raise ValueError("routine.delete requiere 'id'")
                                try:
                                    cur.execute("DELETE FROM rutinas WHERE id = %s", (row_id,))
                                except Exception:
                                    pass
                                try:
                                    cur.execute(
                                        "INSERT INTO sync_deletes(entity, key, updated_at, device_id) VALUES (%s, %s::jsonb, NOW(), %s)",
                                        ("routine", json.dumps({"id": row_id}), device_id),
                                    )
                                except Exception:
                                    pass
                                applied += 1
                                try:
                                    if sp_name:
                                        cur.execute(f"RELEASE SAVEPOINT {sp_name}")
                                except Exception:
                                    pass
                            # Rutinas - ejercicios
                            elif name in ("routine_exercise.add", "routine_exercise.create"):
                                row_id = payload.get("id")
                                rutina_id = payload.get("rutina_id")
                                ejercicio_id = payload.get("ejercicio_id")
                                dia_semana = payload.get("dia_semana")
                                series = payload.get("series")
                                repeticiones = payload.get("repeticiones")
                                orden = payload.get("orden")
                                if row_id:
                                    cur.execute(
                                        """
                                        INSERT INTO rutinas_ejercicios (id, rutina_id, ejercicio_id, dia_semana, series, repeticiones, orden, updated_at, updated_by_device)
                                        VALUES (%s, %s, %s, %s, %s, %s, %s, NOW(), %s)
                                        ON CONFLICT (id) DO UPDATE SET
                                            rutina_id = COALESCE(EXCLUDED.rutina_id, rutinas_ejercicios.rutina_id),
                                            ejercicio_id = COALESCE(EXCLUDED.ejercicio_id, rutinas_ejercicios.ejercicio_id),
                                            dia_semana = COALESCE(EXCLUDED.dia_semana, rutinas_ejercicios.dia_semana),
                                            series = COALESCE(EXCLUDED.series, rutinas_ejercicios.series),
                                            repeticiones = COALESCE(EXCLUDED.repeticiones, rutinas_ejercicios.repeticiones),
                                            orden = COALESCE(EXCLUDED.orden, rutinas_ejercicios.orden),
                                            updated_at = NOW(),
                                            updated_by_device = EXCLUDED.updated_by_device
                                        """,
                                        (row_id, rutina_id, ejercicio_id, dia_semana, series, repeticiones, orden, device_id),
                                    )
                                else:
                                    cur.execute(
                                        """
                                        INSERT INTO rutinas_ejercicios (rutina_id, ejercicio_id, dia_semana, series, repeticiones, orden, updated_at, updated_by_device)
                                        VALUES (%s, %s, %s, %s, %s, %s, NOW(), %s)
                                        RETURNING id
                                        """,
                                        (rutina_id, ejercicio_id, dia_semana, series, repeticiones, orden, device_id),
                                    )
                                    _ = cur.fetchone()
                                applied += 1
                            elif name == "routine_exercise.update":
                                row_id = payload.get("id")
                                if not row_id:
                                    raise ValueError("routine_exercise.update requiere 'id'")
                                rutina_id = payload.get("rutina_id")
                                ejercicio_id = payload.get("ejercicio_id")
                                dia_semana = payload.get("dia_semana")
                                series = payload.get("series")
                                repeticiones = payload.get("repeticiones")
                                orden = payload.get("orden")
                                cur.execute(
                                    """
                                    UPDATE rutinas_ejercicios
                                    SET rutina_id = COALESCE(%s, rutina_id),
                                        ejercicio_id = COALESCE(%s, ejercicio_id),
                                        dia_semana = COALESCE(%s, dia_semana),
                                        series = COALESCE(%s, series),
                                        repeticiones = COALESCE(%s, repeticiones),
                                        orden = COALESCE(%s, orden),
                                        updated_at = NOW(),
                                        updated_by_device = %s
                                    WHERE id = %s
                                    """,
                                    (rutina_id, ejercicio_id, dia_semana, series, repeticiones, orden, device_id, row_id),
                                )
                                applied += 1
                            elif name == "routine_exercise.delete":
                                row_id = payload.get("id")
                                if not row_id:
                                    raise ValueError("routine_exercise.delete requiere 'id'")
                                try:
                                    cur.execute("DELETE FROM rutinas_ejercicios WHERE id = %s", (row_id,))
                                except Exception:
                                    pass
                                try:
                                    cur.execute(
                                        "INSERT INTO sync_deletes(entity, key, updated_at, device_id) VALUES (%s, %s::jsonb, NOW(), %s)",
                                        ("routine_exercise", json.dumps({"id": row_id}), device_id),
                                    )
                                except Exception:
                                    pass
                                applied += 1
                            # Catálogo de ejercicios (opcional)
                            elif name in ("exercise.add", "exercise.create"):
                                row_id = payload.get("id")
                                nombre = payload.get("nombre") or payload.get("name")
                                grupo = payload.get("grupo_muscular") or payload.get("grupo")
                                descripcion = payload.get("descripcion")
                                if not nombre:
                                    raise ValueError("exercise.add requiere 'nombre'")
                                if row_id:
                                    cur.execute(
                                        """
                                        INSERT INTO ejercicios (id, nombre, grupo_muscular, descripcion, updated_at, updated_by_device)
                                        VALUES (%s, %s, %s, %s, NOW(), %s)
                                        ON CONFLICT (id) DO UPDATE SET
                                            nombre = COALESCE(EXCLUDED.nombre, ejercicios.nombre),
                                            grupo_muscular = COALESCE(EXCLUDED.grupo_muscular, ejercicios.grupo_muscular),
                                            descripcion = COALESCE(EXCLUDED.descripcion, ejercicios.descripcion),
                                            updated_at = NOW(),
                                            updated_by_device = EXCLUDED.updated_by_device
                                        """,
                                        (row_id, nombre, grupo, descripcion, device_id),
                                    )
                                else:
                                    cur.execute(
                                        """
                                        INSERT INTO ejercicios (nombre, grupo_muscular, descripcion, updated_at, updated_by_device)
                                        VALUES (%s, %s, %s, NOW(), %s)
                                        RETURNING id
                                        """,
                                        (nombre, grupo, descripcion, device_id),
                                    )
                                    _ = cur.fetchone()
                                applied += 1
                            elif name == "exercise.update":
                                row_id = payload.get("id")
                                if not row_id:
                                    raise ValueError("exercise.update requiere 'id'")
                                nombre = payload.get("nombre") or payload.get("name")
                                grupo = payload.get("grupo_muscular") or payload.get("grupo")
                                descripcion = payload.get("descripcion")
                                cur.execute(
                                    """
                                    UPDATE ejercicios
                                    SET nombre = COALESCE(%s, nombre),
                                        grupo_muscular = COALESCE(%s, grupo_muscular),
                                        descripcion = COALESCE(%s, descripcion),
                                        updated_at = NOW(),
                                        updated_by_device = %s
                                    WHERE id = %s
                                    """,
                                    (nombre, grupo, descripcion, device_id, row_id),
                                )
                                applied += 1
                            elif name == "exercise.delete":
                                row_id = payload.get("id")
                                if not row_id:
                                    raise ValueError("exercise.delete requiere 'id'")
                                try:
                                    cur.execute("DELETE FROM ejercicios WHERE id = %s", (row_id,))
                                except Exception:
                                    pass
                                try:
                                    cur.execute(
                                        "INSERT INTO sync_deletes(entity, key, updated_at, device_id) VALUES (%s, %s::jsonb, NOW(), %s)",
                                        ("exercise", json.dumps({"id": row_id}), device_id),
                                    )
                                except Exception:
                                    pass
                                applied += 1
                            # Etiquetas (tags)
                            elif name in ("tag.add", "tag.create"):
                                tag_id = payload.get("id")
                                nombre = payload.get("nombre") or payload.get("name")
                                color = payload.get("color") or payload.get("color_hex")
                                descripcion = payload.get("descripcion") or payload.get("description")
                                activo = payload.get("activo")
                                if not nombre and not tag_id:
                                    raise ValueError("tag.add requiere 'nombre' o 'id'")
                                if tag_id:
                                    cur.execute(
                                        """
                                        INSERT INTO etiquetas (id, nombre, color, descripcion, updated_at, updated_by_device)
                                        VALUES (%s, %s, %s, %s, NOW(), %s)
                                        ON CONFLICT (id) DO UPDATE SET
                                            nombre = COALESCE(EXCLUDED.nombre, etiquetas.nombre),
                                            color = COALESCE(EXCLUDED.color, etiquetas.color),
                                            descripcion = COALESCE(EXCLUDED.descripcion, etiquetas.descripcion),
                                            updated_at = NOW(),
                                            updated_by_device = EXCLUDED.updated_by_device
                                        """,
                                        (tag_id, nombre, color, descripcion, device_id),
                                    )
                                    if activo is not None:
                                        cur.execute("UPDATE etiquetas SET activo = %s, updated_at = NOW(), updated_by_device = %s WHERE id = %s", (bool(activo), device_id, tag_id))
                                else:
                                    cur.execute(
                                        """
                                        INSERT INTO etiquetas (nombre, color, descripcion, updated_at, updated_by_device)
                                        VALUES (%s, %s, %s, NOW(), %s)
                                        RETURNING id
                                        """,
                                        (nombre, color, descripcion, device_id),
                                    )
                                    _ = cur.fetchone()
                                applied += 1
                            elif name == "tag.update":
                                tag_id = payload.get("id")
                                nombre = payload.get("nombre") or payload.get("name")
                                color = payload.get("color") or payload.get("color_hex")
                                descripcion = payload.get("descripcion") or payload.get("description")
                                activo = payload.get("activo")
                                if not tag_id:
                                    raise ValueError("tag.update requiere 'id'")
                                cur.execute(
                                    """
                                    UPDATE etiquetas
                                    SET nombre = COALESCE(%s, nombre),
                                        color = COALESCE(%s, color),
                                        descripcion = COALESCE(%s, descripcion),
                                        activo = COALESCE(%s, activo),
                                        updated_at = NOW(),
                                        updated_by_device = %s
                                    WHERE id = %s
                                    """,
                                    (nombre, color, descripcion, None if activo is None else bool(activo), device_id, tag_id),
                                )
                                applied += 1
                            elif name == "tag.delete":
                                tag_id = payload.get("id")
                                nombre = payload.get("nombre") or payload.get("name")
                                if not tag_id and not nombre:
                                    raise ValueError("tag.delete requiere 'id' o 'nombre'")
                                try:
                                    if tag_id:
                                        cur.execute("DELETE FROM etiquetas WHERE id = %s", (tag_id,))
                                    else:
                                        cur.execute("DELETE FROM etiquetas WHERE nombre = %s", (nombre,))
                                except Exception:
                                    pass
                                try:
                                    key = {"id": tag_id} if tag_id else {"nombre": nombre}
                                    cur.execute(
                                        "INSERT INTO sync_deletes(entity, key, updated_at, device_id) VALUES (%s, %s::jsonb, NOW(), %s)",
                                        ("tag", json.dumps(key), device_id),
                                    )
                                except Exception:
                                    pass
                                applied += 1
                            # Relación usuario-etiqueta
                            elif name in ("user_tag.add", "user_tag.create"):
                                usuario_id = payload.get("usuario_id") or payload.get("user_id")
                                dni = payload.get("dni")
                                etiqueta_id = payload.get("etiqueta_id") or payload.get("tag_id")
                                if not usuario_id and dni:
                                    try:
                                        cur.execute("SELECT id FROM usuarios WHERE dni = %s", (dni,))
                                        r = cur.fetchone()
                                        if r:
                                            usuario_id = r.get("id") if isinstance(r, dict) else r[0]
                                    except Exception:
                                        pass
                                if not usuario_id or not etiqueta_id:
                                    raise ValueError("user_tag.add requiere 'usuario_id' o 'dni' y 'etiqueta_id'")
                                try:
                                    cur.execute(
                                        """
                                        INSERT INTO usuario_etiquetas (usuario_id, etiqueta_id, updated_at, updated_by_device)
                                        VALUES (%s, %s, NOW(), %s)
                                        ON CONFLICT (usuario_id, etiqueta_id) DO UPDATE SET
                                            updated_at = NOW(),
                                            updated_by_device = EXCLUDED.updated_by_device
                                        """,
                                        (usuario_id, etiqueta_id, device_id),
                                    )
                                except Exception:
                                    # Fallback si no hay restricción única
                                    cur.execute(
                                        "UPDATE usuario_etiquetas SET updated_at = NOW(), updated_by_device = %s WHERE usuario_id = %s AND etiqueta_id = %s",
                                        (device_id, usuario_id, etiqueta_id),
                                    )
                                applied += 1
                            elif name == "user_tag.delete":
                                usuario_id = payload.get("usuario_id") or payload.get("user_id")
                                dni = payload.get("dni")
                                etiqueta_id = payload.get("etiqueta_id") or payload.get("tag_id")
                                if not usuario_id and dni:
                                    try:
                                        cur.execute("SELECT id FROM usuarios WHERE dni = %s", (dni,))
                                        r = cur.fetchone()
                                        if r:
                                            usuario_id = r.get("id") if isinstance(r, dict) else r[0]
                                    except Exception:
                                        pass
                                if not usuario_id or not etiqueta_id:
                                    raise ValueError("user_tag.delete requiere 'usuario_id' o 'dni' y 'etiqueta_id'")
                                try:
                                    cur.execute("DELETE FROM usuario_etiquetas WHERE usuario_id = %s AND etiqueta_id = %s", (usuario_id, etiqueta_id))
                                except Exception:
                                    pass
                                try:
                                    cur.execute(
                                        "INSERT INTO sync_deletes(entity, key, updated_at, device_id) VALUES (%s, %s::jsonb, NOW(), %s)",
                                        ("user_tag", json.dumps({"usuario_id": usuario_id, "etiqueta_id": etiqueta_id}), device_id),
                                    )
                                except Exception:
                                    pass
                                applied += 1
                            # Notas de usuario
                            elif name in ("note.add", "note.create"):
                                note_id = payload.get("id")
                                usuario_id = payload.get("usuario_id") or payload.get("user_id")
                                dni = payload.get("dni")
                                categoria = (payload.get("categoria") or payload.get("category") or "general")
                                titulo = (payload.get("titulo") or payload.get("title") or "")
                                contenido = (payload.get("contenido") or payload.get("content") or "")
                                importancia = (payload.get("importancia") or payload.get("priority") or "normal")
                                activa = payload.get("activa")
                                if not usuario_id and dni:
                                    try:
                                        cur.execute("SELECT id FROM usuarios WHERE dni = %s", (dni,))
                                        r = cur.fetchone()
                                        if r:
                                            usuario_id = r.get("id") if isinstance(r, dict) else r[0]
                                    except Exception:
                                        pass
                                if not usuario_id:
                                    raise ValueError("note.add requiere 'usuario_id' o 'dni'")
                                if note_id:
                                    cur.execute(
                                        """
                                        INSERT INTO usuario_notas (id, usuario_id, categoria, titulo, contenido, importancia, updated_at, updated_by_device)
                                        VALUES (%s, %s, %s, %s, %s, %s, NOW(), %s)
                                        ON CONFLICT (id) DO UPDATE SET
                                            usuario_id = COALESCE(EXCLUDED.usuario_id, usuario_notas.usuario_id),
                                            categoria = COALESCE(EXCLUDED.categoria, usuario_notas.categoria),
                                            titulo = COALESCE(EXCLUDED.titulo, usuario_notas.titulo),
                                            contenido = COALESCE(EXCLUDED.contenido, usuario_notas.contenido),
                                            importancia = COALESCE(EXCLUDED.importancia, usuario_notas.importancia),
                                            updated_at = NOW(),
                                            updated_by_device = EXCLUDED.updated_by_device
                                        """,
                                        (note_id, usuario_id, categoria, titulo, contenido, importancia, device_id),
                                    )
                                    if activa is not None:
                                        cur.execute("UPDATE usuario_notas SET activa = %s, updated_at = NOW(), updated_by_device = %s WHERE id = %s", (bool(activa), device_id, note_id))
                                else:
                                    cur.execute(
                                        """
                                        INSERT INTO usuario_notas (usuario_id, categoria, titulo, contenido, importancia, updated_at, updated_by_device)
                                        VALUES (%s, %s, %s, %s, %s, NOW(), %s)
                                        RETURNING id
                                        """,
                                        (usuario_id, categoria, titulo, contenido, importancia, device_id),
                                    )
                                    _ = cur.fetchone()
                                applied += 1
                            elif name == "note.update":
                                note_id = payload.get("id")
                                if not note_id:
                                    raise ValueError("note.update requiere 'id'")
                                usuario_id = payload.get("usuario_id") or payload.get("user_id")
                                categoria = payload.get("categoria") or payload.get("category")
                                titulo = payload.get("titulo") or payload.get("title")
                                contenido = payload.get("contenido") or payload.get("content")
                                importancia = payload.get("importancia")
                                activa = payload.get("activa")
                                cur.execute(
                                    """
                                    UPDATE usuario_notas
                                    SET usuario_id = COALESCE(%s, usuario_id),
                                        categoria = COALESCE(%s, categoria),
                                        titulo = COALESCE(%s, titulo),
                                        contenido = COALESCE(%s, contenido),
                                        importancia = COALESCE(%s, importancia),
                                        activa = COALESCE(%s, activa),
                                        updated_at = NOW(),
                                        updated_by_device = %s
                                    WHERE id = %s
                                    """,
                                    (usuario_id, categoria, titulo, contenido, importancia, None if activa is None else bool(activa), device_id, note_id),
                                )
                                applied += 1
                            elif name == "note.delete":
                                note_id = payload.get("id")
                                if not note_id:
                                    raise ValueError("note.delete requiere 'id'")
                                try:
                                    cur.execute("DELETE FROM usuario_notas WHERE id = %s", (note_id,))
                                except Exception:
                                    pass
                                try:
                                    cur.execute(
                                        "INSERT INTO sync_deletes(entity, key, updated_at, device_id) VALUES (%s, %s::jsonb, NOW(), %s)",
                                        ("note", json.dumps({"id": note_id}), device_id),
                                    )
                                except Exception:
                                    pass
                                applied += 1
                            elif name in ("professor_schedule.add", "professor_schedule.update"):
                                # Campos: id(opcional para add), profesor_id, dia_semana, hora_inicio, hora_fin, disponible
                                row_id = payload.get("id")
                                profesor_id = payload.get("profesor_id")
                                dia_semana = payload.get("dia_semana")
                                hora_inicio = payload.get("hora_inicio")
                                hora_fin = payload.get("hora_fin")
                                disponible = payload.get("disponible")
                                if name == "professor_schedule.add":
                                    if row_id:
                                        # UPSERT por id explícito si llega
                                        cur.execute(
                                            """
                                            INSERT INTO horarios_profesores (id, profesor_id, dia_semana, hora_inicio, hora_fin, disponible, updated_at, updated_by_device)
                                            VALUES (%s, %s, %s, %s, %s, COALESCE(%s, TRUE), NOW(), %s)
                                            ON CONFLICT (id) DO UPDATE SET
                                                profesor_id = COALESCE(EXCLUDED.profesor_id, horarios_profesores.profesor_id),
                                                dia_semana = COALESCE(EXCLUDED.dia_semana, horarios_profesores.dia_semana),
                                                hora_inicio = COALESCE(EXCLUDED.hora_inicio, horarios_profesores.hora_inicio),
                                                hora_fin = COALESCE(EXCLUDED.hora_fin, horarios_profesores.hora_fin),
                                                disponible = COALESCE(EXCLUDED.disponible, horarios_profesores.disponible),
                                                updated_at = NOW(),
                                                updated_by_device = EXCLUDED.updated_by_device
                                            """,
                                            (row_id, profesor_id, dia_semana, hora_inicio, hora_fin, disponible, device_id),
                                    )
                                    
                                    else:
                                        cur.execute(
                                            """
                                            INSERT INTO horarios_profesores (profesor_id, dia_semana, hora_inicio, hora_fin, disponible, updated_at, updated_by_device)
                                            VALUES (%s, %s, %s, %s, COALESCE(%s, TRUE), NOW(), %s)
                                            RETURNING id
                                            """,
                                            (profesor_id, dia_semana, hora_inicio, hora_fin, disponible, device_id),
                                        )
                                        _ = cur.fetchone()
                                else:  # update
                                    if not row_id:
                                        raise ValueError("professor_schedule.update requiere 'id'")
                                    cur.execute(
                                        """
                                        UPDATE horarios_profesores
                                        SET profesor_id = COALESCE(%s, profesor_id),
                                            dia_semana = COALESCE(%s, dia_semana),
                                            hora_inicio = COALESCE(%s, hora_inicio),
                                            hora_fin = COALESCE(%s, hora_fin),
                                            disponible = COALESCE(%s, disponible),
                                            updated_at = NOW(),
                                            updated_by_device = %s
                                        WHERE id = %s
                                        """,
                                        (profesor_id, dia_semana, hora_inicio, hora_fin, disponible, device_id, row_id),
                                    )
                                applied += 1
                            elif name == "professor_schedule.delete":
                                row_id = payload.get("id")
                                if not row_id:
                                    raise ValueError("professor_schedule.delete requiere 'id'")
                                try:
                                    cur.execute("DELETE FROM horarios_profesores WHERE id = %s", (row_id,))
                                except Exception:
                                    pass
                                try:
                                    cur.execute(
                                        "INSERT INTO sync_deletes(entity, key, updated_at, device_id) VALUES (%s, %s::jsonb, NOW(), %s)",
                                        ("professor_schedule", json.dumps({"id": row_id}), device_id),
                                    )
                                except Exception:
                                    pass
                                applied += 1
                            elif name in ("professor_substitution.add", "professor_substitution.update"):
                                # Campos: id(opcional add), asignacion_id, profesor_suplente_id, fecha_clase, motivo, estado, notas
                                row_id = payload.get("id")
                                asignacion_id = payload.get("asignacion_id")
                                profesor_suplente_id = payload.get("profesor_suplente_id")
                                fecha_clase = payload.get("fecha_clase")
                                motivo = payload.get("motivo")
                                estado = payload.get("estado")
                                notas = payload.get("notas")
                                if name == "professor_substitution.add":
                                    if row_id:
                                        cur.execute(
                                            """
                                            INSERT INTO profesor_suplencias (id, asignacion_id, profesor_suplente_id, fecha_clase, motivo, estado, notas, updated_at, updated_by_device)
                                            VALUES (%s, %s, %s, %s, %s, %s, %s, NOW(), %s)
                                            ON CONFLICT (id) DO UPDATE SET
                                                asignacion_id = COALESCE(EXCLUDED.asignacion_id, profesor_suplencias.asignacion_id),
                                                profesor_suplente_id = COALESCE(EXCLUDED.profesor_suplente_id, profesor_suplencias.profesor_suplente_id),
                                                fecha_clase = COALESCE(EXCLUDED.fecha_clase, profesor_suplencias.fecha_clase),
                                                motivo = COALESCE(EXCLUDED.motivo, profesor_suplencias.motivo),
                                                estado = COALESCE(EXCLUDED.estado, profesor_suplencias.estado),
                                                notas = COALESCE(EXCLUDED.notas, profesor_suplencias.notas),
                                                updated_at = NOW(),
                                                updated_by_device = EXCLUDED.updated_by_device
                                            """,
                                            (row_id, asignacion_id, profesor_suplente_id, fecha_clase, motivo, estado, notas, device_id),
                                        )
                                    else:
                                        cur.execute(
                                            """
                                            INSERT INTO profesor_suplencias (asignacion_id, profesor_suplente_id, fecha_clase, motivo, estado, notas, updated_at, updated_by_device)
                                            VALUES (%s, %s, %s, %s, %s, %s, NOW(), %s)
                                            RETURNING id
                                            """,
                                            (asignacion_id, profesor_suplente_id, fecha_clase, motivo, estado, notas, device_id),
                                        )
                                        _ = cur.fetchone()
                                else:  # update
                                    if not row_id:
                                        raise ValueError("professor_substitution.update requiere 'id'")
                                    cur.execute(
                                        """
                                        UPDATE profesor_suplencias
                                        SET asignacion_id = COALESCE(%s, asignacion_id),
                                            profesor_suplente_id = COALESCE(%s, profesor_suplente_id),
                                            fecha_clase = COALESCE(%s, fecha_clase),
                                            motivo = COALESCE(%s, motivo),
                                            estado = COALESCE(%s, estado),
                                            notas = COALESCE(%s, notas),
                                            updated_at = NOW(),
                                            updated_by_device = %s
                                        WHERE id = %s
                                        """,
                                        (asignacion_id, profesor_suplente_id, fecha_clase, motivo, estado, notas, device_id, row_id),
                                    )
                                applied += 1
                            elif name == "professor_substitution.delete":
                                row_id = payload.get("id")
                                if not row_id:
                                    raise ValueError("professor_substitution.delete requiere 'id'")
                                try:
                                    cur.execute("DELETE FROM profesor_suplencias WHERE id = %s", (row_id,))
                                except Exception:
                                    pass
                                try:
                                    cur.execute(
                                        "INSERT INTO sync_deletes(entity, key, updated_at, device_id) VALUES (%s, %s::jsonb, NOW(), %s)",
                                        ("professor_substitution", json.dumps({"id": row_id}), device_id),
                                    )
                                except Exception:
                                    pass
                                applied += 1
                            elif name in ("payment.add", "payment.update"):
                                # Campos esperados: dni (o user_id), mes, año, monto, fecha_pago, metodo_pago_id
                                max_attempts = 3
                                for attempt in range(1, max_attempts + 1):
                                    try:
                                        dni = payload.get("dni")
                                        uid = payload.get("user_id") or payload.get("usuario_id")
                                        mes = payload.get("mes")
                                        anio = payload.get("año") or payload.get("anio") or payload.get("year")
                                        monto = payload.get("monto")
                                        fecha_pago = payload.get("fecha_pago")
                                        metodo_pago_id = payload.get("metodo_pago_id")
                                        # Resolver usuario_id por DNI si no viene id
                                        if not uid and dni:
                                            cur.execute("SELECT id FROM usuarios WHERE dni = %s", (dni,))
                                            row_u = cur.fetchone()
                                            uid = row_u.get("id") if row_u else None
                                        if not uid:
                                            raise ValueError("payment.add/update requiere 'dni' o 'user_id'")
                                        # Derivar mes/año desde fecha_pago si no llegan explícitos
                                        if (mes is None or anio is None) and fecha_pago:
                                            try:
                                                cur.execute("SELECT EXTRACT(MONTH FROM CAST(%s AS timestamp))::int AS m, EXTRACT(YEAR FROM CAST(%s AS timestamp))::int AS y", (fecha_pago, fecha_pago))
                                                rmy = cur.fetchone() or {}
                                                mes = mes if mes is not None else rmy.get("m")
                                                anio = anio if anio is not None else rmy.get("y")
                                            except Exception:
                                                pass
                                        if mes is None or anio is None:
                                            raise ValueError("payment.add/update requiere 'mes' y 'año' (o 'fecha_pago')")
                                        # Upsert manual por (usuario_id, mes, año)
                                        cur.execute(
                                            "SELECT id FROM pagos WHERE usuario_id = %s AND mes = %s AND año = %s",
                                            (uid, int(mes), int(anio)),
                                        )
                                        row = cur.fetchone()
                                        if row:
                                            pid = row.get("id")
                                            cur.execute(
                                                """
                                                UPDATE pagos
                                                SET monto = COALESCE(%s, monto),
                                                    fecha_pago = COALESCE(%s, fecha_pago),
                                                    metodo_pago_id = COALESCE(%s, metodo_pago_id),
                                                    updated_at = NOW(),
                                                    updated_by_device = %s
                                                WHERE id = %s
                                                """,
                                                (monto, fecha_pago, metodo_pago_id, device_id, pid),
                                            )
                                        else:
                                            cur.execute(
                                                """
                                                INSERT INTO pagos (usuario_id, monto, fecha_pago, mes, año, metodo_pago_id, updated_at, updated_by_device)
                                                VALUES (%s, %s, COALESCE(CAST(%s AS timestamp), NOW()), %s, %s, %s, NOW(), %s)
                                                RETURNING id
                                                """,
                                                (uid, monto, fecha_pago, int(mes), int(anio), metodo_pago_id, device_id),
                                            )
                                            _ = cur.fetchone()
                                        applied += 1
                                        break
                                    except Exception as _e:
                                        code = getattr(_e, "pgcode", "") or ""
                                        if sp_name and code in ("40001", "40P01", "55P03") and attempt < max_attempts:
                                            try:
                                                logging.warning(f"sync upload retry rid={rid} op={name} attempt={attempt} code={code}")
                                            except Exception:
                                                pass
                                            try:
                                                cur.execute(f"ROLLBACK TO SAVEPOINT {sp_name}")
                                            except Exception:
                                                pass
                                            try:
                                                import time as _time
                                                _time.sleep(min(0.15 * (2 ** (attempt - 1)), 0.8))
                                            except Exception:
                                                pass
                                            continue
                                        raise
                            elif name == "payment.delete":
                                max_attempts = 3
                                for attempt in range(1, max_attempts + 1):
                                    try:
                                        dni = payload.get("dni")
                                        uid = payload.get("user_id") or payload.get("usuario_id")
                                        mes = payload.get("mes")
                                        anio = payload.get("año") or payload.get("anio") or payload.get("year")
                                        if not uid and dni:
                                            cur.execute("SELECT id FROM usuarios WHERE dni = %s", (dni,))
                                            row_u = cur.fetchone()
                                            uid = row_u.get("id") if row_u else None
                                        if not uid or mes is None or anio is None:
                                            raise ValueError("payment.delete requiere 'dni' o 'user_id' y 'mes' y 'año'")
                                        cur.execute("DELETE FROM pagos WHERE usuario_id = %s AND mes = %s AND año = %s", (uid, int(mes), int(anio)))
                                        # Registrar delete para downstream
                                        try:
                                            cur.execute(
                                                "INSERT INTO sync_deletes(entity, key, updated_at, device_id) VALUES (%s, %s::jsonb, NOW(), %s)",
                                                ("payment", json.dumps({"dni": dni, "user_id": uid, "mes": int(mes), "año": int(anio)}), device_id),
                                            )
                                        except Exception:
                                            pass
                                        applied += 1
                                        break
                                    except Exception as _e:
                                        code = getattr(_e, "pgcode", "") or ""
                                        if sp_name and code in ("40001", "40P01", "55P03") and attempt < max_attempts:
                                            try:
                                                logging.warning(f"sync upload retry rid={rid} op={name} attempt={attempt} code={code}")
                                            except Exception:
                                                pass
                                            try:
                                                cur.execute(f"ROLLBACK TO SAVEPOINT {sp_name}")
                                            except Exception:
                                                pass
                                            try:
                                                import time as _time
                                                _time.sleep(min(0.15 * (2 ** (attempt - 1)), 0.8))
                                            except Exception:
                                                pass
                                            continue
                                        raise
                            elif name in ("attendance.add", "attendance.update"):
                                max_attempts = 3
                                for attempt in range(1, max_attempts + 1):
                                    try:
                                        dni = payload.get("dni")
                                        uid = payload.get("user_id") or payload.get("usuario_id")
                                        fecha = payload.get("fecha")  # ISO date
                                        hora = payload.get("hora") or payload.get("hora_registro")
                                        if not uid and dni:
                                            cur.execute("SELECT id FROM usuarios WHERE dni = %s", (dni,))
                                            row_u = cur.fetchone()
                                            uid = row_u.get("id") if row_u else None
                                        if not uid or not fecha:
                                            raise ValueError("attendance.add/update requiere 'dni' o 'user_id' y 'fecha'")
                                        # Upsert manual por (usuario_id, fecha)
                                        cur.execute("SELECT id FROM asistencias WHERE usuario_id = %s AND fecha::date = CAST(%s AS date)", (uid, fecha))
                                        row = cur.fetchone()
                                        if row:
                                            aid = row.get("id")
                                            cur.execute(
                                                "UPDATE asistencias SET hora_registro = COALESCE(%s, hora_registro), updated_at = NOW(), updated_by_device = %s WHERE id = %s",
                                                (hora, device_id, aid),
                                            )
                                        else:
                                            cur.execute(
                                                "INSERT INTO asistencias (usuario_id, fecha, hora_registro, updated_at, updated_by_device) VALUES (%s, CAST(%s AS timestamp), COALESCE(%s, CAST(NOW() AS time)), NOW(), %s) RETURNING id",
                                                (uid, fecha, hora, device_id),
                                            )
                                            _ = cur.fetchone()
                                        applied += 1
                                        break
                                    except Exception as _e:
                                        code = getattr(_e, "pgcode", "") or ""
                                        if sp_name and code in ("40001", "40P01", "55P03") and attempt < max_attempts:
                                            try:
                                                logging.warning(f"sync upload retry rid={rid} op={name} attempt={attempt} code={code}")
                                            except Exception:
                                                pass
                                            try:
                                                cur.execute(f"ROLLBACK TO SAVEPOINT {sp_name}")
                                            except Exception:
                                                pass
                                            try:
                                                import time as _time
                                                _time.sleep(min(0.15 * (2 ** (attempt - 1)), 0.8))
                                            except Exception:
                                                pass
                                            continue
                                        raise
                            elif name == "attendance.delete":
                                max_attempts = 3
                                for attempt in range(1, max_attempts + 1):
                                    try:
                                        dni = payload.get("dni")
                                        uid = payload.get("user_id") or payload.get("usuario_id")
                                        fecha = payload.get("fecha")
                                        if not uid and dni:
                                            cur.execute("SELECT id FROM usuarios WHERE dni = %s", (dni,))
                                            row_u = cur.fetchone()
                                            uid = row_u.get("id") if row_u else None
                                        if not uid or not fecha:
                                            raise ValueError("attendance.delete requiere 'dni' o 'user_id' y 'fecha'")
                                        cur.execute("DELETE FROM asistencias WHERE usuario_id = %s AND fecha::date = CAST(%s AS date)", (uid, fecha))
                                        try:
                                            cur.execute(
                                                "INSERT INTO sync_deletes(entity, key, updated_at, device_id) VALUES (%s, %s::jsonb, NOW(), %s)",
                                                ("attendance", json.dumps({"dni": dni, "user_id": uid, "fecha": fecha}), device_id),
                                            )
                                        except Exception:
                                            pass
                                        applied += 1
                                        break
                                    except Exception as _e:
                                        code = getattr(_e, "pgcode", "") or ""
                                        if sp_name and code in ("40001", "40P01", "55P03") and attempt < max_attempts:
                                            try:
                                                logging.warning(f"sync upload retry rid={rid} op={name} attempt={attempt} code={code}")
                                            except Exception:
                                                pass
                                            try:
                                                cur.execute(f"ROLLBACK TO SAVEPOINT {sp_name}")
                                            except Exception:
                                                pass
                                            try:
                                                import time as _time
                                                _time.sleep(min(0.15 * (2 ** (attempt - 1)), 0.8))
                                            except Exception:
                                                pass
                                            continue
                                        raise
                            elif name in ("class.add", "class.update"):
                                # Campos: id(opcional), nombre, descripcion, activa, tipo_clase_id
                                row_id = payload.get("id")
                                nombre = payload.get("nombre")
                                descripcion = payload.get("descripcion")
                                activa = payload.get("activa")
                                tipo_clase_id = payload.get("tipo_clase_id")
                                if name == "class.add":
                                    if row_id:
                                        cur.execute("SELECT id FROM clases WHERE id = %s", (row_id,))
                                        r = cur.fetchone()
                                        if r:
                                            cur.execute(
                                                """
                                                UPDATE clases
                                                SET nombre = COALESCE(%s, nombre),
                                                    descripcion = COALESCE(%s, descripcion),
                                                    activa = COALESCE(%s, activa),
                                                    tipo_clase_id = COALESCE(%s, tipo_clase_id),
                                                    updated_at = NOW(),
                                                    updated_by_device = %s
                                                WHERE id = %s
                                                """,
                                                (nombre, descripcion, activa, tipo_clase_id, device_id, row_id),
                                            )
                                        else:
                                            cur.execute(
                                                """
                                                INSERT INTO clases (id, nombre, descripcion, activa, tipo_clase_id, updated_at, updated_by_device)
                                                VALUES (%s, COALESCE(%s, 'Clase'), %s, COALESCE(%s, TRUE), %s, NOW(), %s)
                                                RETURNING id
                                                """,
                                                (row_id, nombre, descripcion, activa, tipo_clase_id, device_id),
                                            )
                                            _ = cur.fetchone()
                                    else:
                                        cur.execute(
                                            """
                                            INSERT INTO clases (nombre, descripcion, activa, tipo_clase_id, updated_at, updated_by_device)
                                            VALUES (COALESCE(%s, 'Clase'), %s, COALESCE(%s, TRUE), %s, NOW(), %s)
                                            RETURNING id
                                            """,
                                            (nombre, descripcion, activa, tipo_clase_id, device_id),
                                        )
                                        _ = cur.fetchone()
                                else:
                                    updated = 0
                                    if row_id:
                                        cur.execute(
                                            """
                                            UPDATE clases
                                            SET nombre = COALESCE(%s, nombre),
                                                descripcion = COALESCE(%s, descripcion),
                                                activa = COALESCE(%s, activa),
                                                tipo_clase_id = COALESCE(%s, tipo_clase_id),
                                                updated_at = NOW(),
                                                updated_by_device = %s
                                            WHERE id = %s
                                            """,
                                            (nombre, descripcion, activa, tipo_clase_id, device_id, row_id),
                                        )
                                        updated = getattr(cur, 'rowcount', 0) or 0
                                    if updated == 0 and nombre:
                                        cur.execute(
                                            """
                                            UPDATE clases
                                            SET descripcion = COALESCE(%s, descripcion),
                                                activa = COALESCE(%s, activa),
                                                tipo_clase_id = COALESCE(%s, tipo_clase_id),
                                                updated_at = NOW(),
                                                updated_by_device = %s
                                            WHERE nombre = %s
                                            """,
                                            (descripcion, activa, tipo_clase_id, device_id, nombre),
                                        )
                                        updated = getattr(cur, 'rowcount', 0) or 0
                                    if updated == 0:
                                        cur.execute(
                                            """
                                            INSERT INTO clases (nombre, descripcion, activa, tipo_clase_id, updated_at, updated_by_device)
                                            VALUES (COALESCE(%s, 'Clase'), %s, COALESCE(%s, TRUE), %s, NOW(), %s)
                                            ON CONFLICT (nombre) DO UPDATE SET
                                                descripcion = COALESCE(EXCLUDED.descripcion, clases.descripcion),
                                                activa = COALESCE(EXCLUDED.activa, clases.activa),
                                                tipo_clase_id = COALESCE(EXCLUDED.tipo_clase_id, clases.tipo_clase_id),
                                                updated_at = NOW(),
                                                updated_by_device = EXCLUDED.updated_by_device
                                            """,
                                            (nombre, descripcion, activa, tipo_clase_id, device_id),
                                        )
                                applied += 1
                            elif name == "class.delete":
                                row_id = payload.get("id")
                                nombre = payload.get("nombre")
                                deleted = 0
                                if row_id:
                                    cur.execute("DELETE FROM clases WHERE id = %s", (row_id,))
                                    deleted = getattr(cur, 'rowcount', 0) or 0
                                if deleted == 0 and nombre:
                                    cur.execute("DELETE FROM clases WHERE nombre = %s", (nombre,))
                                    deleted = getattr(cur, 'rowcount', 0) or 0
                                try:
                                    cur.execute(
                                        "INSERT INTO sync_deletes(entity, key, updated_at, device_id) VALUES (%s, %s::jsonb, NOW(), %s)",
                                        ("class", json.dumps({"id": row_id, "nombre": nombre}), device_id),
                                    )
                                except Exception:
                                    pass
                                applied += 1
                            elif name in ("class_schedule.add", "class_schedule.update"):
                                # Campos: id(opc.), clase_id, dia_semana, hora_inicio, hora_fin, cupo_maximo, activo
                                sid = payload.get("id")
                                clase_id = payload.get("clase_id")
                                dia_semana = payload.get("dia_semana")
                                hora_inicio = payload.get("hora_inicio")
                                hora_fin = payload.get("hora_fin")
                                cupo_maximo = payload.get("cupo_maximo")
                                activo = payload.get("activo")
                                if name == "class_schedule.add":
                                    if sid:
                                        cur.execute("SELECT id FROM clases_horarios WHERE id = %s", (sid,))
                                        r = cur.fetchone()
                                        if r:
                                            cur.execute(
                                                """
                                                UPDATE clases_horarios
                                                SET clase_id = COALESCE(%s, clase_id),
                                                    dia_semana = COALESCE(%s, dia_semana),
                                                    hora_inicio = COALESCE(CAST(%s AS time), hora_inicio),
                                                    hora_fin = COALESCE(CAST(%s AS time), hora_fin),
                                                    cupo_maximo = COALESCE(%s, cupo_maximo),
                                                    activo = COALESCE(%s, activo),
                                                    updated_at = NOW(),
                                                    updated_by_device = %s
                                                WHERE id = %s
                                                """,
                                                (clase_id, dia_semana, hora_inicio, hora_fin, cupo_maximo, activo, device_id, sid),
                                            )
                                        else:
                                            cur.execute(
                                                """
                                                INSERT INTO clases_horarios (id, clase_id, dia_semana, hora_inicio, hora_fin, cupo_maximo, activo, updated_at, updated_by_device)
                                                VALUES (%s, %s, %s, CAST(%s AS time), CAST(%s AS time), COALESCE(%s, 20), COALESCE(%s, TRUE), NOW(), %s)
                                                RETURNING id
                                                """,
                                                (sid, clase_id, dia_semana, hora_inicio, hora_fin, cupo_maximo, activo, device_id),
                                            )
                                            _ = cur.fetchone()
                                    else:
                                        # Intentar localizar por clave natural
                                        nid = None
                                        if clase_id and dia_semana is not None and hora_inicio and hora_fin:
                                            cur.execute(
                                                """
                                                SELECT id FROM clases_horarios
                                                WHERE clase_id = %s AND dia_semana = %s AND hora_inicio = CAST(%s AS time) AND hora_fin = CAST(%s AS time)
                                                """,
                                                (int(clase_id), dia_semana, hora_inicio, hora_fin),
                                            )
                                            r = cur.fetchone()
                                            nid = r.get("id") if r else None
                                        if nid:
                                            cur.execute(
                                                """
                                                UPDATE clases_horarios
                                                SET cupo_maximo = COALESCE(%s, cupo_maximo),
                                                    activo = COALESCE(%s, activo),
                                                    updated_at = NOW(),
                                                    updated_by_device = %s
                                                WHERE id = %s
                                                """,
                                                (cupo_maximo, activo, device_id, nid),
                                            )
                                        else:
                                            cur.execute(
                                                """
                                                INSERT INTO clases_horarios (clase_id, dia_semana, hora_inicio, hora_fin, cupo_maximo, activo, updated_at, updated_by_device)
                                                VALUES (%s, %s, %s, CAST(%s AS time), CAST(%s AS time), COALESCE(%s, 20), COALESCE(%s, TRUE), NOW(), %s)
                                                RETURNING id
                                                """,
                                                (clase_id, dia_semana, hora_inicio, hora_fin, cupo_maximo, activo, device_id),
                                            )
                                            _ = cur.fetchone()
                                else:
                                    updated = 0
                                    if sid:
                                        cur.execute(
                                            """
                                            UPDATE clases_horarios
                                            SET clase_id = COALESCE(%s, clase_id),
                                                dia_semana = COALESCE(%s, dia_semana),
                                                hora_inicio = COALESCE(CAST(%s AS time), hora_inicio),
                                                hora_fin = COALESCE(CAST(%s AS time), hora_fin),
                                                cupo_maximo = COALESCE(%s, cupo_maximo),
                                                activo = COALESCE(%s, activo),
                                                updated_at = NOW(),
                                                updated_by_device = %s
                                            WHERE id = %s
                                            """,
                                            (clase_id, dia_semana, hora_inicio, hora_fin, cupo_maximo, activo, device_id, sid),
                                        )
                                        updated = getattr(cur, 'rowcount', 0) or 0
                                    if updated == 0 and clase_id and dia_semana is not None and hora_inicio and hora_fin:
                                        cur.execute(
                                            """
                                            UPDATE clases_horarios
                                            SET cupo_maximo = COALESCE(%s, cupo_maximo),
                                                activo = COALESCE(%s, activo),
                                                updated_at = NOW(),
                                                updated_by_device = %s
                                            WHERE clase_id = %s AND dia_semana = %s AND hora_inicio = CAST(%s AS time) AND hora_fin = CAST(%s AS time)
                                            """,
                                            (cupo_maximo, activo, device_id, int(clase_id), dia_semana, hora_inicio, hora_fin),
                                        )
                                        updated = getattr(cur, 'rowcount', 0) or 0
                                    if updated == 0:
                                        cur.execute(
                                            """
                                            INSERT INTO clases_horarios (clase_id, dia_semana, hora_inicio, hora_fin, cupo_maximo, activo, updated_at, updated_by_device)
                                            VALUES (%s, %s, %s, CAST(%s AS time), CAST(%s AS time), COALESCE(%s, 20), COALESCE(%s, TRUE), NOW(), %s)
                                            RETURNING id
                                            """,
                                            (clase_id, dia_semana, hora_inicio, hora_fin, cupo_maximo, activo, device_id),
                                        )
                                        _ = cur.fetchone()
                                applied += 1
                            elif name == "class_schedule.delete":
                                sid = payload.get("id")
                                clase_id = payload.get("clase_id")
                                dia_semana = payload.get("dia_semana")
                                hora_inicio = payload.get("hora_inicio")
                                hora_fin = payload.get("hora_fin")
                                deleted = 0
                                if sid:
                                    cur.execute("DELETE FROM clases_horarios WHERE id = %s", (sid,))
                                    deleted = getattr(cur, 'rowcount', 0) or 0
                                if deleted == 0 and clase_id and dia_semana is not None and hora_inicio and hora_fin:
                                    cur.execute(
                                        """
                                        DELETE FROM clases_horarios
                                        WHERE clase_id = %s AND dia_semana = %s AND hora_inicio = CAST(%s AS time) AND hora_fin = CAST(%s AS time)
                                        """,
                                        (int(clase_id), dia_semana, hora_inicio, hora_fin),
                                    )
                                    deleted = getattr(cur, 'rowcount', 0) or 0
                                try:
                                    cur.execute(
                                        "INSERT INTO sync_deletes(entity, key, updated_at, device_id) VALUES (%s, %s::jsonb, NOW(), %s)",
                                        ("class_schedule", json.dumps({"id": sid, "clase_id": clase_id, "dia_semana": dia_semana, "hora_inicio": hora_inicio, "hora_fin": hora_fin}), device_id),
                                    )
                                except Exception:
                                    pass
                                applied += 1
                            elif name in ("class_membership.add", "class_membership.update"):
                                # Campos: dni|user_id, clase_horario_id, fecha_inscripcion(opc)
                                max_attempts = 3
                                for attempt in range(1, max_attempts + 1):
                                    try:
                                        dni = payload.get("dni")
                                        uid = payload.get("user_id") or payload.get("usuario_id")
                                        clase_horario_id = payload.get("clase_horario_id") or payload.get("horario_id")
                                        fecha_inscripcion = payload.get("fecha_inscripcion")
                                        if not uid and dni:
                                            cur.execute("SELECT id FROM usuarios WHERE dni = %s", (dni,))
                                            row_u = cur.fetchone()
                                            uid = row_u.get("id") if row_u else None
                                        if not uid or not clase_horario_id:
                                            raise ValueError("class_membership.add/update requiere 'dni' o 'user_id' y 'clase_horario_id'")
                                        # Upsert por (clase_horario_id, usuario_id)
                                        cur.execute(
                                            "SELECT id FROM clase_usuarios WHERE clase_horario_id = %s AND usuario_id = %s",
                                            (int(clase_horario_id), int(uid)),
                                        )
                                        r = cur.fetchone()
                                        if r:
                                            cid = r.get("id")
                                            cur.execute(
                                                """
                                                UPDATE clase_usuarios
                                                SET fecha_inscripcion = COALESCE(CAST(%s AS timestamp), fecha_inscripcion),
                                                    updated_at = NOW(),
                                                    updated_by_device = %s
                                                WHERE id = %s
                                                """,
                                                (fecha_inscripcion, device_id, cid),
                                            )
                                        else:
                                            cur.execute(
                                                """
                                                INSERT INTO clase_usuarios (clase_horario_id, usuario_id, fecha_inscripcion, updated_at, updated_by_device)
                                                VALUES (%s, %s, CAST(%s AS timestamp), NOW(), %s)
                                                RETURNING id
                                                """,
                                                (int(clase_horario_id), int(uid), fecha_inscripcion, device_id),
                                            )
                                            _ = cur.fetchone()
                                        applied += 1
                                        break
                                    except Exception as _e:
                                        code = getattr(_e, "pgcode", "") or ""
                                        if sp_name and code in ("40001", "40P01", "55P03") and attempt < max_attempts:
                                            try:
                                                logging.warning(f"sync upload retry rid={rid} op={name} attempt={attempt} code={code}")
                                            except Exception:
                                                pass
                                            try:
                                                cur.execute(f"ROLLBACK TO SAVEPOINT {sp_name}")
                                            except Exception:
                                                pass
                                            try:
                                                import time as _time
                                                _time.sleep(min(0.15 * (2 ** (attempt - 1)), 0.8))
                                            except Exception:
                                                pass
                                            continue
                                        raise
                            elif name == "class_membership.delete":
                                max_attempts = 3
                                for attempt in range(1, max_attempts + 1):
                                    try:
                                        dni = payload.get("dni")
                                        uid = payload.get("user_id") or payload.get("usuario_id")
                                        clase_horario_id = payload.get("clase_horario_id") or payload.get("horario_id")
                                        if not uid and dni:
                                            cur.execute("SELECT id FROM usuarios WHERE dni = %s", (dni,))
                                            row_u = cur.fetchone()
                                            uid = row_u.get("id") if row_u else None
                                        if not uid or not clase_horario_id:
                                            raise ValueError("class_membership.delete requiere 'dni' o 'user_id' y 'clase_horario_id'")
                                        cur.execute(
                                            "DELETE FROM clase_usuarios WHERE clase_horario_id = %s AND usuario_id = %s",
                                            (int(clase_horario_id), int(uid)),
                                        )
                                        try:
                                            cur.execute(
                                                "INSERT INTO sync_deletes(entity, key, updated_at, device_id) VALUES (%s, %s::jsonb, NOW(), %s)",
                                                ("class_membership", json.dumps({"dni": dni, "user_id": int(uid) if uid else None, "clase_horario_id": int(clase_horario_id) if clase_horario_id else None}), device_id),
                                            )
                                        except Exception:
                                            pass
                                        applied += 1
                                        break
                                    except Exception as _e:
                                        code = getattr(_e, "pgcode", "") or ""
                                        if sp_name and code in ("40001", "40P01", "55P03") and attempt < max_attempts:
                                            try:
                                                logging.warning(f"sync upload retry rid={rid} op={name} attempt={attempt} code={code}")
                                            except Exception:
                                                pass
                                            try:
                                                cur.execute(f"ROLLBACK TO SAVEPOINT {sp_name}")
                                            except Exception:
                                                pass
                                            try:
                                                import time as _time
                                                _time.sleep(min(0.15 * (2 ** (attempt - 1)), 0.8))
                                            except Exception:
                                                pass
                                            continue
                                        raise
                            elif name in ("class_attendance.add", "class_attendance.update"):
                                # Campos: dni|user_id, clase_horario_id, fecha_clase, estado_asistencia, hora_llegada, observaciones
                                max_attempts = 3
                                for attempt in range(1, max_attempts + 1):
                                    try:
                                        dni = payload.get("dni")
                                        uid = payload.get("user_id") or payload.get("usuario_id")
                                        clase_horario_id = payload.get("clase_horario_id") or payload.get("horario_id")
                                        fecha_clase = payload.get("fecha_clase") or payload.get("fecha")
                                        estado = payload.get("estado_asistencia")
                                        hora_llegada = payload.get("hora_llegada")
                                        observaciones = payload.get("observaciones")
                                        if not uid and dni:
                                            cur.execute("SELECT id FROM usuarios WHERE dni = %s", (dni,))
                                            row_u = cur.fetchone()
                                            uid = row_u.get("id") if row_u else None
                                        if not uid or not clase_horario_id or not fecha_clase:
                                            raise ValueError("class_attendance.add/update requiere 'dni' o 'user_id', 'clase_horario_id' y 'fecha_clase'")
                                        # Upsert manual por (clase_horario_id, usuario_id, fecha_clase)
                                        cur.execute(
                                            "SELECT id FROM clase_asistencia_historial WHERE clase_horario_id = %s AND usuario_id = %s AND fecha_clase = CAST(%s AS date)",
                                            (int(clase_horario_id), int(uid), fecha_clase),
                                        )
                                        row = cur.fetchone()
                                        if row:
                                            aid = row.get("id")
                                            cur.execute(
                                                """
                                                UPDATE clase_asistencia_historial
                                                SET estado_asistencia = COALESCE(%s, estado_asistencia),
                                                    hora_llegada = COALESCE(CAST(%s AS time), hora_llegada),
                                                    observaciones = COALESCE(%s, observaciones),
                                                    updated_at = NOW(),
                                                    updated_by_device = %s
                                                WHERE id = %s
                                                """,
                                                (estado, hora_llegada, observaciones, device_id, aid),
                                            )
                                        else:
                                            cur.execute(
                                                """
                                                INSERT INTO clase_asistencia_historial
                                                (clase_horario_id, usuario_id, fecha_clase, estado_asistencia, hora_llegada, observaciones, updated_at, updated_by_device)
                                                VALUES (%s, %s, CAST(%s AS date), %s, CAST(%s AS time), %s, NOW(), %s)
                                                RETURNING id
                                                """,
                                                (int(clase_horario_id), int(uid), fecha_clase, estado, hora_llegada, observaciones, device_id),
                                            )
                                            _ = cur.fetchone()
                                        applied += 1
                                        break
                                    except Exception as _e:
                                        code = getattr(_e, "pgcode", "") or ""
                                        if sp_name and code in ("40001", "40P01", "55P03") and attempt < max_attempts:
                                            try:
                                                logging.warning(f"sync upload retry rid={rid} op={name} attempt={attempt} code={code}")
                                            except Exception:
                                                pass
                                            try:
                                                cur.execute(f"ROLLBACK TO SAVEPOINT {sp_name}")
                                            except Exception:
                                                pass
                                            try:
                                                import time as _time
                                                _time.sleep(min(0.15 * (2 ** (attempt - 1)), 0.8))
                                            except Exception:
                                                pass
                                            continue
                                        raise
                            elif name == "class_attendance.delete":
                                max_attempts = 3
                                for attempt in range(1, max_attempts + 1):
                                    try:
                                        dni = payload.get("dni")
                                        uid = payload.get("user_id") or payload.get("usuario_id")
                                        clase_horario_id = payload.get("clase_horario_id") or payload.get("horario_id")
                                        fecha_clase = payload.get("fecha_clase") or payload.get("fecha")
                                        if not uid and dni:
                                            cur.execute("SELECT id FROM usuarios WHERE dni = %s", (dni,))
                                            row_u = cur.fetchone()
                                            uid = row_u.get("id") if row_u else None
                                        if not uid or not clase_horario_id or not fecha_clase:
                                            raise ValueError("class_attendance.delete requiere 'dni' o 'user_id', 'clase_horario_id' y 'fecha_clase'")
                                        cur.execute(
                                            "DELETE FROM clase_asistencia_historial WHERE clase_horario_id = %s AND usuario_id = %s AND fecha_clase = CAST(%s AS date)",
                                            (int(clase_horario_id), int(uid), fecha_clase),
                                        )
                                        try:
                                            cur.execute(
                                                "INSERT INTO sync_deletes(entity, key, updated_at, device_id) VALUES (%s, %s::jsonb, NOW(), %s)",
                                                ("class_attendance", json.dumps({
                                                    "dni": dni, "user_id": int(uid) if uid else None,
                                                    "clase_horario_id": int(clase_horario_id) if clase_horario_id else None,
                                                    "fecha_clase": fecha_clase
                                                }), device_id),
                                            )
                                        except Exception:
                                            pass
                                        applied += 1
                                        break
                                    except Exception as _e:
                                        code = getattr(_e, "pgcode", "") or ""
                                        if sp_name and code in ("40001", "40P01", "55P03") and attempt < max_attempts:
                                            try:
                                                cur.execute(f"ROLLBACK TO SAVEPOINT {sp_name}")
                                            except Exception:
                                                pass
                                            try:
                                                import time as _time
                                                _time.sleep(min(0.15 * (2 ** (attempt - 1)), 0.8))
                                            except Exception:
                                                pass
                                            continue
                                        raise
                                else:
                                    # Por ahora, ignorar otros tipos no soportados
                                    skipped += 1
                                # Epílogo común: liberar savepoint si existe
                                try:
                                    if sp_name:
                                        cur.execute(f"RELEASE SAVEPOINT {sp_name}")
                                except Exception:
                                    pass
                    # fin for raw in ops
                    break  # conexión y procesamiento exitosos
                except (psycopg2.OperationalError, psycopg2.InterfaceError) as ce:
                    _conn_attempts += 1
                    try:
                        logging.warning(f"sync upload: error de conexión intento={_conn_attempts}: {ce}")
                    except Exception:
                        pass
                    if _conn_attempts >= 2:
                        raise
                    try:
                        time.sleep(0.6)
                        _force_db_init()
                    except Exception:
                        pass
                    continue
        except (psycopg2.OperationalError, psycopg2.InterfaceError) as e:
            logging.exception("sync upload: error de conexión persistente")
            return JSONResponse({"success": False, "message": "DB no disponible (conexión)"}, status_code=503)
        except Exception as e:
            logging.exception("sync upload: error procesando lote")
            return JSONResponse({"success": False, "message": str(e)}, status_code=500)

        return JSONResponse({
            "success": True,
            "received": len(ops),
            "applied": applied,
            "skipped": skipped,
            "failed": failed,
            "errors": errors if errors else None,
        }, status_code=202)
    except Exception as e:
        try:
            logging.exception("Error en /api/sync/upload")
        except Exception:
            pass
        return JSONResponse({"success": False, "message": str(e)}, status_code=500)

'''  # end legacy sync block


# @app.post("/api/admin/sync-migrate")  # disabled: legacy sync removed
async def admin_sync_migrate(request: Request):
    """
    Ejecuta una migración idempotente para habilitar el sistema de sincronización
    (tablas y columnas necesarias). Protegido por dev_password u owner_password.
    """
    return JSONResponse(
        {"detail": "Legacy sync removed. Use SymmetricDS."},
        status_code=410,
    )
    ''' Legacy admin_sync_migrate body disabled; use SymmetricDS migration.
    try:
        content_type = request.headers.get("content-type", "")
        if content_type.startswith("application/json"):
            data = await request.json()
        else:
            data = await request.form()
    except Exception:
        data = {}

    dev_pwd = str(data.get("dev_password", "") or "").strip()
    owner_pwd = str(data.get("owner_password", "") or "").strip()

    # Resolver credenciales válidas
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
        real_dev = os.getenv("DEV_PASSWORD", "").strip()

    owner_ok = False
    try:
        if owner_pwd and owner_pwd == _get_password():
            owner_ok = True
    except Exception:
        owner_ok = False

    if not ((dev_pwd and real_dev and dev_pwd == real_dev) or owner_ok):
        return JSONResponse({"ok": False, "message": "No autorizado"}, status_code=401)

    db = _get_db()
    if not db:
        return JSONResponse({"ok": False, "message": "DatabaseManager no disponible"}, status_code=500)

    try:
        changes = _ensure_sync_schema()
        return JSONResponse({"ok": True, "applied": changes})
    except Exception as e:
        try:
            logging.exception("Error en /api/admin/sync-migrate")
        except Exception:
            pass
        return JSONResponse({"ok": False, "message": str(e)}, status_code=500)

'''  # end legacy admin_sync_migrate block

# @app.get("/api/sync/download")  # disabled: legacy sync removed
async def api_sync_download(request: Request):
    """Devuelve cambios de usuarios desde un instante dado.

    Parámetros:
      - since: ISO8601 opcional. Si no se provee, devuelve vacío.

    Respuesta:
      { "success": true, "operations": [ {"type": "user.update", "payload": {...}, "ts": "..." } ], "latest": "..." }
    """
    return JSONResponse(
        {"detail": "Legacy sync removed. Use SymmetricDS."},
        status_code=410,
    )
    ''' Legacy download body disabled; see SymmetricDS setup.
    try:
        # Autenticación opcional por token
        try:
            expected = os.getenv("SYNC_API_TOKEN", "").strip()
            if expected:
                auth = request.headers.get("Authorization") or ""
                if not isinstance(auth, str) or not auth.strip().startswith("Bearer ") or auth.strip()[7:] != expected:
                    return JSONResponse({"success": False, "message": "No autorizado"}, status_code=401)
        except Exception:
            pass
        since = request.query_params.get("since")
        device_id = request.query_params.get("device_id")
        db = _get_db()
        if db is None:
            return JSONResponse({"success": False, "message": "DB no disponible"}, status_code=503)

        operations = []
        latest_ts: Optional[str] = None

        # Si no se provee since, devolver vacío pero con tiempo del servidor
        if not since:
            return JSONResponse({
                "success": True,
                "operations": [],
                "latest": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
            })

        # Normalizar since (aceptar ...Z convirtiéndolo a +00:00)
        try:
            since_param = since.strip()
            if since_param.endswith("Z"):
                since_param = since_param[:-1] + "+00:00"
        except Exception:
            since_param = since

        # Consultar filas cambiadas desde 'since' para usuarios, pagos, asistencias y borrados
        try:
            # Ejecutar migraciones ligeras en una sesión dedicada en autocommit
            with db.autocommit_session() as conn:  # type: ignore
                with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:  # type: ignore
                    # Migraciones ligeras para asegurar columnas y tablas necesarias
                    try:
                        cur.execute("ALTER TABLE IF EXISTS usuarios ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ DEFAULT NOW()")
                        cur.execute("ALTER TABLE IF EXISTS usuarios ADD COLUMN IF NOT EXISTS updated_by_device TEXT")
                        cur.execute("ALTER TABLE IF EXISTS pagos ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ DEFAULT NOW()")
                        cur.execute("ALTER TABLE IF EXISTS pagos ADD COLUMN IF NOT EXISTS updated_by_device TEXT")
                        cur.execute("ALTER TABLE IF EXISTS asistencias ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ DEFAULT NOW()")
                        cur.execute("ALTER TABLE IF EXISTS asistencias ADD COLUMN IF NOT EXISTS updated_by_device TEXT")
                        cur.execute("ALTER TABLE IF EXISTS clase_asistencia_historial ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ DEFAULT NOW()")
                        cur.execute("ALTER TABLE IF EXISTS clase_asistencia_historial ADD COLUMN IF NOT EXISTS updated_by_device TEXT")
                        # Extensiones de sincronización nuevas
                        cur.execute("ALTER TABLE IF EXISTS horarios_profesores ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ DEFAULT NOW()")
                        cur.execute("ALTER TABLE IF EXISTS horarios_profesores ADD COLUMN IF NOT EXISTS updated_by_device TEXT")
                        cur.execute("ALTER TABLE IF EXISTS profesor_suplencias ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ DEFAULT NOW()")
                        cur.execute("ALTER TABLE IF EXISTS profesor_suplencias ADD COLUMN IF NOT EXISTS updated_by_device TEXT")
                        cur.execute(
                            """
                            CREATE TABLE IF NOT EXISTS sync_deletes (
                                id BIGSERIAL PRIMARY KEY,
                                entity TEXT NOT NULL,
                                key JSONB NOT NULL,
                                updated_at TIMESTAMPTZ DEFAULT NOW()
                            )
                            """
                        )
                        cur.execute("ALTER TABLE IF EXISTS sync_deletes ADD COLUMN IF NOT EXISTS device_id TEXT")
                        # Clases y derivados
                        cur.execute("ALTER TABLE IF EXISTS clases ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ DEFAULT NOW()")
                        cur.execute("ALTER TABLE IF EXISTS clases ADD COLUMN IF NOT EXISTS updated_by_device TEXT")
                        cur.execute("ALTER TABLE IF EXISTS clases_horarios ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ DEFAULT NOW()")
                        cur.execute("ALTER TABLE IF EXISTS clases_horarios ADD COLUMN IF NOT EXISTS updated_by_device TEXT")
                        cur.execute("ALTER TABLE IF EXISTS clase_usuarios ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ DEFAULT NOW()")
                        cur.execute("ALTER TABLE IF EXISTS clase_usuarios ADD COLUMN IF NOT EXISTS updated_by_device TEXT")
                        # Rutinas y ejercicios
                        cur.execute("ALTER TABLE IF EXISTS rutinas ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ DEFAULT NOW()")
                        cur.execute("ALTER TABLE IF EXISTS rutinas ADD COLUMN IF NOT EXISTS updated_by_device TEXT")
                        cur.execute("ALTER TABLE IF EXISTS rutinas_ejercicios ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ DEFAULT NOW()")
                        cur.execute("ALTER TABLE IF EXISTS rutinas_ejercicios ADD COLUMN IF NOT EXISTS updated_by_device TEXT")
                        cur.execute("ALTER TABLE IF EXISTS ejercicios ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ DEFAULT NOW()")
                        cur.execute("ALTER TABLE IF EXISTS ejercicios ADD COLUMN IF NOT EXISTS updated_by_device TEXT")
                    except Exception:
                        pass

                    # Extensiones de sincronización para etiquetas y notas
                    try:
                        cur.execute("ALTER TABLE IF EXISTS etiquetas ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ DEFAULT NOW()")
                        cur.execute("ALTER TABLE IF EXISTS etiquetas ADD COLUMN IF NOT EXISTS updated_by_device TEXT")
                        cur.execute("ALTER TABLE IF EXISTS usuario_etiquetas ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ DEFAULT NOW()")
                        cur.execute("ALTER TABLE IF EXISTS usuario_etiquetas ADD COLUMN IF NOT EXISTS updated_by_device TEXT")
                        cur.execute("ALTER TABLE IF EXISTS usuario_notas ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ DEFAULT NOW()")
                        cur.execute("ALTER TABLE IF EXISTS usuario_notas ADD COLUMN IF NOT EXISTS updated_by_device TEXT")
                        try:
                            cur.execute("ALTER TABLE IF EXISTS usuario_notas ALTER COLUMN categoria SET DEFAULT 'general'")
                            cur.execute("ALTER TABLE IF EXISTS usuario_notas ALTER COLUMN importancia SET DEFAULT 'normal'")
                        except Exception:
                            pass
                        # Saneamiento de registros existentes
                        try:
                            cur.execute("UPDATE usuario_notas SET categoria = 'general' WHERE categoria IS NULL")
                        except Exception:
                            pass
                        try:
                            cur.execute("UPDATE usuario_notas SET importancia = 'normal' WHERE importancia IS NULL")
                        except Exception:
                            pass
                        # Tablas adicionales operativas no cubiertas (segundo bloque)
                        try:
                            cur.execute("ALTER TABLE IF EXISTS rutina_ejercicios ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ DEFAULT NOW()")
                            cur.execute("ALTER TABLE IF EXISTS rutina_ejercicios ADD COLUMN IF NOT EXISTS updated_by_device TEXT")
                            cur.execute("ALTER TABLE IF EXISTS clase_ejercicios ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ DEFAULT NOW()")
                            cur.execute("ALTER TABLE IF EXISTS clase_ejercicios ADD COLUMN IF NOT EXISTS updated_by_device TEXT")
                            cur.execute("ALTER TABLE IF EXISTS ejercicio_grupos ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ DEFAULT NOW()")
                            cur.execute("ALTER TABLE IF EXISTS ejercicio_grupos ADD COLUMN IF NOT EXISTS updated_by_device TEXT")
                            cur.execute("ALTER TABLE IF EXISTS ejercicio_grupo_items ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ DEFAULT NOW()")
                            cur.execute("ALTER TABLE IF EXISTS ejercicio_grupo_items ADD COLUMN IF NOT EXISTS updated_by_device TEXT")
                            cur.execute("ALTER TABLE IF EXISTS profesores ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ DEFAULT NOW()")
                            cur.execute("ALTER TABLE IF EXISTS profesores ADD COLUMN IF NOT EXISTS updated_by_device TEXT")
                            cur.execute("ALTER TABLE IF EXISTS profesores_horarios_disponibilidad ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ DEFAULT NOW()")
                            cur.execute("ALTER TABLE IF EXISTS profesores_horarios_disponibilidad ADD COLUMN IF NOT EXISTS updated_by_device TEXT")
                            cur.execute("ALTER TABLE IF EXISTS profesor_evaluaciones ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ DEFAULT NOW()")
                            cur.execute("ALTER TABLE IF EXISTS profesor_evaluaciones ADD COLUMN IF NOT EXISTS updated_by_device TEXT")
                            cur.execute("ALTER TABLE IF EXISTS profesor_disponibilidad ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ DEFAULT NOW()")
                            cur.execute("ALTER TABLE IF EXISTS profesor_disponibilidad ADD COLUMN IF NOT EXISTS updated_by_device TEXT")
                            cur.execute("ALTER TABLE IF EXISTS profesor_suplencias_generales ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ DEFAULT NOW()")
                            cur.execute("ALTER TABLE IF EXISTS profesor_suplencias_generales ADD COLUMN IF NOT EXISTS updated_by_device TEXT")
                            cur.execute("ALTER TABLE IF EXISTS tipos_cuota ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ DEFAULT NOW()")
                            cur.execute("ALTER TABLE IF EXISTS tipos_cuota ADD COLUMN IF NOT EXISTS updated_by_device TEXT")
                            cur.execute("ALTER TABLE IF EXISTS numeracion_comprobantes ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ DEFAULT NOW()")
                            cur.execute("ALTER TABLE IF EXISTS numeracion_comprobantes ADD COLUMN IF NOT EXISTS updated_by_device TEXT")
                            cur.execute("ALTER TABLE IF EXISTS comprobantes_pago ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ DEFAULT NOW()")
                            cur.execute("ALTER TABLE IF EXISTS comprobantes_pago ADD COLUMN IF NOT EXISTS updated_by_device TEXT")
                            cur.execute("ALTER TABLE IF EXISTS pago_detalles ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ DEFAULT NOW()")
                            cur.execute("ALTER TABLE IF EXISTS pago_detalles ADD COLUMN IF NOT EXISTS updated_by_device TEXT")
                            cur.execute("ALTER TABLE IF EXISTS historial_estados ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ DEFAULT NOW()")
                            cur.execute("ALTER TABLE IF EXISTS historial_estados ADD COLUMN IF NOT EXISTS updated_by_device TEXT")
                            cur.execute("ALTER TABLE IF EXISTS especialidades ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ DEFAULT NOW()")
                            cur.execute("ALTER TABLE IF EXISTS especialidades ADD COLUMN IF NOT EXISTS updated_by_device TEXT")
                            cur.execute("ALTER TABLE IF EXISTS profesor_especialidades ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ DEFAULT NOW()")
                            cur.execute("ALTER TABLE IF EXISTS profesor_especialidades ADD COLUMN IF NOT EXISTS updated_by_device TEXT")
                            cur.execute("ALTER TABLE IF EXISTS profesor_certificaciones ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ DEFAULT NOW()")
                            cur.execute("ALTER TABLE IF EXISTS profesor_certificaciones ADD COLUMN IF NOT EXISTS updated_by_device TEXT")
                            cur.execute("ALTER TABLE IF EXISTS profesor_horas_trabajadas ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ DEFAULT NOW()")
                            cur.execute("ALTER TABLE IF EXISTS profesor_horas_trabajadas ADD COLUMN IF NOT EXISTS updated_by_device TEXT")
                            cur.execute("ALTER TABLE IF EXISTS schedule_conflicts ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ DEFAULT NOW()")
                            cur.execute("ALTER TABLE IF EXISTS schedule_conflicts ADD COLUMN IF NOT EXISTS updated_by_device TEXT")
                            cur.execute("ALTER TABLE IF EXISTS configuracion_comprobantes ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ DEFAULT NOW()")
                            cur.execute("ALTER TABLE IF EXISTS configuracion_comprobantes ADD COLUMN IF NOT EXISTS updated_by_device TEXT")
                            # WhatsApp operational tables tracking (segundo bloque)
                            cur.execute("ALTER TABLE IF EXISTS whatsapp_messages ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ DEFAULT NOW()")
                            cur.execute("ALTER TABLE IF EXISTS whatsapp_messages ADD COLUMN IF NOT EXISTS updated_by_device TEXT")
                            cur.execute("ALTER TABLE IF EXISTS whatsapp_templates ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ DEFAULT NOW()")
                            cur.execute("ALTER TABLE IF EXISTS whatsapp_templates ADD COLUMN IF NOT EXISTS updated_by_device TEXT")
                            cur.execute("ALTER TABLE IF EXISTS whatsapp_config ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ DEFAULT NOW()")
                            cur.execute("ALTER TABLE IF EXISTS whatsapp_config ADD COLUMN IF NOT EXISTS updated_by_device TEXT")
                            # Defaults y sanitización para WhatsApp (bloque 2)
                            cur.execute("ALTER TABLE IF EXISTS whatsapp_messages ALTER COLUMN status SET DEFAULT 'sent'")
                            cur.execute("ALTER TABLE IF EXISTS whatsapp_messages ALTER COLUMN message_content SET DEFAULT ''")
                            cur.execute("UPDATE whatsapp_messages SET status = 'sent' WHERE status IS NULL")
                            cur.execute("UPDATE whatsapp_messages SET message_content = '' WHERE message_content IS NULL")
                            cur.execute("ALTER TABLE IF EXISTS whatsapp_templates ALTER COLUMN header_text SET DEFAULT ''")
                            cur.execute("ALTER TABLE IF EXISTS whatsapp_templates ALTER COLUMN variables SET DEFAULT '{}'::jsonb")
                            cur.execute("UPDATE whatsapp_templates SET header_text = '' WHERE header_text IS NULL")
                            cur.execute("UPDATE whatsapp_templates SET variables = '{}'::jsonb WHERE variables IS NULL")
                            cur.execute("ALTER TABLE IF EXISTS whatsapp_config ALTER COLUMN access_token SET DEFAULT ''")
                            cur.execute("ALTER TABLE IF EXISTS whatsapp_config ALTER COLUMN phone_id SET DEFAULT ''")
                            cur.execute("ALTER TABLE IF EXISTS whatsapp_config ALTER COLUMN waba_id SET DEFAULT ''")
                            cur.execute("UPDATE whatsapp_config SET access_token = '' WHERE access_token IS NULL")
                            cur.execute("UPDATE whatsapp_config SET phone_id = '' WHERE phone_id IS NULL")
                            cur.execute("UPDATE whatsapp_config SET waba_id = '' WHERE waba_id IS NULL")
                            cur.execute("ALTER TABLE IF EXISTS professor_availability ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ DEFAULT NOW()")
                            cur.execute("ALTER TABLE IF EXISTS professor_availability ADD COLUMN IF NOT EXISTS updated_by_device TEXT")
                            cur.execute("ALTER TABLE IF EXISTS professor_availability ALTER COLUMN status SET DEFAULT 'disponible'")
                            cur.execute("ALTER TABLE IF EXISTS professor_availability ALTER COLUMN notes SET DEFAULT ''")
                            cur.execute("UPDATE professor_availability SET status = 'disponible' WHERE status IS NULL")
                            cur.execute("UPDATE professor_availability SET notes = '' WHERE notes IS NULL")
                        except Exception:
                            pass
                        try:
                            cur.execute("UPDATE usuario_notas SET titulo = '' WHERE titulo IS NULL")
                        except Exception:
                            pass
                        try:
                            cur.execute("UPDATE usuario_notas SET contenido = '' WHERE contenido IS NULL")
                        except Exception:
                            pass
                    except Exception:
                        pass

            # Abrir conexión normal para consultas de descarga
            with db.get_connection_context() as conn:  # type: ignore
                with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:  # type: ignore
                    # Rutinas - DOWNLOAD
                    try:
                        cur.execute(
                            """
                            SELECT id, usuario_id, nombre_rutina, descripcion, dias_semana, categoria, activa,
                                   COALESCE(updated_at, NOW()) AS updated_at
                            FROM rutinas
                            WHERE COALESCE(updated_at, NOW()) >= CAST(%s AS timestamptz)
                              AND (%s IS NULL OR updated_by_device IS DISTINCT FROM %s)
                            ORDER BY updated_at ASC
                            LIMIT 500
                            """,
                            (since_param, device_id, device_id),
                        )
                        rows_rut = cur.fetchall() or []
                        for r in rows_rut:
                            op = {
                                "type": "routine.update",
                                "payload": {
                                    "id": r.get("id"),
                                    "usuario_id": r.get("usuario_id"),
                                    "nombre_rutina": r.get("nombre_rutina"),
                                    "descripcion": r.get("descripcion"),
                                    "dias_semana": r.get("dias_semana"),
                                    "categoria": r.get("categoria"),
                                    "activa": r.get("activa"),
                                },
                                "ts": None,
                            }
                            try:
                                ts = r.get("updated_at")
                                ts_str = ts.isoformat() if hasattr(ts, 'isoformat') else str(ts)
                                if ts_str and not ts_str.endswith("Z"):
                                    ts_str = ts_str.replace("+00:00", "Z") if "+00:00" in ts_str else ts_str + "Z"
                                op["ts"] = ts_str
                                latest_ts = max(latest_ts or ts_str, ts_str)
                            except Exception:
                                pass
                            operations.append(op)
                    except Exception:
                        pass

                    # Rutinas - Ejercicios - DOWNLOAD
                    try:
                        cur.execute(
                            """
                            SELECT id, rutina_id, ejercicio_id, dia_semana, series, repeticiones, orden,
                                   COALESCE(updated_at, NOW()) AS updated_at
                            FROM rutinas_ejercicios
                            WHERE COALESCE(updated_at, NOW()) >= CAST(%s AS timestamptz)
                              AND (%s IS NULL OR updated_by_device IS DISTINCT FROM %s)
                            ORDER BY updated_at ASC
                            LIMIT 500
                            """,
                            (since_param, device_id, device_id),
                        )
                        rows_rut_ej = cur.fetchall() or []
                        for r in rows_rut_ej:
                            op = {
                                "type": "routine_exercise.update",
                                "payload": {
                                    "id": r.get("id"),
                                    "rutina_id": r.get("rutina_id"),
                                    "ejercicio_id": r.get("ejercicio_id"),
                                    "dia_semana": r.get("dia_semana"),
                                    "series": r.get("series"),
                                    "repeticiones": r.get("repeticiones"),
                                    "orden": r.get("orden"),
                                },
                                "ts": None,
                            }
                            try:
                                ts = r.get("updated_at")
                                ts_str = ts.isoformat() if hasattr(ts, 'isoformat') else str(ts)
                                if ts_str and not ts_str.endswith("Z"):
                                    ts_str = ts_str.replace("+00:00", "Z") if "+00:00" in ts_str else ts_str + "Z"
                                op["ts"] = ts_str
                                latest_ts = max(latest_ts or ts_str, ts_str)
                            except Exception:
                                pass
                            operations.append(op)
                    except Exception:
                        pass

                    # Catálogo de ejercicios - DOWNLOAD
                    try:
                        cur.execute(
                            """
                            SELECT id, nombre, grupo_muscular, descripcion,
                                   COALESCE(updated_at, NOW()) AS updated_at
                            FROM ejercicios
                            WHERE COALESCE(updated_at, NOW()) >= CAST(%s AS timestamptz)
                              AND (%s IS NULL OR updated_by_device IS DISTINCT FROM %s)
                            ORDER BY updated_at ASC
                            LIMIT 500
                            """,
                            (since_param, device_id, device_id),
                        )
                        rows_ej = cur.fetchall() or []
                        for r in rows_ej:
                            op = {
                                "type": "exercise.update",
                                "payload": {
                                    "id": r.get("id"),
                                    "nombre": r.get("nombre"),
                                    "grupo_muscular": r.get("grupo_muscular"),
                                    "descripcion": r.get("descripcion"),
                                },
                                "ts": None,
                            }
                            try:
                                ts = r.get("updated_at")
                                ts_str = ts.isoformat() if hasattr(ts, 'isoformat') else str(ts)
                                if ts_str and not ts_str.endswith("Z"):
                                    ts_str = ts_str.replace("+00:00", "Z") if "+00:00" in ts_str else ts_str + "Z"
                                op["ts"] = ts_str
                                latest_ts = max(latest_ts or ts_str, ts_str)
                            except Exception:
                                pass
                            operations.append(op)
                    except Exception:
                        pass

                    # Etiquetas (tags) - DOWNLOAD
                    try:
                        cur.execute(
                            """
                            SELECT id, nombre, color, descripcion, 
                                   COALESCE(updated_at, NOW()) AS updated_at
                            FROM etiquetas
                            WHERE COALESCE(updated_at, NOW()) >= CAST(%s AS timestamptz)
                              AND (%s IS NULL OR updated_by_device IS DISTINCT FROM %s)
                            ORDER BY updated_at ASC
                            LIMIT 500
                            """,
                            (since_param, device_id, device_id),
                        )
                        rows_tags = cur.fetchall() or []
                        for r in rows_tags:
                            op = {
                                "type": "tag.update",
                                "payload": {
                                    "id": r.get("id"),
                                    "nombre": r.get("nombre"),
                                    "color": r.get("color"),
                                    "descripcion": r.get("descripcion"),
                                },
                                "ts": None,
                            }
                            try:
                                ts = r.get("updated_at")
                                ts_str = ts.isoformat() if hasattr(ts, 'isoformat') else str(ts)
                                if ts_str and not ts_str.endswith("Z"):
                                    ts_str = ts_str.replace("+00:00", "Z") if "+00:00" in ts_str else ts_str + "Z"
                                op["ts"] = ts_str
                                latest_ts = max(latest_ts or ts_str, ts_str)
                            except Exception:
                                pass
                            operations.append(op)
                    except Exception:
                        pass

                    # Usuarios
                    cur.execute(
                        """
                        SELECT id, dni, nombre, telefono, tipo_cuota, activo,
                               COALESCE(updated_at, NOW()) AS updated_at
                        FROM usuarios
                        WHERE COALESCE(updated_at, NOW()) >= CAST(%s AS timestamptz)
                          AND (%s IS NULL OR updated_by_device IS DISTINCT FROM %s)
                        ORDER BY updated_at ASC
                        LIMIT 500
                        """,
                        (since_param, device_id, device_id),
                    )
                    rows_users = cur.fetchall() or []
                    for r in rows_users:
                        is_active = bool(r.get("activo"))
                        op_type = "user.update" if is_active else "user.delete"
                        op = {
                            "type": op_type,
                            "payload": {
                                "id": r.get("id"),
                                "dni": r.get("dni"),
                                "name": r.get("nombre"),
                                "phone": r.get("telefono"),
                                "membership_type": r.get("tipo_cuota"),
                                "active": is_active,
                            },
                            "ts": None,
                        }
                        try:
                            ts = r.get("updated_at")
                            ts_str = None
                            if ts is not None:
                                if hasattr(ts, 'isoformat'):
                                    ts_str = ts.isoformat()
                                else:
                                    ts_str = str(ts)
                                try:
                                    if ts_str.endswith("Z"):
                                        pass
                                    elif "+00:00" in ts_str:
                                        ts_str = ts_str.replace("+00:00", "Z")
                                    elif "+" not in ts_str and "-" not in ts_str[10:]:
                                        ts_str = ts_str + "Z"
                                except Exception:
                                    pass
                                op["ts"] = ts_str
                                latest_ts = max(latest_ts or ts_str, ts_str)
                        except Exception:
                            pass
                        operations.append(op)

                    # Pagos
                    try:
                        cur.execute(
                            """
                            SELECT p.id, p.usuario_id, u.dni, p.mes, p.año, p.monto, p.fecha_pago,
                                   COALESCE(p.updated_at, NOW()) AS updated_at
                            FROM pagos p
                            JOIN usuarios u ON u.id = p.usuario_id
                            WHERE COALESCE(p.updated_at, NOW()) >= CAST(%s AS timestamptz)
                              AND (%s IS NULL OR p.updated_by_device IS DISTINCT FROM %s)
                            ORDER BY p.updated_at ASC
                            LIMIT 500
                            """,
                            (since_param, device_id, device_id),
                        )
                        rows_pay = cur.fetchall() or []
                        for r in rows_pay:
                            op = {
                                "type": "payment.update",
                                "payload": {
                                    "user_id": r.get("usuario_id"),
                                    "dni": r.get("dni"),
                                    "mes": r.get("mes"),
                                    "año": r.get("año"),
                                    "monto": float(r.get("monto")) if r.get("monto") is not None else None,
                                    "fecha_pago": str(r.get("fecha_pago")) if r.get("fecha_pago") is not None else None,
                                },
                                "ts": None,
                            }
                            try:
                                ts = r.get("updated_at")
                                ts_str = ts.isoformat() if hasattr(ts, 'isoformat') else str(ts)
                                if ts_str and not ts_str.endswith("Z"):
                                    ts_str = ts_str.replace("+00:00", "Z") if "+00:00" in ts_str else ts_str + "Z"
                                op["ts"] = ts_str
                                latest_ts = max(latest_ts or ts_str, ts_str)
                            except Exception:
                                pass
                                operations.append(op)
                    except Exception:
                        pass

                    # Usuario-Etiquetas (asignaciones) - DOWNLOAD
                    try:
                        cur.execute(
                            """
                            SELECT ue.usuario_id, u.dni, ue.etiqueta_id,
                                   COALESCE(ue.updated_at, NOW()) AS updated_at
                            FROM usuario_etiquetas ue
                            LEFT JOIN usuarios u ON u.id = ue.usuario_id
                            WHERE COALESCE(ue.updated_at, NOW()) >= CAST(%s AS timestamptz)
                              AND (%s IS NULL OR ue.updated_by_device IS DISTINCT FROM %s)
                            ORDER BY ue.updated_at ASC
                            LIMIT 500
                            """,
                            (since_param, device_id, device_id),
                        )
                        rows_ut = cur.fetchall() or []
                        for r in rows_ut:
                            op = {
                                "type": "user_tag.update",
                                "payload": {
                                    "usuario_id": r.get("usuario_id"),
                                    "dni": r.get("dni"),
                                    "etiqueta_id": r.get("etiqueta_id"),
                                },
                                "ts": None,
                            }
                            try:
                                ts = r.get("updated_at")
                                ts_str = ts.isoformat() if hasattr(ts, 'isoformat') else str(ts)
                                if ts_str and not ts_str.endswith("Z"):
                                    ts_str = ts_str.replace("+00:00", "Z") if "+00:00" in ts_str else ts_str + "Z"
                                op["ts"] = ts_str
                                latest_ts = max(latest_ts or ts_str, ts_str)
                            except Exception:
                                pass
                            operations.append(op)
                    except Exception:
                        pass

                    # Notas de usuario - DOWNLOAD
                    try:
                        cur.execute(
                            """
                            SELECT n.id, n.usuario_id, u.dni, n.categoria, n.titulo, n.contenido, n.importancia, n.activa,
                                   COALESCE(n.updated_at, NOW()) AS updated_at
                            FROM usuario_notas n
                            LEFT JOIN usuarios u ON u.id = n.usuario_id
                            WHERE COALESCE(n.updated_at, NOW()) >= CAST(%s AS timestamptz)
                              AND (%s IS NULL OR n.updated_by_device IS DISTINCT FROM %s)
                            ORDER BY n.updated_at ASC
                            LIMIT 500
                            """,
                            (since_param, device_id, device_id),
                        )
                        rows_notes = cur.fetchall() or []
                        for r in rows_notes:
                            op = {
                                "type": "note.update",
                                "payload": {
                                    "id": r.get("id"),
                                    "usuario_id": r.get("usuario_id"),
                                    "dni": r.get("dni"),
                                    "categoria": r.get("categoria"),
                                    "titulo": r.get("titulo"),
                                    "contenido": r.get("contenido"),
                                    "importancia": r.get("importancia"),
                                    "activa": r.get("activa"),
                                },
                                "ts": None,
                            }
                            try:
                                ts = r.get("updated_at")
                                ts_str = ts.isoformat() if hasattr(ts, 'isoformat') else str(ts)
                                if ts_str and not ts_str.endswith("Z"):
                                    ts_str = ts_str.replace("+00:00", "Z") if "+00:00" in ts_str else ts_str + "Z"
                                op["ts"] = ts_str
                                latest_ts = max(latest_ts or ts_str, ts_str)
                            except Exception:
                                pass
                            operations.append(op)
                    except Exception:
                        pass

                    # Horarios de profesores (disponibilidad) - DOWNLOAD
                    try:
                        cur.execute(
                            """
                            SELECT id, profesor_id, dia_semana, hora_inicio, hora_fin, disponible,
                                   COALESCE(updated_at, NOW()) AS updated_at
                            FROM horarios_profesores
                            WHERE COALESCE(updated_at, NOW()) >= CAST(%s AS timestamptz)
                              AND (%s IS NULL OR updated_by_device IS DISTINCT FROM %s)
                            ORDER BY updated_at ASC
                            LIMIT 500
                            """,
                            (since_param, device_id, device_id),
                        )
                        rows_hp = cur.fetchall() or []
                        for r in rows_hp:
                            op = {
                                "type": "professor_schedule.update",
                                "payload": {
                                    "id": r.get("id"),
                                    "profesor_id": r.get("profesor_id"),
                                    "dia_semana": r.get("dia_semana"),
                                    "hora_inicio": str(r.get("hora_inicio")) if r.get("hora_inicio") is not None else None,
                                    "hora_fin": str(r.get("hora_fin")) if r.get("hora_fin") is not None else None,
                                    "disponible": r.get("disponible"),
                                },
                                "ts": None,
                            }
                            try:
                                ts = r.get("updated_at")
                                ts_str = ts.isoformat() if hasattr(ts, 'isoformat') else str(ts)
                                if ts_str and not ts_str.endswith("Z"):
                                    ts_str = ts_str.replace("+00:00", "Z") if "+00:00" in ts_str else ts_str + "Z"
                                op["ts"] = ts_str
                                latest_ts = max(latest_ts or ts_str, ts_str)
                            except Exception:
                                pass
                            operations.append(op)
                    except Exception:
                        pass

                    # Suplencias de profesor (por clase) - DOWNLOAD
                    try:
                        cur.execute(
                            """
                            SELECT id, asignacion_id, profesor_suplente_id,
                                   fecha_clase::date AS fecha_clase, motivo, estado, notas,
                                   COALESCE(updated_at, NOW()) AS updated_at
                            FROM profesor_suplencias
                            WHERE COALESCE(updated_at, NOW()) >= CAST(%s AS timestamptz)
                              AND (%s IS NULL OR updated_by_device IS DISTINCT FROM %s)
                            ORDER BY updated_at ASC
                            LIMIT 500
                            """,
                            (since_param, device_id, device_id),
                        )
                        rows_ps = cur.fetchall() or []
                        for r in rows_ps:
                            op = {
                                "type": "professor_substitution.update",
                                "payload": {
                                    "id": r.get("id"),
                                    "asignacion_id": r.get("asignacion_id"),
                                    "profesor_suplente_id": r.get("profesor_suplente_id"),
                                    "fecha_clase": str(r.get("fecha_clase")) if r.get("fecha_clase") is not None else None,
                                    "motivo": r.get("motivo"),
                                    "estado": r.get("estado"),
                                    "notas": r.get("notas"),
                                },
                                "ts": None,
                            }
                            try:
                                ts = r.get("updated_at")
                                ts_str = ts.isoformat() if hasattr(ts, 'isoformat') else str(ts)
                                if ts_str and not ts_str.endswith("Z"):
                                    ts_str = ts_str.replace("+00:00", "Z") if "+00:00" in ts_str else ts_str + "Z"
                                op["ts"] = ts_str
                                latest_ts = max(latest_ts or ts_str, ts_str)
                            except Exception:
                                pass
                            operations.append(op)
                    except Exception:
                        pass

                    # Asistencias
                    try:
                        cur.execute(
                            """
                            SELECT a.id, a.usuario_id, u.dni, a.fecha::date AS fecha, a.hora_registro,
                                   COALESCE(a.updated_at, NOW()) AS updated_at
                            FROM asistencias a
                            JOIN usuarios u ON u.id = a.usuario_id
                            WHERE COALESCE(a.updated_at, NOW()) >= CAST(%s AS timestamptz)
                              AND (%s IS NULL OR a.updated_by_device IS DISTINCT FROM %s)
                            ORDER BY a.updated_at ASC
                            LIMIT 500
                            """,
                            (since_param, device_id, device_id),
                        )
                        rows_att = cur.fetchall() or []
                        for r in rows_att:
                            op = {
                                "type": "attendance.update",
                                "payload": {
                                    "user_id": r.get("usuario_id"),
                                    "dni": r.get("dni"),
                                    "fecha": str(r.get("fecha")) if r.get("fecha") is not None else None,
                                    "hora": str(r.get("hora_registro")) if r.get("hora_registro") is not None else None,
                                },
                                "ts": None,
                            }
                            try:
                                ts = r.get("updated_at")
                                ts_str = ts.isoformat() if hasattr(ts, 'isoformat') else str(ts)
                                if ts_str and not ts_str.endswith("Z"):
                                    ts_str = ts_str.replace("+00:00", "Z") if "+00:00" in ts_str else ts_str + "Z"
                                op["ts"] = ts_str
                                latest_ts = max(latest_ts or ts_str, ts_str)
                            except Exception:
                                pass
                            operations.append(op)
                    except Exception:
                        pass

                    # Asistencias de clase
                    try:
                        cur.execute(
                            """
                            SELECT ah.id, ah.clase_horario_id, ah.usuario_id, u.dni,
                                   ah.fecha_clase::date AS fecha_clase, ah.estado_asistencia,
                                   ah.hora_llegada, ah.observaciones,
                                   COALESCE(ah.updated_at, NOW()) AS updated_at
                            FROM clase_asistencia_historial ah
                            JOIN usuarios u ON u.id = ah.usuario_id
                            WHERE COALESCE(ah.updated_at, NOW()) >= CAST(%s AS timestamptz)
                              AND (%s IS NULL OR ah.updated_by_device IS DISTINCT FROM %s)
                            ORDER BY ah.updated_at ASC
                            LIMIT 500
                            """,
                            (since_param, device_id, device_id),
                        )
                        rows_catt = cur.fetchall() or []
                        for r in rows_catt:
                            op = {
                                "type": "class_attendance.update",
                                "payload": {
                                    "user_id": r.get("usuario_id"),
                                    "dni": r.get("dni"),
                                    "clase_horario_id": r.get("clase_horario_id"),
                                    "fecha_clase": str(r.get("fecha_clase")) if r.get("fecha_clase") is not None else None,
                                    "estado_asistencia": r.get("estado_asistencia"),
                                    "hora_llegada": str(r.get("hora_llegada")) if r.get("hora_llegada") is not None else None,
                                    "observaciones": r.get("observaciones"),
                                },
                                "ts": None,
                            }
                            try:
                                ts = r.get("updated_at")
                                ts_str = ts.isoformat() if hasattr(ts, 'isoformat') else str(ts)
                                if ts_str and not ts_str.endswith("Z"):
                                    ts_str = ts_str.replace("+00:00", "Z") if "+00:00" in ts_str else ts_str + "Z"
                                op["ts"] = ts_str
                                latest_ts = max(latest_ts or ts_str, ts_str)
                            except Exception:
                                pass
                            operations.append(op)
                    except Exception:
                        pass

                    # Clases - DOWNLOAD
                    try:
                        cur.execute(
                            """
                            SELECT id, nombre, descripcion, activa, tipo_clase_id,
                                   COALESCE(updated_at, NOW()) AS updated_at
                            FROM clases
                            WHERE COALESCE(updated_at, NOW()) >= CAST(%s AS timestamptz)
                              AND (%s IS NULL OR updated_by_device IS DISTINCT FROM %s)
                            ORDER BY updated_at ASC
                            LIMIT 500
                            """,
                            (since_param, device_id, device_id),
                        )
                        rows_cls = cur.fetchall() or []
                        for r in rows_cls:
                            op = {
                                "type": "class.update",
                                "payload": {
                                    "id": r.get("id"),
                                    "nombre": r.get("nombre"),
                                    "descripcion": r.get("descripcion"),
                                    "activa": r.get("activa"),
                                    "tipo_clase_id": r.get("tipo_clase_id"),
                                },
                                "ts": None,
                            }
                            try:
                                ts = r.get("updated_at")
                                ts_str = ts.isoformat() if hasattr(ts, 'isoformat') else str(ts)
                                if ts_str and not ts_str.endswith("Z"):
                                    ts_str = ts_str.replace("+00:00", "Z") if "+00:00" in ts_str else ts_str + "Z"
                                op["ts"] = ts_str
                                latest_ts = max(latest_ts or ts_str, ts_str)
                            except Exception:
                                pass
                            operations.append(op)
                    except Exception:
                        pass

                    # Horarios de clases - DOWNLOAD
                    try:
                        cur.execute(
                            """
                            SELECT id, clase_id, dia_semana, hora_inicio, hora_fin, cupo_maximo, activo,
                                   COALESCE(updated_at, NOW()) AS updated_at
                            FROM clases_horarios
                            WHERE COALESCE(updated_at, NOW()) >= CAST(%s AS timestamptz)
                              AND (%s IS NULL OR updated_by_device IS DISTINCT FROM %s)
                            ORDER BY updated_at ASC
                            LIMIT 500
                            """,
                            (since_param, device_id, device_id),
                        )
                        rows_ch = cur.fetchall() or []
                        for r in rows_ch:
                            op = {
                                "type": "class_schedule.update",
                                "payload": {
                                    "id": r.get("id"),
                                    "clase_id": r.get("clase_id"),
                                    "dia_semana": r.get("dia_semana"),
                                    "hora_inicio": str(r.get("hora_inicio")) if r.get("hora_inicio") is not None else None,
                                    "hora_fin": str(r.get("hora_fin")) if r.get("hora_fin") is not None else None,
                                    "cupo_maximo": r.get("cupo_maximo"),
                                    "activo": r.get("activo"),
                                },
                                "ts": None,
                            }
                            try:
                                ts = r.get("updated_at")
                                ts_str = ts.isoformat() if hasattr(ts, 'isoformat') else str(ts)
                                if ts_str and not ts_str.endswith("Z"):
                                    ts_str = ts_str.replace("+00:00", "Z") if "+00:00" in ts_str else ts_str + "Z"
                                op["ts"] = ts_str
                                latest_ts = max(latest_ts or ts_str, ts_str)
                            except Exception:
                                pass
                            operations.append(op)
                    except Exception:
                        pass

                    # Membresías de clase (inscripciones) - DOWNLOAD
                    try:
                        cur.execute(
                            """
                            SELECT cu.id, cu.clase_horario_id, cu.usuario_id, u.dni,
                                   cu.fecha_inscripcion,
                                   COALESCE(cu.updated_at, NOW()) AS updated_at
                            FROM clase_usuarios cu
                            JOIN usuarios u ON u.id = cu.usuario_id
                            WHERE cu.updated_at IS NOT NULL AND cu.updated_at >= CAST(%s AS timestamptz)
                              AND (%s IS NULL OR cu.updated_by_device IS DISTINCT FROM %s)
                            ORDER BY cu.updated_at ASC
                            LIMIT 500
                            """,
                            (since_param, device_id, device_id),
                        )
                        rows_cm = cur.fetchall() or []
                        for r in rows_cm:
                            op = {
                                "type": "class_membership.update",
                                "payload": {
                                    "id": r.get("id"),
                                    "clase_horario_id": r.get("clase_horario_id"),
                                    "usuario_id": r.get("usuario_id"),
                                    "dni": r.get("dni"),
                                    "fecha_inscripcion": str(r.get("fecha_inscripcion")) if r.get("fecha_inscripcion") is not None else None,
                                },
                                "ts": None,
                            }
                            try:
                                ts = r.get("updated_at")
                                ts_str = ts.isoformat() if hasattr(ts, 'isoformat') else str(ts)
                                if ts_str and not ts_str.endswith("Z"):
                                    ts_str = ts_str.replace("+00:00", "Z") if "+00:00" in ts_str else ts_str + "Z"
                                op["ts"] = ts_str
                                latest_ts = max(latest_ts or ts_str, ts_str)
                            except Exception:
                                pass
                            operations.append(op)
                    except Exception:
                        pass

                    # Borrados (pagos, asistencias, clases, horarios, membresías, etc.) registrados en sync_deletes
                    try:
                        cur.execute(
                            """
                            SELECT entity, key, COALESCE(updated_at, NOW()) AS updated_at
                            FROM sync_deletes
                            WHERE updated_at IS NOT NULL AND updated_at >= CAST(%s AS timestamptz)
                              AND (%s IS NULL OR device_id IS DISTINCT FROM %s)
                            ORDER BY updated_at ASC
                            LIMIT 500
                            """,
                            (since_param, device_id, device_id),
                        )
                        rows_del = cur.fetchall() or []
                        for r in rows_del:
                            entity = str(r.get("entity") or "").strip()
                            if not entity:
                                continue
                            key = r.get("key") or {}
                            op = {
                                "type": f"{entity}.delete",
                                "payload": key,
                                "ts": None,
                            }
                            try:
                                ts = r.get("updated_at")
                                ts_str = ts.isoformat() if hasattr(ts, 'isoformat') else str(ts)
                                if ts_str and not ts_str.endswith("Z"):
                                    ts_str = ts_str.replace("+00:00", "Z") if "+00:00" in ts_str else ts_str + "Z"
                                op["ts"] = ts_str
                                latest_ts = max(latest_ts or ts_str, ts_str)
                            except Exception:
                                pass
                            operations.append(op)
                    except Exception:
                        pass
        except Exception as e:
            # No romper el sync si hay error: devolver vacío y latest=since (no adelantar watermark)
            try:
                logging.exception("sync download: error consultando cambios")
            except Exception:
                pass
            return JSONResponse({
                "success": True,
                "operations": operations,
                "latest": (since_param or since or ""),
                "message": str(e),
            }, status_code=200)

        # Ordenar por timestamp por si llegaron mezcladas (usuarios/pagos/asistencias)
        try:
            operations.sort(key=lambda x: (x.get("ts") or ""))
        except Exception:
            pass
        return JSONResponse({
            "success": True,
            "operations": operations,
            "latest": (latest_ts or since_param or since or "").replace("+00:00", "Z"),
        })
    except Exception as e:
        # Endurecer el endpoint: no propagar 5xx al cliente de sync.
        # En caso de error inesperado, responder 200 con operaciones vacías
        # y latest=since para no adelantar el marcador del cliente.
        try:
            logging.exception("Error en /api/sync/download")
        except Exception:
            pass
        try:
            nowz = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
        except Exception:
            nowz = ""
        return JSONResponse({
            "success": True,
            "operations": [],
            "latest": (since or ""),
            "message": str(e),
        }, status_code=200)

'''  # end legacy download block

# Evitar 404 de clientes de Vite durante desarrollo: devolver stub vacío
@app.get("/@vite/client")
async def vite_client_stub():
    return Response("// Vite client stub (deshabilitado en esta app)", media_type="application/javascript")

# Estado de SymmetricDS para toast/diagnóstico en producción (Railway)
@app.get("/webapp/symmetricds/status")
async def symmetricds_status():
    try:
        from pathlib import Path as _Path
        import json as _json
        base_dir = _Path(__file__).resolve().parent.parent
        status_path = base_dir / 'symmetricds' / 'status.json'
        if status_path.exists():
            try:
                data = _json.loads(status_path.read_text(encoding='utf-8') or '{}')
            except Exception:
                data = {}
        else:
            data = {}
        return JSONResponse({
            "running": bool(data.get("running", False)),
            "message": str(data.get("message", "Sin estado disponible")),
            "railway_port": data.get("railway_port"),
            "local_port": data.get("local_port"),
            "java_version": data.get("java_version"),
            "external_id": data.get("external_id"),
            "last_check_ts": data.get("last_check_ts"),
        }, status_code=200)
    except Exception as e:
        return JSONResponse({
            "running": False,
            "message": f"Error leyendo estado: {e}",
        }, status_code=200)

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
    # Preferir la contraseña del dueño desde caché TTL del DatabaseManager
    try:
        db = _get_db()
        if db and hasattr(db, 'get_owner_password_cached'):
            pwd = db.get_owner_password_cached(ttl_seconds=600)  # type: ignore
            if pwd:
                return str(pwd).strip()
    except Exception:
        pass
    # Fallback: variable de entorno
    pwd = os.getenv("WEBAPP_OWNER_PASSWORD", "").strip()
    if pwd:
        return pwd
    # Fallback adicional: contraseña de desarrollador si existe
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
    except Exception:
        # No bloquear el arranque; los endpoints intentarán reintentar
        pass

# Arrancar SymmetricDS en Railway/headless para replicación automática
@app.on_event("startup")
async def _startup_launch_symmetricds():
    try:
        # Evitar arranque si no hay DB todavía
        dbm = _get_db()
        if dbm is None:
            return
        # Gate por variable de entorno: por defecto NO iniciar en web
        try:
            if str(os.getenv("SYM_START_ON_WEB", "0")).strip().lower() not in ("1", "true", "yes"):
                return
        except Exception:
            return
        try:
            from symmetricds.setup_symmetric import start_symmetricds_background  # type: ignore
        except Exception:
            start_symmetricds_background = None  # type: ignore
        if start_symmetricds_background is not None:
            try:
                start_symmetricds_background(dbm, logger=logging)
                logging.info("SymmetricDS iniciado en segundo plano (startup web)")
            except Exception as e:
                logging.warning(f"No se pudo iniciar SymmetricDS en startup web: {e}")
    except Exception:
        # No bloquear el arranque del servidor web
        pass

# Apagado gracioso de SymmetricDS si fue iniciado desde la web
@app.on_event("shutdown")
async def _shutdown_stop_symmetricds():
    try:
        # Solo detener si estaba habilitado iniciar en la web
        if str(os.getenv("SYM_START_ON_WEB", "0")).strip().lower() not in ("1", "true", "yes"):
            return
        try:
            from symmetricds.setup_symmetric import stop_symmetricds  # type: ignore
        except Exception:
            stop_symmetricds = None  # type: ignore
        if stop_symmetricds is not None:
            try:
                stop_symmetricds(logger=logging)
                logging.info("SymmetricDS detenido (shutdown web)")
            except Exception as e:
                logging.warning(f"No se pudo detener SymmetricDS en shutdown web: {e}")
    except Exception:
        pass


def require_owner(request: Request):
    if not request.session.get("logged_in"):
        raise HTTPException(status_code=401, detail="Acceso restringido al dueño")
    return True


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
        return RedirectResponse(url="/?error=Ingrese%20la%20contrase%C3%B1a", status_code=303)
    ok = False
    # Contraseña única: obtenida por _get_password() (BD -> ENV [solo seed] -> DEV como último recurso)
    if password == _get_password():
        ok = True
    # Nota: Se ha eliminado el uso del PIN del dueño para autenticación web
    if ok:
        request.session["logged_in"] = True
        request.session["role"] = "dueño"
        return RedirectResponse(url="/dashboard", status_code=303)
    return RedirectResponse(url="/?error=Credenciales%20inv%C3%A1lidas", status_code=303)


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
                        CREATE TABLE IF NOT EXISTS configuraciones (
                            clave TEXT PRIMARY KEY,
                            valor TEXT
                        )
                    """)
                except Exception:
                    pass
                cur.execute(
                    """
                    INSERT INTO configuraciones (clave, valor)
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
            now = _dt.datetime.now()
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
                # Excluir SOLO "estandar" (sin acento)
                if nombre.strip().lower() == 'estandar':
                    continue
                dist[nombre] = int(r.get('cantidad') or 0)
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