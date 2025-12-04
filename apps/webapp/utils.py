import sys
import os
import logging
import threading
import contextvars
from pathlib import Path
from typing import Dict, List, Any, Optional
import urllib.parse
import psycopg2
import psycopg2.extras
from psycopg2 import sql

from fastapi import Request
from fastapi.responses import RedirectResponse, JSONResponse

# Import dependencies
from apps.webapp.dependencies import get_db, get_admin_db, CURRENT_TENANT, DatabaseManager

# Import from sibling modules if available
try:
    from apps.webapp.qss_to_css import read_theme_vars
except ImportError:
    def read_theme_vars(path: Path) -> Dict[str, str]:
        return {}

logger = logging.getLogger(__name__)

# Global cache for tenant DBs
_tenant_dbs: Dict[str, DatabaseManager] = {}
_tenant_lock = threading.RLock()

def _circuit_guard_json(db: Optional[DatabaseManager], endpoint: str = "") -> Optional[JSONResponse]:
    if db is None:
        return JSONResponse({"error": "DB no disponible"}, status_code=503)
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
                    logger.warning(f"{endpoint or '[endpoint]'}: circuito abierto -> 503; state={state}")
                except Exception:
                    pass
                return JSONResponse({
                    "error": "Servicio temporalmente no disponible",
                    "circuit": state,
                }, status_code=503)
    except Exception as e:
        try:
            logger.exception(f"{endpoint or '[endpoint]'}: error comprobando circuito: {e}")
        except Exception:
            pass
    return None

def _compute_base_dir() -> Path:
    """Determina la carpeta base desde la cual resolver recursos."""
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
        # webapp/utils.py está en webapp/, subimos a apps/
        return Path(__file__).resolve().parent.parent
    except Exception:
        return Path('.')

BASE_DIR = _compute_base_dir()

def _resolve_existing_dir(*parts: str) -> Path:
    candidates = []
    try:
        if parts and parts[0] == "webapp":
            try:
                local = Path(__file__).resolve().parent.joinpath(*parts[1:])
                candidates.append(local)
            except Exception:
                pass
    except Exception:
        pass
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
        # Attempt to find repo root
        repo_root = Path(__file__).resolve().parent.parent.parent
        candidates.append(repo_root.joinpath(*parts))
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
    return candidates[0] if candidates else Path(*parts)

# Define static_dir as it is used in theme resolution
static_dir = _resolve_existing_dir("webapp", "static")

def _get_theme_from_db() -> Dict[str, str]:
    out: Dict[str, str] = {}
    db = get_db()
    if db is None:
        return out
    keys = [
        ("theme_primary", "--primary"),
        ("theme_secondary", "--secondary"),
        ("theme_accent", "--accent"),
        ("theme_bg", "--bg"),
        ("theme_card", "--card"),
        ("theme_text", "--text"),
        ("theme_muted", "--muted"),
        ("theme_border", "--border"),
        ("font_base", "--font-base"),
        ("font_heading", "--font-heading"),
    ]
    for cfg_key, css_var in keys:
        try:
            val = db.obtener_configuracion(cfg_key)  # type: ignore
        except Exception:
            val = None
        v = str(val or "").strip()
        if v:
            out[css_var] = v
    return out

def _resolve_theme_vars() -> Dict[str, str]:
    base = read_theme_vars(static_dir / "style.css")
    dbv = _get_theme_from_db()
    return {**base, **dbv}

def _normalize_public_url(url: str) -> str:
    try:
        if not url:
            return url
        
        # If it's a B2/CDN file key (e.g. "some-file.jpg" or "path/to/file.mp4")
        # and NOT a full URL (http/https), we might want to prepend the CDN domain
        # IF we know it's an asset.
        # However, some URLs might be local ("/assets/...") or external.
        
        # Check if it's likely a B2 key (no protocol, no starting slash)
        # But wait, `logo.svg` is local. `assets/logo.svg` is local.
        # B2 keys usually don't start with slash.
        # Let's look for B2 bucket context.
        # If we have a CDN_CUSTOM_DOMAIN env var, we can construct the URL.
        
        cdn = os.getenv("CDN_CUSTOM_DOMAIN", "").strip()
        bucket = os.getenv("B2_BUCKET_NAME", "").strip()
        
        if cdn and bucket and url and not url.startswith("http") and not url.startswith("/"):
            # Assume it's a B2 key if it doesn't look like a local path
            # But "assets/logo.png" could be local.
            # Usually B2 keys for gyms might be "subdomain-assets/logo.png"
            # Let's be conservative. If it contains "assets/" and NOT "/assets/", it might be B2?
            # Or if we have a way to know it came from B2.
            # For now, let's just normalize existing full URLs or leave as is.
            pass

        # If it IS a B2 URL (e.g. f005.backblazeb2.com...), replace with CDN
        if cdn and "backblazeb2.com" in url:
            # Replace B2 domain with CDN domain
            # Pattern: https://f005.backblazeb2.com/file/<bucket>/<key>
            # Target: https://<cdn>/file/<bucket>/<key>
            # Or if using custom domain mapping directly to bucket: https://<cdn>/<key>
            # Based on user instruction: https://cdn.gymms-motiona.xyz/file/motiona-assets/<key>
            
            # Simple replacement of hostname if structure matches
            try:
                parsed = urllib.parse.urlparse(url)
                if "backblazeb2.com" in parsed.netloc:
                    return url.replace(parsed.netloc, cdn)
            except Exception:
                pass
                
        return url
    except Exception:
        return url

def _resolve_logo_url() -> str:
    # Primero intentar obtener URL desde gym_config; luego desde configuracion
    try:
        db = get_db()
        if db is not None:
            # Prioridad: gym_config
            if hasattr(db, 'obtener_configuracion_gimnasio'):
                try:
                    cfg = db.obtener_configuracion_gimnasio()  # type: ignore
                except Exception:
                    cfg = {}
                if isinstance(cfg, dict):
                    # En gym_config la clave se almacena como 'logo_url'
                    u1 = str(cfg.get('logo_url') or '').strip()
                    if u1:
                        return _normalize_public_url(u1)
            # Fallback: tabla configuracion
            if hasattr(db, 'obtener_configuracion'):
                try:
                    url = db.obtener_configuracion('gym_logo_url')  # type: ignore
                except Exception:
                    url = None
                if isinstance(url, str) and url.strip():
                    return _normalize_public_url(url.strip())
    except Exception:
        pass
    # Fallback a assets locales
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

def get_gym_name(default: str = "Gimnasio") -> str:
    # Wrapper around DB config
    try:
        db = get_db()
        if db and hasattr(db, 'obtener_configuracion'):
             n = db.obtener_configuracion('gym_name')
             if n: return str(n)
    except Exception:
        pass
    return default

def _get_password() -> str:
    # Leer directamente desde la base de datos para sincronización inmediata
    try:
        db = get_db()
        if db and hasattr(db, 'obtener_configuracion'):
            pwd = db.obtener_configuracion('owner_password', timeout_ms=700)  # type: ignore
            if isinstance(pwd, str) and pwd.strip():
                return pwd.strip()
    except Exception:
        pass
    # Fallback: leer desde Admin DB por subdominio
    try:
        tenant = None
        try:
            tenant = CURRENT_TENANT.get()
        except Exception:
            tenant = None
        if tenant:
            adm = get_admin_db()
            if adm is not None:
                with adm.db.get_connection_context() as conn:  # type: ignore
                    cur = conn.cursor()
                    cur.execute("SELECT owner_password_hash FROM gyms WHERE subdominio = %s", (str(tenant).strip().lower(),))
                    row = cur.fetchone()
                    if row and isinstance(row[0], str) and row[0].strip():
                        return row[0].strip()
    except Exception:
        pass
    # Fallback: variables de entorno
    try:
        env_pwd = (os.getenv("WEBAPP_OWNER_PASSWORD", "") or os.getenv("OWNER_PASSWORD", "")).strip()
    except Exception:
        env_pwd = ""
    if env_pwd:
        return env_pwd
    return "admin"

def _verify_owner_password(password: str) -> bool:
    """Verifica la contraseña del dueño usando bcrypt con fallback a texto plano."""
    try:
        # Importar aquí para evitar dependencias circulares
        from apps.core.security_utils import SecurityUtils
        
        # Obtener la contraseña almacenada (puede ser hash o texto plano)
        stored_password = _get_password()
        
        # Intentar verificar con bcrypt primero
        if SecurityUtils.verify_password(password, stored_password):
            return True
            
        # Fallback: comparación directa para contraseñas antiguas en texto plano
        if password == stored_password:
            return True
            
        return False
    except Exception:
        # Fallback final: comparación directa
        try:
            return password == _get_password()
        except Exception:
            return False

def _filter_existing_columns(conn, schema: str, table: str, data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Filtra las claves del diccionario 'data' preservando solo aquellas que
    coinciden con columnas existentes en la tabla indicada.
    """
    try:
        cur = conn.cursor()
        # Consulta segura de columnas en information_schema
        cur.execute(
            """
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_schema = %s AND table_name = %s
            """,
            (schema, table)
        )
        rows = cur.fetchall() or []
        valid_cols = {r[0] for r in rows}
        return {k: v for k, v in data.items() if k in valid_cols}
    except Exception:
        return {}

def _apply_change_idempotent(conn, schema: str, table: str, operation: str, keys: Dict[str, Any], data: Dict[str, Any], where: List = None) -> bool:
    try:
        cur = conn.cursor()
        if operation.upper() == "UPDATE":
            filtered_data = _filter_existing_columns(conn, schema, table, data)
            if not filtered_data:
                return False
            
            set_clause = sql.SQL(", ").join([
                sql.SQL("{} = {}").format(sql.Identifier(k), sql.Placeholder(k))
                for k in filtered_data.keys()
            ])
            
            where_clause = sql.SQL(" AND ").join([
                sql.SQL("{} = {}").format(sql.Identifier(k), sql.Placeholder(f"w_{k}"))
                for k in keys.keys()
            ])
            
            query = sql.SQL("UPDATE {}.{} SET {} WHERE {}").format(
                sql.Identifier(schema),
                sql.Identifier(table),
                set_clause,
                where_clause
            )
            
            params = filtered_data.copy()
            for k, v in keys.items():
                params[f"w_{k}"] = v
                
            cur.execute(query, params)
            return cur.rowcount > 0

        elif operation.upper() == "DELETE":
            where_clause = sql.SQL(" AND ").join([
                sql.SQL("{} = {}").format(sql.Identifier(k), sql.Placeholder(k))
                for k in keys.keys()
            ])
            query = sql.SQL("DELETE FROM {}.{} WHERE {}").format(
                sql.Identifier(schema),
                sql.Identifier(table),
                where_clause
            )
            cur.execute(query, keys)
            return cur.rowcount > 0
            
        return False
    except Exception as e:
        logger.error(f"_apply_change_idempotent error: {e}")
        return False

# --- Multi-tenant Helpers ---

def _get_multi_tenant_mode() -> bool:
    try:
        v = os.getenv("MULTI_TENANT_MODE", "false").strip().lower()
        return v in ("1", "true", "yes", "on")
    except Exception:
        return False

def _get_request_host(request: Request) -> str:
    try:
        h = request.headers.get("x-forwarded-host") or request.headers.get("host") or ""
        h = h.strip()
        if h:
            return h.split(":")[0].strip().lower()
        try:
            return (request.url.hostname or "").strip().lower()
        except Exception:
            return ""
    except Exception:
        return ""

def _extract_tenant_from_host(host: str) -> Optional[str]:
    try:
        base = os.getenv("TENANT_BASE_DOMAIN", "gymms-motiona.xyz").strip().lower().lstrip(".")
    except Exception:
        base = "gymms-motiona.xyz"
    h = (host or "").strip().lower()
    if not h:
        return None
    if "localhost" in h or h.endswith(".localhost"):
        return None
    def _extract_with_base(hh: str, bb: str) -> Optional[str]:
        if not bb or not hh.endswith(bb):
            return None
        try:
            pref = hh[: max(0, len(hh) - len(bb))].rstrip(".")
        except Exception:
            pref = ""
        if not pref:
            return None
        try:
            s = pref.split(".")[0].strip()
        except Exception:
            s = pref
        if not s:
            return None
        try:
            if s.lower() == "www":
                return None
        except Exception:
            pass
        return s
    sub = _extract_with_base(h, base)
    if sub:
        return sub
    try:
        v = (os.getenv("VERCEL_URL") or os.getenv("VERCEL_BRANCH_URL") or os.getenv("VERCEL_PROJECT_PRODUCTION_URL") or "").strip()
        if v:
            import urllib.parse as _up
            try:
                u = _up.urlparse(v if (v.startswith("http://") or v.startswith("https://")) else ("https://" + v))
                vb = (u.hostname or "").strip().lower()
            except Exception:
                vb = v.split("/")[0].strip().lower()
            if vb:
                sub = _extract_with_base(h, vb)
                if sub:
                    return sub
    except Exception:
        pass
    return None

def _get_tenant_from_request(request: Request) -> Optional[str]:
    """Helper para extraer el tenant directamente del request."""
    host = _get_request_host(request)
    return _extract_tenant_from_host(host)

def _resolve_base_db_params() -> Dict[str, Any]:
    host = os.getenv("DB_HOST", "localhost").strip()
    try:
        port = int(os.getenv("DB_PORT", 5432))
    except Exception:
        port = 5432
    user = os.getenv("DB_USER", "postgres").strip()
    password = os.getenv("DB_PASSWORD", "")
    sslmode = os.getenv("DB_SSLMODE", "prefer").strip()
    try:
        connect_timeout = int(os.getenv("DB_CONNECT_TIMEOUT", 10))
    except Exception:
        connect_timeout = 10
    application_name = os.getenv("DB_APPLICATION_NAME", "gym_management_system").strip()
    database = os.getenv("DB_NAME", "gimnasio").strip()
    try:
        h = host.lower()
        if ("neon.tech" in h) or ("neon" in h):
            if not sslmode or sslmode.lower() in ("disable", "prefer"):
                sslmode = "require"
    except Exception:
        pass
    params: Dict[str, Any] = {
        "host": host,
        "port": port,
        "database": database,
        "user": user,
        "password": password,
        "sslmode": sslmode,
        "connect_timeout": connect_timeout,
        "application_name": application_name,
    }
    return params

def _get_db_for_tenant(tenant: str) -> Optional[DatabaseManager]:
    t = (tenant or "").strip().lower()
    if not t:
        return None
    with _tenant_lock:
        dm = _tenant_dbs.get(t)
        if dm is not None:
            return dm
        base = _resolve_base_db_params()
        db_name = None
        adm = get_admin_db()
        if adm is not None:
            try:
                with adm.db.get_connection_context() as conn:  # type: ignore
                    cur = conn.cursor()
                    cur.execute("SELECT db_name FROM gyms WHERE subdominio = %s", (t,))
                    row = cur.fetchone()
                    if row:
                        db_name = str(row[0] or "").strip()
            except Exception:
                db_name = None
        if not db_name:
            return None
        base["database"] = db_name
        try:
            dm = DatabaseManager(connection_params=base)  # type: ignore
        except Exception:
            return None
        try:
            with dm.get_connection_context() as conn:  # type: ignore
                cur = conn.cursor()
                cur.execute("SELECT 1")
                _ = cur.fetchone()
        except Exception:
            return None
        _tenant_dbs[t] = dm
        return dm

def _is_tenant_suspended(tenant: str) -> bool:
    adm = get_admin_db()
    if adm is None:
        return False
    try:
        return bool(adm.is_gym_suspended(tenant))  # type: ignore
    except Exception:
        return False

def _get_tenant_suspension_info(tenant: str) -> Optional[Dict[str, Any]]:
    adm = get_admin_db()
    if adm is None:
        return None
    try:
        with adm.db.get_connection_context() as conn:  # type: ignore
            cur = conn.cursor()
            cur.execute(
                "SELECT hard_suspend, suspended_until, suspended_reason FROM gyms WHERE subdominio = %s",
                (tenant.strip().lower(),),
            )
            row = cur.fetchone()
            if not row:
                return None
            hard, until, reason = row[0], row[1], row[2]
            try:
                u = until.isoformat() if hasattr(until, "isoformat") and until else (str(until or ""))
            except Exception:
                u = str(until or "")
            return {"hard": bool(hard), "until": u, "reason": str(reason or "")}
    except Exception:
        return None

def _get_session_secret() -> str:
    # 1) Prefer an explicit, stable secret via environment
    try:
        env = os.getenv("WEBAPP_SECRET_KEY", "").strip()
        if env:
            return env
    except Exception:
        pass

    # 2) In serverless environments (e.g., Vercel) the filesystem is ephemeral.
    try:
        if os.getenv("VERCEL") or os.getenv("VERCEL_ENV"):
            base = (os.getenv("WHATSAPP_APP_SECRET", "") + "|" + os.getenv("WHATSAPP_VERIFY_TOKEN", "")).strip()
            if base:
                import hashlib
                return hashlib.sha256(base.encode("utf-8")).hexdigest()
    except Exception:
        pass
    
    # 3) Non-serverless: try to read/persist in config/config.json for stability
    try:
        # Try to find config path
        base_dir = _resolve_existing_dir("config")
        cfg_path = base_dir / "config.json"
        
        cfg = {}
        if cfg_path.exists():
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
                if not base_dir.exists():
                    os.makedirs(base_dir, exist_ok=True)
                with open(cfg_path, "w", encoding="utf-8") as f:
                    _json.dump(cfg, f, ensure_ascii=False, indent=2)
            except Exception:
                pass
        return secret
    except Exception:
        pass
    
    # 4) Absolute last resort: ephemeral secret
    import secrets as _secrets
    return _secrets.token_urlsafe(32)
