import os
import shutil
import logging
import subprocess
from pathlib import Path

from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import JSONResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from starlette.middleware.sessions import SessionMiddleware
from starlette.middleware.trustedhost import TrustedHostMiddleware
from starlette.middleware.cors import CORSMiddleware

from apps.webapp.utils import _get_session_secret, _resolve_existing_dir
from apps.webapp.middlewares import (
    RequestIDMiddleware, TimingAndCircuitMiddleware, CacheHeadersMiddleware,
    TenantMiddleware, ForceHTTPSProtoMiddleware, SecurityHeadersMiddleware,
    TenantGuardMiddleware, TenantApiPrefixMiddleware, TenantHeaderEnforcerMiddleware
)
from apps.webapp.routers import auth, users, payments, gym, attendance, whatsapp, admin, public, reports

# Configuración de logging
try:
    from apps.core.logger_config import setup_logging
    setup_logging()
except ImportError:
    logging.basicConfig(level=logging.INFO)

logger = logging.getLogger(__name__)

app = FastAPI(
    title="GymMS WebApp",
    version="2.0",
    root_path=os.getenv("ROOT_PATH", "").strip(),
)

# Middlewares
app.add_middleware(SessionMiddleware, secret_key=_get_session_secret(), https_only=True, same_site="lax")

try:
    _base_dom = (os.getenv("TENANT_BASE_DOMAIN") or "").strip().lower()
    _allow_hosts = [f"*.{_base_dom}", _base_dom, "localhost", "127.0.0.1", "*.vercel.app"] if _base_dom else ["*"]
    app.add_middleware(TrustedHostMiddleware, allowed_hosts=_allow_hosts)
except Exception:
    pass

try:
    _cors_origins = []
    if _base_dom:
        _cors_origins = [f"https://{_base_dom}", f"https://*.{_base_dom}"]
    app.add_middleware(CORSMiddleware, allow_origins=_cors_origins or ["*"], allow_credentials=True, allow_methods=["GET", "POST", "PUT", "DELETE", "OPTIONS"], allow_headers=["*"])
except Exception:
    pass

# Custom middlewares (Order matters: reverse execution)
app.add_middleware(TenantHeaderEnforcerMiddleware)
app.add_middleware(TenantApiPrefixMiddleware)
app.add_middleware(TenantGuardMiddleware)
app.add_middleware(SecurityHeadersMiddleware)
app.add_middleware(ForceHTTPSProtoMiddleware)
app.add_middleware(TenantMiddleware)
app.add_middleware(CacheHeadersMiddleware)
app.add_middleware(TimingAndCircuitMiddleware)
app.add_middleware(RequestIDMiddleware)

# Static Files
static_dir = _resolve_existing_dir("webapp", "static")
if static_dir.exists():
    app.mount("/static", StaticFiles(directory=str(static_dir)), name="static")

# Routers
app.include_router(auth.router)
app.include_router(users.router)
app.include_router(payments.router)
app.include_router(gym.router)
app.include_router(attendance.router)
app.include_router(whatsapp.router)
app.include_router(admin.router)
app.include_router(public.router)
app.include_router(reports.router)

# Exception Handlers
@app.exception_handler(HTTPException)
async def _http_exc_redirect_handler(request: Request, exc: HTTPException):
    try:
        if exc.status_code == 401:
            path = (request.url.path or "")
            if path.startswith("/usuario/"):
                return RedirectResponse(url="/usuario/login", status_code=303)
            if path.startswith("/gestion"):
                return RedirectResponse(url="/gestion/login", status_code=303)
            if path.startswith("/dashboard"):
                return RedirectResponse(url="/login", status_code=303)
    except Exception:
        pass
    return JSONResponse({"detail": exc.detail}, status_code=exc.status_code)

# Startup Event
@app.on_event("startup")
async def _startup_init():
    # Check LibreOffice
    try:
        path = shutil.which("soffice") or shutil.which("soffice.exe")
        if path:
            try:
                res = subprocess.run([path, "--version"], stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
                ver = (res.stdout or "").strip()
            except Exception:
                ver = "(versión no disponible)"
            logging.info(f"LibreOffice disponible: {path} | {ver}")
        else:
            logging.warning("LibreOffice NO encontrado en el sistema")
    except Exception as e:
        logging.exception(f"Error comprobando LibreOffice en startup: {e}")
        
    # Init DB concepts if needed (only in single tenant or if no tenant context needed)
    try:
        from apps.webapp.dependencies import get_pm, get_db
        from apps.webapp.utils import _get_multi_tenant_mode
        
        if not _get_multi_tenant_mode():
             # Trigger DB init via get_db if needed, but get_db is request-scoped usually or lazy global.
             # We can just try to get pm and run ensure concepts
             # We need a context for get_db if it relies on request, but apps/webapp/dependencies.py's get_db
             # relies on global _db in utils or similar? 
             # Actually dependencies.py imports DatabaseManager from core.
             # Let's try to use DatabaseManager directly if available.
             try:
                 from core.database import DatabaseManager
                 if DatabaseManager:
                     # Just a test connection
                     DatabaseManager.test_connection()
             except Exception:
                 pass
                 
             # Ensure payment concepts
             try:
                 pm = get_pm()
                 if pm:
                     pm.asegurar_concepto_cuota_mensual()
             except Exception:
                 pass
    except Exception:
        pass
