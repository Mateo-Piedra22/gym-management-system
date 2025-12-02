import os
import time
import uuid
import logging
import threading
import psutil
from typing import Callable, Optional
from pathlib import Path
from datetime import datetime, timezone

from fastapi import Request
from fastapi.responses import JSONResponse, RedirectResponse
from starlette.middleware.base import BaseHTTPMiddleware
from fastapi.templating import Jinja2Templates

from apps.webapp.dependencies import get_admin_db, CURRENT_TENANT, DatabaseManager
from apps.webapp.utils import (
    _get_request_host, _extract_tenant_from_host, _get_multi_tenant_mode, 
    _resolve_base_db_params, _resolve_theme_vars, _resolve_logo_url, 
    get_gym_name, _is_tenant_suspended, _get_tenant_suspension_info
)

logger = logging.getLogger(__name__)

templates_dir = Path(__file__).resolve().parent / "templates"
templates = Jinja2Templates(directory=str(templates_dir))

class RequestIDMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next: Callable):
        rid = request.headers.get("X-Request-ID")
        if not rid:
            rid = str(uuid.uuid4())
        try:
            request.state.request_id = rid
        except Exception:
            pass
        response = await call_next(request)
        response.headers["X-Request-ID"] = rid
        return response

class TimingAndCircuitMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next: Callable):
        start_time = time.time()
        path = request.url.path
        
        # Circuit breaker check for overload
        try:
            cpu = psutil.cpu_percent(interval=None)
            mem = psutil.virtual_memory().percent
            if cpu > 95 or mem > 95:
                # Allow health checks or critical admin paths?
                # For now, just log and maybe block heavy paths
                if not path.startswith("/static") and not path.startswith("/favicon"):
                    logger.warning(f"High load: CPU={cpu}% MEM={mem}%")
        except Exception:
            pass

        try:
            response = await call_next(request)
        except Exception as e:
            logger.exception(f"Unhandled exception in middleware for {path}: {e}")
            raise e
            
        process_time = (time.time() - start_time) * 1000
        response.headers["X-Process-Time"] = f"{process_time:.2f}ms"
        return response

class CacheHeadersMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next: Callable):
        response = await call_next(request)
        path = request.url.path
        if path.startswith("/static/") or path.endswith(".css") or path.endswith(".js") or path.endswith(".png") or path.endswith(".jpg"):
            # Cache static assets for 1 day
            response.headers["Cache-Control"] = "public, max-age=86400"
        elif path.startswith("/api/"):
            # No cache for API by default
            if "Cache-Control" not in response.headers:
                response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
                response.headers["Pragma"] = "no-cache"
                response.headers["Expires"] = "0"
        return response

class TenantMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next: Callable):
        sub = None
        token = None
        if _get_multi_tenant_mode():
            host = _get_request_host(request)
            sub = _extract_tenant_from_host(host)
        else:
            try:
                base = _resolve_base_db_params()
                dbn = str((base.get("database") or "")).strip()
            except Exception:
                dbn = ""
            if not sub and dbn:
                adm = get_admin_db()
                if adm is not None:
                    try:
                        with adm.db.get_connection_context() as conn:  # type: ignore
                            cur = conn.cursor()
                            cur.execute("SELECT subdominio FROM gyms WHERE db_name = %s", (dbn,))
                            row = cur.fetchone()
                            if row:
                                sub = str(row[0] or "").strip().lower() or None
                    except Exception:
                        sub = None
        if sub:
            token = CURRENT_TENANT.set(sub)
            try:
                request.state.tenant = sub
            except Exception:
                pass
            try:
                path = str(getattr(request.url, "path", "/"))
                adm = get_admin_db()
                active_maint = False
                maint_msg = None
                maint_until = None
                if adm is not None:
                    try:
                        with adm.db.get_connection_context() as conn:  # type: ignore
                            cur = conn.cursor()
                            cur.execute("SELECT status, suspended_reason, suspended_until FROM gyms WHERE subdominio = %s", (str(sub).strip().lower(),))
                            row = cur.fetchone()
                            if row:
                                st = str((row[0] or "")).lower()
                                maint_msg = row[1]
                                maint_until = row[2]
                                if st == "maintenance":
                                    try:
                                        if maint_until:
                                            dt = maint_until if hasattr(maint_until, "tzinfo") else datetime.fromisoformat(str(maint_until))
                                            now = datetime.utcnow().replace(tzinfo=timezone.utc)
                                            active_maint = bool(dt <= now)
                                        else:
                                            active_maint = True
                                    except Exception:
                                        active_maint = True
                    except Exception:
                        active_maint = False
                if _is_tenant_suspended(sub):
                    info = _get_tenant_suspension_info(sub) or {}
                    allow = path.startswith("/static/") or (path == "/favicon.ico")
                    if not allow:
                        theme_vars = _resolve_theme_vars()
                        ctx = {
                            "request": request,
                            "theme": theme_vars,
                            "gym_name": get_gym_name("Gimnasio"),
                            "logo_url": _resolve_logo_url(),
                            "reason": str(info.get("reason") or ""),
                            "until": str(info.get("until") or ""),
                        }
                        return templates.TemplateResponse("suspension.html", ctx, status_code=403)
                if active_maint:
                    allow = path.startswith("/static/") or (path == "/favicon.ico")
                    if not allow:
                        theme_vars = _resolve_theme_vars()
                        ctx = {
                            "request": request,
                            "theme": theme_vars,
                            "gym_name": get_gym_name("Gimnasio"),
                            "logo_url": _resolve_logo_url(),
                            "message": str(maint_msg or ""),
                            "until": str(maint_until or ""),
                        }
                        return templates.TemplateResponse("maintenance.html", ctx, status_code=503)
            except Exception:
                pass
        try:
            response = await call_next(request)
        finally:
            if token is not None:
                try:
                    CURRENT_TENANT.reset(token)
                except Exception:
                    pass
        return response

class ForceHTTPSProtoMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next: Callable):
        try:
            xfproto = (request.headers.get("x-forwarded-proto") or "").strip().lower()
            if xfproto == "https":
                try:
                    request.scope["scheme"] = "https"
                except Exception:
                    pass
        except Exception:
            pass
        response = await call_next(request)
        return response

class SecurityHeadersMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next: Callable):
        resp = await call_next(request)
        try:
            resp.headers["Strict-Transport-Security"] = "max-age=31536000; includeSubDomains; preload"
            resp.headers["X-Content-Type-Options"] = "nosniff"
            resp.headers["Referrer-Policy"] = "strict-origin-when-cross-origin"
            path = str(getattr(request.url, "path", "/"))
            if path.startswith("/admin"):
                csp = (
                    "default-src 'self' https:; "
                    "img-src 'self' data: https:; "
                    "media-src 'self' https: blob: data:; "
                    "style-src 'self' https: 'unsafe-inline'; "
                    "font-src 'self' https:; "
                    "script-src 'self' https: 'unsafe-inline' 'unsafe-eval'; "
                    "connect-src 'self' https:;"
                )
            else:
                csp = (
                    "default-src 'self' https:; "
                    "img-src 'self' data: https:; "
                    "media-src 'self' https: blob: data:; "
                    "style-src 'self' https: 'unsafe-inline'; "
                    "font-src 'self' https:; "
                    "script-src 'self' https: 'unsafe-inline' 'unsafe-eval'; "
                    "connect-src 'self' https:;"
                )
            resp.headers["Content-Security-Policy"] = csp
        except Exception:
            pass
        return resp

class TenantGuardMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next: Callable):
        try:
            path = request.url.path or "/"
            if path.startswith("/static/") or path == "/favicon.ico":
                return await call_next(request)
            sub = None
            try:
                sub = CURRENT_TENANT.get()
            except Exception:
                sub = None
            if not sub:
                try:
                    p = path or "/"
                except Exception:
                    p = "/"
                try:
                    host = _get_request_host(request)
                except Exception:
                    host = ""
                try:
                    base = (os.getenv("TENANT_BASE_DOMAIN") or "").strip().lower().lstrip(".")
                except Exception:
                    base = ""
                is_base_host = bool(base and (host == base or host == ("www." + base))) or (host in ("localhost", "127.0.0.1"))
                if p == "/" and is_base_host:
                    return RedirectResponse(url="/admin", status_code=303)
                if p.startswith("/admin"):
                    if is_base_host:
                        return await call_next(request)
                    return JSONResponse({"error": "tenant_not_found"}, status_code=404)
                return JSONResponse({"error": "tenant_not_found"}, status_code=404)
            try:
                if _is_tenant_suspended(sub):
                    return JSONResponse({"error": "tenant_suspended"}, status_code=423)
            except Exception:
                pass
        except Exception:
            pass
        resp = await call_next(request)
        return resp

class TenantApiPrefixMiddleware:
    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        try:
            if scope.get("type") == "http":
                path = scope.get("path") or "/"
                if path.startswith("/api/"):
                    try:
                        headers = dict((k.decode('latin1'), v.decode('latin1')) for k, v in (scope.get('headers') or []))
                    except Exception:
                        headers = {}
                    host = (headers.get('host') or headers.get('Host') or '').strip().lower()
                    sub = _extract_tenant_from_host(host) or ''
                    parts = path.split('/')
                    if len(parts) >= 4 and parts[2] and parts[2] == sub:
                        rest = '/' + '/'.join(['api'] + parts[3:])
                        try:
                            scope = dict(scope)
                            scope['path'] = rest
                        except Exception:
                            pass
                        if sub:
                            try:
                                hdrs = list(scope.get('headers') or [])
                                hdrs.append((b'X-Tenant-ID', sub.encode('latin1')))
                                scope['headers'] = hdrs
                            except Exception:
                                pass
        except Exception:
            pass
        return await self.app(scope, receive, send)

class TenantHeaderEnforcerMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next: Callable):
        try:
            p = request.url.path or "/"
            if p.startswith("/api/"):
                expected = None
                try:
                    expected = CURRENT_TENANT.get()
                except Exception:
                    expected = None
                try:
                    provided = request.headers.get('X-Tenant-ID') or request.headers.get('x-tenant-id') or ''
                except Exception:
                    provided = ''
                if not expected:
                    try:
                        host = _get_request_host(request)
                        expected = _extract_tenant_from_host(host) or None
                    except Exception:
                        expected = None
                if expected:
                    if (not provided) or (provided.strip().lower() != str(expected).strip().lower()):
                        return JSONResponse({"error": "invalid_tenant_header"}, status_code=400)
        except Exception:
            pass
        return await call_next(request)
