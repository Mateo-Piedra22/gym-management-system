import os
import logging
from typing import Optional, List, Dict, Any

try:
    from core.logger_config import setup_logging
    setup_logging()
except ImportError:
    logging.basicConfig(level=logging.INFO)

logger = logging.getLogger(__name__)

from fastapi import FastAPI, Request, HTTPException, Form, BackgroundTasks
from fastapi.responses import JSONResponse, Response, RedirectResponse
from fastapi.templating import Jinja2Templates
from pathlib import Path
from starlette.middleware.sessions import SessionMiddleware

try:
    import requests
except ImportError:
    logger.warning("No se pudo importar requests", exc_info=True)
    requests = None

try:
    from requests.adapters import HTTPAdapter
    from urllib3.util.retry import Retry
except ImportError:
    logger.warning("No se pudo importar HTTPAdapter/Retry", exc_info=True)
    HTTPAdapter = None
    Retry = None

from datetime import datetime
from core.secure_config import SecureConfig
from core.database.raw_manager import RawPostgresManager
from core.services.admin_service import AdminService

try:
    import psycopg2
    import psycopg2.extras
except ImportError:
    logger.warning("No se pudo importar psycopg2", exc_info=True)
    psycopg2 = None


from fastapi.staticfiles import StaticFiles
from starlette.middleware.cors import CORSMiddleware

admin_app = FastAPI(title="GymMS Admin", version="1.0")
_cookie_domain = os.getenv("SESSION_COOKIE_DOMAIN", "").strip() or None
_cookie_secure = (os.getenv("SESSION_COOKIE_SECURE", "1").strip().lower() in ("1", "true", "yes"))
_cookie_samesite = (os.getenv("SESSION_COOKIE_SAMESITE", "lax").strip().lower() or "lax")
admin_app.add_middleware(SessionMiddleware, secret_key=os.getenv("ADMIN_SESSION_SECRET", "admin-session"), domain=_cookie_domain, https_only=_cookie_secure, same_site=_cookie_samesite)

try:
    # CORS for Admin
    admin_app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"], # Allow all for admin panel simplicity or restrict if needed
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )
except Exception:
    pass

try:
    # Try to mount static files from webapp/static for shared styles
    # Adjust path relative to apps/admin/main.py -> ../webapp/static
    static_path = Path(__file__).parent.parent / "webapp" / "static"
    if static_path.exists():
        admin_app.mount("/static", StaticFiles(directory=str(static_path)), name="static")
except Exception:
    pass

try:
    setattr(admin_app.state, "session_version", int(getattr(admin_app.state, "session_version", 1)))
    setattr(admin_app.state, "rate_limits", dict(getattr(admin_app.state, "rate_limits", {})))
except Exception:
    logger.warning("Error inicializando estado de sesión admin", exc_info=True)

templates = Jinja2Templates(directory=str(Path(__file__).parent / "templates"))

_admin_service: Optional[AdminService] = None

def _resolve_admin_db_params() -> Dict[str, Any]:
    host = os.getenv("ADMIN_DB_HOST", "").strip()
    try:
        port = int(os.getenv("ADMIN_DB_PORT", "5432"))
    except Exception:
        port = 5432
    user = os.getenv("ADMIN_DB_USER", "").strip()
    password = os.getenv("ADMIN_DB_PASSWORD", "")
    sslmode = os.getenv("ADMIN_DB_SSLMODE", "require").strip()
    try:
        connect_timeout = int(os.getenv("ADMIN_DB_CONNECT_TIMEOUT", "4"))
    except Exception:
        connect_timeout = 4
    application_name = os.getenv("ADMIN_DB_APPLICATION_NAME", "gym_management_admin").strip()
    database = os.getenv("ADMIN_DB_NAME", "gymms_admin").strip()

    try:
        h = host.lower()
        if ("neon.tech" in h) or ("neon" in h):
            if not sslmode or sslmode.lower() in ("disable", "prefer"):
                sslmode = "require"
    except Exception:
        pass
    return {
        "host": host or "localhost",
        "port": port,
        "database": database or "gymms_admin",
        "user": user or "postgres",
        "password": password,
        "sslmode": sslmode or "require",
        "connect_timeout": connect_timeout,
        "application_name": application_name or "gym_management_admin",
    }

def _get_admin_service() -> Optional[AdminService]:
    global _admin_service
    if _admin_service is not None:
        return _admin_service
    try:
        params = _resolve_admin_db_params()
        db = RawPostgresManager(connection_params=params)
        # Initialize if needed
        try:
             # Try to connect to ensure DB exists, if not it might need bootstrap which is handled inside AdminService implicitly or explicitly
             # But DatabaseManager wraps connection.
             pass
        except Exception:
             pass
             
        _admin_service = AdminService(db)
        return _admin_service
    except Exception:
        logger.error("Error obteniendo admin service", exc_info=True)
        return None

def _is_logged_in(request: Request) -> bool:
    try:
        return bool(request.session.get("admin_logged_in"))
    except Exception:
        logger.warning("Error verificando login admin", exc_info=True)
        return False

def _require_admin(request: Request) -> None:
    if _is_logged_in(request):
        return
    secret = os.getenv("ADMIN_SECRET", "").strip()
    if secret:
        hdr = request.headers.get("x-admin-secret") or ""
        if hdr.strip() == secret:
            return
    raise HTTPException(status_code=401, detail="Acceso restringido")

def _get_http_session():
    if requests is None:
        return None
    try:
        s = getattr(admin_app.state, "http_session", None)
    except Exception:
        logger.warning("Error obteniendo http session", exc_info=True)
        s = None
    if s is not None:
        return s
    try:
        sess = requests.Session()
        if HTTPAdapter is not None and Retry is not None:
            retry = Retry(total=2, connect=2, read=2, backoff_factor=0.3, status_forcelist=[429, 500, 502, 503, 504], allowed_methods=["GET", "POST"])  # type: ignore
            adapter = HTTPAdapter(max_retries=retry)  # type: ignore
            sess.mount("https://", adapter)
            sess.mount("http://", adapter)
        setattr(admin_app.state, "http_session", sess)
        return sess
    except Exception:
        return None

@admin_app.get("/login")
async def admin_login_form(request: Request):
    try:
        if _is_logged_in(request):
            return RedirectResponse(url="/admin", status_code=303)
    except Exception:
        pass
    return templates.TemplateResponse("login.html", {"request": request, "hide_sidebar": True})

@admin_app.post("/login")
async def admin_login(request: Request):
    try:
        adm = _get_admin_service()
        if adm is None:
            try:
                provided = ""
                try:
                    form = await request.form()
                    provided = str((form.get("password") or "").strip())
                except Exception:
                    pass
                if not provided:
                    try:
                        import urllib.parse as _u
                        body = (await request.body()) or b""
                        qs = _u.parse_qs(body.decode("utf-8"))
                        provided = str((qs.get("password", [""])[0]) or "").strip()
                    except Exception:
                        pass
                if not provided:
                    try:
                        js = await request.json()
                        provided = str((js.get("password") or "").strip())
                    except Exception:
                        pass
            except Exception:
                provided = ""
            secret = os.getenv("ADMIN_SECRET", "").strip()
            if secret and provided and provided == secret:
                try:
                    request.session["admin_logged_in"] = True
                    try:
                        request.session["session_version"] = int(getattr(admin_app.state, "session_version", 1))
                    except Exception:
                        request.session["session_version"] = 1
                except Exception:
                    pass
                try:
                    adm2 = _get_admin_service()
                    if adm2:
                        adm2.log_action("owner", "login_without_db_bootstrap", None, None)
                except Exception:
                    pass
                is_hx = (str(request.headers.get("hx-request") or "").lower() == "true")
                if not is_hx:
                    return RedirectResponse(url="/admin", status_code=303)
                return JSONResponse({"ok": True}, status_code=200)
            is_hx = (str(request.headers.get("hx-request") or "").lower() == "true")
            if not is_hx:
                return RedirectResponse(url="/admin/login", status_code=303)
            return JSONResponse({"ok": False, "error": "db_unavailable"}, status_code=503)
        try:
            form = None
            try:
                form = await request.form()
            except Exception:
                form = None
            pwd = ""
            try:
                if form:
                    pwd = str((form.get("password") or "").strip())
            except Exception:
                pwd = ""
            if not pwd:
                try:
                    import urllib.parse as _u
                    body = (await request.body()) or b""
                    qs = _u.parse_qs(body.decode("utf-8"))
                    pwd = str((qs.get("password", [""])[0]) or "").strip()
                except Exception:
                    pwd = ""
            if not pwd:
                try:
                    js = await request.json()
                    pwd = str((js.get("password") or "").strip())
                except Exception:
                    pass
        except Exception:
            pwd = ""
        try:
            adm._ensure_owner_user()
        except Exception:
            pass
        ok = adm.verificar_owner_password(pwd)
        if not ok:
            try:
                candidate1 = (os.getenv("ADMIN_INITIAL_PASSWORD", "").strip())
            except Exception:
                candidate1 = ""
            try:
                candidate2 = (os.getenv("ADMIN_SECRET", "").strip())
            except Exception:
                candidate2 = ""
            try:
                candidate3 = (os.getenv("DEV_PASSWORD", "").strip())
            except Exception:
                candidate3 = ""
            try:
                provided = (pwd or "").strip()
            except Exception:
                provided = pwd
            if provided and (provided == candidate1 or provided == candidate2 or provided == candidate3):
                try:
                    adm._ensure_owner_user()
                except Exception:
                    pass
                ok2 = adm.set_admin_owner_password(provided)
                if ok2:
                    ok = True
                else:
                    try:
                        request.session["admin_logged_in"] = True
                        try:
                            request.session["session_version"] = int(getattr(admin_app.state, "session_version", 1))
                        except Exception:
                            request.session["session_version"] = 1
                    except Exception:
                        pass
                    try:
                        adm.log_action("owner", "login_via_secret_password", None, None)
                    except Exception:
                        pass
                    is_hx = (str(request.headers.get("hx-request") or "").lower() == "true")
                    if not is_hx:
                        return RedirectResponse(url="/admin", status_code=303)
                    return JSONResponse({"ok": True}, status_code=200)
        if not ok:
            is_hx = (str(request.headers.get("hx-request") or "").lower() == "true")
            if not is_hx:
                return RedirectResponse(url="/admin/login?error=Credenciales", status_code=303)
            return JSONResponse({"ok": False}, status_code=401)
        try:
            request.session["admin_logged_in"] = True
            try:
                request.session["session_version"] = int(getattr(admin_app.state, "session_version", 1))
            except Exception:
                request.session["session_version"] = 1
        except Exception:
            pass
        try:
            adm.log_action("owner", "login", None, None)
        except Exception:
            pass
        is_hx = (str(request.headers.get("hx-request") or "").lower() == "true")
        if not is_hx:
            return RedirectResponse(url="/admin", status_code=303)
        return JSONResponse({"ok": True}, status_code=200)
    except Exception:
        is_hx = (str(request.headers.get("hx-request") or "").lower() == "true")
        if not is_hx:
            return RedirectResponse(url="/admin/login?error=Error interno", status_code=303)
        return JSONResponse({"ok": False, "error": "internal"}, status_code=500)

@admin_app.post("/logout")
async def admin_logout(request: Request):
    try:
        request.session.clear()
    except Exception:
        pass
    try:
        adm = _get_admin_service()
        if adm:
            adm.log_action("owner", "logout", None, None)
    except Exception:
        pass
    return JSONResponse({"ok": True}, status_code=200)

@admin_app.get("/")
async def admin_home(request: Request):
    acc = (request.headers.get("accept") or "").lower()
    wants_html = ("text/html" in acc) or (request.query_params.get("ui") == "1")
    try:
        logged = _is_logged_in(request)
    except Exception:
        logged = False
    if not logged:
        try:
            if wants_html:
                return templates.TemplateResponse("login.html", {"request": request, "hide_sidebar": True})
            # If API request to /admin without auth, redirect to login for consistency if browser, else 401
            if "text/html" in acc:
                 return RedirectResponse(url="/login", status_code=303)
            return JSONResponse({"ok": False, "error": "unauthorized"}, status_code=401)
        except Exception:
            if wants_html:
                return RedirectResponse(url="/login", status_code=303)
            return JSONResponse({"ok": False, "error": "unauthorized"}, status_code=401)
    warnings: List[str] = []
    try:
        adm = _get_admin_service()
        if adm is not None:
            ws = adm.obtener_warnings_admin()
            if ws:
                warnings = list(ws)
    except Exception:
        warnings = []
    try:
        adm = _get_admin_service()
        metrics = adm.obtener_metricas_agregadas() if adm else {"gyms": {"total": 0, "active": 0, "suspended": 0, "maintenance": 0, "last_7": 0, "last_30": 0, "series_30": []}, "whatsapp": {"configured": 0}, "storage": {"configured": 0}, "subscriptions": {"active": 0, "overdue": 0}, "payments": {"last_30_sum": 0.0}}
        series = list((metrics.get("gyms") or {}).get("series_30") or [])
        series_max = max([int(it.get("count") or 0) for it in series] or [1])
        upcoming = adm.listar_proximos_vencimientos(14) if adm else []
        recent_payload = adm.listar_gimnasios_con_resumen(1, 8, None, None, "created_at", "DESC") if adm else {"items": []}
        recent_gyms = list((recent_payload or {}).get("items") or [])
        recent_payments = adm.listar_pagos_recientes(10) if adm else []
        audit = adm.resumen_auditoria(7) if adm else {"by_action": [], "by_actor": [], "days": 7}
        return templates.TemplateResponse(
            "home.html",
            {
                "request": request,
                "warnings": warnings,
                "metrics": metrics,
                "series_max": int(series_max),
                "upcoming": upcoming,
                "recent_gyms": recent_gyms,
                "recent_payments": recent_payments,
                "audit": audit,
            },
        )
    except Exception:
        if wants_html:
            return Response(content="<div class=\"p-6 text-red-300\">Error interno del panel</div>", media_type="text/html", status_code=500)
        return JSONResponse({"ok": False, "error": "internal_home_render"}, status_code=500)

@admin_app.get("/admin")
async def admin_home_alias(request: Request):
    return RedirectResponse(url="/", status_code=303)

def _lim_key(request: Request, bucket: str) -> str:
    try:
        trust_proxy = str(os.getenv("PROXY_HEADERS_ENABLED", "0")).strip().lower() in ("1", "true", "yes", "on")
        if trust_proxy:
            xff = request.headers.get("x-forwarded-for") or ""
            if xff:
                try:
                    ip = xff.split(",")[0].strip()
                except Exception:
                    ip = xff.strip()
            else:
                xri = request.headers.get("x-real-ip") or ""
                if xri:
                    ip = xri.strip()
                else:
                    ip = (request.client.host if request.client else "-")
        else:
            ip = (request.client.host if request.client else "-")
    except Exception:
        ip = "-"
    try:
        path = request.url.path
    except Exception:
        path = "-"
    return f"{bucket}:{ip}:{path}"

def _check_rate_limit(request: Request, bucket: str, limit: int = 30, window_seconds: int = 60):
    try:
        acc = (request.headers.get("accept") or "").lower()
    except Exception:
        acc = ""
    try:
        wants_html = ("text/html" in acc) or (request.query_params.get("ui") == "1")
    except Exception:
        wants_html = False
    if wants_html:
        return None
    k = _lim_key(request, bucket)
    try:
        now = int(datetime.utcnow().timestamp())
    except Exception:
        now = 0
    w = int(window_seconds)
    store = getattr(admin_app.state, "rate_limits", {})
    rec = store.get(k)
    if not rec or int(rec.get("start") or 0) + w <= now:
        store[k] = {"count": 1, "start": now}
        setattr(admin_app.state, "rate_limits", store)
        return None
    try:
        c = int(rec.get("count") or 0) + 1
    except Exception:
        c = (rec.get("count") or 0) + 1
    rec["count"] = c
    store[k] = rec
    setattr(admin_app.state, "rate_limits", store)
    try:
        lim = int(limit)
    except Exception:
        lim = limit
    if c > lim:
        return JSONResponse({"error": "rate_limited"}, status_code=429)
    return None

def _guard_html_login_redirect(request: Request):
    try:
        if not _is_logged_in(request):
            acc = (request.headers.get("accept") or "").lower()
            wants_html = ("text/html" in acc) or (request.query_params.get("ui") == "1")
            if wants_html:
                return RedirectResponse(url="/admin/login", status_code=303)
    except Exception:
        pass
    return None

@admin_app.get("/gyms")
async def listar_gimnasios(request: Request):
    gr = _guard_html_login_redirect(request)
    if gr:
        return gr
    rl = _check_rate_limit(request, "gyms_list", 120, 60)
    if rl:
        return rl
    adm = _get_admin_service()
    if adm is None:
        return JSONResponse({"error": "DB admin no disponible"}, status_code=500)
    try:
        page = int(request.query_params.get("page") or 1)
    except Exception:
        page = 1
    try:
        page_size = int(request.query_params.get("page_size") or 20)
    except Exception:
        page_size = 20
    q = (request.query_params.get("q") or "").strip()
    status_q = (request.query_params.get("status") or "").strip()
    order_by = (request.query_params.get("order_by") or "id").strip()
    order_dir = (request.query_params.get("order_dir") or "DESC").strip()
    view = (request.query_params.get("view") or "cards").strip()
    payload = adm.listar_gimnasios_avanzado(page, page_size, q or None, status_q or None, order_by or None, order_dir or None)
    accept = (request.headers.get("accept") or "").lower()
    wants_html = ("text/html" in accept) or (request.query_params.get("ui") == "1") or (str(request.headers.get("hx-request") or "").lower() == "true")
    if not wants_html:
        return JSONResponse(payload, status_code=200)
    try:
        payload_grid = adm.listar_gimnasios_con_resumen(page, page_size, q or None, status_q or None, order_by or None, order_dir or None)
    except Exception:
        payload_grid = payload
    items: List[Dict[str, Any]] = list((payload_grid or {}).get("items") or [])
    try:
        base_url = SecureConfig.get_webapp_base_url()
    except Exception:
        base_url = ""
    try:
        dom = os.getenv("TENANT_BASE_DOMAIN", "").strip().lstrip(".")
    except Exception:
        dom = ""
    try:
        for g in items:
            try:
                subdom = str((g or {}).get("subdominio") or "").strip().lower()
            except Exception:
                subdom = ""
            preview_url = ""
            try:
                if dom and subdom:
                    preview_url = f"https://{subdom}.{dom}"
                elif base_url:
                    preview_url = base_url
            except Exception:
                preview_url = base_url or ""
            g["webapp_url"] = preview_url
    except Exception:
        pass
    total = int((payload or {}).get("total") or 0)
    p = int((payload or {}).get("page") or page)
    ps = int((payload or {}).get("page_size") or page_size)
    prev_link = f"/admin/gyms?ui=1&page={max(p-1,1)}&page_size={ps}&q={q}&status={status_q}&order_by={order_by}&order_dir={order_dir}&view={view}"
    next_link = f"/admin/gyms?ui=1&page={p+1}&page_size={ps}&q={q}&status={status_q}&order_by={order_by}&order_dir={order_dir}&view={view}"
    last_page = max((total + ps - 1) // ps, 1)
    if p >= last_page:
        next_link = prev_link
    return templates.TemplateResponse(
        "gyms.html",
        {
            "request": request,
            "items": items,
            "q": q,
            "status_q": status_q,
            "page_size": ps,
            "page": p,
            "prev_link": prev_link,
            "next_link": next_link,
            "last_page": last_page,
            "total": total,
            "order_by": order_by,
            "order_dir": order_dir,
            "view": view,
        },
    )

@admin_app.post("/gyms")
async def crear_gimnasio(request: Request, background_tasks: BackgroundTasks, nombre: str = Form(...), subdominio: Optional[str] = Form(None), owner_phone: Optional[str] = Form(None), whatsapp_phone_id: Optional[str] = Form(None), whatsapp_access_token: Optional[str] = Form(None), whatsapp_business_account_id: Optional[str] = Form(None), whatsapp_verify_token: Optional[str] = Form(None), whatsapp_app_secret: Optional[str] = Form(None), whatsapp_nonblocking: Optional[bool] = Form(False), whatsapp_send_timeout_seconds: Optional[str] = Form(None)):
    _require_admin(request)
    rl = _check_rate_limit(request, "gym_create", 20, 60)
    if rl:
        return rl
    adm = _get_admin_service()
    if adm is None:
        return JSONResponse({"error": "DB admin no disponible"}, status_code=500)
    sd_in = str(subdominio or "").strip().lower()
    if not sd_in:
        try:
            sd_in = adm.sugerir_subdominio_unico(nombre)
        except Exception:
            sd_in = ""
    if not sd_in:
        return JSONResponse({"ok": False, "error": "invalid_subdomain"}, status_code=400)
    try:
        av = bool(adm.subdominio_disponible(sd_in))
    except Exception:
        av = False
    if not av:
        sug = sd_in
        try:
            sug = adm.sugerir_subdominio_unico(nombre)
        except Exception:
            pass
        return JSONResponse({"ok": False, "error": "subdominio_in_use", "suggestion": sug}, status_code=400)
    wsts = None
    try:
        raw = (whatsapp_send_timeout_seconds or "").strip()
        if raw:
            try:
                wsts = float(raw)
            except Exception:
                wsts = None
    except Exception:
        wsts = None
    res = adm.crear_gimnasio(nombre, sd_in, whatsapp_phone_id, whatsapp_access_token, owner_phone, whatsapp_business_account_id, whatsapp_verify_token, whatsapp_app_secret, whatsapp_nonblocking, wsts)
    try:
        adm.log_action("owner", "create_gym", res.get("id") if isinstance(res, dict) else None, f"{nombre}|{sd_in}")
    except Exception:
        pass
    if "error" in res:
        return JSONResponse(res, status_code=400)
    gid = int(res.get("id")) if isinstance(res, dict) else None
    if gid:
        try:
            adm._push_whatsapp_to_gym_db(int(gid))
        except Exception:
            pass
    out = dict(res)
    out["ok"] = True
    return JSONResponse(out, status_code=201)

@admin_app.get("/subdomains/check")
async def check_subdomain(request: Request, sub: Optional[str] = None, name: Optional[str] = None):
    _require_admin(request)
    adm = _get_admin_service()
    if adm is None:
        return JSONResponse({"error": "DB admin no disponible"}, status_code=500)
    s = str(sub or "").strip().lower()
    if not s and name:
        try:
            s = adm.sugerir_subdominio_unico(str(name))
        except Exception:
            s = ""
    if not s:
        return JSONResponse({"ok": False, "available": False, "suggestion": ""}, status_code=200)
    try:
        av = bool(adm.subdominio_disponible(s))
    except Exception:
        av = False
    sug = s
    if not av:
        try:
            sug = adm.sugerir_subdominio_unico(str(name or s))
        except Exception:
            sug = s
    accept = (request.headers.get("accept") or "").lower()
    wants_html = ("text/html" in accept) or (request.query_params.get("ui") == "1")
    if wants_html:
        return templates.TemplateResponse(
            "subdomain-status.html",
            {"request": request, "available": bool(av), "suggestion": sug},
        )
    return JSONResponse({"ok": True, "available": av, "suggestion": sug, "sub": s}, status_code=200)

@admin_app.post("/gyms/{gym_id}/update")
async def update_gym(request: Request, gym_id: int, nombre: Optional[str] = Form(None), subdominio: Optional[str] = Form(None), auto_subdomain: Optional[bool] = Form(False), disable_sync: Optional[bool] = Form(False)):
    _require_admin(request)
    rl = _check_rate_limit(request, "gym_update", 60, 60)
    if rl:
        return rl
    adm = _get_admin_service()
    if adm is None:
        return JSONResponse({"error": "DB admin no disponible"}, status_code=500)
    nm = (nombre or "").strip()
    sd_in = (subdominio or "").strip().lower()
    sd = sd_in
    try:
        gcur = adm.obtener_gimnasio(int(gym_id))
    except Exception:
        gcur = None
    try:
        old_sub = str((gcur or {}).get("subdominio") or "").strip().lower()
    except Exception:
        old_sub = ""
    try:
        if nm:
            if not bool(disable_sync):
                sd = adm.sugerir_subdominio_unico(nm or "")
            else:
                sd = sd_in or old_sub
    except Exception:
        sd = sd_in or old_sub
    if sd:
        try:
            av = bool(adm.subdominio_disponible(sd))
        except Exception:
            av = False
        if not av:
            return JSONResponse({"ok": False, "error": "subdominio_in_use"}, status_code=400)
    do_assets = False
    try:
        if nm and sd and old_sub and (sd != old_sub) and (not bool(disable_sync)):
            do_assets = True
    except Exception:
        do_assets = False
    if do_assets:
        res = adm.renombrar_gimnasio_y_assets(int(gym_id), nm or None, sd or None)
    else:
        res = adm.actualizar_gimnasio(int(gym_id), nm or None, sd or None)
    try:
        adm.log_action("owner", "update_gym", int(gym_id), {"nombre": nm, "subdominio": sd})
    except Exception:
        pass
    sc = 200 if bool(res.get("ok")) else 400
    return JSONResponse(res, status_code=sc)

@admin_app.post("/gyms/{gym_id}/contact")
async def actualizar_contacto(request: Request, gym_id: int, owner_phone: Optional[str] = Form(None)):
    _require_admin(request)
    adm = _get_admin_service()
    if adm is None:
        return JSONResponse({"error": "DB admin no disponible"}, status_code=500)
    ok = adm.set_gym_owner_phone(int(gym_id), owner_phone)
    try:
        adm.log_action("owner", "set_owner_phone", int(gym_id), str(owner_phone or ""))
    except Exception:
        pass
    return JSONResponse({"ok": bool(ok)}, status_code=200)

@admin_app.delete("/gyms/{gym_id}")
async def eliminar_gimnasio(request: Request, gym_id: int):
    _require_admin(request)
    rl = _check_rate_limit(request, "gym_delete", 10, 300)
    if rl:
        return rl
    adm = _get_admin_service()
    if adm is None:
        return JSONResponse({"error": "DB admin no disponible"}, status_code=500)
    ok = adm.eliminar_gimnasio(int(gym_id))
    try:
        adm.log_action("owner", "delete_gym", int(gym_id), None)
    except Exception:
        pass
    return JSONResponse({"ok": bool(ok)}, status_code=200)

@admin_app.post("/gyms/{gym_id}/suspend")
async def suspender_gimnasio(request: Request, gym_id: int, reason: Optional[str] = Form(None), until: Optional[str] = Form(None), hard: Optional[bool] = Form(False)):
    _require_admin(request)
    rl = _check_rate_limit(request, "gym_suspend", 30, 60)
    if rl:
        return rl
    adm = _get_admin_service()
    if adm is None:
        return JSONResponse({"error": "DB admin no disponible"}, status_code=500)
    ok = adm.set_estado_gimnasio(int(gym_id), "suspended", True, until, reason)
    try:
        adm.log_action("owner", "suspend_gym", int(gym_id), f"{reason}|{until}|{bool(hard)}")
    except Exception:
        pass
    return JSONResponse({"ok": bool(ok)}, status_code=200)

@admin_app.post("/gyms/{gym_id}/unsuspend")
async def reactivar_gimnasio(request: Request, gym_id: int):
    _require_admin(request)
    rl = _check_rate_limit(request, "gym_unsuspend", 30, 60)
    if rl:
        return rl
    adm = _get_admin_service()
    if adm is None:
        return JSONResponse({"error": "DB admin no disponible"}, status_code=500)
    ok = adm.set_estado_gimnasio(int(gym_id), "active", False, None, None)
    try:
        adm.log_action("owner", "unsuspend_gym", int(gym_id), None)
    except Exception:
        pass
    return JSONResponse({"ok": bool(ok)}, status_code=200)

@admin_app.get("/gyms/{gym_id}/whatsapp")
async def whatsapp_form(request: Request, gym_id: int):
    _require_admin(request)
    adm = _get_admin_service()
    if adm is None:
        return JSONResponse({"error": "DB admin no disponible"}, status_code=500)
    g = adm.obtener_gimnasio(int(gym_id))
    if not g:
        return JSONResponse({"error": "gym_not_found"}, status_code=404)
    pid = str(g.get("whatsapp_phone_id") or "")
    tok = str(g.get("whatsapp_access_token") or "")
    waba = str(g.get("whatsapp_business_account_id") or "")
    vt = str(g.get("whatsapp_verify_token") or "")
    asct = str(g.get("whatsapp_app_secret") or "")
    nb = "checked" if bool(g.get("whatsapp_nonblocking")) else ""
    sto = str(g.get("whatsapp_send_timeout_seconds") or "")
    return templates.TemplateResponse(
        "gym-settings.html",
        {
            "request": request,
            "section": "whatsapp",
            "gid": int(gym_id),
            "phone_id": pid,
            "access_token": tok,
            "waba_id": waba,
            "verify_token": vt,
            "app_secret": asct,
            "nonblocking": bool(g.get("whatsapp_nonblocking") or False),
            "send_timeout_seconds": sto,
        },
    )

@admin_app.post("/gyms/{gym_id}/whatsapp")
async def whatsapp_save(request: Request, gym_id: int, phone_id: Optional[str] = Form(None), access_token: Optional[str] = Form(None), waba_id: Optional[str] = Form(None), verify_token: Optional[str] = Form(None), app_secret: Optional[str] = Form(None), nonblocking: Optional[bool] = Form(False), send_timeout_seconds: Optional[str] = Form(None)):
    _require_admin(request)
    rl = _check_rate_limit(request, "whatsapp_save", 40, 60)
    if rl:
        return rl
    adm = _get_admin_service()
    if adm is None:
        return JSONResponse({"error": "DB admin no disponible"}, status_code=500)
    sto = None
    try:
        raw = (send_timeout_seconds or "").strip()
        if raw:
            try:
                sto = float(raw)
            except Exception:
                sto = None
    except Exception:
        sto = None
    ok = adm.set_gym_whatsapp_config(int(gym_id), phone_id, access_token, waba_id, verify_token, app_secret, nonblocking, sto)
    try:
        adm.log_action("owner", "set_whatsapp_config", int(gym_id), None)
    except Exception:
        pass
    return JSONResponse({"ok": bool(ok)}, status_code=200)

@admin_app.post("/gyms/{gym_id}/whatsapp/test")
async def whatsapp_test(request: Request, gym_id: int, to: str = Form(None), message: str = Form(None)):
    _require_admin(request)
    rl = _check_rate_limit(request, "whatsapp_test", 40, 60)
    if rl:
        return rl
    adm = _get_admin_service()
    if adm is None:
        return JSONResponse({"error": "DB admin no disponible"}, status_code=500)
    g = adm.obtener_gimnasio(int(gym_id))
    if not g:
        return JSONResponse({"error": "gym_not_found"}, status_code=404)
    phone_id = (str(g.get("whatsapp_phone_id") or "").strip())
    access_token = (str(g.get("whatsapp_access_token") or "").strip())
    owner_phone = (str(g.get("owner_phone") or "").strip())
    dest = (to or owner_phone or "").strip()
    if not phone_id or not access_token:
        return JSONResponse({"ok": False, "error": "whatsapp_not_configured"}, status_code=400)
    if not dest:
        return JSONResponse({"ok": False, "error": "no_destination"}, status_code=400)
    text = (message or "Mensaje de prueba de GymMS").strip()
    try:
        import httpx
        api_version = (os.getenv("WHATSAPP_API_VERSION") or "v19.0").strip()
        url = f"https://graph.facebook.com/{api_version}/{phone_id}/messages"
        headers = {"Authorization": f"Bearer {access_token}", "Content-Type": "application/json"}
        payload = {"messaging_product": "whatsapp", "to": dest, "type": "text", "text": {"body": text}}
        async with httpx.AsyncClient(timeout=10) as client:
            r = await client.post(url, headers=headers, json=payload)
        if r.status_code >= 300:
            return JSONResponse({"ok": False, "status": r.status_code, "error": (r.text or "")[:500]}, status_code=400)
        try:
            adm.log_action("owner", "whatsapp_test", int(gym_id), None)
        except Exception:
            pass
        return JSONResponse({"ok": True}, status_code=200)
    except Exception as e:
        return JSONResponse({"ok": False, "error": str(e)}, status_code=500)

@admin_app.get("/gyms/{gym_id}/storage")
async def storage_form(request: Request, gym_id: int):
    _require_admin(request)
    adm = _get_admin_service()
    if adm is None:
        return JSONResponse({"error": "DB admin no disponible"}, status_code=500)
    g = adm.obtener_gimnasio(int(gym_id))
    if not g:
        return JSONResponse({"error": "gym_not_found"}, status_code=404)
    folder_prefix = f"{str(g.get('subdominio') or '').strip()}-assets"
    return templates.TemplateResponse(
        "gym-settings.html",
        {
            "request": request,
            "section": "storage",
            "gid": int(gym_id),
            "folder_prefix": folder_prefix,
        },
    )

# --- Endpoints para las secciones faltantes ---

@admin_app.get("/gyms/{gym_id}/audit")
async def gym_audit(request: Request, gym_id: int):
    _require_admin(request)
    adm = _get_admin_service()
    audit_logs = adm.obtener_auditoria_gym(int(gym_id)) if adm else []
    return templates.TemplateResponse("gym-settings.html", {"request": request, "section": "audit", "gid": int(gym_id), "audit_logs": audit_logs})

@admin_app.get("/gyms/{gym_id}/subscriptions")
async def gym_subscriptions(request: Request, gym_id: int):
    _require_admin(request)
    adm = _get_admin_service()
    plans = adm.listar_planes() if adm else []
    # Fetch current subscription info if needed, but gym-settings.html template might expect 'subscription' object or 'plans' list
    # Currently 'subscription' section in template uses 'plans' loop (line 187) and 'subscription' object (line 177)
    # I should probably fetch subscription details here too, similar to how I might have done in other places or rely on passed data
    # Wait, looking at existing 'subscription' section in template, it uses `subscription` and `plans`.
    # Where does `subscription` come from? In `listar_gimnasios_con_resumen` query, there is some sub info.
    # But for detailed view, I might need to fetch it.
    # `admin_service` doesn't have `obtener_suscripcion_gym`.
    # I should probably add it or infer it.
    # Let's look at `gym_subscriptions` endpoint.
    # I'll fetch plans. I'll try to fetch subscription details if I can.
    # Actually, `gym-settings.html` expects `plans` for the dropdown.
    # It also expects `subscription` object with `plan`, `start_date`, `valid_until`.
    # I should fetch these.
    sub = None
    try:
        with adm.db.get_connection_context() as conn:
             cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
             cur.execute("SELECT gs.start_date, gs.next_due_date as valid_until, gs.status, p.name as plan_name, p.amount, p.currency FROM gym_subscriptions gs LEFT JOIN plans p ON p.id = gs.plan_id WHERE gs.gym_id = %s", (int(gym_id),))
             row = cur.fetchone()
             if row:
                 sub = {
                     "plan": row.get("plan_name"),
                     "start_date": row.get("start_date"),
                     "valid_until": row.get("valid_until"),
                     "status": row.get("status")
                 }
    except Exception:
        pass
    
    return templates.TemplateResponse("gym-settings.html", {"request": request, "section": "subscription", "gid": int(gym_id), "plans": plans, "subscription": sub or {}})

@admin_app.get("/gyms/{gym_id}/plans")
async def gym_plans(request: Request, gym_id: int):
    _require_admin(request)
    adm = _get_admin_service()
    plans = adm.listar_planes() if adm else []
    return templates.TemplateResponse("gym-settings.html", {"request": request, "section": "plans", "gid": int(gym_id), "plans": plans})

@admin_app.get("/gyms/{gym_id}/templates")
async def gym_templates(request: Request, gym_id: int):
    _require_admin(request)
    adm = _get_admin_service()
    tmpls = adm.listar_templates() if adm else []
    return templates.TemplateResponse("gym-settings.html", {"request": request, "section": "templates", "gid": int(gym_id), "templates": tmpls})

@admin_app.get("/gyms/{gym_id}/payments")
async def gym_payments(request: Request, gym_id: int):
    _require_admin(request)
    adm = _get_admin_service()
    items = adm.listar_pagos(int(gym_id)) if adm else []
    return templates.TemplateResponse("gym-settings.html", {"request": request, "section": "payments", "gid": int(gym_id), "items": items})

@admin_app.get("/gyms/{gym_id}/maintenance")
async def gym_maintenance(request: Request, gym_id: int):
    _require_admin(request)
    return templates.TemplateResponse("gym-settings.html", {"request": request, "section": "maintenance", "gid": int(gym_id)})

@admin_app.get("/gyms/{gym_id}/branding")
async def gym_branding(request: Request, gym_id: int):
    _require_admin(request)
    adm = _get_admin_service()
    g = adm.obtener_gimnasio(int(gym_id)) if adm else {}
    return templates.TemplateResponse("gym-settings.html", {"request": request, "section": "branding", "gid": int(gym_id), "gym": g})

@admin_app.get("/gyms/{gym_id}/password")
async def gym_password(request: Request, gym_id: int):
    _require_admin(request)
    return templates.TemplateResponse("gym-settings.html", {"request": request, "section": "password", "gid": int(gym_id)})

@admin_app.post("/gyms/{gym_id}/password")
async def update_gym_password(request: Request, gym_id: int, password: str = Form(...)):
    _require_admin(request)
    adm = _get_admin_service()
    if adm is None:
         return JSONResponse({"error": "DB admin no disponible"}, status_code=500)
    ok = adm.set_gym_owner_password(int(gym_id), password)
    try:
        adm.log_action("owner", "set_gym_password", int(gym_id), None)
    except Exception:
        pass
    return JSONResponse({"ok": bool(ok)}, status_code=200)
