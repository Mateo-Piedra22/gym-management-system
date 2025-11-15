import os
from typing import Optional, List, Dict, Any

from fastapi import FastAPI, Request, HTTPException, Form, BackgroundTasks
from fastapi.responses import JSONResponse, Response, RedirectResponse
from fastapi.templating import Jinja2Templates
from pathlib import Path
from starlette.middleware.sessions import SessionMiddleware
try:
    import requests  # type: ignore
except Exception:
    requests = None  # type: ignore
from datetime import datetime

from .database import AdminDatabaseManager, _resolve_admin_db_params
try:
    from core.database import DatabaseManager  # type: ignore
except Exception:
    DatabaseManager = None  # type: ignore


admin_app = FastAPI(title="GymMS Admin", version="1.0")
_cookie_domain = os.getenv("SESSION_COOKIE_DOMAIN", "").strip() or None
_cookie_secure = (os.getenv("SESSION_COOKIE_SECURE", "1").strip().lower() in ("1", "true", "yes"))
_cookie_samesite = (os.getenv("SESSION_COOKIE_SAMESITE", "lax").strip().lower() or "lax")
admin_app.add_middleware(SessionMiddleware, secret_key=os.getenv("ADMIN_SESSION_SECRET", "admin-session"), domain=_cookie_domain, https_only=_cookie_secure, same_site=_cookie_samesite)
try:
    setattr(admin_app.state, "session_version", int(getattr(admin_app.state, "session_version", 1)))
    setattr(admin_app.state, "rate_limits", dict(getattr(admin_app.state, "rate_limits", {})))
except Exception:
    pass
templates = Jinja2Templates(directory=str(Path(__file__).parent / "templates"))

_admin_db: Optional[AdminDatabaseManager] = None

def _get_admin_db() -> Optional[AdminDatabaseManager]:
    global _admin_db
    if _admin_db is not None:
        return _admin_db
    try:
        _admin_db = AdminDatabaseManager()
        return _admin_db
    except Exception:
        return None

def _is_logged_in(request: Request) -> bool:
    try:
        return bool(request.session.get("admin_logged_in"))
    except Exception:
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
def _sidebar_html() -> str:
    return (
        "<aside style=\"background:#0b1222;border-right:1px solid #333;padding:16px\">"
        + "<div style=\"font-size:18px;font-weight:700;color:#fff\">GymMS Admin</div>"
        + "<nav style=\"margin-top:12px;display:grid;gap:8px\">"
        + "<a href=\"/admin/dashboard?ui=1\" style=\"padding:8px 10px;border-radius:8px;background:#111827;color:#fff;text-decoration:none\">Dashboard</a>"
        + "<a href=\"/admin/gyms?ui=1\" style=\"padding:8px 10px;border-radius:8px;background:#111827;color:#fff;text-decoration:none\">Gimnasios</a>"
        + "<a href=\"/admin/metrics?ui=1\" style=\"padding:8px 10px;border-radius:8px;background:#111827;color:#fff;text-decoration:none\">Métricas</a>"
        + "<a href=\"/admin/audit?ui=1\" style=\"padding:8px 10px;border-radius:8px;background:#111827;color:#fff;text-decoration:none\">Auditoría</a>"
        + "<a href=\"/admin/subscriptions/dashboard?ui=1\" style=\"padding:8px 10px;border-radius:8px;background:#111827;color:#fff;text-decoration:none\">Suscripciones</a>"
        + "</nav></aside>"
    )
def _admin_wrap(content: str) -> str:
    return (
        "<div class=\"dark\"><div style=\"display:grid;grid-template-columns:240px 1fr;min-height:100vh;font-family:system-ui\">"
        + _sidebar_html()
        + "<main style=\"padding:20px\">"
        + content
        + "</main></div></div>"
    )

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
        adm = _get_admin_db()
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
                    adm2 = _get_admin_db()
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
        adm = _get_admin_db()
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
            return JSONResponse({"ok": False, "error": "unauthorized"}, status_code=401)
        except Exception:
            if wants_html:
                return RedirectResponse(url="/admin/login", status_code=303)
            return JSONResponse({"ok": False, "error": "unauthorized"}, status_code=401)
    warnings: List[str] = []
    try:
        adm = _get_admin_db()
        if adm is not None:
            ws = adm.obtener_warnings_admin()
            if ws:
                warnings = list(ws)
    except Exception:
        warnings = []
    try:
        adm = _get_admin_db()
        metrics = adm.obtener_metricas_agregadas() if adm else {"gyms": {"total": 0, "active": 0, "suspended": 0, "maintenance": 0, "last_7": 0, "last_30": 0, "series_30": []}, "whatsapp": {"configured": 0}, "storage": {"configured": 0}, "subscriptions": {"active": 0, "overdue": 0}, "payments": {"last_30_sum": 0.0}}
        series = list((metrics.get("gyms") or {}).get("series_30") or [])
        series_max = max([int(it.get("count") or 0) for it in series] or [1])
        upcoming = adm.listar_proximos_vencimientos(14) if adm else []
        recent_payload = adm.listar_gimnasios_con_resumen(1, 8, None, None, "created_at", "DESC") if adm else {"items": []}
        recent_gyms = list((recent_payload or {}).get("items") or [])
        recent_payments = adm.listar_pagos_recientes(10) if adm else []
        audit = adm.resumen_auditoria(7) if adm else {"by_action": [], "by_actor": [], "days": 7}
        return templates.TemplateResponse("home.html", {"request": request, "warnings": warnings, "metrics": metrics, "series_max": int(series_max), "upcoming": upcoming, "recent_gyms": recent_gyms, "recent_payments": recent_payments, "audit": audit})
    except Exception:
        if wants_html:
            return Response(content="<div class=\"p-6 text-red-300\">Error interno del panel</div>", media_type="text/html", status_code=500)
        return JSONResponse({"ok": False, "error": "internal_home_render"}, status_code=500)
    html = """
    <div class=\"dark\"><div style=\"display:grid;grid-template-columns:240px 1fr;min-height:100vh;font-family:system-ui\"><aside style=\"background:#0b1222;border-right:1px solid #333;padding:16px\"><div style=\"font-size:18px;font-weight:700;color:#fff\">GymMS Admin</div><nav style=\"margin-top:12px;display:grid;gap:8px\"><a href=\"/admin/dashboard?ui=1\" style=\"padding:8px 10px;border-radius:8px;background:#111827;color:#fff;text-decoration:none\">Dashboard</a><a href=\"/admin/gyms?ui=1\" style=\"padding:8px 10px;border-radius:8px;background:#111827;color:#fff;text-decoration:none\">Gimnasios</a><a href=\"/admin/metrics?ui=1\" style=\"padding:8px 10px;border-radius:8px;background:#111827;color:#fff;text-decoration:none\">Métricas</a><a href=\"/admin/audit?ui=1\" style=\"padding:8px 10px;border-radius:8px;background:#111827;color:#fff;text-decoration:none\">Auditoría</a><a href=\"/admin/subscriptions/dashboard?ui=1\" style=\"padding:8px 10px;border-radius:8px;background:#111827;color:#fff;text-decoration:none\">Suscripciones</a></nav></aside><main style=\"padding:20px\"><h1 style=\"font-size:24px;font-weight:600\">Panel Admin</h1><div style=\"display:flex;gap:12px;margin-top:12px\"><a href=\"/admin/login\" style=\"padding:10px 14px;border-radius:6px;background:#444;color:#fff;text-decoration:none\">Cambiar cuenta</a></div><h2 style=\"font-size:18px;margin-top:16px\">Crear gimnasio</h2><div style=\"color:#9ca3af;margin-top:6px\">Los campos marcados con * son obligatorios</div><form id=\"create-form\" method=\"post\" action=\"/admin/gyms\" style=\"display:grid;gap:8px;margin-top:8px;max-width:640px\"><label style=\"display:grid;gap:4px\"><span style=\"color:#fff\">Nombre *</span><input id=\"create-name\" name=\"nombre\" placeholder=\"Nombre\" required style=\"padding:8px;border:1px solid #333;border-radius:6px\" /></label><div style=\"display:grid;gap:4px\"><span style=\"color:#fff\">Subdominio (opcional)</span><div style=\"display:flex;gap:8px;align-items:center\"><input id=\"create-sub\" name=\"subdominio\" placeholder=\"Subdominio (opcional)\" style=\"padding:8px;border:1px solid #333;border-radius:6px\" /><span id=\"create-sub-status\" style=\"color:#9ca3af;font-size:12px\"></span></div></div><label style=\"display:grid;gap:4px\"><span style=\"color:#fff\">Teléfono dueño (opcional)</span><input name=\"owner_phone\" placeholder=\"Teléfono dueño (+54...) (opcional)\" style=\"padding:8px;border:1px solid #333;border-radius:6px\" /></label><label style=\"display:grid;gap:4px\"><span style=\"color:#fff\">WhatsApp Phone ID (opcional)</span><input name=\"whatsapp_phone_id\" placeholder=\"WhatsApp Phone ID (opcional)\" style=\"padding:8px;border:1px solid #333;border-radius:6px\" /></label><label style=\"display:grid;gap:4px\"><span style=\"color:#fff\">WhatsApp Access Token (opcional)</span><input name=\"whatsapp_access_token\" placeholder=\"WhatsApp Access Token (opcional)\" style=\"padding:8px;border:1px solid #333;border-radius:6px\" /></label><label style=\"display:grid;gap:4px\"><span style=\"color:#fff\">WhatsApp Business Account ID (opcional)</span><input name=\"whatsapp_business_account_id\" placeholder=\"WhatsApp Business Account ID (opcional)\" style=\"padding:8px;border:1px solid #333;border-radius:6px\" /></label><label style=\"display:grid;gap:4px\"><span style=\"color:#fff\">WhatsApp Verify Token (opcional)</span><input name=\"whatsapp_verify_token\" placeholder=\"WhatsApp Verify Token (opcional)\" style=\"padding:8px;border:1px solid #333;border-radius:6px\" /></label><label style=\"display:grid;gap:4px\"><span style=\"color:#fff\">WhatsApp App Secret (opcional)</span><input name=\"whatsapp_app_secret\" placeholder=\"WhatsApp App Secret (opcional)\" style=\"padding:8px;border:1px solid #333;border-radius:6px\" /></label><label style=\"display:flex;align-items:center;gap:8px\"><input type=\"checkbox\" name=\"whatsapp_nonblocking\" value=\"true\"/> WhatsApp no bloqueante</label><label style=\"display:grid;gap:4px\"><span style=\"color:#fff\">Timeout envío WhatsApp (seg) (opcional)</span><input type=\"number\" step=\"0.1\" name=\"whatsapp_send_timeout_seconds\" placeholder=\"Timeout envío WhatsApp (seg) (opcional)\" style=\"padding:8px;border:1px solid #333;border-radius:6px\" /></label><button type=\"submit\" style=\"padding:10px 14px;border-radius:6px;background:#111;color:#fff\">Crear gimnasio</button></form></main></div></div>
    """
    try:
        html = html.replace("/admin/subscriptions/upcoming?ui=1", "/admin/subscriptions/dashboard?ui=1")
        html = html.replace("Próximos vencimientos", "Dashboard de vencimientos")
    except Exception:
        pass
    extra = """
    <div class=\"dark\"><style>@keyframes shimmer{0%{background-position:-200px 0}100%{background-position:200px 0}}.skeleton{background:#1f2937;background-image:linear-gradient(90deg,#1f2937 0,#374151 50%,#1f2937 100%);background-size:200px 100%;animation:shimmer 1.2s infinite linear}</style><button id=\"open-k\" style=\"position:fixed;bottom:16px;right:16px;padding:10px 12px;border-radius:999px;background:#374151;color:#fff\">Ctrl+K</button><div id=\"k-overlay\" style=\"position:fixed;inset:0;background:rgba(0,0,0,0.6);display:none;align-items:center;justify-content:center;z-index:40\"><div style=\"width:90%;max-width:720px;background:#111827;border:1px solid #333;border-radius:12px\"><div style=\"padding:12px;border-bottom:1px solid #333;display:flex;justify-content:space-between;align-items:center\"><div style=\"font-weight:600\">Búsqueda</div><button id=\"k-close\" style=\"background:#1f2937;color:#fff;border-radius:6px;padding:6px 8px\">Cerrar</button></div><div style=\"padding:12px\"><input id=\"k-input\" placeholder=\"Buscar gimnasios o templates\" style=\"padding:8px;border:1px solid #333;border-radius:8px;width:100%\"/><div id=\"k-results\" style=\"margin-top:12px;display:grid;gap:8px\"></div></div></div></div></div>
    <script>function _slugify(v){v=String(v||'').toLowerCase();v=v.normalize('NFD').replace(/[\u0300-\u036f]/g,'');v=v.replace(/[^a-z0-9]+/g,'-').replace(/-+/g,'-');v=v.replace(/^-|-$/g,'');return v}function _updateSubFromName(){var n=document.getElementById('create-name');var s=document.getElementById('create-sub');var st=document.getElementById('create-sub-status');if(!n||!s)return;var base=_slugify(n.value);if(!base){s.value='';if(st){st.textContent=''}return}fetch('/admin/subdomains/check?sub='+encodeURIComponent(base)+'&name='+encodeURIComponent(n.value),{headers:{'accept':'application/json'}}).then(function(r){return r.json()}).then(function(j){var sug=String((j&&j.suggestion)||base);s.value=sug;if(st){if(j&&j.available){st.textContent='Disponible';st.style.color='#16a34a'}else{st.textContent='Sugerido: '+sug;st.style.color='#f59e0b'}}}).catch(function(){s.value=base;if(st){st.textContent=''}})}function _handleCreateSubmit(e){e.preventDefault();var f=document.getElementById('create-form');if(!f)return;var btn=f.querySelector('button[type="submit"]');var s=document.getElementById('create-sub');var retried=false;function submitOnce(){if(btn){btn.disabled=true;btn.textContent='Creando...'}var fd=new FormData(f);fetch('/admin/gyms',{method:'POST',headers:{'accept':'application/json'},body:fd}).then(function(r){return r.json().then(function(j){return {ok:r.ok, status:r.status, body:j}})}).then(function(res){if(res.ok&&res.body&&res.body.id){window.location.href='/admin/gyms?ui=1';return}if(!res.ok&&res.body&&res.body.error==='subdominio_in_use'&&res.body.suggestion&&!retried){if(s){s.value=String(res.body.suggestion||s.value)}retried=true;submitOnce();return}alert('Error al crear gimnasio')}).catch(function(){alert('Error de red al crear gimnasio')}).finally(function(){if(btn){btn.disabled=false;btn.textContent='Crear gimnasio'}})}submitOnce()}document.addEventListener('DOMContentLoaded',function(){var n=document.getElementById('create-name');if(n){n.addEventListener('input',_updateSubFromName);_updateSubFromName()}var f=document.getElementById('create-form');if(f){f.addEventListener('submit',_handleCreateSubmit)}});</script>
    """
    warnings_html = ""
    try:
        adm = _get_admin_db()
        if adm is not None:
            ws = adm.obtener_warnings_admin()
            if ws:
                items = "".join([f"<li>{w}</li>" for w in ws])
                warnings_html = f"<div class=\"dark\"><div style=\"max-width:720px;margin:0 auto;padding:0 20px\"><div style=\"margin-top:12px;padding:10px;border-radius:8px;background:#ef4444;color:#fff\"><div style=\"font-weight:600\">Warnings</div><ul style=\"margin:8px 0 0 18px\">{items}</ul></div></div></div>"
    except Exception:
        warnings_html = ""
    html = warnings_html + html + extra
    links_extra = """
    <div class=\"dark\"><div style=\"max-width:720px;margin:0 auto;padding:0 20px\"><div style=\"margin-top:12px;display:flex;gap:12px;flex-wrap:wrap\"><a href=\"/admin/audit?ui=1\" style=\"padding:8px 12px;border-radius:6px;background:#1f2937;color:#fff;text-decoration:none\">Auditoría</a><a href=\"/admin/metrics?ui=1\" style=\"padding:8px 12px;border-radius:6px;background:#1f2937;color:#fff;text-decoration:none\">Métricas</a><form method=\"post\" action=\"/admin/sessions/invalidate\" style=\"display:inline\"><button type=\"submit\" style=\"padding:8px 12px;border-radius:6px;background:#ef4444;color:#fff\">Invalidar sesiones</button></form></div></div></div>
    """
    html = html + links_extra
    try:
        start = html.find('<main')
        if start >= 0:
            start = html.find('>', start) + 1
            end = html.rfind('</main>')
            if end > start:
                content_only = html[start:end]
                html = _admin_wrap(content_only)
            else:
                html = _admin_wrap(html)
        else:
            html = _admin_wrap(html)
    except Exception:
        html = _admin_wrap(html)

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

@admin_app.get("/gyms")
async def listar_gimnasios(request: Request):
    gr = _guard_html_login_redirect(request)
    if gr:
        return gr
    rl = _check_rate_limit(request, "gyms_list", 120, 60)
    if rl:
        return rl
    adm = _get_admin_db()
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
    wants_html = ("text/html" in accept) or (request.query_params.get("ui") == "1")
    snippet = (str(request.query_params.get("snippet") or "").strip() == "1") or (str(request.headers.get("hx-request") or "").lower() == "true")
    if not wants_html:
        return JSONResponse(payload, status_code=200)
    try:
        payload_grid = adm.listar_gimnasios_con_resumen(page, page_size, q or None, status_q or None, order_by or None, order_dir or None)
    except Exception:
        payload_grid = payload
    items: List[Dict[str, Any]] = list((payload_grid or {}).get("items") or [])
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
    def chip(s: str) -> str:
        t = (s or "").strip().lower()
        bg = "#1f2937"
        fg = "#fff"
        txt = s or ""
        if t == "active":
            bg = "#16a34a"
        elif t == "suspended":
            bg = "#ef4444"
        elif t == "maintenance":
            bg = "#f59e0b"
        elif t == "pending":
            bg = "#374151"
        return f"<span style=\"display:inline-block;padding:4px 8px;border-radius:999px;background:{bg};color:{fg};font-size:12px\">{txt}</span>"
    rows: List[str] = []
    for g in items:
        gid = str(g.get("id") or "")
        nombre = str(g.get("nombre") or "")
        sub = str(g.get("subdominio") or "")
        owner_phone = str(g.get("owner_phone") or "")
        status = str(g.get("status") or "")
        hard = "Sí" if bool(g.get("hard_suspend")) else "No"
        until = str(g.get("suspended_until") or "")
        bname = str(g.get("b2_bucket_name") or "")
        salud = f"<div id=\"health-{gid}\" style=\"display:flex;gap:10px;align-items:center\"><div class=\"skeleton\" style=\"width:96px;height:14px;border-radius:6px\"></div></div>"
        status_html = chip(status)
        actions = f"<form method=\"post\" action=\"/admin/gyms/{gid}/contact\" style=\"display:inline\"><input name=\"owner_phone\" value=\"{owner_phone}\" placeholder=\"+54...\" style=\"padding:6px;border:1px solid #333;border-radius:6px;width:160px\"/><button type=\"submit\" style=\"margin-left:6px\">Guardar</button></form> <a href=\"/admin/gyms/{gid}/owner?ui=1\" style=\"margin-left:6px\">Contraseña dueño</a> <a href=\"/admin/gyms/{gid}/branding?ui=1\" style=\"margin-left:6px\">Branding</a> <a href=\"#\" data-edit=\"{gid}\" style=\"margin-left:6px\">Editar</a> <form method=\"post\" action=\"/admin/gyms/{gid}/provision\" style=\"display:inline;margin-left:6px\"><button type=\"submit\">Provisionar</button></form> <form method=\"post\" action=\"/admin/gyms/{gid}/suspend\" style=\"display:inline;margin-left:6px\"><input type=\"hidden\" name=\"hard\" value=\"false\"/><button type=\"submit\">Suspender</button></form> <form method=\"post\" action=\"/admin/gyms/{gid}/unsuspend\" style=\"display:inline;margin-left:6px\"><button type=\"submit\">Reactivar</button></form> <a href=\"/admin/gyms/{gid}/subscription?ui=1\" style=\"margin-left:6px\">Suscripción</a> <a href=\"/admin/gyms/{gid}/payments?ui=1\" style=\"margin-left:6px\">Pagos</a> <a href=\"/admin/gyms/{gid}/maintenance?ui=1\" style=\"margin-left:6px\">Mantenimiento</a> <a href=\"/admin/gyms/{gid}/whatsapp?ui=1\" style=\"margin-left:6px\">WhatsApp</a> <a href=\"/admin/gyms/{gid}/health?ui=1\" style=\"margin-left:6px\">Salud</a> <form method=\"post\" action=\"/admin/gyms/{gid}/b2/regenerate-key\" style=\"display:inline;margin-left:6px\"><button type=\"submit\">Regenerar clave B2</button></form> <form method=\"post\" action=\"/admin/gyms/{gid}/b2/delete-bucket\" style=\"display:inline;margin-left:6px\"><button type=\"submit\">Eliminar bucket B2</button></form> <form method=\"post\" action=\"/admin/gyms/{gid}/delete\" style=\"display:inline;margin-left:6px\"><button type=\"submit\">Eliminar</button></form>"
        created = str(g.get("created_at") or "")
        rows.append(f"<tr data-gym-id=\"{gid}\"><td>{gid}</td><td>{nombre}</td><td>{sub}</td><td>{created}</td><td>{owner_phone}</td><td>{status_html}</td><td>{salud}</td><td>{hard}</td><td>{until}</td><td>{bname}</td><td>{actions}</td></tr>")
    def link_for(col: str) -> str:
        dir_next = "ASC" if order_dir.upper() == "DESC" else "DESC"
        return f"/admin/gyms?ui=1&page={p}&page_size={ps}&q={q}&status={status_q}&order_by={col}&order_dir={dir_next}"
    prev_link = f"/admin/gyms?ui=1&page={max(p-1,1)}&page_size={ps}&q={q}&status={status_q}&order_by={order_by}&order_dir={order_dir}"
    next_link = f"/admin/gyms?ui=1&page={p+1}&page_size={ps}&q={q}&status={status_q}&order_by={order_by}&order_dir={order_dir}"
    last_page = max((total + ps - 1) // ps, 1)
    if p >= last_page:
        next_link = prev_link
    style = "<style>@keyframes shimmer{0%{background-position:-200px 0}100%{background-position:200px 0}}.skeleton{background:#1f2937;background-image:linear-gradient(90deg,#1f2937 0,#374151 50%,#1f2937 100%);background-size:200px 100%;animation:shimmer 1.2s infinite linear}</style>"
    def _card_for(g: Dict[str, Any]) -> str:
        gid = str(g.get("id") or "")
        nombre = str(g.get("nombre") or "")
        sub = str(g.get("subdominio") or "")
        status_html = chip(str(g.get("status") or ""))
        created = str(g.get("created_at") or "")
        owner_phone = str(g.get("owner_phone") or "")
        salud = f"<div id=\"health-{gid}\" style=\"display:flex;gap:10px;align-items:center\"><div class=\"skeleton\" style=\"width:96px;height:14px;border-radius:6px\"></div></div>"
        next_due = str(g.get("next_due_date") or "")
        last_amt = g.get("last_payment_amount")
        last_cur = str(g.get("last_payment_currency") or "")
        last_at = str(g.get("last_payment_at") or "")
        resumen = ""
        if next_due or last_amt:
            resumen = f"<div style=\"display:grid;grid-template-columns:1fr 1fr;gap:8px\"><div style=\"color:#9ca3af\">Vence</div><div style=\"color:#fff\">{next_due}</div><div style=\"color:#9ca3af\">Último pago</div><div style=\"color:#fff\">{str(last_amt or '')} {last_cur} {('<span style=\"color:#9ca3af\">· ' + last_at + '</span>') if last_at else ''}</div></div>"
        quick = f"<div style=\"display:flex;gap:6px\"><a href=\"/admin/gyms/{gid}/subscription?ui=1\" style=\"padding:6px 8px;border-radius:6px;background:#1f2937;color:#fff;text-decoration:none\">Subs</a><a href=\"/admin/gyms/{gid}/whatsapp?ui=1\" style=\"padding:6px 8px;border-radius:6px;background:#1f2937;color:#fff;text-decoration:none\">WA</a><a href=\"/admin/gyms/{gid}/maintenance?ui=1\" style=\"padding:6px 8px;border-radius:6px;background:#1f2937;color:#fff;text-decoration:none\">Mant.</a><a href=\"/admin/gyms/{gid}/health?ui=1\" style=\"padding:6px 8px;border-radius:6px;background:#1f2937;color:#fff;text-decoration:none\">Salud</a></div>"
        return f"<div data-gym-id=\"{gid}\" style=\"padding:12px;border:1px solid #333;border-radius:12px;background:#0b1222;display:grid;gap:8px\"><div style=\"display:flex;justify-content:space-between;align-items:center\"><div style=\"display:flex;gap:8px;align-items:center\"><input type=\"checkbox\" data-select=\"{gid}\"/><div style=\"font-weight:700;color:#fff\">{nombre}</div><div style=\"color:#9ca3af;cursor:copy\" data-sub=\"{sub}\">{sub}</div></div><div>{status_html}</div></div><div style=\"display:grid;grid-template-columns:1fr 1fr;gap:8px\"><div style=\"color:#9ca3af\">Creado</div><div style=\"color:#fff\">{created}</div><div style=\"color:#9ca3af\">Teléfono</div><div style=\"color:#fff\">{owner_phone}</div></div>{resumen}<div style=\"display:flex;justify-content:space-between;align-items:center\"><div style=\"display:flex;gap:8px;align-items:center\"><div style=\"color:#9ca3af\">Salud</div>{salud}</div><div style=\"display:flex;gap:8px\"><button data-open=\"{gid}\" style=\"padding:6px 10px;border-radius:6px;background:#1e40af;color:#fff\">Ver detalles</button>{quick}</div></div></div>"
    _cards_html = "".join([_card_for(g) for g in items])
    html_grid = (
        "<div class=\"dark\"><div style=\"display:grid;grid-template-columns:240px 1fr;min-height:100vh;font-family:system-ui\">"
        + "<aside style=\"background:#0b1222;border-right:1px solid #333;padding:16px\"><div style=\"font-size:18px;font-weight:700;color:#fff\">GymMS Admin</div><div style=\"margin-top:10px\"><input id=\"sb-search\" placeholder=\"Buscar...\" style=\"padding:8px;border:1px solid #333;border-radius:8px;width:100%\"/><div id=\"sb-results\" style=\"margin-top:8px;display:grid;gap:6px\"></div></div><nav style=\"margin-top:12px;display:grid;gap:8px\"><a href=\"/admin/dashboard?ui=1\" style=\"padding:8px 10px;border-radius:8px;background:#111827;color:#fff;text-decoration:none\">Dashboard</a><a href=\"/admin/gyms?ui=1\" style=\"padding:8px 10px;border-radius:8px;background:#111827;color:#fff;text-decoration:none\">Gimnasios</a><a href=\"/admin/metrics?ui=1\" style=\"padding:8px 10px;border-radius:8px;background:#111827;color:#fff;text-decoration:none\">Métricas</a><a href=\"/admin/audit?ui=1\" style=\"padding:8px 10px;border-radius:8px;background:#111827;color:#fff;text-decoration:none\">Auditoría</a><a href=\"/admin/subscriptions/dashboard?ui=1\" style=\"padding:8px 10px;border-radius:8px;background:#111827;color:#fff;text-decoration:none\">Suscripciones</a></nav></aside>"
        + "<main style=\"padding:20px\"><h1 style=\"font-size:24px;font-weight:600;color:#fff\">Gimnasios</h1>"
        + "<div style=\"margin:12px 0;display:flex;gap:12px;flex-wrap:wrap\"><form method=\"get\" action=\"/admin/gyms\" style=\"display:flex;gap:8px;flex-wrap:wrap\"><input name=\"q\" value=\"" + q + "\" placeholder=\"Buscar por nombre o subdominio\" style=\"padding:8px;border:1px solid #333;border-radius:6px;min-width:240px\"/><select name=\"status\" style=\"padding:8px;border:1px solid #333;border-radius:6px\"><option value=\"\">Todos</option><option value=\"active\"" + (" selected\"" if status_q == "active" else "\"") + ">Activos</option><option value=\"suspended\"" + (" selected\"" if status_q == "suspended" else "\"") + ">Suspendidos</option><option value=\"maintenance\"" + (" selected\"" if status_q == "maintenance" else "\"") + ">Mantenimiento</option></select><input type=\"hidden\" name=\"ui\" value=\"1\"/><button type=\"submit\" style=\"padding:8px 12px;border-radius:6px;background:#1e40af;color:#fff\">Filtrar</button></form><form method=\"get\" action=\"/admin/gyms\" style=\"display:flex;gap:8px\"><input name=\"page_size\" value=\"" + str(ps) + "\" style=\"padding:8px;border:1px solid #333;border-radius:6px;width:80px\"/><input type=\"hidden\" name=\"ui\" value=\"1\"/><input type=\"hidden\" name=\"page\" value=\"1\"/><button type=\"submit\" style=\"padding:8px 12px;border-radius:6px;background:#111827;color:#fff\">Tamaño página</button></form></div>"
        + style
        + "<div style=\"display:grid;grid-template-columns:repeat(auto-fit,minmax(260px,1fr));gap:12px\">" + _cards_html + "</div><div style=\"margin-top:12px;display:flex;gap:8px;align-items:center\"><a id=\"prev-link\" href=\"" + prev_link + "\" style=\"padding:8px 12px;border-radius:6px;background:#1f2937;color:#fff;text-decoration:none\">Anterior</a><a id=\"next-link\" href=\"" + next_link + "\" style=\"padding:8px 12px;border-radius:6px;background:#1f2937;color:#fff;text-decoration:none\">Siguiente</a><div style=\"color:#9ca3af\">Página " + str(p) + " de " + str(last_page) + " · " + str(total) + " resultados</div><div id=\"sel-count\" style=\"color:#9ca3af;margin-left:auto\">Seleccionados: 0</div><div style=\"display:flex;gap:8px\"><button id=\"batch-prov\" style=\"padding:8px 12px;border-radius:6px;background:#1e40af;color:#fff\">Provisionar</button><button id=\"batch-sus\" style=\"padding:8px 12px;border-radius:6px;background:#ef4444;color:#fff\">Suspender</button><button id=\"batch-uns\" style=\"padding:8px 12px;border-radius:6px;background:#16a34a;color:#fff\">Reactivar</button></div><div id=\"toast-container\" style=\"display:grid;gap:8px;position:fixed;top:16px;right:16px\"></div></div><div id=\"details-overlay\" style=\"position:fixed;inset:0;background:rgba(0,0,0,0.6);display:none;align-items:center;justify-content:center;z-index:50\"><div style=\"width:92%;max-width:920px;background:#111827;border:1px solid #333;border-radius:12px\"><div style=\"padding:12px;border-bottom:1px solid #333;display:flex;justify-content:space-between;align-items:center\"><div style=\"font-weight:600;color:#fff\">Detalles del gimnasio</div><button id=\"details-close\" style=\"background:#1f2937;color:#fff;border-radius:6px;padding:6px 8px\">Cerrar</button></div><div id=\"details-content\" style=\"padding:12px\"></div></div></div><button id=\"open-k-float\" style=\"position:fixed;bottom:16px;right:16px;padding:10px 12px;border-radius:999px;background:#374151;color:#fff\">Ctrl+K</button></main></div></div>"
    )
    js_grid = """
    <script>
    function toast(msg, type){var c=document.getElementById('toast-container');if(!c){var t=document.createElement('div');t.id='toast-container';t.setAttribute('style','display:grid;gap:8px;position:fixed;top:16px;right:16px');document.body.appendChild(t);c=t}var bg=type==='error'?'#ef4444':(type==='info'?'#374151':'#16a34a');var el=document.createElement('div');el.setAttribute('style','background:'+bg+';color:#fff;padding:10px 12px;border-radius:8px;box-shadow:0 10px 15px rgba(0,0,0,0.2)');el.textContent=msg;c.appendChild(el);setTimeout(function(){if(el&&el.parentNode){el.parentNode.removeChild(el)}},3000)}
    function _slugify(v){v=String(v||'').toLowerCase();v=v.normalize('NFD').replace(/[\u0300-\u036f]/g,'');v=v.replace(/[^a-z0-9]+/g,'-').replace(/-+/g,'-');v=v.replace(/^-|-$/g,'');return v}
    function loadHealth(gid){var el=document.getElementById('health-'+gid);if(!el)return;fetch('/admin/gyms/'+gid+'/health',{headers:{'accept':'application/json'}}).then(function(r){if(!r.ok){throw new Error('HTTP '+r.status)}return r.json()}).then(function(j){var d=j&&j.db&&j.db.ok;var w=j&&j.whatsapp&&j.whatsapp.ok;var s=j&&j.storage&&j.storage.ok;var html='';html+="<div style=\"display:flex;align-items:center;gap:4px\"><span style=\"width:8px;height:8px;border-radius:999px;background:"+(d?'#16a34a':'#ef4444')+"\"></span><span style=\"color:#9ca3af\">DB</span></div>";html+="<div style=\"display:flex;align-items:center;gap:4px\"><span style=\"width:8px;height:8px;border-radius:999px;background:"+(w?'#16a34a':'#ef4444')+"\"></span><span style=\"color:#9ca3af\">WA</span></div>";html+="<div style=\"display:flex;align-items:center;gap:4px\"><span style=\"width:8px;height:8px;border-radius:999px;background:"+(s?'#16a34a':'#ef4444')+"\"></span><span style=\"color:#9ca3af\">ST</span></div>";el.innerHTML=html}).catch(function(e){var html='';html+="<div style=\"display:flex;align-items:center;gap:4px\"><span style=\"width:8px;height:8px;border-radius:999px;background:#ef4444\"></span><span style=\"color:#9ca3af\">DB</span></div>";html+="<div style=\"display:flex;align-items:center;gap:4px\"><span style=\"width:8px;height:8px;border-radius:999px;background:#ef4444\"></span><span style=\"color:#9ca3af\">WA</span></div>";html+="<div style=\"display:flex;align-items:center;gap:4px\"><span style=\"width:8px;height:8px;border-radius:999px;background:#ef4444\"></span><span style=\"color:#9ca3af\">ST</span></div>";el.innerHTML=html})}
    function initHealth(){var cards=document.querySelectorAll('[data-gym-id]');var ids=[];for(var i=0;i<cards.length;i++){ids.push(cards[i].getAttribute('data-gym-id'))}var idx=0;var conc=3;function runOne(){if(idx>=ids.length)return;var gid=ids[idx++];loadHealth(gid);setTimeout(runOne,400)}for(var k=0;k<conc;k++){runOne()}}
    function openK(){var o=document.getElementById('k-overlay');if(o){o.style.display='flex';var inp=document.getElementById('k-input');if(inp){setTimeout(function(){inp.focus()},50)}}}
    function closeK(){var o=document.getElementById('k-overlay');if(o){o.style.display='none'}}
    var _kTimer=null;function searchK(q){var res=document.getElementById('k-results');if(!res)return;clearTimeout(_kTimer);_kTimer=setTimeout(function(){res.innerHTML='<div class="skeleton" style="width:100%;height:18px;border-radius:6px"></div>';var list=[];fetch('/admin/gyms?q='+encodeURIComponent(q)+'&page=1&page_size=10',{headers:{'accept':'application/json'}}).then(function(r){return r.json()}).then(function(j){var items=(j&&j.items)||[];for(var i=0;i<items.length;i++){var g=items[i];list.push({t:'gym',id:String(g.id||''),label:String(g.nombre||'')+' · '+String(g.subdominio||'')})}return fetch('/admin/templates',{headers:{'accept':'application/json'}})}).then(function(r){return r.json()}).then(function(j){var t=(j&&j.templates)||[];for(var i=0;i<t.length;i++){list.push({t:'template',id:String(i+1),label:String(t[i]||'')})}var html='';for(var k=0;k<list.length;k++){var it=list[k];var href=it.t==='gym'?('/admin/gyms/'+it.id+'/health?ui=1'):('/admin/templates?ui=1');html+="<a href=\""+href+"\" style=\"display:flex;justify-content:space-between;align-items:center;padding:8px 10px;border:1px solid #333;border-radius:8px;background:#0b1222;color:#fff;text-decoration:none\"><span>"+it.label+"</span><span style=\"color:#9ca3af;font-size:12px\">"+it.t+"</span></a>"}if(!html){html='<div style=\"color:#9ca3af\">Sin resultados</div>'}res.innerHTML=html}).catch(function(){res.innerHTML='<div style=\"color:#ef4444\">Error en búsqueda</div>'})},300)}
    function openDetails(gid){var o=document.getElementById('details-overlay');var c=document.getElementById('details-content');if(o&&c){o.style.display='flex';c.innerHTML='<div class="skeleton" style="width:100%;height:18px;border-radius:6px"></div>';fetch('/admin/gyms/'+gid+'/details',{headers:{'accept':'application/json'}}).then(function(r){return r.json().then(function(j){return {ok:r.ok, body:j}})}).then(function(res){if(!res.ok){c.innerHTML='<div style=\"color:#ef4444\">Error al cargar detalles</div>';return}var d=res.body||{};var g=d.gym||{};var h=d.health||{};var s=d.subscription||{};var pays=d.payments||[];var ok=function(v){return v?'<span style=\"color:#16a34a\">OK</span>':'<span style=\"color:#ef4444\">Fallo</span>'};var html='';html+='<div style=\"display:grid;grid-template-columns:repeat(auto-fit,minmax(260px,1fr));gap:12px\">';html+='<div style=\"padding:12px;border:1px solid #333;border-radius:12px;background:#0b1222\"><div style=\"color:#9ca3af\">ID</div><div style=\"color:#fff\">'+String(g.id||'')+'</div><div style=\"color:#9ca3af\">Nombre</div><div style=\"color:#fff\">'+String(g.nombre||'')+'</div><div style=\"color:#9ca3af\">Subdominio</div><div style=\"color:#fff\">'+String(g.subdominio||'')+'</div><div style=\"color:#9ca3af\">Estado</div><div>'+String(g.status||'')+'</div></div>';html+='<div style=\"padding:12px;border:1px solid #333;border-radius:12px;background:#0b1222\"><div style=\"font-weight:600;color:#fff\">Salud</div><div style=\"margin-top:8px;display:grid;grid-template-columns:1fr 1fr;gap:8px\"><div>DB</div><div>'+ok(h.db&&h.db.ok)+'</div><div>WhatsApp</div><div>'+ok(h.whatsapp&&h.whatsapp.ok)+'</div><div>Storage</div><div>'+ok(h.storage&&h.storage.ok)+'</div></div></div>';html+='<div style=\"padding:12px;border:1px solid #333;border-radius:12px;background:#0b1222\"><div style=\"font-weight:600;color:#fff\">Suscripción</div><div style=\"margin-top:8px;display:grid;grid-template-columns:1fr 1fr;gap:8px\"><div>Plan</div><div>'+String((s&&s.plan_name)||'')+'</div><div>Estado</div><div>'+String((s&&s.status)||'')+'</div><div>Vence</div><div>'+String((s&&s.next_due_date)||'')+'</div></div></div>';var plist='';for(var i=0;i<Math.min(pays.length,5);i++){var p=pays[i];plist+='<tr><td>'+String(p.paid_at||'')+'</td><td>'+String(p.amount||'')+' '+String(p.currency||'')+'</td><td>'+String(p.status||'')+'</td></tr>'}html+='<div style=\"padding:12px;border:1px solid #333;border-radius:12px;background:#0b1222\"><div style=\"font-weight:600;color:#fff\">Pagos recientes</div><div style=\"overflow-x:auto;margin-top:8px\"><table style=\"width:100%;border-collapse:collapse\"><thead><tr><th>Fecha</th><th>Monto</th><th>Estado</th></tr></thead><tbody>'+plist+'</tbody></table></div></div>';html+='</div>';html+='<div style=\"margin-top:12px;display:flex;gap:8px;flex-wrap:wrap\"><form method=\"post\" action=\"/admin/gyms/'+String(g.id||'')+'/provision\" style=\"display:inline\"><button type=\"submit\" style=\"padding:8px 12px;border-radius:6px;background:#1e40af;color:#fff\">Provisionar</button></form><form method=\"post\" action=\"/admin/gyms/'+String(g.id||'')+'/suspend\" style=\"display:inline\"><input type=\"hidden\" name=\"hard\" value=\"false\"/><button type=\"submit\" style=\"padding:8px 12px;border-radius:6px;background:#ef4444;color:#fff\">Suspender</button></form><form method=\"post\" action=\"/admin/gyms/'+String(g.id||'')+'/unsuspend\" style=\"display:inline\"><button type=\"submit\" style=\"padding:8px 12px;border-radius:6px;background:#16a34a;color:#fff\">Reactivar</button></form><a href=\"/admin/gyms/'+String(g.id||'')+'/branding?ui=1\" style=\"padding:8px 12px;border-radius:6px;background:#374151;color:#fff;text-decoration:none\">Branding</a><a href=\"/admin/gyms/'+String(g.id||'')+'/owner?ui=1\" style=\"padding:8px 12px;border-radius:6px;background:#374151;color:#fff;text-decoration:none\">Contraseña dueño</a><form method=\"post\" action=\"/admin/gyms/'+String(g.id||'')+'/b2/regenerate-key\" style=\"display:inline\"><button type=\"submit\" style=\"padding:8px 12px;border-radius:6px;background:#111827;color:#fff\">Regenerar clave B2</button></form><form method=\"post\" action=\"/admin/gyms/'+String(g.id||'')+'/b2/delete-bucket\" style=\"display:inline\"><button type=\"submit\" style=\"padding:8px 12px;border-radius:6px;background:#111827;color:#fff\">Eliminar bucket B2</button></form></div>';c.innerHTML=html;}).catch(function(){c.innerHTML='<div style=\"color:#ef4444\">Error al cargar detalles</div>'})}}
    var dc=document.getElementById('details-close');if(dc){dc.addEventListener('click',function(){var o=document.getElementById('details-overlay');if(o){o.style.display='none'}})}
    function initAdminPage(){initHealth();var ok=document.getElementById('open-k');if(ok){ok.addEventListener('click',openK)}var okf=document.getElementById('open-k-float');if(okf){okf.addEventListener('click',openK)}var kc=document.getElementById('k-close');if(kc){kc.addEventListener('click',closeK)}var ki=document.getElementById('k-input');if(ki){ki.addEventListener('input',function(){searchK(ki.value)})}var toolbar=document.querySelector('h1').parentNode;try{var inp=document.createElement('input');inp.id='page-jump';inp.placeholder='Ir a página';inp.setAttribute('style','padding:8px;border:1px solid #333;border-radius:6px;width:100px');var btn=document.createElement('button');btn.id='go-page';btn.textContent='Ir';btn.setAttribute('style','padding:8px 12px;border-radius:6px;background:#374151;color:#fff;margin-left:8px');var rng=document.createElement('div');rng.textContent='Rango: 1–{LP}';rng.setAttribute('style','color:#9ca3af;margin-left:8px');toolbar.appendChild(inp);toolbar.appendChild(btn);toolbar.appendChild(rng);}catch(e){}var cards=document.querySelectorAll('[data-gym-id]');var stored=(localStorage.getItem('admin_selected_gym_ids')||'').split(',').filter(function(x){return !!x});var selected=new Set(stored);function updateSel(){var c=document.getElementById('sel-count');if(c){c.textContent='Seleccionados: '+selected.size;localStorage.setItem('admin_selected_gym_ids',Array.from(selected).join(','))}}for(var i=0;i<cards.length;i++){(function(r){var gid=r.getAttribute('data-gym-id');var btn=r.querySelector('button[data-open]');var chk=r.querySelector('input[type="checkbox"][data-select]');var sub=r.querySelector('[data-sub]');if(btn){btn.addEventListener('click',function(){openDetails(gid)})}if(chk){chk.checked=selected.has(gid);chk.addEventListener('change',function(){if(chk.checked){selected.add(gid)}else{selected.delete(gid)}updateSel()})}if(sub){sub.addEventListener('click',function(){var tx=sub.textContent||'';if(navigator.clipboard&&navigator.clipboard.writeText){navigator.clipboard.writeText(tx).then(function(){toast('Copiado: '+tx,'info')}).catch(function(){toast('No se pudo copiar','error')})}else{toast(tx,'info')}})}})(cards[i])}updateSel();var sbs=document.getElementById('sb-search');if(sbs){sbs.addEventListener('input',function(){sbSearch(sbs.value)})}var bp=document.getElementById('batch-prov');if(bp){bp.addEventListener('click',function(){runBatch('provision',selected)})}var bs=document.getElementById('batch-sus');if(bs){bs.addEventListener('click',function(){runBatch('suspend',selected)})}var bu=document.getElementById('batch-uns');if(bu){bu.addEventListener('click',function(){runBatch('unsuspend',selected)})}var bui=document.getElementById('batch-uns');var parent= bui?bui.parentNode:null;if(parent){var im=document.createElement('input');im.id='batch-msg';im.placeholder='Mensaje';im.setAttribute('style','padding:8px;border:1px solid #333;border-radius:6px;min-width:240px');var br=document.createElement('button');br.id='batch-remind';br.textContent='Recordar';br.setAttribute('style','padding:8px 12px;border-radius:6px;background:#9333ea;color:#fff;margin-left:8px');parent.appendChild(im);parent.appendChild(br);br.addEventListener('click',function(){var form=new URLSearchParams();form.append('gym_ids',Array.from(selected).join(','));var msg=document.getElementById('batch-msg');form.append('message',msg?String(msg.value||''):'');fetch('/admin/gyms/remind/batch',{method:'POST',headers:{'accept':'application/json','content-type':'application/x-www-form-urlencoded'},body:form.toString()}).then(function(r){return r.json().then(function(j){return {ok:r.ok,status:r.status,body:j}})}).then(function(res){if(res.ok&&res.body&&res.body.ok){toast('Recordatorios enviados','success')}else{if(res.status===429){toast('Rate limit excedido','error')}else{toast('Error al recordar','error')}}}).catch(function(){toast('Error de red','error')})})}}
    if(document.readyState==='loading'){document.addEventListener('DOMContentLoaded',initAdminPage)}else{initAdminPage()}
    document.addEventListener('keydown',function(e){if((e.ctrlKey||e.metaKey)&&e.key.toLowerCase()==='k'){e.preventDefault();openK()}if(e.key==='['){var link=document.getElementById('prev-link');if(link){link.click()}}if(e.key===']'){var link2=document.getElementById('next-link');if(link2){link2.click()}}});
    var gp=document.getElementById('go-page');if(gp){gp.addEventListener('click',function(){var inp=document.getElementById('page-jump');var v=inp&&inp.value?parseInt(inp.value,10):NaN;if(!isNaN(v)&&v>0){var url='/admin/gyms?ui=1&page='+v+'&page_size={PS}&q='+encodeURIComponent("{Q}")+'&status={STATUS}&order_by={ORDER_BY}&order_dir={ORDER_DIR}';window.location.href=url}})}
    function runBatch(action, selected){if(!selected||selected.size===0){toast('No hay seleccionados','info');return}var form=new URLSearchParams();form.append('action',action);form.append('gym_ids',Array.from(selected).join(','));fetch('/admin/gyms/batch',{method:'POST',headers:{'accept':'application/json','content-type':'application/x-www-form-urlencoded'},body:form.toString()}).then(function(r){return r.json().then(function(j){return {ok:r.ok,body:j}})}).then(function(res){if(res.ok&&res.body&&res.body.ok){toast('Acción '+action+' aplicada','success');setTimeout(function(){window.location.reload()},600)}else{toast('Error en lote','error')}}).catch(function(){toast('Error de red','error')})}
    var _sbTimer=null;function sbSearch(q){var res=document.getElementById('sb-results');if(!res)return;var list=[];if(!q||q.length<2){res.innerHTML='';return}clearTimeout(_sbTimer);_sbTimer=setTimeout(function(){res.innerHTML='<div class="skeleton" style="width:100%;height:18px;border-radius:6px"></div>';fetch('/admin/gyms?q='+encodeURIComponent(q)+'&page=1&page_size=5',{headers:{'accept':'application/json'}}).then(function(r){return r.json()}).then(function(j){var items=(j&&j.items)||[];var html='';for(var i=0;i<items.length;i++){var g=items[i];html+='<button data-open="'+String(g.id||'')+'" style="display:flex;justify-content:space-between;align-items:center;padding:8px 10px;border:1px solid #333;border-radius:8px;background:#0b1222;color:#fff">'+String(g.nombre||'')+' · '+String(g.subdominio||'')+'<span style="color:#9ca3af;font-size:12px">Ver</span></button>'}if(!html){html='<div style="color:#9ca3af">Sin resultados</div>'}res.innerHTML=html;var btns=res.querySelectorAll('button[data-open]');for(var k=0;k<btns.length;k++){(function(b){b.addEventListener('click',function(){openDetails(b.getAttribute('data-open'))})})(btns[k])}}).catch(function(){res.innerHTML='<div style="color:#ef4444">Error</div>'})},300)}
    document.addEventListener('submit',function(e){var t=e.target;try{if(t&&t.tagName==='FORM'){var a=t.getAttribute('action')||'';var need=false;if(a.indexOf('/provision')>=0||a.indexOf('/suspend')>=0||a.indexOf('/unsuspend')>=0||a.indexOf('/delete-bucket')>=0||a.indexOf('/b2/regenerate-key')>=0||a.indexOf('/delete')>=0||a.match(new RegExp('^/admin/gyms/\\d+$'))){need=true}if(need){var ok=window.confirm('¿Confirmar la acción?');if(!ok){e.preventDefault();return false}}}}catch(err){}});
    </script>
    """.replace("{LP}", str(last_page)).replace("{PS}", str(ps)).replace("{Q}", q).replace("{STATUS}", status_q).replace("{ORDER_BY}", order_by).replace("{ORDER_DIR}", order_dir)
    use_grid = True
    html = """
    <div class=\"dark\"><div style=\"max-width:1200px;margin:0 auto;padding:20px;font-family:system-ui\"><style>@keyframes shimmer{0%{background-position:-200px 0}100%{background-position:200px 0}}.skeleton{background:#1f2937;background-image:linear-gradient(90deg,#1f2937 0,#374151 50%,#1f2937 100%);background-size:200px 100%;animation:shimmer 1.2s infinite linear}</style><h1 style=\"font-size:24px;font-weight:600\">Gimnasios</h1><div style=\"margin:12px 0;display:flex;gap:12px;flex-wrap:wrap\"><a href=\"/admin/\" style=\"padding:8px 12px;border-radius:6px;background:#111;color:#fff;text-decoration:none\">Inicio</a><button id=\"open-k\" style=\"padding:8px 12px;border-radius:6px;background:#374151;color:#fff\">Ctrl+K</button><form method=\"get\" action=\"/admin/gyms\" style=\"display:flex;gap:8px;flex-wrap:wrap\"><input name=\"q\" value=\"{q}\" placeholder=\"Buscar por nombre o subdominio\" style=\"padding:8px;border:1px solid #333;border-radius:6px;min-width:240px\"/><select name=\"status\" style=\"padding:8px;border:1px solid #333;border-radius:6px\"><option value=\"\">Todos</option><option value=\"active\"{act_sel}>Activos</option><option value=\"suspended\"{sus_sel}>Suspendidos</option><option value=\"maintenance\"{mnt_sel}>Mantenimiento</option></select><input type=\"hidden\" name=\"ui\" value=\"1\"/><button type=\"submit\" style=\"padding:8px 12px;border-radius:6px;background:#1e40af;color:#fff\">Filtrar</button></form><form method=\"get\" action=\"/admin/gyms\" style=\"display:flex;gap:8px\"><input name=\"page_size\" value=\"{ps}\" style=\"padding:8px;border:1px solid #333;border-radius:6px;width:80px\"/><input type=\"hidden\" name=\"ui\" value=\"1\"/><input type=\"hidden\" name=\"page\" value=\"1\"/><button type=\"submit\" style=\"padding:8px 12px;border-radius:6px;background:#111827;color:#fff\">Tamaño página</button></form></div><div style=\"overflow-x:auto\"><table style=\"width:100%;border-collapse:collapse\"><thead><tr><th><a href=\"{link_id}\" style=\"color:#9ca3af\">ID</a></th><th><a href=\"{link_nombre}\" style=\"color:#9ca3af\">Nombre</a></th><th><a href=\"{link_sub}\" style=\"color:#9ca3af\">Subdominio</a></th><th><a href=\"{link_created}\" style=\"color:#9ca3af\">Creado</a></th><th>Teléfono</th><th>Status</th><th>Salud</th><th>Hard</th><th>Hasta</th><th>Bucket</th><th>Acciones</th></tr></thead><tbody>{rows}</tbody></table></div><div style=\"margin-top:12px;display:flex;gap:8px;align-items:center\"><a href=\"{prev}\" id=\"prev-link\" style=\"padding:8px 12px;border-radius:6px;background:#374151;color:#fff;text-decoration:none\">Anterior</a><div style=\"color:#9ca3af\">Página {p} de {lp} • {total} resultados</div><a href=\"{next}\" id=\"next-link\" style=\"padding:8px 12px;border-radius:6px;background:#374151;color:#fff;text-decoration:none\">Siguiente</a></div><h2 style=\"font-size:18px;margin-top:16px\">Crear gimnasio</h2><div style=\"color:#9ca3af;margin-top:6px\">Los campos marcados con * son obligatorios</div><form method=\"post\" action=\"/admin/gyms\" style=\"display:grid;grid-template-columns:repeat(auto-fit,minmax(200px,1fr));gap:8px;margin-top:8px\"><input id=\"list-create-name\" name=\"nombre\" placeholder=\"Nombre *\" required style=\"padding:8px;border:1px solid #333;border-radius:6px\"/><div style=\"display:flex;gap:8px;align-items:center\"><input id=\"list-create-sub\" name=\"subdominio\" placeholder=\"Subdominio (opcional)\" style=\"padding:8px;border:1px solid #333;border-radius:6px\"/><span id=\"list-create-status\" style=\"color:#9ca3af;font-size:12px\"></span></div><input name=\"owner_phone\" placeholder=\"Teléfono dueño (+54...) (opcional)\" style=\"padding:8px;border:1px solid #333;border-radius:6px\"/><input name=\"whatsapp_phone_id\" placeholder=\"WhatsApp Phone ID (opcional)\" style=\"padding:8px;border:1px solid #333;border-radius:6px\"/><input name=\"whatsapp_access_token\" placeholder=\"WhatsApp Access Token (opcional)\" style=\"padding:8px;border:1px solid #333;border-radius:6px\"/><button type=\"submit\" style=\"padding:10px 14px;border-radius:6px;background:#16a34a;color:#fff\">Crear</button></form><div id=\"toast-container\" style=\"position:fixed;top:16px;right:16px;display:flex;flex-direction:column;gap:8px;z-index:50\"></div><div id=\"k-overlay\" style=\"position:fixed;inset:0;background:rgba(0,0,0,0.6);display:none;align-items:center;justify-content:center;z-index:40\"><div style=\"width:90%;max-width:720px;background:#111827;border:1px solid #333;border-radius:12px\"><div style=\"padding:12px;border-bottom:1px solid #333;display:flex;justify-content:space-between;align-items:center\"><div style=\"font-weight:600\">Búsqueda</div><button id=\"k-close\" style=\"background:#1f2937;color:#fff;border-radius:6px;padding:6px 8px\">Cerrar</button></div><div style=\"padding:12px\"><input id=\"k-input\" placeholder=\"Buscar gimnasios o templates\" style=\"padding:8px;border:1px solid #333;border-radius:8px;width:100%\"/><div id=\"k-results\" style=\"margin-top:12px;display:grid;gap:8px\"></div></div></div></div><div id=\"edit-overlay\" style=\"position:fixed;inset:0;background:rgba(0,0,0,0.6);display:none;align-items:center;justify-content:center;z-index:50\"><div style=\"width:90%;max-width:520px;background:#111827;border:1px solid #333;border-radius:12px\"><div style=\"padding:12px;border-bottom:1px solid #333;display:flex;justify-content:space-between;align-items:center\"><div style=\"font-weight:600\">Editar gimnasio</div><button id=\"edit-close\" style=\"background:#1f2937;color:#fff;border-radius:6px;padding:6px 8px\">Cerrar</button></div><div style=\"padding:12px;display:grid;gap:8px\"><input id=\"edit-name\" placeholder=\"Nombre *\" style=\"padding:8px;border:1px solid #333;border-radius:8px\"/><div style=\"display:flex;gap:8px;align-items:center\"><input id=\"edit-sub\" placeholder=\"Subdominio (opcional)\" style=\"padding:8px;border:1px solid #333;border-radius:8px\"/><span id=\"edit-status\" style=\"color:#9ca3af;font-size:12px\"></span></div><div style=\"display:flex;gap:8px\"><button id=\"edit-save\" style=\"padding:8px 12px;border-radius:6px;background:#16a34a;color:#fff\">Guardar</button><button id=\"edit-cancel\" style=\"padding:8px 12px;border-radius:6px;background:#374151;color:#fff\">Cancelar</button></div><input id=\"edit-id\" type=\"hidden\"/></div></div><button id=\"open-k-float\" style=\"position:fixed;bottom:16px;right:16px;padding:10px 12px;border-radius:999px;background:#374151;color:#fff\">Ctrl+K</button></div>
    """.replace("{rows}", "".join(rows)).replace("{prev}", prev_link).replace("{next}", next_link).replace("{p}", str(p)).replace("{lp}", str(last_page)).replace("{total}", str(total)).replace("{link_id}", link_for("id")).replace("{link_nombre}", link_for("nombre")).replace("{link_sub}", link_for("subdominio")).replace("{link_created}", link_for("created_at")).replace("{q}", q).replace("{act_sel}", " selected\"" if status_q == "active" else "\"").replace("{sus_sel}", " selected\"" if status_q == "suspended" else "\"").replace("{mnt_sel}", " selected\"" if status_q == "maintenance" else "\"")
    js = """
    <script>
    function toast(msg, type){var c=document.getElementById('toast-container');if(!c)return;var bg=type==='error'?'#ef4444':(type==='info'?'#374151':'#16a34a');var el=document.createElement('div');el.setAttribute('style','background:'+bg+';color:#fff;padding:10px 12px;border-radius:8px;box-shadow:0 10px 15px rgba(0,0,0,0.2)');el.textContent=msg;c.appendChild(el);setTimeout(function(){if(el&&el.parentNode){el.parentNode.removeChild(el)}},3000)}
    function _slugify(v){v=String(v||'').toLowerCase();v=v.normalize('NFD').replace(/[\u0300-\u036f]/g,'');v=v.replace(/[^a-z0-9]+/g,'-').replace(/-+/g,'-');v=v.replace(/^-|-$/g,'');return v}
    function _updateListSub(){var n=document.getElementById('list-create-name');var s=document.getElementById('list-create-sub');var st=document.getElementById('list-create-status');if(!n||!s)return;var base=_slugify(n.value);if(!base){s.value='';if(st){st.textContent=''}return}fetch('/admin/subdomains/check?sub='+encodeURIComponent(base)+'&name='+encodeURIComponent(n.value),{headers:{'accept':'application/json'}}).then(function(r){return r.json()}).then(function(j){var sug=String((j&&j.suggestion)||base);s.value=sug;if(st){if(j&&j.available){st.textContent='Disponible';st.style.color='#16a34a'}else{st.textContent='Sugerido: '+sug;st.style.color='#f59e0b'}}}).catch(function(){s.value=base;if(st){st.textContent=''}})}
    function loadHealth(gid){var el=document.getElementById('health-'+gid);if(!el)return;fetch('/admin/gyms/'+gid+'/health',{headers:{'accept':'application/json'}}).then(function(r){if(!r.ok){throw new Error('HTTP '+r.status)}return r.json()}).then(function(j){var d=j&&j.db&&j.db.ok;var w=j&&j.whatsapp&&j.whatsapp.ok;var s=j&&j.storage&&j.storage.ok;var html='';html+="<div style=\"display:flex;align-items:center;gap:4px\"><span style=\"width:8px;height:8px;border-radius:999px;background:"+(d?'#16a34a':'#ef4444')+"\"></span><span style=\"color:#9ca3af\">DB</span></div>";html+="<div style=\"display:flex;align-items:center;gap:4px\"><span style=\"width:8px;height:8px;border-radius:999px;background:"+(w?'#16a34a':'#ef4444')+"\"></span><span style=\"color:#9ca3af\">WA</span></div>";html+="<div style=\"display:flex;align-items:center;gap:4px\"><span style=\"width:8px;height:8px;border-radius:999px;background:"+(s?'#16a34a':'#ef4444')+"\"></span><span style=\"color:#9ca3af\">ST</span></div>";el.innerHTML=html}).catch(function(e){var html='';html+="<div style=\"display:flex;align-items:center;gap:4px\"><span style=\"width:8px;height:8px;border-radius:999px;background:#ef4444\"></span><span style=\"color:#9ca3af\">DB</span></div>";html+="<div style=\"display:flex;align-items:center;gap:4px\"><span style=\"width:8px;height:8px;border-radius:999px;background:#ef4444\"></span><span style=\"color:#9ca3af\">WA</span></div>";html+="<div style=\"display:flex;align-items:center;gap:4px\"><span style=\"width:8px;height:8px;border-radius:999px;background:#ef4444\"></span><span style=\"color:#9ca3af\">ST</span></div>";el.innerHTML=html})}
    function initHealth(){var rows=document.querySelectorAll('tr[data-gym-id]');var ids=[];for(var i=0;i<rows.length;i++){ids.push(rows[i].getAttribute('data-gym-id'))}var idx=0;var conc=3;function runOne(){if(idx>=ids.length)return;var gid=ids[idx++];loadHealth(gid);setTimeout(runOne,400)}for(var k=0;k<conc;k++){runOne()}}
    function openK(){var o=document.getElementById('k-overlay');if(o){o.style.display='flex';var inp=document.getElementById('k-input');if(inp){setTimeout(function(){inp.focus()},50)}}}
    function closeK(){var o=document.getElementById('k-overlay');if(o){o.style.display='none'}}
    function searchK(q){var res=document.getElementById('k-results');if(!res)return;res.innerHTML='<div class="skeleton" style="width:100%;height:18px;border-radius:6px"></div>';var list=[];fetch('/admin/gyms?q='+encodeURIComponent(q)+'&page=1&page_size=10',{headers:{'accept':'application/json'}}).then(function(r){return r.json()}).then(function(j){var items=(j&&j.items)||[];for(var i=0;i<items.length;i++){var g=items[i];list.push({t:'gym',id:String(g.id||''),label:String(g.nombre||'')+' · '+String(g.subdominio||'')})}return fetch('/admin/templates',{headers:{'accept':'application/json'}})}).then(function(r){return r.json()}).then(function(j){var t=(j&&j.templates)||[];for(var i=0;i<t.length;i++){list.push({t:'template',id:String(i+1),label:String(t[i]||'')})}var html='';for(var k=0;k<list.length;k++){var it=list[k];var href=it.t==='gym'?('/admin/gyms/'+it.id+'/health?ui=1'):('/admin/templates?ui=1');html+="<a href=\""+href+"\" style=\"display:flex;justify-content:space-between;align-items:center;padding:8px 10px;border:1px solid #333;border-radius:8px;background:#0b1222;color:#fff;text-decoration:none\"><span>"+it.label+"</span><span style=\"color:#9ca3af;font-size:12px\">"+it.t+"</span></a>"}if(!html){html='<div style="color:#9ca3af">Sin resultados</div>'}res.innerHTML=html}).catch(function(){res.innerHTML='<div style="color:#ef4444">Error en búsqueda</div>'})}
    document.addEventListener('DOMContentLoaded',function(){initHealth();var ok=document.getElementById('open-k');if(ok){ok.addEventListener('click',openK)}var okf=document.getElementById('open-k-float');if(okf){okf.addEventListener('click',openK)}var kc=document.getElementById('k-close');if(kc){kc.addEventListener('click',closeK)}var ki=document.getElementById('k-input');if(ki){ki.addEventListener('input',function(){searchK(ki.value)})}
    var ln=document.getElementById('list-create-name');if(ln){ln.addEventListener('input',_updateListSub);_updateListSub()}
    var th=document.querySelector('thead');if(th){th.style.position='sticky';th.style.top='0';th.style.background='#111827';th.style.zIndex='5'}
    var toolbar=document.querySelector('h1').parentNode;try{var inp=document.createElement('input');inp.id='page-jump';inp.placeholder='Ir a página';inp.setAttribute('style','padding:8px;border:1px solid #333;border-radius:6px;width:100px');var btn=document.createElement('button');btn.id='go-page';btn.textContent='Ir';btn.setAttribute('style','padding:8px 12px;border-radius:6px;background:#374151;color:#fff;margin-left:8px');var rng=document.createElement('div');rng.textContent='Rango: 1–'+"{lp}";rng.setAttribute('style','color:#9ca3af;margin-left:8px');toolbar.appendChild(inp);toolbar.appendChild(btn);toolbar.appendChild(rng);}catch(e){}
    document.addEventListener('keydown',function(e){if((e.ctrlKey||e.metaKey)&&e.key.toLowerCase()==='k'){e.preventDefault();openK()}if(e.key==='['){var pl=document.querySelector('a[href*="page="]');var prev=document.querySelector('a[href*="page="][href*="Anterior"],#prev-link');var link=document.getElementById('prev-link');if(link){link.click()}}if(e.key===']'){var link2=document.getElementById('next-link');if(link2){link2.click()}}});
    var eo=document.getElementById('edit-overlay');var ec=document.getElementById('edit-close');var es=document.getElementById('edit-save');var en=document.getElementById('edit-name');var su=document.getElementById('edit-sub');var st=document.getElementById('edit-status');var ei=document.getElementById('edit-id');if(ec){ec.addEventListener('click',function(){if(eo){eo.style.display='none'}})}var rows=document.querySelectorAll('tr[data-gym-id]');for(var i=0;i<rows.length;i++){(function(r){var gid=r.getAttribute('data-gym-id');var ed=r.querySelector('a[data-edit]');if(ed){ed.addEventListener('click',function(ev){ev.preventDefault();var nombre=r.children[1].textContent||'';var sub=r.children[2].textContent||'';if(en)en.value=nombre;if(su)su.value=sub;if(ei)ei.value=gid;if(eo)eo.style.display='flex';})}})(rows[i])}
    function _updateEditStatus(){var v=su?su.value:'';v=String(v||'').trim().toLowerCase();if(!v){if(st){st.textContent=''}return}fetch('/admin/subdomains/check?sub='+encodeURIComponent(v),{headers:{'accept':'application/json'}}).then(function(r){return r.json()}).then(function(j){if(st){if(j&&j.available){st.textContent='Disponible';st.style.color='#16a34a'}else{st.textContent='Ocupado';st.style.color='#ef4444'}}}).catch(function(){})}
    if(su){su.addEventListener('input',_updateEditStatus);_updateEditStatus()}
    if(es){es.addEventListener('click',function(){var gid=ei?ei.value:'';var form=new URLSearchParams();if(en&&en.value){form.append('nombre',en.value)}if(su&&su.value){form.append('subdominio',su.value)}fetch('/admin/gyms/'+gid+'/update',{method:'POST',headers:{'accept':'application/json','content-type':'application/x-www-form-urlencoded'},body:form.toString()}).then(function(r){return r.json().then(function(j){return {ok:r.ok, body:j}})}).then(function(res){if(res.ok&&res.body&&res.body.ok){toast('Actualizado','success');if(eo){eo.style.display='none'}var row=document.querySelector('tr[data-gym-id="'+gid+'"]');if(row){if(en&&en.value){row.children[1].textContent=en.value}if(su&&su.value){row.children[2].textContent=su.value}}}else{toast('Error al actualizar','error')}}).catch(function(){toast('Error al actualizar','error')})})}
    var gp=document.getElementById('go-page');if(gp){gp.addEventListener('click',function(){var inp=document.getElementById('page-jump');var v=inp&&inp.value?parseInt(inp.value,10):NaN;if(!isNaN(v)&&v>0){var url='/admin/gyms?ui=1&page='+v+'&page_size='+"{ps}"+'&q='+encodeURIComponent("{q}")+'&status='+'"+"{status_q}"+"'+'&order_by='+'"+"{order_by}"+"'+'&order_dir='+'"+"{order_dir}"+"';window.location.href=url}})}
    document.addEventListener('submit',function(e){var t=e.target;try{if(t&&t.tagName==='FORM'){var a=t.getAttribute('action')||'';var need=false;if(a.indexOf('/provision')>=0||a.indexOf('/suspend')>=0||a.indexOf('/unsuspend')>=0||a.indexOf('/delete-bucket')>=0||a.indexOf('/b2/regenerate-key')>=0||a.indexOf('/delete')>=0||a.match(new RegExp('^/admin/gyms/\\d+$'))){need=true}if(need){var ok=window.confirm('¿Confirmar la acción?');if(!ok){e.preventDefault();return false}}}}catch(err){}});
    var rows=document.querySelectorAll('tr[data-gym-id]');for(var i=0;i<rows.length;i++){(function(r){var sub=r.children[2];if(sub){sub.style.cursor='copy';sub.addEventListener('click',function(){var tx=sub.textContent||'';navigator.clipboard&&navigator.clipboard.writeText?navigator.clipboard.writeText(tx).then(function(){toast('Copiado: '+tx,'info')}).catch(function(){toast('No se pudo copiar','error')}):toast(tx,'info')})}})(rows[i])}
    });
    </script>
    """
    html = html + js
    if use_grid:
        html = html_grid + js_grid
    try:
        start = html.find('<main')
        if start >= 0:
            start = html.find('>', start) + 1
            end = html.rfind('</main>')
            if end > start:
                html = _admin_wrap(html[start:end])
            else:
                html = _admin_wrap(html)
        else:
            html = _admin_wrap(html)
    except Exception:
        html = _admin_wrap(html)

@admin_app.post("/gyms")
async def crear_gimnasio(request: Request, background_tasks: BackgroundTasks, nombre: str = Form(...), subdominio: Optional[str] = Form(None), owner_phone: Optional[str] = Form(None), whatsapp_phone_id: Optional[str] = Form(None), whatsapp_access_token: Optional[str] = Form(None), whatsapp_business_account_id: Optional[str] = Form(None), whatsapp_verify_token: Optional[str] = Form(None), whatsapp_app_secret: Optional[str] = Form(None), whatsapp_nonblocking: Optional[bool] = Form(False), whatsapp_send_timeout_seconds: Optional[str] = Form(None)):
    _require_admin(request)
    rl = _check_rate_limit(request, "gym_create", 20, 60)
    if rl:
        return rl
    adm = _get_admin_db()
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
    provision = None
    provisioning_ok = False
    if gid:
        try:
            provision = adm.provisionar_recursos(int(gid))
            provisioning_ok = bool((provision or {}).get("ok"))
        except Exception:
            provisioning_ok = False
        try:
            adm._push_whatsapp_to_gym_db(int(gid))
        except Exception:
            pass
    out = dict(res)
    out["provisioning"] = bool(provisioning_ok)
    if provision is not None:
        out["provision"] = provision
    return JSONResponse(out, status_code=201)

@admin_app.get("/subdomains/check")
async def check_subdomain(request: Request, sub: Optional[str] = None, name: Optional[str] = None):
    _require_admin(request)
    adm = _get_admin_db()
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
async def update_gym(request: Request, gym_id: int, nombre: Optional[str] = Form(None), subdominio: Optional[str] = Form(None)):
    _require_admin(request)
    rl = _check_rate_limit(request, "gym_update", 60, 60)
    if rl:
        return rl
    adm = _get_admin_db()
    if adm is None:
        return JSONResponse({"error": "DB admin no disponible"}, status_code=500)
    nm = (nombre or "").strip()
    sd = (subdominio or "").strip().lower()
    if sd:
        try:
            av = bool(adm.subdominio_disponible(sd))
        except Exception:
            av = False
        if not av:
            return JSONResponse({"ok": False, "error": "subdominio_in_use"}, status_code=400)
    res = adm.actualizar_gimnasio(int(gym_id), nm or None, sd or None)
    try:
        adm.log_action("owner", "update_gym", int(gym_id), f"{nm}|{sd}")
    except Exception:
        pass
    sc = 200 if bool(res.get("ok")) else 400
    return JSONResponse(res, status_code=sc)

@admin_app.post("/gyms/{gym_id}/contact")
async def actualizar_contacto(request: Request, gym_id: int, owner_phone: Optional[str] = Form(None)):
    _require_admin(request)
    adm = _get_admin_db()
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
    adm = _get_admin_db()
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
    adm = _get_admin_db()
    if adm is None:
        return JSONResponse({"error": "DB admin no disponible"}, status_code=500)
    ok = adm.set_estado_gimnasio(int(gym_id), "suspended", bool(hard), until, reason)
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
    adm = _get_admin_db()
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
    adm = _get_admin_db()
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
    adm = _get_admin_db()
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

@admin_app.get("/gyms/{gym_id}/maintenance")
async def mantenimiento_form(request: Request, gym_id: int):
    _require_admin(request)
    content = """
    <div style=\"max-width:640px;margin:0 auto\"><h1 style=\"font-size:24px;font-weight:600\">Modo mantenimiento</h1><div style=\"margin:12px 0\"><a href=\"/admin/gyms?ui=1\" style=\"padding:8px 12px;border-radius:6px;background:#111;color:#fff;text-decoration:none\">Volver</a></div><form method=\"post\" action=\"/admin/gyms/{gid}/maintenance\" style=\"display:grid;gap:8px\"><textarea name=\"message\" placeholder=\"Mensaje para usuarios\" style=\"padding:8px;border:1px solid #333;border-radius:6px;height:120px\"></textarea><button type=\"submit\" style=\"padding:10px 14px;border-radius:6px;background:#f59e0b;color:#000\">Activar mantenimiento</button></form><form method=\"post\" action=\"/admin/gyms/{gid}/maintenance/clear\" style=\"margin-top:12px\"><button type=\"submit\" style=\"padding:10px 14px;border-radius:6px;background:#16a34a;color:#fff\">Desactivar mantenimiento</button></form></div>
    """.replace("{gid}", str(int(gym_id)))
    html = _admin_wrap(content)
    return templates.TemplateResponse(
        "gym-settings.html",
        {"request": request, "section": "maintenance", "gid": int(gym_id)},
    )

@admin_app.post("/gyms/{gym_id}/maintenance")
async def activar_mantenimiento(request: Request, gym_id: int, message: Optional[str] = Form(None)):
    _require_admin(request)
    rl = _check_rate_limit(request, "maintenance_on", 20, 60)
    if rl:
        return rl
    adm = _get_admin_db()
    if adm is None:
        return JSONResponse({"error": "DB admin no disponible"}, status_code=500)
    ok = adm.set_mantenimiento(int(gym_id), message)
    try:
        adm.log_action("owner", "maintenance_on", int(gym_id), message or "")
    except Exception:
        pass
    return JSONResponse({"ok": bool(ok)}, status_code=200)

@admin_app.post("/gyms/{gym_id}/maintenance/clear")
async def desactivar_mantenimiento(request: Request, gym_id: int):
    _require_admin(request)
    rl = _check_rate_limit(request, "maintenance_off", 20, 60)
    if rl:
        return rl
    adm = _get_admin_db()
    if adm is None:
        return JSONResponse({"error": "DB admin no disponible"}, status_code=500)
    ok = adm.clear_mantenimiento(int(gym_id))
    try:
        adm.log_action("owner", "maintenance_off", int(gym_id), None)
    except Exception:
        pass
    return JSONResponse({"ok": bool(ok)}, status_code=200)

@admin_app.post("/gyms/{gym_id}/maintenance/schedule")
async def programar_mantenimiento(request: Request, gym_id: int, until: Optional[str] = Form(None), message: Optional[str] = Form(None)):
    _require_admin(request)
    rl = _check_rate_limit(request, "maintenance_schedule", 20, 60)
    if rl:
        return rl
    adm = _get_admin_db()
    if adm is None:
        return JSONResponse({"error": "DB admin no disponible"}, status_code=500)
    ok = adm.schedule_mantenimiento(int(gym_id), until, message)
    try:
        adm.log_action("owner", "maintenance_schedule", int(gym_id), {"until": until, "message": message})
    except Exception:
        pass
    return JSONResponse({"ok": bool(ok)}, status_code=200)

@admin_app.post("/gyms/{gym_id}/maintenance/notify")
async def avisar_mantenimiento_gym(request: Request, gym_id: int, message: Optional[str] = Form(None)):
    _require_admin(request)
    rl = _check_rate_limit(request, "maintenance_notify_one", 40, 60)
    if rl:
        return rl
    adm = _get_admin_db()
    if adm is None:
        return JSONResponse({"error": "DB admin no disponible"}, status_code=500)
    g = adm.obtener_gimnasio(int(gym_id))
    if not g:
        return JSONResponse({"error": "gym_not_found"}, status_code=404)
    nombre = str(g.get("nombre") or "")
    razon = str(g.get("suspended_reason") or "")
    hasta = str(g.get("suspended_until") or "")
    base_msg = message or (f"Hola {nombre}, el gimnasio está en mantenimiento. {razon}" + (f" Hasta: {hasta}" if hasta else ""))
    res = _send_whatsapp_text_for_gym(adm, int(gym_id), base_msg)
    try:
        adm.log_action("owner", "send_maintenance_notice", int(gym_id), base_msg)
    except Exception:
        pass
    sc = 200 if res.get("ok") else 400
    return JSONResponse(res, status_code=sc)

@admin_app.post("/admin/gyms/maintenance/notify/batch")
async def avisar_mantenimiento_batch(request: Request, gym_ids: str = Form(...), message: Optional[str] = Form(None)):
    _require_admin(request)
    rl = _check_rate_limit(request, "maintenance_notify_batch", 60, 60)
    if rl:
        return rl
    adm = _get_admin_db()
    if adm is None:
        return JSONResponse({"error": "DB admin no disponible"}, status_code=500)
    ids = [int(x) for x in (gym_ids or "").split(",") if x.strip()]
    results = []
    for gid in ids:
        g = adm.obtener_gimnasio(int(gid))
        nombre = str((g or {}).get("nombre") or "")
        razon = str((g or {}).get("suspended_reason") or "")
        hasta = str((g or {}).get("suspended_until") or "")
        base_msg = message or (f"Hola {nombre}, el gimnasio está en mantenimiento. {razon}" + (f" Hasta: {hasta}" if hasta else ""))
        res = _send_whatsapp_text_for_gym(adm, int(gid), base_msg)
        try:
            adm.log_action("owner", "send_maintenance_notice", int(gid), base_msg)
        except Exception:
            pass
        results.append({"gym_id": gid, "ok": bool(res.get("ok")), "status": res.get("status")})
    return JSONResponse({"ok": True, "count": len(results), "results": results}, status_code=200)

@admin_app.get("/gyms/{gym_id}/payments")
async def listar_pagos_gym(request: Request, gym_id: int):
    _require_admin(request)
    adm = _get_admin_db()
    if adm is None:
        return JSONResponse({"error": "DB admin no disponible"}, status_code=500)
    items = adm.listar_pagos(int(gym_id))
    accept = (request.headers.get("accept") or "").lower()
    wants_html = ("text/html" in accept) or (request.query_params.get("ui") == "1")
    if not wants_html:
        return JSONResponse({"items": items}, status_code=200)
    return templates.TemplateResponse(
        "gym-settings.html",
        {"request": request, "section": "payments", "items": items, "gid": int(gym_id)},
    )

@admin_app.post("/gyms/{gym_id}/payments")
async def registrar_pago_gym(request: Request, gym_id: int, plan: Optional[str] = Form(None), amount: Optional[str] = Form(None), currency: Optional[str] = Form(None), valid_until: Optional[str] = Form(None), status: Optional[str] = Form(None), notes: Optional[str] = Form(None)):
    _require_admin(request)
    adm = _get_admin_db()
    if adm is None:
        return JSONResponse({"error": "DB admin no disponible"}, status_code=500)
    amt = None
    try:
        raw = (amount or "").strip()
        if raw:
            try:
                amt = float(raw)
            except Exception:
                amt = None
    except Exception:
        amt = None
    ok = adm.registrar_pago(int(gym_id), plan, amt, currency, valid_until, status, notes)
    try:
        details = f"{plan}|{amt}|{currency}|{valid_until}|{status}"
        adm.log_action("owner", "register_payment", int(gym_id), details)
    except Exception:
        pass
    return JSONResponse({"ok": bool(ok)}, status_code=200)

@admin_app.get("/plans")
async def listar_planes(request: Request):
    gr = _guard_html_login_redirect(request)
    if gr:
        return gr
    rl = _check_rate_limit(request, "plans_list", 60, 60)
    if rl:
        return rl
    adm = _get_admin_db()
    if adm is None:
        return JSONResponse({"error": "DB admin no disponible"}, status_code=500)
    items = adm.listar_planes()
    accept = (request.headers.get("accept") or "").lower()
    wants_html = ("text/html" in accept) or (request.query_params.get("ui") == "1")
    if not wants_html:
        return JSONResponse({"items": items}, status_code=200)
    return templates.TemplateResponse("plans.html", {"request": request, "items": items})

@admin_app.post("/plans")
async def crear_plan(request: Request, name: str = Form(...), amount: float = Form(...), currency: str = Form(...), period_days: int = Form(...)):
    _require_admin(request)
    rl = _check_rate_limit(request, "plans_change", 40, 60)
    if rl:
        return rl
    adm = _get_admin_db()
    if adm is None:
        return JSONResponse({"error": "DB admin no disponible"}, status_code=500)
    try:
        c = str(currency or "").strip().upper()
        if not c or len(c) not in (3, 4):
            return JSONResponse({"ok": False, "error": "invalid_currency"}, status_code=400)
    except Exception:
        return JSONResponse({"ok": False, "error": "invalid_currency"}, status_code=400)
    try:
        if float(amount) <= 0:
            return JSONResponse({"ok": False, "error": "invalid_amount"}, status_code=400)
        if int(period_days) <= 0:
            return JSONResponse({"ok": False, "error": "invalid_period"}, status_code=400)
    except Exception:
        return JSONResponse({"ok": False, "error": "invalid_numbers"}, status_code=400)
    ok = adm.crear_plan(name, amount, currency, period_days)
    try:
        adm.log_action("owner", "create_plan", None, f"{name}|{amount}|{currency}|{period_days}")
    except Exception:
        pass
    return JSONResponse({"ok": bool(ok)}, status_code=200)

@admin_app.post("/plans/{plan_id}")
async def actualizar_plan(request: Request, plan_id: int, name: Optional[str] = Form(None), amount: Optional[str] = Form(None), currency: Optional[str] = Form(None), period_days: Optional[str] = Form(None)):
    _require_admin(request)
    rl = _check_rate_limit(request, "plans_change", 40, 60)
    if rl:
        return rl
    adm = _get_admin_db()
    if adm is None:
        return JSONResponse({"error": "DB admin no disponible"}, status_code=500)
    if currency:
        try:
            c = str(currency or "").strip().upper()
            if not c or len(c) not in (3, 4):
                return JSONResponse({"ok": False, "error": "invalid_currency"}, status_code=400)
            currency = c
        except Exception:
            return JSONResponse({"ok": False, "error": "invalid_currency"}, status_code=400)
    amt = None
    if amount is not None:
        try:
            raw_amt = (amount or "").strip()
            if raw_amt:
                try:
                    v = float(raw_amt)
                except Exception:
                    return JSONResponse({"ok": False, "error": "invalid_amount"}, status_code=400)
                if v <= 0:
                    return JSONResponse({"ok": False, "error": "invalid_amount"}, status_code=400)
                amt = v
        except Exception:
            return JSONResponse({"ok": False, "error": "invalid_amount"}, status_code=400)
    pd = None
    if period_days is not None:
        try:
            raw_pd = (period_days or "").strip()
            if raw_pd:
                try:
                    vp = int(raw_pd)
                except Exception:
                    return JSONResponse({"ok": False, "error": "invalid_period"}, status_code=400)
                if vp <= 0:
                    return JSONResponse({"ok": False, "error": "invalid_period"}, status_code=400)
                pd = vp
        except Exception:
            return JSONResponse({"ok": False, "error": "invalid_period"}, status_code=400)
    ok = adm.actualizar_plan(int(plan_id), name, amt, currency, pd)
    try:
        adm.log_action("owner", "update_plan", None, f"{plan_id}")
    except Exception:
        pass
    return JSONResponse({"ok": bool(ok)}, status_code=200 if ok else 400)

@admin_app.post("/plans/{plan_id}/toggle")
async def toggle_plan(request: Request, plan_id: int, active: bool = Form(...)):
    _require_admin(request)
    rl = _check_rate_limit(request, "plans_change", 40, 60)
    if rl:
        return rl
    adm = _get_admin_db()
    if adm is None:
        return JSONResponse({"error": "DB admin no disponible"}, status_code=500)
    ok = adm.toggle_plan(int(plan_id), bool(active))
    try:
        adm.log_action("owner", "toggle_plan", None, f"{plan_id}|{active}")
    except Exception:
        pass
    return JSONResponse({"ok": bool(ok)}, status_code=200 if ok else 400)

@admin_app.get("/gyms/{gym_id}/subscription")
async def ver_subscription(request: Request, gym_id: int):
    _require_admin(request)
    adm = _get_admin_db()
    if adm is None:
        return JSONResponse({"error": "DB admin no disponible"}, status_code=500)
    sub = adm.obtener_subscription(int(gym_id))
    planes = adm.listar_planes()
    accept = (request.headers.get("accept") or "").lower()
    wants_html = ("text/html" in accept) or (request.query_params.get("ui") == "1")
    if not wants_html:
        return JSONResponse({"subscription": sub, "plans": planes}, status_code=200)
    return templates.TemplateResponse("gym-settings.html", {"request": request, "section": "subscription", "gid": int(gym_id), "subscription": sub, "plans": planes})

@admin_app.post("/gyms/{gym_id}/subscription")
async def set_subscription(request: Request, gym_id: int, plan_id: int = Form(...), start_date: str = Form(...)):
    _require_admin(request)
    adm = _get_admin_db()
    if adm is None:
        return JSONResponse({"error": "DB admin no disponible"}, status_code=500)
    ok = adm.set_subscription(int(gym_id), int(plan_id), start_date)
    try:
        adm.log_action("owner", "set_subscription", int(gym_id), f"{plan_id}|{start_date}")
    except Exception:
        pass
    return JSONResponse({"ok": bool(ok)}, status_code=200)

def _send_whatsapp_text_for_gym(adm: AdminDatabaseManager, gym_id: int, text: str) -> Dict[str, Any]:
    g = adm.obtener_gimnasio(int(gym_id))
    if not g:
        return {"ok": False, "error": "gym_not_found"}
    phone_id = str(g.get("whatsapp_phone_id") or "").strip()
    access_token = str(g.get("whatsapp_access_token") or "").strip()
    to_number = str(g.get("owner_phone") or "").strip()
    if not phone_id or not access_token or not to_number:
        return {"ok": False, "error": "missing_credentials_or_phone"}
    if requests is None:
        return {"ok": False, "error": "requests_not_available"}
    try:
        url = f"https://graph.facebook.com/v17.0/{phone_id}/messages"
        payload = {"messaging_product": "whatsapp", "to": to_number, "type": "text", "text": {"body": text}}
        headers = {"Authorization": f"Bearer {access_token}", "Content-Type": "application/json"}
        r = requests.post(url, json=payload, headers=headers, timeout=10)
        ok = 200 <= r.status_code < 300
        return {"ok": ok, "status": r.status_code, "response": r.json() if hasattr(r, "json") else None}
    except Exception as e:
        return {"ok": False, "error": str(e)}

@admin_app.post("/gyms/{gym_id}/subscription/remind")
async def enviar_recordatorio_gym(request: Request, gym_id: int, message: Optional[str] = Form(None)):
    _require_admin(request)
    rl = _check_rate_limit(request, "remind_one", 40, 60)
    if rl:
        return rl
    adm = _get_admin_db()
    if adm is None:
        return JSONResponse({"error": "DB admin no disponible"}, status_code=500)
    sub = adm.obtener_subscription(int(gym_id))
    g = adm.obtener_gimnasio(int(gym_id))
    if not g:
        return JSONResponse({"error": "gym_not_found"}, status_code=404)
    nombre = str(g.get("nombre") or "")
    nd = ""
    if sub and sub.get("next_due_date"):
        nd = str(sub.get("next_due_date"))
    base_msg = message or f"Hola {nombre}, tu suscripción de GymMS vence el {nd}. Por favor realiza el pago para evitar suspensión."
    res = _send_whatsapp_text_for_gym(adm, int(gym_id), base_msg)
    try:
        adm.log_action("owner", "send_subscription_reminder", int(gym_id), base_msg)
    except Exception:
        pass
    sc = 200 if res.get("ok") else 400
    return JSONResponse(res, status_code=sc)

@admin_app.get("/subscriptions/upcoming")
async def subs_upcoming(request: Request, days: Optional[int] = None):
    _require_admin(request)
    adm = _get_admin_db()
    if adm is None:
        return JSONResponse({"error": "DB admin no disponible"}, status_code=500)
    d = int(days or 14)
    items = adm.listar_proximos_vencimientos(d)
    accept = (request.headers.get("accept") or "").lower()
    wants_html = ("text/html" in accept) or (request.query_params.get("ui") == "1")
    if not wants_html:
        return JSONResponse({"items": items, "days": d}, status_code=200)
    return templates.TemplateResponse(
        "subscriptions.html",
        {"request": request, "mode": "upcoming", "items": items, "days": int(d)},
    )

@admin_app.post("/subscriptions/auto-suspend")
async def auto_suspend_overdue(request: Request, grace_days: int = Form(...)):
    _require_admin(request)
    rl = _check_rate_limit(request, "auto_suspend_overdue", 10, 60)
    if rl:
        return rl
    adm = _get_admin_db()
    if adm is None:
        return JSONResponse({"error": "DB admin no disponible"}, status_code=500)
    cnt = adm.auto_suspend_overdue(int(grace_days))
    try:
        adm.log_action("owner", "auto_suspend_overdue", None, {"grace_days": int(grace_days), "suspended": int(cnt)})
    except Exception:
        pass
    return JSONResponse({"ok": True, "suspended": int(cnt)}, status_code=200)

@admin_app.post("/subscriptions/remind")
async def enviar_recordatorios_batch(request: Request, days: Optional[int] = Form(None)):
    _require_admin(request)
    rl = _check_rate_limit(request, "remind_batch", 60, 60)
    if rl:
        return rl
    adm = _get_admin_db()
    if adm is None:
        return JSONResponse({"error": "DB admin no disponible"}, status_code=500)
    d = int(days or 3)
    items = adm.listar_proximos_vencimientos(d)
    sent: List[Dict[str, Any]] = []
    for it in items:
        gid = int(it.get("gym_id") or 0)
        nombre = str(it.get("nombre") or "")
        nd = str(it.get("next_due_date") or "")
        msg = f"Hola {nombre}, tu suscripción de GymMS vence el {nd}. Por favor realiza el pago para evitar suspensión."
        res = _send_whatsapp_text_for_gym(adm, gid, msg)
        try:
            adm.log_action("owner", "send_subscription_reminder", gid, msg)
        except Exception:
            pass
        sent.append({"gym_id": gid, "ok": bool(res.get("ok")), "status": res.get("status")})
    return JSONResponse({"ok": True, "count": len(sent), "results": sent}, status_code=200)

@admin_app.get("/subscriptions/dashboard")
async def subs_dashboard(request: Request, days: Optional[int] = None, q: Optional[str] = None):
    _require_admin(request)
    adm = _get_admin_db()
    if adm is None:
        return JSONResponse({"error": "DB admin no disponible"}, status_code=500)
    try:
        d = int(days or int(request.query_params.get("days") or 14))
    except Exception:
        d = 14
    items = adm.listar_proximos_vencimientos(d)
    accept = (request.headers.get("accept") or "").lower()
    wants_html = ("text/html" in accept) or (request.query_params.get("ui") == "1")
    query = (q or request.query_params.get("q") or "").strip().lower()
    if query:
        items = [it for it in items if query in str(it.get('nombre') or '').lower() or query in str(it.get('subdominio') or '').lower()]
    if not wants_html:
        return JSONResponse({"items": items, "days": d, "q": query}, status_code=200)
    return templates.TemplateResponse(
        "subscriptions.html",
        {"request": request, "mode": "dashboard", "items": items, "days": int(d), "q": (query or "")},
    )

@admin_app.post("/subscriptions/remind-selected")
async def enviar_recordatorios_seleccionados(request: Request, message: Optional[str] = Form(None)):
    _require_admin(request)
    rl = _check_rate_limit(request, "remind_selected", 60, 60)
    if rl:
        return rl
    adm = _get_admin_db()
    if adm is None:
        return JSONResponse({"error": "DB admin no disponible"}, status_code=500)
    try:
        form = await request.form()
    except Exception:
        form = {}
    raw = form.get("gym_ids")
    ids: List[int] = []
    if isinstance(raw, list):
        for v in raw:
            try:
                ids.append(int(str(v)))
            except Exception:
                continue
    elif isinstance(raw, str):
        for part in raw.split(","):
            try:
                ids.append(int(part.strip()))
            except Exception:
                continue
    sent: List[Dict[str, Any]] = []
    for gid in ids:
        sub = adm.obtener_subscription(int(gid))
        g = adm.obtener_gimnasio(int(gid))
        nombre = str((g or {}).get("nombre") or "")
        nd = str((sub or {}).get("next_due_date") or "")
        msg = message or f"Hola {nombre}, tu suscripción de GymMS vence el {nd}. Por favor realiza el pago para evitar suspensión."
        res = _send_whatsapp_text_for_gym(adm, int(gid), msg)
        try:
            adm.log_action("owner", "send_subscription_reminder", int(gid), msg)
        except Exception:
            pass
        sent.append({"gym_id": int(gid), "ok": bool(res.get("ok")), "status": res.get("status")})
    return JSONResponse({"ok": True, "count": len(sent), "results": sent}, status_code=200)

@admin_app.get("/cron/daily-reminders")
async def cron_daily_reminders(request: Request, token: Optional[str] = None, days: Optional[int] = None):
    t = (token or request.query_params.get("token") or "").strip()
    sec = os.getenv("CRON_SECRET", "").strip() or os.getenv("ADMIN_SECRET", "").strip()
    if not sec or t != sec:
        raise HTTPException(status_code=401, detail="Unauthorized")
    adm = _get_admin_db()
    if adm is None:
        return JSONResponse({"error": "DB admin no disponible"}, status_code=500)
    d = int(days or 3)
    items = adm.listar_proximos_vencimientos(d)
    sent = 0
    for it in items:
        gid = int(it.get("gym_id") or 0)
        nombre = str(it.get("nombre") or "")
        nd = str(it.get("next_due_date") or "")
        msg = f"Hola {nombre}, tu suscripción de GymMS vence el {nd}. Por favor realiza el pago para evitar suspensión."
        res = _send_whatsapp_text_for_gym(adm, gid, msg)
        if res.get("ok"):
            sent += 1
        try:
            adm.log_action("owner", "cron_send_subscription_reminder", gid, nd)
        except Exception:
            pass
    try:
        grace = int(os.getenv("AUTO_SUSPEND_GRACE_DAYS", "5"))
    except Exception:
        grace = 5
    auto = adm.auto_suspend_overdue(grace)
    return JSONResponse({"ok": True, "days": d, "count": sent, "auto_suspended": int(auto), "grace_days": int(grace)}, status_code=200)
@admin_app.post("/gyms/{gym_id}/b2/regenerate-key")
async def b2_regenerate_key(request: Request, gym_id: int):
    _require_admin(request)
    adm = _get_admin_db()
    if adm is None:
        return JSONResponse({"error": "DB admin no disponible"}, status_code=500)
    res = adm.regenerar_clave_b2(int(gym_id))
    try:
        adm.log_action("owner", "b2_regenerate_key", int(gym_id), None)
    except Exception:
        pass
    return JSONResponse(res, status_code=200 if res.get("ok") else 400)

@admin_app.post("/gyms/{gym_id}/b2/delete-bucket")
async def b2_delete_bucket(request: Request, gym_id: int):
    _require_admin(request)
    adm = _get_admin_db()
    if adm is None:
        return JSONResponse({"error": "DB admin no disponible"}, status_code=500)
    ok = adm.eliminar_bucket_gym(int(gym_id))
    try:
        adm.log_action("owner", "b2_delete_bucket", int(gym_id), None)
    except Exception:
        pass
    return JSONResponse({"ok": bool(ok)}, status_code=200 if ok else 400)

@admin_app.post("/gyms/{gym_id}/delete")
async def delete_gym_post(request: Request, gym_id: int):
    _require_admin(request)
    rl = _check_rate_limit(request, "gym_delete", 10, 300)
    if rl:
        return rl
    adm = _get_admin_db()
    if adm is None:
        return JSONResponse({"error": "DB admin no disponible"}, status_code=500)
    ok = adm.eliminar_gimnasio(int(gym_id))
    try:
        adm.log_action("owner", "delete_gym", int(gym_id), None)
    except Exception:
        pass
    return JSONResponse({"ok": bool(ok)}, status_code=200 if ok else 400)
@admin_app.get("/gyms/{gym_id}/owner")
async def owner_password_form(request: Request, gym_id: int):
    _require_admin(request)
    adm = _get_admin_db()
    if adm is None:
        return JSONResponse({"error": "DB admin no disponible"}, status_code=500)
    g = adm.obtener_gimnasio(int(gym_id))
    if not g:
        return JSONResponse({"error": "gym_not_found"}, status_code=404)
    sub = str(g.get("subdominio") or "")
    return templates.TemplateResponse(
        "passwords.html",
        {"request": request, "mode": "owner", "gid": int(gym_id), "sub": sub},
    )

@admin_app.post("/gyms/{gym_id}/owner")
async def owner_password_save(request: Request, gym_id: int, new_password: str = Form(...), confirm_password: str = Form(...)):
    _require_admin(request)
    rl = _check_rate_limit(request, "owner_pwd_change", 20, 60)
    if rl:
        return rl
    if (new_password or "").strip() != (confirm_password or "").strip():
        return JSONResponse({"ok": False, "error": "mismatch"}, status_code=400)
    adm = _get_admin_db()
    if adm is None:
        return JSONResponse({"error": "DB admin no disponible"}, status_code=500)
    ok = adm.set_gym_owner_password(int(gym_id), new_password)
    try:
        adm.log_action("owner", "set_owner_password", int(gym_id), None)
    except Exception:
        pass
    return JSONResponse({"ok": bool(ok)}, status_code=200 if ok else 400)
@admin_app.get("/owner/password")
async def admin_owner_password_form(request: Request):
    _require_admin(request)
    html = """
    <div style=\"max-width:480px;margin:0 auto;padding:20px;font-family:system-ui\"><h1 style=\"font-size:22px;font-weight:600\">Cambiar contraseña Admin</h1><div style=\"margin:12px 0\"><a href=\"/admin/\" style=\"padding:8px 12px;border-radius:6px;background:#111;color:#fff;text-decoration:none\">Inicio</a></div><form method=\"post\" action=\"/admin/owner/password\" style=\"display:grid;gap:8px\"><input type=\"password\" name=\"current_password\" placeholder=\"Contraseña actual\" required style=\"padding:8px;border:1px solid #333;border-radius:6px\"/><input type=\"password\" name=\"new_password\" placeholder=\"Nueva contraseña\" required style=\"padding:8px;border:1px solid #333;border-radius:6px\"/><input type=\"password\" name=\"confirm_password\" placeholder=\"Confirmar contraseña\" required style=\"padding:8px;border:1px solid #333;border-radius:6px\"/><button type=\"submit\" style=\"padding:10px 14px;border-radius:6px;background:#111;color:#fff\">Guardar</button></form></div>
    """
    html = _admin_wrap(html)
    return templates.TemplateResponse(
        "passwords.html",
        {"request": request, "mode": "admin"},
    )

@admin_app.post("/owner/password")
async def admin_owner_password_save(request: Request, current_password: str = Form(...), new_password: str = Form(...), confirm_password: str = Form(...)):
    _require_admin(request)
    rl = _check_rate_limit(request, "admin_pwd_change", 10, 300)
    if rl:
        return rl
    adm = _get_admin_db()
    if adm is None:
        return JSONResponse({"error": "DB admin no disponible"}, status_code=500)
    if (new_password or "").strip() != (confirm_password or "").strip():
        return JSONResponse({"ok": False, "error": "mismatch"}, status_code=400)
    if not adm.verificar_owner_password(current_password):
        return JSONResponse({"ok": False, "error": "wrong_current"}, status_code=401)
    ok = adm.set_admin_owner_password(new_password)
    try:
        adm.log_action("owner", "set_admin_owner_password", None, None)
    except Exception:
        pass
    return JSONResponse({"ok": bool(ok)}, status_code=200 if ok else 400)

@admin_app.post("/owner/password/reset")
async def admin_owner_password_reset(request: Request, new_password: str = Form(None)):
    hdr = request.headers.get("x-admin-secret") or ""
    secret = os.getenv("ADMIN_SECRET", "").strip()
    if not secret or hdr.strip() != secret:
        return JSONResponse({"error": "unauthorized"}, status_code=401)
    adm = _get_admin_db()
    if adm is None:
        return JSONResponse({"error": "DB admin no disponible"}, status_code=500)
    pwd = (new_password or os.getenv("ADMIN_INITIAL_PASSWORD", "")).strip()
    if not pwd:
        return JSONResponse({"error": "missing_password"}, status_code=400)
    ok = adm.set_admin_owner_password(pwd)
    if not ok:
        try:
            adm._ensure_owner_user()
            ok = adm.set_admin_owner_password(pwd)
        except Exception:
            ok = False
    if not ok:
        try:
            with adm.db.get_connection_context() as conn:  # type: ignore
                cur = conn.cursor()
                try:
                    cur.execute("CREATE TABLE IF NOT EXISTS admin_users (id BIGSERIAL PRIMARY KEY, username TEXT UNIQUE NOT NULL, password_hash TEXT NOT NULL, created_at TIMESTAMP WITHOUT TIME ZONE DEFAULT NOW())")
                except Exception:
                    pass
                cur.execute("SELECT id FROM admin_users WHERE username = %s", ("owner",))
                row = cur.fetchone()
                ph = adm._hash_password(pwd)
                if not row:
                    cur.execute("INSERT INTO admin_users (username, password_hash) VALUES (%s, %s)", ("owner", ph))
                else:
                    cur.execute("UPDATE admin_users SET password_hash = %s WHERE username = %s", (ph, "owner"))
                conn.commit()
                ok = True
        except Exception:
            ok = False
    try:
        adm.log_action("system", "admin_owner_password_reset", None, None)
    except Exception:
        pass
    return JSONResponse({"ok": bool(ok)}, status_code=200 if ok else 400)

@admin_app.get("/secret-login")
async def admin_secret_login(request: Request, token: str):
    secret = os.getenv("ADMIN_SECRET", "").strip()
    if not secret or (str(token or "").strip() != secret):
        return JSONResponse({"error": "unauthorized"}, status_code=401)
    try:
        request.session["admin_logged_in"] = True
        try:
            request.session["session_version"] = int(getattr(admin_app.state, "session_version", 1))
        except Exception:
            request.session["session_version"] = 1
    except Exception:
        pass
    try:
        adm = _get_admin_db()
        if adm:
            adm.log_action("owner", "secret_login", None, None)
    except Exception:
        pass
    return RedirectResponse(url="/admin", status_code=303)

@admin_app.get("/owner/password/reset")
async def admin_owner_password_reset_get(request: Request, token: str, new: str | None = None):
    secret = os.getenv("ADMIN_SECRET", "").strip()
    if not secret or str(token or "").strip() != secret:
        return JSONResponse({"error": "unauthorized"}, status_code=401)
    adm = _get_admin_db()
    if adm is None:
        return JSONResponse({"error": "DB admin no disponible"}, status_code=500)
    pwd = (new or os.getenv("ADMIN_INITIAL_PASSWORD", "")).strip()
    if not pwd:
        return JSONResponse({"error": "missing_password"}, status_code=400)
    ok = adm.set_admin_owner_password(pwd)
    if not ok:
        try:
            adm._ensure_owner_user()
            ok = adm.set_admin_owner_password(pwd)
        except Exception:
            ok = False
    if not ok:
        try:
            with adm.db.get_connection_context() as conn:  # type: ignore
                cur = conn.cursor()
                try:
                    cur.execute("CREATE TABLE IF NOT EXISTS admin_users (id BIGSERIAL PRIMARY KEY, username TEXT UNIQUE NOT NULL, password_hash TEXT NOT NULL, created_at TIMESTAMP WITHOUT TIME ZONE DEFAULT NOW())")
                except Exception:
                    pass
                cur.execute("SELECT id FROM admin_users WHERE username = %s", ("owner",))
                row = cur.fetchone()
                ph = adm._hash_password(pwd)
                if not row:
                    cur.execute("INSERT INTO admin_users (username, password_hash) VALUES (%s, %s)", ("owner", ph))
                else:
                    cur.execute("UPDATE admin_users SET password_hash = %s WHERE username = %s", (ph, "owner"))
                conn.commit()
                ok = True
        except Exception:
            ok = False
    if ok:
        return RedirectResponse(url="/admin/login?ui=1", status_code=303)
    return JSONResponse({"ok": False}, status_code=400)

@admin_app.post("/owner/password/hash")
async def admin_owner_password_set_hash(request: Request, password_hash: str = Form(...)):
    hdr = request.headers.get("x-admin-secret") or ""
    secret = os.getenv("ADMIN_SECRET", "").strip()
    if not secret or hdr.strip() != secret:
        return JSONResponse({"error": "unauthorized"}, status_code=401)
    adm = _get_admin_db()
    if adm is None:
        return JSONResponse({"error": "DB admin no disponible"}, status_code=500)
    try:
        with adm.db.get_connection_context() as conn:  # type: ignore
            cur = conn.cursor()
            try:
                cur.execute("CREATE TABLE IF NOT EXISTS admin_users (id BIGSERIAL PRIMARY KEY, username TEXT UNIQUE NOT NULL, password_hash TEXT NOT NULL, created_at TIMESTAMP WITHOUT TIME ZONE DEFAULT NOW())")
            except Exception:
                pass
            cur.execute("SELECT id FROM admin_users WHERE username = %s", ("owner",))
            row = cur.fetchone()
            if not row:
                cur.execute("INSERT INTO admin_users (username, password_hash) VALUES (%s, %s)", ("owner", password_hash))
            else:
                cur.execute("UPDATE admin_users SET password_hash = %s WHERE username = %s", (password_hash, "owner"))
            conn.commit()
    except Exception:
        return JSONResponse({"ok": False}, status_code=400)
    try:
        adm.log_action("system", "admin_owner_password_set_hash", None, None)
    except Exception:
        pass
    return JSONResponse({"ok": True}, status_code=200)
@admin_app.get("/gyms/{gym_id}/branding")
async def branding_form(request: Request, gym_id: int):
    _require_admin(request)
    adm = _get_admin_db()
    if adm is None:
        return JSONResponse({"error": "DB admin no disponible"}, status_code=500)
    g = adm.obtener_gimnasio(int(gym_id))
    if not g:
        return JSONResponse({"error": "gym_not_found"}, status_code=404)
    html = """
    <div class=\"dark\"><div style=\"max-width:720px;margin:0 auto;padding:20px;font-family:system-ui\"><h1 style=\"font-size:24px;font-weight:600\">Branding</h1><div style=\"margin:12px 0\"><a href=\"/admin/gyms?ui=1\" style=\"padding:8px 12px;border-radius:6px;background:#111;color:#fff;text-decoration:none\">Volver</a></div><div class=\"grid\" style=\"display:grid;grid-template-columns:1fr;gap:16px\"><form id=\"branding-form\" method=\"post\" action=\"/admin/gyms/{gid}/branding\" style=\"display:grid;gap:8px\"><input id=\"gym_name\" name=\"gym_name\" placeholder=\"Nombre público\" style=\"padding:8px;border:1px solid #333;border-radius:6px\"/><input id=\"gym_address\" name=\"gym_address\" placeholder=\"Dirección\" style=\"padding:8px;border:1px solid #333;border-radius:6px\"/><input id=\"logo_url\" name=\"logo_url\" placeholder=\"Logo URL\" style=\"padding:8px;border:1px solid #333;border-radius:6px\"/><div style=\"margin-top:12px\"><h2 style=\"font-size:18px\">Colores</h2><div style=\"display:grid;grid-template-columns:repeat(auto-fit,minmax(160px,1fr));gap:8px\"><input id=\"primary\" name=\"primary\" placeholder=\"#2b8a3e\" style=\"padding:8px;border:1px solid #333;border-radius:6px\"/><input id=\"secondary\" name=\"secondary\" placeholder=\"#1e3a8a\" style=\"padding:8px;border:1px solid #333;border-radius:6px\"/><input id=\"accent\" name=\"accent\" placeholder=\"#f59e0b\" style=\"padding:8px;border:1px solid #333;border-radius:6px\"/><input id=\"bg\" name=\"bg\" placeholder=\"#0f172a\" style=\"padding:8px;border:1px solid #333;border-radius:6px\"/><input id=\"card\" name=\"card\" placeholder=\"#111827\" style=\"padding:8px;border:1px solid #333;border-radius:6px\"/><input id=\"text\" name=\"text\" placeholder=\"#e5e7eb\" style=\"padding:8px;border:1px solid #333;border-radius:6px\"/><input id=\"muted\" name=\"muted\" placeholder=\"#9ca3af\" style=\"padding:8px;border:1px solid #333;border-radius:6px\"/><input id=\"border\" name=\"border\" placeholder=\"#374151\" style=\"padding:8px;border:1px solid #333;border-radius:6px\"/></div></div><div style=\"margin-top:12px\"><h2 style=\"font-size:18px\">Tipografías</h2><div style=\"display:grid;grid-template-columns:repeat(auto-fit,minmax(200px,1fr));gap:8px\"><input id=\"font_base\" name=\"font_base\" placeholder=\"Inter, system-ui\" style=\"padding:8px;border:1px solid #333;border-radius:6px\"/><input id=\"font_heading\" name=\"font_heading\" placeholder=\"Inter, system-ui\" style=\"padding:8px;border:1px solid #333;border-radius:6px\"/></div></div><div class=\"actions\" style=\"display:flex;gap:8px;margin-top:8px\"><button id=\"btn-preview\" type=\"button\" style=\"padding:10px 14px;border-radius:6px;background:#374151;color:#fff\">Vista previa</button><button type=\"submit\" style=\"padding:10px 14px;border-radius:6px;background:#111;color:#fff\">Guardar</button></div><div id=\"errors\" style=\"margin-top:8px;color:#ef4444\"></div></form><div id=\"preview\" style=\"padding:20px;border-radius:16px;border:1px solid #333;background:#111827\"><div style=\"display:flex;align-items:center;gap:12px\"><div style=\"width:44px;height:44px;border-radius:8px;background:var(--accent,#f59e0b)\"></div><div><div style=\"font-weight:600;font-size:18px\">Vista previa</div><div style=\"color:#9ca3af\">Colores y tipografías aplicados</div></div></div><div style=\"margin-top:12px;display:flex;gap:8px\"><button id=\"btn-reset\" type=\"button\" style=\"padding:10px 14px;border-radius:6px;background:#4b5563;color:#fff\">Reset</button></div></div></div><script>(function(){function isHex(s){var x=(s||'').trim();return /^#([0-9a-fA-F]{6}|[0-9a-fA-F]{3})$/.test(x);}function applyPreview(){var m=['primary','secondary','accent','bg','card','text','muted','border'];var errs=[];var root=document.getElementById('preview');var css='';for(var i=0;i<m.length;i++){var id=m[i];var v=(document.getElementById(id)||{}).value||'';if(v){if(!isHex(v)){errs.push('Color inválido: '+id);}else{css+=id+'='+v+';';root.style.setProperty('--'+id,v);}}}var fb=(document.getElementById('font_base')||{}).value||'';var fh=(document.getElementById('font_heading')||{}).value||'';if(fb){root.style.setProperty('--font-base',fb);}if(fh){root.style.setProperty('--font-heading',fh);}var e=document.getElementById('errors');e.textContent=errs.join(' \u2022 ');}function resetPreview(){var m=['primary','secondary','accent','bg','card','text','muted','border'];var root=document.getElementById('preview');for(var i=0;i<m.length;i++){root.style.removeProperty('--'+m[i]);}root.style.removeProperty('--font-base');root.style.removeProperty('--font-heading');var e=document.getElementById('errors');e.textContent='';}var bp=document.getElementById('btn-preview');if(bp){bp.addEventListener('click',applyPreview);}var br=document.getElementById('btn-reset');if(br){br.addEventListener('click',resetPreview);}var form=document.getElementById('branding-form');if(form){form.addEventListener('submit',function(ev){var m=['primary','secondary','accent','bg','card','text','muted','border'];var errs=[];for(var i=0;i<m.length;i++){var id=m[i];var v=(document.getElementById(id)||{}).value||'';if(v && !isHex(v)){errs.push('Color inválido: '+id);}}var e=document.getElementById('errors');if(errs.length){ev.preventDefault();e.textContent=errs.join(' \u2022 ');}else{e.textContent='';}});} })();</script>
    """.replace("{gid}", str(int(gym_id)))
    html = _admin_wrap(html)
    return templates.TemplateResponse(
        "gym-settings.html",
        {"request": request, "section": "branding", "gid": int(gym_id), "gym": g},
    )

@admin_app.post("/gyms/{gym_id}/branding")
async def branding_save(request: Request, gym_id: int, gym_name: Optional[str] = Form(None), gym_address: Optional[str] = Form(None), logo_url: Optional[str] = Form(None), primary: Optional[str] = Form(None), secondary: Optional[str] = Form(None), accent: Optional[str] = Form(None), bg: Optional[str] = Form(None), card: Optional[str] = Form(None), text: Optional[str] = Form(None), muted: Optional[str] = Form(None), border: Optional[str] = Form(None), font_base: Optional[str] = Form(None), font_heading: Optional[str] = Form(None)):
    _require_admin(request)
    rl = _check_rate_limit(request, "branding_change", 40, 60)
    if rl:
        return rl
    adm = _get_admin_db()
    if adm is None:
        return JSONResponse({"error": "DB admin no disponible"}, status_code=500)
    ok_brand = adm.set_gym_branding(int(gym_id), gym_name, gym_address, logo_url)
    theme_payload = {
        "primary": primary,
        "secondary": secondary,
        "accent": accent,
        "bg": bg,
        "card": card,
        "text": text,
        "muted": muted,
        "border": border,
    }
    ok_theme = adm.set_gym_theme(int(gym_id), theme_payload, font_base, font_heading)
    try:
        adm.log_action("owner", "set_branding", int(gym_id), None)
    except Exception:
        pass
    return JSONResponse({"ok": bool(ok_brand and ok_theme)}, status_code=200 if (ok_brand and ok_theme) else 400)

@admin_app.get("/templates")
async def listar_templates(request: Request):
    gr = _guard_html_login_redirect(request)
    if gr:
        return gr
    names = [
        "aviso_de_confirmacion_de_pago_de_cuota_gimnasio_para_usuario_especifico_en_sistema_de_management_de_gimnasios_profesional",
        "aviso_de_vencimiento_de_cuota_gimnasio_para_usuario_especifico_en_sistema_de_management_de_gimnasios_profesional",
        "aviso_de_confirmacion_de_ingreso_a_gimnasio_para_usuario_especifico_en_sistema_de_management_de_gimnasios_profesional",
        "aviso_de_desactivacion_por_falta_de_pago_para_usuario_especifico_en_sistema_de_management_de_gimnasios_profesional",
        "aviso_de_recordatorio_de_horario_de_clase_para_usuario_especifico_en_sistema_de_management_de_gimnasios_profesional",
        "aviso_de_promocion_de_lista_de_espera_para_clase_para_usuario_especifico_en_sistema_de_management_de_gimnasios_profesional",
        "aviso_de_promocion_a_lista_principal_para_clase_para_usuario_especifico_en_sistema_de_management_de_gimnasios_profesional",
        "mensaje_de_bienvenida_a_gimnasio_para_usuario_especifico_en_sistema_de_management_de_gimnasios_profesional",
    ]
    accept = (request.headers.get("accept") or "").lower()
    wants_html = ("text/html" in accept) or (request.query_params.get("ui") == "1")
    name = (request.query_params.get("name") or "").strip()
    if not wants_html:
        return JSONResponse({"templates": names, "name": name or None, "text": (_template_text(name) if name else None)}, status_code=200)
    text = _template_text(name) if name else ""
    return templates.TemplateResponse("templates.html", {"request": request, "names": names, "name": name, "text": text})

def _template_text(name: str) -> str:
    n = (name or "").strip().lower()
    m = {
        "aviso_de_confirmacion_de_pago_de_cuota_gimnasio_para_usuario_especifico_en_sistema_de_management_de_gimnasios_profesional": "Tu pago ha sido confirmado. ¡Gracias por seguir entrenando!",
        "aviso_de_vencimiento_de_cuota_gimnasio_para_usuario_especifico_en_sistema_de_management_de_gimnasios_profesional": "Tu cuota vence pronto. Por favor realiza el pago para evitar suspensión.",
        "aviso_de_confirmacion_de_ingreso_a_gimnasio_para_usuario_especifico_en_sistema_de_management_de_gimnasios_profesional": "Ingreso registrado. ¡A darle con todo hoy!",
        "aviso_de_desactivacion_por_falta_de_pago_para_usuario_especifico_en_sistema_de_management_de_gimnasios_profesional": "Tu acceso fue desactivado por falta de pago. Contáctanos para reactivarlo.",
        "aviso_de_recordatorio_de_horario_de_clase_para_usuario_especifico_en_sistema_de_management_de_gimnasios_profesional": "Recordatorio: tienes clase agendada en breve. ¡Te esperamos!",
        "aviso_de_promocion_de_lista_de_espera_para_clase_para_usuario_especifico_en_sistema_de_management_de_gimnasios_profesional": "¡Buenas noticias! Saliste de lista de espera. Responde para confirmar tu lugar.",
        "aviso_de_promocion_a_lista_principal_para_clase_para_usuario_especifico_en_sistema_de_management_de_gimnasios_profesional": "Fuiste movido a la lista principal de la clase. ¡No faltes!",
        "mensaje_de_bienvenida_a_gimnasio_para_usuario_especifico_en_sistema_de_management_de_gimnasios_profesional": "¡Bienvenido/a al gimnasio! Cualquier duda, estamos para ayudarte.",
    }
    return m.get(n) or name

@admin_app.get("/templates/preview")
async def preview_template(request: Request):
    _require_admin(request)
    rl = _check_rate_limit(request, "templates_preview", 120, 60)
    if rl:
        return rl
    name = (request.query_params.get("name") or "").strip()
    accept = (request.headers.get("accept") or "").lower()
    wants_html = ("text/html" in accept) or (request.query_params.get("ui") == "1")
    text = _template_text(name) if name else "Selecciona un template"
    if not wants_html:
        return JSONResponse({"name": name, "text": text}, status_code=200)
    base = [
        "aviso_de_confirmacion_de_pago_de_cuota_gimnasio_para_usuario_especifico_en_sistema_de_management_de_gimnasios_profesional",
        "aviso_de_vencimiento_de_cuota_gimnasio_para_usuario_especifico_en_sistema_de_management_de_gimnasios_profesional",
        "aviso_de_confirmacion_de_ingreso_a_gimnasio_para_usuario_especifico_en_sistema_de_management_de_gimnasios_profesional",
        "aviso_de_desactivacion_por_falta_de_pago_para_usuario_especifico_en_sistema_de_management_de_gimnasios_profesional",
        "aviso_de_recordatorio_de_horario_de_clase_para_usuario_especifico_en_sistema_de_management_de_gimnasios_profesional",
        "aviso_de_promocion_de_lista_de_espera_para_clase_para_usuario_especifico_en_sistema_de_management_de_gimnasios_profesional",
        "aviso_de_promocion_a_lista_principal_para_clase_para_usuario_especifico_en_sistema_de_management_de_gimnasios_profesional",
        "mensaje_de_bienvenida_a_gimnasio_para_usuario_especifico_en_sistema_de_management_de_gimnasios_profesional",
    ]
    return templates.TemplateResponse("templates.html", {"request": request, "names": base, "name": name, "text": text})

@admin_app.post("/gyms/test-send")
async def send_template_test(request: Request, gym_id: int = Form(...), name: str = Form(...)):
    _require_admin(request)
    rl = _check_rate_limit(request, "templates_send", 20, 60)
    if rl:
        return rl
    adm = _get_admin_db()
    if adm is None:
        return JSONResponse({"error": "DB admin no disponible"}, status_code=500)
    text = _template_text(name)
    res = _send_whatsapp_text_for_gym(adm, int(gym_id), text)
    try:
        adm.log_action("owner", "send_template_test", int(gym_id), name)
    except Exception:
        pass
    sc = 200 if res.get("ok") else 400
    return JSONResponse(res, status_code=sc)
@admin_app.get("/gyms/{gym_id}/health")
async def health_check(request: Request, gym_id: int):
    _require_admin(request)
    rl = _check_rate_limit(request, "health_check", 60, 60)
    if rl:
        return rl
    adm = _get_admin_db()
    if adm is None:
        return JSONResponse({"error": "DB admin no disponible"}, status_code=500)
    try:
        cache = getattr(admin_app.state, "health_cache", {})
    except Exception:
        cache = {}
    try:
        now = int(datetime.utcnow().timestamp())
    except Exception:
        now = 0
    try:
        ttl = int(os.getenv("ADMIN_HEALTH_TTL_SECONDS", "600"))
    except Exception:
        ttl = 600
    acc0 = (request.headers.get("accept") or "").lower()
    is_hx = (str(request.headers.get("hx-request") or "").lower() == "true")
    wants_html0 = ("text/html" in acc0) or (request.query_params.get("ui") == "1")
    try:
        ent = cache.get(int(gym_id)) if isinstance(cache, dict) else None
        if ent and (now - int(ent.get("ts") or 0) < ttl):
            res_cached = ent.get("val") or {}
            if wants_html0 and is_hx:
                return templates.TemplateResponse(
                    "health-snippet.html",
                    {
                        "request": request,
                        "db_ok": bool((res_cached.get("db") or {}).get("ok")),
                        "wa_ok": bool((res_cached.get("whatsapp") or {}).get("ok")),
                        "st_ok": bool((res_cached.get("storage") or {}).get("ok")),
                    },
                )
            if not wants_html0:
                return JSONResponse(res_cached, status_code=200)
    except Exception:
        pass
    g = adm.obtener_gimnasio(int(gym_id))
    if not g:
        return JSONResponse({"error": "gym_not_found"}, status_code=404)
    db_ok = False
    db_err = None
    try:
        base = _resolve_admin_db_params()
    except Exception:
        base = None
    try:
        dbn = str(g.get("db_name") or "").strip()
        if base and dbn:
            params = dict(base)
            params["database"] = dbn
            try:
                ct = int(os.getenv("ADMIN_DB_CONNECT_TIMEOUT", "4"))
            except Exception:
                ct = 4
            params["connect_timeout"] = ct
            try:
                if DatabaseManager is not None:
                    db_ok = DatabaseManager.test_connection(params=params, timeout_seconds=6)
                else:
                    db_ok = False
            except Exception as _e:
                db_ok = False
                db_err = str(_e)
    except Exception as e:
        db_ok = False
        db_err = str(e)
    wa_ok = False
    wa_status = None
    try:
        pid = str(g.get("whatsapp_phone_id") or "").strip()
        tok = str(g.get("whatsapp_access_token") or "").strip()
        if pid and tok and requests is not None:
            url = f"https://graph.facebook.com/v17.0/{pid}"
            r = requests.get(url, headers={"Authorization": f"Bearer {tok}"}, timeout=8)
            wa_status = int(r.status_code)
            wa_ok = 200 <= r.status_code < 300
    except Exception as e:
        wa_ok = False
        wa_status = None
    st_ok = False
    st_cfg = False
    try:
        bname = str(g.get("b2_bucket_name") or "").strip()
        bid = str(g.get("b2_bucket_id") or "").strip()
        st_cfg = bool(bname and bid)
        st_ok = st_cfg
    except Exception:
        st_ok = False
        st_cfg = False
    accept = (request.headers.get("accept") or "").lower()
    wants_html = ("text/html" in accept) or (request.query_params.get("ui") == "1")
    snippet = (str(request.query_params.get("snippet") or "").strip() == "1") or (str(request.headers.get("hx-request") or "").lower() == "true")
    res = {"db": {"ok": bool(db_ok), "error": db_err}, "whatsapp": {"ok": bool(wa_ok), "status": wa_status}, "storage": {"ok": bool(st_ok), "configured": bool(st_cfg)}}
    try:
        cache[int(gym_id)] = {"ts": int(now), "val": res}
        setattr(admin_app.state, "health_cache", cache)
    except Exception:
        pass
    if not wants_html:
        return JSONResponse(res, status_code=200)
    if snippet:
        return templates.TemplateResponse(
            "health-snippet.html",
            {
                "request": request,
                "db_ok": bool(db_ok),
                "wa_ok": bool(wa_ok),
                "st_ok": bool(st_ok),
            },
        )
    return templates.TemplateResponse(
        "gym-settings.html",
        {
            "request": request,
            "section": "health",
            "gid": int(gym_id),
            "db_ok": bool(db_ok),
            "db_err": db_err,
            "wa_ok": bool(wa_ok),
            "wa_status": wa_status,
            "st_ok": bool(st_ok),
            "st_cfg": bool(st_cfg),
        },
    )

@admin_app.get("/gyms/{gym_id}/details")
async def gym_details(request: Request, gym_id: int):
    _require_admin(request)
    adm = _get_admin_db()
    if adm is None:
        return JSONResponse({"error": "DB admin no disponible"}, status_code=500)
    g = adm.obtener_gimnasio(int(gym_id))
    if not g:
        return JSONResponse({"error": "gym_not_found"}, status_code=404)
    db_ok = False
    db_err = None
    try:
        base = _resolve_admin_db_params()
    except Exception:
        base = None
    try:
        dbn = str(g.get("db_name") or "").strip()
        if base and dbn:
            params = dict(base)
            params["database"] = dbn
            try:
                ct = int(os.getenv("ADMIN_DB_CONNECT_TIMEOUT", "4"))
            except Exception:
                ct = 4
            params["connect_timeout"] = ct
            try:
                if DatabaseManager is not None:
                    db_ok = DatabaseManager.test_connection(params=params, timeout_seconds=6)
                else:
                    db_ok = False
            except Exception as _e:
                db_ok = False
                db_err = str(_e)
    except Exception as e:
        db_ok = False
        db_err = str(e)
    wa_ok = False
    wa_status = None
    try:
        pid = str(g.get("whatsapp_phone_id") or "").strip()
        tok = str(g.get("whatsapp_access_token") or "").strip()
        if pid and tok and requests is not None:
            url = f"https://graph.facebook.com/v17.0/{pid}"
            r = requests.get(url, headers={"Authorization": f"Bearer {tok}"}, timeout=8)
            wa_status = int(r.status_code)
            wa_ok = 200 <= r.status_code < 300
    except Exception:
        wa_ok = False
        wa_status = None
    st_ok = False
    st_cfg = False
    try:
        bname = str(g.get("b2_bucket_name") or "").strip()
        bid = str(g.get("b2_bucket_id") or "").strip()
        st_cfg = bool(bname and bid)
        st_ok = st_cfg
    except Exception:
        st_ok = False
        st_cfg = False
    sub = adm.obtener_subscription(int(gym_id))
    pays = adm.listar_pagos(int(gym_id))
    safe = {
        "id": int(g.get("id") or int(gym_id)),
        "nombre": str(g.get("nombre") or ""),
        "subdominio": str(g.get("subdominio") or ""),
        "status": str(g.get("status") or ""),
        "created_at": str(g.get("created_at") or ""),
        "owner_phone": str(g.get("owner_phone") or ""),
        "db_name": str(g.get("db_name") or ""),
        "b2_bucket_name": str(g.get("b2_bucket_name") or ""),
        "b2_bucket_id": str(g.get("b2_bucket_id") or ""),
        "hard_suspend": bool(g.get("hard_suspend") or False),
        "suspended_until": str(g.get("suspended_until") or ""),
    }
    health = {"db": {"ok": bool(db_ok), "error": db_err}, "whatsapp": {"ok": bool(wa_ok), "status": wa_status}, "storage": {"ok": bool(st_ok), "configured": bool(st_cfg)}}
    return JSONResponse({"gym": safe, "health": health, "subscription": sub, "payments": (pays or [])[:8]}, status_code=200)

@admin_app.post("/gyms/batch")
async def gyms_batch(request: Request, action: str = Form(...), gym_ids: str = Form(...)):
    _require_admin(request)
    rl = _check_rate_limit(request, "gyms_batch", 40, 60)
    if rl:
        return rl
    adm = _get_admin_db()
    if adm is None:
        return JSONResponse({"error": "DB admin no disponible"}, status_code=500)
    ids: List[int] = []
    for part in str(gym_ids or "").split(","):
        try:
            v = int(part.strip())
            if v:
                ids.append(v)
        except Exception:
            continue
    okc = 0
    errs = []
    for gid in ids:
        try:
            if action == "provision":
                res = adm.provisionar_recursos(int(gid))
                if res and res.get("ok"):
                    okc += 1
                else:
                    errs.append({"id": gid, "error": res.get("error") if isinstance(res, dict) else "unknown"})
            elif action == "suspend":
                if adm.set_estado_gimnasio(int(gid), "suspended", False, None, "batch"):
                    okc += 1
                else:
                    errs.append({"id": gid, "error": "set_estado_failed"})
            elif action == "unsuspend":
                if adm.set_estado_gimnasio(int(gid), "active", False, None, "batch"):
                    okc += 1
                else:
                    errs.append({"id": gid, "error": "set_estado_failed"})
            else:
                errs.append({"id": gid, "error": "unknown_action"})
        except Exception as e:
            errs.append({"id": gid, "error": str(e)})
    try:
        adm.log_action("system", "gyms_batch_" + str(action or ""), None, {"ids": ids, "ok": okc, "errs": len(errs)})
    except Exception:
        pass
    return JSONResponse({"ok": True, "action": action, "processed": okc, "errors": errs}, status_code=200)

@admin_app.post("/gyms/remind/batch")
async def gyms_remind_batch(request: Request, gym_ids: str = Form(...), message: Optional[str] = Form(None)):
    _require_admin(request)
    rl = _check_rate_limit(request, "gyms_remind_batch", 40, 60)
    if rl:
        return rl
    adm = _get_admin_db()
    if adm is None:
        return JSONResponse({"error": "DB admin no disponible"}, status_code=500)
    ids: List[int] = []
    for part in str(gym_ids or "").split(","):
        try:
            v = int(part.strip())
            if v:
                ids.append(v)
        except Exception:
            continue
    okc = 0
    errs = []
    for gid in ids:
        try:
            sub = adm.obtener_subscription(int(gid))
            g = adm.obtener_gimnasio(int(gid))
            nombre = str((g or {}).get("nombre") or "")
            nd = str((sub or {}).get("next_due_date") or "")
            msg = message or (f"Hola {nombre}, tu suscripción vence el {nd}.")
            res = _send_whatsapp_text_for_gym(adm, int(gid), msg)
            if res.get("ok"):
                okc += 1
            else:
                errs.append({"id": gid, "error": res.get("error") or "send_failed"})
        except Exception as e:
            errs.append({"id": gid, "error": str(e)})
    try:
        adm.log_action("owner", "gyms_remind_batch", None, {"ids": ids, "ok": okc, "errs": len(errs)})
    except Exception:
        pass
    return JSONResponse({"ok": True, "processed": okc, "errors": errs}, status_code=200)

@admin_app.get("/audit")
async def ver_auditoria(request: Request):
    _require_admin(request)
    adm = _get_admin_db()
    if adm is None:
        return JSONResponse({"error": "DB admin no disponible"}, status_code=500)
    actor = (request.query_params.get("actor") or "").strip()
    action = (request.query_params.get("action") or "").strip()
    try:
        gym_id = int(request.query_params.get("gym_id") or 0) or None
    except Exception:
        gym_id = None
    from_date = (request.query_params.get("from") or "").strip()
    to_date = (request.query_params.get("to") or "").strip()
    try:
        page = int(request.query_params.get("page") or 1)
    except Exception:
        page = 1
    try:
        page_size = int(request.query_params.get("page_size") or 50)
    except Exception:
        page_size = 50
    payload = adm.listar_auditoria(actor or None, action or None, gym_id, from_date or None, to_date or None, page, page_size)
    summary = adm.resumen_auditoria(7)
    accept = (request.headers.get("accept") or "").lower()
    wants_html = ("text/html" in accept) or (request.query_params.get("ui") == "1")
    if not wants_html:
        return JSONResponse({"items": payload.get("items"), "total": payload.get("total"), "page": payload.get("page"), "page_size": payload.get("page_size"), "summary": summary}, status_code=200)
    return templates.TemplateResponse(
        "audit.html",
        {
            "request": request,
            "items": list(payload.get("items") or []),
            "summary": summary,
            "actor": actor,
            "action": action,
            "gym_id": gym_id,
            "from": from_date,
            "to": to_date,
            "page": int(payload.get("page") or 1),
            "page_size": int(payload.get("page_size") or 50),
            "total": int(payload.get("total") or 0),
        },
    )

@admin_app.get("/metrics")
async def ver_metricas(request: Request):
    _require_admin(request)
    adm = _get_admin_db()
    if adm is None:
        return JSONResponse({"error": "DB admin no disponible"}, status_code=500)
    m = adm.obtener_metricas_agregadas()
    accept = (request.headers.get("accept") or "").lower()
    wants_html = ("text/html" in accept) or (request.query_params.get("ui") == "1")
    if not wants_html:
        return JSONResponse(m, status_code=200)
    upcoming = adm.listar_proximos_vencimientos(14)
    series = list((m.get("gyms") or {}).get("series_30") or [])
    trend_dates = [str(it.get("date") or "") for it in series]
    trend_counts = [int(it.get("count") or 0) for it in series]
    return templates.TemplateResponse(
        "dashboard.html",
        {
            "request": request,
            "metrics": m,
            "upcoming": upcoming,
            "trend_dates": trend_dates,
            "trend_counts": trend_counts,
        },
    )

@admin_app.post("/sessions/invalidate")
async def invalidate_sessions(request: Request):
    _require_admin(request)
    rl = _check_rate_limit(request, "sessions_invalidate", 5, 60)
    if rl:
        return rl
    try:
        cur = int(getattr(admin_app.state, "session_version", 1))
        setattr(admin_app.state, "session_version", cur + 1)
    except Exception:
        setattr(admin_app.state, "session_version", 1)
    adm = _get_admin_db()
    try:
        if adm:
            adm.log_action("owner", "invalidate_sessions", None, None)
    except Exception:
        pass
    return JSONResponse({"ok": True, "new_version": int(getattr(admin_app.state, "session_version", 1))}, status_code=200)
@admin_app.post("/gyms/{gym_id}/provision")
async def provision_gym(request: Request, gym_id: int):
    _require_admin(request)
    rl = _check_rate_limit(request, "gym_provision", 20, 60)
    if rl:
        return rl
    adm = _get_admin_db()
    if adm is None:
        return JSONResponse({"error": "DB admin no disponible"}, status_code=500)
    try:
        res = adm.provisionar_recursos(int(gym_id))
    except Exception as e:
        res = {"ok": False, "error": str(e)}
    try:
        adm.log_action("owner", "provision_gym", int(gym_id), None)
    except Exception:
        pass
    sc = 200 if bool(res.get("ok")) else 400
    return JSONResponse(res, status_code=sc)
@admin_app.get("/dashboard")
async def unified_dashboard(request: Request):
    gr = _guard_html_login_redirect(request)
    if gr:
        return gr
    adm = _get_admin_db()
    if adm is None:
        return JSONResponse({"error": "DB admin no disponible"}, status_code=500)
    m = adm.obtener_metricas_agregadas()
    upcoming = adm.listar_proximos_vencimientos(14)
    accept = (request.headers.get("accept") or "").lower()
    wants_html = ("text/html" in accept) or (request.query_params.get("ui") == "1")
    if not wants_html:
        return JSONResponse({"metrics": m, "upcoming": upcoming}, status_code=200)
    series = list((m.get("gyms") or {}).get("series_30") or [])
    trend_dates = [str(it.get("date") or "") for it in series]
    trend_counts = [int(it.get("count") or 0) for it in series]
    return templates.TemplateResponse(
        "dashboard.html",
        {
            "request": request,
            "metrics": m,
            "upcoming": upcoming,
            "trend_dates": trend_dates,
            "trend_counts": trend_counts,
        },
    )
def _guard_html_login_redirect(request: Request):
    if _is_logged_in(request):
        return None
    try:
        acc = (request.headers.get("accept") or "").lower()
    except Exception:
        acc = ""
    try:
        wants_html = ("text/html" in acc) or (request.query_params.get("ui") == "1")
    except Exception:
        wants_html = False
    if wants_html:
        from urllib.parse import quote
        try:
            p = str(request.url.path or "/admin")
        except Exception:
            p = "/admin"
        try:
            q = str(request.url.query or "")
            if q:
                p = f"{p}?{q}"
        except Exception:
            pass
        return RedirectResponse(url=f"/admin/login?next={quote(p)}", status_code=302)
    _require_admin(request)
    return None