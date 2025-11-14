import os
from typing import Optional, List, Dict, Any

from fastapi import FastAPI, Request, HTTPException, Form
from fastapi.responses import JSONResponse, Response, RedirectResponse
from starlette.middleware.sessions import SessionMiddleware
try:
    import requests  # type: ignore
except Exception:
    requests = None  # type: ignore
from datetime import datetime

from .database import AdminDatabaseManager, _resolve_admin_db_params
from core.database import DatabaseManager  # type: ignore


admin_app = FastAPI(title="GymMS Admin", version="1.0")
admin_app.add_middleware(SessionMiddleware, secret_key=os.getenv("ADMIN_SESSION_SECRET", "admin-session"))
try:
    setattr(admin_app.state, "session_version", int(getattr(admin_app.state, "session_version", 1)))
    setattr(admin_app.state, "rate_limits", dict(getattr(admin_app.state, "rate_limits", {})))
except Exception:
    pass

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
        v = int(request.session.get("session_version") or 0)
        cur = int(getattr(admin_app.state, "session_version", 1))
        return bool(request.session.get("admin_logged_in")) and v == cur
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

@admin_app.get("/login")
async def admin_login_form(request: Request):
    html = """
    <div class="dark"><div style="max-width:360px;margin:0 auto;padding:20px;font-family:system-ui"><h1 style="font-size:20px;font-weight:600">Acceso admin</h1><form method="post" action="/admin/login" style="display:grid;gap:8px;margin-top:16px"><input type="password" name="password" placeholder="Contraseña" required style="padding:8px;border:1px solid #ccc;border-radius:6px" /><button type="submit" style="padding:10px 14px;border-radius:6px;background:#111;color:#fff">Entrar</button></form></div></div>
    """
    return Response(content=html, media_type="text/html")

@admin_app.post("/login")
async def admin_login(request: Request, password: str = Form(...)):
    adm = _get_admin_db()
    if adm is None:
        return JSONResponse({"error": "DB admin no disponible"}, status_code=500)
    ok = adm.verificar_owner_password(password)
    if not ok:
        acc = (request.headers.get("accept") or "").lower()
        if ("text/html" in acc) or (request.query_params.get("ui") == "1"):
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
    acc = (request.headers.get("accept") or "").lower()
    if ("text/html" in acc) or (request.query_params.get("ui") == "1"):
        return RedirectResponse(url="/admin", status_code=303)
    return JSONResponse({"ok": True}, status_code=200)

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
    if ("text/html" in acc) or (request.query_params.get("ui") == "1"):
        try:
            v = int(request.session.get("session_version") or 0)
            cur = int(getattr(admin_app.state, "session_version", 1))
            logged = bool(request.session.get("admin_logged_in")) and v == cur
        except Exception:
            logged = False
        if not logged:
            secret = os.getenv("ADMIN_SECRET", "").strip()
            hdr = request.headers.get("x-admin-secret") or ""
            if not secret or hdr.strip() != secret:
                return RedirectResponse(url="/admin/login", status_code=303)
    _require_admin(request)
    html = """
    <div class="dark"><div style="max-width:720px;margin:0 auto;padding:20px;font-family:system-ui"><h1 style="font-size:24px;font-weight:600">Panel Admin</h1><div style="display:flex;gap:12px;margin-top:12px"><a href="/admin/gyms" style="padding:10px 14px;border-radius:6px;background:#111;color:#fff;text-decoration:none">Ver gimnasios</a><a href="/admin/owner/password?ui=1" style="padding:10px 14px;border-radius:6px;background:#374151;color:#fff;text-decoration:none">Contraseña Admin</a><a href="/admin/subscriptions/upcoming?ui=1" style="padding:10px 14px;border-radius:6px;background:#1e40af;color:#fff;text-decoration:none">Próximos vencimientos</a><a href="/admin/login" style="padding:10px 14px;border-radius:6px;background:#444;color:#fff;text-decoration:none">Cambiar cuenta</a></div><form method="post" action="/admin/gyms" style="display:grid;gap:8px;margin-top:16px"><input name="nombre" placeholder="Nombre" required style="padding:8px;border:1px solid #ccc;border-radius:6px" /><input name="subdominio" placeholder="Subdominio" required style="padding:8px;border:1px solid #ccc;border-radius:6px" /><input name="owner_phone" placeholder="Teléfono dueño (+54...)" style="padding:8px;border:1px solid #ccc;border-radius:6px" /><input name="whatsapp_phone_id" placeholder="WhatsApp Phone ID" style="padding:8px;border:1px solid #ccc;border-radius:6px" /><input name="whatsapp_access_token" placeholder="WhatsApp Access Token" style="padding:8px;border:1px solid #ccc;border-radius:6px" /><input name="whatsapp_business_account_id" placeholder="WhatsApp Business Account ID" style="padding:8px;border:1px solid #ccc;border-radius:6px" /><input name="whatsapp_verify_token" placeholder="WhatsApp Verify Token" style="padding:8px;border:1px solid #ccc;border-radius:6px" /><input name="whatsapp_app_secret" placeholder="WhatsApp App Secret" style="padding:8px;border:1px solid #ccc;border-radius:6px" /><label style="display:flex;align-items:center;gap:8px"><input type="checkbox" name="whatsapp_nonblocking" value="true"/> WhatsApp no bloqueante</label><input type="number" step="0.1" name="whatsapp_send_timeout_seconds" placeholder="Timeout envío WhatsApp (seg)" style="padding:8px;border:1px solid #ccc;border-radius:6px" /><button type="submit" style="padding:10px 14px;border-radius:6px;background:#111;color:#fff">Crear gimnasio</button></form></div></div>
    """
    try:
        html = html.replace("/admin/subscriptions/upcoming?ui=1", "/admin/subscriptions/dashboard?ui=1")
        html = html.replace("Próximos vencimientos", "Dashboard de vencimientos")
    except Exception:
        pass
    extra = """
    <div class=\"dark\"><style>@keyframes shimmer{0%{background-position:-200px 0}100%{background-position:200px 0}}.skeleton{background:#1f2937;background-image:linear-gradient(90deg,#1f2937 0,#374151 50%,#1f2937 100%);background-size:200px 100%;animation:shimmer 1.2s infinite linear}</style><button id=\"open-k\" style=\"position:fixed;bottom:16px;right:16px;padding:10px 12px;border-radius:999px;background:#374151;color:#fff\">Ctrl+K</button><div id=\"k-overlay\" style=\"position:fixed;inset:0;background:rgba(0,0,0,0.6);display:none;align-items:center;justify-content:center;z-index:40\"><div style=\"width:90%;max-width:720px;background:#111827;border:1px solid #333;border-radius:12px\"><div style=\"padding:12px;border-bottom:1px solid #333;display:flex;justify-content:space-between;align-items:center\"><div style=\"font-weight:600\">Búsqueda</div><button id=\"k-close\" style=\"background:#1f2937;color:#fff;border-radius:6px;padding:6px 8px\">Cerrar</button></div><div style=\"padding:12px\"><input id=\"k-input\" placeholder=\"Buscar gimnasios o templates\" style=\"padding:8px;border:1px solid #333;border-radius:8px;width:100%\"/><div id=\"k-results\" style=\"margin-top:12px;display:grid;gap:8px\"></div></div></div></div></div>
    <script>function openK(){var o=document.getElementById('k-overlay');if(o){o.style.display='flex';var inp=document.getElementById('k-input');if(inp){setTimeout(function(){inp.focus()},50)}}}function closeK(){var o=document.getElementById('k-overlay');if(o){o.style.display='none'}}function searchK(q){var res=document.getElementById('k-results');if(!res)return;res.innerHTML='<div class=\"skeleton\" style=\"width:100%;height:18px;border-radius:6px\"></div>';var list=[];fetch('/admin/gyms?q='+encodeURIComponent(q)+'&page=1&page_size=10',{headers:{'accept':'application/json'}}).then(function(r){return r.json()}).then(function(j){var items=(j&&j.items)||[];for(var i=0;i<items.length;i++){var g=items[i];list.push({t:'gym',id:String(g.id||''),label:String(g.nombre||'')+' · '+String(g.subdominio||'')})}return fetch('/admin/templates',{headers:{'accept':'application/json'}})}).then(function(r){return r.json()}).then(function(j){var t=(j&&j.templates)||[];for(var i=0;i<t.length;i++){list.push({t:'template',id:String(i+1),label:String(t[i]||'')})}var html='';for(var k=0;k<list.length;k++){var it=list[k];var href=it.t==='gym'?('/admin/gyms/'+it.id+'/health?ui=1'):('/admin/templates?ui=1');html+="<a href=\""+href+"\" style=\"display:flex;justify-content:space-between;align-items:center;padding:8px 10px;border:1px solid #333;border-radius:8px;background:#0b1222;color:#fff;text-decoration:none\"><span>"+it.label+"</span><span style=\"color:#9ca3af;font-size:12px\">"+it.t+"</span></a>"}if(!html){html='<div style=\"color:#9ca3af\">Sin resultados</div>'}res.innerHTML=html}).catch(function(){res.innerHTML='<div style=\"color:#ef4444\">Error en búsqueda</div>'})}document.addEventListener('DOMContentLoaded',function(){var ok=document.getElementById('open-k');if(ok){ok.addEventListener('click',openK)}var kc=document.getElementById('k-close');if(kc){kc.addEventListener('click',closeK)}var ki=document.getElementById('k-input');if(ki){ki.addEventListener('input',function(){searchK(ki.value)})}document.addEventListener('keydown',function(e){if((e.ctrlKey||e.metaKey)&&e.key.toLowerCase()==='k'){e.preventDefault();openK()}})});</script>
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
    return Response(content=html, media_type="text/html")

def _lim_key(request: Request, bucket: str) -> str:
    try:
        ip = (request.client.host if request.client else "-")
    except Exception:
        ip = "-"
    return f"{bucket}:{ip}"

def _check_rate_limit(request: Request, bucket: str, limit: int = 30, window_seconds: int = 60):
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
    c = int(rec.get("count") or 0) + 1
    rec["count"] = c
    store[k] = rec
    setattr(admin_app.state, "rate_limits", store)
    if c > int(limit):
        return JSONResponse({"error": "rate_limited"}, status_code=429)
    return None

@admin_app.get("/gyms")
async def listar_gimnasios(request: Request):
    _require_admin(request)
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
    payload = adm.listar_gimnasios_avanzado(page, page_size, q or None, status_q or None, order_by or None, order_dir or None)
    accept = (request.headers.get("accept") or "").lower()
    wants_html = ("text/html" in accept) or (request.query_params.get("ui") == "1")
    if not wants_html:
        return JSONResponse(payload, status_code=200)
    items: List[Dict[str, Any]] = list(payload.get("items") or [])
    total = int(payload.get("total") or 0)
    p = int(payload.get("page") or page)
    ps = int(payload.get("page_size") or page_size)
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
        actions = f"<form method=\"post\" action=\"/admin/gyms/{gid}/contact\" style=\"display:inline\"><input name=\"owner_phone\" value=\"{owner_phone}\" placeholder=\"+54...\" style=\"padding:6px;border:1px solid #333;border-radius:6px;width:160px\"/><button type=\"submit\" style=\"margin-left:6px\">Guardar</button></form> <a href=\"/admin/gyms/{gid}/owner?ui=1\" style=\"margin-left:6px\">Contraseña dueño</a> <a href=\"/admin/gyms/{gid}/branding?ui=1\" style=\"margin-left:6px\">Branding</a> <form method=\"post\" action=\"/admin/gyms/{gid}/suspend\" style=\"display:inline;margin-left:6px\"><input type=\"hidden\" name=\"hard\" value=\"false\"/><button type=\"submit\">Suspender</button></form> <form method=\"post\" action=\"/admin/gyms/{gid}/unsuspend\" style=\"display:inline;margin-left:6px\"><button type=\"submit\">Reactivar</button></form> <a href=\"/admin/gyms/{gid}/subscription?ui=1\" style=\"margin-left:6px\">Suscripción</a> <a href=\"/admin/gyms/{gid}/payments?ui=1\" style=\"margin-left:6px\">Pagos</a> <a href=\"/admin/gyms/{gid}/maintenance?ui=1\" style=\"margin-left:6px\">Mantenimiento</a> <a href=\"/admin/gyms/{gid}/whatsapp?ui=1\" style=\"margin-left:6px\">WhatsApp</a> <a href=\"/admin/gyms/{gid}/health?ui=1\" style=\"margin-left:6px\">Salud</a> <form method=\"post\" action=\"/admin/gyms/{gid}/b2/regenerate-key\" style=\"display:inline;margin-left:6px\"><button type=\"submit\">Regenerar clave B2</button></form> <form method=\"post\" action=\"/admin/gyms/{gid}/b2/delete-bucket\" style=\"display:inline;margin-left:6px\"><button type=\"submit\">Eliminar bucket B2</button></form>"
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
    html = """
    <div class=\"dark\"><div style=\"max-width:1200px;margin:0 auto;padding:20px;font-family:system-ui\"><style>@keyframes shimmer{0%{background-position:-200px 0}100%{background-position:200px 0}}.skeleton{background:#1f2937;background-image:linear-gradient(90deg,#1f2937 0,#374151 50%,#1f2937 100%);background-size:200px 100%;animation:shimmer 1.2s infinite linear}</style><h1 style=\"font-size:24px;font-weight:600\">Gimnasios</h1><div style=\"margin:12px 0;display:flex;gap:12px;flex-wrap:wrap\"><a href=\"/admin/\" style=\"padding:8px 12px;border-radius:6px;background:#111;color:#fff;text-decoration:none\">Inicio</a><button id=\"open-k\" style=\"padding:8px 12px;border-radius:6px;background:#374151;color:#fff\">Ctrl+K</button><form method=\"get\" action=\"/admin/gyms\" style=\"display:flex;gap:8px;flex-wrap:wrap\"><input name=\"q\" value=\"{q}\" placeholder=\"Buscar por nombre o subdominio\" style=\"padding:8px;border:1px solid #333;border-radius:6px;min-width:240px\"/><select name=\"status\" style=\"padding:8px;border:1px solid #333;border-radius:6px\"><option value=\"\">Todos</option><option value=\"active\"{act_sel}>Activos</option><option value=\"suspended\"{sus_sel}>Suspendidos</option><option value=\"maintenance\"{mnt_sel}>Mantenimiento</option></select><input type=\"hidden\" name=\"ui\" value=\"1\"/><button type=\"submit\" style=\"padding:8px 12px;border-radius:6px;background:#1e40af;color:#fff\">Filtrar</button></form><form method=\"get\" action=\"/admin/gyms\" style=\"display:flex;gap:8px\"><input name=\"page_size\" value=\"{ps}\" style=\"padding:8px;border:1px solid #333;border-radius:6px;width:80px\"/><input type=\"hidden\" name=\"ui\" value=\"1\"/><input type=\"hidden\" name=\"page\" value=\"1\"/><button type=\"submit\" style=\"padding:8px 12px;border-radius:6px;background:#111827;color:#fff\">Tamaño página</button></form></div><div style=\"overflow-x:auto\"><table style=\"width:100%;border-collapse:collapse\"><thead><tr><th><a href=\"{link_id}\" style=\"color:#9ca3af\">ID</a></th><th><a href=\"{link_nombre}\" style=\"color:#9ca3af\">Nombre</a></th><th><a href=\"{link_sub}\" style=\"color:#9ca3af\">Subdominio</a></th><th><a href=\"{link_created}\" style=\"color:#9ca3af\">Creado</a></th><th>Teléfono</th><th>Status</th><th>Salud</th><th>Hard</th><th>Hasta</th><th>Bucket</th><th>Acciones</th></tr></thead><tbody>{rows}</tbody></table></div><div style=\"margin-top:12px;display:flex;gap:8px;align-items:center\"><a href=\"{prev}\" id=\"prev-link\" style=\"padding:8px 12px;border-radius:6px;background:#374151;color:#fff;text-decoration:none\">Anterior</a><div style=\"color:#9ca3af\">Página {p} de {lp} • {total} resultados</div><a href=\"{next}\" id=\"next-link\" style=\"padding:8px 12px;border-radius:6px;background:#374151;color:#fff;text-decoration:none\">Siguiente</a></div><h2 style=\"font-size:18px;margin-top:16px\">Crear gimnasio</h2><form method=\"post\" action=\"/admin/gyms\" style=\"display:grid;grid-template-columns:repeat(auto-fit,minmax(200px,1fr));gap:8px;margin-top:8px\"><input name=\"nombre\" placeholder=\"Nombre\" required style=\"padding:8px;border:1px solid #333;border-radius:6px\"/><input name=\"subdominio\" placeholder=\"subdominio\" required style=\"padding:8px;border:1px solid #333;border-radius:6px\"/><input name=\"owner_phone\" placeholder=\"Teléfono dueño (+54...)\" style=\"padding:8px;border:1px solid #333;border-radius:6px\"/><input name=\"whatsapp_phone_id\" placeholder=\"WhatsApp Phone ID\" style=\"padding:8px;border:1px solid #333;border-radius:6px\"/><input name=\"whatsapp_access_token\" placeholder=\"WhatsApp Access Token\" style=\"padding:8px;border:1px solid #333;border-radius:6px\"/><button type=\"submit\" style=\"padding:10px 14px;border-radius:6px;background:#16a34a;color:#fff\">Crear</button></form><div id=\"toast-container\" style=\"position:fixed;top:16px;right:16px;display:flex;flex-direction:column;gap:8px;z-index:50\"></div><div id=\"k-overlay\" style=\"position:fixed;inset:0;background:rgba(0,0,0,0.6);display:none;align-items:center;justify-content:center;z-index:40\"><div style=\"width:90%;max-width:720px;background:#111827;border:1px solid #333;border-radius:12px\"><div style=\"padding:12px;border-bottom:1px solid #333;display:flex;justify-content:space-between;align-items:center\"><div style=\"font-weight:600\">Búsqueda</div><button id=\"k-close\" style=\"background:#1f2937;color:#fff;border-radius:6px;padding:6px 8px\">Cerrar</button></div><div style=\"padding:12px\"><input id=\"k-input\" placeholder=\"Buscar gimnasios o templates\" style=\"padding:8px;border:1px solid #333;border-radius:8px;width:100%\"/><div id=\"k-results\" style=\"margin-top:12px;display:grid;gap:8px\"></div></div></div></div><button id=\"open-k-float\" style=\"position:fixed;bottom:16px;right:16px;padding:10px 12px;border-radius:999px;background:#374151;color:#fff\">Ctrl+K</button></div>
    """.replace("{rows}", "".join(rows)).replace("{prev}", prev_link).replace("{next}", next_link).replace("{p}", str(p)).replace("{lp}", str(last_page)).replace("{total}", str(total)).replace("{link_id}", link_for("id")).replace("{link_nombre}", link_for("nombre")).replace("{link_sub}", link_for("subdominio")).replace("{link_created}", link_for("created_at")).replace("{q}", q).replace("{act_sel}", " selected\"" if status_q == "active" else "\"").replace("{sus_sel}", " selected\"" if status_q == "suspended" else "\"").replace("{mnt_sel}", " selected\"" if status_q == "maintenance" else "\"")
    js = """
    <script>
    function toast(msg, type){var c=document.getElementById('toast-container');if(!c)return;var bg=type==='error'?'#ef4444':(type==='info'?'#374151':'#16a34a');var el=document.createElement('div');el.setAttribute('style','background:'+bg+';color:#fff;padding:10px 12px;border-radius:8px;box-shadow:0 10px 15px rgba(0,0,0,0.2)');el.textContent=msg;c.appendChild(el);setTimeout(function(){if(el&&el.parentNode){el.parentNode.removeChild(el)}},3000)}
    function loadHealth(gid){var el=document.getElementById('health-'+gid);if(!el)return;fetch('/admin/gyms/'+gid+'/health',{headers:{'accept':'application/json'}}).then(function(r){if(!r.ok){throw new Error('HTTP '+r.status)}return r.json()}).then(function(j){var d=j&&j.db&&j.db.ok;var w=j&&j.whatsapp&&j.whatsapp.ok;var s=j&&j.storage&&j.storage.ok;var html='';html+="<div style=\"display:flex;align-items:center;gap:4px\"><img src=\"https://img.icons8.com/ios-filled/18/database.png\" width=\"18\" height=\"18\" alt=\"DB\"/><span style=\"width:8px;height:8px;border-radius:999px;background:"+(d?'#16a34a':'#ef4444')+"\"></span></div>";html+="<div style=\"display:flex;align-items:center;gap:4px\"><img src=\"https://img.icons8.com/color/18/whatsapp--v1.png\" width=\"18\" height=\"18\" alt=\"WA\"/><span style=\"width:8px;height:8px;border-radius:999px;background:"+(w?'#16a34a':'#ef4444')+"\"></span></div>";html+="<div style=\"display:flex;align-items:center;gap:4px\"><img src=\"https://img.icons8.com/ios-filled/18/cloud.png\" width=\"18\" height=\"18\" alt=\"ST\"/><span style=\"width:8px;height:8px;border-radius:999px;background:"+(s?'#16a34a':'#ef4444')+"\"></span></div>";el.innerHTML=html}).catch(function(e){toast('No se pudo cargar salud #'+gid,'error')})}
    function initHealth(){var rows=document.querySelectorAll('tr[data-gym-id]');for(var i=0;i<rows.length;i++){var gid=rows[i].getAttribute('data-gym-id');loadHealth(gid)}}
    function openK(){var o=document.getElementById('k-overlay');if(o){o.style.display='flex';var inp=document.getElementById('k-input');if(inp){setTimeout(function(){inp.focus()},50)}}}
    function closeK(){var o=document.getElementById('k-overlay');if(o){o.style.display='none'}}
    function searchK(q){var res=document.getElementById('k-results');if(!res)return;res.innerHTML='<div class="skeleton" style="width:100%;height:18px;border-radius:6px"></div>';var list=[];fetch('/admin/gyms?q='+encodeURIComponent(q)+'&page=1&page_size=10',{headers:{'accept':'application/json'}}).then(function(r){return r.json()}).then(function(j){var items=(j&&j.items)||[];for(var i=0;i<items.length;i++){var g=items[i];list.push({t:'gym',id:String(g.id||''),label:String(g.nombre||'')+' · '+String(g.subdominio||'')})}return fetch('/admin/templates',{headers:{'accept':'application/json'}})}).then(function(r){return r.json()}).then(function(j){var t=(j&&j.templates)||[];for(var i=0;i<t.length;i++){list.push({t:'template',id:String(i+1),label:String(t[i]||'')})}var html='';for(var k=0;k<list.length;k++){var it=list[k];var href=it.t==='gym'?('/admin/gyms/'+it.id+'/health?ui=1'):('/admin/templates?ui=1');html+="<a href=\""+href+"\" style=\"display:flex;justify-content:space-between;align-items:center;padding:8px 10px;border:1px solid #333;border-radius:8px;background:#0b1222;color:#fff;text-decoration:none\"><span>"+it.label+"</span><span style=\"color:#9ca3af;font-size:12px\">"+it.t+"</span></a>"}if(!html){html='<div style="color:#9ca3af">Sin resultados</div>'}res.innerHTML=html}).catch(function(){res.innerHTML='<div style="color:#ef4444">Error en búsqueda</div>'})}
    document.addEventListener('DOMContentLoaded',function(){initHealth();var ok=document.getElementById('open-k');if(ok){ok.addEventListener('click',openK)}var okf=document.getElementById('open-k-float');if(okf){okf.addEventListener('click',openK)}var kc=document.getElementById('k-close');if(kc){kc.addEventListener('click',closeK)}var ki=document.getElementById('k-input');if(ki){ki.addEventListener('input',function(){searchK(ki.value)})}
    var th=document.querySelector('thead');if(th){th.style.position='sticky';th.style.top='0';th.style.background='#111827';th.style.zIndex='5'}
    var toolbar=document.querySelector('h1').parentNode;try{var inp=document.createElement('input');inp.id='page-jump';inp.placeholder='Ir a página';inp.setAttribute('style','padding:8px;border:1px solid #333;border-radius:6px;width:100px');var btn=document.createElement('button');btn.id='go-page';btn.textContent='Ir';btn.setAttribute('style','padding:8px 12px;border-radius:6px;background:#374151;color:#fff;margin-left:8px');var rng=document.createElement('div');rng.textContent='Rango: 1–'+"{lp}";rng.setAttribute('style','color:#9ca3af;margin-left:8px');toolbar.appendChild(inp);toolbar.appendChild(btn);toolbar.appendChild(rng);}catch(e){}
    document.addEventListener('keydown',function(e){if((e.ctrlKey||e.metaKey)&&e.key.toLowerCase()==='k'){e.preventDefault();openK()}if(e.key==='['){var pl=document.querySelector('a[href*="page="]');var prev=document.querySelector('a[href*="page="][href*="Anterior"],#prev-link');var link=document.getElementById('prev-link');if(link){link.click()}}if(e.key===']'){var link2=document.getElementById('next-link');if(link2){link2.click()}}});
    var gp=document.getElementById('go-page');if(gp){gp.addEventListener('click',function(){var inp=document.getElementById('page-jump');var v=inp&&inp.value?parseInt(inp.value,10):NaN;if(!isNaN(v)&&v>0){var url='/admin/gyms?ui=1&page='+v+'&page_size='+"{ps}"+'&q='+encodeURIComponent("{q}")+'&status='+'"+"{status_q}"+"'+'&order_by='+'"+"{order_by}"+"'+'&order_dir='+'"+"{order_dir}"+"';window.location.href=url}})}
    document.addEventListener('submit',function(e){var t=e.target;try{if(t&&t.tagName==='FORM'){var a=t.getAttribute('action')||'';var need=false;if(a.indexOf('/suspend')>=0||a.indexOf('/unsuspend')>=0||a.indexOf('/delete-bucket')>=0||a.indexOf('/b2/regenerate-key')>=0||a.match(/\\/admin\\/gyms\\/[0-9]+$/)){need=true}if(need){var ok=window.confirm('¿Confirmar la acción?');if(!ok){e.preventDefault();return false}}}}catch(err){}});
    var rows=document.querySelectorAll('tr[data-gym-id]');for(var i=0;i<rows.length;i++){(function(r){var sub=r.children[2];if(sub){sub.style.cursor='copy';sub.addEventListener('click',function(){var tx=sub.textContent||'';navigator.clipboard&&navigator.clipboard.writeText?navigator.clipboard.writeText(tx).then(function(){toast('Copiado: '+tx,'info')}).catch(function(){toast('No se pudo copiar','error')}):toast(tx,'info')})}})(rows[i])}
    });
    </script>
    """
    html = html + js
    return Response(content=html, media_type="text/html")

@admin_app.post("/gyms")
async def crear_gimnasio(request: Request, nombre: str = Form(...), subdominio: str = Form(...), owner_phone: Optional[str] = Form(None), whatsapp_phone_id: Optional[str] = Form(None), whatsapp_access_token: Optional[str] = Form(None), whatsapp_business_account_id: Optional[str] = Form(None), whatsapp_verify_token: Optional[str] = Form(None), whatsapp_app_secret: Optional[str] = Form(None), whatsapp_nonblocking: Optional[bool] = Form(False), whatsapp_send_timeout_seconds: Optional[float] = Form(None)):
    _require_admin(request)
    rl = _check_rate_limit(request, "gym_create", 20, 60)
    if rl:
        return rl
    adm = _get_admin_db()
    if adm is None:
        return JSONResponse({"error": "DB admin no disponible"}, status_code=500)
    res = adm.crear_gimnasio(nombre, subdominio, whatsapp_phone_id, whatsapp_access_token, owner_phone, whatsapp_business_account_id, whatsapp_verify_token, whatsapp_app_secret, whatsapp_nonblocking, whatsapp_send_timeout_seconds)
    try:
        adm.log_action("owner", "create_gym", res.get("id") if isinstance(res, dict) else None, f"{nombre}|{subdominio}")
    except Exception:
        pass
    if "error" in res:
        return JSONResponse(res, status_code=400)
    return JSONResponse(res, status_code=201)

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
    html = """
    <div class=\"dark\"><div style=\"max-width:640px;margin:0 auto;padding:20px;font-family:system-ui\"><h1 style=\"font-size:24px;font-weight:600\">Configurar WhatsApp</h1><div style=\"margin:12px 0\"><a href=\"/admin/gyms?ui=1\" style=\"padding:8px 12px;border-radius:6px;background:#111;color:#fff;text-decoration:none\">Volver</a></div><form method=\"post\" action=\"/admin/gyms/{gid}/whatsapp\" style=\"display:grid;gap:8px\"><input name=\"phone_id\" value=\"{pid}\" placeholder=\"Phone ID\" style=\"padding:8px;border:1px solid #333;border-radius:6px\"/><input name=\"access_token\" value=\"{tok}\" placeholder=\"Access Token\" style=\"padding:8px;border:1px solid #333;border-radius:6px\"/><input name=\"waba_id\" value=\"{waba}\" placeholder=\"WABA ID\" style=\"padding:8px;border:1px solid #333;border-radius:6px\"/><input name=\"verify_token\" value=\"{vt}\" placeholder=\"Verify Token\" style=\"padding:8px;border:1px solid #333;border-radius:6px\"/><input name=\"app_secret\" value=\"{asct}\" placeholder=\"App Secret\" style=\"padding:8px;border:1px solid #333;border-radius:6px\"/><label style=\"display:flex;align-items:center;gap:8px\"><input type=\"checkbox\" name=\"nonblocking\" {nb}/> Envío no bloqueante</label><input name=\"send_timeout_seconds\" value=\"{sto}\" placeholder=\"Timeout segundos\" style=\"padding:8px;border:1px solid #333;border-radius:6px\"/><button type=\"submit\" style=\"padding:10px 14px;border-radius:6px;background:#22c55e;color:#fff\">Guardar</button></form></div></div>
    """.replace("{gid}", str(int(gym_id))).replace("{pid}", pid).replace("{tok}", tok).replace("{waba}", waba).replace("{vt}", vt).replace("{asct}", asct).replace("{nb}", nb).replace("{sto}", sto)
    return Response(content=html, media_type="text/html")

@admin_app.post("/gyms/{gym_id}/whatsapp")
async def whatsapp_save(request: Request, gym_id: int, phone_id: Optional[str] = Form(None), access_token: Optional[str] = Form(None), waba_id: Optional[str] = Form(None), verify_token: Optional[str] = Form(None), app_secret: Optional[str] = Form(None), nonblocking: Optional[bool] = Form(False), send_timeout_seconds: Optional[float] = Form(None)):
    _require_admin(request)
    rl = _check_rate_limit(request, "whatsapp_save", 40, 60)
    if rl:
        return rl
    adm = _get_admin_db()
    if adm is None:
        return JSONResponse({"error": "DB admin no disponible"}, status_code=500)
    ok = adm.set_gym_whatsapp_config(int(gym_id), phone_id, access_token, waba_id, verify_token, app_secret, nonblocking, send_timeout_seconds)
    try:
        adm.log_action("owner", "set_whatsapp_config", int(gym_id), None)
    except Exception:
        pass
    return JSONResponse({"ok": bool(ok)}, status_code=200)

@admin_app.get("/gyms/{gym_id}/maintenance")
async def mantenimiento_form(request: Request, gym_id: int):
    _require_admin(request)
    html = """
    <div class=\"dark\"><div style=\"max-width:640px;margin:0 auto;padding:20px;font-family:system-ui\"><h1 style=\"font-size:24px;font-weight:600\">Modo mantenimiento</h1><div style=\"margin:12px 0\"><a href=\"/admin/gyms?ui=1\" style=\"padding:8px 12px;border-radius:6px;background:#111;color:#fff;text-decoration:none\">Volver</a></div><form method=\"post\" action=\"/admin/gyms/{gid}/maintenance\" style=\"display:grid;gap:8px\"><textarea name=\"message\" placeholder=\"Mensaje para usuarios\" style=\"padding:8px;border:1px solid #333;border-radius:6px;height:120px\"></textarea><button type=\"submit\" style=\"padding:10px 14px;border-radius:6px;background:#f59e0b;color:#000\">Activar mantenimiento</button></form><form method=\"post\" action=\"/admin/gyms/{gid}/maintenance/clear\" style=\"margin-top:12px\"><button type=\"submit\" style=\"padding:10px 14px;border-radius:6px;background:#16a34a;color:#fff\">Desactivar mantenimiento</button></form></div></div>
    """.replace("{gid}", str(int(gym_id)))
    return Response(content=html, media_type="text/html")

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
    rows = []
    for p in items:
        pid = str(p.get("id") or "")
        plan = str(p.get("plan") or "")
        amount = str(p.get("amount") or "")
        currency = str(p.get("currency") or "")
        paid_at = str(p.get("paid_at") or "")
        valid_until = str(p.get("valid_until") or "")
        status = str(p.get("status") or "")
        notes = str(p.get("notes") or "")
        rows.append(f"<tr><td>{pid}</td><td>{plan}</td><td>{amount}</td><td>{currency}</td><td>{paid_at}</td><td>{valid_until}</td><td>{status}</td><td>{notes}</td></tr>")
    html = """
    <div class="dark"><div style="max-width:1024px;margin:0 auto;padding:20px;font-family:system-ui"><h1 style="font-size:24px;font-weight:600">Pagos</h1><div style="margin:12px 0"><a href="/admin/gyms?ui=1" style="padding:8px 12px;border-radius:6px;background:#111;color:#fff;text-decoration:none">Volver</a></div><div style="overflow-x:auto"><table style="width:100%;border-collapse:collapse"><thead><tr><th>ID</th><th>Plan</th><th>Monto</th><th>Moneda</th><th>Pagado</th><th>Válido hasta</th><th>Status</th><th>Notas</th></tr></thead><tbody>{rows}</tbody></table></div><h2 style="font-size:18px;margin-top:16px">Registrar pago</h2><form method="post" action="/admin/gyms/{gid}/payments" style="display:grid;grid-template-columns:repeat(auto-fit,minmax(200px,1fr));gap:8px;margin-top:8px"><input name="plan" placeholder="Plan" style="padding:8px;border:1px solid #333;border-radius:6px"/><input name="amount" placeholder="Monto" type="number" step="0.01" style="padding:8px;border:1px solid #333;border-radius:6px"/><input name="currency" placeholder="Moneda" style="padding:8px;border:1px solid #333;border-radius:6px"/><input name="valid_until" placeholder="YYYY-MM-DD" style="padding:8px;border:1px solid #333;border-radius:6px"/><input name="status" placeholder="status" style="padding:8px;border:1px solid #333;border-radius:6px"/><input name="notes" placeholder="Notas" style="padding:8px;border:1px solid #333;border-radius:6px"/><button type="submit" style="padding:10px 14px;border-radius:6px;background:#2563eb;color:#fff">Guardar</button></form></div></div>
    """.replace("{rows}", "".join(rows)).replace("{gid}", str(int(gym_id)))
    return Response(content=html, media_type="text/html")

@admin_app.post("/gyms/{gym_id}/payments")
async def registrar_pago_gym(request: Request, gym_id: int, plan: Optional[str] = Form(None), amount: Optional[float] = Form(None), currency: Optional[str] = Form(None), valid_until: Optional[str] = Form(None), status: Optional[str] = Form(None), notes: Optional[str] = Form(None)):
    _require_admin(request)
    adm = _get_admin_db()
    if adm is None:
        return JSONResponse({"error": "DB admin no disponible"}, status_code=500)
    ok = adm.registrar_pago(int(gym_id), plan, amount, currency, valid_until, status, notes)
    try:
        details = f"{plan}|{amount}|{currency}|{valid_until}|{status}"
        adm.log_action("owner", "register_payment", int(gym_id), details)
    except Exception:
        pass
    return JSONResponse({"ok": bool(ok)}, status_code=200)

@admin_app.get("/plans")
async def listar_planes(request: Request):
    _require_admin(request)
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
    rows = []
    for p in items:
        pid = str(p.get('id') or '')
        name = str(p.get('name') or '')
        amount = str(p.get('amount') or '')
        currency = str(p.get('currency') or '')
        pdays = str(p.get('period_days') or '')
        active = bool(p.get('active'))
        toggle_lbl = 'Desactivar' if active else 'Activar'
        rows.append(f"<tr><td>{pid}</td><td><form method=\"post\" action=\"/admin/plans/{pid}\" style=\"display:flex;gap:6px;align-items:center\"><input name=\"name\" value=\"{name}\" style=\"padding:6px;border:1px solid #333;border-radius:6px\"/></td><td><input name=\"amount\" value=\"{amount}\" type=\"number\" step=\"0.01\" style=\"padding:6px;border:1px solid #333;border-radius:6px;width:120px\"/></td><td><input name=\"currency\" value=\"{currency}\" style=\"padding:6px;border:1px solid #333;border-radius:6px;width:100px\"/></td><td><input name=\"period_days\" value=\"{pdays}\" type=\"number\" style=\"padding:6px;border:1px solid #333;border-radius:6px;width:100px\"/></td><td><button type=\"submit\" style=\"padding:6px 10px;border-radius:6px;background:#22c55e;color:#fff\">Guardar</button></form></td><td><form method=\"post\" action=\"/admin/plans/{pid}/toggle\" style=\"display:inline\"><input type=\"hidden\" name=\"active\" value=\"{str(not active).lower()}\"/><button type=\"submit\" style=\"padding:6px 10px;border-radius:6px;background:#374151;color:#fff\">{toggle_lbl}</button></form></td></tr>")
    html = """
    <div class=\"dark\"><div style=\"max-width:900px;margin:0 auto;padding:20px;font-family:system-ui\"><h1 style=\"font-size:24px;font-weight:600\">Planes</h1><div style=\"margin:12px 0\"><a href=\"/admin/\" style=\"padding:8px 12px;border-radius:6px;background:#111;color:#fff;text-decoration:none\">Inicio</a></div><div style=\"overflow-x:auto\"><table style=\"width:100%;border-collapse:collapse\"><thead><tr><th>ID</th><th>Nombre</th><th>Monto</th><th>Moneda</th><th>Días</th><th>Acciones</th><th>Estado</th></tr></thead><tbody>{rows}</tbody></table></div><h2 style=\"font-size:18px;margin-top:16px\">Crear plan</h2><form method=\"post\" action=\"/admin/plans\" style=\"display:grid;grid-template-columns:repeat(auto-fit,minmax(200px,1fr));gap:8px;margin-top:8px\"><input name=\"name\" placeholder=\"Nombre\" required style=\"padding:8px;border:1px solid #333;border-radius:6px\"/><input name=\"amount\" type=\"number\" step=\"0.01\" placeholder=\"Monto\" required style=\"padding:8px;border:1px solid #333;border-radius:6px\"/><input name=\"currency\" placeholder=\"Moneda (e.g. ARS)\" required style=\"padding:8px;border:1px solid #333;border-radius:6px\"/><input name=\"period_days\" type=\"number\" placeholder=\"Días\" required style=\"padding:8px;border:1px solid #333;border-radius:6px\"/><button type=\"submit\" style=\"padding:10px 14px;border-radius:6px;background:#9333ea;color:#fff\">Crear</button></form></div></div>
    """.replace("{rows}", "".join(rows))
    return Response(content=html, media_type="text/html")

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
async def actualizar_plan(request: Request, plan_id: int, name: Optional[str] = Form(None), amount: Optional[float] = Form(None), currency: Optional[str] = Form(None), period_days: Optional[int] = Form(None)):
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
    if amount is not None:
        try:
            if float(amount) <= 0:
                return JSONResponse({"ok": False, "error": "invalid_amount"}, status_code=400)
        except Exception:
            return JSONResponse({"ok": False, "error": "invalid_amount"}, status_code=400)
    if period_days is not None:
        try:
            if int(period_days) <= 0:
                return JSONResponse({"ok": False, "error": "invalid_period"}, status_code=400)
        except Exception:
            return JSONResponse({"ok": False, "error": "invalid_period"}, status_code=400)
    ok = adm.actualizar_plan(int(plan_id), name, amount, currency, period_days)
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
    opts = []
    for p in planes:
        pid = str(p.get("id") or "")
        name = str(p.get("name") or "")
        selected = " selected" if sub and str(sub.get("plan_id")) == pid else ""
        opts.append(f"<option value=\"{pid}\"{selected}>{name}</option>")
    cur = "" if not sub else str(sub.get("start_date") or "")
    nd = "" if not sub else str(sub.get("next_due_date") or "")
    html = """
    <div class="dark"><div style="max-width:640px;margin:0 auto;padding:20px;font-family:system-ui"><h1 style="font-size:24px;font-weight:600">Suscripción</h1><div style="margin:12px 0"><a href="/admin/gyms?ui=1" style="padding:8px 12px;border-radius:6px;background:#111;color:#fff;text-decoration:none">Volver</a></div><div style="margin-top:8px"><p>Inicio actual: {cur}</p><p>Próximo vencimiento: {nd}</p></div><form method="post" action="/admin/gyms/{gid}/subscription" style="display:grid;gap:8px;margin-top:12px"><label>Plan<select name="plan_id" style="padding:8px;border:1px solid #333;border-radius:6px">{opts}</select></label><input name="start_date" placeholder="YYYY-MM-DD" required style="padding:8px;border:1px solid #333;border-radius:6px"/><button type="submit" style="padding:10px 14px;border-radius:6px;background:#22c55e;color:#fff">Guardar</button></form></div></div>
    """.replace("{opts}", "".join(opts)).replace("{cur}", cur).replace("{nd}", nd).replace("{gid}", str(int(gym_id)))
    return Response(content=html, media_type="text/html")

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
    rows = []
    for it in items:
        gid = str(it.get('gym_id') or '')
        nd = str(it.get('next_due_date') or '')
        rows.append(f"<tr><td>{gid}</td><td>{it.get('nombre')}</td><td>{it.get('subdominio')}</td><td>{nd}</td><td><form method=\"post\" action=\"/admin/gyms/{gid}/subscription/remind\" style=\"display:inline\"><input type=\"hidden\" name=\"message\" value=\"Recordatorio: vence el {nd}\"/><button type=\"submit\">Enviar recordatorio</button></form></td></tr>")
    html = """
    <div class="dark"><div style="max-width:800px;margin:0 auto;padding:20px;font-family:system-ui"><h1 style="font-size:24px;font-weight:600">Próximos vencimientos</h1><div style="margin:12px 0"><a href="/admin/" style="padding:8px 12px;border-radius:6px;background:#111;color:#fff;text-decoration:none">Inicio</a> <form method="post" action="/admin/subscriptions/remind" style="display:inline;margin-left:12px"><input name="days" value="{days}" style="padding:6px;border:1px solid #333;border-radius:6px;width:80px"/><button type="submit" style="margin-left:6px;padding:8px 12px;border-radius:6px;background:#1e40af;color:#fff">Enviar recordatorios</button></form></div><div style="overflow-x:auto"><table style="width:100%;border-collapse:collapse"><thead><tr><th>Gym ID</th><th>Nombre</th><th>Subdominio</th><th>Próximo vencimiento</th><th>Acciones</th></tr></thead><tbody>{rows}</tbody></table></div></div></div>
    """.replace("{rows}", "".join(rows)).replace("{days}", str(int(d)))
    return Response(content=html, media_type="text/html")

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
    rows = []
    for it in items:
        gid = str(it.get('gym_id') or '')
        nd = str(it.get('next_due_date') or '')
        nombre = str(it.get('nombre') or '')
        sub = str(it.get('subdominio') or '')
        rows.append(f"<tr><td><input type=\"checkbox\" name=\"gym_ids\" value=\"{gid}\"/></td><td>{gid}</td><td>{nombre}</td><td>{sub}</td><td>{nd}</td><td><form method=\"post\" action=\"/admin/gyms/{gid}/subscription/remind\" style=\"display:inline\"><input type=\"hidden\" name=\"message\" value=\"Recordatorio: vence el {nd}\"/><button type=\"submit\">Recordar</button></form></td></tr>")
    html = """
    <div class=\"dark\"><div style=\"max-width:960px;margin:0 auto;padding:20px;font-family:system-ui\"><h1 style=\"font-size:24px;font-weight:600\">Dashboard de vencimientos</h1><div style=\"margin:12px 0;display:flex;gap:12px;flex-wrap:wrap\"><a href=\"/admin/\" style=\"padding:8px 12px;border-radius:6px;background:#111;color:#fff;text-decoration:none\">Inicio</a><form method=\"get\" action=\"/admin/subscriptions/dashboard\" style=\"display:flex;gap:8px\"><input name=\"days\" value=\"{days}\" placeholder=\"Días\" style=\"padding:6px;border:1px solid #333;border-radius:6px;width:80px\"/><input name=\"q\" value=\"{q}\" placeholder=\"Buscar nombre o subdominio\" style=\"padding:6px;border:1px solid #333;border-radius:6px;min-width:240px\"/><button type=\"submit\" style=\"padding:8px 12px;border-radius:6px;background:#374151;color:#fff\">Filtrar</button></form><form method=\"post\" action=\"/admin/subscriptions/remind\" style=\"display:inline\"><input name=\"days\" value=\"{days}\" style=\"padding:6px;border:1px solid #333;border-radius:6px;width:80px\"/><button type=\"submit\" style=\"margin-left:6px;padding:8px 12px;border-radius:6px;background:#1e40af;color:#fff\">Recordar por ventana</button></form></div><div style=\"overflow-x:auto\"><form method=\"post\" action=\"/admin/subscriptions/remind-selected\"><table style=\"width:100%;border-collapse:collapse\"><thead><tr><th><input id=\"select-all\" type=\"checkbox\"/></th><th>Gym ID</th><th>Nombre</th><th>Subdominio</th><th>Próximo vencimiento</th><th>Acciones</th></tr></thead><tbody>{rows}</tbody></table><div style=\"margin-top:12px;display:flex;gap:8px\"><input name=\"message\" placeholder=\"Mensaje opcional\" style=\"padding:6px;border:1px solid #333;border-radius:6px;min-width:320px\"/><button type=\"submit\" style=\"padding:8px 12px;border-radius:6px;background:#16a34a;color:#fff\">Recordar seleccionados</button></div></form></div><script>var sa=document.getElementById('select-all');if(sa){sa.addEventListener('change',function(){var c=document.querySelectorAll('input[name=\"gym_ids\"]');for(var i=0;i<c.length;i++){c[i].checked=sa.checked;}});} </script></div></div>
    """.replace("{rows}", "".join(rows)).replace("{days}", str(int(d))).replace("{q}", (query or ""))
    return Response(content=html, media_type="text/html")

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
    html = """
    <div class=\"dark\"><div style=\"max-width:520px;margin:0 auto;padding:20px;font-family:system-ui\"><h1 style=\"font-size:22px;font-weight:600\">Contraseña de dueño</h1><div style=\"margin:12px 0\"><a href=\"/admin/gyms?ui=1\" style=\"padding:8px 12px;border-radius:6px;background:#111;color:#fff;text-decoration:none\">Volver</a></div><p style=\"margin:8px 0\">Gimnasio: {sub}</p><form method=\"post\" action=\"/admin/gyms/{gid}/owner\" style=\"display:grid;gap:8px\"><input type=\"password\" name=\"new_password\" placeholder=\"Nueva contraseña\" required style=\"padding:8px;border:1px solid #333;border-radius:6px\"/><input type=\"password\" name=\"confirm_password\" placeholder=\"Confirmar contraseña\" required style=\"padding:8px;border:1px solid #333;border-radius:6px\"/><button type=\"submit\" style=\"padding:10px 14px;border-radius:6px;background:#111;color:#fff\">Guardar</button></form></div></div>
    """.replace("{gid}", str(int(gym_id))).replace("{sub}", sub)
    return Response(content=html, media_type="text/html")

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
    <div class=\"dark\"><div style=\"max-width:480px;margin:0 auto;padding:20px;font-family:system-ui\"><h1 style=\"font-size:22px;font-weight:600\">Cambiar contraseña Admin</h1><div style=\"margin:12px 0\"><a href=\"/admin/\" style=\"padding:8px 12px;border-radius:6px;background:#111;color:#fff;text-decoration:none\">Inicio</a></div><form method=\"post\" action=\"/admin/owner/password\" style=\"display:grid;gap:8px\"><input type=\"password\" name=\"current_password\" placeholder=\"Contraseña actual\" required style=\"padding:8px;border:1px solid #333;border-radius:6px\"/><input type=\"password\" name=\"new_password\" placeholder=\"Nueva contraseña\" required style=\"padding:8px;border:1px solid #333;border-radius:6px\"/><input type=\"password\" name=\"confirm_password\" placeholder=\"Confirmar contraseña\" required style=\"padding:8px;border:1px solid #333;border-radius:6px\"/><button type=\"submit\" style=\"padding:10px 14px;border-radius:6px;background:#111;color:#fff\">Guardar</button></form></div></div>
    """
    return Response(content=html, media_type="text/html")

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
    return Response(content=html, media_type="text/html")

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
    _require_admin(request)
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
    if not wants_html:
        return JSONResponse({"templates": names}, status_code=200)
    items = "".join([f"<li style=\"padding:6px 0\">{n}</li>" for n in names])
    html = f"<div class=\"dark\"><div style=\"max-width:800px;margin:0 auto;padding:20px;font-family:system-ui\"><h1 style=\"font-size:24px;font-weight:600\">Templates WhatsApp</h1><div style=\"margin:12px 0\"><a href=\"/admin/\" style=\"padding:8px 12px;border-radius:6px;background:#111;color:#fff;text-decoration:none\">Inicio</a> <a href=\"/admin/templates/preview?ui=1\" style=\"padding:8px 12px;border-radius:6px;background:#374151;color:#fff;text-decoration:none;margin-left:8px\">Previsualizar</a></div><p>Se usan de forma estándar y no se personalizan por gimnasio.</p><ul>{items}</ul></div></div>"
    return Response(content=html, media_type="text/html")

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
    opts = []
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
    for n in base:
        sel = " selected" if n == name else ""
        opts.append(f"<option value=\"{n}\"{sel}>{n}</option>")
    html = """
    <div class=\"dark\"><div style=\"max-width:720px;margin:0 auto;padding:20px;font-family:system-ui\"><h1 style=\"font-size:24px;font-weight:600\">Previsualización de template</h1><div style=\"margin:12px 0\"><a href=\"/admin/templates?ui=1\" style=\"padding:8px 12px;border-radius:6px;background:#111;color:#fff;text-decoration:none\">Volver</a></div><form method=\"get\" action=\"/admin/templates/preview\" style=\"display:flex;gap:8px\"><select name=\"name\" style=\"padding:8px;border:1px solid #333;border-radius:6px;min-width:280px\">{opts}</select><input type=\"hidden\" name=\"ui\" value=\"1\"/><button type=\"submit\" style=\"padding:8px 12px;border-radius:6px;background:#374151;color:#fff\">Ver</button></form><div style=\"margin-top:16px;padding:12px;border:1px solid #333;border-radius:12px;background:#0b1222\"><div style=\"color:#9ca3af\">Texto</div><div style=\"font-size:16px\">{text}</div></div><h2 style=\"font-size:18px;margin-top:16px\">Enviar prueba</h2><form method=\"post\" action=\"/admin/gyms/test-send\" style=\"display:flex;gap:8px;flex-wrap:wrap\"><input name=\"gym_id\" placeholder=\"Gym ID\" style=\"padding:8px;border:1px solid #333;border-radius:6px;width:120px\"/><input name=\"name\" value=\"{name}\" style=\"padding:8px;border:1px solid #333;border-radius:6px;min-width:280px\"/><button type=\"submit\" style=\"padding:8px 12px;border-radius:6px;background:#16a34a;color:#fff\">Enviar</button></form></div></div>
    """.replace("{opts}", "".join(opts)).replace("{text}", text).replace("{name}", name)
    return Response(content=html, media_type="text/html")

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
    rl = _check_rate_limit(request, "health_check", 200, 60)
    if rl:
        return rl
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
            db_ok = DatabaseManager.test_connection(params=params, timeout_seconds=6)
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
    res = {"db": {"ok": bool(db_ok), "error": db_err}, "whatsapp": {"ok": bool(wa_ok), "status": wa_status}, "storage": {"ok": bool(st_ok), "configured": bool(st_cfg)}}
    if not wants_html:
        return JSONResponse(res, status_code=200)
    ok = lambda v: "<span style=\"color:#16a34a\">OK</span>" if v else "<span style=\"color:#ef4444\">Fallo</span>"
    html = f"<div class=\"dark\"><div style=\"max-width:640px;margin:0 auto;padding:20px;font-family:system-ui\"><h1 style=\"font-size:24px;font-weight:600\">Salud del gimnasio #{int(gym_id)}</h1><div style=\"margin:12px 0\"><a href=\"/admin/gyms?ui=1\" style=\"padding:8px 12px;border-radius:6px;background:#111;color:#fff;text-decoration:none\">Volver</a> <a href=\"/admin/gyms/{int(gym_id)}/health?ui=1\" style=\"padding:8px 12px;border-radius:6px;background:#374151;color:#fff;text-decoration:none;margin-left:8px\">Reverificar</a></div><div class=\"cards\" style=\"display:grid;grid-template-columns:repeat(auto-fit,minmax(220px,1fr));gap:12px\"><div style=\"padding:12px;border-radius:12px;border:1px solid #333;background:#111827\"><div style=\"font-weight:600\">Base de datos</div><div>{ok(db_ok)}</div><div style=\"color:#9ca3af\">{(db_err or '')}</div></div><div style=\"padding:12px;border-radius:12px;border:1px solid #333;background:#111827\"><div style=\"font-weight:600\">WhatsApp</div><div>{ok(wa_ok)}</div><div style=\"color:#9ca3af\">HTTP {str(wa_status or '')}</div></div><div style=\"padding:12px;border-radius:12px;border:1px solid #333;background:#111827\"><div style=\"font-weight:600\">Almacenamiento</div><div>{ok(st_ok)}</div><div style=\"color:#9ca3af\">Configurado: {('Sí' if st_cfg else 'No')}</div></div></div></div>"
    return Response(content=html, media_type="text/html")

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
    rows = []
    for it in list(payload.get("items") or []):
        rid = str(it.get("id") or "")
        actor_u = str(it.get("actor_username") or "")
        act = str(it.get("action") or "")
        gid = str(it.get("gym_id") or "")
        det = str(it.get("details") or "")
        when = str(it.get("created_at") or "")
        rows.append(f"<tr><td>{rid}</td><td>{actor_u}</td><td>{act}</td><td>{gid}</td><td>{det}</td><td>{when}</td></tr>")
    by_action = list(summary.get("by_action") or [])
    by_actor = list(summary.get("by_actor") or [])
    sa = "".join([f"<div style=\"padding:6px 10px;border:1px solid #333;border-radius:8px\"><div style=\"color:#9ca3af\">{str(x.get('action') or '')}</div><div style=\"font-weight:600\">{int(x.get('c') or 0)}</div></div>" for x in by_action])
    su = "".join([f"<div style=\"padding:6px 10px;border:1px solid #333;border-radius:8px\"><div style=\"color:#9ca3af\">{str(x.get('actor_username') or '')}</div><div style=\"font-weight:600\">{int(x.get('c') or 0)}</div></div>" for x in by_actor])
    html = """
    <div class=\"dark\"><div style=\"max-width:1200px;margin:0 auto;padding:20px;font-family:system-ui\"><h1 style=\"font-size:24px;font-weight:600\">Auditoría</h1><div style=\"margin:12px 0;display:flex;gap:12px;flex-wrap:wrap\"><a href=\"/admin/\" style=\"padding:8px 12px;border-radius:6px;background:#111;color:#fff;text-decoration:none\">Inicio</a><form method=\"get\" action=\"/admin/audit\" style=\"display:flex;gap:8px;flex-wrap:wrap\"><input name=\"actor\" value=\"{actor}\" placeholder=\"Actor\" style=\"padding:8px;border:1px solid #333;border-radius:6px\"/><input name=\"action\" value=\"{action}\" placeholder=\"Acción\" style=\"padding:8px;border:1px solid #333;border-radius:6px\"/><input name=\"gym_id\" value=\"{gid}\" placeholder=\"Gym ID\" style=\"padding:8px;border:1px solid #333;border-radius:6px;width:120px\"/><input name=\"from\" value=\"{from}\" placeholder=\"Desde YYYY-MM-DD\" style=\"padding:8px;border:1px solid #333;border-radius:6px\"/><input name=\"to\" value=\"{to}\" placeholder=\"Hasta YYYY-MM-DD\" style=\"padding:8px;border:1px solid #333;border-radius:6px\"/><input type=\"hidden\" name=\"ui\" value=\"1\"/><button type=\"submit\" style=\"padding:8px 12px;border-radius:6px;background:#1e40af;color:#fff\">Filtrar</button></form></div><div style=\"display:grid;grid-template-columns:repeat(auto-fit,minmax(160px,1fr));gap:8px\"><div style=\"font-weight:600\">Top acciones</div>{sa}<div style=\"font-weight:600\">Top actores</div>{su}</div><div style=\"overflow-x:auto;margin-top:12px\"><table style=\"width:100%;border-collapse:collapse\"><thead><tr><th>ID</th><th>Actor</th><th>Acción</th><th>Gym</th><th>Detalles</th><th>Fecha</th></tr></thead><tbody>{rows}</tbody></table></div></div></div>
    """.replace("{rows}", "".join(rows)).replace("{actor}", actor).replace("{action}", action).replace("{gid}", str(gym_id or "")).replace("{from}", from_date).replace("{to}", to_date).replace("{sa}", sa).replace("{su}", su)
    return Response(content=html, media_type="text/html")

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
    g = m.get("gyms") or {}
    w = m.get("whatsapp") or {}
    s = m.get("storage") or {}
    sub = m.get("subscriptions") or {}
    pay = m.get("payments") or {}
    cards = []
    def card(title, value):
        return f"<div style=\"padding:12px;border:1px solid #333;border-radius:12px;background:#0b1222\"><div style=\"color:#9ca3af\">{title}</div><div style=\"font-size:22px;font-weight:700;color:#fff\">{value}</div></div>"
    cards.append(card("Gimnasios", int(g.get("total") or 0)))
    cards.append(card("Activos", int(g.get("active") or 0)))
    cards.append(card("Suspendidos", int(g.get("suspended") or 0)))
    cards.append(card("Mantenimiento", int(g.get("maintenance") or 0)))
    cards.append(card("Nuevos 7d", int(g.get("last_7") or 0)))
    cards.append(card("Nuevos 30d", int(g.get("last_30") or 0)))
    cards.append(card("WhatsApp configurado", int(w.get("configured") or 0)))
    cards.append(card("Storage configurado", int(s.get("configured") or 0)))
    cards.append(card("Subs activas", int(sub.get("active") or 0)))
    cards.append(card("Subs vencidas", int(sub.get("overdue") or 0)))
    cards.append(card("Pagos 30d", float(pay.get("last_30_sum") or 0.0)))
    html = """
    <div class=\"dark\"><div style=\"max-width:1200px;margin:0 auto;padding:20px;font-family:system-ui\"><h1 style=\"font-size:24px;font-weight:600\">Métricas</h1><div style=\"margin:12px 0\"><a href=\"/admin/\" style=\"padding:8px 12px;border-radius:6px;background:#111;color:#fff;text-decoration:none\">Inicio</a></div><div style=\"display:grid;grid-template-columns:repeat(auto-fit,minmax(180px,1fr));gap:12px\">{cards}</div></div></div>
    """.replace("{cards}", "".join(cards))
    return Response(content=html, media_type="text/html")

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