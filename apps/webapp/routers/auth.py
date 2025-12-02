from pathlib import Path
import psycopg2
import psycopg2.extras
from fastapi import APIRouter, Request, Depends, status, Form
from fastapi.responses import RedirectResponse, HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates

from apps.webapp.dependencies import get_db
from apps.webapp.utils import (
    _verify_owner_password, _resolve_theme_vars, _resolve_logo_url, 
    get_gym_name
)

router = APIRouter()

# Setup templates
templates_dir = Path(__file__).resolve().parent.parent / "templates"
templates = Jinja2Templates(directory=str(templates_dir))

@router.get("/login", response_class=HTMLResponse)
async def public_login_page(request: Request, error: str = ""):
    theme_vars = _resolve_theme_vars()
    ctx = {
        "request": request,
        "theme": theme_vars,
        "error": error,
        "gym_name": get_gym_name("Gimnasio"),
        "logo_url": _resolve_logo_url(),
    }
    return templates.TemplateResponse("login.html", ctx)

@router.post("/login")
async def public_login_post(request: Request):
    try:
        form = await request.form()
        password = str(form.get("password") or "").strip()
    except Exception:
        password = ""
        
    if not password:
        return RedirectResponse(url="/login?error=Contrase%C3%B1a%20requerida", status_code=303)
        
    if _verify_owner_password(password):
        request.session.clear()
        request.session["logged_in"] = True
        request.session["role"] = "dueño"
        return RedirectResponse(url="/dashboard", status_code=303)
        
    return RedirectResponse(url="/login?error=Credenciales%20inv%C3%A1lidas", status_code=303)

@router.get("/usuario/login", response_class=HTMLResponse)
async def usuario_login_page(request: Request, error: str = ""):
    theme_vars = _resolve_theme_vars()
    ctx = {
        "request": request,
        "theme": theme_vars,
        "error": error,
        "gym_name": get_gym_name("Gimnasio"),
        "logo_url": _resolve_logo_url(),
    }
    return templates.TemplateResponse("usuario_login.html", ctx)

@router.post("/usuario/login")
async def usuario_login_post(request: Request):
    try:
        form = await request.form()
        dni = str(form.get("dni") or "").strip()
        pin = str(form.get("pin") or "").strip()
    except Exception:
        dni = ""
        pin = ""
        
    if not dni or not pin:
        return RedirectResponse(url="/usuario/login?error=Datos%20incompletos", status_code=303)

    db = get_db()
    if db is None:
         return RedirectResponse(url="/usuario/login?error=Error%20del%20sistema", status_code=303)

    user_id = None
    active = False
    
    # Verify user via DB
    try:
        with db.get_connection_context() as conn:  # type: ignore
            cur = conn.cursor()
            cur.execute("SELECT id, pin, activo FROM usuarios WHERE dni = %s", (dni,))
            row = cur.fetchone()
            if row:
                 uid, stored_pin, is_active = row
                 if str(stored_pin or "").strip() == pin:
                      user_id = uid
                      active = bool(is_active)
    except Exception:
        pass
        
    if user_id:
        if not active:
             return RedirectResponse(url="/usuario/login?error=Usuario%20inactivo", status_code=303)
             
        request.session.clear()
        request.session["user_id"] = int(user_id)
        return RedirectResponse(url="/usuario/panel", status_code=303)
        
    return RedirectResponse(url="/usuario/login?error=Credenciales%20inv%C3%A1lidas", status_code=303)

@router.get("/logout")
async def logout(request: Request):
    request.session.clear()
    return RedirectResponse(url="/login", status_code=303)

@router.get("/usuario/logout")
async def usuario_logout(request: Request):
    request.session.clear()
    return RedirectResponse(url="/usuario/login", status_code=303)

@router.get("/dashboard/logout")
async def dashboard_logout(request: Request):
    return await logout(request)

@router.get("/checkin/logout")
async def checkin_logout(request: Request):
    return await logout(request)

@router.get("/gestion/login", response_class=HTMLResponse)
async def login_page(request: Request, error: str = ""):
    theme_vars = _resolve_theme_vars()
    logo_url = _resolve_logo_url()
    gym_name = get_gym_name()
    return templates.TemplateResponse("login.html", {
        "request": request,
        "error": error,
        "theme_vars": theme_vars,
        "logo_url": logo_url,
        "gym_name": gym_name
    })

@router.get("/gestion/logout")
async def gestion_logout(request: Request):
    request.session.clear()
    return RedirectResponse(url="/gestion/login", status_code=303)

@router.post("/gestion/auth")
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
    
    db = get_db()
    if db is None:
        return RedirectResponse(url="/gestion/login?error=Base%20de%20datos%20no%20disponible", status_code=303)

    # Owner login logic
    if isinstance(usuario_id_raw, str) and usuario_id_raw == "__OWNER__":
        if not owner_password:
            return RedirectResponse(url="/gestion/login?error=Ingrese%20la%20contrase%C3%B1a", status_code=303)
        if _verify_owner_password(owner_password):
            request.session.clear()
            request.session["logged_in"] = True
            request.session["role"] = "dueño"
            return RedirectResponse(url="/gestion", status_code=303)
        return RedirectResponse(url="/gestion/login?error=Credenciales%20inv%C3%A1lidas", status_code=303)

    # Professor/User login logic
    try:
        usuario_id = int(usuario_id_raw) if usuario_id_raw is not None else None
    except Exception:
        usuario_id = None
        
    pin = str(pin_raw or "").strip()
    
    if not usuario_id or not pin:
        return RedirectResponse(url="/gestion/login?error=Par%C3%A1metros%20inv%C3%A1lidos", status_code=303)
        
    ok = False
    try:
        # Assuming verificar_pin_usuario returns truthy if valid
        ok = bool(db.verificar_pin_usuario(usuario_id, pin))  # type: ignore
    except Exception:
        ok = False
        
    if not ok:
        return RedirectResponse(url="/gestion/login?error=PIN%20inv%C3%A1lido", status_code=303)
        
    # Clear session but keep specific flags if needed? original server.py cleared it.
    try:
        request.session.clear()
    except Exception:
        pass
        
    # Setup session for user/professor
    profesor_id = None
    prof = None
    try:
        prof = db.obtener_profesor_por_usuario_id(usuario_id)  # type: ignore
    except Exception:
        prof = None
        
    user_role = None
    try:
        u = db.obtener_usuario_por_id(usuario_id)  # type: ignore
        if u is not None:
            # Handle both object and dict
            user_role = getattr(u, 'rol', None) or (u.get('rol') if isinstance(u, dict) else None)
    except Exception:
        user_role = None
        
    try:
        if prof:
            profesor_id = getattr(prof, 'profesor_id', None)
            if profesor_id is None and isinstance(prof, dict):
                profesor_id = prof.get('profesor_id')
        
        # Auto-create professor if role matches but no professor record exists
        if (profesor_id is None) and (user_role == 'profesor'):
            profesor_id = db.crear_profesor(usuario_id)  # type: ignore
    except Exception:
        profesor_id = None
        
    request.session["gestion_profesor_user_id"] = usuario_id
    try:
        request.session["role"] = "profesor"
    except Exception:
        pass
        
    if profesor_id:
        request.session["gestion_profesor_id"] = int(profesor_id)
        try:
            db.iniciar_sesion_trabajo_profesor(int(profesor_id), 'Trabajo')  # type: ignore
        except Exception:
            pass
            
    return RedirectResponse(url="/gestion", status_code=303)

@router.post("/api/auth/logout")
async def api_auth_logout(request: Request):
    request.session.clear()
    return JSONResponse({"ok": True})

@router.post("/checkin/auth")
async def checkin_auth(request: Request):
    try:
        form = await request.form()
        password = str(form.get("password") or "").strip()
    except Exception:
        password = ""
    
    if _verify_owner_password(password):
        request.session["logged_in"] = True
        request.session["role"] = "dueño"
        return RedirectResponse(url="/checkin", status_code=303)
        
    return RedirectResponse(url="/login?error=Credenciales%20inv%C3%A1lidas", status_code=303)

@router.post("/api/usuario/change_pin")
async def api_usuario_change_pin(request: Request):
    user_id = request.session.get("user_id")
    if not user_id:
        return JSONResponse({"ok": False, "error": "No autenticado"}, status_code=401)
        
    try:
        data = await request.json()
        new_pin = str(data.get("pin") or "").strip()
    except Exception:
        return JSONResponse({"ok": False, "error": "Datos inválidos"}, status_code=400)
        
    if not new_pin or len(new_pin) < 4:
        return JSONResponse({"ok": False, "error": "PIN inválido (mínimo 4 dígitos)"}, status_code=400)
        
    db = get_db()
    if db is None:
        return JSONResponse({"ok": False, "error": "DB error"}, status_code=500)
        
    try:
        with db.get_connection_context() as conn: # type: ignore
             cur = conn.cursor()
             cur.execute("UPDATE usuarios SET pin = %s WHERE id = %s", (new_pin, int(user_id)))
             conn.commit()
        return JSONResponse({"ok": True})
    except Exception as e:
        return JSONResponse({"ok": False, "error": str(e)}, status_code=500)
