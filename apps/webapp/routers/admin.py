import logging
import os
import json
from typing import Optional, List, Dict, Any

from fastapi import APIRouter, Request, Depends, HTTPException, status
from fastapi.responses import JSONResponse, HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from pathlib import Path

from apps.webapp.dependencies import get_db, get_admin_db, require_owner
from apps.webapp.utils import (
    _circuit_guard_json, _resolve_theme_vars, _resolve_logo_url, get_gym_name
)

router = APIRouter()
logger = logging.getLogger(__name__)

templates_dir = Path(__file__).resolve().parent.parent / "templates"
templates = Jinja2Templates(directory=str(templates_dir))

@router.get("/gestion", response_class=HTMLResponse)
async def gestion_index(request: Request):
    if not request.session.get("logged_in") and not request.session.get("gestion_profesor_id"):
         return RedirectResponse(url="/gestion/login", status_code=303)
         
    theme_vars = _resolve_theme_vars()
    ctx = {
        "request": request,
        "theme": theme_vars,
        "gym_name": get_gym_name("Gimnasio"),
        "logo_url": _resolve_logo_url(),
    }
    return templates.TemplateResponse("gestion.html", ctx)

@router.get("/dashboard", response_class=HTMLResponse)
async def dashboard(request: Request):
    if not request.session.get("logged_in"):
         return RedirectResponse(url="/login", status_code=303)

    theme_vars = _resolve_theme_vars()
    ctx = {
        "request": request,
        "theme": theme_vars,
        "gym_name": get_gym_name("Gimnasio"),
        "logo_url": _resolve_logo_url(),
    }
    return templates.TemplateResponse("dashboard.html", ctx)

@router.post("/api/admin/owner-password")
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
            data = await request.form()
    except Exception:
        data = {}

    dev_pwd = str(data.get("dev_password", "")).strip()
    new_pwd = str(data.get("new_password", "")).strip()

    if not dev_pwd or not new_pwd:
        return JSONResponse({"success": False, "message": "Parámetros inválidos"}, status_code=400)
    if len(new_pwd) < 4:
        return JSONResponse({"success": False, "message": "La nueva contraseña debe tener al menos 4 caracteres"}, status_code=400)

    # Validar fortaleza de la contraseña
    try:
        from core.security_utils import SecurityUtils
        is_strong, message = SecurityUtils.validate_password_strength(new_pwd)
        if not is_strong:
            return JSONResponse({"success": False, "message": f"Contraseña débil: {message}"}, status_code=400)
    except Exception:
        pass

    # Resolver DEV_PASSWORD real
    real_dev = None
    try:
        # Try env var first
        real_dev = os.getenv("DEV_PASSWORD", "").strip()
        if not real_dev:
            # Try SecureConfig
            try:
                from core.secure_config import SecureConfig as _SC
                real_dev = str(_SC.get_dev_password()).strip()
            except ImportError:
                # Fallback if apps.core is not directly importable
                try:
                    from core.secure_config import SecureConfig as _SC
                    real_dev = str(_SC.get_dev_password()).strip()
                except Exception:
                    pass
    except Exception:
        real_dev = None

    if not real_dev or dev_pwd != real_dev:
        return JSONResponse({"success": False, "message": "No autorizado"}, status_code=401)

    db = get_db()
    if not db:
        return JSONResponse({"success": False, "message": "DatabaseManager no disponible"}, status_code=500)

    try:
        ok = False
        
        # Hashear la nueva contraseña antes de almacenarla
        try:
            from core.security_utils import SecurityUtils
            hashed_pwd = SecurityUtils.hash_password(new_pwd)
        except Exception:
            hashed_pwd = new_pwd
        
        if hasattr(db, 'actualizar_configuracion'):
            ok = bool(db.actualizar_configuracion('owner_password', hashed_pwd))  # type: ignore
        else:
            # Fallback SQL directo si el método no existe
            with db.get_connection_context() as conn:  # type: ignore
                cur = conn.cursor()
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
                    (hashed_pwd,),
                )
                conn.commit()
                ok = True
        if ok:
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

@router.post("/api/admin/renumerar_usuarios")
async def api_admin_renumerar_usuarios(request: Request, _=Depends(require_owner)):
    """
    Renumera de forma segura los IDs de usuarios empezando desde "start_id".
    Actualiza referencias en todas las tablas relacionadas.
    Acceso: dueño (sesión web de Gestión).
    """
    db = get_db()
    if db is None:
        raise HTTPException(status_code=503, detail="DB no disponible")
    guard = _circuit_guard_json(db, "/api/admin/renumerar_usuarios")
    if guard:
        return guard
    try:
        content_type = (request.headers.get("content-type", "") or "").strip()
        if content_type.startswith("application/json"):
            payload = await request.json()
        else:
            try:
                payload = await request.form()
            except Exception:
                payload = {}
    except Exception:
        payload = {}
    try:
        start_id_raw = payload.get("start_id", 1)
        try:
            start_id = int(start_id_raw)
        except Exception:
            start_id = 1
        if start_id < 1:
            return JSONResponse({"success": False, "message": "start_id inválido"}, status_code=400)
        res = db.renumerar_usuario_ids(start_id)  # type: ignore
        if isinstance(res, dict):
            return JSONResponse(res, status_code=200)
        return JSONResponse({"success": True, "result": res}, status_code=200)
    except HTTPException:
        raise
    except Exception as e:
        try:
            logging.exception("Error en /api/admin/renumerar_usuarios")
        except Exception:
            pass
        return JSONResponse({"success": False, "error": str(e)}, status_code=500)

@router.post("/api/admin/secure_owner")
async def api_admin_secure_owner(request: Request, _=Depends(require_owner)):
    """
    Asegura la existencia del usuario Dueño y su protección.
    """
    db = get_db()
    if db is None:
        raise HTTPException(status_code=503, detail="DB no disponible")
    guard = _circuit_guard_json(db, "/api/admin/secure_owner")
    if guard:
        return guard
    try:
        changes = []
        owner_id = None
        with db.get_connection_context() as conn:  # type: ignore
            cur = conn.cursor()
            try:
                cur.execute("SET LOCAL session_replication_role = 'replica'")
            except Exception:
                pass
            try:
                cur.execute("ALTER TABLE usuarios NO FORCE ROW LEVEL SECURITY")
            except Exception:
                pass
            try:
                cur.execute("ALTER TABLE usuarios DISABLE ROW LEVEL SECURITY")
            except Exception:
                pass
            
            cur.execute("SELECT id FROM usuarios WHERE rol = 'dueño' LIMIT 1")
            row = cur.fetchone()
            if row is None:
                cur.execute("SELECT 1 FROM usuarios WHERE id = 1")
                id1_exists = cur.fetchone() is not None
                target_id = 1
                if id1_exists:
                    cur.execute("SELECT COALESCE(MAX(id), 0) + 1 FROM usuarios")
                    target_id = int((cur.fetchone() or [1])[0] or 1)
                _owner_pin = "2203"
                cur.execute(
                    """
                    INSERT INTO usuarios (id, nombre, dni, telefono, pin, rol, activo, tipo_cuota)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (target_id, "DUEÑO DEL GIMNASIO", "00000000", "N/A", _owner_pin, "dueño", True, "estandar"),
                )
                owner_id = target_id
                changes.append({"created_owner_id": target_id})
            else:
                try:
                    owner_id = int(row[0])
                except Exception:
                    owner_id = row[0]
            
            if owner_id != 1:
                cur.execute("SELECT 1 FROM usuarios WHERE id = 1")
                id1_exists = cur.fetchone() is not None
                if id1_exists:
                    cur.execute("SELECT COALESCE(MAX(id), 0) + 1 FROM usuarios")
                    new_id_for_old1 = int((cur.fetchone() or [2])[0] or 2)
                    updates = [
                        ("UPDATE pagos SET usuario_id = %s WHERE usuario_id = %s", (new_id_for_old1, 1)),
                        ("UPDATE asistencias SET usuario_id = %s WHERE usuario_id = %s", (new_id_for_old1, 1)),
                        ("UPDATE rutinas SET usuario_id = %s WHERE usuario_id = %s", (new_id_for_old1, 1)),
                        ("UPDATE clase_usuarios SET usuario_id = %s WHERE usuario_id = %s", (new_id_for_old1, 1)),
                        ("UPDATE clase_lista_espera SET usuario_id = %s WHERE usuario_id = %s", (new_id_for_old1, 1)),
                        ("UPDATE usuario_notas SET usuario_id = %s WHERE usuario_id = %s", (new_id_for_old1, 1)),
                        ("UPDATE usuario_etiquetas SET usuario_id = %s WHERE usuario_id = %s", (new_id_for_old1, 1)),
                        ("UPDATE usuario_estados SET usuario_id = %s WHERE usuario_id = %s", (new_id_for_old1, 1)),
                        ("UPDATE profesores SET usuario_id = %s WHERE usuario_id = %s", (new_id_for_old1, 1)),
                        ("UPDATE notificaciones_cupos SET usuario_id = %s WHERE usuario_id = %s", (new_id_for_old1, 1)),
                        ("UPDATE audit_logs SET user_id = %s WHERE user_id = %s", (new_id_for_old1, 1)),
                        ("UPDATE checkin_pending SET usuario_id = %s WHERE usuario_id = %s", (new_id_for_old1, 1)),
                        ("UPDATE whatsapp_messages SET user_id = %s WHERE user_id = %s", (new_id_for_old1, 1)),
                        ("UPDATE usuario_notas SET autor_id = %s WHERE autor_id = %s", (new_id_for_old1, 1)),
                        ("UPDATE usuario_etiquetas SET asignado_por = %s WHERE asignado_por = %s", (new_id_for_old1, 1)),
                        ("UPDATE usuario_estados SET creado_por = %s WHERE creado_por = %s", (new_id_for_old1, 1)),
                    ]
                    for sql, params in updates:
                        try:
                            cur.execute(sql, params)
                        except Exception:
                            pass
                    try:
                        cur.execute(
                            "UPDATE acciones_masivas_pendientes SET usuario_ids = array_replace(usuario_ids, %s, %s) WHERE %s = ANY(usuario_ids)",
                            (1, new_id_for_old1, 1),
                        )
                    except Exception:
                        pass
                    cur.execute("UPDATE usuarios SET id = %s WHERE id = %s AND rol <> 'dueño'", (new_id_for_old1, 1))
                    changes.append({"moved_user": {"from": 1, "to": new_id_for_old1}})
                cur.execute("UPDATE usuarios SET id = 1 WHERE id = %s AND rol = 'dueño'", (owner_id,))
                changes.append({"owner_id_changed": {"from": owner_id, "to": 1}})
                owner_id = 1
            conn.commit()
            return JSONResponse({"success": True, "changes": changes})
    except Exception as e:
        return JSONResponse({"success": False, "error": str(e)}, status_code=500)

@router.get("/api/admin/reminder")
async def api_admin_reminder(request: Request):
    try:
        db = get_db()
    except Exception:
        db = None
    if db is None:
        return JSONResponse({"active": False, "message": ""})
    try:
        msg = db.obtener_configuracion("admin_reminder_message")  # type: ignore
        act = db.obtener_configuracion("admin_reminder_active")  # type: ignore
        active = str(act or "").strip().lower() in ("1", "true", "yes", "on")
        return JSONResponse({"active": bool(active), "message": str(msg or "")})
    except Exception:
        return JSONResponse({"active": False, "message": ""})
