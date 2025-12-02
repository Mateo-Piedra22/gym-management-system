import logging
import psycopg2
import psycopg2.extras
from datetime import datetime, timezone, date
from typing import Optional, List, Dict, Any

from fastapi import APIRouter, Request, Depends, HTTPException, status
from fastapi.responses import JSONResponse, HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from pathlib import Path

from apps.webapp.dependencies import get_db, require_gestion_access, require_owner
from apps.webapp.utils import (
    _circuit_guard_json, _resolve_theme_vars, _resolve_logo_url, get_gym_name
)
from core.models import Usuario

# Fallback for UsuarioEstado if not imported correctly or available
try:
    from core.models import UsuarioEstado
except ImportError:
    UsuarioEstado = None

router = APIRouter()
logger = logging.getLogger(__name__)

templates_dir = Path(__file__).resolve().parent.parent / "templates"
templates = Jinja2Templates(directory=str(templates_dir))

@router.get("/usuario/panel", response_class=HTMLResponse)
async def usuario_panel(request: Request):
    user_id = request.session.get("user_id")
    if not user_id:
        return RedirectResponse(url="/usuario/login", status_code=303)

    db = get_db()
    if db is None:
         return HTMLResponse("Error de base de datos", status_code=500)

    try:
        u = db.obtener_usuario_por_id(int(user_id))
    except Exception:
        u = None
        
    if not u:
        request.session.clear()
        return RedirectResponse(url="/usuario/login", status_code=303)

    # Basic calculation
    dias_restantes = None
    fpv = getattr(u, 'fecha_proximo_vencimiento', None)
    if fpv:
         try:
             if isinstance(fpv, str):
                 fpv = datetime.fromisoformat(fpv).date()
             elif isinstance(fpv, datetime):
                 fpv = fpv.date()
             
             if fpv:
                 from datetime import date
                 delta = (fpv - date.today()).days
                 dias_restantes = delta
         except Exception:
             pass

    # Pagos
    pagos = []
    try:
        with db.get_connection_context() as conn:  # type: ignore
             cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
             cur.execute("SELECT * FROM pagos WHERE usuario_id = %s ORDER BY fecha_pago DESC LIMIT 10", (int(user_id),))
             pagos = cur.fetchall() or []
    except Exception:
        pass

    # Rutinas
    rutinas = []
    try:
        with db.get_connection_context() as conn:  # type: ignore
             cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
             cur.execute("SELECT * FROM rutinas WHERE usuario_id = %s AND activa = TRUE", (int(user_id),))
             rutinas = cur.fetchall() or []
    except Exception:
        pass

    theme_vars = _resolve_theme_vars()
    ctx = {
        "request": request,
        "theme": theme_vars,
        "gym_name": get_gym_name("Gimnasio"),
        "logo_url": _resolve_logo_url(),
        "usuario": u,
        "active": bool(getattr(u, 'activo', False)),
        "dias_restantes": dias_restantes,
        "ultimo_pago": getattr(u, 'ultimo_pago', None),
        "pagos": pagos,
        "rutinas": rutinas
    }
    return templates.TemplateResponse("usuario_panel.html", ctx)

# --- API Usuarios ---

@router.get("/api/usuarios")
async def api_usuarios_list(q: Optional[str] = None, limit: int = 50, offset: int = 0, _=Depends(require_gestion_access)):
    db = get_db()
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

@router.get("/api/usuarios/{usuario_id}")
async def api_usuario_get(usuario_id: int, request: Request, _=Depends(require_gestion_access)):
    db = get_db()
    if db is None:
        raise HTTPException(status_code=503, detail="DB no disponible")
    guard = _circuit_guard_json(db, "/api/usuarios/{id}")
    if guard:
        return guard
    try:
        u = db.obtener_usuario_por_id(usuario_id)  # type: ignore
        if not u:
            raise HTTPException(status_code=404, detail="Usuario no encontrado")
        # Aplicar visibilidad de PIN: profesor no ve PIN de otro profesor
        pin_value = getattr(u, "pin", None)
        try:
            prof_uid = request.session.get("gestion_profesor_user_id")
            if prof_uid and str(getattr(u, "rol", "")).strip().lower() == "profesor" and int(usuario_id) != int(prof_uid):
                pin_value = None
        except Exception:
            pass
        return {
            "id": u.id,
            "nombre": u.nombre,
            "dni": u.dni,
            "telefono": u.telefono,
            "pin": pin_value,
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

@router.post("/api/usuarios")
async def api_usuario_create(request: Request, _=Depends(require_gestion_access)):
    db = get_db()
    if db is None:
        raise HTTPException(status_code=503, detail="DB no disponible")
    guard = _circuit_guard_json(db, "/api/usuarios")
    if guard:
        return guard
    payload = await request.json()
    try:
        nombre = ((payload.get("nombre") or "").strip()).upper()
        dni = str(payload.get("dni") or "").strip()
        telefono = str(payload.get("telefono") or "").strip() or None
        pin_raw = payload.get("pin") if isinstance(payload, dict) else None
        pin = None
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

@router.put("/api/usuarios/{usuario_id}")
async def api_usuario_update(usuario_id: int, request: Request, _=Depends(require_gestion_access)):
    db = get_db()
    if db is None:
        raise HTTPException(status_code=503, detail="DB no disponible")
    guard = _circuit_guard_json(db, "/api/usuarios/{id}")
    if guard:
        return guard
    payload = await request.json()
    try:
        if not db.usuario_id_existe(usuario_id):  # type: ignore
            raise HTTPException(status_code=404, detail="Usuario no encontrado")
        nombre = ((payload.get("nombre") or "").strip()).upper()
        dni = str(payload.get("dni") or "").strip()
        telefono = str(payload.get("telefono") or "").strip() or None
        pin_raw = payload.get("pin") if isinstance(payload, dict) else None
        pin = None
        try:
            orig = db.obtener_usuario_por_id(usuario_id)  # type: ignore
        except Exception:
            orig = None
        try:
            session_prof_uid = request.session.get("gestion_profesor_user_id")
            if session_prof_uid and orig and str(getattr(orig, "rol", "")).strip().lower() == "profesor" and int(usuario_id) != int(session_prof_uid):
                pin = getattr(orig, "pin", None)
            else:
                if (payload is not None) and ("pin" in payload):
                    pv = pin_raw
                    try:
                        pv_str = str(pv).strip() if pv is not None else None
                    except Exception:
                        pv_str = None
                    if pv_str:
                        pin = pv_str
                    else:
                        pin = getattr(orig, "pin", None) if orig is not None else None
                else:
                    pin = getattr(orig, "pin", None) if orig is not None else None
        except Exception:
            pass
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
        # Manejar cambio de ID si el dueño lo solicita
        try:
            new_id_raw = payload.get("new_id")
        except Exception:
            new_id_raw = None
        new_id_val = None
        try:
            new_id_val = int(new_id_raw) if new_id_raw is not None else None
        except Exception:
            new_id_val = None
        if new_id_val is not None and int(new_id_val) != int(usuario_id):
            is_owner = bool(request.session.get("logged_in")) and str(request.session.get("role") or "").strip().lower() == "dueño"
            if not is_owner:
                raise HTTPException(status_code=403, detail="Solo el dueño puede cambiar el ID de usuario")
            if int(new_id_val) <= 0:
                raise HTTPException(status_code=400, detail="El nuevo ID debe ser un entero positivo")
            try:
                db.cambiar_usuario_id(int(usuario_id), int(new_id_val))  # type: ignore
                usuario_id = int(new_id_val)
            except PermissionError as e:
                raise HTTPException(status_code=403, detail=str(e))
            except ValueError as e:
                raise HTTPException(status_code=400, detail=str(e))
            except Exception as e:
                raise HTTPException(status_code=500, detail=str(e))
        return {"ok": True, "id": usuario_id}
    except HTTPException:
        raise
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)

@router.delete("/api/usuarios/{usuario_id}")
async def api_usuario_delete(usuario_id: int, _=Depends(require_gestion_access)):
    db = get_db()
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

# --- API Etiquetas de usuario ---
@router.get("/api/usuarios/{usuario_id}/etiquetas")
async def api_usuario_etiquetas_get(usuario_id: int, _=Depends(require_gestion_access)):
    db = get_db()
    if db is None:
        raise HTTPException(status_code=503, detail="DB no disponible")
    guard = _circuit_guard_json(db, "/api/usuarios/{id}/etiquetas")
    if guard:
        return guard
    try:
        if not db.usuario_id_existe(usuario_id):  # type: ignore
            raise HTTPException(status_code=404, detail="Usuario no encontrado")
        etiquetas = db.obtener_etiquetas_usuario(usuario_id)  # type: ignore
        items = []
        for e in etiquetas:
            try:
                items.append({
                    "id": getattr(e, "id", None),
                    "nombre": getattr(e, "nombre", None),
                    "color": getattr(e, "color", None),
                    "descripcion": getattr(e, "descripcion", None),
                    "activo": getattr(e, "activo", True),
                })
            except Exception:
                items.append(e)
        return {"items": items}
    except HTTPException:
        raise
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)

@router.post("/api/usuarios/{usuario_id}/etiquetas")
async def api_usuario_etiquetas_add(usuario_id: int, request: Request, _=Depends(require_gestion_access)):
    db = get_db()
    if db is None:
        raise HTTPException(status_code=503, detail="DB no disponible")
    guard = _circuit_guard_json(db, "/api/usuarios/{id}/etiquetas")
    if guard:
        return guard
    payload = await request.json()
    try:
        if not db.usuario_id_existe(usuario_id):  # type: ignore
            raise HTTPException(status_code=404, detail="Usuario no encontrado")
        etiqueta_id = payload.get("etiqueta_id")
        nombre = (payload.get("nombre") or "").strip()
        asignado_por = None
        try:
            asignado_por = int(request.session.get("user_id")) if request.session.get("user_id") else None
        except Exception:
            asignado_por = None
        if etiqueta_id is None and not nombre:
            raise HTTPException(status_code=400, detail="Se requiere 'etiqueta_id' o 'nombre'")
        if etiqueta_id is None and nombre:
            try:
                et = db.obtener_o_crear_etiqueta(nombre)  # type: ignore
                etiqueta_id = getattr(et, "id", None)
            except Exception:
                etiqueta_id = None
        if etiqueta_id is None:
            raise HTTPException(status_code=400, detail="Etiqueta inválida")
        ok = db.asignar_etiqueta_usuario(usuario_id, int(etiqueta_id), asignado_por)  # type: ignore
        return {"ok": bool(ok)}
    except HTTPException:
        raise
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)

@router.delete("/api/usuarios/{usuario_id}/etiquetas/{etiqueta_id}")
async def api_usuario_etiquetas_remove(usuario_id: int, etiqueta_id: int, _=Depends(require_gestion_access)):
    db = get_db()
    if db is None:
        raise HTTPException(status_code=503, detail="DB no disponible")
    guard = _circuit_guard_json(db, "/api/usuarios/{id}/etiquetas/{etiqueta_id}")
    if guard:
        return guard
    try:
        if not db.usuario_id_existe(usuario_id):  # type: ignore
            raise HTTPException(status_code=404, detail="Usuario no encontrado")
        ok = db.desasignar_etiqueta_usuario(usuario_id, etiqueta_id)  # type: ignore
        return {"ok": bool(ok)}
    except HTTPException:
        raise
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)

# --- API Estados de usuario ---
@router.get("/api/usuarios/{usuario_id}/estados")
async def api_usuario_estados_get(usuario_id: int, _=Depends(require_gestion_access)):
    db = get_db()
    if db is None:
        raise HTTPException(status_code=503, detail="DB no disponible")
    guard = _circuit_guard_json(db, "/api/usuarios/{id}/estados")
    if guard:
        return guard
    try:
        if not db.usuario_id_existe(usuario_id):  # type: ignore
            raise HTTPException(status_code=404, detail="Usuario no encontrado")
        estados = db.obtener_estados_usuario(usuario_id, solo_activos=True)  # type: ignore
        items = []
        for est in estados:
            try:
                items.append({
                    "id": getattr(est, "id", None),
                    "usuario_id": getattr(est, "usuario_id", None),
                    "estado": getattr(est, "estado", None),
                    "descripcion": getattr(est, "descripcion", None),
                    "fecha_inicio": getattr(est, "fecha_inicio", None),
                    "fecha_vencimiento": getattr(est, "fecha_vencimiento", None),
                    "activo": getattr(est, "activo", True),
                    "creado_por": getattr(est, "creado_por", None),
                })
            except Exception:
                items.append(est)
        return {"items": items}
    except HTTPException:
        raise
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)

@router.get("/api/usuarios_morosidad_ids")
async def api_usuarios_morosidad_ids(_=Depends(require_gestion_access)):
    db = get_db()
    if db is None:
        return []
    guard = _circuit_guard_json(db, "/api/usuarios_morosidad_ids")
    if guard:
        return guard
    try:
        with db.get_connection_context() as conn:  # type: ignore
            cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            cur.execute(
                """
                SELECT usuario_id
                FROM usuario_estados
                WHERE activo = TRUE
                  AND LOWER(estado) = 'desactivado_por_morosidad'
                """
            )
            rows = cur.fetchall() or []
            ids = []
            for r in rows:
                try:
                    uid = int(r.get("usuario_id"))
                    ids.append(uid)
                except Exception:
                    continue
            return ids
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)

@router.post("/api/usuarios/{usuario_id}/estados")
async def api_usuario_estados_add(usuario_id: int, request: Request, _=Depends(require_gestion_access)):
    db = get_db()
    if db is None:
        raise HTTPException(status_code=503, detail="DB no disponible")
    guard = _circuit_guard_json(db, "/api/usuarios/{id}/estados")
    if guard:
        return guard
    payload = await request.json()
    try:
        if not db.usuario_id_existe(usuario_id):  # type: ignore
            raise HTTPException(status_code=404, detail="Usuario no encontrado")
        nombre = (payload.get("estado") or payload.get("nombre") or "").strip()
        descripcion = (payload.get("descripcion") or "").strip() or None
        fecha_vencimiento = payload.get("fecha_vencimiento") or payload.get("fecha_fin")
        creado_por = None
        try:
            creado_por = int(request.session.get("user_id")) if request.session.get("user_id") else None
        except Exception:
            creado_por = None
        if not nombre:
            raise HTTPException(status_code=400, detail="'estado' es obligatorio")
        try:
            # Ensure UsuarioEstado is imported or mock it if we are in fallback
            if UsuarioEstado is not None:
                estado = UsuarioEstado(usuario_id=usuario_id, estado=nombre, descripcion=descripcion, fecha_vencimiento=fecha_vencimiento, creado_por=creado_por)  # type: ignore
            else:
                estado = type("E", (), {"usuario_id": usuario_id, "estado": nombre, "descripcion": descripcion, "fecha_vencimiento": fecha_vencimiento, "creado_por": creado_por})()
        except Exception:
            estado = type("E", (), {"usuario_id": usuario_id, "estado": nombre, "descripcion": descripcion, "fecha_vencimiento": fecha_vencimiento, "creado_por": creado_por})()
        eid = db.crear_estado_usuario(estado)  # type: ignore
        return {"ok": True, "id": int(eid)}
    except HTTPException:
        raise
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)

@router.put("/api/usuarios/{usuario_id}/estados/{estado_id}")
async def api_usuario_estados_update(usuario_id: int, estado_id: int, request: Request, _=Depends(require_gestion_access)):
    db = get_db()
    if db is None:
        raise HTTPException(status_code=503, detail="DB no disponible")
    guard = _circuit_guard_json(db, "/api/usuarios/{id}/estados/{estado_id}")
    if guard:
        return guard
    payload = await request.json()
    try:
        if not db.usuario_id_existe(usuario_id):  # type: ignore
            raise HTTPException(status_code=404, detail="Usuario no encontrado")
        nombre = (payload.get("estado") or payload.get("nombre") or "").strip()
        descripcion = (payload.get("descripcion") or "").strip() or None
        fecha_vencimiento = payload.get("fecha_vencimiento") or payload.get("fecha_fin")
        activo = bool(payload.get("activo", True))
        usuario_modificador = None
        try:
            usuario_modificador = int(request.session.get("user_id")) if request.session.get("user_id") else None
        except Exception:
            usuario_modificador = None
        try:
            if UsuarioEstado is not None:
                estado = UsuarioEstado(id=estado_id, usuario_id=usuario_id, estado=nombre, descripcion=descripcion, fecha_vencimiento=fecha_vencimiento, activo=activo)  # type: ignore
            else:
                estado = type("E", (), {"id": estado_id, "usuario_id": usuario_id, "estado": nombre, "descripcion": descripcion, "fecha_vencimiento": fecha_vencimiento, "activo": activo})()
        except Exception:
            estado = type("E", (), {"id": estado_id, "usuario_id": usuario_id, "estado": nombre, "descripcion": descripcion, "fecha_vencimiento": fecha_vencimiento, "activo": activo})()
        ok = db.actualizar_estado_usuario(estado, usuario_modificador=usuario_modificador)  # type: ignore
        return {"ok": bool(ok)}
    except HTTPException:
        raise
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)

@router.delete("/api/usuarios/{usuario_id}/estados/{estado_id}")
async def api_usuario_estados_delete(usuario_id: int, estado_id: int, request: Request, _=Depends(require_gestion_access)):
    db = get_db()
    if db is None:
        raise HTTPException(status_code=503, detail="DB no disponible")
    guard = _circuit_guard_json(db, "/api/usuarios/{id}/estados/{estado_id}")
    if guard:
        return guard
    try:
        if not db.usuario_id_existe(usuario_id):  # type: ignore
            raise HTTPException(status_code=404, detail="Usuario no encontrado")
        usuario_modificador = None
        try:
            usuario_modificador = int(request.session.get("user_id")) if request.session.get("user_id") else None
        except Exception:
            usuario_modificador = None
        ok = db.eliminar_estado_usuario(int(estado_id), usuario_modificador=usuario_modificador)  # type: ignore
        return {"ok": bool(ok)}
    except HTTPException:
        raise
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)

@router.get("/api/estados/plantillas")
async def api_estados_plantillas(_=Depends(require_gestion_access)):
    db = get_db()
    if db is None:
        return {"items": []}
    guard = _circuit_guard_json(db, "/api/estados/plantillas")
    if guard:
        return guard
    try:
        items = db.obtener_plantillas_estados()  # type: ignore
        return {"items": items}
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)

# --- API Profesores ---

@router.get("/api/profesores_basico")
async def api_profesores_basico(_=Depends(require_gestion_access)):
    db = get_db()
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

@router.get("/api/profesores_detalle")
async def api_profesores_detalle(request: Request, _=Depends(require_owner)):
    db = get_db()
    if db is None:
        return []
    try:
        start = request.query_params.get("start")
        end = request.query_params.get("end")
        if not start or (isinstance(start, str) and start.strip() == ""):
            start = None
        if not end or (isinstance(end, str) and end.strip() == ""):
            end = None

        from datetime import datetime as _dt
        from datetime import datetime
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
        now = datetime.now()
        mes_actual = now.month
        anio_actual = now.year

        with db.get_connection_context() as conn:  # type: ignore
            cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            cur.execute(
                """
                WITH sesiones AS (
                    SELECT profesor_id,
                           COUNT(*) AS sesiones_mes,
                           COALESCE(SUM(minutos_totales) / 60.0, 0) AS horas_mes
                    FROM profesor_horas_trabajadas
                    WHERE hora_fin IS NOT NULL
                      AND (
                        ( %s IS NOT NULL AND %s IS NOT NULL AND fecha BETWEEN %s AND %s )
                        OR ( (%s IS NULL OR %s IS NULL) AND EXTRACT(MONTH FROM fecha) = %s AND EXTRACT(YEAR FROM fecha) = %s )
                      )
                    GROUP BY profesor_id
                ),
                horarios AS (
                    SELECT hp.profesor_id,
                           COUNT(hp.id) AS horarios_count,
                           JSON_AGG(
                               JSON_BUILD_OBJECT(
                                   'dia', hp.dia_semana,
                                   'inicio', hp.hora_inicio::text,
                                   'fin', hp.hora_fin::text
                               )
                               ORDER BY CASE hp.dia_semana 
                                   WHEN 'Lunes' THEN 1 
                                   WHEN 'Martes' THEN 2 
                                   WHEN 'Miércoles' THEN 3 
                                   WHEN 'Jueves' THEN 4 
                                   WHEN 'Viernes' THEN 5 
                                   WHEN 'Sábado' THEN 6 
                                   WHEN 'Domingo' THEN 7 
                               END, hp.hora_inicio
                           ) AS horarios
                    FROM horarios_profesores hp
                    GROUP BY hp.profesor_id
                )
                SELECT p.id AS id,
                       COALESCE(u.nombre,'') AS nombre,
                       ''::text AS email,
                       COALESCE(u.telefono,'') AS telefono,
                       COALESCE(h.horarios_count, 0) AS horarios_count,
                       COALESCE(h.horarios, '[]'::json) AS horarios,
                       COALESCE(s.sesiones_mes, 0) AS sesiones_mes,
                       COALESCE(s.horas_mes, 0) AS horas_mes
                FROM profesores p
                JOIN usuarios u ON u.id = p.usuario_id
                LEFT JOIN horarios h ON h.profesor_id = p.id
                LEFT JOIN sesiones s ON s.profesor_id = p.id
                ORDER BY p.id
                """,
                (
                    start_date, end_date, start_date, end_date,
                    start_date, end_date, mes_actual, anio_actual
                )
            )
            rows = cur.fetchall() or []
        return rows
    except Exception as e:
        logging.exception("Error final en /api/profesores_detalle")
        return []

@router.get("/api/profesores/{profesor_id}")
async def api_profesor_get(profesor_id: int, _=Depends(require_owner)):
    db = get_db()
    if db is None:
        return JSONResponse({"error": "DB no disponible"}, status_code=500)
    try:
        with db.get_connection_context() as conn:  # type: ignore
            cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            try:
                cur.execute("SELECT column_name FROM information_schema.columns WHERE table_name='profesores'")
                cols = {row.get("column_name") for row in (cur.fetchall() or [])}
            except Exception:
                cols = set()
            selects = [
                "p.id AS id",
                "p.usuario_id AS usuario_id",
                "COALESCE(u.nombre,'') AS usuario_nombre",
                "COALESCE(u.telefono,'') AS usuario_telefono",
                "COALESCE(u.notas,'') AS usuario_notas",
            ]
            if "sueldo" in cols:
                selects.append("p.sueldo AS sueldo")
            elif "salario" in cols:
                selects.append("p.salario AS salario")
            if "notas" in cols:
                selects.append("p.notas AS notas")
            if "tipo" in cols:
                selects.append("p.tipo AS tipo")
            if "especialidades" in cols:
                selects.append("p.especialidades AS especialidades")
            if "certificaciones" in cols:
                selects.append("p.certificaciones AS certificaciones")
            if "experiencia_años" in cols:
                selects.append("p.experiencia_años AS experiencia_años")
            if "tarifa_por_hora" in cols:
                selects.append("p.tarifa_por_hora AS tarifa_por_hora")
            if "fecha_contratacion" in cols:
                selects.append("p.fecha_contratacion AS fecha_contratacion")
            if "biografia" in cols:
                selects.append("p.biografia AS biografia")
            if "telefono_emergencia" in cols:
                selects.append("p.telefono_emergencia AS telefono_emergencia")
            sql = f"SELECT {', '.join(selects)} FROM profesores p LEFT JOIN usuarios u ON u.id = p.usuario_id WHERE p.id = %s"
            cur.execute(sql, (profesor_id,))
            row = cur.fetchone()
            if not row:
                return JSONResponse({"error": "not_found"}, status_code=404)
            sueldo_val = None
            try:
                if "sueldo" in row and row["sueldo"] is not None:
                    sueldo_val = float(row["sueldo"])  # type: ignore
                elif "salario" in row and row["salario"] is not None:
                    sueldo_val = float(row["salario"])  # type: ignore
            except Exception:
                sueldo_val = row.get("sueldo") or row.get("salario") or None
            tarifa_val = None
            try:
                if "tarifa_por_hora" in row and row["tarifa_por_hora"] is not None:
                    tarifa_val = float(row["tarifa_por_hora"])  # type: ignore
            except Exception:
                tarifa_val = row.get("tarifa_por_hora") or None
            experiencia_val = None
            try:
                if "experiencia_años" in row and row["experiencia_años"] is not None:
                    experiencia_val = int(row["experiencia_años"])  # type: ignore
            except Exception:
                experiencia_val = row.get("experiencia_años") or None
            return {
                "profesor_id": int(row.get("id") or profesor_id),
                "usuario_id": int(row.get("usuario_id") or 0),
                "usuario_nombre": row.get("usuario_nombre") or "",
                "usuario_telefono": row.get("usuario_telefono") or "",
                "usuario_notas": row.get("usuario_notas"),
                "sueldo": sueldo_val,
                "notas": row.get("notas"),
                "tipo": row.get("tipo"),
                "especialidades": row.get("especialidades"),
                "certificaciones": row.get("certificaciones"),
                "experiencia_años": experiencia_val,
                "tarifa_por_hora": tarifa_val,
                "fecha_contratacion": row.get("fecha_contratacion"),
                "biografia": row.get("biografia"),
                "telefono_emergencia": row.get("telefono_emergencia"),
            }
    except Exception as e:
        logging.exception("Error en /api/profesores/{id}")
        return JSONResponse({"error": str(e)}, status_code=500)

@router.put("/api/profesores/{profesor_id}")
async def api_profesor_update(profesor_id: int, request: Request, _=Depends(require_owner)):
    db = get_db()
    if db is None:
        return JSONResponse({"error": "DB no disponible"}, status_code=500)
    try:
        try:
            payload = await request.json()
        except Exception:
            payload = {}
        usuario_id = payload.get("usuario_id")
        sueldo = payload.get("sueldo")
        salario = payload.get("salario")
        notas = payload.get("notas")
        tipo = payload.get("tipo") or payload.get("especialidad")
        especialidades = payload.get("especialidades")
        certificaciones = payload.get("certificaciones")
        experiencia = payload.get("experiencia_años", payload.get("experiencia"))
        tarifa = payload.get("tarifa_por_hora", payload.get("tarifa"))
        fecha_contratacion = payload.get("fecha_contratacion")
        biografia = payload.get("biografia")
        telefono_emergencia = payload.get("telefono_emergencia")
        with db.get_connection_context() as conn:  # type: ignore
            cur = conn.cursor()
            try:
                cur2 = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
                cur2.execute("SELECT column_name FROM information_schema.columns WHERE table_name='profesores'")
                cols = {r.get("column_name") for r in (cur2.fetchall() or [])}
            except Exception:
                cols = set()
            sets = []
            params = []
            if (sueldo is not None or salario is not None) and ("sueldo" in cols or "salario" in cols):
                val = sueldo if sueldo is not None else salario
                try:
                    val_num = float(val) if val is not None else None
                except Exception:
                    val_num = None
                if "sueldo" in cols:
                    sets.append("sueldo = %s")
                    params.append(val_num)
                elif "salario" in cols:
                    sets.append("salario = %s")
                    params.append(val_num)
            if "notas" in cols and notas is not None:
                sets.append("notas = %s")
                params.append(str(notas))
            if "tipo" in cols and tipo is not None:
                sets.append("tipo = %s")
                params.append(str(tipo))
            if "especialidades" in cols and especialidades is not None:
                sets.append("especialidades = %s")
                params.append(str(especialidades))
            if "certificaciones" in cols and certificaciones is not None:
                sets.append("certificaciones = %s")
                params.append(str(certificaciones))
            if "experiencia_años" in cols and experiencia is not None:
                try:
                    exp_num = int(experiencia)
                except Exception:
                    exp_num = 0
                sets.append("experiencia_años = %s")
                params.append(exp_num)
            if "tarifa_por_hora" in cols and tarifa is not None:
                try:
                    tarifa_num = float(tarifa)
                except Exception:
                    tarifa_num = 0.0
                sets.append("tarifa_por_hora = %s")
                params.append(tarifa_num)
            if "fecha_contratacion" in cols and fecha_contratacion is not None:
                sets.append("fecha_contratacion = %s")
                params.append(str(fecha_contratacion))
            if "biografia" in cols and biografia is not None:
                sets.append("biografia = %s")
                params.append(str(biografia))
            if "telefono_emergencia" in cols and telefono_emergencia is not None:
                sets.append("telefono_emergencia = %s")
                params.append(str(telefono_emergencia))
            
            if sets:
                sql = f"UPDATE profesores SET {', '.join(sets)} WHERE id = %s"
                params.append(int(profesor_id))
                cur.execute(sql, params)
                try:
                    conn.commit()
                except Exception:
                    pass
                return {"success": True, "updated": 1}
            else:
                # Fallback update notes in users table
                updated = False
                try:
                    if notas is not None and "notas" not in cols:
                        uid = None
                        try:
                            uid = int(usuario_id) if usuario_id is not None else None
                        except Exception:
                            uid = None
                        if uid is None:
                            try:
                                cur2 = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
                                cur2.execute("SELECT usuario_id FROM profesores WHERE id = %s", (int(profesor_id),))
                                row2 = cur2.fetchone() or {}
                                uid = int(row2.get("usuario_id")) if row2.get("usuario_id") is not None else None
                            except Exception:
                                uid = None
                        if uid:
                            cur.execute("UPDATE usuarios SET notas = %s WHERE id = %s", (str(notas), int(uid)))
                            try:
                                conn.commit()
                            except Exception:
                                pass
                            updated = True
                except Exception:
                    updated = False
                return {"success": updated, "updated": 1 if updated else 0}
    except Exception as e:
        logging.exception("Error en /api/profesores/{id} PUT")
        return JSONResponse({"error": str(e)}, status_code=500)

# --- Endpoints de detalle para pagos y asistencias (Profesor) ---
# (Including only the professor session ones here as requested)

@router.get("/api/profesor_sesiones")
async def api_profesor_sesiones(request: Request, _=Depends(require_owner)):
    db = get_db()
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
        from datetime import datetime
        fecha_inicio = None
        fecha_fin = None
        try:
            if start:
                fecha_inicio = datetime.strptime(start, "%Y-%m-%d").date()
            if end:
                fecha_fin = datetime.strptime(end, "%Y-%m-%d").date()
        except Exception:
            fecha_inicio = None
            fecha_fin = None

        try:
            sesiones = db.obtener_horas_trabajadas_profesor(profesor_id, fecha_inicio, fecha_fin)  # type: ignore
        except Exception:
            sesiones = []

        out = []
        for s in sesiones or []:
            minutos_val = s.get("minutos_totales")
            fin_val = s.get("hora_fin")
            try:
                minutos_num = int(minutos_val) if minutos_val is not None else 0
            except Exception:
                minutos_num = 0
            if fin_val is None:
                continue
            def _fmt_date(d):
                try:
                    return str(d)[:10] if d is not None else ""
                except Exception:
                    return str(d) if d is not None else ""
            def _fmt_time(t):
                try:
                    if t is None:
                        return ""
                    hh = getattr(t, "hour", None)
                    mm = getattr(t, "minute", None)
                    if hh is not None and mm is not None:
                        return f"{int(hh):02d}:{int(mm):02d}"
                    s = str(t)
                    for sep in ("T", " "):
                        if sep in s:
                            tail = s.split(sep, 1)[1]
                            if len(tail) >= 5 and tail[0:2].isdigit() and tail[2] == ":" and tail[3:5].isdigit():
                                return tail[:5]
                    if len(s) >= 5 and s[0:2].isdigit() and s[2] == ":" and s[3:5].isdigit():
                        return s[:5]
                    try:
                        from datetime import datetime as _dt
                        dt = _dt.fromisoformat(s.replace('Z', '+00:00'))
                        return f"{dt.hour:02d}:{dt.minute:02d}"
                    except Exception:
                        pass
                    return ""
                except Exception:
                    return ""
            out.append({
                "id": s.get("id"),
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

@router.put("/api/profesor_sesion/{sesion_id}")
async def api_profesor_sesion_update(sesion_id: int, request: Request, _=Depends(require_gestion_access)):
    db = get_db()
    if db is None:
        raise HTTPException(status_code=503, detail="DB no disponible")
    guard = _circuit_guard_json(db, "/api/profesor_sesion_update")
    if guard:
        return guard
    try:
        try:
            body = await request.json()
        except Exception:
            body = {}
        if not isinstance(body, dict):
            body = {}
        fecha = body.get("fecha")
        inicio = body.get("inicio")
        fin = body.get("fin")
        tipo = body.get("tipo")
        minutos_raw = body.get("minutos")
        minutos_int = None
        if minutos_raw is not None:
            try:
                minutos_int = int(minutos_raw)
                if minutos_int < 0:
                    minutos_int = 0
            except Exception:
                minutos_int = None

        if fecha is not None and not isinstance(fecha, str):
            fecha = None
        if inicio is not None and not isinstance(inicio, str):
            inicio = None
        if fin is not None and not isinstance(fin, str):
            fin = None
        if tipo is not None and not isinstance(tipo, str):
            tipo = None

        try:
            result = db.actualizar_profesor_sesion(  # type: ignore
                sesion_id,
                fecha=fecha,
                hora_inicio=inicio,
                hora_fin=fin,
                tipo_actividad=tipo,
                minutos_totales=minutos_int,
            )
        except Exception as e:
            logging.exception("Error en actualizar_profesor_sesion")
            raise HTTPException(status_code=500, detail=str(e))

        if not result or not result.get("success"):
            msg = (result or {}).get("error") or "No se pudo actualizar la sesión"
            status = 400
            if msg == "Sesión no encontrada":
                status = 404
            elif msg == "ID de sesión inválido":
                status = 400
            elif msg == "Sin cambios para aplicar":
                status = 400
            return JSONResponse(status_code=status, content={"detail": msg})

        updated_raw = result.get("updated")
        def _fmt_date(d):
            try:
                return str(d)[:10] if d is not None else None
            except Exception:
                return str(d) if d is not None else None
        def _fmt_time(t):
            try:
                sstr = str(t) if t is not None else None
                if sstr is None:
                    return None
                return sstr[:5] if len(sstr) >= 5 else sstr
            except Exception:
                return None
        raw_dict = None
        try:
            if updated_raw is not None:
                raw_dict = dict(updated_raw)
        except Exception:
            raw_dict = updated_raw if isinstance(updated_raw, dict) else None
        updated_safe = None
        if isinstance(raw_dict, dict):
            updated_safe = {
                "id": int(raw_dict.get("id")) if raw_dict.get("id") is not None else None,
                "profesor_id": int(raw_dict.get("profesor_id")) if raw_dict.get("profesor_id") is not None else None,
                "fecha": _fmt_date(raw_dict.get("fecha")),
                "hora_inicio": _fmt_time(raw_dict.get("hora_inicio")),
                "hora_fin": _fmt_time(raw_dict.get("hora_fin")),
                "minutos_totales": int(raw_dict.get("minutos_totales") or 0),
                "horas_totales": float(raw_dict.get("horas_totales") or 0.0),
                "tipo_actividad": raw_dict.get("tipo_actividad") or None,
            }

        return JSONResponse(status_code=200, content={"success": True, "updated": updated_safe})
    except HTTPException:
        raise
    except Exception as e:
        logging.exception("Error en /api/profesor_sesion/{sesion_id} PUT")
        raise HTTPException(status_code=500, detail=str(e))

@router.delete("/api/profesor_sesion/{sesion_id}")
async def api_profesor_sesion_delete(sesion_id: int, _=Depends(require_gestion_access)):
    db = get_db()
    if db is None:
        raise HTTPException(status_code=503, detail="DB no disponible")
    guard = _circuit_guard_json(db, "/api/profesor_sesion_delete")
    if guard:
        return guard
    try:
        res = db.eliminar_profesor_sesion(sesion_id)  # type: ignore
        if not res or not res.get("success"):
            msg = (res or {}).get("error") or "No se pudo eliminar la sesión"
            status = 400
            if msg == "Sesión no encontrada":
                status = 404
            elif msg == "ID de sesión inválido":
                status = 400
            return JSONResponse(status_code=status, content={"detail": msg})
        return JSONResponse(status_code=200, content={"success": True, "deleted_id": res.get("deleted_id")})
    except Exception as e:
        logging.exception("Error en /api/profesor_sesion/{sesion_id} DELETE")
        raise HTTPException(status_code=500, detail=str(e))

# --- CRUD Horarios de profesores ---

@router.get("/api/profesor_horarios")
async def api_profesor_horarios(request: Request, _=Depends(require_gestion_access)):
    db = get_db()
    if db is None:
        return []
    guard = _circuit_guard_json(db, "/api/profesor_horarios")
    if guard:
        return guard
    try:
        pid = request.query_params.get("profesor_id")
        is_owner = bool(request.session.get("logged_in"))
        ses_prof_id = request.session.get("gestion_profesor_id")
        profesor_id = None
        if is_owner:
            if not pid:
                return []
            try:
                profesor_id = int(pid)
            except Exception:
                return []
        else:
            try:
                profesor_id = int(ses_prof_id) if ses_prof_id is not None else None
            except Exception:
                profesor_id = None
            if profesor_id is None:
                return []
        try:
            items = db.obtener_horarios_disponibilidad_profesor(profesor_id)  # type: ignore
        except Exception:
            items = []
        def _fmt_time(t):
            try:
                if t is None:
                    return ""
                hh = getattr(t, "hour", None)
                mm = getattr(t, "minute", None)
                if hh is not None and mm is not None:
                    return f"{int(hh):02d}:{int(mm):02d}"
                s = str(t)
                for sep in ("T", " "):
                    if sep in s:
                        tail = s.split(sep, 1)[1]
                        if len(tail) >= 5 and tail[0:2].isdigit() and tail[2] == ":" and tail[3:5].isdigit():
                            return tail[:5]
                if len(s) >= 5 and s[0:2].isdigit() and s[2] == ":" and s[3:5].isdigit():
                    return s[:5]
                try:
                    from datetime import datetime as _dt
                    dt = _dt.fromisoformat(s.replace('Z', '+00:00'))
                    return f"{dt.hour:02d}:{dt.minute:02d}"
                except Exception:
                    pass
                return ""
            except Exception:
                return ""
        out = []
        for h in (items or []):
            out.append({
                "id": h.get("id"),
                "dia": h.get("dia_semana"),
                "inicio": _fmt_time(h.get("hora_inicio")),
                "fin": _fmt_time(h.get("hora_fin")),
                "disponible": bool(h.get("disponible", True)),
            })
        return out
    except Exception as e:
        logging.exception("Error en /api/profesor_horarios [GET]")
        return []

@router.post("/api/profesor_horarios")
async def api_profesor_horarios_create(request: Request, _=Depends(require_owner)):
    db = get_db()
    if db is None:
        return JSONResponse({"error": "no_db"}, status_code=500)
    guard = _circuit_guard_json(db, "/api/profesor_horarios[POST]")
    if guard:
        return guard
    try:
        data = await request.json()
        profesor_id = data.get("profesor_id")
        dia = data.get("dia")
        inicio = data.get("inicio")
        fin = data.get("fin")
        disponible = data.get("disponible")
        if profesor_id is None or dia is None or inicio is None or fin is None:
            return JSONResponse({"error": "missing_fields"}, status_code=400)
        try:
            profesor_id = int(profesor_id)
        except Exception:
            return JSONResponse({"error": "invalid_profesor_id"}, status_code=400)
        try:
            def _parse_time(s):
                if s is None:
                    raise ValueError("hora_requerida")
                ss = str(s).strip()
                if not ss:
                    raise ValueError("hora_requerida")
                from datetime import datetime as _dt
                for fmt in ("%H:%M:%S", "%H:%M"):
                    try:
                        return _dt.strptime(ss, fmt).time()
                    except Exception:
                        pass
                raise ValueError("formato_invalido")
            tinicio = _parse_time(inicio)
            tfin = _parse_time(fin)
            if not (tinicio < tfin):
                return JSONResponse({"error": "invalid_time_range"}, status_code=400)
        except ValueError as ve:
            msg = str(ve)
            if msg == "hora_requerida":
                return JSONResponse({"error": "times_required"}, status_code=400)
            return JSONResponse({"error": "invalid_time_format"}, status_code=400)
        disponible_val = bool(disponible) if disponible is not None else True
        try:
            created = db.crear_horario_profesor(profesor_id, str(dia), str(inicio), str(fin), disponible_val)  # type: ignore
        except Exception as e:
            logging.exception("Error crear horario profesor")
            msg = str(e)
            if "Profesor no existe" in msg:
                return JSONResponse({"error": "profesor_not_found"}, status_code=404)
            if "Día inválido" in msg:
                return JSONResponse({"error": "invalid_day"}, status_code=400)
            if "hora_inicio debe ser menor" in msg:
                return JSONResponse({"error": "invalid_time_range"}, status_code=400)
            if "horas_requeridas" in msg:
                return JSONResponse({"error": "times_required"}, status_code=400)
            if "violates foreign key constraint" in msg:
                return JSONResponse({"error": "profesor_not_found"}, status_code=404)
            return JSONResponse({"error": msg}, status_code=500)
        return {"ok": True, "horario": created}
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)

        return {"ok": bool(updated), "horario": updated}
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)

@router.delete("/api/profesor_horarios/{horario_id}")
async def api_profesor_horarios_delete(horario_id: int, _=Depends(require_owner)):
    db = get_db()
    if db is None:
        return JSONResponse({"error": "no_db"}, status_code=500)
    guard = _circuit_guard_json(db, "/api/profesor_horarios[DELETE]")
    if guard:
        return guard
    try:
        try:
            deleted = db.eliminar_horario_profesor(horario_id)  # type: ignore
        except Exception as e:
            logging.exception("Error eliminar horario profesor")
            return JSONResponse({"error": str(e)}, status_code=500)
        return {"ok": bool(deleted)}
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)

# --- Sesiones de trabajo de profesores ---

@router.post("/api/profesor_sesion_inicio")
async def api_profesor_sesion_inicio(request: Request, _=Depends(require_gestion_access)):
    db = get_db()
    if db is None:
        return JSONResponse({"error": "no_db"}, status_code=500)
    guard = _circuit_guard_json(db, "/api/profesor_sesion_inicio")
    if guard:
        return guard
    try:
        data = await request.json()
        profesor_id = data.get("profesor_id")
        tipo = data.get("tipo") or data.get("tipo_actividad") or "Trabajo"
        # Determinar permisos según rol: dueño puede iniciar para cualquiera; profesor sólo para sí mismo
        is_owner = bool(request.session.get("logged_in"))
        ses_prof_id = request.session.get("gestion_profesor_id")
        effective_prof_id = None
        if is_owner:
            if profesor_id is None:
                return JSONResponse({"error": "missing_fields"}, status_code=400)
            try:
                effective_prof_id = int(profesor_id)
            except Exception:
                return JSONResponse({"error": "invalid_profesor_id"}, status_code=400)
        else:
            try:
                effective_prof_id = int(ses_prof_id) if ses_prof_id is not None else None
            except Exception:
                effective_prof_id = None
            if effective_prof_id is None:
                return JSONResponse({"error": "invalid_profesor_session"}, status_code=403)
        try:
            res = db.iniciar_sesion_trabajo_profesor(effective_prof_id, str(tipo))  # type: ignore
        except Exception as e:
            logging.exception("Error iniciar sesión trabajo profesor")
            return JSONResponse({"error": str(e)}, status_code=500)
        # Serialización segura de la sesión y consistencia de campos
        def _fmt_date(d):
            try:
                return str(d)[:10] if d is not None else None
            except Exception:
                return None
        def _fmt_time(t):
            try:
                if t is None:
                    return None
                hh = getattr(t, "hour", None)
                mm = getattr(t, "minute", None)
                if hh is not None and mm is not None:
                    return f"{int(hh):02d}:{int(mm):02d}"
                s = str(t)
                for sep in ("T", " "):
                    if sep in s:
                        tail = s.split(sep, 1)[1]
                        if len(tail) >= 5 and tail[0:2].isdigit() and tail[2] == ":" and tail[3:5].isdigit():
                            return tail[:5]
                if len(s) >= 5 and s[0:2].isdigit() and s[2] == ":" and s[3:5].isdigit():
                    return s[:5]
                try:
                    from datetime import datetime as _dt
                    dt = _dt.fromisoformat(s.replace('Z', '+00:00'))
                    return f"{dt.hour:02d}:{dt.minute:02d}"
                except Exception:
                    pass
                return None
            except Exception:
                return None
        datos = None
        try:
            datos = res.get("datos") if isinstance(res, dict) else None
        except Exception:
            datos = None
        raw = None
        if datos is not None:
            try:
                raw = dict(datos)
            except Exception:
                raw = datos if isinstance(datos, dict) else None
        sesion_safe = None
        if isinstance(raw, dict):
            minutos = int(raw.get("minutos_totales") or 0)
            horas = round(minutos / 60.0, 2)
            sesion_safe = {
                "id": int(raw.get("id") or 0),
                "profesor_id": int(raw.get("profesor_id") or 0),
                "fecha": _fmt_date(raw.get("fecha")),
                "hora_inicio": _fmt_time(raw.get("hora_inicio")),
                "hora_fin": _fmt_time(raw.get("hora_fin")),
                "minutos_totales": minutos,
                "horas_totales": horas,
                "tipo_actividad": raw.get("tipo_actividad") or str(tipo),
            }
        success_val = bool((res or {}).get("success")) if isinstance(res, dict) else True
        mensaje = (res or {}).get("mensaje") if isinstance(res, dict) else None
        return JSONResponse(status_code=200, content={"success": success_val, "mensaje": mensaje, "sesion": sesion_safe})
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)

@router.post("/api/profesor_sesion_fin")
async def api_profesor_sesion_fin(request: Request, _=Depends(require_gestion_access)):
    db = get_db()
    if db is None:
        return JSONResponse({"error": "no_db"}, status_code=500)
    guard = _circuit_guard_json(db, "/api/profesor_sesion_fin")
    if guard:
        return guard
    try:
        data = {}
        try:
            data = await request.json()
        except Exception:
            data = {}
        profesor_id = data.get("profesor_id") or request.session.get("gestion_profesor_id")
        if profesor_id is None:
            return JSONResponse({"error": "missing_fields"}, status_code=400)
        try:
            profesor_id = int(profesor_id)
        except Exception:
            return JSONResponse({"error": "invalid_profesor_id"}, status_code=400)
        try:
            res = db.finalizar_sesion_trabajo_profesor(profesor_id)  # type: ignore
        except Exception as e:
            logging.exception("Error finalizar sesión trabajo profesor")
            return JSONResponse({"error": str(e)}, status_code=500)
        # Serialización segura de la sesión cerrada
        def _fmt_date(d):
            try:
                return str(d)[:10] if d is not None else None
            except Exception:
                return None
        def _fmt_time(t):
            try:
                if t is None:
                    return None
                hh = getattr(t, "hour", None)
                mm = getattr(t, "minute", None)
                if hh is not None and mm is not None:
                    return f"{int(hh):02d}:{int(mm):02d}"
                s = str(t)
                for sep in ("T", " "):
                    if sep in s:
                        tail = s.split(sep, 1)[1]
                        if len(tail) >= 5 and tail[0:2].isdigit() and tail[2] == ":" and tail[3:5].isdigit():
                            return tail[:5]
                if len(s) >= 5 and s[0:2].isdigit() and s[2] == ":" and s[3:5].isdigit():
                    return s[:5]
                try:
                    from datetime import datetime as _dt
                    dt = _dt.fromisoformat(s.replace('Z', '+00:00'))
                    return f"{dt.hour:02d}:{dt.minute:02d}"
                except Exception:
                    pass
                return None
            except Exception:
                return None
        datos = None
        try:
            datos = res.get("datos") if isinstance(res, dict) else None
        except Exception:
            datos = None
        raw = None
        if datos is not None:
            try:
                raw = dict(datos)
            except Exception:
                raw = datos if isinstance(datos, dict) else None
        sesion_safe = None
        minutos = 0
        horas = 0.0
        if isinstance(raw, dict):
            minutos = int(raw.get("minutos_totales") or 0)
            horas = round(minutos / 60.0, 2)
            sesion_safe = {
                "id": int(raw.get("id") or 0),
                "profesor_id": int(raw.get("profesor_id") or 0),
                "fecha": _fmt_date(raw.get("fecha")),
                "hora_inicio": _fmt_time(raw.get("hora_inicio")),
                "hora_fin": _fmt_time(raw.get("hora_fin")),
                "minutos_totales": minutos,
                "horas_totales": horas,
                "tipo_actividad": raw.get("tipo_actividad") or "Trabajo",
            }
        success_val = bool((res or {}).get("success")) if isinstance(res, dict) else True
        mensaje = (res or {}).get("mensaje") if isinstance(res, dict) else None
        return JSONResponse(status_code=200, content={"success": success_val, "mensaje": mensaje, "sesion": sesion_safe, "minutos": minutos, "horas": horas})
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)

@router.get("/api/profesor_sesion_activa")
async def api_profesor_sesion_activa(request: Request, _=Depends(require_gestion_access)):
    db = get_db()
    if db is None:
        return {"activa": False}
    guard = _circuit_guard_json(db, "/api/profesor_sesion_activa")
    if guard:
        return guard
    try:
        # Resolver profesor_id según rol
        is_owner = bool(request.session.get("logged_in"))
        pid = request.query_params.get("profesor_id")
        profesor_id = None
        if is_owner:
            if not pid:
                return {"activa": False}
            try:
                profesor_id = int(pid)
            except Exception:
                return JSONResponse({"activa": False}, status_code=200)
        else:
            ses_prof_id = request.session.get("gestion_profesor_id")
            try:
                profesor_id = int(ses_prof_id) if ses_prof_id is not None else None
            except Exception:
                profesor_id = None
            if profesor_id is None:
                return {"activa": False}
        try:
            ses = db.obtener_sesion_activa_profesor(profesor_id)  # type: ignore
        except Exception:
            ses = None
        activa = False
        tipo = None
        sesion_raw = None
        if isinstance(ses, dict):
            try:
                activa = bool(ses.get("tiene_sesion_activa"))
            except Exception:
                activa = False
            sesion_raw = ses.get("sesion_activa") or None
        def _fmt_date(d):
            try:
                return str(d)[:10] if d is not None else None
            except Exception:
                return None
        def _fmt_time(t):
            try:
                if t is None:
                    return None
                hh = getattr(t, "hour", None)
                mm = getattr(t, "minute", None)
                if hh is not None and mm is not None:
                    return f"{int(hh):02d}:{int(mm):02d}"
                s = str(t)
                for sep in ("T", " "):
                    if sep in s:
                        tail = s.split(sep, 1)[1]
                        if len(tail) >= 5 and tail[0:2].isdigit() and tail[2] == ":" and tail[3:5].isdigit():
                            return tail[:5]
                if len(s) >= 5 and s[0:2].isdigit() and s[2] == ":" and s[3:5].isdigit():
                    return s[:5]
                return None
            except Exception:
                return None
        sesion_safe = None
        if isinstance(sesion_raw, dict):
            tipo = sesion_raw.get("tipo_actividad")
            minutos = int(sesion_raw.get("minutos_totales") or 0)
            horas = round(minutos / 60.0, 2)
            sesion_safe = {
                "id": int(sesion_raw.get("id") or 0),
                "profesor_id": int(sesion_raw.get("profesor_id") or 0),
                "fecha": _fmt_date(sesion_raw.get("fecha")),
                "hora_inicio": _fmt_time(sesion_raw.get("hora_inicio")),
                "hora_fin": _fmt_time(sesion_raw.get("hora_fin")),
                "minutos_totales": minutos,
                "horas_totales": horas,
                "tipo_actividad": tipo,
            }
        return JSONResponse({"activa": activa, "tipo_actividad": tipo, "sesion": sesion_safe}, status_code=200)
    except Exception as e:
        logging.exception("Error en /api/profesor_sesion_activa")
        return JSONResponse({"activa": False}, status_code=200)

@router.get("/api/profesor_sesion_duracion")
async def api_profesor_sesion_duracion(request: Request, _=Depends(require_gestion_access)):
    db = get_db()
    if db is None:
        return {"minutos": 0}
    guard = _circuit_guard_json(db, "/api/profesor_sesion_duracion")
    if guard:
        return guard
    try:
        # Resolver profesor_id según rol
        is_owner = bool(request.session.get("logged_in"))
        pid = request.query_params.get("profesor_id")
        profesor_id = None
        if is_owner:
            if not pid:
                return {"minutos": 0}
            try:
                profesor_id = int(pid)
            except Exception:
                return {"minutos": 0}
        else:
            ses_prof_id = request.session.get("gestion_profesor_id")
            try:
                profesor_id = int(ses_prof_id) if ses_prof_id is not None else None
            except Exception:
                profesor_id = None
            if profesor_id is None:
                return {"minutos": 0}
        try:
            dur = db.obtener_duracion_sesion_actual_profesor(profesor_id)  # type: ignore
        except Exception:
            dur = None
        minutos = 0
        if isinstance(dur, dict):
            try:
                minutos = int(dur.get("minutos_transcurridos") or 0)
            except Exception:
                minutos = 0
        elif isinstance(dur, (int, float)):
            minutos = int(dur)
        return JSONResponse({"minutos": minutos}, status_code=200)
    except Exception as e:
        logging.exception("Error en /api/profesor_sesion_duracion")
        return JSONResponse({"minutos": 0}, status_code=200)

