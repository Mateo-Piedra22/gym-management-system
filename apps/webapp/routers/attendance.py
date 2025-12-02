import logging
import secrets
from datetime import datetime, date
from typing import Optional, List, Dict, Any

from fastapi import APIRouter, Request, Depends, HTTPException, status
from fastapi.responses import JSONResponse

from apps.webapp.dependencies import get_db, require_gestion_access, require_owner
from apps.webapp.utils import _circuit_guard_json

router = APIRouter()
logger = logging.getLogger(__name__)

# --- API Check-in y Asistencias ---

@router.post("/api/checkin/validate")
async def api_checkin_validate(request: Request):
    """Valida el token escaneado y registra asistencia si corresponde."""
    rid = getattr(getattr(request, 'state', object()), 'request_id', '-')
    db = get_db()
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
        # Verificar estado activo del usuario en cada validación para evitar registros de inactivos
        try:
            with db.get_connection_context() as conn:  # type: ignore
                cur = conn.cursor()
                cur.execute(
                    """
                    SELECT activo, LOWER(COALESCE(rol,'socio')) AS rol, COALESCE(cuotas_vencidas,0) AS cuotas_vencidas
                    FROM usuarios WHERE id = %s LIMIT 1
                    """,
                    (int(socio_id),)
                )
                r = cur.fetchone()
                if r:
                    activo_flag = bool(r[0]) if r[0] is not None else True
                    rol = (r[1] or 'socio').lower()
                    exento = rol in ('profesor','owner','dueño','dueno')
                    cuotas_vencidas = int(r[2] or 0)
                    if (not activo_flag) and (not exento):
                        motivo = 'Desactivado por falta de pagos' if cuotas_vencidas >= 3 else 'Desactivado por administración'
                        return JSONResponse({"success": False, "message": motivo}, status_code=403)
        except Exception:
            try:
                logging.warning("/api/checkin/validate: fallo al verificar activo; continuando")
            except Exception:
                pass
        # Orden de parámetros: (token, socio_id)
        ok, msg = db.validar_token_y_registrar_asistencia(token, int(socio_id))  # type: ignore
        status_code = 200 if ok else 400
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
        return JSONResponse({"success": ok, "message": msg}, status_code=status_code)
    except Exception as e:
        try:
            logging.exception(f"Error en /api/checkin/validate rid={rid}")
        except Exception:
            pass
        return JSONResponse({"success": False, "message": str(e)}, status_code=500)


@router.get("/api/checkin/token_status")
async def api_checkin_token_status(request: Request):
    """Consulta el estado de un token: { exists, used, expired }."""
    rid = getattr(getattr(request, 'state', object()), 'request_id', '-')
    db = get_db()
    if db is None:
        return JSONResponse({"exists": False, "used": False, "expired": True}, status_code=200)
    
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
            now = datetime.now(timezone.utc).replace(tzinfo=None) # naive UTC for comparison if expires_at is naive
            # Adjust if DB returns timezone aware
            if expires_at and expires_at.tzinfo:
                 now = datetime.now(timezone.utc)
            
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

@router.post("/api/checkin/create_token")
async def api_checkin_create_token(request: Request, _=Depends(require_gestion_access)):
    rid = getattr(getattr(request,'state',object()), 'request_id', '-')
    db = get_db()
    if db is None:
        raise HTTPException(status_code=500, detail="DB no disponible")
    payload = await request.json()
    usuario_id = int(payload.get("usuario_id") or 0)
    expires_minutes = int(payload.get("expires_minutes") or 5)
    if not usuario_id:
        raise HTTPException(status_code=400, detail="usuario_id es requerido")
    token = secrets.token_urlsafe(12)
    try:
        db.crear_checkin_token(usuario_id, token, expires_minutes)  # type: ignore
        try:
            logging.info(f"/api/checkin/create_token: usuario_id={usuario_id} token=***{token[-4:]} expires={expires_minutes}m rid={rid}")
        except Exception:
            pass
        return JSONResponse({"success": True, "token": token, "expires_minutes": expires_minutes}, status_code=200)
    except PermissionError as e:
        raise HTTPException(status_code=403, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@router.post("/api/asistencias/registrar")
async def api_asistencias_registrar(request: Request, _=Depends(require_gestion_access)):
    rid = getattr(getattr(request,'state',object()), 'request_id', '-')
    db = get_db()
    if db is None:
        raise HTTPException(status_code=500, detail="DB no disponible")
    payload = await request.json()
    usuario_id = int(payload.get("usuario_id") or 0)
    fecha_str = str(payload.get("fecha") or "").strip()
    if not usuario_id:
        raise HTTPException(status_code=400, detail="usuario_id es requerido")
    fecha = None
    try:
        if fecha_str:
            parts = fecha_str.split("-")
            if len(parts) == 3:
                fecha = date(int(parts[0]), int(parts[1]), int(parts[2]))
    except Exception:
        fecha = None
    try:
        asistencia_id = db.registrar_asistencia(usuario_id, fecha)  # type: ignore
        try:
            logging.info(f"/api/asistencias/registrar: usuario_id={usuario_id} fecha={fecha} rid={rid}")
        except Exception:
            pass
        return JSONResponse({"success": True, "asistencia_id": asistencia_id}, status_code=200)
    except PermissionError as e:
        raise HTTPException(status_code=403, detail=str(e))
    except ValueError as e:
        try:
            logging.info(f"/api/asistencias/registrar: ya existía asistencia usuario_id={usuario_id} rid={rid}")
        except Exception:
            pass
        return JSONResponse({"success": True, "message": str(e)}, status_code=200)
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.delete("/api/asistencias/eliminar")
async def api_asistencias_eliminar(request: Request, _=Depends(require_gestion_access)):
    rid = getattr(getattr(request,'state',object()), 'request_id', '-')
    db = get_db()
    if db is None:
        raise HTTPException(status_code=500, detail="DB no disponible")
    payload = await request.json()
    usuario_id = int(payload.get("usuario_id") or 0)
    fecha_str = str(payload.get("fecha") or "").strip()
    if not usuario_id:
        raise HTTPException(status_code=400, detail="usuario_id es requerido")
    fecha = None
    try:
        if fecha_str:
            parts = fecha_str.split("-")
            if len(parts) == 3:
                fecha = date(int(parts[0]), int(parts[1]), int(parts[2]))
        else:
            fecha = date.today()
    except Exception:
        fecha = None
    try:
        db.eliminar_asistencia(usuario_id, fecha)  # type: ignore
        try:
            logging.info(f"/api/asistencias/eliminar: usuario_id={usuario_id} fecha={fecha} rid={rid}")
        except Exception:
            pass
        return JSONResponse({"success": True}, status_code=200)
    except PermissionError as e:
        raise HTTPException(status_code=403, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/api/asistencia_30d")
async def api_asistencia_30d(request: Request, _=Depends(require_owner)):
    db = get_db()
    series: Dict[str, int] = {}
    if db is None:
        return series
    guard = _circuit_guard_json(db, "/api/asistencia_30d")
    if guard:
        return guard
    try:
        start = request.query_params.get("start")
        end = request.query_params.get("end")
        if start and end:
            data = db.obtener_asistencias_por_rango_diario(start, end)  # type: ignore
        else:
            data = db.obtener_asistencias_por_dia(30)  # type: ignore
        for d, c in (data or []):
            series[str(d)] = int(c or 0)
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


@router.get("/api/asistencia_por_hora_30d")
async def api_asistencia_por_hora_30d(request: Request, _=Depends(require_owner)):
    db = get_db()
    series: Dict[str, int] = {}
    if db is None:
        return series
    guard = _circuit_guard_json(db, "/api/asistencia_por_hora_30d")
    if guard:
        return guard
    try:
        start = request.query_params.get("start")
        end = request.query_params.get("end")
        data = db.obtener_asistencias_por_hora(30, start, end)  # type: ignore
        for h, c in (data or []):
            series[str(h)] = int(c or 0)
        return series
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)

@router.get("/api/asistencias_hoy_ids")
async def api_asistencias_hoy_ids(_=Depends(require_gestion_access)):
    db = get_db()
    if db is None:
        return []
    try:
        with db.get_connection_context() as conn:  # type: ignore
            cur = conn.cursor()
            cur.execute("SELECT DISTINCT usuario_id FROM asistencias WHERE fecha::date = CURRENT_DATE")
            rows = cur.fetchall() or []
            out = []
            for r in rows:
                try:
                    out.append(int(r[0]))
                except Exception:
                    pass
            return out
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)

@router.get("/api/asistencias_detalle")
async def api_asistencias_detalle(request: Request, _=Depends(require_owner)):
    """Listado de asistencias con nombre del usuario para un rango de fechas (por defecto últimos 30 días), con búsqueda y paginación."""
    db = get_db()
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
