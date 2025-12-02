import logging
import os
import json
from datetime import datetime, timezone, date
from typing import Optional, List, Dict, Any

import psycopg2
import psycopg2.extras
from fastapi import APIRouter, Request, Depends, HTTPException, status
from fastapi.responses import JSONResponse, FileResponse

from apps.webapp.dependencies import get_db, get_pm, require_gestion_access, require_owner
from apps.webapp.utils import _circuit_guard_json, _apply_change_idempotent, _filter_existing_columns
from core.models import MetodoPago, Pago

router = APIRouter()
logger = logging.getLogger(__name__)

# --- API Metadatos de pago ---

@router.get("/api/metodos_pago")
async def api_metodos_pago(_=Depends(require_gestion_access)):
    db = get_db()
    if db is None:
        return []
    guard = _circuit_guard_json(db, "/api/metodos_pago")
    if guard:
        return guard
    try:
        rows = db.obtener_metodos_pago(solo_activos=True)  # type: ignore
        return [
            {
                'id': r.get('id'),
                'nombre': r.get('nombre'),
                'activo': r.get('activo'),
                'color': r.get('color'),
                'comision': r.get('comision'),
                'icono': r.get('icono'),
            }
            for r in (rows or [])
        ]
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)

@router.post("/api/metodos_pago")
async def api_metodos_pago_create(request: Request, _=Depends(require_gestion_access)):
    pm = get_pm()
    db = get_db()
    if db is None:
        raise HTTPException(status_code=503, detail="DB no disponible")
    guard = _circuit_guard_json(db, "/api/metodos_pago[POST]")
    if guard:
        return guard
    if pm is None or MetodoPago is None:
        raise HTTPException(status_code=503, detail="PaymentManager o modelo MetodoPago no disponible")
    payload = await request.json()
    try:
        nombre = (payload.get("nombre") or "").strip()
        if not nombre:
            raise HTTPException(status_code=400, detail="'nombre' es obligatorio")
        icono = payload.get("icono")
        color = (payload.get("color") or "#3498db").strip() or "#3498db"
        comision_raw = payload.get("comision")
        comision = float(comision_raw) if comision_raw is not None else 0.0
        if comision < 0 or comision > 100:
            raise HTTPException(status_code=400, detail="'comision' debe estar entre 0 y 100")
        activo = bool(payload.get("activo", True))
        descripcion = payload.get("descripcion")
        metodo = MetodoPago(nombre=nombre, icono=icono, color=color, comision=comision, activo=activo, descripcion=descripcion)  # type: ignore
        new_id = pm.crear_metodo_pago(metodo)
        return {"ok": True, "id": int(new_id)}
    except ValueError as ve:
        raise HTTPException(status_code=400, detail=str(ve))
    except HTTPException:
        raise
    except Exception as e:
        import traceback
        traceback.print_exc()
        return JSONResponse({"error": str(e)}, status_code=500)

@router.put("/api/metodos_pago/{metodo_id}")
async def api_metodos_pago_update(metodo_id: int, request: Request, _=Depends(require_gestion_access)):
    pm = get_pm()
    db = get_db()
    if db is None:
        raise HTTPException(status_code=503, detail="DB no disponible")
    guard = _circuit_guard_json(db, f"/api/metodos_pago/{metodo_id}[PUT]")
    if guard:
        return guard
    if pm is None or MetodoPago is None:
        raise HTTPException(status_code=503, detail="PaymentManager o modelo MetodoPago no disponible")
    payload = await request.json()
    try:
        existing = pm.obtener_metodo_pago(int(metodo_id))
        if not existing:
            raise HTTPException(status_code=404, detail="Método de pago no encontrado")
        nombre = (payload.get("nombre") or existing.nombre or "").strip() or existing.nombre
        icono = payload.get("icono") if ("icono" in payload) else existing.icono
        color = (payload.get("color") or existing.color or "#3498db").strip() or existing.color
        comision = float(payload.get("comision")) if (payload.get("comision") is not None) else float(existing.comision or 0.0)
        if comision < 0 or comision > 100:
            raise HTTPException(status_code=400, detail="'comision' debe estar entre 0 y 100")
        activo = bool(payload.get("activo")) if ("activo" in payload) else bool(existing.activo)
        descripcion = payload.get("descripcion") if ("descripcion" in payload) else existing.descripcion
        metodo = MetodoPago(id=int(metodo_id), nombre=nombre, icono=icono, color=color, comision=comision, activo=activo, descripcion=descripcion)  # type: ignore
        updated = pm.actualizar_metodo_pago(metodo)
        if not updated:
            raise HTTPException(status_code=404, detail="No se pudo actualizar el método de pago")
        return {"ok": True, "id": int(metodo_id)}
    except ValueError as ve:
        raise HTTPException(status_code=400, detail=str(ve))
    except HTTPException:
        raise
    except Exception as e:
        import traceback
        traceback.print_exc()
        return JSONResponse({"error": str(e)}, status_code=500)

@router.delete("/api/metodos_pago/{metodo_id}")
async def api_metodos_pago_delete(metodo_id: int, _=Depends(require_gestion_access)):
    pm = get_pm()
    db = get_db()
    if db is None:
        raise HTTPException(status_code=503, detail="DB no disponible")
    guard = _circuit_guard_json(db, f"/api/metodos_pago/{metodo_id}[DELETE]")
    if guard:
        return guard
    pm_ready = (pm is not None)
    try:
        if pm_ready:
            deleted = pm.eliminar_metodo_pago(int(metodo_id))
            if not deleted:
                raise HTTPException(status_code=404, detail="No se pudo eliminar el método de pago")
            return {"ok": True}
        else:
            with db.get_connection_context() as conn:  # type: ignore
                ok = _apply_change_idempotent(
                    conn,
                    schema="public",
                    table="metodos_pago",
                    operation="DELETE",
                    key_column="id",
                    key_value=int(metodo_id),
                    where=[("id", int(metodo_id))],
                )
                if not ok:
                    raise HTTPException(status_code=404, detail="No se pudo eliminar el método de pago")
                conn.commit()
                return {"ok": True}
    except ValueError as ve:
        raise HTTPException(status_code=400, detail=str(ve))
    except HTTPException:
        raise
    except Exception as e:
        import traceback
        traceback.print_exc()
        return JSONResponse({"error": str(e)}, status_code=500)

# --- Tipos de Cuota (Planes) ---

@router.get("/api/tipos_cuota_activos")
async def api_tipos_cuota_activos(_=Depends(require_gestion_access)):
    db = get_db()
    if db is None:
        return []
    guard = _circuit_guard_json(db, "/api/tipos_cuota_activos")
    if guard:
        return guard
    try:
        tipos = db.obtener_tipos_cuota_activos()  # type: ignore
        tipos = sorted(tipos or [], key=lambda t: (float(getattr(t, 'precio', 0.0) or 0.0), (getattr(t, 'nombre', '') or '')))
        return [
            {
                "id": int(getattr(t, 'id')) if getattr(t, 'id') is not None else None,
                "nombre": (getattr(t, 'nombre', '') or '').strip(),
                "precio": float(getattr(t, 'precio', 0.0) or 0.0),
                "duracion_dias": int(getattr(t, 'duracion_dias', 30) or 30),
                "activo": bool(getattr(t, 'activo', True)),
            }
            for t in tipos
        ]
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)

@router.get("/api/tipos_cuota_catalogo")
async def api_tipos_cuota_catalogo(_=Depends(require_gestion_access)):
    db = get_db()
    if db is None:
        return []
    guard = _circuit_guard_json(db, "/api/tipos_cuota_catalogo")
    if guard:
        return guard
    try:
        tipos = db.obtener_tipos_cuota(solo_activos=False)  # type: ignore
        tipos = sorted(tipos or [], key=lambda t: (
            0 if bool(getattr(t, 'activo', True)) else 1,
            float(getattr(t, 'precio', 0.0) or 0.0),
            (getattr(t, 'nombre', '') or '')
        ))
        return [
            {
                "id": int(getattr(t, 'id')) if getattr(t, 'id') is not None else None,
                "nombre": (getattr(t, 'nombre', '') or '').strip(),
                "precio": float(getattr(t, 'precio', 0.0) or 0.0),
                "duracion_dias": int(getattr(t, 'duracion_dias', 30) or 30),
                "activo": bool(getattr(t, 'activo', True)),
                "descripcion": getattr(t, 'descripcion', None),
                "icono_path": getattr(t, 'icono_path', None),
            }
            for t in tipos
        ]
    except Exception as e:
        import traceback
        traceback.print_exc()
        return JSONResponse({"error": str(e)}, status_code=500)

@router.post("/api/tipos_cuota")
async def api_tipos_cuota_create(request: Request, _=Depends(require_gestion_access)):
    db = get_db()
    if db is None:
        raise HTTPException(status_code=503, detail="DB no disponible")
    guard = _circuit_guard_json(db, "/api/tipos_cuota[POST]")
    if guard:
        return guard
    payload = await request.json()
    try:
        nombre = (payload.get("nombre") or "").strip()
        if not nombre:
            raise HTTPException(status_code=400, detail="'nombre' es obligatorio")
        precio_raw = payload.get("precio")
        precio = float(precio_raw) if precio_raw is not None else 0.0
        if precio < 0:
            raise HTTPException(status_code=400, detail="'precio' no puede ser negativo")
        duracion_raw = payload.get("duracion_dias")
        duracion_dias = int(duracion_raw) if duracion_raw is not None else 30
        if duracion_dias <= 0:
            raise HTTPException(status_code=400, detail="'duracion_dias' debe ser > 0")
        activo = bool(payload.get("activo", True))
        descripcion = payload.get("descripcion")
        icono_path = payload.get("icono_path")
        with db.get_connection_context() as conn:  # type: ignore
            from psycopg2 import sql as _sql
            data = {
                "nombre": nombre,
                "precio": precio,
                "duracion_dias": duracion_dias,
                "activo": activo,
                "descripcion": descripcion,
                "icono_path": icono_path,
            }
            filtered = _filter_existing_columns(conn, "public", "tipos_cuota", data)
            if not filtered:
                raise HTTPException(status_code=400, detail="No hay columnas válidas para insertar")
            cols = list(filtered.keys())
            stmt = _sql.SQL("INSERT INTO {}.{} ({}) VALUES ({}) RETURNING id").format(
                _sql.Identifier("public"),
                _sql.Identifier("tipos_cuota"),
                _sql.SQL(", ").join([_sql.Identifier(c) for c in cols]),
                _sql.SQL(", ").join([_sql.Placeholder() for _ in cols]),
            )
            cur = conn.cursor()
            cur.execute(stmt, [filtered[c] for c in cols])
            new_id_row = cur.fetchone()
            new_id = int(new_id_row[0]) if new_id_row else None
            if new_id is None:
                raise HTTPException(status_code=500, detail="No se pudo crear el tipo de cuota")
            conn.commit()
            try:
                db.cache.invalidate('tipos')  # type: ignore
            except Exception:
                pass
            return {"ok": True, "id": new_id}
    except ValueError as ve:
        raise HTTPException(status_code=400, detail=str(ve))
    except HTTPException:
        raise
    except Exception as e:
        import traceback
        traceback.print_exc()
        return JSONResponse({"error": str(e)}, status_code=500)

@router.put("/api/tipos_cuota/{tipo_id}")
async def api_tipos_cuota_update(tipo_id: int, request: Request, _=Depends(require_gestion_access)):
    db = get_db()
    if db is None:
        raise HTTPException(status_code=503, detail="DB no disponible")
    guard = _circuit_guard_json(db, f"/api/tipos_cuota/{tipo_id}[PUT]")
    if guard:
        return guard
    payload = await request.json()
    try:
        with db.get_connection_context() as conn:  # type: ignore
            cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            cur.execute("SELECT id, nombre, precio, duracion_dias, activo, descripcion, icono_path FROM tipos_cuota WHERE id = %s", (int(tipo_id),))
            row = cur.fetchone()
            if not row:
                raise HTTPException(status_code=404, detail="Tipo de cuota no encontrado")
        nombre = (payload.get("nombre") or row.get("nombre") or "").strip() or row.get("nombre")
        precio = float(payload.get("precio")) if (payload.get("precio") is not None) else float(row.get("precio") or 0.0)
        if precio < 0:
            raise HTTPException(status_code=400, detail="'precio' no puede ser negativo")
        duracion_dias = int(payload.get("duracion_dias")) if (payload.get("duracion_dias") is not None) else int(row.get("duracion_dias") or 30)
        if duracion_dias <= 0:
            raise HTTPException(status_code=400, detail="'duracion_dias' debe ser > 0")
        activo = bool(payload.get("activo")) if ("activo" in payload) else bool(row.get("activo"))
        descripcion = payload.get("descripcion") if ("descripcion" in payload) else row.get("descripcion")
        icono_path = payload.get("icono_path") if ("icono_path" in payload) else row.get("icono_path")
        updates = {
            "nombre": nombre,
            "precio": precio,
            "duracion_dias": duracion_dias,
            "activo": activo,
            "descripcion": descripcion,
            "icono_path": icono_path,
        }
        with db.get_connection_context() as conn:  # type: ignore
            ok = _apply_change_idempotent(conn, "public", "tipos_cuota", "UPDATE", {"id": int(tipo_id)}, updates)
            if not ok:
                raise HTTPException(status_code=500, detail="No se pudo actualizar el tipo de cuota")
            conn.commit()
            try:
                db.cache.invalidate('tipos')  # type: ignore
            except Exception:
                pass
            return {"ok": True, "id": int(tipo_id)}
    except ValueError as ve:
        raise HTTPException(status_code=400, detail=str(ve))
    except HTTPException:
        raise
    except Exception as e:
        import traceback
        traceback.print_exc()
        return JSONResponse({"error": str(e)}, status_code=500)

@router.delete("/api/tipos_cuota/{tipo_id}")
async def api_tipos_cuota_delete(tipo_id: int, _=Depends(require_gestion_access)):
    db = get_db()
    if db is None:
        raise HTTPException(status_code=503, detail="DB no disponible")
    guard = _circuit_guard_json(db, f"/api/tipos_cuota/{tipo_id}[DELETE]")
    if guard:
        return guard
    try:
        with db.get_connection_context() as conn:  # type: ignore
            ok = _apply_change_idempotent(conn, "public", "tipos_cuota", "DELETE", {"id": int(tipo_id)}, {})
            if not ok:
                raise HTTPException(status_code=500, detail="No se pudo eliminar el tipo de cuota")
            conn.commit()
            try:
                db.cache.invalidate('tipos')  # type: ignore
            except Exception:
                pass
            return {"ok": True}
    except HTTPException:
        raise
    except Exception as e:
        import traceback
        traceback.print_exc()
        return JSONResponse({"error": str(e)}, status_code=500)

# --- Pagos y Recibos ---

@router.get("/api/pagos_detalle")
async def api_pagos_detalle(request: Request, _=Depends(require_gestion_access)):
    db = get_db()
    if db is None:
        return {"count": 0, "items": []}
    guard = _circuit_guard_json(db, "/api/pagos_detalle")
    if guard:
        return guard
    try:
        start = request.query_params.get("start")
        end = request.query_params.get("end")
        q = request.query_params.get("q")
        limit_q = request.query_params.get("limit")
        offset_q = request.query_params.get("offset")
        try:
            limit = int(limit_q) if (limit_q and str(limit_q).isdigit()) else 50
        except Exception:
            limit = 50
        try:
            offset = int(offset_q) if (offset_q and str(offset_q).isdigit()) else 0
        except Exception:
            offset = 0
        if start and isinstance(start, str) and start.strip() == "":
            start = None
        if end and isinstance(end, str) and end.strip() == "":
            end = None

        rows = db.obtener_pagos_por_fecha(start, end)  # type: ignore
        items = rows
        if q and isinstance(q, str) and q.strip() != "":
            ql = q.lower()
            def _match(r: Dict[str, Any]) -> bool:
                try:
                    nombre = str(r.get("usuario_nombre") or r.get("nombre") or "").lower()
                    dni = str(r.get("dni") or "").lower()
                    metodo = str(r.get("metodo_pago") or r.get("metodo") or "").lower()
                    concepto = str(r.get("concepto_pago") or r.get("concepto") or "").lower()
                    return (ql in nombre) or (ql in dni) or (ql in metodo) or (ql in concepto)
                except Exception:
                    return False
            items = [r for r in rows if _match(r)]
        total = len(items)
        sliced = items[offset:offset+limit]
        return {"count": total, "items": sliced}
    except Exception as e:
        import traceback
        traceback.print_exc()
        return JSONResponse({"error": str(e)}, status_code=500)

@router.get("/api/pagos/{pago_id}")
async def api_pago_resumen(pago_id: int, _=Depends(require_gestion_access)):
    db = get_db()
    if db is None:
        raise HTTPException(status_code=503, detail="DB no disponible")
    try:
        with db.get_connection_context() as conn:  # type: ignore
            cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            cur.execute(
                """
                SELECT 
                    p.id AS id,
                    p.usuario_id AS usuario_id,
                    p.monto AS monto,
                    p.mes AS mes,
                    p.año AS año,
                    p.fecha_pago AS fecha_pago,
                    p.metodo_pago_id AS metodo_pago_id,
                    u.id AS usuario_id_ref,
                    u.nombre AS usuario_nombre,
                    u.dni AS dni,
                    COALESCE(SUM(COALESCE(pd.cantidad,1) * COALESCE(pd.precio_unitario,0)), 0) AS total_detalles,
                    JSON_AGG(
                        JSON_BUILD_OBJECT(
                            'id', pd.id,
                            'pago_id', pd.pago_id,
                            'concepto_nombre', COALESCE(cp.nombre, pd.descripcion),
                            'cantidad', COALESCE(pd.cantidad, 1),
                            'precio_unitario', COALESCE(pd.precio_unitario, 0),
                            'subtotal', COALESCE(pd.subtotal, COALESCE(pd.cantidad,1) * COALESCE(pd.precio_unitario,0))
                        )
                    ) FILTER (WHERE pd.id IS NOT NULL) AS detalles
                FROM pagos p
                JOIN usuarios u ON u.id = p.usuario_id
                LEFT JOIN pago_detalles pd ON pd.pago_id = p.id
                LEFT JOIN conceptos_pago cp ON cp.id = pd.concepto_id
                WHERE p.id = %s
                GROUP BY p.id, u.id
                """,
                (pago_id,)
            )
            row = cur.fetchone()
            if not row:
                raise HTTPException(status_code=404, detail="Pago no encontrado")
        total_detalles = float(row.get("total_detalles") or 0)
        detalles = row.get("detalles") or []
        pago = {k: v for k, v in row.items() if k not in ("detalles", "total_detalles")}
        return {"pago": pago, "detalles": detalles, "total_detalles": total_detalles}
    except HTTPException:
        raise
    except Exception as e:
        import traceback
        traceback.print_exc()
        return JSONResponse({"error": str(e)}, status_code=500)

@router.get("/api/pagos/{pago_id}/recibo.pdf")
async def api_pago_recibo_pdf(pago_id: int, request: Request, _=Depends(require_gestion_access)):
    pm = get_pm()
    db = get_db()
    if db is None:
        raise HTTPException(status_code=503, detail="DB no disponible")
    if pm is None:
        raise HTTPException(status_code=503, detail="PaymentManager no disponible")
    try:
        pago = pm.obtener_pago(int(pago_id))
        if not pago:
            raise HTTPException(status_code=404, detail="Pago no encontrado")
        usuario = db.obtener_usuario_por_id(int(getattr(pago, 'usuario_id', 0)))  # type: ignore
        if not usuario:
            raise HTTPException(status_code=404, detail="Usuario del pago no encontrado")

        try:
            detalles = pm.obtener_detalles_pago(int(pago_id))
        except Exception:
            detalles = []
        subtotal = 0.0
        try:
            subtotal = sum(float(getattr(d, 'subtotal', 0.0) or 0.0) for d in (detalles or [])) if detalles else float(getattr(pago, 'monto', 0.0) or 0.0)
        except Exception:
            subtotal = float(getattr(pago, 'monto', 0.0) or 0.0)
        metodo_id = getattr(pago, 'metodo_pago_id', None)
        try:
            totales = pm.calcular_total_con_comision(subtotal, metodo_id)
        except Exception:
            totales = {"subtotal": subtotal, "comision": 0.0, "total": subtotal}

        qp = request.query_params
        preview_mode = False
        try:
            qpv = qp.get("preview")
            preview_mode = True if (qpv and str(qpv).lower() in ("1","true","yes")) else False
        except Exception:
            preview_mode = False
        numero_override = None
        try:
            nraw = qp.get("numero")
            numero_override = (str(nraw).strip() or None) if (nraw is not None) else None
        except Exception:
            numero_override = None

        obs_text = None
        try:
            oraw = qp.get("observaciones")
            obs_text = (str(oraw).strip() or None) if (oraw is not None) else None
        except Exception:
            obs_text = None
        emitido_por = None
        try:
            eraw = qp.get("emitido_por")
            emitido_por = (str(eraw).strip() or None) if (eraw is not None) else None
        except Exception:
            emitido_por = None
        try:
            if not emitido_por:
                prof_uid = request.session.get("gestion_profesor_user_id")
                prof_id = request.session.get("gestion_profesor_id")
                with db.get_connection_context() as conn:  # type: ignore
                    cur = conn.cursor()
                    if prof_uid:
                        try:
                            cur.execute("SELECT nombre FROM usuarios WHERE id = %s", (int(prof_uid),))
                            r = cur.fetchone()
                            if r and r[0]:
                                emitido_por = str(r[0])
                        except Exception:
                            pass
                    elif prof_id:
                        try:
                            cur.execute("SELECT u.nombre FROM profesores p JOIN usuarios u ON p.usuario_id = u.id WHERE p.id = %s", (int(prof_id),))
                            r = cur.fetchone()
                            if r and r[0]:
                                emitido_por = str(r[0])
                        except Exception:
                            pass
        except Exception:
            pass

        def _qp_bool(val):
            try:
                s = str(val).strip().lower()
            except Exception:
                return None
            if s in ("1","true","yes","on"): return True
            if s in ("0","false","no","off"): return False
            return None

        titulo = None
        try:
            titulo = (str(qp.get("titulo")).strip() or None) if (qp.get("titulo") is not None) else None
        except Exception:
            titulo = None
        gym_name_override = None
        gym_address_override = None
        try:
            gym_name_override = (str(qp.get("gym_name")).strip() or None) if (qp.get("gym_name") is not None) else None
        except Exception:
            gym_name_override = None
        try:
            gym_address_override = (str(qp.get("gym_address")).strip() or None) if (qp.get("gym_address") is not None) else None
        except Exception:
            gym_address_override = None

        fecha_emision_disp = None
        try:
            fraw = qp.get("fecha")
            if fraw is not None:
                s = str(fraw).strip()
                try:
                    if "/" in s:
                        dt = datetime.strptime(s, "%d/%m/%Y")
                    else:
                        dt = datetime.strptime(s, "%Y-%m-%d")
                    fecha_emision_disp = dt.strftime("%d/%m/%Y")
                except Exception:
                    fecha_emision_disp = s or None
        except Exception:
            fecha_emision_disp = None

        metodo_override = None
        try:
            metodo_override = (str(qp.get("metodo")).strip() or None) if (qp.get("metodo") is not None) else None
        except Exception:
            metodo_override = None

        tipo_cuota_override = None
        try:
            tipo_cuota_override = (str(qp.get("tipo_cuota")).strip() or None) if (qp.get("tipo_cuota") is not None) else None
        except Exception:
            tipo_cuota_override = None
        periodo_override = None
        try:
            periodo_override = (str(qp.get("periodo")).strip() or None) if (qp.get("periodo") is not None) else None
        except Exception:
            periodo_override = None

        usuario_nombre_override = None
        usuario_dni_override = None
        try:
            usuario_nombre_override = (str(qp.get("usuario_nombre")).strip() or None) if (qp.get("usuario_nombre") is not None) else None
        except Exception:
            usuario_nombre_override = None
        try:
            usuario_dni_override = (str(qp.get("usuario_dni")).strip() or None) if (qp.get("usuario_dni") is not None) else None
        except Exception:
            usuario_dni_override = None

        mostrar_logo = _qp_bool(qp.get("mostrar_logo"))
        mostrar_metodo = _qp_bool(qp.get("mostrar_metodo"))
        mostrar_dni = _qp_bool(qp.get("mostrar_dni"))

        detalles_override = None
        try:
            iraw = qp.get("items")
            if iraw is not None:
                obj = json.loads(str(iraw))
                if isinstance(obj, list):
                    detalles_override = obj
        except Exception:
            detalles_override = None

        try:
            sub_o = qp.get("subtotal")
            com_o = qp.get("comision")
            tot_o = qp.get("total")
            if sub_o is not None or com_o is not None or tot_o is not None:
                s = float(sub_o) if (sub_o is not None and str(sub_o).strip() != "") else float(totales.get("subtotal", 0.0))
                c = float(com_o) if (com_o is not None and str(com_o).strip() != "") else float(totales.get("comision", 0.0))
                t = float(tot_o) if (tot_o is not None and str(tot_o).strip() != "") else float(totales.get("total", s + c))
                totales = {"subtotal": s, "comision": c, "total": t}
        except Exception:
            pass

        numero_comprobante = None
        comprobante_id = None
        try:
            with db.get_connection_context() as conn:  # type: ignore
                cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
                cur.execute(
                    """
                    SELECT id, numero_comprobante
                    FROM comprobantes_pago
                    WHERE pago_id = %s AND estado = 'emitido'
                    ORDER BY fecha_creacion DESC
                    LIMIT 1
                    """,
                    (int(pago_id),)
                )
                row = cur.fetchone()
                if row:
                    comprobante_id = int(row.get("id"))
                    numero_comprobante = row.get("numero_comprobante")
        except Exception:
            numero_comprobante = None

        if preview_mode:
            if numero_override:
                numero_comprobante = numero_override
        else:
            try:
                if not numero_comprobante:
                    comprobante_id = db.crear_comprobante(
                        tipo_comprobante='recibo',
                        pago_id=int(pago_id),
                        usuario_id=int(getattr(pago, 'usuario_id', 0)),
                        monto_total=float(getattr(pago, 'monto', 0.0) or 0.0),
                        plantilla_id=None,
                        datos_comprobante=None,
                        emitido_por=emitido_por
                    )
                    comp = db.obtener_comprobante(int(comprobante_id))
                    if comp:
                        numero_comprobante = comp.get('numero_comprobante')
                if numero_override:
                    numero_comprobante = numero_override
            except Exception:
                numero_comprobante = None

        try:
            from apps.core.pdf_generator import PDFGenerator
        except ImportError:
            # If apps.core is not in path directly, assume it is available via sys.path from dependencies
            from core.pdf_generator import PDFGenerator

        pdfg = PDFGenerator()
        filepath = pdfg.generar_recibo(
            pago,
            usuario,
            numero_comprobante,
            detalles=detalles,
            totales=totales,
            observaciones=obs_text,
            emitido_por=emitido_por,
            titulo=titulo,
            gym_name=gym_name_override,
            gym_address=gym_address_override,
            fecha_emision=fecha_emision_disp,
            metodo_pago=metodo_override,
            usuario_nombre=usuario_nombre_override,
            usuario_dni=usuario_dni_override,
            detalles_override=detalles_override,
            mostrar_logo=mostrar_logo,
            mostrar_metodo=mostrar_metodo,
            mostrar_dni=mostrar_dni,
            tipo_cuota=tipo_cuota_override,
            periodo=periodo_override,
        )

        try:
            if comprobante_id is not None and filepath:
                with db.get_connection_context() as conn:  # type: ignore
                    cur = conn.cursor()
                    cur.execute(
                        "UPDATE comprobantes_pago SET archivo_pdf = %s WHERE id = %s",
                        (str(filepath), int(comprobante_id))
                    )
                    conn.commit()
        except Exception:
            pass

        filename = os.path.basename(filepath)
        resp = FileResponse(filepath, media_type="application/pdf")
        try:
            resp.headers["Content-Disposition"] = f'inline; filename="{filename}"'
        except Exception:
            pass
        return resp
    except HTTPException:
        raise
    except Exception as e:
        import traceback
        traceback.print_exc()
        return JSONResponse({"error": str(e)}, status_code=500)

@router.get("/api/recibos/numero-proximo")
async def api_recibos_numero_proximo(_=Depends(require_gestion_access)):
    db = get_db()
    if db is None:
        raise HTTPException(status_code=503, detail="DB no disponible")
    try:
        numero = db.get_next_receipt_number()
        return {"numero": str(numero)}
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)

@router.get("/api/recibos/config")
async def api_recibos_config_get(_=Depends(require_gestion_access)):
    db = get_db()
    if db is None:
        raise HTTPException(status_code=503, detail="DB no disponible")
    try:
        cfg = db.get_receipt_numbering_config()
        return cfg
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)

@router.put("/api/recibos/config")
async def api_recibos_config_put(request: Request, _=Depends(require_gestion_access)):
    db = get_db()
    if db is None:
        raise HTTPException(status_code=503, detail="DB no disponible")
    try:
        payload = await request.json()
        ok = db.save_receipt_numbering_config(payload)
        if ok:
            return {"ok": True}
        return JSONResponse({"error": "No se pudo guardar la configuración"}, status_code=400)
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)

@router.post("/api/pagos")
async def api_pagos_create(request: Request, _=Depends(require_gestion_access)):
    pm = get_pm()
    db = get_db()
    if db is None:
        raise HTTPException(status_code=503, detail="DB no disponible")
    guard = _circuit_guard_json(db, "/api/pagos[POST]")
    if guard:
        return guard
    if pm is None:
        raise HTTPException(status_code=503, detail="PaymentManager no disponible")
    payload = await request.json()
    try:
        usuario_id_raw = payload.get("usuario_id")
        monto_raw = payload.get("monto")
        mes_raw = payload.get("mes")
        año_raw = payload.get("año")
        metodo_pago_id = payload.get("metodo_pago_id")
        conceptos_raw = payload.get("conceptos")
        if not isinstance(conceptos_raw, list) or len(conceptos_raw) == 0:
            alt = payload.get("conceptos_raw")
            if isinstance(alt, list):
                conceptos_raw = alt
            else:
                conceptos_raw = []
        fecha_pago_raw = payload.get("fecha_pago")

        if usuario_id_raw is None:
            raise HTTPException(status_code=400, detail="'usuario_id' es obligatorio")
        try:
            usuario_id = int(usuario_id_raw)
            metodo_pago_id_int = int(metodo_pago_id) if metodo_pago_id is not None else None
        except Exception:
            raise HTTPException(status_code=400, detail="Tipos inválidos en payload")

        if isinstance(conceptos_raw, list) and len(conceptos_raw) > 0:
            conceptos: list[dict] = []
            for c in conceptos_raw:
                cid_raw = c.get("concepto_id")
                cid_val = None
                try:
                    if cid_raw is not None and str(cid_raw).strip() != "":
                        cid_val = int(cid_raw)
                except Exception:
                    cid_val = None
                descripcion = c.get("descripcion")
                try:
                    cantidad = int(c.get("cantidad") or 1)
                    precio_unitario = float(c.get("precio_unitario") or 0.0)
                except Exception:
                    raise HTTPException(status_code=400, detail="Conceptos inválidos en payload")
                if cantidad <= 0 or precio_unitario < 0:
                    raise HTTPException(status_code=400, detail="Cantidad/precio inválidos en conceptos")
                if cid_val is None and (not descripcion or str(descripcion).strip() == ""):
                    raise HTTPException(status_code=400, detail="Cada ítem debe tener 'concepto_id' o 'descripcion'")
                conceptos.append({
                    "concepto_id": cid_val,
                    "descripcion": descripcion,
                    "cantidad": cantidad,
                    "precio_unitario": precio_unitario
                })

            fecha_dt = None
            try:
                if fecha_pago_raw:
                    fecha_dt = datetime.fromisoformat(str(fecha_pago_raw))
                elif mes_raw is not None and año_raw is not None:
                    mes_i = int(mes_raw); año_i = int(año_raw)
                    fecha_dt = datetime(int(año_i), int(mes_i), 1)
            except Exception:
                raise HTTPException(status_code=400, detail="fecha_pago inválida")

            try:
                pago_id = pm.registrar_pago_avanzado(usuario_id, metodo_pago_id_int, conceptos, fecha_dt)
            except ValueError as ve:
                raise HTTPException(status_code=400, detail=str(ve))
            except Exception as e:
                raise HTTPException(status_code=500, detail=str(e))
            return {"ok": True, "id": int(pago_id)}

        if monto_raw is None or mes_raw is None or año_raw is None:
            raise HTTPException(status_code=400, detail="'monto', 'mes' y 'año' son obligatorios cuando no hay 'conceptos'")
        try:
            monto = float(monto_raw)
            mes = int(mes_raw)
            año = int(año_raw)
        except Exception:
            raise HTTPException(status_code=400, detail="Tipos inválidos en payload")
        if not (1 <= mes <= 12):
            raise HTTPException(status_code=400, detail="'mes' debe estar entre 1 y 12")
        if monto <= 0:
            raise HTTPException(status_code=400, detail="'monto' debe ser mayor a 0")

        try:
            pago_id = pm.registrar_pago(usuario_id, monto, mes, año, metodo_pago_id_int)
        except ValueError as ve:
            raise HTTPException(status_code=400, detail=str(ve))
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))

        return {"ok": True, "id": int(pago_id)}
    except HTTPException:
        raise
    except Exception as e:
        import traceback
        traceback.print_exc()
        return JSONResponse({"error": str(e)}, status_code=500)

@router.put("/api/pagos/{pago_id}")
async def api_pagos_update(pago_id: int, request: Request, _=Depends(require_gestion_access)):
    pm = get_pm()
    db = get_db()
    if db is None:
        raise HTTPException(status_code=503, detail="DB no disponible")
    guard = _circuit_guard_json(db, f"/api/pagos/{pago_id}[PUT]")
    if guard:
        return guard
    if pm is None or Pago is None:
        raise HTTPException(status_code=503, detail="PaymentManager o modelo Pago no disponible")
    payload = await request.json()
    try:
        usuario_id_raw = payload.get("usuario_id")
        monto_raw = payload.get("monto")
        fecha_raw = payload.get("fecha_pago")
        mes_raw = payload.get("mes")
        año_raw = payload.get("año")
        metodo_pago_id = payload.get("metodo_pago_id")
        conceptos_raw = payload.get("conceptos")

        advanced_conceptos = isinstance(conceptos_raw, list) and len(conceptos_raw) > 0
        if usuario_id_raw is None or (monto_raw is None and not advanced_conceptos):
            raise HTTPException(status_code=400, detail="'usuario_id' es obligatorio y 'monto' cuando no hay 'conceptos'")
        try:
            usuario_id = int(usuario_id_raw)
            monto = float(monto_raw) if monto_raw is not None else None
            metodo_pago_id_int = int(metodo_pago_id) if metodo_pago_id is not None else None
        except Exception:
            raise HTTPException(status_code=400, detail="Tipos inválidos en payload")

        fecha_dt = None
        if fecha_raw is not None:
            try:
                if isinstance(fecha_raw, str):
                    fecha_dt = datetime.fromisoformat(fecha_raw)
                else:
                    raise ValueError("fecha_pago debe ser string ISO")
            except Exception:
                raise HTTPException(status_code=400, detail="'fecha_pago' inválida, use ISO 8601 (YYYY-MM-DD)")
        else:
            if mes_raw is not None and año_raw is not None:
                try:
                    mes = int(mes_raw)
                    año = int(año_raw)
                    if not (1 <= mes <= 12):
                        raise HTTPException(status_code=400, detail="'mes' debe estar entre 1 y 12")
                    fecha_dt = datetime(año, mes, 1)
                except HTTPException:
                    raise
                except Exception:
                    raise HTTPException(status_code=400, detail="'mes'/'año' inválidos")
            else:
                try:
                    existing = pm.obtener_pago(pago_id)
                    if existing and getattr(existing, 'fecha_pago', None):
                        fecha_dt = existing.fecha_pago if not isinstance(existing.fecha_pago, str) else datetime.fromisoformat(existing.fecha_pago)
                    else:
                        fecha_dt = datetime.now()
                except Exception:
                    fecha_dt = datetime.now()

        if mes_raw is not None and año_raw is not None:
            try:
                mes = int(mes_raw)
                año = int(año_raw)
            except Exception:
                mes = fecha_dt.month
                año = fecha_dt.year
        else:
            mes = fecha_dt.month
            año = fecha_dt.year

        if advanced_conceptos:
            conceptos: list[dict] = []
            try:
                for c in conceptos_raw:
                    cid_raw = c.get("concepto_id")
                    try:
                        cid_val = int(cid_raw) if cid_raw is not None else None
                    except Exception:
                        cid_val = None
                    descripcion = c.get("descripcion")
                    try:
                        cantidad = int(c.get("cantidad") or 1)
                        precio_unitario = float(c.get("precio_unitario") or 0.0)
                    except Exception:
                        raise HTTPException(status_code=400, detail="Conceptos inválidos en payload")
                    if cantidad <= 0 or precio_unitario < 0:
                        raise HTTPException(status_code=400, detail="Cantidad/precio inválidos en conceptos")
                    if cid_val is None and (not descripcion or str(descripcion).strip() == ""):
                        raise HTTPException(status_code=400, detail="Cada ítem debe tener 'concepto_id' o 'descripcion'")
                    conceptos.append({
                        "concepto_id": cid_val,
                        "descripcion": descripcion,
                        "cantidad": cantidad,
                        "precio_unitario": precio_unitario
                    })
            except HTTPException:
                raise
            except Exception:
                raise HTTPException(status_code=400, detail="'conceptos' inválidos")

            try:
                pm.modificar_pago_avanzado(int(pago_id), usuario_id, metodo_pago_id_int, conceptos, fecha_dt)
            except ValueError as ve:
                raise HTTPException(status_code=400, detail=str(ve))
            except Exception as e:
                raise HTTPException(status_code=500, detail=str(e))
            return {"ok": True, "id": int(pago_id)}

        if monto is None or float(monto) <= 0:
            raise HTTPException(status_code=400, detail="'monto' debe ser mayor a 0")

        pago = Pago(id=int(pago_id), usuario_id=usuario_id, monto=float(monto), mes=mes, año=año, fecha_pago=fecha_dt, metodo_pago_id=metodo_pago_id_int)
        try:
            pm.modificar_pago(pago)
        except ValueError as ve:
            raise HTTPException(status_code=400, detail=str(ve))
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))
        return {"ok": True, "id": int(pago_id)}
    except HTTPException:
        raise
    except Exception as e:
        import traceback
        traceback.print_exc()
        return JSONResponse({"error": str(e)}, status_code=500)

@router.delete("/api/pagos/{pago_id}")
async def api_pagos_delete(pago_id: int, _=Depends(require_gestion_access)):
    pm = get_pm()
    db = get_db()
    if db is None:
        raise HTTPException(status_code=503, detail="DB no disponible")
    guard = _circuit_guard_json(db, f"/api/pagos/{pago_id}[DELETE]")
    if guard:
        return guard
    if pm is None:
        raise HTTPException(status_code=503, detail="PaymentManager no disponible")
    try:
        pm.eliminar_pago(int(pago_id))
        return {"ok": True}
    except ValueError as ve:
        raise HTTPException(status_code=400, detail=str(ve))
    except Exception as e:
        import traceback
        traceback.print_exc()
        return JSONResponse({"error": str(e)}, status_code=500)

@router.get("/api/usuario_pagos")
async def api_usuario_pagos(request: Request, _=Depends(require_owner)):
    """Lista de pagos reales de un usuario con soporte de búsqueda y paginación."""
    db = get_db()
    if db is None:
        return []
    try:
        usuario_id = request.query_params.get("usuario_id")
        q = request.query_params.get("q")
        limit = request.query_params.get("limit")
        offset = request.query_params.get("offset")
        if not usuario_id:
            return []
        lim = int(limit) if limit and limit.isdigit() else 50
        off = int(offset) if offset and offset.isdigit() else 0
        with db.get_connection_context() as conn:  # type: ignore
            cur = conn.cursor()
            if q:
                cur.execute(
                    """
                    SELECT p.fecha_pago::date, p.monto, COALESCE(u.tipo_cuota,'')
                    FROM pagos p
                    JOIN usuarios u ON u.id = p.usuario_id
                    WHERE p.usuario_id = %s AND (u.tipo_cuota ILIKE %s)
                    ORDER BY p.fecha_pago DESC
                    LIMIT %s OFFSET %s
                    """,
                    (int(usuario_id), f"%{q}%", lim, off)
                )
            else:
                cur.execute(
                    """
                    SELECT p.fecha_pago::date, p.monto, COALESCE(u.tipo_cuota,'')
                    FROM pagos p
                    JOIN usuarios u ON u.id = p.usuario_id
                    WHERE p.usuario_id = %s
                    ORDER BY p.fecha_pago DESC
                    LIMIT %s OFFSET %s
                    """,
                    (int(usuario_id), lim, off)
                )
            rows = []
            for r in cur.fetchall():
                rows.append({
                    "fecha": str(r[0]) if r[0] is not None else None,
                    "monto": float(r[1] or 0),
                    "tipo_cuota": r[2],
                })
        return rows
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)
