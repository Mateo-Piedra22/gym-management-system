import logging
import os
import json
from typing import Optional, List, Dict, Any

import psycopg2
import psycopg2.extras
from fastapi import APIRouter, Request, Depends, HTTPException, status, Response
from fastapi.responses import JSONResponse

from apps.webapp.dependencies import get_db, get_pm, require_gestion_access, require_owner
from apps.webapp.utils import _circuit_guard_json, get_gym_name

router = APIRouter()
logger = logging.getLogger(__name__)

@router.get("/api/whatsapp/state")
async def api_whatsapp_state(_=Depends(require_gestion_access)):
    pm = get_pm()
    db = get_db()
    if db is None:
        return {"disponible": False, "habilitado": False, "servidor_activo": False, "configuracion_valida": False}
    guard = _circuit_guard_json(db, "/api/whatsapp/state")
    if guard:
        return guard
    if pm is None:
        return {"disponible": False, "habilitado": False, "servidor_activo": False, "configuracion_valida": False}
    try:
        return pm.obtener_estado_whatsapp()
    except Exception as e:
        import traceback; traceback.print_exc()
        return JSONResponse({"error": str(e)}, status_code=500)

@router.get("/api/whatsapp/stats")
async def api_whatsapp_stats(_=Depends(require_gestion_access)):
    pm = get_pm()
    db = get_db()
    if db is None:
        return {"error": "DB no disponible"}
    guard = _circuit_guard_json(db, "/api/whatsapp/stats")
    if guard:
        return guard
    if pm is None:
        return {"error": "PaymentManager no disponible"}
    try:
        return pm.obtener_estadisticas_whatsapp()
    except Exception as e:
        import traceback; traceback.print_exc()
        return JSONResponse({"error": str(e)}, status_code=500)

@router.get("/api/whatsapp/pendings")
async def api_whatsapp_pendings(request: Request, _=Depends(require_gestion_access)):
    pm = get_pm()
    db = get_db()
    if db is None:
        return JSONResponse({"items": []})
    guard = _circuit_guard_json(db, "/api/whatsapp/pendings")
    if guard:
        return guard
    try:
        dias_param = request.query_params.get("dias")
        try:
            dias = int(dias_param) if dias_param else 30
        except Exception:
            dias = 30
        limite_param = request.query_params.get("limit")
        try:
            limite = int(limite_param) if limite_param else 200
        except Exception:
            limite = 200
        interval_str = f"{dias} days"
        items: list[dict[str, Any]] = []
        with db.get_connection_context() as conn:  # type: ignore
            cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            cur.execute(
                """
                SELECT DISTINCT ON (wm.phone_number)
                       wm.id,
                       wm.user_id,
                       COALESCE(u.nombre,'') AS usuario_nombre,
                       COALESCE(u.telefono,'') AS usuario_telefono,
                       wm.phone_number,
                       wm.message_type,
                       wm.template_name,
                       wm.message_content,
                       wm.status,
                       wm.message_id,
                       wm.sent_at AS fecha_envio
                FROM whatsapp_messages wm
                LEFT JOIN usuarios u ON u.id = wm.user_id
                WHERE wm.status = 'failed'
                  AND wm.sent_at >= CURRENT_TIMESTAMP - (%s::interval)
                ORDER BY wm.phone_number, wm.sent_at DESC
                LIMIT %s
                """,
                (interval_str, limite)
            )
            for row in cur.fetchall() or []:
                try:
                    r = dict(row)
                except Exception:
                    r = row  # type: ignore
                # Asegurar serialización de fecha
                if r.get("fecha_envio") is not None:
                    try:
                        r["fecha_envio"] = str(r["fecha_envio"])  # ISO-like
                    except Exception:
                        pass
                items.append(r)
        return {"items": items}
    except Exception as e:
        import traceback; traceback.print_exc()
        return JSONResponse({"error": str(e), "items": []}, status_code=500)

@router.post("/api/whatsapp/retry")
async def api_whatsapp_retry(request: Request, _=Depends(require_owner)):
    pm = get_pm()
    db = get_db()
    if db is None:
        return JSONResponse({"success": False, "message": "DB no disponible"}, status_code=503)
    guard = _circuit_guard_json(db, "/api/whatsapp/retry")
    if guard:
        return guard
    if pm is None:
        return JSONResponse({"success": False, "message": "PaymentManager no disponible"}, status_code=503)
    try:
        try:
            payload = await request.json()
        except Exception:
            payload = {}
        telefono = str(payload.get("telefono") or payload.get("phone") or "").strip()
        usuario_id = payload.get("usuario_id")

        uid = None
        if usuario_id is not None:
            try:
                uid = int(usuario_id)
            except Exception:
                uid = None
        if uid is None and telefono:
            try:
                uid = db._obtener_user_id_por_telefono_whatsapp(telefono)  # type: ignore
            except Exception:
                uid = None
        if uid is None:
            return JSONResponse({"success": False, "message": "usuario_id no encontrado"}, status_code=400)

        last_type = None
        with db.get_connection_context() as conn:  # type: ignore
            cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            if telefono:
                cur.execute(
                    """
                    SELECT message_type, template_name, message_content
                    FROM whatsapp_messages
                    WHERE phone_number = %s AND status = 'failed'
                    ORDER BY sent_at DESC
                    LIMIT 1
                    """,
                    (telefono,)
                )
            else:
                cur.execute(
                    """
                    SELECT message_type, template_name, message_content
                    FROM whatsapp_messages
                    WHERE user_id = %s AND status = 'failed'
                    ORDER BY sent_at DESC
                    LIMIT 1
                    """,
                    (uid,)
                )
            row = cur.fetchone() or {}
            try:
                last_type = (row.get("message_type") or "").strip().lower()
            except Exception:
                last_type = None

        if last_type in ("welcome", "bienvenida"):
            ok = pm.enviar_mensaje_bienvenida_whatsapp(int(uid))
            return {"success": bool(ok), "tipo": last_type or "welcome"}
        elif last_type in ("overdue", "recordatorio_vencida", "payment_reminder", "pago_recordatorio"):
            if getattr(pm, 'whatsapp_manager', None):
                ok = pm.whatsapp_manager.enviar_recordatorio_cuota_vencida(int(uid))
                return {"success": bool(ok), "tipo": last_type or "recordatorio_vencida"}
            else:
                return JSONResponse({"success": False, "message": "Gestor WhatsApp no disponible"}, status_code=503)
        elif last_type in ("class_reminder", "recordatorio_clase"):
            return JSONResponse({"success": False, "message": "recordatorio_clase requiere datos de clase"}, status_code=400)
        else:
            ok = pm.enviar_mensaje_bienvenida_whatsapp(int(uid))
            return {"success": bool(ok), "tipo": last_type or "welcome"}
    except Exception as e:
        import traceback; traceback.print_exc()
        return JSONResponse({"success": False, "error": str(e)}, status_code=500)

@router.post("/api/whatsapp/clear_failures")
async def api_whatsapp_clear_failures(request: Request, _=Depends(require_owner)):
    pm = get_pm()
    db = get_db()
    if db is None:
        return JSONResponse({"success": False, "message": "DB no disponible"}, status_code=503)
    guard = _circuit_guard_json(db, "/api/whatsapp/clear_failures")
    if guard:
        return guard
    try:
        try:
            payload = await request.json()
        except Exception:
            payload = {}
        telefono = str(payload.get("telefono") or payload.get("phone") or "").strip()
        dias_param = payload.get("desde_dias") or payload.get("days")
        try:
            dias = int(dias_param) if dias_param is not None else 30
        except Exception:
            dias = 30
        interval_str = f"{dias} days"

        total_deleted = 0
        phones: list[str] = []
        with db.get_connection_context() as conn:  # type: ignore
            cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            if telefono:
                cur.execute(
                    """
                    DELETE FROM whatsapp_messages
                    WHERE phone_number = %s AND status = 'failed'
                      AND sent_at >= CURRENT_TIMESTAMP - (%s::interval)
                    """,
                    (telefono, interval_str)
                )
                try:
                    total_deleted = int(cur.rowcount or 0)
                except Exception:
                    total_deleted = 0
                try:
                    conn.commit()
                except Exception:
                    pass
                phones = [telefono] if telefono else []
            else:
                # Obtener teléfonos con fallidos recientes y limpiar todos
                cur.execute(
                    """
                    SELECT DISTINCT phone_number
                    FROM whatsapp_messages
                    WHERE status = 'failed'
                      AND sent_at >= CURRENT_TIMESTAMP - (%s::interval)
                    ORDER BY phone_number
                    """,
                    (interval_str,)
                )
                phones = [r.get("phone_number") for r in (cur.fetchall() or []) if r.get("phone_number")]
                for ph in phones:
                    try:
                        cur.execute(
                            """
                            DELETE FROM whatsapp_messages
                            WHERE phone_number = %s AND status = 'failed'
                              AND sent_at >= CURRENT_TIMESTAMP - (%s::interval)
                            """,
                            (ph, interval_str)
                        )
                        total_deleted += int(cur.rowcount or 0)
                    except Exception:
                        pass
                try:
                    conn.commit()
                except Exception:
                    pass
        return {"success": True, "deleted": int(total_deleted), "phones": phones}
    except Exception as e:
        import traceback; traceback.print_exc()
        return JSONResponse({"success": False, "error": str(e)}, status_code=500)

@router.post("/api/whatsapp/server/start")
async def api_whatsapp_server_start(_=Depends(require_owner)):
    pm = get_pm()
    db = get_db()
    if db is None:
        return JSONResponse({"success": False, "message": "DB no disponible"}, status_code=503)
    guard = _circuit_guard_json(db, "/api/whatsapp/server/start")
    if guard:
        return guard
    if pm is None:
        return JSONResponse({"success": False, "message": "PaymentManager no disponible"}, status_code=503)
    try:
        ok = pm.iniciar_servidor_whatsapp()
        return {"success": bool(ok)}
    except Exception as e:
        import traceback; traceback.print_exc()
        return JSONResponse({"success": False, "error": str(e)}, status_code=500)

@router.post("/api/whatsapp/server/stop")
async def api_whatsapp_server_stop(_=Depends(require_owner)):
    pm = get_pm()
    db = get_db()
    if db is None:
        return JSONResponse({"success": False, "message": "DB no disponible"}, status_code=503)
    guard = _circuit_guard_json(db, "/api/whatsapp/server/stop")
    if guard:
        return guard
    if pm is None:
        return JSONResponse({"success": False, "message": "PaymentManager no disponible"}, status_code=503)
    try:
        ok = pm.detener_servidor_whatsapp()
        return {"success": bool(ok)}
    except Exception as e:
        import traceback; traceback.print_exc()
        return JSONResponse({"success": False, "error": str(e)}, status_code=500)

@router.post("/api/whatsapp/config")
async def api_whatsapp_config(request: Request, _=Depends(require_owner)):
    pm = get_pm()
    db = get_db()
    if db is None:
        return JSONResponse({"success": False, "message": "DB no disponible"}, status_code=503)
    guard = _circuit_guard_json(db, "/api/whatsapp/config")
    if guard:
        return guard
    if pm is None:
        return JSONResponse({"success": False, "message": "PaymentManager no disponible"}, status_code=503)
    try:
        content_type = request.headers.get("content-type", "")
        if content_type.startswith("application/json"):
            data = await request.json()
        else:
            data = await request.form()
    except Exception:
        data = {}
    # Filtrar claves permitidas (configuración conocida)
    allowed_keys = {
        "phone_number_id", "whatsapp_business_account_id", "access_token",
        "allowlist_numbers", "allowlist_enabled", "enable_webhook",
        "max_retries", "retry_delay_seconds"
    }
    try:
        cfg = {k: (data.get(k)) for k in allowed_keys if (k in data)}
        # Normalizar booleanos
        for bk in ("allowlist_enabled", "enable_webhook"):
            if bk in cfg and cfg[bk] is not None:
                val = cfg[bk]
                if isinstance(val, bool):
                    cfg[bk] = "true" if val else "false"
                else:
                    cfg[bk] = str(val).strip().lower()
        ok = pm.configurar_whatsapp(cfg)
        try:
            pm.start_whatsapp_initialization(background=True, delay_seconds=1.5)
        except Exception:
            pass
        return {"success": bool(ok), "applied_keys": list(cfg.keys())}
    except Exception as e:
        import traceback; traceback.print_exc()
        return JSONResponse({"success": False, "error": str(e)}, status_code=500)

@router.post("/api/usuarios/{usuario_id}/whatsapp/bienvenida")
async def api_usuario_whatsapp_bienvenida(usuario_id: int, _=Depends(require_gestion_access)):
    pm = get_pm()
    db = get_db()
    if db is None:
        return JSONResponse({"success": False, "message": "DB no disponible"}, status_code=503)
    guard = _circuit_guard_json(db, "/api/usuarios/{id}/whatsapp/bienvenida")
    if guard:
        return guard
    if pm is None:
        return JSONResponse({"success": False, "message": "PaymentManager no disponible"}, status_code=503)
    try:
        ok = pm.enviar_mensaje_bienvenida_whatsapp(int(usuario_id))
        return {"success": bool(ok)}
    except Exception as e:
        import traceback; traceback.print_exc()
        return JSONResponse({"success": False, "error": str(e)}, status_code=500)

@router.post("/api/usuarios/{usuario_id}/whatsapp/confirmacion_pago")
async def api_usuario_whatsapp_confirmacion_pago(usuario_id: int, request: Request, _=Depends(require_gestion_access)):
    """Envía confirmación de pago por WhatsApp para un usuario.
    Si no se proporcionan datos en el payload, intenta usar el último pago del usuario.
    """
    pm = get_pm()
    db = get_db()
    if db is None:
        return JSONResponse({"success": False, "message": "DB no disponible"}, status_code=503)
    guard = _circuit_guard_json(db, "/api/usuarios/{id}/whatsapp/confirmacion_pago")
    if guard:
        return guard
    if pm is None or not getattr(pm, 'whatsapp_manager', None):
        return JSONResponse({"success": False, "message": "Gestor WhatsApp no disponible"}, status_code=503)
    try:
        try:
            payload = await request.json()
        except Exception:
            payload = {}
        monto = payload.get("monto")
        mes = payload.get("mes") or payload.get("month")
        anio = payload.get("año") or payload.get("anio") or payload.get("year")

        nombre = None
        telefono = None
        try:
            u = db.obtener_usuario_por_id(int(usuario_id))  # type: ignore
            if u:
                nombre = getattr(u, 'nombre', None)
                telefono = getattr(u, 'telefono', None)
        except Exception:
            pass

        # Si faltan datos, intentar obtener el pago más reciente
        if (monto is None or mes is None or anio is None):
            try:
                # Usa PaymentManager para obtener historial y tomar el más reciente
                pagos = []
                if pm and hasattr(pm, 'obtener_historial_pagos'):
                    pagos = pm.obtener_historial_pagos(int(usuario_id), limit=1)  # type: ignore
                if pagos:
                    p0 = pagos[0]
                    monto = getattr(p0, 'monto', None)
                    mes = getattr(p0, 'mes', None)
                    anio = getattr(p0, 'año', None)
            except Exception:
                pass

        # Validar datos mínimos
        if not telefono or monto is None or mes is None or anio is None:
            return JSONResponse({"success": False, "message": "Datos insuficientes para confirmación"}, status_code=400)

        payment_data = {
            'user_id': int(usuario_id),
            'phone': str(telefono),
            'name': str(nombre or ""),
            'amount': float(monto),
            'date': f"{int(mes):02d}/{int(anio)}"
        }

        ok = pm.whatsapp_manager.send_payment_confirmation(payment_data)
        return {"success": bool(ok)}
    except Exception as e:
        import traceback; traceback.print_exc()
        return JSONResponse({"success": False, "error": str(e)}, status_code=500)

@router.post("/api/usuarios/{usuario_id}/whatsapp/desactivacion")
async def api_usuario_whatsapp_desactivacion(usuario_id: int, request: Request, _=Depends(require_gestion_access)):
    """Envía notificación de desactivación por cuotas vencidas u otro motivo."""
    pm = get_pm()
    db = get_db()
    if db is None:
        return JSONResponse({"success": False, "message": "DB no disponible"}, status_code=503)
    guard = _circuit_guard_json(db, "/api/usuarios/{id}/whatsapp/desactivacion")
    if guard:
        return guard
    if pm is None or not getattr(pm, 'whatsapp_manager', None):
        return JSONResponse({"success": False, "message": "Gestor WhatsApp no disponible"}, status_code=503)
    try:
        try:
            payload = await request.json()
        except Exception:
            payload = {}
        motivo = (payload.get("motivo") or "cuotas vencidas").strip()
        ok = pm.whatsapp_manager.enviar_notificacion_desactivacion(usuario_id=int(usuario_id), motivo=motivo, force_send=True)
        return {"success": bool(ok)}
    except Exception as e:
        import traceback; traceback.print_exc()
        return JSONResponse({"success": False, "error": str(e)}, status_code=500)

@router.post("/api/usuarios/{usuario_id}/whatsapp/recordatorio_vencida")
async def api_usuario_whatsapp_recordatorio_vencida(usuario_id: int, _=Depends(require_gestion_access)):
    pm = get_pm()
    db = get_db()
    if db is None:
        return JSONResponse({"success": False, "message": "DB no disponible"}, status_code=503)
    guard = _circuit_guard_json(db, "/api/usuarios/{id}/whatsapp/recordatorio_vencida")
    if guard:
        return guard
    if pm is None or not getattr(pm, 'whatsapp_manager', None):
        return JSONResponse({"success": False, "message": "Gestor WhatsApp no disponible"}, status_code=503)
    try:
        ok = pm.whatsapp_manager.enviar_recordatorio_cuota_vencida(int(usuario_id))
        return {"success": bool(ok)}
    except Exception as e:
        import traceback; traceback.print_exc()
        return JSONResponse({"success": False, "error": str(e)}, status_code=500)

@router.post("/api/usuarios/{usuario_id}/whatsapp/recordatorio_clase")
async def api_usuario_whatsapp_recordatorio_clase(usuario_id: int, request: Request, _=Depends(require_gestion_access)):
    pm = get_pm()
    db = get_db()
    if db is None:
        return JSONResponse({"success": False, "message": "DB no disponible"}, status_code=503)
    guard = _circuit_guard_json(db, "/api/usuarios/{id}/whatsapp/recordatorio_clase")
    if guard:
        return guard
    if pm is None or not getattr(pm, 'whatsapp_manager', None):
        return JSONResponse({"success": False, "message": "Gestor WhatsApp no disponible"}, status_code=503)
    try:
        try:
            payload = await request.json()
        except Exception:
            payload = {}
        clase_info = {
            'tipo_clase': (payload.get('tipo_clase') or payload.get('clase_nombre') or ''),
            'fecha': (payload.get('fecha') or ''),
            'hora': (payload.get('hora') or ''),
        }
        ok = pm.whatsapp_manager.enviar_recordatorio_horario_clase(int(usuario_id), clase_info)
        return {"success": bool(ok)}
    except Exception as e:
        import traceback; traceback.print_exc()
        return JSONResponse({"success": False, "error": str(e)}, status_code=500)

@router.get("/api/usuarios/{usuario_id}/whatsapp/ultimo")
async def api_usuario_whatsapp_ultimo(usuario_id: int, request: Request, _=Depends(require_owner)):
    db = get_db()
    if db is None:
        return JSONResponse({"success": False, "message": "DB no disponible"}, status_code=503)
    guard = _circuit_guard_json(db, "/api/usuarios/{id}/whatsapp/ultimo")
    if guard:
        return guard
    try:
        direccion = request.query_params.get("direccion") or None
        tipo = request.query_params.get("tipo") or None
        if direccion not in (None, "enviado", "recibido"):
            direccion = None
        item = db.obtener_ultimo_mensaje_whatsapp(user_id=int(usuario_id), telefono=None, message_type=tipo, direccion=direccion)  # type: ignore
        if not item:
            return {"success": True, "item": None}
        return {"success": True, "item": item}
    except Exception as e:
        import traceback; traceback.print_exc()
        return JSONResponse({"success": False, "error": str(e)}, status_code=500)

@router.get("/api/usuarios/{usuario_id}/whatsapp/historial")
async def api_usuario_whatsapp_historial(usuario_id: int, request: Request, _=Depends(require_gestion_access)):
    db = get_db()
    if db is None:
        return JSONResponse({"success": False, "message": "DB no disponible"}, status_code=503)
    guard = _circuit_guard_json(db, "/api/usuarios/{id}/whatsapp/historial")
    if guard:
        return guard
    try:
        tipo = request.query_params.get("tipo") or None
        limite_q = request.query_params.get("limit")
        limite = 50
        try:
            limite = int(limite_q) if (limite_q and str(limite_q).isdigit()) else 50
        except Exception:
            limite = 50
        items = db.obtener_historial_mensajes_whatsapp(user_id=int(usuario_id), message_type=(tipo or None), limit=int(limite))  # type: ignore
        # Normalizar fechas a string para evitar problemas de serialización
        for it in items or []:
            for k in ("sent_at", "created_at"):
                if it.get(k) is not None:
                    try:
                        it[k] = str(it[k])
                    except Exception:
                        pass
        return {"success": True, "items": items}
    except Exception as e:
        import traceback; traceback.print_exc()
        return JSONResponse({"success": False, "error": str(e)}, status_code=500)

@router.delete("/api/usuarios/{usuario_id}/whatsapp/{message_pk}")
async def api_usuario_whatsapp_delete(usuario_id: int, message_pk: int, request: Request, _=Depends(require_owner)):
    db = get_db()
    if db is None:
        return JSONResponse({"success": False, "message": "DB no disponible"}, status_code=503)
    guard = _circuit_guard_json(db, "/api/usuarios/{id}/whatsapp/delete")
    if guard:
        return guard
    try:
        # Obtener valores previos para auditoría desde repositorio
        try:
            old_item = db.obtener_mensaje_whatsapp_por_pk(int(usuario_id), int(message_pk))  # type: ignore
        except Exception:
            old_item = None

        ok = bool(db.eliminar_mensaje_whatsapp_por_pk(int(usuario_id), int(message_pk)))  # type: ignore
        if not ok:
            return JSONResponse({"success": False, "message": "Mensaje no encontrado"}, status_code=404)

        # Registrar auditoría de eliminación
        try:
            ip_addr = (getattr(request, 'client', None).host if getattr(request, 'client', None) else None)
            ua = request.headers.get('user-agent', '')
            sid = getattr(getattr(request, 'session', {}), 'get', lambda *a, **k: None)('session_id')
            db.registrar_audit_log(  # type: ignore
                user_id=int(usuario_id),
                action="DELETE",
                table_name="whatsapp_messages",
                record_id=int(message_pk),
                old_values=json.dumps(old_item, default=str) if old_item else None,
                new_values=None,
                ip_address=ip_addr,
                user_agent=ua,
                session_id=sid,
            )
        except Exception:
            pass

        return {"success": True, "deleted": int(message_pk)}
    except Exception as e:
        import traceback; traceback.print_exc()
        return JSONResponse({"success": False, "error": str(e)}, status_code=500)

@router.delete("/api/usuarios/{usuario_id}/whatsapp/by-mid/{message_id}")
async def api_usuario_whatsapp_delete_by_mid(usuario_id: int, message_id: str, request: Request, _=Depends(require_owner)):
    db = get_db()
    if db is None:
        return JSONResponse({"success": False, "message": "DB no disponible"}, status_code=503)
    guard = _circuit_guard_json(db, "/api/usuarios/{id}/whatsapp/delete_by_mid")
    if guard:
        return guard
    try:
        # Obtener item previo y pk utilizando repositorio
        pk_id = None
        try:
            old_item = db.obtener_mensaje_whatsapp_por_message_id(int(usuario_id), str(message_id))  # type: ignore
            if old_item:
                try:
                    pk_id = int(old_item.get("id"))
                except Exception:
                    pk_id = None
        except Exception:
            old_item = None

        # Realizar borrado por message_id usando método del repositorio
        try:
            deleted = bool(db.eliminar_mensaje_whatsapp_por_message_id(int(usuario_id), str(message_id)))  # type: ignore
        except Exception:
            deleted = False

        if not deleted:
            return JSONResponse({"success": False, "message": "Mensaje no encontrado"}, status_code=404)

        # Registrar auditoría
        try:
            ip_addr = (getattr(request, 'client', None).host if getattr(request, 'client', None) else None)
            ua = request.headers.get('user-agent', '')
            sid = getattr(getattr(request, 'session', {}), 'get', lambda *a, **k: None)('session_id')
            db.registrar_audit_log(  # type: ignore
                user_id=int(usuario_id),
                action="DELETE",
                table_name="whatsapp_messages",
                record_id=int(pk_id) if pk_id is not None else None,
                old_values=json.dumps(old_item, default=str) if old_item else None,
                new_values=None,
                ip_address=ip_addr,
                user_agent=ua,
                session_id=sid,
            )
        except Exception:
            pass

        return {"success": True, "deleted_mid": str(message_id), "deleted_pk": int(pk_id) if pk_id is not None else None}
    except Exception as e:
        import traceback; traceback.print_exc()
        return JSONResponse({"success": False, "error": str(e)}, status_code=500)

# --- Webhooks ---

@router.get("/webhooks/whatsapp")
async def whatsapp_verify(request: Request):
    try:
        mode = request.query_params.get("hub.mode")
        token = request.query_params.get("hub.verify_token")
        challenge = request.query_params.get("hub.challenge")
        expected = os.getenv("WHATSAPP_VERIFY_TOKEN", "")
        if mode == "subscribe" and expected and token == expected and challenge:
            return Response(content=str(challenge), media_type="text/plain")
    except Exception as e:
        logging.getLogger(__name__).error(f"WhatsApp verify error: {e}")
    raise HTTPException(status_code=403, detail="Invalid verify token")

@router.post("/webhooks/whatsapp")
async def whatsapp_webhook(request: Request):
    # Verificación de firma (si se configura WHATSAPP_APP_SECRET)
    logger = logging.getLogger(__name__)
    try:
        raw = await request.body()
        app_secret = os.getenv("WHATSAPP_APP_SECRET", "")
        if app_secret:
            try:
                import hmac, hashlib
                sig_header = request.headers.get("X-Hub-Signature-256") or ""
                expected = "sha256=" + hmac.new(app_secret.encode("utf-8"), raw, hashlib.sha256).hexdigest()
                if not hmac.compare_digest(expected, sig_header):
                    raise HTTPException(status_code=403, detail="Invalid signature")
            except HTTPException:
                raise
            except Exception as e:
                logger.error(f"WhatsApp signature check error: {e}")
                raise HTTPException(status_code=400, detail="Signature verification error")
        import json as _json
        try:
            payload = _json.loads(raw.decode("utf-8"))
        except Exception:
            raise HTTPException(status_code=400, detail="Invalid JSON")
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"WhatsApp webhook read error: {e}")
        raise HTTPException(status_code=400, detail="Bad Request")

    # DB
    try:
        db = get_db()
    except Exception:
        db = None

    # Procesamiento de estados y mensajes
    try:
        for entry in (payload.get("entry") or []):
            for change in (entry.get("changes") or []):
                value = change.get("value") or {}
                # Actualizaciones de estado
                for status in (value.get("statuses") or []):
                    mid = status.get("id")
                    st = status.get("status")
                    if db and mid and st:
                        try:
                            db.actualizar_estado_mensaje_whatsapp(mid, st)  # type: ignore
                        except Exception as e:
                            logger.error(f"Estado WA update failed id={mid} status={st}: {e}")
                # Mensajes entrantes
                for msg in (value.get("messages") or []):
                    mid = msg.get("id")
                    mtype = msg.get("type")
                    wa_from = msg.get("from")
                    
                    text = None
                    button_id = None
                    button_title = None
                    list_id = None
                    list_title = None
                    try:
                        if mtype == "text":
                            text = (msg.get("text") or {}).get("body")
                        elif mtype == "button":
                            text = (msg.get("button") or {}).get("text")
                        elif mtype == "interactive":
                            ir = msg.get("interactive") or {}
                            br = ir.get("button_reply") or {}
                            lr = ir.get("list_reply") or {}
                            button_id = br.get("id")
                            button_title = br.get("title")
                            list_id = lr.get("id")
                            list_title = lr.get("title")
                            text = button_title or list_title
                        elif mtype == "image":
                            text = "[imagen]"
                        elif mtype == "audio":
                            text = "[audio]"
                        elif mtype == "video":
                            text = "[video]"
                        elif mtype == "document":
                            text = "[documento]"
                    except Exception:
                        pass

                    # Registrar en DB (mensaje recibido)
                    if db:
                        try:
                            uid = None
                            try:
                                uid = db._obtener_user_id_por_telefono_whatsapp(wa_from)  # type: ignore
                            except Exception:
                                uid = None
                            db.registrar_mensaje_whatsapp(  # type: ignore
                                user_id=uid,
                                message_type="welcome",
                                template_name="incoming",
                                phone_number=wa_from,
                                message_content=(text or ""),
                                status="received",
                                message_id=mid,
                            )
                        except Exception as e:
                            logger.error(f"WA incoming log failed id={mid}: {e}")

                    # Auto-acciones: promoción/declinación desde lista de espera
                    try:
                        import unicodedata as _unic
                        def _sanitize_text(s: str) -> str:
                            s = (s or "").strip()
                            s = "".join(c for c in _unic.normalize("NFD", s) if _unic.category(c) != "Mn")
                            s = s.lower()
                            # Quitar signos de puntuación comunes
                            for ch in [".", ",", ";", "!", "?", "¡", "¿"]:
                                s = s.replace(ch, "")
                            return s

                        # Señales de SI/NO desde texto o interacción
                        tid = button_id or list_id or ""
                        ttitle = button_title or list_title or text or ""
                        stext = _sanitize_text(ttitle)
                        yes_signal = stext == "si"
                        no_signal = stext == "no"

                        # IDs de interacción con clase_horario_id: WAITLIST_PROMOTE:<id> / WAITLIST_DECLINE:<id>
                        target_clase_id = None
                        try:
                            if isinstance(tid, str):
                                if tid.startswith("WAITLIST_PROMOTE:"):
                                    target_clase_id = int(tid.split(":", 1)[1])
                                    yes_signal = True
                                elif tid.startswith("WAITLIST_DECLINE:"):
                                    target_clase_id = int(tid.split(":", 1)[1])
                                    no_signal = True
                        except Exception:
                            target_clase_id = None

                        if db and (yes_signal or no_signal):
                            # Resolver usuario desde teléfono con normalización mejorada
                            uid = None
                            try:
                                uid = db._obtener_user_id_por_telefono_whatsapp(wa_from)  # type: ignore
                            except Exception:
                                uid = None
                            if uid:
                                # Fallback para clase objetivo: primera lista de espera activa del usuario
                                if target_clase_id is None:
                                    try:
                                        with db.get_connection_context() as conn:  # type: ignore
                                            cur = conn.cursor()
                                            cur.execute(
                                                """
                                                SELECT clase_horario_id
                                                FROM clase_lista_espera
                                                WHERE usuario_id = %s AND activo = true
                                                ORDER BY posicion ASC
                                                LIMIT 1
                                                """,
                                                (int(uid),)
                                            )
                                            row = cur.fetchone()
                                            if row:
                                                try:
                                                    target_clase_id = int(row[0])
                                                except Exception:
                                                    target_clase_id = None
                                    except Exception:
                                        target_clase_id = None

                                if target_clase_id:
                                    if yes_signal:
                                        # Registrar auditoría de confirmación; desktop realiza inscripción y mensajería
                                        try:
                                            ip_addr = (getattr(request, 'client', None).host if getattr(request, 'client', None) else None)
                                            ua = request.headers.get('user-agent', '')
                                            sid = getattr(getattr(request, 'session', {}), 'get', lambda *a, **k: None)('session_id')
                                            db.registrar_audit_log(  # type: ignore
                                                user_id=int(uid),
                                                action="auto_promote_waitlist",
                                                table_name="clase_lista_espera",
                                                record_id=int(target_clase_id),
                                                old_values=None,
                                                new_values=json.dumps({"confirmado": True}),
                                                ip_address=ip_addr,
                                                user_agent=ua,
                                                session_id=sid,
                                            )
                                        except Exception as e:
                                            logger.error(f"WA auto-promote audit log failed uid={uid} clase_id={target_clase_id}: {e}")
                                        logger.info(f"WA auto-promote: audit registrada usuario_id={uid} clase_horario_id={target_clase_id} from={wa_from} mid={mid}")
                                    elif no_signal:
                                        # Declinación explícita: mantener en lista; envío delegado al desktop
                                        try:
                                            ip_addr = (getattr(request, 'client', None).host if getattr(request, 'client', None) else None)
                                            ua = request.headers.get('user-agent', '')
                                            sid = getattr(getattr(request, 'session', {}), 'get', lambda *a, **k: None)('session_id')
                                            db.registrar_audit_log(  # type: ignore
                                                user_id=int(uid),
                                                action="decline_waitlist_promotion",
                                                table_name="clase_lista_espera",
                                                record_id=int(target_clase_id),
                                                old_values=None,
                                                new_values=json.dumps({"declinado": True}),
                                                ip_address=ip_addr,
                                                user_agent=ua,
                                                session_id=sid,
                                            )
                                        except Exception:
                                            pass
                    except Exception:
                        pass
        return JSONResponse({"status": "ok"})
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"WhatsApp webhook processing error: {e}")
        return JSONResponse({"status": "error"}, status_code=500)
