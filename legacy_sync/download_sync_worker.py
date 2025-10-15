import threading
import time
import logging
import os
import json
from typing import Any, Dict, List, Optional

try:
    import requests  # type: ignore
except Exception:  # pragma: no cover
    requests = None  # type: ignore

try:
    from sync_client import _resolve_base_url as _get_base_url, resolve_auth_headers as _get_auth_headers  # type: ignore
except Exception:  # pragma: no cover
    def _get_base_url() -> str:  # type: ignore
        return "http://127.0.0.1:8080"
    def _get_auth_headers() -> Dict[str, str]:  # type: ignore
        return {}

try:
    from device_id import get_device_id  # type: ignore
except Exception:  # pragma: no cover
    def get_device_id() -> str:  # type: ignore
        return "unknown-device"


class DownloadSyncWorker:
    """Hilo ligero que consulta /api/sync/download en el proxy local y
    aplica cambios en la base local. Pensado para ejecutarse en Desktop.
    """

    def __init__(self, db_manager, interval_sec: int = 30):
        self.db_manager = db_manager
        self.interval_sec = max(5, int(interval_sec))
        self._stop = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._last_since: Optional[str] = None  # ISO-8601
        self._state_path = self._resolve_state_path()
        self._device_id = get_device_id()
        # Backoff y control de frecuencia ante errores o 429/5xx
        self._next_allowed_at: float = 0.0
        self._err_attempts: int = 0
        self._load_state()

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._thread = threading.Thread(target=self._run, name="DownloadSyncWorker", daemon=True)
        self._thread.start()
        logging.info("DownloadSyncWorker: iniciado en segundo plano")

    def stop(self) -> None:
        try:
            self._stop.set()
            self._save_state()
        except Exception:
            pass

    def _run(self) -> None:
        while not self._stop.is_set():
            try:
                self._tick()
            except Exception as e:
                logging.debug(f"DownloadSyncWorker: tick error: {e}")
            try:
                time.sleep(self.interval_sec)
            except Exception:
                pass

    def _tick(self) -> None:
        if requests is None:
            return
        # Respetar backoff configurado por errores previos
        try:
            if time.time() < self._next_allowed_at:
                return
        except Exception:
            pass
        base = _get_base_url().rstrip('/')
        url = f"{base}/api/sync/download"
        params = {}
        if self._last_since:
            params['since'] = self._last_since
        try:
            if self._device_id:
                params['device_id'] = self._device_id
        except Exception:
            pass
        try:
            headers: Dict[str, str] = {}
            try:
                headers = _get_auth_headers() or {}
            except Exception:
                headers = {}
            resp = requests.get(url, params=params, headers=headers, timeout=4.0)
            if resp.status_code != 200:
                # Manejo explícito de 429 y 5xx con backoff
                now = time.time()
                if resp.status_code == 429:
                    retry_after = 10.0
                    try:
                        ra = resp.headers.get('Retry-After')
                        if ra:
                            retry_after = float(ra)
                    except Exception:
                        pass
                    self._next_allowed_at = now + max(1.0, min(120.0, retry_after))
                elif 500 <= resp.status_code < 600:
                    delay = min(60.0, max(2.0, 2 ** max(0, self._err_attempts)))
                    self._err_attempts = min(self._err_attempts + 1, 10)
                    self._next_allowed_at = now + delay
                else:
                    # Otros códigos: reintentar en el próximo tick sin penalidad fuerte
                    self._err_attempts = 0
                return
            try:
                data = resp.json() or {}
            except Exception:
                # JSON inválido: aplicar backoff corto
                self._next_allowed_at = time.time() + 5.0
                self._err_attempts = min(self._err_attempts + 1, 10)
                return
            # Éxito: resetear backoff
            self._err_attempts = 0
            self._next_allowed_at = 0.0
            ops: List[Dict[str, Any]] = data.get('operations') or []
            # Honrar hints de backoff del proxy/servidor aún si status es 200
            try:
                retry_hint = data.get('retry_after_sec')
                if (ops is None or len(ops) == 0) and isinstance(retry_hint, (int, float)) and retry_hint > 0:
                    self._next_allowed_at = time.time() + max(1.0, min(120.0, float(retry_hint)))
                    # Evitar avanzar marcador en respuestas vacías
                    return
                msg = data.get('message')
                if (ops is None or len(ops) == 0) and isinstance(msg, str) and ('circuit' in msg.lower() or 'upstream' in msg.lower()):
                    # Backoff prudente si el proxy indicó circuito abierto o upstream en fallo
                    self._next_allowed_at = time.time() + 10.0
                    return
            except Exception:
                pass
            if not ops:
                # No avanzar marcador en respuestas vacías para evitar saltos de watermark
                return
            applied_any = self._apply_operations(ops)
            # Avanzar marca de tiempo si se provee
            self._last_since = data.get('latest') or self._last_since
            if not self._last_since:
                # Calcular desde las ops aplicadas
                try:
                    ts_values = [op.get('ts') for op in ops if isinstance(op.get('ts'), str)]
                    if ts_values:
                        self._last_since = max(ts_values)
                except Exception:
                    pass
            # Guardar estado aunque no hayamos aplicado nada (podrían ser ops del mismo dispositivo)
            self._save_state()
        except Exception as e:  # pragma: no cover
            logging.debug(f"DownloadSyncWorker: fallo GET download: {e}")

    # Aplicadores mínimos. Ajustar según esquema servidor.
    def _apply_operations(self, ops: List[Dict[str, Any]]) -> bool:
        applied = False
        for op in ops:
            try:
                name = op.get('name') or op.get('type') or ''
                payload: Dict[str, Any] = op.get('payload') or op.get('data') or {}
                # Evitar re-aplicar operaciones originadas en este mismo dispositivo
                src = op.get('source') or {}
                op_device = None
                try:
                    if isinstance(src, dict):
                        op_device = src.get('device_id')
                    if not op_device:
                        op_device = op.get('device_id')
                except Exception:
                    op_device = None
                if op_device and str(op_device) == str(self._device_id):
                    continue
                if name in ("user.create", "user.add"):
                    self._apply_user_add(payload)
                    applied = True
                elif name == "user.update":
                    self._apply_user_update(payload)
                    applied = True
                elif name == "user.delete":
                    self._apply_user_delete(payload)
                    applied = True
                elif name == "routine.assign":
                    self._apply_routine_assign(payload)
                    applied = True
                elif name == "routine.unassign":
                    self._apply_routine_unassign(payload)
                    applied = True
                elif name in ("payment.create", "payment.add", "payment.update"):
                    self._apply_payment_upsert(payload)
                    applied = True
                elif name == "payment.delete":
                    self._apply_payment_delete(payload)
                    applied = True
                elif name in ("attendance.create", "attendance.add", "attendance.update"):
                    self._apply_attendance_upsert(payload)
                    applied = True
                elif name == "attendance.delete":
                    self._apply_attendance_delete(payload)
                    applied = True
                elif name in ("class_attendance.create", "class_attendance.add", "class_attendance.update"):
                    self._apply_class_attendance_upsert(payload)
                    applied = True
                elif name == "class_attendance.delete":
                    self._apply_class_attendance_delete(payload)
                    applied = True
                # ---- Classes ----
                elif name in ("class.create", "class.add", "class.update"):
                    self._apply_class_upsert(payload)
                    applied = True
                elif name == "class.delete":
                    self._apply_class_delete(payload)
                    applied = True
                # ---- Class schedules ----
                elif name in ("class_schedule.create", "class_schedule.add", "class_schedule.update"):
                    self._apply_class_schedule_upsert(payload)
                    applied = True
                elif name == "class_schedule.delete":
                    self._apply_class_schedule_delete(payload)
                    applied = True
                # ---- Class memberships ----
                elif name in ("class_membership.create", "class_membership.add", "class_membership.update"):
                    self._apply_class_membership_upsert(payload)
                    applied = True
                elif name == "class_membership.delete":
                    self._apply_class_membership_delete(payload)
                    applied = True
                # ---- Routines ----
                elif name in ("routine.create", "routine.add", "routine.update"):
                    self._apply_routine_upsert(payload)
                    applied = True
                elif name == "routine.delete":
                    self._apply_routine_delete(payload)
                    applied = True
                # ---- Routine exercises ----
                elif name in ("routine_exercise.create", "routine_exercise.add", "routine_exercise.update"):
                    self._apply_routine_exercise_upsert(payload)
                    applied = True
                elif name == "routine_exercise.delete":
                    self._apply_routine_exercise_delete(payload)
                    applied = True
                # ---- Exercise catalog ----
                elif name in ("exercise.create", "exercise.add", "exercise.update"):
                    self._apply_exercise_upsert(payload)
                    applied = True
                elif name == "exercise.delete":
                    self._apply_exercise_delete(payload)
                    applied = True
                # ---- Professor schedules ----
                elif name in ("professor_schedule.create", "professor_schedule.add", "professor_schedule.update"):
                    self._apply_professor_schedule_upsert(payload)
                    applied = True
                elif name == "professor_schedule.delete":
                    self._apply_professor_schedule_delete(payload)
                    applied = True
                # ---- Professor substitutions ----
                elif name in ("professor_substitution.create", "professor_substitution.add", "professor_substitution.update"):
                    self._apply_professor_substitution_upsert(payload)
                    applied = True
                elif name == "professor_substitution.delete":
                    self._apply_professor_substitution_delete(payload)
                    applied = True
                # ---- Tags ----
                elif name in ("tag.create", "tag.add", "tag.update"):
                    self._apply_tag_upsert(payload)
                    applied = True
                elif name == "tag.delete":
                    self._apply_tag_delete(payload)
                    applied = True
                # ---- User tags ----
                elif name in ("user_tag.create", "user_tag.add", "user_tag.update"):
                    self._apply_user_tag_upsert(payload)
                    applied = True
                elif name == "user_tag.delete":
                    self._apply_user_tag_delete(payload)
                    applied = True
                # ---- User notes ----
                elif name in ("note.create", "note.add", "note.update"):
                    self._apply_note_upsert(payload)
                    applied = True
                elif name == "note.delete":
                    self._apply_note_delete(payload)
                    applied = True
            except Exception as e:
                logging.debug(f"DownloadSyncWorker: error aplicando op {op}: {e}")
        return applied

    def _apply_user_add(self, p: Dict[str, Any]) -> None:
        try:
            # Idempotencia básica: si existe, actualizarlo
            uid = p.get('user_id') or p.get('id')
            dni = p.get('dni')
            nombre = p.get('name') or p.get('nombre')
            telefono = p.get('phone') or p.get('telefono')
            tipo = p.get('membership_type') or p.get('tipo_cuota')
            inicio = p.get('start_date') or p.get('fecha_inicio')
            # Si tenemos DNI, reconciliar por clave natural
            found_id = None
            if dni:
                try:
                    found_id = self._find_user_id_by_dni(str(dni))
                except Exception:
                    found_id = None
            if (uid and hasattr(self.db_manager, 'obtener_usuario') and self.db_manager.obtener_usuario(uid)) or found_id:
                target_id = found_id or uid
                try:
                    if hasattr(self.db_manager, 'update_user'):
                        self.db_manager.update_user(target_id, nombre, telefono, tipo)
                        return
                except Exception:
                    pass
                # Fallback a SQL directa si no hay método expuesto
                try:
                    with self.db_manager.get_connection_context() as conn:
                        cur = conn.cursor()
                        cur.execute(
                            """
                            UPDATE usuarios
                            SET nombre = COALESCE(%s, nombre),
                                telefono = COALESCE(%s, telefono),
                                tipo_cuota = COALESCE(%s, tipo_cuota),
                                updated_at = NOW(),
                                activo = TRUE
                            WHERE id = %s
                            """,
                            (nombre, telefono, tipo, target_id),
                        )
                        conn.commit()
                        return
                except Exception:
                    pass
            # Crear si no existe
            try:
                if hasattr(self.db_manager, 'add_user'):
                    self.db_manager.add_user(nombre, telefono, tipo, inicio)
                    return
            except Exception:
                pass
            # Fallback a inserción por SQL con DNI si está disponible
            try:
                with self.db_manager.get_connection_context() as conn:
                    cur = conn.cursor()
                    if dni:
                        cur.execute(
                            """
                            INSERT INTO usuarios (dni, nombre, telefono, tipo_cuota, activo, rol, updated_at)
                            VALUES (
                                %s,
                                COALESCE(%s, 'Usuario'),
                                COALESCE(%s, ''),
                                COALESCE(%s, 'estandar'),
                                TRUE,
                                'socio',
                                NOW()
                            )
                            ON CONFLICT (dni) DO UPDATE SET
                                nombre = COALESCE(EXCLUDED.nombre, usuarios.nombre, 'Usuario'),
                                telefono = COALESCE(EXCLUDED.telefono, usuarios.telefono, ''),
                                tipo_cuota = COALESCE(EXCLUDED.tipo_cuota, usuarios.tipo_cuota, 'estandar'),
                                activo = TRUE,
                                updated_at = NOW()
                            """,
                            (dni, nombre, telefono, tipo),
                        )
                    else:
                        cur.execute(
                            """
                            INSERT INTO usuarios (nombre, telefono, tipo_cuota, activo, rol, updated_at)
                            VALUES (
                                COALESCE(%s, 'Usuario'),
                                COALESCE(%s, ''),
                                COALESCE(%s, 'estandar'),
                                TRUE,
                                'socio',
                                NOW()
                            )
                            """,
                            (nombre, telefono, tipo),
                        )
                    conn.commit()
            except Exception:
                pass
        except Exception:
            pass

    def _apply_user_update(self, p: Dict[str, Any]) -> None:
        try:
            uid = p.get('user_id') or p.get('id')
            dni = p.get('dni')
            nombre = p.get('name') or p.get('nombre')
            telefono = p.get('phone') or p.get('telefono')
            tipo = p.get('membership_type') or p.get('tipo_cuota')
            active = p.get('active')
            if active is False:
                # Tratar como delete lógica
                self._apply_user_delete(p)
                return
            if dni:
                # Intentar actualizar por DNI
                try:
                    with self.db_manager.get_connection_context() as conn:
                        cur = conn.cursor()
                        cur.execute(
                            """
                            UPDATE usuarios
                            SET nombre = COALESCE(%s, nombre),
                                telefono = COALESCE(%s, telefono),
                                tipo_cuota = COALESCE(%s, tipo_cuota),
                                updated_at = NOW(),
                                activo = TRUE
                            WHERE dni = %s
                            """,
                            (nombre, telefono, tipo, dni),
                        )
                        conn.commit()
                        return
                except Exception:
                    pass
            if uid and hasattr(self.db_manager, 'update_user'):
                self.db_manager.update_user(uid, nombre, telefono, tipo)
                return
            # Fallback por id si existen utilidades SQL
            if uid:
                try:
                    with self.db_manager.get_connection_context() as conn:
                        cur = conn.cursor()
                        cur.execute(
                            """
                            UPDATE usuarios
                            SET nombre = COALESCE(%s, nombre),
                                telefono = COALESCE(%s, telefono),
                                tipo_cuota = COALESCE(%s, tipo_cuota),
                                updated_at = NOW()
                            WHERE id = %s
                            """,
                            (nombre, telefono, tipo, uid),
                        )
                        conn.commit()
                except Exception:
                    pass
        except Exception:
            pass

    # ---- Classes ----
    def _apply_class_upsert(self, p: Dict[str, Any]) -> None:
        try:
            cid = p.get('id')
            nombre = p.get('nombre') or p.get('name')
            descripcion = p.get('descripcion') or p.get('description')
            activa = p.get('activa') if 'activa' in p else p.get('active')
            tipo_clase_id = p.get('tipo_clase_id') or p.get('class_type_id')
            with self.db_manager.get_connection_context() as conn:
                cur = conn.cursor()
                updated = 0
                if cid:
                    cur.execute(
                        """
                        UPDATE clases
                        SET nombre = COALESCE(%s, nombre),
                            descripcion = COALESCE(%s, descripcion),
                            activa = COALESCE(%s, activa),
                            tipo_clase_id = COALESCE(%s, tipo_clase_id),
                            updated_at = NOW()
                        WHERE id = %s
                        """,
                        (nombre, descripcion, activa, tipo_clase_id, cid),
                    )
                    updated = getattr(cur, 'rowcount', 0) or 0
                if updated == 0 and nombre:
                    cur.execute(
                        """
                        UPDATE clases
                        SET descripcion = COALESCE(%s, descripcion),
                            activa = COALESCE(%s, activa),
                            tipo_clase_id = COALESCE(%s, tipo_clase_id),
                            updated_at = NOW()
                        WHERE nombre = %s
                        """,
                        (descripcion, activa, tipo_clase_id, nombre),
                    )
                    updated = getattr(cur, 'rowcount', 0) or 0
                if updated == 0:
                    # Insertar; respetar unicidad por nombre
                    cur.execute(
                        """
                        INSERT INTO clases (nombre, descripcion, activa, tipo_clase_id, updated_at)
                        VALUES (%s, %s, COALESCE(%s, TRUE), %s, NOW())
                        ON CONFLICT (nombre) DO UPDATE SET
                            descripcion = COALESCE(EXCLUDED.descripcion, clases.descripcion),
                            activa = COALESCE(EXCLUDED.activa, clases.activa),
                            tipo_clase_id = COALESCE(EXCLUDED.tipo_clase_id, clases.tipo_clase_id),
                            updated_at = NOW()
                        """,
                        (nombre or 'Clase', descripcion, activa, tipo_clase_id),
                    )
                conn.commit()
        except Exception:
            pass

    def _apply_class_delete(self, p: Dict[str, Any]) -> None:
        try:
            cid = p.get('id')
            nombre = p.get('nombre') or p.get('name')
            with self.db_manager.get_connection_context() as conn:
                cur = conn.cursor()
                if cid:
                    cur.execute("DELETE FROM clases WHERE id = %s", (cid,))
                elif nombre:
                    cur.execute("DELETE FROM clases WHERE nombre = %s", (nombre,))
                conn.commit()
        except Exception:
            pass

    # ---- Class schedules ----
    def _apply_class_schedule_upsert(self, p: Dict[str, Any]) -> None:
        try:
            sid = p.get('id')
            clase_id = p.get('clase_id') or p.get('class_id')
            dia_semana = p.get('dia_semana') or p.get('weekday')
            hora_inicio = p.get('hora_inicio') or p.get('start_time')
            hora_fin = p.get('hora_fin') or p.get('end_time')
            cupo_maximo = p.get('cupo_maximo') or p.get('capacity')
            activo = p.get('activo') if 'activo' in p else p.get('active')
            with self.db_manager.get_connection_context() as conn:
                cur = conn.cursor()
                updated = 0
                if sid:
                    cur.execute(
                        """
                        UPDATE clases_horarios
                        SET clase_id = COALESCE(%s, clase_id),
                            dia_semana = COALESCE(%s, dia_semana),
                            hora_inicio = COALESCE(CAST(%s AS time), hora_inicio),
                            hora_fin = COALESCE(CAST(%s AS time), hora_fin),
                            cupo_maximo = COALESCE(%s, cupo_maximo),
                            activo = COALESCE(%s, activo),
                            updated_at = NOW()
                        WHERE id = %s
                        """,
                        (clase_id, dia_semana, hora_inicio, hora_fin, cupo_maximo, activo, sid),
                    )
                    updated = getattr(cur, 'rowcount', 0) or 0
                if updated == 0 and clase_id and dia_semana is not None and hora_inicio and hora_fin:
                    cur.execute(
                        """
                        UPDATE clases_horarios
                        SET cupo_maximo = COALESCE(%s, cupo_maximo),
                            activo = COALESCE(%s, activo),
                            updated_at = NOW()
                        WHERE clase_id = %s AND dia_semana = %s AND hora_inicio = CAST(%s AS time) AND hora_fin = CAST(%s AS time)
                        """,
                        (cupo_maximo, activo, int(clase_id), dia_semana, hora_inicio, hora_fin),
                    )
                    updated = getattr(cur, 'rowcount', 0) or 0
                if updated == 0:
                    cur.execute(
                        """
                        INSERT INTO clases_horarios (clase_id, dia_semana, hora_inicio, hora_fin, cupo_maximo, activo, updated_at)
                        VALUES (%s, %s, %s, CAST(%s AS time), CAST(%s AS time), COALESCE(%s, 20), COALESCE(%s, TRUE), NOW())
                        """,
                        (clase_id, dia_semana, hora_inicio, hora_fin, cupo_maximo, activo),
                    )
                conn.commit()
        except Exception:
            pass

    def _apply_class_schedule_delete(self, p: Dict[str, Any]) -> None:
        try:
            sid = p.get('id')
            clase_id = p.get('clase_id') or p.get('class_id')
            dia_semana = p.get('dia_semana') or p.get('weekday')
            hora_inicio = p.get('hora_inicio') or p.get('start_time')
            hora_fin = p.get('hora_fin') or p.get('end_time')
            with self.db_manager.get_connection_context() as conn:
                cur = conn.cursor()
                if sid:
                    cur.execute("DELETE FROM clases_horarios WHERE id = %s", (sid,))
                elif clase_id and dia_semana is not None and hora_inicio and hora_fin:
                    cur.execute(
                        """
                        DELETE FROM clases_horarios
                        WHERE clase_id = %s AND dia_semana = %s AND hora_inicio = CAST(%s AS time) AND hora_fin = CAST(%s AS time)
                        """,
                        (int(clase_id), dia_semana, hora_inicio, hora_fin),
                    )
                conn.commit()
        except Exception:
            pass

    # ---- Class memberships ----
    def _apply_class_membership_upsert(self, p: Dict[str, Any]) -> None:
        try:
            dni = p.get('dni')
            uid = p.get('user_id') or p.get('usuario_id')
            clase_horario_id = p.get('clase_horario_id') or p.get('horario_id')
            fecha_inscripcion = p.get('fecha_inscripcion')
            if not uid and dni:
                try:
                    with self.db_manager.get_connection_context() as conn:
                        cur = conn.cursor()
                        cur.execute("SELECT id FROM usuarios WHERE dni = %s", (dni,))
                        r = cur.fetchone()
                        if r:
                            uid = r[0] if not isinstance(r, dict) else (r.get('id') if hasattr(r, 'get') else None)
                except Exception:
                    pass
            if not uid or not clase_horario_id:
                return
            with self.db_manager.get_connection_context() as conn:
                cur = conn.cursor()
                cur.execute(
                    "SELECT id FROM clase_usuarios WHERE clase_horario_id = %s AND usuario_id = %s",
                    (int(clase_horario_id), int(uid)),
                )
                found = cur.fetchone()
                if found:
                    cid = found[0] if not isinstance(found, dict) else (found.get('id') if hasattr(found, 'get') else None)
                    cur.execute(
                        """
                        UPDATE clase_usuarios
                        SET fecha_inscripcion = COALESCE(CAST(%s AS timestamp), fecha_inscripcion),
                            updated_at = NOW()
                        WHERE id = %s
                        """,
                        (fecha_inscripcion, cid),
                    )
                else:
                    cur.execute(
                        """
                        INSERT INTO clase_usuarios (clase_horario_id, usuario_id, fecha_inscripcion, updated_at)
                        VALUES (%s, %s, CAST(%s AS timestamp), NOW())
                        """,
                        (int(clase_horario_id), int(uid), fecha_inscripcion),
                    )
                conn.commit()
        except Exception:
            pass

    def _apply_class_membership_delete(self, p: Dict[str, Any]) -> None:
        try:
            dni = p.get('dni')
            uid = p.get('user_id') or p.get('usuario_id')
            clase_horario_id = p.get('clase_horario_id') or p.get('horario_id')
            if not uid and dni:
                try:
                    with self.db_manager.get_connection_context() as conn:
                        cur = conn.cursor()
                        cur.execute("SELECT id FROM usuarios WHERE dni = %s", (dni,))
                        r = cur.fetchone()
                        if r:
                            uid = r[0] if not isinstance(r, dict) else (r.get('id') if hasattr(r, 'get') else None)
                except Exception:
                    pass
            if not uid or not clase_horario_id:
                return
            with self.db_manager.get_connection_context() as conn:
                cur = conn.cursor()
                cur.execute(
                    "DELETE FROM clase_usuarios WHERE clase_horario_id = %s AND usuario_id = %s",
                    (int(clase_horario_id), int(uid)),
                )
                conn.commit()
        except Exception:
            pass

    def _apply_routine_upsert(self, p: Dict[str, Any]) -> None:
        try:
            rid = p.get('id') or p.get('rutina_id')
            usuario_id = p.get('usuario_id') or p.get('user_id')
            nombre = p.get('nombre_rutina') or p.get('nombre') or p.get('name')
            descripcion = p.get('descripcion') or p.get('description')
            dias_semana = p.get('dias_semana')
            categoria = p.get('categoria')
            activa = p.get('activa')
            with self.db_manager.get_connection_context() as conn:
                cur = conn.cursor()
                updated = False
                if rid:
                    try:
                        cur.execute(
                            """
                            UPDATE rutinas
                            SET usuario_id = COALESCE(%s, usuario_id),
                                nombre_rutina = COALESCE(%s, nombre_rutina),
                                descripcion = COALESCE(%s, descripcion),
                                dias_semana = COALESCE(%s, dias_semana),
                                categoria = COALESCE(%s, categoria),
                                activa = COALESCE(%s, activa),
                                updated_at = NOW()
                            WHERE id = %s
                            """,
                            (usuario_id, nombre, descripcion, dias_semana, categoria, activa, int(rid)),
                        )
                        updated = (cur.rowcount and cur.rowcount > 0)
                    except Exception:
                        updated = False
                if not updated:
                    try:
                        if rid:
                            cur.execute(
                                """
                                INSERT INTO rutinas (id, usuario_id, nombre_rutina, descripcion, dias_semana, categoria, activa, updated_at)
                                VALUES (%s, %s, %s, %s, %s, %s, %s, NOW())
                                ON CONFLICT (id) DO UPDATE SET
                                    usuario_id = EXCLUDED.usuario_id,
                                    nombre_rutina = EXCLUDED.nombre_rutina,
                                    descripcion = EXCLUDED.descripcion,
                                    dias_semana = EXCLUDED.dias_semana,
                                    categoria = EXCLUDED.categoria,
                                    activa = EXCLUDED.activa,
                                    updated_at = NOW()
                                """,
                                (int(rid), usuario_id, nombre, descripcion, dias_semana, categoria, activa),
                            )
                        else:
                            cur.execute(
                                """
                                INSERT INTO rutinas (usuario_id, nombre_rutina, descripcion, dias_semana, categoria, activa, updated_at)
                                VALUES (%s, %s, %s, %s, %s, %s, NOW())
                                """,
                                (usuario_id, nombre, descripcion, dias_semana, categoria, activa),
                            )
                        conn.commit()
                    except Exception:
                        pass
                else:
                    try:
                        conn.commit()
                    except Exception:
                        pass
        except Exception:
            pass

    def _apply_routine_delete(self, p: Dict[str, Any]) -> None:
        try:
            rid = p.get('id') or p.get('rutina_id')
            if not rid:
                return
            with self.db_manager.get_connection_context() as conn:
                cur = conn.cursor()
                try:
                    cur.execute("DELETE FROM rutinas WHERE id = %s", (int(rid),))
                    conn.commit()
                except Exception:
                    pass
        except Exception:
            pass

    def _apply_routine_exercise_upsert(self, p: Dict[str, Any]) -> None:
        try:
            re_id = p.get('id')
            rutina_id = p.get('rutina_id')
            ejercicio_id = p.get('ejercicio_id')
            dia_semana = p.get('dia_semana')
            series = p.get('series')
            repeticiones = p.get('repeticiones')
            orden = p.get('orden')
            with self.db_manager.get_connection_context() as conn:
                cur = conn.cursor()
                updated = False
                if re_id:
                    try:
                        cur.execute(
                            """
                            UPDATE rutinas_ejercicios
                            SET rutina_id = COALESCE(%s, rutina_id),
                                ejercicio_id = COALESCE(%s, ejercicio_id),
                                dia_semana = COALESCE(%s, dia_semana),
                                series = COALESCE(%s, series),
                                repeticiones = COALESCE(%s, repeticiones),
                                orden = COALESCE(%s, orden),
                                updated_at = NOW()
                            WHERE id = %s
                            """,
                            (rutina_id, ejercicio_id, dia_semana, series, repeticiones, orden, int(re_id)),
                        )
                        updated = (cur.rowcount and cur.rowcount > 0)
                    except Exception:
                        updated = False
                if not updated:
                    try:
                        if re_id:
                            cur.execute(
                                """
                                INSERT INTO rutinas_ejercicios (id, rutina_id, ejercicio_id, dia_semana, series, repeticiones, orden, updated_at)
                                VALUES (%s, %s, %s, %s, %s, %s, %s, NOW())
                                ON CONFLICT (id) DO UPDATE SET
                                    rutina_id = EXCLUDED.rutina_id,
                                    ejercicio_id = EXCLUDED.ejercicio_id,
                                    dia_semana = EXCLUDED.dia_semana,
                                    series = EXCLUDED.series,
                                    repeticiones = EXCLUDED.repeticiones,
                                    orden = EXCLUDED.orden,
                                    updated_at = NOW()
                                """,
                                (int(re_id), rutina_id, ejercicio_id, dia_semana, series, repeticiones, orden),
                            )
                        else:
                            cur.execute(
                                """
                                INSERT INTO rutinas_ejercicios (rutina_id, ejercicio_id, dia_semana, series, repeticiones, orden, updated_at)
                                VALUES (%s, %s, %s, %s, %s, %s, NOW())
                                """,
                                (rutina_id, ejercicio_id, dia_semana, series, repeticiones, orden),
                            )
                        conn.commit()
                    except Exception:
                        pass
                else:
                    try:
                        conn.commit()
                    except Exception:
                        pass
        except Exception:
            pass

    def _apply_routine_exercise_delete(self, p: Dict[str, Any]) -> None:
        try:
            re_id = p.get('id')
            if not re_id:
                # Intentar por claves naturales
                rutina_id = p.get('rutina_id')
                ejercicio_id = p.get('ejercicio_id')
                dia_semana = p.get('dia_semana')
                orden = p.get('orden')
                if not (rutina_id and ejercicio_id is not None):
                    return
                with self.db_manager.get_connection_context() as conn:
                    cur = conn.cursor()
                    try:
                        if dia_semana is not None and orden is not None:
                            cur.execute(
                                """
                                DELETE FROM rutinas_ejercicios
                                WHERE rutina_id = %s AND ejercicio_id = %s AND dia_semana = %s AND orden = %s
                                """,
                                (int(rutina_id), int(ejercicio_id), int(dia_semana), int(orden)),
                            )
                        else:
                            cur.execute(
                                """
                                DELETE FROM rutinas_ejercicios
                                WHERE rutina_id = %s AND ejercicio_id = %s
                                """,
                                (int(rutina_id), int(ejercicio_id)),
                            )
                        conn.commit()
                    except Exception:
                        pass
                return
            with self.db_manager.get_connection_context() as conn:
                cur = conn.cursor()
                try:
                    cur.execute("DELETE FROM rutinas_ejercicios WHERE id = %s", (int(re_id),))
                    conn.commit()
                except Exception:
                    pass
        except Exception:
            pass

    def _apply_exercise_upsert(self, p: Dict[str, Any]) -> None:
        try:
            eid = p.get('id')
            nombre = p.get('nombre') or p.get('name')
            grupo = p.get('grupo_muscular') or p.get('grupo')
            descripcion = p.get('descripcion') or p.get('description')
            with self.db_manager.get_connection_context() as conn:
                cur = conn.cursor()
                updated = False
                if eid:
                    try:
                        cur.execute(
                            """
                            UPDATE ejercicios
                            SET nombre = COALESCE(%s, nombre),
                                grupo_muscular = COALESCE(%s, grupo_muscular),
                                descripcion = COALESCE(%s, descripcion),
                                updated_at = NOW()
                            WHERE id = %s
                            """,
                            (nombre, grupo, descripcion, int(eid)),
                        )
                        updated = (cur.rowcount and cur.rowcount > 0)
                    except Exception:
                        updated = False
                if not updated:
                    try:
                        if eid:
                            cur.execute(
                                """
                                INSERT INTO ejercicios (id, nombre, grupo_muscular, descripcion, updated_at)
                                VALUES (%s, %s, %s, %s, NOW())
                                ON CONFLICT (id) DO UPDATE SET
                                    nombre = EXCLUDED.nombre,
                                    grupo_muscular = EXCLUDED.grupo_muscular,
                                    descripcion = EXCLUDED.descripcion,
                                    updated_at = NOW()
                                """,
                                (int(eid), nombre, grupo, descripcion),
                            )
                        else:
                            cur.execute(
                                """
                                INSERT INTO ejercicios (nombre, grupo_muscular, descripcion, updated_at)
                                VALUES (%s, %s, %s, NOW())
                                """,
                                (nombre, grupo, descripcion),
                            )
                        conn.commit()
                    except Exception:
                        pass
                else:
                    try:
                        conn.commit()
                    except Exception:
                        pass
        except Exception:
            pass

    def _apply_exercise_delete(self, p: Dict[str, Any]) -> None:
        try:
            eid = p.get('id')
            nombre = p.get('nombre') or p.get('name')
            with self.db_manager.get_connection_context() as conn:
                cur = conn.cursor()
                try:
                    if eid:
                        cur.execute("DELETE FROM ejercicios WHERE id = %s", (int(eid),))
                    elif nombre:
                        cur.execute("DELETE FROM ejercicios WHERE nombre = %s", (nombre,))
                    conn.commit()
                except Exception:
                    pass
        except Exception:
            pass

    # Estado persistente
    def _resolve_state_path(self) -> str:
        try:
            base_dir = os.path.dirname(os.path.abspath(__file__))
            cfg_dir = os.path.join(base_dir, 'config')
            os.makedirs(cfg_dir, exist_ok=True)
            return os.path.join(cfg_dir, 'sync_state.json')
        except Exception:
            return 'sync_state.json'

    def _load_state(self) -> None:
        try:
            if os.path.exists(self._state_path):
                with open(self._state_path, 'r', encoding='utf-8') as f:
                    data = json.load(f) or {}
                if isinstance(data.get('last_since'), str):
                    self._last_since = data['last_since']
            # Seed inicial para backfill controlado si no hay estado previo
            if not self._last_since:
                seed = os.getenv('SYNC_BOOTSTRAP_SINCE', '').strip()
                if not seed:
                    # Por defecto, backfill completo
                    seed = '1970-01-01T00:00:00Z'
                self._last_since = seed
        except Exception:
            pass

    def _save_state(self) -> None:
        try:
            data = {
                'last_since': self._last_since,
                'saved_at': time.time(),
            }
            with open(self._state_path, 'w', encoding='utf-8') as f:
                json.dump(data, f)
        except Exception:
            pass

    def _apply_user_delete(self, p: Dict[str, Any]) -> None:
        try:
            uid = p.get('user_id') or p.get('id')
            dni = p.get('dni')
            if dni:
                try:
                    with self.db_manager.get_connection_context() as conn:
                        cur = conn.cursor()
                        cur.execute("UPDATE usuarios SET activo = FALSE, updated_at = NOW() WHERE dni = %s", (dni,))
                        conn.commit()
                        return
                except Exception:
                    pass
            if uid and hasattr(self.db_manager, 'delete_user'):
                self.db_manager.delete_user(uid)
                return
            if uid:
                try:
                    with self.db_manager.get_connection_context() as conn:
                        cur = conn.cursor()
                        cur.execute("UPDATE usuarios SET activo = FALSE, updated_at = NOW() WHERE id = %s", (uid,))
                        conn.commit()
                except Exception:
                    pass
        except Exception:
            pass

    def _find_user_id_by_dni(self, dni: str) -> Optional[int]:
        try:
            with self.db_manager.get_connection_context() as conn:
                cur = conn.cursor()
                cur.execute("SELECT id FROM usuarios WHERE dni = %s LIMIT 1", (dni,))
                row = cur.fetchone()
                if not row:
                    return None
                try:
                    return int(row[0])
                except Exception:
                    try:
                        return int(row.get('id'))  # type: ignore
                    except Exception:
                        return None
        except Exception:
            return None

    def _apply_routine_assign(self, p: Dict[str, Any]) -> None:
        try:
            uid = int(p.get('user_id')) if p.get('user_id') is not None else None
            rid = int(p.get('routine_id')) if p.get('routine_id') is not None else None
            if uid and rid and hasattr(self.db_manager, 'assign_routine_to_user'):
                self.db_manager.assign_routine_to_user(uid, rid)
        except Exception:
            pass

    def _apply_routine_unassign(self, p: Dict[str, Any]) -> None:
        try:
            uid = int(p.get('user_id')) if p.get('user_id') is not None else None
            rid = int(p.get('routine_id')) if p.get('routine_id') is not None else None
            if uid and rid and hasattr(self.db_manager, 'unassign_routine_from_user'):
                self.db_manager.unassign_routine_from_user(uid, rid)
        except Exception:
            pass

    # ---- Payments ----
    def _apply_payment_upsert(self, p: Dict[str, Any]) -> None:
        try:
            dni = p.get('dni')
            uid = p.get('user_id')
            mes = p.get('mes') or p.get('month')
            anio = p.get('año') if 'año' in p else p.get('ano') or p.get('year')
            monto = p.get('monto') or p.get('amount')
            fecha_pago = p.get('fecha_pago') or p.get('paid_at')
            # Resolver usuario_id por DNI si es necesario
            if not uid and dni:
                try:
                    uid = self._find_user_id_by_dni(str(dni))
                except Exception:
                    uid = None
            if not uid or mes is None or anio is None:
                return
            with self.db_manager.get_connection_context() as conn:
                cur = conn.cursor()
                # Intentar UPDATE por clave natural (usuario_id, año, mes)
                cur.execute(
                    """
                    UPDATE pagos
                    SET monto = COALESCE(%s, monto),
                        fecha_pago = COALESCE(%s, fecha_pago),
                        updated_at = NOW()
                    WHERE usuario_id = %s AND mes = %s AND (año = %s OR ano = %s)
                    """,
                    (monto, fecha_pago, uid, mes, anio, anio),
                )
                if hasattr(cur, 'rowcount') and cur.rowcount and cur.rowcount > 0:
                    conn.commit()
                    return
                # Si no existe, insertar
                try:
                    cur.execute(
                        """
                        INSERT INTO pagos (usuario_id, mes, año, monto, fecha_pago, updated_at)
                        VALUES (%s, %s, %s, %s, %s, NOW())
                        """,
                        (uid, mes, anio, monto, fecha_pago),
                    )
                except Exception:
                    # Fallback si la columna se llama 'ano'
                    try:
                        cur.execute(
                            """
                            INSERT INTO pagos (usuario_id, mes, ano, monto, fecha_pago, updated_at)
                            VALUES (%s, %s, %s, %s, %s, NOW())
                            """,
                            (uid, mes, anio, monto, fecha_pago),
                        )
                    except Exception:
                        pass
                try:
                    conn.commit()
                except Exception:
                    pass
        except Exception:
            pass

    def _apply_payment_delete(self, p: Dict[str, Any]) -> None:
        try:
            dni = p.get('dni')
            uid = p.get('user_id')
            mes = p.get('mes') or p.get('month')
            anio = p.get('año') if 'año' in p else p.get('ano') or p.get('year')
            if not uid and dni:
                try:
                    uid = self._find_user_id_by_dni(str(dni))
                except Exception:
                    uid = None
            if not uid or mes is None or anio is None:
                return
            with self.db_manager.get_connection_context() as conn:
                cur = conn.cursor()
                try:
                    cur.execute("DELETE FROM pagos WHERE usuario_id = %s AND mes = %s AND (año = %s OR ano = %s)", (uid, mes, anio, anio))
                except Exception:
                    try:
                        cur.execute("DELETE FROM pagos WHERE usuario_id = %s AND mes = %s AND ano = %s", (uid, mes, anio))
                    except Exception:
                        pass
                try:
                    conn.commit()
                except Exception:
                    pass
        except Exception:
            pass

    # ---- Attendance ----
    def _apply_attendance_upsert(self, p: Dict[str, Any]) -> None:
        try:
            dni = p.get('dni')
            uid = p.get('user_id')
            fecha = p.get('fecha') or p.get('date')
            hora = p.get('hora') or p.get('hora_registro') or p.get('time')
            if not uid and dni:
                try:
                    uid = self._find_user_id_by_dni(str(dni))
                except Exception:
                    uid = None
            if not uid or not fecha:
                return
            with self.db_manager.get_connection_context() as conn:
                cur = conn.cursor()
                # Intentar update por clave natural (usuario_id, fecha)
                cur.execute(
                    """
                    UPDATE asistencias
                    SET hora_registro = COALESCE(%s, hora_registro),
                        updated_at = NOW()
                    WHERE usuario_id = %s AND fecha::date = %s::date
                    """,
                    (hora, uid, fecha),
                )
                if hasattr(cur, 'rowcount') and cur.rowcount and cur.rowcount > 0:
                    try:
                        conn.commit()
                    except Exception:
                        pass
                    return
                # Insertar si no existe
                try:
                    cur.execute(
                        """
                        INSERT INTO asistencias (usuario_id, fecha, hora_registro, updated_at)
                        VALUES (%s, %s, %s, NOW())
                        """,
                        (uid, fecha, hora),
                    )
                except Exception:
                    pass
                try:
                    conn.commit()
                except Exception:
                    pass
        except Exception:
            pass

    def _apply_class_attendance_upsert(self, p: Dict[str, Any]) -> None:
        try:
            dni = p.get('dni')
            uid = p.get('user_id') or p.get('usuario_id')
            clase_horario_id = p.get('clase_horario_id') or p.get('horario_id')
            fecha_clase = p.get('fecha_clase') or p.get('fecha')
            estado = p.get('estado_asistencia')
            hora_llegada = p.get('hora_llegada')
            observaciones = p.get('observaciones')
            if not uid and dni:
                try:
                    uid = self._find_user_id_by_dni(str(dni))
                except Exception:
                    uid = None
            if not uid or not clase_horario_id or not fecha_clase:
                return
            with self.db_manager.get_connection_context() as conn:
                cur = conn.cursor()
                # Intentar update por clave natural (clase_horario_id, usuario_id, fecha_clase)
                cur.execute(
                    """
                    UPDATE clase_asistencia_historial
                    SET estado_asistencia = COALESCE(%s, estado_asistencia),
                        hora_llegada = COALESCE(CAST(%s AS time), hora_llegada),
                        observaciones = COALESCE(%s, observaciones),
                        updated_at = NOW()
                    WHERE clase_horario_id = %s AND usuario_id = %s AND fecha_clase = CAST(%s AS date)
                    """,
                    (estado, hora_llegada, observaciones, int(clase_horario_id), int(uid), fecha_clase),
                )
                if hasattr(cur, 'rowcount') and cur.rowcount and cur.rowcount > 0:
                    try:
                        conn.commit()
                    except Exception:
                        pass
                    return
                # Insertar si no existe
                try:
                    cur.execute(
                        """
                        INSERT INTO clase_asistencia_historial
                        (clase_horario_id, usuario_id, fecha_clase, estado_asistencia, hora_llegada, observaciones, updated_at)
                        VALUES (%s, %s, CAST(%s AS date), %s, CAST(%s AS time), %s, NOW())
                        """,
                        (int(clase_horario_id), int(uid), fecha_clase, estado, hora_llegada, observaciones),
                    )
                except Exception:
                    pass
                try:
                    conn.commit()
                except Exception:
                    pass
        except Exception:
            pass

    def _apply_class_attendance_delete(self, p: Dict[str, Any]) -> None:
        try:
            dni = p.get('dni')
            uid = p.get('user_id') or p.get('usuario_id')
            clase_horario_id = p.get('clase_horario_id') or p.get('horario_id')
            fecha_clase = p.get('fecha_clase') or p.get('fecha')
            if not uid and dni:
                try:
                    uid = self._find_user_id_by_dni(str(dni))
                except Exception:
                    uid = None
            if not uid or not clase_horario_id or not fecha_clase:
                return
            with self.db_manager.get_connection_context() as conn:
                cur = conn.cursor()
                cur.execute(
                    """
                    DELETE FROM clase_asistencia_historial
                    WHERE clase_horario_id = %s AND usuario_id = %s AND fecha_clase = CAST(%s AS date)
                    """,
                    (int(clase_horario_id), int(uid), fecha_clase),
                )
                try:
                    conn.commit()
                except Exception:
                    pass
        except Exception:
            pass

    # ---- Professor schedule apply methods ----
    def _apply_professor_schedule_upsert(self, p: Dict[str, Any]) -> None:
        try:
            sid = p.get('id')
            profesor_id = p.get('profesor_id') or p.get('professor_id')
            dia_semana = p.get('dia_semana') or p.get('day_of_week')
            hora_inicio = p.get('hora_inicio') or p.get('start_time')
            hora_fin = p.get('hora_fin') or p.get('end_time')
            disponible = p.get('disponible')
            with self.db_manager.get_connection_context() as conn:
                cur = conn.cursor()
                updated = 0
                try:
                    if sid is not None:
                        cur.execute(
                            """
                            UPDATE horarios_profesores
                            SET profesor_id = COALESCE(%s, profesor_id),
                                dia_semana = COALESCE(%s, dia_semana),
                                hora_inicio = COALESCE(CAST(%s AS time), hora_inicio),
                                hora_fin = COALESCE(CAST(%s AS time), hora_fin),
                                disponible = COALESCE(%s, disponible),
                                updated_at = NOW()
                            WHERE id = %s
                            """,
                            (profesor_id, dia_semana, hora_inicio, hora_fin, disponible, sid),
                        )
                        updated = getattr(cur, 'rowcount', 0) or 0
                except Exception:
                    updated = 0
                if updated == 0 and profesor_id and dia_semana is not None and hora_inicio and hora_fin:
                    cur.execute(
                        """
                        UPDATE horarios_profesores
                        SET disponible = COALESCE(%s, disponible), updated_at = NOW()
                        WHERE profesor_id = %s AND dia_semana = %s AND hora_inicio = CAST(%s AS time) AND hora_fin = CAST(%s AS time)
                        """,
                        (disponible, int(profesor_id), int(dia_semana), hora_inicio, hora_fin),
                    )
                    updated = getattr(cur, 'rowcount', 0) or 0
                if updated == 0:
                    try:
                        cur.execute(
                            """
                            INSERT INTO horarios_profesores (profesor_id, dia_semana, hora_inicio, hora_fin, disponible, updated_at)
                            VALUES (%s, %s, CAST(%s AS time), CAST(%s AS time), COALESCE(%s, TRUE), NOW())
                            """,
                            (int(profesor_id) if profesor_id else None, int(dia_semana) if dia_semana is not None else None, hora_inicio, hora_fin, disponible),
                        )
                    except Exception:
                        pass
                try:
                    conn.commit()
                except Exception:
                    pass
        except Exception:
            pass

    def _apply_professor_schedule_delete(self, p: Dict[str, Any]) -> None:
        try:
            sid = p.get('id')
            profesor_id = p.get('profesor_id') or p.get('professor_id')
            dia_semana = p.get('dia_semana') or p.get('day_of_week')
            hora_inicio = p.get('hora_inicio') or p.get('start_time')
            hora_fin = p.get('hora_fin') or p.get('end_time')
            with self.db_manager.get_connection_context() as conn:
                cur = conn.cursor()
                deleted = 0
                try:
                    if sid is not None:
                        cur.execute("DELETE FROM horarios_profesores WHERE id = %s", (sid,))
                        deleted = getattr(cur, 'rowcount', 0) or 0
                except Exception:
                    deleted = 0
                if deleted == 0 and profesor_id and dia_semana is not None and hora_inicio and hora_fin:
                    cur.execute(
                        "DELETE FROM horarios_profesores WHERE profesor_id = %s AND dia_semana = %s AND hora_inicio = CAST(%s AS time) AND hora_fin = CAST(%s AS time)",
                        (int(profesor_id), int(dia_semana), hora_inicio, hora_fin),
                    )
                try:
                    conn.commit()
                except Exception:
                    pass
        except Exception:
            pass

    # ---- Professor substitution apply methods ----
    def _apply_professor_substitution_upsert(self, p: Dict[str, Any]) -> None:
        try:
            sid = p.get('id')
            asignacion_id = p.get('asignacion_id')
            profesor_suplente_id = p.get('profesor_suplente_id')
            fecha_clase = p.get('fecha_clase') or p.get('fecha')
            motivo = p.get('motivo')
            estado = p.get('estado')
            notas = p.get('notas')
            if not asignacion_id or not fecha_clase:
                return
            with self.db_manager.get_connection_context() as conn:
                cur = conn.cursor()
                updated = 0
                try:
                    if sid is not None:
                        cur.execute(
                            """
                            UPDATE profesor_suplencias
                            SET asignacion_id = COALESCE(%s, asignacion_id),
                                profesor_suplente_id = %s,
                                fecha_clase = COALESCE(CAST(%s AS date), fecha_clase),
                                motivo = COALESCE(%s, motivo),
                                estado = COALESCE(%s, estado),
                                notas = COALESCE(%s, notas),
                                updated_at = NOW()
                            WHERE id = %s
                            """,
                            (asignacion_id, profesor_suplente_id, fecha_clase, motivo, estado, notas, sid),
                        )
                        updated = getattr(cur, 'rowcount', 0) or 0
                except Exception:
                    updated = 0
                if updated == 0:
                    cur.execute(
                        """
                        UPDATE profesor_suplencias
                        SET profesor_suplente_id = %s,
                            motivo = COALESCE(%s, motivo),
                            estado = COALESCE(%s, estado),
                            notas = COALESCE(%s, notas),
                            updated_at = NOW()
                        WHERE asignacion_id = %s AND fecha_clase = CAST(%s AS date)
                        """,
                        (profesor_suplente_id, motivo, estado, notas, asignacion_id, fecha_clase),
                    )
                    updated = getattr(cur, 'rowcount', 0) or 0
                if updated == 0:
                    try:
                        cur.execute(
                            """
                            INSERT INTO profesor_suplencias (asignacion_id, profesor_suplente_id, fecha_clase, motivo, estado, notas, updated_at)
                            VALUES (%s, %s, CAST(%s AS date), %s, COALESCE(%s, 'Pendiente'), %s, NOW())
                            """,
                            (int(asignacion_id), profesor_suplente_id, fecha_clase, motivo, estado, notas),
                        )
                    except Exception:
                        pass
                try:
                    conn.commit()
                except Exception:
                    pass
        except Exception:
            pass

    def _apply_professor_substitution_delete(self, p: Dict[str, Any]) -> None:
        try:
            sid = p.get('id')
            asignacion_id = p.get('asignacion_id')
            fecha_clase = p.get('fecha_clase') or p.get('fecha')
            with self.db_manager.get_connection_context() as conn:
                cur = conn.cursor()
                deleted = 0
                try:
                    if sid is not None:
                        cur.execute("DELETE FROM profesor_suplencias WHERE id = %s", (sid,))
                        deleted = getattr(cur, 'rowcount', 0) or 0
                except Exception:
                    deleted = 0
                if deleted == 0 and asignacion_id and fecha_clase:
                    cur.execute(
                        "DELETE FROM profesor_suplencias WHERE asignacion_id = %s AND fecha_clase = CAST(%s AS date)",
                        (int(asignacion_id), fecha_clase),
                    )
                try:
                    conn.commit()
                except Exception:
                    pass
        except Exception:
            pass

    def _apply_attendance_delete(self, p: Dict[str, Any]) -> None:
        try:
            dni = p.get('dni')
            uid = p.get('user_id')
            fecha = p.get('fecha') or p.get('date')
            if not uid and dni:
                try:
                    uid = self._find_user_id_by_dni(str(dni))
                except Exception:
                    uid = None
            if not uid or not fecha:
                return
            with self.db_manager.get_connection_context() as conn:
                cur = conn.cursor()
                try:
                    cur.execute("DELETE FROM asistencias WHERE usuario_id = %s AND fecha::date = %s::date", (uid, fecha))
                except Exception:
                    pass
                try:
                    conn.commit()
                except Exception:
                    pass
        except Exception:
            pass

    # ---- Tags ----
    def _apply_tag_upsert(self, p: Dict[str, Any]) -> None:
        try:
            tid = p.get('id')
            nombre = p.get('nombre') or p.get('name')
            color = p.get('color')
            descripcion = p.get('descripcion') or p.get('description')
            with self.db_manager.get_connection_context() as conn:
                cur = conn.cursor()
                updated = 0
                if tid is not None:
                    try:
                        cur.execute(
                            """
                            UPDATE etiquetas
                            SET nombre = COALESCE(%s, nombre),
                                color = COALESCE(%s, color),
                                descripcion = COALESCE(%s, descripcion),
                                updated_at = NOW()
                            WHERE id = %s
                            """,
                            (nombre, color, descripcion, int(tid)),
                        )
                        updated = getattr(cur, 'rowcount', 0) or 0
                    except Exception:
                        updated = 0
                if updated == 0:
                    try:
                        if tid is not None:
                            cur.execute(
                                """
                                INSERT INTO etiquetas (id, nombre, color, descripcion, updated_at)
                                VALUES (%s, %s, %s, %s, NOW())
                                ON CONFLICT (id) DO UPDATE SET
                                    nombre = EXCLUDED.nombre,
                                    color = EXCLUDED.color,
                                    descripcion = EXCLUDED.descripcion,
                                    updated_at = NOW()
                                """,
                                (int(tid), nombre, color, descripcion),
                            )
                        else:
                            cur.execute(
                                """
                                INSERT INTO etiquetas (nombre, color, descripcion, updated_at)
                                VALUES (%s, %s, %s, NOW())
                                ON CONFLICT (nombre) DO UPDATE SET
                                    color = EXCLUDED.color,
                                    descripcion = EXCLUDED.descripcion,
                                    updated_at = NOW()
                                """,
                                (nombre, color, descripcion),
                            )
                    except Exception:
                        pass
                try:
                    conn.commit()
                except Exception:
                    pass
        except Exception:
            pass

    def _apply_tag_delete(self, p: Dict[str, Any]) -> None:
        try:
            tid = p.get('id')
            nombre = p.get('nombre') or p.get('name')
            with self.db_manager.get_connection_context() as conn:
                cur = conn.cursor()
                try:
                    if tid is not None:
                        cur.execute("DELETE FROM etiquetas WHERE id = %s", (int(tid),))
                    elif nombre:
                        cur.execute("DELETE FROM etiquetas WHERE nombre = %s", (nombre,))
                except Exception:
                    pass
                try:
                    conn.commit()
                except Exception:
                    pass
        except Exception:
            pass

    # ---- User tags ----
    def _apply_user_tag_upsert(self, p: Dict[str, Any]) -> None:
        try:
            uid = p.get('usuario_id') or p.get('user_id')
            dni = p.get('dni')
            tag_id = p.get('etiqueta_id') or p.get('tag_id')
            if not uid and dni:
                try:
                    uid = self._find_user_id_by_dni(str(dni))
                except Exception:
                    uid = None
            if not uid or not tag_id:
                return
            with self.db_manager.get_connection_context() as conn:
                cur = conn.cursor()
                updated = 0
                try:
                    cur.execute(
                        """
                        UPDATE usuario_etiquetas
                        SET updated_at = NOW()
                        WHERE usuario_id = %s AND etiqueta_id = %s
                        """,
                        (int(uid), int(tag_id)),
                    )
                    updated = getattr(cur, 'rowcount', 0) or 0
                except Exception:
                    updated = 0
                if updated == 0:
                    try:
                        cur.execute(
                            """
                            INSERT INTO usuario_etiquetas (usuario_id, etiqueta_id, updated_at)
                            VALUES (%s, %s, NOW())
                            ON CONFLICT (usuario_id, etiqueta_id) DO UPDATE SET updated_at = NOW()
                            """,
                            (int(uid), int(tag_id)),
                        )
                    except Exception:
                        pass
                try:
                    conn.commit()
                except Exception:
                    pass
        except Exception:
            pass

    def _apply_user_tag_delete(self, p: Dict[str, Any]) -> None:
        try:
            uid = p.get('usuario_id') or p.get('user_id')
            dni = p.get('dni')
            tag_id = p.get('etiqueta_id') or p.get('tag_id')
            if not uid and dni:
                try:
                    uid = self._find_user_id_by_dni(str(dni))
                except Exception:
                    uid = None
            if not uid or not tag_id:
                return
            with self.db_manager.get_connection_context() as conn:
                cur = conn.cursor()
                try:
                    cur.execute(
                        "DELETE FROM usuario_etiquetas WHERE usuario_id = %s AND etiqueta_id = %s",
                        (int(uid), int(tag_id)),
                    )
                except Exception:
                    pass
                try:
                    conn.commit()
                except Exception:
                    pass
        except Exception:
            pass

    # ---- User notes ----
    def _apply_note_upsert(self, p: Dict[str, Any]) -> None:
        try:
            nid = p.get('id')
            uid = p.get('usuario_id') or p.get('user_id')
            dni = p.get('dni')
            categoria = (p.get('categoria') or p.get('category') or 'general')
            titulo = (p.get('titulo') or p.get('title') or '')
            contenido = (p.get('contenido') or p.get('content') or '')
            importancia = (p.get('importancia') or p.get('priority') or 'normal')
            activa = p.get('activa') if 'activa' in p else p.get('active')
            if not uid and dni:
                try:
                    uid = self._find_user_id_by_dni(str(dni))
                except Exception:
                    uid = None
            if not uid:
                return
            with self.db_manager.get_connection_context() as conn:
                cur = conn.cursor()
                updated = 0
                if nid is not None:
                    try:
                        cur.execute(
                            """
                            UPDATE usuario_notas
                            SET usuario_id = COALESCE(%s, usuario_id),
                                categoria = COALESCE(%s, categoria),
                                titulo = COALESCE(%s, titulo),
                                contenido = COALESCE(%s, contenido),
                                importancia = COALESCE(%s, importancia),
                                activa = COALESCE(%s, activa),
                                updated_at = NOW()
                            WHERE id = %s
                            """,
                            (int(uid), categoria, titulo, contenido, importancia, activa, int(nid)),
                        )
                        updated = getattr(cur, 'rowcount', 0) or 0
                    except Exception:
                        updated = 0
                if updated == 0:
                    try:
                        if nid is not None:
                            cur.execute(
                                """
                                INSERT INTO usuario_notas (id, usuario_id, categoria, titulo, contenido, importancia, activa, updated_at)
                                VALUES (%s, %s, %s, %s, %s, %s, COALESCE(%s, TRUE), NOW())
                                ON CONFLICT (id) DO UPDATE SET
                                    usuario_id = EXCLUDED.usuario_id,
                                    categoria = EXCLUDED.categoria,
                                    titulo = EXCLUDED.titulo,
                                    contenido = EXCLUDED.contenido,
                                    importancia = EXCLUDED.importancia,
                                    activa = EXCLUDED.activa,
                                    updated_at = NOW()
                                """,
                                (int(nid), int(uid), categoria, titulo, contenido, importancia, activa),
                            )
                        else:
                            cur.execute(
                                """
                                INSERT INTO usuario_notas (usuario_id, categoria, titulo, contenido, importancia, activa, updated_at)
                                VALUES (%s, %s, %s, %s, %s, COALESCE(%s, TRUE), NOW())
                                """,
                                (int(uid), categoria, titulo, contenido, importancia, activa),
                            )
                    except Exception:
                        pass
                try:
                    conn.commit()
                except Exception:
                    pass
        except Exception:
            pass

    def _apply_note_delete(self, p: Dict[str, Any]) -> None:
        try:
            nid = p.get('id')
            if nid is None:
                return
            with self.db_manager.get_connection_context() as conn:
                cur = conn.cursor()
                try:
                    cur.execute("DELETE FROM usuario_notas WHERE id = %s", (int(nid),))
                except Exception:
                    try:
                        cur.execute("UPDATE usuario_notas SET activa = FALSE, updated_at = NOW() WHERE id = %s", (int(nid),))
                    except Exception:
                        pass
                try:
                    conn.commit()
                except Exception:
                    pass
        except Exception:
            pass