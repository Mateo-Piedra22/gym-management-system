from datetime import datetime, date, timedelta
from typing import List, Optional, Dict, Set, Tuple, Any
import json
import logging
import psycopg2
import psycopg2.extras
from .base import BaseRepository
from ..connection import database_retry
from ...models import Asistencia, Usuario
from ...utils import get_gym_name

class AttendanceRepository(BaseRepository):
    pass

    # --- Methods moved from DatabaseManager ---

    def registrar_asistencia_comun(self, usuario_id: int, fecha: date) -> int:
        """Registra asistencia común diaria de un usuario"""
        with self.get_connection_context() as conn:
            with conn.cursor() as cursor:
                # Validar que el usuario esté activo
                cursor.execute("SELECT activo FROM usuarios WHERE id = %s", (usuario_id,))
                user_row = cursor.fetchone()
                if not user_row or (isinstance(user_row, tuple) and not user_row[0]):
                    raise PermissionError("El usuario está inactivo: no se puede registrar asistencia")
                # Verificar si ya existe una asistencia para este usuario en esta fecha
                cursor.execute("""
                    SELECT id FROM asistencias WHERE usuario_id = %s AND fecha = %s
                """, (usuario_id, fecha))
                
                existing_record = cursor.fetchone()
                if existing_record:
                    # Ya existe una asistencia para este usuario en esta fecha
                    logging.warning(f"Asistencia ya registrada para usuario {usuario_id} en fecha {fecha}")
                    raise ValueError(f"Ya existe una asistencia registrada para este usuario en la fecha {fecha}")
                
                hora_actual = datetime.now()
                cursor.execute("""
                    INSERT INTO asistencias (usuario_id, fecha, hora_registro) 
                    VALUES (%s, %s, %s) RETURNING id
                """, (usuario_id, fecha, hora_actual))
                
                # MANEJO SEGURO DE cursor.fetchone() - verificar None
                result = cursor.fetchone()
                if result is None:
                    logging.error(f"registrar_asistencia: Error al insertar asistencia - resultado None para usuario {usuario_id}")
                    raise Exception("Error al registrar asistencia: no se pudo obtener ID")
                
                asistencia_id = result[0]
                logging.debug(f"registrar_asistencia: Asistencia registrada con ID {asistencia_id} para usuario {usuario_id}")
                conn.commit()
                
                # Registrar en auditoría
                if self.audit_logger:
                    new_values = {
                        'id': asistencia_id,
                        'usuario_id': usuario_id,
                        'fecha': fecha.isoformat(),
                        'hora_registro': hora_actual.isoformat()
                    }
                    self.audit_logger.log_operation('CREATE', 'asistencias', asistencia_id, None, new_values)
                
                
                
                return asistencia_id


    def obtener_ids_asistencia_hoy(self) -> Set[int]:
        """Obtiene los IDs de usuarios que asistieron hoy.

        Robusto ante esquemas donde `fecha` pueda ser TIMESTAMP: usa `fecha::date`.
        """
        hoy = date.today()
        with self.get_connection_context() as conn:
            with conn.cursor() as cursor:
                cursor.execute("SELECT usuario_id FROM asistencias WHERE fecha::date = %s", (hoy,))
                return {row[0] for row in cursor.fetchall()}


    def obtener_asistencias_por_fecha(self, fecha) -> List[Dict]:
        """Obtiene asistencias de una fecha específica con información de usuario"""
        try:
            with self.get_connection_context() as conn:
                cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
                
                # Manejar diferentes tipos de fecha
                if hasattr(fecha, 'date'):
                    fecha_param = fecha.date()
                elif isinstance(fecha, str):
                    from datetime import datetime
                    fecha_param = datetime.strptime(fecha, "%Y-%m-%d").date()
                else:
                    fecha_param = fecha
                
                sql = """
                    SELECT a.id, a.usuario_id, u.nombre as nombre_usuario, u.dni as dni_usuario, 
                           a.fecha, a.hora_registro
                    FROM asistencias a 
                    JOIN usuarios u ON a.usuario_id = u.id 
                    WHERE a.fecha = %s
                    ORDER BY a.hora_registro DESC
                """
                cursor.execute(sql, (fecha_param,))
                
                asistencias = []
                for row in cursor.fetchall():
                    asistencias.append({
                        'id': row['id'],
                        'usuario_id': row['usuario_id'],
                        'nombre_usuario': row['nombre_usuario'],
                        'dni_usuario': row['dni_usuario'],
                        'fecha': row['fecha'],
                        'hora_registro': row['hora_registro']
                    })
                
                return asistencias
                
        except Exception as e:
            logging.error(f"Error obteniendo asistencias por fecha: {e}")
            return []

    # --- MÉTODOS DE CONFIGURACIÓN ---
    
    @database_retry()

    def registrar_asistencia(self, usuario_id: int, fecha: date = None) -> int:
        """Registra una asistencia con validaciones"""
        if fecha is None:
            fecha = date.today()
        
        with self.get_connection_context() as conn:
            cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            # Validar que el usuario esté activo
            cursor.execute("SELECT activo FROM usuarios WHERE id = %s", (usuario_id,))
            user_row = cursor.fetchone()
            if not user_row or not user_row.get('activo'):
                raise PermissionError(f"Usuario {usuario_id} inactivo: no se puede registrar asistencia")
            
            # Verificar si ya existe asistencia para esa fecha
            cursor.execute("""
                SELECT id FROM asistencias 
                WHERE usuario_id = %s AND fecha = %s
            """, (usuario_id, fecha))
            
            if cursor.fetchone():
                raise ValueError(f"Ya existe una asistencia registrada para la fecha {fecha}")
            
            # Insertar la asistencia
            cursor.execute("""
                INSERT INTO asistencias (usuario_id, fecha)
                VALUES (%s, %s)
                RETURNING id
            """, (usuario_id, fecha))
            result = cursor.fetchone()
            if not result:
                raise Exception("No se pudo obtener el id de asistencia")
            try:
                asistencia_id = result['id'] if isinstance(result, dict) else result[0]
            except Exception:
                # Compatibilidad con diferentes tipos de cursor/row
                asistencia_id = result[0]
            conn.commit()
            
            # Registrar en auditoría
            if self.audit_logger:
                new_values = {
                    'id': asistencia_id,
                    'usuario_id': usuario_id,
                    'fecha': fecha.isoformat()
                }
                self.audit_logger.log_operation('CREATE', 'asistencias', asistencia_id, None, new_values)
            
            # Limpiar cache de asistencias
            self.cache.invalidate('asistencias')
            return asistencia_id


    def registrar_asistencias_batch(self, asistencias: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Inserta asistencias en lote, filtrando usuarios inactivos y evitando duplicados por (usuario_id, fecha).

        Cada item debe tener: {'usuario_id': int, 'fecha': date | str | None}.
        Devuelve {'insertados': [ids], 'omitidos': [{'usuario_id':..,'fecha':..,'motivo':..}], 'count': int}.
        """
        if not asistencias:
            return {'insertados': [], 'omitidos': [], 'count': 0}
        # Normalizar filas
        now_dt = datetime.now()
        rows: List[Tuple[int, date, datetime]] = []
        normalized: List[Tuple[int, date]] = []
        omitidos: List[Dict[str, Any]] = []
        for item in asistencias:
            try:
                uid = int(item.get('usuario_id'))
                fraw = item.get('fecha')
                if fraw is None:
                    f = date.today()
                elif isinstance(fraw, date):
                    f = fraw
                elif isinstance(fraw, str):
                    try:
                        f = datetime.fromisoformat(fraw).date()
                    except Exception:
                        f = date.today()
                else:
                    f = date.today()
                rows.append((uid, f, now_dt))
                normalized.append((uid, f))
            except Exception as e:
                omitidos.append({'usuario_id': item.get('usuario_id'), 'fecha': item.get('fecha'), 'motivo': f'payload inválido: {e}'})

        try:
            with self.atomic_transaction(isolation_level="READ COMMITTED") as conn:
                cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
                # Insertar solo asistencias de usuarios activos y que no existen aún para esa fecha
                insert_sql = (
                    """
                    INSERT INTO asistencias (usuario_id, fecha, hora_registro)
                    SELECT v.usuario_id, v.fecha, v.hora_registro
                    FROM (VALUES %s) AS v(usuario_id, fecha, hora_registro)
                    WHERE EXISTS (
                        SELECT 1 FROM usuarios u WHERE u.id = v.usuario_id AND u.activo = TRUE
                    )
                    AND NOT EXISTS (
                        SELECT 1 FROM asistencias a WHERE a.usuario_id = v.usuario_id AND a.fecha = v.fecha
                    )
                    RETURNING id, usuario_id, fecha
                    """
                )
                psycopg2.extras.execute_values(cur, insert_sql, rows, page_size=250)
                inserted = cur.fetchall() or []

                # Determinar omitidos por inactivos o duplicados
                activos_sql = "SELECT id FROM usuarios WHERE id = ANY(%s) AND activo = TRUE"
                uids = list({uid for uid, _f in normalized})
                cur.execute(activos_sql, (uids,))
                activos_set = {int(r['id']) for r in (cur.fetchall() or [])}
                existentes_sql = (
                    "SELECT usuario_id, fecha FROM asistencias WHERE (usuario_id, fecha) IN (SELECT * FROM (VALUES %s) AS t(usuario_id, fecha))"
                )
                psycopg2.extras.execute_values(cur, existentes_sql, normalized, page_size=250)
                existentes = {(int(r['usuario_id']), r['fecha']) for r in (cur.fetchall() or [])}

                for uid, f in normalized:
                    if uid not in activos_set:
                        omitidos.append({'usuario_id': uid, 'fecha': f, 'motivo': 'usuario inactivo'})
                    elif (uid, f) in existentes:
                        omitidos.append({'usuario_id': uid, 'fecha': f, 'motivo': 'duplicado'})

                # Auditoría por cada inserción realizada
                if self.audit_logger:
                    try:
                        for r in inserted:
                            self.audit_logger.log_operation(
                                'CREATE', 'asistencias', int(r['id']), None,
                                {'usuario_id': int(r['usuario_id']), 'fecha': str(r['fecha']), 'accion': 'batch'}
                            )
                    except Exception:
                        pass
                return {
                    'insertados': [int(r['id']) for r in inserted],
                    'omitidos': omitidos,
                    'count': len(inserted)
                }
        except Exception as e:
            logging.error(f"Error registrar_asistencias_batch: {e}")
            raise

    # --- MÉTODOS PARA CHECK-IN INVERSO POR QR ---
    @database_retry()

    def crear_checkin_token(self, usuario_id: int, token: str, expires_minutes: int = 5) -> int:
        """Crea un registro temporal de check-in para un usuario.

        El token NO contiene datos sensibles y expira en `expires_minutes`.
        """
        expires_at = datetime.utcnow() + timedelta(minutes=expires_minutes)
        with self.get_connection_context() as conn:
            with conn.cursor() as cursor:
                # Validar que el usuario esté activo
                cursor.execute("SELECT activo FROM usuarios WHERE id = %s", (usuario_id,))
                row = cursor.fetchone()
                if not row or (isinstance(row, tuple) and not row[0]):
                    raise PermissionError("El usuario está inactivo: no se puede generar token de check-in")

                cursor.execute(
                    """
                    INSERT INTO checkin_pending (usuario_id, token, expires_at)
                    VALUES (%s, %s, %s)
                    RETURNING id
                    """,
                    (usuario_id, token, expires_at)
                )
                result = cursor.fetchone()
                if not result:
                    raise Exception("No se pudo crear el token de check-in")
                token_id = result[0]
                conn.commit()
                return token_id


    def obtener_checkin_por_token(self, token: str) -> Optional[Dict]:
        """Obtiene el registro de check-in temporal por token."""
        with self.get_connection_context() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
                cursor.execute(
                    "SELECT id, usuario_id, token, created_at, expires_at, used FROM checkin_pending WHERE token = %s",
                    (token,)
                )
                row = cursor.fetchone()
                return dict(row) if row else None

    @database_retry()

    def marcar_checkin_usado(self, token: str) -> None:
        """Marca un token de check-in como usado."""
        with self.get_connection_context() as conn:
            with conn.cursor() as cursor:
                cursor.execute("UPDATE checkin_pending SET used = TRUE WHERE token = %s", (token,))
                conn.commit()

    @database_retry()

    def validar_token_y_registrar_asistencia(self, token: str, socio_id: int) -> Tuple[bool, str]:
        """Valida el token y registra asistencia si corresponde.

        Retorna (success, message).
        """
        now = datetime.utcnow()
        with self.get_connection_context() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
                cursor.execute(
                    """
                    SELECT id, usuario_id, expires_at, used
                    FROM checkin_pending
                    WHERE token = %s
                    """,
                    (token,)
                )
                row = cursor.fetchone()
                try:
                    logging.debug(f"Check-in token lookup: token={token}, socio_id={socio_id}, row={dict(row) if row else None}")
                except Exception:
                    pass
                if not row:
                    logging.info(f"Check-in: token no encontrado (token={token})")
                    return (False, "Token inválido")
                if row['used']:
                    logging.info(f"Check-in: token ya utilizado (token={token})")
                    return (False, "Token ya utilizado")
                if row['expires_at'] <= now:
                    logging.info(f"Check-in: token expirado (token={token}, expires_at={row['expires_at']}, now={now})")
                    return (False, "Token expirado")
                if int(row['usuario_id']) != int(socio_id):
                    logging.info(f"Check-in: token usuario_id={row['usuario_id']} no coincide con socio_id={socio_id}")
                    return (False, "El token no corresponde al socio autenticado")

                # Intentar registrar asistencia para hoy
                try:
                    logging.debug(f"Check-in: intentando registrar asistencia (usuario_id={socio_id}, fecha={date.today().isoformat()})")
                    asistencia_id = self.registrar_asistencia(socio_id, date.today())
                except PermissionError as e:
                    # Usuario inactivo u otra restricción de permisos
                    logging.warning(f"Check-in: permiso denegado al registrar asistencia: {e}")
                    return (False, str(e))
                except ValueError as e:
                    # Ya existe asistencia hoy, considerar como éxito
                    logging.info(f"Check-in: asistencia ya registrada para hoy, considerando éxito: {e}")
                    asistencia_id = None
                except Exception:
                    # Capturar stack trace completo para diagnóstico
                    logging.exception("Error registrando asistencia con token")
                    return (False, "No se pudo registrar la asistencia: error interno")

                # Marcar como usado
                cursor.execute("UPDATE checkin_pending SET used = TRUE WHERE token = %s", (token,))
                conn.commit()

                # Auditoría
                if self.audit_logger:
                    self.audit_logger.log_operation(
                        'CREATE', 'asistencias', asistencia_id or -1,
                        None,
                        {
                            'usuario_id': socio_id,
                            'fecha': date.today().isoformat(),
                            'accion': 'checkin_qr'
                        }
                    )

                logging.info(f"Check-in: asistencia registrada correctamente (usuario_id={socio_id}, token={token})")
                return (True, "Asistencia registrada")
            
            return asistencia_id


    def obtener_asistencias_fecha(self, fecha: date) -> List[dict]:
        """Obtiene todas las asistencias de una fecha específica con información del usuario"""
        cache_key = f"asistencias_fecha_{fecha.isoformat()}"
        cached_result = self.cache.get('asistencias', cache_key)
        if cached_result:
            return cached_result
        
        with self.get_connection_context() as conn:
            cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            cursor.execute("""
                SELECT 
                    a.id, a.usuario_id, a.fecha, a.hora_registro, a.hora_entrada,
                    u.nombre AS usuario_nombre, u.dni, u.telefono
                FROM asistencias a
                JOIN usuarios u ON a.usuario_id = u.id
                WHERE a.fecha = %s
                ORDER BY a.id DESC
            """, (fecha,))
            
            asistencias = []
            for row in cursor.fetchall():
                asistencia_dict = dict(row)
                asistencias.append(asistencia_dict)
            
            # Cache por 2 horas
            self.cache.set('asistencias', cache_key, asistencias, ttl_seconds=7200)
            
            return asistencias


    def eliminar_asistencia(self, asistencia_id_or_user_id: int, fecha: date = None):
        """Elimina una asistencia con auditoría y sincronización usando DELETE ... RETURNING en una sola operación"""
        with self.get_connection_context() as conn:
            cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

            deleted_row = None
            if fecha is not None:
                cursor.execute(
                    """
                    DELETE FROM asistencias
                    WHERE usuario_id = %s AND fecha = %s
                    RETURNING 
                        id,
                        usuario_id,
                        fecha,
                        hora_registro,
                        hora_entrada,
                        (SELECT dni FROM usuarios u WHERE u.id = asistencias.usuario_id) AS dni
                    """,
                    (asistencia_id_or_user_id, fecha)
                )
                deleted_row = cursor.fetchone()
            else:
                cursor.execute(
                    """
                    DELETE FROM asistencias
                    WHERE id = %s
                    RETURNING 
                        id,
                        usuario_id,
                        fecha,
                        hora_registro,
                        hora_entrada,
                        (SELECT dni FROM usuarios u WHERE u.id = asistencias.usuario_id) AS dni
                    """,
                    (asistencia_id_or_user_id,)
                )
                deleted_row = cursor.fetchone()

            if not deleted_row:
                return

            conn.commit()

            # Auditoría con valores eliminados
            if self.audit_logger:
                self.audit_logger.log_operation('DELETE', 'asistencias', deleted_row['id'], dict(deleted_row), None)

            # Encolar sincronización upstream (delete)
            # Limpiar cache de asistencias
            self.cache.invalidate('asistencias')


    def obtener_estadisticas_asistencias(self, fecha_inicio: date = None, fecha_fin: date = None) -> dict:
        """Obtiene estadísticas de asistencias en un rango de fechas"""
        if fecha_inicio is None:
            fecha_inicio = date.today().replace(day=1)  # Primer día del mes actual
        if fecha_fin is None:
            fecha_fin = date.today()
        
        cache_key = f"estadisticas_asistencias_{fecha_inicio.isoformat()}_{fecha_fin.isoformat()}"
        cached_result = self.cache.get('reportes', cache_key)
        if cached_result:
            return cached_result
        
        with self.get_connection_context() as conn:
            cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            
            # Estadísticas generales
            cursor.execute("""
                SELECT 
                    COUNT(*) as total_asistencias,
                    COUNT(DISTINCT usuario_id) as usuarios_unicos,
                    COUNT(DISTINCT fecha) as dias_con_asistencias
                FROM asistencias 
                WHERE fecha BETWEEN %s AND %s
            """, (fecha_inicio, fecha_fin))
            
            row = cursor.fetchone()
            estadisticas = {
                'periodo': {
                    'fecha_inicio': fecha_inicio.isoformat(),
                    'fecha_fin': fecha_fin.isoformat()
                },
                'total_asistencias': row[0] or 0,
                'usuarios_unicos': row[1] or 0,
                'dias_con_asistencias': row[2] or 0,
                'promedio_diario': round((row[0] or 0) / max((fecha_fin - fecha_inicio).days + 1, 1), 2)
            }
            
            # Top usuarios por asistencias
            cursor.execute("""
                SELECT u.nombre, u.dni, COUNT(*) as total_asistencias
                FROM asistencias a
                JOIN usuarios u ON a.usuario_id = u.id
                WHERE a.fecha BETWEEN %s AND %s
                GROUP BY u.id, u.nombre, u.dni
                ORDER BY total_asistencias DESC
                LIMIT 10
            """, (fecha_inicio, fecha_fin))
            
            estadisticas['top_usuarios'] = []
            for row in cursor.fetchall():
                estadisticas['top_usuarios'].append({
                    'nombre': row[0],
                    'dni': row[1],
                    'total_asistencias': row[2]
                })
            
            # Cache por 30 minutos
            self.cache.set('reportes', cache_key, estadisticas)
            
            return estadisticas

    # === AGREGACIONES DE ASISTENCIAS PARA GRÁFICOS ===

    def obtener_asistencias_por_dia(self, dias: int = 30):
        """Devuelve lista (fecha, conteo) para últimos 'dias' con timeouts y caché."""
        cache_key = ('por_dia', int(dias))
        # Caché memoria
        try:
            cached = self.cache.get('asistencias', cache_key)
            if cached is not None:
                return cached
        except Exception:
            pass
        
        try:
            with self.get_connection_context() as conn:
                with conn.cursor() as cursor:
                    try:
                        self._apply_readonly_timeouts(cursor, lock_ms=800, statement_ms=1500, idle_s=2)
                    except Exception:
                        pass
                    cursor.execute(
                        """
                        SELECT fecha::date AS d, COUNT(*)
                        FROM asistencias
                        WHERE fecha >= CURRENT_DATE - INTERVAL %s
                        GROUP BY 1 ORDER BY 1
                        """,
                        (f"{int(dias)} days",)
                    )
                    rows = [(row[0], row[1]) for row in cursor.fetchall()]
                    try:
                        self.cache.set('asistencias', cache_key, rows, ttl_seconds=1800)
                    except Exception:
                        pass
                    return rows
        except Exception as e:
            logging.error(f"Error en obtener_asistencias_por_dia: {e}")
            return []


    def obtener_asistencias_por_rango_diario(self, fecha_inicio: str, fecha_fin: str):
        """Devuelve lista (fecha, conteo) para rango especificado con timeouts y caché."""
        cache_key = ('por_rango_diario', str(fecha_inicio), str(fecha_fin))
        try:
            cached = self.cache.get('asistencias', cache_key)
            if cached is not None:
                return cached
        except Exception:
            pass
        
        try:
            with self.get_connection_context() as conn:
                with conn.cursor() as cursor:
                    try:
                        self._apply_readonly_timeouts(cursor, lock_ms=800, statement_ms=2000, idle_s=2)
                    except Exception:
                        pass
                    cursor.execute(
                        """
                        SELECT fecha::date AS d, COUNT(*)
                        FROM asistencias
                        WHERE fecha BETWEEN %s AND %s
                        GROUP BY 1 ORDER BY 1
                        """,
                        (fecha_inicio, fecha_fin)
                    )
                    rows = [(row[0], row[1]) for row in cursor.fetchall()]
                    try:
                        self.cache.set('asistencias', cache_key, rows, ttl_seconds=1800)
                    except Exception:
                        pass
                    return rows
        except Exception as e:
            logging.error(f"Error en obtener_asistencias_por_rango_diario: {e}")
            return []


    def obtener_asistencias_por_hora(self, dias: int = 30):
        """Devuelve lista (hora, conteo) para últimos 'dias' con timeouts y caché."""
        cache_key = ('por_hora', int(dias))
        try:
            cached = self.cache.get('asistencias', cache_key)
            if cached is not None:
                return cached
        except Exception:
            pass
        
        try:
            with self.get_connection_context() as conn:
                with conn.cursor() as cursor:
                    try:
                        self._apply_readonly_timeouts(cursor, lock_ms=800, statement_ms=1500, idle_s=2)
                    except Exception:
                        pass
                    cursor.execute(
                        """
                        SELECT EXTRACT(HOUR FROM hora_registro) AS h, COUNT(*)
                        FROM asistencias
                        WHERE fecha >= CURRENT_DATE - INTERVAL %s AND hora_registro IS NOT NULL
                        GROUP BY 1 ORDER BY 1
                        """,
                        (f"{int(dias)} days",)
                    )
                    rows = [(int(row[0]), row[1]) for row in cursor.fetchall()]
                    try:
                        self.cache.set('asistencias', cache_key, rows, ttl_seconds=1800)
                    except Exception:
                        pass
                    return rows
        except Exception as e:
            logging.error(f"Error en obtener_asistencias_por_hora: {e}")
            return []


    def obtener_asistencias_por_hora_rango(self, fecha_inicio: str, fecha_fin: str):
        """Devuelve lista (hora, conteo) para rango especificado con timeouts y caché."""
        cache_key = ('por_hora_rango', str(fecha_inicio), str(fecha_fin))
        try:
            cached = self.cache.get('asistencias', cache_key)
            if cached is not None:
                return cached
        except Exception:
            pass
        
        try:
            with self.get_connection_context() as conn:
                with conn.cursor() as cursor:
                    try:
                        self._apply_readonly_timeouts(cursor, lock_ms=800, statement_ms=2000, idle_s=2)
                    except Exception:
                        pass
                    cursor.execute(
                        """
                        SELECT EXTRACT(HOUR FROM hora_registro) AS h, COUNT(*)
                        FROM asistencias
                        WHERE fecha BETWEEN %s AND %s AND hora_registro IS NOT NULL
                        GROUP BY 1 ORDER BY 1
                        """,
                        (fecha_inicio, fecha_fin)
                    )
                    rows = [(int(row[0]), row[1]) for row in cursor.fetchall()]
                    try:
                        self.cache.set('asistencias', cache_key, rows, ttl_seconds=1800)
                    except Exception:
                        pass
                    return rows
        except Exception as e:
            logging.error(f"Error en obtener_asistencias_por_hora_rango: {e}")
            return []

    # --- MÉTODOS PARA GRUPOS DE EJERCICIOS ---

    def obtener_asistencias_por_dia_semana(self, dias: int = 30) -> Dict[str, int]:
        """Obtiene estadísticas de asistencias por día de la semana con caché y timeouts."""
        cache_key = ('por_dia_semana', int(dias))

        # 1) Caché en memoria
        try:
            cached = self.cache.get('asistencias', cache_key)
            if cached is not None:
                return cached
        except Exception:
            pass

        
        try:
            with self.get_connection_context() as conn:
                with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
                    try:
                        self._apply_readonly_timeouts(cursor, lock_ms=800, statement_ms=1500, idle_s=2)
                    except Exception:
                        pass

                    fecha_limite = (datetime.now() - timedelta(days=dias)).date()

                    # Mapeo de números de día a nombres en español
                    dias_semana = {
                        '0': 'Domingo', '1': 'Lunes', '2': 'Martes', '3': 'Miércoles',
                        '4': 'Jueves', '5': 'Viernes', '6': 'Sábado'
                    }

                    asistencias = {dia: 0 for dia in dias_semana.values()}

                    cursor.execute(
                        """
                        SELECT EXTRACT(DOW FROM fecha) AS dia, COUNT(*) AS conteo
                        FROM asistencias
                        WHERE fecha >= %s
                        GROUP BY EXTRACT(DOW FROM fecha)
                        ORDER BY 1
                        """,
                        (fecha_limite,)
                    )

                    for row in cursor.fetchall():
                        dia_num = str(int(row['dia']))
                        if dia_num in dias_semana:
                            asistencias[dias_semana[dia_num]] = row['conteo']

                    # Actualizar cachés
                    try:
                        self.cache.set('asistencias', cache_key, asistencias, ttl_seconds=1800)
                    except Exception:
                        pass
                    return asistencias
        except Exception as e:
            logging.error(f"Error en obtener_asistencias_por_dia_semana: {e}")
            # En error, devolver estructura vacía consistente
            dias_semana = {
                '0': 'Domingo', '1': 'Lunes', '2': 'Martes', '3': 'Miércoles',
                '4': 'Jueves', '5': 'Viernes', '6': 'Sábado'
            }
            return {dia: 0 for dia in dias_semana.values()}
            
            # Ordenar días de la semana
            orden_dias = ['Lunes', 'Martes', 'Miércoles', 'Jueves', 'Viernes', 'Sábado', 'Domingo']
            return {dia: asistencias[dia] for dia in orden_dias}
    

    def obtener_asistencias_por_fecha_limite(self, fecha_limite):
        """Obtiene asistencias registradas después de una fecha límite"""
        try:
            with self.get_connection_context() as conn:
                cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
                
                # Manejar diferentes tipos de fecha
                if hasattr(fecha_limite, 'date'):
                    fecha_param = fecha_limite.date()
                elif isinstance(fecha_limite, str):
                    from datetime import datetime
                    fecha_param = datetime.strptime(fecha_limite, "%Y-%m-%d").date()
                else:
                    fecha_param = fecha_limite
                
                cursor.execute("""
                    SELECT a.id, a.usuario_id, u.nombre, u.dni, a.fecha, a.hora_registro
                    FROM asistencias a
                    JOIN usuarios u ON a.usuario_id = u.id
                    WHERE a.fecha >= %s::date
                    ORDER BY a.fecha DESC, a.hora_registro DESC
                """, (fecha_param,))
                
                asistencias = []
                for row in cursor.fetchall():
                    # row es RealDictRow, acceder por claves para evitar errores de índice
                    asistencias.append({
                        'id': row.get('id'),
                        'usuario_id': row.get('usuario_id'),
                        'nombre_usuario': row.get('nombre'),
                        'dni_usuario': row.get('dni'),
                        'fecha': row.get('fecha'),
                        'hora_registro': row.get('hora_registro')
                    })
                
                return asistencias
            
        except Exception as e:
            logging.error(f"Error obteniendo asistencias por fecha límite: {e}")
            return []
    

    def registrar_asistencia_clase_completa(self, clase_horario_id: int, usuario_id: int, fecha_clase: str,
                           estado_asistencia: str = 'presente', hora_llegada: str = None,
                           observaciones: str = None, registrado_por: int = None) -> int:
        """Registra la asistencia de un usuario a una clase específica PostgreSQL."""
        with self.get_connection_context() as conn:
            cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            # Validar que el usuario esté activo antes de registrar asistencia de clase
            cursor.execute("SELECT activo FROM usuarios WHERE id = %s", (usuario_id,))
            user_row = cursor.fetchone()
            if not user_row or not user_row.get('activo'):
                raise PermissionError(f"Usuario {usuario_id} inactivo: no se puede registrar asistencia a clase")
            sql = """
            INSERT INTO clase_asistencia_historial 
            (clase_horario_id, usuario_id, fecha_clase, estado_asistencia, 
             hora_llegada, observaciones, registrado_por)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (clase_horario_id, usuario_id, fecha_clase) 
            DO UPDATE SET 
                estado_asistencia = EXCLUDED.estado_asistencia,
                hora_llegada = EXCLUDED.hora_llegada,
                observaciones = EXCLUDED.observaciones,
                registrado_por = EXCLUDED.registrado_por
            RETURNING id
            """
            cursor.execute(sql, (clase_horario_id, usuario_id, fecha_clase, 
                               estado_asistencia, hora_llegada, observaciones, registrado_por))
            result = cursor.fetchone()
            conn.commit()
            return result['id'] if result else None
    

    def obtener_asistencia_clase_completa(self, clase_horario_id: int, fecha_clase: str) -> List[Dict]:
        """Obtiene la asistencia de todos los usuarios en una clase específica PostgreSQL."""
        with self.get_connection_context() as conn:
            cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            sql = """
            SELECT 
                ah.*,
                u.nombre as usuario_nombre,
                u.telefono
            FROM clase_asistencia_historial ah
            JOIN usuarios u ON ah.usuario_id = u.id
            WHERE ah.clase_horario_id = %s AND ah.fecha_clase = %s
            ORDER BY u.nombre
            """
            cursor.execute(sql, (clase_horario_id, fecha_clase))
            return [dict(zip([desc[0] for desc in cursor.description], row)) for row in cursor.fetchall()]
    

    def _migrar_tabla_clase_asistencia_historial(self, cursor):
        """Migra la tabla para el historial de asistencia a clases PostgreSQL"""
        try:
            # Crear tabla clase_asistencia_historial
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS clase_asistencia_historial (
                    id SERIAL PRIMARY KEY,
                    clase_horario_id INTEGER NOT NULL,
                    usuario_id INTEGER NOT NULL,
                    fecha_clase DATE NOT NULL,
                    estado_asistencia VARCHAR(20) DEFAULT 'presente',
                    hora_llegada TIME,
                    observaciones TEXT,
                    registrado_por INTEGER,
                    fecha_registro TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (clase_horario_id) REFERENCES clases_horarios(id) ON DELETE CASCADE,
                    FOREIGN KEY (usuario_id) REFERENCES usuarios(id) ON DELETE CASCADE,
                    FOREIGN KEY (registrado_por) REFERENCES usuarios(id) ON DELETE SET NULL,
                    UNIQUE(clase_horario_id, usuario_id, fecha_clase)
                )
            """)
            
            # Crear índices para optimizar consultas
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_clase_asistencia_historial_clase_horario_id ON clase_asistencia_historial(clase_horario_id)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_clase_asistencia_historial_usuario_id ON clase_asistencia_historial(usuario_id)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_clase_asistencia_historial_fecha ON clase_asistencia_historial(fecha_clase)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_clase_asistencia_historial_estado ON clase_asistencia_historial(estado_asistencia)")
            
            logging.info("Tabla clase_asistencia_historial creada exitosamente")
            
        except Exception as e:
            logging.error(f"Error en migración de tabla clase_asistencia_historial: {e}")
            # En caso de error, no interrumpir la inicialización


    def calcular_ingresos_totales(self, fecha_inicio=None, fecha_fin=None) -> float:
        """Calcula los ingresos totales en un rango de fechas."""
        try:
            with self.get_connection_context() as conn:
                cursor = conn.cursor()
                
                if fecha_inicio and fecha_fin:
                    sql = "SELECT COALESCE(SUM(monto), 0) FROM pagos WHERE fecha_pago BETWEEN %s AND %s"
                    cursor.execute(sql, (fecha_inicio, fecha_fin))
                else:
                    sql = "SELECT COALESCE(SUM(monto), 0) FROM pagos"
                    cursor.execute(sql)
                    
                resultado = cursor.fetchone()
                return float(resultado[0]) if resultado else 0.0
                
        except Exception as e:
            logging.error(f"Error calculando ingresos totales: {str(e)}")
            return 0.0
    

    def obtener_tendencia_ingresos(self, fecha_inicio=None, fecha_fin=None, periodo='6_meses') -> list:
        """Obtiene la tendencia de ingresos mensual usando fecha_pago o mes/año si falta la fecha.

        Devuelve lista de dicts con claves: mes ('YYYY-MM') y total_ingresos.
        """
        try:
            with self.get_connection_context() as conn:
                cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

                # Expresión de mes contable con soporte mixto y casteos seguros
                month_expr = "date_trunc('month', CASE WHEN fecha_pago IS NOT NULL THEN fecha_pago WHEN año ~ '^[0-9]+' AND mes ~ '^[0-9]+' THEN make_date(año::int, mes::int, 1) ELSE NULL END)"

                if fecha_inicio and fecha_fin:
                    sql = f"""
                        SELECT to_char({month_expr}, 'YYYY-MM') AS mes,
                               COALESCE(SUM(monto), 0) AS total_ingresos
                        FROM pagos
                        WHERE {month_expr} BETWEEN date_trunc('month', %s::date) AND date_trunc('month', %s::date)
                        GROUP BY 1
                        ORDER BY 1
                    """
                    cursor.execute(sql, (fecha_inicio, fecha_fin))
                else:
                    if periodo == '6_meses':
                        meses = 6
                    elif periodo == '12_meses':
                        meses = 12
                    elif periodo == '3_meses':
                        meses = 3
                    else:
                        meses = 6
                    sql = f"""
                        WITH meses AS (
                            SELECT date_trunc('month', CURRENT_DATE) - (s * interval '1 month') AS periodo
                            FROM generate_series(0, %s) s
                        ), pagos_mes AS (
                            SELECT date_trunc('month', CASE WHEN fecha_pago IS NOT NULL THEN fecha_pago WHEN año ~ '^[0-9]+' AND mes ~ '^[0-9]+' THEN make_date(año::int, mes::int, 1) ELSE NULL END) AS periodo,
                                   COALESCE(SUM(monto), 0) AS total_ingresos
                            FROM pagos
                            WHERE (
                                (fecha_pago IS NOT NULL AND fecha_pago >= date_trunc('month', CURRENT_DATE) - %s * interval '1 month')
                                OR (
                                    fecha_pago IS NULL AND año ~ '^[0-9]+' AND mes ~ '^[0-9]+' AND make_date(año::int, mes::int, 1) >= date_trunc('month', CURRENT_DATE) - %s * interval '1 month'
                                )
                            )
                            GROUP BY 1
                        )
                        SELECT to_char(m.periodo, 'YYYY-MM') AS mes,
                               COALESCE(pm.total_ingresos, 0) AS total_ingresos
                        FROM meses m
                        LEFT JOIN pagos_mes pm ON pm.periodo = m.periodo
                        ORDER BY mes
                    """
                    cursor.execute(sql, (meses-1, meses-1))

                rows = cursor.fetchall()
                return [dict(row) for row in rows]

        except Exception as e:
            logging.error(f"Error obteniendo tendencia de ingresos: {str(e)}")
            return []
    
    # ===== MÉTODOS PARA CONFIGURATION MANAGEMENT =====
    

    def _get_asistencias_today(self):
        """Obtiene asistencias del día actual"""
        self.progress.emit(20)
        
        with self.db_manager.readonly_session() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute("""
                    SELECT a.id, a.usuario_id, a.fecha, a.hora_registro, u.nombre
                    FROM asistencias a
                    JOIN usuarios u ON u.id = a.usuario_id
                    WHERE a.fecha = CURRENT_DATE
                    ORDER BY a.hora_registro DESC
                """)
                self.progress.emit(80)
                return cur.fetchall()
    

    def _get_reporte_asistencias(self):
        """Obtiene reporte de asistencias por día"""
        dias = self.params.get('dias', 30)
        fecha_inicio = date.today() - timedelta(days=dias)
        
        with self.db_manager.readonly_session() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute("""
                    SELECT 
                        fecha,
                        COUNT(*) as cantidad_asistencias
                    FROM asistencias
                    WHERE fecha >= %s
                    GROUP BY fecha
                    ORDER BY fecha DESC
                """, (fecha_inicio,))
                return cur.fetchall()


class BulkDatabaseWorker(QThread):
    """Worker para operaciones masivas de base de datos"""
    
    started = pyqtSignal(str)
    finished = pyqtSignal(object)
    error = pyqtSignal(str)
    progress = pyqtSignal(int)
    

    def _bulk_insert_asistencias(self):
        """Inserta múltiples asistencias"""
        total = len(self.data)
        inserted = 0
        
        with self.db_manager.connection() as conn:
            with conn.cursor() as cur:
                for i, asistencia in enumerate(self.data):
                    if not self._is_running:
                        break
                    
                    try:
                        cur.execute("""
                            INSERT INTO asistencias (usuario_id, fecha, hora_registro)
                            VALUES (%s, %s, %s)
                        """, (
                            asistencia.get('usuario_id'),
                            asistencia.get('fecha'),
                            asistencia.get('hora_registro')
                        ))
                        inserted += 1
                        
                        if i % 20 == 0:
                            progress = int((i / total) * 100)
                            self.progress.emit(progress)
                            
                    except Exception as e:
                        logging.warning(f"Error insertando asistencia: {e}")
                        continue
                
                conn.commit()
        
        self.progress.emit(100)
        return {'inserted': inserted, 'total': total}


class DatabaseOperationManager(QObject):
    """Gestor de operaciones de base de datos asíncronas"""
    
    operation_started = pyqtSignal(str)
    operation_finished = pyqtSignal(str, object)
    operation_error = pyqtSignal(str, str)
    operation_progress = pyqtSignal(str, int)
    
