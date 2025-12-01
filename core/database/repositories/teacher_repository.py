from datetime import datetime, date, timedelta
from typing import List, Optional, Dict, Set, Tuple, Any
import json
import logging
import psycopg2
import psycopg2.extras
from .base import BaseRepository
from ..connection import database_retry
from ...models import Usuario, ClaseHorario
from ...utils import get_gym_name

class TeacherRepository(BaseRepository):
    pass

    # --- Methods moved from DatabaseManager ---

    def actualizar_profesor_sesion(
        self,
        sesion_id: int,
        *,
        fecha: Optional[str] = None,
        hora_inicio: Optional[str] = None,
        hora_fin: Optional[str] = None,
        tipo_actividad: Optional[str] = None,
        minutos_totales: Optional[int] = None,
        timeout_ms: int = 1500,
    ) -> Dict[str, Any]:
        """
        Actualiza una sesión de trabajo del profesor en 'profesor_horas_trabajadas'.

        Campos editables:
        - fecha (YYYY-MM-DD)
        - hora_inicio (HH:MM o timestamp)
        - hora_fin (HH:MM o timestamp)
        - tipo_actividad

        Normaliza hora_inicio y hora_fin cuando vienen como "HH:MM" combinándolos con la fecha.
        Maneja sesiones que cruzan medianoche (fin < inicio).

        Recalcula automáticamente minutos/horas vía triggers de la tabla.
        Retorna { success: bool, updated: dict | None, error?: str }
        """
        try:
            if not sesion_id or sesion_id <= 0:
                return {"success": False, "error": "ID de sesión inválido"}

            from datetime import datetime, date, time, timedelta
            import re

            with self.get_connection_context() as conn:
                cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

                # Timeouts razonables
                try:
                    cursor.execute("SET LOCAL statement_timeout = %s", (f"{int(timeout_ms)}ms",))
                    cursor.execute("SET LOCAL lock_timeout = '1000ms'")
                    cursor.execute("SET LOCAL idle_in_transaction_session_timeout = '2s'")
                except Exception:
                    self.logger.debug("Error configurando timeouts locales", exc_info=True)

                # Leer valores actuales para tener fecha base
                cursor.execute(
                    """
                    SELECT fecha::date AS fecha, hora_inicio, hora_fin, profesor_id
                    FROM profesor_horas_trabajadas
                    WHERE id = %s
                    """,
                    (sesion_id,)
                )
                current = cursor.fetchone()
                if not current:
                    return {"success": False, "error": "Sesión no encontrada"}

                # Fecha base: la nueva (si viene) o la actual
                def _parse_date_safe(ds: Optional[str], fallback: date) -> date:
                    if not ds:
                        return fallback
                    try:
                        return datetime.strptime(ds, "%Y-%m-%d").date()
                    except Exception:
                        try:
                            return datetime.fromisoformat(ds).date()
                        except Exception:
                            return fallback

                base_date: date = _parse_date_safe(fecha, current.get("fecha"))

                def _parse_time_to_ts(val: Optional[str], d: date) -> Optional[datetime]:
                    if not val:
                        return None
                    s = str(val).strip()
                    m = re.match(r"^(\d{1,2}):(\d{2})(?::(\d{2}))?$", s)
                    if m:
                        hh = int(m.group(1)); mm = int(m.group(2)); ss = int(m.group(3)) if m.group(3) else 0
                        return datetime.combine(d, time(hh, mm, ss))
                    # Intentar parseos completos
                    try:
                        return datetime.fromisoformat(s.replace('Z', '+00:00'))
                    except Exception:
                        self.logger.debug(f"Error parseando hora completa {s}", exc_info=True)
                    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M"):
                        try:
                            return datetime.strptime(s, fmt)
                        except Exception:
                            continue
                    return None

                ts_inicio_new = _parse_time_to_ts(hora_inicio, base_date)
                ts_fin_new = _parse_time_to_ts(hora_fin, base_date)

                sets = []
                params: list = []

                if fecha is not None:
                    sets.append("fecha = %s")
                    params.append(base_date)

                if ts_inicio_new is not None:
                    sets.append("hora_inicio = %s")
                    params.append(ts_inicio_new)

                if ts_fin_new is not None:
                    # Ajuste para sesiones que cruzan medianoche
                    try:
                        ref_inicio = ts_inicio_new if ts_inicio_new is not None else current.get("hora_inicio")
                        if isinstance(ref_inicio, datetime) and ts_fin_new < ref_inicio:
                            ts_fin_new = ts_fin_new + timedelta(days=1)
                    except Exception:
                        pass
                    sets.append("hora_fin = %s")
                    params.append(ts_fin_new)

                # Normalizar solicitud de tipo_actividad y decidir si se calcula automáticamente
                tipo_norm = None
                if tipo_actividad is not None:
                    try:
                        tipo_norm = str(tipo_actividad).strip()
                    except Exception:
                        tipo_norm = None

                # Calcular/override minutos_totales si corresponde
                
                # Calcular/override minutos_totales si corresponde
                minutos_set_val = None
                if minutos_totales is not None:
                    try:
                        minutos_set_val = int(minutos_totales)
                        if minutos_set_val < 0:
                            minutos_set_val = 0
                    except Exception:
                        minutos_set_val = None
                elif (hora_inicio is not None) or (hora_fin is not None):
                    # Recalcular si vienen cambios de inicio/fin
                    def _to_dt(x, d: date) -> Optional[datetime]:
                        if x is None:
                            return None
                        if isinstance(x, datetime):
                            return x
                        if isinstance(x, time):
                            try:
                                return datetime.combine(d, x)
                            except Exception:
                                return None
                        try:
                            return datetime.fromisoformat(str(x).replace('Z', '+00:00'))
                        except Exception:
                            return None
                    start_dt = _to_dt(ts_inicio_new if ts_inicio_new is not None else current.get("hora_inicio"), base_date)
                    end_dt = _to_dt(ts_fin_new if ts_fin_new is not None else current.get("hora_fin"), base_date)
                    if start_dt and end_dt:
                        if end_dt < start_dt:
                            end_dt = end_dt + timedelta(days=1)
                        try:
                            minutos_set_val = max(0, int((end_dt - start_dt).total_seconds() // 60))
                        except Exception:
                            minutos_set_val = None

                if minutos_set_val is not None:
                    sets.append("minutos_totales = %s")
                    params.append(minutos_set_val)
                    sets.append("horas_totales = %s")
                    try:
                        params.append(round(minutos_set_val / 60.0, 2))
                    except Exception:
                        params.append(float(minutos_set_val) / 60.0)

                # Determinar si corresponde clasificar automáticamente la sesión según horarios
                should_auto_tipo = False
                try:
                    should_auto_tipo = (
                        (tipo_norm is None) or (tipo_norm.lower() in ("auto", "automatico", "automático", ""))
                    ) and (
                        (fecha is not None) or (hora_inicio is not None) or (hora_fin is not None)
                    )
                except Exception:
                    should_auto_tipo = False

                if should_auto_tipo:
                    try:
                        profesor_id = current.get("profesor_id")
                        # Resolver fecha/hora efectivas a usar para la clasificación
                        def _to_dt(x, d: date) -> Optional[datetime]:
                            if x is None:
                                return None
                            if isinstance(x, datetime):
                                return x
                            if isinstance(x, time):
                                try:
                                    return datetime.combine(d, x)
                                except Exception:
                                    return None
                            try:
                                return datetime.fromisoformat(str(x).replace('Z', '+00:00'))
                            except Exception:
                                return None
                        start_dt = _to_dt(ts_inicio_new if ts_inicio_new is not None else current.get("hora_inicio"), base_date)
                        end_dt = _to_dt(ts_fin_new if ts_fin_new is not None else current.get("hora_fin"), base_date)
                        if start_dt and end_dt and end_dt < start_dt:
                            end_dt = end_dt + timedelta(days=1)

                        # Obtener día de la semana de la fecha base
                        try:
                            dia_map = {0: 'Lunes', 1: 'Martes', 2: 'Miércoles', 3: 'Jueves', 4: 'Viernes', 5: 'Sábado', 6: 'Domingo'}
                            dia_nombre = dia_map.get(base_date.weekday())
                        except Exception:
                            dia_nombre = None

                        en_horario = False
                        if profesor_id and dia_nombre and start_dt and end_dt:
                            try:
                                # Obtener bloques del día y fusionarlos para tratar cobertura continua
                                with self.get_connection_context() as conn4:
                                    cur4 = conn4.cursor()
                                    cur4.execute(
                                        """
                                        SELECT hora_inicio, hora_fin
                                        FROM horarios_profesores
                                        WHERE profesor_id = %s AND dia_semana = %s AND disponible = TRUE
                                        """,
                                        (profesor_id, dia_nombre)
                                    )
                                    raw_bloques = cur4.fetchall() or []
                                # Normalizar y ordenar
                                bloques_ord = []
                                for b in raw_bloques:
                                    try:
                                        h_ini, h_fin = b[0], b[1]
                                        if h_ini is not None and h_fin is not None and h_ini < h_fin:
                                            bloques_ord.append((h_ini, h_fin))
                                    except Exception:
                                        continue
                                bloques_ord.sort(key=lambda x: x[0])
                                # Fusionar bloques solapados/contiguos
                                bloques_merged = []
                                for bi, bf in bloques_ord:
                                    if not bloques_merged:
                                        bloques_merged.append([bi, bf])
                                    else:
                                        li, lf = bloques_merged[-1]
                                        if bi <= lf:  # solapa o es contiguo
                                            bloques_merged[-1][1] = max(lf, bf)
                                        else:
                                            bloques_merged.append([bi, bf])
                                ini_t = start_dt.time()
                                fin_t = end_dt.time()
                                for mi, mf in bloques_merged:
                                    if (ini_t >= mi) and (fin_t <= mf):
                                        en_horario = True
                                        break
                            except Exception:
                                en_horario = False

                        tipo_calc = 'En horario' if en_horario else 'Horas extra'
                        sets.append("tipo_actividad = %s")
                        params.append(tipo_calc)
                    except Exception:
                        # Si falla la clasificación, no tocar tipo_actividad
                        pass
                elif tipo_norm is not None:
                    # Validación estricta: si el usuario selecciona explícitamente
                    # "En horario" u "Horas extra", verificamos contra los horarios del profesor
                    # y corregimos en caso de desacuerdo para mantener consistencia en los reportes.
                    try:
                        tn_lower = str(tipo_norm).strip().lower()
                        is_en_horario = ('horario' in tn_lower) and ('en' in tn_lower)
                        is_horas_extra = ('horas' in tn_lower) and ('extra' in tn_lower)
                        if is_en_horario or is_horas_extra:
                            profesor_id = current.get("profesor_id")
                            # Resolver fecha/hora efectivas a usar para la clasificación
                            def _to_dt(x, d: date) -> Optional[datetime]:
                                if x is None:
                                    return None
                                if isinstance(x, datetime):
                                    return x
                                if isinstance(x, time):
                                    try:
                                        return datetime.combine(d, x)
                                    except Exception:
                                        return None
                                try:
                                    return datetime.fromisoformat(str(x).replace('Z', '+00:00'))
                                except Exception:
                                    return None
                            start_dt = _to_dt(ts_inicio_new if ts_inicio_new is not None else current.get("hora_inicio"), base_date)
                            end_dt = _to_dt(ts_fin_new if ts_fin_new is not None else current.get("hora_fin"), base_date)
                            if start_dt and end_dt and end_dt < start_dt:
                                end_dt = end_dt + timedelta(days=1)

                            # Día de la semana
                            try:
                                dia_map = {0: 'Lunes', 1: 'Martes', 2: 'Miércoles', 3: 'Jueves', 4: 'Viernes', 5: 'Sábado', 6: 'Domingo'}
                                dia_nombre = dia_map.get(base_date.weekday())
                            except Exception:
                                dia_nombre = None

                            en_horario = False
                            if profesor_id and dia_nombre and start_dt and end_dt:
                                try:
                                    # Igualar validación manual con la lógica fusionada
                                    with self.get_connection_context() as conn5:
                                        cur5 = conn5.cursor()
                                        cur5.execute(
                                            """
                                            SELECT hora_inicio, hora_fin
                                            FROM horarios_profesores
                                            WHERE profesor_id = %s AND dia_semana = %s AND disponible = TRUE
                                            """,
                                            (profesor_id, dia_nombre)
                                        )
                                        raw_bloques = cur5.fetchall() or []
                                    bloques_ord = []
                                    for b in raw_bloques:
                                        try:
                                            h_ini, h_fin = b[0], b[1]
                                            if h_ini is not None and h_fin is not None and h_ini < h_fin:
                                                bloques_ord.append((h_ini, h_fin))
                                        except Exception:
                                            continue
                                    bloques_ord.sort(key=lambda x: x[0])
                                    bloques_merged = []
                                    for bi, bf in bloques_ord:
                                        if not bloques_merged:
                                            bloques_merged.append([bi, bf])
                                        else:
                                            li, lf = bloques_merged[-1]
                                            if bi <= lf:
                                                bloques_merged[-1][1] = max(lf, bf)
                                            else:
                                                bloques_merged.append([bi, bf])
                                    ini_t = start_dt.time()
                                    fin_t = end_dt.time()
                                    for mi, mf in bloques_merged:
                                        if (ini_t >= mi) and (fin_t <= mf):
                                            en_horario = True
                                            break
                                except Exception:
                                    en_horario = False

                            tipo_calc = 'En horario' if en_horario else 'Horas extra'
                            # Si la selección manual no coincide con la clasificación calculada, corregimos
                            if (is_en_horario and tipo_calc != 'En horario') or (is_horas_extra and tipo_calc != 'Horas extra'):
                                sets.append("tipo_actividad = %s")
                                params.append(tipo_calc)
                            else:
                                sets.append("tipo_actividad = %s")
                                params.append(tipo_norm)
                        else:
                            # Para otros tipos (p.ej. "Trabajo"), respetar selección manual
                            sets.append("tipo_actividad = %s")
                            params.append(tipo_norm)
                    except Exception:
                        # En caso de error al validar, respetar selección manual
                        sets.append("tipo_actividad = %s")
                        params.append(tipo_norm)

                if not sets:
                    return {"success": False, "error": "Sin cambios para aplicar"}

                q = f"UPDATE profesor_horas_trabajadas SET {', '.join(sets)} WHERE id = %s RETURNING id"
                cursor.execute(q, tuple(params + [sesion_id]))
                rid = cursor.fetchone()
                if not rid:
                    try:
                        conn.rollback()
                    except Exception:
                        pass
                    return {"success": False, "error": "Sesión no encontrada"}

                cursor.execute(
                    """
                    SELECT id, profesor_id, fecha, hora_inicio, hora_fin,
                           minutos_totales, horas_totales, tipo_actividad
                    FROM profesor_horas_trabajadas
                    WHERE id = %s
                    """,
                    (sesion_id,)
                )
                updated = cursor.fetchone()
                try:
                    conn.commit()
                except Exception:
                    pass
                return {"success": True, "updated": dict(updated) if updated else None}
        except Exception as e:
            return {"success": False, "error": str(e)}


    def eliminar_profesor_sesion(self, sesion_id: int) -> Dict[str, Any]:
        """
        Elimina una sesión de trabajo de 'profesor_horas_trabajadas' por ID.
        """
        try:
            if not sesion_id or sesion_id <= 0:
                return {"success": False, "error": "ID de sesión inválido"}
            with self.get_connection_context() as conn:
                cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
                cursor.execute(
                    "DELETE FROM profesor_horas_trabajadas WHERE id = %s RETURNING id",
                    (sesion_id,)
                )
                row = cursor.fetchone()
                try:
                    conn.commit()
                except Exception:
                    pass
                if row:
                    return {"success": True, "deleted_id": sesion_id}
                else:
                    return {"success": False, "error": "Sesión no encontrada"}
        except Exception as e:
            try:
                with self.get_connection_context() as conn:
                    conn.rollback()
            except Exception:
                pass
            return {"success": False, "error": str(e)}


    def obtener_minutos_proyectados_profesor_rango(self, profesor_id: int, fecha_inicio: str, fecha_fin: str) -> Dict[str, Any]:
        """
        Calcula los minutos proyectados para un profesor en un rango arbitrario [fecha_inicio, fecha_fin]
        usando exclusivamente las disponibilidades activas en 'horarios_profesores'.

        Parámetros:
        - profesor_id: ID del profesor
        - fecha_inicio: cadena en formato 'YYYY-MM-DD'
        - fecha_fin: cadena en formato 'YYYY-MM-DD'

        Retorna:
        {
          success: bool,
          minutos_proyectados: int,
          horas_proyectadas: float,
          dias_con_disponibilidad: Dict[int, int]  # conteo de ocurrencias por día de semana
        }
        """
        import datetime as _dt

        try:
            # Normalizar fechas
            start_date = _dt.datetime.strptime(fecha_inicio, "%Y-%m-%d").date()
            end_date = _dt.datetime.strptime(fecha_fin, "%Y-%m-%d").date()
            if end_date < start_date:
                return {"success": False, "error": "Rango de fechas inválido"}

            with self.get_connection_context() as conn:
                cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

                # Cargar disponibilidades del profesor
                cursor.execute(
                    """
                    SELECT dia_semana, hora_inicio, hora_fin, disponible
                    FROM horarios_profesores
                    WHERE profesor_id = %s
                    """,
                    (profesor_id,),
                )
                disponibilidades = cursor.fetchall()

                # Mapear minutos por día de semana basados en disponibilidades activas
                minutos_por_dia = {i: 0 for i in range(7)}
                for row in disponibilidades:
                    dia_semana = row['dia_semana'] if isinstance(row, dict) else row[0]
                    hora_inicio = row['hora_inicio'] if isinstance(row, dict) else row[1]
                    hora_fin = row['hora_fin'] if isinstance(row, dict) else row[2]
                    disponible = row['disponible'] if isinstance(row, dict) else row[3]

                    if not disponible:
                        continue
                    def _parse_time(_val):
                        s = str(_val)
                        for fmt in ("%H:%M", "%H:%M:%S"):
                            try:
                                return _dt.datetime.strptime(s, fmt).time()
                            except Exception:
                                pass
                        try:
                            return _dt.time.fromisoformat(s)  # Python 3.7+ compatible
                        except Exception:
                            return None
                    hi = _parse_time(hora_inicio)
                    hf = _parse_time(hora_fin)
                    if hi is None or hf is None:
                        continue
                    # Calcular duración en minutos con manejo de casos fin < inicio
                    start_secs = hi.hour * 3600 + hi.minute * 60 + hi.second
                    end_secs = hf.hour * 3600 + hf.minute * 60 + hf.second
                    diff_secs = end_secs - start_secs
                    if diff_secs <= 0:
                        # Intento de corregir entradas comunes en formato 12h (p.ej. '01:00' usado como 13:00)
                        if end_secs < 12 * 3600:
                            end_secs_alt = end_secs + 12 * 3600
                            if end_secs_alt > start_secs:
                                diff_secs = end_secs_alt - start_secs
                            else:
                                # Atraviesa medianoche: contar como mismo día (aprox.)
                                diff_secs = (24 * 3600 - start_secs) + end_secs
                        else:
                            # Atraviesa medianoche
                            diff_secs = (24 * 3600 - start_secs) + end_secs
                    dur = diff_secs // 60
                    # dia_semana puede venir como texto (Lunes..Domingo) o entero (0..6)
                    if isinstance(dia_semana, str):
                        import unicodedata as _ud
                        s = ''.join(c for c in _ud.normalize('NFKD', str(dia_semana).strip()) if not _ud.combining(c)).lower()
                        mapa = {
                            'lunes': 0, 'martes': 1, 'miercoles': 2, 'jueves': 3,
                            'viernes': 4, 'sabado': 5, 'domingo': 6
                        }
                        dia_idx = mapa.get(s, -1)
                    else:
                        try:
                            dia_idx = int(dia_semana)
                        except Exception:
                            dia_idx = -1

                    if dur > 0 and 0 <= dia_idx <= 6:
                        minutos_por_dia[dia_idx] += dur

                # Fallback eliminado: solo se usan disponibilidades activas de horarios_profesores
                # Contar ocurrencias de cada día de la semana en el rango
                ocurrencias_por_dia = {i: 0 for i in range(7)}
                cur = start_date
                while cur <= end_date:
                    ocurrencias_por_dia[cur.weekday()] += 1
                    cur += _dt.timedelta(days=1)

                # Acumular minutos proyectados
                total_minutos = 0
                for d in range(7):
                    if minutos_por_dia[d] > 0 and ocurrencias_por_dia[d] > 0:
                        total_minutos += minutos_por_dia[d] * ocurrencias_por_dia[d]

                return {
                    "success": True,
                    "minutos_proyectados": int(total_minutos),
                    "horas_proyectadas": round(total_minutos / 60.0, 2),
                    "dias_con_disponibilidad": ocurrencias_por_dia,
                }
        except Exception as e:
            logging.error(f"Error calculando minutos proyectados rango profesor {profesor_id}: {e}")
            return {
                "success": False,
                "minutos_proyectados": 0,
                "horas_proyectadas": 0.0,
                "error": str(e),
            }


    def crear_profesor(self, usuario_id: int, especialidades: str = "", certificaciones: str = "", 
                      experiencia_años: int = 0, tarifa_por_hora: float = 0.0, 
                      fecha_contratacion: date = None, biografia: str = "", 
                      telefono_emergencia: str = "") -> int:
        """Crea un perfil de profesor para un usuario existente"""
        try:
            if fecha_contratacion is None:
                fecha_contratacion = date.today()
            
            with self.get_connection_context() as conn:
                with conn.cursor() as cursor:
                    sql = """
                    INSERT INTO profesores (usuario_id, especialidades, certificaciones, experiencia_años, 
                                          tarifa_por_hora, fecha_contratacion, biografia, telefono_emergencia)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s) RETURNING id
                    """
                    params = (usuario_id, especialidades, certificaciones, experiencia_años, 
                             tarifa_por_hora, fecha_contratacion, biografia, telefono_emergencia)
                    cursor.execute(sql, params)
                    profesor_id = cursor.fetchone()[0]
                    conn.commit()
                    return profesor_id
        except Exception as e:
            logging.error(f"Error en crear_profesor: {e}")
            raise Exception(f"Error creando profesor: {str(e)}")
    

    def obtener_profesor_por_id(self, profesor_id: int) -> Optional[Dict]:
        """Obtiene el perfil completo de un profesor por su ID de profesor"""
        with self.get_connection_context() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
                sql = """
                SELECT p.*, u.nombre, u.dni, u.telefono, u.activo
                FROM profesores p
                JOIN usuarios u ON p.usuario_id = u.id
                WHERE p.id = %s
                """
                cursor.execute(sql, (profesor_id,))
                row = cursor.fetchone()
                return dict(row) if row else None
    

    def actualizar_profesor(self, profesor_id: int, **kwargs) -> bool:
        """Actualiza los datos de un profesor"""
        try:
            # Campos válidos para actualizar en la tabla profesores
            campos_profesores = {
                'tipo', 'especialidades', 'certificaciones', 'experiencia_años', 
                'tarifa_por_hora', 'fecha_contratacion', 'biografia', 'telefono_emergencia', 'estado'
            }
            
            # Campos válidos para actualizar en la tabla usuarios
            campos_usuarios = {
                'nombre', 'apellido', 'email', 'telefono', 'direccion', 'activo'
            }
            
            # Separar campos por tabla
            datos_profesor = {k: v for k, v in kwargs.items() if k in campos_profesores}
            datos_usuario = {k: v for k, v in kwargs.items() if k in campos_usuarios}
            
            with self.get_connection_context() as conn:
                with conn.cursor() as cursor:
                    # Obtener usuario_id del profesor
                    cursor.execute("SELECT usuario_id FROM profesores WHERE id = %s", (profesor_id,))
                    result = cursor.fetchone()
                    if not result:
                        raise Exception(f"Profesor con ID {profesor_id} no encontrado")
                    
                    usuario_id = result[0]
                    
                    # Actualizar tabla profesores si hay datos
                    if datos_profesor:
                        campos = list(datos_profesor.keys())
                        valores = list(datos_profesor.values())
                        set_clause = ', '.join([f"{campo} = %s" for campo in campos])
                        
                        sql_profesor = f"UPDATE profesores SET {set_clause} WHERE id = %s"
                        cursor.execute(sql_profesor, valores + [profesor_id])
                    
                    # Actualizar tabla usuarios si hay datos
                    if datos_usuario:
                        campos = list(datos_usuario.keys())
                        valores = list(datos_usuario.values())
                        set_clause = ', '.join([f"{campo} = %s" for campo in campos])
                        
                        sql_usuario = f"UPDATE usuarios SET {set_clause} WHERE id = %s"
                        cursor.execute(sql_usuario, valores + [usuario_id])
                    
                    conn.commit()
                    return True
                    
        except Exception as e:
            logging.error(f"Error en actualizar_profesor: {e}")
            raise Exception(f"Error actualizando profesor: {str(e)}")


    def actualizar_estado_profesor(self, profesor_id: int, nuevo_estado: str) -> bool:
        """Actualiza el estado de un profesor en ambas tablas (profesores y usuarios)"""
        try:
            # Mapear estado de profesor a estado de usuario
            estado_activo = nuevo_estado == 'activo'
            
            with self.get_connection_context() as conn:
                with conn.cursor() as cursor:
                    # Obtener usuario_id del profesor
                    cursor.execute("SELECT usuario_id FROM profesores WHERE id = %s", (profesor_id,))
                    result = cursor.fetchone()
                    if not result:
                        raise Exception(f"Profesor con ID {profesor_id} no encontrado")
                    
                    usuario_id = result[0]
                    
                    # Actualizar estado en tabla profesores
                    cursor.execute(
                        "UPDATE profesores SET estado = %s WHERE id = %s",
                        (nuevo_estado, profesor_id)
                    )
                    
                    # Actualizar campo activo en tabla usuarios
                    cursor.execute(
                        "UPDATE usuarios SET activo = %s WHERE id = %s",
                        (estado_activo, usuario_id)
                    )
                    
                    conn.commit()
                    return True
                    
        except Exception as e:
            logging.error(f"Error en actualizar_estado_profesor: {e}")
            raise Exception(f"Error actualizando estado del profesor: {str(e)}")

    # --- MÉTODOS DE UTILIDAD ---
    

    def obtener_todos_profesores(self) -> List[Dict]:
        """Obtiene todos los profesores con información completa."""
        try:
            with self.get_connection_context() as conn:
                cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
                # Endurecer consulta: tiempos máximos y modo read-only para evitar bloqueos
                try:
                    cursor.execute("SET LOCAL lock_timeout = '800ms'")
                except Exception:
                    pass
                try:
                    cursor.execute("SET LOCAL statement_timeout = '1500ms'")
                except Exception:
                    pass
                try:
                    cursor.execute("SET LOCAL idle_in_transaction_session_timeout = '2s'")
                except Exception:
                    pass
                try:
                    cursor.execute("SET LOCAL default_transaction_read_only = on")
                except Exception:
                    pass
                sql = """
                SELECT p.*, u.nombre, u.dni, u.telefono, u.activo,
                       (SELECT COUNT(*) FROM profesor_evaluaciones pe 
                        WHERE pe.profesor_id = p.id) as total_evaluaciones
                FROM profesores p
                JOIN usuarios u ON p.usuario_id = u.id
                WHERE u.rol = 'profesor'
                ORDER BY u.nombre
                """
                cursor.execute(sql)
                return [dict(row) for row in cursor.fetchall()]
        except Exception as e:
            logging.error(f"Error en obtener_todos_profesores: {e}")
            return []


    def obtener_profesores(self) -> List[Dict]:
        """Alias para obtener_todos_profesores - mantiene compatibilidad con código existente"""
        return self.obtener_todos_profesores()

    @database_retry(max_retries=3, base_delay=0.8)

    def obtener_profesores_basico(self) -> List[Dict]:
        """Obtiene solo id y nombre de profesores, optimizado para login.

        Usa caché para minimizar latencia y carga de DB en arranque.
        """
        try:
            cached = self.cache.get('profesores', 'basico')
            if cached is not None:
                return cached
        except Exception:
            pass

        try:
            with self.get_connection_context() as conn:
                cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
                # Endurecer consulta liviana con helper de timeouts y read-only
                self._apply_readonly_timeouts(cursor, lock_ms=800, statement_ms=1200, idle_s=2)
                cursor.execute(
                    """
                    SELECT id, nombre
                    FROM usuarios
                    WHERE rol = 'profesor'
                    ORDER BY nombre ASC
                    """
                )
                rows = [dict(r) for r in cursor.fetchall()]
                try:
                    self.cache.set('profesores', 'basico', rows)
                except Exception:
                    pass
                return rows
        except Exception as e:
            logging.error(f"Error obteniendo profesores básicos: {e}")
            return []

    @database_retry(max_retries=3, base_delay=0.8)

    def obtener_profesores_basico_con_ids(self) -> List[Dict]:
        """Obtiene id y nombre de profesores desde profesores/usuarios.

        Usa caché y aplica timeouts de lectura en modo read-only."""
        try:
            cached = self.cache.get('profesores', 'basico_con_ids')
            if cached is not None:
                return cached
        except Exception:
            pass

        try:
            with self.get_connection_context() as conn:
                cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
                # Endurecer lectura ligera
                self._apply_readonly_timeouts(cursor, lock_ms=800, statement_ms=1500, idle_s=2)
                cursor.execute(
                    """
                    SELECT p.id AS id, u.nombre AS nombre
                    FROM profesores p
                    JOIN usuarios u ON p.usuario_id = u.id
                    WHERE u.activo = true
                    ORDER BY u.nombre ASC
                    """
                )
                rows = [dict(r) for r in cursor.fetchall()]
                try:
                    self.cache.set('profesores', 'basico_con_ids', rows)
                except Exception:
                    pass
                return rows
        except Exception as e:
            # Registrar posibles timeouts para métricas
            try:
                msg = str(e).lower()
                if "statement timeout" in msg or "canceling statement due to statement timeout" in msg:
                    self._increment_timeout_metric('statement_timeouts')
                elif "lock timeout" in msg:
                    self._increment_timeout_metric('lock_timeouts')
            except Exception:
                pass
            logging.error(f"Error en obtener_profesores_basico_con_ids: {e}")
            return []


    def existe_conflicto_activo_profesor_fecha(
        self, professor_id: int, conflict_type: str, conflict_date
    ) -> bool:
        """Verifica si ya existe un conflicto activo para un profesor en una fecha."""
        try:
            with self.get_connection_context() as conn:
                cursor = conn.cursor()
                cursor.execute(
                    """
                    SELECT 1 FROM schedule_conflicts 
                    WHERE conflict_type = %s AND professor_id = %s 
                      AND status = 'Activo' AND conflict_date = %s
                    LIMIT 1
                    """,
                    (conflict_type, professor_id, conflict_date),
                )
                return cursor.fetchone() is not None
        except Exception as e:
            logging.error(f"Error en existe_conflicto_activo_profesor_fecha: {e}")
            return False


    def asignar_especialidad_profesor(self, profesor_id: int, especialidad_id: int, 
                                     nivel_experiencia: str, años_experiencia: int = 0) -> int:
        """Asigna una especialidad a un profesor PostgreSQL"""
        try:
            with self.get_connection_context() as conn:
                cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
                sql = """
                INSERT INTO profesor_especialidades 
                (profesor_id, especialidad_id, nivel_experiencia, años_experiencia)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (profesor_id, especialidad_id) 
                DO UPDATE SET 
                    nivel_experiencia = EXCLUDED.nivel_experiencia,
                    años_experiencia = EXCLUDED.años_experiencia
                RETURNING id
                """
                cursor.execute(sql, (profesor_id, especialidad_id, nivel_experiencia, años_experiencia))
                especialidad_profesor_id = cursor.fetchone()['id']
                conn.commit()
                return especialidad_profesor_id
        except Exception as e:
            logging.error(f"Error en asignar_especialidad_profesor: {e}")
            return 0
    

    def quitar_especialidad_profesor(self, profesor_id: int, especialidad_id: int) -> bool:
        """Quita una especialidad de un profesor PostgreSQL"""
        with self.get_connection_context() as conn:
            cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            sql = "DELETE FROM profesor_especialidades WHERE profesor_id = %s AND especialidad_id = %s"
            cursor.execute(sql, (profesor_id, especialidad_id))
            conn.commit()
            return cursor.rowcount > 0
    

    def obtener_especialidades_profesor(self, profesor_id: int) -> List[Dict]:
        """Obtiene todas las especialidades de un profesor PostgreSQL"""
        with self.get_connection_context() as conn:
            cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            sql = """
            SELECT 
                pe.*,
                e.nombre as especialidad_nombre,
                e.descripcion as especialidad_descripcion,
                e.categoria as especialidad_categoria
            FROM profesor_especialidades pe
            JOIN especialidades e ON pe.especialidad_id = e.id
            WHERE pe.profesor_id = %s AND e.activo = TRUE
            ORDER BY e.categoria, e.nombre
            """
            cursor.execute(sql, (profesor_id,))
            return [dict(row) for row in cursor.fetchall()]
    

    def obtener_profesores_por_especialidad(self, especialidad_id: int) -> List[Dict]:
        """Obtiene todos los profesores que tienen una especialidad específica PostgreSQL"""
        with self.get_connection_context() as conn:
            cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            sql = """
            SELECT 
                pe.*,
                p.*,
                u.nombre as profesor_nombre,
                u.telefono
            FROM profesor_especialidades pe
            JOIN profesores p ON pe.profesor_id = p.id
            JOIN usuarios u ON p.usuario_id = u.id
            WHERE pe.especialidad_id = %s AND u.activo = TRUE
            ORDER BY u.nombre
            """
            cursor.execute(sql, (especialidad_id,))
            return [dict(row) for row in cursor.fetchall()]
    
    # === MÉTODOS PARA CERTIFICACIONES DE PROFESORES ===
    

    def crear_certificacion_profesor(self, profesor_id: int, nombre_certificacion: str, 
                                    institucion_emisora: str = None, numero_certificado: str = None,
                                    fecha_obtencion: date = None, fecha_vencimiento: date = None,
                                    archivo_adjunto: str = None, notas: str = None) -> int:
        """Crea una nueva certificación para un profesor PostgreSQL"""
        try:
            with self.get_connection_context() as conn:
                cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
                sql = """
                INSERT INTO profesor_certificaciones 
                (profesor_id, nombre_certificacion, institucion_emisora, numero_certificado,
                 fecha_obtencion, fecha_vencimiento, archivo_adjunto, notas)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s) RETURNING id
                """
                cursor.execute(sql, (profesor_id, nombre_certificacion, institucion_emisora, 
                                   numero_certificado, fecha_obtencion, fecha_vencimiento,
                                   archivo_adjunto, notas))
                certificacion_id = cursor.fetchone()['id']
                conn.commit()
                return certificacion_id
        except Exception as e:
            logging.error(f"Error en crear_certificacion_profesor: {e}")
            return 0
    

    def obtener_certificaciones_profesor(self, profesor_id: int, solo_vigentes: bool = False) -> List[Dict]:
        """Obtiene todas las certificaciones de un profesor PostgreSQL"""
        with self.get_connection_context() as conn:
            cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            sql = "SELECT * FROM profesor_certificaciones WHERE profesor_id = %s"
            params = [profesor_id]
            
            if solo_vigentes:
                sql += " AND estado = 'vigente'"
            
            sql += " ORDER BY fecha_obtencion DESC"
            
            cursor.execute(sql, params)
            return [dict(zip([desc[0] for desc in cursor.description], row)) for row in cursor.fetchall()]
    

    def actualizar_certificacion_profesor(self, certificacion_id: int, **kwargs) -> bool:
        """Actualiza una certificación de profesor PostgreSQL"""
        with self.get_connection_context() as conn:
            cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            
            campos_permitidos = [
                'nombre_certificacion', 'institucion_emisora', 'numero_certificado',
                'fecha_obtencion', 'fecha_vencimiento', 'archivo_adjunto', 'estado', 'notas'
            ]
            
            campos = []
            valores = []
            
            for campo, valor in kwargs.items():
                if campo in campos_permitidos and valor is not None:
                    campos.append(f"{campo} = %s")
                    valores.append(valor)
            
            if not campos:
                return False
            
            valores.append(certificacion_id)
            
            sql = f"UPDATE profesor_certificaciones SET {', '.join(campos)} WHERE id = %s"
            cursor.execute(sql, valores)
            conn.commit()
            return cursor.rowcount > 0
    

    def eliminar_certificacion_profesor(self, certificacion_id: int) -> bool:
        """Elimina una certificación de profesor PostgreSQL"""
        with self.get_connection_context() as conn:
            cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            sql = "DELETE FROM profesor_certificaciones WHERE id = %s"
            cursor.execute(sql, (certificacion_id,))
            conn.commit()
            return cursor.rowcount > 0
    

    def crear_disponibilidad_profesor(self, profesor_id: int, fecha: date, 
                                     tipo_disponibilidad: str, hora_inicio: str = None,
                                     hora_fin: str = None, notas: str = None) -> int:
        """Crea una entrada de disponibilidad para un profesor PostgreSQL"""
        try:
            with self.get_connection_context() as conn:
                cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
                sql = """
                INSERT INTO profesor_disponibilidad 
                (profesor_id, fecha, tipo_disponibilidad, hora_inicio, hora_fin, notas)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (profesor_id, fecha) 
                DO UPDATE SET 
                    tipo_disponibilidad = EXCLUDED.tipo_disponibilidad,
                    hora_inicio = EXCLUDED.hora_inicio,
                    hora_fin = EXCLUDED.hora_fin,
                    notas = EXCLUDED.notas,
                    fecha_modificacion = CURRENT_TIMESTAMP
                RETURNING id
                """
                cursor.execute(sql, (profesor_id, fecha, tipo_disponibilidad, hora_inicio, hora_fin, notas))
                disponibilidad_id = cursor.fetchone()['id']
                conn.commit()
                return disponibilidad_id
        except Exception as e:
            logging.error(f"Error en crear_disponibilidad_profesor: {e}")
            return 0
    

    def obtener_disponibilidad_profesor(self, profesor_id: int, fecha_inicio: date = None, 
                                       fecha_fin: date = None) -> List[Dict]:
        """Obtiene la disponibilidad de un profesor en un rango de fechas PostgreSQL"""
        with self.get_connection_context() as conn:
            cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            
            sql = "SELECT * FROM profesor_disponibilidad WHERE profesor_id = %s"
            params = [profesor_id]
            
            if fecha_inicio:
                sql += " AND fecha >= %s"
                params.append(fecha_inicio)
            
            if fecha_fin:
                sql += " AND fecha <= %s"
                params.append(fecha_fin)
            
            sql += " ORDER BY fecha ASC"
            
            cursor.execute(sql, params)
            return [dict(zip([desc[0] for desc in cursor.description], row)) for row in cursor.fetchall()]
    

    def verificar_disponibilidad_profesor_fecha(self, profesor_id: int, fecha: date, 
                                              hora_inicio: str = None, hora_fin: str = None) -> Dict:
        """Verifica si un profesor está disponible en una fecha específica PostgreSQL"""
        with self.get_connection_context() as conn:
            cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            
            sql = """
            SELECT * FROM profesor_disponibilidad 
            WHERE profesor_id = %s AND fecha = %s
            """
            
            cursor.execute(sql, (profesor_id, fecha))
            disponibilidad = cursor.fetchone()
            
            if not disponibilidad:
                return {'disponible': True, 'tipo': 'sin_configurar', 'conflictos': []}
            
            disponibilidad_dict = dict(zip([desc[0] for desc in cursor.description], disponibilidad))
            
            if disponibilidad_dict['tipo_disponibilidad'] == 'No Disponible':
                return {
                    'disponible': False, 
                    'tipo': 'no_disponible',
                    'motivo': disponibilidad_dict.get('notas', 'No disponible')
                }
            
            # Verificar conflictos de horario si se especifican horas
            conflictos = []
            if hora_inicio and hora_fin:
                conflictos = self._verificar_conflictos_horario_profesor(profesor_id, fecha, hora_inicio, hora_fin)
            
            return {
                'disponible': disponibilidad_dict['tipo_disponibilidad'] in ['Disponible', 'Parcialmente Disponible'],
                'tipo': disponibilidad_dict['tipo_disponibilidad'].lower().replace(' ', '_'),
                'conflictos': conflictos,
                'notas': disponibilidad_dict.get('notas')
            }
    

    def crear_suplencia(self, clase_horario_id: int, profesor_original_id: int, 
                       fecha_clase: date, motivo: str, profesor_suplente_id: int = None,
                       notas: str = None) -> int:
        """Crea una nueva suplencia PostgreSQL"""
        with self.get_connection_context() as conn:
            cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            # Resolver asignacion_id a partir de clase_horario_id y profesor_original_id
            cursor.execute(
                "SELECT id FROM profesor_clase_asignaciones WHERE clase_horario_id = %s AND profesor_id = %s AND activa = TRUE ORDER BY id DESC LIMIT 1",
                (clase_horario_id, profesor_original_id)
            )
            asignacion_row = cursor.fetchone()
            if asignacion_row is None:
                # Si no existe asignación activa, crearla
                self.asignar_profesor_a_clase(clase_horario_id, profesor_original_id, cursor)
                cursor.execute(
                    "SELECT id FROM profesor_clase_asignaciones WHERE clase_horario_id = %s AND profesor_id = %s AND activa = TRUE ORDER BY id DESC LIMIT 1",
                    (clase_horario_id, profesor_original_id)
                )
                asignacion_row = cursor.fetchone()
            asignacion_id = asignacion_row['id'] if isinstance(asignacion_row, dict) else asignacion_row[0]
            sql = """
            INSERT INTO profesor_suplencias 
            (asignacion_id, profesor_suplente_id, fecha_clase, motivo, notas)
            VALUES (%s, %s, %s, %s, %s) RETURNING id
            """
            cursor.execute(sql, (asignacion_id, profesor_suplente_id, 
                               fecha_clase, motivo, notas))
            result = cursor.fetchone()
            suplencia_id = result['id'] if isinstance(result, dict) else result[0]
            conn.commit()
            return suplencia_id
    

    def confirmar_suplencia(self, suplencia_id: int) -> bool:
        """Confirma una suplencia asignada PostgreSQL"""
        with self.get_connection_context() as conn:
            cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            sql = "UPDATE profesor_suplencias SET estado = 'Confirmado' WHERE id = %s"
            cursor.execute(sql, (suplencia_id,))
            conn.commit()
            return cursor.rowcount > 0
    

    def cancelar_suplencia(self, suplencia_id: int, motivo: str = None) -> bool:
        """Cancela una suplencia PostgreSQL"""
        with self.get_connection_context() as conn:
            cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            sql = """
            UPDATE profesor_suplencias 
            SET estado = 'Cancelado', fecha_resolucion = CURRENT_TIMESTAMP,
                notas = CASE WHEN %s IS NOT NULL THEN COALESCE(notas, '') || ' - Cancelado: ' || %s ELSE notas END
            WHERE id = %s
            """
            cursor.execute(sql, (motivo, motivo, suplencia_id))
            conn.commit()
            return cursor.rowcount > 0
    

    def obtener_suplencias_pendientes(self, profesor_id: int = None) -> List[Dict]:
        """Obtiene suplencias pendientes de asignación PostgreSQL"""
        with self.get_connection_context() as conn:
            cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            
            sql = """
            SELECT ps.*, ch.*, c.nombre as clase_nombre, po.nombre as profesor_original_nombre
            FROM profesor_suplencias ps
            JOIN profesor_clase_asignaciones pca ON ps.asignacion_id = pca.id
            JOIN clases_horarios ch ON pca.clase_horario_id = ch.id
            JOIN clases c ON ch.clase_id = c.id
            JOIN profesores po ON pca.profesor_id = po.id
            WHERE ps.estado = 'Pendiente'
            """
            
            params = []
            if profesor_id:
                sql += " AND pca.profesor_id = %s"
                params.append(profesor_id)
            
            sql += " ORDER BY ps.fecha_clase ASC"
            
            cursor.execute(sql, params)
            return [dict(r) for r in cursor.fetchall()]
    

    def obtener_suplencias_profesor(self, profesor_id: int, como_suplente: bool = False) -> List[Dict]:
        """Obtiene suplencias de un profesor (como original o como suplente) PostgreSQL"""
        with self.get_connection_context() as conn:
            cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            
            if como_suplente:
                campo_profesor = "ps.profesor_suplente_id"
            else:
                campo_profesor = "pca.profesor_id"
            
            sql = f"""
            SELECT ps.*, ch.*, c.nombre as clase_nombre,
                   po.nombre as profesor_original_nombre,
                   psup.nombre as profesor_suplente_nombre
            FROM profesor_suplencias ps
            JOIN profesor_clase_asignaciones pca ON ps.asignacion_id = pca.id
            JOIN clases_horarios ch ON pca.clase_horario_id = ch.id
            JOIN clases c ON ch.clase_id = c.id
            JOIN profesores po ON pca.profesor_id = po.id
            LEFT JOIN profesores psup ON ps.profesor_suplente_id = psup.id
            WHERE {campo_profesor} = %s
            ORDER BY ps.fecha_clase DESC
            """
            
            cursor.execute(sql, (profesor_id,))
            return [dict(r) for r in cursor.fetchall()]
    
    # === MÉTODOS PARA NOTIFICACIONES DE PROFESORES ===
    

    def crear_notificacion_profesor(self, profesor_id: int, tipo_notificacion: str, 
                                   titulo: str, mensaje: str, fecha_evento: date = None,
                                   prioridad: str = 'Media', datos_adicionales: str = None) -> int:
        """Crea una nueva notificación para un profesor PostgreSQL"""
        with self.get_connection_context() as conn:
            cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            sql = """
            INSERT INTO profesor_notificaciones 
            (profesor_id, tipo_notificacion, titulo, mensaje, fecha_evento, prioridad, datos_adicionales)
            VALUES (%s, %s, %s, %s, %s, %s, %s) RETURNING id
            """
            cursor.execute(sql, (profesor_id, tipo_notificacion, titulo, mensaje, 
                               fecha_evento, prioridad, datos_adicionales))
            result = cursor.fetchone()
            notificacion_id = result['id'] if result else None
            conn.commit()
            return notificacion_id
    

    def obtener_notificaciones_profesor(self, profesor_id: int, solo_no_leidas: bool = False,
                                       limite: int = 50) -> List[Dict]:
        """Obtiene notificaciones de un profesor PostgreSQL"""
        with self.get_connection_context() as conn:
            cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            
            sql = "SELECT * FROM profesor_notificaciones WHERE profesor_id = %s"
            params = [profesor_id]
            
            if solo_no_leidas:
                sql += " AND leida = FALSE"
            
            sql += " ORDER BY fecha_creacion DESC LIMIT %s"
            params.append(limite)
            
            cursor.execute(sql, params)
            return [dict(zip([desc[0] for desc in cursor.description], row)) for row in cursor.fetchall()]

    # === MÉTODOS PARA SUPLENCIAS GENERALES (INDEPENDIENTE DE CLASES) ===


    def crear_suplencia_general(self, horario_profesor_id: int, profesor_original_id: int,
                                fecha: date, hora_inicio: str, hora_fin: str, motivo: str,
                                profesor_suplente_id: int = None, notas: str = None) -> int:
        """Crea una suplencia general basada en horarios del profesor"""
        with self.get_connection_context() as conn:
            cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            sql = """
            INSERT INTO profesor_suplencias_generales
            (horario_profesor_id, profesor_original_id, profesor_suplente_id, fecha, hora_inicio, hora_fin, motivo, notas)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s) RETURNING id
            """
            cursor.execute(sql, (horario_profesor_id, profesor_original_id, profesor_suplente_id,
                                 fecha, hora_inicio, hora_fin, motivo, notas))
            result = cursor.fetchone()
            suplencia_id = result['id'] if result and 'id' in result else None
            if not suplencia_id or suplencia_id <= 0:
                raise Exception("No se pudo obtener el ID de la suplencia general (RETURNING id).")
            conn.commit()
            return suplencia_id


    def confirmar_suplencia_general(self, suplencia_id: int) -> bool:
        """Confirma una suplencia general"""
        with self.get_connection_context() as conn:
            cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            sql = """
            UPDATE profesor_suplencias_generales
            SET estado = 'Confirmado', fecha_resolucion = CURRENT_TIMESTAMP
            WHERE id = %s
            """
            cursor.execute(sql, (suplencia_id,))
            conn.commit()
            return cursor.rowcount > 0


    def cancelar_suplencia_general(self, suplencia_id: int, motivo: str = None) -> bool:
        """Cancela una suplencia general"""
        with self.get_connection_context() as conn:
            cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            sql = """
            UPDATE profesor_suplencias_generales
            SET estado = 'Cancelado',
                fecha_resolucion = CURRENT_TIMESTAMP,
                notas = CASE WHEN %s IS NOT NULL THEN COALESCE(notas, '') || ' - Cancelado: ' || %s ELSE notas END
            WHERE id = %s
            """
            cursor.execute(sql, (motivo, motivo, suplencia_id))
            conn.commit()
            return cursor.rowcount > 0
    

    def obtener_horas_trabajadas_profesor(self, profesor_id: int, fecha_inicio: date = None,
                                         fecha_fin: date = None) -> List[Dict]:
        """Obtiene las horas trabajadas por un profesor en un período PostgreSQL"""
        with self.get_connection_context() as conn:
            cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            
            sql = """
            SELECT pht.*, c.nombre as clase_nombre
            FROM profesor_horas_trabajadas pht
            LEFT JOIN clases c ON pht.clase_id = c.id
            WHERE pht.profesor_id = %s
            """
            params = [profesor_id]
            
            if fecha_inicio:
                sql += " AND pht.fecha >= %s"
                params.append(fecha_inicio)
            
            if fecha_fin:
                sql += " AND pht.fecha <= %s"
                params.append(fecha_fin)

            # Solo devolver sesiones cerradas, evitar registros en curso sin hora_fin
            sql += " AND pht.hora_fin IS NOT NULL"
            
            sql += " ORDER BY pht.fecha DESC, pht.hora_inicio DESC"
            
            cursor.execute(sql, params)
            # Con RealDictCursor, cada fila ya es un dict; evitar zip que devuelve claves como valores
            return [dict(row) for row in cursor.fetchall()]
    

    def obtener_resumen_horas_profesor(self, profesor_id: int, mes: int, año: int) -> Dict[str, Any]:
        """Obtiene un resumen de horas trabajadas por un profesor en un mes PostgreSQL"""
        try:
            with self.get_connection_context() as conn:
                cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
                
                sql = """
                SELECT 
                    COUNT(*) as total_registros,
                    COALESCE(SUM(horas_totales), 0) as total_horas,
                    COALESCE(AVG(horas_totales), 0) as promedio_horas,
                    tipo_actividad,
                    COUNT(*) as cantidad_por_tipo
                FROM profesor_horas_trabajadas
                WHERE profesor_id = %s 
                  AND EXTRACT(MONTH FROM fecha) = %s 
                  AND EXTRACT(YEAR FROM fecha) = %s
                  AND horas_totales IS NOT NULL
                GROUP BY tipo_actividad
                """
                
                cursor.execute(sql, (profesor_id, mes, año))
                resultados = cursor.fetchall()
                
                # Calcular totales generales
                total_horas = 0.0
                total_registros = 0
                por_tipo_actividad = []
                
                for row in resultados:
                    row_dict = dict(row)
                    # Convertir a float para evitar conflictos entre float y Decimal
                    total_horas_tipo = float(row_dict['total_horas'] or 0)
                    total_horas += total_horas_tipo
                    total_registros += int(row_dict['cantidad_por_tipo'] or 0)
                    
                    por_tipo_actividad.append({
                        'tipo_actividad': row_dict['tipo_actividad'],
                        'total_horas': total_horas_tipo,
                        'total_sesiones': int(row_dict['cantidad_por_tipo'] or 0),
                        'promedio_horas': float(row_dict['promedio_horas'] or 0)
                    })
                
                resumen = {
                    'success': True,
                    'totales': {
                        'total_horas': total_horas,
                        'total_sesiones': total_registros,
                        'promedio_diario': 0.0
                    },
                    'por_tipo_actividad': por_tipo_actividad,
                    'mes': mes,
                    'año': año
                }
                
                # Calcular promedio diario (asumiendo días laborables)
                import calendar
                try:
                    # Obtener todos los días del mes
                    cal = calendar.monthcalendar(año, mes)
                    dias_laborables = 0
                    for semana in cal:
                        for i, dia in enumerate(semana):
                            if dia != 0 and i < 5:  # Lunes a Viernes (0-4)
                                dias_laborables += 1
                    
                    if dias_laborables > 0:
                        resumen['totales']['promedio_diario'] = round(total_horas / dias_laborables, 2)
                except Exception as cal_error:
                    logging.warning(f"Error calculando días laborables: {cal_error}")
                    resumen['totales']['promedio_diario'] = 0.0
                
                return resumen
                
        except Exception as e:
            logging.error(f"Error obteniendo resumen de horas profesor {profesor_id}: {e}")
            return {
                'success': False,
                'totales': {
                    'total_horas': 0.0,
                    'total_sesiones': 0,
                    'promedio_diario': 0.0
                },
                'por_tipo_actividad': [],
                'mes': mes,
                'año': año,
                'error': str(e)
            }
    

    def obtener_horas_trabajadas_profesor_mes(self, profesor_id: int, mes: int, año: int) -> Dict[str, Any]:
        """Obtiene las horas trabajadas por un profesor en un mes específico con formato para el widget"""
        try:
            with self.get_connection_context() as conn:
                cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
                
                # Obtener todas las sesiones del mes
                sql = """
                SELECT 
                    fecha,
                    hora_inicio,
                    hora_fin,
                    COALESCE(horas_totales, 0) as horas_totales,
                    tipo_actividad,
                    notas
                FROM profesor_horas_trabajadas
                WHERE profesor_id = %s 
                  AND EXTRACT(MONTH FROM fecha) = %s 
                  AND EXTRACT(YEAR FROM fecha) = %s
                  AND horas_totales IS NOT NULL
                ORDER BY fecha DESC, hora_inicio DESC
                """
                
                cursor.execute(sql, (profesor_id, mes, año))
                sesiones = cursor.fetchall()
                
                # Calcular métricas
                total_horas = sum(float(s['horas_totales'] or 0) for s in sesiones)
                dias_trabajados = len(set(s['fecha'] for s in sesiones if s['fecha']))
                
                # Formatear sesiones para el widget
                sesiones_formateadas = []
                for sesion in sesiones:
                    try:
                        fecha_formateada = sesion['fecha'].strftime('%Y-%m-%d') if sesion['fecha'] else ''
                    except Exception:
                        fecha_formateada = str(sesion['fecha']) if sesion['fecha'] else ''
                    
                    sesiones_formateadas.append({
                        'fecha': fecha_formateada,
                        'horas_totales': round(float(sesion['horas_totales'] or 0), 2),
                        'tipo_actividad': sesion['tipo_actividad'] or '',
                        'notas': sesion['notas'] or ''
                    })
                
                return {
                    'success': True,
                    'total_horas': round(total_horas, 2),
                    'dias_trabajados': dias_trabajados,
                    'sesiones': sesiones_formateadas,
                    'mes': mes,
                    'año': año
                }
                
        except Exception as e:
            logging.error(f"Error obteniendo horas trabajadas del profesor {profesor_id}: {e}")
            return {
                'success': False,
                'total_horas': 0.0,
                'dias_trabajados': 0,
                'sesiones': [],
                'mes': mes,
                'año': año,
                'error': str(e)
            }
 
     # === MÉTODOS ADICIONALES DE OPTIMIZACIÓN ===
    

    def eliminar_profesor(self, profesor_id: int) -> bool:
        """Elimina un profesor y actualiza el usuario asociado PostgreSQL"""
        try:
            with self.get_connection_context() as conn:
                cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
                
                # Obtener usuario_id antes de eliminar
                cursor.execute("SELECT usuario_id FROM profesores WHERE id = %s", (profesor_id,))
                result = cursor.fetchone()
                
                if not result:
                    return False
                
                usuario_id = result['usuario_id']
                
                # Eliminar profesor
                cursor.execute("DELETE FROM profesores WHERE id = %s", (profesor_id,))
                
                # Cambiar rol del usuario a 'socio'
                cursor.execute("UPDATE usuarios SET rol = 'socio' WHERE id = %s", (usuario_id,))
                
                conn.commit()
                return cursor.rowcount > 0
        except Exception as e:
            logging.error(f"Error en eliminar_profesor: {e}")
            return False

    # === MÉTODOS PARA GESTIÓN DE SESIONES DE TRABAJO DE PROFESORES ===
    
    @database_retry(max_retries=3, base_delay=1.0, max_delay=10.0)

    def iniciar_sesion_trabajo_profesor(self, profesor_id: int, tipo_actividad: str = 'Trabajo') -> Dict[str, Any]:
        """Inicia una sesión de trabajo para un profesor usando solo la BD (idempotente)."""
        logging.info(f"Iniciando sesión de trabajo | profesor_id={profesor_id} | actividad={tipo_actividad}")

        try:
            # Usar la BD como única fuente de verdad
            with self.get_connection_context() as conn:
                cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

                # 1) Comprobar si ya existe una sesión activa en BD
                cursor.execute(
                    """
                    SELECT *
                    FROM profesor_horas_trabajadas
                    WHERE profesor_id = %s AND hora_fin IS NULL
                    ORDER BY hora_inicio DESC
                    LIMIT 1
                    """,
                    (profesor_id,)
                )
                sesion_activa = cursor.fetchone()

                if sesion_activa:
                    logging.info(f"Sesión activa encontrada en BD | id={sesion_activa['id']}")
                    # Registrar auditoría de reuso/idempotencia
                    try:
                        uid = None
                        try:
                            with self.get_connection_context() as conn2:
                                cur2 = conn2.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
                                cur2.execute("SELECT usuario_id FROM profesores WHERE id = %s", (profesor_id,))
                                r2 = cur2.fetchone()
                                if r2:
                                    uid = r2['usuario_id'] if isinstance(r2, dict) else (r2[0] if len(r2) > 0 else None)
                        except Exception:
                            uid = None
                        if uid is not None:
                            self.registrar_audit_log(
                                user_id=int(uid),
                                action='sesion_reusada',
                                table_name='profesor_horas_trabajadas',
                                record_id=sesion_activa['id'],
                                old_values=None,
                                new_values=json.dumps({'tipo_actividad': sesion_activa.get('tipo_actividad')}),
                                ip_address=None,
                                user_agent='desktop-app',
                                session_id=str(sesion_activa['id'])
                            )
                    except Exception as _e:
                        logging.debug(f"No se pudo registrar audit de reuso: {_e}")

                    return {
                        'success': True,
                        'sesion_id': sesion_activa['id'],
                        'mensaje': 'Sesión activa reutilizada (BD)',
                        'datos': dict(sesion_activa)
                    }

                logging.info("No hay sesión activa; creando nueva sesión en BD...")

                # 2) Crear nueva sesión en BD de forma segura
                sql_insert = """
                INSERT INTO profesor_horas_trabajadas 
                (profesor_id, fecha, hora_inicio, tipo_actividad, fecha_creacion)
                VALUES (%s, CURRENT_DATE, CURRENT_TIMESTAMP, %s, CURRENT_TIMESTAMP)
                RETURNING *
                """
                cursor.execute(sql_insert, (profesor_id, tipo_actividad))
                nueva_sesion = cursor.fetchone()
                conn.commit()

                logging.info(f"Nueva sesión creada en BD | id={nueva_sesion['id']} | actividad={tipo_actividad}")

                # Auditoría creación de sesión
                try:
                    uid = None
                    try:
                        with self.get_connection_context() as conn2:
                            cur2 = conn2.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
                            cur2.execute("SELECT usuario_id FROM profesores WHERE id = %s", (profesor_id,))
                            r2 = cur2.fetchone()
                            if r2:
                                uid = r2['usuario_id'] if isinstance(r2, dict) else (r2[0] if len(r2) > 0 else None)
                    except Exception:
                        uid = None
                    if uid is not None:
                        self.registrar_audit_log(
                            user_id=int(uid),
                            action='sesion_iniciada',
                            table_name='profesor_horas_trabajadas',
                            record_id=nueva_sesion['id'],
                            old_values=None,
                            new_values=json.dumps({'tipo_actividad': tipo_actividad}),
                            ip_address=None,
                            user_agent='desktop-app',
                            session_id=str(nueva_sesion['id'])
                        )
                except Exception as _e:
                    logging.debug(f"No se pudo registrar audit de inicio: {_e}")

                return {
                    'success': True,
                    'sesion_id': nueva_sesion['id'],
                    'mensaje': 'Nueva sesión iniciada (BD)',
                    'datos': dict(nueva_sesion)
                }

        except Exception as e:
            logging.error(f"Error iniciando sesión de trabajo profesor {profesor_id}: {e}")
            return {
                'success': False,
                'sesion_id': None,
                'mensaje': f'Error al iniciar sesión: {str(e)}',
                'datos': None
            }
    
    @database_retry(max_retries=3, base_delay=1.0, max_delay=10.0)

    def finalizar_sesion_trabajo_profesor(self, profesor_id: int) -> Dict[str, Any]:
        """Finaliza la sesión de trabajo usando solo la BD (idempotente)."""
        logging.info(f"Finalizando sesión de trabajo | profesor_id={profesor_id}")

        try:
            # Buscar sesión activa directamente en BD
            with self.get_connection_context() as conn:
                cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

                cursor.execute(
                    """
                    SELECT 
                        id, profesor_id, fecha, hora_inicio, hora_fin,
                        minutos_totales, horas_totales, tipo_actividad,
                        clase_id, notas, fecha_creacion
                    FROM profesor_horas_trabajadas
                    WHERE profesor_id = %s AND hora_fin IS NULL
                    ORDER BY hora_inicio DESC
                    LIMIT 1
                    """,
                    (profesor_id,)
                )
                sesion_db = cursor.fetchone()

                if not sesion_db:
                    logging.warning("No hay sesión activa en BD para finalizar")
                    return {
                        'success': False,
                        'mensaje': 'No hay sesión activa para finalizar',
                        'datos': None
                    }

                # Obtener tiempo de fin desde la BD para evitar desfases de zona horaria
                try:
                    with self.get_connection_context() as conn_ts:
                        cur_ts = conn_ts.cursor()
                        cur_ts.execute("SELECT CURRENT_TIMESTAMP")
                        r_ts = cur_ts.fetchone()
                        fin = r_ts[0] if r_ts else datetime.now()
                except Exception:
                    fin = datetime.now()
                inicio_db = sesion_db['hora_inicio']
                # Intentar calcular la duración en minutos directamente en SQL para mayor consistencia
                duracion_minutos_db = None
                try:
                    with self.get_connection_context() as conn_calc:
                        cur_calc = conn_calc.cursor()
                        cur_calc.execute(
                            """
                            SELECT ROUND(EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - %s)) / 60.0)
                            """,
                            (inicio_db,)
                        )
                        r_calc = cur_calc.fetchone()
                        if r_calc:
                            duracion_minutos_db = float(r_calc[0])
                except Exception:
                    duracion_minutos_db = None
                if duracion_minutos_db is None:
                    try:
                        duracion_minutos_db = (fin - inicio_db).total_seconds() / 60.0
                    except Exception:
                        duracion_minutos_db = 0.0
                if duracion_minutos_db < 1:
                    duracion_minutos_db = 1.0  # mínimo 1 minuto

                # Clasificar la sesión: 'En horario' si cae dentro de un bloque asignado, si no 'Horas extra'
                try:
                    dia_map = {0: 'Lunes', 1: 'Martes', 2: 'Miércoles', 3: 'Jueves', 4: 'Viernes', 5: 'Sábado', 6: 'Domingo'}
                    dia_nombre = dia_map.get(inicio_db.weekday())
                except Exception:
                    dia_nombre = None
                en_horario = False
                if dia_nombre:
                    try:
                        with self.get_connection_context() as conn4:
                            cur4 = conn4.cursor()
                            cur4.execute(
                                """
                                SELECT hora_inicio, hora_fin
                                FROM horarios_profesores
                                WHERE profesor_id = %s AND dia_semana = %s AND disponible = TRUE
                                """,
                                (profesor_id, dia_nombre)
                            )
                            bloques = cur4.fetchall() or []
                        ini_t = inicio_db.time()
                        fin_t = fin.time()
                        for b in bloques:
                            try:
                                h_ini, h_fin = b[0], b[1]
                                if (ini_t >= h_ini) and (fin_t <= h_fin):
                                    en_horario = True
                                    break
                            except Exception:
                                continue
                    except Exception:
                        en_horario = False
                tipo_actividad = 'En horario' if en_horario else 'Horas extra'

                # Finalizar sesión con tipo_actividad calculado y duración consistente en SQL
                with self.get_connection_context() as conn:
                    cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
                    sql = """
                    UPDATE profesor_horas_trabajadas 
                    SET hora_fin = CURRENT_TIMESTAMP,
                        minutos_totales = ROUND(EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - hora_inicio)) / 60.0),
                        tipo_actividad = %s
                    WHERE id = %s
                    RETURNING 
                        id, profesor_id, fecha, hora_inicio, hora_fin,
                        minutos_totales, horas_totales, tipo_actividad,
                        clase_id, notas, fecha_creacion
                    """
                    cursor.execute(sql, (tipo_actividad, sesion_db['id']))
                    sesion_finalizada = cursor.fetchone()
                    conn.commit()

                # Usar los minutos_totales devueltos por la BD para imprimir y auditar
                try:
                    duracion_minutos = float(sesion_finalizada.get('minutos_totales', duracion_minutos_db)) if sesion_finalizada else float(duracion_minutos_db)
                except Exception:
                    duracion_minutos = float(duracion_minutos_db)

                if sesion_finalizada:
                    # Intentar limpiar cualquier estado local residual
                    try:
                        with self._sesiones_lock:
                            if profesor_id in self._sesiones_locales:
                                del self._sesiones_locales[profesor_id]
                    except Exception:
                        pass

                    # Mostrar información de finalización
                    horas = int(duracion_minutos // 60)
                    minutos = int(duracion_minutos % 60)

                    logging.info(f"Sesión finalizada exitosamente | id={sesion_db['id']} | duracion={horas}h {minutos}m ({duracion_minutos:.2f} minutos)")

                    # Auditoría finalización de sesión
                    try:
                        uid = None
                        try:
                            with self.get_connection_context() as conn2:
                                cur2 = conn2.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
                                cur2.execute("SELECT usuario_id FROM profesores WHERE id = %s", (profesor_id,))
                                r2 = cur2.fetchone()
                                if r2:
                                    uid = r2['usuario_id'] if isinstance(r2, dict) else (r2[0] if len(r2) > 0 else None)
                        except Exception:
                            uid = None
                        if uid is not None:
                            self.registrar_audit_log(
                                user_id=int(uid),
                                action='sesion_finalizada',
                                table_name='profesor_horas_trabajadas',
                                record_id=sesion_db['id'],
                                old_values=None,
                                new_values=json.dumps({'minutos_totales': round(duracion_minutos)}),
                                ip_address=None,
                                user_agent='desktop-app',
                                session_id=str(sesion_db['id'])
                            )
                    except Exception as _e:
                        logging.debug(f"No se pudo registrar audit de finalización: {_e}")

                    # Obtener estadísticas mensuales
                    try:
                        with self.get_connection_context() as conn3:
                            cur3 = conn3.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
                            cur3.execute(
                                """
                                SELECT 
                                    COALESCE(SUM(minutos_totales), 0) as total_minutos_mes,
                                    COUNT(*) as dias_trabajados
                                FROM profesor_horas_trabajadas 
                                WHERE profesor_id = %s 
                                AND EXTRACT(MONTH FROM fecha) = EXTRACT(MONTH FROM CURRENT_DATE)
                                AND EXTRACT(YEAR FROM fecha) = EXTRACT(YEAR FROM CURRENT_DATE)
                                AND hora_fin IS NOT NULL
                                """,
                                (profesor_id,)
                            )
                            resultado_mes = cur3.fetchone()
                            if resultado_mes:
                                total_minutos_mes = float(resultado_mes['total_minutos_mes'])
                                dias_trabajados = resultado_mes['dias_trabajados']
                                horas_mes = int(total_minutos_mes // 60)
                                minutos_mes = int(total_minutos_mes % 60)

                                logging.info(f"Estadísticas mensuales | horas_mes={horas_mes}h {minutos_mes}m | dias_trabajados={dias_trabajados}")
                                logging.info(f"Sesión finalizada - Profesor {profesor_id}: {horas}h {minutos}m. Mes: {horas_mes}h {minutos_mes}m en {dias_trabajados} días")
                    except Exception as e:
                        logging.error(f"Error obteniendo estadísticas mensuales profesor {profesor_id}: {e}")

                    return {
                        'success': True,
                        'mensaje': 'Sesión finalizada correctamente (BD)',
                        'datos': dict(sesion_finalizada)
                    }
                else:
                    logging.error("Error al actualizar sesión en DB")
                    return {
                        'success': False,
                        'mensaje': 'Error al actualizar sesión en base de datos',
                        'datos': None
                    }
        except Exception as e:
            logging.error(f"Error finalizando sesión profesor {profesor_id}: {e}")
            return {
                'success': False,
                'mensaje': f'Error al finalizar sesión: {str(e)}',
                'datos': None
            }
    

    def obtener_sesion_activa_profesor(self, profesor_id: int) -> Dict[str, Any]:
        """Obtiene la sesión de trabajo activa de un profesor PostgreSQL"""
        try:
            with self.get_connection_context() as conn:
                cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
                sql = """
                SELECT 
                    id, profesor_id, fecha, hora_inicio, hora_fin,
                    minutos_totales, horas_totales, tipo_actividad,
                    clase_id, notas, fecha_creacion
                FROM profesor_horas_trabajadas 
                WHERE profesor_id = %s AND hora_fin IS NULL
                ORDER BY hora_inicio DESC
                LIMIT 1
                """
                cursor.execute(sql, (profesor_id,))
                result = cursor.fetchone()
                
                if result:
                    return {
                        'success': True,
                        'tiene_sesion_activa': True,
                        'sesion_activa': dict(result),
                        'mensaje': 'Sesión activa encontrada'
                    }
                else:
                    return {
                        'success': True,
                        'tiene_sesion_activa': False,
                        'sesion_activa': None,
                        'mensaje': 'No hay sesión activa'
                    }
        except Exception as e:
            logging.error(f"Error obteniendo sesión activa profesor {profesor_id}: {e}")
            return {
                'success': False,
                'tiene_sesion_activa': False,
                'sesion_activa': None,
                'error': str(e)
            }
    

    def obtener_duracion_sesion_actual_profesor(self, profesor_id: int) -> Dict[str, Any]:
        """Obtiene la duración de la sesión actual directamente desde la BD, calculada en SQL para evitar desfases de zona horaria."""
        try:
            with self.get_connection_context() as conn:
                cursor = conn.cursor()
                cursor.execute(
                    """
                    SELECT 
                        id,
                        hora_inicio,
                        EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - hora_inicio)) AS duracion_segundos
                    FROM profesor_horas_trabajadas
                    WHERE profesor_id = %s AND hora_fin IS NULL
                    ORDER BY hora_inicio DESC
                    LIMIT 1
                    """,
                    (profesor_id,),
                )
                row = cursor.fetchone()
                if not row:
                    return {
                        'success': True,
                        'tiene_sesion_activa': False,
                        'minutos_transcurridos': 0,
                        'horas_transcurridas': 0.0,
                        'tiempo_formateado': '0h 0m',
                        'mensaje': 'No hay sesión activa'
                    }

                sesion_id, _hora_inicio, duracion_segundos = row
                try:
                    segundos = max(0, int(duracion_segundos))
                except Exception:
                    segundos = 0
                minutos_transcurridos_int = segundos // 60
                horas_transcurridas = minutos_transcurridos_int / 60.0
                horas = minutos_transcurridos_int // 60
                mins = minutos_transcurridos_int % 60
                tiempo_formateado = f"{horas}h {mins}m"

                return {
                    'success': True,
                    'tiene_sesion_activa': True,
                    'sesion_id': sesion_id,
                    'minutos_transcurridos': minutos_transcurridos_int,
                    'horas_transcurridas': horas_transcurridas,
                    'tiempo_formateado': tiempo_formateado,
                }
        except Exception as e:
            logging.error(f"Error obteniendo duración de sesión actual para profesor {profesor_id}: {e}")
            return {
                'success': False,
                'tiene_sesion_activa': False,
                'minutos_transcurridos': 0,
                'horas_transcurridas': 0.0,
                'tiempo_formateado': '0h 0m',
                'error': str(e)
            }
    

    def normalizar_sesiones_profesor(
        self,
        profesor_id: int,
        fecha_inicio: Optional[date] = None,
        fecha_fin: Optional[date] = None,
        preferencia: str = 'minutos',
        tolerancia_minutos: int = 5
    ) -> Dict[str, Any]:
        """Normaliza sesiones antiguas corrigiendo 'hora_fin' o 'minutos_totales'.

        Regla robusta y segura (por defecto): confiar en los minutos trabajados
        para reconstruir la hora de fin (`hora_fin = hora_inicio + minutos_totales`).
        Corrige desfases típicos de zona horaria (p. ej. +/-1h, +/-2h, +/-3h).

        Heurísticas:
        - Si el delta entre `hora_fin` actual y la esperada por minutos coincide
          con offsets comunes dentro de la tolerancia, se corrige `hora_fin`.
        - Si `minutos_totales` está vacío/<=0 pero las marcas de tiempo indican
          duración positiva, se recalculan los minutos desde timestamps.
        - Si la preferencia es 'timestamps', se prioriza ajustar los minutos para
          que coincidan con (hora_fin - hora_inicio).

        Devuelve resumen con conteos y detalles por registro actualizado.
        """
        # Normalización segura sin bloque try externo
        resumen = {
            'success': True,
            'profesor_id': profesor_id,
            'preferencia': preferencia,
            'tolerancia_minutos': tolerancia_minutos,
            'total_inspeccionados': 0,
            'actualizados_hora_fin': 0,
            'actualizados_minutos': 0,
            'omitidos': 0,
            'detalles': []
        }

        try:
            with self.get_connection_context() as conn:
                cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

                sql = (
                    """
                    SELECT id, fecha, hora_inicio, hora_fin, minutos_totales
                    FROM profesor_horas_trabajadas
                    WHERE profesor_id = %s AND hora_fin IS NOT NULL
                    """
                )
                params: List[Any] = [profesor_id]
                if fecha_inicio is not None:
                    sql += " AND fecha >= %s"
                    params.append(fecha_inicio)
                if fecha_fin is not None:
                    sql += " AND fecha <= %s"
                    params.append(fecha_fin)
                sql += " ORDER BY fecha ASC, hora_inicio ASC"

                cursor.execute(sql, params)
                filas = cursor.fetchall() or []
                resumen['total_inspeccionados'] = len(filas)

                tz_offsets = [180, 120, 60, -60, -120, -180]

                for row in filas:
                    try:
                        rid = int(row['id'])
                        inicio = row['hora_inicio']
                        fin = row['hora_fin']
                        minutos = row.get('minutos_totales')
                        if inicio is None or fin is None:
                            resumen['omitidos'] += 1
                            resumen['detalles'].append({'id': rid, 'accion': 'omitido', 'motivo': 'faltan timestamps'})
                            continue

                        # Duración basada en timestamps actuales
                        try:
                            dur_ts_min = (fin - inicio).total_seconds() / 60.0
                        except Exception:
                            dur_ts_min = 0.0

                        # Hora de fin esperada confiando en minutos_totales (robusta)
                        try:
                            minutos_safe = int(minutos) if minutos is not None else 0
                        except Exception:
                            minutos_safe = 0
                        from datetime import timedelta
                        fin_esperada = inicio + timedelta(minutes=minutos_safe)

                        # Delta en minutos entre fin actual y esperada
                        try:
                            delta_min = (fin - fin_esperada).total_seconds() / 60.0
                        except Exception:
                            delta_min = 0.0

                        ajustar_fin = False
                        ajustar_minutos = False

                        # 1) Si minutos están vacíos/<=0 pero timestamps muestran duración positiva
                        if (minutos is None or minutos_safe <= 0) and (dur_ts_min > 0.5):
                            ajustar_minutos = True

                        # 2) Detectar desfase típico de zona horaria (+/-1,2,3 horas)
                        if not ajustar_fin:
                            for off in tz_offsets:
                                if abs(delta_min - off) <= tolerancia_minutos:
                                    ajustar_fin = True
                                    break

                        # 3) Regla principal por preferencia
                        if not ajustar_fin and not ajustar_minutos:
                            if abs(delta_min) > tolerancia_minutos:
                                if str(preferencia).lower() in ('minutos', 'minutes'):
                                    ajustar_fin = True
                                else:
                                    ajustar_minutos = True

                        # 4) Si timestamps dan duración negativa y hay minutos positivos, corregir fin
                        if not ajustar_fin and dur_ts_min < -0.5 and minutos_safe > 0:
                            ajustar_fin = True

                        if ajustar_fin:
                            try:
                                cursor.execute(
                                    "UPDATE profesor_horas_trabajadas SET hora_fin = %s WHERE id = %s",
                                    (fin_esperada, rid)
                                )
                                resumen['actualizados_hora_fin'] += 1
                                resumen['detalles'].append({
                                    'id': rid,
                                    'accion': 'hora_fin_actualizada',
                                    'delta_minutos': round(delta_min, 2),
                                    'fin_anterior': fin.isoformat(),
                                    'fin_nueva': fin_esperada.isoformat()
                                })
                            except Exception as e:
                                resumen['omitidos'] += 1
                                resumen['detalles'].append({'id': rid, 'accion': 'error_update_fin', 'error': str(e)})
                                continue
                        elif ajustar_minutos:
                            try:
                                minutos_nuevos = int(round(dur_ts_min))
                                if minutos_nuevos < 0:
                                    minutos_nuevos = 0
                                cursor.execute(
                                    "UPDATE profesor_horas_trabajadas SET minutos_totales = %s WHERE id = %s",
                                    (minutos_nuevos, rid)
                                )
                                resumen['actualizados_minutos'] += 1
                                resumen['detalles'].append({
                                    'id': rid,
                                    'accion': 'minutos_actualizados',
                                    'minutos_anteriores': int(minutos) if minutos is not None else None,
                                    'minutos_nuevos': minutos_nuevos
                                })
                            except Exception as e:
                                resumen['omitidos'] += 1
                                resumen['detalles'].append({'id': rid, 'accion': 'error_update_minutos', 'error': str(e)})
                                continue
                        else:
                            resumen['omitidos'] += 1
                            resumen['detalles'].append({
                                'id': rid,
                                'accion': 'sin_cambios',
                                'delta_minutos': round(delta_min, 2)
                            })
                    except Exception as e:
                        resumen['omitidos'] += 1
                        try:
                            registro_id = int(row.get('id')) if isinstance(row, dict) else None
                        except Exception:
                            registro_id = None
                        resumen['detalles'].append({
                            'id': registro_id,
                            'accion': 'error_procesamiento_registro',
                            'error': str(e)
                        })

                # Commit de todos los cambios y dejar que triggers recalculen horas/minutos
                conn.commit()

            return resumen
        except Exception as e:
            logging.error(f"Error normalizando sesiones del profesor {profesor_id}: {e}")
            return {
                'success': False,
                'error': str(e)
            }
    

    def obtener_horas_extras_profesor(self, profesor_id: int, fecha_inicio: date = None, fecha_fin: date = None) -> Dict[str, Any]:
        """Detecta horas extras trabajadas fuera de los horarios establecidos PostgreSQL"""
        try:
            if fecha_inicio is None:
                fecha_inicio = date.today().replace(day=1)  # Primer día del mes actual
            if fecha_fin is None:
                fecha_fin = date.today()
            
            with self.get_connection_context() as conn:
                cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
                
                # Obtener horarios establecidos del profesor desde el widget Horarios (tabla horarios_profesores)
                sql_horarios = """
                SELECT dia_semana, hora_inicio, hora_fin, disponible
                FROM horarios_profesores 
                WHERE profesor_id = %s AND disponible = true
                """
                cursor.execute(sql_horarios, (profesor_id,))
                horarios_establecidos = cursor.fetchall()
                
                if not horarios_establecidos:
                    return {
                        'success': True,
                        'horas_extras': [],
                        'total_horas_extras': 0,
                        'mensaje': 'No hay horarios establecidos para este profesor'
                    }
                
                # Obtener sesiones trabajadas en el período
                sql_sesiones = """
                SELECT 
                    id, fecha, hora_inicio, hora_fin, minutos_totales,
                    EXTRACT(DOW FROM fecha) as dia_semana_num,
                    CASE EXTRACT(DOW FROM fecha)
                        WHEN 0 THEN 'Domingo'
                        WHEN 1 THEN 'Lunes' 
                        WHEN 2 THEN 'Martes'
                        WHEN 3 THEN 'Miércoles'
                        WHEN 4 THEN 'Jueves'
                        WHEN 5 THEN 'Viernes'
                        WHEN 6 THEN 'Sábado'
                    END as dia_semana_nombre
                FROM profesor_horas_trabajadas 
                WHERE profesor_id = %s 
                AND fecha BETWEEN %s AND %s
                AND hora_fin IS NOT NULL
                ORDER BY fecha, hora_inicio
                """
                cursor.execute(sql_sesiones, (profesor_id, fecha_inicio, fecha_fin))
                sesiones = cursor.fetchall()
                
                horas_extras = []
                total_horas_extras = 0
                
                # Crear diccionario de horarios por día (clave unificada en número 0-6)
                dias_map_nombre = {0: 'Domingo', 1: 'Lunes', 2: 'Martes', 3: 'Miércoles', 4: 'Jueves', 5: 'Viernes', 6: 'Sábado'}
                dias_map_num = {v: k for k, v in dias_map_nombre.items()}
                horarios_por_dia = {}
                for horario in horarios_establecidos:
                    dia_raw = horario['dia_semana']
                    try:
                        # Unificar la clave del día a número
                        if isinstance(dia_raw, (int, float)):
                            dia_key = int(dia_raw) % 7
                        else:
                            dia_key = dias_map_num.get(str(dia_raw), None)
                        if dia_key is None:
                            # Si no se puede mapear, intentar heurística simple
                            val = str(dia_raw).strip().lower()
                            names = {'domingo':0,'lunes':1,'martes':2,'miércoles':3,'miercoles':3,'jueves':4,'viernes':5,'sábado':6,'sabado':6}
                            dia_key = names.get(val, None)
                        if dia_key is None:
                            continue
                        if dia_key not in horarios_por_dia:
                            horarios_por_dia[dia_key] = []
                        horarios_por_dia[dia_key].append({
                            'inicio': horario['hora_inicio'],
                            'fin': horario['hora_fin']
                        })
                    except Exception:
                        # Omite entradas de horario mal formateadas
                        continue
                
                # Verificar cada sesión con cobertura por unión de bloques y arreglar variable no definida
                for sesion in sesiones:
                    dia_num = int(sesion.get('dia_semana_num', 0) or 0)
                    dia_nombre = dias_map_nombre.get(dia_num, sesion.get('dia_semana_nombre'))
                    hora_inicio_sesion = sesion['hora_inicio'].time() if hasattr(sesion['hora_inicio'], 'time') else sesion['hora_inicio']
                    hora_fin_sesion = sesion['hora_fin'].time() if hasattr(sesion['hora_fin'], 'time') else sesion['hora_fin']

                    # Construir bloques fusionados (solapados/contiguos) del día
                    merged = []
                    if dia_num in horarios_por_dia:
                        bloques = horarios_por_dia[dia_num]
                        # ordenar
                        bloques_ord = sorted([(b['inicio'], b['fin']) for b in bloques], key=lambda x: x[0])
                        for bi, bf in bloques_ord:
                            if not merged:
                                merged.append([bi, bf])
                            else:
                                li, lf = merged[-1]
                                if bi <= lf:  # solapa o contiguo
                                    merged[-1][1] = max(lf, bf)
                                else:
                                    merged.append([bi, bf])

                    # Conversión a minutos para operar robustamente
                    def _to_min(t):
                        return (t.hour * 60 + t.minute)
                    s_ini = _to_min(hora_inicio_sesion)
                    s_fin = _to_min(hora_fin_sesion)

                    if dia_num not in horarios_por_dia or not merged:
                        # Día sin disponibilidad: todo es extra
                        minutos_extras = sesion['minutos_totales']
                        horas_extras.append({
                            'sesion_id': sesion['id'],
                            'fecha': sesion['fecha'],
                            'dia': dia_nombre,
                            'hora_inicio': hora_inicio_sesion,
                            'hora_fin': hora_fin_sesion,
                            'horas_extras': round(minutos_extras / 60.0, 2),
                            'motivo': 'Día no programado'
                        })
                        total_horas_extras += minutos_extras / 60.0
                    else:
                        # Calcular cobertura dentro de la unión de bloques
                        cubierto_min = 0
                        for bi, bf in merged:
                            bi_m = _to_min(bi)
                            bf_m = _to_min(bf)
                            # solapamiento con sesión
                            overlap = max(0, min(s_fin, bf_m) - max(s_ini, bi_m))
                            cubierto_min += overlap
                        sesion_min = int(sesion['minutos_totales'] or max(0, s_fin - s_ini))
                        extra_min = max(0, sesion_min - cubierto_min)
                        if extra_min > 0:
                            horas_extras.append({
                                'sesion_id': sesion['id'],
                                'fecha': sesion['fecha'],
                                'dia': dia_nombre,
                                'hora_inicio': hora_inicio_sesion,
                                'hora_fin': hora_fin_sesion,
                                'horas_extras': round(extra_min / 60.0, 2),
                                'motivo': 'Parcialmente fuera de horario' if cubierto_min > 0 else 'Fuera de horario establecido'
                            })
                            total_horas_extras += (extra_min / 60.0)
                
                return {
                    'success': True,
                    'horas_extras': horas_extras,
                    'total_horas_extras': round(total_horas_extras, 2),
                    'periodo': {
                        'fecha_inicio': fecha_inicio,
                        'fecha_fin': fecha_fin
                    },
                    'mensaje': f'Se encontraron {len(horas_extras)} sesiones con horas extras'
                }
                
        except Exception as e:
            logging.error(f"Error obteniendo horas extras profesor {profesor_id}: {e}")
            return {
                'success': False,
                'horas_extras': [],
                'total_horas_extras': 0,
                'error': str(e),
                'mensaje': f'Error calculando horas extras: {str(e)}'
            }
    
    # === MÉTODOS PARA HISTORIAL DE HORAS MENSUALES ===
    

    def obtener_horas_mensuales_profesor(self, profesor_id: int, año: int = None, mes: int = None) -> Dict[str, Any]:
        """Obtiene las horas trabajadas de un profesor en un mes específico"""
        try:
            if año is None:
                año = date.today().year
            if mes is None:
                mes = date.today().month
                
            with self.get_connection_context() as conn:
                cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
                
                # Calcular fechas del mes
                fecha_inicio = date(año, mes, 1)
                if mes == 12:
                    fecha_fin = date(año + 1, 1, 1) - timedelta(days=1)
                else:
                    fecha_fin = date(año, mes + 1, 1) - timedelta(days=1)
                
                # Obtener horas trabajadas del mes
                sql = """
                SELECT 
                    fecha,
                    hora_inicio,
                    hora_fin,
                    minutos_totales,
                    tipo_actividad,
                    notas
                FROM profesor_horas_trabajadas 
                WHERE profesor_id = %s 
                AND fecha BETWEEN %s AND %s
                AND hora_fin IS NOT NULL
                ORDER BY fecha, hora_inicio
                """
                cursor.execute(sql, (profesor_id, fecha_inicio, fecha_fin))
                sesiones = cursor.fetchall()
                
                # Calcular totales - solo trabajamos con minutos
                total_minutos = sum(float(s['minutos_totales'] or 0) for s in sesiones)
                
                # Convertir minutos a horas y minutos para mostrar
                horas_display = int(total_minutos // 60)
                minutos_display = int(total_minutos % 60)
                
                return {
                    'success': True,
                    'año': año,
                    'mes': mes,
                    'sesiones': [dict(s) for s in sesiones],
                    'total_minutos': round(total_minutos, 1),
                    'horas_display': horas_display,
                    'minutos_display': minutos_display,
                    'total_dias_trabajados': len(set(s['fecha'] for s in sesiones)),
                    'periodo': {
                        'fecha_inicio': fecha_inicio,
                        'fecha_fin': fecha_fin
                    }
                }
                
        except Exception as e:
            logging.error(f"Error obteniendo horas mensuales profesor {profesor_id}: {e}")
            return {
                'success': False,
                'error': str(e),
                'mensaje': f'Error obteniendo horas mensuales: {str(e)}'
            }
    

    def obtener_historial_meses_profesor(self, profesor_id: int, limite_meses: int = 12) -> Dict[str, Any]:
        """Obtiene el historial de horas trabajadas por mes de un profesor"""
        try:
            with self.get_connection_context() as conn:
                cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
                
                # Obtener resumen por mes - solo trabajamos con minutos
                sql = """
                SELECT 
                    EXTRACT(YEAR FROM fecha) as año,
                    EXTRACT(MONTH FROM fecha) as mes,
                    COUNT(*) as total_sesiones,
                    SUM(minutos_totales) as total_minutos,
                    COUNT(DISTINCT fecha) as dias_trabajados,
                    MIN(fecha) as primera_sesion,
                    MAX(fecha) as ultima_sesion
                FROM profesor_horas_trabajadas 
                WHERE profesor_id = %s 
                AND hora_fin IS NOT NULL
                GROUP BY EXTRACT(YEAR FROM fecha), EXTRACT(MONTH FROM fecha)
                ORDER BY año DESC, mes DESC
                LIMIT %s
                """
                cursor.execute(sql, (profesor_id, limite_meses))
                meses = cursor.fetchall()
                
                return {
                    'success': True,
                    'historial_meses': [dict(m) for m in meses],
                    'total_meses': len(meses)
                }
                
        except Exception as e:
            logging.error(f"Error obteniendo historial meses profesor {profesor_id}: {e}")
            return {
                'success': False,
                'error': str(e),
                'mensaje': f'Error obteniendo historial: {str(e)}'
            }
    

    def obtener_horas_mes_actual_profesor(self, profesor_id: int) -> Dict[str, Any]:
        """Obtiene las horas trabajadas del mes actual en tiempo real"""
        try:
            hoy = date.today()
            return self.obtener_horas_mensuales_profesor(profesor_id, hoy.year, hoy.month)
        except Exception as e:
            logging.error(f"Error obteniendo horas mes actual profesor {profesor_id}: {e}")
            return {
                'success': False,
                'error': str(e),
                'mensaje': f'Error obteniendo horas actuales: {str(e)}'
            }


    def obtener_minutos_mes_actual_profesor(self, profesor_id: int) -> Dict[str, Any]:
        """Obtiene los minutos trabajados del mes actual y detecta cambio de mes"""
        try:
            hoy = date.today()
            resultado = self.obtener_horas_mensuales_profesor(profesor_id, hoy.year, hoy.month)
            
            if resultado.get('success', False):
                # Agregar información sobre cambio de mes
                resultado['mes_actual'] = hoy.month
                resultado['año_actual'] = hoy.year
                resultado['cambio_mes'] = False  # Se detectará en la interfaz
                
            return resultado
        except Exception as e:
            logging.error(f"Error obteniendo minutos mes actual profesor {profesor_id}: {e}")
            return {
                'success': False,
                'error': str(e),
                'mensaje': f'Error obteniendo minutos actuales: {str(e)}'
            }


    def obtener_horas_proyectadas_profesor(self, profesor_id: int, año: int | None = None, mes: int | None = None) -> Dict[str, Any]:
        """
        Calcula las horas proyectadas semanales y mensuales para el profesor según sus disponibilidades en horarios_profesores.
        - semanal: suma de horas por día disponible (una ocurrencia por día en una semana estándar).
        - mensual: suma de horas por día multiplicadas por la cantidad de ocurrencias de ese día en el mes indicado.
        Si no se especifican año/mes, se usa el mes actual.
        """
        try:
            if año is None or mes is None:
                hoy = date.today()
                año = hoy.year if año is None else año
                mes = hoy.month if mes is None else mes

            # 1) Traer horas por día configuradas en horarios_profesores
            horas_por_dia_map: Dict[int, float] = {}
            map_dia = {
                'Lunes': 0, 'Martes': 1, 'Miércoles': 2, 'Miercoles': 2,
                'Jueves': 3, 'Viernes': 4, 'Sábado': 5, 'Sabado': 5, 'Domingo': 6
            }
            with self.get_connection_context() as conn:
                c = conn.cursor()
                c.execute(
                    """
                    SELECT dia_semana,
                           COALESCE(SUM(EXTRACT(EPOCH FROM (hora_fin - hora_inicio)) / 3600.0), 0) AS horas_dia
                    FROM horarios_profesores
                    WHERE profesor_id = %s AND disponible = true
                    GROUP BY dia_semana
                    """,
                    (profesor_id,)
                )
                for dia_semana, horas_dia in c.fetchall():
                    dnum: int | None = None
                    try:
                        raw = int(dia_semana)
                        # Aceptar 0..6 con 0=Domingo, o 1..7 con 7=Domingo
                        if raw == 0:
                            dnum = 6  # Domingo -> 6 en Python
                        elif 1 <= raw <= 7:
                            dnum = raw - 1 if raw != 7 else 6
                    except Exception:
                        dnum = map_dia.get(str(dia_semana), None)
                    if dnum is not None and 0 <= dnum <= 6:
                        horas_por_dia_map[dnum] = horas_por_dia_map.get(dnum, 0.0) + float(horas_dia or 0.0)

            # 2) Horas semanales proyectadas: una ocurrencia por día de la semana
            horas_semanales = float(sum(horas_por_dia_map.values()))

            # 3) Horas mensuales proyectadas: ocurrencias de cada weekday en el mes
            days_in_month = calendar.monthrange(año, mes)[1]
            weekday_counts = [0] * 7  # Monday=0..Sunday=6
            for d in range(1, days_in_month + 1):
                wd = calendar.weekday(año, mes, d)
                weekday_counts[wd] += 1
            horas_mensuales = 0.0
            for wd, horas in horas_por_dia_map.items():
                horas_mensuales += weekday_counts[wd] * horas

            return {
                'success': True,
                'año': año,
                'mes': mes,
                'horas_semanales': round(horas_semanales, 2),
                'horas_mensuales': round(horas_mensuales, 2),
                'detalle': {
                    'horas_por_dia': horas_por_dia_map,
                    'weekday_counts': weekday_counts,
                }
            }
        except Exception as e:
            logging.error(f"Error obteniendo horas proyectadas profesor {profesor_id}: {e}")
            return {
                'success': False,
                'error': str(e),
                'mensaje': f'Error obteniendo horas proyectadas: {str(e)}'
            }

    # === MÉTODOS PARA VERIFICACIÓN DE CERTIFICACIONES ===
    

    def obtener_horas_trabajadas_profesor(self, profesor_id: int, fecha_inicio: str = None, fecha_fin: str = None) -> List[Dict]:
        """Obtiene el historial de horas trabajadas de un profesor PostgreSQL (incluye minutos 0)."""
        with self.get_connection_context() as conn:
            cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

            sql = (
                "SELECT id, profesor_id, fecha, hora_inicio, hora_fin, "
                "minutos_totales, horas_totales, tipo_actividad, clase_id, notas, fecha_creacion "
                "FROM profesor_horas_trabajadas "
                "WHERE profesor_id = %s "
                "AND hora_fin IS NOT NULL"
            )
            params = [profesor_id]

            if fecha_inicio:
                sql += " AND fecha >= %s"
                params.append(fecha_inicio)

            if fecha_fin:
                sql += " AND fecha <= %s"
                params.append(fecha_fin)

            sql += " ORDER BY fecha DESC, hora_inicio DESC"

            cursor.execute(sql, params)
            # RealDictCursor ya provee filas como diccionarios; no re-mapear usando zip
            return [dict(row) for row in cursor.fetchall()]
    

    

    def _migrar_columna_tipo_profesores(self, cursor):
        """Migra la columna tipo en la tabla profesores si no existe"""
        try:
            # Verificar si la columna tipo ya existe
            cursor.execute("""
                SELECT column_name FROM information_schema.columns 
                WHERE table_name = 'profesores' AND column_name = 'tipo'
            """)
            
            if not cursor.fetchone():
                # Agregar la columna tipo con valor por defecto 'Musculación'
                cursor.execute("ALTER TABLE profesores ADD COLUMN tipo VARCHAR(50) DEFAULT 'Musculación'")
                logging.info("Campo 'tipo' agregado a la tabla profesores")
                
                # Actualizar registros existentes con el valor por defecto
                cursor.execute("""
                    UPDATE profesores 
                    SET tipo = 'Musculación'
                    WHERE tipo IS NULL
                """)
                logging.info("Registros existentes actualizados con tipo 'Musculación'")
            
        except Exception as e:
            logging.error(f"Error en migración de columna tipo en profesores: {e}")
            # En caso de error, no interrumpir la inicialización


    def obtener_estadisticas_profesor(self, profesor_id: int) -> dict:
        """Obtiene estadísticas de un profesor específico con horas reales"""
        try:
            with self.get_connection_context() as conn:
                cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
                
                # Obtener evaluaciones del profesor
                cursor.execute("""
                    SELECT COUNT(*) as total_evaluaciones,
                           AVG(puntuacion) as puntuacion_promedio
                    FROM profesor_evaluaciones 
                    WHERE profesor_id = %s
                """, (profesor_id,))
                evaluaciones_data = cursor.fetchone()
                
                # Obtener clases del profesor
                cursor.execute("""
                    SELECT COUNT(DISTINCT ch.clase_id) as total_clases
                    FROM clases_horarios ch
                    JOIN profesor_clase_asignaciones pca ON ch.id = pca.clase_horario_id
                    WHERE pca.profesor_id = %s AND pca.activa = true
                """, (profesor_id,))
                clases_data = cursor.fetchone()
                
                # Obtener estudiantes únicos
                cursor.execute("""
                    SELECT COUNT(DISTINCT cu.usuario_id) as estudiantes_unicos
                    FROM clase_usuarios cu
                    JOIN clases_horarios ch ON cu.clase_horario_id = ch.id
                    JOIN profesor_clase_asignaciones pca ON ch.id = pca.clase_horario_id
                    WHERE pca.profesor_id = %s AND pca.activa = true
                """, (profesor_id,))
                estudiantes_data = cursor.fetchone()
                
                # CAMBIO PRINCIPAL: Obtener horas trabajadas reales del mes actual
                from datetime import datetime
                mes_actual = datetime.now().month
                año_actual = datetime.now().year
                
                cursor.execute("""
                    SELECT COALESCE(SUM(horas_totales), 0) as horas_trabajadas
                    FROM profesor_horas_trabajadas 
                    WHERE profesor_id = %s 
                    AND EXTRACT(MONTH FROM fecha) = %s
                    AND EXTRACT(YEAR FROM fecha) = %s
                    AND hora_fin IS NOT NULL
                """, (profesor_id, mes_actual, año_actual))
                horas_data = cursor.fetchone()
                
                return {
                    'success': True,
                    'total_evaluaciones': evaluaciones_data['total_evaluaciones'] or 0,
                    'puntuacion_promedio': float(evaluaciones_data['puntuacion_promedio'] or 0),
                    'total_clases': clases_data['total_clases'] or 0,
                    'estudiantes_unicos': estudiantes_data['estudiantes_unicos'] or 0,
                    'horas_trabajadas': float(horas_data['horas_trabajadas'] or 0)
                }
                
        except Exception as e:
            logging.error(f"Error obteniendo estadísticas del profesor {profesor_id}: {str(e)}")
            return {
                'success': False,
                'mensaje': f"Error obteniendo estadísticas: {str(e)}",
                'total_evaluaciones': 0,
                'puntuacion_promedio': 0.0,
                'total_clases': 0,
                'estudiantes_unicos': 0,
                'horas_trabajadas': 0.0
            }
    

    def obtener_evaluaciones_profesor(self, profesor_id: int, limit: int = 10) -> dict:
        """Obtiene las evaluaciones recientes de un profesor"""
        try:
            with self.get_connection_context() as conn:
                cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
                cursor.execute("""
                    SELECT pe.*, u.nombre as usuario_nombre
                    FROM profesor_evaluaciones pe
                    JOIN usuarios u ON pe.usuario_id = u.id
                    WHERE pe.profesor_id = %s
                    ORDER BY pe.fecha_evaluacion DESC
                    LIMIT %s
                """, (profesor_id, limit))
                evaluaciones = cursor.fetchall()
                
                return {
                    'success': True,
                    'evaluaciones': evaluaciones
                }
                
        except Exception as e:
            logging.error(f"Error obteniendo evaluaciones del profesor {profesor_id}: {str(e)}")
            return {
                'success': False,
                'mensaje': f"Error obteniendo evaluaciones: {str(e)}",
                'evaluaciones': []
            }
    

    def optimizar_base_datos_horas_profesores(self):
        """Aplica optimizaciones de base de datos para el sistema de horas de profesores"""
        try:
            with self.get_connection_context() as conn:
                with conn.cursor() as cursor:
                    # Crear índices optimizados para consultas de horas
                    indices_sql = [
                        # Índice compuesto para consultas por profesor y fecha
                        """
                        CREATE INDEX IF NOT EXISTS idx_profesor_horas_profesor_fecha 
                        ON profesor_horas_trabajadas(profesor_id, fecha DESC)
                        """,
                        
                        # Índice para consultas mensuales
                        """
                        CREATE INDEX IF NOT EXISTS idx_profesor_horas_mes_año 
                        ON profesor_horas_trabajadas(profesor_id, EXTRACT(YEAR FROM fecha), EXTRACT(MONTH FROM fecha))
                        """,
                        
                        # Índice para sesiones abiertas
                        """
                        CREATE INDEX IF NOT EXISTS idx_profesor_horas_sesiones_abiertas 
                        ON profesor_horas_trabajadas(profesor_id, fecha, hora_inicio) 
                        WHERE hora_fin IS NULL
                        """,
                        
                        # Índice para horarios de profesores
                        """
                        CREATE INDEX IF NOT EXISTS idx_horarios_profesores_dia 
                        ON horarios_profesores(profesor_id, dia_semana, disponible)
                        """
                    ]
                    
                    for indice_sql in indices_sql:
                        try:
                            cursor.execute(indice_sql)
                            print(f"✓ Índice creado exitosamente")
                        except Exception as e:
                            print(f"⚠️ Error creando índice: {e}")
                    
                    # Crear función para calcular horas fuera de horario
                    funcion_horas_fuera = """
                    CREATE OR REPLACE FUNCTION calcular_horas_fuera_horario(
                        p_profesor_id INTEGER,
                        p_fecha DATE,
                        p_hora_inicio TIME,
                        p_hora_fin TIME
                    ) RETURNS DECIMAL AS $$
                    DECLARE
                        horas_programadas DECIMAL := 0;
                        horas_trabajadas DECIMAL;
                        horas_fuera DECIMAL := 0;
                    BEGIN
                        -- Calcular horas trabajadas
                        horas_trabajadas := EXTRACT(EPOCH FROM (p_hora_fin - p_hora_inicio)) / 3600;
                        
                        -- Obtener horas programadas para ese día
                        SELECT COALESCE(SUM(EXTRACT(EPOCH FROM (hora_fin - hora_inicio)) / 3600), 0)
                        INTO horas_programadas
                        FROM horarios_profesores
                        WHERE profesor_id = p_profesor_id
                        AND dia_semana = CASE EXTRACT(DOW FROM p_fecha)
                            WHEN 0 THEN 'Domingo'
                            WHEN 1 THEN 'Lunes'
                            WHEN 2 THEN 'Martes'
                            WHEN 3 THEN 'Miércoles'
                            WHEN 4 THEN 'Jueves'
                            WHEN 5 THEN 'Viernes'
                            WHEN 6 THEN 'Sábado'
                        END
                        AND disponible = true;
                        
                        -- Si trabajó más horas de las programadas
                        IF horas_trabajadas > horas_programadas THEN
                            horas_fuera := horas_trabajadas - horas_programadas;
                        END IF;
                        
                        RETURN horas_fuera;
                    END;
                    $$ LANGUAGE plpgsql;
                    """
                    
                    try:
                        cursor.execute(funcion_horas_fuera)
                        print("✓ Función calcular_horas_fuera_horario creada exitosamente")
                    except Exception as e:
                        print(f"⚠️ Error creando función: {e}")
                    
                    # Crear trigger para calcular automáticamente horas_totales
                    trigger_sql = """
                    CREATE OR REPLACE FUNCTION calcular_horas_totales()
                    RETURNS TRIGGER AS $$
                    BEGIN
                        IF NEW.hora_fin IS NOT NULL AND NEW.hora_inicio IS NOT NULL THEN
                            NEW.horas_totales := EXTRACT(EPOCH FROM (NEW.hora_fin - NEW.hora_inicio)) / 3600;
                        END IF;
                        RETURN NEW;
                    END;
                    $$ LANGUAGE plpgsql;
                    
                    DROP TRIGGER IF EXISTS trigger_calcular_horas_totales ON profesor_horas_trabajadas;
                    
                    CREATE TRIGGER trigger_calcular_horas_totales
                        BEFORE INSERT OR UPDATE ON profesor_horas_trabajadas
                        FOR EACH ROW
                        EXECUTE FUNCTION calcular_horas_totales();
                    """
                    
                    try:
                        cursor.execute(trigger_sql)
                        print("✓ Trigger calcular_horas_totales creado exitosamente")
                    except Exception as e:
                        print(f"⚠️ Error creando trigger: {e}")
                    
                    # Crear trigger para calcular automáticamente minutos_totales
                    trigger_minutos_sql = """
                    CREATE OR REPLACE FUNCTION calcular_minutos_totales()
                    RETURNS TRIGGER AS $$
                    BEGIN
                        IF NEW.hora_fin IS NOT NULL AND NEW.hora_inicio IS NOT NULL THEN
                            NEW.minutos_totales := EXTRACT(EPOCH FROM (NEW.hora_fin - NEW.hora_inicio)) / 60;
                        END IF;
                        RETURN NEW;
                    END;
                    $$ LANGUAGE plpgsql;
                    
                    DROP TRIGGER IF EXISTS trigger_calcular_minutos_totales ON profesor_horas_trabajadas;
                    
                    CREATE TRIGGER trigger_calcular_minutos_totales
                        BEFORE INSERT OR UPDATE ON profesor_horas_trabajadas
                        FOR EACH ROW
                        EXECUTE FUNCTION calcular_minutos_totales();
                    """
                    
                    try:
                        cursor.execute(trigger_minutos_sql)
                        print("✓ Trigger calcular_minutos_totales creado exitosamente")
                    except Exception as e:
                        print(f"⚠️ Error creando trigger para minutos_totales: {e}")
                    
                    # Insertar configuraciones del sistema de horas
                    configuraciones_sql = """
                    INSERT INTO configuraciones (clave, valor, descripcion) VALUES
                    ('max_horas_sesion', '12', 'Máximo de horas por sesión antes de alerta'),
                    ('alerta_horas_fuera_horario', 'true', 'Activar alertas por horas fuera de horario'),
                    ('reset_mensual_automatico', 'true', 'Reset automático de contadores mensuales')
                    ON CONFLICT (clave) DO NOTHING;
                    """
                    
                    try:
                        cursor.execute(configuraciones_sql)
                        print("✓ Configuraciones del sistema de horas agregadas")
                    except Exception as e:
                        print(f"⚠️ Error agregando configuraciones: {e}")
                    
                    conn.commit()
                    print("✓ Optimizaciones de base de datos aplicadas exitosamente")
                    
                    return {
                        'success': True,
                        'message': 'Optimizaciones aplicadas correctamente'
                    }
                    
        except Exception as e:
            logging.error(f"Error aplicando optimizaciones de base de datos: {e}")
            return {
                'success': False,
                'error': str(e)
            }
    
    # ==========================================
    # SISTEMA DE MENSAJERÍA WHATSAPP BUSINESS
    # ==========================================
    

    def _get_profesores_activos(self):
        """Obtiene profesores activos"""
        with self.db_manager.readonly_session() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute("""
                    SELECT p.id, u.nombre, u.telefono
                    FROM profesores p
                    JOIN usuarios u ON u.id = p.usuario_id
                    ORDER BY u.nombre
                """)
                return cur.fetchall()
    
