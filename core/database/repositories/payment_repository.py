from datetime import datetime, date, timedelta
from typing import List, Optional, Dict, Set, Tuple, Any
import json
import logging
import psycopg2
import psycopg2.extras
from .base import BaseRepository
from ..connection import database_retry
from ...models import Pago, TipoCuota
from ...utils import get_gym_name

class PaymentRepository(BaseRepository):
    pass

    # --- Methods moved from DatabaseManager ---

    def _migrar_tipos_cuota_existentes(self, cursor):
        """Migra los tipos de cuota existentes"""
        try:
            cursor.execute("SELECT COUNT(*) FROM tipos_cuota")
            if cursor.fetchone()[0] > 0:
                return
            
            cursor.execute("SELECT valor FROM configuracion WHERE clave = 'precio_estandar'")
            precio_estandar_row = cursor.fetchone()
            precio_estandar = float(precio_estandar_row[0]) if precio_estandar_row else Config.DEFAULT_MEMBERSHIP_PRICE
            
            cursor.execute("SELECT valor FROM configuracion WHERE clave = 'precio_estudiante'")
            precio_estudiante_row = cursor.fetchone()
            precio_estudiante = float(precio_estudiante_row[0]) if precio_estudiante_row else Config.DEFAULT_STUDENT_PRICE
            
            tipos_default = [
                ('Estándar', precio_estandar, 'icons/standard.png', True, 'Cuota mensual estándar para socios regulares'),
                ('Estudiante', precio_estudiante, 'icons/student.png', True, 'Cuota mensual con descuento para estudiantes')
            ]
            
            cursor.executemany(
                "INSERT INTO tipos_cuota (nombre, precio, icono_path, activo, descripcion) VALUES (%s, %s, %s, %s, %s)",
                tipos_default
            )
            
            logging.info("Migración de tipos de cuota completada exitosamente")
            
        except Exception as e:
            logging.error(f"Error en migración de tipos de cuota: {e}")


    def _migrar_metodos_conceptos_pago(self, cursor):
        """Migra métodos de pago y conceptos predeterminados"""
        try:
            cursor.execute("SELECT COUNT(*) FROM metodos_pago")
            if cursor.fetchone()[0] == 0:
                metodos_default = [
                    ('Efectivo', '💵', '#27ae60', 0.0, True),
                    ('Tarjeta de Débito', '💳', '#3498db', 2.5, True),
                    ('Tarjeta de Crédito', '💳', '#e74c3c', 3.5, True),
                    ('Transferencia', '🏦', '#9b59b6', 1.0, True),
                    ('MercadoPago', '💰', '#00b4d8', 4.0, True)
                ]
                
                cursor.executemany(
                    "INSERT INTO metodos_pago (nombre, icono, color, comision, activo) VALUES (%s, %s, %s, %s, %s)",
                    metodos_default
                )
                
                logging.info("Métodos de pago predeterminados creados exitosamente")
            
            cursor.execute("SELECT COUNT(*) FROM conceptos_pago")
            if cursor.fetchone()[0] == 0:
                conceptos_default = [
                    ('Cuota Mensual', 'Pago de cuota mensual del gimnasio', 0.0, 'variable', True),
                    ('Matrícula', 'Pago único de inscripción al gimnasio', 5000.0, 'fijo', True),
                    ('Clase Personal', 'Sesión de entrenamiento personalizado', 8000.0, 'fijo', True),
                    ('Suplementos', 'Venta de productos nutricionales', 0.0, 'variable', True),
                    ('Multa por Retraso', 'Recargo por pago tardío', 1000.0, 'fijo', True)
                ]
                
                cursor.executemany(
                    "INSERT INTO conceptos_pago (nombre, descripcion, precio_base, tipo, activo) VALUES (%s, %s, %s, %s, %s)",
                    conceptos_default
                )
                
                logging.info("Conceptos de pago predeterminados creados exitosamente")
                
        except Exception as e:
            logging.error(f"Error en migración de métodos y conceptos de pago: {e}")

    # --- MÉTODOS DE USUARIO ---
    
    @database_retry()

    def obtener_todos_pagos(self) -> List:
        """Obtiene todos los pagos optimizado: columnas específicas, timeouts y caché"""
        # 1) Caché en memoria
        try:
            cached = self.cache.get('pagos', ('all_basic',))
            if cached is not None:
                return cached
        except Exception:
            pass

        
        # 3) Consulta con columnas específicas y timeouts
        try:
            with self.get_connection_context() as conn:
                with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
                    try:
                        self._apply_readonly_timeouts(cursor, lock_ms=1000, statement_ms=2000, idle_s=2)
                    except Exception:
                        pass
                    try:
                        cursor.execute("SET LOCAL enable_seqscan = off")
                    except Exception:
                        pass
                    cursor.execute(
                        """
                        SELECT 
                            id, usuario_id, monto, mes, año, fecha_pago, metodo_pago_id
                        FROM pagos
                        ORDER BY fecha_pago DESC, año DESC, mes DESC
                        """
                    )
                    rows = [dict(row) for row in cursor.fetchall()]

                    # Actualizar caché
                    try:
                        self.cache.set('pagos', ('all_basic',), rows)
                    except Exception:
                        pass
                    return rows
        except Exception as e:
            logging.error(f"Error al obtener todos los pagos: {str(e)}")
            return []
    

    def obtener_metodos_pago(self, solo_activos: bool = True) -> List[Dict]:
        """Obtiene todos los métodos de pago con selección explícita de columnas, con caché TTL."""
        try:
            cache_key = f"activos:{1 if solo_activos else 0}"
            cached = self.cache.get('metodos_pago', cache_key)
            if isinstance(cached, list):
                return cached
        except Exception:
            pass

        with self.get_connection_context() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
                _cols = self.get_table_columns('metodos_pago')
                _desired = ['id','nombre','icono','color','comision','activo','fecha_creacion','descripcion']
                _sel = ", ".join([c for c in _desired if c in (_cols or [])]) or "id, nombre, activo"
                sql = f"SELECT {_sel} FROM metodos_pago"
                params: List[Any] = []
                if solo_activos:
                    sql += " WHERE activo = true"
                sql += " ORDER BY nombre"
                cursor.execute(sql, params)
                rows = [dict(row) for row in cursor.fetchall()]
        try:
            self.cache.set('metodos_pago', cache_key, rows)
        except Exception:
            pass
        return rows


    def obtener_conceptos_pago(self, solo_activos: bool = True) -> List[Dict]:
        """Obtiene todos los conceptos de pago con selección explícita de columnas, con caché TTL."""
        try:
            cache_key = f"activos:{1 if solo_activos else 0}"
            cached = self.cache.get('conceptos_pago', cache_key)
            if isinstance(cached, list):
                return cached
        except Exception:
            pass

        with self.get_connection_context() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
                _cols = self.get_table_columns('conceptos_pago')
                _desired = ['id','nombre','descripcion','precio_base','tipo','activo','fecha_creacion']
                _sel = ", ".join([c for c in _desired if c in (_cols or [])]) or "id, nombre, activo"
                sql = f"SELECT {_sel} FROM conceptos_pago"
                if solo_activos:
                    sql += " WHERE activo = true"
                sql += " ORDER BY nombre"
                cursor.execute(sql)
                rows = [dict(row) for row in cursor.fetchall()]
        try:
            self.cache.set('conceptos_pago', cache_key, rows)
        except Exception:
            pass
        return rows

    # --- MÉTODOS DE EJERCICIO (BATCH) ---


    def registrar_pagos_batch(self, pagos_items: List[Dict[str, Any]], skip_duplicates: bool = False, validate_data: bool = True, auto_crear_metodos_pago: bool = False) -> Dict[str, Any]:
        """Registra/actualiza pagos en lote.

        - Inserta nuevos pagos y, si `skip_duplicates` es False, actualiza existentes por (usuario_id, mes, año) usando UPDATE FROM.
        - Filtra usuarios inactivos.
        - Resuelve `metodo_pago_id` a partir de `metodo_pago` por nombre (case-insensitive) si es necesario.
        - Opcional: crea métodos de pago faltantes cuando llega `metodo_pago` y no existe.

        Cada item admite claves: usuario_id, monto, fecha_pago (str|date|datetime), mes, año,
        metodo_pago_id, metodo_pago. Retorna dict con 'insertados', 'actualizados', 'omitidos' y 'count'.
        """
        if not pagos_items:
            return {'insertados': [], 'actualizados': [], 'omitidos': [], 'count': 0}

        omitidos: List[Dict[str, Any]] = []
        normalized: List[Tuple[int, int, int]] = []  # (usuario_id, mes, año)
        rows_input: List[Dict[str, Any]] = []

        # Resolver nombres de métodos de pago si vienen como string
        metodo_nombres_original: Dict[str, str] = {}
        for item in pagos_items:
            nombre = item.get('metodo_pago')
            if nombre and not item.get('metodo_pago_id'):
                try:
                    key = str(nombre).strip().lower()
                    if key not in metodo_nombres_original:
                        metodo_nombres_original[key] = str(nombre).strip()
                except Exception:
                    pass

        metodo_map: Dict[str, int] = {}
        try:
            if metodo_nombres_original:
                with self.get_connection_context() as conn:
                    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
                    cur.execute(
                        "SELECT id, LOWER(nombre) AS nombre FROM metodos_pago WHERE LOWER(nombre) = ANY(%s)",
                        (list(metodo_nombres_original.keys()),)
                    )
                    for r in cur.fetchall() or []:
                        metodo_map[str(r['nombre']).lower()] = int(r['id'])

                    # Crear faltantes si se solicitó
                    faltantes = [n for n in metodo_nombres_original.keys() if n not in metodo_map]
                    if auto_crear_metodos_pago and faltantes:
                        valores = []
                        for key in faltantes:
                            nombre_vis = metodo_nombres_original.get(key, key)
                            # Defaults razonables
                            valores.append((nombre_vis, '💳', '#9b59b6', 0.0, True))

                        insert_sql = (
                            """
                            INSERT INTO metodos_pago (nombre, icono, color, comision, activo)
                            SELECT v.nombre, v.icono, v.color, v.comision, v.activo
                            FROM (VALUES %s) AS v(nombre, icono, color, comision, activo)
                            WHERE NOT EXISTS (
                                SELECT 1 FROM metodos_pago m WHERE LOWER(m.nombre) = LOWER(v.nombre)
                            )
                            RETURNING id, nombre
                            """
                        )
                        psycopg2.extras.execute_values(cur, insert_sql, valores, page_size=200)
                        for r in cur.fetchall() or []:
                            metodo_map[str(r['nombre']).strip().lower()] = int(r['id'])
        except Exception as e:
            logging.debug(f"Mapeo/creación de métodos de pago falló/no crítico: {e}")

        # Normalización y validaciones básicas
        for item in pagos_items:
            try:
                uid = int(item.get('usuario_id'))
                monto = float(item.get('monto'))
                if validate_data and monto < 0:
                    raise ValueError('monto negativo')

                # fecha_pago
                fraw = item.get('fecha_pago')
                fdt: datetime
                if fraw is None:
                    fdt = datetime.now()
                elif isinstance(fraw, datetime):
                    fdt = fraw
                elif isinstance(fraw, date):
                    fdt = datetime.combine(fraw, datetime.min.time())
                elif isinstance(fraw, str):
                    try:
                        fdt = datetime.fromisoformat(fraw)
                    except Exception:
                        fdt = datetime.now()
                else:
                    fdt = datetime.now()

                # mes/año
                mes_val = item.get('mes')
                if mes_val is None:
                    mes_val = item.get('mes_pagado')
                if mes_val is None:
                    mes_val = fdt.month
                mes_int = int(mes_val)
                if validate_data and not (1 <= mes_int <= 12):
                    raise ValueError('mes inválido')

                año_val = item.get('año')
                if año_val is None:
                    año_val = item.get('año_pagado')
                if año_val is None:
                    año_val = fdt.year
                año_int = int(año_val)

                metodo_id = item.get('metodo_pago_id')
                if metodo_id is None:
                    nombre = item.get('metodo_pago')
                    if nombre:
                        metodo_id = metodo_map.get(str(nombre).strip().lower())

                normalized.append((uid, mes_int, año_int))
                rows_input.append({
                    'usuario_id': uid,
                    'mes': mes_int,
                    'año': año_int,
                    'monto': monto,
                    'fecha_pago': fdt,
                    'metodo_pago_id': int(metodo_id) if metodo_id is not None else None
                })
            except Exception as e:
                omitidos.append({'usuario_id': item.get('usuario_id'), 'mes': item.get('mes'), 'año': item.get('año'), 'motivo': f'payload inválido: {e}'})

        if not rows_input:
            return {'insertados': [], 'actualizados': [], 'omitidos': omitidos, 'count': 0}

        try:
            with self.atomic_transaction(isolation_level="READ COMMITTED") as conn:
                cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

                # Filtrar usuarios activos
                uids = list({uid for uid, _m, _a in normalized})
                cur.execute("SELECT id FROM usuarios WHERE id = ANY(%s) AND activo = TRUE", (uids,))
                activos_set = {int(r['id']) for r in (cur.fetchall() or [])}

                rows_activos = [r for r in rows_input if int(r['usuario_id']) in activos_set]
                normalized_activos = [(r['usuario_id'], r['mes'], r['año']) for r in rows_activos]

                if not rows_activos:
                    return {'insertados': [], 'actualizados': [], 'omitidos': omitidos + [{'motivo': 'todos inactivos'}], 'count': 0}

                actualizados: List[Tuple[int, int, int]] = []
                insertados_ids: List[int] = []

                # Si no se deben saltar duplicados, actualizar existentes primero
                if not skip_duplicates:
                    update_sql = (
                        """
                        UPDATE pagos AS p
                        SET monto = v.monto,
                            fecha_pago = v.fecha_pago,
                            metodo_pago_id = COALESCE(v.metodo_pago_id, p.metodo_pago_id)
                        FROM (VALUES %s) AS v(usuario_id, mes, año, monto, fecha_pago, metodo_pago_id)
                        WHERE p.usuario_id = v.usuario_id AND p.mes = v.mes AND p.año = v.año
                        RETURNING p.id, p.usuario_id, p.mes, p.año
                        """
                    )
                    rows_for_update = [(r['usuario_id'], r['mes'], r['año'], r['monto'], r['fecha_pago'], r['metodo_pago_id']) for r in rows_activos]
                    psycopg2.extras.execute_values(cur, update_sql, rows_for_update, page_size=250)
                    upd_rows = cur.fetchall() or []
                    actualizados = [(int(r['usuario_id']), int(r['mes']), int(r['año'])) for r in upd_rows]

                # Insertar los que no existen aún por (usuario_id, mes, año)
                insert_sql = (
                    """
                    INSERT INTO pagos (usuario_id, monto, mes, año, fecha_pago, metodo_pago_id)
                    SELECT v.usuario_id, v.monto, v.mes, v.año, v.fecha_pago, v.metodo_pago_id
                    FROM (VALUES %s) AS v(usuario_id, mes, año, monto, fecha_pago, metodo_pago_id)
                    WHERE NOT EXISTS (
                        SELECT 1 FROM pagos p WHERE p.usuario_id = v.usuario_id AND p.mes = v.mes AND p.año = v.año
                    )
                    RETURNING id, usuario_id, mes, año, fecha_pago
                    """
                )
                rows_for_insert = [(r['usuario_id'], r['mes'], r['año'], r['monto'], r['fecha_pago'], r['metodo_pago_id']) for r in rows_activos]
                psycopg2.extras.execute_values(cur, insert_sql, rows_for_insert, page_size=250)
                ins_rows = cur.fetchall() or []
                insertados_ids = [int(r['id']) for r in ins_rows]

                # Determinar omitidos por duplicado (si se pidió saltar) o usuario inactivo
                if skip_duplicates:
                    existentes_sql = (
                        "SELECT usuario_id, mes, año FROM pagos WHERE (usuario_id, mes, año) IN (SELECT * FROM (VALUES %s) AS t(usuario_id, mes, año))"
                    )
                    psycopg2.extras.execute_values(cur, existentes_sql, normalized_activos, page_size=250)
                    existentes = {(int(r['usuario_id']), int(r['mes']), int(r['año'])) for r in (cur.fetchall() or [])}
                    for uid, m, a in normalized_activos:
                        if (uid, m, a) in existentes:
                            omitidos.append({'usuario_id': uid, 'mes': m, 'año': a, 'motivo': 'duplicado'})

                # Actualización masiva de usuarios con vencimiento y último pago en base a fecha_pago de insertados
                pv_rows: List[Tuple[int, datetime]] = [(int(r['usuario_id']), r['fecha_pago']) for r in ins_rows if r and r.get('usuario_id')]
                if pv_rows:
                    update_user_sql = (
                        """
                        UPDATE usuarios AS u
                        SET 
                            fecha_proximo_vencimiento = pv.fecha_pago::date + make_interval(days => COALESCE(tc.duracion_dias, 30)),
                            ultimo_pago = pv.fecha_pago::date,
                            activo = TRUE,
                            cuotas_vencidas = 0
                        FROM (VALUES %s) AS pv(usuario_id, fecha_pago)
                        LEFT JOIN tipos_cuota tc ON u.tipo_cuota = tc.nombre OR u.tipo_cuota::text = tc.id::text
                        WHERE u.id = pv.usuario_id
                        """
                    )
                    psycopg2.extras.execute_values(cur, update_user_sql, pv_rows, page_size=250)

                # Auditoría
                if self.audit_logger:
                    try:
                        for r in ins_rows:
                            self.audit_logger.log_operation(
                                'CREATE', 'pagos', int(r['id']), None,
                                {'usuario_id': int(r['usuario_id']), 'mes': int(r['mes']), 'año': int(r['año']), 'accion': 'batch'}
                            )
                        for uid, m, a in actualizados:
                            self.audit_logger.log_operation(
                                'UPDATE', 'pagos', None, None,
                                {'usuario_id': int(uid), 'mes': int(m), 'año': int(a), 'accion': 'batch'}
                            )
                    except Exception:
                        pass

                

                # Limpiar caché
                try:
                    self.cache.invalidate('pagos')
                except Exception:
                    pass

                return {
                    'insertados': insertados_ids,
                    'actualizados': actualizados,
                    'omitidos': omitidos,
                    'count': len(insertados_ids) + len(actualizados)
                }
        except Exception as e:
            logging.error(f"Error registrar_pagos_batch: {e}")
            raise
    
    # --- MÉTODOS DE ASISTENCIA ---
    
    @database_retry()

    def actualizar_fecha_proximo_vencimiento(self, usuario_id: int, fecha_pago: date = None) -> bool:
        """Actualiza la fecha de próximo vencimiento basada en el último pago usando duracion_dias del tipo de cuota"""
        try:
            with self.atomic_transaction(isolation_level="REPEATABLE READ") as conn:
                with conn.cursor() as cursor:
                    if fecha_pago is None:
                        fecha_pago = date.today()
                    
                    # Obtener el tipo de cuota del usuario para usar su duracion_dias
                    cursor.execute("""
                        SELECT tc.duracion_dias 
                        FROM usuarios u 
                        JOIN tipos_cuota tc ON u.tipo_cuota = tc.nombre 
                        WHERE u.id = %s
                    """, (usuario_id,))
                    
                    result = cursor.fetchone()
                    duracion_dias = result[0] if result else 30  # Valor por defecto si no se encuentra
                    
                    # Calcular próximo vencimiento usando duracion_dias
                    proximo_vencimiento = fecha_pago + timedelta(days=duracion_dias)
                    
                    # Actualizar fecha de próximo vencimiento y resetear contador de cuotas vencidas
                    cursor.execute("""
                        UPDATE usuarios 
                        SET fecha_proximo_vencimiento = %s, 
                            cuotas_vencidas = 0,
                            ultimo_pago = %s
                        WHERE id = %s
                    """, (proximo_vencimiento, fecha_pago, usuario_id))
            
            # Limpiar cache (luego del commit automático de atomic_transaction)
            self.cache.invalidate('usuarios', usuario_id)
            
            logging.info(f"Fecha de próximo vencimiento actualizada para usuario {usuario_id}: {proximo_vencimiento} (usando {duracion_dias} días)")
            return True
                    
        except Exception as e:
            logging.error(f"Error al actualizar fecha de próximo vencimiento: {e}")
            return False
    

    def incrementar_cuotas_vencidas(self, usuario_id: int) -> bool:
        """Incrementa el contador de cuotas vencidas de un usuario (excepto dueños y profesores)"""
        try:
            with self.atomic_transaction(isolation_level="REPEATABLE READ") as conn:
                with conn.cursor() as cursor:
                    # Evitar incremento para roles exentos
                    cursor.execute("SELECT rol FROM usuarios WHERE id = %s", (usuario_id,))
                    row = cursor.fetchone()
                    rol_lower = str(row[0] if row and len(row) > 0 and row[0] is not None else "").lower()
                    if rol_lower in ("profesor", "dueño", "owner"):
                        logging.info(f"Evitar incremento de cuotas vencidas para usuario {usuario_id} con rol '{rol_lower}'")
                        return True
                    cursor.execute("""
                        UPDATE usuarios 
                        SET cuotas_vencidas = COALESCE(cuotas_vencidas, 0) + 1
                        WHERE id = %s
                    """, (usuario_id,))
            # Limpiar cache tras commit
            self.cache.invalidate('usuarios', usuario_id)
            logging.info(f"Contador de cuotas vencidas incrementado para usuario {usuario_id}")
            return True
        except Exception as e:
            logging.error(f"Error al incrementar cuotas vencidas: {e}")
            return False
    

    def obtener_precio_cuota(self, tipo_cuota: str) -> float:
        """Obtiene el precio de un tipo de cuota desde la tabla tipos_cuota"""
        try:
            # Obtener desde la nueva tabla tipos_cuota
            tipo = self.obtener_tipo_cuota_por_nombre(tipo_cuota)
            if tipo and tipo.activo:
                return float(tipo.precio)
            
            # Si no se encuentra el tipo específico, usar el primer tipo activo disponible
            tipos_activos = self.obtener_tipos_cuota_activos()
            if tipos_activos:
                return float(tipos_activos[0].precio)
            
            return 5000.0  # Valor por defecto si no hay tipos activos
        except Exception as e:
            logging.warning(f"Error al obtener precio para tipo de cuota '{tipo_cuota}': {e}")
            return 5000.0  # Valor por defecto en caso de error


    def actualizar_precio_cuota(self, tipo_cuota: str, nuevo_precio: float):
        """Actualiza el precio de un tipo de cuota en la tabla tipos_cuota"""
        try:
            # Actualizar en la nueva tabla tipos_cuota
            tipo = self.obtener_tipo_cuota_por_nombre(tipo_cuota)
            if tipo:
                tipo.precio = nuevo_precio
                self.actualizar_tipo_cuota(tipo)
            else:
                logging.warning(f"No se encontró el tipo de cuota '{tipo_cuota}' para actualizar")
        except Exception as e:
            logging.error(f"Error al actualizar precio para tipo de cuota '{tipo_cuota}': {e}")

    # --- MÉTODOS DE TIPOS DE CUOTA ---
    

    def obtener_tipo_cuota_por_nombre(self, nombre: str) -> Optional[TipoCuota]:
        """Obtiene un tipo de cuota por su nombre"""
        with self.get_connection_context() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
                cursor.execute(
                    "SELECT id, nombre, precio, icono_path, activo, fecha_creacion, descripcion, duracion_dias, fecha_modificacion FROM tipos_cuota WHERE nombre = %s",
                    (nombre,)
                )
                row = cursor.fetchone()
                return TipoCuota(**dict(row)) if row else None


    # --- MÉTODOS DE NOTAS DE USUARIOS ---
    

    def verificar_vencimientos_cuotas_automatico(self, dias_vencimiento: int = 30, dias_alerta: int = 5) -> dict:
        """MÉTODO OBSOLETO - Usar procesar_vencimientos_automaticos() en su lugar.
        Verifica automáticamente vencimientos de cuotas y actualiza estados de usuarios."""
        logging.warning("verificar_vencimientos_cuotas_automatico está obsoleto. Usar procesar_vencimientos_automaticos()")
        
        # Redirigir al nuevo método para mantener compatibilidad
        return self.procesar_vencimientos_automaticos()
    
    # --- MÉTODOS DE UTILIDADES Y CONFIGURACIÓN ---
    

    def obtener_pago(self, pago_id: int) -> Optional[Pago]:
        """Obtiene un pago por su ID"""
        with self.get_connection_context() as conn:
            cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            cursor.execute(
                "SELECT id, usuario_id, monto, mes, año, fecha_pago, metodo_pago_id FROM pagos WHERE id = %s",
                (pago_id,)
            )
            row = cursor.fetchone()
            if not row:
                return None
            pago_dict = dict(row)
            allowed = {'id', 'usuario_id', 'monto', 'mes', 'año', 'fecha_pago', 'metodo_pago_id'}
            pago_clean = {k: pago_dict.get(k) for k in allowed if k in pago_dict}
            return Pago(**pago_clean)


    def obtener_pagos_mes(self, mes: int, año: int) -> List[Pago]:
        """Obtiene todos los pagos de un mes específico"""
        cache_key = f"pagos_mes_{mes}_{año}"
        cached_result = self.cache.get('pagos', cache_key)
        if cached_result:
            return cached_result
        
        with self.get_connection_context() as conn:
            cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            cursor.execute("""
                SELECT p.*, u.nombre as usuario_nombre 
                FROM pagos p
                JOIN usuarios u ON p.usuario_id = u.id
                WHERE EXTRACT(MONTH FROM p.fecha_pago) = %s AND EXTRACT(YEAR FROM p.fecha_pago) = %s
                ORDER BY p.fecha_pago DESC
            """, (mes, año))
            
            pagos = []
            for row in cursor.fetchall():
                pago_dict = dict(row)
                # Remover campos que no pertenecen al modelo Pago
                usuario_nombre = pago_dict.pop('usuario_nombre', None)
                allowed = {'id', 'usuario_id', 'monto', 'mes', 'año', 'fecha_pago', 'metodo_pago_id'}
                pago_clean = {k: pago_dict.get(k) for k in allowed if k in pago_dict}
                pago = Pago(**pago_clean)
                pago.usuario_nombre = usuario_nombre  # Agregar como atributo adicional
                pagos.append(pago)
            
            # Cache por 30 minutos
            self.cache.set('pagos', cache_key, pagos)
            
            return pagos


    def eliminar_pago(self, pago_id: int):
        """Elimina un pago con auditoría"""
        # Obtener datos del pago antes de eliminar para auditoría
        pago_to_delete = self.obtener_pago(pago_id)
        
        with self.get_connection_context() as conn:
            cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            cursor.execute("DELETE FROM pagos WHERE id = %s", (pago_id,))
            conn.commit()
            
            # Registrar en auditoría
            if self.audit_logger and pago_to_delete:
                old_values = {
                    'id': pago_to_delete.id,
                    'usuario_id': pago_to_delete.usuario_id,
                    'monto': pago_to_delete.monto,
                    'fecha_pago': pago_to_delete.fecha_pago,
                    'metodo_pago_id': pago_to_delete.metodo_pago_id
                }
                self.audit_logger.log_operation('DELETE', 'pagos', pago_id, old_values, None)
            
            # Limpiar cache de pagos
            self.cache.invalidate('pagos')
    

    def modificar_pago(self, pago: Pago):
        """Modifica un pago existente con auditoría"""
        # Obtener datos del pago antes de modificar para auditoría
        pago_original = self.obtener_pago(pago.id)
        
        with self.get_connection_context() as conn:
            cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            cursor.execute("""
                UPDATE pagos 
                SET usuario_id = %s, monto = %s, fecha_pago = %s, metodo_pago_id = %s
                WHERE id = %s
            """, (pago.usuario_id, pago.monto, pago.fecha_pago, 
                  getattr(pago, 'metodo_pago_id', None), pago.id))
            conn.commit()
            
            # Registrar en auditoría
            if self.audit_logger and pago_original:
                old_values = {
                    'usuario_id': pago_original.usuario_id,
                    'monto': pago_original.monto,
                    'fecha_pago': pago_original.fecha_pago,
                    'metodo_pago_id': getattr(pago_original, 'metodo_pago_id', None)
                }
                new_values = {
                    'usuario_id': pago.usuario_id,
                    'monto': pago.monto,
                    'fecha_pago': pago.fecha_pago,
                    'metodo_pago_id': getattr(pago, 'metodo_pago_id', None)
                }
                self.audit_logger.log_operation('UPDATE', 'pagos', pago.id, old_values, new_values)
            
            # Encolar sync: payment.update (o add con upsert en servidor)
            # Limpiar cache de pagos
            self.cache.invalidate('pagos')


    def verificar_pago_existe(self, usuario_id: int, mes: int, año: int) -> bool:
        """Verifica si existe un pago para un usuario en un mes/año específico"""
        with self.get_connection_context() as conn:
            cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            cursor.execute("""
                SELECT 1 FROM pagos 
                WHERE usuario_id = %s AND EXTRACT(MONTH FROM fecha_pago) = %s AND EXTRACT(YEAR FROM fecha_pago) = %s
                LIMIT 1
            """, (usuario_id, mes, año))
            return cursor.fetchone() is not None

    # Nota: definición consolidada de registrar_pagos_batch se encuentra arriba.


    def obtener_estadisticas_pagos(self, año: int = None) -> dict:
        """Obtiene estadísticas de pagos con optimizaciones"""
        if año is None:
            año = datetime.now().year
        
        cache_key = f"estadisticas_pagos_{año}"
        cached_result = self.cache.get('reportes', cache_key)
        if cached_result:
            return cached_result
        
        with self.get_connection_context() as conn:
            cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            
            # Estadísticas generales del año
            cursor.execute("""
                SELECT 
                    COUNT(*) as total_pagos,
                    SUM(monto) as total_recaudado,
                    AVG(monto) as promedio_pago,
                    MIN(monto) as pago_minimo,
                    MAX(monto) as pago_maximo
                FROM pagos 
                WHERE EXTRACT(YEAR FROM fecha_pago) = %s
            """, (año,))
            
            row = cursor.fetchone()
            estadisticas = {
                'año': año,
                'total_pagos': row[0] or 0,
                'total_recaudado': float(row[1] or 0),
                'promedio_pago': float(row[2] or 0),
                'pago_minimo': float(row[3] or 0),
                'pago_maximo': float(row[4] or 0)
            }
            
            # Estadísticas por mes
            cursor.execute("""
                SELECT EXTRACT(MONTH FROM fecha_pago) as mes, COUNT(*) as cantidad, SUM(monto) as total
                FROM pagos 
                WHERE EXTRACT(YEAR FROM fecha_pago) = %s
                GROUP BY EXTRACT(MONTH FROM fecha_pago)
                ORDER BY mes
            """, (año,))
            
            estadisticas['por_mes'] = {}
            for row in cursor.fetchall():
                mes, cantidad, total = row
                estadisticas['por_mes'][mes] = {
                    'cantidad': cantidad,
                    'total': float(total)
                }
            
            # Cache por 1 hora
            self.cache.set('reportes', cache_key, estadisticas)
            
            return estadisticas


    def obtener_precio_cuota(self, tipo_cuota: str) -> float:
        """Obtiene el precio de un tipo de cuota desde la tabla tipos_cuota"""
        try:
            # Obtener desde la nueva tabla tipos_cuota
            tipo = self.obtener_tipo_cuota_por_nombre(tipo_cuota)
            if tipo and tipo.activo:
                return float(tipo.precio)
            
            # Si no se encuentra el tipo específico, usar el primer tipo activo disponible
            tipos_activos = self.obtener_tipos_cuota_activos()
            if tipos_activos:
                return float(tipos_activos[0].precio)
            
            return 5000.0  # Valor por defecto si no hay tipos activos
        except Exception as e:
            logging.warning(f"Error al obtener precio para tipo de cuota '{tipo_cuota}': {e}")
            return 5000.0  # Valor por defecto en caso de error


    def actualizar_precio_cuota(self, tipo_cuota: str, nuevo_precio: float):
        """Actualiza el precio de un tipo de cuota en la tabla tipos_cuota"""
        try:
            # Actualizar en la nueva tabla tipos_cuota
            tipo = self.obtener_tipo_cuota_por_nombre(tipo_cuota)
            if tipo:
                tipo.precio = nuevo_precio
                self.actualizar_tipo_cuota(tipo)
            else:
                logging.warning(f"No se encontró el tipo de cuota '{tipo_cuota}' para actualizar")
        except Exception as e:
            logging.error(f"Error al actualizar precio para tipo de cuota '{tipo_cuota}': {e}")

    # --- MÉTODOS DE TIPOS DE CUOTA AVANZADOS ---

    def obtener_tipos_cuota_activos(self) -> List[TipoCuota]:
        """Obtiene todos los tipos de cuota activos con caché y manejo de lock timeout."""
        cache_key = ('tipos_cuota', 'activos')
        # Intento caché en memoria primero
        try:
            cached = self.cache.get('tipos', cache_key)
            if cached is not None:
                return cached
        except Exception:
            pass

        try:
            with self.readonly_session(lock_ms=800, statement_ms=2000, idle_s=2, seqscan_off=True) as conn:
                with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
                    cursor.execute("SELECT id, nombre, precio, icono_path, activo, fecha_creacion, descripcion, duracion_dias FROM tipos_cuota WHERE activo = true ORDER BY nombre")
                    rows = cursor.fetchall() or []
            tipos = []
            for row in rows:
                tipos.append(
                    TipoCuota(
                        id=row.get('id'),
                        nombre=row.get('nombre') or '',
                        precio=float(row.get('precio') or 0.0),
                        icono_path=row.get('icono_path'),
                        activo=bool(row.get('activo')),
                        fecha_creacion=row.get('fecha_creacion'),
                        descripcion=row.get('descripcion'),
                        duracion_dias=row.get('duracion_dias') or 30,
                    )
                )
            try:
                self.cache.set('tipos', cache_key, tipos)
            except Exception:
                pass
            return tipos
        except Exception as e:
            msg = str(e).lower()
            # Manejo específico de lock timeout con retry sin espera de lock
            if 'lock timeout' in msg or 'canceling statement due to lock timeout' in msg:
                try:
                    with self.readonly_session(lock_ms=0, statement_ms=2500, idle_s=2, seqscan_off=False) as conn2:
                        with conn2.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as c2:
                            c2.execute("SELECT id, nombre, precio, icono_path, activo, fecha_creacion, descripcion, duracion_dias FROM tipos_cuota WHERE activo = true ORDER BY nombre")
                            rows2 = c2.fetchall() or []
                    tipos2 = []
                    for row in rows2:
                        tipos2.append(
                            TipoCuota(
                                id=row.get('id'),
                                nombre=row.get('nombre') or '',
                                precio=float(row.get('precio') or 0.0),
                                icono_path=row.get('icono_path'),
                                activo=bool(row.get('activo')),
                                fecha_creacion=row.get('fecha_creacion'),
                                descripcion=row.get('descripcion'),
                                duracion_dias=row.get('duracion_dias') or 30,
                            )
                        )
                    try:
                        self.cache.set('tipos', cache_key, tipos2)
                    except Exception:
                        pass
                    return tipos2
                except Exception:
                    pass
            # Fallback: caché en memoria
            try:
                cached_mem = self.cache.get('tipos', cache_key)
                if cached_mem is not None:
                    return cached_mem
            except Exception:
                pass
            logging.error(f"Error al obtener tipos de cuota activos: {e}")
            return []


    def obtener_tipo_cuota_por_nombre(self, nombre: str) -> Optional[TipoCuota]:
        """Obtiene un tipo de cuota por su nombre"""
        with self.get_connection_context() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    "SELECT * FROM tipos_cuota WHERE nombre = %s",
                    (nombre,)
                )
                row = cursor.fetchone()
                if row:
                    return TipoCuota(**dict(zip([desc[0] for desc in cursor.description], row)))
                return None


    def obtener_tipo_cuota_por_id(self, tipo_id: int) -> Optional[TipoCuota]:
        """Obtiene un tipo de cuota por su ID"""
        try:
            with self.get_connection_context() as conn:
                with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
                    cursor.execute(
                        "SELECT id, nombre, precio, icono_path, activo, fecha_creacion, fecha_modificacion, descripcion, duracion_dias FROM tipos_cuota WHERE id = %s",
                        (tipo_id,)
                    )
                    row = cursor.fetchone()
                    if row:
                        tipo = TipoCuota()
                        tipo.id = row['id']
                        tipo.nombre = row['nombre'] or ""
                        tipo.precio = float(row['precio']) if row['precio'] else 0.0
                        tipo.icono_path = row['icono_path']
                        tipo.activo = row['activo'] if row['activo'] is not None else True
                        tipo.fecha_creacion = row['fecha_creacion'].isoformat() if row['fecha_creacion'] else None
                        tipo.fecha_modificacion = row['fecha_modificacion'].isoformat() if row['fecha_modificacion'] else None
                        tipo.descripcion = row['descripcion']
                        tipo.duracion_dias = row['duracion_dias'] if row['duracion_dias'] else 30
                        return tipo
                    return None
        except Exception as e:
            logging.error(f"Error al obtener tipo de cuota por ID {tipo_id}: {e}")
            return None


    def actualizar_tipo_cuota(self, tipo_cuota: TipoCuota) -> bool:
        """Actualiza un tipo de cuota existente"""
        try:
            with self.get_connection_context() as conn:
                with conn.cursor() as cursor:
                    # Incluir icono_path en la actualización para soportar emojis
                    cursor.execute(
                        """
                        UPDATE tipos_cuota 
                        SET nombre = %s, precio = %s, descripcion = %s, 
                            duracion_dias = %s, activo = %s, icono_path = %s, 
                            fecha_modificacion = CURRENT_TIMESTAMP
                        WHERE id = %s
                        """,
                        (tipo_cuota.nombre, tipo_cuota.precio, tipo_cuota.descripcion,
                         tipo_cuota.duracion_dias, tipo_cuota.activo, tipo_cuota.icono_path, tipo_cuota.id)
                    )
                    conn.commit()
                    return cursor.rowcount > 0
        except Exception as e:
            logging.error(f"Error al actualizar tipo de cuota: {e}")
            return False
    

    def automatizar_estados_por_vencimiento_optimizada(self) -> dict:
        """Automatiza la gestión de estados por vencimiento con optimizaciones PostgreSQL"""
        resultados = {
            'estados_actualizados': 0,
            'alertas_generadas': 0,
            'usuarios_reactivados': 0,
            'errores': [],
            'tiempo_procesamiento': 0
        }
        
        inicio_tiempo = time.time()
        
        try:
            with self.get_connection_context() as conn:
                cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
                fecha_actual = datetime.now().date()
                
                # 1. Obtener usuarios con cuotas vencidas (optimizado con CTE)
                cursor.execute("""
                    WITH usuarios_vencidos AS (
                        SELECT DISTINCT 
                            u.id as usuario_id,
                            u.nombre,
                            MAX(p.fecha_vencimiento) as ultima_fecha_vencimiento,
                            CASE 
                                WHEN MAX(p.fecha_vencimiento) < %s THEN 'vencido'
                                WHEN MAX(p.fecha_vencimiento) BETWEEN %s AND %s THEN 'proximo_vencimiento'
                                ELSE 'activo'
                            END as estado_calculado
                        FROM usuarios u
                        LEFT JOIN pagos p ON u.id = p.usuario_id
                        WHERE u.rol = 'socio' AND u.activo = true
                        GROUP BY u.id, u.nombre
                        HAVING MAX(p.fecha_vencimiento) IS NOT NULL
                    )
                    SELECT usuario_id, nombre, ultima_fecha_vencimiento, estado_calculado FROM usuarios_vencidos 
                    WHERE estado_calculado IN ('vencido', 'proximo_vencimiento')
                    ORDER BY ultima_fecha_vencimiento
                """, (fecha_actual, fecha_actual, fecha_actual + timedelta(days=7)))
                
                usuarios_procesamiento = cursor.fetchall()
                
                # 2. Procesar estados vencidos en lotes
                estados_a_insertar = []
                estados_a_desactivar = []
                usuarios_a_reactivar = []
                
                for usuario in usuarios_procesamiento:
                    try:
                        # Verificar estado actual
                        cursor.execute("""
                            SELECT id, estado FROM usuario_estados 
                            WHERE usuario_id = %s AND activo = true
                            ORDER BY fecha_creacion DESC LIMIT 1
                        """, (usuario['usuario_id'],))
                        
                        estado_actual = cursor.fetchone()
                        nuevo_estado = usuario['estado_calculado']
                        
                        if not estado_actual or estado_actual.get('estado') != nuevo_estado:
                            # Desactivar estado anterior si existe
                            if estado_actual:
                                estados_a_desactivar.append({
                                    'estado_id': estado_actual.get('id'),
                                    'usuario_id': usuario['usuario_id'],
                                    'estado': estado_actual.get('estado')
                                })
                            
                            # Preparar nuevo estado
                            estados_a_insertar.append({
                                'usuario_id': usuario['usuario_id'],
                                'estado': nuevo_estado,
                                'fecha_creacion': datetime.now(),
                                'activo': True,
                                'observaciones': f"Generado automáticamente - Vencimiento: {usuario['ultima_fecha_vencimiento']}"
                            })
                            
                            resultados['estados_actualizados'] += 1
                            
                            # Generar alerta si es necesario
                            if nuevo_estado == 'proximo_vencimiento':
                                resultados['alertas_generadas'] += 1
                        
                        # Verificar si el usuario necesita reactivación
                        if (nuevo_estado == 'activo' and estado_actual and 
                            (estado_actual.get('estado') in ['vencido', 'suspendido'])):
                            usuarios_a_reactivar.append(usuario)
                            
                    except Exception as e:
                        resultados['errores'].append({
                            'usuario_id': usuario['usuario_id'],
                            'error': str(e),
                            'contexto': 'procesamiento_usuario'
                        })
                
                # 3. Ejecutar operaciones en lotes
                if estados_a_insertar:
                    for estado in estados_a_insertar:
                        try:
                            # Validar existencia antes de insertar
                            if hasattr(self, 'usuario_id_existe') and not self.usuario_id_existe(estado['usuario_id']):
                                resultados['errores'].append({
                                    'usuario_id': estado['usuario_id'],
                                    'error': 'Usuario no existe; se omite inserción de estado',
                                    'contexto': 'batch_insert_usuario_estados'
                                })
                                continue
                        except Exception:
                            pass
                        cursor.execute("""
                            INSERT INTO usuario_estados (usuario_id, estado, fecha_creacion, activo, observaciones)
                            VALUES (%s, %s, %s, %s, %s)
                        """, (estado['usuario_id'], estado['estado'], estado['fecha_creacion'], 
                               estado['activo'], estado['observaciones']))
                
                # 4. Reactivar usuarios elegibles
                for usuario in usuarios_a_reactivar:
                    try:
                        cursor.execute(
                            "UPDATE usuarios SET activo = true WHERE id = %s",
                            (usuario['usuario_id'],)
                        )
                        
                        resultados['usuarios_reactivados'] += 1
                        
                    except Exception as e:
                        resultados['errores'].append({
                            'usuario_id': usuario['usuario_id'],
                            'error': str(e),
                            'contexto': 'reactivacion_usuario'
                        })
                
                # 5. Desactivar estados vencidos
                for estado in estados_a_desactivar:
                    try:
                        cursor.execute(
                            "UPDATE usuario_estados SET activo = false WHERE id = %s",
                            (estado['estado_id'],)
                        )
                        
                    except Exception as e:
                        resultados['errores'].append({
                            'estado_id': estado['estado_id'],
                            'error': str(e),
                            'contexto': 'desactivacion_estado'
                        })
                
                conn.commit()
                
                # Calcular tiempo de procesamiento
                resultados['tiempo_procesamiento'] = round(time.time() - inicio_tiempo, 2)
                
                logging.info(f"Automatización de estados por vencimiento completada: {resultados}")
                return resultados
                
        except Exception as e:
            resultados['errores'].append({
                'error': str(e),
                'contexto': 'procesamiento_general'
            })
            resultados['tiempo_procesamiento'] = round(time.time() - inicio_tiempo, 2)
            logging.error(f"Error en automatización de estados por vencimiento: {e}")
            return resultados
    

    def obtener_alertas_vencimiento_proactivas(self, dias_anticipacion: int = 7) -> List[dict]:
        """Obtiene alertas proactivas de usuarios próximos a vencer"""
        try:
            with self.get_connection_context() as conn:
                cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
                fecha_actual = datetime.now().date()
                fecha_limite = fecha_actual + timedelta(days=dias_anticipacion)
                
                cursor.execute("""
                    SELECT 
                        u.id, u.nombre, u.dni, u.telefono,
                        p_info.proximo_vencimiento,
                        p_info.dias_restantes
                    FROM usuarios u
                    JOIN (
                        SELECT 
                            usuario_id,
                            MAX(fecha_vencimiento) as proximo_vencimiento,
                            EXTRACT(DAY FROM MAX(fecha_vencimiento) - %s::date) as dias_restantes
                        FROM pagos
                        GROUP BY usuario_id
                    ) p_info ON u.id = p_info.usuario_id
                    WHERE p_info.proximo_vencimiento BETWEEN %s AND %s
                      AND u.rol = 'socio'
                      AND u.activo = true
                    ORDER BY p_info.proximo_vencimiento ASC
                """, (fecha_actual, fecha_actual, fecha_limite))
                
                alertas = []
                for row in cursor.fetchall():
                    alertas.append({
                        'usuario_id': row[0],
                        'nombre': row[1],
                        'dni': row[2],
                        'telefono': row[3],
                        'fecha_vencimiento': row[4],
                        'dias_restantes': int(row[5]) if row[5] else 0,
                        'tipo_alerta': 'vencimiento_proximo',
                        'prioridad': 'alta' if int(row[5] or 0) <= 3 else 'media'
                    })
                
                return alertas
                
        except Exception as e:
            logging.error(f"Error obteniendo alertas proactivas: {e}")
            return []
    
    # === BACKUP AND EXPORT METHODS ===
    

    def obtener_alertas_vencimientos_configurables(self, dias_anticipacion: int = 5) -> List[Dict]:
        """Obtiene alertas de vencimientos próximos con días de anticipación configurables PostgreSQL"""
        with self.get_connection_context() as conn:
            cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            
            fecha_limite = date.today() + timedelta(days=dias_anticipacion)
            
            sql = """
            SELECT u.id, u.nombre, u.dni, u.telefono, u.tipo_cuota,
                   MAX(p.fecha_pago) as ultimo_pago,
                   tc.precio,
                   CASE 
                       WHEN MAX(p.fecha_pago) IS NULL THEN 'Sin pagos registrados'
                       ELSE CAST(EXTRACT(DAY FROM (CURRENT_TIMESTAMP - MAX(p.fecha_pago))) AS INTEGER) || ' días desde último pago'
                   END as dias_desde_pago,
                   CASE 
                       WHEN MAX(p.fecha_pago) IS NULL THEN 0
                       ELSE CAST(EXTRACT(DAY FROM (CURRENT_TIMESTAMP - MAX(p.fecha_pago))) AS INTEGER)
                   END as dias_restantes
            FROM usuarios u
            LEFT JOIN pagos p ON u.id = p.usuario_id
            LEFT JOIN tipos_cuota tc ON u.tipo_cuota = tc.nombre
            WHERE u.activo = TRUE AND u.rol = 'socio'
            GROUP BY u.id, u.nombre, u.dni, u.telefono, u.tipo_cuota, tc.precio
            HAVING MAX(p.fecha_pago) IS NULL OR 
                   EXTRACT(DAY FROM (CURRENT_TIMESTAMP - MAX(p.fecha_pago))) >= (30 - %s)
            ORDER BY MAX(p.fecha_pago) ASC
            """
            
            cursor.execute(sql, (dias_anticipacion,))
            results = []
            for row in cursor.fetchall():
                row_dict = dict(zip([desc[0] for desc in cursor.description], row))
                
                # Agregar campos necesarios para compatibilidad
                # Asegurar que dias_restantes sea un entero
                try:
                    dias_desde_ultimo_pago = int(row_dict.get('dias_restantes', 0))
                except (ValueError, TypeError):
                    dias_desde_ultimo_pago = 0
                
                # Asegurar que dias_restantes en el diccionario sea entero
                row_dict['dias_restantes'] = dias_desde_ultimo_pago
                
                # Determinar tipo de alerta basado en días desde último pago
                if dias_desde_ultimo_pago >= 30:
                    tipo_alerta = 'cuota_vencida'
                elif dias_desde_ultimo_pago >= (30 - dias_anticipacion):
                    tipo_alerta = 'vencimiento_proximo'
                else:
                    tipo_alerta = 'estado_activo'
                
                row_dict['tipo_alerta'] = tipo_alerta
                row_dict['prioridad'] = 'alta' if dias_desde_ultimo_pago >= 30 else 'media'
                
                results.append(row_dict)
            
            return results
    

    def _migrar_campo_metodo_pago_id(self, cursor):
        """Migra el campo metodo_pago_id para pagos existentes PostgreSQL"""
        try:
            # Verificar si la columna metodo_pago_id ya existe
            cursor.execute("""
                SELECT column_name FROM information_schema.columns 
                WHERE table_name = 'pagos' AND column_name = 'metodo_pago_id'
            """)
            
            if not cursor.fetchone():
                # Agregar la columna metodo_pago_id con valor por defecto NULL
                cursor.execute("ALTER TABLE pagos ADD COLUMN metodo_pago_id INTEGER")
                
                # Obtener el ID del método de pago 'Efectivo' como predeterminado
                cursor.execute("SELECT id FROM metodos_pago WHERE nombre = 'Efectivo' LIMIT 1")
                efectivo_row = cursor.fetchone()
                
                if efectivo_row:
                    efectivo_id = efectivo_row[0]
                    # Actualizar pagos existentes para usar 'Efectivo' como método predeterminado
                    cursor.execute("UPDATE pagos SET metodo_pago_id = %s WHERE metodo_pago_id IS NULL", (efectivo_id,))
                    logging.info(f"Pagos existentes actualizados con método de pago 'Efectivo' (ID: {efectivo_id})")
                
                logging.info("Campo 'metodo_pago_id' agregado a la tabla pagos")
            
        except Exception as e:
            logging.error(f"Error en migración del campo metodo_pago_id en pagos: {e}")
            # En caso de error, no interrumpir la inicialización


    def _migrar_campos_cuotas_vencidas(self, cursor):
        """Migra los campos para el sistema de cuotas vencidas PostgreSQL"""
        try:
            # Verificar si la columna fecha_proximo_vencimiento ya existe
            cursor.execute("""
                SELECT column_name FROM information_schema.columns 
                WHERE table_name = 'usuarios' AND column_name = 'fecha_proximo_vencimiento'
            """)
            
            if not cursor.fetchone():
                # Agregar la columna fecha_proximo_vencimiento
                cursor.execute("ALTER TABLE usuarios ADD COLUMN fecha_proximo_vencimiento DATE")
                logging.info("Campo 'fecha_proximo_vencimiento' agregado a la tabla usuarios")
            
            # Verificar si la columna cuotas_vencidas ya existe
            cursor.execute("""
                SELECT column_name FROM information_schema.columns 
                WHERE table_name = 'usuarios' AND column_name = 'cuotas_vencidas'
            """)
            
            if not cursor.fetchone():
                # Agregar la columna cuotas_vencidas con valor por defecto 0
                cursor.execute("ALTER TABLE usuarios ADD COLUMN cuotas_vencidas INTEGER DEFAULT 0")
                logging.info("Campo 'cuotas_vencidas' agregado a la tabla usuarios")
            
        except Exception as e:
            logging.error(f"Error en migración de campos de cuotas vencidas: {e}")
            # En caso de error, no interrumpir la inicialización


    def crear_tipo_cuota(self, tipo_cuota: TipoCuota) -> int:
        """Crea un nuevo tipo de cuota."""
        try:
            with self.get_connection_context() as conn:
                cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
                sql = """
                INSERT INTO tipos_cuota (nombre, precio, icono_path, activo, descripcion, duracion_dias)
                VALUES (%s, %s, %s, %s, %s, %s)
                RETURNING id
                """
                cursor.execute(sql, (
                    tipo_cuota.nombre,
                    tipo_cuota.precio,
                    tipo_cuota.icono_path,
                    tipo_cuota.activo,
                    tipo_cuota.descripcion,
                    tipo_cuota.duracion_dias
                ))
                conn.commit()
                result = cursor.fetchone()
                return result['id'] if result else 0
        except Exception as e:
            logging.error(f"Error al crear tipo de cuota: {str(e)}")
            return 0
    

    def eliminar_tipo_cuota(self, tipo_id: int) -> bool:
        """Elimina un tipo de cuota (eliminación suave)."""
        try:
            with self.get_connection_context() as conn:
                cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
                # Verificar si hay usuarios usando este tipo de cuota
                cursor.execute(
                    "SELECT COUNT(*) as total FROM usuarios WHERE tipo_cuota = (SELECT nombre FROM tipos_cuota WHERE id = %s) AND activo = true",
                    (tipo_id,)
                )
                result = cursor.fetchone()
                count = result['total'] if result else 0
                
                if count > 0:
                    # Si hay usuarios, solo desactivar
                    cursor.execute(
                        "UPDATE tipos_cuota SET activo = false WHERE id = %s",
                        (tipo_id,)
                    )
                else:
                    # Si no hay usuarios, eliminar físicamente
                    cursor.execute(
                        "DELETE FROM tipos_cuota WHERE id = %s",
                        (tipo_id,)
                    )
                
                conn.commit()
                return cursor.rowcount > 0
        except Exception as e:
            logging.error(f"Error al eliminar tipo de cuota: {str(e)}")
            return False
    

    def obtener_tipo_cuota_por_id(self, tipo_id):
        """Obtiene un tipo de cuota específico por su ID."""
        try:
            with self.get_connection_context() as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    SELECT id, nombre, precio, icono_path, activo, fecha_creacion, 
                           fecha_modificacion, descripcion, duracion_dias
                    FROM tipos_cuota 
                    WHERE id = %s
                """, (tipo_id,))
                
                row = cursor.fetchone()
                if row:
                    return TipoCuota(
                        id=row[0],
                        nombre=row[1],
                        precio=row[2],
                        icono_path=row[3],
                        activo=row[4],
                        fecha_creacion=row[5].isoformat() if row[5] else None,
                        fecha_modificacion=row[6].isoformat() if row[6] else None,
                        descripcion=row[7],
                        duracion_dias=row[8] if row[8] is not None else 30
                    )
                return None
                
        except Exception as e:
            logging.error(f"Error al obtener tipo de cuota por ID {tipo_id}: {e}")
            return None
    

    def obtener_tipos_cuota(self, solo_activos: bool = False) -> List[TipoCuota]:
        """Obtiene todos los tipos de cuota con caché y manejo de lock timeout."""
        cache_key = ('tipos_cuota', 'todos_activos' if solo_activos else 'todos')
        # Intento caché primero
        try:
            cached = self.cache.get('tipos', cache_key)
            if cached is not None:
                return cached
        except Exception:
            pass

        base_sql = "SELECT id, nombre, precio, icono_path, activo, fecha_creacion, descripcion, duracion_dias FROM tipos_cuota"
        if solo_activos:
            base_sql += " WHERE activo = true"
        base_sql += " ORDER BY nombre"
        try:
            with self.readonly_session(lock_ms=800, statement_ms=2000, idle_s=2, seqscan_off=True) as conn:
                with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
                    cursor.execute(base_sql)
                    rows = cursor.fetchall() or []
            tipos = []
            for row in rows:
                tipos.append(
                    TipoCuota(
                        id=row.get('id'),
                        nombre=row.get('nombre') or '',
                        precio=float(row.get('precio') or 0.0),
                        icono_path=row.get('icono_path'),
                        activo=bool(row.get('activo')),
                        fecha_creacion=row.get('fecha_creacion'),
                        descripcion=row.get('descripcion'),
                        duracion_dias=row.get('duracion_dias') or 30,
                    )
                )
            try:
                self.cache.set('tipos', cache_key, tipos)
            except Exception:
                pass
            return tipos
        except Exception as e:
            msg = str(e).lower()
            if 'lock timeout' in msg or 'canceling statement due to lock timeout' in msg:
                try:
                    with self.readonly_session(lock_ms=0, statement_ms=2500, idle_s=2, seqscan_off=False) as conn2:
                        with conn2.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as c2:
                            c2.execute(base_sql)
                            rows2 = c2.fetchall() or []
                    tipos2 = []
                    for row in rows2:
                        tipos2.append(
                            TipoCuota(
                                id=row.get('id'),
                                nombre=row.get('nombre') or '',
                                precio=float(row.get('precio') or 0.0),
                                icono_path=row.get('icono_path'),
                                activo=bool(row.get('activo')),
                                fecha_creacion=row.get('fecha_creacion'),
                                descripcion=row.get('descripcion'),
                                duracion_dias=row.get('duracion_dias') or 30,
                            )
                        )
                    try:
                        self.cache.set('tipos', cache_key, tipos2)
                    except Exception:
                        pass
                    return tipos2
                except Exception:
                    pass
            # Fallback a caché en memoria
            try:
                cached_mem = self.cache.get('tipos', cache_key)
                if cached_mem is not None:
                    return cached_mem
            except Exception:
                pass
            logging.error(f"Error al obtener tipos de cuota (solo_activos={solo_activos}): {e}")
            return []
    

    def obtener_conteo_tipos_cuota(self) -> Dict[str, int]:
        """Obtiene el conteo de usuarios por tipo de cuota, admitiendo nombre o ID y normalizando etiquetas."""
        try:
            with self.get_connection_context() as conn:
                cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
                cursor.execute(
                    """
                    SELECT COALESCE(tc.nombre, u.tipo_cuota, '') AS tipo_cuota
                    FROM usuarios u
                    LEFT JOIN tipos_cuota tc ON u.tipo_cuota = tc.nombre OR u.tipo_cuota::text = tc.id::text
                    WHERE u.activo = true AND u.rol IN ('socio','profesor','miembro')
                    """
                )

                def norm(label: str) -> Optional[str]:
                    s = (label or '').strip().lower()
                    if s in ('', 'n/a', 'na', 'none', 'sin', 'null', '-'):
                        return None
                    replacements = {
                        'estandar': 'Estándar', 'estándar': 'Estándar', 'standard': 'Estándar',
                        'estudiante': 'Estudiante', 'student': 'Estudiante',
                        'funcional': 'Funcional', 'functional': 'Funcional',
                    }
                    return replacements.get(s, s.title())

                resultado: Dict[str, int] = {}
                rows = cursor.fetchall() or []
                for row in rows:
                    etiqueta = norm(row.get('tipo_cuota') or '')
                    if etiqueta:
                        resultado[etiqueta] = resultado.get(etiqueta, 0) + 1

                # Prefill con nombres de tipos conocidos si no hay datos
                if not resultado:
                    try:
                        cur2 = conn.cursor()
                        cur2.execute("SELECT nombre FROM tipos_cuota ORDER BY nombre")
                        for r in cur2.fetchall() or []:
                            etq = norm(r[0] if r and len(r) > 0 else '')
                            if etq:
                                resultado.setdefault(etq, 0)
                    except Exception:
                        pass

                return resultado

        except Exception as e:
            logging.error(f"Error al obtener conteo tipos de cuota: {str(e)}")
            return {}


    def obtener_estadisticas_tipos_cuota(self) -> List[Dict[str, Any]]:
        """Obtiene estadísticas detalladas de tipos de cuota."""
        try:
            with self.get_connection_context() as conn:
                cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
                sql = """
                SELECT 
                    tc.id,
                    tc.nombre,
                    tc.precio,
                    tc.activo,
                    COUNT(u.id) as usuarios_activos,
                    COALESCE(SUM(CASE WHEN u.activo = true THEN 1 ELSE 0 END), 0) as usuarios_con_cuota_activa
                FROM tipos_cuota tc
                LEFT JOIN usuarios u ON u.tipo_cuota = tc.nombre AND u.rol IN ('socio','profesor')
                GROUP BY tc.id, tc.nombre, tc.precio, tc.activo
                ORDER BY tc.nombre
                """
                cursor.execute(sql)
                
                estadisticas = []
                for row in cursor.fetchall():
                    precio = float(row['precio']) if row['precio'] else 0.0
                    usuarios_con_cuota_activa = row['usuarios_con_cuota_activa']
                    ingresos_potenciales = precio * usuarios_con_cuota_activa
                    
                    estadisticas.append({
                        'id': row['id'],
                        'nombre': row['nombre'],
                        'precio': precio,
                        'activo': row['activo'],
                        'usuarios_activos': row['usuarios_activos'],
                        'usuarios_con_cuota_activa': usuarios_con_cuota_activa,
                        'ingresos_potenciales': ingresos_potenciales
                    })
                
                return estadisticas
                
        except Exception as e:
            logging.error(f"Error al obtener estadísticas tipos de cuota: {str(e)}")
            return []


    def exportar_pagos_csv(self, fecha_inicio=None, fecha_fin=None) -> str:
        """Exporta pagos a CSV y retorna la ruta del archivo."""
        try:
            import csv
            import tempfile
            import os
            from datetime import datetime
            
            # Crear archivo temporal
            temp_dir = tempfile.gettempdir()
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"pagos_export_{timestamp}.csv"
            filepath = os.path.join(temp_dir, filename)
            
            with self.get_connection_context() as conn:
                cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
                
                # Construir consulta con filtros de fecha
                sql = """
                SELECT p.id, u.nombre, u.dni, p.monto, p.fecha_pago, p.mes, p.año
                FROM pagos p
                JOIN usuarios u ON p.usuario_id = u.id
                WHERE 1=1
                """
                params = []
                
                if fecha_inicio:
                    sql += " AND p.fecha_pago >= %s"
                    params.append(fecha_inicio)
                if fecha_fin:
                    sql += " AND p.fecha_pago <= %s"
                    params.append(fecha_fin)
                    
                sql += " ORDER BY p.fecha_pago DESC"
                
                cursor.execute(sql, params)
                pagos = cursor.fetchall()
                
                # Escribir CSV
                with open(filepath, 'w', newline='', encoding='utf-8') as csvfile:
                    fieldnames = ['ID', 'Nombre', 'DNI', 'Monto', 'Fecha Pago', 'Mes', 'Año']
                    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                    
                    writer.writeheader()
                    for pago in pagos:
                        writer.writerow({
                            'ID': pago['id'],
                            'Nombre': pago['nombre'],
                            'DNI': pago['dni'],
                            'Monto': pago['monto'],
                            'Fecha Pago': pago['fecha_pago'],
                            'Mes': pago['mes'],
                            'Año': pago['año']
                        })
                
                return filepath
                
        except Exception as e:
            logging.error(f"Error exportando pagos a CSV: {str(e)}")
            return ""
    

    def obtener_resumen_pagos_por_metodo(self, fecha_inicio=None, fecha_fin=None) -> dict:
        """Obtiene resumen de pagos agrupados por método de pago."""
        try:
            with self.get_connection_context() as conn:
                cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
                
                sql = """
                SELECT 
                    COALESCE(metodo_pago_id::text, 'Efectivo') as metodo,
                    COUNT(*) as cantidad,
                    COALESCE(SUM(monto), 0) as total
                FROM pagos
                WHERE 1=1
                """
                params = []
                
                if fecha_inicio:
                    sql += " AND fecha_pago >= %s"
                    params.append(fecha_inicio)
                if fecha_fin:
                    sql += " AND fecha_pago <= %s"
                    params.append(fecha_fin)
                    
                sql += " GROUP BY metodo_pago_id ORDER BY total DESC"
                
                cursor.execute(sql, params)
                resultados = cursor.fetchall()
                
                resumen = {}
                for row in resultados:
                    resumen[row['metodo']] = {
                        'cantidad': row['cantidad'],
                        'total': float(row['total'])
                    }
                
                return resumen
                
        except Exception as e:
            logging.error(f"Error obteniendo resumen de pagos por método: {str(e)}")
            return {}
    

    def analizar_metodos_pago(self) -> dict:
        """Analiza los métodos de pago más utilizados."""
        try:
            with self.get_connection_context() as conn:
                cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
                
                sql = """
                SELECT 
                    COALESCE(metodo_pago_id::text, 'Efectivo') as metodo,
                    COUNT(*) as frecuencia,
                    COALESCE(SUM(monto), 0) as total_monto,
                    COALESCE(AVG(monto), 0) as promedio_monto
                FROM pagos
                WHERE fecha_pago >= CURRENT_DATE - INTERVAL '30 days'
                GROUP BY metodo_pago_id
                ORDER BY frecuencia DESC
                """
                
                cursor.execute(sql)
                resultados = cursor.fetchall()
                
                analisis = {
                    'metodos': [],
                    'total_transacciones': 0,
                    'metodo_preferido': None
                }
                
                total_transacciones = 0
                for row in resultados:
                    metodo_info = {
                        'metodo': row['metodo'],
                        'frecuencia': row['frecuencia'],
                        'total_monto': float(row['total_monto']),
                        'promedio_monto': float(row['promedio_monto'])
                    }
                    analisis['metodos'].append(metodo_info)
                    total_transacciones += row['frecuencia']
                
                analisis['total_transacciones'] = total_transacciones
                if analisis['metodos']:
                    analisis['metodo_preferido'] = analisis['metodos'][0]['metodo']
                
                return analisis
                
        except Exception as e:
            logging.error(f"Error analizando métodos de pago: {str(e)}")
            return {'metodos': [], 'total_transacciones': 0, 'metodo_preferido': None}
    

    def calcular_pago_promedio(self) -> float:
        """Calcula el pago promedio de todos los pagos registrados."""
        try:
            with self.get_connection_context() as conn:
                cursor = conn.cursor()
                
                sql = "SELECT COALESCE(AVG(monto), 0) as promedio FROM pagos"
                cursor.execute(sql)
                resultado = cursor.fetchone()
                
                return float(resultado[0]) if resultado else 0.0
                
        except Exception as e:
            logging.error(f"Error calculando pago promedio: {str(e)}")
            return 0.0
    

    def obtener_pagos_por_fecha(self, fecha_inicio=None, fecha_fin=None) -> list:
        """Obtiene pagos filtrados por rango de fechas."""
        try:
            with self.get_connection_context() as conn:
                cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

                # Columnas explícitas para evitar SELECT * y mejorar planificación
                base_select = """
                    SELECT 
                        p.id,
                        p.usuario_id,
                        p.monto,
                        p.mes,
                        p.año,
                        p.fecha_pago,
                        p.metodo_pago_id,
                        u.nombre AS usuario_nombre,
                        u.dni,
                        mp.nombre AS metodo_pago,
                        COALESCE(agg.concepto_pago, '') AS concepto_pago
                    FROM pagos p
                    JOIN usuarios u ON p.usuario_id = u.id
                    LEFT JOIN metodos_pago mp ON mp.id = p.metodo_pago_id
                    LEFT JOIN LATERAL (
                        SELECT 
                            CASE WHEN COUNT(*) > 0 THEN 
                                STRING_AGG(names.item_name, ' · ' ORDER BY names.first_pos)
                            ELSE '' END AS concepto_pago
                        FROM (
                            SELECT 
                                NULLIF(TRIM(COALESCE(cp.nombre, pd.descripcion)), '') AS item_name,
                                MIN(pd.id) AS first_pos
                            FROM pago_detalles pd
                            LEFT JOIN conceptos_pago cp ON cp.id = pd.concepto_id
                            WHERE pd.pago_id = p.id
                            GROUP BY item_name
                        ) AS names
                        WHERE names.item_name IS NOT NULL
                    ) AS agg ON TRUE
                """

                if fecha_inicio and fecha_fin:
                    # Rango de fechas específico (utiliza índice en fecha_pago)
                    sql = base_select + "\nWHERE p.fecha_pago BETWEEN %s AND %s\nORDER BY p.fecha_pago DESC"
                    cursor.execute(sql, (fecha_inicio, fecha_fin))
                elif fecha_inicio:
                    # Día específico: reescribir como rango [fecha, fecha + 1 día) para usar índice
                    sql = base_select + "\nWHERE p.fecha_pago >= %s AND p.fecha_pago < (%s::date + INTERVAL '1 day')\nORDER BY p.fecha_pago DESC"
                    cursor.execute(sql, (fecha_inicio, fecha_inicio))
                else:
                    # Todos los pagos con orden por fecha
                    sql = base_select + "\nORDER BY p.fecha_pago DESC"
                    cursor.execute(sql)

                return [dict(row) for row in cursor.fetchall()]
                
        except Exception as e:
            logging.error(f"Error obteniendo pagos por fecha: {str(e)}")
            return []
    

    def obtener_pagos_por_rango_fechas(self, fecha_inicio, fecha_fin) -> list:
        """Obtiene pagos en un rango de fechas específico."""
        try:
            with self.get_connection_context() as conn:
                cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
                cursor.execute("""
                    SELECT p.*, u.nombre as usuario_nombre
                    FROM pagos p
                    JOIN usuarios u ON p.usuario_id = u.id
                    WHERE p.fecha_pago BETWEEN %s AND %s
                    ORDER BY p.fecha_pago DESC
                """, (fecha_inicio, fecha_fin))
                return cursor.fetchall()
        except Exception as e:
            logging.error(f"Error obteniendo pagos por rango de fechas: {str(e)}")
            return []
    

    def obtener_pago_actual(self, usuario_id: int, mes: int = None, anio: int = None) -> Optional[Dict]:
        """Obtiene el pago actual de un usuario para el mes y año especificados."""
        try:
            # Si no se especifica mes/año, usar el actual
            if mes is None or anio is None:
                from datetime import datetime
                now = datetime.now()
                mes = mes or now.month
                anio = anio or now.year
            
            with self.get_connection_context() as conn:
                cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
                
                # Primero buscar si existe un pago para el mes/año especificado
                cursor.execute("""
                    SELECT p.*, u.nombre as usuario_nombre, u.telefono,
                           tc.nombre as tipo_cuota_nombre, tc.precio as tipo_cuota_precio
                    FROM pagos p
                    JOIN usuarios u ON p.usuario_id = u.id
                    LEFT JOIN tipos_cuota tc ON u.tipo_cuota = tc.nombre
                    WHERE p.usuario_id = %s 
                    AND p.mes = %s 
                    AND p.año = %s
                    ORDER BY p.fecha_pago DESC
                    LIMIT 1
                """, (usuario_id, mes, anio))
                
                row = cursor.fetchone()
                if row:
                    pago_dict = dict(row)
                    # Calcular fecha de vencimiento (último día del mes)
                    from calendar import monthrange
                    ultimo_dia = monthrange(anio, mes)[1]
                    pago_dict['fecha_vencimiento'] = f"{ultimo_dia:02d}/{mes:02d}/{anio}"
                    pago_dict['vencido'] = True  # Si existe pago, no está vencido
                    return pago_dict
                else:
                    # Si no hay pago, buscar información del usuario para crear recordatorio
                    cursor.execute("""
                        SELECT u.id, u.nombre, u.telefono, u.tipo_cuota,
                               tc.precio as monto
                        FROM usuarios u
                        LEFT JOIN tipos_cuota tc ON u.tipo_cuota = tc.nombre
                        WHERE u.id = %s AND u.activo = true
                    """, (usuario_id,))
                    
                    user_row = cursor.fetchone()
                    if user_row:
                        user_dict = dict(user_row)
                        # Calcular fecha de vencimiento (último día del mes)
                        from calendar import monthrange
                        ultimo_dia = monthrange(anio, mes)[1]
                        user_dict['fecha_vencimiento'] = f"{ultimo_dia:02d}/{mes:02d}/{anio}"
                        user_dict['monto'] = user_dict.get('monto', 0)
                        user_dict['vencido'] = True  # No hay pago, está vencido
                        user_dict['usuario_nombre'] = user_dict['nombre']
                        return user_dict
                    
                return None
                
        except Exception as e:
            logging.error(f"Error al obtener pago actual: {e}")
            return None
    

    def procesar_vencimientos_automaticos(self) -> dict:
        """
        Procesa automáticamente los vencimientos de cuotas usando duracion_dias del tipo de cuota.
        Actualiza las fechas de próximo vencimiento basándose en la duración específica de cada tipo de cuota.
        
        Returns:
            dict: Estadísticas del procesamiento con usuarios actualizados y errores
        """
        try:
            usuarios_actualizados = 0
            errores = []
            updated_user_ids = []
            processed_count = 0
            with self.atomic_transaction(isolation_level="REPEATABLE READ") as conn:
                with conn.cursor() as cursor:
                    # Obtener usuarios con pagos recientes que necesitan actualización de fecha de vencimiento
                    cursor.execute("""
                        SELECT DISTINCT 
                            u.id, 
                            u.nombre, 
                            '' AS apellido, 
                            p.fecha_pago, 
                            u.tipo_cuota, 
                            tc.duracion_dias
                        FROM usuarios u
                        INNER JOIN pagos p ON u.id = p.usuario_id
                        INNER JOIN tipos_cuota tc ON u.tipo_cuota = tc.nombre
                        WHERE p.fecha_pago >= CURRENT_DATE - INTERVAL '7 days'
                        AND u.activo = true
                        ORDER BY p.fecha_pago DESC
                    """)
                    
                    usuarios_para_procesar = cursor.fetchall()
                    processed_count = len(usuarios_para_procesar)
                    
                    for usuario_data in usuarios_para_procesar:
                        usuario_id, nombre, apellido, fecha_pago, tipo_cuota, duracion_dias = usuario_data
                        
                        try:
                            # Calcular nueva fecha de vencimiento usando duracion_dias
                            nueva_fecha_vencimiento = fecha_pago + timedelta(days=duracion_dias)
                            
                            # Actualizar fecha_proximo_vencimiento del usuario
                            cursor.execute("""
                                UPDATE usuarios 
                                SET fecha_proximo_vencimiento = %s,
                                    cuotas_vencidas = 0,
                                    ultimo_pago = %s,
                                    fecha_modificacion = CURRENT_TIMESTAMP
                                WHERE id = %s
                            """, (nueva_fecha_vencimiento, fecha_pago, usuario_id))
                            
                            if cursor.rowcount > 0:
                                usuarios_actualizados += 1
                                updated_user_ids.append(usuario_id)
                                logging.info(f"Fecha de vencimiento actualizada para {nombre} {apellido} (ID: {usuario_id}): {nueva_fecha_vencimiento} (usando {duracion_dias} días del tipo '{tipo_cuota}')")
                            
                        except Exception as e:
                            error_msg = f"Error procesando usuario {nombre} {apellido} (ID: {usuario_id}): {str(e)}"
                            errores.append(error_msg)
                            logging.error(error_msg)
            
            # Invalida caché de usuarios actualizados tras el commit
            for uid in set(updated_user_ids):
                try:
                    self.cache.invalidate('usuarios', uid)
                except Exception as cache_e:
                    logging.warning(f"No se pudo invalidar la caché para el usuario {uid}: {cache_e}")
            
            resultado = {
                'usuarios_procesados': processed_count,
                'usuarios_actualizados': usuarios_actualizados,
                'errores': len(errores),
                'detalles_errores': errores,
                'fecha_procesamiento': datetime.now().isoformat()
            }
            
            logging.info(f"Procesamiento automático de vencimientos completado: {usuarios_actualizados} usuarios actualizados de {processed_count} procesados")
            
            return resultado
            
        except Exception as e:
            error_msg = f"Error en procesamiento automático de vencimientos: {str(e)}"
            logging.error(error_msg)
            return {
                'usuarios_procesados': 0,
                'usuarios_actualizados': 0,
                'errores': 1,
                'detalles_errores': [error_msg],
                'fecha_procesamiento': datetime.now().isoformat()
            }


    def _get_reporte_pagos(self):
        """Obtiene reporte de pagos por mes"""
        mes = self.params.get('mes', date.today().month)
        anio = self.params.get('anio', date.today().year)
        
        with self.db_manager.readonly_session() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute("""
                    SELECT 
                        DATE_TRUNC('day', fecha_pago) as fecha,
                        COUNT(*) as cantidad_pagos,
                        SUM(monto) as total_monto
                    FROM pagos
                    WHERE mes = %s AND año = %s
                    GROUP BY DATE_TRUNC('day', fecha_pago)
                    ORDER BY fecha
                """, (mes, anio))
                return cur.fetchall()
    

    def _bulk_update_pagos(self):
        """Actualiza múltiples pagos"""
        total = len(self.data)
        updated = 0
        
        with self.db_manager.connection() as conn:
            with conn.cursor() as cur:
                for i, pago in enumerate(self.data):
                    if not self._is_running:
                        break
                    
                    try:
                        cur.execute("""
                            UPDATE pagos 
                            SET monto = %s, fecha_pago = %s, metodo_pago_id = %s
                            WHERE id = %s
                        """, (
                            pago.get('monto'),
                            pago.get('fecha_pago'),
                            pago.get('metodo_pago_id'),
                            pago.get('id')
                        ))
                        updated += cur.rowcount
                        
                        if i % 10 == 0:
                            progress = int((i / total) * 100)
                            self.progress.emit(progress)
                            
                    except Exception as e:
                        logging.warning(f"Error actualizando pago {pago.get('id')}: {e}")
                        continue
                
                conn.commit()
        
        self.progress.emit(100)
        return {'updated': updated, 'total': total}
    
