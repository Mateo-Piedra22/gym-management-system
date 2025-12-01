import logging
import threading
from typing import Optional, Dict, List, Any, Tuple
from datetime import datetime, date
from .connection import ConnectionPool, CacheManager, database_retry
from .repositories.user_repository import UserRepository
from .repositories.payment_repository import PaymentRepository
from .repositories.attendance_repository import AttendanceRepository
from .repositories.gym_repository import GymRepository
from .repositories.teacher_repository import TeacherRepository
from .repositories.audit_repository import AuditRepository
from .repositories.whatsapp_repository import WhatsappRepository
from .repositories.reports_repository import ReportsRepository
import os

class DatabaseManager:
    # --- Facade Methods ---
    def __init__(self, connection_params: dict = None):
        """
        Inicializa el gestor de base de datos PostgreSQL optimizado para conexión remota São Paulo -> Argentina
        
        Args:
            connection_params: Diccionario con parámetros de conexión PostgreSQL
                - host: Servidor de base de datos
                - port: Puerto (default: 5432)
                - database: Nombre de la base de datos
                - user: Usuario de la base de datos
                - password: Contraseña
                - sslmode: Modo SSL (default: 'prefer')
        """
        if connection_params is None:
            connection_params = self._get_default_connection_params()
        
        # Configuración optimizada para conexión remota São Paulo -> Argentina
        optimized_params = connection_params.copy()
        try:
            ct_env = os.getenv('DB_CONNECT_TIMEOUT')
            ct_val = int(ct_env) if (ct_env and ct_env.strip()) else int(connection_params.get('connect_timeout') or 30)
        except Exception:
            ct_val = int(connection_params.get('connect_timeout') or 30)
        try:
            app_env = os.getenv('DB_APPLICATION_NAME')
            app_name = (app_env.strip() if app_env else 'GymManagementSystem_Argentina')
        except Exception:
            app_name = 'GymManagementSystem_Argentina'
        optimized_params.update({
            'connect_timeout': ct_val,
            'application_name': app_name,
        })
        try:
            host_lc = str(connection_params.get('host') or '').lower()
            if ('neon.tech' in host_lc) or ('neon' in host_lc):
                optimized_params['options'] = ''
            else:
                base_opts = str(optimized_params.get('options') or '').strip()
                extra_opts = "-c TimeZone=America/Argentina/Buenos_Aires -c statement_timeout=60s -c lock_timeout=10s -c idle_in_transaction_session_timeout=30s"
                opts = (base_opts + (' ' if base_opts else '') + extra_opts).strip()
                optimized_params['options'] = opts
        except Exception:
            optimized_params['options'] = "-c TimeZone=America/Argentina/Buenos_Aires -c statement_timeout=60s -c lock_timeout=10s -c idle_in_transaction_session_timeout=30s"
        
        self.connection_params = optimized_params
        # Atributo de compatibilidad con código SQLite existente
        self.db_path = f"postgresql://{connection_params.get('user', 'postgres')}@{connection_params.get('host', 'localhost')}:{connection_params.get('port', 5432)}/{connection_params.get('database', 'gym_management')}"
        self._initializing = False
        self.audit_logger = None
        self.logger = logging.getLogger(__name__)
        try:
            _safe = {
                'host': connection_params.get('host'),
                'port': connection_params.get('port'),
                'database': connection_params.get('database'),
                'user': connection_params.get('user'),
                'sslmode': connection_params.get('sslmode'),
                'connect_timeout': optimized_params.get('connect_timeout'),
                'application_name': optimized_params.get('application_name'),
                'options_present': bool(optimized_params.get('options')),
            }
            self.logger.info(f"DatabaseManager: init con params optimizados para conexión remota={_safe}")
        except Exception:
            pass
        
        try:
            appname_l = str(optimized_params.get('application_name') or '').lower()
        except Exception:
            appname_l = ''
        try:
            pool_env = os.getenv('ADMIN_DB_POOL_MAX') if ('admin' in appname_l) else os.getenv('DB_POOL_MAX')
            pool_max = int(pool_env) if (pool_env and pool_env.strip()) else 10
        except Exception:
            pool_max = 3
        try:
            if (os.getenv('VERCEL') or os.getenv('VERCEL_ENV') or os.getenv('RAILWAY')):
                pool_max = max(2, min(pool_max, 4))
        except Exception:
            pass
        try:
            tout_env = os.getenv('DB_POOL_TIMEOUT')
            tout = float(tout_env) if (tout_env and tout_env.strip()) else 8.0
        except Exception:
            tout = 8.0
        self._connection_pool = ConnectionPool(
            connection_params=optimized_params,
            max_connections=pool_max,
            timeout=tout
        )
        try:
            self.logger.info(f"DatabaseManager: pool creado max_connections={pool_max}, timeout={tout}s")
        except Exception:
            pass
        
        self._cache_config = {
            'usuarios': {'duration': 900, 'max_size': 1000},      # 15 minutos, 1000 usuarios
            'pagos': {'duration': 600, 'max_size': 500},        # 10 minutos, 500 pagos
            'asistencias': {'duration': 300, 'max_size': 300},   # 5 minutos, 300 asistencias
            'reportes': {'duration': 1200, 'max_size': 200},    # 20 minutos, 200 reportes
            'profesores': {'duration': 1800, 'max_size': 200},   # 30 minutos, 200 profesores
            'clases': {'duration': 600, 'max_size': 300},       # 10 minutos, 300 clases
            'ejercicios': {'duration': 900, 'max_size': 1000},   # 15 minutos, 1000 ejercicios
            'rutinas': {'duration': 600, 'max_size': 600},       # 10 minutos, 600 rutinas y vistas asociadas
            'clase_ejercicios': {'duration': 600, 'max_size': 600}, # 10 minutos, ejercicios por clase
            'clase_ejercicios_detalle': {'duration': 600, 'max_size': 600}, # 10 minutos, ejercicios por clase con metadatos
            'config': {'duration': 3600, 'max_size': 200},      # 1 hora, 200 configs
            'metodos_pago': {'duration': 1800, 'max_size': 200},  # 30 minutos, 200 métodos
            'conceptos_pago': {'duration': 1800, 'max_size': 300} # 30 minutos, 300 conceptos
        }
        self.cache = CacheManager(self._cache_config)
        self._cache_cleanup_thread = None
        self._stop_event = threading.Event()
        # Caché de columnas por tabla para construir SELECT explícitos sin '*'
        self._table_columns_cache: Dict[str, List[str]] = {}
        self._table_columns_lock = threading.RLock()
        
        # Prepared statements para consultas frecuentes
        self._prepared_statements = {
            'usuarios_by_rol': "PREPARE get_usuarios_by_rol(TEXT) AS SELECT id, nombre, dni, telefono, pin, rol, notas, fecha_registro, activo, tipo_cuota, fecha_proximo_vencimiento, cuotas_vencidas, ultimo_pago FROM usuarios WHERE rol = $1 ORDER BY nombre",
            'usuarios_by_id': "PREPARE get_usuario_by_id(BIGINT) AS SELECT id, nombre, dni, telefono, pin, rol, notas, fecha_registro, activo, tipo_cuota, fecha_proximo_vencimiento, cuotas_vencidas, ultimo_pago FROM usuarios WHERE id = $1",
            'usuarios_active': "PREPARE get_usuarios_active() AS SELECT id, nombre, dni, telefono, pin, rol, notas, fecha_registro, activo, tipo_cuota, fecha_proximo_vencimiento, cuotas_vencidas, ultimo_pago FROM usuarios WHERE activo = true ORDER BY nombre",
            'pagos_by_usuario': "PREPARE get_pagos_by_usuario(BIGINT) AS SELECT id, usuario_id, monto, mes, año, fecha_pago, metodo_pago_id FROM pagos WHERE usuario_id = $1 ORDER BY fecha_pago DESC",
            'asistencias_today': "PREPARE get_asistencias_today() AS SELECT id, usuario_id, fecha, hora_registro FROM asistencias WHERE fecha = CURRENT_DATE ORDER BY hora_registro DESC",
            'asistencias_by_usuario_date': "PREPARE get_asistencias_by_usuario_date(BIGINT, DATE, DATE) AS SELECT id, usuario_id, fecha, hora_registro FROM asistencias WHERE usuario_id = $1 AND fecha BETWEEN $2 AND $3 ORDER BY fecha DESC",
            'clases_active': "PREPARE get_clases_active() AS SELECT id, nombre, descripcion, activa FROM clases WHERE activa = true ORDER BY nombre",
            'profesores_active': "PREPARE get_profesores_active() AS SELECT p.id, u.nombre, u.telefono FROM profesores p JOIN usuarios u ON u.id = p.usuario_id ORDER BY u.nombre"
        }
        
        # Estadísticas de rendimiento
        self._query_stats = {
            'total_queries': 0,
            'slow_queries': 0,
            'cache_hits': 0,
            'cache_misses': 0,
            'average_query_time': 0.0
        }
        # Locks y caché ligera para estadísticas de rendimiento
        self._query_stats_lock = threading.RLock()
        self._perf_stats_cache = {'value': None, 'expires_at': 0.0}
        # TTL corto para evitar recomputaciones frecuentes bajo carga (segundos)
        self._perf_stats_cache_ttl = 3.0
        self._query_time_threshold = 2.0  # 2 segundos para considerar una consulta lenta

        # Initialize Repositories
        self.user_repo = UserRepository(self._connection_pool, self.cache, self.logger)
        self.payment_repo = PaymentRepository(self._connection_pool, self.cache, self.logger)
        self.attendance_repo = AttendanceRepository(self._connection_pool, self.cache, self.logger)
        self.gym_repo = GymRepository(self._connection_pool, self.cache, self.logger)
        self.teacher_repo = TeacherRepository(self._connection_pool, self.cache, self.logger)
        self.audit_repo = AuditRepository(self._connection_pool, self.cache, self.logger)
        self.whatsapp_repo = WhatsappRepository(self._connection_pool, self.cache, self.logger)
        self.reports_repo = ReportsRepository(self._connection_pool, self.cache, self.logger)

    def get_table_columns(self, table_name: str) -> List[str]:
        """Devuelve la lista de columnas de una tabla en orden, con caché.

        Usa information_schema para descubrir columnas y cachea el resultado
        para minimizar consultas adicionales.
        """
        try:
            # Cache hit
            cols = self._table_columns_cache.get(table_name)
            if cols:
                return cols
        except Exception:
            pass
        with self._table_columns_lock:
            # Revalidar dentro del lock
            cols2 = self._table_columns_cache.get(table_name)
            if cols2:
                return cols2
            try:
                with self.readonly_session(lock_ms=0, statement_ms=2000, idle_s=2, seqscan_off=True) as conn:
                    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                        cur.execute(
                            """
                            SELECT column_name
                            FROM information_schema.columns
                            WHERE table_schema = 'public' AND table_name = %s
                            ORDER BY ordinal_position
                            """,
                            (table_name,)
                        )
                        rows = cur.fetchall() or []
                        names = [r['column_name'] for r in rows]
                        # Cachear
                        self._table_columns_cache[table_name] = names
                        return names
            except Exception:
                # Fallback: devolver vacío para evitar romper llamadas; el caller puede usar '*'
                return []

    def invalidate_table_columns_cache(self, table_name: str) -> None:
        try:
            with self._table_columns_lock:
                if table_name in self._table_columns_cache:
                    try:
                        del self._table_columns_cache[table_name]
                    except Exception:
                        self._table_columns_cache.pop(table_name, None)
        except Exception:
            pass

    def _column_exists(self, conn, table_name: str, column_name: str) -> bool:
        """Verifica si existe una columna en una tabla (schema público)."""
        try:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT EXISTS(
                        SELECT 1 FROM information_schema.columns
                        WHERE table_schema = 'public' AND table_name = %s AND column_name = %s
                    ) AS exists
                    """,
                    (table_name, column_name)
                )
                row = cur.fetchone()
                return bool(row and row.get('exists'))
        except Exception:
            return False

    def ensure_rutina_uuid_ready(self, conn) -> bool:
        """Asegura que la columna uuid_rutina exista y tenga valores únicos.

        - Crea la columna si no existe (intenta con DEFAULT gen_random_uuid())
        - Rellena valores faltantes con uuid4() en caso necesario
        - Crea índice único si falta

        Devuelve True si la columna existe al finalizar (aunque sin default), False si no se pudo asegurar.
        """
        try:
            has_col = self._column_exists(conn, 'rutinas', 'uuid_rutina')
            if not has_col:
                with conn.cursor() as cur:
                    # Intentar crear extensión y columna con default gen_random_uuid()
                    created = False
                    try:
                        cur.execute('CREATE EXTENSION IF NOT EXISTS pgcrypto')
                        cur.execute("ALTER TABLE rutinas ADD COLUMN uuid_rutina TEXT DEFAULT gen_random_uuid()::text")
                        created = True
                    except Exception:
                        # Fallback: crear sin default
                        try:
                            cur.execute("ALTER TABLE rutinas ADD COLUMN uuid_rutina TEXT")
                            created = True
                        except Exception:
                            created = False
                    if created:
                        try:
                            cur.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_rutinas_uuid ON rutinas (uuid_rutina)")
                        except Exception:
                            pass
                        conn.commit()
                    else:
                        conn.rollback()
                        return False
                has_col = True

            # Rellenar valores nulos con UUIDs
            try:
                with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                    cur.execute("SELECT id FROM rutinas WHERE uuid_rutina IS NULL OR uuid_rutina = ''")
                    rows = cur.fetchall() or []
                    if rows:
                        # Generar y actualizar en lotes
                        for r in rows:
                            rid = r['id'] if isinstance(r, dict) else r[0]
                            new_uuid = str(uuid.uuid4())
                            try:
                                cur.execute("UPDATE rutinas SET uuid_rutina = %s WHERE id = %s", (new_uuid, rid))
                            except Exception:
                                # Si falla por duplicado improbable, generar otro
                                try:
                                    cur.execute("UPDATE rutinas SET uuid_rutina = %s WHERE id = %s", (str(uuid.uuid4()), rid))
                                except Exception:
                                    pass
                        try:
                            cur.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_rutinas_uuid ON rutinas (uuid_rutina)")
                        except Exception:
                            pass
                        conn.commit()
            except Exception:
                # No bloquear si algo falla al rellenar
                try:
                    conn.rollback()
                except Exception:
                    pass

            return has_col
        except Exception:
            return False

    def initialize_prepared_statements(self):
        """Inicializa las declaraciones preparadas para consultas frecuentes"""
        try:
            with self.connection() as conn:
                with conn.cursor() as cur:
                    for stmt_name, stmt_sql in self._prepared_statements.items():
                        try:
                            cur.execute(stmt_sql)
                            self.logger.info(f"Declaración preparada creada: {stmt_name}")
                        except Exception as e:
                            self.logger.warning(f"Error al crear declaración preparada {stmt_name}: {e}")
                    
                    conn.commit()
                    self.logger.info("Declaraciones preparadas inicializadas exitosamente")
        except Exception as e:
            self.logger.error(f"Error al inicializar declaraciones preparadas: {e}")

    def execute_prepared_query(self, stmt_name: str, params: tuple = ()) -> List[Dict]:
        """Ejecuta una declaración preparada con monitoreo de rendimiento"""
        start_time = time.time()
        try:
            with self.readonly_session(lock_ms=0, statement_ms=3000, idle_s=2, seqscan_off=True) as conn:
                with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                    # Construir la consulta EXECUTE con parámetros
                    execute_sql = f"EXECUTE {stmt_name}"
                    if params:
                        execute_sql += f"({', '.join(['%s'] * len(params))})"
                    
                    cur.execute(execute_sql, params)
                    results = cur.fetchall()
                    
                    # Actualizar estadísticas
                    query_time = time.time() - start_time
                    self._update_query_stats(query_time)
                    
                    return results
                    
        except Exception as e:
            query_time = time.time() - start_time
            self.logger.warning(f"Error en declaración preparada {stmt_name} ({query_time:.2f}s): {e}")
            raise

    def _update_query_stats(self, query_time: float):
        return self.reports_repo._update_query_stats(query_time)

    def get_query_performance_stats(self) -> Dict:
        return self.reports_repo.get_query_performance_stats()

    def test_connection(params: Optional[dict] = None, timeout_seconds: int = 5) -> bool:
        """Prueba una conexión simple a PostgreSQL.

        - Si `params` es None, intenta resolver parámetros desde entorno/config.
        - Respeta `connect_timeout` provisto, con mínimo de seguridad.
        - Ejecuta `SELECT 1` y cierra la conexión.
        """
        try:
            # Resolver parámetros: usar los provistos o intentar leer entorno/config de forma liviana
            if not params:
                try:
                    # Fallback mínimo: leer entorno y defaults razonables sin instanciar la clase
                    host = str(os.getenv('DB_HOST', 'localhost'))
                    port = int(os.getenv('DB_PORT', 5432))
                    database = str(os.getenv('DB_NAME', 'gimnasio'))
                    user = str(os.getenv('DB_USER', 'postgres'))
                    sslmode = str(os.getenv('DB_SSLMODE', 'prefer'))
                    connect_timeout = int(os.getenv('DB_CONNECT_TIMEOUT', timeout_seconds))
                    application_name = str(os.getenv('DB_APPLICATION_NAME', 'gym_management_system'))

                    # Opciones de sesión básicas para limitar tiempos
                    options_parts = []
                    st_timeout = str(os.getenv('DB_STATEMENT_TIMEOUT', '4s'))
                    if st_timeout:
                        options_parts.append(f"-c statement_timeout={st_timeout}")
                    lk_timeout = str(os.getenv('DB_LOCK_TIMEOUT', '2s'))
                    if lk_timeout:
                        options_parts.append(f"-c lock_timeout={lk_timeout}")
                    idle_trx_timeout = str(os.getenv('DB_IDLE_IN_TRX_TIMEOUT', '30s'))
                    if idle_trx_timeout:
                        options_parts.append(f"-c idle_in_transaction_session_timeout={idle_trx_timeout}")
                    tz = str(os.getenv('DB_TIME_ZONE', 'America/Argentina/Buenos_Aires'))
                    if tz:
                        options_parts.append(f"-c TimeZone={tz}")
                    options = " ".join(options_parts).strip()

                    params = {
                        'host': host,
                        'port': port,
                        'database': database,
                        'user': user,
                        'password': str(os.getenv('DB_PASSWORD', '')),
                        'sslmode': sslmode,
                        'connect_timeout': connect_timeout,
                        'application_name': application_name,
                        'options': options,
                    }
                except Exception:
                    params = {}

            # Mapear a argumentos de psycopg2
            test_params = {
                'host': params.get('host', 'localhost'),
                'port': int(params.get('port', 5432) or 5432),
                'dbname': params.get('database') or params.get('dbname') or 'gimnasio',
                'user': params.get('user', 'postgres'),
                'password': params.get('password', ''),
                'sslmode': params.get('sslmode', 'prefer'),
                'connect_timeout': int(params.get('connect_timeout', timeout_seconds) or timeout_seconds),
                'application_name': params.get('application_name', 'gym_management_system'),
            }
            opts = params.get('options')
            if opts:
                test_params['options'] = opts
            try:
                _safe = {
                    'host': test_params.get('host'),
                    'port': test_params.get('port'),
                    'dbname': test_params.get('dbname'),
                    'user': test_params.get('user'),
                    'sslmode': test_params.get('sslmode'),
                    'connect_timeout': test_params.get('connect_timeout'),
                    'application_name': test_params.get('application_name'),
                    'options_present': ('options' in test_params),
                }
                logging.debug(f"DatabaseManager.test_connection: intentando con {_safe}")
            except Exception:
                pass

            conn = psycopg2.connect(**test_params)
            try:
                with conn.cursor() as cur:
                    cur.execute("SELECT 1")
                    cur.fetchone()
            finally:
                try:
                    conn.close()
                except Exception:
                    pass
            try:
                logging.info("DatabaseManager.test_connection: conexión exitosa")
            except Exception:
                pass
            return True
        except Exception as e:
            try:
                logging.warning(f"DatabaseManager.test_connection: fallo de conexión ({type(e).__name__})")
            except Exception:
                pass
            return False

    def _apply_readonly_timeouts(self, cursor, lock_ms: int = 800, statement_ms: int = 1500, idle_s: int = 2):
        return self.gym_repo._apply_readonly_timeouts(cursor, lock_ms, statement_ms, idle_s)

    def readonly_session(self, lock_ms: int = 800, statement_ms: int = 1500, idle_s: int = 2, seqscan_off: bool = True):
        """Contexto de sesión de solo lectura endurecida.

        - Activa autocommit para evitar transacciones largas en lecturas.
        - Aplica SET LOCAL de lock_timeout, statement_timeout e idle_in_transaction_session_timeout.
        - Opcionalmente desactiva seqscan para favorecer índices.
        """
        try:
            self.logger.debug(f"readonly_session: start lock_ms={lock_ms}, statement_ms={statement_ms}, idle_s={idle_s}, seqscan_off={seqscan_off}")
        except Exception:
            pass
        with self.get_connection_context() as conn:
            # Activar autocommit de forma temporal
            prev_autocommit = getattr(conn, 'autocommit', None)
            try:
                conn.autocommit = True
            except Exception:
                prev_autocommit = None
            try:
                cursor = conn.cursor()
                try:
                    self._apply_readonly_timeouts(cursor, lock_ms=lock_ms, statement_ms=statement_ms, idle_s=idle_s)
                except Exception:
                    pass
                if seqscan_off:
                    try:
                        cursor.execute("SET LOCAL enable_seqscan = off")
                    except Exception:
                        pass
                # Cerrar cursor de configuración; el consumidor abrirá el suyo propio si requiere factory especial
                try:
                    cursor.close()
                except Exception:
                    pass
                yield conn
            finally:
                # Restaurar autocommit si era distinto
                try:
                    if prev_autocommit is not None:
                        conn.autocommit = prev_autocommit
                except Exception:
                    pass
                try:
                    self.logger.debug("readonly_session: end")
                except Exception:
                    pass

    def _init_database_once_deferred(self):
        """Inicializa la base de datos una sola vez por proceso, en background si es posible."""
        global _INIT_ONCE_DONE
        if _INIT_ONCE_DONE:
            return
        def _do_init():
            global _INIT_ONCE_DONE
            with _INIT_ONCE_LOCK:
                if _INIT_ONCE_DONE:
                    return
                try:
                    self.inicializar_base_datos()
                    _INIT_ONCE_DONE = True
                    try:
                        logging.info("Inicialización de base de datos completada (una vez)")
                    except Exception:
                        pass
                except Exception as e:
                    try:
                        logging.error(f"Error en inicialización única de DB: {e}")
                    except Exception:
                        pass
        try:
            threading.Thread(target=_do_init, daemon=True).start()
        except Exception:
            # Fallback: ejecutar sincrónicamente si falló el hilo
            _do_init()

    def get_owner_password_cached(self, ttl_seconds: int = 600) -> Optional[str]:
        return self.gym_repo.get_owner_password_cached(ttl_seconds)

    def get_owner_user_cached(self, ttl_seconds: int = 600, timeout_ms: int = 1200) -> Optional[dict]:
        return self.user_repo.get_owner_user_cached(ttl_seconds, timeout_ms)

    def prefetch_owner_credentials_async(self, ttl_seconds: int = 600):
        return self.gym_repo.prefetch_owner_credentials_async(ttl_seconds)

    def _is_write_operation(self, func_name: str) -> bool:
        """Heurística mínima para detectar operaciones de escritura."""
        prefixes = (
            'crear_', 'actualizar_', 'eliminar_', 'registrar_', 'inscribir_',
            'finalizar_', 'activar_', 'desactivar_', 'asignar_', 'desasignar_',
            'procesar_', 'guardar_', 'set_', 'insertar_', 'borrar_'
        )
        return func_name.startswith(prefixes)

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
        return self.teacher_repo.actualizar_profesor_sesion(
            sesion_id,
            fecha=fecha,
            hora_inicio=hora_inicio,
            hora_fin=hora_fin,
            tipo_actividad=tipo_actividad,
            minutos_totales=minutos_totales,
            timeout_ms=timeout_ms
        )

    def eliminar_profesor_sesion(self, sesion_id: int) -> Dict[str, Any]:
        return self.teacher_repo.eliminar_profesor_sesion(sesion_id)

    def obtener_minutos_proyectados_profesor_rango(self, profesor_id: int, fecha_inicio: str, fecha_fin: str) -> Dict[str, Any]:
        return self.teacher_repo.obtener_minutos_proyectados_profesor_rango(profesor_id, fecha_inicio, fecha_fin)

    def _is_read_operation(self, func_name: str) -> bool:
        """Heurística para detectar operaciones de lectura frecuentes."""
        prefixes = (
            'obtener_', 'get_', 'listar_', 'buscar_', 'consultar_', 'filtrar_'
        )
        return func_name.startswith(prefixes)

    def _get_default_offline_return(self, func_name: str):
        """Retornos seguros mínimos para no romper flujo en modo offline.

        Cubre tanto operaciones de escritura como lecturas frecuentes con heurísticas.
        """
        # Escrituras: IDs temporales o confirmación booleana
        if func_name.startswith(('crear_', 'insertar_', 'registrar_')):
            return -1  # ID temporal negativo
        if func_name.startswith(('actualizar_', 'eliminar_', 'inscribir_', 'finalizar_', 'activar_', 'desactivar_', 'asignar_', 'desasignar_', 'guardar_', 'set_')):
            return True  # Éxito lógico

        # Lecturas comunes: listas devuelven vacío, objetos devuelven None
        if func_name.startswith(('obtener_', 'get_', 'listar_', 'buscar_', 'consultar_', 'filtrar_')):
            plural_hints = ('listar', 'buscar', 'filtrar', 'todos', '_all', '_usuarios', '_pagos', '_clases', '_rutinas', '_asistencias', '_etiquetas')
            if any(h in func_name for h in plural_hints) or func_name.endswith('s'):
                return []

            # Casos puntuales conocidos
            if func_name in (
                'obtener_configuracion_whatsapp',
                'get_whatsapp_config',
            ):
                return {
                    'whatsapp_enabled': False,
                    'api_key': None,
                    'phone_number_id': None,
                    'business_account_id': None,
                    'templates': {},
                }

            # Por defecto para lecturas singulares
            return None

        # Por defecto, retornar None
        return None

    def _get_default_connection_params(self) -> dict:
        return self.gym_repo._get_default_connection_params()

    def obtener_conexion(self):
        """Obtiene una conexión optimizada del pool de conexiones"""
        if hasattr(self, '_initializing') and self._initializing:
            return self._crear_conexion_directa()
        
        start_time = time.time()
        try:
            conn = self._connection_pool.get_connection()
            
            query_time = time.time() - start_time
            self._track_query_performance(query_time)
            # Registrar éxito para el Circuit Breaker
            try:
                self._cb_register_success()
            except Exception:
                pass
            
            return conn
        except Exception as e:
            logging.error(f"Error al obtener conexión: {e}")
            try:
                self._cb_register_failure(e)
            except Exception:
                pass
            raise

    def get_connection_context(self):
        """Context manager para manejo automático de conexiones"""
        # Circuit Breaker: bloquear temprano si está abierto
        try:
            if getattr(self, '_cb_is_open', False):
                open_until = getattr(self, '_cb_open_until', 0.0)
                if time.time() < float(open_until):
                    try:
                        self.logger.warning(f"get_connection_context: circuito abierto; bloqueado hasta {open_until:.3f}")
                    except Exception:
                        pass
                    raise RuntimeError('Database circuit open')
                else:
                    # Semi-apertura: permitir un intento y cerrar si falla
                    setattr(self, '_cb_is_open', False)
        except Exception:
            pass

        if hasattr(self, '_initializing') and self._initializing:
            conn = self._crear_conexion_directa()
            try:
                self.logger.debug("get_connection_context: conexión directa adquirida (inicializando)")
            except Exception:
                pass
            try:
                # Timeouts de sesión aplicados vía 'options' en la conexión
                try:
                    yield conn
                except Exception as e:
                    try:
                        self._cb_register_failure(e)
                    except Exception:
                        pass
                    raise
                else:
                    try:
                        self._cb_register_success()
                    except Exception:
                        pass
            finally:
                try:
                    self.logger.debug("get_connection_context: conexión directa liberada")
                except Exception:
                    pass
                conn.close()
        else:
            with self._connection_pool.connection() as conn:
                # Timeouts de sesión aplicados vía 'options' en la conexión
                try:
                    self.logger.debug("get_connection_context: conexión de pool adquirida")
                except Exception:
                    pass
                try:
                    yield conn
                except Exception as e:
                    try:
                        self._cb_register_failure(e)
                    except Exception:
                        pass
                    raise
                else:
                    try:
                        self._cb_register_success()
                    except Exception:
                        pass
                    try:
                        self.logger.debug("get_connection_context: conexión de pool liberada")
                    except Exception:
                        pass

    def _cb_register_failure(self, error: Exception):
        return self.gym_repo._cb_register_failure(error)

    def _cb_register_success(self):
        return self.gym_repo._cb_register_success()

    def is_circuit_open(self) -> bool:
        return self.gym_repo.is_circuit_open()

    def get_circuit_state(self) -> dict:
        return self.gym_repo.get_circuit_state()

    def atomic_transaction(self, isolation_level: Optional[str] = None):
        """Context manager para transacciones atómicas con manejo robusto de errores.
        Opcionalmente permite fijar el nivel de aislamiento como primera instrucción.
        """
        max_retries = 3
        base_delay = 0.1
        
        for attempt in range(max_retries):
            if hasattr(self, '_initializing') and self._initializing:
                conn = self._crear_conexion_directa()
            else:
                conn = self._connection_pool.get_connection()
            
            try:
                # Configurar transacción
                conn.autocommit = False
                
                # Aplicar aislamiento como PRIMERA instrucción si se solicita
                if isolation_level:
                    valid_levels = {
                        'READ UNCOMMITTED', 'READ COMMITTED', 'REPEATABLE READ', 'SERIALIZABLE'
                    }
                    level = isolation_level.upper()
                    if level not in valid_levels:
                        raise ValueError(f"Nivel de aislamiento inválido: {isolation_level}")
                    with conn.cursor() as iso_cur:
                        iso_cur.execute(f"SET TRANSACTION ISOLATION LEVEL {level}")
                
                # Configuraciones adicionales para transacciones robustas
                with conn.cursor() as setup_cursor:
                    setup_cursor.execute("SET statement_timeout = '30s'")
                    setup_cursor.execute("SET lock_timeout = '5s'")
                    setup_cursor.execute("SET idle_in_transaction_session_timeout = '60s'")
                    setup_cursor.execute("SET TIME ZONE 'America/Argentina/Buenos_Aires'")
                
                logging.debug(f"Iniciando transacción atómica (intento {attempt + 1}/{max_retries})")
                
                yield conn
                
                # Si llegamos aquí, todo fue exitoso
                conn.commit()
                logging.debug("Transacción atómica completada exitosamente")
                
                if hasattr(self, '_initializing') and self._initializing:
                    conn.close()
                else:
                    self._connection_pool.return_connection(conn)
                return
                
            except psycopg2.OperationalError as e:
                error_msg = str(e).lower()
                
                try:
                    conn.rollback()
                    logging.debug("Rollback ejecutado por error operacional")
                except psycopg2.Error as rollback_error:
                    logging.error(f"Error durante rollback: {rollback_error}")
                
                if hasattr(self, '_initializing') and self._initializing:
                    conn.close()
                else:
                    # Marcar conexión como inválida y obtener una nueva
                    try:
                        self._connection_pool.return_connection(conn, is_broken=True)
                    except:
                        pass
                
                # Reintentar en caso de deadlock o timeout
                if ("deadlock" in error_msg or "timeout" in error_msg or "lock" in error_msg) and attempt < max_retries - 1:
                    import random
                    delay = base_delay * (2 ** attempt) + random.uniform(0, 0.1)
                    logging.warning(f"Error de concurrencia detectado, reintentando en {delay:.2f}s: {e}")
                    time.sleep(delay)
                    continue
                else:
                    logging.error(f"Error operacional en transacción después de {attempt + 1} intentos: {e}")
                    raise
                    
            except Exception as e:
                try:
                    conn.rollback()
                    logging.debug("Rollback ejecutado por error general")
                except psycopg2.Error as rollback_error:
                    logging.error(f"Error durante rollback: {rollback_error}")
                
                if hasattr(self, '_initializing') and self._initializing:
                    conn.close()
                else:
                    self._connection_pool.return_connection(conn)
                
                logging.error(f"Error en transacción atómica: {e}")
                raise
        
        raise psycopg2.OperationalError(f"Transacción falló después de {max_retries} intentos")

    def transaction(self):
        """Context manager para transacciones explícitas"""
        if hasattr(self, '_initializing') and self._initializing:
            conn = self._crear_conexion_directa()
            try:
                conn.autocommit = False
                yield conn
                conn.commit()
            except Exception as e:
                try:
                    conn.rollback()
                except psycopg2.Error:
                    pass
                raise
            finally:
                conn.close()
        else:
            with self._connection_pool.transaction() as conn:
                yield conn

    def get_connection_stats(self) -> Dict:
        """Obtiene estadísticas del pool de conexiones"""
        return self._connection_pool.get_stats()

    def get_cache_stats(self) -> Dict:
        """Obtiene estadísticas del cache"""
        return self.cache.get_stats()

    def get_performance_stats(self) -> Dict:
        """Obtiene estadísticas de rendimiento de la base de datos"""
        with self._cache_lock:
            stats = self._performance_stats.copy()
            if stats['query_count'] > 0:
                stats['avg_query_time'] = stats['total_query_time'] / stats['query_count']
                stats['queries_per_second'] = stats['query_count'] / max(stats['total_query_time'], 0.001)
                stats['cache_hit_ratio'] = self.cache.get_stats().get('hit_ratio', 0.0)
            return stats

    def _track_query_performance(self, query_time: float):
        """Registra estadísticas de rendimiento de consultas"""
        with self._cache_lock:
            self._performance_stats['query_count'] += 1
            self._performance_stats['total_query_time'] += query_time
            
            # Rastrear consultas lentas (>100ms)
            if query_time > 0.1:
                if 'slow_queries' not in self._performance_stats:
                    self._performance_stats['slow_queries'] = []
                
                self._performance_stats['slow_queries'].append({
                    'time': query_time,
                    'timestamp': time.time()
                })
                
                # Mantener solo las últimas 10 consultas lentas
                if len(self._performance_stats['slow_queries']) > 10:
                    self._performance_stats['slow_queries'] = self._performance_stats['slow_queries'][-10:]
                
                if query_time > 0.5:
                    logging.warning(f"Consulta muy lenta detectada: {query_time:.3f}s")

    def _invalidate_connection(self):
        """Invalida las conexiones del pool para forzar reconexión"""
        try:
            if hasattr(self, '_connection_pool'):
                # Cerrar todas las conexiones del pool
                self._connection_pool.close_all()
                logging.info("Pool de conexiones invalidado y cerrado")
        except Exception as e:
            logging.error(f"Error al invalidar conexiones: {e}")

    def close_connections(self):
        """Cierra todas las conexiones del pool y el sistema de cola"""
        if hasattr(self, '_connection_pool'):
            self._connection_pool.close_all()
        if hasattr(self, 'mass_operation_queue'):
            self.mass_operation_queue.shutdown()
        self._stop_cache_cleanup_thread()

    def _start_cache_cleanup_thread(self):
        self._cache_cleanup_thread = threading.Thread(target=self._cache_cleanup_task, daemon=True)
        self._cache_cleanup_thread.start()

    def _stop_cache_cleanup_thread(self):
        """Para el thread de limpieza del cache de forma segura"""
        try:
            self._stop_event.set()
            if self._cache_cleanup_thread and self._cache_cleanup_thread.is_alive():
                self._cache_cleanup_thread.join(timeout=2.0)
                if self._cache_cleanup_thread.is_alive():
                    logging.warning("Cache cleanup thread no terminó en el tiempo esperado")
        except Exception as e:
            logging.error(f"Error al detener cache cleanup thread: {e}")

    def _cache_cleanup_task(self):
        while not self._stop_event.is_set():
            self.cache.clear_expired()
            time.sleep(60)  # Cleanup every 60 seconds

    def _crear_conexion_directa(self):
        """Crea una conexión directa sin usar el pool"""
        # Si falta contraseña y estamos en ejecutable, pedirla una vez y guardarla
        try:
            is_frozen = getattr(sys, 'frozen', False)
        except Exception:
            is_frozen = False
        try:
            if is_frozen and not self.connection_params.get('password'):
                try:
                    import os
                    p = (os.getenv('PGPASSWORD') or os.getenv('DB_PASSWORD') or os.getenv('POSTGRES_PASSWORD') or '').strip()
                    if p:
                        self.connection_params['password'] = p
                except Exception:
                    pass
        except Exception:
            pass
        # Normalizar parámetros para psycopg2 (dbname)
        params = dict(self.connection_params)
        # Evitar conflicto: no especificar ambos 'database' y 'dbname'
        if 'dbname' in params and 'database' in params:
            try:
                del params['database']
            except Exception:
                pass
        elif 'dbname' not in params and 'database' in params:
            params['dbname'] = params['database']
            try:
                del params['database']
            except Exception:
                pass
        conn = psycopg2.connect(**params)
        # Importante: no ejecutar ninguna instrucción inmediatamente después de conectar.
        # Ejecutar un "SET" aquí iniciaba una transacción implícita y rompía
        # operaciones que requieren auto-commit (p.ej. CREATE INDEX CONCURRENTLY).
        conn.autocommit = False
        return conn

    def _execute_with_direct_connection(self, func, *args, **kwargs):
        """
        Ejecuta una función usando una conexión directa como fallback.
        Usado cuando el pool de conexiones falla completamente.
        """
        direct_conn = None
        try:
            logging.info("Creando conexión directa para operación de emergencia...")
            direct_conn = self._crear_conexion_directa()
            
            # Temporalmente reemplazar la conexión del pool con la directa
            original_obtener_conexion = self.obtener_conexion
            
            def obtener_conexion_directa():
                return direct_conn
            
            # Reemplazar temporalmente el método
            self.obtener_conexion = obtener_conexion_directa
            
            try:
                # Ejecutar la función original con la conexión directa
                result = func(self, *args, **kwargs)
                direct_conn.commit()  # Confirmar transacción
                logging.info("Operación completada exitosamente con conexión directa")
                return result
                
            except Exception as e:
                direct_conn.rollback()  # Revertir en caso de error
                logging.error(f"Error en operación con conexión directa: {e}")
                raise
            finally:
                # Restaurar el método original
                self.obtener_conexion = original_obtener_conexion
                
        except Exception as e:
            logging.error(f"Error configurando conexión directa: {e}")
            raise
        finally:
            if direct_conn:
                try:
                    direct_conn.close()
                    logging.info("Conexión directa cerrada")
                except Exception as e:
                    logging.warning(f"Error cerrando conexión directa: {e}")

    def _ensure_gym_config_table(self, conn):
        return self.gym_repo._ensure_gym_config_table(conn)

    def obtener_configuracion_gimnasio(self, timeout_ms: int = 1000) -> Dict[str, str]:
        return self.gym_repo.obtener_configuracion_gimnasio(timeout_ms)

    def actualizar_configuracion_gimnasio(self, data: dict) -> bool:
        return self.gym_repo.actualizar_configuracion_gimnasio(data)

    def obtener_logo_url(self) -> Optional[str]:
        return self.gym_repo.obtener_logo_url()

    def actualizar_logo_url(self, url: str) -> bool:
        return self.gym_repo.actualizar_logo_url(url)

    def inicializar_base_datos(self):
        """Inicializa todas las tablas y datos por defecto en PostgreSQL"""
        try:
            dbkey = f"{self.connection_params.get('host')}:{self.connection_params.get('port')}:{self.connection_params.get('dbname') or self.connection_params.get('database')}"
        except Exception:
            dbkey = str(self.connection_params.get('dbname') or self.connection_params.get('database') or '')
        try:
            global _INIT_DONE_DBS
        except NameError:
            _INIT_DONE_DBS = set()
        try:
            if dbkey and dbkey in _INIT_DONE_DBS:
                return
        except Exception:
            pass
        self._initializing = True
        try:
            conn = self._crear_conexion_directa()
            with conn:
                with conn.cursor() as cursor:
                    # Extensión para generar UUIDs desde la base de datos
                    try:
                        cursor.execute("CREATE EXTENSION IF NOT EXISTS pgcrypto")
                    except Exception:
                        # Si no se puede crear, se generarán UUIDs en la aplicación
                        pass
                    
                    # --- TABLAS BÁSICAS DEL SISTEMA ---
                    
                    # Tabla de usuarios
                    cursor.execute("""
                    CREATE TABLE IF NOT EXISTS usuarios (
                        id SERIAL PRIMARY KEY,
                        nombre VARCHAR(255) NOT NULL,
                        dni VARCHAR(20) UNIQUE,
                        telefono VARCHAR(50) NOT NULL,
                        pin VARCHAR(10) DEFAULT '1234',
                        rol VARCHAR(50) DEFAULT 'socio' NOT NULL,
                        notas TEXT,
                        fecha_registro TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        activo BOOLEAN DEFAULT TRUE,
                        tipo_cuota VARCHAR(100) DEFAULT 'estandar',
                        ultimo_pago DATE,
                        fecha_proximo_vencimiento DATE,
                        cuotas_vencidas INTEGER DEFAULT 0
                    )""")

                    # Bypass seguro: desactivar temporalmente protecciones del rol 'dueño'
                    # para poder verificar e insertar el usuario dueño si falta.
                    try:
                        cursor.execute("ALTER TABLE usuarios DISABLE ROW LEVEL SECURITY")
                    except Exception:
                        pass
                    try:
                        cursor.execute("ALTER TABLE usuarios NO FORCE ROW LEVEL SECURITY")
                    except Exception:
                        pass
                    # Eliminar políticas/funciones/triggers si existían de ejecuciones previas
                    try:
                        cursor.execute("DROP POLICY IF EXISTS usuarios_block_owner_select ON usuarios")
                    except Exception:
                        pass
                    try:
                        cursor.execute("DROP POLICY IF EXISTS usuarios_block_owner_update ON usuarios")
                    except Exception:
                        pass
                    try:
                        cursor.execute("DROP POLICY IF EXISTS usuarios_block_owner_delete ON usuarios")
                    except Exception:
                        pass
                    try:
                        cursor.execute("DROP POLICY IF EXISTS usuarios_block_owner_insert ON usuarios")
                    except Exception:
                        pass
                    try:
                        cursor.execute("DROP TRIGGER IF EXISTS trg_usuarios_bloquear_ins_upd_dueno ON usuarios")
                    except Exception:
                        pass
                    try:
                        cursor.execute("DROP TRIGGER IF EXISTS trg_usuarios_bloquear_del_dueno ON usuarios")
                    except Exception:
                        pass
                    try:
                        cursor.execute("DROP FUNCTION IF EXISTS usuarios_bloquear_dueno_ins_upd()")
                    except Exception:
                        pass
                    try:
                        cursor.execute("DROP FUNCTION IF EXISTS usuarios_bloquear_dueno_delete()")
                    except Exception:
                        pass

                    # Usuario dueño por defecto antes de las protecciones
                    cursor.execute("SELECT id FROM usuarios WHERE rol = 'dueño'")
                    if cursor.fetchone() is None:
                        # Insertar Dueño con PIN plano
                        _owner_pin = "2203"
                        cursor.execute(
                            """
                            INSERT INTO usuarios (nombre, dni, telefono, pin, rol, activo, tipo_cuota)
                            VALUES (%s, %s, %s, %s, %s, %s, %s)
                            ON CONFLICT (dni) DO NOTHING
                            """,
                            ("DUEÑO DEL GIMNASIO", "00000000", "N/A", _owner_pin, "dueño", True, "estandar")
                        )
                    # Determinar ID del dueño para seeds posteriores
                    owner_id = None
                    try:
                        cursor.execute("SELECT id FROM usuarios WHERE rol = 'dueño' ORDER BY id LIMIT 1")
                        _row = cursor.fetchone()
                        if _row and _row[0] is not None:
                            owner_id = int(_row[0])
                    except Exception:
                        owner_id = None

                    # Proteger completamente a los usuarios con rol 'dueño':
                    # - No se pueden consultar (SELECT), modificar (UPDATE), borrar (DELETE) ni insertar (INSERT)
                    #   filas con rol 'dueño' desde la aplicación.
                    # - Además, se bloquea por triggers cualquier intento de crear/actualizar/borrar filas 'dueño'.
                    cursor.execute("""
                        -- Habilitar y forzar Row Level Security
                        ALTER TABLE usuarios ENABLE ROW LEVEL SECURITY;
                        ALTER TABLE usuarios FORCE ROW LEVEL SECURITY;

                        -- Políticas RLS: bloquear acceso a filas con rol 'dueño'
                        DROP POLICY IF EXISTS usuarios_block_owner_select ON usuarios;
                        DROP POLICY IF EXISTS usuarios_block_owner_update ON usuarios;
                        DROP POLICY IF EXISTS usuarios_block_owner_delete ON usuarios;
                        DROP POLICY IF EXISTS usuarios_block_owner_insert ON usuarios;

                        CREATE POLICY usuarios_block_owner_select ON usuarios
                            FOR SELECT
                            USING (rol IS DISTINCT FROM 'dueño');

                        CREATE POLICY usuarios_block_owner_update ON usuarios
                            FOR UPDATE
                            USING (rol IS DISTINCT FROM 'dueño')
                            WITH CHECK (rol IS DISTINCT FROM 'dueño');

                        CREATE POLICY usuarios_block_owner_delete ON usuarios
                            FOR DELETE
                            USING (rol IS DISTINCT FROM 'dueño');

                        CREATE POLICY usuarios_block_owner_insert ON usuarios
                            FOR INSERT
                            WITH CHECK (rol IS DISTINCT FROM 'dueño');

                        -- Triggers defensivos: impedir que se inserte/actualice/borré un usuario 'dueño'
                        DROP TRIGGER IF EXISTS trg_usuarios_bloquear_ins_upd_dueno ON usuarios;
                        DROP TRIGGER IF EXISTS trg_usuarios_bloquear_del_dueno ON usuarios;
                        DROP FUNCTION IF EXISTS usuarios_bloquear_dueno_ins_upd();
                        DROP FUNCTION IF EXISTS usuarios_bloquear_dueno_delete();

                        CREATE FUNCTION usuarios_bloquear_dueno_ins_upd() RETURNS trigger AS $$
                        BEGIN
                            IF NEW.rol = 'dueño' THEN
                                RAISE EXCEPTION 'Operación no permitida: los usuarios con rol "dueño" son inafectables';
                            END IF;
                            RETURN NEW;
                        END;
                        $$ LANGUAGE plpgsql;

                        CREATE FUNCTION usuarios_bloquear_dueno_delete() RETURNS trigger AS $$
                        BEGIN
                            IF OLD.rol = 'dueño' THEN
                                RAISE EXCEPTION 'Operación no permitida: los usuarios con rol "dueño" no pueden eliminarse';
                            END IF;
                            RETURN OLD;
                        END;
                        $$ LANGUAGE plpgsql;

                        CREATE TRIGGER trg_usuarios_bloquear_ins_upd_dueno
                        BEFORE INSERT OR UPDATE ON usuarios
                        FOR EACH ROW EXECUTE FUNCTION usuarios_bloquear_dueno_ins_upd();

                    CREATE TRIGGER trg_usuarios_bloquear_del_dueno
                    BEFORE DELETE ON usuarios
                    FOR EACH ROW EXECUTE FUNCTION usuarios_bloquear_dueno_delete();
                    """)

                    
                    # Tabla de pagos
                    cursor.execute("""
                    CREATE TABLE IF NOT EXISTS pagos (
                        id SERIAL PRIMARY KEY,
                        usuario_id INTEGER NOT NULL,
                        monto DECIMAL(10,2) NOT NULL,
                        mes INTEGER NOT NULL,
                        año INTEGER NOT NULL,
                        fecha_pago TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        metodo_pago_id INTEGER,
                        FOREIGN KEY (usuario_id) REFERENCES usuarios (id) ON DELETE CASCADE
                    )""")
                    
                    # Índice único para asegurar un pago por usuario/mes/año
                    cursor.execute(
                        """
                        CREATE UNIQUE INDEX IF NOT EXISTS idx_pagos_usuario_mes_año
                        ON pagos (usuario_id, mes, año)
                        """
                    )
                    # Extensiones de pagos solicitadas (concepto, metodo_pago, estado)
                    try:
                        cursor.execute("ALTER TABLE pagos ADD COLUMN IF NOT EXISTS concepto VARCHAR(100)")
                    except Exception:
                        pass
                    try:
                        cursor.execute("ALTER TABLE pagos ADD COLUMN IF NOT EXISTS metodo_pago VARCHAR(50)")
                    except Exception:
                        pass
                    try:
                        cursor.execute("ALTER TABLE pagos ADD COLUMN IF NOT EXISTS estado VARCHAR(20) DEFAULT 'pagado'")
                    except Exception:
                        pass
                    
                    # Tabla de configuración
                    cursor.execute("""
                    CREATE TABLE IF NOT EXISTS configuracion (
                        id SERIAL PRIMARY KEY,
                        clave VARCHAR(255) UNIQUE NOT NULL,
                        valor TEXT NOT NULL,
                        tipo VARCHAR(50) DEFAULT 'string',
                        descripcion TEXT
                    )""")

                    # Tabla centralizada de datos del gimnasio
                    # Se asegura su existencia durante la inicialización del sistema.
                    try:
                        self._ensure_gym_config_table(conn)
                    except Exception:
                        pass

                    # Migración inicial de claves existentes en 'configuracion' hacia 'gym_config'
                    # Mantiene compatibilidad con instalaciones previas.
                    try:
                        with conn.cursor() as mig:
                            # Migrar logo si existe
                            try:
                                mig.execute("SELECT valor FROM configuracion WHERE clave = 'gym_logo_url'")
                                r = mig.fetchone()
                                if r and r[0]:
                                    mig.execute(
                                        "UPDATE gym_config SET logo_url = %s, updated_at = CURRENT_TIMESTAMP "
                                        "WHERE id = (SELECT id FROM gym_config ORDER BY id LIMIT 1)",
                                        (str(r[0]) ,)
                                    )
                            except Exception:
                                pass

                            # Migrar datos básicos si existen
                            keys = ['gym_name','gym_slogan','gym_address','gym_phone','gym_email',
                                    'gym_website','facebook','instagram','twitter']
                            existing = {}
                            for k in keys:
                                try:
                                    mig.execute("SELECT valor FROM configuracion WHERE clave = %s", (k,))
                                    rv = mig.fetchone()
                                    if rv and isinstance(rv[0], str) and rv[0].strip():
                                        existing[k] = rv[0].strip()
                                except Exception:
                                    pass
                            if existing:
                                set_clause = ", ".join([f"{k} = %s" for k in existing.keys()])
                                mig.execute(
                                    f"UPDATE gym_config SET {set_clause}, updated_at = CURRENT_TIMESTAMP "
                                    "WHERE id = (SELECT id FROM gym_config ORDER BY id LIMIT 1)",
                                    [str(existing[k]) for k in existing.keys()]
                                )

                            # Migrar claves históricas en español → columnas de gym_config
                            # Mapa: gym_nombre→gym_name, gym_direccion→gym_address, gym_telefono→gym_phone, gym_correo→gym_email
                            spanish_map = {
                                'gym_nombre': 'gym_name',
                                'gym_direccion': 'gym_address',
                                'gym_telefono': 'gym_phone',
                                'gym_correo': 'gym_email'
                            }
                            # Obtener valores actuales de gym_config para no sobreescribir si ya están poblados
                            current_row = None
                            try:
                                mig.execute(
                                    """
                                    SELECT gym_name, gym_slogan, gym_address, gym_phone, gym_email,
                                           gym_website, facebook, instagram, twitter, logo_url
                                    FROM gym_config ORDER BY id LIMIT 1
                                    """
                                )
                                current_row = mig.fetchone()
                            except Exception:
                                current_row = None
                            current_vals = {
                                'gym_name': '', 'gym_slogan': '', 'gym_address': '', 'gym_phone': '', 'gym_email': '',
                                'gym_website': '', 'facebook': '', 'instagram': '', 'twitter': '', 'logo_url': ''
                            }
                            if current_row:
                                try:
                                    current_vals['gym_name'] = str(current_row[0] or '')
                                    current_vals['gym_address'] = str(current_row[2] or '')
                                    current_vals['gym_phone'] = str(current_row[3] or '')
                                    current_vals['gym_email'] = str(current_row[4] or '')
                                except Exception:
                                    pass

                            spanish_updates = {}
                            for sk, col in spanish_map.items():
                                try:
                                    mig.execute("SELECT valor FROM configuracion WHERE clave = %s", (sk,))
                                    rv = mig.fetchone()
                                    val = (rv[0].strip() if rv and isinstance(rv[0], str) else '')
                                    if val and not (current_vals.get(col, '').strip()):
                                        spanish_updates[col] = val
                                except Exception:
                                    pass
                            if spanish_updates:
                                set_clause_es = ", ".join([f"{k} = %s" for k in spanish_updates.keys()])
                                mig.execute(
                                    f"UPDATE gym_config SET {set_clause_es}, updated_at = CURRENT_TIMESTAMP "
                                    "WHERE id = (SELECT id FROM gym_config ORDER BY id LIMIT 1)",
                                    [str(spanish_updates[k]) for k in spanish_updates.keys()]
                                )
                    except Exception:
                        pass
                    
                    # Tabla de auditoría
                    cursor.execute("""
                    CREATE TABLE IF NOT EXISTS auditoria (
                        id SERIAL PRIMARY KEY,
                        tabla VARCHAR(100) NOT NULL,
                        operacion VARCHAR(50) NOT NULL,
                        timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )""")
                    
                    # Tabla de estados de usuario
                    cursor.execute("""
                    CREATE TABLE IF NOT EXISTS estados_usuario (
                        id SERIAL PRIMARY KEY,
                        usuario_id INTEGER NOT NULL,
                        estado VARCHAR(100) NOT NULL,
                        fecha_inicio TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        fecha_fin TIMESTAMP,
                        activo BOOLEAN DEFAULT TRUE,
                        FOREIGN KEY (usuario_id) REFERENCES usuarios (id) ON DELETE CASCADE
                    )""")
                    
                    # Tabla de asistencias
                    cursor.execute("""
                    CREATE TABLE IF NOT EXISTS asistencias (
                        id SERIAL PRIMARY KEY,
                        usuario_id INTEGER NOT NULL,
                        fecha DATE DEFAULT CURRENT_DATE,
                        hora_registro TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        hora_entrada TIME,
                        FOREIGN KEY (usuario_id) REFERENCES usuarios (id) ON DELETE CASCADE,
                        UNIQUE(usuario_id, fecha)
                    )""")
                    
                    # Tabla de clases
                    cursor.execute("""
                    CREATE TABLE IF NOT EXISTS clases (
                        id SERIAL PRIMARY KEY,
                        nombre VARCHAR(255) UNIQUE NOT NULL,
                        descripcion TEXT,
                        activa BOOLEAN DEFAULT TRUE
                    )""")

                    # Tabla de tipos de clases (para clasificar clases: Yoga, Funcional, etc.)
                    cursor.execute("""
                    CREATE TABLE IF NOT EXISTS tipos_clases (
                        id SERIAL PRIMARY KEY,
                        nombre VARCHAR(255) UNIQUE NOT NULL,
                        descripcion TEXT,
                        activo BOOLEAN DEFAULT TRUE
                    )""")

                    # Agregar relación opcional clases.tipo_clase_id
                    cursor.execute("""
                    ALTER TABLE clases
                    ADD COLUMN IF NOT EXISTS tipo_clase_id INTEGER
                    """)

                    # Índice para búsquedas por tipo de clase
                    cursor.execute(
                        "CREATE INDEX IF NOT EXISTS idx_clases_tipo_clase_id ON clases (tipo_clase_id)"
                    )
                    
                    # Tabla de horarios de clases (sin profesor_id)
                    cursor.execute("""
                    CREATE TABLE IF NOT EXISTS clases_horarios (
                        id SERIAL PRIMARY KEY,
                        clase_id INTEGER NOT NULL,
                        dia_semana VARCHAR(20) NOT NULL,
                        hora_inicio TIME NOT NULL,
                        hora_fin TIME NOT NULL,
                        cupo_maximo INTEGER DEFAULT 20,
                        activo BOOLEAN DEFAULT TRUE,
                        FOREIGN KEY (clase_id) REFERENCES clases (id) ON DELETE CASCADE
                    )""")
                    
                    # Tabla de lista de espera por clase (PostgreSQL)
                    cursor.execute("""
                    CREATE TABLE IF NOT EXISTS clase_lista_espera (
                        id SERIAL PRIMARY KEY,
                        clase_horario_id INTEGER NOT NULL,
                        usuario_id INTEGER NOT NULL,
                        posicion INTEGER NOT NULL,
                        activo BOOLEAN DEFAULT TRUE,
                        fecha_creacion TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        FOREIGN KEY (clase_horario_id) REFERENCES clases_horarios (id) ON DELETE CASCADE,
                        FOREIGN KEY (usuario_id) REFERENCES usuarios (id) ON DELETE CASCADE,
                        UNIQUE (clase_horario_id, usuario_id)
                    )""")
                    # Índices para rendimiento en lista de espera
                    cursor.execute("CREATE INDEX IF NOT EXISTS idx_clase_lista_espera_clase ON clase_lista_espera (clase_horario_id)")
                    cursor.execute("CREATE INDEX IF NOT EXISTS idx_clase_lista_espera_activo ON clase_lista_espera (activo)")
                    cursor.execute("CREATE INDEX IF NOT EXISTS idx_clase_lista_espera_posicion ON clase_lista_espera (posicion)")

                    # Tabla de notificaciones de cupo (PostgreSQL)
                    cursor.execute("""
                    CREATE TABLE IF NOT EXISTS notificaciones_cupos (
                        id SERIAL PRIMARY KEY,
                        usuario_id INTEGER NOT NULL REFERENCES usuarios (id) ON DELETE CASCADE,
                        clase_horario_id INTEGER NOT NULL REFERENCES clases_horarios (id) ON DELETE CASCADE,
                        tipo_notificacion VARCHAR(50) NOT NULL CHECK (tipo_notificacion IN ('cupo_liberado','promocion','recordatorio')),
                        mensaje TEXT,
                        leida BOOLEAN DEFAULT FALSE,
                        activa BOOLEAN DEFAULT TRUE,
                        fecha_creacion TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        fecha_lectura TIMESTAMP
                    )""")
                    # Índices para notificaciones de cupo
                    cursor.execute("CREATE INDEX IF NOT EXISTS idx_notif_cupos_usuario_activa ON notificaciones_cupos (usuario_id, activa)")
                    cursor.execute("CREATE INDEX IF NOT EXISTS idx_notif_cupos_clase ON notificaciones_cupos (clase_horario_id)")
                    cursor.execute("CREATE INDEX IF NOT EXISTS idx_notif_cupos_leida ON notificaciones_cupos (leida)")
                    cursor.execute("CREATE INDEX IF NOT EXISTS idx_notif_cupos_tipo ON notificaciones_cupos (tipo_notificacion)")
                    
                    # Tabla de ejercicios
                    cursor.execute("""
                    CREATE TABLE IF NOT EXISTS ejercicios (
                        id SERIAL PRIMARY KEY,
                        nombre VARCHAR(255) UNIQUE NOT NULL,
                        descripcion TEXT,
                        grupo_muscular VARCHAR(100),
                        objetivo VARCHAR(100) DEFAULT 'general'
                    )""")
                    # Asegurar columna 'objetivo' para tablas existentes
                    try:
                        if not self._column_exists(conn, 'ejercicios', 'objetivo'):
                            cursor.execute("ALTER TABLE ejercicios ADD COLUMN objetivo VARCHAR(100) DEFAULT 'general'")
                    except Exception:
                        pass
                    # Columnas de medios para ejercicios
                    try:
                        # Compatibilidad con PostgreSQL antiguos: sin IF NOT EXISTS
                        if not self._column_exists(conn, 'ejercicios', 'video_url'):
                            cursor.execute("ALTER TABLE ejercicios ADD COLUMN video_url VARCHAR(512)")
                    except Exception:
                        pass
                    try:
                        if not self._column_exists(conn, 'ejercicios', 'video_mime'):
                            cursor.execute("ALTER TABLE ejercicios ADD COLUMN video_mime VARCHAR(50)")
                    except Exception:
                        pass
                    
                    # Tabla de rutinas
                    cursor.execute("""
                    CREATE TABLE IF NOT EXISTS rutinas (
                        id SERIAL PRIMARY KEY,
                        usuario_id INTEGER,
                        nombre_rutina VARCHAR(255) NOT NULL,
                        descripcion TEXT,
                        dias_semana INTEGER,
                        categoria VARCHAR(100) DEFAULT 'general',
                        fecha_creacion TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        activa BOOLEAN DEFAULT TRUE,
                        FOREIGN KEY (usuario_id) REFERENCES usuarios (id) ON DELETE CASCADE
                    )""")
                    # uuid_rutina para rutinas (VARCHAR(36) con default si la extensión está disponible)
                    try:
                        cursor.execute(
                            """
                            ALTER TABLE rutinas
                            ADD COLUMN IF NOT EXISTS uuid_rutina VARCHAR(36) UNIQUE DEFAULT gen_random_uuid()::text
                            """
                        )
                        cursor.execute(
                            "CREATE UNIQUE INDEX IF NOT EXISTS idx_rutinas_uuid_rutina ON rutinas (uuid_rutina)"
                        )
                    except Exception:
                        # Fallback sin default
                        try:
                            cursor.execute(
                                "ALTER TABLE rutinas ADD COLUMN IF NOT EXISTS uuid_rutina VARCHAR(36) UNIQUE"
                            )
                            cursor.execute(
                                "CREATE UNIQUE INDEX IF NOT EXISTS idx_rutinas_uuid_rutina ON rutinas (uuid_rutina)"
                            )
                        except Exception:
                            pass
                    
                    # Tabla de rutina_ejercicios
                    cursor.execute("""
                    CREATE TABLE IF NOT EXISTS rutina_ejercicios (
                        id SERIAL PRIMARY KEY,
                        rutina_id INTEGER NOT NULL,
                        ejercicio_id INTEGER NOT NULL,
                        dia_semana INTEGER,
                        series INTEGER,
                        repeticiones VARCHAR(50),
                        orden INTEGER,
                        FOREIGN KEY (rutina_id) REFERENCES rutinas (id) ON DELETE CASCADE,
                        FOREIGN KEY (ejercicio_id) REFERENCES ejercicios (id) ON DELETE CASCADE
                    )""")
                    
                    # Tabla de clase_usuarios
                    cursor.execute("""
                    CREATE TABLE IF NOT EXISTS clase_usuarios (
                        id SERIAL PRIMARY KEY,
                        clase_horario_id INTEGER NOT NULL,
                        usuario_id INTEGER NOT NULL,
                        fecha_inscripcion TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        FOREIGN KEY (clase_horario_id) REFERENCES clases_horarios (id) ON DELETE CASCADE,
                        FOREIGN KEY (usuario_id) REFERENCES usuarios (id) ON DELETE CASCADE,
                        UNIQUE(clase_horario_id, usuario_id)
                    )""")
                    
                    # Tabla de clase_ejercicios
                    cursor.execute("""
                    CREATE TABLE IF NOT EXISTS clase_ejercicios (
                        clase_id INTEGER NOT NULL,
                        ejercicio_id INTEGER NOT NULL,
                        FOREIGN KEY (clase_id) REFERENCES clases (id) ON DELETE CASCADE,
                        FOREIGN KEY (ejercicio_id) REFERENCES ejercicios (id) ON DELETE CASCADE,
                        PRIMARY KEY (clase_id, ejercicio_id)
                    )""")
                    # Extender esquema con columnas de orden y metadatos (si no existen)
                    try:
                        cursor.execute("ALTER TABLE clase_ejercicios ADD COLUMN IF NOT EXISTS orden INTEGER DEFAULT 0")
                    except Exception:
                        pass
                    try:
                        cursor.execute("ALTER TABLE clase_ejercicios ADD COLUMN IF NOT EXISTS series INTEGER DEFAULT 0")
                    except Exception:
                        pass
                    try:
                        cursor.execute("ALTER TABLE clase_ejercicios ADD COLUMN IF NOT EXISTS repeticiones VARCHAR(50) DEFAULT ''")
                    except Exception:
                        pass
                    try:
                        cursor.execute("ALTER TABLE clase_ejercicios ADD COLUMN IF NOT EXISTS descanso_segundos INTEGER DEFAULT 0")
                    except Exception:
                        pass
                    try:
                        cursor.execute("ALTER TABLE clase_ejercicios ADD COLUMN IF NOT EXISTS notas TEXT")
                    except Exception:
                        pass

                    # --- TABLAS DE BLOQUES DE EJERCICIOS POR CLASE ---
                    # Tabla de bloques asociados a una clase
                    cursor.execute(
                        """
                        CREATE TABLE IF NOT EXISTS clase_bloques (
                            id SERIAL PRIMARY KEY,
                            clase_id INTEGER NOT NULL,
                            nombre TEXT NOT NULL,
                            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                            FOREIGN KEY (clase_id) REFERENCES clases (id) ON DELETE CASCADE
                        )
                        """
                    )
                    # Índices para clase_bloques
                    cursor.execute("CREATE INDEX IF NOT EXISTS idx_clase_bloques_clase ON clase_bloques(clase_id)")
                    cursor.execute("CREATE INDEX IF NOT EXISTS idx_clase_bloques_nombre ON clase_bloques(nombre)")

                    # Tabla de items dentro de un bloque
                    cursor.execute(
                        """
                        CREATE TABLE IF NOT EXISTS clase_bloque_items (
                            id SERIAL PRIMARY KEY,
                            bloque_id INTEGER NOT NULL REFERENCES clase_bloques(id) ON DELETE CASCADE,
                            ejercicio_id INTEGER NOT NULL REFERENCES ejercicios(id) ON DELETE CASCADE,
                            orden INTEGER NOT NULL DEFAULT 0,
                            series INTEGER DEFAULT 0,
                            repeticiones TEXT,
                            descanso_segundos INTEGER DEFAULT 0,
                            notas TEXT
                        )
                        """
                    )
                    # Índices para clase_bloque_items
                    cursor.execute("CREATE INDEX IF NOT EXISTS idx_bloque_items_bloque ON clase_bloque_items(bloque_id)")
                    cursor.execute("CREATE INDEX IF NOT EXISTS idx_bloque_items_bloque_orden ON clase_bloque_items(bloque_id, orden)")

                    # --- TABLAS PARA GRUPOS DE EJERCICIOS ---
                    cursor.execute("""
                    CREATE TABLE IF NOT EXISTS ejercicio_grupos (
                        id SERIAL PRIMARY KEY,
                        nombre VARCHAR(255) UNIQUE NOT NULL
                    )""")
                
                    cursor.execute("""
                    CREATE TABLE IF NOT EXISTS ejercicio_grupo_items (
                        grupo_id INTEGER NOT NULL,
                        ejercicio_id INTEGER NOT NULL,
                        FOREIGN KEY (grupo_id) REFERENCES ejercicio_grupos (id) ON DELETE CASCADE,
                        FOREIGN KEY (ejercicio_id) REFERENCES ejercicios (id) ON DELETE CASCADE,
                        PRIMARY KEY (grupo_id, ejercicio_id)
                    )""")
                
                    # --- TABLA ESPECÍFICA PARA PROFESORES ---
                    cursor.execute("""
                    CREATE TABLE IF NOT EXISTS profesores (
                        id SERIAL PRIMARY KEY,
                        usuario_id INTEGER UNIQUE NOT NULL,
                        tipo VARCHAR(50) DEFAULT 'Musculación',
                        especialidades TEXT,
                        certificaciones TEXT,
                        experiencia_años INTEGER DEFAULT 0,
                        tarifa_por_hora DECIMAL(10,2) DEFAULT 0.0,
                        horario_disponible TEXT,
                        fecha_contratacion DATE,
                        estado VARCHAR(20) DEFAULT 'activo' CHECK(estado IN ('activo', 'inactivo', 'vacaciones')),
                        biografia TEXT,
                        foto_perfil VARCHAR(255),
                        telefono_emergencia VARCHAR(50),
                        fecha_creacion TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        fecha_actualizacion TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        FOREIGN KEY (usuario_id) REFERENCES usuarios (id) ON DELETE CASCADE
                    )""")
                    
                    # --- TABLA PARA HORARIOS DE PROFESORES ---
                    cursor.execute("""
                    CREATE TABLE IF NOT EXISTS horarios_profesores (
                        id SERIAL PRIMARY KEY,
                        profesor_id INTEGER NOT NULL,
                        dia_semana VARCHAR(20) NOT NULL,
                        hora_inicio TIME NOT NULL,
                        hora_fin TIME NOT NULL,
                        disponible BOOLEAN DEFAULT true,
                        fecha_creacion TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        FOREIGN KEY (profesor_id) REFERENCES profesores (id) ON DELETE CASCADE
                    )""")
                    
                    # --- TABLA PARA HORARIOS DE DISPONIBILIDAD DE PROFESORES ---
                    cursor.execute("""
                    CREATE TABLE IF NOT EXISTS profesores_horarios_disponibilidad (
                        id SERIAL PRIMARY KEY,
                        profesor_id INTEGER NOT NULL,
                        dia_semana INTEGER NOT NULL CHECK(dia_semana BETWEEN 0 AND 6),
                        hora_inicio TIME NOT NULL,
                        hora_fin TIME NOT NULL,
                        disponible BOOLEAN DEFAULT true,
                        tipo_disponibilidad VARCHAR(50) DEFAULT 'regular',
                        fecha_creacion TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        fecha_actualizacion TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        FOREIGN KEY (profesor_id) REFERENCES profesores (id) ON DELETE CASCADE
                    )""")
                
                    # --- TABLA PARA EVALUACIONES DE PROFESORES ---
                    cursor.execute("""
                    CREATE TABLE IF NOT EXISTS profesor_evaluaciones (
                        id SERIAL PRIMARY KEY,
                        profesor_id INTEGER NOT NULL,
                        usuario_id INTEGER NOT NULL,
                        puntuacion INTEGER CHECK(puntuacion >= 1 AND puntuacion <= 5),
                        comentario TEXT,
                        fecha_evaluacion TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        FOREIGN KEY (profesor_id) REFERENCES profesores (id) ON DELETE CASCADE,
                        FOREIGN KEY (usuario_id) REFERENCES usuarios (id) ON DELETE CASCADE,
                        UNIQUE(profesor_id, usuario_id)
                    )""")
                                    
                    # Tabla profesor_disponibilidad
                    cursor.execute("""
                    CREATE TABLE IF NOT EXISTS profesor_disponibilidad (
                        id SERIAL PRIMARY KEY,
                        profesor_id INTEGER NOT NULL,
                        fecha DATE NOT NULL,
                        tipo_disponibilidad VARCHAR(50) NOT NULL CHECK(tipo_disponibilidad IN ('Disponible', 'No Disponible', 'Parcialmente Disponible')),
                        hora_inicio TIME,
                        hora_fin TIME,
                        notas TEXT,
                        fecha_creacion TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        fecha_modificacion TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        UNIQUE(profesor_id, fecha),
                        FOREIGN KEY (profesor_id) REFERENCES profesores (id) ON DELETE CASCADE
                    )""")
                
                    # Tabla para asignaciones profesor-clase (única relación permitida)
                    cursor.execute("""
                    CREATE TABLE IF NOT EXISTS profesor_clase_asignaciones (
                        id SERIAL PRIMARY KEY,
                        clase_horario_id INTEGER NOT NULL,
                        profesor_id INTEGER NOT NULL,
                        fecha_asignacion TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        activa BOOLEAN DEFAULT TRUE,
                        FOREIGN KEY (clase_horario_id) REFERENCES clases_horarios (id) ON DELETE CASCADE,
                        FOREIGN KEY (profesor_id) REFERENCES profesores (id) ON DELETE CASCADE,
                        UNIQUE(clase_horario_id, profesor_id)
                    )""")                    
                    
                    # --- TABLA PARA SUPLENCIAS DE PROFESORES (CLASES) ---
                    # Eliminar tabla existente si tiene estructura incorrecta
                    cursor.execute("DROP TABLE IF EXISTS profesor_suplencias CASCADE")
                    cursor.execute("""
                    CREATE TABLE profesor_suplencias (
                        id SERIAL PRIMARY KEY,
                        asignacion_id INTEGER NOT NULL,
                        profesor_suplente_id INTEGER,
                        fecha_clase DATE NOT NULL,
                        motivo TEXT NOT NULL,
                        estado VARCHAR(20) DEFAULT 'Pendiente' CHECK(estado IN ('Pendiente', 'Asignado', 'Confirmado', 'Cancelado')),
                        notas TEXT,
                        fecha_creacion TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        fecha_resolucion TIMESTAMP,
                        FOREIGN KEY (asignacion_id) REFERENCES profesor_clase_asignaciones (id) ON DELETE CASCADE,
                        FOREIGN KEY (profesor_suplente_id) REFERENCES profesores (id) ON DELETE SET NULL
                    )""")

                    # --- TABLA PARA SUPLENCIAS GENERALES (INDEPENDIENTE DE CLASES) ---
                    cursor.execute("""
                    CREATE TABLE IF NOT EXISTS profesor_suplencias_generales (
                        id SERIAL PRIMARY KEY,
                        horario_profesor_id INTEGER,
                        profesor_original_id INTEGER NOT NULL,
                        profesor_suplente_id INTEGER,
                        fecha DATE NOT NULL,
                        hora_inicio TIME NOT NULL,
                        hora_fin TIME NOT NULL,
                        motivo TEXT NOT NULL,
                        estado VARCHAR(20) DEFAULT 'Pendiente' CHECK(estado IN ('Pendiente', 'Asignado', 'Confirmado', 'Cancelado')),
                        notas TEXT,
                        fecha_creacion TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        fecha_resolucion TIMESTAMP,
                        FOREIGN KEY (horario_profesor_id) REFERENCES horarios_profesores (id) ON DELETE SET NULL,
                        FOREIGN KEY (profesor_original_id) REFERENCES profesores (id) ON DELETE CASCADE,
                        FOREIGN KEY (profesor_suplente_id) REFERENCES profesores (id) ON DELETE SET NULL
                    )
                    """)

                    # --- TABLA PARA TIPOS DE CUOTA DINÁMICOS ---
                    cursor.execute("""
                    CREATE TABLE IF NOT EXISTS tipos_cuota (
                        id SERIAL PRIMARY KEY,
                        nombre VARCHAR(100) UNIQUE NOT NULL,
                        precio DECIMAL(10,2) NOT NULL CHECK(precio >= 0),
                        icono_path VARCHAR(255),
                        activo BOOLEAN DEFAULT TRUE,
                        fecha_creacion TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        descripcion TEXT,
                        duracion_dias INTEGER DEFAULT 30 CHECK(duracion_dias > 0)
                    )""")
                    
                    # Agregar columna duracion_dias si no existe
                    cursor.execute("""
                        ALTER TABLE tipos_cuota 
                        ADD COLUMN IF NOT EXISTS duracion_dias INTEGER DEFAULT 30 CHECK(duracion_dias > 0)
                    """)
                    
                    # Agregar columna fecha_modificacion si no existe
                    cursor.execute("""
                        ALTER TABLE tipos_cuota 
                        ADD COLUMN IF NOT EXISTS fecha_modificacion TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    """)
                    
                    # --- TABLAS PARA SISTEMA DE NOTAS Y ETIQUETAS ---
                    
                    # Tabla para notas de usuarios
                    cursor.execute("""
                    CREATE TABLE IF NOT EXISTS usuario_notas (
                        id SERIAL PRIMARY KEY,
                        usuario_id INTEGER NOT NULL,
                        categoria VARCHAR(50) NOT NULL DEFAULT 'general' CHECK(categoria IN ('general', 'medica', 'administrativa', 'comportamiento')),
                        titulo VARCHAR(255) NOT NULL,
                        contenido TEXT NOT NULL,
                        importancia VARCHAR(20) NOT NULL DEFAULT 'normal' CHECK(importancia IN ('baja', 'normal', 'alta', 'critica')),
                        fecha_creacion TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        fecha_modificacion TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        activa BOOLEAN DEFAULT TRUE,
                        autor_id INTEGER,
                        FOREIGN KEY (usuario_id) REFERENCES usuarios (id) ON DELETE CASCADE,
                        FOREIGN KEY (autor_id) REFERENCES usuarios (id) ON DELETE SET NULL
                    )""")
                    
                    # Tabla para etiquetas
                    cursor.execute("""
                    CREATE TABLE IF NOT EXISTS etiquetas (
                        id SERIAL PRIMARY KEY,
                        nombre VARCHAR(100) UNIQUE NOT NULL,
                        color VARCHAR(7) NOT NULL DEFAULT '#3498db',
                        descripcion TEXT,
                        fecha_creacion TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        activo BOOLEAN DEFAULT TRUE
                    )""")
                    
                    # Tabla de relación usuario-etiquetas
                    cursor.execute("""
                    CREATE TABLE IF NOT EXISTS usuario_etiquetas (
                        usuario_id INTEGER NOT NULL,
                        etiqueta_id INTEGER NOT NULL,
                        fecha_asignacion TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        asignado_por INTEGER,
                        PRIMARY KEY (usuario_id, etiqueta_id),
                        FOREIGN KEY (usuario_id) REFERENCES usuarios (id) ON DELETE CASCADE,
                        FOREIGN KEY (etiqueta_id) REFERENCES etiquetas (id) ON DELETE CASCADE,
                        FOREIGN KEY (asignado_por) REFERENCES usuarios (id) ON DELETE SET NULL
                    )""")
                    
                    # Tabla para estados temporales de usuarios
                    cursor.execute("""
                    CREATE TABLE IF NOT EXISTS usuario_estados (
                        id SERIAL PRIMARY KEY,
                        usuario_id INTEGER NOT NULL,
                        estado VARCHAR(100) NOT NULL,
                        descripcion TEXT,
                        fecha_inicio TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        fecha_vencimiento TIMESTAMP,
                        activo BOOLEAN DEFAULT TRUE,
                        creado_por INTEGER,
                        FOREIGN KEY (usuario_id) REFERENCES usuarios (id) ON DELETE CASCADE,
                        FOREIGN KEY (creado_por) REFERENCES usuarios (id) ON DELETE SET NULL
                    )""")
                    
                    # Tabla para métodos de pago
                    cursor.execute("""
                    CREATE TABLE IF NOT EXISTS metodos_pago (
                        id SERIAL PRIMARY KEY,
                        nombre VARCHAR(100) UNIQUE NOT NULL,
                        icono VARCHAR(10),
                        color VARCHAR(7) NOT NULL DEFAULT '#3498db',
                        comision DECIMAL(5,2) DEFAULT 0.0,
                        activo BOOLEAN DEFAULT TRUE,
                        fecha_creacion TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        descripcion TEXT
                    )""")
                    
                    # Agregar columna descripcion si no existe (para bases de datos existentes)
                    cursor.execute("""
                    DO $$ 
                    BEGIN 
                        IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                                      WHERE table_name='metodos_pago' AND column_name='descripcion') THEN
                            ALTER TABLE metodos_pago ADD COLUMN descripcion TEXT;
                        END IF;
                    END $$;
                    """)
                    
                    # Tabla para conceptos de pago
                    cursor.execute("""
                    CREATE TABLE IF NOT EXISTS conceptos_pago (
                        id SERIAL PRIMARY KEY,
                        nombre VARCHAR(100) UNIQUE NOT NULL,
                        descripcion TEXT,
                        precio_base DECIMAL(10,2) DEFAULT 0.0,
                        tipo VARCHAR(20) NOT NULL DEFAULT 'fijo',
                        activo BOOLEAN DEFAULT TRUE,
                        fecha_creacion TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )""")
                    
                    # --- TABLAS PARA TEMAS Y PROGRAMACIÓN ---
                    
                    # Tabla de temas personalizados
                    cursor.execute("""
                    CREATE TABLE IF NOT EXISTS custom_themes (
                        id SERIAL PRIMARY KEY,
                        nombre VARCHAR(100) NOT NULL,
                        name VARCHAR(100) NOT NULL,
                        data JSONB,
                        colores JSONB NOT NULL,
                        activo BOOLEAN DEFAULT TRUE,
                        fecha_creacion TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        usuario_creador_id INTEGER REFERENCES usuarios(id)
                    )""")
                    
                    # Agregar columnas faltantes si no existen
                    cursor.execute("""
                        ALTER TABLE custom_themes 
                        ADD COLUMN IF NOT EXISTS name VARCHAR(100),
                        ADD COLUMN IF NOT EXISTS data JSONB
                    """)
                    
                    # Tabla de programación de temas
                    cursor.execute("""
                    CREATE TABLE IF NOT EXISTS theme_schedules (
                        id SERIAL PRIMARY KEY,
                        name VARCHAR(200) NOT NULL,
                        theme_name VARCHAR(100) NOT NULL,
                        theme_id INTEGER REFERENCES custom_themes(id),
                        start_time TIME NOT NULL,
                        end_time TIME NOT NULL,
                        monday BOOLEAN DEFAULT FALSE,
                        tuesday BOOLEAN DEFAULT FALSE,
                        wednesday BOOLEAN DEFAULT FALSE,
                        thursday BOOLEAN DEFAULT FALSE,
                        friday BOOLEAN DEFAULT FALSE,
                        saturday BOOLEAN DEFAULT FALSE,
                        sunday BOOLEAN DEFAULT FALSE,
                        is_active BOOLEAN DEFAULT TRUE,
                        fecha_inicio DATE,
                        fecha_fin DATE,
                        activo BOOLEAN DEFAULT TRUE,
                        fecha_creacion TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )""")
                    
                    # Tabla de configuración de programación de temas
                    cursor.execute("""
                    CREATE TABLE IF NOT EXISTS theme_scheduling_config (
                        id SERIAL PRIMARY KEY,
                        clave VARCHAR(100) UNIQUE NOT NULL,
                        valor TEXT NOT NULL,
                        config_data JSONB,
                        config_type VARCHAR(50) DEFAULT 'general',
                        descripcion TEXT,
                        fecha_actualizacion TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )""")
                    
                    # Agregar columnas faltantes si no existen
                    cursor.execute("""
                        ALTER TABLE theme_scheduling_config 
                        ADD COLUMN IF NOT EXISTS config_data JSONB,
                        ADD COLUMN IF NOT EXISTS config_type VARCHAR(50) DEFAULT 'general'
                    """)
                    
                    # Agregar columnas faltantes a theme_schedules
                    cursor.execute("""
                        ALTER TABLE theme_schedules 
                        ADD COLUMN IF NOT EXISTS name VARCHAR(200),
                        ADD COLUMN IF NOT EXISTS theme_name VARCHAR(100),
                        ADD COLUMN IF NOT EXISTS start_time TIME,
                        ADD COLUMN IF NOT EXISTS end_time TIME,
                        ADD COLUMN IF NOT EXISTS monday BOOLEAN DEFAULT FALSE,
                        ADD COLUMN IF NOT EXISTS tuesday BOOLEAN DEFAULT FALSE,
                        ADD COLUMN IF NOT EXISTS wednesday BOOLEAN DEFAULT FALSE,
                        ADD COLUMN IF NOT EXISTS thursday BOOLEAN DEFAULT FALSE,
                        ADD COLUMN IF NOT EXISTS friday BOOLEAN DEFAULT FALSE,
                        ADD COLUMN IF NOT EXISTS saturday BOOLEAN DEFAULT FALSE,
                        ADD COLUMN IF NOT EXISTS sunday BOOLEAN DEFAULT FALSE,
                        ADD COLUMN IF NOT EXISTS is_active BOOLEAN DEFAULT TRUE
                    """)
                    
                    # --- TABLAS FALTANTES PARA ERRORES ---
                    
                    # Tabla de numeración de comprobantes
                    cursor.execute("""
                    CREATE TABLE IF NOT EXISTS numeracion_comprobantes (
                        id SERIAL PRIMARY KEY,
                        tipo_comprobante VARCHAR(50) UNIQUE NOT NULL,
                        prefijo VARCHAR(10) NOT NULL DEFAULT '',
                        numero_inicial INTEGER NOT NULL DEFAULT 1,
                        separador VARCHAR(5) NOT NULL DEFAULT '-',
                        reiniciar_anual BOOLEAN DEFAULT FALSE,
                        longitud_numero INTEGER NOT NULL DEFAULT 8,
                        activo BOOLEAN DEFAULT TRUE,
                        fecha_creacion TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )""")
                    
                    # Tabla de logs de auditoría
                    cursor.execute("""
                    CREATE TABLE IF NOT EXISTS audit_logs (
                        id SERIAL PRIMARY KEY,
                        user_id INTEGER,
                        action VARCHAR(50) NOT NULL,
                        table_name VARCHAR(100) NOT NULL,
                        record_id INTEGER,
                        old_values TEXT,
                        new_values TEXT,
                        ip_address INET,
                        user_agent TEXT,
                        session_id VARCHAR(255),
                        timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        FOREIGN KEY (user_id) REFERENCES usuarios (id) ON DELETE SET NULL
                    )""")
                    
                    # Tabla de acciones masivas pendientes
                    cursor.execute("""
                    CREATE TABLE IF NOT EXISTS acciones_masivas_pendientes (
                        id SERIAL PRIMARY KEY,
                        operation_id VARCHAR(100) UNIQUE NOT NULL,
                        tipo VARCHAR(50) NOT NULL,
                        descripcion TEXT,
                        usuario_ids INTEGER[] NOT NULL,
                        parametros JSONB,
                        estado VARCHAR(20) DEFAULT 'pendiente',
                        fecha_programada TIMESTAMP,
                        fecha_creacion TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        fecha_ejecucion TIMESTAMP,
                        creado_por INTEGER,
                        resultado JSONB,
                        error_message TEXT,
                        FOREIGN KEY (creado_por) REFERENCES usuarios (id) ON DELETE SET NULL
                    )""")

                    # --- TABLA TEMPORAL DE CHECK-IN POR QR ---
                    cursor.execute("""
                    CREATE TABLE IF NOT EXISTS checkin_pending (
                        id SERIAL PRIMARY KEY,
                        usuario_id INTEGER NOT NULL REFERENCES usuarios (id) ON DELETE CASCADE,
                        token VARCHAR(64) UNIQUE NOT NULL,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        expires_at TIMESTAMP NOT NULL,
                        used BOOLEAN DEFAULT FALSE
                    )""")

                    # Índices para checkin_pending
                    cursor.execute("CREATE INDEX IF NOT EXISTS idx_checkin_pending_expires_at ON checkin_pending (expires_at)")
                    cursor.execute("CREATE INDEX IF NOT EXISTS idx_checkin_pending_used ON checkin_pending (used)")
                    
                    

                    # --- CREAR ÍNDICES PARA OPTIMIZACIÓN ---
                    cursor.execute("CREATE INDEX IF NOT EXISTS idx_usuarios_nombre ON usuarios(nombre)")
                    cursor.execute("CREATE INDEX IF NOT EXISTS idx_usuarios_dni ON usuarios(dni)")
                    cursor.execute("CREATE INDEX IF NOT EXISTS idx_usuarios_activo ON usuarios(activo)")
                    cursor.execute("CREATE INDEX IF NOT EXISTS idx_usuarios_rol ON usuarios(rol)")
                    cursor.execute("CREATE INDEX IF NOT EXISTS idx_pagos_usuario_id ON pagos(usuario_id)")
                    cursor.execute("CREATE INDEX IF NOT EXISTS idx_pagos_fecha ON pagos(fecha_pago)")
                    cursor.execute("CREATE INDEX IF NOT EXISTS idx_asistencias_usuario_id ON asistencias(usuario_id)")
                    cursor.execute("CREATE INDEX IF NOT EXISTS idx_asistencias_fecha ON asistencias(fecha)")
                    # Índice eliminado: profesor_id ya no existe en clases_horarios
                    cursor.execute("CREATE INDEX IF NOT EXISTS idx_rutinas_usuario_id ON rutinas(usuario_id)")
                    cursor.execute("CREATE INDEX IF NOT EXISTS idx_profesor_suplencias_asignacion_fecha ON profesor_suplencias(asignacion_id, fecha_clase)")
                    cursor.execute("CREATE INDEX IF NOT EXISTS idx_profesor_suplencias_asignacion ON profesor_suplencias(asignacion_id)")
                    cursor.execute("CREATE INDEX IF NOT EXISTS idx_profesor_suplencias_suplente ON profesor_suplencias(profesor_suplente_id)")
                    cursor.execute("CREATE INDEX IF NOT EXISTS idx_profesor_suplencias_generales_fecha ON profesor_suplencias_generales(fecha)")
                    cursor.execute("CREATE INDEX IF NOT EXISTS idx_profesor_suplencias_generales_estado ON profesor_suplencias_generales(estado)")
                    cursor.execute("CREATE INDEX IF NOT EXISTS idx_profesor_suplencias_estado ON profesor_suplencias(estado)")
                    cursor.execute("CREATE INDEX IF NOT EXISTS idx_profesor_suplencias_fecha_clase ON profesor_suplencias(fecha_clase)")
                    cursor.execute("CREATE INDEX IF NOT EXISTS idx_custom_themes_activo ON custom_themes(activo)")
                    cursor.execute("CREATE INDEX IF NOT EXISTS idx_theme_schedules_activo ON theme_schedules(activo)")
                    cursor.execute("CREATE INDEX IF NOT EXISTS idx_theme_schedules_fechas ON theme_schedules(fecha_inicio, fecha_fin)")
                    cursor.execute("CREATE INDEX IF NOT EXISTS idx_acciones_masivas_estado ON acciones_masivas_pendientes(estado)")
                    cursor.execute("CREATE INDEX IF NOT EXISTS idx_acciones_masivas_usuario_ids ON acciones_masivas_pendientes USING GIN(usuario_ids)")
                    cursor.execute("CREATE INDEX IF NOT EXISTS idx_acciones_masivas_fecha_programada ON acciones_masivas_pendientes(fecha_programada)")
                    # Índices adicionales para optimizar claves foráneas y filtros frecuentes
                    cursor.execute("CREATE INDEX IF NOT EXISTS idx_estados_usuario_usuario_id ON estados_usuario(usuario_id)")
                    cursor.execute("CREATE INDEX IF NOT EXISTS idx_clases_horarios_clase_id ON clases_horarios(clase_id)")
                    cursor.execute("CREATE INDEX IF NOT EXISTS idx_rutina_ejercicios_rutina_id ON rutina_ejercicios(rutina_id)")
                    cursor.execute("CREATE INDEX IF NOT EXISTS idx_rutina_ejercicios_ejercicio_id ON rutina_ejercicios(ejercicio_id)")
                    cursor.execute("CREATE INDEX IF NOT EXISTS idx_rutina_ejercicios_rutina_ejercicio ON rutina_ejercicios(rutina_id, ejercicio_id)")
                    cursor.execute("CREATE INDEX IF NOT EXISTS idx_clase_usuarios_clase_horario_id ON clase_usuarios(clase_horario_id)")
                    cursor.execute("CREATE INDEX IF NOT EXISTS idx_clase_usuarios_usuario_id ON clase_usuarios(usuario_id)")
                    cursor.execute("CREATE INDEX IF NOT EXISTS idx_clase_ejercicios_ejercicio_id ON clase_ejercicios(ejercicio_id)")
                    cursor.execute("CREATE INDEX IF NOT EXISTS idx_clase_ejercicios_clase_orden ON clase_ejercicios(clase_id, orden)")
                    cursor.execute("CREATE INDEX IF NOT EXISTS idx_horarios_profesores_profesor_id ON horarios_profesores(profesor_id)")
                    cursor.execute("CREATE INDEX IF NOT EXISTS idx_profesores_horarios_disponibilidad_profesor_id ON profesores_horarios_disponibilidad(profesor_id)")
                    cursor.execute("CREATE INDEX IF NOT EXISTS idx_profesor_evaluaciones_profesor_id ON profesor_evaluaciones(profesor_id)")
                    cursor.execute("CREATE INDEX IF NOT EXISTS idx_profesor_evaluaciones_usuario_id ON profesor_evaluaciones(usuario_id)")
                    cursor.execute("CREATE INDEX IF NOT EXISTS idx_profesor_clase_asignaciones_profesor_id ON profesor_clase_asignaciones(profesor_id)")
                    cursor.execute("CREATE INDEX IF NOT EXISTS idx_usuario_notas_usuario_id ON usuario_notas(usuario_id)")
                    cursor.execute("CREATE INDEX IF NOT EXISTS idx_usuario_notas_autor_id ON usuario_notas(autor_id)")
                    cursor.execute("CREATE INDEX IF NOT EXISTS idx_usuario_etiquetas_usuario_id ON usuario_etiquetas(usuario_id)")
                    cursor.execute("CREATE INDEX IF NOT EXISTS idx_usuario_etiquetas_etiqueta_id ON usuario_etiquetas(etiqueta_id)")
                    cursor.execute("CREATE INDEX IF NOT EXISTS idx_usuario_etiquetas_asignado_por ON usuario_etiquetas(asignado_por)")
                    cursor.execute("CREATE INDEX IF NOT EXISTS idx_usuario_estados_usuario_id ON usuario_estados(usuario_id)")
                    cursor.execute("CREATE INDEX IF NOT EXISTS idx_usuario_estados_creado_por ON usuario_estados(creado_por)")
                    cursor.execute("CREATE INDEX IF NOT EXISTS idx_audit_logs_user_id ON audit_logs(user_id)")
                    cursor.execute("CREATE INDEX IF NOT EXISTS idx_theme_schedules_theme_id ON theme_schedules(theme_id)")
                    # Índices compuestos para ordenar y filtrar usuarios eficientemente
                    cursor.execute("CREATE INDEX IF NOT EXISTS idx_usuarios_rol_nombre ON usuarios(rol, nombre)")
                    cursor.execute("CREATE INDEX IF NOT EXISTS idx_usuarios_activo_rol_nombre ON usuarios(activo, rol, nombre)")
                    # Índices adicionales para 'clases' y operación de asistencias
                    cursor.execute("CREATE INDEX IF NOT EXISTS idx_clases_nombre ON clases(nombre)")
                    cursor.execute("CREATE INDEX IF NOT EXISTS idx_clases_activa_true_nombre ON clases(nombre) WHERE activa = TRUE")
                    cursor.execute("CREATE INDEX IF NOT EXISTS idx_clases_tipo_clase_id ON clases(tipo_clase_id)")
                    cursor.execute("CREATE INDEX IF NOT EXISTS idx_asistencias_usuario_fecha ON asistencias(usuario_id, fecha)")

                    # Recomendaciones de índices adicionales (comentadas) para patrones comunes:
                    # CREATE INDEX IF NOT EXISTS idx_clases_activa_nombre ON clases(activa, nombre);
                    # CREATE INDEX IF NOT EXISTS idx_clases_activa_true ON clases(id) WHERE activa = TRUE;
                    # CREATE INDEX IF NOT EXISTS idx_usuario_notas_usuario_activa_fecha ON usuario_notas(usuario_id, fecha_creacion) WHERE activa = TRUE;
                    # CREATE INDEX IF NOT EXISTS idx_usuario_notas_categoria_activa_fecha ON usuario_notas(categoria, fecha_creacion) WHERE activa = TRUE;
                    # CREATE INDEX IF NOT EXISTS idx_usuario_notas_importancia_activa_fecha ON usuario_notas(importancia, fecha_creacion) WHERE activa = TRUE;
                    # CREATE INDEX IF NOT EXISTS idx_usuario_estados_usuario_activo_fecha_inicio_desc ON usuario_estados(usuario_id, fecha_inicio DESC) WHERE activo = TRUE;
                    # CREATE INDEX IF NOT EXISTS idx_usuario_estados_activo_vencimiento ON usuario_estados(fecha_vencimiento) WHERE activo = TRUE;
                    # Índice funcional para optimizar consultas por mes/año en pagos
                    cursor.execute("CREATE INDEX IF NOT EXISTS idx_pagos_month_year ON pagos ((EXTRACT(MONTH FROM fecha_pago)), (EXTRACT(YEAR FROM fecha_pago)))")
                    # Índices compuestos para ordenar por fecha en historial
                    cursor.execute("CREATE INDEX IF NOT EXISTS idx_pagos_usuario_fecha_desc ON pagos(usuario_id, fecha_pago DESC)")
                    cursor.execute("CREATE INDEX IF NOT EXISTS idx_asistencias_usuario_fecha_desc ON asistencias(usuario_id, fecha DESC)")
                    # Actualizar estadísticas del planificador tras crear índices
                    cursor.execute("ANALYZE")
                    
                    # --- CREAR TABLAS ADICIONALES FALTANTES ---
                    
                    # Tabla especialidades
                    cursor.execute("""
                        CREATE TABLE IF NOT EXISTS especialidades (
                            id SERIAL PRIMARY KEY,
                            nombre VARCHAR(100) NOT NULL UNIQUE,
                            descripcion TEXT,
                            categoria VARCHAR(50),
                            activo BOOLEAN DEFAULT true,
                            fecha_creacion TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                        )
                    """)
                    
                    # Tabla profesor_especialidades
                    cursor.execute("""
                        CREATE TABLE IF NOT EXISTS profesor_especialidades (
                            id SERIAL PRIMARY KEY,
                            profesor_id INTEGER NOT NULL,
                            especialidad_id INTEGER NOT NULL,
                            fecha_asignacion TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                            activo BOOLEAN DEFAULT true,
                            FOREIGN KEY (profesor_id) REFERENCES profesores (id) ON DELETE CASCADE,
                            FOREIGN KEY (especialidad_id) REFERENCES especialidades (id) ON DELETE CASCADE,
                            UNIQUE(profesor_id, especialidad_id)
                        )
                    """)
                    
                    # Tabla profesor_certificaciones
                    cursor.execute("""
                        CREATE TABLE IF NOT EXISTS profesor_certificaciones (
                            id SERIAL PRIMARY KEY,
                            profesor_id INTEGER NOT NULL,
                            nombre_certificacion VARCHAR(200) NOT NULL,
                            institucion_emisora VARCHAR(200),
                            fecha_obtencion DATE,
                            fecha_vencimiento DATE,
                            numero_certificado VARCHAR(100),
                            archivo_adjunto VARCHAR(500),
                            estado VARCHAR(20) DEFAULT 'vigente',
                            notas TEXT,
                            activo BOOLEAN DEFAULT true,
                            fecha_creacion TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                            FOREIGN KEY (profesor_id) REFERENCES profesores (id) ON DELETE CASCADE
                        )
                    """)
                    
                    # Tabla profesor_horas_trabajadas
                    cursor.execute("""
                        CREATE TABLE IF NOT EXISTS profesor_horas_trabajadas (
                            id SERIAL PRIMARY KEY,
                            profesor_id INTEGER NOT NULL,
                            fecha DATE NOT NULL,
                            hora_inicio TIMESTAMP NOT NULL,
                            hora_fin TIMESTAMP,
                            minutos_totales INTEGER,
                            horas_totales DECIMAL(8,2),
                            tipo_actividad VARCHAR(50),
                            clase_id INTEGER,
                            notas TEXT,
                            fecha_creacion TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                            FOREIGN KEY (profesor_id) REFERENCES profesores (id) ON DELETE CASCADE,
                            FOREIGN KEY (clase_id) REFERENCES clases (id) ON DELETE SET NULL
                        )
                    """)
                    cursor.execute("ALTER TABLE IF EXISTS profesor_horas_trabajadas ALTER COLUMN tipo_actividad DROP DEFAULT")
                    
                    # Tabla pago_detalles
                    cursor.execute("""
                        CREATE TABLE IF NOT EXISTS pago_detalles (
                            id SERIAL PRIMARY KEY,
                            pago_id INTEGER NOT NULL,
                            concepto_id INTEGER,
                            descripcion TEXT,
                            cantidad DECIMAL(10,2) DEFAULT 1,
                            precio_unitario DECIMAL(10,2) NOT NULL,
                            subtotal DECIMAL(10,2) NOT NULL,
                            descuento DECIMAL(10,2) DEFAULT 0,
                            total DECIMAL(10,2) NOT NULL,
                            fecha_creacion TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                            FOREIGN KEY (pago_id) REFERENCES pagos (id) ON DELETE CASCADE,
                            FOREIGN KEY (concepto_id) REFERENCES conceptos_pago (id) ON DELETE SET NULL
                        )
                     """)
                     
                    # --- CREAR ÍNDICES PARA LAS NUEVAS TABLAS ---
                    cursor.execute("CREATE INDEX IF NOT EXISTS idx_especialidades_nombre ON especialidades(nombre)")
                    cursor.execute("CREATE INDEX IF NOT EXISTS idx_especialidades_activo ON especialidades(activo)")
                    cursor.execute("CREATE INDEX IF NOT EXISTS idx_profesor_especialidades_profesor_id ON profesor_especialidades(profesor_id)")
                    cursor.execute("CREATE INDEX IF NOT EXISTS idx_profesor_especialidades_especialidad_id ON profesor_especialidades(especialidad_id)")
                    cursor.execute("CREATE INDEX IF NOT EXISTS idx_profesor_especialidades_activo ON profesor_especialidades(activo)")
                    cursor.execute("CREATE INDEX IF NOT EXISTS idx_profesor_certificaciones_profesor_id ON profesor_certificaciones(profesor_id)")
                    cursor.execute("CREATE INDEX IF NOT EXISTS idx_profesor_certificaciones_fecha_vencimiento ON profesor_certificaciones(fecha_vencimiento)")
                    cursor.execute("CREATE INDEX IF NOT EXISTS idx_profesor_certificaciones_activo ON profesor_certificaciones(activo)")
                    cursor.execute("CREATE INDEX IF NOT EXISTS idx_profesor_horas_trabajadas_profesor_id ON profesor_horas_trabajadas(profesor_id)")
                    cursor.execute("CREATE INDEX IF NOT EXISTS idx_profesor_horas_trabajadas_fecha ON profesor_horas_trabajadas(fecha)")
                    cursor.execute("CREATE INDEX IF NOT EXISTS idx_profesor_horas_trabajadas_clase_id ON profesor_horas_trabajadas(clase_id)")
                    # Garantiza una sola sesión activa por profesor (parcial y única)
                    cursor.execute("""
                        CREATE UNIQUE INDEX IF NOT EXISTS uniq_sesion_activa_por_profesor
                        ON profesor_horas_trabajadas (profesor_id)
                        WHERE hora_fin IS NULL
                    """)
                    cursor.execute("CREATE INDEX IF NOT EXISTS idx_pago_detalles_pago_id ON pago_detalles(pago_id)")
                    cursor.execute("CREATE INDEX IF NOT EXISTS idx_pago_detalles_concepto_id ON pago_detalles(concepto_id)")
                    cursor.execute("CREATE INDEX IF NOT EXISTS idx_profesor_disponibilidad_profesor_id ON profesor_disponibilidad(profesor_id)")
                    cursor.execute("CREATE INDEX IF NOT EXISTS idx_profesor_disponibilidad_fecha ON profesor_disponibilidad(fecha)")
                    cursor.execute("CREATE INDEX IF NOT EXISTS idx_profesor_disponibilidad_profesor_fecha ON profesor_disponibilidad(profesor_id, fecha)")
                    
                    # --- TABLA HISTORIAL DE ESTADOS ---
                    cursor.execute("""
                    CREATE TABLE IF NOT EXISTS historial_estados (
                        id SERIAL PRIMARY KEY,
                        usuario_id INTEGER NOT NULL,
                        estado_id INTEGER,
                        accion VARCHAR(50) NOT NULL,
                        fecha_accion TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        detalles TEXT,
                        creado_por INTEGER,
                        FOREIGN KEY (usuario_id) REFERENCES usuarios (id) ON DELETE CASCADE,
                        FOREIGN KEY (estado_id) REFERENCES usuario_estados (id) ON DELETE CASCADE,
                        FOREIGN KEY (creado_por) REFERENCES usuarios (id) ON DELETE SET NULL
                    )""")
                    
                    # Modificar columna estado_id para permitir NULL si ya existe la tabla
                    cursor.execute("""
                        ALTER TABLE historial_estados 
                        ALTER COLUMN estado_id DROP NOT NULL
                    """)
                    
                    # Índices para historial_estados
                    cursor.execute("CREATE INDEX IF NOT EXISTS idx_historial_estados_usuario_id ON historial_estados(usuario_id)")
                    cursor.execute("CREATE INDEX IF NOT EXISTS idx_historial_estados_estado_id ON historial_estados(estado_id)")
                    cursor.execute("CREATE INDEX IF NOT EXISTS idx_historial_estados_fecha ON historial_estados(fecha_accion)")
                     
                     # --- INSERTAR DATOS POR DEFECTO ---
                    
                    # Configuración por defecto
                    default_info_html = "<h3>GMS</h3><p><b>Versión:</b> 4.0</p><p>Sistema de Gestión Integral.</p><p>Creado por Mateo Piedrabuena.</p>"
                    cursor.execute("INSERT INTO configuracion (clave, valor, descripcion) VALUES (%s, %s, %s) ON CONFLICT (clave) DO NOTHING", 
                                 ('precio_estandar', str(Config.DEFAULT_MEMBERSHIP_PRICE), 'Precio mensual de la cuota Estándar'))
                    cursor.execute("INSERT INTO configuracion (clave, valor, descripcion) VALUES (%s, %s, %s) ON CONFLICT (clave) DO NOTHING", 
                                 ('precio_estudiante', str(Config.DEFAULT_STUDENT_PRICE), 'Precio mensual de la cuota Estudiante'))
                    cursor.execute("INSERT INTO configuracion (clave, valor, descripcion) VALUES (%s, %s, %s) ON CONFLICT (clave) DO NOTHING", 
                                 ('system_info_html', default_info_html, 'Contenido HTML de la información del sistema'))
                    cursor.execute("INSERT INTO configuracion (clave, valor, descripcion) VALUES (%s, %s, %s) ON CONFLICT (clave) DO NOTHING", 
                                 ('global_font_size', '10', 'Tamaño de fuente global de la aplicación'))
                    
                    # Configuraciones adicionales de precios de cuotas
                    cursor.execute("INSERT INTO configuracion (clave, valor, descripcion) VALUES (%s, %s, %s) ON CONFLICT (clave) DO NOTHING", 
                                 ('precio_cuota_estandar', str(Config.DEFAULT_MEMBERSHIP_PRICE), 'Precio mensual de la cuota Estándar'))
                    cursor.execute("INSERT INTO configuracion (clave, valor, descripcion) VALUES (%s, %s, %s) ON CONFLICT (clave) DO NOTHING", 
                                 ('precio_cuota_estudiante', str(Config.DEFAULT_STUDENT_PRICE), 'Precio mensual de la cuota Estudiante'))
                    cursor.execute("INSERT INTO configuracion (clave, valor, descripcion) VALUES (%s, %s, %s) ON CONFLICT (clave) DO NOTHING", 
                                 ('precio_cuota_profesor', '15000', 'Precio mensual de la cuota Profesor'))
                    cursor.execute("INSERT INTO configuracion (clave, valor, descripcion) VALUES (%s, %s, %s) ON CONFLICT (clave) DO NOTHING", 
                                 ('precio_cuota_jubilado', '12000', 'Precio mensual de la cuota Jubilado'))
                    cursor.execute("INSERT INTO configuracion (clave, valor, descripcion) VALUES (%s, %s, %s) ON CONFLICT (clave) DO NOTHING", 
                                 ('precio_cuota_familiar', '25000', 'Precio mensual de la cuota Familiar'))
                    cursor.execute("INSERT INTO configuracion (clave, valor, descripcion) VALUES (%s, %s, %s) ON CONFLICT (clave) DO NOTHING", 
                                 ('precio_cuota_corporativa', '18000', 'Precio mensual de la cuota Corporativa'))
                    cursor.execute("INSERT INTO configuracion (clave, valor, descripcion) VALUES (%s, %s, %s) ON CONFLICT (clave) DO NOTHING", 
                                 ('precio_cuota_premium', '35000', 'Precio mensual de la cuota Premium'))
                    cursor.execute("INSERT INTO configuracion (clave, valor, descripcion) VALUES (%s, %s, %s) ON CONFLICT (clave) DO NOTHING", 
                                 ('precio_cuota_vip', '50000', 'Precio mensual de la cuota VIP'))
                    
                    # Configuraciones del gimnasio
                    cursor.execute("INSERT INTO configuracion (clave, valor, descripcion) VALUES (%s, %s, %s) ON CONFLICT (clave) DO NOTHING", 
                                 ('gym_name', 'Gimnasio GMS', 'Nombre del gimnasio'))
                    cursor.execute("INSERT INTO configuracion (clave, valor, descripcion) VALUES (%s, %s, %s) ON CONFLICT (clave) DO NOTHING", 
                                 ('gym_address', 'Dirección no configurada', 'Dirección del gimnasio'))
                    cursor.execute("INSERT INTO configuracion (clave, valor, descripcion) VALUES (%s, %s, %s) ON CONFLICT (clave) DO NOTHING", 
                                 ('gym_phone', 'Teléfono no configurado', 'Teléfono del gimnasio'))
                    cursor.execute("INSERT INTO configuracion (clave, valor, descripcion) VALUES (%s, %s, %s) ON CONFLICT (clave) DO NOTHING", 
                                 ('gym_email', 'email@gimnasio.com', 'Email del gimnasio'))
                    
                    # Configuraciones de backup y sistema
                    cursor.execute("INSERT INTO configuracion (clave, valor, descripcion) VALUES (%s, %s, %s) ON CONFLICT (clave) DO NOTHING", 
                                 ('backup_frequency', '7', 'Frecuencia de backup en días'))
                    cursor.execute("INSERT INTO configuracion (clave, valor, descripcion) VALUES (%s, %s, %s) ON CONFLICT (clave) DO NOTHING", 
                                 ('max_backup_files', '10', 'Número máximo de archivos de backup a mantener'))
                    cursor.execute("INSERT INTO configuracion (clave, valor, descripcion) VALUES (%s, %s, %s) ON CONFLICT (clave) DO NOTHING", 
                                 ('session_timeout', '30', 'Tiempo de sesión en minutos'))
                    cursor.execute("INSERT INTO configuracion (clave, valor, descripcion) VALUES (%s, %s, %s) ON CONFLICT (clave) DO NOTHING", 
                                 ('auto_logout', 'false', 'Logout automático habilitado'))
                    cursor.execute("INSERT INTO configuracion (clave, valor, descripcion) VALUES (%s, %s, %s) ON CONFLICT (clave) DO NOTHING", 
                                 ('theme_mode', 'light', 'Modo de tema (light/dark)'))
                    cursor.execute("INSERT INTO configuracion (clave, valor, descripcion) VALUES (%s, %s, %s) ON CONFLICT (clave) DO NOTHING", 
                                 ('language', 'es', 'Idioma del sistema'))
                    # Sembrar contraseña del Dueño desde entorno (hasheada) si está disponible
                    try:
                        env_pwd = (os.getenv('WEBAPP_OWNER_PASSWORD', '') or os.getenv('OWNER_PASSWORD', '')).strip()
                    except Exception:
                        env_pwd = ''
                    if env_pwd:
                        try:
                            from .security_utils import SecurityUtils
                            hashed_pwd = SecurityUtils.hash_password(env_pwd)
                        except Exception:
                            # Fallback a texto plano solo si falla bcrypt
                            hashed_pwd = env_pwd
                        try:
                            cursor.execute(
                                "INSERT INTO configuracion (clave, valor, descripcion) VALUES (%s, %s, %s) ON CONFLICT (clave) DO NOTHING",
                                ('owner_password', hashed_pwd, 'Contraseña del Dueño para acceso administrativo')
                            )
                        except Exception:
                            # No bloquear inicialización por errores de seed
                            pass

                    # Usuario dueño por defecto ya creado antes de aplicar protecciones (ver sección anterior)

                    # Insertar estados básicos si no existen (después de crear el usuario dueño)
                    if owner_id is not None:
                        cursor.execute(
                            """
                            INSERT INTO usuario_estados (id, usuario_id, estado, descripcion, activo) 
                            SELECT 1, %s, 'activo', 'Estado activo por defecto', true
                            WHERE NOT EXISTS (SELECT 1 FROM usuario_estados WHERE id = 1)
                            """,
                            (owner_id,)
                        )
                        cursor.execute(
                            """
                            INSERT INTO usuario_estados (id, usuario_id, estado, descripcion, activo) 
                            SELECT 2, %s, 'inactivo', 'Estado inactivo por defecto', true
                            WHERE NOT EXISTS (SELECT 1 FROM usuario_estados WHERE id = 2)
                            """,
                            (owner_id,)
                        )
                        cursor.execute(
                            """
                            INSERT INTO usuario_estados (id, usuario_id, estado, descripcion, activo) 
                            SELECT 3, %s, 'suspendido', 'Estado suspendido por defecto', true
                            WHERE NOT EXISTS (SELECT 1 FROM usuario_estados WHERE id = 3)
                            """,
                            (owner_id,)
                        )
                    
                    # Migrar datos por defecto
                    self._migrar_tipos_cuota_existentes(cursor)
                    self._migrar_metodos_conceptos_pago(cursor)
                    
                    # Migrar tabla de historial de asistencia a clases
                    self._migrar_tabla_clase_asistencia_historial(cursor)
                    
                    # Migrar sistema de comprobantes de pago
                    self._migrar_sistema_comprobantes(cursor)
                    
                    conn.commit()
                    
                # Crear tablas de WhatsApp después del commit principal
                self._crear_tablas_whatsapp()
                
                # Migrar campos adicionales para cuotas vencidas
                with self.get_connection_context() as conn:
                    with conn.cursor() as cursor:
                        self._migrar_campos_cuotas_vencidas(cursor)
                        self._verificar_columna_minutos_totales(cursor)
                        self._migrar_columna_tipo_profesores(cursor)
                        self._migrar_series_a_varchar(cursor)
                    conn.commit()
                
                # Asegurar índices críticos tras la creación y migraciones
                try:
                    self.ensure_indexes()
                except Exception as e:
                    logging.warning(f"Error al asegurar índices durante la inicialización: {e}")
                try:
                    dbkey = f"{self.connection_params.get('host')}:{self.connection_params.get('port')}:{self.connection_params.get('dbname') or self.connection_params.get('database')}"
                except Exception:
                    dbkey = str(self.connection_params.get('dbname') or self.connection_params.get('database') or '')
                try:
                    if dbkey:
                        _INIT_DONE_DBS.add(dbkey)
                except Exception:
                    pass
        finally:
            self._initializing = False

    def _migrar_tipos_cuota_existentes(self, cursor):
        return self.payment_repo._migrar_tipos_cuota_existentes(cursor)

    def _migrar_metodos_conceptos_pago(self, cursor):
        return self.payment_repo._migrar_metodos_conceptos_pago(cursor)

    def crear_usuario(self, usuario: Usuario) -> int:
        return self.user_repo.crear_usuario(usuario)

    def registrar_usuarios_batch(self, items: List[Dict[str, Any]], *, skip_duplicates: bool = True, validate_data: bool = True) -> Dict[str, Any]:
        return self.user_repo.registrar_usuarios_batch(items, skip_duplicates=skip_duplicates, validate_data=validate_data)

    def obtener_usuario(self, usuario_id: int) -> Optional[Usuario]:
        return self.user_repo.obtener_usuario(usuario_id)

    def obtener_usuario_por_id(self, usuario_id: int) -> Optional[Usuario]:
        return self.user_repo.obtener_usuario_por_id(usuario_id)

    def obtener_todos_usuarios(self) -> List[Usuario]:
        return self.user_repo.obtener_todos_usuarios()

    def obtener_todos_pagos(self) -> List:
        return self.payment_repo.obtener_todos_pagos()

    def _crear_tabla_acciones_masivas_pendientes(self):
        return self.gym_repo._crear_tabla_acciones_masivas_pendientes()

    def obtener_todos_ejercicios(self) -> List:
        return self.gym_repo.obtener_todos_ejercicios()

    def obtener_todas_rutinas(self) -> List:
        return self.gym_repo.obtener_todas_rutinas()

    def obtener_todas_clases(self) -> List:
        return self.gym_repo.obtener_todas_clases()

    def obtener_usuario_por_rol(self, rol: str, timeout_ms: Optional[int] = None) -> Optional[Usuario]:
        return self.user_repo.obtener_usuario_por_rol(rol, timeout_ms)

    def desactivar_usuarios_por_falta_de_pago(self) -> List[Dict]:
        return self.user_repo.desactivar_usuarios_por_falta_de_pago()

    def obtener_usuarios_por_rol(self, rol: str) -> List[Usuario]:
        return self.user_repo.obtener_usuarios_por_rol(rol)

    def obtener_arpu_y_morosos_mes_actual(self) -> Tuple[float, int]:
        return self.gym_repo.obtener_arpu_y_morosos_mes_actual()

    def actualizar_usuario(self, usuario: Usuario):
        return self.user_repo.actualizar_usuario(usuario)

    def eliminar_usuario(self, usuario_id: int):
        return self.user_repo.eliminar_usuario(usuario_id)

    def dni_existe(self, dni: str, user_id_to_ignore: Optional[int] = None) -> bool:
        return self.gym_repo.dni_existe(dni, user_id_to_ignore)

    def usuario_id_existe(self, usuario_id: int) -> bool:
        return self.user_repo.usuario_id_existe(usuario_id)

    def obtener_resumen_referencias_usuario(self, usuario_id: int) -> dict:
        return self.user_repo.obtener_resumen_referencias_usuario(usuario_id)

    def obtener_metodos_pago(self, solo_activos: bool = True) -> List[Dict]:
        return self.payment_repo.obtener_metodos_pago(solo_activos)

    def obtener_conceptos_pago(self, solo_activos: bool = True) -> List[Dict]:
        return self.payment_repo.obtener_conceptos_pago(solo_activos)

    def registrar_ejercicios_batch(self, ejercicios_items: List[Dict[str, Any]], skip_duplicates: bool = True, validate_data: bool = True) -> Dict[str, Any]:
        return self.gym_repo.registrar_ejercicios_batch(ejercicios_items, skip_duplicates, validate_data)

    def registrar_clases_batch(self, clases_items: List[Dict[str, Any]], skip_duplicates: bool = True, validate_data: bool = True) -> Dict[str, Any]:
        return self.gym_repo.registrar_clases_batch(clases_items, skip_duplicates, validate_data)

    def registrar_pagos_batch(self, pagos_items: List[Dict[str, Any]], skip_duplicates: bool = False, validate_data: bool = True, auto_crear_metodos_pago: bool = False) -> Dict[str, Any]:
        return self.payment_repo.registrar_pagos_batch(pagos_items, skip_duplicates, validate_data, auto_crear_metodos_pago)

    def registrar_asistencia_comun(self, usuario_id: int, fecha: date) -> int:
        return self.attendance_repo.registrar_asistencia_comun(usuario_id, fecha)

    def obtener_ids_asistencia_hoy(self) -> Set[int]:
        return self.attendance_repo.obtener_ids_asistencia_hoy()

    def obtener_asistencias_por_fecha(self, fecha) -> List[Dict]:
        return self.attendance_repo.obtener_asistencias_por_fecha(fecha)

    def obtener_configuracion(self, clave: str, timeout_ms: int = 800) -> Optional[str]:
        return self.gym_repo.obtener_configuracion(clave, timeout_ms)

    def actualizar_configuracion(self, clave: str, valor: str) -> bool:
        return self.gym_repo.actualizar_configuracion(clave, valor)

    def actualizar_fecha_proximo_vencimiento(self, usuario_id: int, fecha_pago: date = None) -> bool:
        return self.payment_repo.actualizar_fecha_proximo_vencimiento(usuario_id, fecha_pago)

    def obtener_usuarios_con_cuotas_por_vencer(self, dias_anticipacion: int = 3) -> List[Dict[str, Any]]:
        return self.user_repo.obtener_usuarios_con_cuotas_por_vencer(dias_anticipacion)

    def obtener_usuarios_morosos(self) -> List[Dict[str, Any]]:
        return self.user_repo.obtener_usuarios_morosos()

    def incrementar_cuotas_vencidas(self, usuario_id: int) -> bool:
        return self.payment_repo.incrementar_cuotas_vencidas(usuario_id)

    def desactivar_usuario_por_cuotas_vencidas(self, usuario_id: int, motivo: str = "3 cuotas vencidas consecutivas") -> bool:
        return self.user_repo.desactivar_usuario_por_cuotas_vencidas(usuario_id, motivo)

    def actualizar_configuracion_moneda(self, moneda: str, simbolo: str = ""):
        return self.gym_repo.actualizar_configuracion_moneda(moneda, simbolo)

    def actualizar_configuracion_politica_contraseñas(self, longitud_minima: int = 8, 
                                                      requiere_mayusculas: bool = True,
                                                      requiere_numeros: bool = True,
                                                      requiere_simbolos: bool = False):
        return self.gym_repo.actualizar_configuracion_politica_contraseñas(longitud_minima, requiere_mayusculas, requiere_numeros, requiere_simbolos)

    def actualizar_configuracion_notificaciones_email(self, habilitado: bool = True,
                                                      servidor_smtp: str = "",
                                                      puerto: int = 587,
                                                      usuario: str = "",
                                                      usar_tls: bool = True):
        return self.gym_repo.actualizar_configuracion_notificaciones_email(habilitado, servidor_smtp, puerto, usuario, usar_tls)

    def actualizar_configuracion_notificaciones_sms(self, habilitado: bool = False,
                                                    proveedor: str = "",
                                                    api_key: str = "",
                                                    numero_remitente: str = ""):
        return self.gym_repo.actualizar_configuracion_notificaciones_sms(habilitado, proveedor, api_key, numero_remitente)

    def actualizar_configuracion_umbrales_alerta(self, capacidad_maxima: int = 100,
                                                 umbral_ocupacion: int = 80,
                                                 dias_vencimiento_membresia: int = 7,
                                                 monto_minimo_pago: float = 0.0):
        return self.gym_repo.actualizar_configuracion_umbrales_alerta(capacidad_maxima, umbral_ocupacion, dias_vencimiento_membresia, monto_minimo_pago)

    def actualizar_configuracion_automatizacion(self, configuraciones: dict):
        return self.gym_repo.actualizar_configuracion_automatizacion(configuraciones)

    def guardar_configuracion_sistema(self, clave: str, valor: str):
        return self.gym_repo.guardar_configuracion_sistema(clave, valor)

    def crear_clase(self, clase: Clase) -> int:
        return self.gym_repo.crear_clase(clase)

    def obtener_clases(self) -> List[Clase]:
        return self.gym_repo.obtener_clases()

    def crear_profesor(self, usuario_id: int, especialidades: str = "", certificaciones: str = "", 
                      experiencia_años: int = 0, tarifa_por_hora: float = 0.0, 
                      fecha_contratacion: date = None, biografia: str = "", 
                      telefono_emergencia: str = "") -> int:
        return self.teacher_repo.crear_profesor(usuario_id, especialidades, certificaciones, experiencia_años, tarifa_por_hora, fecha_contratacion, biografia, telefono_emergencia)

    def obtener_profesor_por_usuario_id(self, usuario_id: int) -> Optional['Usuario']:
        return self.user_repo.obtener_profesor_por_usuario_id(usuario_id)

    def obtener_profesor_por_id(self, profesor_id: int) -> Optional[Dict]:
        return self.teacher_repo.obtener_profesor_por_id(profesor_id)

    def actualizar_profesor(self, profesor_id: int, **kwargs) -> bool:
        return self.teacher_repo.actualizar_profesor(profesor_id, **kwargs)

    def actualizar_estado_profesor(self, profesor_id: int, nuevo_estado: str) -> bool:
        return self.teacher_repo.actualizar_estado_profesor(profesor_id, nuevo_estado)

    def limpiar_cache_usuarios(self):
        return self.user_repo.limpiar_cache_usuarios()

    def backup_database(self, backup_path: str) -> bool:
        return self.gym_repo.backup_database(backup_path)

    def _create_sql_backup_basic(self, backup_path: str) -> bool:
        return self.gym_repo._create_sql_backup_basic(backup_path)

    def obtener_kpis_generales(self) -> Dict:
        return self.reports_repo.obtener_kpis_generales()

    def verificar_conexion(self) -> bool:
        return self.gym_repo.verificar_conexion()

    def obtener_info_base_datos(self) -> Dict:
        return self.gym_repo.obtener_info_base_datos()

    def _procesar_lote_cambio_rol_robusto(self, cursor, lote: List[int], nuevo_rol: str, resultados: dict):
        return self.user_repo._procesar_lote_cambio_rol_robusto(cursor, lote, nuevo_rol, resultados)

    def _procesar_lote_cambio_rol(self, cursor, lote: List[int], nuevo_rol: str, resultados: dict):
        return self.user_repo._procesar_lote_cambio_rol(cursor, lote, nuevo_rol, resultados)

    def _procesar_acciones_individuales_robusto(self, cursor, lote: List[int], accion: str, parametros: dict, resultados: dict):
        return self.gym_repo._procesar_acciones_individuales_robusto(cursor, lote, accion, parametros, resultados)

    def _procesar_acciones_individuales(self, cursor, lote: List[int], accion: str, parametros: dict, resultados: dict):
        return self.gym_repo._procesar_acciones_individuales(cursor, lote, accion, parametros, resultados)

    def _procesar_agregar_estado_usuario(self, cursor, usuario_id: int, parametros: dict, resultados: dict):
        return self.user_repo._procesar_agregar_estado_usuario(cursor, usuario_id, parametros, resultados)

    def _procesar_asignar_etiqueta_usuario(self, cursor, usuario_id: int, parametros: dict, resultados: dict):
        return self.user_repo._procesar_asignar_etiqueta_usuario(cursor, usuario_id, parametros, resultados)

    def limpiar_cache_usuarios(self):
        return self.user_repo.limpiar_cache_usuarios()

    def buscar_usuarios(self, termino_busqueda: str, limite: int = 50) -> List[Usuario]:
        return self.user_repo.buscar_usuarios(termino_busqueda, limite)

    def obtener_estadisticas_usuarios(self) -> dict:
        return self.user_repo.obtener_estadisticas_usuarios()

    def usuario_tiene_pagos(self, usuario_id: int) -> bool:
        return self.user_repo.usuario_tiene_pagos(usuario_id)

    def usuario_tiene_asistencias(self, usuario_id: int) -> bool:
        return self.user_repo.usuario_tiene_asistencias(usuario_id)

    def usuario_tiene_rutinas(self, usuario_id: int) -> bool:
        return self.user_repo.usuario_tiene_rutinas(usuario_id)

    def usuario_tiene_clases(self, usuario_id: int) -> bool:
        return self.user_repo.usuario_tiene_clases(usuario_id)

    def cambiar_usuario_id(self, old_id: int, new_id: int) -> None:
        return self.user_repo.cambiar_usuario_id(old_id, new_id)

    def renumerar_usuario_ids(self, start_id: int = 1) -> dict:
        return self.user_repo.renumerar_usuario_ids(start_id)

    def crear_grupo_ejercicios(self, nombre_grupo: str, ejercicio_ids: List[int]) -> int:
        return self.gym_repo.crear_grupo_ejercicios(nombre_grupo, ejercicio_ids)

    def obtener_grupos_ejercicios(self) -> List[EjercicioGrupo]:
        return self.gym_repo.obtener_grupos_ejercicios()

    def obtener_ejercicios_de_grupo(self, grupo_id: int) -> List[Ejercicio]:
        return self.gym_repo.obtener_ejercicios_de_grupo(grupo_id)

    def eliminar_grupo_ejercicios(self, grupo_id: int) -> bool:
        return self.gym_repo.eliminar_grupo_ejercicios(grupo_id)

    def limpiar_datos_antiguos(self, years: int) -> tuple[int, int]:
        return self.gym_repo.limpiar_datos_antiguos(years)

    def obtener_precio_cuota(self, tipo_cuota: str) -> float:
        return self.payment_repo.obtener_precio_cuota(tipo_cuota)

    def actualizar_precio_cuota(self, tipo_cuota: str, nuevo_precio: float):
        return self.payment_repo.actualizar_precio_cuota(tipo_cuota, nuevo_precio)

    def obtener_tipo_cuota_por_nombre(self, nombre: str) -> Optional[TipoCuota]:
        return self.payment_repo.obtener_tipo_cuota_por_nombre(nombre)

    def crear_nota_usuario(self, nota) -> int:
        return self.user_repo.crear_nota_usuario(nota)

    def obtener_notas_usuario(self, usuario_id: int, solo_activas: bool = True) -> List:
        return self.user_repo.obtener_notas_usuario(usuario_id, solo_activas)

    def obtener_nota_por_id(self, nota_id: int):
        return self.gym_repo.obtener_nota_por_id(nota_id)

    def actualizar_nota_usuario(self, nota) -> bool:
        return self.user_repo.actualizar_nota_usuario(nota)

    def eliminar_nota_usuario(self, nota_id: int, eliminar_permanente: bool = False) -> bool:
        return self.user_repo.eliminar_nota_usuario(nota_id, eliminar_permanente)

    def obtener_notas_por_categoria(self, categoria: str) -> List:
        return self.gym_repo.obtener_notas_por_categoria(categoria)

    def obtener_notas_por_importancia(self, importancia: str) -> List:
        return self.gym_repo.obtener_notas_por_importancia(importancia)

    def obtener_notas_y_etiquetas_usuario(self, usuario_id: int, solo_activas_notas: bool = True) -> Dict[str, List]:
        return self.user_repo.obtener_notas_y_etiquetas_usuario(usuario_id, solo_activas_notas)

    def crear_etiqueta(self, etiqueta) -> int:
        return self.gym_repo.crear_etiqueta(etiqueta)

    def obtener_todas_etiquetas(self, solo_activas: bool = True) -> List:
        return self.gym_repo.obtener_todas_etiquetas(solo_activas)

    def obtener_etiquetas(self, solo_activas: bool = True) -> List:
        return self.gym_repo.obtener_etiquetas(solo_activas)

    def obtener_etiqueta_por_id(self, etiqueta_id: int):
        return self.gym_repo.obtener_etiqueta_por_id(etiqueta_id)

    def obtener_etiqueta_por_nombre(self, nombre: str):
        return self.gym_repo.obtener_etiqueta_por_nombre(nombre)

    def obtener_o_crear_etiqueta(self, nombre: str, color: str = "#007bff", descripcion: str = ""):
        return self.gym_repo.obtener_o_crear_etiqueta(nombre, color, descripcion)

    def actualizar_etiqueta(self, etiqueta) -> bool:
        return self.gym_repo.actualizar_etiqueta(etiqueta)

    def eliminar_etiqueta(self, etiqueta_id: int) -> bool:
        return self.gym_repo.eliminar_etiqueta(etiqueta_id)

    def asignar_etiqueta_usuario(self, usuario_id: int, etiqueta_id: int, asignado_por: int = None) -> bool:
        return self.user_repo.asignar_etiqueta_usuario(usuario_id, etiqueta_id, asignado_por)

    def asignar_etiquetas_usuario_bulk(self, usuario_id: int, etiqueta_ids: List[int], asignado_por: int = None) -> Dict[str, int]:
        return self.user_repo.asignar_etiquetas_usuario_bulk(usuario_id, etiqueta_ids, asignado_por)

    def desasignar_etiqueta_usuario(self, usuario_id: int, etiqueta_id: int) -> bool:
        return self.user_repo.desasignar_etiqueta_usuario(usuario_id, etiqueta_id)

    def obtener_etiquetas_usuario(self, usuario_id: int) -> List:
        return self.user_repo.obtener_etiquetas_usuario(usuario_id)

    def obtener_usuarios_por_etiqueta(self, etiqueta_id: int) -> List:
        return self.user_repo.obtener_usuarios_por_etiqueta(etiqueta_id)

    def crear_estado_usuario(self, estado, motivo: str = None, ip_origen: str = None) -> int:
        return self.user_repo.crear_estado_usuario(estado, motivo, ip_origen)

    def obtener_estados_usuario(self, usuario_id: int, solo_activos: bool = True) -> List:
        return self.user_repo.obtener_estados_usuario(usuario_id, solo_activos)

    def obtener_plantillas_estados(self) -> List:
        return self.gym_repo.obtener_plantillas_estados()

    def actualizar_estado_usuario(self, estado, usuario_modificador: int = None, motivo: str = None, ip_origen: str = None) -> bool:
        return self.user_repo.actualizar_estado_usuario(estado, usuario_modificador, motivo, ip_origen)

    def eliminar_estado_usuario(self, estado_id: int, usuario_modificador: int = None, motivo: str = None, ip_origen: str = None) -> bool:
        return self.user_repo.eliminar_estado_usuario(estado_id, usuario_modificador, motivo, ip_origen)

    def obtener_estados_vencidos(self) -> List:
        return self.gym_repo.obtener_estados_vencidos()

    def desactivar_estados_vencidos(self) -> int:
        return self.gym_repo.desactivar_estados_vencidos()

    def limpiar_estados_vencidos(self) -> int:
        return self.gym_repo.limpiar_estados_vencidos()

    def obtener_historial_estados_usuario(self, usuario_id: int, limite: int = 50) -> List:
        return self.user_repo.obtener_historial_estados_usuario(usuario_id, limite)

    def verificar_vencimientos_cuotas_automatico(self, dias_vencimiento: int = 30, dias_alerta: int = 5) -> dict:
        return self.payment_repo.verificar_vencimientos_cuotas_automatico(dias_vencimiento, dias_alerta)

    def generar_reporte_usuarios_periodo(self, fecha_inicio: str, fecha_fin: str, incluir_inactivos: bool = False) -> dict:
        return self.user_repo.generar_reporte_usuarios_periodo(fecha_inicio, fecha_fin, incluir_inactivos)

    def verificar_pin_usuario(self, usuario_id: int, pin: str) -> bool:
        return self.user_repo.verificar_pin_usuario(usuario_id, pin)

    def generar_reporte_automatico_periodo(self, tipo_reporte: str, fecha_inicio: date, fecha_fin: date) -> dict:
        return self.reports_repo.generar_reporte_automatico_periodo(tipo_reporte, fecha_inicio, fecha_fin)

    def obtener_configuracion_numeracion(self, tipo_comprobante: str = None) -> List[Dict]:
        return self.gym_repo.obtener_configuracion_numeracion(tipo_comprobante)

    def actualizar_configuracion_numeracion(self, tipo_comprobante: str, prefijo: str = None, 
                                          siguiente_numero: int = None, longitud_numero: int = None,
                                          activo: bool = None) -> bool:
        return self.gym_repo.actualizar_configuracion_numeracion(tipo_comprobante, prefijo, siguiente_numero, longitud_numero, activo)

    def get_receipt_numbering_config(self) -> dict:
        return self.gym_repo.get_receipt_numbering_config()

    def save_receipt_numbering_config(self, config: dict) -> bool:
        return self.gym_repo.save_receipt_numbering_config(config)

    def get_next_receipt_number(self) -> str:
        return self.gym_repo.get_next_receipt_number()

    def actualizar_estados_automaticos(self) -> dict:
        return self.gym_repo.actualizar_estados_automaticos()

    def obtener_configuracion_automatizacion(self) -> dict:
        return self.gym_repo.obtener_configuracion_automatizacion()

    def _vencer_estados_antiguos(self, cursor) -> list:
        return self.gym_repo._vencer_estados_antiguos(cursor)

    def _identificar_usuarios_para_actualizar(self, cursor, config) -> list:
        return self.user_repo._identificar_usuarios_para_actualizar(cursor, config)

    def _aplicar_actualizaciones_de_estado(self, cursor, usuarios, config) -> tuple[int, list]:
        return self.gym_repo._aplicar_actualizaciones_de_estado(cursor, usuarios, config)

    def _cambiar_estado_usuario(self, cursor, usuario_id, nuevo_estado, descripcion):
        return self.user_repo._cambiar_estado_usuario(cursor, usuario_id, nuevo_estado, descripcion)

    def registrar_historial_estado(self, usuario_id: int, estado_id: int = None, accion: str = 'crear',
                                 estado_anterior: str = None, estado_nuevo: str = None,
                                 motivo: str = None, usuario_modificador: int = None):
        return self.gym_repo.registrar_historial_estado(usuario_id, estado_id, accion, estado_anterior, estado_nuevo, motivo, usuario_modificador)

    def _registrar_historial_fallback(self, usuario_id: int, accion: str, estado_anterior: str = None, estado_nuevo: str = None):
        return self.gym_repo._registrar_historial_fallback(usuario_id, accion, estado_anterior, estado_nuevo)

    def obtener_alertas_estados_proximos_vencer(self, dias_anticipacion: int = 7) -> List[dict]:
        return self.gym_repo.obtener_alertas_estados_proximos_vencer(dias_anticipacion)

    def ejecutar_accion_masiva_usuarios(self, usuario_ids: List[int], accion: str, parametros: dict = None) -> dict:
        return self.user_repo.ejecutar_accion_masiva_usuarios(usuario_ids, accion, parametros)

    def _ejecutar_accion_masiva_usuarios_interno(self, usuario_ids: List[int], accion: str, parametros: dict = None) -> dict:
        return self.user_repo._ejecutar_accion_masiva_usuarios_interno(usuario_ids, accion, parametros)

    def _procesar_lote_activacion_robusto(self, cursor, lote: List[int], accion: str, resultados: dict):
        return self.gym_repo._procesar_lote_activacion_robusto(cursor, lote, accion, resultados)

    def _procesar_lote_activacion(self, cursor, lote: List[int], accion: str, resultados: dict):
        return self.gym_repo._procesar_lote_activacion(cursor, lote, accion, resultados)

    def _procesar_lote_eliminacion_robusto(self, cursor, lote: List[int], resultados: dict):
        return self.gym_repo._procesar_lote_eliminacion_robusto(cursor, lote, resultados)

    def _procesar_lote_cambio_rol_robusto(self, cursor, lote: List[int], nuevo_rol: str, resultados: dict):
        return self.user_repo._procesar_lote_cambio_rol_robusto(cursor, lote, nuevo_rol, resultados)

    def _procesar_acciones_individuales_robusto(self, cursor, lote: List[int], accion: str, parametros: dict, resultados: dict):
        return self.gym_repo._procesar_acciones_individuales_robusto(cursor, lote, accion, parametros, resultados)

    def crear_rutina(self, rutina: Rutina) -> int:
        return self.gym_repo.crear_rutina(rutina)

    def actualizar_rutina(self, rutina: Rutina) -> bool:
        return self.gym_repo.actualizar_rutina(rutina)

    def obtener_rutinas_por_usuario(self, usuario_id: int) -> List[Rutina]:
        return self.user_repo.obtener_rutinas_por_usuario(usuario_id)

    def obtener_rutinas_usuario(self, usuario_id: int) -> List[Rutina]:
        return self.user_repo.obtener_rutinas_usuario(usuario_id)

    def obtener_rutina_completa(self, rutina_id: int) -> Optional[Rutina]:
        return self.gym_repo.obtener_rutina_completa(rutina_id)

    def obtener_rutina_completa_por_uuid(self, uuid_rutina: str) -> Optional[Rutina]:
        return self.gym_repo.obtener_rutina_completa_por_uuid(uuid_rutina)

    def guardar_ejercicios_de_rutina(self, rutina_id: int, rutina_ejercicios: List[RutinaEjercicio]) -> bool:
        return self.gym_repo.guardar_ejercicios_de_rutina(rutina_id, rutina_ejercicios)

    def actualizar_ejercicio(self, ejercicio: Ejercicio):
        return self.gym_repo.actualizar_ejercicio(ejercicio)

    def eliminar_ejercicio(self, ejercicio_id: int):
        return self.gym_repo.eliminar_ejercicio(ejercicio_id)

    def obtener_plantillas_rutina(self) -> List[Rutina]:
        return self.gym_repo.obtener_plantillas_rutina()

    def eliminar_rutina(self, rutina_id: int) -> bool:
        return self.gym_repo.eliminar_rutina(rutina_id)

    def actualizar_clase(self, clase: Clase):
        return self.gym_repo.actualizar_clase(clase)

    def eliminar_clase(self, clase_id: int):
        return self.gym_repo.eliminar_clase(clase_id)

    def crear_horario_clase(self, horario: ClaseHorario) -> int:
        return self.gym_repo.crear_horario_clase(horario)

    def obtener_horarios_de_clase(self, clase_id: int) -> List[ClaseHorario]:
        return self.gym_repo.obtener_horarios_de_clase(clase_id)

    def obtener_horario_por_id(self, clase_horario_id: int) -> Optional[Dict]:
        return self.gym_repo.obtener_horario_por_id(clase_horario_id)

    def obtener_horarios_profesor(self, profesor_id: int) -> List[dict]:
        return self.gym_repo.obtener_horarios_profesor(profesor_id)

    def obtener_horarios_profesor_dia(self, profesor_id: int, dia: str) -> List[dict]:
        return self.gym_repo.obtener_horarios_profesor_dia(profesor_id, dia)

    def crear_horario_profesor(self, profesor_id: int, dia: str, hora_inicio: str, hora_fin: str, disponible: bool = True) -> int:
        return self.gym_repo.crear_horario_profesor(profesor_id, dia, hora_inicio, hora_fin, disponible)

    def actualizar_horario_profesor(self, horario_id: int, dia: str, hora_inicio: str, hora_fin: str, disponible: bool = True):
        return self.gym_repo.actualizar_horario_profesor(horario_id, dia, hora_inicio, hora_fin, disponible)

    def eliminar_horario_profesor(self, horario_id: int):
        return self.gym_repo.eliminar_horario_profesor(horario_id)

    def obtener_horarios_disponibilidad_profesor(self, profesor_id: int) -> List[dict]:
        return self.gym_repo.obtener_horarios_disponibilidad_profesor(profesor_id)

    def obtener_todos_horarios_clases(self, activos_solo=False):
        return self.gym_repo.obtener_todos_horarios_clases(activos_solo)

    def actualizar_horario_clase(self, horario: ClaseHorario):
        return self.gym_repo.actualizar_horario_clase(horario)

    def eliminar_horario_clase(self, horario_id: int):
        return self.gym_repo.eliminar_horario_clase(horario_id)

    def asignar_profesor_a_clase(self, clase_horario_id: int, profesor_id: int, cursor=None):
        return self.gym_repo.asignar_profesor_a_clase(clase_horario_id, profesor_id, cursor)

    def desasignar_profesor_de_clase(self, clase_horario_id: int, profesor_id: int):
        return self.gym_repo.desasignar_profesor_de_clase(clase_horario_id, profesor_id)

    def inscribir_usuario_en_clase(self, clase_horario_id: int, usuario_id: int) -> bool:
        return self.user_repo.inscribir_usuario_en_clase(clase_horario_id, usuario_id)

    def verificar_cupo_disponible(self, clase_horario_id: int) -> bool:
        return self.gym_repo.verificar_cupo_disponible(clase_horario_id)

    def agregar_a_lista_espera(self, clase_horario_id: int, usuario_id: int):
        return self.gym_repo.agregar_a_lista_espera(clase_horario_id, usuario_id)

    def obtener_actividad_reciente(self, limit: int = 10) -> List[dict]:
        return self.gym_repo.obtener_actividad_reciente(limit)

    def obtener_profesores_asignados_a_clase(self, clase_horario_id: int) -> List[dict]:
        return self.gym_repo.obtener_profesores_asignados_a_clase(clase_horario_id)

    def obtener_clase_por_id(self, clase_id: int) -> Optional[dict]:
        return self.gym_repo.obtener_clase_por_id(clase_id)

    def obtener_estudiantes_clase(self, clase_horario_id: int) -> List[dict]:
        return self.gym_repo.obtener_estudiantes_clase(clase_horario_id)

    def obtener_pago(self, pago_id: int) -> Optional[Pago]:
        return self.payment_repo.obtener_pago(pago_id)

    def obtener_pagos_mes(self, mes: int, año: int) -> List[Pago]:
        return self.payment_repo.obtener_pagos_mes(mes, año)

    def eliminar_pago(self, pago_id: int):
        return self.payment_repo.eliminar_pago(pago_id)

    def modificar_pago(self, pago: Pago):
        return self.payment_repo.modificar_pago(pago)

    def verificar_pago_existe(self, usuario_id: int, mes: int, año: int) -> bool:
        return self.payment_repo.verificar_pago_existe(usuario_id, mes, año)

    def obtener_estadisticas_pagos(self, año: int = None) -> dict:
        return self.payment_repo.obtener_estadisticas_pagos(año)

    def obtener_nuevos_usuarios_por_mes(self) -> List[dict]:
        return self.user_repo.obtener_nuevos_usuarios_por_mes()

    def obtener_nuevos_usuarios_por_mes_ultimos_12(self) -> Dict[str, int]:
        return self.user_repo.obtener_nuevos_usuarios_por_mes_ultimos_12()

    def obtener_nuevos_usuarios_por_mes_rango(self, fecha_inicio: date, fecha_fin: date) -> Dict[str, int]:
        return self.user_repo.obtener_nuevos_usuarios_por_mes_rango(fecha_inicio, fecha_fin)

    def obtener_arpu_por_mes_ultimos_12(self) -> Dict[str, float]:
        return self.gym_repo.obtener_arpu_por_mes_ultimos_12()

    def registrar_asistencia(self, usuario_id: int, fecha: date = None) -> int:
        return self.attendance_repo.registrar_asistencia(usuario_id, fecha)

    def registrar_asistencias_batch(self, asistencias: List[Dict[str, Any]]) -> Dict[str, Any]:
        return self.attendance_repo.registrar_asistencias_batch(asistencias)

    def crear_checkin_token(self, usuario_id: int, token: str, expires_minutes: int = 5) -> int:
        return self.attendance_repo.crear_checkin_token(usuario_id, token, expires_minutes)

    def obtener_checkin_por_token(self, token: str) -> Optional[Dict]:
        return self.attendance_repo.obtener_checkin_por_token(token)

    def marcar_checkin_usado(self, token: str) -> None:
        return self.attendance_repo.marcar_checkin_usado(token)

    def validar_token_y_registrar_asistencia(self, token: str, socio_id: int) -> Tuple[bool, str]:
        return self.attendance_repo.validar_token_y_registrar_asistencia(token, socio_id)

    def obtener_asistencias_usuario(self, usuario_id: int, fecha_inicio: date = None, fecha_fin: date = None) -> List[dict]:
        return self.user_repo.obtener_asistencias_usuario(usuario_id, fecha_inicio, fecha_fin)

    def obtener_asistencias_fecha(self, fecha: date) -> List[dict]:
        return self.attendance_repo.obtener_asistencias_fecha(fecha)

    def eliminar_asistencia(self, asistencia_id_or_user_id: int, fecha: date = None):
        return self.attendance_repo.eliminar_asistencia(asistencia_id_or_user_id, fecha)

    def obtener_estadisticas_asistencias(self, fecha_inicio: date = None, fecha_fin: date = None) -> dict:
        return self.attendance_repo.obtener_estadisticas_asistencias(fecha_inicio, fecha_fin)

    def obtener_asistencias_por_dia(self, dias: int = 30):
        return self.attendance_repo.obtener_asistencias_por_dia(dias)

    def obtener_asistencias_por_rango_diario(self, fecha_inicio: str, fecha_fin: str):
        return self.attendance_repo.obtener_asistencias_por_rango_diario(fecha_inicio, fecha_fin)

    def obtener_asistencias_por_hora(self, dias: int = 30):
        return self.attendance_repo.obtener_asistencias_por_hora(dias)

    def obtener_asistencias_por_hora_rango(self, fecha_inicio: str, fecha_fin: str):
        return self.attendance_repo.obtener_asistencias_por_hora_rango(fecha_inicio, fecha_fin)

    def crear_grupo_ejercicios(self, nombre_grupo: str, ejercicio_ids: List[int]) -> int:
        return self.gym_repo.crear_grupo_ejercicios(nombre_grupo, ejercicio_ids)

    def obtener_grupos_ejercicios(self) -> List[EjercicioGrupo]:
        return self.gym_repo.obtener_grupos_ejercicios()

    def obtener_ejercicios_de_grupo(self, grupo_id: int) -> List[Ejercicio]:
        return self.gym_repo.obtener_ejercicios_de_grupo(grupo_id)

    def eliminar_grupo_ejercicios(self, grupo_id: int) -> bool:
        return self.gym_repo.eliminar_grupo_ejercicios(grupo_id)

    def obtener_precio_cuota(self, tipo_cuota: str) -> float:
        return self.payment_repo.obtener_precio_cuota(tipo_cuota)

    def actualizar_precio_cuota(self, tipo_cuota: str, nuevo_precio: float):
        return self.payment_repo.actualizar_precio_cuota(tipo_cuota, nuevo_precio)

    def obtener_tipos_cuota_activos(self) -> List[TipoCuota]:
        return self.payment_repo.obtener_tipos_cuota_activos()

    def obtener_tipo_cuota_por_nombre(self, nombre: str) -> Optional[TipoCuota]:
        return self.payment_repo.obtener_tipo_cuota_por_nombre(nombre)

    def obtener_tipo_cuota_por_id(self, tipo_id: int) -> Optional[TipoCuota]:
        return self.payment_repo.obtener_tipo_cuota_por_id(tipo_id)

    def actualizar_tipo_cuota(self, tipo_cuota: TipoCuota) -> bool:
        return self.payment_repo.actualizar_tipo_cuota(tipo_cuota)

    def contar_usuarios_por_tipo_cuota(self, tipo_cuota: str) -> int:
        return self.user_repo.contar_usuarios_por_tipo_cuota(tipo_cuota)

    def crear_rutina(self, rutina) -> int:
        return self.gym_repo.crear_rutina(rutina)

    def actualizar_rutina(self, rutina) -> bool:
        return self.gym_repo.actualizar_rutina(rutina)

    def enforce_single_active_rutina_usuario(self, usuario_id: int) -> int:
        return self.user_repo.enforce_single_active_rutina_usuario(usuario_id)

    def obtener_rutinas_por_usuario(self, usuario_id: int) -> List:
        return self.user_repo.obtener_rutinas_por_usuario(usuario_id)

    def obtener_rutina_completa(self, rutina_id: int) -> Optional[dict]:
        return self.gym_repo.obtener_rutina_completa(rutina_id)

    def obtener_rutina_completa_por_uuid_dict(self, uuid_rutina: str) -> Optional[Dict[str, Any]]:
        return self.gym_repo.obtener_rutina_completa_por_uuid_dict(uuid_rutina)

    def set_rutina_activa_por_uuid(self, usuario_id: int, uuid_rutina: str, activa: bool) -> Optional[Dict[str, Any]]:
        return self.gym_repo.set_rutina_activa_por_uuid(usuario_id, uuid_rutina, activa)

    def set_rutina_activa_por_id(self, usuario_id: int, rutina_id: int, activa: bool) -> Optional[Dict[str, Any]]:
        return self.gym_repo.set_rutina_activa_por_id(usuario_id, rutina_id, activa)

    def obtener_ejercicios(self, filtro: str = "", objetivo: str = "", 
                          grupo_muscular: str = "") -> List[Ejercicio]:
        return self.gym_repo.obtener_ejercicios(filtro, objetivo, grupo_muscular)

    def crear_ejercicio(self, ejercicio) -> int:
        return self.gym_repo.crear_ejercicio(ejercicio)

    def obtener_usuarios_en_clase(self, clase_horario_id: int) -> List[dict]:
        return self.user_repo.obtener_usuarios_en_clase(clase_horario_id)

    def quitar_usuario_de_clase(self, clase_horario_id: int, usuario_id: int):
        return self.user_repo.quitar_usuario_de_clase(clase_horario_id, usuario_id)

    def guardar_ejercicios_para_clase(self, clase_id: int, items: List[Any]):
        return self.gym_repo.guardar_ejercicios_para_clase(clase_id, items)

    def obtener_ejercicios_de_clase(self, clase_id: int) -> List[Ejercicio]:
        return self.gym_repo.obtener_ejercicios_de_clase(clase_id)

    def obtener_ejercicios_de_clase_detalle(self, clase_id: int) -> List[Dict]:
        return self.gym_repo.obtener_ejercicios_de_clase_detalle(clase_id)

    def obtener_todos_profesores(self) -> List[Dict]:
        return self.teacher_repo.obtener_todos_profesores()

    def obtener_profesores(self) -> List[Dict]:
        return self.teacher_repo.obtener_profesores()

    def obtener_profesores_basico(self) -> List[Dict]:
        return self.teacher_repo.obtener_profesores_basico()

    def obtener_profesores_basico_con_ids(self) -> List[Dict]:
        return self.teacher_repo.obtener_profesores_basico_con_ids()

    def _increment_timeout_metric(self, key: str) -> None:
        return self.gym_repo._increment_timeout_metric(key)

    def get_timeout_metrics(self) -> Dict[str, int]:
        return self.gym_repo.get_timeout_metrics()

    def ensure_schedule_conflicts_table(self) -> None:
        return self.gym_repo.ensure_schedule_conflicts_table()

    def listar_conflictos_activos(self) -> List[Dict]:
        return self.gym_repo.listar_conflictos_activos()

    def listar_historial_conflictos(self, limit: int = 100) -> List[Dict]:
        return self.gym_repo.listar_historial_conflictos(limit)

    def obtener_conflicto_por_id(self, conflicto_id: int) -> Optional[Dict]:
        return self.gym_repo.obtener_conflicto_por_id(conflicto_id)

    def crear_conflicto_horario(
        self,
        conflict_type: str,
        severity: str,
        professor_id: Optional[int],
        description: str,
        conflict_date,
        conflict_time,
        class_id: Optional[int] = None,
        room_id: Optional[int] = None,
    ) -> Optional[int]:
        return self.gym_repo.crear_conflicto_horario(conflict_type, severity, professor_id, description, conflict_date, conflict_time, class_id, room_id)

    def resolver_conflicto_horario(        self, conflicto_id: int, resolution_type: str, resolution_notes: str, resolved_by: str = 'Usuario'
    ) -> bool:
        return self.gym_repo.resolver_conflicto_horario(conflicto_id, resolution_type, resolution_notes, resolved_by)

    def ignorar_conflicto_horario(self, conflicto_id: int) -> bool:
        return self.gym_repo.ignorar_conflicto_horario(conflicto_id)

    def existe_conflicto_activo_profesor_fecha(
        self, professor_id: int, conflict_type: str, conflict_date
    ) -> bool:
        return self.teacher_repo.existe_conflicto_activo_profesor_fecha(professor_id, conflict_type, conflict_date)

    def obtener_solapamientos_horarios(self) -> List[Dict]:
        return self.gym_repo.obtener_solapamientos_horarios()

    def contar_conflictos_activos(self) -> int:
        return self.gym_repo.contar_conflictos_activos()

    def contar_conflictos_criticos_activos(self) -> int:
        return self.gym_repo.contar_conflictos_criticos_activos()

    def contar_conflictos_activos_hoy(self) -> int:
        return self.gym_repo.contar_conflictos_activos_hoy()

    def obtener_asistencias_por_dia_semana(self, dias: int = 30) -> Dict[str, int]:
        return self.attendance_repo.obtener_asistencias_por_dia_semana(dias)

    def obtener_actividad_reciente(self, limit: int = 10, current_user_id: int | None = None) -> List[dict]:
        return self.gym_repo.obtener_actividad_reciente(limit, current_user_id)

    def automatizar_estados_por_vencimiento_optimizada(self) -> dict:
        return self.payment_repo.automatizar_estados_por_vencimiento_optimizada()

    def obtener_alertas_vencimiento_proactivas(self, dias_anticipacion: int = 7) -> List[dict]:
        return self.payment_repo.obtener_alertas_vencimiento_proactivas(dias_anticipacion)

    def crear_backup_selectivo_usuarios(self, criterios: dict = None) -> dict:
        return self.user_repo.crear_backup_selectivo_usuarios(criterios)

    def generar_reporte_automatico_periodo(self, tipo_reporte: str, fecha_inicio: date, fecha_fin: date) -> dict:
        return self.reports_repo.generar_reporte_automatico_periodo(tipo_reporte, fecha_inicio, fecha_fin)

    def obtener_usuarios_por_fecha_registro(self, fecha_limite):
        return self.user_repo.obtener_usuarios_por_fecha_registro(fecha_limite)

    def obtener_asistencias_por_fecha_limite(self, fecha_limite):
        return self.attendance_repo.obtener_asistencias_por_fecha_limite(fecha_limite)

    def obtener_usuarios_sin_pagos_recientes(self, fecha_limite):
        return self.user_repo.obtener_usuarios_sin_pagos_recientes(fecha_limite)

    def obtener_usuarios_reporte_completo(self):
        return self.user_repo.obtener_usuarios_reporte_completo()

    def obtener_asistencias_usuario(self, usuario_id, limit=None):
        return self.user_repo.obtener_asistencias_usuario(usuario_id, limit)

    def registrar_audit_log(self, user_id: int, action: str, table_name: str, record_id: int = None, 
                           old_values: str = None, new_values: str = None, ip_address: str = None, 
                           user_agent: str = None, session_id: str = None):
        return self.audit_repo.registrar_audit_log(user_id, action, table_name, record_id, old_values, new_values, ip_address, user_agent, session_id)

    def obtener_audit_logs(self, limit: int = 100, offset: int = 0, user_id: int = None, 
                          table_name: str = None, action: str = None, fecha_inicio: date = None, fecha_fin: date = None) -> List[Dict]:
        return self.audit_repo.obtener_audit_logs(limit, offset, user_id, table_name, action, fecha_inicio, fecha_fin)

    def obtener_estadisticas_auditoria(self, dias: int = 30) -> Dict:
        return self.audit_repo.obtener_estadisticas_auditoria(dias)

    def registrar_diagnostico(self, diagnostic_type: str, component: str, status: str, 
                             details: str = None, metrics: str = None):
        return self.audit_repo.registrar_diagnostico(diagnostic_type, component, status, details, metrics)

    def obtener_diagnosticos(self, limit: int = 50, component: str = None, 
                            status: str = None, diagnostic_type: str = None) -> List[Dict]:
        return self.audit_repo.obtener_diagnosticos(limit, component, status, diagnostic_type)

    def resolver_diagnostico(self, diagnostico_id: int, resolved_by: int) -> bool:
        return self.audit_repo.resolver_diagnostico(diagnostico_id, resolved_by)

    def crear_tarea_mantenimiento(self, task_name: str, task_type: str, description: str = None, 
                                 scheduled_at: datetime = None, created_by: int = None, 
                                 auto_schedule: bool = False, frequency_days: int = None) -> int:
        return self.gym_repo.crear_tarea_mantenimiento(task_name, task_type, description, scheduled_at, created_by, auto_schedule, frequency_days)

    def obtener_tareas_mantenimiento(self, status: str = None, limit: int = 50) -> List[Dict]:
        return self.gym_repo.obtener_tareas_mantenimiento(status, limit)

    def ejecutar_tarea_mantenimiento(self, task_id: int, executed_by: int, result: str = None, 
                                    error_message: str = None) -> bool:
        return self.gym_repo.ejecutar_tarea_mantenimiento(task_id, executed_by, result, error_message)

    def cerrar_sesiones_huerfanas(self, threshold_hours: int = 24, cap_hours: int = 12, 
                                 executed_by: int = None) -> Dict[str, Any]:
        return self.gym_repo.cerrar_sesiones_huerfanas(threshold_hours, cap_hours, executed_by)

    def programar_cierre_sesiones_huerfanas(self, threshold_hours: int = 24, cap_hours: int = 12, 
                                            created_by: int | None = None, auto_schedule: bool = True, 
                                            frequency_days: int = 1) -> Dict[str, Any]:
        return self.gym_repo.programar_cierre_sesiones_huerfanas(threshold_hours, cap_hours, created_by, auto_schedule, frequency_days)

    def obtener_tareas_pendientes(self) -> List[Dict]:
        return self.gym_repo.obtener_tareas_pendientes()

    def obtener_alertas_vencimientos_configurables(self, dias_anticipacion: int = 5) -> List[Dict]:
        return self.payment_repo.obtener_alertas_vencimientos_configurables(dias_anticipacion)

    def obtener_rutina_completa(self, rutina_id: int) -> Optional[Rutina]:
        return self.gym_repo.obtener_rutina_completa(rutina_id)

    def crear_rutina(self, rutina: Rutina) -> int:
        return self.gym_repo.crear_rutina(rutina)

    def crear_especialidad(self, nombre: str, descripcion: str = None, categoria: str = None) -> int:
        return self.gym_repo.crear_especialidad(nombre, descripcion, categoria)

    def obtener_especialidades(self, solo_activas: bool = True) -> List[Dict]:
        return self.gym_repo.obtener_especialidades(solo_activas)

    def actualizar_especialidad(self, especialidad_id: int, nombre: str = None, 
                               descripcion: str = None, categoria: str = None, 
                               activa: bool = None) -> bool:
        return self.gym_repo.actualizar_especialidad(especialidad_id, nombre, descripcion, categoria, activa)

    def eliminar_especialidad(self, especialidad_id: int) -> bool:
        return self.gym_repo.eliminar_especialidad(especialidad_id)

    def asignar_especialidad_profesor(self, profesor_id: int, especialidad_id: int, 
                                     nivel_experiencia: str, años_experiencia: int = 0) -> int:
        return self.teacher_repo.asignar_especialidad_profesor(profesor_id, especialidad_id, nivel_experiencia, años_experiencia)

    def quitar_especialidad_profesor(self, profesor_id: int, especialidad_id: int) -> bool:
        return self.teacher_repo.quitar_especialidad_profesor(profesor_id, especialidad_id)

    def obtener_especialidades_profesor(self, profesor_id: int) -> List[Dict]:
        return self.teacher_repo.obtener_especialidades_profesor(profesor_id)

    def obtener_profesores_por_especialidad(self, especialidad_id: int) -> List[Dict]:
        return self.teacher_repo.obtener_profesores_por_especialidad(especialidad_id)

    def crear_certificacion_profesor(self, profesor_id: int, nombre_certificacion: str, 
                                    institucion_emisora: str = None, numero_certificado: str = None,
                                    fecha_obtencion: date = None, fecha_vencimiento: date = None,
                                    archivo_adjunto: str = None, notas: str = None) -> int:
        return self.teacher_repo.crear_certificacion_profesor(profesor_id, nombre_certificacion, institucion_emisora, numero_certificado, fecha_obtencion, fecha_vencimiento, archivo_adjunto, notas)

    def obtener_certificaciones_profesor(self, profesor_id: int, solo_vigentes: bool = False) -> List[Dict]:
        return self.teacher_repo.obtener_certificaciones_profesor(profesor_id, solo_vigentes)

    def actualizar_certificacion_profesor(self, certificacion_id: int, **kwargs) -> bool:
        return self.teacher_repo.actualizar_certificacion_profesor(certificacion_id, **kwargs)

    def eliminar_certificacion_profesor(self, certificacion_id: int) -> bool:
        return self.teacher_repo.eliminar_certificacion_profesor(certificacion_id)

    def obtener_certificaciones_vencidas(self, dias_anticipacion: int = 30) -> List[Dict]:
        return self.gym_repo.obtener_certificaciones_vencidas(dias_anticipacion)

    def crear_disponibilidad_profesor(self, profesor_id: int, fecha: date, 
                                     tipo_disponibilidad: str, hora_inicio: str = None,
                                     hora_fin: str = None, notas: str = None) -> int:
        return self.teacher_repo.crear_disponibilidad_profesor(profesor_id, fecha, tipo_disponibilidad, hora_inicio, hora_fin, notas)

    def obtener_disponibilidad_profesor(self, profesor_id: int, fecha_inicio: date = None, 
                                       fecha_fin: date = None) -> List[Dict]:
        return self.teacher_repo.obtener_disponibilidad_profesor(profesor_id, fecha_inicio, fecha_fin)

    def verificar_disponibilidad_profesor_fecha(self, profesor_id: int, fecha: date, 
                                              hora_inicio: str = None, hora_fin: str = None) -> Dict:
        return self.teacher_repo.verificar_disponibilidad_profesor_fecha(profesor_id, fecha, hora_inicio, hora_fin)

    def _verificar_conflictos_horario_profesor(self, profesor_id: int, fecha: date, 
                                             hora_inicio: str, hora_fin: str) -> List[Dict]:
        return self.teacher_repo._verificar_conflictos_horario_profesor(profesor_id, fecha, hora_inicio, hora_fin)

    def crear_suplencia(self, clase_horario_id: int, profesor_original_id: int, 
                       fecha_clase: date, motivo: str, profesor_suplente_id: int = None,
                       notas: str = None) -> int:
        return self.teacher_repo.crear_suplencia(clase_horario_id, profesor_original_id, fecha_clase, motivo, profesor_suplente_id, notas)

    def asignar_suplente(self, suplencia_id: int, profesor_suplente_id: int, notas: str = None) -> bool:
        return self.gym_repo.asignar_suplente(suplencia_id, profesor_suplente_id, notas)

    def confirmar_suplencia(self, suplencia_id: int) -> bool:
        return self.teacher_repo.confirmar_suplencia(suplencia_id)

    def cancelar_suplencia(self, suplencia_id: int, motivo: str = None) -> bool:
        return self.teacher_repo.cancelar_suplencia(suplencia_id, motivo)

    def obtener_suplencias_pendientes(self, profesor_id: int = None) -> List[Dict]:
        return self.teacher_repo.obtener_suplencias_pendientes(profesor_id)

    def obtener_suplencias_profesor(self, profesor_id: int, como_suplente: bool = False) -> List[Dict]:
        return self.teacher_repo.obtener_suplencias_profesor(profesor_id, como_suplente)

    def crear_notificacion_profesor(self, profesor_id: int, tipo_notificacion: str, 
                                   titulo: str, mensaje: str, fecha_evento: date = None,
                                   prioridad: str = 'Media', datos_adicionales: str = None) -> int:
        return self.teacher_repo.crear_notificacion_profesor(profesor_id, tipo_notificacion, titulo, mensaje, fecha_evento, prioridad, datos_adicionales)

    def obtener_notificaciones_profesor(self, profesor_id: int, solo_no_leidas: bool = False, 
                                       limite: int = 50) -> List[Dict]:
        return self.teacher_repo.obtener_notificaciones_profesor(profesor_id, solo_no_leidas, limite)

    def crear_suplencia_general(self, horario_profesor_id: int, profesor_original_id: int, 
                               fecha: date, hora_inicio: str, hora_fin: str, motivo: str, 
                               profesor_suplente_id: int = None, notas: str = None) -> int:
        return self.teacher_repo.crear_suplencia_general(horario_profesor_id, profesor_original_id, fecha, hora_inicio, hora_fin, motivo, profesor_suplente_id, notas)

    def asignar_suplente_general(self, suplencia_id: int, profesor_suplente_id: int, notas: str = None) -> bool:
        return self.gym_repo.asignar_suplente_general(suplencia_id, profesor_suplente_id, notas)

    def confirmar_suplencia_general(self, suplencia_id: int) -> bool:
        return self.teacher_repo.confirmar_suplencia_general(suplencia_id)

    def cancelar_suplencia_general(self, suplencia_id: int, motivo: str = None) -> bool:
        return self.teacher_repo.cancelar_suplencia_general(suplencia_id, motivo)

    def marcar_notificacion_leida(self, notificacion_id: int) -> bool:
        return self.whatsapp_repo.marcar_notificacion_leida(notificacion_id)

    def obtener_count_notificaciones_no_leidas(self, profesor_id: int) -> int:
        return self.whatsapp_repo.obtener_count_notificaciones_no_leidas(profesor_id)

    def registrar_horas_trabajadas(self, profesor_id: int, fecha: date, hora_inicio: datetime, 
                                  hora_fin: datetime, tipo_actividad: str, clase_id: int = None, 
                                  notas: str = None) -> int:
        return self.gym_repo.registrar_horas_trabajadas(profesor_id, fecha, hora_inicio, hora_fin, tipo_actividad, clase_id, notas)

    def obtener_horas_trabajadas_profesor(self, profesor_id: int, fecha_inicio: str = None, 
                                         fecha_fin: str = None) -> List[Dict]:
        return self.teacher_repo.obtener_horas_trabajadas_profesor(profesor_id, fecha_inicio, fecha_fin)

    def obtener_resumen_horas_profesor(self, profesor_id: int, mes: int, año: int) -> Dict[str, Any]:
        return self.teacher_repo.obtener_resumen_horas_profesor(profesor_id, mes, año)

    def obtener_horas_trabajadas_profesor_mes(self, profesor_id: int, mes: int, año: int) -> Dict[str, Any]:
        return self.teacher_repo.obtener_horas_trabajadas_profesor_mes(profesor_id, mes, año)

    def aplicar_optimizaciones_database(self) -> Dict[str, Any]:
        return self.gym_repo.aplicar_optimizaciones_database()

    def obtener_politica_cancelacion(self, clase_id: int = None) -> Dict:
        return self.gym_repo.obtener_politica_cancelacion(clase_id)

    def verificar_puede_cancelar(self, usuario_id: int, clase_horario_id: int) -> Dict:
        return self.gym_repo.verificar_puede_cancelar(usuario_id, clase_horario_id)

    def eliminar_profesor(self, profesor_id: int) -> bool:
        return self.teacher_repo.eliminar_profesor(profesor_id)

    def iniciar_sesion_trabajo_profesor(self, profesor_id: int, tipo_actividad: str = 'Trabajo') -> Dict[str, Any]:
        return self.teacher_repo.iniciar_sesion_trabajo_profesor(profesor_id, tipo_actividad)

    def finalizar_sesion_trabajo_profesor(self, profesor_id: int) -> Dict[str, Any]:
        return self.teacher_repo.finalizar_sesion_trabajo_profesor(profesor_id)

    def obtener_sesion_activa_profesor(self, profesor_id: int) -> Dict[str, Any]:
        return self.teacher_repo.obtener_sesion_activa_profesor(profesor_id)

    def obtener_duracion_sesion_actual_profesor(self, profesor_id: int) -> Dict[str, Any]:
        return self.teacher_repo.obtener_duracion_sesion_actual_profesor(profesor_id)

    def normalizar_sesiones_profesor(
        self,
        profesor_id: int,
        fecha_inicio: Optional[date] = None,
        fecha_fin: Optional[date] = None,
        preferencia: str = 'minutos',
        tolerancia_minutos: int = 5,
    ) -> Dict[str, Any]:
        return self.teacher_repo.normalizar_sesiones_profesor(profesor_id, fecha_inicio, fecha_fin, preferencia, tolerancia_minutos)

    def verificar_sesiones_abiertas(self) -> List[Dict]:
        return self.gym_repo.verificar_sesiones_abiertas()

    def contar_sesiones_activas(self) -> int:
        return self.gym_repo.contar_sesiones_activas()

    def verificar_reinicio_mensual_horas(self, profesor_id: int) -> Dict[str, Any]:
        return self.gym_repo.verificar_reinicio_mensual_horas(profesor_id)

    def obtener_horas_extras_profesor(self, profesor_id: int, fecha_inicio: date = None, fecha_fin: date = None) -> Dict[str, Any]:
        return self.teacher_repo.obtener_horas_extras_profesor(profesor_id, fecha_inicio, fecha_fin)

    def obtener_horas_mensuales_profesor(self, profesor_id: int, año: int = None, mes: int = None) -> Dict[str, Any]:
        return self.teacher_repo.obtener_horas_mensuales_profesor(profesor_id, año, mes)

    def obtener_historial_meses_profesor(self, profesor_id: int, limite_meses: int = 12) -> Dict[str, Any]:
        return self.teacher_repo.obtener_historial_meses_profesor(profesor_id, limite_meses)

    def obtener_horas_mes_actual_profesor(self, profesor_id: int) -> Dict[str, Any]:
        return self.teacher_repo.obtener_horas_mes_actual_profesor(profesor_id)

    def obtener_minutos_mes_actual_profesor(self, profesor_id: int) -> Dict[str, Any]:
        return self.teacher_repo.obtener_minutos_mes_actual_profesor(profesor_id)

    def obtener_horas_proyectadas_profesor(self, profesor_id: int, año: int | None = None, mes: int | None = None) -> Dict[str, Any]:
        return self.teacher_repo.obtener_horas_proyectadas_profesor(profesor_id, año, mes)

    def verificar_certificaciones_vencidas(self) -> List[Dict]:
        return self.gym_repo.verificar_certificaciones_vencidas()

    def obtener_certificaciones_por_vencer(self, dias: int = 30) -> List[Dict]:
        return self.gym_repo.obtener_certificaciones_por_vencer(dias)

    def verificar_indices_database(self) -> Dict[str, Any]:
        return self.gym_repo.verificar_indices_database()

    def ensure_indexes(self) -> None:
        return self.gym_repo.ensure_indexes()

    def generar_reporte_optimizacion(self) -> Dict[str, Any]:
        return self.reports_repo.generar_reporte_optimizacion()

    def agregar_a_lista_espera_completo(self, clase_horario_id: int, usuario_id: int) -> int:
        return self.gym_repo.agregar_a_lista_espera_completo(clase_horario_id, usuario_id)

    def quitar_de_lista_espera_completo(self, clase_horario_id: int, usuario_id: int) -> bool:
        return self.gym_repo.quitar_de_lista_espera_completo(clase_horario_id, usuario_id)

    def obtener_lista_espera(self, clase_horario_id: int) -> List[Dict]:
        return self.gym_repo.obtener_lista_espera(clase_horario_id)

    def quitar_de_lista_espera(self, clase_horario_id: int, usuario_id: int) -> bool:
        return self.gym_repo.quitar_de_lista_espera(clase_horario_id, usuario_id)

    def obtener_lista_espera_completa(self, clase_horario_id: int) -> List[Dict]:
        return self.gym_repo.obtener_lista_espera_completa(clase_horario_id)

    def obtener_siguiente_en_lista_espera_completo(self, clase_horario_id: int) -> Dict:
        return self.gym_repo.obtener_siguiente_en_lista_espera_completo(clase_horario_id)

    def procesar_liberacion_cupo_completo(self, clase_horario_id: int) -> bool:
        return self.gym_repo.procesar_liberacion_cupo_completo(clase_horario_id)

    def migrar_lista_espera_legacy(self, drop_legacy: bool = True) -> Dict[str, int]:
        return self.gym_repo.migrar_lista_espera_legacy(drop_legacy)

    def registrar_asistencia_clase_completa(self, clase_horario_id: int, usuario_id: int, fecha_clase: str, 
                                           estado_asistencia: str = 'presente', hora_llegada: str = None, 
                                           observaciones: str = None, registrado_por: int = None) -> int:
        return self.attendance_repo.registrar_asistencia_clase_completa(clase_horario_id, usuario_id, fecha_clase, estado_asistencia, hora_llegada, observaciones, registrado_por)

    def obtener_historial_asistencia_usuario_completo(self, usuario_id: int, fecha_desde: str = None, 
                                                     fecha_hasta: str = None) -> List[Dict]:
        return self.user_repo.obtener_historial_asistencia_usuario_completo(usuario_id, fecha_desde, fecha_hasta)

    def obtener_asistencia_clase_completa(self, clase_horario_id: int, fecha_clase: str) -> List[Dict]:
        return self.attendance_repo.obtener_asistencia_clase_completa(clase_horario_id, fecha_clase)

    def obtener_estadisticas_asistencia_usuario_completas(self, usuario_id: int, meses: int = 3) -> Dict:
        return self.user_repo.obtener_estadisticas_asistencia_usuario_completas(usuario_id, meses)

    def crear_notificacion_cupo_completa(self, usuario_id: int, clase_horario_id: int, 
                                        tipo_notificacion: str, mensaje: str) -> int:
        return self.whatsapp_repo.crear_notificacion_cupo_completa(usuario_id, clase_horario_id, tipo_notificacion, mensaje)

    def obtener_notificaciones_usuario_completas(self, usuario_id: int, solo_no_leidas: bool = False) -> List[Dict]:
        return self.user_repo.obtener_notificaciones_usuario_completas(usuario_id, solo_no_leidas)

    def marcar_notificacion_leida_completa(self, notificacion_id: int) -> bool:
        return self.whatsapp_repo.marcar_notificacion_leida_completa(notificacion_id)

    def obtener_count_notificaciones_no_leidas_completo(self, usuario_id: int) -> int:
        return self.whatsapp_repo.obtener_count_notificaciones_no_leidas_completo(usuario_id)

    def obtener_politica_cancelacion_completa(self, clase_id: int = None) -> Dict:
        return self.gym_repo.obtener_politica_cancelacion_completa(clase_id)

    def verificar_puede_cancelar_completo(self, usuario_id: int, clase_horario_id: int) -> Dict:
        return self.gym_repo.verificar_puede_cancelar_completo(usuario_id, clase_horario_id)

    def verificar_certificaciones_vencidas(self) -> List[Dict]:
        return self.gym_repo.verificar_certificaciones_vencidas()

    def obtener_certificaciones_por_vencer(self, dias: int = 30) -> List[Dict]:
        return self.gym_repo.obtener_certificaciones_por_vencer(dias)

    def obtener_horas_trabajadas_profesor(self, profesor_id: int, fecha_inicio: str = None, fecha_fin: str = None) -> List[Dict]:
        return self.teacher_repo.obtener_horas_trabajadas_profesor(profesor_id, fecha_inicio, fecha_fin)

    def verificar_sesiones_abiertas(self) -> List[Dict]:
        return self.gym_repo.verificar_sesiones_abiertas()

    def optimizar_consultas_criticas(self) -> Dict[str, Any]:
        return self.gym_repo.optimizar_consultas_criticas()

    def _migrar_sistema_auditoria(self, cursor):
        return self.audit_repo._migrar_sistema_auditoria(cursor)

    def _migrar_campo_objetivo_ejercicios(self, cursor):
        return self.gym_repo._migrar_campo_objetivo_ejercicios(cursor)

    def _migrar_campo_categoria_rutinas(self, cursor):
        return self.gym_repo._migrar_campo_categoria_rutinas(cursor)

    def _migrar_campo_metodo_pago_id(self, cursor):
        return self.payment_repo._migrar_campo_metodo_pago_id(cursor)

    def _migrar_columnas_activa_activo(self, cursor):
        return self.gym_repo._migrar_columnas_activa_activo(cursor)

    def _migrar_campos_cuotas_vencidas(self, cursor):
        return self.payment_repo._migrar_campos_cuotas_vencidas(cursor)

    def _verificar_columna_minutos_totales(self, cursor):
        return self.gym_repo._verificar_columna_minutos_totales(cursor)

    def _migrar_columna_tipo_profesores(self, cursor):
        return self.teacher_repo._migrar_columna_tipo_profesores(cursor)

    def _migrar_series_a_varchar(self, cursor):
        return self.gym_repo._migrar_series_a_varchar(cursor)

    def _migrar_tablas_temas_personalizados(self, cursor):
        return self.gym_repo._migrar_tablas_temas_personalizados(cursor)

    def _migrar_tabla_clase_asistencia_historial(self, cursor):
        return self.attendance_repo._migrar_tabla_clase_asistencia_historial(cursor)

    def _migrar_sistema_comprobantes(self, cursor):
        return self.gym_repo._migrar_sistema_comprobantes(cursor)

    def aplicar_optimizacion_completa(self) -> Dict[str, Any]:
        return self.gym_repo.aplicar_optimizacion_completa()

    def obtener_conteo_activos_inactivos(self) -> Dict[str, int]:
        return self.gym_repo.obtener_conteo_activos_inactivos()

    def crear_tipo_cuota(self, tipo_cuota: TipoCuota) -> int:
        return self.payment_repo.crear_tipo_cuota(tipo_cuota)

    def eliminar_tipo_cuota(self, tipo_id: int) -> bool:
        return self.payment_repo.eliminar_tipo_cuota(tipo_id)

    def obtener_tipo_cuota_por_id(self, tipo_id):
        return self.payment_repo.obtener_tipo_cuota_por_id(tipo_id)

    def obtener_tipos_cuota(self, solo_activos: bool = False) -> List[TipoCuota]:
        return self.payment_repo.obtener_tipos_cuota(solo_activos)

    def obtener_conteo_tipos_cuota(self) -> Dict[str, int]:
        return self.payment_repo.obtener_conteo_tipos_cuota()

    def ensure_indexes_secondary(self) -> None:
        return self.gym_repo.ensure_indexes_secondary()

    def obtener_estadisticas_tipos_cuota(self) -> List[Dict[str, Any]]:
        return self.payment_repo.obtener_estadisticas_tipos_cuota()

    def obtener_proximo_numero_comprobante(self, tipo_comprobante: str) -> str:
        return self.gym_repo.obtener_proximo_numero_comprobante(tipo_comprobante)

    def crear_comprobante(self, tipo_comprobante: str, pago_id: int, usuario_id: int, monto_total, plantilla_id=None, datos_comprobante=None, emitido_por=None) -> int:
        return self.gym_repo.crear_comprobante(tipo_comprobante, pago_id, usuario_id, monto_total, plantilla_id, datos_comprobante, emitido_por)

    def obtener_comprobante(self, comprobante_id: int):
        return self.gym_repo.obtener_comprobante(comprobante_id)

    def exportar_pagos_csv(self, fecha_inicio=None, fecha_fin=None) -> str:
        return self.payment_repo.exportar_pagos_csv(fecha_inicio, fecha_fin)

    def verificar_integridad_base_datos(self) -> dict:
        return self.gym_repo.verificar_integridad_base_datos()

    def listar_respaldos_disponibles(self) -> list:
        return self.gym_repo.listar_respaldos_disponibles()

    def optimizar_base_datos(self) -> bool:
        return self.gym_repo.optimizar_base_datos()

    def obtener_configuracion_respaldo_automatico(self) -> dict:
        return self.gym_repo.obtener_configuracion_respaldo_automatico()

    def validar_respaldo(self, ruta_respaldo: str) -> bool:
        return self.gym_repo.validar_respaldo(ruta_respaldo)

    def calcular_tamano_base_datos(self) -> float:
        return self.gym_repo.calcular_tamano_base_datos()

    def obtener_resumen_pagos_por_metodo(self, fecha_inicio=None, fecha_fin=None) -> dict:
        return self.payment_repo.obtener_resumen_pagos_por_metodo(fecha_inicio, fecha_fin)

    def obtener_distribucion_usuarios_por_edad(self) -> dict:
        return self.user_repo.obtener_distribucion_usuarios_por_edad()

    def analizar_metodos_pago(self) -> dict:
        return self.payment_repo.analizar_metodos_pago()

    def calcular_pago_promedio(self) -> float:
        return self.payment_repo.calcular_pago_promedio()

    def calcular_ingresos_totales(self, fecha_inicio=None, fecha_fin=None) -> float:
        return self.attendance_repo.calcular_ingresos_totales(fecha_inicio, fecha_fin)

    def obtener_kpis_dashboard(self) -> dict:
        return self.reports_repo.obtener_kpis_dashboard()

    def obtener_pagos_por_fecha(self, fecha_inicio=None, fecha_fin=None) -> list:
        return self.payment_repo.obtener_pagos_por_fecha(fecha_inicio, fecha_fin)

    def contar_usuarios_totales(self) -> int:
        return self.user_repo.contar_usuarios_totales()

    def contar_clases_totales(self) -> int:
        return self.gym_repo.contar_clases_totales()

    def obtener_tendencia_ingresos(self, fecha_inicio=None, fecha_fin=None, periodo='6_meses') -> list:
        return self.attendance_repo.obtener_tendencia_ingresos(fecha_inicio, fecha_fin, periodo)

    def obtener_todas_configuraciones(self) -> dict:
        return self.gym_repo.obtener_todas_configuraciones()

    def eliminar_configuracion(self, clave: str) -> bool:
        return self.gym_repo.eliminar_configuracion(clave)

    def obtener_estadisticas_base_datos(self) -> dict:
        return self.reports_repo.obtener_estadisticas_base_datos()

    def crear_respaldo_base_datos(self, ruta_destino: str = None) -> str:
        return self.gym_repo.crear_respaldo_base_datos(ruta_destino)

    def contar_usuarios_activos(self) -> int:
        return self.user_repo.contar_usuarios_activos()

    def contar_clases_activas(self) -> int:
        return self.gym_repo.contar_clases_activas()

    def exportar_usuarios_csv(self, filtros=None) -> str:
        return self.user_repo.exportar_usuarios_csv(filtros)

    def obtener_alertas_sistema(self) -> list:
        return self.gym_repo.obtener_alertas_sistema()

    def contar_usuarios_nuevos_periodo(self, fecha_inicio, fecha_fin) -> int:
        return self.user_repo.contar_usuarios_nuevos_periodo(fecha_inicio, fecha_fin)

    def obtener_estadisticas_ocupacion_clases(self) -> dict:
        return self.gym_repo.obtener_estadisticas_ocupacion_clases()

    def exportar_clases_csv(self, fecha_inicio=None, fecha_fin=None) -> str:
        return self.gym_repo.exportar_clases_csv(fecha_inicio, fecha_fin)

    def obtener_pagos_por_rango_fechas(self, fecha_inicio, fecha_fin) -> list:
        return self.payment_repo.obtener_pagos_por_rango_fechas(fecha_inicio, fecha_fin)

    def obtener_distribucion_usuarios_por_genero(self) -> dict:
        return self.user_repo.obtener_distribucion_usuarios_por_genero()

    def obtener_clases_mas_populares(self, limit=10) -> list:
        return self.gym_repo.obtener_clases_mas_populares(limit)

    def generar_reporte_completo(self, fecha_inicio, fecha_fin) -> dict:
        return self.reports_repo.generar_reporte_completo(fecha_inicio, fecha_fin)

    def obtener_estadisticas_profesor(self, profesor_id: int) -> dict:
        return self.teacher_repo.obtener_estadisticas_profesor(profesor_id)

    def obtener_horas_fuera_horario_profesor(self, profesor_id: int, mes: int = None, año: int = None) -> dict:
        return self.gym_repo.obtener_horas_fuera_horario_profesor(profesor_id, mes, año)

    def obtener_evaluaciones_profesor(self, profesor_id: int, limit: int = 10) -> dict:
        return self.teacher_repo.obtener_evaluaciones_profesor(profesor_id, limit)

    def obtener_estadisticas_automatizacion(self) -> dict:
        return self.reports_repo.obtener_estadisticas_automatizacion()

    def simular_automatizacion_estados(self, config: dict = None) -> dict:
        return self.gym_repo.simular_automatizacion_estados(config)

    def crear_backup_selectivo_usuarios_mejorado(self, file_path: str, criterios: dict) -> dict:
        return self.user_repo.crear_backup_selectivo_usuarios_mejorado(file_path, criterios)

    def obtener_acciones_masivas_pendientes(self, usuario_id=None, estado=None):
        return self.gym_repo.obtener_acciones_masivas_pendientes(usuario_id, estado)

    def cancelar_acciones_masivas(self, operation_ids):
        return self.gym_repo.cancelar_acciones_masivas(operation_ids)

    def optimizar_base_datos_horas_profesores(self):
        return self.teacher_repo.optimizar_base_datos_horas_profesores()

    def _crear_tablas_whatsapp(self):
        return self.whatsapp_repo._crear_tablas_whatsapp()

    def registrar_mensaje_whatsapp(self, user_id: int, message_type: str, template_name: str, 
                                 phone_number: str, message_content: str = None, status: str = 'sent', 
                                 message_id: str = None) -> bool:
        return self.whatsapp_repo.registrar_mensaje_whatsapp(user_id, message_type, template_name, phone_number, message_content, status, message_id)

    def obtener_plantilla_whatsapp(self, template_name: str) -> Optional[Dict]:
        return self.whatsapp_repo.obtener_plantilla_whatsapp(template_name)

    def obtener_plantillas_whatsapp(self, activas_solo: bool = True) -> List[Dict]:
        return self.whatsapp_repo.obtener_plantillas_whatsapp(activas_solo)

    def obtener_configuracion_whatsapp(self) -> Optional[Dict]:
        return self.gym_repo.obtener_configuracion_whatsapp()

    def obtener_configuracion_whatsapp_completa(self) -> Dict[str, Any]:
        return self.gym_repo.obtener_configuracion_whatsapp_completa()

    def actualizar_configuracion_whatsapp(self, phone_id: str = None, waba_id: str = None, 
                                        access_token: str = None) -> bool:
        return self.gym_repo.actualizar_configuracion_whatsapp(phone_id, waba_id, access_token)

    def verificar_mensaje_enviado_reciente(self, user_id: int, message_type: str, 
                                         horas_limite: int = 24) -> bool:
        return self.whatsapp_repo.verificar_mensaje_enviado_reciente(user_id, message_type, horas_limite)

    def obtener_historial_mensajes_whatsapp(self, user_id: int = None, 
                                           message_type: str = None, 
                                           limit: int = 100) -> List[Dict]:
        return self.whatsapp_repo.obtener_historial_mensajes_whatsapp(user_id, message_type, limit)

    def obtener_pago_actual(self, usuario_id: int, mes: int = None, anio: int = None) -> Optional[Dict]:
        return self.payment_repo.obtener_pago_actual(usuario_id, mes, anio)

    def obtener_usuarios_morosos_por_mes(self, mes: int = None, anio: int = None) -> List[Dict]:
        return self.user_repo.obtener_usuarios_morosos_por_mes(mes, anio)

    def _enviar_mensaje_bienvenida_automatico(self, usuario_id: int, nombre: str, telefono: str):
        return self.whatsapp_repo._enviar_mensaje_bienvenida_automatico(usuario_id, nombre, telefono)

    def obtener_estadisticas_whatsapp(self) -> Dict:
        return self.whatsapp_repo.obtener_estadisticas_whatsapp()

    def limpiar_mensajes_antiguos_whatsapp(self, dias_antiguedad: int = 90) -> int:
        return self.whatsapp_repo.limpiar_mensajes_antiguos_whatsapp(dias_antiguedad)

    def _obtener_user_id_por_telefono_whatsapp(self, telefono: str) -> Optional[int]:
        return self.user_repo._obtener_user_id_por_telefono_whatsapp(telefono)

    def contar_mensajes_whatsapp_periodo(self, user_id: int = None, telefono: str = None, 
                                       start_time: datetime = None, end_time: datetime = None,
                                       fecha_desde: datetime = None, fecha_hasta: datetime = None,
                                       direccion: str = None, tipo_mensaje: str = None, estado: str = None) -> int:
        return self.whatsapp_repo.contar_mensajes_whatsapp_periodo(user_id, telefono, start_time, end_time, fecha_desde, fecha_hasta, direccion, tipo_mensaje, estado)

    def obtener_ultimo_mensaje_whatsapp(self, user_id: int = None, telefono: str = None, 
                                      message_type: str = None, direccion: str = None) -> Optional[Dict]:
        return self.whatsapp_repo.obtener_ultimo_mensaje_whatsapp(user_id, telefono, message_type, direccion)

    def obtener_mensaje_whatsapp_por_pk(self, user_id: int, pk_id: int) -> Optional[Dict]:
        return self.whatsapp_repo.obtener_mensaje_whatsapp_por_pk(user_id, pk_id)

    def obtener_mensaje_whatsapp_por_message_id(self, user_id: int, message_id: str) -> Optional[Dict]:
        return self.whatsapp_repo.obtener_mensaje_whatsapp_por_message_id(user_id, message_id)

    def obtener_telefonos_con_mensajes_fallidos(self, fecha_limite: datetime) -> List[str]:
        return self.whatsapp_repo.obtener_telefonos_con_mensajes_fallidos(fecha_limite)

    def limpiar_mensajes_fallidos_usuario(self, telefono: str, fecha_limite: datetime) -> bool:
        return self.user_repo.limpiar_mensajes_fallidos_usuario(telefono, fecha_limite)

    def actualizar_estado_mensaje_whatsapp(self, message_id: str, nuevo_estado: str) -> bool:
        return self.whatsapp_repo.actualizar_estado_mensaje_whatsapp(message_id, nuevo_estado)

    def eliminar_mensaje_whatsapp_por_pk(self, user_id: int, pk_id: int) -> bool:
        return self.whatsapp_repo.eliminar_mensaje_whatsapp_por_pk(user_id, pk_id)

    def eliminar_mensaje_whatsapp_por_message_id(self, user_id: int, message_id: str) -> bool:
        return self.whatsapp_repo.eliminar_mensaje_whatsapp_por_message_id(user_id, message_id)

    def procesar_vencimientos_automaticos(self) -> dict:
        return self.payment_repo.procesar_vencimientos_automaticos()

    def limpiar_datos_innecesarios(
        self,
        whatsapp_days: int = 120,
        audit_logs_days: int = 180,
        auditoria_days: int = 180,
        notificaciones_days: int = 90,
        sysdiag_days: int = 180,
    ) -> Dict[str, int]:
        return self.gym_repo.limpiar_datos_innecesarios(
            whatsapp_days,
            audit_logs_days,
            auditoria_days,
            notificaciones_days,
            sysdiag_days
        )

    def eliminar_objetos_sync_antiguos(self) -> Dict[str, Any]:
        return self.gym_repo.eliminar_objetos_sync_antiguos()

