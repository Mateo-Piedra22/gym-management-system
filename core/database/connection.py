import psycopg2
import psycopg2.pool
import psycopg2.extras
import logging
import time
import threading
import concurrent.futures
from contextlib import contextmanager
from queue import Queue, Empty
from datetime import datetime, date, timedelta
from typing import List, Optional, Dict, Set, Tuple, Any
import os
import sys
from pathlib import Path
import json
import functools
import random
import calendar
import uuid

# Importar configuración de logs
try:
    from core.logger_config import setup_logging
except ImportError:
    # Fallback si no se puede importar logger_config
    logging.basicConfig(level=logging.INFO)
    logging.getLogger(__name__).warning("No se pudo importar logger_config, usando configuración básica", exc_info=True)

logger = logging.getLogger(__name__)

# Importar PyQt6 para workers (con fallback seguro en entornos sin GUI)
try:
    from PyQt6.QtCore import QThread, pyqtSignal, QObject  # type: ignore
    _HAS_QT = True
except ImportError:
    logger.debug("PyQt6 no disponible. Usando stubs para compatibilidad.")
    _HAS_QT = False
    # Stubs mínimos para permitir importación en entornos web (Railway) sin PyQt6
    class QObject:  # type: ignore
        def __init__(self, *args, **kwargs):
            pass

    class QThread:  # type: ignore
        def __init__(self, *args, **kwargs):
            pass
        def start(self):
            pass
        def run(self):
            pass
        def isRunning(self):
            return False
        def quit(self):
            pass
        def wait(self, msecs: int | None = None):
            pass
        def setPriority(self, *args, **kwargs):
            pass

    def pyqtSignal(*args, **kwargs):  # type: ignore
        class _Signal:
            def __init__(self):
                self._callbacks = []
            def connect(self, cb):
                try:
                    self._callbacks.append(cb)
                except Exception:
                    logger.exception("Error al conectar señal simulada")
            def emit(self, *a, **kw):
                # Ejecutar callbacks de forma segura; en web normalmente no se usarán
                for cb in list(self._callbacks):
                    try:
                        cb(*a, **kw)
                    except Exception:
                        # No bloquear por errores en callbacks
                        logger.exception("Error en callback de señal simulada")
        return _Signal()
except Exception:
    logger.exception("Error inesperado al importar PyQt6 o crear stubs")
    _HAS_QT = False
    # Definir stubs mínimos en caso de error catastrófico
    class QObject: pass # type: ignore
    class QThread: pass # type: ignore
    def pyqtSignal(*args, **kwargs): return None # type: ignore

# Guards globales para inicialización única y creación de índices
_INIT_ONCE_LOCK = threading.RLock()
_INIT_ONCE_DONE = False
_INDEX_ONCE_LOCK = threading.RLock()
_INDEX_ONCE_DONE = False

# Optimized indexes for frequently queried tables
OPTIMIZED_INDEXES = {
    'usuarios': [
        'CREATE INDEX IF NOT EXISTS idx_usuarios_activo_rol ON usuarios(activo, rol) WHERE activo = true',
        'CREATE INDEX IF NOT EXISTS idx_usuarios_nombre_lower ON usuarios(LOWER(nombre))',
        'CREATE INDEX IF NOT EXISTS idx_usuarios_dni ON usuarios(dni) WHERE dni IS NOT NULL',
        'CREATE INDEX IF NOT EXISTS idx_usuarios_telefono ON usuarios(telefono) WHERE telefono IS NOT NULL',
        'CREATE INDEX IF NOT EXISTS idx_usuarios_fecha_registro ON usuarios(fecha_registro)',
        'CREATE INDEX IF NOT EXISTS idx_usuarios_tipo_cuota ON usuarios(tipo_cuota)',
        'CREATE INDEX IF NOT EXISTS idx_usuarios_vencimiento ON usuarios(fecha_proximo_vencimiento) WHERE fecha_proximo_vencimiento IS NOT NULL'
    ],
    'pagos': [
        'CREATE INDEX IF NOT EXISTS idx_pagos_usuario_fecha ON pagos(usuario_id, fecha_pago DESC)',
        'CREATE INDEX IF NOT EXISTS idx_pagos_fecha_mes ON pagos(fecha_pago, mes, año)',
        'CREATE INDEX IF NOT EXISTS idx_pagos_mes_año ON pagos(mes, año)',
        'CREATE INDEX IF NOT EXISTS idx_pagos_usuario_mes_año ON pagos(usuario_id, mes, año)'
    ],
    'asistencias': [
        'CREATE INDEX IF NOT EXISTS idx_asistencias_usuario_fecha ON asistencias(usuario_id, fecha DESC)',
        'CREATE INDEX IF NOT EXISTS idx_asistencias_fecha ON asistencias(fecha DESC)',
        'CREATE INDEX IF NOT EXISTS idx_asistencias_fecha_hora ON asistencias(fecha, hora_registro)',
        'CREATE INDEX IF NOT EXISTS idx_asistencias_usuario_actual ON asistencias(usuario_id, fecha) WHERE fecha = CURRENT_DATE'
    ],
    'clase_horarios': [
        'CREATE INDEX IF NOT EXISTS idx_clase_horarios_dia ON clase_horarios(dia_semana)',
        'CREATE INDEX IF NOT EXISTS idx_clase_horarios_clase ON clase_horarios(clase_id)',
        'CREATE INDEX IF NOT EXISTS idx_clase_horarios_profesor ON clase_horarios(profesor_id)',
        'CREATE INDEX IF NOT EXISTS idx_clase_horarios_activo ON clase_horarios(activo) WHERE activo = true'
    ],
    'clase_usuarios': [
        'CREATE INDEX IF NOT EXISTS idx_clase_usuarios_horario ON clase_usuarios(clase_horario_id)',
        'CREATE INDEX IF NOT EXISTS idx_clase_usuarios_usuario ON clase_usuarios(usuario_id)',
        'CREATE INDEX IF NOT EXISTS idx_clase_usuarios_combined ON clase_usuarios(clase_horario_id, usuario_id)'
    ],
    'rutinas': [
        'CREATE INDEX IF NOT EXISTS idx_rutinas_usuario ON rutinas(usuario_id)',
        'CREATE INDEX IF NOT EXISTS idx_rutinas_activa ON rutinas(activa) WHERE activa = true',
        'CREATE INDEX IF NOT EXISTS idx_rutinas_categoria ON rutinas(categoria)'
    ],
    'rutina_ejercicios': [
        'CREATE INDEX IF NOT EXISTS idx_rutina_ejercicios_rutina ON rutina_ejercicios(rutina_id)',
        'CREATE INDEX IF NOT EXISTS idx_rutina_ejercicios_ejercicio ON rutina_ejercicios(ejercicio_id)',
        'CREATE INDEX IF NOT EXISTS idx_rutina_ejercicios_dia ON rutina_ejercicios(dia_semana)'
    ]
}


def database_retry(func=None, *, max_retries=3, base_delay=1.0, max_delay=10.0):
    """
    Decorador para manejar reconexiones automáticas a la base de datos.
    Incluye fallback a conexión directa cuando el pool falla completamente.
    
    Args:
        max_retries: Número máximo de reintentos (default: 3)
        base_delay: Delay base en segundos (default: 1.0)
        max_delay: Delay máximo en segundos (default: 10.0)
    """
    # Soporta uso tanto como @database_retry como @database_retry(...)
    def _decorate(f):
        @functools.wraps(f)
        def wrapper(self, *args, **kwargs):
            last_exception = None
            pool_failed = False
            func_name = f.__name__
            # Guardado temprano: si el Circuit Breaker está abierto, evitar golpear la DB
            try:
                if hasattr(self, 'is_circuit_open') and callable(getattr(self, 'is_circuit_open')):
                    if self.is_circuit_open():  # type: ignore
                        # Lecturas: servir un retorno seguro si el circuito está abierto
                        try:
                            if hasattr(self, '_is_read_operation') and self._is_read_operation(func_name):
                                if hasattr(self, '_get_default_offline_return'):
                                    return self._get_default_offline_return(func_name)
                                return None
                        except Exception:
                            logger.exception(f"Error al verificar operación de lectura en Circuit Breaker para {func_name}")
                        # Escrituras: devolver retorno seguro sin encolar
                        try:
                            if hasattr(self, '_is_write_operation') and self._is_write_operation(func_name):
                                if hasattr(self, '_get_default_offline_return'):
                                    return self._get_default_offline_return(func_name)
                                return None
                        except Exception:
                            logger.exception(f"Error al verificar operación de escritura en Circuit Breaker para {func_name}")
            except Exception:
                logger.exception(f"Error general en chequeo de Circuit Breaker para {func_name}")
            
            for attempt in range(max_retries + 1):
                try:
                    result = f(self, *args, **kwargs)
                    # No usar caché persistente offline
                    # Registrar éxito en Circuit Breaker
                    try:
                        if hasattr(self, '_cb_register_success'):
                            self._cb_register_success()
                    except Exception:
                        logger.exception(f"Error al registrar éxito en Circuit Breaker para {func_name}")
                    return result
                
                except (psycopg2.OperationalError, psycopg2.InterfaceError, psycopg2.DatabaseError) as e:
                    last_exception = e
                    error_msg = str(e).lower()
                    
                    # Verificar si es un error de conexión que justifica retry
                    connection_errors = [
                        'server closed the connection unexpectedly',
                        'connection already closed',
                        'connection not open',
                        'could not connect to server',
                        'timeout expired',
                        'connection timed out',
                        'connection refused',
                        'connection reset by peer',
                        'broken pipe',
                        'no connection to the server'
                    ]
                    
                    is_connection_error = any(err in error_msg for err in connection_errors)
                    is_server_closed = 'server closed the connection unexpectedly' in error_msg
                    
                    if not is_connection_error:
                        # Si no es error de conexión, re-lanzar inmediatamente
                        logging.error(f"Error no recuperable en {f.__name__}: {e}", exc_info=True)
                        raise e
                    
                    if attempt == max_retries:
                        # Último intento: usar conexión directa como fallback (sin caché persistente)
                        logging.warning(f"Agotados reintentos normales para {f.__name__}, intentando conexión directa...")
                        try:
                            if hasattr(self, '_crear_conexion_directa'):
                                logging.info("Usando conexión directa como fallback...")
                                pool_failed = True
                                return self._execute_with_direct_connection(f, *args, **kwargs)
                            else:
                                logging.error(f"No hay método de conexión directa disponible")
                                raise e
                        except Exception as direct_error:
                            logging.error(f"Falló también la conexión directa: {direct_error}", exc_info=True)
                            raise last_exception
                    
                    # Calcular delay con backoff exponencial y jitter
                    delay = min(base_delay * (2 ** attempt), max_delay)
                    jitter = random.uniform(0, delay * 0.1)  # 10% de jitter
                    total_delay = delay + jitter
                    
                    # Registrar fallo para Circuit Breaker
                    try:
                        if hasattr(self, '_cb_register_failure'):
                            self._cb_register_failure(e)
                    except Exception:
                        logger.exception(f"Error al registrar fallo en Circuit Breaker para {func_name}")

                    logging.warning(f"Error de conexión en {f.__name__} (intento {attempt + 1}/{max_retries + 1}): {e}")
                    logging.info(f"Reintentando en {total_delay:.2f} segundos...")
                    
                    time.sleep(total_delay)
                    
                    # Limpiar el pool de conexiones si es error de servidor cerrado
                    if hasattr(self, 'connection_pool') and self.connection_pool and is_server_closed:
                        try:
                            logging.info("Limpiando conexiones muertas del pool...")
                            if hasattr(self.connection_pool, '_cleanup_dead_connections'):
                                dead_count = self.connection_pool._cleanup_dead_connections()
                                logging.info(f"Limpiadas {dead_count} conexiones muertas del pool")
                            
                            # Forzar recreación del pool si es necesario
                            if attempt >= max_retries // 2:  # En la mitad de los intentos
                                logging.info("Forzando recreación del pool de conexiones...")
                                if hasattr(self.connection_pool, 'close_all'):
                                    self.connection_pool.close_all()
                                    
                        except Exception as pool_error:
                            logging.warning(f"Error al limpiar pool: {pool_error}", exc_info=True)
                            pool_failed = True
                
                except Exception as e:
                    # Para otros tipos de errores, no reintentar
                    logging.error(f"Error no recuperable en {f.__name__}: {e}", exc_info=True)
                    raise e
            
            # Si llegamos aquí, agotamos todos los reintentos
            logging.error(f"Agotados todos los reintentos para {f.__name__}")
            # Modo offline: devolver valor seguro sin caché persistente ni encolado
            try:
                if hasattr(self, '_is_read_operation') and self._is_read_operation(func_name):
                    if hasattr(self, '_get_default_offline_return'):
                        return self._get_default_offline_return(func_name)
                if hasattr(self, '_is_write_operation') and self._is_write_operation(func_name):
                    if hasattr(self, '_get_default_offline_return'):
                        return self._get_default_offline_return(func_name)
            except Exception:
                # No ocultar el error original
                logger.exception(f"Error al obtener valor offline por defecto para {func_name}")
            raise last_exception
            
        return wrapper
    # Si se llamó como @database_retry sin paréntesis
    if callable(func):
        return _decorate(func)
    # Si se llamó como fábrica con parámetros: @database_retry(...)
    def decorator(f):
        return _decorate(f)
    return decorator


class ConnectionPool:
    """Pool de conexiones optimizado para PostgreSQL con manejo de concurrencia"""
    
    def __init__(self, connection_params: dict, max_connections: int = 10, timeout: float = 30.0):
        self.connection_params = connection_params
        self.max_connections = max_connections
        self.timeout = timeout
        self._pool = Queue(maxsize=max_connections)
        self._lock = threading.RLock()
        self._all_connections = set()
        self.stats = {
            'connections_created': 0,
            'connections_reused': 0,
            'timeouts': 0,
            'errors': 0
        }

    def _create_connection(self) -> psycopg2.extensions.connection:
        """Crea una nueva conexión PostgreSQL optimizada"""
        try:
            # Normalizar parámetros para psycopg2: asegurar 'dbname'
            params = dict(self.connection_params)
            # Evitar conflicto: no especificar ambos 'database' y 'dbname'
            if 'dbname' in params and 'database' in params:
                try:
                    del params['database']
                except Exception:
                    logger.warning("Error menor al limpiar parámetro 'database' redundante", exc_info=True)
            elif 'dbname' not in params and 'database' in params:
                params['dbname'] = params['database']
                try:
                    del params['database']
                except Exception:
                    logger.warning("Error menor al renombrar parámetro 'database' a 'dbname'", exc_info=True)
            
            conn = psycopg2.connect(**params)
            conn.autocommit = False  # Explicit transaction control
            try:
                conn.set_client_encoding('UTF8')
            except Exception:
                logger.warning("No se pudo establecer codificación UTF8 en la conexión", exc_info=True)
            
            # Configuraciones optimizadas para PostgreSQL
            with conn.cursor() as cursor:
                cursor.execute("SET statement_timeout = '30s'")
                cursor.execute("SET lock_timeout = '5s'")
                cursor.execute("SET idle_in_transaction_session_timeout = '20s'")
                cursor.execute("SET TIME ZONE 'America/Argentina/Buenos_Aires'")
                # Tuning opcional: ignorar parámetros no permitidos por el servidor
                for stmt in [
                    "SET work_mem = '16MB'",
                    "SET maintenance_work_mem = '64MB'",
                    "SET effective_cache_size = '256MB'",
                    "SET random_page_cost = 1.2",
                    "SET seq_page_cost = 1.0",
                ]:
                    try:
                        cursor.execute(stmt)
                    except Exception as e:
                        logging.debug(f"Ignorando SET no permitido: {stmt}: {e}")
            
            conn.commit()
            
            # Rastrear conexión y estadísticas
            with self._lock:
                self._all_connections.add(id(conn))
                self.stats['connections_created'] += 1
            
            return conn
        except Exception as e:
            with self._lock:
                self.stats['errors'] += 1
            logger.error("Error crítico creando conexión a base de datos", exc_info=True)
            raise

    def _cleanup_dead_connections(self):
        """Limpia conexiones muertas del pool"""
        dead_connections = []
        
        # Crear una lista temporal para verificar conexiones
        temp_connections = []
        
        try:
            # Extraer todas las conexiones del pool para verificarlas
            while True:
                try:
                    conn = self._pool.get_nowait()
                    temp_connections.append(conn)
                except Empty:
                    break
            
            # Verificar cada conexión
            for conn in temp_connections:
                try:
                    # Verificar si la conexión está viva
                    with conn.cursor() as cursor:
                        cursor.execute("SELECT 1")
                        cursor.fetchone()
                        # Asegurar que el health check no deje una transacción abierta
                    try:
                        conn.rollback()
                    except psycopg2.Error:
                        logger.warning("Error al hacer rollback en health check", exc_info=True)
                    # Si llegamos aquí, la conexión está viva
                    self._pool.put_nowait(conn)
                except (psycopg2.OperationalError, psycopg2.InterfaceError, psycopg2.DatabaseError) as e:
                    # Conexión muerta, no la devolvemos al pool
                    dead_connections.append(conn)
                    try:
                        conn.close()
                    except Exception:
                        logger.warning("Error al cerrar conexión muerta (ignorable)", exc_info=True)
                    
                    with self._lock:
                        self.stats['connections_closed'] = self.stats.get('connections_closed', 0) + 1
                        if id(conn) in self._all_connections:
                            self._all_connections.remove(id(conn))
                            
        except Exception as e:
            # En caso de error, devolver todas las conexiones válidas al pool
            logger.error("Error durante limpieza de conexiones muertas", exc_info=True)
            for conn in temp_connections:
                if conn not in dead_connections:
                    try:
                        self._pool.put_nowait(conn)
                    except Exception:
                        logger.error("Error al devolver conexión al pool tras fallo de limpieza", exc_info=True)
        
        return len(dead_connections)

    def get_connection(self) -> psycopg2.extensions.connection:
        """Obtiene una conexión del pool con reintentos automáticos y fallback robusto"""
        try:
            env_r = os.getenv('DB_CONNECT_RETRIES')
            max_retries = int(env_r) if (env_r and env_r.strip()) else 2
        except Exception:
            logger.warning("Error al leer DB_CONNECT_RETRIES, usando default 2", exc_info=True)
            max_retries = 2
        retry_delay = 0.1
        
        for attempt in range(max_retries):
            try:
                # Intentar obtener conexión existente del pool
                conn = self._pool.get_nowait()
                
                # Verificar si la conexión está viva
                try:
                    conn.cursor().execute("SELECT 1")
                    # Asegurar que el health check no deje una transacción abierta
                    try:
                        conn.rollback()
                    except psycopg2.Error:
                        logger.warning("Error al rollback en get_connection", exc_info=True)
                    with self._lock:
                        self.stats['connections_reused'] += 1
                    return conn
                except (psycopg2.OperationalError, psycopg2.InterfaceError) as e:
                    # Conexión muerta, cerrarla y continuar
                    logging.warning(f"Conexión muerta detectada: {e}")
                    try:
                        conn.close()
                    except Exception:
                        logger.warning("Error al cerrar conexión muerta en get_connection", exc_info=True)
                    with self._lock:
                        self._all_connections.discard(conn)
                    continue
                    
            except Empty:
                try:
                    with self._lock:
                        if len(self._all_connections) < self.max_connections:
                            return self._create_connection()
                    
                    # Esperar por una conexión disponible
                    conn = self._pool.get(timeout=self.timeout)
                    
                    # Verificar si la conexión está viva
                    try:
                        conn.cursor().execute("SELECT 1")
                        # Asegurar que el health check no deje una transacción abierta
                        try:
                            conn.rollback()
                        except psycopg2.Error:
                            logger.warning("Error al rollback en get_connection (wait)", exc_info=True)
                        with self._lock:
                            self.stats['connections_reused'] += 1
                        return conn
                    except (psycopg2.OperationalError, psycopg2.InterfaceError) as e:
                        # Conexión muerta, cerrarla y continuar
                        logging.warning(f"Conexión muerta detectada: {e}")
                        try:
                            conn.close()
                        except Exception:
                            logger.warning("Error al cerrar conexión muerta en get_connection (wait)", exc_info=True)
                        with self._lock:
                            self._all_connections.discard(conn)
                        continue
                    
                except (psycopg2.OperationalError, psycopg2.DatabaseError) as e:
                    with self._lock:
                        self.stats['errors'] += 1
                    
                    error_msg = str(e).lower()
                    if any(phrase in error_msg for phrase in ["server closed", "connection", "terminated abnormally"]):
                        logging.warning(f"Error de servidor detectado: {e}")
                        if attempt < max_retries - 1:
                            # Limpiar pool de conexiones muertas
                            self._cleanup_dead_connections()
                            time.sleep(retry_delay * (2 ** attempt))
                            continue
                    raise
                except Exception as e:
                    with self._lock:
                        if "timeout" in str(e).lower():
                            self.stats['timeouts'] += 1
                        else:
                            self.stats['errors'] += 1
                    if attempt < max_retries - 1:
                        time.sleep(retry_delay)
                        continue
                    raise
        
        # Si todos los reintentos fallan y no se alcanzó el máximo, crear conexión
        try:
            with self._lock:
                if len(self._all_connections) < self.max_connections:
                    return self._create_connection()
            raise TimeoutError("Pool max connections reached")
        except Exception:
            logger.error("Error final al intentar crear conexión tras agotarse reintentos", exc_info=True)
            raise
        except Exception as e:
            logging.error(f"Falló la creación de conexión de emergencia: {e}")
            # Como último recurso, intentar conexión directa si tenemos acceso al DatabaseManager
            raise

    def _emergency_connection_recovery(self, connection_params: dict = None) -> psycopg2.extensions.connection:
        """
        Método de recuperación de emergencia que intenta crear una conexión directa
        cuando el pool falla completamente
        """
        try:
            # Usar parámetros de conexión proporcionados o los del pool
            params = connection_params or self.connection_params
            params = dict(params)
            if 'dbname' not in params and 'database' in params:
                params['dbname'] = params['database']
            
            # Crear conexión directa sin usar el pool
            conn = psycopg2.connect(**params)
            conn.autocommit = False
            try:
                conn.set_client_encoding('UTF8')
            except Exception:
                pass
            
            # Configuraciones básicas para la conexión de emergencia
            with conn.cursor() as cursor:
                cursor.execute("SET statement_timeout = '60s'")
                cursor.execute("SET lock_timeout = '10s'")
                cursor.execute("SET idle_in_transaction_session_timeout = '30s'")
                cursor.execute("SET TIME ZONE 'America/Argentina/Buenos_Aires'")
            
            conn.commit()
            
            # Registrar estadísticas de conexión de emergencia
            with self._lock:
                self.stats['emergency_connections'] = self.stats.get('emergency_connections', 0) + 1
                self.stats['connections_created'] += 1
            
            logging.warning("Conexión de emergencia creada exitosamente")
            return conn
            
        except Exception as e:
            with self._lock:
                self.stats['emergency_failures'] = self.stats.get('emergency_failures', 0) + 1
            logging.error(f"Falló la conexión de emergencia: {e}")
            raise

    def return_connection(self, conn: psycopg2.extensions.connection, is_broken: bool = False):
        """Devuelve una conexión al pool"""
        try:
            if is_broken:
                try:
                    conn.close()
                except psycopg2.Error:
                    pass
                with self._lock:
                    if id(conn) in self._all_connections:
                        self._all_connections.remove(id(conn))
                return
            if not conn.closed:
                with conn.cursor() as cursor:
                    cursor.execute("SELECT 1")
                # Asegurar que el health check no deje una transacción abierta
                try:
                    conn.rollback()
                except psycopg2.Error:
                    pass
                self._pool.put_nowait(conn)
        except (psycopg2.Error, Exception):
            try:
                conn.close()
            except psycopg2.Error:
                pass

    @contextmanager
    def connection(self):
        """Context manager para manejo automático de conexiones"""
        conn = self.get_connection()
        try:
            yield conn
        except Exception as e:
            # Rollback en caso de error
            try:
                conn.rollback()
            except psycopg2.Error:
                pass
            raise
        finally:
            self.return_connection(conn)
    
    @contextmanager
    def transaction(self):
        """Context manager para transacciones explícitas con manejo robusto"""
        max_retries = 3
        base_delay = 0.1
        
        for attempt in range(max_retries):
            try:
                conn = self.get_connection()
                prev_autocommit = conn.autocommit
                conn.autocommit = False
                with conn.cursor() as cursor:
                    cursor.execute("SET TIME ZONE 'America/Argentina/Buenos_Aires'")
                yield conn
                conn.commit()
                return
            except psycopg2.OperationalError as e:
                try:
                    if conn:
                        conn.rollback()
                except psycopg2.Error:
                    pass
                
                if "deadlock" in str(e).lower() and attempt < max_retries - 1:
                    import random
                    delay = base_delay * (2 ** attempt) + random.uniform(0, 0.1)
                    time.sleep(delay)
                    continue
                else:
                    raise
            except Exception as e:
                try:
                    if conn:
                        conn.rollback()
                except psycopg2.Error:
                    pass
                raise
            finally:
                try:
                    if conn:
                        try:
                            conn.autocommit = prev_autocommit
                        except Exception:
                            pass
                        self.return_connection(conn)
                except Exception:
                    pass
        
        raise psycopg2.OperationalError("Transaction failed after maximum retries")

    def close_all(self):
        """Cierra todas las conexiones del pool"""
        while not self._pool.empty():
            try:
                conn = self._pool.get_nowait()
                conn.close()
            except Empty:
                break
    
    def get_stats(self) -> Dict:
        """Obtiene estadísticas del pool"""
        with self._lock:
            return {
                **self.stats,
                'pool_size': self._pool.qsize(),
                'total_connections': len(self._all_connections),
                'max_connections': self.max_connections
            }

class CacheManager:
    def __init__(self, config: Dict[str, Any]):
        self._cache = {}
        self._config = config
        self._lock = threading.RLock()
        self._lru_order = {}
        self._stats = {
            'hits': 0,
            'misses': 0,
            'evictions': 0
        }

    def get(self, cache_type: str, key: Any) -> Optional[Any]:
        with self._lock:
            if cache_type in self._cache and key in self._cache[cache_type]:
                entry = self._cache[cache_type][key]
                if time.time() < entry['expires_at']:
                    self._lru_order[cache_type][key] = time.time()
                    self._stats['hits'] += 1
                    return entry['value']
            self._stats['misses'] += 1
        return None

    def set(self, cache_type: str, key: Any, value: Any, ttl_seconds: Optional[float] = None):
        with self._lock:
            if cache_type not in self._cache:
                self._cache[cache_type] = {}
                self._lru_order[cache_type] = {}
            
            config = self._config.get(cache_type, {'duration': 300, 'max_size': 100})
            # Permite override de TTL por llamada, si se especifica
            expires_at = time.time() + (ttl_seconds if ttl_seconds is not None else config['duration'])
            
            self._cache[cache_type][key] = {'value': value, 'expires_at': expires_at}
            self._lru_order[cache_type][key] = time.time()
            
            if len(self._cache[cache_type]) > config['max_size']:
                self._evict(cache_type)

    def _evict(self, cache_type: str):
        with self._lock:
            if cache_type in self._cache:
                if self._lru_order[cache_type]:
                    lru_key = min(self._lru_order[cache_type], key=self._lru_order[cache_type].get)
                    del self._cache[cache_type][lru_key]
                    del self._lru_order[cache_type][lru_key]
                    self._stats['evictions'] += 1

    def clear_expired(self):
        with self._lock:
            for cache_type in self._cache:
                expired_keys = []
                for key, entry in self._cache[cache_type].items():
                    if time.time() >= entry['expires_at']:
                        expired_keys.append(key)
                for key in expired_keys:
                    del self._cache[cache_type][key]
                    if cache_type in self._lru_order and key in self._lru_order[cache_type]:
                        del self._lru_order[cache_type][key]

    def get_stats(self) -> dict:
        with self._lock:
            return self._stats.copy()

    def invalidate(self, cache_type: str, key: Any = None):
        with self._lock:
            if key:
                if cache_type in self._cache and key in self._cache[cache_type]:
                    del self._cache[cache_type][key]
            else:
                if cache_type in self._cache:
                    self._cache[cache_type].clear()

class MassOperationQueue:
    """Sistema de cola para operaciones masivas"""
    
    def __init__(self, max_workers: int = 2):
        self.max_workers = max_workers
        self._queue = Queue()
        self._executor = concurrent.futures.ThreadPoolExecutor(max_workers=max_workers)
        self._active_operations = set()
        self._lock = threading.RLock()
        self._stats = {
            'total_operations': 0,
            'successful_operations': 0,
            'failed_operations': 0,
            'average_processing_time': 0.0
        }
    
    def submit_operation(self, operation_id: str, operation_func, *args, **kwargs) -> concurrent.futures.Future:
        """Envía una operación masiva a la cola"""
        with self._lock:
            if operation_id in self._active_operations:
                raise ValueError(f"Operación {operation_id} ya está en progreso")
            
            self._active_operations.add(operation_id)
            
            def wrapped_operation():
                start_time = time.time()
                try:
                    result = operation_func(*args, **kwargs)
                    self._stats['successful_operations'] += 1
                    return result
                except Exception as e:
                    self._stats['failed_operations'] += 1
                    raise
                finally:
                    processing_time = time.time() - start_time
                    self._update_stats(processing_time)
                    with self._lock:
                        self._active_operations.discard(operation_id)
            
            future = self._executor.submit(wrapped_operation)
            self._stats['total_operations'] += 1
            return future
    
    def _update_stats(self, processing_time: float):
        """Actualiza estadísticas de procesamiento"""
        with self._lock:
            total_ops = self._stats['successful_operations'] + self._stats['failed_operations']
            if total_ops > 0:
                current_avg = self._stats['average_processing_time']
                self._stats['average_processing_time'] = (
                    (current_avg * (total_ops - 1) + processing_time) / total_ops
                )
    
    def get_status(self, operation_id: str) -> bool:
        """Verifica si una operación está en progreso"""
        with self._lock:
            return operation_id in self._active_operations
    
    def shutdown(self, wait: bool = True):
        """Cierra el sistema de cola"""
        self._executor.shutdown(wait=wait)
