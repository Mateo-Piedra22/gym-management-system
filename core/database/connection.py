import os
import logging
import time
import threading
import concurrent.futures
from queue import Queue
from typing import Generator, Any, Dict, Optional
from contextlib import contextmanager
import functools
import random

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session, scoped_session
from sqlalchemy.exc import SQLAlchemyError, OperationalError

# Configuración de logs
logger = logging.getLogger(__name__)

# --- SQLAlchemy Configuration ---

def get_database_url() -> str:
    """Construye la URL de conexión a partir de variables de entorno."""
    url = os.getenv("DATABASE_URL")
    if url:
        return url
    
    user = os.getenv("DB_USER", "postgres")
    password = os.getenv("DB_PASSWORD", "")
    host = os.getenv("DB_HOST", "localhost")
    port = os.getenv("DB_PORT", "5432")
    db_name = os.getenv("DB_NAME", "gym_management")
    
    # Fallback for empty password
    auth = f"{user}:{password}" if password else user
    
    return f"postgresql+psycopg2://{auth}@{host}:{port}/{db_name}"

DATABASE_URL = get_database_url()

# Configuración del Engine
engine = create_engine(
    DATABASE_URL,
    pool_pre_ping=True,       # Verifica la conexión antes de usarla
    pool_size=10,             # Tamaño del pool
    max_overflow=20,          # Conexiones extra permitidas
    pool_recycle=1800,        # Reciclar conexiones cada 30 mins
    connect_args={
        "options": "-c timezone=America/Argentina/Buenos_Aires"
    }
)

session_factory = sessionmaker(autocommit=False, autoflush=False, bind=engine)
SessionLocal = scoped_session(session_factory)

def get_db() -> Generator[Session, None, None]:
    """Generador de sesiones para inyección de dependencias."""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

# --- Legacy Support & Utilities ---

def database_retry(func=None, *, max_retries=3, base_delay=1.0, max_delay=10.0):
    """
    Decorador para manejar reconexiones automáticas (adaptado para SQLAlchemy).
    """
    def _decorate(f):
        @functools.wraps(f)
        def wrapper(*args, **kwargs):
            last_exception = None
            for attempt in range(max_retries + 1):
                try:
                    return f(*args, **kwargs)
                except (OperationalError, SQLAlchemyError) as e:
                    last_exception = e
                    error_msg = str(e).lower()
                    
                    # Detectar errores de conexión recuperables
                    recoverable = any(err in error_msg for err in [
                        'server closed the connection',
                        'connection', 
                        'timeout',
                        'could not connect'
                    ])
                    
                    if not recoverable or attempt == max_retries:
                        logger.error(f"Error no recuperable o reintentos agotados en {f.__name__}: {e}")
                        raise e
                    
                    delay = min(base_delay * (2 ** attempt), max_delay)
                    jitter = random.uniform(0, delay * 0.1)
                    time.sleep(delay + jitter)
                    logger.warning(f"Reintentando {f.__name__} (Intento {attempt+1}) tras error: {e}")
            
            raise last_exception
        return wrapper

    if callable(func):
        return _decorate(func)
    def decorator(f):
        return _decorate(f)
    return decorator

class CacheManager:
    """Gestor de caché simple en memoria (Legacy preservado)."""
    def __init__(self, config: Dict[str, Any]):
        self._cache = {}
        self._config = config
        self._lock = threading.RLock()
        self._lru_order = {}
        self._stats = {'hits': 0, 'misses': 0, 'evictions': 0}

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
            expires_at = time.time() + (ttl_seconds if ttl_seconds is not None else config['duration'])
            
            self._cache[cache_type][key] = {'value': value, 'expires_at': expires_at}
            self._lru_order[cache_type][key] = time.time()
            
            if len(self._cache[cache_type]) > config['max_size']:
                self._evict(cache_type)

    def _evict(self, cache_type: str):
        with self._lock:
            if cache_type in self._cache and self._lru_order[cache_type]:
                lru_key = min(self._lru_order[cache_type], key=self._lru_order[cache_type].get)
                del self._cache[cache_type][lru_key]
                del self._lru_order[cache_type][lru_key]
                self._stats['evictions'] += 1

    def clear_expired(self):
        with self._lock:
            for cache_type in self._cache:
                expired_keys = [k for k, v in self._cache[cache_type].items() if time.time() >= v['expires_at']]
                for k in expired_keys:
                    del self._cache[cache_type][k]
                    if k in self._lru_order.get(cache_type, {}):
                        del self._lru_order[cache_type][k]

    def invalidate(self, cache_type: str, key: Any = None):
        with self._lock:
            if key:
                if cache_type in self._cache and key in self._cache[cache_type]:
                    del self._cache[cache_type][key]
            else:
                if cache_type in self._cache:
                    self._cache[cache_type].clear()

    def get_stats(self) -> dict:
        with self._lock:
            return self._stats.copy()

class MassOperationQueue:
    """Sistema de cola para operaciones masivas (Legacy preservado)."""
    
    def __init__(self, max_workers: int = 2):
        self.max_workers = max_workers
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
                except Exception:
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
        with self._lock:
            total_ops = self._stats['successful_operations'] + self._stats['failed_operations']
            if total_ops > 0:
                current_avg = self._stats['average_processing_time']
                self._stats['average_processing_time'] = (
                    (current_avg * (total_ops - 1) + processing_time) / total_ops
                )
    
    def get_status(self, operation_id: str) -> bool:
        with self._lock:
            return operation_id in self._active_operations
    
    def shutdown(self, wait: bool = True):
        self._executor.shutdown(wait=wait)
