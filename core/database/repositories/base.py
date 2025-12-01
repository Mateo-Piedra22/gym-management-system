from typing import Any, Dict, List, Optional, Tuple, Union
import logging
import psycopg2
from ..connection import ConnectionPool, CacheManager, database_retry

class BaseRepository:
    def __init__(self, connection_pool: ConnectionPool, cache: CacheManager, logger: logging.Logger):
        self.pool = connection_pool
        self.cache = cache
        self.logger = logger

    @property
    def connection(self):
        return self.pool.connection()
    
    @property
    def transaction(self):
        return self.pool.transaction()
    
    def _invalidate_cache(self, cache_type: str, key: Any = None):
        """Invalidate cache helper"""
        self.cache.invalidate(cache_type, key)
