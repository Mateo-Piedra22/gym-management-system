from typing import Any, Optional
import logging
from sqlalchemy.orm import Session
from ..connection import CacheManager

class BaseRepository:
    def __init__(self, db: Session, cache: Optional[CacheManager] = None, logger: Optional[logging.Logger] = None):
        self.db = db
        self.cache = cache
        self.logger = logger or logging.getLogger(__name__)

    def _invalidate_cache(self, cache_type: str, key: Any = None):
        """Invalidate cache helper"""
        if self.cache:
            self.cache.invalidate(cache_type, key)
