from typing import Generic, TypeVar, Type, Optional, List, Any
from sqlalchemy.orm import Session
from core.database.connection import SessionLocal

class BaseService:
    def __init__(self, db: Session = None):
        self.db = db or SessionLocal()

    def close(self):
        if self.db:
            self.db.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
