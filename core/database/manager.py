import logging
import os
import threading
from typing import Optional, Dict, Any
from .connection import SessionLocal, CacheManager, MassOperationQueue, database_retry
from .repositories.user_repository import UserRepository
from .repositories.payment_repository import PaymentRepository
from .repositories.attendance_repository import AttendanceRepository
from .repositories.gym_repository import GymRepository
from .repositories.teacher_repository import TeacherRepository
from .repositories.reports_repository import ReportsRepository
from .repositories.audit_repository import AuditRepository
from .repositories.whatsapp_repository import WhatsappRepository

class DatabaseManager:
    _instance = None
    _lock = threading.Lock()

    def __new__(cls):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super(DatabaseManager, cls).__new__(cls)
                    cls._instance._initialized = False
        return cls._instance

    def __init__(self):
        if self._initialized:
            return
            
        self.logger = logging.getLogger(__name__)
        
        # Cache Config
        cache_config = {
            'usuarios': {'duration': 300, 'max_size': 1000},
            'pagos': {'duration': 60, 'max_size': 500},
            'config': {'duration': 3600, 'max_size': 50},
            'ejercicios': {'duration': 1800, 'max_size': 200},
            'asistencias': {'duration': 60, 'max_size': 500},
        }
        self.cache = CacheManager(cache_config)
        self.mass_operation_queue = MassOperationQueue(max_workers=2)
        
        # Session Factory (Scoped)
        self.session = SessionLocal
        
        # Repositories
        # Passing scoped session proxy. 
        # Repositories will call methods on it, which delegates to thread-local session.
        self.users = UserRepository(self.session, self.cache, self.logger)
        self.pagos = PaymentRepository(self.session, self.cache, self.logger)
        self.gym = GymRepository(self.session, self.cache, self.logger)
        self.asistencias = AttendanceRepository(self.session, self.cache, self.logger)
        self.profesores = TeacherRepository(self.session, self.cache, self.logger)
        self.reportes = ReportsRepository(self.session, self.cache, self.logger)
        self.audit = AuditRepository(self.session, self.cache, self.logger)
        self.whatsapp = WhatsappRepository(self.session, self.cache, self.logger)
        
        # Aliases for legacy compatibility (matching old names if any, or creating consistent ones)
        self.user_repo = self.users
        self.payment_repo = self.pagos
        self.gym_repo = self.gym
        self.attendance_repo = self.asistencias
        self.teacher_repo = self.profesores
        self.reports_repo = self.reportes
        self.audit_repo = self.audit
        self.whatsapp_repo = self.whatsapp
        
        self._initialized = True
        self.logger.info("DatabaseManager initialized (SQLAlchemy ORM)")

    def close(self):
        """Cierra la sesión del hilo actual y recursos"""
        self.session.remove()
        self.mass_operation_queue.shutdown()

    def inicializar_base_datos(self):
        """
        Legacy method. Now managed by Alembic.
        """
        self.logger.info("La inicialización de la base de datos ahora es manejada por Alembic.")

    # Legacy support wrapper for methods that might still be called on manager directly
    # (If any exist that I missed moving to repos, they will fail now, forcing clean up)
    
    @property
    def session_scope(self):
        return self.session
