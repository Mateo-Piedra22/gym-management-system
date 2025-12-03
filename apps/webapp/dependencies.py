import sys
import logging
import contextvars
from pathlib import Path
from typing import Optional, Generator

from fastapi import Request, HTTPException, status, Depends
from fastapi.responses import RedirectResponse

# Adjust sys.path to ensure we can import from core
try:
    _here = Path(__file__).resolve()
    project_root = _here.parent.parent.parent
    if project_root.exists() and str(project_root) not in sys.path:
        sys.path.insert(0, str(project_root))
except Exception:
    pass

try:
    from core.database import DatabaseManager
    from core.payment_manager import PaymentManager
    from core.routine_manager import RoutineTemplateManager as RoutineManager
    # Services
    from core.services import UserService, PaymentService, GymService, AttendanceService, TeacherService
    from core.services.admin_service import AdminService
    from core.database.connection import SessionLocal
except ImportError as e:
    logging.warning(f"Could not import core modules in dependencies.py. Ensure PYTHONPATH is set. Error: {e}")
    DatabaseManager = None
    PaymentManager = None
    RoutineManager = None
    UserService = None
    PaymentService = None
    GymService = None
    AttendanceService = None
    TeacherService = None
    AdminService = None

logger = logging.getLogger(__name__)

# Global ContextVar for Tenant
CURRENT_TENANT = contextvars.ContextVar("current_tenant", default=None)

def get_db() -> Optional[DatabaseManager]:
    """
    DEPRECATED: Use specific services instead.
    Dependency to get DatabaseManager instance.
    Returns None if instantiation fails.
    """
    try:
        return DatabaseManager()
    except Exception as e:
        logger.error(f"Error instantiating DatabaseManager: {e}")
        return None

def require_db() -> DatabaseManager:
    """
    DEPRECATED: Use specific services instead.
    """
    db = get_db()
    if db is None:
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail="DB no disponible")
    return db

# --- Service Dependencies ---

def get_db_session():
    session = SessionLocal()
    try:
        yield session
    finally:
        session.close()

def get_user_service(session = Depends(get_db_session)) -> UserService:
    return UserService(session)

def get_payment_service(session = Depends(get_db_session)) -> PaymentService:
    return PaymentService(session)

def get_gym_service(session = Depends(get_db_session)) -> GymService:
    return GymService(session)

def get_attendance_service(session = Depends(get_db_session)) -> AttendanceService:
    return AttendanceService(session)

def get_teacher_service(session = Depends(get_db_session)) -> TeacherService:
    return TeacherService(session)

def get_admin_service() -> Optional[AdminService]:
    try:
        if AdminService is None:
            return None
        params = AdminService.resolve_admin_db_params()
        db = DatabaseManager(connection_params=params)
        return AdminService(db)
    except Exception as e:
        logger.error(f"Error instantiating AdminService: {e}")
        return None

# --- Legacy Managers ---

def get_pm() -> Optional[PaymentManager]:
    try:
        return PaymentManager()
    except Exception as e:
        logger.error(f"Error instantiating PaymentManager: {e}")
        return None

def require_pm() -> PaymentManager:
    pm = get_pm()
    if pm is None:
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail="PaymentManager no disponible")
    return pm

def get_rm() -> Optional[RoutineManager]:
    try:
        if RoutineManager is None:
            return None
        db = get_db()
        if db is None:
            return None
        return RoutineManager(database_manager=db)
    except Exception as e:
        logger.error(f"Error instantiating RoutineManager: {e}")
        return None

def get_admin_db() -> Optional[AdminService]:
    """Deprecated alias for get_admin_service"""
    return get_admin_service()

# --- Security Dependencies ---

async def require_gestion_access(request: Request):
    if not request.session.get("logged_in"):
        if request.url.path.startswith("/api/"):
             raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Unauthorized")
        return RedirectResponse(url="/gestion/login", status_code=303)
    return True

async def require_owner(request: Request):
    if not request.session.get("logged_in"):
         if request.url.path.startswith("/api/"):
             raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Unauthorized")
         return RedirectResponse(url="/gestion/login", status_code=303)
    
    role = request.session.get("role")
    if role not in ("dueño", "dueno", "owner", "admin", "administrador"):
         if request.url.path.startswith("/api/"):
             raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Forbidden")
         return RedirectResponse(url="/gestion", status_code=303)
    return True

async def require_admin(request: Request):
    return await require_owner(request)

async def require_profesor(request: Request):
    if not request.session.get("logged_in"):
         if request.url.path.startswith("/api/"):
             raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Unauthorized")
         return RedirectResponse(url="/gestion/login", status_code=303)
    
    role = request.session.get("role")
    if role not in ("profesor", "dueño", "dueno", "owner", "admin", "administrador"):
         if request.url.path.startswith("/api/"):
             raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Forbidden")
         return RedirectResponse(url="/gestion", status_code=303)
    return True
