import sys
import logging
import contextvars
from pathlib import Path
from typing import Optional

from fastapi import Request, HTTPException, status
from fastapi.responses import RedirectResponse

# Adjust sys.path to ensure we can import from core
try:
    _here = Path(__file__).resolve()
    # Try to find the project root (gym-management-system)
    # Structure: gym-management-system/apps/webapp/dependencies.py
    # So root is 3 levels up
    project_root = _here.parent.parent.parent
    if project_root.exists() and str(project_root) not in sys.path:
        sys.path.insert(0, str(project_root))
except Exception:
    pass

try:
    from core.database import DatabaseManager
    from core.payment_manager import PaymentManager
    from apps.admin.database import AdminDatabaseManager as AdminDBManager
    from core.routine_manager import RoutineTemplateManager as RoutineManager
    # Import models to ensure they are available if needed, though typically used inside functions
    from core.models import Usuario, Pago, MetodoPago, Clase, ClaseHorario
except ImportError as e:
    logging.warning(f"Could not import core modules in dependencies.py. Ensure PYTHONPATH is set. Error: {e}")
    DatabaseManager = None
    PaymentManager = None
    AdminDBManager = None
    RoutineManager = None
    Usuario = None
    Pago = None
    MetodoPago = None
    Clase = None
    ClaseHorario = None

logger = logging.getLogger(__name__)

# Global ContextVar for Tenant
CURRENT_TENANT = contextvars.ContextVar("current_tenant", default=None)

def get_db() -> Optional[DatabaseManager]:
    """
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
    Dependency that raises HTTP 503 if DatabaseManager is not available.
    """
    db = get_db()
    if db is None:
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail="DB no disponible")
    return db

def get_pm() -> Optional[PaymentManager]:
    """
    Dependency to get PaymentManager instance.
    """
    try:
        return PaymentManager()
    except Exception as e:
        logger.error(f"Error instantiating PaymentManager: {e}")
        return None

def require_pm() -> PaymentManager:
    """
    Dependency that raises HTTP 503 if PaymentManager is not available.
    """
    pm = get_pm()
    if pm is None:
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail="PaymentManager no disponible")
    return pm

def get_rm() -> Optional[RoutineManager]:
    """
    Dependency to get RoutineManager instance.
    """
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

def get_admin_db() -> Optional[AdminDBManager]:
    """
    Dependency to get AdminDBManager instance.
    """
    try:
        return AdminDBManager()
    except Exception as e:
        logger.error(f"Error instantiating AdminDBManager: {e}")
        return None

# Security Dependencies

async def require_gestion_access(request: Request):
    """
    Verifies if the user is logged in for gestion access.
    Redirects to login if not, or returns 401 for API calls.
    """
    if not request.session.get("logged_in"):
        if request.url.path.startswith("/api/"):
             raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Unauthorized")
        return RedirectResponse(url="/gestion/login", status_code=303)
    return True

async def require_owner(request: Request):
    """
    Verifies if the user is logged in and has 'owner' or 'admin' role.
    """
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
    """
    Alias for require_owner for now.
    """
    return await require_owner(request)

async def require_profesor(request: Request):
    """
    Verifies if the user is logged in and has 'profesor' or owner privileges.
    """
    if not request.session.get("logged_in"):
         if request.url.path.startswith("/api/"):
             raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Unauthorized")
         return RedirectResponse(url="/gestion/login", status_code=303)
    
    role = request.session.get("role")
    # Owners can also access professor routes
    if role not in ("profesor", "dueño", "dueno", "owner", "admin", "administrador"):
         if request.url.path.startswith("/api/"):
             raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Forbidden")
         return RedirectResponse(url="/gestion", status_code=303)
    return True
