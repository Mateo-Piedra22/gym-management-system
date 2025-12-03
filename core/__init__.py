import warnings
# Ignore SyntaxWarnings from xltpl library
warnings.filterwarnings("ignore", category=SyntaxWarning, module=".*xltpl.*")

from .database import DatabaseManager
from .models import (
    Usuario,
    Pago,
    MetodoPago,
    ConceptoPago,
    Ejercicio,
    Rutina,
    RutinaEjercicio,
    Clase,
    ClaseHorario,
    ClaseUsuario,
    EjercicioGrupo,
    EjercicioGrupoItem,
    TipoCuota,
    UsuarioNota,
    Etiqueta,
    UsuarioEtiqueta,
    UsuarioEstado,
    Asistencia,
    PagoDetalle,
)
from .payment_manager import PaymentManager
from .pdf_generator import PDFGenerator
from .routine_manager import RoutineTemplateManager
try:
    from .export_manager import ExportManager
except Exception:
    ExportManager = None
from .utils import (
    read_gym_data,
    get_gym_value,
    get_gym_name,
    get_webapp_base_url,
    resource_path,
    safe_get,
    terminate_tunnel_processes,
    collect_log_candidates,
    collect_temp_candidates,
    delete_files,
    get_public_tunnel_enabled,
)

__all__ = [
    "DatabaseManager",
    "Usuario",
    "Pago",
    "MetodoPago",
    "ConceptoPago",
    "Ejercicio",
    "Rutina",
    "RutinaEjercicio",
    "Clase",
    "ClaseHorario",
    "ClaseUsuario",
    "EjercicioGrupo",
    "EjercicioGrupoItem",
    "TipoCuota",
    "UsuarioNota",
    "Etiqueta",
    "UsuarioEtiqueta",
    "UsuarioEstado",
    "Asistencia",
    "PagoDetalle",
    "PaymentManager",
    "PDFGenerator",
    "RoutineTemplateManager",
    "ExportManager",
    "read_gym_data",
    "get_gym_value",
    "get_gym_name",
    "get_webapp_base_url",
    "resource_path",
    "safe_get",
    "terminate_tunnel_processes",
    "collect_log_candidates",
    "collect_temp_candidates",
    "delete_files",
    "get_public_tunnel_enabled",
]