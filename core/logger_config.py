import logging
from logging.handlers import RotatingFileHandler
import os
import sys  # Importar sys para el manejo de excepciones
from datetime import datetime
import traceback # Importar traceback para formatear la excepción
import io  # Para envolver stdout/stderr con UTF-8
import tempfile

# --- FUNCIÓN NUEVA: Manejador global de excepciones ---
def handle_exception(exc_type, exc_value, exc_traceback):
    """
    Captura y registra cualquier excepción no controlada en la aplicación.
    Esto asegura que incluso si la app crashea, el error quedará en el log.
    """
    if issubclass(exc_type, KeyboardInterrupt):
        # No registrar el error si el usuario cierra la app con Ctrl+C
        sys.__excepthook__(exc_type, exc_value, exc_traceback)
        return

    # Formatear el traceback para que sea legible
    error_msg = "".join(traceback.format_exception(exc_type, exc_value, exc_traceback))
    
    # Registrar el error como CRÍTICO
    logging.critical(f"Excepción no controlada:\n{error_msg}")
    

def setup_logging():
    """Configura el sistema de logging para la aplicación con rutas resistentes en .exe."""
    # Asegurar entorno UTF-8 para evitar errores de 'charmap'
    try:
        os.environ.setdefault("PYTHONIOENCODING", "utf-8")
    except Exception:
        pass

    # Elegir un directorio de logs escribible, con fallback para entorno empaquetado
    log_dir = None
    try:
        if getattr(sys, "frozen", False):
            exe_dir = os.path.dirname(sys.executable)
        else:
            exe_dir = os.getcwd()
        candidate = os.path.join(exe_dir, 'logs')
        os.makedirs(candidate, exist_ok=True)
        log_dir = candidate
    except Exception:
        log_dir = None
    if log_dir is None:
        try:
            lad = os.getenv("LOCALAPPDATA")
            if lad:
                candidate = os.path.join(lad, 'GymMS', 'logs')
                os.makedirs(candidate, exist_ok=True)
                log_dir = candidate
        except Exception:
            log_dir = None
    if log_dir is None:
        try:
            candidate = os.path.join(tempfile.gettempdir(), 'GymMS', 'logs')
            os.makedirs(candidate, exist_ok=True)
            log_dir = candidate
        except Exception:
            log_dir = None

    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    log_filename = f"log_{timestamp}.log"
    log_filepath = os.path.join(log_dir, log_filename) if log_dir else None

    log_formatter = logging.Formatter(
        '%(asctime)s - %(levelname)s - [%(module)s:%(lineno)d] - %(message)s'
    )

    file_handler = None
    if log_filepath:
        try:
            file_handler = logging.FileHandler(log_filepath, encoding='utf-8')
            file_handler.setFormatter(log_formatter)
        except Exception:
            file_handler = None

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(log_formatter)

    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    if logger.hasHandlers():
        logger.handlers.clear()

    if file_handler:
        logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    
    # Forzar stdout/stderr a UTF-8 para evitar UnicodeEncodeError en 'print' con emojis/símbolos
    try:
        if hasattr(sys, "stdout"):
            if sys.stdout is None:
                sys.stdout = open(os.devnull, "w", encoding="utf-8")
            elif hasattr(sys.stdout, "buffer"):
                sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
        if hasattr(sys, "stderr"):
            if sys.stderr is None:
                sys.stderr = open(os.devnull, "w", encoding="utf-8")
            elif hasattr(sys.stderr, "buffer"):
                sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")
    except Exception:
        # No romper si el entorno no permite envolver los streams
        pass
    
    # --- LÍNEA CLAVE: Establecer nuestro manejador como el por defecto ---
    sys.excepthook = handle_exception

    if log_filepath:
        logging.info(f"Sistema de logging configurado. Registrando en: {log_filepath}")
    else:
        logging.info("Sistema de logging configurado. Registrando solo en consola (sin ruta de archivo escribible)")
import sys as _sys_logger