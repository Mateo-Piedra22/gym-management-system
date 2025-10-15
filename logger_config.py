import logging
from logging.handlers import RotatingFileHandler
import os
import sys  # Importar sys para el manejo de excepciones
from datetime import datetime
import traceback # Importar traceback para formatear la excepción
import io  # Para envolver stdout/stderr con UTF-8

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
    
    # Opcional: Mostrar un mensaje de error al usuario antes de cerrar
    # from PyQt6.QtWidgets import QMessageBox
    # QMessageBox.critical(None, "Error Inesperado", 
    #                      "La aplicación ha encontrado un error fatal y debe cerrarse. "
    #                      "Por favor, revise los archivos de log para más detalles.")

def setup_logging():
    """Configura el sistema de logging para la aplicación."""
    # Asegurar entorno UTF-8 para evitar errores de 'charmap'
    try:
        os.environ.setdefault("PYTHONIOENCODING", "utf-8")
    except Exception:
        pass

    log_dir = 'logs'
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)

    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    log_filename = f"log_{timestamp}.log"
    log_filepath = os.path.join(log_dir, log_filename)

    log_formatter = logging.Formatter(
        '%(asctime)s - %(levelname)s - [%(module)s:%(lineno)d] - %(message)s'
    )

    file_handler = logging.FileHandler(log_filepath, encoding='utf-8')
    file_handler.setFormatter(log_formatter)

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(log_formatter)

    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    if logger.hasHandlers():
        logger.handlers.clear()

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

    logging.info(f"Sistema de logging configurado. Registrando en: {log_filepath}")