import os
import json
import logging
from pathlib import Path
from typing import Dict, Any
from .utils import get_gym_name
from .secure_config import SecureConfig as SecureConfig

logger = logging.getLogger(__name__)

# Ruta base del proyecto
BASE_DIR = Path(__file__).parent
CONFIG_DIR = BASE_DIR / "config"

# Cargar configuración desde JSON (para compatibilidad hacia atrás)
config_path = CONFIG_DIR / "config.json"
if config_path.exists():
    with open(config_path, 'r', encoding='utf-8') as f:
        CONFIG = json.load(f)
else:
    CONFIG = {}

# IMPORTANTE: Las credenciales ahora deben obtenerse desde variables de entorno
# usando el módulo secure_config. Los valores en config.json están obsoletos.

# Nombre de servicio para el almacén seguro de credenciales (keyring)
# Centraliza la etiqueta usada para guardar/leer contraseñas de DB
KEYRING_SERVICE_NAME = "GymMS_DB"

# Etiquetas anteriores para migración automática de contraseñas guardadas.
# Si cambias KEYRING_SERVICE_NAME, el código intentará copiar desde estas etiquetas.
LEGACY_KEYRING_SERVICE_NAMES = [
    "GymMS_DB",  # etiqueta usada anteriormente
]

class Config:
    """
    Clase de configuración del sistema.
    Centraliza parámetros del sistema para fácil mantenimiento.
    """
    
    # --- Configuración de la base de datos ---
    # DSN dinámico derivado exclusivamente de .env
    try:
        _db = SecureConfig.get_db_config('local')
        DATABASE_PATH = f"postgresql://{_db.get('user','postgres')}@{_db.get('host','localhost')}:{_db.get('port',5432)}/{_db.get('database','gimnasio')}"
    except Exception:
        DATABASE_PATH = "postgresql://localhost/gimnasio"
    
    # --- Configuración de directorios de archivos ---
    PDF_OUTPUT_DIR = "recibos"
    EXPORT_DIR = "exports"
    LOGS_DIR = "logs"
    BACKUP_DIR = "backups" # Directorio para copias de seguridad
    
    # --- Configuración de precios por defecto ---
    DEFAULT_MEMBERSHIP_PRICE = 30000.0   # Precio cuota estándar
    DEFAULT_STUDENT_PRICE = 27000.0      # Precio cuota estudiante
    
    # --- Información del Gimnasio (para recibos, etc.) ---
    GYM_INFO = {
        "nombre": get_gym_name("Gimnasio"),
        "direccion": "Saavedra 2343, Santa Fe",
        "telefono": "+54 342 123 4567",
        "email": "info@gimnasiozurka.com",
        "website": "www.gimnasiozurka.com",
    }
    
    @classmethod
    def ensure_directories(cls):
        """Asegura que todos los directorios necesarios existan."""
        directories = [
            cls.PDF_OUTPUT_DIR,
            cls.EXPORT_DIR,
            cls.LOGS_DIR,
            cls.BACKUP_DIR
        ]
        for directory in directories:
            try:
                Path(directory).mkdir(exist_ok=True)
            except Exception as e:
                print(f"Error al crear directorio {directory}: {e}")

def get_system_info() -> Dict[str, Any]:
    """Obtiene información completa del sistema para mostrar en la UI."""
    return {
        "gimnasio": Config.GYM_INFO,
        "version": "4.0", # VERSIÓN ACTUALIZADA
        "database_path": Config.DATABASE_PATH,
    }

# Llama a esta función al inicio para asegurar que los directorios están listos
Config.ensure_directories()
# Flag para controlar si se muestra el prompt de aviso de cupo
ENABLE_WAITLIST_PROMPT = True
