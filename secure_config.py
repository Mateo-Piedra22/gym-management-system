"""
Módulo de configuración segura para el Sistema de Gestión de Gimnasio.

Este módulo maneja la carga segura de variables de entorno y proporciona
una interfaz centralizada para acceder a todas las configuraciones del sistema.

IMPORTANTE: Nunca hardcodear credenciales en este archivo.
"""

import os
import sys
from pathlib import Path
from typing import Optional, Dict, Any
from dotenv import load_dotenv
from security_utils import SecurityUtils

# Cargar variables de entorno desde archivo .env
env_path = Path(__file__).parent / '.env'
if env_path.exists():
    load_dotenv(env_path)

class SecureConfig:
    """Clase para manejar configuraciones seguras del sistema."""
    
    # =============================================================================
    # MÉTODOS DE UTILIDAD PARA OBTENER VARIABLES DE ENTORNO
    # =============================================================================
    
    @staticmethod
    def get_env_variable(key: str, default: Optional[str] = None, required: bool = True) -> str:
        """
        Obtiene una variable de entorno de manera segura.
        
        Args:
            key: Nombre de la variable de entorno
            default: Valor por defecto si no existe
            required: Si es True y no existe la variable, lanza error
            
        Returns:
            Valor de la variable de entorno
            
        Raises:
            ValueError: Si la variable requerida no existe
        """
        value = os.getenv(key, default)
        if required and value is None:
            raise ValueError(f"Variable de entorno requerida no configurada: {key}")
        return value
    
    @staticmethod
    def get_env_bool(key: str, default: bool = False, required: bool = False) -> bool:
        """Obtiene una variable de entorno como booleano."""
        value = os.getenv(key)
        if value is None:
            if required:
                raise ValueError(f"Variable de entorno requerida no configurada: {key}")
            return default
        return value.lower() in ('true', '1', 'yes', 'on')
    
    @staticmethod
    def get_env_int(key: str, default: int = 0, required: bool = False) -> int:
        """Obtiene una variable de entorno como entero."""
        value = os.getenv(key)
        if value is None:
            if required:
                raise ValueError(f"Variable de entorno requerida no configurada: {key}")
            return default
        try:
            return int(value)
        except ValueError:
            if required:
                raise ValueError(f"Variable de entorno debe ser entero: {key}")
            return default
    
    # =============================================================================
    # CONFIGURACIONES DE BASE DE DATOS
    # =============================================================================
    
    @classmethod
    def get_db_config(cls, profile: str = None) -> Dict[str, Any]:
        """
        Obtiene configuración de base de datos para el modelo único Neon.
        Prioriza variables genéricas `DB_*` y acepta `DB_LOCAL_*` como fallback.
        """
        if profile is None:
            profile = cls.get_env_variable('DB_PROFILE', 'local')
        
        if profile == 'local':
            # Resolver host/port/db/user con genéricos primero (recortando espacios)
            host = (os.getenv('DB_HOST') or cls.get_env_variable('DB_LOCAL_HOST', 'localhost', required=False) or 'localhost')
            host = str(host).strip()
            try:
                port = int(os.getenv('DB_PORT') or '')
            except Exception:
                port = 0
            if not port:
                port = cls.get_env_int('DB_LOCAL_PORT', 5432)

            database = (os.getenv('DB_NAME') or cls.get_env_variable('DB_LOCAL_DATABASE', 'gimnasio', required=False) or 'gimnasio')
            database = str(database).strip()
            user = (os.getenv('DB_USER') or cls.get_env_variable('DB_LOCAL_USER', 'postgres', required=False) or 'postgres')
            user = str(user).strip()

            # Password desde múltiples fuentes de entorno (no se usa config.json aquí)
            password = os.getenv('DB_PASSWORD') or os.getenv('DB_LOCAL_PASSWORD') or os.getenv('PGPASSWORD')
            if not (password and str(password).strip()):
                raise ValueError('Variable de entorno requerida no configurada: DB_PASSWORD/DB_LOCAL_PASSWORD/PGPASSWORD')

            sslmode = (os.getenv('DB_SSLMODE') or cls.get_env_variable('DB_LOCAL_SSLMODE', 'prefer', required=False) or 'prefer')
            sslmode = str(sslmode).strip()
            try:
                connect_timeout = int(os.getenv('DB_CONNECT_TIMEOUT') or '')
            except Exception:
                connect_timeout = 0
            if not connect_timeout:
                connect_timeout = cls.get_env_int('DB_LOCAL_CONNECT_TIMEOUT', 10)

            application_name = (os.getenv('DB_APPLICATION_NAME') or cls.get_env_variable('DB_LOCAL_APPLICATION_NAME', 'gym_management_system', required=False) or 'gym_management_system')
            application_name = str(application_name).strip()

            return {
                'host': host,
                'port': port,
                'database': database,
                'user': user,
                'password': password,
                'sslmode': sslmode,
                'connect_timeout': connect_timeout,
                'application_name': application_name,
            }
        else:
            raise ValueError(f"Perfil de base de datos no válido: {profile}")
    
    # =============================================================================
    # CONFIGURACIONES DE SEGURIDAD
    # =============================================================================
    
    @classmethod
    def get_dev_password(cls) -> str:
        """Obtiene la contraseña de desarrollador/dueño."""
        return cls.get_env_variable('DEV_PASSWORD', required=True)
    
    @classmethod
    def get_owner_password(cls) -> str:
        """Obtiene la contraseña del propietario."""
        return cls.get_env_variable('OWNER_PASSWORD', required=True)
    
    @classmethod
    def get_webapp_session_secret(cls) -> str:
        """Obtiene el secreto de sesión de la aplicación web."""
        return cls.get_env_variable('WEBAPP_SESSION_SECRET', required=True)
    
    @classmethod
    def get_whatsapp_access_token(cls) -> str:
        """Obtiene el token de acceso de WhatsApp Business API."""
        return cls.get_env_variable('WHATSAPP_ACCESS_TOKEN', required=True)
    
    @classmethod
    def get_tailscale_auth_key(cls) -> Optional[str]:
        """Obtiene la clave de autenticación de Tailscale."""
        return cls.get_env_variable('TAILSCALE_AUTH_KEY', required=False)
    
    # =============================================================================
    # MÉTODOS DE SEGURIDAD CON BCRYPT
    # =============================================================================
    
    @classmethod
    def verify_owner_password(cls, password: str) -> bool:
        """
        Verifica la contraseña del propietario usando bcrypt.
        
        Args:
            password: Contraseña en texto plano
            
        Returns:
            True si la contraseña es válida, False en caso contrario
        """
        stored_hash = cls.get_env_variable('OWNER_PASSWORD_HASH', required=False)
        
        # Si no hay hash almacenado, usar la contraseña en texto plano (retrocompatibilidad)
        if not stored_hash:
            stored_password = cls.get_env_variable('OWNER_PASSWORD', required=False)
            return stored_password and password == stored_password
        
        # Verificar con bcrypt
        return SecurityUtils.verify_password(password, stored_hash)
    
    @classmethod
    def verify_dev_password(cls, password: str) -> bool:
        """
        Verifica la contraseña de desarrollador usando bcrypt.
        
        Args:
            password: Contraseña en texto plano
            
        Returns:
            True si la contraseña es válida, False en caso contrario
        """
        stored_hash = cls.get_env_variable('DEV_PASSWORD_HASH', required=False)
        
        # Si no hay hash almacenado, usar la contraseña en texto plano (retrocompatibilidad)
        if not stored_hash:
            stored_password = cls.get_env_variable('DEV_PASSWORD', required=False)
            return stored_password and password == stored_password
        
        # Verificar con bcrypt
        return SecurityUtils.verify_password(password, stored_hash)
    
    # =============================================================================
    # CONFIGURACIONES DE APLICACIÓN
    # =============================================================================
    
    @classmethod
    def get_webapp_base_url(cls) -> str:
        """Obtiene la URL base de la aplicación web."""
        return cls.get_env_variable('WEBAPP_BASE_URL', required=True)
    
    @classmethod
    def get_client_base_url(cls) -> str:
        """Obtiene la URL del cliente."""
        return cls.get_env_variable('CLIENT_BASE_URL', '')
    
    @classmethod
    def get_server_public_ip(cls) -> str:
        """Obtiene la IP pública del servidor."""
        return cls.get_env_variable('SERVER_PUBLIC_IP', required=True)
    
    # =============================================================================
    # CONFIGURACIONES DE TAREAS PROGRAMADAS
    # =============================================================================
    
    @classmethod
    def get_scheduled_tasks_config(cls) -> Dict[str, Any]:
        """Obtiene configuración de tareas programadas (sin replicación)."""
        return {
            'enabled': cls.get_env_bool('SCHEDULED_TASKS_ENABLED', True),
            # Solo tareas esenciales: limpieza y respaldo
            'cleanup': {
                'enabled': cls.get_env_bool('CLEANUP_ENABLED', True),
                'time': cls.get_env_variable('CLEANUP_TIME', '03:15'),
            },
            'backup': {
                'enabled': cls.get_env_bool('BACKUP_ENABLED', True),
                'time': cls.get_env_variable('BACKUP_TIME', '02:30'),
            },
        }
    
    # =============================================================================
    # CONFIGURACIONES DE REPLICACIÓN (DEPRECADO - SE USA BASE DE DATOS ÚNICA NEON)
    # =============================================================================
    
    # =============================================================================
    # CONFIGURACIONES DE TÚNEL PÚBLICO
    # =============================================================================
    
    @classmethod
    def get_public_tunnel_config(cls) -> Dict[str, Any]:
        """Obtiene configuración de túnel público."""
        return {
            'enabled': cls.get_env_bool('PUBLIC_TUNNEL_ENABLED', False),
            'subdomain': cls.get_env_variable('PUBLIC_TUNNEL_SUBDOMAIN', '')
        }


# =============================================================================
# INSTANCIA GLOBAL PARA ACCESO FÁCIL
# =============================================================================

# Crear instancia global para importar fácilmente
config = SecureConfig()
