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
        Obtiene configuración de base de datos según el perfil.
        
        Args:
            profile: 'local' o 'remote'. Si es None, usa DB_PROFILE
            
        Returns:
            Diccionario con configuración de base de datos
        """
        if profile is None:
            profile = cls.get_env_variable('DB_PROFILE', 'local')
        
        if profile == 'local':
            return {
                'host': cls.get_env_variable('DB_LOCAL_HOST', 'localhost'),
                'port': cls.get_env_int('DB_LOCAL_PORT', 5432),
                'database': cls.get_env_variable('DB_LOCAL_DATABASE', 'gimnasio'),
                'user': cls.get_env_variable('DB_LOCAL_USER', 'postgres'),
                'password': cls.get_env_variable('DB_LOCAL_PASSWORD', required=True),
                'sslmode': cls.get_env_variable('DB_LOCAL_SSLMODE', 'prefer'),
                'connect_timeout': cls.get_env_int('DB_LOCAL_CONNECT_TIMEOUT', 10),
                'application_name': cls.get_env_variable('DB_LOCAL_APPLICATION_NAME', 'gym_management_system')
            }
        elif profile == 'remote':
            return {
                'host': cls.get_env_variable('DB_REMOTE_HOST', required=True),
                'port': cls.get_env_int('DB_REMOTE_PORT', required=True),
                'database': cls.get_env_variable('DB_REMOTE_DATABASE', required=True),
                'user': cls.get_env_variable('DB_REMOTE_USER', required=True),
                'password': cls.get_env_variable('DB_REMOTE_PASSWORD', required=True),
                'sslmode': cls.get_env_variable('DB_REMOTE_SSLMODE', 'require'),
                'connect_timeout': cls.get_env_int('DB_REMOTE_CONNECT_TIMEOUT', 10),
                'application_name': cls.get_env_variable('DB_REMOTE_APPLICATION_NAME', 'gym_management_system')
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
    def get_sync_upload_token(cls) -> str:
        """Obtiene el token de sincronización."""
        return cls.get_env_variable('SYNC_UPLOAD_TOKEN', required=True)
    
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
        """Obtiene configuración de tareas programadas."""
        return {
            'enabled': cls.get_env_bool('SCHEDULED_TASKS_ENABLED', True),
            'uploader': {
                'enabled': cls.get_env_bool('UPLOADER_ENABLED', True),
                'interval_minutes': cls.get_env_int('UPLOADER_INTERVAL_MINUTES', 3)
            },
            'reconcile_r2l': {
                'enabled': cls.get_env_bool('RECONCILE_R2L_ENABLED', True)
            }
        }
    
    # =============================================================================
    # CONFIGURACIONES DE REPLICACIÓN
    # =============================================================================
    
    @classmethod
    def get_replication_config(cls) -> Dict[str, Any]:
        """Obtiene configuración de replicación."""
        return {
            'subscription_name': cls.get_env_variable('REPLICATION_SUBSCRIPTION_NAME', 'gym_sub'),
            'publication_name': cls.get_env_variable('REPLICATION_PUBLICATION_NAME', 'gym_pub'),
            'remote_can_reach_local': cls.get_env_bool('REMOTE_CAN_REACH_LOCAL', False)
        }
    
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