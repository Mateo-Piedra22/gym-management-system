import os
import logging
import base64
import json
from typing import Any, Dict
from .security_utils import SecurityUtils

class SecureConfig:
    @classmethod
    def get_env_variable(cls, key: str, default: Any = None, required: bool = False) -> Any:
        val = os.getenv(key)
        if val is None:
            if required:
                raise RuntimeError(f"Missing required environment variable: {key}")
            return default
        return val

    @classmethod
    def get_env_bool(cls, key: str, default: bool = False) -> bool:
        val = os.getenv(key)
        if val is None:
            return default
        s = str(val).strip().lower()
        return s in ("1", "true", "yes", "on")

    @classmethod
    def get_env_int(cls, key: str, default: int = 0) -> int:
        val = os.getenv(key)
        if val is None:
            return default
        try:
            return int(str(val).strip())
        except Exception:
            return default

    @classmethod
    def get_env_float(cls, key: str, default: float = 0.0) -> float:
        val = os.getenv(key)
        if val is None:
            return default
        try:
            return float(str(val).strip())
        except Exception:
            return default

    @classmethod
    def get_db_config(cls, profile: str | None = None) -> Dict[str, Any]:
        return {
            'host': cls.get_env_variable('DB_HOST', 'localhost'),
            'port': cls.get_env_int('DB_PORT', 5432),
            'user': cls.get_env_variable('DB_USER', 'postgres'),
            'password': cls.get_env_variable('DB_PASSWORD', ''),
            'database': cls.get_env_variable('DB_NAME', 'gymdb'),
            'sslmode': cls.get_env_variable('DB_SSLMODE', 'prefer'),
        }

    @classmethod
    def get_whatsapp_access_token(cls) -> str | None:
        val = cls.get_env_variable('WHATSAPP_ACCESS_TOKEN', required=False)
        if val:
            return val
        try:
            b64 = cls.get_env_variable('WHATSAPP_ACCESS_TOKEN_B64', required=False)
            if b64:
                return base64.b64decode(b64).decode('utf-8')
        except Exception:
            pass
        return None

    @classmethod
    def get_dev_password(cls) -> str:
        val = cls.get_env_variable('DEV_PASSWORD', required=False)
        if val:
            return str(val)
        return ''

    @classmethod
    def get_login_attempts_limit(cls) -> int:
        return cls.get_env_int('LOGIN_ATTEMPTS_LIMIT', 5)

    @classmethod
    def get_login_window_minutes(cls) -> int:
        return cls.get_env_int('LOGIN_WINDOW_MINUTES', 15)

    @classmethod
    def verify_owner_password(cls, password: str) -> bool:
        try:
            stored_hash = cls.get_env_variable('DEV_PASSWORD_HASH', required=False)
        except Exception:
            stored_hash = None
        if not stored_hash:
            stored_password = cls.get_env_variable('DEV_PASSWORD', required=False)
            return stored_password and password == stored_password
        return SecurityUtils.verify_password(password, stored_hash)

    @classmethod
    def get_webapp_base_url(cls) -> str:
        val = cls.get_env_variable('WEBAPP_BASE_URL', required=False)
        if isinstance(val, str) and val.strip():
            return val.strip()
        vercel = (
            os.getenv('VERCEL_URL') or os.getenv('VERCEL_BRANCH_URL') or os.getenv('VERCEL_PROJECT_PRODUCTION_URL') or ''
        ).strip()
        if vercel:
            if vercel.startswith('http://') or vercel.startswith('https://'):
                return vercel
            return f"https://{vercel}"
        return ''

    @classmethod
    def get_client_base_url(cls) -> str:
        return cls.get_env_variable('CLIENT_BASE_URL', '')

    @classmethod
    def get_server_public_ip(cls) -> str:
        return cls.get_env_variable('SERVER_PUBLIC_IP', required=True)

    @classmethod
    def get_scheduled_tasks_config(cls) -> Dict[str, Any]:
        return {
            'enabled': cls.get_env_bool('SCHEDULED_TASKS_ENABLED', True),
            'cleanup': {
                'enabled': cls.get_env_bool('CLEANUP_ENABLED', True),
                'time': cls.get_env_variable('CLEANUP_TIME', '03:15'),
            },
            'backup': {
                'enabled': cls.get_env_bool('BACKUP_ENABLED', True),
                'time': cls.get_env_variable('BACKUP_TIME', '02:30'),
            },
        }

    @classmethod
    def get_public_tunnel_config(cls) -> Dict[str, Any]:
        return {
            'enabled': cls.get_env_bool('PUBLIC_TUNNEL_ENABLED', False),
            'subdomain': cls.get_env_variable('PUBLIC_TUNNEL_SUBDOMAIN', '')
        }

config = SecureConfig