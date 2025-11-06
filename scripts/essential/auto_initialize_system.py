#!/usr/bin/env python3
"""
Auto-inicialización del sistema:
- Carga .env y valida variables críticas
- Verifica conexiones local/remoto
- Sin replicación - se usa base de datos única Neon
"""

import os
import sys
import json
from pathlib import Path

from dotenv import load_dotenv

BASE = Path(__file__).resolve().parent.parent
ENV_PATH = BASE / '.env'

def main():
    if ENV_PATH.exists():
        load_dotenv(ENV_PATH)
    # Asegurar importaciones de paquetes del proyecto
    if str(BASE) not in sys.path:
        sys.path.append(str(BASE))

    from utils_modules.env_validation import validate_environment
    env = validate_environment()
    if not env.get('ok'):
        print(json.dumps({
            'ok': False,
            'step': 'env_validation',
            'missing': env.get('missing'),
        }, ensure_ascii=False))
        return 1

    # 1) Verificar conexión local (base única)
    from database import DatabaseManager
    from secure_config import SecureConfig
    local = SecureConfig.get_db_config('local')
    ok_local = DatabaseManager.test_connection(local, timeout_seconds=int(local.get('connect_timeout', 5)))
    
    # 2) Verificación de salud básica
    health = {'ok': True, 'message': 'Sistema inicializado con base de datos única'}

    print(json.dumps({
        'ok': bool(ok_local), 
        'env': env, 
        'vpn': {'ok': True, 'skipped': True, 'message': 'VPN deshabilitado - base de datos única Neon'},
        'connections': {'local': ok_local},
        'health': health
    }, ensure_ascii=False))
    return 0 if ok_local else 1

if __name__ == '__main__':
    sys.exit(main())