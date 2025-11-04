#!/usr/bin/env python3
"""
Verificador de configuración de entorno (.env) para el sistema
- Comprueba variables requeridas (local/remoto)
- Valida conexiones básicas
"""

import json
from dotenv import load_dotenv
from pathlib import Path
import sys

def main():
    base = Path(__file__).resolve().parent.parent
    env_path = base / '.env'
    if env_path.exists():
        load_dotenv(env_path)
    # Asegurar importaciones relativas al proyecto
    if str(base) not in sys.path:
        sys.path.append(str(base))
    from utils_modules.env_validation import validate_environment
    env = validate_environment()
    from database import DatabaseManager
    ok_local = DatabaseManager.test_connection(None, timeout_seconds=5)
    result = {
        'ok': bool(env.get('ok') and ok_local),
        'env': env,
        'local_connection': ok_local,
    }
    print(json.dumps(result, ensure_ascii=False))
    return 0 if result['ok'] else 1

if __name__ == '__main__':
    import sys
    sys.exit(main())
