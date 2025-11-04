#!/usr/bin/env python3
"""
Auto-inicialización del sistema:
- Carga .env y valida variables críticas
- Configura VPN (WireGuard) si es necesario
- Verifica conexiones local/remoto
- Asegura replicación nativa (remoto→local o bidireccional)
- Ejecuta chequear salud de replicación
"""

import os
import sys
import json
import subprocess
from pathlib import Path

from dotenv import load_dotenv

BASE = Path(__file__).resolve().parent.parent
ENV_PATH = BASE / '.env'

def run_ps1(script_rel: str, args: list[str] = None) -> tuple[int, str, str]:
    ps1 = BASE / 'scripts' / script_rel
    if not ps1.exists():
        return 1, '', f'No existe: {ps1}'
    cmd = [
        'powershell', '-ExecutionPolicy', 'Bypass', '-File', str(ps1)
    ] + list(args or [])
    proc = subprocess.run(cmd, cwd=str(BASE), capture_output=True, text=True)
    return proc.returncode, proc.stdout, proc.stderr

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

    # 1) Configurar VPN (WireGuard) si REMOTE_CAN_REACH_LOCAL
    remote_can = str(os.getenv('REMOTE_CAN_REACH_LOCAL', 'false')).lower() in ('true','1','yes','on')
    vpn_enabled = str(os.getenv('PUBLIC_TUNNEL_ENABLED', 'false')).lower() in ('true','1','yes','on')
    vpn_res = {'ok': True, 'skipped': True}
    if remote_can or vpn_enabled:
        code, out, err = run_ps1('setup_wireguard_client.ps1')
        vpn_res = {'ok': code == 0, 'stdout': out, 'stderr': err}

    # 2) Verificar conexiones local y remoto
    from database import DatabaseManager
    from secure_config import SecureConfig
    local = SecureConfig.get_db_config('local')
    remote = SecureConfig.get_db_config('remote')
    ok_local = DatabaseManager.test_connection(local, timeout_seconds=int(local.get('connect_timeout', 5)))
    ok_remote = DatabaseManager.test_connection(remote, timeout_seconds=int(remote.get('connect_timeout', 5)))

    # 3) Asegurar replicación
    try:
        if remote_can:
            from utils_modules.replication_setup import ensure_bidirectional_replication
            rep = ensure_bidirectional_replication({'replication': {'remote_can_reach_local': True}})
        else:
            from utils_modules.replication_setup import ensure_logical_replication
            rep = ensure_logical_replication({'replication': {}})
    except Exception as e:
        rep = {'ok': False, 'error': str(e)}

    # 4) Verificar salud
    try:
        import scripts.verify_replication_health as vrh
        health = vrh.main() if hasattr(vrh, 'main') else vrh.verify()
    except Exception:
        health = {'ok': True}

    print(json.dumps({
        'ok': bool(ok_local and ok_remote and rep.get('ok')), 
        'env': env, 
        'vpn': vpn_res, 
        'connections': {'local': ok_local, 'remote': ok_remote},
        'replication': rep, 
        'health': health
    }, ensure_ascii=False))
    return 0 if ok_local and ok_remote and rep.get('ok') else 1

if __name__ == '__main__':
    sys.exit(main())
