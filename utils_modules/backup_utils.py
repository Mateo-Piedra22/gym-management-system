#!/usr/bin/env python3
"""
Utilidades de backup de base de datos (PostgreSQL) usando pg_dump.
- Prioriza variables de entorno y keyring para credenciales (no lee password de config.json).
- Genera archivo .db en 'backups' para compatibilidad con la UI.
"""
import os
import json
import subprocess
import shutil
from datetime import datetime
from pathlib import Path
from typing import Tuple

try:
    from config import KEYRING_SERVICE_NAME
except Exception:
    KEYRING_SERVICE_NAME = "GymMS_DB"

PROJECT_ROOT = Path(__file__).resolve().parent.parent
CONFIG_PATH = PROJECT_ROOT / 'config' / 'config.json'
BACKUPS_DIR = PROJECT_ROOT / 'backups'


def _load_config() -> dict:
    if CONFIG_PATH.exists():
        try:
            with open(CONFIG_PATH, 'r', encoding='utf-8') as f:
                return json.load(f) or {}
        except Exception:
            return {}
    return {}


def _build_local_params(cfg: dict) -> dict:
    node = cfg.get('db_connection') or cfg.get('db_local') or cfg
    # Preferir variables genéricas de entorno
    host = os.getenv('DB_HOST') or node.get('host') or cfg.get('host') or 'localhost'
    try:
        port_env = os.getenv('DB_PORT')
        port = int(port_env) if port_env else int(node.get('port') or cfg.get('port') or 5432)
    except Exception:
        port = 5432
    database = os.getenv('DB_NAME') or node.get('database') or cfg.get('database') or 'gimnasio'
    user = os.getenv('DB_USER') or node.get('user') or cfg.get('user') or 'postgres'

    # Password: solo desde entorno o almacén seguro
    password = (os.getenv('DB_PASSWORD') or os.getenv('DB_LOCAL_PASSWORD') or os.getenv('PGPASSWORD') or '')
    if not password:
        try:
            import keyring
            acct = f"{user}@{host}:{port}"
            saved_pwd = keyring.get_password(KEYRING_SERVICE_NAME, acct)
            if saved_pwd:
                password = saved_pwd
        except Exception:
            pass
    return {
        'host': host,
        'port': port,
        'dbname': database,
        'user': user,
        'password': password,
    }


def _resolve_pg_dump() -> str:
    try:
        p = shutil.which('pg_dump')
        if p:
            return p
    except Exception:
        pass
    candidate = Path("C:/Program Files/PostgreSQL/17/bin/pg_dump.exe")
    if candidate.exists():
        return str(candidate)
    return 'pg_dump'


def perform_quick_backup() -> Tuple[int, str]:
    """Ejecuta pg_dump para la base local y retorna (rc, ruta_salida)."""
    cfg = _load_config()
    params = _build_local_params(cfg)
    BACKUPS_DIR.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime('%Y%m%d_%H%M%S')
    fname = f"quick_backup_{ts}.db"
    out_path = BACKUPS_DIR / fname

    pg_dump = _resolve_pg_dump()
    # Si pg_dump no está disponible en el sistema, evitar fallos: saltar backup limpiamente.
    try:
        pg_dump_available = bool(shutil.which('pg_dump')) or Path(pg_dump).exists()
    except Exception:
        pg_dump_available = Path(pg_dump).exists()
    if not pg_dump_available:
        # No provocar errores en tareas programadas: indicar salto y éxito.
        return 0, "SKIPPED (pg_dump not found)"
    env = os.environ.copy()
    if params.get('password'):
        env['PGPASSWORD'] = params['password']

    args = [
        pg_dump,
        '-h', params['host'],
        '-p', str(params['port']),
        '-U', params['user'],
        '-d', params['dbname'],
        '-f', str(out_path),
    ]

    try:
        # En Windows, ejecutar pg_dump sin crear ventana de consola
        if os.name == 'nt':
            try:
                proc = subprocess.run(
                    args,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    text=True,
                    env=env,
                    creationflags=getattr(subprocess, 'CREATE_NO_WINDOW', 0)
                )
            except Exception:
                proc = subprocess.run(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, env=env)
        else:
            proc = subprocess.run(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, env=env)
        if proc.returncode == 0:
            return 0, str(out_path)
        else:
            return proc.returncode, str(out_path)
    except Exception:
        return 1, str(out_path)