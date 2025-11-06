#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Consulta de parámetros de PostgreSQL para base de datos única Neon.

Salida JSON con claves:
- ok: bool general
- local: { settings, server_info }

Nota: Replicación deshabilitada - se usa base de datos única Neon.
Evita problemas de decodificación usando psycopg2 y RealDictCursor.
"""

import json
import os
import sys
from typing import Any, Dict, List

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, BASE_DIR)

import psycopg2  # type: ignore
import psycopg2.extras  # type: ignore

from secure_config import config as secure_config  # type: ignore


PARAM_KEYS: List[str] = [
    'wal_level',
    'track_commit_timestamp',
    'max_worker_processes',
    # Parámetros de replicación eliminados - se usa base de datos única Neon
    # 'max_wal_senders',
    # 'max_replication_slots',
    # 'max_logical_replication_workers',
    # 'logical_decoding_work_mem',
]


def _connect():
    cfg = secure_config.get_db_config('local')
    # psycopg2 admite kwargs directos
    return psycopg2.connect(
        host=cfg.get('host'),
        port=cfg.get('port'),
        dbname=cfg.get('database'),
        user=cfg.get('user'),
        password=cfg.get('password'),
        sslmode=cfg.get('sslmode'),
        application_name=cfg.get('application_name', 'gym_management_system'),
        connect_timeout=int(cfg.get('connect_timeout') or 10),
    )


def read_settings(conn) -> Dict[str, Any]:
    out: Dict[str, Any] = {'settings': {}, 'server_info': {}}
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    # Leer valores de pg_settings por clave
    try:
        sql = """
            SELECT name, setting
            FROM pg_settings
            WHERE name = ANY(%s)
        """
        cur.execute(sql, (PARAM_KEYS,))
        for row in cur.fetchall() or []:
            out['settings'][row['name']] = row['setting']
    except Exception:
        # Fallback a SHOW por clave
        for k in PARAM_KEYS:
            try:
                cur.execute(f"SHOW {k}")
                v = cur.fetchone()
                out['settings'][k] = v[0] if v else None
            except Exception:
                out['settings'][k] = None
    # Info de servidor
    try:
        cur.execute("SELECT version(), current_database(), current_user")
        v = cur.fetchone() or {}
        out['server_info'] = {
            'version': v.get('version') if isinstance(v, dict) else None,
            'database': v.get('current_database') if isinstance(v, dict) else None,
            'user': v.get('current_user') if isinstance(v, dict) else None,
        }
    except Exception:
        try:
            cur.execute("SELECT version()")
            out['server_info']['version'] = (cur.fetchone() or [None])[0]
        except Exception:
            pass
    return out


def main() -> int:
    result: Dict[str, Any] = {'ok': False, 'local': {}, 'error': None}
    try:
        # Única conexión (local/Neon)
        with _connect() as lconn:
            result['local'] = read_settings(lconn)
        result['ok'] = True
    except Exception as e:
        result['error'] = str(e)
        result['ok'] = False

    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0 if result['ok'] else 1


if __name__ == '__main__':
    sys.exit(main())

