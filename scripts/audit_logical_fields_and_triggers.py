# -*- coding: utf-8 -*-
"""
Audita columnas lógicas, triggers y replica identity en tablas sincronizadas.

Salida: JSON con resultados por base (local y remota).

Uso:
  python scripts/audit_logical_fields_and_triggers.py

Requisitos:
  - psycopg2
  - config/config.json con db_local y db_remote
  - config/sync_tables.json con publishes_remote_to_local y uploads_local_to_remote
"""

import json
import os
from pathlib import Path
from typing import Dict, List, Tuple, Any

try:
    import psycopg2
    import psycopg2.extras
except Exception as e:
    raise SystemExit(json.dumps({"ok": False, "error": "psycopg2_missing"}, ensure_ascii=False))

PROJ_ROOT = Path(__file__).resolve().parent.parent
CONFIG_DIR = PROJ_ROOT / 'config'
CONFIG_JSON = CONFIG_DIR / 'config.json'
SYNC_TABLES_JSON = CONFIG_DIR / 'sync_tables.json'


def _read_json(path: Path) -> Dict[str, Any]:
    try:
        with open(path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception:
        return {}


def _resolve_conn(profile: str, cfg: Dict[str, Any]) -> Dict[str, Any]:
    params = (cfg.get(f'db_{profile}') or {})
    return {
        'host': params.get('host') or 'localhost',
        'port': int(params.get('port') or (5432 if profile == 'local' else 5432)),
        'dbname': params.get('database') or ('gimnasio' if profile == 'local' else 'railway'),
        'user': params.get('user') or 'postgres',
        'password': params.get('password') or None,
        'sslmode': params.get('sslmode') or ('prefer' if profile == 'local' else 'require'),
        'application_name': params.get('application_name') or 'audit_logical_fields',
    }


def _connect(params: Dict[str, Any]):
    return psycopg2.connect(
        host=params['host'], port=params['port'], dbname=params['dbname'],
        user=params['user'], password=params.get('password'), sslmode=params.get('sslmode') or 'prefer',
        application_name=params.get('application_name') or 'audit_logical_fields',
        connect_timeout=10,
    )


def _load_tables() -> Tuple[List[str], List[str]]:
    data = _read_json(SYNC_TABLES_JSON)
    pubs = [t.strip() for t in (data.get('publishes_remote_to_local') or []) if isinstance(t, str) and t.strip()]
    uploads = [t.strip() for t in (data.get('uploads_local_to_remote') or []) if isinstance(t, str) and t.strip()]
    # Unificar listas, deduplicar y mantener orden básico
    seen = set()
    union = []
    for t in pubs + uploads:
        if t and t not in seen:
            seen.add(t)
            union.append(t)
    return union, uploads


def _table_exists(conn, schema: str, table: str) -> bool:
    with conn.cursor() as cur:
        cur.execute("SELECT to_regclass(%s)", (f"{schema}.{table}",))
        reg = cur.fetchone()
        return bool(reg and reg[0] is not None)


def _has_columns(conn, schema: str, table: str, cols: List[str]) -> Dict[str, bool]:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT column_name FROM information_schema.columns WHERE table_schema=%s AND table_name=%s",
            (schema, table),
        )
        names = {r[0] for r in cur.fetchall() or []}
    return {c: (c in names) for c in cols}


def _trigger_names(conn, schema: str, table: str) -> List[str]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT t.tgname
            FROM pg_trigger t
            JOIN pg_class c ON c.oid = t.tgrelid
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname = %s AND c.relname = %s AND NOT t.tgisinternal
            """,
            (schema, table),
        )
        return [r[0] for r in cur.fetchall() or []]


def _replica_identity(conn, schema: str, table: str) -> str:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT c.relreplident
            FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname=%s AND c.relname=%s
            """,
            (schema, table),
        )
        r = cur.fetchone()
        return (r[0] if r else None) or ''


def _has_primary_key(conn, schema: str, table: str) -> bool:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT EXISTS (
              SELECT 1
              FROM pg_index i
              JOIN pg_class c ON c.oid = i.indrelid
              JOIN pg_namespace n ON n.oid = c.relnamespace
              WHERE n.nspname=%s AND c.relname=%s AND i.indisprimary
            )
            """,
            (schema, table),
        )
        return bool(cur.fetchone() and cur.fetchone()[0])


def _audit_db(conn, name: str, tables: List[str], uploads: List[str], is_local: bool) -> Dict[str, Any]:
    schema = 'public'
    res = {
        'ok': True,
        'schema': schema,
        'tables_checked': 0,
        'missing': {
            'logical_ts': [],
            'last_op_id': [],
        },
        'triggers_missing': {
            'ensure_logical_fields': [],
            'outbox_ins': [],
            'outbox_upd': [],
            'outbox_del': [],
        },
        'replica_identity_full': [],
        'no_primary_key': [],
        'not_found': [],
    }
    try:
        for t in tables:
            if not _table_exists(conn, schema, t):
                res['not_found'].append(t)
                continue
            res['tables_checked'] += 1
            cols = _has_columns(conn, schema, t, ['logical_ts', 'last_op_id'])
            if not cols.get('logical_ts'): res['missing']['logical_ts'].append(t)
            if not cols.get('last_op_id'): res['missing']['last_op_id'].append(t)

            triggers = _trigger_names(conn, schema, t)
            # ensure_logical_fields trigger name per migración
            expected_ensure = f"trg_{t}_ensure_logical_fields"
            if expected_ensure not in triggers:
                res['triggers_missing']['ensure_logical_fields'].append(t)
            # outbox triggers: solo para tablas en uploads_local_to_remote y solo en LOCAL
            if is_local and (t in uploads):
                if 'sync_outbox_ins' not in triggers:
                    res['triggers_missing']['outbox_ins'].append(t)
                if 'sync_outbox_upd' not in triggers:
                    res['triggers_missing']['outbox_upd'].append(t)
                if 'sync_outbox_del' not in triggers:
                    res['triggers_missing']['outbox_del'].append(t)

            replident = _replica_identity(conn, schema, t)
            if replident == 'f':
                res['replica_identity_full'].append(t)

            # PK presence
            has_pk = False
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT EXISTS (
                      SELECT 1
                      FROM pg_index i
                      JOIN pg_class c ON c.oid = i.indrelid
                      JOIN pg_namespace n ON n.oid = c.relnamespace
                      WHERE n.nspname=%s AND c.relname=%s AND i.indisprimary
                    )
                    """,
                    (schema, t),
                )
                has_pk = bool(cur.fetchone()[0])
            if not has_pk:
                res['no_primary_key'].append(t)
        return res
    except Exception as e:
        return {'ok': False, 'error': str(e)}


def main():
    cfg = _read_json(CONFIG_JSON)
    tables, uploads = _load_tables()
    if not tables:
        print(json.dumps({"ok": False, "error": "no_tables_in_sync_config"}, ensure_ascii=False))
        return 2

    # Conexiones
    local_params = _resolve_conn('local', cfg)
    remote_params = _resolve_conn('remote', cfg)
    local_conn = _connect(local_params)
    remote_conn = _connect(remote_params)

    try:
        local_res = _audit_db(local_conn, 'local', tables, uploads, True)
        remote_res = _audit_db(remote_conn, 'remote', tables, uploads, False)
        output = {
            'ok': bool(local_res.get('ok') and remote_res.get('ok')),
            'local': local_res,
            'remote': remote_res,
        }
        print(json.dumps(output, ensure_ascii=False, indent=2))
        return 0
    finally:
        try:
            local_conn.close()
        except Exception:
            pass
        try:
            remote_conn.close()
        except Exception:
            pass


if __name__ == '__main__':
    raise SystemExit(main())