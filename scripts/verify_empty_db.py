import os
import json
from pathlib import Path

import psycopg2
from urllib.parse import urlparse, parse_qs


SYSTEM_SCHEMAS = {
    'pg_catalog', 'information_schema', 'pg_toast'
}


def _parse_dsn(dsn: str):
    host = port = db = user = password = sslmode = None
    if not dsn:
        return host, port, db, user, password, sslmode
    try:
        u = urlparse(dsn)
        host = u.hostname
        port = int(u.port) if u.port else None
        db = (u.path or '').lstrip('/') or None
        user = u.username
        password = u.password
        q = parse_qs(u.query or '')
        sslmode = (q.get('sslmode') or [None])[0]
    except Exception:
        pass
    return host, port, db, user, password, sslmode


def _load_profile(cfg, profile: str):
    node = cfg.get('db_local') if profile == 'local' else cfg.get('db_remote')
    return node or {}


def run():
    base_dir = Path(__file__).resolve().parent.parent
    cfg_path = base_dir / 'config' / 'config.json'
    with open(cfg_path, 'r', encoding='utf-8') as f:
        cfg = json.load(f)

    profile = os.environ.get('PGVERIFY_PROFILE') or 'remote'
    dsn = os.environ.get('PGVERIFY_DSN') or ''
    node = _load_profile(cfg, profile)

    host = node.get('host') or cfg.get('host')
    port = int(node.get('port') or cfg.get('port') or 5432)
    db = node.get('database') or cfg.get('database')
    user = node.get('user') or cfg.get('user')
    password = node.get('password') or None
    sslmode = node.get('sslmode') or cfg.get('sslmode')
    timeout = int(node.get('connect_timeout') or cfg.get('connect_timeout') or 10)

    dh, dp, dd, du, dpw, dsm = _parse_dsn(dsn)
    host = dh or host
    port = dp or port
    db = dd or db
    user = du or user
    password = os.environ.get('PGVERIFY_PASSWORD') or dpw or password
    sslmode = dsm or sslmode or 'prefer'

    print(f"Verificando base '{profile}': host={host} port={port} db={db} user={user} sslmode={sslmode} password={'SI' if password else 'NO'}")

    conn = None
    try:
        if dsn:
            conn = psycopg2.connect(dsn, connect_timeout=timeout)
        else:
            conn = psycopg2.connect(host=host, port=port, dbname=db, user=user, password=password, sslmode=sslmode, connect_timeout=timeout)
        with conn.cursor() as cur:
            cur.execute(
                """
                WITH objs AS (
                  SELECT 'table' AS kind, n.nspname AS schema_name, c.relname AS obj_name
                  FROM pg_class c
                  JOIN pg_namespace n ON n.oid = c.relnamespace
                  WHERE n.nspname NOT IN ('pg_catalog','information_schema','pg_toast')
                    AND n.nspname NOT LIKE 'pg_%'
                    AND c.relkind IN ('r','p','v','m')
                  UNION ALL
                  SELECT 'sequence', sequence_schema, sequence_name
                  FROM information_schema.sequences
                  WHERE sequence_schema NOT IN ('pg_catalog','information_schema','pg_toast')
                    AND sequence_schema NOT LIKE 'pg_%'
                  UNION ALL
                  SELECT 'function', n.nspname, p.proname || '(' || pg_get_function_identity_arguments(p.oid) || ')'
                  FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace
                  WHERE n.nspname NOT IN ('pg_catalog','information_schema','pg_toast')
                    AND n.nspname NOT LIKE 'pg_%'
                  UNION ALL
                  SELECT 'trigger', n.nspname, t.tgname
                  FROM pg_trigger t JOIN pg_class c ON c.oid = t.tgrelid JOIN pg_namespace n ON n.oid = c.relnamespace
                  WHERE NOT t.tgisinternal
                    AND n.nspname NOT IN ('pg_catalog','information_schema','pg_toast')
                    AND n.nspname NOT LIKE 'pg_%'
                )
                SELECT * FROM objs ORDER BY kind, schema_name, obj_name;
                """
            )
            rows = cur.fetchall()
            if not rows:
                print('Base vacía: OK')
            else:
                print('Objetos de usuario encontrados:')
                for row in rows:
                    print(row)
    except Exception as e:
        print(f"Verificación: FALLÓ ({e})")
    finally:
        try:
            if conn:
                conn.close()
        except Exception:
            pass


if __name__ == '__main__':
    run()