import os
import json
from pathlib import Path

import psycopg2
from psycopg2 import sql
from urllib.parse import urlparse, parse_qs

KEYRING_SERVICE_NAME = "GymMS_DB"


def _parse_dsn(dsn: str, defaults: dict):
    host = defaults.get('host')
    port = int(defaults.get('port') or 5432)
    db = defaults.get('database')
    user = defaults.get('user')
    password = defaults.get('password')
    sslmode = defaults.get('sslmode') or 'prefer'

    if not dsn:
        return host, port, db, user, password, sslmode
    try:
        u = urlparse(dsn)
        host = u.hostname or host
        port = int(u.port or port)
        db = (u.path or '').lstrip('/') or db
        user = u.username or user
        password = u.password or password
        q = parse_qs(u.query or '')
        sslmode = (q.get('sslmode') or [sslmode])[0]
    except Exception:
        pass
    return host, port, db, user, password, sslmode


def _terminate_connections(conn, target_db: str):
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT pg_terminate_backend(pid)
            FROM pg_stat_activity
            WHERE datname = %s AND pid <> pg_backend_pid()
            """,
            (target_db,),
        )


def _drop_create_database(conn, db_name: str, owner: str):
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute(sql.SQL("DROP DATABASE IF EXISTS {};").format(sql.Identifier(db_name)))
        cur.execute(
            sql.SQL("CREATE DATABASE {} WITH OWNER {} TEMPLATE template0;").format(
                sql.Identifier(db_name), sql.Identifier(owner)
            )
        )


def run():
    base_dir = Path(__file__).resolve().parent.parent
    cfg_path = base_dir / 'config' / 'config.json'
    with open(cfg_path, 'r', encoding='utf-8') as f:
        cfg = json.load(f)

    local = cfg.get('db_local') or {}
    host = local.get('host') or cfg.get('host') or 'localhost'
    port = int(local.get('port') or cfg.get('port') or 5432)
    db = local.get('database') or cfg.get('database') or 'gimnasio'
    user = local.get('user') or cfg.get('user') or 'postgres'
    sslmode = local.get('sslmode') or cfg.get('sslmode') or 'prefer'
    timeout = int(local.get('connect_timeout') or cfg.get('connect_timeout') or 10)

    dsn = os.environ.get('PGLOCAL_DSN') or ''
    host, port, db, user, password, sslmode = _parse_dsn(
        dsn, {"host": host, "port": port, "database": db, "user": user, "password": local.get('password'), "sslmode": sslmode}
    )

    password = os.environ.get('PGLOCAL_PASSWORD') or password or local.get('password')

    print(
        f"Reset local: host={host} port={port} db={db} user={user} sslmode={sslmode} password={'SI' if password else 'NO'}"
    )

    # Conectarse al DB 'postgres' para realizar DROP/CREATE
    admin_conn = None
    try:
        admin_conn = psycopg2.connect(
            host=host, port=port, dbname='postgres', user=user, password=password, sslmode=sslmode, connect_timeout=timeout
        )
        admin_conn.autocommit = True
        _terminate_connections(admin_conn, db)
        _drop_create_database(admin_conn, db, user)
        print('Reset local por DROP/CREATE: OK')
    except Exception as e:
        print(f"Reset local por DROP/CREATE: FALLÓ ({e})")
    finally:
        try:
            if admin_conn:
                admin_conn.close()
        except Exception:
            pass


if __name__ == '__main__':
    run()