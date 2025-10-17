import os
import json
from pathlib import Path

import psycopg2
from psycopg2 import sql
from urllib.parse import urlparse, parse_qs

try:
    import keyring
except Exception:
    keyring = None

KEYRING_SERVICE_NAME = "GymMS_DB"


def _parse_dsn(dsn: str, defaults: dict):
    host = defaults.get('host')
    port = int(defaults.get('port') or 5432)
    db = defaults.get('database')
    user = defaults.get('user')
    password = None
    sslmode = defaults.get('sslmode') or 'require'

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


def _admin_dsn_from_dsn(dsn: str) -> str:
    try:
        u = urlparse(dsn)
        # Cambiar path a /postgres para operaciones administrativas
        admin_url = u._replace(path='/postgres')
        return admin_url.geturl()
    except Exception:
        return ''


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


def _wipe_inside_database(host, port, db, user, password, sslmode, timeout):
    # Fallback: si no podemos acceder a 'postgres', limpiar desde dentro del DB
    conn = psycopg2.connect(
        host=host,
        port=port,
        dbname=db,
        user=user,
        password=password,
        sslmode=sslmode,
        connect_timeout=timeout,
    )
    conn.autocommit = True
    try:
        with conn.cursor() as cur:
            # Eliminar extensiones de usuario
            cur.execute("SELECT extname FROM pg_extension WHERE extname NOT IN ('plpgsql');")
            exts = [row[0] for row in cur.fetchall()]
            for ext in exts:
                cur.execute(sql.SQL("DROP EXTENSION IF EXISTS {} CASCADE;").format(sql.Identifier(ext)))

            # Eliminar esquemas de usuario (incluye public)
            cur.execute(
                """
                SELECT nspname FROM pg_namespace
                WHERE nspname NOT IN ('pg_catalog','information_schema','pg_toast')
                  AND nspname NOT LIKE 'pg_%'
                """
            )
            schemas = [row[0] for row in cur.fetchall()]
            for s in schemas:
                cur.execute(sql.SQL("DROP SCHEMA IF EXISTS {} CASCADE;").format(sql.Identifier(s)))

            # Recrear public y otorgar permisos básicos
            cur.execute("CREATE SCHEMA IF NOT EXISTS public;")
            cur.execute(sql.SQL("ALTER SCHEMA public OWNER TO {};").format(sql.Identifier(user)))
            cur.execute("GRANT ALL ON SCHEMA public TO public;")
    finally:
        try:
            conn.close()
        except Exception:
            pass


def run():
    base_dir = Path(__file__).resolve().parent.parent
    cfg_path = base_dir / 'config' / 'config.json'
    with open(cfg_path, 'r', encoding='utf-8') as f:
        cfg = json.load(f)

    remote = cfg.get('db_remote') or {}
    host = remote.get('host') or ''
    port = int(remote.get('port') or 5432)
    db = remote.get('database') or 'railway'
    user = remote.get('user') or 'postgres'
    sslmode = remote.get('sslmode') or 'require'
    timeout = int(remote.get('connect_timeout') or cfg.get('connect_timeout') or 10)

    dsn = os.environ.get('PGREMOTE_DSN') or ''
    host, port, db, user, pw_from_dsn, sslmode = _parse_dsn(
        dsn, {"host": host, "port": port, "database": db, "user": user, "sslmode": sslmode}
    )

    password = os.environ.get('PGREMOTE_PASSWORD') or pw_from_dsn
    if not password and keyring is not None:
        try:
            password = keyring.get_password(KEYRING_SERVICE_NAME, user)
        except Exception:
            password = None

    print(
        f"Reset remoto: host={host} port={port} db={db} user={user} sslmode={sslmode} password={'SI' if password else 'NO'}"
    )

    # Primero intentar vía DB 'postgres'
    admin_conn = None
    try:
        if dsn:
            admin_dsn = _admin_dsn_from_dsn(dsn)
            if admin_dsn:
                admin_conn = psycopg2.connect(admin_dsn, connect_timeout=timeout)
            else:
                # Sin DSN válido, caer a parámetros directos
                admin_conn = psycopg2.connect(
                    host=host, port=port, dbname='postgres', user=user, password=password, sslmode=sslmode, connect_timeout=timeout
                )
        else:
            admin_conn = psycopg2.connect(
                host=host, port=port, dbname='postgres', user=user, password=password, sslmode=sslmode, connect_timeout=timeout
            )
        admin_conn.autocommit = True
        _terminate_connections(admin_conn, db)
        _drop_create_database(admin_conn, db, user)
        print('Reset remoto por DROP/CREATE: OK')
    except Exception as e:
        print(f"Reset remoto por DROP/CREATE: FALLÓ ({e}). Intentando limpieza in-place...")
        try:
            _wipe_inside_database(host, port, db, user, password, sslmode, timeout)
            print('Reset remoto in-place: OK')
        except Exception as e2:
            print(f"Reset remoto in-place: FALLÓ ({e2})")
    finally:
        try:
            if admin_conn:
                admin_conn.close()
        except Exception:
            pass


if __name__ == '__main__':
    run()