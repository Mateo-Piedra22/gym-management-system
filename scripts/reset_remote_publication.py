import json
from pathlib import Path
import psycopg2
from psycopg2 import sql
import os

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
    password = defaults.get('password')
    sslmode = defaults.get('sslmode') or 'require'
    appname = defaults.get('application_name') or 'gym_management_system'
    timeout = int(defaults.get('connect_timeout') or 10)
    if not dsn:
        return host, port, db, user, password, sslmode, appname, timeout
    try:
        from urllib.parse import urlparse, parse_qs
        u = urlparse(dsn)
        host = u.hostname or host
        port = int(u.port or port)
        db = (u.path or '').lstrip('/') or db
        user = u.username or user
        password = u.password or password
        q = parse_qs(u.query or '')
        sslmode = (q.get('sslmode') or [sslmode])[0]
        appname = (q.get('application_name') or [appname])[0]
        timeout = int((q.get('connect_timeout') or [timeout])[0])
    except Exception:
        pass
    return host, port, db, user, password, sslmode, appname, timeout


def _resolve_remote_credentials(cfg: dict):
    remote = cfg.get('db_remote') or {}
    host = remote.get('host') or ''
    port = int(remote.get('port') or 5432)
    db = remote.get('database') or 'railway'
    user = os.environ.get('PGREMOTE_USER') or remote.get('user') or 'postgres'
    sslmode = remote.get('sslmode') or 'require'
    appname = remote.get('application_name') or cfg.get('application_name') or 'gym_management_system'
    timeout = int(remote.get('connect_timeout') or cfg.get('connect_timeout') or 10)
    dsn = os.environ.get('PGREMOTE_DSN') or ''
    host, port, db, user, pw_from_dsn, sslmode, appname, timeout = _parse_dsn(
        dsn,
        {
            'host': host,
            'port': port,
            'database': db,
            'user': user,
            'password': remote.get('password'),
            'sslmode': sslmode,
            'application_name': appname,
            'connect_timeout': timeout,
        },
    )
    password = os.environ.get('PGREMOTE_PASSWORD') or pw_from_dsn or remote.get('password')
    if not password and keyring is not None:
        for account in (f"{user}@railway", f"{user}@{host}:{port}", user):
            try:
                password = keyring.get_password(KEYRING_SERVICE_NAME, account)
                if password:
                    break
            except Exception:
                password = None
    return {
        'host': host,
        'port': port,
        'database': db,
        'user': user,
        'password': password,
        'sslmode': sslmode,
        'application_name': appname,
        'connect_timeout': timeout,
        'dsn': dsn,
    }


def _connect(params: dict, dbname: str = None):
    dsn = params.get('dsn') or ''
    timeout = int(params.get('connect_timeout') or 10)
    if dsn:
        return psycopg2.connect(dsn, connect_timeout=timeout)
    return psycopg2.connect(
        host=params['host'], port=params['port'], dbname=dbname or params.get('database') or 'postgres',
        user=params['user'], password=params.get('password'), sslmode=params['sslmode'],
        application_name=params.get('application_name') or 'gym_management_system',
        connect_timeout=timeout,
    )


def run(pubname: str = 'gym_pub'):
    base_dir = Path(__file__).resolve().parent.parent
    cfg_path = base_dir / 'config' / 'config.json'
    sync_path = base_dir / 'config' / 'sync_tables.json'

    with open(cfg_path, 'r', encoding='utf-8') as f:
        cfg = json.load(f)
    remote = _resolve_remote_credentials(cfg)

    # Load desired tables from sync_tables.json
    try:
        with open(sync_path, 'r', encoding='utf-8') as f:
            sync_cfg = json.load(f) or {}
        desired_tables = list(sync_cfg.get('publishes_remote_to_local') or [])
    except Exception:
        desired_tables = []

    conn = None
    try:
        conn = _connect(remote)
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute("SELECT pubname, puballtables FROM pg_publication WHERE pubname = %s", (pubname,))
            row = cur.fetchone()
            if row:
                pub_all = bool(row[1])
                # Drop existing publication to switch away from FOR ALL TABLES
                cur.execute(sql.SQL("DROP PUBLICATION {}" ).format(sql.Identifier(pubname)))
                print(f"Dropped existing publication: {pubname} (FOR ALL TABLES={pub_all})")
            # Create new explicit publication for desired tables
            if desired_tables:
                # Filter by existing tables
                cur.execute(
                    """
                    SELECT table_name
                    FROM information_schema.tables
                    WHERE table_schema='public' AND table_type='BASE TABLE'
                    """
                )
                remote_tables = {r[0] for r in (cur.fetchall() or [])}
                pub_tables = [t for t in desired_tables if t in remote_tables]
                if pub_tables:
                    parts = [
                        sql.SQL("{}.{}" ).format(sql.Identifier('public'), sql.Identifier(t))
                        for t in pub_tables
                    ]
                    create_stmt = sql.SQL("CREATE PUBLICATION {} FOR TABLE ")\
                        .format(sql.Identifier(pubname)) + sql.SQL(", ").join(parts)
                    cur.execute(create_stmt)
                    print(f"Created publication {pubname} with {len(pub_tables)} tables")
                else:
                    # Fallback: no tables found -> publish all
                    cur.execute(sql.SQL("CREATE PUBLICATION {} FOR ALL TABLES").format(sql.Identifier(pubname)))
                    print(f"Created publication {pubname} FOR ALL TABLES (fallback)")
            else:
                # No desired list -> publish all
                cur.execute(sql.SQL("CREATE PUBLICATION {} FOR ALL TABLES").format(sql.Identifier(pubname)))
                print(f"Created publication {pubname} FOR ALL TABLES (no explicit list)")
    finally:
        try:
            if conn:
                conn.close()
        except Exception:
            pass


if __name__ == '__main__':
    run()