import os
import json
from pathlib import Path

import psycopg2

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

def apply_logical_settings(params: dict, slots: int, senders: int):
    conn = None
    try:
        conn = _connect(params, dbname='postgres')
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute("ALTER SYSTEM SET wal_level = 'logical'")
            cur.execute("ALTER SYSTEM SET max_replication_slots = %s", (slots,))
            cur.execute("ALTER SYSTEM SET max_wal_senders = %s", (senders,))
            try:
                cur.execute("SELECT pg_reload_conf()")
            except Exception:
                pass
        return {"ok": True}
    except Exception as e:
        return {"ok": False, "error": str(e)}
    finally:
        try:
            if conn:
                conn.close()
        except Exception:
            pass

def show_current_settings(params: dict):
    info = {"wal_level": None, "max_replication_slots": None, "max_wal_senders": None}
    conn = None
    try:
        conn = _connect(params)
        with conn.cursor() as cur:
            try:
                cur.execute("SHOW wal_level")
                info["wal_level"] = (cur.fetchone() or [None])[0]
            except Exception:
                info["wal_level"] = None
            try:
                cur.execute("SHOW max_replication_slots")
                info["max_replication_slots"] = (cur.fetchone() or [None])[0]
            except Exception:
                info["max_replication_slots"] = None
            try:
                cur.execute("SHOW max_wal_senders")
                info["max_wal_senders"] = (cur.fetchone() or [None])[0]
            except Exception:
                info["max_wal_senders"] = None
    except Exception:
        pass
    finally:
        try:
            if conn:
                conn.close()
        except Exception:
            pass
    return info

def run():
    base_dir = Path(__file__).resolve().parent.parent
    cfg_path = base_dir / 'config' / 'config.json'
    with open(cfg_path, 'r', encoding='utf-8') as f:
        cfg = json.load(f)
    remote = _resolve_remote_credentials(cfg)
    slots = int(os.environ.get('PGREMOTE_MAX_REPLICATION_SLOTS') or 2)
    senders = int(os.environ.get('PGREMOTE_MAX_WAL_SENDERS') or 2)
    res = apply_logical_settings(remote, slots, senders)
    if not res.get('ok'):
        print(f"Aplicación de parámetros remotos: FALLÓ ({res.get('error')})")
    else:
        print("Aplicación de parámetros remotos: OK")
    info = show_current_settings(remote)
    wl = info.get('wal_level')
    mrs = info.get('max_replication_slots')
    mws = info.get('max_wal_senders')
    print(f"wal_level={wl} slots={mrs} senders={mws}")
    print("Si wal_level no muestra 'logical', reinicia el servicio en Railway para aplicar ALTER SYSTEM.")

if __name__ == '__main__':
    run()