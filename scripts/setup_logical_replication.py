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
    password = defaults.get('password')
    sslmode = defaults.get('sslmode') or 'require'
    appname = defaults.get('application_name') or 'gym_management_system'
    timeout = int(defaults.get('connect_timeout') or 10)

    if not dsn:
        return host, port, db, user, password, sslmode, appname, timeout
    try:
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
        dsn, {
            'host': host,
            'port': port,
            'database': db,
            'user': user,
            'password': remote.get('password'),
            'sslmode': sslmode,
            'application_name': appname,
            'connect_timeout': timeout,
        }
    )

    # Orden: ENV > DSN > CONFIG > KEYRING (con variantes)
    password = os.environ.get('PGREMOTE_PASSWORD') or pw_from_dsn or remote.get('password')
    if not password and keyring is not None:
        # Probar variantes de cuenta para distinguir entornos
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


def _resolve_local_credentials(cfg: dict):
    local = cfg.get('db_local') or cfg
    host = local.get('host') or cfg.get('host') or 'localhost'
    port = int(local.get('port') or cfg.get('port') or 5432)
    db = local.get('database') or cfg.get('database') or 'gimnasio'
    user = local.get('user') or cfg.get('user') or 'postgres'
    password = os.environ.get('PGLOCAL_PASSWORD') or local.get('password') or cfg.get('password')
    sslmode = local.get('sslmode') or cfg.get('sslmode') or 'prefer'
    appname = local.get('application_name') or cfg.get('application_name') or 'gym_management_system'
    timeout = int(local.get('connect_timeout') or cfg.get('connect_timeout') or 10)

    dsn = os.environ.get('PGLOCAL_DSN') or ''
    host, port, db, user, pw_from_dsn, sslmode, appname, timeout = _parse_dsn(
        dsn, {
            'host': host,
            'port': port,
            'database': db,
            'user': user,
            'password': password,
            'sslmode': sslmode,
            'application_name': appname,
            'connect_timeout': timeout,
        }
    )
    password = os.environ.get('PGLOCAL_PASSWORD') or pw_from_dsn or password

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
    """Devuelve una conexión psycopg2 usando parámetros directos o DSN."""
    dbname = dbname or params.get('database')
    dsn = params.get('dsn') or ''
    timeout = int(params.get('connect_timeout') or 10)
    if dsn:
        return psycopg2.connect(dsn, connect_timeout=timeout)
    return psycopg2.connect(
        host=params['host'], port=params['port'], dbname=dbname,
        user=params['user'], password=params.get('password'), sslmode=params['sslmode'],
        application_name=params.get('application_name') or 'gym_management_system',
        connect_timeout=timeout,
    )


def _ensure_publication_on_remote(remote_params: dict, pubname: str = 'gym_pub'):
    conn = None
    try:
        conn = _connect(remote_params)
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute("SELECT 1 FROM pg_publication WHERE pubname = %s", (pubname,))
            exists = bool(cur.fetchone())
            if not exists:
                # Publicación mínima: todas las tablas
                cur.execute(sql.SQL("CREATE PUBLICATION {} FOR ALL TABLES").format(sql.Identifier(pubname)))
                print(f"Publicación creada en remoto: {pubname}")
            else:
                print(f"Publicación ya existe en remoto: {pubname}")
    finally:
        try:
            if conn:
                conn.close()
        except Exception:
            pass


def _ensure_subscription_on_local(local_params: dict, remote_params: dict, subname: str = 'gym_sub', pubname: str = 'gym_pub'):
    conn = None
    try:
        conn = _connect(local_params)
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute("SELECT 1 FROM pg_subscription WHERE subname = %s", (subname,))
            exists = bool(cur.fetchone())

            # Construir cadena de conexión para la suscripción (DSN estilo libpq)
            rpwd = remote_params.get('password') or ''
            rdsn = remote_params.get('dsn') or ''
            if rdsn:
                sub_conn_str = rdsn
            else:
                parts = [
                    f"host={remote_params['host']}",
                    f"port={remote_params['port']}",
                    f"dbname={remote_params['database']}",
                    f"user={remote_params['user']}",
                    f"sslmode={remote_params['sslmode']}",
                    f"application_name={remote_params.get('application_name') or 'gym_management_system'}",
                    f"connect_timeout={int(remote_params.get('connect_timeout') or 10)}",
                ]
                if rpwd:
                    parts.append(f"password={rpwd}")
                sub_conn_str = " ".join(parts)

            if not exists:
                # Crear suscripción y slot
                cur.execute(
                    sql.SQL(
                        """
                        CREATE SUBSCRIPTION {} CONNECTION %s PUBLICATION {} WITH (
                            create_slot = true,
                            enabled = true,
                            copy_data = true
                        )
                        """
                    ).format(sql.Identifier(subname), sql.Identifier(pubname)),
                    (sub_conn_str,)
                )
                print(f"Suscripción creada en local: {subname}")
            else:
                # Asegurar que apunta a la publicación correcta y habilitada
                try:
                    cur.execute(sql.SQL("ALTER SUBSCRIPTION {} SET PUBLICATION {};").format(sql.Identifier(subname), sql.Identifier(pubname)))
                except Exception:
                    pass
                try:
                    cur.execute(sql.SQL("ALTER SUBSCRIPTION {} ENABLE;").format(sql.Identifier(subname)))
                except Exception:
                    pass
                print(f"Suscripción ya existe en local: {subname} (actualizada/habilitada)")

            # Mostrar estado básico
            try:
                cur.execute("SELECT subname, slot_name, sync_state FROM pg_stat_subscription")
                rows = cur.fetchall() or []
                for r in rows:
                    print(f"Estado suscripción: sub={r[0]} slot={r[1]} sync_state={r[2]}")
            except Exception:
                pass
    finally:
        try:
            if conn:
                conn.close()
        except Exception:
            pass


def _check_remote_logical_capability(params: dict) -> Dict[str, Any]:
    info: Dict[str, Any] = {
        "ok": False,
        "wal_level": None,
        "max_replication_slots": None,
        "max_wal_senders": None,
        "in_recovery": None,
    }
    conn = None
    try:
        conn = _connect(params)
        with conn.cursor() as cur:
            try:
                cur.execute("SHOW wal_level")
                wl = (cur.fetchone() or [None])[0]
            except Exception:
                wl = None
            try:
                cur.execute("SHOW max_replication_slots")
                mrs = (cur.fetchone() or [None])[0]
            except Exception:
                mrs = None
            try:
                cur.execute("SHOW max_wal_senders")
                mws = (cur.fetchone() or [None])[0]
            except Exception:
                mws = None
            try:
                cur.execute("SELECT pg_is_in_recovery()")
                rec = (cur.fetchone() or [False])[0]
            except Exception:
                rec = None
        info["wal_level"] = (str(wl).lower() if wl is not None else None)
        try:
            info["max_replication_slots"] = int(mrs) if mrs is not None else None
        except Exception:
            info["max_replication_slots"] = mrs
        try:
            info["max_wal_senders"] = int(mws) if mws is not None else None
        except Exception:
            info["max_wal_senders"] = mws
        info["in_recovery"] = bool(rec) if rec is not None else None
        info["ok"] = (
            (info["wal_level"] == "logical") and
            (isinstance(info["max_replication_slots"], int) and info["max_replication_slots"] > 0) and
            (isinstance(info["max_wal_senders"], int) and info["max_wal_senders"] > 0) and
            (info["in_recovery"] is False)
        )
        return info
    except Exception:
        return info
    finally:
        try:
            if conn:
                conn.close()
        except Exception:
            pass


def run():
    base_dir = Path(__file__).resolve().parent.parent
    cfg_path = base_dir / 'config' / 'config.json'
    with open(cfg_path, 'r', encoding='utf-8') as f:
        cfg = json.load(f)

    remote = _resolve_remote_credentials(cfg)
    local = _resolve_local_credentials(cfg)

    print(
        f"Configurando replicación lógica:\n  Remoto: host={remote['host']} port={remote['port']} db={remote['database']} user={remote['user']} sslmode={remote['sslmode']} password={'SI' if remote['password'] else 'NO'}\n  Local: host={local['host']} port={local['port']} db={local['database']} user={local['user']} sslmode={local['sslmode']}"
    )

    # Crear publicación en remoto (si credenciales disponibles)
    try:
        if remote.get('user') and (remote.get('password') or remote.get('dsn')):
            _ensure_publication_on_remote(remote)
        else:
            print("Aviso: no se pudo crear publicación remota (faltan credenciales).")
    except Exception as e:
        print(f"Fallo creando/verificando publicación remota: {e}")

    # Crear/asegurar suscripción en local
    try:
        _ensure_subscription_on_local(local, remote)
    except Exception as e:
        print(f"Fallo creando/verificando suscripción local: {e}")


if __name__ == '__main__':
    run()