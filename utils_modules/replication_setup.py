import os
import json
import logging
from typing import Dict, Any, Optional

from pathlib import Path

try:
    import psycopg2
    from psycopg2 import sql
except Exception:
    psycopg2 = None
    sql = None

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


def _keyring_get_variants(user: str, host: str = '', port: Optional[int] = None, scope_hint: Optional[str] = None) -> Optional[str]:
    """Intenta obtener contraseña desde keyring usando variantes de nombre de cuenta."""
    if keyring is None:
        return None
    candidates = []
    try:
        if host:
            if port:
                candidates.append(f"{user}@{host}:{port}")
            candidates.append(f"{user}@{host}")
        if scope_hint:
            candidates.append(f"{user}@{scope_hint}")
        candidates.append(user)
    except Exception:
        candidates = [user]
    for account in candidates:
        try:
            pwd = keyring.get_password(KEYRING_SERVICE_NAME, account)
            if pwd:
                return pwd
        except Exception:
            continue
    return None


def _keyring_set_if_missing(account: str, password: Optional[str]) -> None:
    if keyring is None:
        return
    if not password:
        return
    try:
        existing = keyring.get_password(KEYRING_SERVICE_NAME, account)
    except Exception:
        existing = None
    if existing:
        return
    try:
        keyring.set_password(KEYRING_SERVICE_NAME, account, password)
        logging.info(f"Keyring: almacenada contraseña para '{account}' en servicio '{KEYRING_SERVICE_NAME}'")
    except Exception:
        logging.warning(f"Keyring: no se pudo almacenar contraseña para '{account}'")


def resolve_remote_credentials(cfg: dict) -> dict:
    remote = cfg.get('db_remote') or {}
    host = remote.get('host') or ''
    port = int(remote.get('port') or 5432)
    db = remote.get('database') or 'railway'
    user = os.environ.get('PGREMOTE_USER') or remote.get('user') or 'postgres'
    sslmode = remote.get('sslmode') or cfg.get('sslmode') or 'require'
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

    # Orden: ENV > DSN > CONFIG > KEYRING
    password = os.environ.get('PGREMOTE_PASSWORD') or pw_from_dsn or remote.get('password')
    if not password:
        password = _keyring_get_variants(user, host=host, port=port, scope_hint='railway')

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


def resolve_local_credentials(cfg: dict) -> dict:
    local = cfg.get('db_local') or cfg
    host = local.get('host') or cfg.get('host') or 'localhost'
    port = int(local.get('port') or cfg.get('port') or 5432)
    db = local.get('database') or cfg.get('database') or 'gimnasio'
    user = local.get('user') or cfg.get('user') or 'postgres'
    password_cfg = local.get('password') or cfg.get('password')
    sslmode = local.get('sslmode') or cfg.get('sslmode') or 'prefer'
    appname = local.get('application_name') or cfg.get('application_name') or 'gym_management_system'
    timeout = int(local.get('connect_timeout') or cfg.get('connect_timeout') or 10)

    dsn = os.environ.get('PGLOCAL_DSN') or ''
    host, port, db, user, pw_from_dsn, sslmode, appname, timeout = _parse_dsn(
        dsn,
        {
            'host': host,
            'port': port,
            'database': db,
            'user': user,
            'password': password_cfg,
            'sslmode': sslmode,
            'application_name': appname,
            'connect_timeout': timeout,
        },
    )

    # Orden: ENV > DSN > CONFIG > KEYRING
    password = os.environ.get('PGLOCAL_PASSWORD') or pw_from_dsn or password_cfg
    if not password:
        password = _keyring_get_variants(user, host=host, port=port, scope_hint='local')

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


def _connect(params: dict, dbname: Optional[str] = None):
    if psycopg2 is None:
        raise RuntimeError("psycopg2 no disponible")
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


def check_remote_logical_capability(remote_params: dict) -> Dict[str, Any]:
    """Verifica si el servidor remoto soporta replicación lógica.
    Chequea wal_level, max_replication_slots, max_wal_senders y si está en recuperación.
    """
    info: Dict[str, Any] = {
        "ok": False,
        "wal_level": None,
        "max_replication_slots": None,
        "max_wal_senders": None,
        "in_recovery": None,
    }
    conn = None
    try:
        conn = _connect(remote_params)
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
        try:
            info["wal_level"] = (str(wl).lower() if wl is not None else None)
        except Exception:
            info["wal_level"] = wl
        try:
            info["max_replication_slots"] = int(mrs) if mrs is not None else None
        except Exception:
            info["max_replication_slots"] = mrs
        try:
            info["max_wal_senders"] = int(mws) if mws is not None else None
        except Exception:
            info["max_wal_senders"] = mws
        info["in_recovery"] = bool(rec) if rec is not None else None
        # Condición mínima para publicar: wal_level=logical, slots>0, senders>0 y no estar en recovery
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


def ensure_publication_on_remote(remote_params: dict, pubname: str = 'gym_pub') -> Dict[str, Any]:
    """Asegura PUBLICATION remoto filtrando tablas transaccionales locales.
    Excluye tablas listadas en `config/sync_tables.json` bajo `uploads_local_to_remote`.
    Si no hay tablas específicas, hace fallback a `FOR ALL TABLES`.
    """
    changed = False
    conn = None
    added_count = 0
    dropped_count = 0
    try:
        # Cargar configuración de tablas a excluir de la publicación remota
        try:
            base_dir = Path(__file__).resolve().parent.parent
            cfg_path = base_dir / 'config' / 'sync_tables.json'
            with open(cfg_path, 'r', encoding='utf-8') as f:
                cfg_tables = json.load(f) or {}
            uploads_local_to_remote = list(cfg_tables.get('uploads_local_to_remote') or [])
        except Exception:
            uploads_local_to_remote = []

        conn = _connect(remote_params)
        conn.autocommit = True
        with conn.cursor() as cur:
            # Enumerar tablas actuales en remoto (schema public)
            try:
                cur.execute(
                    """
                    SELECT table_name
                    FROM information_schema.tables
                    WHERE table_schema='public' AND table_type='BASE TABLE'
                    ORDER BY 1
                    """
                )
                remote_tables = [r[0] for r in (cur.fetchall() or [])]
            except Exception:
                remote_tables = []

            # Calcular la lista de tablas a publicar: todas menos las transaccionales locales
            pub_tables = [t for t in remote_tables if t not in uploads_local_to_remote]

            cur.execute("SELECT 1 FROM pg_publication WHERE pubname = %s", (pubname,))
            exists = bool(cur.fetchone())
            if not exists:
                if pub_tables:
                    # Crear publicación explícita solo para tablas permitidas
                    parts = [
                        sql.SQL("{}.{}").format(sql.Identifier('public'), sql.Identifier(t))
                        for t in pub_tables
                    ]
                    create_stmt = sql.SQL("CREATE PUBLICATION {} FOR TABLE ")\
                        .format(sql.Identifier(pubname)) + sql.SQL(", ").join(parts)
                    cur.execute(create_stmt)
                else:
                    # Fallback: publicar todas las tablas
                    cur.execute(sql.SQL("CREATE PUBLICATION {} FOR ALL TABLES").format(sql.Identifier(pubname)))
                changed = True
            else:
                # Sincronizar conjunto de tablas de la publicación existente
                try:
                    cur.execute("SELECT tablename FROM pg_publication_tables WHERE pubname = %s", (pubname,))
                    current = {r[0] for r in (cur.fetchall() or [])}
                except Exception:
                    current = set()
                desired = set(pub_tables) if pub_tables else set(remote_tables)
                to_add = sorted(list(desired - current))
                to_drop = sorted(list(current - desired))
                for t in to_add:
                    try:
                        cur.execute(sql.SQL("ALTER PUBLICATION {} ADD TABLE {}.{}")
                                    .format(sql.Identifier(pubname), sql.Identifier('public'), sql.Identifier(t)))
                        added_count += 1
                        changed = True
                    except Exception:
                        pass
                for t in to_drop:
                    try:
                        cur.execute(sql.SQL("ALTER PUBLICATION {} DROP TABLE {}.{}")
                                    .format(sql.Identifier(pubname), sql.Identifier('public'), sql.Identifier(t)))
                        dropped_count += 1
                        changed = True
                    except Exception:
                        pass
        return {
            "ok": True,
            "changed": changed,
            "message": f"PUBLICATION remoto verificado/ajustado (ADD={added_count} DROP={dropped_count})",
            "added": added_count,
            "dropped": dropped_count,
        }
    except Exception as e:
        return {"ok": False, "changed": changed, "message": f"Fallo PUBLICATION remoto: {e}"}
    finally:
        try:
            if conn:
                conn.close()
        except Exception:
            pass


def ensure_subscription_on_local(local_params: dict, remote_params: dict, subname: str = 'gym_sub', pubname: str = 'gym_pub') -> Dict[str, Any]:
    changed = False
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
                cur.execute(
                    sql.SQL(
                        "CREATE SUBSCRIPTION {} CONNECTION %s PUBLICATION {} WITH (create_slot = true, enabled = true)"
                    ).format(sql.Identifier(subname), sql.Identifier(pubname)),
                    (sub_conn_str,),
                )
                changed = True
            else:
                # Forzar enable si estuviera deshabilitada
                cur.execute(sql.SQL("ALTER SUBSCRIPTION {} ENABLE").format(sql.Identifier(subname)))
        return {"ok": True, "changed": changed, "message": "SUBSCRIPTION local verificada/creada"}
    except Exception as e:
        return {"ok": False, "changed": changed, "message": f"Fallo SUBSCRIPTION local: {e}"}
    finally:
        try:
            if conn:
                conn.close()
        except Exception:
            pass


def seed_keyring_credentials(cfg: dict) -> Dict[str, Any]:
    """Si faltan, almacena contraseñas en keyring bajo cuentas diferenciadas."""
    info = {"local": False, "remote": False}
    try:
        local = cfg.get('db_local') or cfg
        l_user = local.get('user') or cfg.get('user') or 'postgres'
        l_pwd = local.get('password') or cfg.get('password')
        _keyring_set_if_missing(f"{l_user}@local", l_pwd)
        if l_pwd:
            info["local"] = True
    except Exception:
        pass
    try:
        remote = cfg.get('db_remote') or {}
        r_user = remote.get('user') or 'postgres'
        r_pwd = remote.get('password')
        _keyring_set_if_missing(f"{r_user}@railway", r_pwd)
        # También por host:port si está disponible
        host = remote.get('host') or ''
        port = remote.get('port')
        if host and port:
            _keyring_set_if_missing(f"{r_user}@{host}:{port}", r_pwd)
        if r_pwd:
            info["remote"] = True
    except Exception:
        pass
    return info


def ensure_logical_replication(cfg: dict) -> Dict[str, Any]:
    """Automatiza setup de replicación: siembra keyring y asegura publicación/suscripción."""
    results: Dict[str, Any] = {"ok": True, "steps": []}
    try:
        if psycopg2 is None:
            raise RuntimeError("psycopg2 no disponible")

        # Paso 1: siembra keyring (idempotente)
        seeded = seed_keyring_credentials(cfg)
        results["steps"].append({"seed_keyring": seeded})

        # Paso 2: resolver credenciales
        remote = resolve_remote_credentials(cfg)
        local = resolve_local_credentials(cfg)
        results["steps"].append({"resolve": {"remote": {k: remote.get(k) for k in ("host","port","database","user","sslmode")}, "local": {k: local.get(k) for k in ("host","port","database","user","sslmode")}}})

        # Paso 2.1: verificar capacidades de replicación lógica en remoto
        remote_checks = check_remote_logical_capability(remote)
        results["steps"].append({"remote_checks": remote_checks})
        if not remote_checks.get("ok"):
            results["ok"] = False
            results["message"] = (
                "Remoto no soporta replicación lógica: "
                f"wal_level={remote_checks.get('wal_level')} "
                f"slots={remote_checks.get('max_replication_slots')} "
                f"senders={remote_checks.get('max_wal_senders')} "
                f"in_recovery={remote_checks.get('in_recovery')}"
            )
            return results

        # Paso 3: asegurar PUBLICATION remoto (si hay credenciales suficientes)
        rep_cfg = cfg.get('replication') or {}
        pubname = rep_cfg.get('publication_name') or 'gym_pub'
        subname = rep_cfg.get('subscription_name') or 'gym_sub'

        pub_res = {"ok": False, "message": "Saltado"}
        if remote.get('user') and (remote.get('password') or remote.get('dsn')):
            pub_res = ensure_publication_on_remote(remote, pubname=pubname)
        results["steps"].append({"publication": pub_res})
        if not pub_res.get("ok"):
            logging.warning(pub_res.get("message"))

        # Paso 4: asegurar SUBSCRIPTION local
        sub_res = ensure_subscription_on_local(local, remote, subname=subname, pubname=pubname)
        results["steps"].append({"subscription": sub_res})

        results["ok"] = bool(pub_res.get("ok") and sub_res.get("ok"))
        return results
    except Exception as e:
        logging.exception("Error asegurando replicación lógica")
        return {"ok": False, "error": str(e), "steps": results.get("steps", [])}


def ensure_logical_replication_from_config_path(cfg_path: Path) -> Dict[str, Any]:
    with open(cfg_path, 'r', encoding='utf-8') as f:
        cfg = json.load(f)
    return ensure_logical_replication(cfg)