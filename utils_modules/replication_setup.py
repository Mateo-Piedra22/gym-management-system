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
    retries = int(os.environ.get('PG_CONNECT_RETRIES') or remote.get('connect_retries') or cfg.get('connect_retries') or 3)
    retry_delay = float(os.environ.get('PG_CONNECT_RETRY_DELAY') or remote.get('connect_retry_delay') or cfg.get('connect_retry_delay') or 1.0)

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
        'connect_retries': retries,
        'connect_retry_delay': retry_delay,
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
    retries = int(os.environ.get('PG_CONNECT_RETRIES') or local.get('connect_retries') or cfg.get('connect_retries') or 3)
    retry_delay = float(os.environ.get('PG_CONNECT_RETRY_DELAY') or local.get('connect_retry_delay') or cfg.get('connect_retry_delay') or 1.0)

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
        'connect_retries': retries,
        'connect_retry_delay': retry_delay,
        'dsn': dsn,
    }


def _connect(params: dict, dbname: Optional[str] = None):
    if psycopg2 is None:
        raise RuntimeError("psycopg2 no disponible")
    import time
    dbname = dbname or params.get('database')
    dsn = params.get('dsn') or ''
    timeout = int(params.get('connect_timeout') or 10)
    retries = int(params.get('connect_retries') or 3)
    delay = float(params.get('connect_retry_delay') or 1.0)
    last_exc = None
    for attempt in range(1, max(1, retries) + 1):
        try:
            if dsn:
                return psycopg2.connect(dsn, connect_timeout=timeout)
            return psycopg2.connect(
                host=params['host'], port=params['port'], dbname=dbname,
                user=params['user'], password=params.get('password'), sslmode=params['sslmode'],
                application_name=params.get('application_name') or 'gym_management_system',
                connect_timeout=timeout,
            )
        except Exception as e:
            last_exc = e
            if attempt >= max(1, retries):
                raise
            try:
                time.sleep(delay)
            except Exception:
                pass
            delay = min(delay * 1.6, 5.0)
    # Si todos los intentos fallan, relanzar la última excepción
    if last_exc:
        raise last_exc
    raise RuntimeError("Error de conexión desconocido")


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
        info["in_recovery"] = rec
        try:
            is_logical = (str(info.get("wal_level")).lower() == "logical")
        except Exception:
            is_logical = False
        try:
            max_slots = int(info.get("max_replication_slots") or 0)
        except Exception:
            max_slots = 0
        try:
            max_senders = int(info.get("max_wal_senders") or 0)
        except Exception:
            max_senders = 0
        info["ok"] = bool(is_logical and max_slots > 0 and max_senders > 0 and (rec is False))
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
    """Asegura PUBLICATION remoto basada en `publishes_remote_to_local`.
    Si hay tablas listadas en `config/sync_tables.json` bajo `publishes_remote_to_local`,
    publica sólo esas. Si no hay listas, hace fallback a `FOR ALL TABLES`.
    """
    changed = False
    conn = None
    added_count = 0
    dropped_count = 0
    try:
        # Cargar configuración de tablas a publicar desde el remoto hacia el local
        try:
            base_dir = Path(__file__).resolve().parent.parent
            cfg_path = base_dir / 'config' / 'sync_tables.json'
            with open(cfg_path, 'r', encoding='utf-8') as f:
                cfg_tables = json.load(f) or {}
            publishes_remote_to_local = list(cfg_tables.get('publishes_remote_to_local') or [])
        except Exception:
            publishes_remote_to_local = []

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

            # Calcular la lista de tablas a publicar
            if publishes_remote_to_local:
                pub_tables = [t for t in remote_tables if t in publishes_remote_to_local]
            else:
                # Fallback: publicar todas las tablas del esquema
                pub_tables = remote_tables

            cur.execute("SELECT 1 FROM pg_publication WHERE pubname = %s", (pubname,))
            exists = bool(cur.fetchone())
            if not exists:
                if pub_tables:
                    parts = [
                        sql.SQL("{}.{}").format(sql.Identifier('public'), sql.Identifier(t))
                        for t in pub_tables
                    ]
                    try:
                        create_stmt = sql.SQL("CREATE PUBLICATION {} FOR TABLE ")\
                            .format(sql.Identifier(pubname)) + sql.SQL(", ").join(parts) + sql.SQL(" WITH (publish = 'insert, update, delete, truncate', publish_via_partition_root = true)")
                        cur.execute(create_stmt)
                    except Exception:
                        try:
                            create_stmt = sql.SQL("CREATE PUBLICATION {} FOR TABLE ")\
                                .format(sql.Identifier(pubname)) + sql.SQL(", ").join(parts) + sql.SQL(" WITH (publish = 'insert, update, delete, truncate')")
                            cur.execute(create_stmt)
                        except Exception:
                            pass
                else:
                    try:
                        cur.execute(sql.SQL("CREATE PUBLICATION {} FOR ALL TABLES WITH (publish = 'insert, update, delete, truncate', publish_via_partition_root = true)").format(sql.Identifier(pubname)))
                    except Exception:
                        try:
                            cur.execute(sql.SQL("CREATE PUBLICATION {} FOR ALL TABLES WITH (publish = 'insert, update, delete, truncate')").format(sql.Identifier(pubname)))
                        except Exception:
                            pass
            else:
                # Alinear publicación con tablas existentes
                try:
                    cur.execute(
                        """
                        SELECT pt.nspname, pt.relname
                        FROM pg_publication p
                        JOIN pg_publication_rel pr ON pr.prpubid = p.oid
                        JOIN pg_class c ON c.oid = pr.prrelid
                        JOIN pg_namespace pt ON pt.oid = c.relnamespace
                        WHERE p.pubname = %s
                        ORDER BY 1,2
                        """,
                        (pubname,)
                    )
                    already = [r[1] for r in (cur.fetchall() or [])]
                except Exception:
                    already = []
                desired = set(pub_tables)
                current = set(already)
                to_add = list(desired - current)
                to_drop = list(current - desired)
                added_count = 0
                dropped_count = 0
                if to_add:
                    parts = [
                        sql.SQL("{}.{}").format(sql.Identifier('public'), sql.Identifier(t))
                        for t in to_add
                    ]
                    try:
                        alter_add = sql.SQL("ALTER PUBLICATION {} ADD TABLE ")\
                            .format(sql.Identifier(pubname)) + sql.SQL(", ").join(parts)
                        cur.execute(alter_add)
                        added_count = len(to_add)
                    except Exception:
                        pass
                if to_drop:
                    parts = [
                        sql.SQL("{}.{}").format(sql.Identifier('public'), sql.Identifier(t))
                        for t in to_drop
                    ]
                    try:
                        alter_drop = sql.SQL("ALTER PUBLICATION {} DROP TABLE ")\
                            .format(sql.Identifier(pubname)) + sql.SQL(", ").join(parts)
                        cur.execute(alter_drop)
                        dropped_count = len(to_drop)
                    except Exception:
                        pass
        return {"ok": True, "changed": added_count > 0 or dropped_count > 0, "message": "PUBLICATION remoto verificado/creado", "added": added_count, "dropped": dropped_count}
    except Exception as e:
        return {"ok": False, "changed": False, "message": f"Fallo PUBLICATION remoto: {e}"}
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
                        "CREATE SUBSCRIPTION {} CONNECTION %s PUBLICATION {} WITH (create_slot = true, enabled = true, copy_data = true)"
                    ).format(sql.Identifier(subname), sql.Identifier(pubname)),
                    (sub_conn_str,),
                )
                changed = True
            else:
                # Forzar enable si estuviera deshabilitada
                cur.execute(sql.SQL("ALTER SUBSCRIPTION {} ENABLE").format(sql.Identifier(subname)))
                # Refrescar publicación para incluir tablas nuevas y asegurar snapshot
                try:
                    cur.execute(sql.SQL("ALTER SUBSCRIPTION {} REFRESH PUBLICATION WITH (copy_data = true)").format(sql.Identifier(subname)))
                except Exception:
                    pass
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
    # Local
    try:
        local = cfg.get('db_local') or cfg
        l_user = local.get('user') or cfg.get('user') or 'postgres'
        # Resolver host/port desde config y PGLOCAL_DSN
        l_host = local.get('host') or cfg.get('host') or 'localhost'
        try:
            l_port = int(local.get('port') or cfg.get('port') or 5432)
        except Exception:
            l_port = 5432
        l_dsn = os.environ.get('PGLOCAL_DSN') or ''
        if l_dsn:
            try:
                l_host, l_port, _db, l_user, l_pwd_dsn, _ssl, _app, _to = _parse_dsn(l_dsn, {
                    'host': l_host, 'port': l_port, 'database': local.get('database') or cfg.get('database') or 'gimnasio',
                    'user': l_user, 'password': local.get('password') or cfg.get('password'),
                    'sslmode': local.get('sslmode') or cfg.get('sslmode') or 'prefer',
                    'application_name': local.get('application_name') or cfg.get('application_name') or 'gym_management_system',
                    'connect_timeout': local.get('connect_timeout') or cfg.get('connect_timeout') or 10,
                })
            except Exception:
                l_pwd_dsn = None
        else:
            l_pwd_dsn = None
        # Prioridad de password: ENV > DSN > CONFIG
        l_pwd = os.environ.get('PGLOCAL_PASSWORD') \
            or os.environ.get('DB_LOCAL_PASSWORD') \
            or os.environ.get('DB_PASSWORD') \
            or os.environ.get('PGPASSWORD') \
            or os.environ.get('POSTGRES_PASSWORD') \
            or l_pwd_dsn \
            or local.get('password') \
            or cfg.get('password')
        # Guardar en variantes
        for acct in [f"{l_user}@local", f"{l_user}@{l_host}:{l_port}", l_user]:
            _keyring_set_if_missing(acct, l_pwd)
        if l_pwd:
            info["local"] = True
    except Exception:
        pass
    # Remoto
    try:
        remote = cfg.get('db_remote') or {}
        r_user = os.environ.get('PGREMOTE_USER') or remote.get('user') or 'postgres'
        r_host = remote.get('host') or ''
        try:
            r_port = int(remote.get('port') or 5432)
        except Exception:
            r_port = 5432
        r_dsn = os.environ.get('PGREMOTE_DSN') or ''
        if r_dsn:
            try:
                r_host, r_port, _db, r_user, r_pwd_dsn, _ssl, _app, _to = _parse_dsn(r_dsn, {
                    'host': r_host, 'port': r_port, 'database': remote.get('database') or 'railway',
                    'user': r_user, 'password': remote.get('password'), 'sslmode': remote.get('sslmode') or 'require',
                    'application_name': remote.get('application_name') or 'gym_management_system', 'connect_timeout': remote.get('connect_timeout') or 10,
                })
            except Exception:
                r_pwd_dsn = None
        else:
            r_pwd_dsn = None
        r_pwd = os.environ.get('PGREMOTE_PASSWORD') or r_pwd_dsn or remote.get('password')
        for acct in [f"{r_user}@railway", (f"{r_user}@{r_host}:{r_port}" if r_host else None), r_user]:
            if acct:
                _keyring_set_if_missing(acct, r_pwd)
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


def ensure_publication_on_local(local_params: dict, pubname: str = 'gym_pub_local') -> Dict[str, Any]:
    """Asegura PUBLICATION local basada en `uploads_local_to_remote`.
    Si hay tablas listadas en `config/sync_tables.json` bajo `uploads_local_to_remote`,
    publica sólo esas. Si no hay listas, hace fallback a `FOR ALL TABLES`.
    """
    changed = False
    conn = None
    added_count = 0
    dropped_count = 0
    try:
        try:
            base_dir = Path(__file__).resolve().parent.parent
            cfg_path = base_dir / 'config' / 'sync_tables.json'
            with open(cfg_path, 'r', encoding='utf-8') as f:
                cfg_tables = json.load(f) or {}
            uploads_local_to_remote = list(cfg_tables.get('uploads_local_to_remote') or [])
        except Exception:
            uploads_local_to_remote = []

        conn = _connect(local_params)
        conn.autocommit = True
        with conn.cursor() as cur:
            try:
                cur.execute(
                    """
                    SELECT table_name
                    FROM information_schema.tables
                    WHERE table_schema='public' AND table_type='BASE TABLE'
                    ORDER BY 1
                    """
                )
                local_tables = [r[0] for r in (cur.fetchall() or [])]
            except Exception:
                local_tables = []

            if uploads_local_to_remote:
                pub_tables = [t for t in local_tables if t in uploads_local_to_remote]
            else:
                pub_tables = local_tables

            cur.execute("SELECT 1 FROM pg_publication WHERE pubname = %s", (pubname,))
            exists = bool(cur.fetchone())
            if not exists:
                if pub_tables:
                    parts = [
                        sql.SQL("{}.{}").format(sql.Identifier('public'), sql.Identifier(t))
                        for t in pub_tables
                    ]
                    try:
                        create_stmt = sql.SQL("CREATE PUBLICATION {} FOR TABLE ")\
                            .format(sql.Identifier(pubname)) + sql.SQL(", ").join(parts) + sql.SQL(" WITH (publish = 'insert, update, delete, truncate', publish_via_partition_root = true)")
                        cur.execute(create_stmt)
                    except Exception:
                        try:
                            create_stmt = sql.SQL("CREATE PUBLICATION {} FOR TABLE ")\
                                .format(sql.Identifier(pubname)) + sql.SQL(", ").join(parts) + sql.SQL(" WITH (publish = 'insert, update, delete, truncate')")
                            cur.execute(create_stmt)
                        except Exception:
                            pass
                else:
                    try:
                        cur.execute(sql.SQL("CREATE PUBLICATION {} FOR ALL TABLES WITH (publish = 'insert, update, delete, truncate', publish_via_partition_root = true)").format(sql.Identifier(pubname)))
                    except Exception:
                        try:
                            cur.execute(sql.SQL("CREATE PUBLICATION {} FOR ALL TABLES WITH (publish = 'insert, update, delete, truncate')").format(sql.Identifier(pubname)))
                        except Exception:
                            pass
                changed = True
            else:
                try:
                    cur.execute("SELECT tablename FROM pg_publication_tables WHERE pubname = %s", (pubname,))
                    current = {r[0] for r in (cur.fetchall() or [])}
                except Exception:
                    current = set()
                desired = set(pub_tables) if pub_tables else set(local_tables)
                to_add = sorted(list(desired - current))
                to_drop = sorted(list(current - desired))
                for t in to_add:
                    try:
                        cur.execute(sql.SQL("ALTER PUBLICATION {} ADD TABLE {}.{}").format(
                            sql.Identifier(pubname), sql.Identifier('public'), sql.Identifier(t)))
                        added_count += 1
                    except Exception:
                        pass
                for t in to_drop:
                    try:
                        cur.execute(sql.SQL("ALTER PUBLICATION {} DROP TABLE {}.{}").format(
                            sql.Identifier(pubname), sql.Identifier('public'), sql.Identifier(t)))
                        dropped_count += 1
                    except Exception:
                        pass
                try:
                    cur.execute(sql.SQL("ALTER PUBLICATION {} SET (publish = 'insert, update, delete, truncate', publish_via_partition_root = true)").format(sql.Identifier(pubname)))
                except Exception:
                    try:
                        cur.execute(sql.SQL("ALTER PUBLICATION {} SET (publish = 'insert, update, delete, truncate')").format(sql.Identifier(pubname)))
                    except Exception:
                        pass

        return {
            "ok": True,
            "changed": changed,
            "added_tables": added_count,
            "dropped_tables": dropped_count,
            "message": f"Publication local '{pubname}' sincronizada"
        }
    except Exception as e:
        logging.exception("Error asegurando publicación en local")
        return {"ok": False, "error": str(e), "message": "Fallo en ensure_publication_on_local"}
    finally:
        try:
            if conn:
                conn.close()
        except Exception:
            pass


def ensure_subscription_on_remote(remote_params: dict, local_params: dict, subname: str = 'gym_sub_remote', pubname: str = 'gym_pub_local') -> Dict[str, Any]:
    """Crea/verifica SUBSCRIPTION en remoto apuntando a publicación local.
    Requiere que el servidor remoto pueda alcanzar la base local por red.
    """
    changed = False
    conn = None
    try:
        conn = _connect(remote_params)
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute("SELECT 1 FROM pg_subscription WHERE subname = %s", (subname,))
            exists = bool(cur.fetchone())

            lpwd = local_params.get('password') or ''
            ldsn = local_params.get('dsn') or ''
            if ldsn:
                sub_conn_str = ldsn
            else:
                parts = [
                    f"host={local_params['host']}",
                    f"port={local_params['port']}",
                    f"dbname={local_params['database']}",
                    f"user={local_params['user']}",
                    f"sslmode={local_params['sslmode']}",
                    f"application_name={local_params.get('application_name') or 'gym_management_system'}",
                    f"connect_timeout={int(local_params.get('connect_timeout') or 10)}",
                ]
                if lpwd:
                    parts.append(f"password={lpwd}")
                sub_conn_str = " ".join(parts)

            if not exists:
                cur.execute(
                    sql.SQL(
                        "CREATE SUBSCRIPTION {} CONNECTION %s PUBLICATION {} WITH (create_slot = true, enabled = true, copy_data = true)"
                    ).format(sql.Identifier(subname), sql.Identifier(pubname)),
                    (sub_conn_str,),
                )
                changed = True
            else:
                cur.execute(sql.SQL("ALTER SUBSCRIPTION {} ENABLE").format(sql.Identifier(subname)))
                try:
                    cur.execute(sql.SQL("ALTER SUBSCRIPTION {} REFRESH PUBLICATION WITH (copy_data = true)").format(sql.Identifier(subname)))
                except Exception:
                    pass
        return {"ok": True, "changed": changed, "message": "SUBSCRIPTION remota verificada/creada"}
    except Exception as e:
        return {"ok": False, "changed": changed, "message": f"Fallo SUBSCRIPTION remota: {e}"}
    finally:
        try:
            if conn:
                conn.close()
        except Exception:
            pass


def ensure_bidirectional_replication(cfg: dict) -> Dict[str, Any]:
    """Orquesta replicación lógica bidireccional (segura) con snapshot inicial.
    - Remoto -> Local: publicación en remoto y suscripción en local.
    - Local -> Remoto: publicación en local y suscripción en remoto (opcional).
    """
    results: Dict[str, Any] = {"ok": True, "steps": []}
    try:
        if psycopg2 is None:
            raise RuntimeError("psycopg2 no disponible")

        seeded = seed_keyring_credentials(cfg)
        results["steps"].append({"seed_keyring": seeded})

        remote = resolve_remote_credentials(cfg)
        local = resolve_local_credentials(cfg)
        results["steps"].append({"resolve": {"remote": {k: remote.get(k) for k in ("host","port","database","user","sslmode")}, "local": {k: local.get(k) for k in ("host","port","database","user","sslmode")}}})

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

        rep_cfg = cfg.get('replication') or {}
        remote_pubname = rep_cfg.get('publication_name') or 'gym_pub'
        local_pubname = rep_cfg.get('local_publication_name') or 'gym_pub_local'
        local_subname = rep_cfg.get('subscription_name') or 'gym_sub'
        remote_subname = rep_cfg.get('remote_subscription_name') or 'gym_sub_remote'
        remote_can_reach_local = bool(rep_cfg.get('remote_can_reach_local'))

        pub_remote_res = {"ok": False, "message": "Saltado"}
        if remote.get('user') and (remote.get('password') or remote.get('dsn')):
            pub_remote_res = ensure_publication_on_remote(remote, pubname=remote_pubname)
        results["steps"].append({"publication_remote": pub_remote_res})

        # Asegurar REPLICA IDENTITY FULL en tablas remotas sin PK (publisher remoto)
        try:
            conn_align_r = _connect(remote)
            conn_align_r.autocommit = True
            with conn_align_r.cursor() as cur:
                cur.execute(
                    """
                    SELECT c.relname
                    FROM pg_class c
                    JOIN pg_namespace n ON n.oid = c.relnamespace
                    WHERE n.nspname = 'public'
                      AND c.relkind = 'r'
                      AND NOT EXISTS (
                        SELECT 1 FROM pg_index i
                        WHERE i.indrelid = c.oid AND i.indisprimary
                      )
                    """
                )
                no_pk_r = [r[0] for r in (cur.fetchall() or [])]
                for t in no_pk_r:
                    try:
                        cur.execute(sql.SQL("ALTER TABLE {}.{} REPLICA IDENTITY FULL").format(
                            sql.Identifier('public'), sql.Identifier(t)))
                    except Exception:
                        pass
            results["steps"].append({"replica_identity_remote": {"ok": True, "count_full": len(no_pk_r)}})
        except Exception as e:
            results["steps"].append({"replica_identity_remote": {"ok": False, "error": str(e)}})
        finally:
            try:
                if conn_align_r:
                    conn_align_r.close()
            except Exception:
                pass

        sub_local_res = ensure_subscription_on_local(local, remote, subname=local_subname, pubname=remote_pubname)
        results["steps"].append({"subscription_local": sub_local_res})

        pub_local_res = ensure_publication_on_local(local, pubname=local_pubname)
        results["steps"].append({"publication_local": pub_local_res})

        # Asegurar REPLICA IDENTITY FULL en tablas sin PK (local)
        try:
            conn_align = _connect(local)
            conn_align.autocommit = True
            with conn_align.cursor() as cur:
                cur.execute(
                    """
                    SELECT c.relname
                    FROM pg_class c
                    JOIN pg_namespace n ON n.oid = c.relnamespace
                    WHERE n.nspname = 'public'
                      AND c.relkind = 'r'
                      AND NOT EXISTS (
                        SELECT 1 FROM pg_index i
                        WHERE i.indrelid = c.oid AND i.indisprimary
                      )
                    """
                )
                no_pk = [r[0] for r in (cur.fetchall() or [])]
                for t in no_pk:
                    try:
                        cur.execute(sql.SQL("ALTER TABLE {}.{} REPLICA IDENTITY FULL").format(
                            sql.Identifier('public'), sql.Identifier(t)))
                    except Exception:
                        pass
            results["steps"].append({"replica_identity_local": {"ok": True, "count_full": len(no_pk)}})
        except Exception as e:
            results["steps"].append({"replica_identity_local": {"ok": False, "error": str(e)}})
        finally:
            try:
                if conn_align:
                    conn_align.close()
            except Exception:
                pass

        # Alinear secuencias locales a MAX(id) tras snapshot inicial
        try:
            conn_seq = _connect(local)
            conn_seq.autocommit = True
            with conn_seq.cursor() as cur:
                cur.execute(
                    """
                    SELECT c.relname AS table, a.attname AS column,
                           pg_get_serial_sequence(format('%I.%I', 'public', c.relname), a.attname) AS seq
                    FROM pg_class c
                    JOIN pg_namespace n ON n.oid = c.relnamespace
                    JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum > 0
                    JOIN pg_attrdef d ON d.adrelid = c.oid AND d.adnum = a.attnum
                    WHERE n.nspname = 'public' AND c.relkind = 'r'
                      AND pg_get_expr(d.adbin, d.adrelid) LIKE 'nextval%'
                    """
                )
                seqs = cur.fetchall() or []
                aligned = 0
                for table, column, seq in seqs:
                    if not seq:
                        continue
                    try:
                        cur.execute(sql.SQL("SELECT COALESCE(MAX({}), 0) FROM {}.{}").format(
                            sql.Identifier(column), sql.Identifier('public'), sql.Identifier(table)))
                        maxv = (cur.fetchone() or [0])[0]
                        cur.execute("SELECT setval(%s, %s, true)", (seq, int(maxv) + 1))
                        aligned += 1
                    except Exception:
                        pass
            results["steps"].append({"align_sequences_local": {"ok": True, "count": aligned}})
        except Exception as e:
            results["steps"].append({"align_sequences_local": {"ok": False, "error": str(e)}})
        finally:
            try:
                if conn_seq:
                    conn_seq.close()
            except Exception:
                pass

        sub_remote_res = {"ok": False, "message": "Saltado (remoto no puede alcanzar local)"}
        if remote_can_reach_local:
            sub_remote_res = ensure_subscription_on_remote(remote, local, subname=remote_subname, pubname=local_pubname)
        results["steps"].append({"subscription_remote": sub_remote_res})

        results["ok"] = bool(pub_remote_res.get("ok") and sub_local_res.get("ok") and pub_local_res.get("ok") and (remote_can_reach_local == False or sub_remote_res.get("ok")))
        return results
    except Exception as e:
        logging.exception("Error asegurando replicación bidireccional")
        return {"ok": False, "error": str(e), "steps": results.get("steps", [])}


def ensure_logical_replication_from_config_path(cfg_path: Path) -> Dict[str, Any]:
    with open(cfg_path, 'r', encoding='utf-8') as f:
        cfg = json.load(f)
    return ensure_logical_replication(cfg)