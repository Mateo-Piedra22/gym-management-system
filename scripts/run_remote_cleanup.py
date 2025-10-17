import os
import json
from pathlib import Path

import psycopg2
from urllib.parse import urlparse, parse_qs

try:
    import keyring
except Exception:
    keyring = None

# Evitar problemas de import; replicamos la constante usada por la app
KEYRING_SERVICE_NAME = "GymMS_DB"


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

    # Permitir DSN completo vía entorno
    dsn = os.environ.get('PGREMOTE_DSN') or ''

    # Resolver contraseña: entorno > DSN > keyring
    password = os.environ.get('PGREMOTE_PASSWORD') or None
    if dsn:
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
    if not password and keyring is not None:
        try:
            password = keyring.get_password(KEYRING_SERVICE_NAME, user)
        except Exception:
            password = None

    print(f"Intentando limpieza remota: host={host} port={port} db={db} user={user} sslmode={sslmode} password={'SI' if password else 'NO'}")

    sql = (base_dir / 'scripts' / 'cleanup_symmetricds.sql').read_text(encoding='utf-8')

    # Permitir override de usuario por entorno y probar fallback comunes
    user_override = os.environ.get('PGREMOTE_USER') or ''
    if user_override:
        user = user_override

    conn = None
    try:
        if dsn:
            conn = psycopg2.connect(dsn, connect_timeout=10)
        else:
            conn = psycopg2.connect(host=host, port=port, dbname=db, user=user, password=password, sslmode=sslmode, connect_timeout=10)
        conn.autocommit = True
        cur = conn.cursor()
        print(f"Conectado correctamente como usuario '{user}'. Ejecutando limpieza...")
        cur.execute(sql)
        print('Cleanup remoto: OK')
        cur.close()
    except Exception as e:
        print(f"Cleanup remoto: FALLÓ ({e})")
    finally:
        try:
            if conn:
                conn.close()
        except Exception:
            pass


if __name__ == '__main__':
    run()