import os
import json
from pathlib import Path

import psycopg2

try:
    import keyring
except Exception:
    keyring = None

# Evitar dependencia de imports de la app; usar constante directa
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

    # Contraseña remota hardcodeada por solicitud
    password = "uDEvhRmVlvaiyRWPPRuSPfVKavIKwmLm"

    print(f"Verificando objetos remotos: host={host} port={port} db={db} user={user} sslmode={sslmode} password={'SI' if password else 'NO'}")

    # Permitir override de usuario y fallback comunes
    user_override = os.environ.get('PGREMOTE_USER') or ''
    candidates = [u for u in [user_override, 'postgres'] if u]
    seen = set()
    users_to_try = []
    for u in candidates:
        if u not in seen:
            seen.add(u)
            users_to_try.append(u)

    last_error = None
    for utry in users_to_try:
        conn = None
        try:
            conn = psycopg2.connect(host=host, port=port, dbname=db, user=utry, password=password, sslmode=sslmode, connect_timeout=10)
            with conn.cursor() as cur:
                cur.execute(
                """
                WITH objs AS (
                  SELECT 'table' AS kind, schemaname AS schema_name, tablename AS obj_name
                  FROM pg_tables WHERE tablename ILIKE 'sym_%'
                  UNION ALL
                  SELECT 'sequence', sequence_schema, sequence_name FROM information_schema.sequences WHERE sequence_name ILIKE 'sym_%'
                  UNION ALL
                  SELECT 'function', n.nspname, p.proname || '(' || pg_get_function_identity_arguments(p.oid) || ')'
                  FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace WHERE p.proname ILIKE 'sym_%'
                  UNION ALL
                  SELECT 'trigger', n.nspname, t.tgname
                  FROM pg_trigger t JOIN pg_class c ON c.oid = t.tgrelid JOIN pg_namespace n ON n.oid = c.relnamespace
                  WHERE NOT t.tgisinternal AND t.tgname ILIKE 'sym_%'
                )
                SELECT * FROM objs ORDER BY kind, schema_name, obj_name;
                """
                )
                rows = cur.fetchall()
                print(f"Conectado como '{utry}'. Objetos remanentes (remoto):")
                if not rows:
                    print('(ninguno)')
                else:
                    for row in rows:
                        print(row)
            try:
                conn.close()
            except Exception:
                pass
            return
        except Exception as e:
            last_error = e
            try:
                if conn:
                    conn.close()
            except Exception:
                pass
            print(f"Intento con usuario '{utry}': FALLÓ ({e})")

    print('Verificación remota: FALLÓ')
    if last_error:
        print(last_error)


if __name__ == '__main__':
    run()