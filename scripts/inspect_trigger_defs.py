import os
import re
from urllib.parse import parse_qs
import psycopg2


def parse_properties(path: str) -> dict:
    props = {}
    try:
        with open(path, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith('#'):
                    continue
                if '=' in line:
                    k, v = line.split('=', 1)
                    props[k.strip()] = v.strip()
    except Exception:
        pass
    return props


def jdbc_to_conn_kwargs(jdbc_url: str) -> dict:
    m = re.match(r"jdbc:postgresql://([^:/?#]+):(\d+)/(\w+)(?:\?(.*))?", jdbc_url)
    if not m:
        raise ValueError(f"Unrecognized JDBC: {jdbc_url}")
    host, port, db, qs = m.group(1), int(m.group(2)), m.group(3), m.group(4)
    kwargs = {"host": host, "port": port, "dbname": db}
    if qs:
        q = parse_qs(qs)
        sslmode = q.get("sslmode", [None])[0]
        if sslmode:
            kwargs["sslmode"] = sslmode
    return kwargs


def connect_from_props(props_path: str) -> psycopg2.extensions.connection:
    props = parse_properties(props_path)
    jdbc = props.get("db.url", "")
    kwargs = jdbc_to_conn_kwargs(jdbc)
    user = props.get("db.user", "postgres")
    pwd = props.get("db.password") or os.environ.get("PGPASSWORD") or ""
    return psycopg2.connect(user=user, password=pwd, **kwargs)


def main():
    target = os.environ.get("TARGET", "local").lower()
    table = os.environ.get("TABLE", "usuarios")
    base = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    engines = os.path.join(base, "symmetricds", "engines")
    props_path = os.path.join(engines, f"{target}.properties")
    conn = None
    try:
        conn = connect_from_props(props_path)
        with conn.cursor() as cur:
            print(f"=== Definiciones de triggers para '{table}' en {target} ===")
            cur.execute(
                """
                SELECT tg.tgname, pg_catalog.pg_get_triggerdef(tg.oid)
                FROM pg_trigger tg
                JOIN pg_class c ON tg.tgrelid = c.oid
                JOIN pg_namespace n ON c.relnamespace = n.oid
                WHERE c.relname=%s AND n.nspname='public' AND NOT tg.tgisinternal
                ORDER BY tg.tgname
                """,
                (table,)
            )
            for name, defn in cur.fetchall():
                print(f"\n-- {name} --\n{defn}")
            # Opcional: funciones subyacentes
            print("\n=== Funciones de trigger relacionadas ===")
            cur.execute(
                """
                SELECT DISTINCT p.proname, pg_catalog.pg_get_functiondef(p.oid)
                FROM pg_trigger tg
                JOIN pg_proc p ON tg.tgfoid = p.oid
                JOIN pg_class c ON tg.tgrelid = c.oid
                JOIN pg_namespace n ON c.relnamespace = n.oid
                WHERE c.relname=%s AND n.nspname='public' AND NOT tg.tgisinternal
                ORDER BY p.proname
                """,
                (table,)
            )
            for fname, fdef in cur.fetchall():
                print(f"\n-- {fname} --\n{fdef}")
    finally:
        try:
            if conn:
                conn.close()
        except Exception:
            pass


if __name__ == "__main__":
    main()