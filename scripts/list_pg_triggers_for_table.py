import os
import psycopg2
from typing import Dict


def read_properties(path: str) -> Dict[str, str]:
    props: Dict[str, str] = {}
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            if "=" in line:
                k, v = line.split("=", 1)
                props[k.strip()] = v.strip()
    return props


def parse_jdbc_url(jdbc: str):
    url = jdbc[len("jdbc:postgresql://"):]
    base, _, _ = url.partition("?")
    hostport, _, dbname = base.partition("/")
    host, _, port_str = hostport.partition(":")
    port = int(port_str) if port_str else 5432
    return host, port, dbname


def connect_local():
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    props_path = os.path.join(base_dir, "symmetricds", "engines", "local.properties")
    props = read_properties(props_path)
    jdbc = props.get("db.url")
    user = props.get("db.user")
    password = props.get("db.password")
    host, port, dbname = parse_jdbc_url(jdbc)
    conn = psycopg2.connect(host=host, port=port, dbname=dbname, user=user, password=password)
    conn.autocommit = True
    return conn


def main():
    table = os.environ.get("TABLE", "usuarios")
    schema = os.environ.get("SCHEMA", "public")
    with connect_local() as conn:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT t.tgname,
                   CASE t.tgtype & 1 WHEN 1 THEN 'ROW' ELSE 'STATEMENT' END as level,
                   CASE t.tgtype & 2 WHEN 2 THEN 'BEFORE' ELSE 'AFTER' END as timing,
                   CASE t.tgtype & 4 WHEN 4 THEN 'INSERT' ELSE '' END ||
                   CASE t.tgtype & 8 WHEN 8 THEN 'UPDATE' ELSE '' END ||
                   CASE t.tgtype & 16 WHEN 16 THEN 'DELETE' ELSE '' END as events,
                   p.proname
            FROM pg_trigger t
            JOIN pg_class c ON c.oid = t.tgrelid
            JOIN pg_namespace n ON n.oid = c.relnamespace
            JOIN pg_proc p ON p.oid = t.tgfoid
            WHERE n.nspname = %s AND c.relname = %s AND NOT t.tgisinternal
            ORDER BY t.tgname
            """,
            (schema, table)
        )
        rows = cur.fetchall()
        print(f"Triggers on {schema}.{table}:")
        for r in rows:
            print(" | ".join(str(x) for x in r))
        cur.close()


if __name__ == "__main__":
    main()