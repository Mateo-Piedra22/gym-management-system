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
    if not jdbc.startswith("jdbc:postgresql://"):
        raise ValueError(f"Unsupported JDBC URL: {jdbc}")
    url = jdbc[len("jdbc:postgresql://"):]
    base, _, query = url.partition("?")
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
    with connect_local() as conn:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT trigger_id, source_table_name, source_schema_name, channel_id, sync_on_insert, sync_on_update, sync_on_delete
            FROM public.sym_trigger
            WHERE source_table_name = 'usuarios' OR source_table_name LIKE 'sym%'
            ORDER BY source_table_name
            """
        )
        rows = cur.fetchall()
        print("sym_trigger entries:")
        for r in rows:
            print(" | ".join(str(x) if x is not None else "NULL" for x in r))

        cur.execute(
            """
            SELECT trigger_hist_id, trigger_id, source_table_name, create_time
            FROM public.sym_trigger_hist
            WHERE source_table_name = 'usuarios'
            ORDER BY trigger_hist_id DESC
            LIMIT 10
            """
        )
        hist = cur.fetchall()
        print("\nsym_trigger_hist for usuarios:")
        for r in hist:
            print(" | ".join(str(x) if x is not None else "NULL" for x in r))
        cur.close()


if __name__ == "__main__":
    main()