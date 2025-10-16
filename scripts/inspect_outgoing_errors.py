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
    with connect_local() as conn:
        cur = conn.cursor()
        # Outgoing errors
        # Select key columns available in local schema
        cur.execute(
            """
            SELECT batch_id, target_table_name, event_type, failed_line_number, last_update_time
            FROM public.sym_outgoing_error
            ORDER BY batch_id DESC
            LIMIT 50
            """
        )
        rows = cur.fetchall()
        print("sym_outgoing_error (recent):")
        if not rows:
            print("(no rows)")
        else:
            for r in rows:
                print(" | ".join(str(x) if x is not None else "NULL" for x in r))

        # Incoming errors (local as server side): align with local schema
        cur.execute(
            """
            SELECT batch_id, node_id, target_table_name, event_type, failed_line_number, last_update_time
            FROM public.sym_incoming_error
            ORDER BY batch_id DESC
            LIMIT 50
            """
        )
        in_rows = cur.fetchall()
        print("\nsym_incoming_error (default channel):")
        if not in_rows:
            print("(no rows)")
        else:
            for r in in_rows:
                print(" | ".join(str(x) if x is not None else "NULL" for x in r))
        cur.close()


if __name__ == "__main__":
    main()