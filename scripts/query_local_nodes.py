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
        # Show nodes and sync_url
        cur.execute(
            """
            SELECT node_id, node_group_id, external_id, sync_enabled, sync_url
            FROM public.sym_node
            ORDER BY node_id
            """
        )
        print("sym_node:")
        for r in cur.fetchall():
            print(" | ".join(str(x) if x is not None else "NULL" for x in r))

        # Show host heartbeat info
        cur.execute(
            """
            SELECT node_id, host_name, heartbeat_time, last_restart_time
            FROM public.sym_node_host
            ORDER BY heartbeat_time DESC NULLS LAST
            LIMIT 10
            """
        )
        print("\nsym_node_host (recent heartbeats):")
        for r in cur.fetchall():
            print(" | ".join(str(x) if x is not None else "NULL" for x in r))
        cur.close()


if __name__ == "__main__":
    main()