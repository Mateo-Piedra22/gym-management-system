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


def upsert_param(conn, external_id: str, key: str, value: str):
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO public.sym_parameter (external_id, node_group_id, param_key, param_value, create_time)
        VALUES (%s, 'client', %s, %s, CURRENT_TIMESTAMP)
        ON CONFLICT (external_id, node_group_id, param_key)
        DO UPDATE SET param_value=EXCLUDED.param_value, last_update_time=CURRENT_TIMESTAMP
        """,
        (external_id, key, value),
    )
    cur.close()


def main():
    with connect_local() as conn:
        # Determine local external_id
        cur = conn.cursor()
        cur.execute("SELECT external_id FROM public.sym_node WHERE node_group_id='client' AND external_id LIKE 'local-%' LIMIT 1")
        row = cur.fetchone()
        external_id = row[0] if row else 'local'
        cur.close()

        upsert_param(conn, external_id, 'push.ack.required', 'false')
        print("Set push.ack.required=false for", external_id)


if __name__ == "__main__":
    main()