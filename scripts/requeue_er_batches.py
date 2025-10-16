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
        cur.execute(
            """
            SELECT batch_id, node_id, status, channel_id, error_flag
            FROM public.sym_outgoing_batch
            WHERE status = 'ER' AND channel_id = 'default' AND node_id = 'railway'
            ORDER BY batch_id DESC
            LIMIT 100
            """
        )
        rows = cur.fetchall()
        print(f"Found {len(rows)} ER batches to requeue")
        ids = [r[0] for r in rows]
        if ids:
            cur.execute(
                """
                UPDATE public.sym_outgoing_batch
                SET status = 'NE', error_flag = 0
                WHERE status = 'ER' AND channel_id = 'default' AND node_id = 'railway'
                """
            )
            print("Requeued batches: ", ids[:10], ("..." if len(ids) > 10 else ""))
        else:
            print("No ER batches found on default channel for node 'railway'.")
        cur.close()


if __name__ == "__main__":
    main()