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


def connect_remote():
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    props_path = os.path.join(base_dir, "symmetricds", "engines", "railway.properties")
    props = read_properties(props_path)
    jdbc = props.get("db.url")
    user = props.get("db.user")
    password = props.get("db.password")
    host, port, dbname = parse_jdbc_url(jdbc)
    conn = psycopg2.connect(host=host, port=port, dbname=dbname, user=user, password=password, sslmode="require")
    conn.autocommit = True
    return conn


def main():
    with connect_remote() as conn:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT channel_id, enabled, processing_order, max_batch_size, max_batch_to_send,
                   max_data_to_route, batch_algorithm, extract_period_millis
            FROM public.sym_channel
            ORDER BY processing_order
            """
        )
        print("channel_id | enabled | processing_order | max_batch_size | max_batch_to_send | max_data_to_route | batch_algorithm | extract_period_millis")
        for row in cur.fetchall():
            channel_id, enabled, processing_order, max_batch_size, max_batch_to_send, max_data_to_route, batch_algorithm, extract_period_millis = row
            print(f"{channel_id} | {enabled} | {processing_order} | {max_batch_size} | {max_batch_to_send} | {max_data_to_route} | {batch_algorithm} | {extract_period_millis}")
        cur.close()


if __name__ == "__main__":
    main()