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
    base, _, query = url.partition("?")
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
        print("sym_channel (default):")
        cur.execute("SELECT channel_id, enabled FROM public.sym_channel WHERE channel_id='default'")
        print(cur.fetchall())

        print("\nsym_trigger for usuarios:")
        cur.execute("SELECT trigger_id, channel_id, sync_on_insert, sync_on_update, sync_on_delete FROM public.sym_trigger WHERE source_table_name='usuarios'")
        print(cur.fetchall())

        print("\nsym_router (top 10):")
        cur.execute("SELECT router_id, router_type, source_node_group_id, target_node_group_id FROM public.sym_router ORDER BY router_id LIMIT 10")
        for r in cur.fetchall():
            print(" | ".join(str(x) for x in r))

        print("\nsym_trigger_router for usuarios:")
        cur.execute("SELECT trigger_id, router_id, initial_load_select, ping_back_enabled FROM public.sym_trigger_router WHERE trigger_id IN (SELECT trigger_id FROM public.sym_trigger WHERE source_table_name='usuarios')")
        print(cur.fetchall())
        cur.close()


if __name__ == "__main__":
    main()