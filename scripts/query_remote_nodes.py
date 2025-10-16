import os
import psycopg2
from typing import Dict


def read_properties(path: str) -> Dict[str, str]:
    props: Dict[str, str] = {}
    with open(path, 'r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith('#'):
                continue
            if '=' in line:
                k, v = line.split('=', 1)
                props[k.strip()] = v.strip()
    return props


def parse_jdbc(jdbc: str):
    url = jdbc[len('jdbc:postgresql://'):]
    base, _, query = url.partition('?')
    hostport, _, dbname = base.partition('/')
    host, _, port_str = hostport.partition(':')
    port = int(port_str) if port_str else 5432
    params: Dict[str, str] = {}
    if query:
        for token in query.split('&'):
            if not token:
                continue
            if '=' in token:
                k, v = token.split('=', 1)
                params[k] = v
            else:
                params[token] = ''
    return host, port, dbname, params


def connect_remote():
    base = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    props_path = os.path.join(base, 'symmetricds', 'engines', 'railway.properties')
    props = read_properties(props_path)
    jdbc = props.get('db.url')
    user = props.get('db.user')
    pwd = props.get('db.password') or os.environ.get('PGPASSWORD') or ''
    host, port, dbname, params = parse_jdbc(jdbc)
    sslmode = params.get('sslmode', 'require')
    conn = psycopg2.connect(host=host, port=port, dbname=dbname, user=user, password=pwd, sslmode=sslmode)
    conn.autocommit = True
    return conn


def main():
    with connect_remote() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT n.node_id, n.node_group_id, n.external_id, n.sync_url,
                       s.lock_enabled, s.lock_time, s.unlimited, s.node_password
                FROM public.sym_node n
                LEFT JOIN public.sym_node_security s ON s.node_id = n.node_id
                ORDER BY n.node_id
                """
            )
            print('node_id | group_id | external_id | sync_url | lock_enabled | unlimited')
            for row in cur.fetchall():
                node_id, group_id, external_id, sync_url, lock_enabled, lock_time, unlimited, node_password = row
                print(f"{node_id} | {group_id} | {external_id} | {sync_url or ''} | {lock_enabled} | {unlimited}")


if __name__ == '__main__':
    main()