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
    if not jdbc.startswith('jdbc:postgresql://'):
        raise ValueError(f'Unsupported JDBC URL: {jdbc}')
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
    jdbc = props.get('db.url', '')
    user = props.get('db.user', 'postgres')
    pwd = props.get('db.password') or os.environ.get('PGPASSWORD') or ''
    host, port, dbname, params = parse_jdbc(jdbc)
    sslmode = params.get('sslmode', 'require')
    conn = psycopg2.connect(host=host, port=port, dbname=dbname, user=user, password=pwd, sslmode=sslmode)
    conn.autocommit = True
    return conn


def main():
    conn = connect_remote()
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT b.batch_id, b.node_id, b.channel_id, b.status,
                   e.failed_line_number,
                   e.target_table_name,
                   e.event_type,
                   e.row_data,
                   e.old_data,
                   e.cur_data
            FROM public.sym_incoming_batch b
            LEFT JOIN public.sym_incoming_error e ON e.batch_id = b.batch_id
            WHERE b.status IN ('NE','ER')
            ORDER BY b.batch_id DESC
            LIMIT 50
            """
        )
        rows = cur.fetchall()
        print('batch_id | node_id | channel_id | status | failed_line | table | event | row_data')
        for r in rows:
            batch_id, node_id, channel_id, status, failed_line, table_name, event_type, row_data, old_data, cur_data = r
            # Prefer row_data; fall back to cur_data/old_data
            sample = (row_data or cur_data or old_data or '')
            sample = sample.replace('\n', ' ').replace('\r', ' ')
            print(f"{batch_id} | {node_id} | {channel_id} | {status} | {failed_line or ''} | {table_name or ''} | {event_type or ''} | {sample}".strip())


if __name__ == '__main__':
    main()