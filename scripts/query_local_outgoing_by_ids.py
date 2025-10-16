import os
import psycopg2
from pathlib import Path


def read_properties(path: str):
    d = {}
    with open(path, 'r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith('#'):
                continue
            if '=' in line:
                k,v=line.split('=',1)
                d[k.strip()] = v.strip()
    return d


def parse_jdbc(jdbc: str):
    assert jdbc.startswith('jdbc:postgresql://')
    u = jdbc[len('jdbc:postgresql://'):]
    if '?' in u:
        base, q = u.split('?',1)
    else:
        base, q = u, ''
    hostport, db = base.split('/',1)
    if ':' in hostport:
        host, port = hostport.split(':',1)
        port = int(port)
    else:
        host, port = hostport, 5432
    return host, port, db


def main():
    ids = [int(x) for x in (os.environ.get('BATCH_IDS') or '159,151,150,149,145').split(',')]
    root = Path(__file__).resolve().parent.parent
    props = read_properties(str(root / 'symmetricds' / 'engines' / 'local.properties'))
    jdbc, user, pw = props['db.url'], props['db.user'], props['db.password']
    host, port, db = parse_jdbc(jdbc)
    conn = psycopg2.connect(host=host, port=port, dbname=db, user=user, password=pw)
    cur = conn.cursor()
    cur.execute("SELECT batch_id, node_id, status, channel_id, create_time, last_update_time FROM public.sym_outgoing_batch WHERE batch_id = ANY(%s) ORDER BY batch_id", (ids,))
    rows = cur.fetchall()
    for r in rows:
        print(r)
    cur.close(); conn.close()


if __name__ == '__main__':
    main()