import os
import psycopg2
from pathlib import Path


def read_properties(path: str):
    d = {}
    with open(path, 'r', encoding='utf-8') as f:
        for line in f:
            line=line.strip()
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
        base, _ = u.split('?',1)
    else:
        base = u
    hostport, db = base.split('/',1)
    if ':' in hostport:
        host, port = hostport.split(':',1); port=int(port)
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
    cur.execute("SELECT column_name FROM information_schema.columns WHERE table_schema='public' AND table_name='sym_outgoing_batch' ORDER BY ordinal_position")
    cols = [r[0] for r in cur.fetchall()]
    wanted = ['batch_id','channel_id','status','extract_count','data_row_count','byte_count','summary']
    select_cols = [c for c in wanted if c in cols]
    cur.execute(f"SELECT {', '.join(select_cols)} FROM public.sym_outgoing_batch WHERE batch_id = ANY(%s) ORDER BY batch_id", (ids,))
    rows = cur.fetchall()
    print(' | '.join(select_cols))
    for r in rows:
        print(' | '.join(str(x) if x is not None else 'NULL' for x in r))
    cur.close(); conn.close()


if __name__ == '__main__':
    main()