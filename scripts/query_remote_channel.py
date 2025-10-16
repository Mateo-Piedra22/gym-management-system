import os
import psycopg2

def load_props(path):
    props = {}
    with open(path, 'r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith('#'):
                continue
            if '=' in line:
                k, v = line.split('=', 1)
                props[k.strip()] = v.strip()
    return props

def connect_remote():
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    props = load_props(os.path.join(base_dir, 'symmetricds', 'engines', 'railway.properties'))
    jdbc = props['db.url']
    user = props['db.user']
    pw = props['db.password']
    url = jdbc[len('jdbc:postgresql://'):]
    base, _, _ = url.partition('?')
    hostport, _, dbname = base.partition('/')
    host, _, port = hostport.partition(':')
    port = int(port) if port else 5432
    conn = psycopg2.connect(host=host, port=port, dbname=dbname, user=user, password=pw, sslmode='require')
    conn.autocommit = True
    return conn

def main():
    channel_id = os.environ.get('CHANNEL_ID', 'default')
    conn = connect_remote()
    cur = conn.cursor()
    cur.execute("SELECT * FROM public.sym_channel WHERE channel_id=%s", (channel_id,))
    row = cur.fetchone()
    if not row:
        print(f"No channel found for id='{channel_id}'")
        return
    cols = [d.name for d in cur.description]
    for i, c in enumerate(cols):
        print(f"{c}: {row[i]}")
    cur.close()
    conn.close()

if __name__ == '__main__':
    main()