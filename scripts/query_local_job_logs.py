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
    root = Path(__file__).resolve().parent.parent
    props = read_properties(str(root / 'symmetricds' / 'engines' / 'local.properties'))
    jdbc, user, pw = props['db.url'], props['db.user'], props['db.password']
    host, port, db = parse_jdbc(jdbc)
    conn = psycopg2.connect(host=host, port=port, dbname=db, user=user, password=pw)
    cur = conn.cursor()
    print("=== Columnas de sym_job (cliente) ===")
    cur.execute("SELECT column_name FROM information_schema.columns WHERE table_schema='public' AND table_name='sym_job' ORDER BY ordinal_position")
    cols = [r[0] for r in cur.fetchall()]
    print(', '.join(cols))
    print("\n=== sym_job (cliente) filas ===")
    cur.execute("SELECT * FROM public.sym_job ORDER BY job_name")
    for r in cur.fetchall():
        print(r)
    print("\n=== Estadísticas (si existen) para 'Push' ===")
    try:
        cur.execute("SELECT job_name, processed_count, total_time_ms, average_time_ms, last_run_time FROM public.sym_job_stat WHERE lower(job_name) LIKE 'push%' ORDER BY last_run_time DESC LIMIT 5")
        stats = cur.fetchall()
        if stats:
            print("\n=== sym_job_stat (push) ===")
            for s in stats:
                print(s)
    except Exception:
        pass
    cur.close(); conn.close()


if __name__ == '__main__':
    main()