import psycopg2
import re
from urllib.parse import parse_qs
from pathlib import Path


def load_props(path):
    props = {}
    for line in Path(path).read_text(encoding='utf-8').splitlines():
        line = line.strip()
        if not line or line.startswith('#'):
            continue
        if '=' in line:
            k, v = line.split('=', 1)
            props[k.strip()] = v.strip()
    return props


def jdbc_to_conn_kwargs(jdbc_url: str) -> dict:
    m = re.match(r"jdbc:postgresql://([^:/?#]+):(\d+)/([\w-]+)(?:\?(.*))?", jdbc_url)
    if not m:
        raise ValueError(f"Unrecognized JDBC: {jdbc_url}")
    host, port, db, qs = m.group(1), int(m.group(2)), m.group(3), m.group(4)
    kwargs = {"host": host, "port": port, "dbname": db}
    if qs:
        q = parse_qs(qs)
        sslmode = q.get("sslmode", [None])[0]
        if sslmode:
            kwargs["sslmode"] = sslmode
    return kwargs


def main():
    base = Path(__file__).resolve().parent.parent
    props = load_props(base / 'symmetricds' / 'engines' / 'railway.properties')
    jdbc = props.get('db.url')
    user = props.get('db.user', 'postgres')
    pwd = props.get('db.password', '')
    kwargs = jdbc_to_conn_kwargs(jdbc)
    conn = psycopg2.connect(user=user, password=pwd, **kwargs)
    cur = conn.cursor()
    print("sym_node_security for local client:")
    cur.execute(
        """
        SELECT node_id, node_password, registration_enabled, initial_load_enabled,
               initial_load_time, registration_time
        FROM public.sym_node_security
        WHERE node_id IN (
            SELECT node_id FROM public.sym_node WHERE external_id LIKE 'local-%'
        )
        ORDER BY registration_time DESC NULLS LAST
        """
    )
    for r in cur.fetchall():
        print(r)
    print("\nCurrent sym_node entries:")
    cur.execute(
        """
        SELECT node_id, external_id, node_group_id, sync_url
        FROM public.sym_node
        WHERE external_id LIKE 'local-%'
        ORDER BY node_id
        """
    )
    for r in cur.fetchall():
        print(r)
    cur.close()
    conn.close()


if __name__ == '__main__':
    main()