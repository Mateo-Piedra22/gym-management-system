import os
import re
from urllib.parse import parse_qs
import psycopg2


def parse_properties(path: str) -> dict:
    props = {}
    try:
        with open(path, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith('#'):
                    continue
                if '=' in line:
                    k, v = line.split('=', 1)
                    props[k.strip()] = v.strip()
    except Exception:
        pass
    return props


def jdbc_to_conn_kwargs(jdbc_url: str) -> dict:
    m = re.match(r"jdbc:postgresql://([^:/?#]+):(\d+)/(\w+)(?:\?(.*))?", jdbc_url)
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


def connect_from_props(props_path: str) -> psycopg2.extensions.connection:
    props = parse_properties(props_path)
    jdbc = props.get("db.url", "")
    kwargs = jdbc_to_conn_kwargs(jdbc)
    user = props.get("db.user", "postgres")
    pwd = props.get("db.password") or os.environ.get("PGPASSWORD") or ""
    return psycopg2.connect(user=user, password=pwd, **kwargs)


def main():
    base = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    engines = os.path.join(base, "symmetricds", "engines")
    remote_props = os.path.join(engines, "railway.properties")

    conn = None
    try:
        conn = connect_from_props(remote_props)
        conn.autocommit = True
        with conn.cursor() as cur:
            print("=== sym_incoming_error (últimos 20) ===")
            # Descubrir columnas disponibles dinámicamente
            cur.execute(
                """
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema='public' AND table_name='sym_incoming_error'
                ORDER BY ordinal_position
                """
            )
            cols = [r[0] for r in cur.fetchall()]
            desired = [
                'batch_id', 'failed_row_number', 'failed_line_number',
                'event_type', 'error_message', 'row_data', 'binary_data'
            ]
            sel = [c for c in desired if c in cols]
            if not sel:
                sel = cols  # fallback: selecciona todas
            q = f"SELECT {', '.join(sel)} FROM sym_incoming_error ORDER BY batch_id DESC, failed_row_number DESC LIMIT 20"
            cur.execute(q)
            rows = cur.fetchall()
            if not rows:
                print("(sin errores)")
            else:
                for r in rows:
                    print(r)
    finally:
        try:
            if conn:
                conn.close()
        except Exception:
            pass


if __name__ == "__main__":
    main()