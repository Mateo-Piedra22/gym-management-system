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
    local_props = os.path.join(engines, "local.properties")
    conn = None
    try:
        conn = connect_from_props(local_props)
        with conn.cursor() as cur:
            print("=== Local sym_outgoing_batch (últimos 30) ===")
            # Descubrir columnas disponibles dinámicamente
            cur.execute(
                """
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema='public' AND table_name='sym_outgoing_batch'
                ORDER BY ordinal_position
                """
            )
            out_cols = [r[0] for r in cur.fetchall()]
            desired_out = [
                'batch_id','node_id','channel_id','status','load_id','extract_time','load_time','error_flag','event_count','summary','create_time'
            ]
            out_sel = [c for c in desired_out if c in out_cols]
            if not out_sel:
                out_sel = out_cols
            q_out = f"SELECT {', '.join(out_sel)} FROM sym_outgoing_batch ORDER BY create_time DESC, batch_id DESC LIMIT 30"
            cur.execute(q_out)
            rows = cur.fetchall()
            for r in rows:
                print(r)
            print("\n=== Local sym_incoming_batch (últimos 30) ===")
            cur.execute(
                """
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema='public' AND table_name='sym_incoming_batch'
                ORDER BY ordinal_position
                """
            )
            in_cols = [r[0] for r in cur.fetchall()]
            desired_in = ['batch_id','node_id','channel_id','status','load_id','extract_time','load_time','error_flag','create_time']
            in_sel = [c for c in desired_in if c in in_cols]
            if not in_sel:
                in_sel = in_cols
            q_in = f"SELECT {', '.join(in_sel)} FROM sym_incoming_batch ORDER BY create_time DESC, batch_id DESC LIMIT 30"
            cur.execute(q_in)
            rows = cur.fetchall()
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