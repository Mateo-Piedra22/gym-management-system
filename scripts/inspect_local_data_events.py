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
            print("=== Eventos de routing para 'usuarios' (Local) ===")
            # Descubrir columnas disponibles de sym_data_event
            cur.execute(
                """
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema='public' AND table_name='sym_data_event'
                ORDER BY ordinal_position
                """
            )
            cols = [r[0] for r in cur.fetchall()]
            desired = ['data_id','batch_id','target_node_id','router_id','route_id','channel_id']
            sel = [c for c in desired if c in cols]
            if not sel:
                sel = cols
            sel_qualified = [f"de.{c}" for c in sel]
            q = f"SELECT {', '.join(sel_qualified)} FROM sym_data_event de JOIN sym_data d ON d.data_id=de.data_id WHERE d.table_name='usuarios' ORDER BY de.data_id DESC LIMIT 30"
            cur.execute(q)
            rows = cur.fetchall()
            if not rows:
                print("(no hay sym_data_event para 'usuarios')")
            else:
                for r in rows:
                    print(r)
                # Enriquecer con info del batch saliente
                batch_ids = tuple({r[1] for r in rows})
                if batch_ids:
                    print("\n=== Detalle de sym_outgoing_batch para batches anteriores ===")
                    cur.execute(
                        """
                        SELECT column_name
                        FROM information_schema.columns
                        WHERE table_schema='public' AND table_name='sym_outgoing_batch'
                        ORDER BY ordinal_position
                        """
                    )
                    out_cols = [r[0] for r in cur.fetchall()]
                    desired = ['batch_id','node_id','channel_id','status','create_time']
                    sel = [c for c in desired if c in out_cols]
                    if not sel:
                        sel = out_cols
                    placeholders = ','.join(['%s']*len(batch_ids))
                    q2 = f"SELECT {', '.join(sel)} FROM sym_outgoing_batch WHERE batch_id IN ({placeholders}) ORDER BY batch_id DESC"
                    cur.execute(q2, batch_ids)
                    for r in cur.fetchall():
                        print(r)
    finally:
        try:
            if conn:
                conn.close()
        except Exception:
            pass


if __name__ == "__main__":
    main()