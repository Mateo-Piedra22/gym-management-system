import os
from typing import List, Dict

import psycopg2


def read_properties(path: str) -> Dict[str, str]:
    props: Dict[str, str] = {}
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            if "=" in line:
                k, v = line.split("=", 1)
                props[k.strip()] = v.strip()
    return props


def parse_jdbc_url(jdbc: str):
    if not jdbc.startswith("jdbc:postgresql://"):
        raise ValueError(f"Unsupported JDBC URL: {jdbc}")
    url = jdbc[len("jdbc:postgresql://"):]
    if "?" in url:
        base, query = url.split("?", 1)
    else:
        base, query = url, ""
    if "/" not in base:
        raise ValueError(f"Invalid JDBC base (missing /dbname): {base}")
    hostport, dbname = base.split("/", 1)
    if ":" in hostport:
        host, port_str = hostport.split(":", 1)
        port = int(port_str)
    else:
        host, port = hostport, 5432
    params: Dict[str, str] = {}
    for token in query.split("&") if query else []:
        if not token:
            continue
        if "=" in token:
            k, v = token.split("=", 1)
            params[k] = v
        else:
            params[token] = ""
    return host, port, dbname, params


def connect(props_name: str):
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    props_path = os.path.join(base_dir, "symmetricds", "engines", props_name)
    props = read_properties(props_path)
    jdbc = props.get("db.url")
    user = props.get("db.user")
    password = props.get("db.password")
    host, port, dbname, params = parse_jdbc_url(jdbc)
    sslmode = params.get("sslmode", "prefer")
    conn = psycopg2.connect(host=host, port=port, dbname=dbname, user=user, password=password, sslmode=sslmode)
    conn.autocommit = True
    return conn


def get_rs_batches_remote(conn, channel: str = "default") -> List[int]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT batch_id
            FROM public.sym_incoming_batch
            WHERE channel_id = %s AND status = 'RS'
            ORDER BY batch_id DESC
            """,
            (channel,),
        )
        return [r[0] for r in cur.fetchall()]


def requeue_remote(conn, batch_ids: List[int]):
    if not batch_ids:
        return 0
    with conn.cursor() as cur:
        cur.execute("UPDATE public.sym_incoming_batch SET status = 'NE' WHERE batch_id = ANY(%s)", (batch_ids,))
        return cur.rowcount


def requeue_local(conn, batch_ids: List[int]):
    if not batch_ids:
        return 0
    with conn.cursor() as cur:
        cur.execute("UPDATE public.sym_outgoing_batch SET status = 'NE' WHERE batch_id = ANY(%s)", (batch_ids,))
        return cur.rowcount


def main():
    confirm = os.environ.get("CONFIRM", "0") in ("1", "true", "True")
    channel = os.environ.get("CHANNEL", "default")
    with connect("railway.properties") as conn_remote, connect("local.properties") as conn_local:
        rs_ids = get_rs_batches_remote(conn_remote, channel)
        print(f"RS en Railway ({channel}): {rs_ids}")
        if not rs_ids:
            return
        if not confirm:
            print("Modo dry-run. Para aplicar, exporte CONFIRM=1")
            return
        n1 = requeue_remote(conn_remote, rs_ids)
        n2 = requeue_local(conn_local, rs_ids)
        print(f"Actualizados en Railway (RS->NE): {n1}")
        print(f"Actualizados en Local (RS->NE): {n2}")


if __name__ == "__main__":
    main()