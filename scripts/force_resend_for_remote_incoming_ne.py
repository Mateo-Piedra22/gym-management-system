import os
from typing import List, Dict, Tuple

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


def parse_jdbc_url(jdbc: str) -> Tuple[str, int, str, Dict[str, str]]:
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


def list_remote_incoming_ne(conn, channel: str = "default") -> List[int]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT batch_id
            FROM public.sym_incoming_batch
            WHERE channel_id = %s AND status = 'NE'
            ORDER BY batch_id DESC
            """,
            (channel,),
        )
        return [r[0] for r in cur.fetchall()]


def mark_local_outgoing_rs(conn, batch_ids: List[int]) -> int:
    if not batch_ids:
        return 0
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE public.sym_outgoing_batch
            SET status = 'RS'
            WHERE batch_id = ANY(%s)
            """,
            (batch_ids,),
        )
        return cur.rowcount


def main():
    channel = os.environ.get("CHANNEL", "default")
    confirm = os.environ.get("CONFIRM", "0") in ("1", "true", "True")
    with connect("railway.properties") as conn_remote, connect("local.properties") as conn_local:
        ne_ids = list_remote_incoming_ne(conn_remote, channel)
        print(f"NE en Railway ({channel}): {ne_ids}")
        if not ne_ids:
            return
        if not confirm:
            print("Modo dry-run. Para aplicar, exporte CONFIRM=1")
            return
        updated = mark_local_outgoing_rs(conn_local, ne_ids)
        print(f"Marcados en Local (OUT status -> RS): {updated}")


if __name__ == "__main__":
    main()