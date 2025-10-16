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


def connect_local():
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    props_path = os.path.join(base_dir, "symmetricds", "engines", "local.properties")
    props = read_properties(props_path)
    jdbc = props.get("db.url")
    user = props.get("db.user")
    password = props.get("db.password")
    host, port, dbname, params = parse_jdbc_url(jdbc)
    sslmode = params.get("sslmode", "prefer")
    conn = psycopg2.connect(host=host, port=port, dbname=dbname, user=user, password=password, sslmode=sslmode)
    conn.autocommit = True
    return conn


def get_columns(cur, table: str) -> List[str]:
    cur.execute(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema='public' AND table_name=%s
        ORDER BY ordinal_position
        """,
        (table,)
    )
    return [r[0] for r in cur.fetchall()]


def main():
    target_id = os.environ.get("USER_ID")
    name_like = os.environ.get("NAME_LIKE", "sync_local_")
    with connect_local() as conn:
        cur = conn.cursor()
        data_cols = get_columns(cur, "sym_data")
        de_cols = get_columns(cur, "sym_data_event")
        og_cols = get_columns(cur, "sym_outgoing_batch")
        print("sym_data columns:", ", ".join(data_cols))
        print("sym_data_event columns:", ", ".join(de_cols))
        print("sym_outgoing_batch columns:", ", ".join(og_cols))

        where_parts: List[str] = ["d.table_name = %s", "d.event_type = 'I'"]
        params: List = ["usuarios"]

        select_cols = ["d.data_id", "d.event_type"]
        if "pk_data" in data_cols:
            select_cols.append("d.pk_data")
        if "row_data" in data_cols:
            select_cols.append("d.row_data")
        sql = f"SELECT {', '.join(select_cols)}, de.batch_id FROM public.sym_data d LEFT JOIN public.sym_data_event de ON de.data_id = d.data_id WHERE " + " AND ".join(where_parts) + " ORDER BY d.data_id DESC LIMIT 20"
        cur.execute(sql, tuple(params))
        rows = cur.fetchall()
        if not rows:
            print("(sin eventos para usuarios)")
        else:
            print("data_id | event_type | batch_id | pk_data | row_data")
            for r in rows:
                # Pad missing pk/row data for print
                out = list(r)
                while len(out) < 5:
                    out.append(None)
                print(" | ".join(str(x) if x is not None else "NULL" for x in out))
            # Consultar info de lotes
            batch_ids = [r[len(select_cols)] for r in rows if r[len(select_cols)] is not None]
            if batch_ids:
                cols = [c for c in (
                    "batch_id",
                    "node_id",
                    "status",
                    "channel_id",
                    "data_row_count",
                    "create_time",
                    "extract_start_time",
                    "transfer_start_time",
                    "load_start_time",
                    "summary",
                    "error_flag",
                    "ignore_count",
                    "sql_state",
                    "sql_code",
                    "sql_message",
                    "sent_count",
                    "load_count",
                    "failed_data_id",
                    "failed_line_number"
                ) if c in og_cols]
                cur.execute(f"SELECT {', '.join(cols)} FROM public.sym_outgoing_batch WHERE batch_id = ANY(%s) ORDER BY batch_id DESC", (batch_ids,))
                og_rows = cur.fetchall()
                print("\nOutgoing batch info:")
                print(" | ".join(cols))
                for orow in og_rows:
                    print(" | ".join(str(x) if x is not None else "NULL" for x in orow))

        cur.close()


if __name__ == "__main__":
    main()