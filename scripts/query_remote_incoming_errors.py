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


def connect_railway():
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    props_path = os.path.join(base_dir, "symmetricds", "engines", "railway.properties")
    props = read_properties(props_path)
    jdbc = props.get("db.url")
    user = props.get("db.user")
    password = props.get("db.password")
    if not jdbc or not user:
        raise RuntimeError(f"Faltan propiedades db.url o db.user en {props_path}")
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
        WHERE table_schema = 'public' AND table_name = %s
        ORDER BY ordinal_position
        """,
        (table,)
    )
    return [r[0] for r in cur.fetchall()]


def main():
    batch_ids_env = os.environ.get("BATCH_IDS", "").strip()
    table_filter = os.environ.get("TABLE", "").strip()
    ids: List[int] = []
    if batch_ids_env:
        for tok in batch_ids_env.split(","):
            tok = tok.strip()
            if tok:
                try:
                    ids.append(int(tok))
                except ValueError:
                    pass

    with connect_railway() as conn:
        cur = conn.cursor()
        cols = get_columns(cur, "sym_incoming_error")
        # Campos comunes bajo distintas versiones
        wanted = [
            "batch_id",
            "row_number",
            "target_table_name",
            "event_type",
            "short_error_message",
            "error_message",
            "error_line",
            "error_detail",
            "create_time",
        ]
        select_cols = [c for c in wanted if c in cols]
        if not select_cols:
            print("sym_incoming_error: no hay columnas reconocibles para mostrar")
            return

        base_sql = f"SELECT {', '.join(select_cols)} FROM public.sym_incoming_error"
        where_clauses = []
        params: List = []
        if ids:
            where_clauses.append("batch_id = ANY(%s)")
            params.append(ids)
        if table_filter:
            if "target_table_name" in cols:
                where_clauses.append("target_table_name = %s")
                params.append(table_filter)
        if where_clauses:
            base_sql += " WHERE " + " AND ".join(where_clauses)
        # ORDER BY usando columnas disponibles
        order_parts: List[str] = []
        if "batch_id" in select_cols:
            order_parts.append("batch_id DESC")
        if "row_number" in select_cols:
            order_parts.append("row_number ASC")
        elif "create_time" in select_cols:
            order_parts.append("create_time ASC")
        if order_parts:
            base_sql += " ORDER BY " + ", ".join(order_parts)

        try:
            cur.execute(base_sql, tuple(params))
            rows = cur.fetchall()
            print(" | ".join(select_cols))
            if not rows:
                print("(sin errores)")
            else:
                for r in rows:
                    print(" | ".join(str(x) if x is not None else "NULL" for x in r))
        except Exception as e:
            print(f"[ERROR] {e}")
        finally:
            cur.close()


if __name__ == "__main__":
    main()