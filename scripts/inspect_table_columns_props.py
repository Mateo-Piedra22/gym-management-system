import os
from typing import Dict

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


def connect_props(props_name: str):
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    props_path = os.path.join(base_dir, "symmetricds", "engines", props_name)
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


def inspect_columns(conn, table: str):
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT column_name, data_type, is_nullable, column_default
            FROM information_schema.columns
            WHERE table_schema='public' AND table_name=%s
            ORDER BY ordinal_position
            """,
            (table,)
        )
        rows = cur.fetchall()
        if not rows:
            print(f"No columns found for table '{table}'")
            return
        for name, dtype, nullable, default in rows:
            nn = 'NULL' if nullable == 'YES' else 'NOT NULL'
            dv = default if default is not None else ''
            print(f"{name} | {dtype} | {nn} | {dv}")


def main():
    table = os.environ.get("TABLE", "usuarios")
    props = os.environ.get("PROPS", "railway.properties")
    with connect_props(props) as conn:
        inspect_columns(conn, table)


if __name__ == "__main__":
    main()