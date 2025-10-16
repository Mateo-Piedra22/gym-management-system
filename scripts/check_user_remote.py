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
    uid = int(os.environ.get("USER_ID", "0"))
    if uid <= 0:
        print("USER_ID inválido")
        return
    base = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    engines = os.path.join(base, "symmetricds", "engines")
    remote_props = os.path.join(engines, "railway.properties")
    conn = None
    try:
        conn = connect_from_props(remote_props)
        with conn.cursor() as cur:
            cur.execute("SELECT id, nombre FROM usuarios WHERE id=%s", (uid,))
            row = cur.fetchone()
            if row:
                print(f"Encontrado en Railway: id={row[0]}, nombre={row[1]}")
            else:
                print("No existe en Railway")
    finally:
        try:
            if conn:
                conn.close()
        except Exception:
            pass


if __name__ == "__main__":
    main()