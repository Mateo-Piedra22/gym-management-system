import os
import re
from urllib.parse import urlparse, parse_qs
import time
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
    # Example: jdbc:postgresql://host:port/database?sslmode=require
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


def insert_local_etiqueta(name: str, color: str):
    local_props = os.path.join("symmetricds", "engines", "local.properties")
    conn = connect_from_props(local_props)
    try:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO etiquetas (nombre, color) VALUES (%s, %s) RETURNING id",
                (name, color)
            )
            new_id = cur.fetchone()[0]
        conn.commit()
        print(f"Inserted local etiquetas id={new_id}")
        return new_id
    finally:
        try:
            conn.close()
        except Exception:
            pass


def check_outgoing_batches_local():
    local_props = os.path.join("symmetricds", "engines", "local.properties")
    conn = connect_from_props(local_props)
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT batch_id, status, channel_id, node_id, error_flag
                FROM sym_outgoing_batch
                ORDER BY batch_id DESC
                LIMIT 5
                """
            )
            rows = cur.fetchall()
            print("Recent local outgoing batches:")
            for r in rows:
                print(r)
    finally:
        try:
            conn.close()
        except Exception:
            pass


def verify_remote_row(name: str):
    # Read remote props from symmetricds/engines/railway.properties
    remote_props = os.path.join("symmetricds", "engines", "railway.properties")
    conn = connect_from_props(remote_props)
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT id, nombre, color FROM etiquetas WHERE nombre=%s ORDER BY id DESC LIMIT 1", (name,))
            row = cur.fetchone()
            if row:
                print(f"Remote row present: {row}")
                return True
            else:
                print("Remote row not found yet.")
                return False
    finally:
        try:
            conn.close()
        except Exception:
            pass


if __name__ == "__main__":
    test_name = f"sync_test_{int(time.time())}"
    test_color = "#00AAFF"
    insert_local_etiqueta(test_name, test_color)
    # Give SymmetricDS a moment to route/push
    time.sleep(2)
    check_outgoing_batches_local()
    # Poll remote up to ~20 seconds
    ok = False
    for _ in range(10):
        if verify_remote_row(test_name):
            ok = True
            break
        time.sleep(2)
    print("Replication OK" if ok else "Replication NOT observed")