import os
import re
import time
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


def count_usuarios(conn):
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM usuarios")
        return cur.fetchone()[0]


def next_usuario_id(conn):
    with conn.cursor() as cur:
        cur.execute("SELECT COALESCE(MAX(id), 0) + 1 FROM usuarios")
        return cur.fetchone()[0]


def ensure_usuario(conn, uid, nombre, telefono="000-000", pin="1234"):
    with conn.cursor() as cur:
        cur.execute("SELECT id FROM usuarios WHERE id = %s", (uid,))
        if cur.fetchone() is None:
            cur.execute(
                """
                INSERT INTO usuarios (id, nombre, telefono, pin, rol, activo)
                VALUES (%s, %s, %s, %s, 'socio', TRUE)
                """,
                (uid, nombre, telefono, pin),
            )


def exists_usuario(conn, uid):
    with conn.cursor() as cur:
        cur.execute("SELECT 1 FROM usuarios WHERE id = %s", (uid,))
        return cur.fetchone() is not None


def main():
    base = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    engines = os.path.join(base, "symmetricds", "engines")
    local_props = os.path.join(engines, "local.properties")
    remote_props = os.path.join(engines, "railway.properties")

    lc = rc = None
    try:
        lc = connect_from_props(local_props)
        rc = connect_from_props(remote_props)
        lc.autocommit = True
        rc.autocommit = True

        local_cnt = count_usuarios(lc)
        remote_cnt = count_usuarios(rc)
        print(f"Usuarios: local={local_cnt}, railway={remote_cnt}")

        # Calcular IDs únicos considerando ambos nodos para evitar colisiones
        def next_unique_id(conn_a, conn_b):
            with conn_a.cursor() as ca, conn_b.cursor() as cb:
                ca.execute("SELECT COALESCE(MAX(id), 0) FROM usuarios")
                a_max = ca.fetchone()[0] or 0
                cb.execute("SELECT COALESCE(MAX(id), 0) FROM usuarios")
                b_max = cb.fetchone()[0] or 0
                return max(a_max, b_max) + 1

        # Inserta en remoto y valida réplica en local usando ID único
        rid = next_unique_id(rc, lc)
        ensure_usuario(rc, rid, f"sync_remote_{int(time.time())}")
        print(f"Insertado en Railway usuario id={rid}")

        # Poll en local
        ok_remote_to_local = False
        for _ in range(20):
            if exists_usuario(lc, rid):
                ok_remote_to_local = True
                break
            time.sleep(1.5)
        print("Ré plica Railway->Local OK" if ok_remote_to_local else "No se observó réplica Railway->Local")

        # Inserta en local y valida réplica en remoto usando ID único
        lid = next_unique_id(lc, rc)
        ensure_usuario(lc, lid, f"sync_local_{int(time.time())}")
        print(f"Insertado en Local usuario id={lid}")

        ok_local_to_remote = False
        for _ in range(20):
            if exists_usuario(rc, lid):
                ok_local_to_remote = True
                break
            time.sleep(1.5)
        print("Ré plica Local->Railway OK" if ok_local_to_remote else "No se observó réplica Local->Railway")

        # Conteos finales
        local_cnt2 = count_usuarios(lc)
        remote_cnt2 = count_usuarios(rc)
        print(f"Usuarios tras operaciones: local={local_cnt2}, railway={remote_cnt2}")

    finally:
        for c in (lc, rc):
            try:
                if c:
                    c.close()
            except Exception:
                pass


if __name__ == "__main__":
    main()