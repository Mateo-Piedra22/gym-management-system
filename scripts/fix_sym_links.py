import os
import sys
from typing import Dict, Tuple

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


def parse_jdbc_url(jdbc: str) -> Tuple[str, int, str]:
    if not jdbc.startswith("jdbc:postgresql://"):
        raise ValueError(f"Unsupported JDBC URL: {jdbc}")
    url = jdbc[len("jdbc:postgresql://"):]
    base = url.split("?", 1)[0]
    hostport, dbname = base.split("/", 1)
    if ":" in hostport:
        host, port_str = hostport.split(":", 1)
        port = int(port_str)
    else:
        host, port = hostport, 5432
    return host, port, dbname


def connect_from_props(props_path: str):
    props = read_properties(props_path)
    jdbc = props.get("db.url")
    user = props.get("db.user")
    password = props.get("db.password")
    if not jdbc or not user:
        raise RuntimeError(f"Faltan propiedades db.url o db.user en {props_path}")
    host, port, dbname = parse_jdbc_url(jdbc)
    conn = psycopg2.connect(host=host, port=port, dbname=dbname, user=user, password=password)
    conn.autocommit = True
    return conn


FIX_SQLS = [
    # Forzar push del cliente al servidor
    ("UPDATE public.sym_node_group_link SET data_event_action = 'P' WHERE source_node_group_id IN ('client','local') AND target_node_group_id IN ('server','railway');",
     "SELECT source_node_group_id, target_node_group_id, data_event_action FROM public.sym_node_group_link ORDER BY 1,2;"),
    # Mantener pull o wait del servidor al cliente
    ("UPDATE public.sym_node_group_link SET data_event_action = 'W' WHERE source_node_group_id IN ('server','railway') AND target_node_group_id IN ('client','local');",
     "SELECT source_node_group_id, target_node_group_id, data_event_action FROM public.sym_node_group_link ORDER BY 1,2;"),
]


def apply_fixes(tag: str, conn) -> None:
    cur = conn.cursor()
    print(f"\n== Aplicando fixes en [{tag}] ==")
    for upd, check in FIX_SQLS:
        try:
            cur.execute(upd)
            print(f"[OK] UPDATE ejecutado: {upd.split(' SET ')[0] + ' SET ...'}")
        except Exception as e:
            print(f"[ERROR] al ejecutar UPDATE en [{tag}]: {e}")
            try:
                conn.rollback()
            except Exception:
                pass
        try:
            cur.execute(check)
            rows = cur.fetchall()
            for r in rows:
                print(" | ".join(str(x) for x in r))
        except Exception as e:
            print(f"[ERROR] al consultar verificación en [{tag}]: {e}")
            try:
                conn.rollback()
            except Exception:
                pass
    try:
        cur.close()
    except Exception:
        pass


def main():
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    engines_dir = os.path.join(base_dir, "symmetricds", "engines")
    local_props = os.path.join(engines_dir, "local.properties")
    railway_props = os.path.join(engines_dir, "railway.properties")

    # Permitir override por CLI: python fix_sym_links.py <local.properties> <railway.properties>
    if len(sys.argv) >= 2:
        local_props = sys.argv[1]
    if len(sys.argv) >= 3:
        railway_props = sys.argv[2]

    # Conectar y aplicar
    try:
        with connect_from_props(local_props) as conn_local:
            apply_fixes("LOCAL", conn_local)
    except Exception as e:
        print(f"[LOCAL ERROR] {e}")

    try:
        with connect_from_props(railway_props) as conn_rw:
            apply_fixes("RAILWAY", conn_rw)
    except Exception as e:
        print(f"[RAILWAY ERROR] {e}")

    print("\nHecho. Verifica que data_event_action sea 'P' en client→server y 'W' en server→client.")


if __name__ == "__main__":
    main()