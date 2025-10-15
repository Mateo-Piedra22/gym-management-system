import os
import sys
from typing import Dict, Tuple, List

import psycopg2
from psycopg2 import sql as psy_sql


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
    # Expected: jdbc:postgresql://host:port/dbname?key=value&key2=value2
    if not jdbc.startswith("jdbc:postgresql://"):
        raise ValueError(f"Unsupported JDBC URL: {jdbc}")
    url = jdbc[len("jdbc:postgresql://"):]
    # Split query params
    if "?" in url:
        base, query = url.split("?", 1)
    else:
        base, query = url, ""
    # Split host:port/db
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


def connect_from_props(props_path: str):
    props = read_properties(props_path)
    jdbc = props.get("db.url")
    user = props.get("db.user")
    password = props.get("db.password")
    if not jdbc or not user:
        raise RuntimeError(f"Faltan propiedades db.url o db.user en {props_path}")
    host, port, dbname, params = parse_jdbc_url(jdbc)
    sslmode = params.get("sslmode", "prefer")
    conn = psycopg2.connect(host=host, port=port, dbname=dbname, user=user, password=password, sslmode=sslmode)
    # Evitar que un error aborte la transacción y bloquee los siguientes SELECT
    conn.autocommit = True
    return conn


QUERIES: List[Tuple[str, str]] = [
    ("sym_node", "SELECT * FROM public.sym_node ORDER BY node_group_id, node_id;"),
    ("sym_node_group", "SELECT * FROM public.sym_node_group ORDER BY node_group_id;"),
    ("sym_node_group_link", "SELECT * FROM public.sym_node_group_link ORDER BY source_node_group_id, target_node_group_id;"),
    # Algunas versiones no tienen la columna 'enabled' en sym_router
    ("sym_router", "SELECT router_id, source_node_group_id, target_node_group_id, router_type FROM public.sym_router ORDER BY router_id;"),
    # Para evitar errores de schema, consultamos columnas seguras
    ("sym_channel", "SELECT channel_id, processing_order FROM public.sym_channel ORDER BY channel_id;"),
    ("sym_trigger", "SELECT trigger_id, source_table_name, channel_id, sync_on_insert, sync_on_update, sync_on_delete FROM public.sym_trigger ORDER BY trigger_id;"),
    ("sym_trigger_router", "SELECT trigger_id, router_id, initial_load_order FROM public.sym_trigger_router ORDER BY trigger_id, router_id;"),
    ("sym_table_reload_request", "SELECT * FROM public.sym_table_reload_request ORDER BY create_time DESC LIMIT 100;"),
    # Evitar columnas no presentes segun versión (sin extract_time)
    # Columnas seguras (presentes en múltiples versiones): sin load_time/extract_time
    ("sym_outgoing_batch", "SELECT batch_id, node_id, status, channel_id, create_time FROM public.sym_outgoing_batch ORDER BY batch_id DESC LIMIT 100;"),
    ("sym_incoming_batch", "SELECT batch_id, node_id, status, channel_id, create_time FROM public.sym_incoming_batch ORDER BY batch_id DESC LIMIT 100;"),
    ("sym_node_security", "SELECT * FROM public.sym_node_security ORDER BY node_id;"),
    ("sym_node_identity", "SELECT * FROM public.sym_node_identity;")
]


def format_row(row: Tuple, headers: List[str]) -> str:
    def fmt(x):
        if x is None:
            return "NULL"
        return str(x)
    return " | ".join(fmt(x) for x in row)


def run_queries(tag: str, conn, out_lines: List[str]):
    for name, query in QUERIES:
        cur = conn.cursor()
        out_lines.append(f"\n=== [{tag}] {name} ===")
        out_lines.append(f"-- {query}")
        try:
            cur.execute(query)
            headers = [d.name for d in cur.description]
            out_lines.append(" | ".join(headers))
            rows = cur.fetchall()
            if not rows:
                out_lines.append("(sin filas)")
            else:
                for row in rows:
                    out_lines.append(format_row(row, headers))
        except Exception as e:
            out_lines.append(f"[ERROR] {e}")
            # Si la conexión quedó en estado abortado, limpiar con rollback
            try:
                conn.rollback()
            except Exception:
                pass
        finally:
            try:
                cur.close()
            except Exception:
                pass


def main():
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    engines_dir = os.path.join(base_dir, "symmetricds", "engines")
    local_props = os.path.join(engines_dir, "local.properties")
    railway_props = os.path.join(engines_dir, "railway.properties")

    # Permitir override por CLI: python dump_sym_tables.py <local.properties> <railway.properties>
    if len(sys.argv) >= 2:
        local_props = sys.argv[1]
    if len(sys.argv) >= 3:
        railway_props = sys.argv[2]

    outputs: List[str] = []
    # Local
    try:
        with connect_from_props(local_props) as conn_local:
            run_queries("LOCAL", conn_local, outputs)
    except Exception as e:
        outputs.append(f"\n[LOCAL ERROR] {e}")

    # Railway
    try:
        with connect_from_props(railway_props) as conn_rw:
            run_queries("RAILWAY", conn_rw, outputs)
    except Exception as e:
        outputs.append(f"\n[RAILWAY ERROR] {e}")

    # Print to stdout
    print("\n".join(outputs))

    # Save to file for convenience
    logs_dir = os.path.join(base_dir, "symmetricds", "logs")
    os.makedirs(logs_dir, exist_ok=True)
    out_path = os.path.join(logs_dir, "sym_dump.txt")
    try:
        with open(out_path, "w", encoding="utf-8") as f:
            f.write("\n".join(outputs))
        print(f"\nGuardado en: {out_path}")
    except Exception as e:
        print(f"No se pudo guardar salida en archivo: {e}")


if __name__ == "__main__":
    main()