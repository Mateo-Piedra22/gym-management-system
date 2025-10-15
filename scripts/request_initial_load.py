import os
import sys
from datetime import datetime
import psycopg2

from register_client_on_server import read_properties, parse_jdbc_url


def connect_from_props(props_path: str):
    props = read_properties(props_path)
    jdbc = props.get("db.url")
    user = props.get("db.user")
    password = props.get("db.password")
    host, port, dbname = parse_jdbc_url(jdbc)
    conn = psycopg2.connect(host=host, port=port, dbname=dbname, user=user, password=password)
    conn.autocommit = True
    return conn


def main():
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    engines_dir = os.path.join(base_dir, "symmetricds", "engines")
    local_props_path = os.path.join(engines_dir, "local.properties")
    railway_props_path = os.path.join(engines_dir, "railway.properties")

    if len(sys.argv) >= 2:
        local_props_path = sys.argv[1]
    if len(sys.argv) >= 3:
        railway_props_path = sys.argv[2]

    local_props = read_properties(local_props_path)
    client_node_id = local_props.get("external.id")
    if not client_node_id:
        raise RuntimeError("No se pudo obtener external.id del cliente")

    with connect_from_props(railway_props_path) as conn:
        cur = conn.cursor()
        print(f"Habilitando carga inicial para: {client_node_id}")
        cur.execute(
            "UPDATE public.sym_node_security SET initial_load_enabled=1, initial_load_time=%s WHERE node_id=%s",
            (datetime.now(), client_node_id)
        )
        print("[OK] sym_node_security actualizado con initial_load_enabled=1")

        # La carga inicial se disparará por el job de Initial Load al ver initial_load_enabled=1.
        # No insertamos peticiones manuales en sym_table_reload_request porque requiere trigger/router específicos.
        cur.close()
    print("Hecho. Reinicia/observa el servidor para que ejecute la carga inicial.")


if __name__ == "__main__":
    main()