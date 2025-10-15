import os
import psycopg2

from register_client_on_server import read_properties, parse_jdbc_url


def connect_local():
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    engines_dir = os.path.join(base_dir, "symmetricds", "engines")
    local_props_path = os.path.join(engines_dir, "local.properties")
    props = read_properties(local_props_path)
    jdbc = props.get("db.url")
    user = props.get("db.user")
    password = props.get("db.password")
    host, port, dbname = parse_jdbc_url(jdbc)
    conn = psycopg2.connect(host=host, port=port, dbname=dbname, user=user, password=password)
    conn.autocommit = True
    return conn


def main():
    with connect_local() as conn:
        cur = conn.cursor()
        print("[FORCE] Eliminando sym_node_identity en cliente para re-registro…")
        cur.execute("DELETE FROM public.sym_node_identity")
        print("[OK] sym_node_identity eliminado")
        # Opcional: limpiar cualquier rastro de nodo servidor en cliente
        cur.execute("DELETE FROM public.sym_node WHERE node_id <> (SELECT external_id FROM public.sym_node LIMIT 1)")
        print("[OK] sym_node (no local) limpiado si existía")
        cur.close()
    print("Hecho. Reinicia el motor SymmetricDS para que el cliente se re-registre.")


if __name__ == "__main__":
    main()