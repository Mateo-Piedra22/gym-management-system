import os
import sys
import secrets
from datetime import datetime
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
    assert jdbc.startswith("jdbc:postgresql://"), f"Unsupported JDBC URL: {jdbc}"
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
    client_sync_url = local_props.get("sync.url")
    if not client_node_id or not client_sync_url:
        raise RuntimeError("No se pudo determinar external.id o sync.url del cliente")

    with connect_from_props(railway_props_path) as conn:
        cur = conn.cursor()
        print(f"Registrando cliente en servidor: node_id={client_node_id}")
        # Crear nodo cliente si no existe
        cur.execute("SELECT 1 FROM public.sym_node WHERE node_id=%s", (client_node_id,))
        if cur.fetchone() is None:
            cur.execute(
                "INSERT INTO public.sym_node (node_id, node_group_id, external_id, sync_enabled, sync_url, deployment_type) "
                "VALUES (%s, %s, %s, %s, %s, %s)",
                (client_node_id, "client", client_node_id, 1, client_sync_url, "trigger-based")
            )
            print("[OK] sym_node insertado para cliente")
        else:
            print("[SKIP] sym_node ya existe para cliente")

        # Habilitar registro en sym_node_security
        cur.execute("SELECT 1 FROM public.sym_node_security WHERE node_id=%s", (client_node_id,))
        if cur.fetchone() is None:
            pwd = secrets.token_urlsafe(24)
            now = datetime.now()
            cur.execute(
                "INSERT INTO public.sym_node_security (node_id, node_password, registration_enabled, registration_time, initial_load_enabled) "
                "VALUES (%s, %s, %s, %s, %s)",
                (client_node_id, pwd, 1, now, 0)
            )
            print("[OK] sym_node_security insertado y registro habilitado")
        else:
            cur.execute(
                "UPDATE public.sym_node_security SET registration_enabled=1, registration_time=%s WHERE node_id=%s",
                (datetime.now(), client_node_id)
            )
            print("[OK] sym_node_security actualizado: registro habilitado")

        # Verificación de enlaces
        cur.execute(
            "SELECT source_node_group_id, target_node_group_id, data_event_action FROM public.sym_node_group_link ORDER BY 1,2"
        )
        links = cur.fetchall()
        for l in links:
            print("link:", " | ".join(str(x) for x in l))
        cur.close()
    print("Hecho. Reinicia el cliente para intentar auto-registro y carga de config.")


if __name__ == "__main__":
    main()