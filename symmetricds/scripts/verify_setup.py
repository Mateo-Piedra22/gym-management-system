"""
Verifica automáticamente el estado básico de configuración SymmetricDS en
Railway (server) y Local (client): routers, triggers y timestamps.

Usa `psql` (no requiere librerías adicionales). Lee defaults de `config/config.json`
y contraseñas desde Keyring/env igual que `apply_setup.py`.

Uso:
  python symmetricds/scripts/verify_setup.py \
    --railway-host shuttle.proxy.rlwy.net --railway-port 5432 \
    --railway-db railway --railway-user postgres \
    --local-host localhost --local-port 5432 \
    --local-db gimnasio --local-user postgres
"""

import os
import sys
import shutil
from pathlib import Path
import argparse
import json
import subprocess

try:
    import keyring  # type: ignore
except Exception:
    keyring = None  # type: ignore

try:
    from config import KEYRING_SERVICE_NAME
except Exception:
    KEYRING_SERVICE_NAME = "GymMS_DB"


def resolve_password(user: str, host: str, port: int, fallback: str = "") -> str:
    """Resuelve contraseña desde keyring o variables de entorno."""
    if keyring is not None:
        candidates = [f"{user}@{host}:{port}", f"{user}@{host}", f"{user}"]
        for account in candidates:
            try:
                pwd = keyring.get_password(KEYRING_SERVICE_NAME, account)
            except Exception:
                pwd = None
            if pwd:
                return pwd
    for env_key in ("PGPASSWORD", "POSTGRES_PASSWORD", "DB_PASSWORD", "PG_PASS", "DATABASE_PASSWORD"):
        val = os.getenv(env_key)
        if val:
            return val
    return fallback


def require_psql() -> str:
    p = shutil.which("psql")
    if not p:
        cand = Path("C:/Program Files/PostgreSQL").glob("*/bin/psql.exe")
        try:
            p = next(cand, None)
        except Exception:
            p = None
        if p and Path(p).exists():
            return str(p)
        raise RuntimeError("No se encontró 'psql' en PATH. Instala el cliente de PostgreSQL.")
    return p


def run_psql_query(psql_bin: str, host: str, port: int, user: str, db: str, password: str, query: str, sslmode: str | None = None) -> tuple[bool, str]:
    env = os.environ.copy()
    env["PGPASSWORD"] = password or ""
    if sslmode:
        env["PGSSLMODE"] = sslmode
    cmd = [
        psql_bin,
        "-h", host,
        "-p", str(port),
        "-U", user,
        "-d", db,
        "-tA",
        "-c", query,
    ]
    proc = subprocess.run(cmd, env=env, capture_output=True, text=True)
    ok = (proc.returncode == 0)
    out = (proc.stdout or proc.stderr or "").strip()
    return ok, out


def get_defaults_from_config() -> tuple[dict, dict]:
    proj_dir = Path(__file__).resolve().parents[2]
    cfg_path = proj_dir / "config" / "config.json"
    cfg = {}
    try:
        if cfg_path.exists():
            with open(cfg_path, "r", encoding="utf-8") as f:
                cfg = json.load(f) or {}
    except Exception:
        cfg = {}
    return cfg.get("db_remote", {}), cfg.get("db_local", {})


def make_arg_parser():
    db_remote, db_local = get_defaults_from_config()
    parser = argparse.ArgumentParser(description="Verificador básico de setup SymmetricDS (Railway y Local)")
    parser.add_argument("--railway-host", default=str(db_remote.get("host", "shuttle.proxy.rlwy.net")))
    parser.add_argument("--railway-port", type=int, default=int(db_remote.get("port", 5432)))
    parser.add_argument("--railway-db", default=str(db_remote.get("database", "railway")))
    parser.add_argument("--railway-user", default=str(db_remote.get("user", "postgres")))

    parser.add_argument("--local-host", default=str(db_local.get("host", "localhost")))
    parser.add_argument("--local-port", type=int, default=int(db_local.get("port", 5432)))
    parser.add_argument("--local-db", default=str(db_local.get("database", "gimnasio")))
    parser.add_argument("--local-user", default=str(db_local.get("user", "postgres")))

    return parser


def _truthy(s: str) -> bool:
    return str(s or "").strip().lower() in ("t", "true", "1", "yes", "y")


def check_site(name: str, psql: str, host: str, port: int, db: str, user: str, pwd: str, sslmode: str | None):
    print(f"\n[{name}] host={host} port={port} db={db} user={user}")

    def q(sql: str):
        ok, out = run_psql_query(psql, host, port, user, db, pwd, sql, sslmode=sslmode)
        return ok, out

    # Sanity: conexión
    ok, ping = q("SELECT 1")
    if not ok:
        print("  Conexión: ERROR")
        return
    else:
        print("  Conexión: OK")

    # Esquema clave
    for t in ("public.sym_channel", "public.sym_node_group", "public.sym_node_group_link", "public.sym_router", "public.sym_trigger", "public.sym_trigger_router", "public.sym_node"):
        ok, out = q(f"SELECT to_regclass('{t}')")
        print(f"  Tabla {t}:", "OK" if (ok and out and out.lower() != "null") else "FALTA")

    # Routers
    ok, cnt_clients = q("SELECT COUNT(*) FROM sym_router WHERE router_id='toClients'")
    ok, cnt_server = q("SELECT COUNT(*) FROM sym_router WHERE router_id='toServer'")
    print(f"  sym_router toClients: {cnt_clients or '0'} | toServer: {cnt_server or '0'}")
    # Detalle de source/target si existen
    ok, stc = q("SELECT source_node_group_id||'->'||target_node_group_id FROM sym_router WHERE router_id='toClients'")
    ok, sts = q("SELECT source_node_group_id||'->'||target_node_group_id FROM sym_router WHERE router_id='toServer'")
    if stc:
        print(f"  toClients source->target: {stc}")
    if sts:
        print(f"  toServer source->target: {sts}")

    # Timestamps en sym_router
    ok, has_sr_ct = q("SELECT EXISTS(SELECT 1 FROM information_schema.columns WHERE table_name='sym_router' AND column_name='create_time')")
    ok, has_sr_lu = q("SELECT EXISTS(SELECT 1 FROM information_schema.columns WHERE table_name='sym_router' AND column_name='last_update_time')")
    if _truthy(has_sr_ct):
        ok, sr_ct_clients = q("SELECT COUNT(*) FROM sym_router WHERE router_id='toClients' AND create_time IS NOT NULL")
        ok, sr_ct_server = q("SELECT COUNT(*) FROM sym_router WHERE router_id='toServer' AND create_time IS NOT NULL")
        print(f"  sym_router create_time: toClients={sr_ct_clients} | toServer={sr_ct_server}")
    else:
        print("  sym_router create_time: columna no existe (omitido)")
    if _truthy(has_sr_lu):
        ok, sr_lu_clients = q("SELECT COUNT(*) FROM sym_router WHERE router_id='toClients' AND last_update_time IS NOT NULL")
        ok, sr_lu_server = q("SELECT COUNT(*) FROM sym_router WHERE router_id='toServer' AND last_update_time IS NOT NULL")
        print(f"  sym_router last_update_time: toClients={sr_lu_clients} | toServer={sr_lu_server}")
    else:
        print("  sym_router last_update_time: columna no existe (omitido)")
    # enabled/sync_config si existen
    ok, has_sr_enabled = q("SELECT EXISTS(SELECT 1 FROM information_schema.columns WHERE table_name='sym_router' AND column_name='enabled')")
    ok, has_sr_sync = q("SELECT EXISTS(SELECT 1 FROM information_schema.columns WHERE table_name='sym_router' AND column_name='sync_config')")
    if _truthy(has_sr_enabled):
        ok, sr_en_clients = q("SELECT COALESCE((SELECT enabled::text FROM sym_router WHERE router_id='toClients'),'')")
        ok, sr_en_server = q("SELECT COALESCE((SELECT enabled::text FROM sym_router WHERE router_id='toServer'),'')")
        print(f"  sym_router enabled: toClients={sr_en_clients or '(nulo)'} | toServer={sr_en_server or '(nulo)'}")
    if _truthy(has_sr_sync):
        ok, sr_sc_clients = q("SELECT COALESCE((SELECT sync_config::text FROM sym_router WHERE router_id='toClients'),'')")
        ok, sr_sc_server = q("SELECT COALESCE((SELECT sync_config::text FROM sym_router WHERE router_id='toServer'),'')")
        print(f"  sym_router sync_config: toClients={sr_sc_clients or '(nulo)'} | toServer={sr_sc_server or '(nulo)'}")

    # Triggers
    ok, trg_cnt = q("SELECT COUNT(*) FROM sym_trigger")
    print(f"  sym_trigger total: {trg_cnt or '0'}")
    ok, has_tr_ct = q("SELECT EXISTS(SELECT 1 FROM information_schema.columns WHERE table_name='sym_trigger' AND column_name='create_time')")
    if _truthy(has_tr_ct):
        ok, trg_ct_cnt = q("SELECT COUNT(*) FROM sym_trigger WHERE create_time IS NOT NULL")
        print(f"  sym_trigger con create_time: {trg_ct_cnt or '0'}")
    else:
        print("  sym_trigger create_time: columna no existe (omitido)")
    # Cobertura de triggers vs tablas públicas
    ok, tbls = q("SELECT STRING_AGG(table_name, ',') FROM information_schema.tables WHERE table_schema='public' AND table_type='BASE TABLE' AND table_name NOT LIKE 'sym_%' AND table_name NOT LIKE 'pg_%' AND table_name NOT LIKE 'sync_%'")
    ok, trg_tables = q("SELECT STRING_AGG(source_table_name, ',') FROM sym_trigger")
    def _set(s: str) -> set:
        return set([x for x in (s or '').split(',') if x])
    pub_set = _set(tbls)
    trg_set = _set(trg_tables)
    missing_triggers = sorted(list(pub_set - trg_set))
    extra_triggers = sorted(list(trg_set - pub_set))
    print(f"  Cobertura de triggers: faltan={len(missing_triggers)} extra={len(extra_triggers)}")
    if len(missing_triggers) > 0:
        print("    Faltantes:")
        print("      " + ", ".join(missing_triggers[:15]) + (" …" if len(missing_triggers) > 15 else ""))
    # Flags comunes en triggers
    ok, trg_si = q("SELECT COUNT(*) FROM sym_trigger WHERE sync_on_insert=1")
    ok, trg_su = q("SELECT COUNT(*) FROM sym_trigger WHERE sync_on_update=1")
    ok, trg_sd = q("SELECT COUNT(*) FROM sym_trigger WHERE sync_on_delete=1")
    print(f"  sym_trigger flags: insert={trg_si} update={trg_su} delete={trg_sd}")
    ok, has_pk = q("SELECT EXISTS(SELECT 1 FROM information_schema.columns WHERE table_name='sym_trigger' AND column_name='use_pk_data')")
    ok, has_lob = q("SELECT EXISTS(SELECT 1 FROM information_schema.columns WHERE table_name='sym_trigger' AND column_name='use_stream_lobs')")
    if _truthy(has_pk):
        ok, trg_pk = q("SELECT COUNT(*) FROM sym_trigger WHERE use_pk_data=1")
        print(f"  sym_trigger use_pk_data=1: {trg_pk}")
    if _truthy(has_lob):
        ok, trg_lob = q("SELECT COUNT(*) FROM sym_trigger WHERE use_stream_lobs=1")
        print(f"  sym_trigger use_stream_lobs=1: {trg_lob}")

    # Trigger routers
    ok, has_trr_ct = q("SELECT EXISTS(SELECT 1 FROM information_schema.columns WHERE table_name='sym_trigger_router' AND column_name='create_time')")
    ok, has_trr_lu = q("SELECT EXISTS(SELECT 1 FROM information_schema.columns WHERE table_name='sym_trigger_router' AND column_name='last_update_time')")
    ok, trr_clients_cnt = q("SELECT COUNT(*) FROM sym_trigger_router WHERE router_id='toClients'")
    ok, trr_server_cnt = q("SELECT COUNT(*) FROM sym_trigger_router WHERE router_id='toServer'")
    print(f"  sym_trigger_router toClients: {trr_clients_cnt or '0'} | toServer: {trr_server_cnt or '0'}")
    if _truthy(has_trr_ct):
        ok, trr_clients_ts = q("SELECT COUNT(*) FROM sym_trigger_router WHERE router_id='toClients' AND create_time IS NOT NULL")
        ok, trr_server_ts = q("SELECT COUNT(*) FROM sym_trigger_router WHERE router_id='toServer' AND create_time IS NOT NULL")
        print(f"  sym_trigger_router create_time: toClients={trr_clients_ts} | toServer={trr_server_ts}")
    else:
        print("  sym_trigger_router create_time: columna no existe (omitido)")
    # initial_load_order no nulo
    ok, trr_ilo_clients = q("SELECT COUNT(*) FROM sym_trigger_router WHERE router_id='toClients' AND initial_load_order IS NOT NULL")
    ok, trr_ilo_server = q("SELECT COUNT(*) FROM sym_trigger_router WHERE router_id='toServer' AND initial_load_order IS NOT NULL")
    print(f"  initial_load_order definido: toClients={trr_ilo_clients} | toServer={trr_ilo_server}")

    # Muestras
    if (trr_clients_cnt and trr_clients_cnt != '0'):
        # No referenciamos columnas opcionales si no existen
        ok, sample_clients = q("SELECT trigger_id, router_id FROM sym_trigger_router WHERE router_id='toClients' ORDER BY trigger_id LIMIT 5")
        print("  Muestra toClients:")
        print("    " + (sample_clients.replace("\n", "\n    ") if sample_clients else "(vacío)"))
    if (trr_server_cnt and trr_server_cnt != '0'):
        ok, sample_server = q("SELECT trigger_id, router_id FROM sym_trigger_router WHERE router_id='toServer' ORDER BY trigger_id LIMIT 5")
        print("  Muestra toServer:")
        print("    " + (sample_server.replace("\n", "\n    ") if sample_server else "(vacío)"))

    # Node groups y links
    ok, ng_server = q("SELECT COUNT(*) FROM sym_node_group WHERE node_group_id='server'")
    ok, ng_client = q("SELECT COUNT(*) FROM sym_node_group WHERE node_group_id='client'")
    print(f"  node_group server={ng_server} client={ng_client}")
    ok, l_sc = q("SELECT COUNT(*) FROM sym_node_group_link WHERE source_node_group_id='server' AND target_node_group_id='client'")
    ok, l_cs = q("SELECT COUNT(*) FROM sym_node_group_link WHERE source_node_group_id='client' AND target_node_group_id='server'")
    print(f"  links server->client={l_sc} client->server={l_cs}")

    # Canal default
    ok, ch_def = q("SELECT COUNT(*) FROM sym_channel WHERE channel_id='default'")
    print(f"  canal 'default' presente: {ch_def}")

    # Batches (si existen)
    ok, has_out = q("SELECT to_regclass('public.sym_outgoing_batch')")
    ok, has_in = q("SELECT to_regclass('public.sym_incoming_batch')")
    if has_out and has_out.lower() != 'null':
        ok, out_stats = q(
            """
            SELECT COALESCE(STRING_AGG(status||':'||cnt, ','), '')
            FROM (
              SELECT status, COUNT(*) AS cnt
              FROM sym_outgoing_batch
              GROUP BY status
            ) t
            """.strip()
        )
        print(f"  outgoing_batch status: {out_stats or '(no hay)'}")
    else:
        print("  outgoing_batch: tabla no existe (omitido)")
    if has_in and has_in.lower() != 'null':
        ok, in_stats = q(
            """
            SELECT COALESCE(STRING_AGG(status||':'||cnt, ','), '')
            FROM (
              SELECT status, COUNT(*) AS cnt
              FROM sym_incoming_batch
              GROUP BY status
            ) t
            """.strip()
        )
        print(f"  incoming_batch status: {in_stats or '(no hay)'}")
    else:
        print("  incoming_batch: tabla no existe (omitido)")


def main():
    parser = make_arg_parser()
    args = parser.parse_args()

    psql = require_psql()

    # Passwords
    rw_pwd = resolve_password(args.railway_user, args.railway_host, args.railway_port, "")
    lc_pwd = resolve_password(args.local_user, args.local_host, args.local_port, "")
    if not rw_pwd:
        print("ERROR: No se pudo resolver la contraseña de Railway desde keyring/entorno.")
        sys.exit(2)
    if not lc_pwd:
        print("ERROR: No se pudo resolver la contraseña de Local desde keyring/entorno.")
        sys.exit(2)

    # SSL modes desde config.json si existen
    db_remote, db_local = get_defaults_from_config()
    ssl_remote = str(db_remote.get("sslmode", "")) or None
    ssl_local = str(db_local.get("sslmode", "")) or None

    # Ejecutar checks
    check_site("Railway", psql, args.railway_host, args.railway_port, args.railway_db, args.railway_user, rw_pwd, ssl_remote)
    check_site("Local", psql, args.local_host, args.local_port, args.local_db, args.local_user, lc_pwd, ssl_local)

    print("\nVerificación completada.")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"ERROR: {e}")
        sys.exit(1)