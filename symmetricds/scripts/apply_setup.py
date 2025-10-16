"""
Aplica los scripts SQL de SymmetricDS usando psql, resolviendo contraseñas
automáticamente desde keyring (Windows Credential Manager) con la misma
convención del proyecto.

Requiere tener el binario `psql` en PATH.

Uso rápido:
  python symmetricds/scripts/apply_setup.py \
    --railway-host shuttle.proxy.rlwy.net --railway-port 5432 \
    --railway-db railway --railway-user postgres \
    --local-host localhost --local-port 5432 \
    --local-db gimnasio --local-user postgres
"""

import os
import sys
import subprocess
import shutil
from pathlib import Path
import argparse
import json

try:
    import keyring  # type: ignore
except Exception:
    keyring = None  # type: ignore

try:
    from config import KEYRING_SERVICE_NAME
except Exception:
    KEYRING_SERVICE_NAME = "GymMS_DB"


def resolve_password(user: str, host: str, port: int, fallback: str = "") -> str:
    """Resuelve contraseña desde keyring usando cuentas compuestas.

    Intenta en orden: user@host:port, user@host, user. Si falla, usa fallback
    o variables de entorno comunes.
    """
    if keyring is not None:
        candidates = [
            f"{user}@{host}:{port}",
            f"{user}@{host}",
            f"{user}",
        ]
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
        # Sugerir ruta típica en Windows si existe
        cand = Path("C:/Program Files/PostgreSQL").glob("*/bin/psql.exe")
        try:
            p = next(cand, None)
        except Exception:
            p = None
        if p and Path(p).exists():
            return str(p)
        raise RuntimeError("No se encontró 'psql' en PATH. Instala el cliente de PostgreSQL.")
    return p


def run_psql(psql_bin: str, host: str, port: int, user: str, db: str, password: str, sql_path: Path, sslmode: str | None = None) -> None:
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
        "-v", "ON_ERROR_STOP=1",
        "-f", str(sql_path),
    ]
    proc = subprocess.run(cmd, env=env)
    if proc.returncode != 0:
        raise RuntimeError(f"psql falló al aplicar {sql_path.name} (exit {proc.returncode})")


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


def parse_properties(path: Path) -> dict:
    props: dict = {}
    try:
        if path.exists():
            with open(path, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line or line.startswith("#"):
                        continue
                    if "=" in line:
                        k, v = line.split("=", 1)
                        props[k.strip()] = v.strip()
    except Exception:
        pass
    return props


def main():
    parser = argparse.ArgumentParser(description="Aplica scripts SQL de SymmetricDS con psql y keyring")
    # Intentar cargar defaults desde config/config.json y entorno
    proj_dir = Path(__file__).resolve().parents[2]
    cfg_path = proj_dir / "config" / "config.json"
    cfg = {}
    try:
        if cfg_path.exists():
            with open(cfg_path, "r", encoding="utf-8") as f:
                cfg = json.load(f) or {}
    except Exception:
        cfg = {}

    # Defaults específicos desde config.json: db_remote para Railway, db_local para Local
    db_remote = cfg.get("db_remote", {})
    db_local = cfg.get("db_local", {})

    parser.add_argument("--railway-host", default=str(db_remote.get("host", "shuttle.proxy.rlwy.net")))
    parser.add_argument("--railway-port", type=int, default=int(db_remote.get("port", 5432)))
    parser.add_argument("--railway-db", default=str(db_remote.get("database", "railway")))
    parser.add_argument("--railway-user", default=str(db_remote.get("user", "postgres")))

    parser.add_argument("--local-host", default=str(db_local.get("host", "localhost")))
    parser.add_argument("--local-port", type=int, default=int(db_local.get("port", 5432)))
    parser.add_argument("--local-db", default=str(db_local.get("database", "gimnasio")))
    parser.add_argument("--local-user", default=str(db_local.get("user", "postgres")))

    args = parser.parse_args()

    base_dir = Path(__file__).resolve().parent
    railway_sql = base_dir / "railway_setup.sql"
    local_sql = base_dir / "local_setup.sql"
    initial_load_sql = base_dir / "initial_load_all_clients.sql"

    for p in (railway_sql, local_sql, initial_load_sql):
        if not p.exists():
            raise FileNotFoundError(f"No se encontró el script SQL: {p}")

    psql_bin = require_psql()

    # Intentar leer contraseñas desde engines/*.properties como fallback
    engines_dir = base_dir.parent / "engines"
    rw_props_path = engines_dir / "railway.properties"
    lc_props_path = engines_dir / "local.properties"
    rw_props = parse_properties(rw_props_path)
    lc_props = parse_properties(lc_props_path)
    rw_fallback_pwd = rw_props.get("db.password", "")
    lc_fallback_pwd = lc_props.get("db.password", "")

    # Passwords desde keyring / entorno, con fallback de properties
    rw_pwd = resolve_password(args.railway_user, args.railway_host, args.railway_port, rw_fallback_pwd)
    lc_pwd = resolve_password(args.local_user, args.local_host, args.local_port, lc_fallback_pwd)

    if not rw_pwd:
        raise RuntimeError("No se pudo resolver la contraseña de Railway desde keyring/entorno.")
    if not lc_pwd:
        raise RuntimeError("No se pudo resolver la contraseña de Local desde keyring/entorno.")

    ssl_remote = str(db_remote.get("sslmode", "")) or None
    ssl_local = str(db_local.get("sslmode", "")) or None

    # Verificar que el esquema SymmetricDS tenga todas las tablas clave; si no, arrancar engines y esperar
    def _schema_ready() -> bool:
        checks = [
            "public.sym_channel", "public.sym_context", "public.sym_node",
            "public.sym_router", "public.sym_trigger", "public.sym_trigger_router"
        ]
        for t in checks:
            ok_t, out_t = run_psql_query(
                psql_bin, args.railway_host, args.railway_port, args.railway_user, args.railway_db, rw_pwd,
                f"SELECT to_regclass('{t}')", sslmode=ssl_remote
            )
            if not (ok_t and out_t not in ("", "NULL", "null")):
                return False
        return True

    if not _schema_ready():
        print("Esquema SymmetricDS incompleto en Railway. Arrancando SymmetricWebServer y esperando tablas…")
        try:
            proj_dir = Path(__file__).resolve().parents[2]
            if str(proj_dir) not in sys.path:
                sys.path.insert(0, str(proj_dir))
            from symmetricds.setup_symmetric import start_symmetricds_background  # type: ignore
            start_symmetricds_background(db_manager=None, logger=print, check_interval_sec=30)
        except Exception as e:
            print(f"No se pudo iniciar SymmetricDS automáticamente: {e}")
        import time
        max_seconds = 300
        interval = 3
        waited = 0
        print("Esperando a que SymmetricDS cree sus tablas clave (hasta 5 min)…")
        while waited < max_seconds and not _schema_ready():
            time.sleep(interval)
            waited += interval
            if waited % 15 == 0:
                print(f"…aún creando tablas (esperados {waited}s).")
        if not _schema_ready():
            raise RuntimeError("Las tablas sym_* no se crearon en Railway tras esperar 5 minutos. Revisa el log de SymmetricDS y la conectividad.")

    print(f"Aplicando configuración en Railway… (host={args.railway_host} port={args.railway_port} db={args.railway_db} user={args.railway_user})")
    run_psql(psql_bin, args.railway_host, args.railway_port, args.railway_user, args.railway_db, rw_pwd, railway_sql, sslmode=ssl_remote)

    print(f"Aplicando configuración en Local… (host={args.local_host} port={args.local_port} db={args.local_db} user={args.local_user})")
    run_psql(psql_bin, args.local_host, args.local_port, args.local_user, args.local_db, lc_pwd, local_sql, sslmode=ssl_local)

    print("Solicitando carga inicial para todos los clientes (en Railway)…")
    run_psql(psql_bin, args.railway_host, args.railway_port, args.railway_user, args.railway_db, rw_pwd, initial_load_sql, sslmode=ssl_remote)

    print("Listo. Configuración aplicada.")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"ERROR: {e}")
        sys.exit(1)