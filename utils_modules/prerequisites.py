import os
import json
import shutil
import subprocess
import sys
import glob
from datetime import datetime
from typing import Optional, Tuple, List
from urllib.parse import urlparse
import logging

try:
    import keyring
except Exception:
    keyring = None
try:
    from config import KEYRING_SERVICE_NAME, Config as AppConfig
except Exception:
    KEYRING_SERVICE_NAME = "GymMS_DB"
    AppConfig = None

PROJECT_ROOT = os.path.dirname(os.path.dirname(__file__))
CONFIG_DIR = os.path.join(PROJECT_ROOT, "config")
PREREQ_MARK_DIR = os.path.join(CONFIG_DIR, "first_run_prereqs")


def _ensure_dirs():
    try:
        os.makedirs(CONFIG_DIR, exist_ok=True)
        os.makedirs(PREREQ_MARK_DIR, exist_ok=True)
    except Exception:
        pass


def _marker_path(device_id: str) -> str:
    _ensure_dirs()
    return os.path.join(PREREQ_MARK_DIR, f"{device_id}.json")


def is_command_available(cmd: str) -> bool:
    try:
        return shutil.which(cmd) is not None
    except Exception:
        return False


def run_cmd_capture(args: list, timeout: Optional[int] = 20) -> Tuple[int, str, str]:
    try:
        kwargs = {
            "stdout": subprocess.PIPE,
            "stderr": subprocess.PIPE,
            "timeout": timeout,
            "text": True,
        }
        # En Windows, ejecutar sin crear ventana de consola.
        if os.name == 'nt':
            try:
                kwargs["creationflags"] = subprocess.CREATE_NO_WINDOW
            except Exception:
                pass
        proc = subprocess.run(args, **kwargs)
        return proc.returncode, proc.stdout.strip(), proc.stderr.strip()
    except Exception as e:
        return 1, "", str(e)


# Eliminado: verificación/instalación de Java ya no es requerida.


def is_postgresql_installed(required_major: int = 17) -> bool:
    # Detectar por comando y por ruta típica
    if is_command_available("psql"):
        code, out, err = run_cmd_capture(["psql", "--version"], timeout=10)
        if code == 0:
            text = (out + "\n" + err).lower()
            # Ejemplo: "psql (PostgreSQL) 17.0"
            return f"postgresql) {required_major}" in text
    # Ruta estándar de Windows
    expected = os.path.join("C:\\Program Files\\PostgreSQL", str(required_major), "bin", "psql.exe")
    return os.path.exists(expected)


def install_postgresql_17() -> Tuple[bool, str]:
    # Winget ID oficial de PostgreSQL (instalador EnterpriseDB)
    # Ejecutar en modo silencioso y aceptar acuerdos para instalación no interactiva
    if not is_command_available("winget"):
        return False, "winget no está disponible en este sistema"
    args_primary = [
        "winget", "install", "-e", "--id", "PostgreSQL.PostgreSQL",
        "--version", "17", "--source", "winget",
        "--silent", "--accept-package-agreements", "--accept-source-agreements",
    ]
    code, out, err = run_cmd_capture(args_primary, timeout=1200)
    if code == 0:
        return True, out
    # Intento alterno por algunos catálogos
    args_alt = [
        "winget", "install", "-e", "--id", "EnterpriseDB.PostgreSQL",
        "--version", "17", "--source", "winget",
        "--silent", "--accept-package-agreements", "--accept-source-agreements",
    ]
    code2, out2, err2 = run_cmd_capture(args_alt, timeout=1200)
    ok = code2 == 0
    msg = out2 if ok else (err + "\n" + err2)
    return ok, msg


def _load_cfg() -> dict:
    try:
        cfg_path = os.path.join(CONFIG_DIR, "config.json")
        if os.path.exists(cfg_path):
            with open(cfg_path, "r", encoding="utf-8") as f:
                return json.load(f) or {}
    except Exception:
        pass
    return {}


def _resolve_pg_bin(required_major: int, exe_name: str) -> Optional[str]:
    # Intentar en PATH
    try:
        found = shutil.which(exe_name)
        if found:
            return found
    except Exception:
        pass
    # Ruta típica de Windows
    candidate = os.path.join("C:\\Program Files\\PostgreSQL", str(required_major), "bin", f"{exe_name}.exe")
    if os.path.exists(candidate):
        return candidate
    return None


def _resolve_pg_password(cfg: dict) -> str:
    # Prioridad: config.json -> entorno común -> keyring
    val = str(cfg.get("password", ""))
    if val:
        return val
    for env_key in ("PGPASSWORD", "POSTGRES_PASSWORD", "DB_PASSWORD", "PG_PASS", "DATABASE_PASSWORD", "DB_LOCAL_PASSWORD"):
        v = os.getenv(env_key, "")
        if v:
            return v
    # Intentar keyring
    if keyring:
        try:
            local = cfg.get("db_local") if isinstance(cfg.get("db_local"), dict) else cfg
            user = str(local.get("user", cfg.get("user", "postgres")))
            pw = keyring.get_password(KEYRING_SERVICE_NAME, user)
            if pw:
                return pw
        except Exception:
            pass
    return ""


def create_database_from_config(required_major: int = 17) -> Tuple[bool, str]:
    cfg = _load_cfg()
    # Preferir perfil local si existe
    profile = cfg.get("db_local") if isinstance(cfg.get("db_local"), dict) else cfg
    host = str(profile.get("host", "localhost"))
    try:
        port = int(profile.get("port", 5432))
    except Exception:
        port = 5432
    dbname = str(profile.get("database", "gimnasio"))
    user = str(profile.get("user", "postgres"))
    password = _resolve_pg_password(cfg)

    # Fallback: parsear DSN de config.py si falta información
    try:
        dsn = None
        if AppConfig and hasattr(AppConfig, "DATABASE_PATH"):
            dsn = str(getattr(AppConfig, "DATABASE_PATH"))
        if dsn:
            u = urlparse(dsn)
            if u.scheme.startswith("postgres"):
                host = u.hostname or host
                if u.port:
                    port = u.port
                # path como "/gimnasio" -> quitar slash
                if (u.path or "").strip("/"):
                    dbname = (u.path or "/gimnasio").lstrip("/")
                if u.username:
                    user = u.username
                if u.password:
                    password = u.password
    except Exception:
        pass

    createdb_exe = _resolve_pg_bin(required_major, "createdb")
    if not createdb_exe:
        # Fallback a psql -c
        psql_exe = _resolve_pg_bin(required_major, "psql")
        if not psql_exe:
            return False, "No se encontró psql/createdb en PATH ni en la ruta estándar"
        env = os.environ.copy()
        if password:
            env["PGPASSWORD"] = password
        args = [psql_exe, "-h", host, "-p", str(port), "-U", user, "-v", "ON_ERROR_STOP=1", "-c", f"CREATE DATABASE \"{dbname}\";"]
        code, out, err = run_cmd_capture(args, timeout=60)
        if code == 0:
            return True, out
        # Si ya existe, consideramos OK
        if "already exists" in (out + "\n" + err).lower():
            return True, "La base ya existía"
        return False, err or out

    # Usar createdb directamente
    env = os.environ.copy()
    if password:
        env["PGPASSWORD"] = password
    args = [createdb_exe, "-h", host, "-p", str(port), "-U", user, dbname]
    try:
        proc = subprocess.run(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=60, text=True, env=env)
        if proc.returncode == 0:
            return True, proc.stdout.strip()
        combined = (proc.stdout or "") + "\n" + (proc.stderr or "")
        if "already exists" in combined.lower():
            return True, "La base ya existía"
        return False, proc.stderr.strip() or proc.stdout.strip()
    except Exception as e:
        return False, str(e)


def read_marker(device_id: str) -> Optional[dict]:
    path = _marker_path(device_id)
    try:
        if os.path.exists(path):
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
    except Exception:
        return None
    return None


def write_marker(device_id: str, payload: dict) -> None:
    path = _marker_path(device_id)
    try:
        payload = dict(payload)
        payload.setdefault("device_id", device_id)
        payload.setdefault("timestamp", datetime.utcnow().isoformat() + "Z")
        with open(path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
    except Exception:
        pass

# --- Bootstrap de credenciales remotas (Railway) para primera ejecución ---

def _remote_bootstrap_marker_path() -> str:
    try:
        os.makedirs(PREREQ_MARK_DIR, exist_ok=True)
    except Exception:
        pass
    return os.path.join(PREREQ_MARK_DIR, "remote_bootstrap_applied.txt")


def _parse_dsn_bootstrap(dsn: str, defaults: dict) -> Tuple[str, int, str, str, str, str, int, str]:
    host = str(defaults.get("host") or "")
    port = int(defaults.get("port") or 5432)
    db = str(defaults.get("database") or "railway")
    user = str(defaults.get("user") or "postgres")
    password = str(defaults.get("password") or "")
    sslmode = str(defaults.get("sslmode") or "require")
    appname = str(defaults.get("application_name") or "gym_management_system")
    timeout = int(defaults.get("connect_timeout") or 10)
    if not dsn:
        return host, port, db, user, password, sslmode, timeout, appname
    try:
        from urllib.parse import urlparse as _urlparse, parse_qs
        u = _urlparse(dsn)
        host = u.hostname or host
        port = int(u.port or port)
        db = (u.path or "").lstrip("/") or db
        user = u.username or user
        password = u.password or password
        q = parse_qs(u.query or "")
        sslmode = (q.get("sslmode") or [sslmode])[0]
        appname = (q.get("application_name") or [appname])[0]
        timeout = int((q.get("connect_timeout") or [timeout])[0])
    except Exception:
        pass
    return host, port, db, user, password, sslmode, timeout, appname


def apply_remote_bootstrap_if_present() -> dict:
    info = {"applied": False, "message": ""}
    try:
        cfg_dir = CONFIG_DIR
        path = os.path.join(cfg_dir, "remote_bootstrap.json")
        marker_path = _remote_bootstrap_marker_path()
        if os.path.exists(marker_path):
            # Ya aplicado previamente
            info["applied"] = True
            info["message"] = "marker_present"
            return info
        if not os.path.exists(path):
            info["message"] = "no_bootstrap_file"
            return info
        # Cargar bootstrap
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f) or {}
        remote = data.get("remote") or data
        dsn = str(remote.get("dsn") or data.get("remote_dsn") or os.getenv("PGREMOTE_DSN", ""))
        host, port, db, user, password, sslmode, timeout, appname = _parse_dsn_bootstrap(
            dsn,
            {
                "host": remote.get("host"),
                "port": remote.get("port"),
                "database": remote.get("database"),
                "user": remote.get("user"),
                "password": remote.get("password"),
                "sslmode": remote.get("sslmode"),
                "application_name": remote.get("application_name"),
                "connect_timeout": remote.get("connect_timeout"),
            },
        )
        # Sembrar keyring si hay contraseña
        if keyring and password:
            for account in (f"{user}@railway", f"{user}@{host}:{port}", user):
                try:
                    existing = keyring.get_password(KEYRING_SERVICE_NAME, account)
                except Exception:
                    existing = None
                if existing:
                    continue
                try:
                    keyring.set_password(KEYRING_SERVICE_NAME, account, password)
                except Exception:
                    pass
        # Inyectar variables de entorno del proceso para uso inmediato
        if dsn:
            try:
                os.environ["PGREMOTE_DSN"] = dsn
            except Exception:
                pass
        try:
            os.environ["PGREMOTE_USER"] = user
        except Exception:
            pass
        if password:
            try:
                os.environ["PGREMOTE_PASSWORD"] = password
            except Exception:
                pass
        # Procesar configuración VPN desde remote_bootstrap.json y exportar variables
        vpn = data.get("vpn") or {}
        provider = (vpn.get("provider") or "").lower()
        if provider:
            try:
                os.environ["VPN_PROVIDER"] = provider
            except Exception:
                pass
        if provider == "tailscale":
            ts_auth = vpn.get("tailscale_auth_key")
            if ts_auth:
                try:
                    os.environ["TAILSCALE_AUTHKEY"] = ts_auth
                except Exception:
                    pass
            hp = vpn.get("hostname_prefix")
            if hp:
                try:
                    os.environ["TAILSCALE_HOSTNAME_PREFIX"] = str(hp)
                except Exception:
                    pass
            cu = vpn.get("control_url")
            if cu:
                try:
                    os.environ["TAILSCALE_CONTROL_URL"] = str(cu)
                except Exception:
                    pass
            for k_src, k_env in [("accept_routes", "TAILSCALE_ACCEPT_ROUTES"), ("accept_dns", "TAILSCALE_ACCEPT_DNS")]:
                val = vpn.get(k_src)
                if val is not None:
                    try:
                        os.environ[k_env] = "true" if bool(val) else "false"
                    except Exception:
                        pass
            tags = vpn.get("advertise_tags") or []
            if tags:
                try:
                    os.environ["TAILSCALE_ADVERTISE_TAGS"] = ",".join([str(t) for t in tags])
                except Exception:
                    pass
        elif provider == "wireguard":
            b64 = vpn.get("wireguard_config_b64")
            pth = vpn.get("wireguard_config_path")
            if b64:
                try:
                    os.environ["WIREGUARD_CONFIG_B64"] = str(b64)
                except Exception:
                    pass
            if pth:
                try:
                    os.environ["WIREGUARD_CONFIG_PATH"] = str(pth)
                except Exception:
                    pass
        
        # Procesar Webapp base URL y token de sincronización
        webapp = data.get("webapp") or {}
        pub_url = str(webapp.get("public_base_url") or data.get("public_webapp_url") or "").strip()
        if pub_url:
            try:
                os.environ["WEBAPP_BASE_URL"] = pub_url
            except Exception:
                pass
        sync_token = str(data.get("sync_upload_token") or "").strip()
        if sync_token:
            try:
                os.environ["SYNC_UPLOAD_TOKEN"] = sync_token
            except Exception:
                pass
        owner_seed = str(data.get("owner_password") or "").strip()
        if owner_seed:
            try:
                os.environ["WEBAPP_OWNER_PASSWORD"] = owner_seed
            except Exception:
                pass
        # Actualizar config.json (solo datos sin password)
        try:
            cfg_path = os.path.join(cfg_dir, "config.json")
            cfg = {}
            if os.path.exists(cfg_path):
                with open(cfg_path, "r", encoding="utf-8") as cf:
                    cfg = json.load(cf) or {}
            remote_cfg = dict(cfg.get("db_remote") or {})
            remote_cfg.update({
                "host": host,
                "port": port,
                "database": db,
                "user": user,
                "sslmode": sslmode or "require",
            })
            cfg["db_remote"] = remote_cfg
            # Persistir webapp_base_url y token si vienen en bootstrap
            if pub_url:
                cfg["webapp_base_url"] = pub_url
            if sync_token:
                cfg["sync_upload_token"] = sync_token
            # Persistir configuración no sensible del VPN
            if provider:
                vpn_cfg = dict(cfg.get("vpn") or {})
                vpn_cfg.update({
                    "provider": provider,
                })
                if provider == "tailscale":
                    if hp:
                        vpn_cfg["hostname_prefix"] = hp
                    if cu:
                        vpn_cfg["control_url"] = cu
                    if vpn.get("accept_routes") is not None:
                        vpn_cfg["accept_routes"] = bool(vpn.get("accept_routes"))
                    if vpn.get("accept_dns") is not None:
                        vpn_cfg["accept_dns"] = bool(vpn.get("accept_dns"))
                    if tags:
                        vpn_cfg["advertise_tags"] = tags
                elif provider == "wireguard":
                    if pth:
                        vpn_cfg["wireguard_config_path"] = pth
                cfg["vpn"] = vpn_cfg
            with open(cfg_path, "w", encoding="utf-8") as cf:
                json.dump(cfg, cf, ensure_ascii=False, indent=2)
        except Exception:
            pass
        # Marcar aplicado y opcionalmente renombrar el archivo para no dejar secretos expuestos
        try:
            with open(marker_path, "w", encoding="utf-8") as mf:
                mf.write(datetime.utcnow().isoformat() + "Z")
        except Exception:
            pass
        try:
            new_path = os.path.join(cfg_dir, "remote_bootstrap.applied.json")
            os.replace(path, new_path)
        except Exception:
            # Si no es posible renombrar, dejar como está
            pass
        info["applied"] = True
        info["message"] = "bootstrap_applied"
        info["remote"] = {"host": host, "port": port, "database": db, "user": user}
    except Exception as e:
        info["applied"] = False
        info["message"] = str(e)
    return info


def ensure_prerequisites(device_id: str) -> dict:
    """
    Verifica e instala (si faltan) prerequisitos de programa:
    - PostgreSQL 17
    Marca la ejecución por device_id para evitar repetir en el futuro.
    Retorna diccionario con resultados por componente.
    """
    _ensure_dirs()
    try:
        logging.info(f"Prerequisitos: inicio para device_id={device_id}")
    except Exception:
        pass
    result = {
        "postgresql": {"installed": False, "attempted": False, "message": ""},
        "marked": False,
        "remote_bootstrap": {"applied": False, "message": ""},
    }

    # Aplicar bootstrap remoto (idempotente, antes de cualquier salida)
    try:
        logging.info("Prerequisitos: aplicando remote_bootstrap si existe")
    except Exception:
        pass
    try:
        result["remote_bootstrap"] = apply_remote_bootstrap_if_present()
    except Exception as e:
        result["remote_bootstrap"] = {"applied": False, "error": str(e)}
    try:
        logging.info(f"Prerequisitos: remote_bootstrap -> applied={result['remote_bootstrap'].get('applied')}")
    except Exception:
        pass

    # Si ya hay marca, no salimos: aplicamos pasos idempotentes (outbox/tareas/red/replicación)
    marker = read_marker(device_id)
    if marker:
        # Solo reportar estado actual de PostgreSQL y continuar
        result["postgresql"]["installed"] = is_postgresql_installed(17)
        result["marked"] = True
        try:
            logging.info("Prerequisitos: marker existente, continuar pasos idempotentes")
        except Exception:
            pass

    # PostgreSQL
    try:
        logging.info("Prerequisitos: verificando PostgreSQL 17")
    except Exception:
        pass
    if is_postgresql_installed(17):
        result["postgresql"]["installed"] = True
        try:
            logging.info("Prerequisitos: PostgreSQL ya instalado")
        except Exception:
            pass
    else:
        ok, msg = install_postgresql_17()
        result["postgresql"]["attempted"] = True
        result["postgresql"]["installed"] = ok and is_postgresql_installed(17)
        result["postgresql"]["message"] = msg
        try:
            logging.info(f"Prerequisitos: instalación PostgreSQL -> ok={ok}")
        except Exception:
            pass

    # Si PostgreSQL está instalado, intentar crear la base definida en config.json
    try:
        if result["postgresql"]["installed"]:
            logging.info("Prerequisitos: creando base de datos desde config.json")
            db_ok, db_msg = create_database_from_config(17)
            result["postgresql"]["db_created"] = bool(db_ok)
            if not db_ok and db_msg:
                result["postgresql"]["db_message"] = db_msg
            try:
                logging.info(f"Prerequisitos: creación DB -> ok={bool(db_ok)}")
            except Exception:
                pass
    except Exception as e:
        result["postgresql"]["db_created"] = False
        result["postgresql"]["db_message"] = str(e)
        try:
            logging.warning(f"Prerequisitos: error creación DB -> {e}")
        except Exception:
            pass

    # Sembrar owner_password en DB si viene por entorno/bootstrap y no existe
    try:
        seed_pwd = os.getenv("WEBAPP_OWNER_PASSWORD") or os.getenv("OWNER_PASSWORD") or ""
        if seed_pwd:
            try:
                from database import DatabaseManager  # type: ignore
                dbm = DatabaseManager()
                cur = dbm.obtener_configuracion("owner_password")
                if not cur:
                    ok_upd = dbm.actualizar_configuracion("owner_password", seed_pwd)
                    result["owner_password_seed"] = {"ok": bool(ok_upd), "via": "DatabaseManager"}
                else:
                    result["owner_password_seed"] = {"ok": True, "skipped": True, "reason": "already_set"}
            except Exception as e_dm:
                # Fallback con psql CLI, sin sobreescribir si ya existe
                try:
                    cfg_local = _load_cfg()
                    profile = cfg_local.get("db_local") if isinstance(cfg_local.get("db_local"), dict) else cfg_local
                    host = str(profile.get("host") or cfg_local.get("host") or "localhost")
                    try:
                        port = int(profile.get("port") or cfg_local.get("port") or 5432)
                    except Exception:
                        port = 5432
                    dbname = str(profile.get("database") or cfg_local.get("database") or "gimnasio")
                    user = str(profile.get("user") or cfg_local.get("user") or "postgres")
                    pwd = _resolve_pg_password(cfg_local)
                    psql_exe = _resolve_pg_bin(17, "psql")
                    if not psql_exe:
                        result["owner_password_seed"] = {"ok": False, "error": "psql no encontrado"}
                    else:
                        env = os.environ.copy()
                        if pwd:
                            env["PGPASSWORD"] = pwd
                        seed_pwd_sql = seed_pwd.replace("'", "''")
                        sql = (
                            "CREATE TABLE IF NOT EXISTS configuracion (clave TEXT PRIMARY KEY, valor TEXT); "
                            f"INSERT INTO configuracion (clave, valor) VALUES ('owner_password', '{seed_pwd_sql}') "
                            "ON CONFLICT (clave) DO NOTHING;"
                        )
                        code, out, err = run_cmd_capture([psql_exe, "-h", host, "-p", str(port), "-U", user, "-d", dbname, "-v", "ON_ERROR_STOP=1", "-c", sql], timeout=20)
                        result["owner_password_seed"] = {"ok": code == 0, "via": "psql", "message": out or err}
                except Exception as e_psql:
                    result["owner_password_seed"] = {"ok": False, "error": str(e_psql)}
        else:
            result["owner_password_seed"] = {"ok": True, "skipped": True, "reason": "no_env_password"}
    except Exception as e_seed:
        result["owner_password_seed"] = {"ok": False, "error": str(e_seed)}
    try:
        logging.info(f"Prerequisitos: seed owner_password -> {result.get('owner_password_seed')}")
    except Exception:
        pass

    # Instalar triggers de outbox de forma idempotente y sin depender de la UI
    try:
        logging.info("Prerequisitos: instalando triggers de outbox")
    except Exception:
        pass
    try:
        from scripts.install_outbox_triggers import run as install_outbox  # type: ignore
        install_outbox()
        result["outbox"] = {"ok": True, "installed": True}
    except Exception as e:
        result["outbox"] = {"ok": False, "error": str(e)}
        try:
            logging.warning(f"Prerequisitos: error instalando outbox -> {e}")
        except Exception:
            pass

    # Asegurar columna/índice/trigger updated_at en tablas sincronizadas (idempotente)
    try:
        logging.info("Prerequisitos: asegurando updated_at en tablas de sincronización")
    except Exception:
        pass
    try:
        # Ejecutar separadamente en LOCAL y REMOTO para aislar fallos de conectividad
        from scripts.ensure_updated_at_triggers import run as ensure_updated_at  # type: ignore
        # Local primero (entorno de escritorio)
        try:
            ensure_updated_at(schema='public', tables=None, apply_local=True, apply_remote=False, dry_run=False, all_tables=True)
            result["updated_at_triggers_local"] = {"ok": True}
        except Exception as e_loc:
            result["updated_at_triggers_local"] = {"ok": False, "error": str(e_loc)}
            try:
                logging.warning(f"Prerequisitos: fallo ensure updated_at LOCAL -> {e_loc}")
            except Exception:
                pass
        # Intentar también en remoto si hay credenciales; no bloquear si falla
        try:
            ensure_updated_at(schema='public', tables=None, apply_local=False, apply_remote=True, dry_run=False, all_tables=True)
            result["updated_at_triggers_remote"] = {"ok": True}
        except Exception as e_rem:
            result["updated_at_triggers_remote"] = {"ok": False, "error": str(e_rem)}
            try:
                logging.warning(f"Prerequisitos: fallo ensure updated_at REMOTO -> {e_rem}")
            except Exception:
                pass
    except Exception as e:
        result["updated_at_triggers"] = {"ok": False, "error": str(e)}
        try:
            logging.warning(f"Prerequisitos: error asegurando updated_at -> {e}")
        except Exception:
            pass

    # Aplicar tareas programadas según config.json (idempotente)
    try:
        logging.info("Prerequisitos: asegurando tareas programadas")
    except Exception:
        pass
    try:
        tasks_res = ensure_scheduled_tasks(device_id)
        result["scheduled_tasks"] = tasks_res
    except Exception as e:
        result["scheduled_tasks"] = {"ok": False, "error": str(e)}
        try:
            logging.warning(f"Prerequisitos: error tareas programadas -> {e}")
        except Exception:
            pass

    # Intentar asegurar conectividad VPN y replicación bidireccional si hay configuración disponible (no bloqueante)
    try:
        cfg = _load_cfg()

        # VPN join automático (si cfg['vpn'] o variables de entorno lo indican)
        try:
            logging.info("Prerequisitos: verificando conectividad VPN")
        except Exception:
            pass
        try:
            from utils_modules.vpn_setup import ensure_vpn_connectivity  # type: ignore
            vpn_info = ensure_vpn_connectivity(cfg, device_id)
        except Exception:
            vpn_info = {"ok": False}
        try:
            logging.info(f"Prerequisitos: VPN -> ok={vpn_info.get('ok')} ip={vpn_info.get('ip')}")
        except Exception:
            pass
        # Asegurar exposición segura de PostgreSQL para acceso vía VPN y Firewall
        try:
            net_res = ensure_postgres_network_access(cfg)
            result.setdefault("postgresql", {})["network"] = net_res
        except Exception as e:
            result.setdefault("postgresql", {})["network_error"] = str(e)
        try:
            logging.info(f"Prerequisitos: red PostgreSQL -> {result.get('postgresql', {}).get('network', {})}")
        except Exception:
            pass

        # Determinar y fijar alcance del remoto al local según VPN/IP (blindado)
        remote_can_reach = False
        ip = None
        try:
            if vpn_info.get("ok"):
                ip = vpn_info.get("ip")
                remote_can_reach = bool(ip)
        except Exception:
            ip = None
            remote_can_reach = False
        try:
            rep_cfg = dict(cfg.get("replication") or {})
            rep_cfg["remote_can_reach_local"] = bool(remote_can_reach)
            cfg["replication"] = rep_cfg
            with open(os.path.join(CONFIG_DIR, "config.json"), "w", encoding="utf-8") as cf:
                json.dump(cfg, cf, ensure_ascii=False, indent=2)
        except Exception:
            pass

        # Exportar o limpiar variables de entorno del túnel para consumidores posteriores
        if remote_can_reach and ip:
            try:
                local = cfg.get("db_local") or {}
                host = str(ip)
                port = int(local.get("port") or cfg.get("port") or 5432)
                dbname = local.get("database") or cfg.get("database") or "gimnasio"
                user = local.get("user") or cfg.get("user") or "postgres"
                pwd = local.get("password") or os.getenv("PGLOCAL_PASSWORD") or ""
                if not pwd and keyring:
                    try:
                        acct = f"{user}@{host}:{port}"
                        saved_pwd = keyring.get_password(KEYRING_SERVICE_NAME, acct)
                        if saved_pwd:
                            pwd = saved_pwd
                    except Exception:
                        pass
                os.environ["PGLOCAL_DSN"] = f"postgres://{user}:{pwd}@{host}:{port}/{dbname}?sslmode=prefer&application_name=gym_management_system"
                if pwd:
                    os.environ["PGLOCAL_PASSWORD"] = pwd
            except Exception:
                pass
        else:
            # Eliminar DSN local si no hay IP de túnel para evitar usos accidentales
            try:
                os.environ.pop("PGLOCAL_DSN", None)
            except Exception:
                pass

        # Decidir flujo de replicación
        sync_tables_path = os.path.join(CONFIG_DIR, "sync_tables.json")
        if os.path.exists(sync_tables_path):
            try:
                # Configurar replicación bidireccional completa si VPN se unió o hay alcance
                from utils_modules.replication_setup import ensure_bidirectional_replication  # type: ignore
                rep_res = ensure_bidirectional_replication(cfg)
                result["replication"] = rep_res
            except Exception as e:
                result["replication"] = {"ok": False, "error": str(e)}
        else:
            try:
                from utils_modules.replication_setup import ensure_logical_replication  # type: ignore
                rep_res = ensure_logical_replication(cfg)
                result["replication"] = rep_res
            except Exception as e:
                result["replication"] = {"ok": False, "error": str(e)}
        try:
            logging.info(f"Prerequisitos: replicación -> ok={result.get('replication', {}).get('ok')}")
        except Exception:
            pass
    except Exception:
        pass

    # Marcar como completado para este device si PostgreSQL está resuelto
    if result["postgresql"]["installed"]:
        write_marker(device_id, {
            "postgresql": result["postgresql"],
        })
        result["marked"] = True
        try:
            logging.info("Prerequisitos: marcado de primer ejecución creado")
        except Exception:
            pass

    return result

# ===== Programación automática de tareas (primer arranque) =====

def _task_marker_path(device_id: str) -> str:
    try:
        os.makedirs(PREREQ_MARK_DIR, exist_ok=True)
    except Exception:
        pass
    return os.path.join(PREREQ_MARK_DIR, f"{device_id}.tasks.json")


def _read_tasks_marker(device_id: str):
    try:
        path = _task_marker_path(device_id)
        if os.path.exists(path):
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
    except Exception:
        pass
    return None


def _write_tasks_marker(device_id: str, payload: dict) -> None:
    try:
        path = _task_marker_path(device_id)
        with open(path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
    except Exception:
        pass


def _task_exists(name: str) -> bool:
    code, out, err = run_cmd_capture(["schtasks", "/Query", "/TN", name], timeout=10)
    return code == 0


def _create_task_schtasks(name: str, action_cmd: str, schedule: dict):
    args = [
        "schtasks", "/Create",
        "/TN", name,
        "/F",
    ]
    sc = schedule.get("SC")
    mo = schedule.get("MO")
    st = schedule.get("ST")
    if sc:
        args.extend(["/SC", str(sc)])
    if mo is not None:
        args.extend(["/MO", str(mo)])
    if st:
        args.extend(["/ST", str(st)])
    # Acción
    args.extend(["/TR", action_cmd])
    code, out, err = run_cmd_capture(args, timeout=15)
    ok = (code == 0)
    msg = out or err
    return ok, msg

# NUEVO: eliminar tarea si existe

def _delete_task_schtasks(name: str):
    args = ["schtasks", "/Delete", "/TN", name, "/F"]
    code, out, err = run_cmd_capture(args, timeout=10)
    ok = (code == 0)
    msg = out or err
    return ok, msg


def ensure_scheduled_tasks(device_id: str) -> dict:
    """
    Aplica configuración de tareas programadas leyendo config.json (idempotente):
    - Respeta el flag maestro scheduled_tasks.enabled
    - Crea/actualiza/elimina: Uploader, Reconcile, Cleanup diario, Backup diario
    Retorna dict con estado por tarea.
    """
    result = {"ok": False, "tasks": {}}
    try:
        if not is_command_available("schtasks"):
            result["error"] = "schtasks no está disponible"
            return result

        cfg = _load_cfg()
        scfg = cfg.get("scheduled_tasks", {}) if isinstance(cfg.get("scheduled_tasks"), dict) else {}
        master_enabled = bool(scfg.get("enabled", False))

        scripts_dir = os.path.join(PROJECT_ROOT, "scripts")
        # Compatibilidad: si sólo existe 'reconcile', mapear a claves nuevas
        legacy = scfg.get("reconcile") if isinstance(scfg.get("reconcile"), dict) else {}
        if legacy and not isinstance(scfg.get("reconcile_r2l"), dict) and not isinstance(scfg.get("reconcile_l2r"), dict):
            try:
                scfg["reconcile_r2l"] = {
                    "enabled": bool(legacy.get("enabled", False)),
                    "interval_minutes": int(legacy.get("interval_minutes", 60)),
                }
                scfg["reconcile_l2r"] = {
                    "enabled": bool(legacy.get("enabled", False)),
                    "time": "02:00",
                }
            except Exception:
                pass

        # Usar ruta absoluta de wscript.exe para evitar problemas de PATH en contexto del Programador de Tareas
        wscript_exe = os.path.join(os.environ.get('WINDIR', 'C\\Windows'), 'System32', 'wscript.exe')
        # Definiciones de tareas con acciones totalmente silenciosas
        tasks_def = [
            {
                "key": "uploader",
                "name": "GymMS_Uploader",
                "action": f'"{wscript_exe}" "{os.path.join(scripts_dir, "run_sync_uploader_hidden.vbs")}"',
                "default": {"interval_minutes": 3},
                "type": "minute",
            },
            {
                "key": "reconcile_r2l",
                "name": "GymMS_ReconcileRemoteToLocal",
                "action": f'"{wscript_exe}" "{os.path.join(scripts_dir, "run_reconcile_remote_to_local_scheduled_hidden.vbs")}"',
                "default": {"interval_minutes": 60},
                "type": "minute",
            },
            {
                "key": "reconcile_l2r",
                "name": "GymMS_ReconcileLocalToRemote",
                "action": f'"{wscript_exe}" "{os.path.join(scripts_dir, "run_reconcile_scheduled_hidden.vbs")}"',
                "default": {"time": "02:00"},
                "type": "daily",
            },
            {
                "key": "cleanup",
                "name": "GymMS_DataCleanup",
                "action": f'PowerShell.exe -NoProfile -NonInteractive -NoLogo -WindowStyle Hidden -ExecutionPolicy Bypass -File "{os.path.join(scripts_dir, "run_cleanup_scheduled.ps1")}"',
                "default": {"time": "03:15"},
                "type": "daily",
            },
            {
                "key": "backup",
                "name": "GymMS_BackupDaily",
                "action": f'PowerShell.exe -NoProfile -NonInteractive -NoLogo -WindowStyle Hidden -ExecutionPolicy Bypass -File "{os.path.join(scripts_dir, "run_backup_scheduled.ps1")}"',
                "default": {"time": "02:30"},
                "type": "daily",
            },
        ]

        # Eliminar tareas legacy con nombres antiguos (evita duplicados y acciones defectuosas)
        try:
            legacy_names = [
                "GymMS_Reconcile_RemoteToLocal",
                "GymMS_Reconcile_LocalToRemote",
            ]
            for ln in legacy_names:
                if _task_exists(ln):
                    _delete_task_schtasks(ln)
        except Exception:
            pass

        for td in tasks_def:
            key = td["key"]
            name = td["name"]
            action = td["action"]
            tcfg = scfg.get(key, {}) if isinstance(scfg.get(key), dict) else {}
            # Si falta el flag 'enabled' en subtarea, habilitar por defecto el uploader
            # cuando el maestro esté activo, para compatibilidad con configs antiguas.
            if tcfg.get("enabled") is None and key == "uploader":
                sub_enabled = True
            else:
                sub_enabled = bool(tcfg.get("enabled", False))
            enabled = bool(master_enabled and sub_enabled)

            # Construir horario deseado
            schedule = {}
            if td["type"] == "minute":
                iv = int(tcfg.get("interval_minutes", td["default"]["interval_minutes"]))
                if iv < 1:
                    iv = td["default"]["interval_minutes"]
                schedule = {"SC": "MINUTE", "MO": iv}
            else:
                hhmm = str(tcfg.get("time", td["default"]["time"]))
                if not hhmm or ":" not in hhmm:
                    hhmm = td["default"]["time"]
                schedule = {"SC": "DAILY", "ST": hhmm}

            exists_before = _task_exists(name)

            if not enabled:
                if exists_before:
                    del_ok, del_msg = _delete_task_schtasks(name)
                    result["tasks"][name] = {
                        "enabled": False,
                        "exists_before": True,
                        "deleted": bool(del_ok),
                        "message": del_msg,
                    }
                else:
                    result["tasks"][name] = {
                        "enabled": False,
                        "exists_before": False,
                        "deleted": False,
                        "message": "task disabled by config",
                    }
                continue

            # enabled: recrear para asegurar schedule actualizado
            created = False
            msg = ""
            if exists_before:
                _delete_task_schtasks(name)
            ok, cmsg = _create_task_schtasks(name, action, schedule)
            created = bool(ok)
            msg = cmsg
            result["tasks"][name] = {
                "enabled": True,
                "exists_before": exists_before,
                "created": created,
                "schedule": schedule,
                "message": msg,
            }

        try:
            payload = {"tasks": result["tasks"], "applied_at": datetime.utcnow().isoformat() + "Z", "config_snapshot": scfg}
            _write_tasks_marker(device_id, payload)
        except Exception:
            pass
        result["ok"] = True
    except Exception as e:
        result["error"] = str(e)
    return result


def _find_pg_data_dir(required_major: int = 17) -> Optional[str]:
    try:
        base = os.path.join("C:\\Program Files\\PostgreSQL", str(required_major), "data")
        if os.path.exists(os.path.join(base, "postgresql.conf")):
            return base
    except Exception:
        pass
    try:
        candidates = glob.glob("C:\\Program Files\\PostgreSQL\\*\\data\\postgresql.conf")
        for conf in candidates:
            d = os.path.dirname(conf)
            if os.path.exists(os.path.join(d, "pg_hba.conf")):
                return d
    except Exception:
        pass
    return None


def _ensure_listen_addresses(data_dir: str, address: str = "*") -> bool:
    path = os.path.join(data_dir, "postgresql.conf")
    if not os.path.exists(path):
        return False
    changed = False
    try:
        with open(path, "r", encoding="utf-8") as f:
            lines = f.readlines()
        new_lines = []
        found = False
        desired_line = f"listen_addresses = '{address}'\n"
        for ln in lines:
            if ln.strip().startswith("listen_addresses"):
                found = True
                if address in ln:
                    new_lines.append(ln)
                else:
                    new_lines.append(desired_line)
                    changed = True
            else:
                new_lines.append(ln)
        if not found:
            new_lines.append("\n" + desired_line)
            changed = True
        if changed:
            with open(path, "w", encoding="utf-8") as f:
                f.writelines(new_lines)
        return changed
    except Exception:
        return False


def _wireguard_allowed_ranges_from_config(conf_path: str) -> List[str]:
    ranges: List[str] = []
    try:
        if not os.path.exists(conf_path):
            return ranges
        with open(conf_path, "r", encoding="utf-8") as f:
            txt = f.read()
        for line in txt.splitlines():
            line = line.strip()
            if line.lower().startswith("allowedips"):
                # Ej: AllowedIPs = 10.10.0.2/32, 10.10.0.0/24
                parts = line.split("=", 1)
                if len(parts) == 2:
                    ips = [p.strip() for p in parts[1].split(",") if p.strip()]
                    for ip in ips:
                        if "/" in ip:
                            ranges.append(ip)
                        else:
                            ranges.append(f"{ip}/32")
        # dedup
        uniq = []
        for r in ranges:
            if r not in uniq:
                uniq.append(r)
        return uniq
    except Exception:
        return ranges


def _ensure_pg_hba_ranges(data_dir: str, ranges: List[str]) -> bool:
    path = os.path.join(data_dir, "pg_hba.conf")
    if not os.path.exists(path):
        return False
    try:
        with open(path, "r", encoding="utf-8") as f:
            content = f.read()
        to_add = []
        for rng in ranges:
            line = f"host all all {rng} md5"
            if line not in content:
                to_add.append(line)
        if to_add:
            with open(path, "a", encoding="utf-8") as f:
                f.write("\n# GymMS VPN access\n")
                for l in to_add:
                    f.write(l + "\n")
            return True
        return False
    except Exception:
        return False


def _ensure_windows_firewall_postgres(port: int, ranges: List[str]) -> dict:
    if os.name != "nt":
        return {"ok": True, "message": "Firewall no aplica (no Windows)"}
    rule_name = f"GymMS-PostgreSQL-{port}"
    addr = ",".join(ranges) if ranges else "Any"
    ps_cmd = (
        "$n='" + rule_name + "';"
        "$p=" + str(port) + ";"
        "$addr='" + addr + "';"
        "$r=Get-NetFirewallRule -DisplayName $n -ErrorAction SilentlyContinue;"
        "if(-not $r){ New-NetFirewallRule -DisplayName $n -Direction Inbound -Action Allow -Protocol TCP -LocalPort $p -Profile Domain,Private -RemoteAddress $addr }"
        " else { Set-NetFirewallRule -DisplayName $n -Direction Inbound -Action Allow -Protocol TCP -LocalPort $p -Profile Domain,Private; Set-NetFirewallAddressFilter -DisplayName $n -RemoteAddress $addr }"
    )
    code, out, err = run_cmd_capture(["powershell", "-NoProfile", "-ExecutionPolicy", "Bypass", "-Command", ps_cmd], timeout=60)
    return {"ok": code == 0, "message": out or err}


def _restart_postgres_service() -> dict:
    if os.name != "nt":
        return {"ok": True, "message": "No Windows"}
    ps_cmd = (
        "$svc=Get-Service | Where-Object {$_.Name -like 'postgresql*'} | Select-Object -First 1;"
        "if($svc){ Restart-Service -Name $svc.Name -Force; Write-Output $svc.Name } else { Write-Error 'Servicio PostgreSQL no encontrado' }"
    )
    code, out, err = run_cmd_capture(["powershell", "-NoProfile", "-ExecutionPolicy", "Bypass", "-Command", ps_cmd], timeout=90)
    return {"ok": code == 0, "message": out or err}


def ensure_postgres_network_access(cfg: dict) -> dict:
    # Determinar puerto local y rangos permitidos según VPN
    local = cfg.get("db_local") or {}
    try:
        port = int(local.get("port") or cfg.get("port") or 5432)
    except Exception:
        port = 5432
    vpn = cfg.get("vpn") or {}
    provider = (vpn.get("provider") or os.getenv("VPN_PROVIDER") or "").lower()
    ranges: List[str] = []
    # Tailscale: rango oficial 100.64.0.0/10
    if provider == "tailscale" or os.getenv("TAILSCALE_AUTHKEY"):
        ranges.append("100.64.0.0/10")
    # WireGuard: intentar extraer AllowedIPs
    if provider == "wireguard" or os.getenv("WIREGUARD_CONFIG_B64") or os.getenv("WIREGUARD_CONFIG_PATH"):
        conf_path = vpn.get("wireguard_config_path") or os.getenv("WIREGUARD_CONFIG_PATH") or ""
        wg_ranges = _wireguard_allowed_ranges_from_config(conf_path) if conf_path else []
        if wg_ranges:
            ranges.extend(wg_ranges)
        else:
            # Fallback común si no se puede parsear config
            ranges.append("10.0.0.0/8")
    # Resolver directorio de datos
    data_dir = _find_pg_data_dir(17)
    if not data_dir:
        return {"ok": False, "message": "No se encontró directorio de datos de PostgreSQL", "changed_conf": False, "changed_hba": False, "firewall": {"ok": False}}
    # listen_addresses: preferir Tail IP o WireGuard IP si disponible
    tail_ip = os.getenv("TAILSCALE_IPV4") or os.getenv("WIREGUARD_IPV4") or ""
    desired = f"localhost,{tail_ip}" if tail_ip else "*"
    ch_conf = _ensure_listen_addresses(data_dir, desired)
    ch_hba = _ensure_pg_hba_ranges(data_dir, ranges)
    fw = _ensure_windows_firewall_postgres(port, ranges)
    # Reiniciar servicio si hubo cambios
    if ch_conf or ch_hba:
        _restart_postgres_service()
    return {"ok": True, "message": "postgresql.conf/pg_hba.conf firewall asegurados", "changed_conf": ch_conf, "changed_hba": ch_hba, "firewall": fw}