import os
import json
import shutil
import subprocess
import sys
from datetime import datetime
from typing import Optional, Tuple
from urllib.parse import urlparse

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
        proc = subprocess.run(
            args,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=timeout,
            text=True,
        )
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
        dsn = str(remote.get("dsn") or os.getenv("PGREMOTE_DSN", ""))
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
    result = {
        "postgresql": {"installed": False, "attempted": False, "message": ""},
        "marked": False,
        "remote_bootstrap": {"applied": False, "message": ""},
    }

    # Aplicar bootstrap remoto (idempotente, antes de salida temprana por marker)
    try:
        result["remote_bootstrap"] = apply_remote_bootstrap_if_present()
    except Exception as e:
        result["remote_bootstrap"] = {"applied": False, "error": str(e)}

    # Si ya hay marca, evitar reinstalar; solo reportar estado actual
    marker = read_marker(device_id)
    if marker:
        # Solo reportar estado actual de PostgreSQL
        result["postgresql"]["installed"] = is_postgresql_installed(17)
        result["marked"] = True
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

        tasks_def = [
            {
                "key": "uploader",
                "name": "GymMS_Uploader",
                "action": f'PowerShell.exe -NoProfile -NonInteractive -NoLogo -WindowStyle Hidden -ExecutionPolicy Bypass -File "{os.path.join(scripts_dir, "run_sync_uploader.ps1")}"',
                "default": {"interval_minutes": 3},
                "type": "minute",
            },
            {
                "key": "reconcile_r2l",
                "name": "GymMS_ReconcileRemoteToLocal",
                "action": f'PowerShell.exe -NoProfile -NonInteractive -NoLogo -WindowStyle Hidden -ExecutionPolicy Bypass -File "{os.path.join(scripts_dir, "run_reconcile_remote_to_local_scheduled.ps1")}"',
                "default": {"interval_minutes": 60},
                "type": "minute",
            },
            {
                "key": "reconcile_l2r",
                "name": "GymMS_ReconcileLocalToRemote",
                "action": f'PowerShell.exe -NoProfile -NonInteractive -NoLogo -WindowStyle Hidden -ExecutionPolicy Bypass -File "{os.path.join(scripts_dir, "run_reconcile_scheduled.ps1")}"',
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

        for td in tasks_def:
            key = td["key"]
            name = td["name"]
            action = td["action"]
            tcfg = scfg.get(key, {}) if isinstance(scfg.get(key), dict) else {}
            enabled = bool(master_enabled and tcfg.get("enabled", False))

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

    # PostgreSQL
    if is_postgresql_installed(17):
        result["postgresql"]["installed"] = True
    else:
        ok, msg = install_postgresql_17()
        result["postgresql"]["attempted"] = True
        result["postgresql"]["installed"] = ok and is_postgresql_installed(17)
        result["postgresql"]["message"] = msg

    # Si PostgreSQL está instalado, intentar crear la base definida en config.json
    try:
        if result["postgresql"]["installed"]:
            db_ok, db_msg = create_database_from_config(17)
            result["postgresql"]["db_created"] = bool(db_ok)
            if not db_ok and db_msg:
                result["postgresql"]["db_message"] = db_msg
    except Exception as e:
        result["postgresql"]["db_created"] = False
        result["postgresql"]["db_message"] = str(e)

    # Intentar asegurar replicación lógica si hay configuración disponible (no bloqueante)
    try:
        cfg = _load_cfg()
        # Sólo si existe archivo de tablas de sincronización, asumimos intención de replicar
        sync_tables_path = os.path.join(CONFIG_DIR, "sync_tables.json")
        if os.path.exists(sync_tables_path):
            try:
                from utils_modules.replication_setup import ensure_logical_replication  # type: ignore
                rep_res = ensure_logical_replication(cfg)
                result["replication"] = rep_res
            except Exception as e:
                result["replication"] = {"ok": False, "error": str(e)}
    except Exception:
        pass

    # Marcar como completado para este device si PostgreSQL está resuelto
    if result["postgresql"]["installed"]:
        write_marker(device_id, {
            "postgresql": result["postgresql"],
        })
        result["marked"] = True

    return result