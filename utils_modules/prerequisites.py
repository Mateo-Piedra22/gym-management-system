import os
import json
import shutil
import subprocess
import sys
from datetime import datetime
from typing import Optional, Tuple


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


def is_java_installed() -> bool:
    if not is_command_available("java"):
        return False
    code, out, err = run_cmd_capture(["java", "-version"], timeout=10)
    if code != 0:
        return False
    text = (out + "\n" + err).lower()
    # Acepta cualquier Java válido; preferimos 17+
    return "version" in text or "openjdk" in text or "temurin" in text


def install_java_temurin17() -> Tuple[bool, str]:
    # Winget ID para Temurin JRE 17
    args = [
        "winget", "install", "-e", "--id", "EclipseAdoptium.Temurin.17.JRE", "--source", "winget",
    ]
    code, out, err = run_cmd_capture(args, timeout=60)
    ok = code == 0
    msg = out if ok else err
    return ok, msg


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
    # Intento primario
    args_primary = ["winget", "install", "-e", "--id", "PostgreSQL.PostgreSQL", "--version", "17", "--source", "winget"]
    code, out, err = run_cmd_capture(args_primary, timeout=120)
    if code == 0:
        return True, out
    # Intento alterno por algunos catálogos
    args_alt = ["winget", "install", "-e", "--id", "EnterpriseDB.PostgreSQL", "--version", "17", "--source", "winget"]
    code2, out2, err2 = run_cmd_capture(args_alt, timeout=120)
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
    # Prioridad: config.json -> entorno común
    val = str(cfg.get("password", ""))
    if val:
        return val
    for env_key in ("PGPASSWORD", "POSTGRES_PASSWORD", "DB_PASSWORD", "PG_PASS", "DATABASE_PASSWORD"):
        v = os.getenv(env_key, "")
        if v:
            return v
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
        code, out, err = run_cmd_capture(args, timeout=30)
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
        proc = subprocess.run(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=30, text=True, env=env)
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


def ensure_prerequisites(device_id: str) -> dict:
    """
    Verifica e instala (si faltan) prerequisitos de programa:
    - Java (Temurin JRE 17)
    - PostgreSQL 17
    Marca la ejecución por device_id para evitar repetir en el futuro.
    Retorna diccionario con resultados por componente.
    """
    _ensure_dirs()
    result = {
        "java": {"installed": False, "attempted": False, "message": ""},
        "postgresql": {"installed": False, "attempted": False, "message": ""},
        "marked": False,
    }

    # Si ya hay marca, evitar reinstalar; solo reportar estado actual
    marker = read_marker(device_id)
    if marker:
        result["java"]["installed"] = is_java_installed()
        result["postgresql"]["installed"] = is_postgresql_installed(17)
        result["marked"] = True
        return result

    # Java
    if is_java_installed():
        result["java"]["installed"] = True
    else:
        ok, msg = install_java_temurin17()
        result["java"]["attempted"] = True
        result["java"]["installed"] = ok and is_java_installed()
        result["java"]["message"] = msg

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

    # Marcar como completado para este device si ambos están resueltos
    if result["java"]["installed"] and result["postgresql"]["installed"]:
        write_marker(device_id, {
            "java": result["java"],
            "postgresql": result["postgresql"],
        })
        result["marked"] = True

    return result