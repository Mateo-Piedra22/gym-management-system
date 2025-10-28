#!/usr/bin/env python3
"""
Compila y empaqueta el Gym Management System usando PyInstaller.
- Genera distribución tipo onedir en 'dist/' con ejecutables GUI y recursos completos.
- Soporte verificado para PyQt6, psycopg2-binary, y librerías del proyecto.
Requisitos:
- pip install pyinstaller
- En Windows: Microsoft Visual C++ 14.0+ (Build Tools for Visual Studio)

Notas:
- PyInstaller crea ejecutables compatibles con todas las dependencias.
- Se mantiene toda la lógica de inclusión de recursos y corrección de _psycopg.pyd.
- Formato de salida: onedir (carpeta 'dist/' con ejecutables y dependencias).
"""
import os
import sys
import shutil
import platform
from pathlib import Path
import subprocess
import argparse
import json
import tempfile

PROJECT_ROOT = Path(__file__).resolve().parent
ENTRY_SCRIPT = PROJECT_ROOT / "main.py"
GYMMSW_SCRIPT = PROJECT_ROOT / "GymMSW.py"
DB_CONFIG_SCRIPT = PROJECT_ROOT / "cdbconfig.py"
AUTO_SETUP_SCRIPT = PROJECT_ROOT / "scripts" / "auto_setup.py"
APP_NAME = "Gym Management System"
ICON_PATH = PROJECT_ROOT / "assets" / "gym_logo.ico"
DIST_DIR = PROJECT_ROOT / "dist"
LOCAL_BUILD_ROOT = Path(os.getenv("LOCALAPPDATA", str(PROJECT_ROOT))) / "GymMS_PyInstallerBuild"

EXCLUDE_DIRS = {"build", "dist", "__pycache__", ".git", ".hg", ".svn", ".idea", ".vscode", ".trae", "nuitka_build_temp"}

# Extensiones de archivos de datos que deben copiarse junto al ejecutable
INCLUDE_FILE_EXTS = {
    ".xlsx", ".xls", ".csv", ".json", ".txt", ".md", ".ini", ".cfg",
    ".qss", ".ico", ".png", ".jpg", ".jpeg", ".svg", ".gif",
    ".sqlite", ".db", ".pdf", ".css", ".html", ".xml", ".yaml", ".yml",
}

# === FUNCION MEJORADA: DETECTA Y CORRIGE _psycopg.pyd AUTOMATICAMENTE ===
def ensure_psycopg2_pyd_fixed():
    """Asegura que _psycopg.pyd exista en la carpeta de psycopg2, incluso si tiene nombre específico."""
    try:
        import psycopg2
        from pathlib import Path as _P

        pkg_dir = _P(psycopg2.__file__).resolve().parent
        pyd_generic = pkg_dir / "_psycopg.pyd"

        # Si ya existe, no hacer nada
        if pyd_generic.exists():
            print("[OK] _psycopg.pyd ya existe.")
            return

        # Buscar cualquier archivo que coincida con _psycopg.*.pyd
        candidates = list(pkg_dir.glob("_psycopg.*.pyd"))
        if candidates:
            src = candidates[0]
            shutil.copy2(src, pyd_generic)
            print(f"[OK] Copiado {src.name} -> _psycopg.pyd")
        else:
            print("[WARNING] No se encontro ningun archivo _psycopg.*.pyd en", pkg_dir)

    except Exception as e:
        print(f"[ERROR] al corregir _psycopg.pyd: {e}")


def find_and_include_psycopg2_dlls():
    """Encuentra _psycopg.pyd y todas las DLLs necesarias para psycopg2-binary en Windows."""
    try:
        import psycopg2
        from pathlib import Path as _P

        pkg_dir = _P(psycopg2.__file__).resolve().parent
        inclusions = []

        # Asegurar que _psycopg.pyd exista antes de copiar
        ensure_psycopg2_pyd_fixed()

        # Incluir _psycopg.pyd
        pyd_file = pkg_dir / "_psycopg.pyd"
        if pyd_file.exists():
            inclusions.append((str(pyd_file), "_psycopg.pyd"))
        else:
            print("[WARNING] _psycopg.pyd no encontrado en", pkg_dir)

        # Incluir todas las DLLs
        for file in pkg_dir.glob("*.dll"):
            inclusions.append((str(file), file.name))

        # Incluir DLLs en subdirectorios
        for dll_file in pkg_dir.rglob("*.dll"):
            if dll_file.parent != pkg_dir:
                rel_path = dll_file.relative_to(pkg_dir)
                inclusions.append((str(dll_file), str(rel_path)))

        if inclusions:
            print(f"[OK] Incluyendo {len(inclusions)} archivos nativos de psycopg2:")
            for src, dest in inclusions:
                print(f"   -> {dest}")
        else:
            print("[WARNING] No se encontraron DLLs ni _psycopg.pyd. El .exe probablemente falle!")

        return inclusions

    except Exception as e:
        print(f"[ERROR] al buscar archivos nativos de psycopg2: {e}")
        return []


def ensure_pyinstaller_installed():
    try:
        import PyInstaller  # noqa: F401
    except Exception:
        print("PyInstaller no esta instalado. Instalando...")
        subprocess.check_call([sys.executable, "-m", "pip", "install", "--upgrade", "pyinstaller"])
        print("PyInstaller instalado correctamente.")


def _is_pure_code_dir(dir_path: Path) -> bool:
    """Devuelve True si el directorio contiene solo codigo/cache Python (sin datos)."""
    code_exts = {".py", ".pyc", ".pyo"}
    for root, dirs, files in os.walk(dir_path):
        dirs[:] = [d for d in dirs if d != "__pycache__"]
        for f in files:
            ext = Path(f).suffix.lower()
            if ext not in code_exts:
                return False
    return True


def ensure_runtime_dirs_present():
    """Crea directorios de runtime esperados para evitar errores en tiempo de ejecucion."""
    try:
        (PROJECT_ROOT / "logs").mkdir(parents=True, exist_ok=True)
    except Exception:
        pass


def check_dependency_versions():
    """Verifica versiones compatibles de dependencias clave y emite advertencias si difieren."""
    expected = {}
    req_path = PROJECT_ROOT / "requirements.txt"
    try:
        if req_path.exists():
            for line in req_path.read_text(encoding="utf-8").splitlines():
                s = line.strip()
                if not s or s.startswith("#"):
                    continue
                for pkg in ("fastapi", "uvicorn", "starlette", "psycopg2-binary"):
                    if s.lower().startswith(pkg):
                        expected[pkg] = s
    except Exception:
        pass

    def _ver(pkg_name: str):
        try:
            mod = __import__(pkg_name.split("[")[0])
            v = getattr(mod, "__version__", None)
            return str(v) if v else "(desconocida)"
        except Exception:
            return "(no importable)"

    for pkg in ("fastapi", "uvicorn", "starlette", "psycopg2"):
        try:
            v = _ver(pkg)
            exp = expected.get(pkg if pkg != "psycopg2" else "psycopg2-binary")
            if exp:
                print(f"- {pkg}: instalada {v} | requirements: {exp}")
            else:
                print(f"- {pkg}: instalada {v}")
        except Exception:
            pass


def discover_data_inclusions(project_root: Path):
    inclusions: list[tuple[str, str]] = []
    for item in project_root.iterdir():
        if item.is_dir():
            name = item.name
            if name in EXCLUDE_DIRS:
                continue
            if _is_pure_code_dir(item):
                continue
            inclusions.append((str(item), name))
        elif item.is_file():
            if item.name == Path(__file__).name:
                continue
            if item.suffix.lower() in INCLUDE_FILE_EXTS:
                inclusions.append((str(item), item.name))
    return inclusions


def copy_data_to_dist(data_pairs, dist_dir):
    """Copia todos los recursos descubiertos al directorio dist."""
    for src, dest in data_pairs:
        src_path = Path(src)
        dest_path = dist_dir / dest
        if src_path.is_file():
            dest_path.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(src_path, dest_path)
        elif src_path.is_dir():
            if dest_path.exists():
                shutil.rmtree(dest_path)
            shutil.copytree(src_path, dest_path)


# --- Generacion de credenciales remotas para primera ejecucion (bootstrap) ---

def _env_get(name: str) -> str | None:
    v = os.getenv(name)
    return v if v and str(v).strip() else None


def generate_remote_bootstrap_from_env(config_dir: Path) -> bool:
    try:
        config_dir.mkdir(parents=True, exist_ok=True)
    except Exception:
        pass

    dsn = _env_get("PGREMOTE_DSN") or _env_get("DATABASE_URL_REMOTE")
    host = _env_get("PGREMOTE_HOST")
    port = _env_get("PGREMOTE_PORT")
    db = _env_get("PGREMOTE_DB") or _env_get("PGREMOTE_DATABASE")
    user = _env_get("PGREMOTE_USER")
    password = _env_get("PGREMOTE_PASSWORD")
    sslmode = _env_get("PGREMOTE_SSLMODE")
    appname = _env_get("PGREMOTE_APPNAME") or "gym_management_system"
    timeout = _env_get("PGREMOTE_TIMEOUT")

    vpn_provider = (_env_get("VPN_PROVIDER") or "").lower()
    ts_authkey = _env_get("TAILSCALE_AUTHKEY")
    ts_hostname_prefix = _env_get("TAILSCALE_HOSTNAME_PREFIX")
    ts_control_url = _env_get("TAILSCALE_CONTROL_URL")
    ts_accept_routes = _env_get("TAILSCALE_ACCEPT_ROUTES")
    ts_accept_dns = _env_get("TAILSCALE_ACCEPT_DNS")
    ts_advertise_tags = _env_get("TAILSCALE_ADVERTISE_TAGS")

    wg_config_b64 = _env_get("WIREGUARD_CONFIG_B64")
    wg_config_path = _env_get("WIREGUARD_CONFIG_PATH")

    remote_present = any([dsn, host, user, password, db])
    vpn_present = any([
        vpn_provider,
        ts_authkey, ts_hostname_prefix, ts_control_url, ts_accept_routes, ts_accept_dns, ts_advertise_tags,
        wg_config_b64, wg_config_path,
    ])

    if not (remote_present or vpn_present):
        return False

    payload = {"remote": {}, "vpn": {}}
    r = payload["remote"]
    if dsn:
        r["dsn"] = dsn
    if host:
        r["host"] = host
    if port:
        try:
            r["port"] = int(port)
        except Exception:
            r["port"] = port
    if db:
        r["database"] = db
    if user:
        r["user"] = user
    if password:
        r["password"] = password
    if sslmode:
        r["sslmode"] = sslmode
    if appname:
        r["application_name"] = appname
    if timeout:
        try:
            r["connect_timeout"] = int(timeout)
        except Exception:
            r["connect_timeout"] = timeout

    v = payload["vpn"]
    if vpn_provider:
        v["provider"] = vpn_provider
    if ts_authkey:
        v["tailscale_auth_key"] = ts_authkey
    if ts_hostname_prefix:
        v["hostname_prefix"] = ts_hostname_prefix
    if ts_control_url:
        v["control_url"] = ts_control_url
    def _to_bool(s: str | None) -> bool | None:
        if s is None:
            return None
        s2 = str(s).strip().lower()
        if s2 in {"1", "true", "yes", "y"}:
            return True
        if s2 in {"0", "false", "no", "n"}:
            return False
        return None
    br = _to_bool(ts_accept_routes)
    bd = _to_bool(ts_accept_dns)
    if br is not None:
        v["accept_routes"] = br
    if bd is not None:
        v["accept_dns"] = bd
    if ts_advertise_tags:
        v["advertise_tags"] = [t.strip() for t in ts_advertise_tags.split(",") if t.strip()]
    if wg_config_b64:
        v["wireguard_config_b64"] = wg_config_b64
    if wg_config_path:
        v["wireguard_config_path"] = wg_config_path

    out_path = config_dir / "remote_bootstrap.json"
    try:
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
        print(f"- Generado {out_path} desde entorno (remoto/VPN).")
        return True
    except Exception as e:
        print(f"[WARNING] No se pudo generar remote_bootstrap.json: {e}")
        return False


def build_with_pyinstaller(script_path: Path, target_name: str, base_dist_dir: Path):
    """Construye un ejecutable con PyInstaller en modo onedir."""
    # Directorio de trabajo para PyInstaller
    work_dir = base_dist_dir / "build"
    work_dir.mkdir(parents=True, exist_ok=True)
    
    # Directorio de salida
    dist_dir = base_dist_dir / "dist"
    dist_dir.mkdir(parents=True, exist_ok=True)

    # Comando basico de PyInstaller
    cmd = [
        sys.executable, "-m", "PyInstaller",
        "--onedir",  # Formato onedir obligatorio
        "--windowed",  # Aplicacion GUI sin consola
        "--clean",  # Limpiar antes de construir
        "--noconfirm",  # No pedir confirmacion
        "--exclude", "PyQt5",  # Excluir PyQt5 para evitar conflictos con PyQt6
        f"--workpath={work_dir}",
        f"--distpath={dist_dir}",
        f"--specpath={base_dist_dir}",
        f"--name={target_name}",  # Especificar el nombre del ejecutable
    ]

    # Agregar icono si existe
    if ICON_PATH.exists():
        cmd.append(f"--icon={ICON_PATH}")

    # Agregar hooks para PyQt6
    cmd.extend([
        "--hidden-import=PyQt6.sip",
        "--hidden-import=PyQt6.QtCore",
        "--hidden-import=PyQt6.QtGui",
        "--hidden-import=PyQt6.QtWidgets",
    ])

    # Agregar otros imports ocultos necesarios
    hidden_imports = [
        "reportlab", "psutil", "keyring", "openpyxl", "xlwt", "xlsxtpl",
        "jinja2", "pywa", "itsdangerous", "psycopg2", "matplotlib", "pandas"
    ]
    
    for imp in hidden_imports:
        cmd.append(f"--hidden-import={imp}")

    # Archivos de datos a incluir
    data_files = discover_data_inclusions(PROJECT_ROOT)
    for src, dest in data_files:
        # Para directorios, necesitamos incluir cada archivo individualmente
        src_path = Path(src)
        if src_path.is_dir():
            # Para directorios, usamos --add-data con el patron recursivo
            cmd.append(f"--add-data={src}{os.pathsep}{dest}")
        else:
            cmd.append(f"--add-data={src}{os.pathsep}{dest}")

    # Incluir assets/templates especificamente
    templates_dir = PROJECT_ROOT / "assets" / "templates"
    if templates_dir.exists():
        cmd.append(f"--add-data={templates_dir}{os.pathsep}assets/templates")

    # Incluir psycopg2 binaries
    psycopg2_files = find_and_include_psycopg2_dlls()
    for src, dest in psycopg2_files:
        cmd.append(f"--add-data={src}{os.pathsep}{dest}")

    # Agregar el script a compilar
    cmd.append(str(script_path))

    # Configurar caches fuera de OneDrive para evitar bloqueos
    env = os.environ.copy()
    cache_base = Path(os.getenv("LOCALAPPDATA", str(tempfile.gettempdir()))) / "PyInstallerCacheGymMS"
    env.setdefault("PYINSTALLER_CACHE_DIR", str(cache_base))

    print(f"\nPyInstaller: compilando {script_path.name} -> {target_name}...")
    print(f"[INFO] Output dir: {dist_dir}")
    print(f"[INFO] Cache dir: {env.get('PYINSTALLER_CACHE_DIR')}")
    
    print("Comando PyInstaller:", " ".join(cmd))
    
    result = subprocess.run(cmd, capture_output=True, text=True, env=env)
    if result.returncode != 0:
        print("[ERROR] en compilacion PyInstaller:")
        print(result.stderr)
        return False  # Cambiamos a return False en lugar de sys.exit(1)

    # Verificar el directorio generado
    expected_dir = dist_dir / target_name
    if expected_dir.exists():
        print(f"[OK] Ejecutable generado: {expected_dir}")
        try:
            size_mb = sum(f.stat().st_size for f in expected_dir.rglob('*') if f.is_file()) / (1024 * 1024)
            print(f"[INFO] Tamaño del ejecutable: {size_mb:.2f} MB")
        except Exception:
            pass
        return True
    else:
        # Verificar si hay otros directorios en dist_dir
        possible_dirs = list(dist_dir.iterdir())
        if possible_dirs:
            print(f"[WARNING] No se encontro el directorio esperado '{expected_dir}', pero se encontraron otros directorios:")
            for d in possible_dirs:
                print(f"   - {d}")
        else:
            print(f"[ERROR] No se encontro el directorio generado: {expected_dir}")
        return False


def main():
    parser = argparse.ArgumentParser(description="Compila el sistema con PyInstaller")
    args_cli = parser.parse_args()

    if not ENTRY_SCRIPT.exists():
        print(f"[ERROR] No se encontro el archivo de entrada: {ENTRY_SCRIPT}")
        sys.exit(1)

    ensure_pyinstaller_installed()
    ensure_runtime_dirs_present()
    check_dependency_versions()
    generate_remote_bootstrap_from_env(PROJECT_ROOT / "config")

    # Crear directorio temporal para builds individuales fuera de OneDrive
    base_dist = LOCAL_BUILD_ROOT
    if base_dist.exists():
        shutil.rmtree(base_dist)
    base_dist.mkdir(parents=True, exist_ok=True)  # Asegurarse de que se creen los directorios padres

    # Compilar los tres ejecutables
    executables = [
        (ENTRY_SCRIPT, APP_NAME.replace(" ", "_")),
        (GYMMSW_SCRIPT, "GymMSW"),
        (DB_CONFIG_SCRIPT, "cdbconfig"),
        (AUTO_SETUP_SCRIPT, "auto_setup"),
    ]

    build_results = []
    for script, target_name in executables:
        if script.exists():
            success = build_with_pyinstaller(script, target_name, base_dist)
            build_results.append((script, target_name, success))
        else:
            print(f"[WARNING] Script no encontrado, omitiendo: {script}")
            build_results.append((script, target_name, False))

    # Verificar que al menos uno se haya construido correctamente
    successful_builds = [r for r in build_results if r[2]]
    if not successful_builds:
        print("\n[ERROR] Ningun ejecutable se genero correctamente.")
        sys.exit(1)

    # Crear dist/ final
    if DIST_DIR.exists():
        shutil.rmtree(DIST_DIR)
    DIST_DIR.mkdir(parents=True, exist_ok=True)

    # Fusionar todos los ejecutables en uno solo (tomamos el primero como base)
    first_script, first_target_name, first_success = build_results[0]
    if first_success:
        base_output = base_dist / "dist" / first_target_name
        
        if base_output.exists():
            shutil.copytree(base_output, DIST_DIR, dirs_exist_ok=True)
            print(f"[INFO] Copiado base: {base_output} -> {DIST_DIR}")

        # Copiar los otros ejecutables al dist final
        for script, target_name, success in build_results[1:]:
            if success and script.exists():
                src_dir = base_dist / "dist" / target_name
                if src_dir.exists():
                    # Copiar solo el ejecutable y sus DLLs
                    exe_name = target_name + (".exe" if platform.system() == "Windows" else "")
                    src_exe = src_dir / exe_name
                    if src_exe.exists():
                        shutil.copy2(src_exe, DIST_DIR / exe_name)
                        print(f"[INFO] Copiado ejecutable adicional: {src_exe} -> {DIST_DIR / exe_name}")

        # Copiar recursos adicionales
        data_pairs = discover_data_inclusions(PROJECT_ROOT)
        templates_dir = PROJECT_ROOT / "assets" / "templates"
        if templates_dir.exists():
            data_pairs.append((str(templates_dir), os.path.join("assets", "templates")))
        copy_data_to_dist(data_pairs, DIST_DIR)

        # Asegurar que remote_bootstrap.json este en dist/config/
        remote_bootstrap_src = PROJECT_ROOT / "config" / "remote_bootstrap.json"
        if remote_bootstrap_src.exists():
            (DIST_DIR / "config").mkdir(exist_ok=True)
            shutil.copy2(remote_bootstrap_src, DIST_DIR / "config" / "remote_bootstrap.json")

        # Limpieza
        shutil.rmtree(base_dist, ignore_errors=True)

        main_exe = DIST_DIR / (APP_NAME.replace(" ", "_") + (".exe" if platform.system() == "Windows" else ""))
        if main_exe.exists():
            try:
                size_mb = main_exe.stat().st_size / (1024 * 1024)
                print(f"\n[OK] Listo! Ejecutable principal: {main_exe} ({size_mb:.2f} MB)")
                print("[INFO] Distribucion completa en 'dist/' (formato onedir)")
            except Exception:
                print(f"\n[OK] Listo! Ejecutable principal: {main_exe}")
                print("[INFO] Distribucion completa en 'dist/' (formato onedir)")
        else:
            print("\n[WARNING] El ejecutable principal no se genero, pero otros componentes pueden haberse creado.")
    else:
        print(f"\n[ERROR] No se pudo construir el ejecutable principal: {first_script}")


if __name__ == "__main__":
    main()