import sys
import os
from typing import Optional, Dict, Any


def safe_get(obj, name, default=None):
    try:
        return obj.get(name, default) if isinstance(obj, dict) else getattr(obj, name, default)
    except Exception:
        return default


def resource_path(relative_path):
    """ 
    Obtiene la ruta absoluta al recurso. Funciona tanto en modo de desarrollo
    como cuando la aplicación está empaquetada con PyInstaller.
    """
    # Priorizar ubicación junto al ejecutable (onedir/onefile con copia de recursos),
    # luego carpeta temporal de PyInstaller (_MEIPASS) y por último el proyecto.
    try:
        if getattr(sys, "frozen", False):
            # Ejecutable PyInstaller
            exe_dir = os.path.dirname(sys.executable)
            candidate = os.path.join(exe_dir, relative_path)
            if os.path.exists(candidate):
                return candidate
            # Carpeta temporal de extracción (onefile)
            meipass = getattr(sys, "_MEIPASS", None)
            if meipass:
                candidate = os.path.join(meipass, relative_path)
                if os.path.exists(candidate):
                    return candidate
            # Fallback: usar exe_dir aunque no exista el recurso (para rutas relativas)
            return os.path.join(exe_dir, relative_path)
    except Exception:
        pass

    # Entorno de desarrollo: relativo al archivo actual
    base_path = os.path.dirname(os.path.abspath(__file__))
    return os.path.join(base_path, relative_path)

# Lectura de gym_data desde DB con fallback a archivo

_GYM_DATA_DEFAULT_PATH = r"c:\\Users\\mateo\\OneDrive\\Escritorio\\gym-management-system\\gym_data.txt"

_cached_gym_data = None

# Inicialización ligera de acceso a DB desde utils
_utils_db = None
_utils_db_init_failed = False

def _get_db_utils():
    global _utils_db, _utils_db_init_failed
    if _utils_db is not None:
        return _utils_db
    if _utils_db_init_failed:
        return None
    try:
        from database import DatabaseManager  # type: ignore
    except Exception:
        _utils_db_init_failed = True
        return None
    try:
        _utils_db = DatabaseManager()
        return _utils_db
    except Exception:
        _utils_db_init_failed = True
        return None


def _resolve_gym_data_path() -> str:
    """Obtiene la ruta del archivo gym_data.txt, priorizando la ruta fija solicitada por el usuario."""
    # 1) Ruta fija provista por el usuario
    if os.path.exists(_GYM_DATA_DEFAULT_PATH):
        return _GYM_DATA_DEFAULT_PATH
    # 2) Mismo directorio del ejecutable/script
    local_path = resource_path('gym_data.txt')
    if os.path.exists(local_path):
        return local_path
    # 3) Recurso empaquetado
    packaged_path = resource_path('gym_data.txt')
    return packaged_path


def read_gym_data(force_reload: bool = False) -> dict:
    """Obtiene datos del gimnasio priorizando la base de datos y cachea en memoria.
    Fallback a gym_data.txt si no están disponibles en DB.
    """
    global _cached_gym_data
    if _cached_gym_data is not None and not force_reload:
        return _cached_gym_data

    merged: Dict[str, Any] = {}

    # 1) Intentar leer desde DB (configuracion)
    db = _get_db_utils()
    if db is not None and hasattr(db, 'obtener_configuracion'):
        try:
            name = db.obtener_configuracion('gym_name')  # type: ignore
            addr = db.obtener_configuracion('gym_address')  # type: ignore
            logo = None
            try:
                logo = db.obtener_configuracion('gym_logo_url')  # type: ignore
            except Exception:
                logo = None
            if isinstance(name, str) and name.strip():
                merged['gym_name'] = name.strip()
            if isinstance(addr, str) and addr.strip():
                merged['gym_address'] = addr.strip()
            if isinstance(logo, str) and logo.strip():
                merged['gym_logo_url'] = logo.strip()
            branding_json = None
            try:
                branding_json = db.obtener_configuracion('branding_config')  # type: ignore
            except Exception:
                branding_json = None
            if branding_json:
                try:
                    import json as _json
                    branding = _json.loads(branding_json)
                    if isinstance(branding, dict):
                        for k in (
                            'gym_slogan','gym_phone','gym_email','gym_website',
                            'facebook','instagram','twitter','primary_color','secondary_color',
                            'accent_color','background_color','alt_background_color'
                        ):
                            v = branding.get(k)
                            if isinstance(v, str):
                                merged[k] = v
                except Exception:
                    pass
        except Exception:
            pass

    # 2) Fallback: merge con archivo gym_data.txt
    file_data: Dict[str, Any] = {}
    path = _resolve_gym_data_path()
    try:
        if os.path.exists(path):
            with open(path, 'r', encoding='utf-8') as f:
                for raw in f:
                    line = raw.strip()
                    if not line or line.startswith('#'):
                        continue
                    if '=' in line:
                        k, v = line.split('=', 1)
                        file_data[k.strip()] = v.strip()
    except Exception:
        file_data = {}

    for k, v in file_data.items():
        if k not in merged or not str(merged.get(k, '')).strip():
            merged[k] = v

    _cached_gym_data = merged
    return merged


def get_gym_value(key: str, default: str = "") -> str:
    """Devuelve el valor de una clave de datos del gimnasio usando caché.
    Usa default si no existe.
    """
    data = read_gym_data()
    return data.get(key, default)


def get_gym_name(default: str = "Gimnasio") -> str:
    """Obtiene el nombre del gimnasio priorizando DB, con fallback a archivo."""
    return (get_gym_value('gym_name', default) or default).strip()


# Utility cleanup functions
from datetime import datetime, timedelta
import tempfile
import os

# --- Webapp Base URL (Railway/Vercel) ---
def get_webapp_base_url(default: str = "") -> str:
    """
    Obtiene la URL base pública de la webapp.

    Prioridad:
    1) ENV `WEBAPP_BASE_URL`
    2) Variables de Vercel (`VERCEL_URL`, `VERCEL_BRANCH_URL`, `VERCEL_PROJECT_PRODUCTION_URL`)
    3) config/config.json → `webapp_base_url` o `public_tunnel.base_url`
    4) Default (si se provee)
    """
    try:
        # 1) ENV explícita
        env_url = os.getenv("WEBAPP_BASE_URL", "").strip()
        if env_url:
            return env_url

        # 2) Detección de dominio Vercel
        vercel = (
            os.getenv("VERCEL_URL")
            or os.getenv("VERCEL_BRANCH_URL")
            or os.getenv("VERCEL_PROJECT_PRODUCTION_URL")
            or ""
        ).strip()
        if vercel:
            if vercel.startswith("http://") or vercel.startswith("https://"):
                return vercel
            return f"https://{vercel}"

        # 3) Buscar en config.json
        try:
            cfg_path = resource_path("config/config.json")
            if os.path.exists(cfg_path):
                import json
                with open(cfg_path, "r", encoding="utf-8") as f:
                    data = json.load(f)
                # Campo dedicado
                c_url = data.get("webapp_base_url")
                if isinstance(c_url, str) and c_url.strip():
                    return c_url.strip()
                # Compatibilidad: permitir en public_tunnel.base_url
                pt = data.get("public_tunnel")
                if isinstance(pt, dict):
                    c2 = pt.get("base_url")
                    if isinstance(c2, str) and c2.strip():
                        return c2.strip()
        except Exception:
            pass

        # 4) Fallback
        return (default or "").strip()
    except Exception:
        return (default or "").strip()


def collect_log_candidates(log_dir: str, retention_days: int):
    """Devuelve lista de candidatos de logs a eliminar como tuplas (path, mtime)."""
    cutoff_time = datetime.now() - timedelta(days=retention_days)
    try:
        files = [f for f in os.listdir(log_dir) if os.path.isfile(os.path.join(log_dir, f))]
    except Exception:
        return []
    files = [f for f in files if f.lower().endswith('.log') or f.startswith('log_')]

    candidates = []
    for filename in files:
        path = os.path.join(log_dir, filename)
        try:
            mtime = datetime.fromtimestamp(os.path.getmtime(path))
            if mtime < cutoff_time:
                candidates.append((path, mtime))
        except Exception:
            continue
    return candidates


def collect_temp_candidates(retention_days: int, temp_dir: str | None = None):
    """Devuelve lista de temporales a eliminar como tuplas (path, mtime)."""
    if temp_dir is None:
        temp_dir = tempfile.gettempdir()
    cutoff_time = datetime.now() - timedelta(days=retention_days)
    safe_exts = ('.tmp', '.temp', '.bak', '.old')
    safe_prefixes = ('gym_', 'gym-', 'tmp_', 'tmp-')

    candidates = []
    for root, _, files in os.walk(temp_dir):
        for f in files:
            fl = f.lower()
            if fl.endswith(safe_exts) or f.startswith(safe_prefixes):
                path = os.path.join(root, f)
                try:
                    mtime = datetime.fromtimestamp(os.path.getmtime(path))
                    if mtime < cutoff_time:
                        candidates.append((path, mtime))
                except Exception:
                    continue
    return candidates


def delete_files(paths: list, progress=None):
    """Elimina una lista de rutas (o tuplas (path, mtime)).
    Si se proporciona 'progress' (QProgressDialog), actualiza la barra de progreso y permite cancelar.
    Devuelve (eliminados, errores).
    """
    deleted = 0
    errors = 0
    total = len(paths)
    for i, item in enumerate(paths):
        path = item if isinstance(item, str) else item[0]
        # Actualizar UI de progreso si está disponible
        if progress is not None:
            try:
                if hasattr(progress, 'wasCanceled') and progress.wasCanceled():
                    break
                if hasattr(progress, 'setValue'):
                    progress.setValue(i)
                if hasattr(progress, 'setLabelText'):
                    progress.setLabelText(f"Eliminando: {os.path.basename(path)}")
                # Intentar procesar eventos si Qt está disponible
                try:
                    from PyQt6.QtWidgets import QApplication
                    QApplication.processEvents()
                except Exception:
                    pass
            except Exception:
                # Ignorar errores de UI, continuar con eliminación
                pass
        try:
            os.remove(path)
            deleted += 1
        except Exception:
            errors += 1
    # Completar progreso si se proporcionó
    try:
        if progress is not None and hasattr(progress, 'setValue'):
            progress.setValue(total)
    except Exception:
        pass
    return deleted, errors

# --- Gestión de procesos de túnel público (deprecado, sin LocalTunnel) ---
def terminate_serveo_tunnel_processes():
    """Alias anterior: redirige a terminate_tunnel_processes()."""
    try:
        return terminate_tunnel_processes()
    except Exception:
        pass

def terminate_tunnel_processes():
    """
    Terminación genérica de procesos residuales de túneles (SSH),
    manteniendo compatibilidad sin referencias a LocalTunnel.
    """
    try:
        import psutil  # type: ignore
    except Exception:
        try:
            if os.name == 'nt':
                os.system('taskkill /F /IM ssh.exe /T')
            else:
                os.system('pkill ssh || true')
        except Exception:
            pass
        return

    try:
        current_pid = os.getpid()
        for proc in psutil.process_iter(attrs=['pid', 'name', 'cmdline']):
            try:
                if proc.info.get('pid') == current_pid:
                    continue
                name = (proc.info.get('name') or '').lower()
                cmdline = ' '.join(proc.info.get('cmdline') or []).lower()
                if ('ssh' in name) or ('ssh' in cmdline):
                    try:
                        proc.terminate()
                        proc.wait(timeout=2)
                    except Exception:
                        try:
                            proc.kill()
                        except Exception:
                            pass
            except Exception:
                continue
    except Exception:
        try:
            if os.name == 'nt':
                os.system('taskkill /F /IM ssh.exe /T')
        except Exception:
            pass

def terminate_public_tunnel_processes():
    """Alias genérico para terminar procesos de túneles (compatibilidad)."""
    try:
        terminate_tunnel_processes()
    except Exception:
        pass
def _parse_bool(value) -> bool | None:
    """Convierte distintos formatos a booleano. Devuelve None si no puede determinarse."""
    try:
        if isinstance(value, bool):
            return value
        if isinstance(value, (int, float)):
            return bool(value)
        if isinstance(value, str):
            v = value.strip().lower()
            if v in ("1", "true", "yes", "on", "y", "t"): return True
            if v in ("0", "false", "no", "off", "n", "f"): return False
    except Exception:
        pass
    return None

def get_public_tunnel_enabled(default: bool = True) -> bool:
    """
    Devuelve si el túnel público debe arrancar automáticamente.
    Prioridad: config/config.json → ENV PUBLIC_TUNNEL_ENABLED → ENV SERVEO_TUNNEL_ENABLED (anterior) → default.
    Claves soportadas en config.json:
      - public_tunnel.enabled (bool o string)
      - public_tunnel_enabled (bool o string, fallback)
    """
    try:
        cfg_path = resource_path("config/config.json")
        if os.path.exists(cfg_path):
            try:
                import json
                with open(cfg_path, "r", encoding="utf-8") as f:
                    data = json.load(f)
                # public_tunnel.enabled
                pt = data.get("public_tunnel")
                if isinstance(pt, dict) and "enabled" in pt:
                    parsed = _parse_bool(pt.get("enabled"))
                    if parsed is not None:
                        return parsed
                # fallback: public_tunnel_enabled plano
                if "public_tunnel_enabled" in data:
                    parsed = _parse_bool(data.get("public_tunnel_enabled"))
                    if parsed is not None:
                        return parsed
            except Exception:
                pass
        # Preferir variable genérica
        env_val = os.getenv("PUBLIC_TUNNEL_ENABLED")
        parsed = _parse_bool(env_val) if env_val is not None else None
        if parsed is not None:
            return parsed
        # Alias anterior soportado
        env_val_legacy = os.getenv("SERVEO_TUNNEL_ENABLED")
        parsed_legacy = _parse_bool(env_val_legacy) if env_val_legacy is not None else None
        if parsed_legacy is not None:
            return parsed_legacy
        return default
    except Exception:
        return default
