import sys
import os
from typing import Optional, Dict, Any

def safe_get(obj, name, default=None):
    try:
        return obj.get(name, default) if isinstance(obj, dict) else getattr(obj, name, default)
    except Exception:
        return default

def resource_path(relative_path):
    try:
        if getattr(sys, "frozen", False):
            exe_dir = os.path.dirname(sys.executable)
            candidate = os.path.join(exe_dir, relative_path)
            if os.path.exists(candidate):
                return candidate
            meipass = getattr(sys, "_MEIPASS", None)
            if meipass:
                candidate = os.path.join(meipass, relative_path)
                if os.path.exists(candidate):
                    return candidate
            return os.path.join(exe_dir, relative_path)
    except Exception:
        pass
    base_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    return os.path.join(base_path, relative_path)

_cached_gym_data = None
_utils_db = None
_utils_db_init_failed = False

def _get_db_utils():
    global _utils_db, _utils_db_init_failed
    if _utils_db is not None:
        return _utils_db
    if _utils_db_init_failed:
        return None
    try:
        from .database import DatabaseManager  
    except Exception:
        _utils_db_init_failed = True
        return None
    try:
        _utils_db = DatabaseManager()
        return _utils_db
    except Exception:
        _utils_db_init_failed = True
        return None

def read_gym_data(force_reload: bool = False) -> dict:
    global _cached_gym_data
    if _cached_gym_data is not None and not force_reload:
        return _cached_gym_data
    merged: Dict[str, Any] = {}
    db = _get_db_utils()
    if db is not None and hasattr(db, 'obtener_configuracion_gimnasio'):
        try:
            cfg = db.obtener_configuracion_gimnasio()  
        except Exception:
            cfg = {}
        if isinstance(cfg, dict):
            gname = cfg.get('gym_name')
            if isinstance(gname, str) and gname.strip():
                merged['gym_name'] = gname.strip()
            gaddr = cfg.get('gym_address')
            if isinstance(gaddr, str) and gaddr.strip():
                merged['gym_address'] = gaddr.strip()
            glogo = cfg.get('logo_url')
            if isinstance(glogo, str) and glogo.strip():
                merged['gym_logo_url'] = glogo.strip()
    if db is not None and hasattr(db, 'obtener_configuracion'):
        try:
            if not (isinstance(merged.get('gym_name'), str) and merged.get('gym_name').strip()):
                name = db.obtener_configuracion('gym_name')  
                if isinstance(name, str) and name.strip():
                    merged['gym_name'] = name.strip()
            if not (isinstance(merged.get('gym_address'), str) and merged.get('gym_address').strip()):
                addr = db.obtener_configuracion('gym_address')  
                if isinstance(addr, str) and addr.strip():
                    merged['gym_address'] = addr.strip()
            if not (isinstance(merged.get('gym_logo_url'), str) and merged.get('gym_logo_url').strip()):
                logo = db.obtener_configuracion('gym_logo_url')  
                if isinstance(logo, str) and logo.strip():
                    merged['gym_logo_url'] = logo.strip()
        except Exception:
            pass
        try:
            branding_json = db.obtener_configuracion('branding_config')  
        except Exception:
            branding_json = None
        if branding_json:
            try:
                import json as _json
                branding = _json.loads(branding_json)
                if isinstance(branding, dict):
                    try:
                        bn = branding.get('gym_name')
                        if isinstance(bn, str) and bn.strip() and not (isinstance(merged.get('gym_name'), str) and merged.get('gym_name').strip()):
                            merged['gym_name'] = bn.strip()
                    except Exception:
                        pass
                    for k in (
                        'gym_slogan','gym_phone','gym_email','gym_website',
                        'facebook','instagram','twitter','primary_color','secondary_color',
                        'accent_color','background_color','alt_background_color'
                    ):
                        v = branding.get(k)
                        if isinstance(v, str) and v:
                            merged[k] = v
            except Exception:
                pass
    defaults: Dict[str, Any] = {
        'gym_name': 'Gimnasio',
        'gym_slogan': 'Tu mejor versión te espera',
        'gym_address': 'Dirección no disponible',
        'gym_phone': 'Teléfono no disponible',
        'gym_email': 'Email no disponible',
        'gym_website': 'Website no disponible',
        'facebook': '@gym',
        'instagram': '@gym',
        'twitter': '@gym'
    }
    for k, v in defaults.items():
        if k not in merged or not str(merged.get(k, '')).strip():
            merged[k] = v
    _cached_gym_data = merged
    return merged

def get_gym_value(key: str, default: str = "") -> str:
    data = read_gym_data()
    return data.get(key, default)

def get_gym_name(default: str = "Gimnasio") -> str:
    return (get_gym_value('gym_name', default) or default).strip()

from datetime import datetime, timedelta
import tempfile
import os as _os

def get_webapp_base_url(default: str = "") -> str:
    try:
        env_url = _os.getenv("WEBAPP_BASE_URL", "").strip()
        if env_url:
            return env_url
        vercel = (
            _os.getenv("VERCEL_URL")
            or _os.getenv("VERCEL_BRANCH_URL")
            or _os.getenv("VERCEL_PROJECT_PRODUCTION_URL")
            or ""
        ).strip()
        if vercel:
            if vercel.startswith("http://") or vercel.startswith("https://"):
                return vercel
            return f"https://{vercel}"
        try:
            cfg_path = resource_path("config/config.json")
            if _os.path.exists(cfg_path):
                import json
                with open(cfg_path, "r", encoding="utf-8") as f:
                    data = json.load(f)
                c_url = data.get("webapp_base_url")
                if isinstance(c_url, str) and c_url.strip():
                    return c_url.strip()
                pt = data.get("public_tunnel")
                if isinstance(pt, dict):
                    c2 = pt.get("base_url")
                    if isinstance(c2, str) and c2.strip():
                        return c2.strip()
        except Exception:
            pass
        return (default or "").strip()
    except Exception:
        return (default or "").strip()

def collect_log_candidates(log_dir: str, retention_days: int):
    cutoff_time = datetime.now() - timedelta(days=retention_days)
    try:
        files = [f for f in _os.listdir(log_dir) if _os.path.isfile(_os.path.join(log_dir, f))]
    except Exception:
        return []
    files = [f for f in files if f.lower().endswith('.log') or f.startswith('log_')]
    candidates = []
    for filename in files:
        path = _os.path.join(log_dir, filename)
        try:
            mtime = datetime.fromtimestamp(_os.path.getmtime(path))
            if mtime < cutoff_time:
                candidates.append((path, mtime))
        except Exception:
            continue
    return candidates

def collect_temp_candidates(retention_days: int, temp_dir: str | None = None):
    if temp_dir is None:
        temp_dir = tempfile.gettempdir()
    cutoff_time = datetime.now() - timedelta(days=retention_days)
    safe_exts = ('.tmp', '.temp', '.bak', '.old')
    safe_prefixes = ('gym_', 'gym-', 'tmp_', 'tmp-')
    candidates = []
    for root, _, files in _os.walk(temp_dir):
        for f in files:
            fl = f.lower()
            if fl.endswith(safe_exts) or f.startswith(safe_prefixes):
                path = _os.path.join(root, f)
                try:
                    mtime = datetime.fromtimestamp(_os.path.getmtime(path))
                    if mtime < cutoff_time:
                        candidates.append((path, mtime))
                except Exception:
                    continue
    return candidates

def delete_files(paths: list, progress=None):
    deleted = 0
    errors = 0
    total = len(paths)
    for i, item in enumerate(paths):
        path = item if isinstance(item, str) else item[0]
        if progress is not None:
            try:
                if hasattr(progress, 'wasCanceled') and progress.wasCanceled():
                    break
                if hasattr(progress, 'setValue'):
                    progress.setValue(i)
                if hasattr(progress, 'setLabelText'):
                    progress.setLabelText(f"Eliminando: {os.path.basename(path)}")
            except Exception:
                pass
        try:
            _os.remove(path)
            deleted += 1
        except Exception:
            errors += 1
    try:
        if progress is not None and hasattr(progress, 'setValue'):
            progress.setValue(total)
    except Exception:
        pass
    return deleted, errors

def terminate_serveo_tunnel_processes():
    try:
        return terminate_tunnel_processes()
    except Exception:
        pass

def terminate_tunnel_processes():
    try:
        import psutil  
    except Exception:
        try:
            if _os.name == 'nt':
                _os.system('taskkill /F /IM ssh.exe /T')
            else:
                _os.system('pkill ssh || true')
        except Exception:
            pass
        return
    try:
        current_pid = _os.getpid()
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
            if _os.name == 'nt':
                _os.system('taskkill /F /IM ssh.exe /T')
        except Exception:
            pass

def terminate_public_tunnel_processes():
    try:
        terminate_tunnel_processes()
    except Exception:
        pass

def _parse_bool(value) -> bool | None:
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
    try:
        cfg_path = resource_path("config/config.json")
        if _os.path.exists(cfg_path):
            try:
                import json
                with open(cfg_path, "r", encoding="utf-8") as f:
                    data = json.load(f)
                pt = data.get("public_tunnel")
                if isinstance(pt, dict) and "enabled" in pt:
                    parsed = _parse_bool(pt.get("enabled"))
                    if parsed is not None:
                        return parsed
                if "public_tunnel_enabled" in data:
                    parsed = _parse_bool(data.get("public_tunnel_enabled"))
                    if parsed is not None:
                        return parsed
            except Exception:
                pass
        env_val = _os.getenv("PUBLIC_TUNNEL_ENABLED")
        parsed = _parse_bool(env_val) if env_val is not None else None
        if parsed is not None:
            return parsed
        env_val_legacy = _os.getenv("SERVEO_TUNNEL_ENABLED")
        parsed_legacy = _parse_bool(env_val_legacy) if env_val_legacy is not None else None
        if parsed_legacy is not None:
            return parsed_legacy
        return default
    except Exception:
        return default