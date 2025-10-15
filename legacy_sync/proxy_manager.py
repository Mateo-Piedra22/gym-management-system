import os
import sys
import time
import logging
import subprocess
import json
from typing import Optional
from pathlib import Path

try:
    import requests  # type: ignore
except Exception:  # pragma: no cover
    requests = None  # type: ignore


DEFAULT_PROXY_BASE = "http://127.0.0.1:8080"


def _read_cfg_proxy_base() -> Optional[str]:
    try:
        base_dir = os.path.dirname(os.path.abspath(__file__))
        # utils_modules/ -> project root
        root = os.path.dirname(base_dir)
        cfg_path = os.path.join(root, "config", "config.json")
        if os.path.exists(cfg_path):
            import json
            with open(cfg_path, "r", encoding="utf-8") as f:
                data = json.load(f) or {}
            val = (
                data.get("LOCAL_PROXY_BASE_URL")
                or data.get("local_proxy_base_url")
                or data.get("proxy_base_url")
            )
            if isinstance(val, str) and val.strip():
                return val.strip()
    except Exception:
        return None
    return None


def resolve_local_proxy_base_url() -> str:
    """Resolve la URL base del proxy local desde env/config o valor por defecto."""
    val = os.getenv("LOCAL_PROXY_BASE_URL", "").strip()
    if val:
        return val
    cfg = _read_cfg_proxy_base()
    if cfg:
        return cfg
    return DEFAULT_PROXY_BASE


def _parse_host_port(base_url: str) -> tuple[str, int]:
    from urllib.parse import urlparse

    try:
        u = urlparse(base_url)
        host = u.hostname or "127.0.0.1"
        port = u.port
        if port is None:
            if (u.scheme or "http").lower() == "https":
                port = 443
            else:
                port = 80
        return host, int(port)
    except Exception:
        return "127.0.0.1", 8080


def _is_local_host(host: str) -> bool:
    h = (host or "").lower()
    return h in ("127.0.0.1", "localhost", "::1")


def is_proxy_healthy(base_url: Optional[str] = None, timeout: float = 1.5) -> bool:
    if requests is None:
        return False
    base = (base_url or resolve_local_proxy_base_url()).rstrip("/")
    try:
        r = requests.get(f"{base}/healthz", timeout=timeout)
        return r.status_code == 200
    except Exception:
        return False


def _spawn_proxy_subprocess(base_url: str) -> None:
    """Lanza local_proxy.py como proceso separado, configurando el puerto según la URL."""
    # Calcular puerto a partir de la URL
    _, port = _parse_host_port(base_url)
    # Resolver ruta del script
    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    script = os.path.join(root, "local_proxy.py")
    if not os.path.exists(script):
        raise FileNotFoundError(f"local_proxy.py no encontrado en {script}")

    env = os.environ.copy()
    env.setdefault("PYTHONUNBUFFERED", "1")
    env["LOCAL_PROXY_PORT"] = str(port)

    creationflags = 0
    start_new_session = False
    if os.name == "nt":
        creationflags = getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0) | getattr(
            subprocess, "DETACHED_PROCESS", 0
        )
    else:
        start_new_session = True

    # Redirigir stdout/err a un log para diagnóstico (antes iban a DEVNULL)
    try:
        logs_dir = Path(os.path.join(root, "logs"))
        logs_dir.mkdir(parents=True, exist_ok=True)
        proxy_log_path = logs_dir / "proxy_subprocess.log"
        log_file = open(proxy_log_path, "a", encoding="utf-8")
    except Exception:
        log_file = subprocess.DEVNULL  # Fallback si no se puede abrir archivo

    try:
        subprocess.Popen(
            [sys.executable, script],
            cwd=root,
            env=env,
            stdout=log_file,
            stderr=log_file,
            stdin=subprocess.DEVNULL,
            creationflags=creationflags,
            start_new_session=start_new_session,
            close_fds=True,
        )
    except Exception:
        # Intentar al menos sin flags especiales
        subprocess.Popen([sys.executable, script], cwd=root, env=env, stdout=log_file, stderr=log_file)


def _find_free_port(start_port: int, max_steps: int = 50) -> Optional[int]:
    """Busca un puerto TCP libre en localhost a partir de start_port."""
    try:
        import socket
        for p in range(int(start_port), int(start_port) + int(max_steps)):
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.settimeout(0.2)
                try:
                    if s.connect_ex(("127.0.0.1", p)) != 0:
                        return p
                except Exception:
                    # Si hay error al conectar, asumimos potencialmente libre
                    return p
    except Exception:
        return None
    return None


def _update_local_proxy_base_url(new_base: str) -> None:
    """Actualiza entorno y, si es posible, persiste en config.json la URL base del proxy."""
    try:
        os.environ["LOCAL_PROXY_BASE_URL"] = new_base
    except Exception:
        pass
    # Persistir en config/config.json si existe
    try:
        root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        cfg_path = os.path.join(root, "config", "config.json")
        data = {}
        if os.path.exists(cfg_path):
            with open(cfg_path, "r", encoding="utf-8") as f:
                try:
                    data = json.load(f) or {}
                except Exception:
                    data = {}
        data["local_proxy_base_url"] = new_base
        # Mantener también alias históricos si existen
        if "LOCAL_PROXY_BASE_URL" in data:
            data["LOCAL_PROXY_BASE_URL"] = new_base
        with open(cfg_path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
    except Exception:
        # No bloquear si no se puede escribir
        pass


def _tcp_port_open(host: str, port: int, timeout: float = 1.0) -> bool:
    try:
        import socket
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(timeout)
        ok = (s.connect_ex((host, int(port))) == 0)
        try:
            s.close()
        except Exception:
            pass
        return ok
    except Exception:
        return False


def ensure_local_proxy_running(wait_seconds: float = 12.0, log: Optional[logging.Logger] = None) -> bool:
    """Asegura que el proxy local esté corriendo. Si no responde a /healthz,
    intenta lanzarlo y espera hasta wait_seconds.

    Devuelve True si queda saludable, False en caso contrario.
    """
    logger = log or logging.getLogger(__name__)
    base = resolve_local_proxy_base_url()
    host, _ = _parse_host_port(base)
    if not _is_local_host(host):
        # Si no es host local, no lanzar nada (modo corporativo/remoto)
        logger.info(f"Proxy base no es localhost ({host}); no se auto-lanza.")
        return is_proxy_healthy(base)

    if is_proxy_healthy(base):
        return True

    # Si el puerto está ocupado pero /healthz no responde, informar claramente conflicto
    try:
        host, port = _parse_host_port(base)
    except Exception:
        host, port = ("127.0.0.1", 8080)
    conflict_detected = False
    try:
        if _tcp_port_open(host, port, timeout=0.3) and not is_proxy_healthy(base):
            conflict_detected = True
            logger.warning(
                f"Puerto {port} ocupado en {host} pero /healthz no responde. Intentando elegir otro puerto automáticamente…"
            )
    except Exception:
        conflict_detected = False

    try:
        if conflict_detected:
            # Elegir nuevo puerto libre y actualizar base
            new_port = _find_free_port(port + 1, max_steps=100) or (port + 1)
            new_base = f"http://127.0.0.1:{int(new_port)}"
            _update_local_proxy_base_url(new_base)
            base = new_base
            logger.info(f"Lanzando proxy local en puerto alternativo {int(new_port)}…")
        else:
            logger.info("Proxy local no responde; intentando lanzarlo…")
    except Exception:
        pass
    _spawn_proxy_subprocess(base)

    # Esperar hasta healthy o agotar tiempo
    deadline = time.time() + max(1.0, float(wait_seconds))
    while time.time() < deadline:
        if is_proxy_healthy(base, timeout=1.0):
            try:
                logger.info(f"Proxy local iniciado correctamente en {base}.")
            except Exception:
                pass
            return True
        try:
            time.sleep(0.5)
        except Exception:
            pass
    try:
        logger.warning("No se pudo confirmar arranque del proxy local a tiempo.")
    except Exception:
        pass
    return False