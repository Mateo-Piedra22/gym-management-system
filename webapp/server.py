import os
import sys
import csv
import json
import secrets
from pathlib import Path
from typing import Optional, Dict, Any, Callable
from datetime import datetime

from fastapi import FastAPI, Request, HTTPException, Depends
import logging
from fastapi.responses import JSONResponse, RedirectResponse, Response
from fastapi.templating import Jinja2Templates
from starlette.middleware.sessions import SessionMiddleware
from starlette.middleware.trustedhost import TrustedHostMiddleware
from starlette.middleware.httpsredirect import HTTPSRedirectMiddleware
from starlette.staticfiles import StaticFiles
import threading
import psutil
import psycopg2
import psycopg2.extras
# HTTP client para probes (con fallback si no está disponible)
try:
    import requests  # type: ignore
except Exception:
    requests = None  # type: ignore

# Importar gestor de base de datos SIN modificar el programa principal
try:
    from database import DatabaseManager  # type: ignore
except Exception:
    DatabaseManager = None  # type: ignore

# Importar utilidades del proyecto principal para branding y contraseña de desarrollador
try:
    from utils import get_gym_name  # type: ignore
except Exception:
    def get_gym_name(default: str = "Gimnasio") -> str:  # type: ignore
        try:
            path = Path("gym_data.txt")
            if path.exists():
                for line in path.read_text(encoding="utf-8").splitlines():
                    if line.startswith("gym_name="):
                        return line.split("=", 1)[1].strip()
        except Exception:
            pass
        return default

try:
    from managers import DEV_PASSWORD  # type: ignore
except Exception:
    DEV_PASSWORD = None  # type: ignore

# Base URL pública (Railway) sin túneles
try:
    from utils import get_webapp_base_url  # type: ignore
except Exception:
    def get_webapp_base_url(default: str = "https://gym-ms-zrk.up.railway.app") -> str:  # type: ignore
        import os as _os
        return _os.getenv("WEBAPP_BASE_URL", default).strip()

# Utilidad para cerrar túneles públicos de forma segura
try:
    from utils import terminate_tunnel_processes  # type: ignore
except Exception:
    def terminate_tunnel_processes() -> None:  # type: ignore
        try:
            for p in psutil.process_iter(attrs=["pid","name","cmdline"]):
                try:
                    cmd = " ".join(p.info.get("cmdline") or [])
                    if ("ssh" in (p.info.get("name") or "")) or ("node" in (p.info.get("name") or "")):
                        p.terminate()
                except Exception:
                    pass
        except Exception:
            pass

from .qss_to_css import generate_css_from_qss, read_theme_vars

_db: Optional[DatabaseManager] = None

# Ajuste de stdout/stderr para ejecutables sin consola en Windows (runw.exe)
# Evita fallos de configuración de logging en Uvicorn cuando sys.stdout/sys.stderr es None.
try:
    if os.name == "nt":
        import os as _os
        import io as _io
        # Asegurar stdout/stderr válidos aunque estén cerrados en ejecutables sin consola
        _sout = getattr(sys, "stdout", None)
        try:
            if _sout is None or getattr(_sout, "closed", False):
                sys.stdout = open(_os.devnull, "w", encoding="utf-8")
            elif hasattr(_sout, "buffer") and not isinstance(_sout, _io.TextIOWrapper):
                sys.stdout = _io.TextIOWrapper(_sout.buffer, encoding="utf-8", errors="replace")
        except Exception:
            pass
        _serr = getattr(sys, "stderr", None)
        try:
            if _serr is None or getattr(_serr, "closed", False):
                sys.stderr = open(_os.devnull, "w", encoding="utf-8")
            elif hasattr(_serr, "buffer") and not isinstance(_serr, _io.TextIOWrapper):
                sys.stderr = _io.TextIOWrapper(_serr.buffer, encoding="utf-8", errors="replace")
        except Exception:
            pass
        # Forzar política de event loop compatible en Windows a nivel global
        try:
            import asyncio as _asyncio
            _asyncio.set_event_loop_policy(_asyncio.WindowsSelectorEventLoopPolicy())
        except Exception:
            pass
except Exception:
    pass

# Fallback defensivo: rebind local de print a versión segura que no falle
# si stdout/stderr está cerrado en ejecutables sin consola.
try:
    import builtins as _builtins
    def _safe_print(*args, **kwargs):
        try:
            _builtins.print(*args, **kwargs)
        except Exception:
            try:
                import logging as _logging
                # Registrar el mensaje concatenado para no perder diagnóstico
                _logging.info(" ".join(str(a) for a in args))
            except Exception:
                # No romper si logging aún no está configurado
                pass
    print = _safe_print  # type: ignore
except Exception:
    pass

def _compute_base_dir() -> Path:
    """Determina la carpeta base desde la cual resolver recursos.
    - En ejecutable PyInstaller (onedir): junto al exe.
    - En modo onefile: carpeta temporal _MEIPASS.
    - En desarrollo: raíz del proyecto.
    """
    try:
        if getattr(sys, "frozen", False):
            exe_dir = Path(sys.executable).resolve().parent
            meipass = getattr(sys, "_MEIPASS", None)
            if meipass:
                return Path(meipass)
            return exe_dir
    except Exception:
        pass
    # Desarrollo: carpeta del proyecto
    try:
        # server.py está en webapp/, subimos un nivel
        return Path(__file__).resolve().parent.parent
    except Exception:
        return Path('.')

def _resolve_existing_dir(*parts: str) -> Path:
    """Devuelve el primer directorio existente entre varias ubicaciones candidatas.
    Prioriza BASE_DIR, luego el directorio del ejecutable (onedir) y por último el proyecto.
    """
    candidates = []
    try:
        candidates.append(BASE_DIR.joinpath(*parts))
    except Exception:
        pass
    try:
        exe_dir = Path(sys.executable).resolve().parent
        candidates.append(exe_dir.joinpath(*parts))
    except Exception:
        pass
    try:
        proj_root = Path(__file__).resolve().parent.parent
        candidates.append(proj_root.joinpath(*parts))
    except Exception:
        pass
    for c in candidates:
        try:
            if c.exists():
                return c
        except Exception:
            continue
    # Fallback: primera opción aunque no exista
    return candidates[0] if candidates else Path(*parts)

# Inicialización de la app web
app = FastAPI(
    title="GymMS WebApp",
    version="2.0",
    # Permite servir detrás de reverse proxy con subpath
    root_path=os.getenv("ROOT_PATH", "").strip(),
)
app.add_middleware(SessionMiddleware, secret_key=os.getenv("WEBAPP_SECRET_KEY", secrets.token_urlsafe(32)))

# Middlewares de producción (opcionales via ENV, cambios mínimos)
try:
    # Restringir hosts confiables. Si no hay ENV, añadir dominio Railway por defecto
    th = os.getenv("TRUSTED_HOSTS", "").strip()
    hosts = [h.strip() for h in th.split(",") if h.strip()] if th else []
    if not hosts:
        hosts = ["gym-ms-zrk.up.railway.app", "localhost", "127.0.0.1"]
    app.add_middleware(TrustedHostMiddleware, allowed_hosts=hosts)
    # Forzar HTTPS en producción si se indica
    if (os.getenv("FORCE_HTTPS", "0").strip() in ("1", "true", "yes")):
        app.add_middleware(HTTPSRedirectMiddleware)
    # Nota: Gestión de cabeceras de proxy delegada a Uvicorn (proxy_headers=True)
    if (os.getenv("PROXY_HEADERS_ENABLED", "1").strip() in ("1", "true", "yes")):
        logging.info("Cabeceras de proxy gestionadas por Uvicorn (proxy_headers=True)")
except Exception:
    pass

# Static y assets basados en BASE_DIR
BASE_DIR = _compute_base_dir()
static_dir = _resolve_existing_dir("webapp", "static")
static_dir.mkdir(parents=True, exist_ok=True)
templates_dir = _resolve_existing_dir("webapp", "templates")
templates = Jinja2Templates(directory=str(templates_dir))

try:
    app.mount("/static", StaticFiles(directory=str(static_dir)), name="static")
except Exception:
    pass
# Preferir assets del proyecto raíz; fallback a assets dentro de webapp
try:
    app.mount("/assets", StaticFiles(directory=str(_resolve_existing_dir("assets"))), name="assets")
except Exception:
    try:
        app.mount("/assets", StaticFiles(directory=str(_resolve_existing_dir("webapp", "assets"))), name="assets")
    except Exception:
        pass

# Endpoint de salud ligero para probes y monitores
@app.get("/healthz")
async def healthz():
    """Devuelve estado 200 si la app responde. Incluye mínimos detalles."""
    try:
        details: Dict[str, Any] = {
            "status": "ok",
            "time": datetime.utcnow().isoformat() + "Z",
        }
        # Comprobación opcional de DB (no bloqueante)
        try:
            db = _get_db()
            if db is not None:
                details["db"] = "ok"
        except Exception:
            details["db"] = "error"
        return JSONResponse(details)
    except Exception:
        # Fallback defensivo: responder 200 para evitar cascadas de reinicios
        return JSONResponse({"status": "ok"})

# Endpoint para exponer la URL base pública (Railway)
@app.get("/webapp/base_url")
async def webapp_base_url():
    try:
        url = get_webapp_base_url()
        return JSONResponse({"base_url": url})
    except Exception:
        return JSONResponse({"base_url": "https://gym-ms-zrk.up.railway.app"})

# Evitar 404 de clientes de Vite durante desarrollo: devolver stub vacío
@app.get("/@vite/client")
async def vite_client_stub():
    return Response("// Vite client stub (deshabilitado en esta app)", media_type="application/javascript")

# Generar CSS desde QSS de forma automática evitando sobrescritura
try:
    css_path = static_dir / "style.css"
    # Solo regenerar si no existe o si se fuerza explícitamente por ENV
    if (not css_path.exists()) or (os.getenv("REGENERATE_STYLE_CSS", "0").strip() == "1"):
        qss_path = _resolve_existing_dir("styles", "style.qss")
        generate_css_from_qss(qss_path, css_path)
except Exception:
    # Fallback silencioso; la UI seguirá mostrando colores por defecto
    pass


def _get_password() -> str:
    # Contraseña exclusiva del dueño (configurable por ENV). Fallback seguro.
    return os.getenv("WEBAPP_OWNER_PASSWORD", "admin").strip()


def _resolve_logo_url() -> str:
    # Resuelve el logo desde assets del proyecto principal
    candidates = [
        _resolve_existing_dir("assets") / "gym_logo.png",
        _resolve_existing_dir("assets") / "logo.svg",
        _resolve_existing_dir("webapp", "assets") / "logo.svg",
    ]
    for p in candidates:
        try:
            if p.exists():
                return "/assets/" + p.name
        except Exception:
            continue
    return "/assets/logo.svg"


def _get_db() -> Optional[DatabaseManager]:
    global _db
    if _db is not None:
        return _db
    if DatabaseManager is None:
        return None
    try:
        _db = DatabaseManager()
        # Opcional: crear índices de rendimiento en pagos si el método existe
        try:
            if hasattr(_db, 'ensure_indexes'):
                _db.ensure_indexes()  # type: ignore
        except Exception:
            pass
    except Exception:
        _db = None
    return _db


def require_owner(request: Request):
    if not request.session.get("logged_in"):
        raise HTTPException(status_code=401, detail="Acceso restringido al dueño")
    return True


def start_web_server(db_manager: Optional[DatabaseManager] = None, host: str = "127.0.0.1", port: int = 8003, log_level: str = "info") -> None:
    """Arranca el servidor web en un hilo daemon para no bloquear la UI.

    Si se pasa una instancia de DatabaseManager, se inyecta para que la web la reutilice.
    """
    try:
        global _db
        if db_manager is not None:
            _db = db_manager

        def _run():
            import time as _t
            while True:
                try:
                    # Liberar/limpiar el puerto si está ocupado por otro proceso
                    try:
                        for conn in psutil.net_connections(kind='inet'):
                            laddr = getattr(conn, 'laddr', None)
                            if not laddr:
                                continue
                            if int(getattr(laddr, 'port', 0)) == int(port):
                                pid = conn.pid
                                if pid and pid != os.getpid():
                                    try:
                                        p = psutil.Process(pid)
                                        p.terminate()
                                        p.wait(timeout=2)
                                    except Exception:
                                        try:
                                            p.kill()
                                        except Exception:
                                            pass
                        # pequeña espera para liberar el puerto completamente
                        _t.sleep(0.5)
                    except Exception:
                        pass
                    import uvicorn
                    # Respetar PORT/HOST de entorno en plataformas de hosting
                    env_port = os.getenv("PORT")
                    try:
                        port = int(env_port) if env_port else port
                    except Exception:
                        pass
                    host = os.getenv("HOST", host)
                    config = uvicorn.Config(
                        app,
                        host=host,
                        port=port,
                        log_level=log_level,
                        log_config=None,  # Evita errores de logging sin TTY en ejecutables sin consola
                        loop="asyncio",
                        http="h11",
                        lifespan="off",
                        proxy_headers=(os.getenv("PROXY_HEADERS_ENABLED", "1").strip() in ("1", "true", "yes")),
                    )
                    server = uvicorn.Server(config=config)
                    # Desactivar instalación de manejadores de señales (no permitidos fuera del hilo principal)
                    try:
                        server.install_signal_handlers = lambda: None
                    except Exception:
                        pass
                    # Crear explícitamente un event loop en este hilo y servir
                    try:
                        import asyncio as _asyncio
                        loop = _asyncio.new_event_loop()
                        _asyncio.set_event_loop(loop)
                        loop.run_until_complete(server.serve())
                    except Exception:
                        # Fallback al modo síncrono
                        server.run()
                except Exception:
                    pass
                # Si el servidor se detiene o falla, esperar y reintentar mientras el programa siga abierto
                _t.sleep(1)

        # En ejecutables congelados en Windows, preferir proceso separado para robustez
        try:
            # Importante: en ejecutables congelados en Windows, evitar multiprocessing.
            # Usar siempre hilo dedicado para prevenir re-ejecución del binario (bucle de relanzamiento).
            t = threading.Thread(target=_run, daemon=True)
            t.start()
        except Exception:
            # Fallback defensivo: no bloquear la UI si el hilo falla
            try:
                t = threading.Thread(target=_run, daemon=True)
                t.start()
            except Exception:
                pass
    except Exception:
        pass


# Callback global de reconexión del túnel público (LocalTunnel por defecto)
_public_tunnel_on_reconnect_cb: Optional[Callable[[str], None]] = None

def set_public_tunnel_reconnect_callback(cb: Optional[Callable[[str], None]]):
    """Registra callback de reconexión del túnel público."""
    global _public_tunnel_on_reconnect_cb
    _public_tunnel_on_reconnect_cb = cb

def set_serveo_reconnect_callback(cb: Optional[Callable[[str], None]]):
    """Alias legado: redirige a set_public_tunnel_reconnect_callback."""
    set_public_tunnel_reconnect_callback(cb)

def start_public_tunnel(subdomain: str = "gym-ms-zrk", local_port: int = 8000, on_reconnect: Optional[Callable[[str], None]] = None) -> Optional[str]:
    """No-op de túnel público: devuelve la URL Railway configurada."""
    try:
        url = get_webapp_base_url()
        return url
    except Exception:
        pass
    try:
        import shutil, subprocess, os, threading, re, webbrowser, time, socket

        # Selección de proveedor centrado en LocalTunnel (default 'localtunnel').
        provider = str(os.getenv("TUNNEL_PROVIDER", "localtunnel")).strip().lower()

        # Localizar binario de SSH
        def _find_ssh() -> Optional[str]:
            b = shutil.which("ssh")
            if not b and os.name == "nt":
                cand = r"C:\\Windows\\System32\\OpenSSH\\ssh.exe"
                if os.path.exists(cand):
                    b = cand
            return b

        # Chequear conectividad TCP simple
        def _tcp_open(host: str, port: int, timeout: float = 2.0) -> bool:
            try:
                s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                s.settimeout(timeout)
                res = (s.connect_ex((host, int(port))) == 0)
                try:
                    s.close()
                except Exception:
                    pass
                return res
            except Exception:
                return False

        ssh_bin = _find_ssh()
        if not ssh_bin and provider != "localtunnel":
            print("Cliente SSH no encontrado; no se puede iniciar túnel SSH")
            return None

        # Esperar a que el servicio local esté escuchando
        try:
            deadline = time.time() + 15
            while time.time() < deadline:
                s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                s.settimeout(1.5)
                try:
                    if s.connect_ex(("127.0.0.1", int(local_port))) == 0:
                        break
                finally:
                    try:
                        s.close()
                    except Exception:
                        pass
                time.sleep(0.5)
        except Exception:
            # Si falla la comprobación, continuamos igualmente
            pass

        # Host y puerto SSH configurables para proveedores SSH genéricos
        ssh_host = os.getenv("PUBLIC_TUNNEL_SSH_HOST", "localhost.run").strip()
        ssh_port_env = os.getenv("PUBLIC_TUNNEL_SSH_PORT")
        ssh_port: int
        try:
            ssh_port = int(ssh_port_env) if ssh_port_env else 22
        except Exception:
            ssh_port = 22

        # Si no especificado por ENV, probar conectividad y usar 443 como fallback
        if ssh_port_env is None:
            if not _tcp_open(ssh_host, 22, 1.5):
                # Intento rápido de 443
                if _tcp_open(ssh_host, 443, 1.5):
                    ssh_port = 443
                else:
                    # Intento de flush DNS en Windows y re-test
                    try:
                        if os.name == "nt":
                            subprocess.run(["ipconfig", "/flushdns"], timeout=5)
                    except Exception:
                        pass
                    if _tcp_open(ssh_host, 22, 1.5):
                        ssh_port = 22
                    elif _tcp_open(ssh_host, 443, 1.5):
                        ssh_port = 443
                    else:
                        # No hay conectividad saliente; continuar igualmente para que el supervisor reintente
                        ssh_port = 22

        # Intento de permitir tráfico saliente de ssh.exe en Windows (no crítico, puede requerir admin)
        _startupinfo = None
        try:
            if os.name == "nt":
                _startupinfo = subprocess.STARTUPINFO()
                _startupinfo.dwFlags |= getattr(subprocess, "STARTF_USESHOWWINDOW", 0)
                try:
                    # SW_HIDE para asegurar que la ventana esté oculta
                    _startupinfo.wShowWindow = 0
                except Exception:
                    pass
        except Exception:
            _startupinfo = None
        try:
            if os.name == "nt" and ssh_bin:
                subprocess.run([
                    "netsh", "advfirewall", "firewall", "add", "rule",
                    "name=GymMS SSH Tunnel",
                    "dir=out",
                    f"program={ssh_bin}",
                    "action=allow",
                    "enable=yes"
                ], timeout=4)
        except Exception:
            pass

        # Selección de proveedor centrado en LocalTunnel (default 'localtunnel'). Sin fallback automático.
        # Bandera de verbosidad SSH configurable
        ssh_verbose = str(os.getenv("PUBLIC_TUNNEL_SSH_VERBOSE", "0")).strip().lower() in ("1", "true", "yes")

        # Helper para construir el comando SSH según proveedor y puerto actual
        def _build_cmd() -> list:
            try:
                if provider == "localtunnel":
                    # LocalTunnel: requiere Node. Intentar usar binario 'lt' o 'npx localtunnel'.
                    lt_bin = shutil.which("lt")
                    npx_bin = shutil.which("npx")
                    if lt_bin:
                        base = [lt_bin, "--port", str(local_port), "--subdomain", subdomain]
                    elif npx_bin:
                        base = [npx_bin, "localtunnel", "--port", str(local_port), "--subdomain", subdomain]
                    else:
                        logging.warning("[Tunnel] LocalTunnel no disponible (no se encontró 'lt' ni 'npx').")
                        return []
                    try:
                        logging.info(f"[Tunnel] provider=localtunnel subdomain={subdomain} port={local_port}")
                    except Exception:
                        pass
                    return base
                if provider == "localhost.run":
                    # localhost.run: túnel HTTP gratuito (dominio aleatorio) con usuario opcional
                    lhr_user = str(os.getenv("LHR_SSH_USER", "nokey")).strip()
                    remote_spec = f"80:localhost:{local_port}"
                    host_spec = f"{lhr_user}@localhost.run" if lhr_user else "localhost.run"
                    base = [
                        ssh_bin,
                        "-o", "StrictHostKeyChecking=no",
                        "-o", "ExitOnForwardFailure=yes",
                        "-o", "ServerAliveInterval=60",
                        "-o", "ServerAliveCountMax=3",
                        "-o", "ConnectTimeout=10",
                        "-o", "ConnectionAttempts=3",
                        "-N",
                        "-T",
                        "-R", remote_spec,
                        host_spec,
                    ]
                    if ssh_verbose:
                        base.insert(1, "-vvv")
                    try:
                        logging.info(f"[Tunnel] provider=localhost.run remote={remote_spec} host={host_spec} port=22")
                    except Exception:
                        pass
                    return base
                else:
                    # SSH genérico: sin soporte de subdominio explícito
                    try:
                        logging.info(f"[Tunnel] provider=ssh remote=80:localhost:{local_port} host={ssh_host} port={ssh_port}")
                    except Exception:
                        pass
                    return []
            except Exception:
                # Fallback seguro en caso de error construyendo comando
                return []

        # Comando inicial según proveedor seleccionado
        cmd = _build_cmd()

        # Log del comando inicial
        try:
            logging.debug(f"[PublicTunnel] comando inicial={' '.join(cmd)}")
        except Exception:
            pass

        attempt = 0

        # Última URL pública detectada desde la salida del proveedor
        last_public_url: Optional[str] = None
        # Contador de fallas para proveedores SSH
        ssh_failures = 0

        def _run():
            nonlocal ssh_port, attempt, provider, last_public_url, ssh_failures
            # Supervisor con backoff exponencial para reconectar si el proceso termina
            backoff = 2.0
            max_backoff = 300.0  # 5 minutos máximo
            # Estado para notificaciones de reconexión con anti-spam
            first_start = True
            last_notify_ts = 0.0
            min_notify_interval = 90.0  # segundos
            while True:
                try:
                    attempt += 1
                    had_banner_refused = False
                    # Reconstruir comando por intento para reflejar cambios de puerto o proveedor
                    cmd = _build_cmd()
                    try:
                        logging.info(f"[PublicTunnel] intento={attempt} lanzando proceso provider={provider} puerto={ssh_port}")
                        logging.debug(f"[PublicTunnel] comando={' '.join(cmd)}")
                    except Exception:
                        pass
                    p = subprocess.Popen(
                        cmd,
                        stdout=subprocess.PIPE,
                        stderr=subprocess.STDOUT,
                        text=True,
                        # En Windows, solo CREATE_NO_WINDOW para evitar consola; quitar flags que pueden provocar ventanas.
                        creationflags=(
                            getattr(subprocess, "CREATE_NO_WINDOW", 0)
                        ),
                        startupinfo=_startupinfo,
                        stdin=subprocess.DEVNULL,
                    )
                    try:
                        logging.info(f"[PublicTunnel] proceso lanzado pid={getattr(p, 'pid', None)}")
                    except Exception:
                        pass
                    # Si el primer arranque falla de inmediato y el puerto era 22, probar 443 en siguiente intento
                    try:
                        start_ts = time.time()
                    except Exception:
                        start_ts = 0.0
                    # Notificar reconexión (si no es el primer arranque) con anti-spam
                    cb = on_reconnect or _public_tunnel_on_reconnect_cb
                    if cb and not first_start:
                        now = time.time()
                        if (now - last_notify_ts) >= min_notify_interval:
                            last_notify_ts = now
                            try:
                                cb(f"https://{subdomain}.loca.lt/")
                                logging.info("[PublicTunnel] notificación de reconexión enviada")
                            except Exception:
                                pass
                    # Parsear salida del proceso para capturar URL pública (LocalTunnel)
                    if p.stdout:
                        refused_pattern = re.compile(r"banner exchange:.*Connection refused", re.IGNORECASE)
                        url_pattern = re.compile(r"https://[^\s]+", re.IGNORECASE)
                        try:
                            for line in p.stdout:
                                try:
                                    ln = (line or "").strip()
                                    if ln:
                                        logging.info(f"[PublicTunnel][stdout] {ln}")
                                        if refused_pattern.search(ln):
                                            had_banner_refused = True
                                        # Intentar capturar la URL pública impresa por el proveedor
                                        murl = url_pattern.search(ln)
                                        if murl:
                                            last_public_url = murl.group(0)
                                except Exception:
                                    pass
                        except Exception:
                            # Continuar aunque el parsing de salida falle
                            try:
                                logging.warning("[PublicTunnel] fallo leyendo stdout del proceso (continuando)")
                            except Exception:
                                pass
                            pass
                    # Esperar a que el proceso termine (por desconexión o error)
                    try:
                        p.wait()
                        try:
                            logging.info(f"[PublicTunnel] proceso terminado rc={getattr(p, 'returncode', None)}")
                        except Exception:
                            pass
                    except Exception:
                        pass
                except Exception:
                    # Fallo al lanzar SSH, continuar con backoff
                    try:
                        logging.error("[PublicTunnel] excepción al lanzar proceso", exc_info=True)
                    except Exception:
                        pass
                    pass
                # Marcar que siguientes lanzamientos serán reconexiones
                try:
                    first_start = False
                except Exception:
                    pass
                # Si el proceso terminó muy rápido y estábamos en 22, probar 443 como siguiente intento (solo serveo)
                # Sin alternancia de puertos ni fallback: centrado en LocalTunnel
                try:
                    pass
                except Exception:
                    pass
                # Dormir y aumentar backoff antes de reintentar
                try:
                    time.sleep(backoff)
                except Exception:
                    pass
                backoff = min(backoff * 2.0, max_backoff)

        threading.Thread(target=_run, daemon=True).start()
        # Retornar última URL pública detectada si existe; de lo contrario, URL por defecto según proveedor
        try:
            if last_public_url:
                return last_public_url
            if provider == "localtunnel":
                return f"https://{subdomain}.loca.lt/"
            else:
                return None
        except Exception:
            try:
                if provider == "localtunnel":
                    return f"https://{subdomain}.loca.lt/"
                else:
                    return None
            except Exception:
                return None
    except Exception:
        return None


@app.get("/")
async def root_selector(request: Request):
    """Página de selección: Dashboard (lleva a login) o Check-in."""
    theme_vars = read_theme_vars(static_dir / "style.css")
    ctx = {
        "request": request,
        "theme": theme_vars,
        "gym_name": get_gym_name("Gimnasio"),
        "logo_url": _resolve_logo_url(),
    }
    return templates.TemplateResponse("index.html", ctx)

@app.get("/tunnel/password")
async def tunnel_password():
    """Endpoint legado deshabilitado.

    En configuración Railway no hay contraseña de túnel.
    """
    return JSONResponse({"password": None, "ok": False})

@app.get("/login")
async def login_page_get(request: Request):
    """Muestra el formulario de login del dueño."""
    theme_vars = read_theme_vars(static_dir / "style.css")
    ctx = {
        "request": request,
        "theme": theme_vars,
        "error": request.query_params.get("error"),
        "gym_name": get_gym_name("Gimnasio"),
        "logo_url": _resolve_logo_url(),
    }
    return templates.TemplateResponse("login.html", ctx)


@app.post("/login")
async def do_login(request: Request):
    content_type = request.headers.get("content-type", "")
    if content_type.startswith("application/x-www-form-urlencoded"):
        data = await request.form()
    elif content_type.startswith("application/json"):
        data = await request.json()
    else:
        data = {}
    password = str(data.get("password", "")).strip()
    if not password:
        return RedirectResponse(url="/?error=Ingrese%20la%20contrase%C3%B1a", status_code=303)
    ok = False
    # 1) ENV
    if password == _get_password():
        ok = True
    # 2) Contraseña de desarrollador
    if not ok and DEV_PASSWORD and password == DEV_PASSWORD:
        ok = True
    # 3) Configuración en BD: owner_password
    if not ok:
        db = _get_db()
        try:
            if db:
                with db.get_connection_context() as conn:  # type: ignore
                    cur = conn.cursor()
                    cur.execute("SELECT valor FROM configuracion WHERE clave='owner_password' ORDER BY id DESC LIMIT 1")
                    row = cur.fetchone()
                    if row and str(row[0]).strip() == password:
                        ok = True
        except Exception:
            pass
    # 4) PIN del dueño en usuarios
    if not ok:
        db = _get_db()
        try:
            if db:
                with db.get_connection_context() as conn:  # type: ignore
                    cur = conn.cursor()
                    cur.execute("SELECT pin FROM usuarios WHERE rol='owner' LIMIT 5")
                    pins = [str(r[0]).strip() for r in cur.fetchall() if r and r[0] is not None]
                    if password in pins:
                        ok = True
        except Exception:
            pass
    if ok:
        request.session["logged_in"] = True
        request.session["role"] = "dueño"
        return RedirectResponse(url="/dashboard", status_code=303)
    return RedirectResponse(url="/?error=Credenciales%20inv%C3%A1lidas", status_code=303)


@app.post("/logout")
async def do_logout(request: Request, _=Depends(require_owner)):
    # Limpiar sesión y redirigir al login para evitar quedarse en una página JSON
    request.session.clear()
    return RedirectResponse(url="/", status_code=303)

@app.get("/logout")
async def logout_get(request: Request):
    # Soporta enlaces directos GET al logout y redirige al login
    request.session.clear()
    return RedirectResponse(url="/", status_code=303)


@app.get("/dashboard")
async def dashboard(request: Request):
    # Redirigir a login si no hay sesión activa
    if not request.session.get("logged_in"):
        return RedirectResponse(url="/", status_code=303)
    theme_vars = read_theme_vars(static_dir / "style.css")
    ctx = {
        "request": request,
        "theme": theme_vars,
        "gym_name": get_gym_name("Gimnasio"),
        "logo_url": _resolve_logo_url(),
    }
    return templates.TemplateResponse("dashboard.html", ctx)


@app.get("/api/theme")
async def api_theme(_=Depends(require_owner)):
    try:
        theme_vars = read_theme_vars(static_dir / "style.css")
        return JSONResponse(theme_vars)
    except Exception as e:
        logging.exception("Error in /api/theme")
        return JSONResponse({"error": str(e)}, status_code=500)


@app.get("/api/kpis")
async def api_kpis(_=Depends(require_owner)):
    db = _get_db()
    if db is None:
        return {"kpis": {"total_activos": 0, "nuevos_30_dias": 0, "ingresos_mes_actual": 0.0, "asistencias_hoy": 0}, "arpu": 0.0, "morosos": 0}
    try:
        data = db.obtener_kpis_generales()
        arpu, morosos = db.obtener_arpu_y_morosos_mes_actual()
        return {"kpis": data, "arpu": arpu, "morosos": morosos}
    except Exception as e:
        logging.exception("Error in /api/kpis")
        return JSONResponse({"error": str(e)}, status_code=500)


@app.get("/api/ingresos12m")
async def api_ingresos12m(request: Request, _=Depends(require_owner)):
    db = _get_db()
    result: Dict[str, float] = {}
    # Sin DB no podemos calcular; devolvemos estructura vacía
    if db is None:
        return {"ingresos": result}
    try:
        # Calcular ingresos por mes con SQL directo respetando rango si se provee
        start = request.query_params.get("start")
        end = request.query_params.get("end")
        with db.get_connection_context() as conn:  # type: ignore
            cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            if start and end:
                cur.execute(
                    """
                    WITH bounds AS (
                      SELECT date_trunc('month', %s::date) AS s, date_trunc('month', %s::date) AS e
                    )
                    SELECT to_char(date_trunc('month', COALESCE(p.fecha_pago::timestamp, make_timestamp(p.año, p.mes, 1, 0, 0, 0))), 'YYYY-MM') AS ym,
                           COALESCE(SUM(p.monto), 0) AS total
                    FROM pagos p, bounds b
                    WHERE date_trunc('month', COALESCE(p.fecha_pago::timestamp, make_timestamp(p.año, p.mes, 1, 0, 0, 0))) BETWEEN b.s AND b.e
                    GROUP BY 1
                    ORDER BY 1
                    """,
                    (start, end)
                )
            else:
                cur.execute(
                    """
                    SELECT to_char(date_trunc('month', COALESCE(p.fecha_pago::timestamp, make_timestamp(p.año, p.mes, 1, 0, 0, 0))), 'YYYY-MM') AS ym,
                           COALESCE(SUM(p.monto), 0) AS total
                    FROM pagos p
                    WHERE date_trunc('month', COALESCE(p.fecha_pago::timestamp, make_timestamp(p.año, p.mes, 1, 0, 0, 0))) 
                          >= date_trunc('month', CURRENT_DATE) - INTERVAL '11 months'
                    GROUP BY 1
                    ORDER BY 1
                    """
                )
            rows = cur.fetchall() or []

        # Construir base del rango solicitado o últimos 12 meses y completar faltantes
        try:
            from datetime import datetime as dt, date, timedelta
            base: Dict[str, float] = {}
            if start and end:
                s = dt.strptime(start, "%Y-%m-%d").date().replace(day=1)
                e = dt.strptime(end, "%Y-%m-%d").date().replace(day=1)
                curm = s
                while curm <= e:
                    base[curm.strftime("%Y-%m")] = 0.0
                    curm = (curm.replace(day=28) + timedelta(days=4)).replace(day=1)
            else:
                hoy = date.today().replace(day=1)
                curm = hoy
                for _ in range(12):
                    base[curm.strftime("%Y-%m")] = 0.0
                    curm = (curm.replace(day=28) + timedelta(days=4)).replace(day=1)
            for r in rows:
                ym = str(r.get('ym'))
                base[ym] = float(r.get('total') or 0.0)
            result = dict(sorted(base.items()))
        except Exception:
            # Si algo falla en el relleno, devolvemos la agregación tal cual
            result = {str(r.get('ym')): float(r.get('total') or 0.0) for r in rows}
        return {"ingresos": result}
    except Exception as e:
        logging.exception("Error en /api/ingresos12m")
        return JSONResponse({"error": str(e)}, status_code=500)


@app.get("/api/nuevos12m")
async def api_nuevos12m(request: Request, _=Depends(require_owner)):
    db = _get_db()
    result: Dict[str, int] = {}
    if db is None:
        return {"nuevos": result}
    try:
        # Delegar completamente al backend de database.py para mantener consistencia
        start = request.query_params.get("start")
        end = request.query_params.get("end")
        from datetime import datetime
        if start and end:
            try:
                inicio = datetime.strptime(start, "%Y-%m-%d").date()
                fin = datetime.strptime(end, "%Y-%m-%d").date()
                result = db.obtener_nuevos_usuarios_por_mes_rango(inicio, fin)  # type: ignore
            except Exception:
                # Si el rango no es válido, usar últimos 12 meses
                result = db.obtener_nuevos_usuarios_por_mes_ultimos_12()  # type: ignore
        else:
            result = db.obtener_nuevos_usuarios_por_mes_ultimos_12()  # type: ignore
        # Rellenar meses faltantes con 0 para evitar gráficos vacíos
        try:
            from datetime import date, timedelta
            base: Dict[str, int] = {}
            hoy = date.today().replace(day=1)
            for i in range(11, -1, -1):
                m = (hoy - timedelta(days=30 * i)).replace(day=1)
                clave = m.strftime("%Y-%m")
                base[clave] = 0
            base.update(result or {})
            result = dict(sorted(base.items()))
        except Exception:
            pass
        return {"nuevos": result}
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


@app.get("/api/arpu12m")
async def api_arpu12m(request: Request, _=Depends(require_owner)):
    db = _get_db()
    result: Dict[str, float] = {}
    if db is None:
        return {"arpu": result}
    try:
        start = request.query_params.get("start")
        end = request.query_params.get("end")
        with db.get_connection_context() as conn:  # type: ignore
            cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            if start and end:
                cur.execute(
                    """
                    WITH bounds AS (
                      SELECT date_trunc('month', %s::date) AS s, date_trunc('month', %s::date) AS e
                    )
                    SELECT to_char(date_trunc('month', COALESCE(p.fecha_pago::timestamp, make_timestamp(p.año, p.mes, 1, 0, 0, 0))), 'YYYY-MM') AS ym,
                           COALESCE(SUM(p.monto), 0) AS ingresos,
                           COUNT(DISTINCT p.usuario_id) AS pagadores
                    FROM pagos p, bounds b
                    WHERE date_trunc('month', COALESCE(p.fecha_pago::timestamp, make_timestamp(p.año, p.mes, 1, 0, 0, 0))) BETWEEN b.s AND b.e
                    GROUP BY 1
                    ORDER BY 1
                    """,
                    (start, end)
                )
            else:
                cur.execute(
                    """
                    SELECT to_char(date_trunc('month', COALESCE(p.fecha_pago::timestamp, make_timestamp(p.año, p.mes, 1, 0, 0, 0))), 'YYYY-MM') AS ym,
                           COALESCE(SUM(p.monto), 0) AS ingresos,
                           COUNT(DISTINCT p.usuario_id) AS pagadores
                    FROM pagos p
                    WHERE date_trunc('month', COALESCE(p.fecha_pago::timestamp, make_timestamp(p.año, p.mes, 1, 0, 0, 0))) 
                          >= date_trunc('month', CURRENT_DATE) - INTERVAL '11 months'
                    GROUP BY 1
                    ORDER BY 1
                    """
                )
            rows = cur.fetchall() or []

        # Construir base del rango solicitado o últimos 12 meses y completar faltantes
        try:
            from datetime import datetime as dt, date, timedelta
            base: Dict[str, float] = {}
            if start and end:
                s = dt.strptime(start, "%Y-%m-%d").date().replace(day=1)
                e = dt.strptime(end, "%Y-%m-%d").date().replace(day=1)
                curm = s
                while curm <= e:
                    base[curm.strftime("%Y-%m")] = 0.0
                    curm = (curm.replace(day=28) + timedelta(days=4)).replace(day=1)
            else:
                hoy = date.today().replace(day=1)
                for i in range(11, -1, -1):
                    m = (hoy - timedelta(days=30 * i)).replace(day=1)
                    base[m.strftime("%Y-%m")] = 0.0
            for r in rows:
                ym = str(r.get('ym'))
                ingresos = float(r.get('ingresos') or 0.0)
                pagadores = int(r.get('pagadores') or 0)
                base[ym] = (ingresos / pagadores) if pagadores > 0 else 0.0
            result = dict(sorted(base.items()))
        except Exception:
            # Si falla el relleno, calcular ARPU directo de filas
            result = {}
            for r in rows:
                ym = str(r.get('ym'))
                ingresos = float(r.get('ingresos') or 0.0)
                pagadores = int(r.get('pagadores') or 0)
                result[ym] = (ingresos / pagadores) if pagadores > 0 else 0.0
        return {"arpu": result}
    except Exception as e:
        import traceback
        traceback.print_exc()
        return JSONResponse({"error": str(e)}, status_code=500)


# --- Check-in inverso por QR (público para socios) ---
@app.get("/checkin")
async def checkin_page(request: Request):
    """Página pública de check-in para socios: autenticación por DNI+teléfono y lector de QR."""
    theme_vars = read_theme_vars(static_dir / "style.css")
    autenticado = bool(request.session.get("checkin_user_id"))
    socio_info = {}
    if autenticado:
        try:
            db = _get_db()
            if db:
                user_id = int(request.session.get("checkin_user_id"))
                with db.get_connection_context() as conn:  # type: ignore
                    cur = conn.cursor()
                    cur.execute(
                        """
                        SELECT id, COALESCE(nombre,'') AS nombre, COALESCE(dni,'') AS dni, COALESCE(telefono,'') AS telefono
                        FROM usuarios WHERE id = %s LIMIT 1
                        """,
                        (user_id,)
                    )
                    row = cur.fetchone()
                    if row:
                        import re as _re
                        user_id_db = int(row[0]) if row[0] is not None else user_id
                        nombre = str(row[1] or "")
                        dni = str(row[2] or "")
                        telefono = str(row[3] or "")
                        digits = _re.sub(r"\D+", "", telefono)
                        masked = ("*" * max(len(digits) - 4, 4)) + (digits[-4:] if len(digits) >= 4 else digits)
                        socio_info = {
                            "id": user_id_db,
                            "nombre": nombre,
                            "dni": dni,
                            "telefono_mask": masked,
                        }
        except Exception:
            pass
    ctx = {
        "request": request,
        "theme": theme_vars,
        "gym_name": get_gym_name("Gimnasio"),
        "logo_url": _resolve_logo_url(),
        "autenticado": autenticado,
        "socio": socio_info,
    }
    return templates.TemplateResponse("checkin.html", ctx)

@app.get("/checkin/logout")
async def checkin_logout(request: Request):
    # Logout específico del flujo de check-in: limpia solo la sesión de socio y vuelve a /checkin
    try:
        if "checkin_user_id" in request.session:
            request.session.pop("checkin_user_id", None)
    except Exception:
        request.session.clear()
    return RedirectResponse(url="/checkin", status_code=303)


@app.post("/checkin/auth")
async def checkin_auth(request: Request):
    """Autentica al socio por DNI y teléfono (ambos numéricos) y guarda la sesión de check-in."""
    db = _get_db()
    if db is None:
        return JSONResponse({"success": False, "message": "Base de datos no disponible"}, status_code=500)
    # Obtener datos del cuerpo (form o JSON)
    content_type = request.headers.get("content-type", "")
    if content_type.startswith("application/x-www-form-urlencoded"):
        data = await request.form()
    else:
        try:
            data = await request.json()
        except Exception:
            data = {}
    dni = str(data.get("dni", "")).strip()
    telefono = str(data.get("telefono", "")).strip()
    # Normalizar: solo dígitos
    import re
    dni_num = re.sub(r"\D+", "", dni)
    tel_num = re.sub(r"\D+", "", telefono)
    if not dni_num or not tel_num:
        return JSONResponse({"success": False, "message": "Ingrese DNI y teléfono válidos"}, status_code=400)
    try:
        with db.get_connection_context() as conn:  # type: ignore
            cur = conn.cursor()
            # Preparar variantes del teléfono para permitir coincidencias flexibles
            tel_like = f"%{tel_num}"
            tel_last10 = tel_num[-10:] if len(tel_num) >= 10 else tel_num
            # Comparar por equivalencia de dígitos para soportar formatos (+54, 0-prefijo, espacios, guiones)
            cur.execute(
                """
                SELECT id FROM usuarios
                WHERE activo = TRUE
                  AND regexp_replace(CAST(dni AS TEXT), '\\D+', '', 'g') = %s
                  AND (
                    regexp_replace(CAST(telefono AS TEXT), '\\D+', '', 'g') = %s
                    OR regexp_replace(CAST(telefono AS TEXT), '\\D+', '', 'g') = '54' || %s
                    OR regexp_replace(CAST(telefono AS TEXT), '\\D+', '', 'g') = '0' || %s
                    OR regexp_replace(CAST(telefono AS TEXT), '\\D+', '', 'g') LIKE %s
                    OR right(regexp_replace(CAST(telefono AS TEXT), '\\D+', '', 'g'), 10) = %s
                  )
                ORDER BY id ASC
                LIMIT 1
                """,
                (dni_num, tel_num, tel_num, tel_num, tel_like, tel_last10)
            )
            row = cur.fetchone()
            if not row:
                # Evitar 404 para no confundir con ruta inexistente
                return JSONResponse({"success": False, "message": "Credenciales inválidas"}, status_code=200)
            user_id = int(row[0])
            request.session["checkin_user_id"] = user_id
            return JSONResponse({"success": True, "message": "Autenticado", "usuario_id": user_id})
    except Exception as e:
        logging.exception("Error en /checkin/auth")
        return JSONResponse({"success": False, "message": str(e)}, status_code=500)


@app.post("/api/checkin/validate")
async def api_checkin_validate(request: Request):
    """Valida el token escaneado y registra asistencia si corresponde."""
    db = _get_db()
    if db is None:
        return JSONResponse({"success": False, "message": "Base de datos no disponible"}, status_code=500)
    try:
        data = await request.json()
        token = str(data.get("token", "")).strip()
        socio_id = request.session.get("checkin_user_id")
        if not socio_id:
            return JSONResponse({"success": False, "message": "Sesión de socio no encontrada"}, status_code=401)
        # Orden de parámetros: (token, socio_id)
        ok, msg = db.validar_token_y_registrar_asistencia(token, int(socio_id))  # type: ignore
        status = 200 if ok else 400
        return JSONResponse({"success": ok, "message": msg}, status_code=status)
    except Exception as e:
        logging.exception("Error en /api/checkin/validate")
        return JSONResponse({"success": False, "message": str(e)}, status_code=500)


@app.get("/api/checkin/token_status")
async def api_checkin_token_status(request: Request):
    """Consulta el estado de un token: { exists, used, expired }.

    Criterio de 'used': se considera usado si el flag en checkin_pending es TRUE
    o si ya existe una asistencia para el usuario en la fecha actual.
    Esto hace el polling del escritorio más robusto ante posibles desincronizaciones.
    """
    db = _get_db()
    if db is None:
        return JSONResponse({"exists": False, "used": False, "expired": True}, status_code=200)
    import datetime as _dt
    token = str(request.query_params.get("token", "")).strip()
    if not token:
        return JSONResponse({"exists": False, "used": False, "expired": True}, status_code=200)
    try:
        with db.get_connection_context() as conn:  # type: ignore
            cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            # Obtener también usuario_id para verificar asistencia del día
            cur.execute("SELECT usuario_id, used, expires_at FROM checkin_pending WHERE token = %s LIMIT 1", (token,))
            row = cur.fetchone()
            if not row:
                return JSONResponse({"exists": False, "used": False, "expired": True}, status_code=200)

            used_flag = bool(row.get("used") or False)
            expires_at = row.get("expires_at")
            now = _dt.datetime.now()
            expired = bool(expires_at and expires_at < now)

            usuario_id = row.get("usuario_id")
            attended_today = False
            try:
                if usuario_id is not None:
                    cur2 = conn.cursor()
                    cur2.execute(
                        "SELECT 1 FROM asistencias WHERE usuario_id = %s AND fecha::date = CURRENT_DATE LIMIT 1",
                        (int(usuario_id),)
                    )
                    attended_today = cur2.fetchone() is not None
            except Exception:
                attended_today = False

            used = bool(used_flag or attended_today)

            # Log a nivel INFO para que aparezca en los logs por defecto
            try:
                logging.info(
                    f"token_status: token={token} usuario_id={usuario_id} used_flag={used_flag} attended_today={attended_today} expired={expired}"
                )
            except Exception:
                pass

            return JSONResponse({"exists": True, "used": used, "expired": expired}, status_code=200)
    except Exception as e:
        logging.exception("Error en /api/checkin/token_status")
        return JSONResponse({"exists": False, "used": False, "expired": True, "error": str(e)}, status_code=200)


@app.get("/api/asistencia_30d")
async def api_asistencia_30d(request: Request, _=Depends(require_owner)):
    db = _get_db()
    series: Dict[str, int] = {}
    if db is None:
        return series
    try:
        start = request.query_params.get("start")
        end = request.query_params.get("end")
        # Delegar a backend: nueva función agregada para consistencia de esquema
        if start and end:
            data = db.obtener_asistencias_por_rango_diario(start, end)  # type: ignore
        else:
            data = db.obtener_asistencias_por_dia(30)  # type: ignore
        for d, c in (data or []):
            series[str(d)] = int(c or 0)
        # Rellenar días faltantes con 0 para últimos 30 días
        try:
            from datetime import date, timedelta
            base: Dict[str, int] = {}
            hoy = date.today()
            for i in range(29, -1, -1):
                dia = hoy - timedelta(days=i)
                clave = dia.strftime("%Y-%m-%d")
                base[clave] = 0
            base.update(series or {})
            series = dict(sorted(base.items()))
        except Exception:
            pass
        return series
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


@app.get("/api/asistencia_por_hora_30d")
async def api_asistencia_por_hora_30d(request: Request, _=Depends(require_owner)):
    db = _get_db()
    series: Dict[str, int] = {}
    if db is None:
        return series
    try:
        start = request.query_params.get("start")
        end = request.query_params.get("end")
        # Delegar a backend: función agregada que agrupa por hora usando hora_registro
        if start and end:
            data = db.obtener_asistencias_por_hora_rango(start, end)  # type: ignore
        else:
            data = db.obtener_asistencias_por_hora(30)  # type: ignore
        for h, c in (data or []):
            label = f"{int(h):02d}:00" if isinstance(h, (int, float)) else str(h)
            series[label] = int(c or 0)
        # Completar horas 00..23 con 0 si faltan
        try:
            base: Dict[str, int] = {f"{i:02d}:00": 0 for i in range(0, 24)}
            base.update(series or {})
            series = dict(sorted(base.items()))
        except Exception:
            pass
        return series
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


@app.get("/api/activos_inactivos")
async def api_activos_inactivos(_=Depends(require_owner)):
    db = _get_db()
    if db is None:
        return {"Activos": 0, "Inactivos": 0}
    try:
        # Delegar al backend validado para conteo
        return db.obtener_conteo_activos_inactivos()  # type: ignore
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


@app.get("/api/tipos_cuota")
async def api_tipos_cuota(_=Depends(require_owner)):
    db = _get_db()
    dist: Dict[str, int] = {}
    if db is None:
        return dist
    try:
        # Contar usuarios activos por tipo de cuota (roles socio/miembro) de forma robusta:
        # - Soporta que usuarios.tipo_cuota sea nombre o id (distintos esquemas)
        # - Evita errores de tipo usando CAST a texto para comparar con id
        with db.get_connection_context() as conn:  # type: ignore
            cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            cur.execute(
                """
                SELECT COALESCE(tc.nombre, CAST(u.tipo_cuota AS TEXT), 'Sin tipo') AS tipo,
                       COUNT(*) AS cantidad
                FROM usuarios u
                LEFT JOIN tipos_cuota tc 
                  ON (CAST(u.tipo_cuota AS TEXT) = tc.nombre) 
                     OR (CAST(u.tipo_cuota AS TEXT) = CAST(tc.id AS TEXT))
                WHERE u.activo = true
                  AND LOWER(COALESCE(u.rol,'')) IN ('socio','profesor')
                GROUP BY COALESCE(tc.nombre, CAST(u.tipo_cuota AS TEXT), 'Sin tipo')
                ORDER BY 1
                """
            )
            def normalize_label(label: str) -> str:
                s = (label or 'Sin tipo').strip().lower()
                replacements = {
                    'estandar': 'Estándar', 'estándar': 'Estándar', 'standard': 'Estándar',
                    'sin tipo': 'Sin tipo',
                    'mensualidad': 'Mensual', 'mensual': 'Mensual',
                }
                return replacements.get(s, s.title())
            for r in cur.fetchall() or []:
                raw = (r.get('tipo') or 'Sin tipo')
                nombre = normalize_label(str(raw))
                # Excluir SOLO "estandar" (sin acento)
                if nombre.strip().lower() == 'estandar':
                    continue
                dist[nombre] = int(r.get('cantidad') or 0)
        return dist
    except Exception as e:
        import traceback
        traceback.print_exc()
        return JSONResponse({"error": str(e)}, status_code=500)


@app.get("/api/kpis_avanzados")
async def api_kpis_avanzados(_=Depends(require_owner)):
    db = _get_db()
    kpis: Dict[str, Any] = {
        'retention_rate': 0.0,
        'churn_rate': 0.0,
        'avg_attendance': 0.0,
        'peak_hour': 'N/A',
        'revenue_growth': 0.0,
        'weekly_active_users': 0,
        'payment_rate': 0.0,
        'ltv_12m': 0.0,
        'arpa': 0.0,
        'mrr': 0.0,
    }
    if db is None:
        return kpis
    try:
        # Fechas clave y métricas utilizando métodos existentes del backend
        from datetime import timedelta
        hoy = datetime.now().date()
        primer_dia_mes = hoy.replace(day=1)
        fin_mes_anterior = primer_dia_mes - timedelta(days=1)
        primer_dia_mes_anterior = fin_mes_anterior.replace(day=1)

        # Ingresos del mes actual y anterior
        ing_act = float(db.calcular_ingresos_totales(primer_dia_mes, hoy) or 0.0)  # type: ignore
        ing_ant = float(db.calcular_ingresos_totales(primer_dia_mes_anterior, fin_mes_anterior) or 0.0)  # type: ignore
        kpis['revenue_growth'] = ((ing_act - ing_ant) / ing_ant * 100.0) if ing_ant > 0 else 0.0

        # Pagadores en el mes actual (roles socio/profesor, robusto a fecha/año-mes)
        with db.get_connection_context() as conn:  # type: ignore
            cur = conn.cursor()
            cur.execute(
                """
                SELECT COUNT(DISTINCT u.id) AS pagadores
                FROM usuarios u
                JOIN pagos p ON p.usuario_id = u.id
                WHERE u.activo = TRUE
                  AND LOWER(COALESCE(u.rol,'')) IN ('socio','profesor')
                  AND date_trunc('month', COALESCE(p.fecha_pago, make_date(p.año, p.mes, 1))) = date_trunc('month', CURRENT_DATE)
                """
            )
            row = cur.fetchone()
            pagadores_mes = int(row[0] if row and row[0] is not None else 0)

        # Lista de pagos del mes actual para métricas de retención
        pagos_mes = db.obtener_pagos_por_fecha(primer_dia_mes, hoy) or []  # type: ignore

        # Usuarios activos totales desde KPIs generales
        kpis_generales = db.obtener_kpis_generales() or {}
        activos_total = int(kpis_generales.get('total_activos', 0) or 0)
        kpis['payment_rate'] = (pagadores_mes / activos_total * 100.0) if activos_total > 0 else 0.0

        # ARPA y MRR
        kpis['mrr'] = ing_act
        kpis['arpa'] = (ing_act / pagadores_mes) if pagadores_mes > 0 else 0.0

        # LTV 12 meses
        inicio_12m = hoy - timedelta(days=365)
        pagos_12m = db.obtener_pagos_por_fecha(inicio_12m, hoy) or []  # type: ignore
        total_12m = sum(float(p.get('monto', 0) or 0.0) for p in pagos_12m if isinstance(p, dict))
        usuarios_12m = len({p.get('usuario_id') for p in pagos_12m if isinstance(p, dict)})
        kpis['ltv_12m'] = (total_12m / usuarios_12m) if usuarios_12m > 0 else 0.0

        # Retención vs mes anterior
        pagos_mes_ant = db.obtener_pagos_por_fecha(primer_dia_mes_anterior, fin_mes_anterior) or []  # type: ignore
        set_act = {p.get('usuario_id') for p in pagos_mes if isinstance(p, dict)}
        set_ant = {p.get('usuario_id') for p in pagos_mes_ant if isinstance(p, dict)}
        kpis['retention_rate'] = (len(set_act & set_ant) / len(set_ant) * 100.0) if len(set_ant) > 0 else 0.0
        kpis['churn_rate'] = 100.0 - kpis['retention_rate']

        # Asistencias últimos 30 días para promedio y hora pico
        asist_30 = db.obtener_asistencias_por_fecha_limite(hoy - timedelta(days=30)) or []  # type: ignore
        from collections import Counter
        dias_conteo = Counter()
        horas_conteo = Counter()
        for a in asist_30:
            f = a.get('fecha')
            h = a.get('hora_registro')
            if hasattr(f, 'date'):
                f = f.date()
            dias_conteo[f] += 1
            if h is not None:
                try:
                    hh = h.hour if hasattr(h, 'hour') else int(str(h).split(':')[0])
                    horas_conteo[hh] += 1
                except Exception:
                    pass
        kpis['avg_attendance'] = (sum(dias_conteo.values()) / len(dias_conteo)) if len(dias_conteo) > 0 else 0.0
        if len(horas_conteo) > 0:
            peak_h = max(horas_conteo.items(), key=lambda kv: kv[1])[0]
            kpis['peak_hour'] = f"{int(peak_h):02d}:00"
        else:
            kpis['peak_hour'] = 'N/A'

        # Usuarios activos semanales por asistencias últimos 7 días
        asist_7 = db.obtener_asistencias_por_fecha_limite(hoy - timedelta(days=7)) or []  # type: ignore
        kpis['weekly_active_users'] = len({a.get('usuario_id') for a in asist_7})

        return kpis
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


@app.get("/api/cohort_retencion_6m")
async def api_cohort_retencion_6m(_=Depends(require_owner)):
    """Retención por cohorte de registro (últimos 6 meses): % del cohorte que pagó este mes."""
    db = _get_db()
    result: Dict[str, float] = {}
    if db is None:
        return result
    try:
        with db.get_connection_context() as conn:  # type: ignore
            cur = conn.cursor()
            cur.execute(
                """
                SELECT to_char(date_trunc('month', u.fecha_registro), 'YYYY-MM') AS cohorte,
                       COUNT(*) AS tam,
                       SUM(CASE WHEN EXISTS (
                           SELECT 1 FROM pagos p
                           WHERE p.usuario_id = u.id
                             AND date_trunc('month', COALESCE(p.fecha_pago, make_date(p.año, p.mes, 1))) = date_trunc('month', CURRENT_DATE)
                       ) THEN 1 ELSE 0 END) AS retenidos
                FROM usuarios u
                WHERE u.fecha_registro IS NOT NULL
                  AND date_trunc('month', u.fecha_registro) >= date_trunc('month', CURRENT_DATE) - INTERVAL '5 months'
                  AND u.activo = true
                  AND LOWER(COALESCE(u.rol,'')) NOT IN ('dueño','dueno','owner','administrador','admin')
                GROUP BY 1
                ORDER BY 1
                """
            )
            for cohorte, tam, retenidos in cur.fetchall():
                tam = int(tam or 0); retenidos = int(retenidos or 0)
                result[str(cohorte)] = (retenidos / tam * 100.0) if tam > 0 else 0.0
        return result
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


@app.get("/api/cohort_retencion_heatmap")
async def api_cohort_retencion_heatmap(months: int = 6, _=Depends(require_owner)):
    """Matriz de retención por cohorte (registro) vs mes de pago.
    Devuelve { months: [...], cohorts: [...], matrix: [[%...], ...] } para últimos N meses.
    """
    db = _get_db()
    res: Dict[str, Any] = { 'months': [], 'cohorts': [], 'matrix': [] }
    if db is None:
        return res
    months = max(1, min(months, 24))
    try:
        with db.get_connection_context() as conn:  # type: ignore
            cur = conn.cursor()
            # Lista de meses (YYYY-MM) para los últimos N meses
            cur.execute("SELECT to_char(date_trunc('month', CURRENT_DATE) - s * INTERVAL '1 month', 'YYYY-MM') FROM generate_series(0,%s) s ORDER BY 1", (months-1,))
            months_list = [row[0] for row in cur.fetchall()]
            cohorts = months_list
            matrix: list[list[float]] = []
            for coh in cohorts:
                # Tamaño de cohorte
                cur.execute("""
                    SELECT COUNT(*) FROM usuarios u
                    WHERE u.activo = true AND u.fecha_registro IS NOT NULL
                      AND LOWER(COALESCE(u.rol,'')) NOT IN ('dueño','dueno','owner','administrador','admin')
                      AND to_char(date_trunc('month', u.fecha_registro), 'YYYY-MM') = %s
                """, (coh,))
                cohort_size = int(cur.fetchone()[0] or 0)
                row: list[float] = []
                for m in months_list:
                    # Usuarios del cohorte que pagaron en mes m
                    cur.execute("""
                        SELECT COUNT(*) FROM usuarios u
                        WHERE u.activo = true AND u.fecha_registro IS NOT NULL
                          AND LOWER(COALESCE(u.rol,'')) NOT IN ('dueño','dueno','owner','administrador','admin')
                          AND to_char(date_trunc('month', u.fecha_registro), 'YYYY-MM') = %s
                          AND EXISTS (
                            SELECT 1 FROM pagos p
                            WHERE p.usuario_id = u.id
                              AND to_char(date_trunc('month', COALESCE(p.fecha_pago, make_date(p.año, p.mes, 1))), 'YYYY-MM') = %s
                          )
                    """, (coh, m))
                    retained = int(cur.fetchone()[0] or 0)
                    pct = (retained / cohort_size * 100.0) if cohort_size > 0 else 0.0
                    row.append(pct)
                matrix.append(row)
            return {"months": months_list, "cohorts": cohorts, "matrix": matrix}
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


@app.get("/api/arpa_por_tipo_cuota")
async def api_arpa_por_tipo_cuota(_=Depends(require_owner)):
    """ARPA por tipo de cuota para el mes actual (monto total / pagadores del tipo)."""
    db = _get_db()
    dist: Dict[str, float] = {}
    if db is None:
        return dist
    try:
        with db.get_connection_context() as conn:  # type: ignore
            cur = conn.cursor()
            def norm(label: str) -> str:
                s = (label or '—').strip().lower()
                replacements = {
                    'estandar': 'Estándar', 'estándar': 'Estándar', 'standard': 'Estándar',
                    'estudiante': 'Estudiante', 'student': 'Estudiante',
                    'funcional': 'Funcional', 'functional': 'Funcional',
                }
                return replacements.get(s, s.title())
            cur.execute(
                """
                SELECT COALESCE(tc.nombre, CAST(u.tipo_cuota AS TEXT), '—') AS tipo,
                       COALESCE(SUM(CASE WHEN date_trunc('month',
                           CASE
                               WHEN p.fecha_pago IS NOT NULL THEN p.fecha_pago
                               WHEN p.año IS NOT NULL AND p.mes IS NOT NULL THEN make_date(p.año, p.mes, 1)
                               ELSE NULL
                           END
                       ) = date_trunc('month', CURRENT_DATE) THEN p.monto END), 0) AS monto_mes,
                       COUNT(DISTINCT CASE WHEN date_trunc('month',
                           CASE
                               WHEN p.fecha_pago IS NOT NULL THEN p.fecha_pago
                               WHEN p.año IS NOT NULL AND p.mes IS NOT NULL THEN make_date(p.año, p.mes, 1)
                               ELSE NULL
                           END
                       ) = date_trunc('month', CURRENT_DATE) THEN u.id END) AS pagadores_mes
                FROM usuarios u
                LEFT JOIN pagos p ON p.usuario_id = u.id
                -- Emparejar robustamente por nombre o id como texto
                LEFT JOIN tipos_cuota tc 
                  ON (CAST(u.tipo_cuota AS TEXT) = tc.nombre)
                  OR (CAST(u.tipo_cuota AS TEXT) = CAST(tc.id AS TEXT))
                WHERE u.activo = true
                  AND LOWER(COALESCE(u.rol,'')) NOT IN ('dueño','dueno','owner','administrador','admin')
                GROUP BY 1
                """
            )
            for tipo, monto_mes, pagadores_mes in cur.fetchall():
                key = norm(str(tipo))
                # Excluir SOLO "estandar" (sin acento)
                if key.strip().lower() == 'estandar':
                    continue
                pagadores = int(pagadores_mes or 0)
                total = float(monto_mes or 0.0)
                dist[key] = (total / pagadores) if pagadores > 0 else 0.0
        return dist
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


@app.get("/api/payment_status_dist")
async def api_payment_status_dist(_=Depends(require_owner)):
    db = _get_db()
    dist = {"no_payments": 0, "up_to_date": 0, "pending": 0, "overdue": 0}
    if db is None:
        return dist
    try:
        with db.get_connection_context() as conn:  # type: ignore
            cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            cur.execute(
                """
                WITH last_pay AS (
                  SELECT u.id AS usuario_id,
                         MAX(p.fecha_pago::timestamp) AS last_date
                  FROM usuarios u
                  LEFT JOIN pagos p ON p.usuario_id = u.id
                  WHERE u.activo = true
                    AND LOWER(COALESCE(u.rol,'')) IN ('socio','profesor')
                  GROUP BY u.id
                )
                SELECT
                  SUM(CASE WHEN last_date IS NULL THEN 1 ELSE 0 END) AS no_payments,
                  SUM(CASE WHEN last_date IS NOT NULL AND date_trunc('month', last_date) = date_trunc('month', CURRENT_DATE::timestamp) THEN 1 ELSE 0 END) AS up_to_date,
                  SUM(CASE WHEN last_date IS NOT NULL AND date_trunc('month', last_date) < date_trunc('month', CURRENT_DATE::timestamp) AND (CURRENT_DATE - last_date::date) <= 30 THEN 1 ELSE 0 END) AS pending,
                  SUM(CASE WHEN last_date IS NOT NULL AND (CURRENT_DATE - last_date::date) > 30 THEN 1 ELSE 0 END) AS overdue
                FROM last_pay
                """
            )
            row = cur.fetchone() or {}
            dist["no_payments"] = int(row.get("no_payments") or 0)
            dist["up_to_date"] = int(row.get("up_to_date") or 0)
            dist["pending"] = int(row.get("pending") or 0)
            dist["overdue"] = int(row.get("overdue") or 0)
        return dist
    except Exception as e:
        import traceback
        traceback.print_exc()
        return JSONResponse({"error": str(e)}, status_code=500)


# --- Tablas detalladas para subpestañas ---

@app.get("/api/usuarios_detalle")
async def api_usuarios_detalle(_=Depends(require_owner)):
    """Detalle de usuarios con métricas reales: pagos y asistencias."""
    db = _get_db()
    if db is None:
        return []
    try:
        with db.get_connection_context() as conn:  # type: ignore
            cur = conn.cursor()
            cur.execute(
                """
                SELECT u.id,
                       COALESCE(u.nombre,'') AS nombre,
                       COALESCE(u.rol,'') AS rol,
                       COALESCE(u.dni,'') AS dni,
                       COALESCE(u.telefono,'') AS telefono,
                       COALESCE(u.tipo_cuota,'') AS tipo_cuota,
                       COALESCE(u.activo,false) AS activo,
                       COALESCE(u.fecha_registro::date, CURRENT_DATE) AS fecha_registro,
                       COALESCE(p.pagos_count,0) AS pagos_count,
                       p.last_pago_fecha::date AS last_pago_fecha,
                       COALESCE(a.asistencias_count,0) AS asistencias_count
                FROM usuarios u
                LEFT JOIN (
                    SELECT usuario_id, COUNT(*) AS pagos_count, MAX(fecha_pago) AS last_pago_fecha
                    FROM pagos
                    GROUP BY usuario_id
                ) p ON p.usuario_id = u.id
                LEFT JOIN (
                    SELECT usuario_id, COUNT(*) AS asistencias_count
                    FROM asistencias
                    GROUP BY usuario_id
                ) a ON a.usuario_id = u.id
                ORDER BY u.id
                """
            )
            rows = []
            for r in cur.fetchall():
                rows.append({
                    "id": r[0],
                    "nombre": r[1],
                    "rol": r[2],
                    "dni": r[3],
                    "telefono": r[4],
                    "tipo_cuota": r[5],
                    "activo": bool(r[6]),
                    "fecha_registro": str(r[7]) if r[7] is not None else None,
                    "pagos_count": int(r[8] or 0),
                    "last_pago_fecha": str(r[9]) if r[9] is not None else None,
                    "asistencias_count": int(r[10] or 0),
                })
        return rows
    except Exception as e:
        import traceback
        traceback.print_exc()
        print("Error in api_profesores_detalle:", repr(e))
        return JSONResponse({"error": str(e)}, status_code=500)


@app.get("/api/profesores_detalle")
async def api_profesores_detalle(request: Request, _=Depends(require_owner)):
    """Detalle de profesores con contacto y horarios/sesiones reales.

    Devuelve por profesor:
    - Datos de contacto (nombre, email, teléfono)
    - Cantidad de horarios de disponibilidad (activos)
    - Resumen de horarios (día, hora_inicio, hora_fin)
    - Sesiones del mes actual y horas trabajadas reales
    """
    print("DEBUG: top-level entry in /api/profesores_detalle")
    db = _get_db()
    if db is None:
        return []
    try:
        with db.get_connection_context() as conn:  # type: ignore
            cur = conn.cursor()
            # Mes/año actuales para métricas de sesiones reales
            # Rango opcional (start/end) para sesiones/horas; si no se envía, usa mes actual
            start = request.query_params.get("start")
            end = request.query_params.get("end")
            # Normalizar parámetros vacíos a None para evitar BETWEEN con cadenas vacías
            if not start or (isinstance(start, str) and start.strip() == ""):
                start = None
            if not end or (isinstance(end, str) and end.strip() == ""):
                end = None
            from datetime import datetime
            now = datetime.now()
            mes_actual = now.month
            anio_actual = now.year

            # Base: profesores + usuario info (sin 'email' para evitar UndefinedColumn en esquemas sin esa columna)
            try:
                cur.execute(
                    """
                    SELECT p.id,
                           COALESCE(u.nombre,'') AS nombre,
                           COALESCE(u.telefono,'') AS telefono
                    FROM profesores p
                    JOIN usuarios u ON u.id = p.usuario_id
                    ORDER BY p.id
                    """
                )
                base_rows = cur.fetchall()
                print(f"DEBUG: profesores base_rows count = {len(base_rows)}")
            except Exception as e:
                import traceback
                traceback.print_exc()
                print("Error base SELECT in profesores_detalle:", repr(e))
                try:
                    conn.rollback()
                except Exception:
                    pass
                base_rows = []

            # Sessions aggregation (controlled reintroduction)
            try:
                # Parsear fechas si vienen como strings
                from datetime import datetime as _dt
                start_date = None
                end_date = None
                try:
                    if start:
                        start_date = _dt.strptime(start, "%Y-%m-%d").date()
                    if end:
                        end_date = _dt.strptime(end, "%Y-%m-%d").date()
                except Exception:
                    start_date = None
                    end_date = None

                if start_date and end_date:
                    cur.execute(
                        """
                        SELECT profesor_id,
                               COUNT(*) AS sesiones_mes,
                               COALESCE(SUM(minutos_totales) / 60.0, 0) AS horas_mes
                        FROM profesor_horas_trabajadas
                        WHERE hora_fin IS NOT NULL AND fecha BETWEEN %s AND %s
                        GROUP BY profesor_id
                        """,
                        (start_date, end_date)
                    )
                else:
                    cur.execute(
                        """
                        SELECT profesor_id,
                               COUNT(*) AS sesiones_mes,
                               COALESCE(SUM(minutos_totales) / 60.0, 0) AS horas_mes
                        FROM profesor_horas_trabajadas
                        WHERE hora_fin IS NOT NULL AND EXTRACT(MONTH FROM fecha) = %s AND EXTRACT(YEAR FROM fecha) = %s
                        GROUP BY profesor_id
                        """,
                        (mes_actual, anio_actual)
                    )
                s_map = {row[0]: (int(row[1] or 0), float(row[2] or 0)) for row in cur.fetchall()}
            except Exception as e:
                logging.exception("Error sessions aggregation in profesores_detalle")
                try:
                    conn.rollback()
                except Exception:
                    pass
                s_map = {}

            # Base-only response plus schedules (controlled reintroduction)
            rows = []
            # Construir respuesta: como 'email' puede no existir en la BD, devolvemos ''
            for pid, nombre, telefono in base_rows:
                # Obtener horarios de disponibilidad del profesor desde database.py
                try:
                    disp = db.obtener_horarios_disponibilidad_profesor(pid)  # type: ignore
                    horarios_list = [
                        {
                            "dia": h.get("dia_semana"),
                            "inicio": str(h.get("hora_inicio")) if h.get("hora_inicio") is not None else None,
                            "fin": str(h.get("hora_fin")) if h.get("hora_fin") is not None else None,
                        }
                        for h in (disp or [])
                    ]
                except Exception as e:
                    logging.exception(f"Error disponibilidad para profesor {pid} en profesores_detalle")
                    try:
                        conn.rollback()
                    except Exception:
                        pass
                    horarios_list = []
                rows.append({
                    "id": pid,
                    "nombre": nombre,
                    "email": "",
                    "telefono": telefono,
                    "horarios_count": len(horarios_list),
                    "horarios": horarios_list,
                    "sesiones_mes": s_map.get(pid, (0, 0.0))[0],
                    "horas_mes": s_map.get(pid, (0, 0.0))[1],
                })
            logging.debug("profesores response rows count = %d", len(rows))
        return rows
    except Exception as e:
        logging.exception("Error final in /api/profesores_detalle")
        return []

# --- Endpoints de detalle para pagos y asistencias ---

@app.get("/api/profesor_sesiones")
async def api_profesor_sesiones(request: Request, _=Depends(require_owner)):
    """Lista de sesiones trabajadas por un profesor, opcionalmente por rango.

    Devuelve datos reales y completos por sesión obtenidos directamente del backend
    (database.py) sin cálculos en la web: fecha, inicio, fin, minutos, horas,
    clase (si existe) y tipo de actividad.
    """
    db = _get_db()
    if db is None:
        return []
    try:
        pid = request.query_params.get("profesor_id")
        if not pid:
            return []
        try:
            profesor_id = int(pid)
        except Exception:
            return []
        start = request.query_params.get("start")
        end = request.query_params.get("end")
        # Delegar completamente al backend (database.py)
        from datetime import datetime
        fecha_inicio = None
        fecha_fin = None
        try:
            if start:
                fecha_inicio = datetime.strptime(start, "%Y-%m-%d").date()
            if end:
                fecha_fin = datetime.strptime(end, "%Y-%m-%d").date()
        except Exception:
            # Si las fechas no son válidas, se ignoran y se usa mes actual dentro del backend
            fecha_inicio = None
            fecha_fin = None

        try:
            sesiones = db.obtener_horas_trabajadas_profesor(profesor_id, fecha_inicio, fecha_fin)  # type: ignore
        except Exception:
            sesiones = []

        # Normalizar y filtrar: solo sesiones cerradas; permitimos minutos 0 para mostrar registro
        out = []
        for s in sesiones or []:
            minutos_val = s.get("minutos_totales")
            fin_val = s.get("hora_fin")
            # filtrar sin fin
            try:
                minutos_num = int(minutos_val) if minutos_val is not None else 0
            except Exception:
                minutos_num = 0
            if fin_val is None:
                continue
            # formato seguro para fecha y hora (HH:MM)
            def _fmt_date(d):
                try:
                    return str(d)[:10] if d is not None else ""
                except Exception:
                    return str(d) if d is not None else ""
            def _fmt_time(t):
                try:
                    sstr = str(t) if t is not None else ""
                    return sstr[:5] if len(sstr) >= 5 else sstr
                except Exception:
                    return ""
            out.append({
                "fecha": _fmt_date(s.get("fecha")),
                "inicio": _fmt_time(s.get("hora_inicio")),
                "fin": _fmt_time(fin_val),
                "minutos": minutos_num,
                "horas": round(minutos_num / 60.0, 2),
                "tipo": s.get("tipo_actividad") or ""
            })
        return out
    except Exception as e:
        logging.exception("Error en /api/profesor_sesiones")
        return []

# Ruta duplicada /api/profesor_resumen eliminada para evitar respuestas inconsistentes

@app.get("/api/usuario_pagos")
async def api_usuario_pagos(request: Request, _=Depends(require_owner)):
    """Lista de pagos reales de un usuario con soporte de búsqueda y paginación."""
    db = _get_db()
    if db is None:
        return []
    try:
        usuario_id = request.query_params.get("usuario_id")
        q = request.query_params.get("q")
        limit = request.query_params.get("limit")
        offset = request.query_params.get("offset")
        if not usuario_id:
            return []
        lim = int(limit) if limit and limit.isdigit() else 50
        off = int(offset) if offset and offset.isdigit() else 0
        with db.get_connection_context() as conn:  # type: ignore
            cur = conn.cursor()
            if q:
                cur.execute(
                    """
                    SELECT p.fecha_pago::date, p.monto, COALESCE(u.tipo_cuota,'')
                    FROM pagos p
                    JOIN usuarios u ON u.id = p.usuario_id
                    WHERE p.usuario_id = %s AND (u.tipo_cuota ILIKE %s)
                    ORDER BY p.fecha_pago DESC
                    LIMIT %s OFFSET %s
                    """,
                    (int(usuario_id), f"%{q}%", lim, off)
                )
            else:
                cur.execute(
                    """
                    SELECT p.fecha_pago::date, p.monto, COALESCE(u.tipo_cuota,'')
                    FROM pagos p
                    JOIN usuarios u ON u.id = p.usuario_id
                    WHERE p.usuario_id = %s
                    ORDER BY p.fecha_pago DESC
                    LIMIT %s OFFSET %s
                    """,
                    (int(usuario_id), lim, off)
                )
            rows = []
            for r in cur.fetchall():
                rows.append({
                    "fecha": str(r[0]) if r[0] is not None else None,
                    "monto": float(r[1] or 0),
                    "tipo_cuota": r[2],
                })
        return rows
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)

@app.get("/api/usuario_asistencias")
async def api_usuario_asistencias(request: Request, _=Depends(require_owner)):
    """Lista de asistencias reales de un usuario con soporte de búsqueda y paginación."""
    db = _get_db()
    if db is None:
        return []
    try:
        usuario_id = request.query_params.get("usuario_id")
        q = request.query_params.get("q")
        limit = request.query_params.get("limit")
        offset = request.query_params.get("offset")
        if not usuario_id:
            return []
        lim = int(limit) if limit and limit.isdigit() else 50
        off = int(offset) if offset and offset.isdigit() else 0
        with db.get_connection_context() as conn:  # type: ignore
            cur = conn.cursor()
            if q:
                cur.execute(
                    """
                    SELECT fecha::date, hora_registro
                    FROM asistencias
                    WHERE usuario_id = %s AND (CAST(hora_registro AS TEXT) ILIKE %s)
                    ORDER BY fecha DESC
                    LIMIT %s OFFSET %s
                    """,
                    (int(usuario_id), f"%{q}%", lim, off)
                )
            else:
                cur.execute(
                    """
                    SELECT fecha::date, hora_registro
                    FROM asistencias
                    WHERE usuario_id = %s
                    ORDER BY fecha DESC
                    LIMIT %s OFFSET %s
                    """,
                    (int(usuario_id), lim, off)
                )
            rows = []
            for r in cur.fetchall():
                rows.append({
                    "fecha": str(r[0]) if r[0] is not None else None,
                    "hora": str(r[1]) if r[1] is not None else None,
                })
        return rows
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)

@app.get("/api/asistencias_detalle")
async def api_asistencias_detalle(request: Request, _=Depends(require_owner)):
    """Listado de asistencias con nombre del usuario para un rango de fechas (por defecto últimos 30 días), con búsqueda y paginación."""
    db = _get_db()
    if db is None:
        return []
    try:
        start = request.query_params.get("start")
        end = request.query_params.get("end")
        q = request.query_params.get("q")
        limit = request.query_params.get("limit")
        offset = request.query_params.get("offset")
        lim = int(limit) if limit and limit.isdigit() else 500
        off = int(offset) if offset and offset.isdigit() else 0
        with db.get_connection_context() as conn:  # type: ignore
            cur = conn.cursor()
            if start and end:
                if q:
                    cur.execute(
                        """
                        SELECT a.fecha::date, a.hora_registro, u.nombre
                        FROM asistencias a
                        JOIN usuarios u ON u.id = a.usuario_id
                        WHERE a.fecha BETWEEN %s AND %s AND (u.nombre ILIKE %s)
                        ORDER BY a.fecha DESC, a.hora_registro DESC
                        LIMIT %s OFFSET %s
                        """,
                        (start, end, f"%{q}%", lim, off)
                    )
                else:
                    cur.execute(
                        """
                        SELECT a.fecha::date, a.hora_registro, u.nombre
                        FROM asistencias a
                        JOIN usuarios u ON u.id = a.usuario_id
                        WHERE a.fecha BETWEEN %s AND %s
                        ORDER BY a.fecha DESC, a.hora_registro DESC
                        LIMIT %s OFFSET %s
                        """,
                        (start, end, lim, off)
                    )
            else:
                if q:
                    cur.execute(
                        """
                        SELECT a.fecha::date, a.hora_registro, u.nombre
                        FROM asistencias a
                        JOIN usuarios u ON u.id = a.usuario_id
                        WHERE a.fecha >= CURRENT_DATE - INTERVAL '30 days' AND (u.nombre ILIKE %s)
                        ORDER BY a.fecha DESC, a.hora_registro DESC
                        LIMIT %s OFFSET %s
                        """,
                        (f"%{q}%", lim, off)
                    )
                else:
                    cur.execute(
                        """
                        SELECT a.fecha::date, a.hora_registro, u.nombre
                        FROM asistencias a
                        JOIN usuarios u ON u.id = a.usuario_id
                        WHERE a.fecha >= CURRENT_DATE - INTERVAL '30 days'
                        ORDER BY a.fecha DESC, a.hora_registro DESC
                        LIMIT %s OFFSET %s
                        """,
                        (lim, off)
                    )
        rows = []
        for r in cur.fetchall():
            rows.append({
                "fecha": str(r[0]) if r[0] is not None else None,
                "hora": str(r[1]) if r[1] is not None else None,
                "usuario": r[2] or ''
            })
        return rows
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)


@app.get("/api/export")
async def api_export(_=Depends(require_owner)):
    # Exporta CSV de KPIs visibles en el dashboard
    rows: list[list[Any]] = [["Métrica", "Valor"]]
    try:
        kpis = await api_kpis()  # type: ignore
        if isinstance(kpis, dict) and 'kpis' in kpis:
            rows.append(["Socios activos", kpis['kpis'].get('total_activos', 0)])
            rows.append(["Ingresos del mes", kpis['kpis'].get('ingresos_mes_actual', 0.0)])
            rows.append(["Asistencias hoy", kpis['kpis'].get('asistencias_hoy', 0)])
            rows.append(["Nuevos (30d)", kpis['kpis'].get('nuevos_30_dias', 0)])
            rows.append(["ARPU (mes)", kpis.get('arpu', 0.0)])
            rows.append(["Morosos", kpis.get('morosos', 0)])
    except Exception:
        pass

    # Generar CSV en memoria y devolver como texto
    from io import StringIO
    buf = StringIO()
    w = csv.writer(buf)
    w.writerows(rows)
    buf.seek(0)
    return JSONResponse({"csv": buf.getvalue()})

# --- Exportación completa en ZIP con múltiples CSVs ---
@app.get("/api/export_csv")
async def api_export_csv(request: Request, _=Depends(require_owner)):
    """Genera un ZIP con múltiples CSVs del dashboard aplicando filtros actuales.

    Incluye:
    - kpis.csv: KPIs generales
    - usuarios.csv: detalle de usuarios con conteos
    - asistencias.csv: asistencias por rango (start/end)
    - pagos.csv: pagos por rango (start/end)
    - profesores.csv: detalle de profesores con sesiones/horas del rango
    - profesor_sesiones.csv: sesiones trabajadas por profesores en el rango
    """
    db = _get_db()
    if db is None:
        return JSONResponse({"error": "DB no disponible"}, status_code=500)

    start = request.query_params.get("start")
    end = request.query_params.get("end")
    prof_start = request.query_params.get("prof_start") or start
    prof_end = request.query_params.get("prof_end") or end

    from io import StringIO, BytesIO
    import zipfile

    def write_csv_to_zip(zf: zipfile.ZipFile, fname: str, headers: list[str], rows: list[list]):
        buf = StringIO()
        w = csv.writer(buf)
        if headers:
            w.writerow(headers)
        for r in rows:
            w.writerow(r)
        zf.writestr(fname, buf.getvalue())

    zip_buf = BytesIO()
    zf = zipfile.ZipFile(zip_buf, mode="w", compression=zipfile.ZIP_DEFLATED)

    # 1) KPIs
    try:
        k = await api_kpis()  # type: ignore
        kpis_rows = [["Métrica", "Valor"]]
        if isinstance(k, dict) and 'kpis' in k:
            kpis_rows += [
                ["Socios activos", k['kpis'].get('total_activos', 0)],
                ["Ingresos del mes", k['kpis'].get('ingresos_mes_actual', 0.0)],
                ["Asistencias hoy", k['kpis'].get('asistencias_hoy', 0)],
                ["Nuevos (30d)", k['kpis'].get('nuevos_30_dias', 0)],
                ["ARPU (mes)", k.get('arpu', 0.0)],
                ["Morosos", k.get('morosos', 0)],
            ]
        write_csv_to_zip(zf, "kpis.csv", [], kpis_rows)
    except Exception:
        write_csv_to_zip(zf, "kpis.csv", [], [["Error obteniendo KPIs"]])

    # 2) Usuarios
    try:
        with db.get_connection_context() as conn:  # type: ignore
            cur = conn.cursor()
            cur.execute(
                """
                SELECT u.id,
                       COALESCE(u.nombre,'') AS nombre,
                       COALESCE(u.rol,'') AS rol,
                       COALESCE(u.dni,'') AS dni,
                       COALESCE(u.telefono,'') AS telefono,
                       COALESCE(u.tipo_cuota,'') AS tipo_cuota,
                       COALESCE(u.activo,false) AS activo,
                       COALESCE(u.fecha_registro::date, CURRENT_DATE) AS fecha_registro,
                       COALESCE(p.pagos_count,0) AS pagos_count,
                       p.last_pago_fecha::date AS last_pago_fecha,
                       COALESCE(a.asistencias_count,0) AS asistencias_count
                FROM usuarios u
                LEFT JOIN (
                    SELECT usuario_id, COUNT(*) AS pagos_count, MAX(fecha_pago) AS last_pago_fecha
                    FROM pagos
                    GROUP BY usuario_id
                ) p ON p.usuario_id = u.id
                LEFT JOIN (
                    SELECT usuario_id, COUNT(*) AS asistencias_count
                    FROM asistencias
                    GROUP BY usuario_id
                ) a ON a.usuario_id = u.id
                ORDER BY u.id
                """
            )
            urows = []
            for r in cur.fetchall():
                urows.append([
                    r[0], r[1], r[2], r[3], r[4], r[5], bool(r[6]),
                    str(r[7]) if r[7] is not None else '',
                    int(r[8] or 0),
                    str(r[9]) if r[9] is not None else '',
                    int(r[10] or 0),
                ])
        write_csv_to_zip(zf, "usuarios.csv", [
            "id","nombre","rol","dni","telefono","tipo_cuota","activo","fecha_registro","pagos_count","last_pago_fecha","asistencias_count"
        ], urows)
    except Exception:
        write_csv_to_zip(zf, "usuarios.csv", [], [["Error exportando usuarios"]])

    # 3) Asistencias por rango
    try:
        with db.get_connection_context() as conn:  # type: ignore
            cur = conn.cursor()
            if start and end:
                cur.execute(
                    """
                    SELECT a.fecha::date, a.hora_registro, u.nombre
                    FROM asistencias a
                    JOIN usuarios u ON u.id = a.usuario_id
                    WHERE a.fecha BETWEEN %s AND %s
                    ORDER BY a.fecha DESC, a.hora_registro DESC
                    """,
                    (start, end)
                )
            else:
                # Últimos 30 días por defecto
                cur.execute(
                    """
                    SELECT a.fecha::date, a.hora_registro, u.nombre
                    FROM asistencias a
                    JOIN usuarios u ON u.id = a.usuario_id
                    WHERE a.fecha >= CURRENT_DATE - INTERVAL '30 day'
                    ORDER BY a.fecha DESC, a.hora_registro DESC
                    """
                )
            arows = [[str(r[0]) if r[0] is not None else '', str(r[1]) if r[1] is not None else '', r[2] or ''] for r in cur.fetchall()]
        write_csv_to_zip(zf, "asistencias.csv", ["fecha","hora","usuario"], arows)
    except Exception:
        write_csv_to_zip(zf, "asistencias.csv", [], [["Error exportando asistencias"]])

    # 4) Pagos por rango
    try:
        with db.get_connection_context() as conn:  # type: ignore
            cur = conn.cursor()
            if start and end:
                cur.execute(
                    """
                    SELECT p.usuario_id, u.nombre, p.fecha_pago::date, p.monto, COALESCE(u.tipo_cuota,'')
                    FROM pagos p
                    JOIN usuarios u ON u.id = p.usuario_id
                    WHERE p.fecha_pago BETWEEN %s AND %s
                    ORDER BY p.fecha_pago DESC
                    """,
                    (start, end)
                )
            else:
                cur.execute(
                    """
                    SELECT p.usuario_id, u.nombre, p.fecha_pago::date, p.monto, COALESCE(u.tipo_cuota,'')
                    FROM pagos p
                    JOIN usuarios u ON u.id = p.usuario_id
                    WHERE p.fecha_pago >= CURRENT_DATE - INTERVAL '30 day'
                    ORDER BY p.fecha_pago DESC
                    """
                )
            prows = []
            for r in cur.fetchall():
                prows.append([
                    int(r[0] or 0), r[1] or '', str(r[2]) if r[2] is not None else '', float(r[3] or 0.0), r[4] or ''
                ])
        write_csv_to_zip(zf, "pagos.csv", ["usuario_id","usuario","fecha_pago","monto","tipo_cuota"], prows)
    except Exception:
        write_csv_to_zip(zf, "pagos.csv", [], [["Error exportando pagos"]])

    # 5) Profesores detalle
    try:
        prof_rows = []
        detalle = await api_profesores_detalle(request)  # type: ignore
        if isinstance(detalle, list):
            for p in detalle:
                # Resumen de horarios como texto
                horarios = p.get("horarios") or []
                resumen = "; ".join([
                    f"{h.get('dia')}: {h.get('inicio')}-{h.get('fin')}" for h in horarios if isinstance(h, dict)
                ])
                prof_rows.append([
                    p.get("id"), p.get("nombre"), p.get("telefono"), int(p.get("horarios_count", 0)), resumen,
                    int(p.get("sesiones_mes", 0)), float(p.get("horas_mes", 0.0))
                ])
        write_csv_to_zip(zf, "profesores.csv", ["id","nombre","telefono","horarios_count","horarios_resumen","sesiones_rango","horas_rango"], prof_rows)
    except Exception:
        write_csv_to_zip(zf, "profesores.csv", [], [["Error exportando profesores"]])

    # 6) Sesiones de profesores por rango
    try:
        with db.get_connection_context() as conn:  # type: ignore
            cur = conn.cursor()
            if prof_start and prof_end:
                cur.execute(
                    """
                    SELECT profesor_id,
                           fecha::date,
                           hora_inicio,
                           hora_fin,
                           COALESCE(minutos_totales,0) AS minutos,
                           COALESCE(horas_totales,0) AS horas,
                           COALESCE(tipo_actividad,'') AS tipo
                    FROM profesor_horas_trabajadas
                    WHERE hora_fin IS NOT NULL AND fecha BETWEEN %s AND %s
                    ORDER BY fecha DESC, profesor_id
                    """,
                    (prof_start, prof_end)
                )
            else:
                # Mes actual por defecto
                from datetime import datetime, date, timedelta
                now = datetime.now()
                start_date = date(now.year, now.month, 1)
                if now.month == 12:
                    end_date = date(now.year, 12, 31)
                else:
                    end_date = date(now.year, now.month + 1, 1) - timedelta(days=1)
                cur.execute(
                    """
                    SELECT profesor_id,
                           fecha::date,
                           hora_inicio,
                           hora_fin,
                           COALESCE(minutos_totales,0) AS minutos,
                           COALESCE(horas_totales,0) AS horas,
                           COALESCE(tipo_actividad,'') AS tipo
                    FROM profesor_horas_trabajadas
                    WHERE hora_fin IS NOT NULL AND fecha BETWEEN %s AND %s
                    ORDER BY fecha DESC, profesor_id
                    """,
                    (start_date, end_date)
                )
            srows = []
            for r in cur.fetchall():
                srows.append([
                    int(r[0] or 0), str(r[1]) if r[1] is not None else '', str(r[2]) if r[2] is not None else '', str(r[3]) if r[3] is not None else '',
                    int(r[4] or 0), float(r[5] or 0.0), r[6] or ''
                ])
        write_csv_to_zip(zf, "profesor_sesiones.csv", ["profesor_id","fecha","hora_inicio","hora_fin","minutos","horas","tipo"], srows)
    except Exception:
        write_csv_to_zip(zf, "profesor_sesiones.csv", [], [["Error exportando sesiones de profesores"]])

    # Finalizar ZIP y devolver
    zf.close()
    zip_buf.seek(0)
    return Response(content=zip_buf.getvalue(), media_type="application/zip", headers={
        "Content-Disposition": "attachment; filename=dashboard_export.zip"
    })
@app.get("/api/profesor_resumen")
async def api_profesor_resumen(request: Request, _=Depends(require_owner)):
    """Resumen de horas trabajadas, proyectadas y extras para un profesor.

    - Trabajadas: suma de `minutos_totales` de sesiones cerradas en el rango.
    - Proyectadas: suma de minutos desde `horarios_profesores` (disponibilidades activas)
      por cada día del rango, contando ocurrencias por día de semana. No depende de clases.
    - Extras: trabajadas - proyectadas.
    """
    db = _get_db()
    if db is None:
        return {
            "total_sesiones": 0,
            "minutos_trabajados": 0,
            "horas_trabajadas": 0.0,
            "minutos_proyectados": 0,
            "horas_proyectadas": 0.0,
            "minutos_extras": 0,
            "horas_extras": 0.0,
        }
    try:
        pid = request.query_params.get("profesor_id")
        if not pid:
            return {
                "total_sesiones": 0,
                "minutos_trabajados": 0,
                "horas_trabajadas": 0.0,
                "minutos_proyectados": 0,
                "horas_proyectadas": 0.0,
                "minutos_extras": 0,
                "horas_extras": 0.0,
            }
        try:
            profesor_id = int(pid)
        except Exception:
            return {
                "total_sesiones": 0,
                "minutos_trabajados": 0,
                "horas_trabajadas": 0.0,
                "minutos_proyectados": 0,
                "horas_proyectadas": 0.0,
                "minutos_extras": 0,
                "horas_extras": 0.0,
            }

        start = request.query_params.get("start")
        end = request.query_params.get("end")
        from datetime import datetime, date, timedelta
        now = datetime.now()
        if start and end:
            try:
                start_date = datetime.strptime(start, "%Y-%m-%d").date()
                end_date = datetime.strptime(end, "%Y-%m-%d").date()
            except Exception:
                start_date = date(now.year, now.month, 1)
                # último día del mes
                if now.month == 12:
                    end_date = date(now.year, 12, 31)
                else:
                    end_date = date(now.year, now.month + 1, 1) - timedelta(days=1)
        else:
            start_date = date(now.year, now.month, 1)
            if now.month == 12:
                end_date = date(now.year, 12, 31)
            else:
                end_date = date(now.year, now.month + 1, 1) - timedelta(days=1)

        with db.get_connection_context() as conn:  # type: ignore
            cur = conn.cursor()

            # 1) Minutos trabajados y cantidad de sesiones cerradas
            cur.execute(
                """
                SELECT COALESCE(SUM(pht.minutos_totales),0) AS min_trab,
                       COUNT(*) AS total_ses
                FROM profesor_horas_trabajadas pht
                WHERE pht.profesor_id = %s
                  AND pht.hora_fin IS NOT NULL
                  AND pht.fecha BETWEEN %s AND %s
                """,
                (profesor_id, start_date, end_date)
            )
            r = cur.fetchone() or (0, 0)
            min_trabajados = int(r[0] or 0)
            total_sesiones = int(r[1] or 0)

        # 2) Minutos proyectados: cálculo centralizado en database.py (no depende de clases)
        min_proyectados = 0
        try:
            if hasattr(db, 'obtener_minutos_proyectados_profesor_rango'):
                res = db.obtener_minutos_proyectados_profesor_rango(  # type: ignore
                    profesor_id,
                    start_date.isoformat(),
                    end_date.isoformat()
                )
                if isinstance(res, dict) and res.get('success'):
                    min_proyectados = int(res.get('minutos_proyectados', 0) or 0)
        except Exception:
            min_proyectados = 0

        return {
            "total_sesiones": total_sesiones,
            "minutos_trabajados": min_trabajados,
            "horas_trabajadas": round(min_trabajados / 60.0, 2),
            "minutos_proyectados": int(min_proyectados),
            "horas_proyectadas": round(min_proyectados / 60.0, 2),
            "minutos_extras": int(min_trabajados - min_proyectados),
            "horas_extras": round((min_trabajados - min_proyectados) / 60.0, 2),
        }
    except Exception as e:
        import traceback
        traceback.print_exc()
        return {
            "total_sesiones": 0,
            "minutos_trabajados": 0,
            "horas_trabajadas": 0.0,
            "minutos_proyectados": 0,
            "horas_proyectadas": 0.0,
            "minutos_extras": 0,
            "horas_extras": 0.0,
        }