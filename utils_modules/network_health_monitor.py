import threading
import time
import logging
from typing import Optional, Callable, Dict
from utils import build_public_url, get_tunnel_provider, terminate_tunnel_processes

# HTTP client (requests) con fallback mínimo
try:
    import requests  # type: ignore
except Exception:
    requests = None  # type: ignore

def _http_status_ok(url: str, timeout: float) -> bool:
    """Realiza una petición GET al `url` y retorna True si responde 2xx.
    Implementa fallback si `requests` no está disponible.
    """
    try:
        if requests is not None:
            r = requests.get(url, timeout=timeout)
            return 200 <= r.status_code < 300
        # Fallback: usar urllib
        try:
            import urllib.request
            with urllib.request.urlopen(url, timeout=timeout) as resp:
                code = getattr(resp, 'status', 200)
                return 200 <= int(code) < 300
        except Exception:
            return False
    except Exception:
        return False

def _tcp_port_open(host: str, port: int, timeout: float) -> bool:
    """Comprueba si un puerto TCP está abierto (fallback ultra-ligero para salud local)."""
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

class NetworkHealthMonitor:
    """Monitor de salud de redes (local y pública) con reinicios controlados.

    - Realiza probes periódicos a `http://127.0.0.1:<port>/healthz` y a la URL pública proporcionada (por defecto LocalTunnel).
    - Aplica umbral de fallos consecutivos antes de reiniciar.
    - Permite callbacks de reinicio para servidor y túnel.
    - Evita spam con un backoff simple y espera tras reinicios.
    """

    def __init__(
        self,
        host: str,
        port: int,
        subdomain: Optional[str] = None,
        public_url: Optional[str] = None,
        restart_server_cb: Optional[Callable[[], None]] = None,
        restart_tunnel_cb: Optional[Callable[[], None]] = None,
        check_interval_local: float = 5.0,
        check_interval_public: float = 15.0,
        timeout_local: float = 1.5,
        timeout_public: float = 2.0,
        failure_threshold: int = 2,
    ):
        self.host = host
        self.port = int(port)
        self.subdomain = subdomain
        self.public_url = public_url
        self.restart_server_cb = restart_server_cb
        self.restart_tunnel_cb = restart_tunnel_cb
        self.check_interval_local = check_interval_local
        self.check_interval_public = check_interval_public
        self.timeout_local = timeout_local
        self.timeout_public = timeout_public
        self.failure_threshold = max(1, int(failure_threshold))

        self._local_failures = 0
        self._public_failures = 0
        self._stop_evt = threading.Event()
        self._thr: Optional[threading.Thread] = None

    def start(self) -> None:
        if self._thr and self._thr.is_alive():
            return
        self._stop_evt.clear()
        self._thr = threading.Thread(target=self._run, daemon=True)
        self._thr.start()

    def stop(self) -> None:
        try:
            self._stop_evt.set()
        except Exception:
            pass

    def _run(self) -> None:
        last_local_check = 0.0
        last_public_check = 0.0
        while not self._stop_evt.is_set():
            now = time.time()

            # Probar salud local con intervalo fijo
            try:
                if now - last_local_check >= self.check_interval_local:
                    last_local_check = now
                    url_local = f"http://127.0.0.1:{self.port}/healthz"
                    ok = _http_status_ok(url_local, self.timeout_local)
                    if not ok:
                        # Fallback adicional: comprobar puerto escuchando
                        ok = _tcp_port_open("127.0.0.1", self.port, self.timeout_local)
                    if ok:
                        self._local_failures = 0
                    else:
                        self._local_failures += 1
                        if self._local_failures >= self.failure_threshold:
                            # Reiniciar servidor si hay callback
                            try:
                                if self.restart_server_cb:
                                    logging.info("Reiniciando servidor local por fallo de salud")
                                    self.restart_server_cb()
                                    time.sleep(2.0)
                            except Exception:
                                logging.warning("Fallo al reiniciar servidor local")
                            self._local_failures = 0
            except Exception:
                # No romper el bucle de monitor por errores puntuales
                pass

            # Probar salud pública si hay URL o subdominio
            try:
                if (self.public_url or self.subdomain) and (now - last_public_check >= self.check_interval_public):
                    last_public_check = now
                    url_public = None
                    try:
                        if self.public_url:
                            url_public = (self.public_url.rstrip('/') + '/healthz')
                        elif self.subdomain:
                            url_public = build_public_url(self.subdomain, "/healthz")
                    except Exception:
                        url_public = None
                    if not url_public:
                        # Si no podemos construir URL pública, saltar check
                        raise Exception("URL pública no disponible")
                    ok_pub = _http_status_ok(url_public, self.timeout_public)
                    if ok_pub:
                        self._public_failures = 0
                    else:
                        self._public_failures += 1
                        if self._public_failures >= self.failure_threshold:
                            # Terminar túneles previos y reiniciar
                            try:
                                terminate_tunnel_processes()
                            except Exception:
                                pass
                            try:
                                if self.restart_tunnel_cb:
                                    logging.info("Reiniciando túnel público por fallo de salud")
                                    self.restart_tunnel_cb()
                                    time.sleep(2.0)
                            except Exception:
                                logging.warning("Fallo al reiniciar túnel público")
                            self._public_failures = 0
            except Exception:
                pass

            # Pequeña espera para no saturar CPU
            try:
                time.sleep(0.2)
            except Exception:
                pass


def start_network_health_monitor(
    host: str,
    port: int,
    subdomain: Optional[str] = None,
    public_url: Optional[str] = None,
    restart_server_cb: Optional[Callable[[], None]] = None,
    restart_tunnel_cb: Optional[Callable[[], None]] = None,
) -> NetworkHealthMonitor:
    m = NetworkHealthMonitor(
        host=host,
        port=port,
        subdomain=subdomain,
        restart_server_cb=restart_server_cb,
        restart_tunnel_cb=restart_tunnel_cb,
        public_url=public_url,
    )
    m.start()
    return m


def stop_network_health_monitor(monitor: Optional[NetworkHealthMonitor]) -> None:
    try:
        if monitor:
            monitor.stop()
    except Exception:
        pass


def test_networks_and_restart(
    host: str,
    port: int,
    subdomain: Optional[str] = None,
    public_url: Optional[str] = None,
    restart_server_cb: Optional[Callable[[], None]] = None,
    restart_tunnel_cb: Optional[Callable[[], None]] = None,
) -> Dict[str, bool]:
    """Testea salud de red local y pública, intentando reiniciar si fallan.

    Retorna dict con claves: local_ok, public_ok, server_restarted, tunnel_restarted.
    """
    result = {
        "local_ok": False,
        "public_ok": False,
        "server_restarted": False,
        "tunnel_restarted": False,
    }

    # Salud local
    try:
        url_local = f"http://127.0.0.1:{int(port)}/healthz"
        ok_local = _http_status_ok(url_local, 1.5) or _tcp_port_open("127.0.0.1", int(port), 1.5)
        result["local_ok"] = bool(ok_local)
        if not ok_local and restart_server_cb:
            try:
                restart_server_cb()
                time.sleep(1.0)
                # Revalidar
                result["local_ok"] = _http_status_ok(url_local, 1.5) or _tcp_port_open("127.0.0.1", int(port), 1.5)
                result["server_restarted"] = True
            except Exception:
                pass
    except Exception:
        pass

    # Salud pública
    try:
        if public_url or subdomain:
            url_public = None
            try:
                if public_url:
                    url_public = (public_url.rstrip('/') + '/healthz')
                elif subdomain:
                    url_public = build_public_url(subdomain, "/healthz")
            except Exception:
                url_public = None
            if not url_public:
                raise Exception("URL pública no disponible")
            ok_pub = _http_status_ok(url_public, 2.0)
            result["public_ok"] = bool(ok_pub)
            if not ok_pub:
                try:
                    terminate_tunnel_processes()
                except Exception:
                    pass
                if restart_tunnel_cb:
                    try:
                        restart_tunnel_cb()
                        time.sleep(2.0)
                        result["public_ok"] = _http_status_ok(url_public, 2.0)
                        result["tunnel_restarted"] = True
                    except Exception:
                        pass
    except Exception:
        pass

    return result