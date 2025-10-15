import logging
import threading
import time
from typing import Optional, Dict

try:
    import requests  # type: ignore
except Exception:  # pragma: no cover
    requests = None  # type: ignore

from .proxy_manager import (
    resolve_local_proxy_base_url,
    ensure_local_proxy_running,
)


class ProxyWatchdog:
    """Watchdog ligero para el proxy local.

    - Verifica periódicamente `healthz` del proxy local y lo relanza si cae.
    - Opcionalmente valida que endpoints `/api/sync/upload` y `/api/sync/download`
      respondan (con OPTIONS/HEAD), registrando tiempos y códigos.
    - No lanza procesos si `LOCAL_PROXY_BASE_URL` apunta a host no local.
    """

    def __init__(
        self,
        interval_sec: float = 60.0,
        verify_endpoints: bool = True,
        timeout_sec: float = 2.5,
        logger: Optional[logging.Logger] = None,
    ):
        self.interval_sec = max(5.0, float(interval_sec))
        self.verify_endpoints = bool(verify_endpoints)
        self.timeout_sec = max(0.5, float(timeout_sec))
        self.log = logger or logging.getLogger(__name__)
        self._stop_evt = threading.Event()
        self._thr: Optional[threading.Thread] = None

    def start(self) -> None:
        if self._thr and self._thr.is_alive():
            return
        self._stop_evt.clear()
        self._thr = threading.Thread(target=self._loop, name="ProxyWatchdog", daemon=True)
        self._thr.start()

    def stop(self) -> None:
        try:
            self._stop_evt.set()
            if self._thr and self._thr.is_alive():
                self._thr.join(timeout=1.5)
        except Exception:
            pass
        finally:
            self._thr = None

    def run_self_test(self) -> Dict[str, bool]:
        """Ejecuta un test inmediato de salud y endpoints.

        Retorna dict con claves: healthy, upload_ok, download_ok.
        """
        base = resolve_local_proxy_base_url().rstrip("/")
        res = {"healthy": False, "upload_ok": False, "download_ok": False}
        try:
            res["healthy"] = self._check_healthz(base)
            if self.verify_endpoints:
                u_ok, d_ok = self._check_optional_endpoints(base)
                res["upload_ok"], res["download_ok"] = u_ok, d_ok
        except Exception:
            pass
        return res

    def _loop(self) -> None:
        # Primer intento: asegurar arranque y validar endpoints una vez
        try:
            ensure_local_proxy_running(wait_seconds=6.0, log=self.log)
            self.run_self_test()
        except Exception:
            pass

        while not self._stop_evt.is_set():
            t0 = time.time()
            try:
                self._tick()
            except Exception:
                pass
            # Esperar el resto del intervalo
            elapsed = max(0.0, time.time() - t0)
            sleep_for = max(1.0, self.interval_sec - elapsed)
            # Despertar pronto si se solicita stop
            if self._stop_evt.wait(timeout=sleep_for):
                break

    def _tick(self) -> None:
        base = resolve_local_proxy_base_url().rstrip("/")
        ok = False
        try:
            ok = self._check_healthz(base)
        except Exception:
            ok = False
        if not ok:
            try:
                self.log.info("Proxy no saludable; intentando relanzar…")
            except Exception:
                pass
            try:
                ensure_local_proxy_running(wait_seconds=6.0, log=self.log)
            except Exception:
                pass
        else:
            if self.verify_endpoints:
                try:
                    self._check_optional_endpoints(base)
                except Exception:
                    pass

    def _check_healthz(self, base: str) -> bool:
        if requests is None:
            return False
        url = base + "/healthz"
        t0 = time.time()
        try:
            r = requests.get(url, timeout=self.timeout_sec)
            dt = (time.time() - t0) * 1000.0
            try:
                self.log.debug(f"healthz {r.status_code} in {dt:.0f}ms")
            except Exception:
                pass
            return r.status_code == 200
        except Exception as e:
            try:
                self.log.debug(f"healthz error: {e}")
            except Exception:
                pass
            return False

    def _check_optional_endpoints(self, base: str) -> tuple[bool, bool]:
        """Valida que los endpoints existan y respondan razonablemente.

        Acepta 200, 204, 405 (Method Not Allowed) como señal de vida del handler.
        """
        if requests is None:
            return False, False
        upload_ok = False
        download_ok = False
        try:
            # Intentar OPTIONS primero (no modifica estado)
            u = base + "/api/sync/upload"
            d = base + "/api/sync/download"
            try:
                ru = requests.options(u, timeout=self.timeout_sec)
                upload_ok = ru.status_code in (200, 204, 405)
            except Exception:
                # Fallback HEAD
                try:
                    ru = requests.head(u, timeout=self.timeout_sec)
                    upload_ok = ru.status_code < 500
                except Exception:
                    upload_ok = False
            try:
                rd = requests.options(d, timeout=self.timeout_sec)
                download_ok = rd.status_code in (200, 204, 405)
            except Exception:
                try:
                    rd = requests.head(d, timeout=self.timeout_sec)
                    download_ok = rd.status_code < 500
                except Exception:
                    download_ok = False
        except Exception:
            upload_ok = False
            download_ok = False
        return upload_ok, download_ok