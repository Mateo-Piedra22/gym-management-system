# -*- coding: utf-8 -*-
"""
SyncService — Servicio ligero de sincronización/observabilidad.

Objetivo:
- Vigilar la cola persistente del cliente de sincronización (`sync_client.get_pending_count`).
- Disparar callbacks de UI cuando cambie el número de operaciones pendientes.
- Notificar cuando la cola queda vacía para refrescar indicadores.

Notas:
- La replicación de datos se maneja por PostgreSQL (publications/subscriptions).
- Este servicio NO envía datos; sólo observa la cola local para mejorar la UX.
"""

from typing import Callable, Optional
import time
import threading

try:
    # Preferir PyQt6 si está disponible
    from PyQt6.QtCore import QObject, QTimer  # type: ignore
except Exception:  # pragma: no cover - entorno alternativo/packaging
    try:
        # Fallback a PyQt5
        from PyQt5.QtCore import QObject, QTimer  # type: ignore
    except Exception:
        # Fallback mínimo para evitar romper importaciones en entornos sin Qt
        class QObject:  # type: ignore
            pass

        class QTimer:  # type: ignore
            def __init__(self, *args, **kwargs):
                pass

            def start(self, *args, **kwargs):
                pass

            def stop(self, *args, **kwargs):
                pass

            def timeout(self, *args, **kwargs):  # type: ignore
                return None

            @staticmethod
            def singleShot(ms: int, cb):  # type: ignore
                # Fallback: ejecutar en hilo aparte con retardo mínimo
                import threading as _th, time as _t
                def _run():
                    try:
                        if ms > 0:
                            _t.sleep(ms / 1000.0)
                    except Exception:
                        pass
                    try:
                        cb()
                    except Exception:
                        pass
                _th.Thread(target=_run, daemon=True).start()


class SyncService(QObject):
    """Servicio que observa la cola del `sync_client` y notifica cambios."""

    def __init__(
        self,
        poll_interval_ms: int = 3000,
        db_manager: Optional[object] = None,
        auto_upload_on_change: bool = True,
        periodic_upload_interval_ms: int = 60000,
    ):
        super().__init__()
        self.poll_interval_ms = int(poll_interval_ms)
        self._timer: Optional[QTimer] = None
        self._last_pending: Optional[int] = None
        # Callbacks configurables desde la UI (MainWindow)
        self.on_pending_change: Optional[Callable[[int], None]] = None
        self.on_queue_empty: Optional[Callable[[], None]] = None
        # Integración mínima: OutboxPoller para envíos Local→Railway
        self._outbox_poller = None
        try:
            from utils_modules.outbox_poller import OutboxPoller  # type: ignore
            dbm = db_manager
            if dbm is None:
                try:
                    from database import DatabaseManager  # type: ignore
                    dbm = DatabaseManager()
                except Exception:
                    dbm = None
            if dbm is not None:
                self._outbox_poller = OutboxPoller(dbm)
        except Exception:
            self._outbox_poller = None
        self.auto_upload_on_change = bool(auto_upload_on_change)
        self.periodic_upload_interval_ms = int(periodic_upload_interval_ms)
        self._upload_running = False
        self._upload_lock = threading.Lock()
        self._last_upload_ts: Optional[float] = None
        # Rastrear hilos en background para parada ordenada
        self._bg_threads: list[threading.Thread] = []

    def start(self):
        """Inicia el polling de la cola de operaciones."""
        # Intentar iniciar timer de UI si Qt está disponible
        try:
            if self._timer is None:
                self._timer = QTimer()
                try:
                    # Tanto en PyQt5 como en PyQt6, timeout expone signal con connect
                    self._timer.timeout.connect(self._tick)  # type: ignore
                except Exception:
                    pass
            try:
                self._timer.start(self.poll_interval_ms)
            except Exception:
                pass
            # Primer tick inmediato para reflejar estado actual
            try:
                self._tick()
            except Exception:
                pass
        except Exception:
            # No bloquear si Qt no está disponible o falla el timer
            pass
        # Iniciar outbox poller aunque el timer de UI no haya podido iniciar
        try:
            if self._outbox_poller:
                self._outbox_poller.start()  # type: ignore
        except Exception:
            pass

    def stop(self):
        """Detiene el servicio y limpia estado interno."""
        try:
            if self._timer:
                try:
                    self._timer.stop()
                except Exception:
                    pass
                self._timer = None
            # Detener outbox poller si estaba corriendo
            try:
                if self._outbox_poller:
                    # Señalar parada y unir brevemente su hilo interno
                    self._outbox_poller.stop()  # type: ignore
                    try:
                        th = getattr(self._outbox_poller, '_thread', None)
                        if th and th.is_alive():
                            th.join(1.0)
                    except Exception:
                        pass
            except Exception:
                pass
            # Intentar unir hilos de background rápidamente
            try:
                self.wait_for_bg_threads(timeout_s=1.0)
            except Exception:
                pass
        finally:
            self._last_pending = None

    def attach_outbox_status_callback(self, cb: Callable[[dict], None]) -> None:
        """Adjunta callback para estado del OutboxPoller (opcional)."""
        try:
            if self._outbox_poller:
                self._outbox_poller.on_status = cb  # type: ignore
        except Exception:
            pass

    def _maybe_auto_upload(self, pending: int) -> None:
        if pending <= 0:
            return
        # Avoid overlapping runs
        if self._upload_running:
            return
        now = time.time()
        should_run = False
        # Fire on change
        if self.auto_upload_on_change and (self._last_pending is None or int(pending) != int(self._last_pending)):
            should_run = True
        # Also fire periodically if interval elapsed
        elif self.periodic_upload_interval_ms > 0:
            last = self._last_upload_ts or 0.0
            if (now - last) * 1000 >= self.periodic_upload_interval_ms:
                should_run = True
        if not should_run:
            return
        self._launch_uploader_flush()

    def _launch_uploader_flush(self) -> None:
        # Double-check guard under lock
        with self._upload_lock:
            if self._upload_running:
                return
            self._upload_running = True
        
        def _runner():
            try:
                from sync_uploader import SyncUploader
                uploader = SyncUploader()
                uploader.flush_once()
                self._last_upload_ts = time.time()
            except Exception:
                pass
            finally:
                with self._upload_lock:
                    self._upload_running = False
        
        th = threading.Thread(target=_runner, name="SyncUploaderFlush", daemon=True)
        try:
            th.start()
            try:
                self._bg_threads.append(th)
            except Exception:
                pass
        except Exception:
            with self._upload_lock:
                self._upload_running = False

    def _tick(self):
        """Consulta la cola y dispara callbacks si hay cambios."""
        try:
            pending = None
            try:
                from sync_client import get_pending_count  # type: ignore
                pending = int(get_pending_count())
            except Exception:
                pending = None

            # Si no podemos leer la cola, no notificamos cambios
            if pending is None:
                return

            # Notificar cambio de pendientes
            if self._last_pending is None or int(pending) != int(self._last_pending):
                self._last_pending = int(pending)
                if self.on_pending_change:
                    try:
                        self.on_pending_change(int(pending))
                    except Exception:
                        pass

                # Notificación adicional cuando la cola queda vacía
                if int(pending) == 0 and self.on_queue_empty:
                    try:
                        self.on_queue_empty()
                    except Exception:
                        pass
            # After notifying listeners, attempt auto upload
            self._maybe_auto_upload(int(pending))
        except Exception:
            # Resiliencia: nunca romper el hilo de UI por errores del observador
            pass

    # Nuevo: ejecución puntual del OutboxPoller para acelerar subida de triggers
    def flush_outbox_once_bg(self, delay_ms: int = 0) -> None:
        try:
            poller = getattr(self, '_outbox_poller', None)
            if not poller:
                return
            def _run():
                try:
                    poller.flush_once()  # type: ignore
                except Exception:
                    pass
            try:
                # Programar en el hilo de UI el arranque de un hilo real de trabajo
                def _start_thread():
                    th = threading.Thread(target=_run, name="OutboxFlushOnce", daemon=True)
                    try:
                        th.start()
                        try:
                            self._bg_threads.append(th)
                        except Exception:
                            pass
                    except Exception:
                        pass
                QTimer.singleShot(int(delay_ms or 0), _start_thread)
            except Exception:
                # Fallback: lanzar directamente en hilo
                th = threading.Thread(target=_run, name="OutboxFlushOnce", daemon=True)
                try:
                    th.start()
                    try:
                        self._bg_threads.append(th)
                    except Exception:
                        pass
                except Exception:
                    pass
        except Exception:
            pass

    def wait_for_bg_threads(self, timeout_s: float = 1.0) -> None:
        """Espera brevemente a que terminen hilos en background para evitar cuelgues."""
        try:
            for th in list(getattr(self, '_bg_threads', [])):
                try:
                    if th and th.is_alive():
                        th.join(timeout_s)
                except Exception:
                    pass
        except Exception:
            pass