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

try:
    from PyQt5.QtCore import QObject, QTimer
except Exception:  # pragma: no cover - entorno alternativo/packaging
    # Fallback mínimo para evitar romper importaciones en entornos sin Qt
    class QObject:  # type: ignore
        pass
    class QTimer:   # type: ignore
        def __init__(self):
            self._interval = 0
            self._cb = None
        def timeout(self):
            return None
        def start(self, *_):
            pass
        def stop(self):
            pass


class SyncService(QObject):
    """Servicio que observa la cola del `sync_client` y notifica cambios."""

    def __init__(self, poll_interval_ms: int = 3000, db_manager: Optional[object] = None):
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

    def start(self):
        """Inicia el polling de la cola de operaciones."""
        try:
            if self._timer is None:
                self._timer = QTimer()
                self._timer.timeout.connect(self._tick)  # type: ignore
            self._timer.start(self.poll_interval_ms)
            # Primer tick inmediato para reflejar estado actual
            self._tick()
            # Iniciar outbox poller si está disponible
            try:
                if self._outbox_poller:
                    self._outbox_poller.start()  # type: ignore
            except Exception:
                pass
        except Exception:
            # No bloquear si Qt no está disponible o falla el timer
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
                    self._outbox_poller.stop()  # type: ignore
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
        except Exception:
            # Resiliencia: nunca romper el hilo de UI por errores del observador
            pass