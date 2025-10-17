# -*- coding: utf-8 -*-
"""
ReplicationObserver — Observador ligero de replicación PostgreSQL.

Objetivo:
- Consultar periódicamente `pg_stat_subscription` para detectar progreso de replicación entrante.
- Disparar callbacks de UI cuando se apliquen nuevos cambios (avance en latest_end_time o receipt_time).
- Reportar métricas básicas de estado (apply_lag, sync_state) para indicadores.

Notas:
- Usa QTimer si PyQt está disponible; de lo contrario, expone métodos vacíos para no romper importaciones.
- No envía datos ni modifica replicación; solo observa.
"""

from typing import Optional, Callable, Dict, Any

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


class ReplicationObserver(QObject):
    """Observa `pg_stat_subscription` y notifica cuando hay cambios entrantes."""

    def __init__(self, db_manager, poll_interval_ms: int = 5000):
        super().__init__()
        self.db_manager = db_manager
        self.poll_interval_ms = int(poll_interval_ms)
        self._timer: Optional[QTimer] = None
        self._last_latest_end_time = None
        self._last_receipt_time = None
        # Callbacks configurables desde la UI (MainWindow)
        self.on_status_update: Optional[Callable[[Dict[str, Any]], None]] = None
        self.on_inbound_change: Optional[Callable[[], None]] = None

    def start(self):
        """Inicia el polling de las métricas de suscripción."""
        try:
            if self._timer is None:
                self._timer = QTimer()
                self._timer.timeout.connect(self._tick)  # type: ignore
            self._timer.start(self.poll_interval_ms)
            # Primer tick inmediato para reflejar estado actual
            self._tick()
        except Exception:
            # No bloquear si Qt no está disponible o falla el timer
            pass

    def stop(self):
        """Detiene el observador y limpia estado interno."""
        try:
            if self._timer:
                try:
                    self._timer.stop()
                except Exception:
                    pass
                self._timer = None
        finally:
            self._last_latest_end_time = None
            self._last_receipt_time = None

    def _tick(self):
        """Consulta `pg_stat_subscription` y dispara callbacks si hay cambios."""
        try:
            metrics = self._read_subscription_metrics()
            # Reportar estado actualizado
            if self.on_status_update:
                try:
                    self.on_status_update(metrics)
                except Exception:
                    pass

            # Detectar avance de replicación entrante
            changed = False
            lt = metrics.get("max_latest_end_time")
            rt = metrics.get("max_last_msg_receipt_time")
            if lt is not None:
                if (self._last_latest_end_time is None) or (lt > self._last_latest_end_time):
                    self._last_latest_end_time = lt
                    changed = True
            if rt is not None:
                if (self._last_receipt_time is None) or (rt > self._last_receipt_time):
                    self._last_receipt_time = rt
                    changed = True

            if changed and self.on_inbound_change:
                try:
                    self.on_inbound_change()
                except Exception:
                    pass
        except Exception:
            # Resiliencia: nunca romper el hilo de UI por errores del observador
            pass

    def _read_subscription_metrics(self) -> Dict[str, Any]:
        """Lee métricas de `pg_stat_subscription` con fallbacks de compatibilidad."""
        metrics: Dict[str, Any] = {
            "row_count": 0,
            "has_subscription": False,
            "max_apply_lag_s": None,
            "max_latest_end_time": None,
            "max_last_msg_receipt_time": None,
            "sync_states": [],
        }
        try:
            with self.db_manager.get_connection_context() as conn:  # type: ignore
                cur = conn.cursor()
                # Intentar consulta completa (columnas modernas)
                try:
                    cur.execute(
                        """
                        SELECT subname,
                               COALESCE(EXTRACT(EPOCH FROM apply_lag), 0) AS apply_lag_s,
                               latest_end_time,
                               last_msg_receipt_time,
                               last_msg_send_time,
                               sync_state
                        FROM pg_stat_subscription
                        """
                    )
                except Exception:
                    # Fallback: sin columnas last_msg_*
                    cur.execute(
                        """
                        SELECT subname,
                               COALESCE(EXTRACT(EPOCH FROM apply_lag), 0) AS apply_lag_s,
                               latest_end_time,
                               NULL::timestamp AS last_msg_receipt_time,
                               NULL::timestamp AS last_msg_send_time,
                               sync_state
                        FROM pg_stat_subscription
                        """
                    )
                rows = cur.fetchall() or []

            metrics["row_count"] = len(rows)
            metrics["has_subscription"] = bool(len(rows) > 0)
            max_lag = None
            max_end = None
            max_rec = None
            sync_states = []
            for r in rows:
                try:
                    apply_lag_s = (float(r[1]) if r[1] is not None else None)
                except Exception:
                    apply_lag_s = None
                let = r[2] if len(r) > 2 else None
                lmr = r[3] if len(r) > 3 else None
                sync_state = r[5] if len(r) > 5 else None
                if apply_lag_s is not None:
                    if (max_lag is None) or (apply_lag_s > max_lag):
                        max_lag = apply_lag_s
                if let is not None:
                    if (max_end is None) or (let > max_end):
                        max_end = let
                if lmr is not None:
                    if (max_rec is None) or (lmr > max_rec):
                        max_rec = lmr
                if sync_state:
                    sync_states.append(str(sync_state))
            metrics["max_apply_lag_s"] = max_lag
            metrics["max_latest_end_time"] = max_end
            metrics["max_last_msg_receipt_time"] = max_rec
            metrics["sync_states"] = sync_states
        except Exception:
            # Mantener valores por defecto si falla la consulta
            pass
        return metrics