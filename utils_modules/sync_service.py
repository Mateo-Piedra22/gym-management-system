# -*- coding: utf-8 -*-
"""
SyncService — Servicio de observabilidad para replicación nativa PostgreSQL.

Objetivo:
- Monitorear el estado de la replicación lógica nativa de PostgreSQL.
- Notificar cambios en el estado de conectividad.
- Refrescar indicadores de sincronización.

Notas:
- La replicación de datos se maneja por PostgreSQL nativo (publications/subscriptions).
- Este servicio NO envía datos; sólo observa el estado para mejorar la UX.
"""

from typing import Callable, Optional
import time
import threading
import logging

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

            def stop(self):
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
    """Servicio que observa el estado de replicación nativa PostgreSQL."""

    def __init__(
        self,
        poll_interval_ms: int = 5000,
        db_manager: Optional[object] = None,
    ):
        super().__init__()
        self.poll_interval_ms = int(poll_interval_ms)
        self._timer: Optional[QTimer] = None
        self._last_replication_status = None
        
        # Callbacks configurables desde la UI (MainWindow)
        self.on_replication_status_change: Optional[Callable[[dict], None]] = None
        self.on_connectivity_change: Optional[Callable[[bool], None]] = None
        
        # Rastrear hilos en background para parada ordenada
        self._bg_threads: list[threading.Thread] = []
        
        # Logger
        self.logger = logging.getLogger(__name__)
        self.logger.info("SyncService iniciado con replicación nativa PostgreSQL")

    def start(self):
        """Inicia el servicio de monitoreo."""
        if self._timer is not None:
            self.logger.warning("SyncService ya está iniciado")
            return
        
        self._timer = QTimer()
        self._timer.timeout.connect(self._check_replication_status)  # type: ignore
        self._timer.start(self.poll_interval_ms)
        self.logger.info(f"SyncService iniciado con intervalo {self.poll_interval_ms}ms")

    def stop(self):
        """Detiene el servicio de monitoreo."""
        if self._timer is not None:
            self._timer.stop()
            self._timer = None
            self.logger.info("SyncService detenido")
        
        # Esperar a que terminen hilos en background
        for th in self._bg_threads:
            if th.is_alive():
                th.join(timeout=2.0)
        self._bg_threads.clear()

    def _check_replication_status(self):
        """Verifica el estado de la replicación nativa PostgreSQL."""
        try:
            status = self._get_replication_status()
            
            # Notificar cambios en estado de replicación
            if status != self._last_replication_status:
                self._last_replication_status = status
                if self.on_replication_status_change:
                    self.on_replication_status_change(status)
            
            # Notificar cambios en conectividad
            is_connected = status.get('is_connected', False)
            if self.on_connectivity_change:
                self.on_connectivity_change(is_connected)
                
        except Exception as e:
            self.logger.error(f"Error verificando estado de replicación: {e}")
            # Notificar pérdida de conectividad
            if self.on_connectivity_change:
                self.on_connectivity_change(False)

    def _get_replication_status(self) -> dict:
        """Obtiene el estado actual de la replicación nativa PostgreSQL."""
        try:
            import psycopg2
            from secure_config import config as secure_config
            
            local_config = secure_config.get_db_config('local')
            
            with psycopg2.connect(**local_config) as conn:
                with conn.cursor() as cur:
                    # Verificar publicación
                    cur.execute("""
                        SELECT pubname, puballtables 
                        FROM pg_publication 
                        WHERE pubname = 'gym_pub'
                    """)
                    pub_result = cur.fetchone()
                    
                    # Verificar suscripción
                    cur.execute("""
                        SELECT subname, subenabled
                        FROM pg_subscription
                        WHERE subname = 'gym_sub'
                    """)
                    sub_result = cur.fetchone()
                    
                    # Verificar workers de replicación
                    cur.execute("""
                        SELECT COUNT(*) as worker_count
                        FROM pg_stat_replication
                    """)
                    worker_count = cur.fetchone()[0]
                    
                    return {
                        'is_connected': True,
                        'publication_active': bool(pub_result),
                        'subscription_active': bool(sub_result) and (sub_result[1] if sub_result else False),
                        'replication_workers': worker_count,
                        'timestamp': time.time()
                    }
                    
        except Exception as e:
            self.logger.error(f"Error obteniendo estado de replicación: {e}")
            return {
                'is_connected': False,
                'error': str(e),
                'timestamp': time.time()
            }

    def get_status_summary(self) -> dict:
        """Obtiene un resumen del estado actual."""
        return {
            'service_running': self._timer is not None,
            'last_status': self._last_replication_status,
            'bg_threads_active': len([th for th in self._bg_threads if th.is_alive()])
        }

    def __del__(self):
        """Destructor - asegura detener el servicio."""
        try:
            self.stop()
        except Exception:
            pass