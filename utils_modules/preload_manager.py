import logging
import threading
from datetime import datetime


class PreloadManager:
    """Gestor de precarga ligera para acelerar la primera interacción.

    Realiza warm-up de caches y consultas de solo lectura en segundo plano
    para evitar bloqueos de UI.
    """

    def __init__(self, db_manager, payment_manager=None, search_manager=None):
        self.db_manager = db_manager
        self.payment_manager = payment_manager
        self.search_manager = search_manager
        self._running = False

    def start(self):
        if self._running:
            return
        self._running = True

        def _run():
            try:
                self._warm_up()
            except Exception as e:
                logging.debug(f"PreloadManager: error en warm-up: {e}")
            finally:
                self._running = False

        try:
            threading.Thread(target=_run, daemon=True).start()
        except Exception:
            try:
                _run()
            except Exception:
                pass

    def _warm_up(self):
        """Ejecuta varias lecturas rápidas de solo lectura para llenar caches."""
        # 1) Warm-up de usuarios en memoria/caché
        try:
            self.db_manager.obtener_todos_usuarios()
        except Exception:
            pass

        # 2) Sugerencias de búsqueda
        try:
            if self.search_manager and hasattr(self.search_manager, 'refresh_cache'):
                self.search_manager.refresh_cache()
        except Exception:
            pass

        # 3) KPIs ligeros de dashboard y asistencias por semana (sin filtros)
        try:
            cache = getattr(self.db_manager, 'cache_manager', None)
            if cache:
                # Ingresos últimos 12 meses
                try:
                    income = self.payment_manager and self.payment_manager.obtener_ingresos_ultimos_12_meses()
                    if income:
                        cache.set('income_12m', income, ttl_ms=10*60*1000)
                except Exception:
                    pass
                # Nuevos por mes
                try:
                    nuevos = self.db_manager.obtener_nuevos_usuarios_por_mes()
                    if nuevos:
                        cache.set('nuevos_por_mes', nuevos, ttl_ms=10*60*1000)
                except Exception:
                    pass
                # Asistencia semana (90 días)
                try:
                    asist = self.db_manager.obtener_asistencias_por_dia_semana(dias=90)
                    if asist:
                        cache.set('asistencia_semana_d90', asist, ttl_ms=10*60*1000)
                except Exception:
                    pass
        except Exception:
            pass

        # 4) Warm-up de índices (ligero, si no está corriendo)
        try:
            if hasattr(self.db_manager, 'ensure_indexes'):
                threading.Thread(target=self.db_manager.ensure_indexes, daemon=True).start()
        except Exception:
            pass