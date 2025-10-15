import time
import json
import logging
from typing import Any, Callable, Optional, Iterable

from PyQt6.QtCore import QObject
from utils_modules.async_runner import TaskThread


def _safe_log_cache_metric(duration_ms: float, source: str = "cache_or_db", count: int = 0) -> None:
    """Registra métricas de tiempo de carga de usuarios en logs/cache_metrics.json.

    - No lanza excepciones; falla en silencio si hay problemas de E/S.
    - Agrega una entrada con timestamp, duración y fuente utilizada.
    """
    try:
        entry = {
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
            "metric": "load_users",
            "duration_ms": round(duration_ms, 2),
            "source": source,
            "count": int(count),
        }
        # Leer archivo existente si posible
        log_path = "logs/cache_metrics.json"
        try:
            with open(log_path, "r", encoding="utf-8") as f:
                data = json.load(f)
                if not isinstance(data, list):
                    data = []
        except Exception:
            data = []
        data.append(entry)
        with open(log_path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
    except Exception:
        try:
            logging.debug("No se pudo registrar cache_metrics.json")
        except Exception:
            pass


def load_users_cached_async(
    db_manager: Any,
    on_success: Callable[[Iterable[Any]], None],
    on_error: Optional[Callable[[str], None]] = None,
    parent: Optional[QObject] = None,
) -> TaskThread:
    """Inicia un hilo para cargar usuarios usando caché si está disponible.

    - Usa `db_manager.obtener_usuarios_con_cache()` si existe; si no, `obtener_todos_usuarios()`.
    - Mide duración y registra métricas en `logs/cache_metrics.json`.
    - Retorna el hilo iniciado para que el llamador pueda gestionar su ciclo de vida si desea.
    """

    def _load():
        start = time.perf_counter()
        source = "unknown"
        try:
            if hasattr(db_manager, "obtener_usuarios_con_cache"):
                source = "cache"
                usuarios = db_manager.obtener_usuarios_con_cache()
            else:
                source = "db"
                usuarios = db_manager.obtener_todos_usuarios()
        except Exception as e:
            _safe_log_cache_metric((time.perf_counter() - start) * 1000.0, source=source, count=0)
            raise e
        _safe_log_cache_metric((time.perf_counter() - start) * 1000.0, source=source, count=len(usuarios or []))
        return usuarios

    thread = TaskThread(_load)
    if parent is not None:
        thread.setParent(parent)
    thread.success.connect(lambda usuarios: on_success(usuarios or []))
    if on_error:
        thread.error.connect(on_error)
    thread.finished.connect(thread.deleteLater)
    thread.start()
    return thread