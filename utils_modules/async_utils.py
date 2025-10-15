import logging
from typing import Callable, Optional
from PyQt6.QtCore import QTimer, QObject

from utils_modules.async_runner import TaskThread


def run_in_background(
    func: Callable[[], object],
    on_success: Callable[[object], None],
    on_error: Optional[Callable[[str], None]] = None,
    parent: Optional[QObject] = None,
    timeout_ms: Optional[int] = None,
    # Compatibilidad adicional
    timeout_seconds: Optional[float] = None,
    description: Optional[str] = None,
):
    """Ejecuta una función bloqueante en segundo plano con soporte de timeout ligero.

    - Ejecuta `func` en un hilo usando TaskThread.
    - Llama `on_success(result)` en el hilo principal cuando termina.
    - Llama `on_error(message)` si ocurre error o si expira el timeout.
    - `timeout_ms`: si se especifica, se arma un QTimer para notificar expiración.

    Nota: El timeout no detiene el hilo de forma forzada (no es seguro),
    pero evita aplicar resultados tardíos y notifica al usuario.
    """

    timed_out = {"value": False}

    def safe_success(result):
        if not timed_out["value"]:
            try:
                on_success(result)
            except Exception as e:
                logging.error(f"Error en callback de éxito: {e}")

    def safe_error(message: str):
        if not timed_out["value"]:
            if on_error:
                try:
                    on_error(message)
                except Exception as e:
                    logging.error(f"Error en callback de error: {e}")
            else:
                logging.error(f"Tarea en segundo plano falló: {message}")

    thread = TaskThread(func)
    thread.success.connect(safe_success)
    thread.error.connect(safe_error)

    # Configurar timeout ligero si se solicita
    # Normalizar timeout desde segundos si se proporciona
    if (timeout_seconds is not None) and (timeout_ms is None):
        try:
            timeout_ms = int(float(timeout_seconds) * 1000)
        except Exception:
            timeout_ms = None

    timer: Optional[QTimer] = None
    if timeout_ms and timeout_ms > 0:
        timer = QTimer(parent)
        timer.setSingleShot(True)

        def on_timeout():
            timed_out["value"] = True
            # Evitar aplicar futuros resultados; notificar
            if on_error:
                try:
                    msg = "Operación excedió el tiempo límite"
                    if description:
                        msg = f"{description}: {msg}"
                    on_error(msg)
                except Exception as e:
                    logging.error(f"Error notificando timeout: {e}")
            else:
                m = "Operación en segundo plano excedió el tiempo límite"
                try:
                    if description:
                        m = f"{description}: {m}"
                except Exception:
                    pass
                logging.warning(m)

        timer.timeout.connect(on_timeout)
        timer.start(timeout_ms)

    # Limpiar timer al finalizar para evitar fugas
    def cleanup():
        if timer:
            try:
                timer.stop()
            except Exception:
                pass

    thread.finished.connect(cleanup)
    thread.finished.connect(thread.deleteLater)
    thread.start()
    return thread