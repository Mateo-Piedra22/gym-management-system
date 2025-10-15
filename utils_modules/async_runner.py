import logging
from typing import Any, Callable, Optional
from PyQt6.QtCore import QThread, pyqtSignal


class TaskThread(QThread):
    """Hilo genérico para ejecutar funciones bloqueantes sin trabar la UI.

    - Ejecuta `func(*args, **kwargs)` en segundo plano
    - Emite señales `success(result)` y `error(message)` en el hilo de UI
    - Acepta callbacks opcionales `on_success` y `on_error` para compatibilidad
      con usos existentes que pasaban estos kwargs al hilo.
    """

    success = pyqtSignal(object)
    error = pyqtSignal(str)

    def __init__(
        self,
        func: Callable[..., Any],
        *args,
        on_success: Optional[Callable[[Any], None]] = None,
        on_error: Optional[Callable[[str], None]] = None,
        **kwargs,
    ):
        # Permitir establecer padre del hilo sin pasar al worker
        parent_obj = kwargs.pop("parent", None)
        try:
            super().__init__(parent_obj)
        except Exception:
            super().__init__()
        self._func = func
        self._args = args
        # Extraer y evitar pasar callbacks al target
        self._on_success = on_success or kwargs.pop("on_success", None)
        self._on_error = on_error or kwargs.pop("on_error", None)
        self._kwargs = kwargs

        # Conectar callbacks si fueron provistos
        if self._on_success:
            try:
                self.success.connect(self._on_success)
            except Exception:
                pass
        if self._on_error:
            try:
                self.error.connect(self._on_error)
            except Exception:
                pass

    def run(self):
        try:
            result = self._func(*self._args, **self._kwargs)
            self.success.emit(result)
        except Exception as e:
            try:
                logging.error(f"TaskThread error: {e}")
            except Exception:
                pass
            self.error.emit(str(e))