"""
Widget de notificación de conflictos desactivado temporalmente.

Este módulo ha sido deshabilitado a petición, manteniendo un stub mínimo para
no romper importaciones ni el flujo de la aplicación. El archivo original ha
sido respaldado como `conflict_notification_widget.disabled.py`.

Para reactivar, restaura el archivo de respaldo o reemplaza esta clase por la
implementación completa cuando esté lista.
"""

from PyQt6.QtWidgets import QWidget
from PyQt6.QtCore import pyqtSignal

class ConflictNotificationWidget(QWidget):
    """Stub temporal del widget de notificación de conflictos.

    Toda la funcionalidad está desactivada. Este stub solo garantiza que las
    importaciones y las referencias de clase no fallen mientras se implementa
    la versión completa.
    """

    # Señales definidas para mantener compatibilidad con el resto de la app
    conflicto_resuelto = pyqtSignal(dict)
    conflicto_detectado = pyqtSignal(dict)
    notificacion_enviada = pyqtSignal(dict)

    def __init__(self, *args, **kwargs):  # db_manager puede venir, se ignora
        super().__init__()
        # No se inicializa UI ni timers; el widget queda inerte

    # Métodos no operativos (placeholders seguros)
    def show(self):  # noqa: D401 - comportamiento estándar
        """Muestra el widget (sin contenido funcional)."""
        try:
            return super().show()
        except Exception:
            pass

    def close(self):  # noqa: D401
        """Cierra el widget (sin efectos adicionales)."""
        try:
            return super().close()
        except Exception:
            pass

    # Stubs adicionales para compatibilidad con llamadas externas
    def refresh_conflicts(self):
        """No-op: refresco desactivado temporalmente."""
        pass

    def add_conflict(self, conflict_data: dict | None = None):
        """No-op: agregado de conflicto desactivado."""
        pass

    def update_notification_count(self):
        """No-op: contador de notificaciones desactivado."""
        pass

    def set_profesor_id(self, profesor_id: int | None):
        """No-op: asociación de profesor desactivada."""
        pass