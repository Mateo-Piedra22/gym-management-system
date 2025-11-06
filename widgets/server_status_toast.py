import logging
from typing import Optional

from PyQt6.QtCore import Qt, QTimer, QEvent
from PyQt6.QtGui import QFont
from PyQt6.QtWidgets import QDialog, QVBoxLayout, QHBoxLayout, QLabel, QFrame, QSizePolicy, QApplication


class ServerStatusToast(QDialog):
    """Toast flotante para mostrar el estado del servidor/túnel.

    Posicionado en la parte superior-centro de la ventana padre.
    Usa QSS dinámico similar al toast de QR.
    """

    # Registro global para evitar toasts duplicados por ventana padre
    _active_toasts_by_parent_id: dict[int, "ServerStatusToast"] = {}

    def __init__(self, parent_window, local_url: str,
                 public_url: Optional[str] = None,
                 message: Optional[str] = None,
                 webapp_ok: Optional[bool] = None,
                 db_ok: Optional[bool] = None,
                 external_id: Optional[str] = None):
        # Robustez: si el padre está destruido o no es válido, crear como diálogo sin padre
        try:
            super().__init__(parent_window)
        except Exception:
            super().__init__(None)
        self.parent_window = parent_window
        self.local_url = local_url
        self.public_url = public_url
        self.message = message
        self.webapp_ok = webapp_ok
        self.db_ok = db_ok
        self.external_id = external_id
        # Estado interno para prevenir re-aperturas y asegurar cierre correcto
        self._closed = False

        # Deduplicación: cerrar cualquier toast activo previo para este padre
        try:
            pid = id(self.parent_window) if self.parent_window is not None else -1
            prev = ServerStatusToast._active_toasts_by_parent_id.get(pid)
            if prev is not None and prev is not self:
                try:
                    prev._safe_close()
                except Exception:
                    pass
            ServerStatusToast._active_toasts_by_parent_id[pid] = self
        except Exception:
            pass

        self.setWindowFlags(
            Qt.WindowType.FramelessWindowHint | Qt.WindowType.Tool | Qt.WindowType.WindowStaysOnTopHint
        )
        self.setAttribute(Qt.WidgetAttribute.WA_TranslucentBackground, True)
        # Eliminar automáticamente al cerrar para evitar artefactos flotantes
        try:
            self.setAttribute(Qt.WidgetAttribute.WA_DeleteOnClose, True)
        except Exception:
            pass
        self.setModal(False)
        try:
            self.setObjectName("serverStatusToast")
        except Exception:
            pass
        try:
            self.setProperty("dynamic_css", "true")
        except Exception:
            pass

        # Cerrar el toast si el padre se destruye (logout/relogin)
        try:
            if hasattr(self.parent_window, 'destroyed'):
                self.parent_window.destroyed.connect(self._on_parent_destroyed)
        except Exception:
            pass

        # Instalar filtro de eventos en el padre para cerrar ante Close/Hide
        try:
            self._event_filter_installed = False
            if hasattr(self.parent_window, 'installEventFilter'):
                self.parent_window.installEventFilter(self)
                self._event_filter_installed = True
        except Exception:
            self._event_filter_installed = False

        # Cerrar también cuando la app esté por finalizar
        try:
            app = QApplication.instance()
            if app:
                app.aboutToQuit.connect(self._safe_close)
        except Exception:
            pass

        self._build_ui()
        self._position_top_center()

        # Autocierre suave después de unos segundos
        self._auto_close_timer = QTimer(self)
        self._auto_close_timer.setSingleShot(True)
        self._auto_close_timer.timeout.connect(self._safe_close)
        try:
            self._auto_close_timer.start(4500)
        except Exception:
            pass

    def _build_ui(self):
        layout = QVBoxLayout()
        layout.setContentsMargins(10, 10, 10, 10)
        layout.setSpacing(8)

        card = QFrame()
        card.setObjectName("serverStatusCard")
        card_layout = QVBoxLayout(card)
        card_layout.setContentsMargins(14, 14, 14, 14)
        card_layout.setSpacing(8)

        # Estilo de respaldo para evitar "sin fondo" si el QSS no carga
        try:
            card.setStyleSheet("""
                QFrame#serverStatusCard {
                    background-color: rgba(30,30,30,230);
                    border-radius: 8px;
                    border: 1px solid rgba(255,255,255,40);
                }
                QLabel#title { color: white; font-weight: bold; }
                QLabel#messageLabel, QLabel#localLabel, QLabel#publicLabel { color: white; }
            """)
        except Exception:
            pass

        title = QLabel("Estado del servidor")
        title.setObjectName("title")
        font = QFont()
        font.setPointSize(11)
        title.setFont(font)
        card_layout.addWidget(title)

        # Mensaje principal
        msg = self.message or self._default_message()
        self.message_label = QLabel(msg)
        self.message_label.setObjectName("messageLabel")
        self.message_label.setWordWrap(True)
        self.message_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self.message_label.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Preferred)
        card_layout.addWidget(self.message_label)

        # URLs y estados
        urls_layout = QVBoxLayout()
        urls_layout.setSpacing(6)

        # Fila de URLs
        url_row = QHBoxLayout()
        url_row.setSpacing(6)
        local_lbl = QLabel(f"Local: {self.local_url}")
        local_lbl.setObjectName("localLabel")
        local_lbl.setAlignment(Qt.AlignmentFlag.AlignCenter)
        url_row.addWidget(local_lbl)
        if self.public_url:
            public_lbl = QLabel(f"Público: {self.public_url}")
            public_lbl.setObjectName("publicLabel")
            public_lbl.setAlignment(Qt.AlignmentFlag.AlignCenter)
            url_row.addWidget(public_lbl)
        urls_layout.addLayout(url_row)

        # Fila de estados (WebApp / DB Local / DB Railway)
        status_row = QHBoxLayout()
        status_row.setSpacing(12)

        def _mk_status(label: str, ok: Optional[bool]) -> QLabel:
            state = "OK" if ok else ("—" if ok is None else "FALLA")
            ql = QLabel(f"{label}: {state}")
            ql.setAlignment(Qt.AlignmentFlag.AlignCenter)
            ql.setObjectName("statusLabel")
            # Colores simples en línea para estados
            try:
                color = "#4caf50" if ok else ("#cccccc" if ok is None else "#ff5252")
                ql.setStyleSheet(f"QLabel#statusLabel {{ color: {color}; font-weight: bold; }}")
            except Exception:
                pass
            return ql

        status_row.addWidget(_mk_status("WebApp", self.webapp_ok))
        status_row.addWidget(_mk_status("Database", self.db_ok))

        urls_layout.addLayout(status_row)

        # Identificador externo (nodo/dispositivo)
        try:
            if self.external_id:
                ext_row = QHBoxLayout()
                ext_row.setSpacing(6)
                ext_lbl_title = QLabel("Nodo:")
                ext_lbl_title.setAlignment(Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter)
                ext_lbl_title.setObjectName("statusLabel")
                ext_lbl_val = QLabel(str(self.external_id))
                ext_lbl_val.setAlignment(Qt.AlignmentFlag.AlignLeft | Qt.AlignmentFlag.AlignVCenter)
                ext_lbl_val.setObjectName("statusLabel")
                try:
                    ext_lbl_title.setStyleSheet("QLabel#statusLabel { color: #cccccc; font-weight: bold; }")
                    ext_lbl_val.setStyleSheet("QLabel#statusLabel { color: #4caf50; font-weight: bold; }")
                except Exception:
                    pass
                ext_row.addWidget(ext_lbl_title)
                ext_row.addWidget(ext_lbl_val)
                urls_layout.addLayout(ext_row)
        except Exception:
            pass
        card_layout.addLayout(urls_layout)

        layout.addWidget(card)
        self.setLayout(layout)

    def _position_top_center(self):
        try:
            geo = self.parent_window.geometry()
            self.adjustSize()
            x = geo.x() + (geo.width() - self.width()) // 2
            y = geo.y() + 24  # margen superior
            self.move(x, y)
        except Exception:
            try:
                self.move(24, 24)
            except Exception:
                pass

    def _default_message(self) -> str:
        try:
            if self.public_url:
                return "WebApp local activa; conexiones de BD verificadas"
            return "WebApp local activa"
        except Exception:
            return "Servidor en ejecución"

    def show_toast(self):
        # Evitar mostrar si ya está cerrado o el padre no es válido
        if getattr(self, '_closed', False):
            return
        # Refuerzo de deduplicación previo a mostrar
        try:
            pid = id(self.parent_window) if self.parent_window is not None else -1
            prev = ServerStatusToast._active_toasts_by_parent_id.get(pid)
            if prev is not None and prev is not self:
                try:
                    prev._safe_close()
                except Exception:
                    pass
            ServerStatusToast._active_toasts_by_parent_id[pid] = self
        except Exception:
            pass
        try:
            if self.parent_window and hasattr(self.parent_window, 'isVisible') and not self.parent_window.isVisible():
                return
        except Exception:
            pass
        try:
            self._position_top_center()
        except Exception:
            pass
        try:
            self.show()
            try:
                self.raise_()
            except Exception:
                pass
        except Exception:
            try:
                self.show()
            except Exception:
                logging.warning("No se pudo mostrar el ServerStatusToast")

    def _safe_close(self):
        # Cierre defensivo para evitar reentradas y artefactos visuales
        if getattr(self, '_closed', False):
            return
        try:
            self._closed = True
        except Exception:
            pass
        try:
            # Detener temporizador de autocierre por si sigue activo
            if hasattr(self, '_auto_close_timer') and self._auto_close_timer:
                try:
                    self._auto_close_timer.stop()
                except Exception:
                    pass
        except Exception:
            pass
        try:
            self.close()
        except Exception:
            # Como último recurso, ocultar
            try:
                self.hide()
            except Exception:
                pass

    def closeEvent(self, event):
        # Asegurar limpieza de temporizadores y estado cerrado
        try:
            self._closed = True
        except Exception:
            pass
        # Remover del registro global si corresponde
        try:
            pid = id(self.parent_window) if self.parent_window is not None else -1
            cur = ServerStatusToast._active_toasts_by_parent_id.get(pid)
            if cur is self:
                ServerStatusToast._active_toasts_by_parent_id.pop(pid, None)
        except Exception:
            pass
        # Remover filtro de eventos si estaba instalado
        try:
            if getattr(self, '_event_filter_installed', False) and self.parent_window:
                self.parent_window.removeEventFilter(self)
                self._event_filter_installed = False
        except Exception:
            pass
        try:
            if hasattr(self, '_auto_close_timer') and self._auto_close_timer:
                self._auto_close_timer.stop()
        except Exception:
            pass
        try:
            super().closeEvent(event)
        except Exception:
            try:
                event.accept()
            except Exception:
                pass

    def _on_parent_destroyed(self, *args, **kwargs):
        # Si el padre se destruye (cambio de sesión), cerrar inmediatamente el toast
        try:
            self._safe_close()
        except Exception:
            pass
        # Limpiar registro asociado al padre destruido
        try:
            pid = id(self.parent_window) if self.parent_window is not None else -1
            cur = ServerStatusToast._active_toasts_by_parent_id.get(pid)
            if cur is self:
                ServerStatusToast._active_toasts_by_parent_id.pop(pid, None)
        except Exception:
            pass

    def eventFilter(self, obj, event):
        # Cerrar el toast cuando el padre se cierra, se oculta o se destruye, para evitar artefactos
        try:
            if obj is self.parent_window:
                et = event.type()
                if et in (QEvent.Close, QEvent.Hide, QEvent.Destroyed):
                    try:
                        self._safe_close()
                    except Exception:
                        pass
                    try:
                        if getattr(self, '_event_filter_installed', False):
                            obj.removeEventFilter(self)
                            self._event_filter_installed = False
                    except Exception:
                        pass
                    return False
        except Exception:
            pass
        return False