import logging
import time
import threading
from typing import Optional

from PyQt6.QtCore import Qt, QTimer, pyqtSignal
from PyQt6.QtGui import QPixmap, QIcon, QFont
from PyQt6.QtWidgets import (
    QDialog, QVBoxLayout, QHBoxLayout, QLabel, QPushButton, QFrame,
    QSizePolicy, QApplication
)

try:
    import requests
except Exception:
    requests = None  # Se maneja fallback en runtime

try:
    import segno  # QR generation sin dependencias pesadas
except Exception:
    segno = None


class QRCheckinToast(QDialog):
    """Toast flotante para mostrar QR de check-in temporal con contador y polling."""

    # Señal emitida cuando el token se procesa (usado o expirado)
    tokenProcessed = pyqtSignal(dict)
    # Señal interna para pasar el estado del token del hilo de polling al hilo de UI
    statusFetched = pyqtSignal(dict)

    def __init__(self, main_window, token: str, expires_minutes: int = 5, parent=None):
        super().__init__(parent or main_window)
        self.main_window = main_window
        self.token = token
        self.expires_minutes = expires_minutes
        # Evitar bloqueos: flag para evitar solapamiento de requests en polling
        self._polling_inflight = False
        # Flag de finalización para cortar cualquier polling residual inmediatamente
        self._token_finalized = False
        # Control de cierre: no permitir cerrar hasta éxito o timeout
        self._allow_close = False
        self.seconds_left = expires_minutes * 60
        # Cache de URL base resuelta para evitar inconsistencias entre túnel público y servidor local
        self._resolved_base_url: Optional[str] = None
        self.setWindowFlags(
            Qt.WindowType.FramelessWindowHint | Qt.WindowType.Tool | Qt.WindowType.WindowStaysOnTopHint
        )
        self.setAttribute(Qt.WidgetAttribute.WA_TranslucentBackground, True)
        self.setModal(False)
        # ObjectName para aplicar QSS dinámico específico del toast
        try:
            self.setObjectName("qrToast")
        except Exception:
            pass

        # Conectar la señal interna para asegurar que el manejo del estado ocurra en el hilo de UI
        try:
            self.statusFetched.connect(self._handle_token_status)
        except Exception:
            pass

        # Integrar con QSS dinámico del sistema
        try:
            self.setProperty("dynamic_css", "true")
        except Exception:
            pass

        # Cerrar siempre si la app está saliendo o el main se destruye
        try:
            app = QApplication.instance()
            if app is not None:
                app.aboutToQuit.connect(self._on_app_quit)
        except Exception:
            pass
        try:
            if hasattr(self.main_window, 'destroyed'):
                self.main_window.destroyed.connect(self._on_app_quit)
        except Exception:
            pass

        self._build_ui()
        # Resolver y cachear la URL base al crear el toast
        try:
            _ = self._get_base_url()
        except Exception:
            pass
        self._setup_timers()

    def _build_ui(self):
        layout = QVBoxLayout()
        layout.setContentsMargins(12, 12, 12, 12)
        layout.setSpacing(10)

        # Card
        card = QFrame()
        card.setObjectName("qrToastCard")
        # Estilos aplicados vía QSS global dinámico (styles/style.qss)
        card_layout = QVBoxLayout(card)
        card_layout.setContentsMargins(16, 16, 16, 16)
        card_layout.setSpacing(8)

        title = QLabel("QR de Check-in")
        title.setObjectName("title")
        font = QFont()
        font.setPointSize(12)
        title.setFont(font)
        card_layout.addWidget(title)

        # QR o fallback
        self.qr_label = QLabel()
        self.qr_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self.qr_label.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Expanding)
        self._render_qr_or_fallback()
        card_layout.addWidget(self.qr_label)

        self.token_label = QLabel(f"Token: {self.token}")
        self.token_label.setObjectName("tokenLabel")
        self.token_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        # Hacer copiable el token desde el label
        try:
            self.token_label.setTextInteractionFlags(
                Qt.TextInteractionFlag.TextSelectableByMouse | Qt.TextInteractionFlag.TextSelectableByKeyboard
            )
        except Exception:
            pass
        card_layout.addWidget(self.token_label)

        self.countdown_label = QLabel(self._format_seconds(self.seconds_left))
        self.countdown_label.setObjectName("countdown")
        self.countdown_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        card_layout.addWidget(self.countdown_label)

        # Indicador de estado (oculto por defecto): muestra confirmación breve antes de cerrar
        self.status_label = QLabel("")
        self.status_label.setObjectName("statusLabel")
        self.status_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self.status_label.setVisible(False)
        card_layout.addWidget(self.status_label)

        btns = QHBoxLayout()
        self.close_btn = QPushButton("Cerrar")
        self.close_btn.setObjectName("closeBtn")
        # No permitir cierre manual antes de éxito o timeout
        try:
            self.close_btn.setEnabled(False)
        except Exception:
            pass
        # Botón para copiar el token al portapapeles
        copy_btn = QPushButton("Copiar token")
        copy_btn.setObjectName("copyBtn")
        try:
            copy_btn.clicked.connect(lambda: QApplication.clipboard().setText(self.token))
        except Exception:
            pass
        btns.addStretch(1)
        btns.addWidget(copy_btn)
        btns.addWidget(self.close_btn)
        card_layout.addLayout(btns)

        layout.addWidget(card)
        self.setLayout(layout)

        # Posicionar bottom-right del MainWindow
        try:
            geo = self.main_window.geometry()
            self.adjustSize()
            self.move(geo.x() + geo.width() - self.width() - 24, geo.y() + geo.height() - self.height() - 24)
        except Exception:
            pass

    def _on_app_quit(self):
        """Forzar cierre seguro cuando la app o el main se cierran."""
        try:
            self._token_finalized = True
            self._allow_close = True
            self._stop_polling_timers()
            try:
                logging.info("QRCheckinToast: cierre forzado por salida de la aplicación")
            except Exception:
                pass
            QTimer.singleShot(0, self.close)
        except Exception:
            try:
                self.close()
            except Exception:
                pass

    def _render_qr_or_fallback(self):
        try:
            if segno:
                qrcode = segno.make(self.token, error='m')
                # Render a QPixmap via bytes
                import io
                buf = io.BytesIO()
                qrcode.save(buf, kind='png', scale=6)
                buf.seek(0)
                from PyQt6.QtGui import QImage
                image = QImage.fromData(buf.read())
                pix = QPixmap.fromImage(image)
                self.qr_label.setPixmap(pix)
                return
        except Exception as e:
            logging.debug(f"QR render fallback: {e}")
        # Fallback: mostrar texto grande
        self.qr_label.setText("No se pudo generar el QR.\nUse el token mostrado.")

    def _setup_timers(self):
        # Countdown
        self.countdown_timer = QTimer(self)
        self.countdown_timer.timeout.connect(self._tick)
        self.countdown_timer.start(1000)

        # Polling estado token
        self.poll_timer = QTimer(self)
        self.poll_timer.timeout.connect(self._poll_token_status)
        self.poll_timer.start(2000)

    def _format_seconds(self, s: int) -> str:
        m, sec = divmod(max(0, s), 60)
        return f"{m:01d}:{sec:02d} restantes"

    def _tick(self):
        self.seconds_left -= 1
        if self.seconds_left <= 0:
            try:
                self.countdown_label.setText("Expirado")
                self.status_label.setText("⏰ Tiempo agotado")
                self.status_label.setVisible(True)
                self._allow_close = True
                self._token_finalized = True
                self._stop_polling_timers()
            except Exception:
                pass
            self.close()
        else:
            self.countdown_label.setText(self._format_seconds(self.seconds_left))

    def _get_base_url(self) -> str:
        """Resuelve y cachea la URL base del servidor web.
        Prioridad y verificación:
        1) ENV `WEBAPP_BASE_URL` si está presente.
        2) Si `public_tunnel.enabled` y `subdomain` están configurados, PRIORIZAR el túnel público.
           Validación ligera (opcional): consulta `/api/checkin/token_status` con timeout bajo.
        3) Fallback local preferente `http://127.0.0.1:8000` (y como alternativa `8003`).
        """
        # Devolver la resolución previa si existe
        if self._resolved_base_url:
            return self._resolved_base_url
        try:
            import os, json
            from pathlib import Path
            try:
                from utils import get_webapp_base_url
            except Exception:
                get_webapp_base_url = None
            env_base = os.getenv("WEBAPP_BASE_URL", "").strip()
            if env_base:
                self._resolved_base_url = env_base
                return self._resolved_base_url
            base_dir = Path(os.path.dirname(os.path.abspath(__file__))).parent
            cfg_path = base_dir / 'config' / 'config.json'
            if cfg_path.exists():
                with open(cfg_path, 'r', encoding='utf-8') as f:
                    cfg = json.load(f) or {}
                # Preferir configuración explícita de URL base del webapp
                candidate = str(cfg.get('webapp_base_url', '')).strip()
                # Si no hay configuración, intentar obtenerla desde utils
                if not candidate and get_webapp_base_url:
                    try:
                        candidate = get_webapp_base_url()
                    except Exception:
                        candidate = None
                if candidate:
                    # Establecer y devolver inmediatamente
                    self._resolved_base_url = candidate
                    # Validación ligera opcional (no bloqueante del retorno)
                    try:
                        if requests is not None:
                            test_url = candidate.rstrip('/') + "/api/checkin/token_status?token=test"
                            requests.get(test_url, timeout=0.6)
                    except Exception:
                        pass
                    return self._resolved_base_url
        except Exception:
            pass
        # Fallback local: preferir 8000 si está disponible, sino 8003
        try:
            if requests is not None:
                for candidate in ("http://127.0.0.1:8000", "http://127.0.0.1:8003"):
                    try:
                        r = requests.get(candidate.rstrip('/') + "/api/checkin/token_status?token=test", timeout=0.5)
                        if r.status_code in (200, 401, 403):
                            self._resolved_base_url = candidate
                            return self._resolved_base_url
                    except Exception:
                        pass
        except Exception:
            pass
        # Último recurso: 8000
        self._resolved_base_url = "http://127.0.0.1:8000"
        return self._resolved_base_url

    def _poll_token_status(self):
        # Evitar solapamiento de consultas en curso
        # Si el token ya fue finalizado (usado confirmado), no seguir pollando
        if getattr(self, "_token_finalized", False):
            return
        if self._polling_inflight:
            return
        self._polling_inflight = True
        threading.Thread(target=self._fetch_token_status, daemon=True).start()

    def _fetch_token_status(self):
        try:
            # Consultar múltiples bases para evitar inconsistencias entre túnel público y servidor local
            bases = []
            try:
                # Primero la base resuelta (túnel público o env)
                resolved = self._get_base_url().rstrip('/')
                if resolved:
                    bases.append(resolved)
                # Luego servidores locales como fallback
                for local in ("http://127.0.0.1:8000", "http://127.0.0.1:8003"):
                    if local not in bases:
                        bases.append(local)
            except Exception:
                bases = [self._get_base_url().rstrip('/')]

            best_data = None
            for base in bases:
                url = base.rstrip('/') + "/api/checkin/token_status"
                data = None
                # Intentar con requests si está disponible
                try:
                    if requests is not None:
                        resp = requests.get(url, params={"token": self.token}, timeout=0.8)
                        if resp.ok:
                            data = resp.json()
                            try:
                                logging.info(
                                    f"QRCheckinToast.poll: base={base} token={self.token} resp={data}"
                                )
                            except Exception:
                                pass
                except Exception:
                    data = None

                # Fallback a urllib si requests no está disponible o falló
                if data is None:
                    try:
                        import urllib.request
                        import urllib.parse
                        import json as _json
                        q = urllib.parse.urlencode({"token": self.token})
                        full = url + ("?" + q)
                        req = urllib.request.Request(full, method="GET")
                        with urllib.request.urlopen(req, timeout=0.8) as r:
                            body = r.read()
                            try:
                                data = _json.loads(body.decode("utf-8"))
                                try:
                                    logging.debug(
                                        f"QRCheckinToast.poll(urllib): base={base} token={self.token} resp={data}"
                                    )
                                except Exception:
                                    pass
                            except Exception:
                                data = None
                    except Exception:
                        data = None

                # Seleccionar el primer resultado que indique cierre (solo usado)
                try:
                    if isinstance(data, dict):
                        used = bool(data.get("used"))
                        expired = bool(data.get("expired"))
                        exists = True if data.get("exists") is None else bool(data.get("exists"))
                        if used:
                            # Marcar finalización inmediatamente para cortar polls futuros
                            try:
                                self._token_finalized = True
                            except Exception:
                                pass
                            # Detener timers en el hilo principal lo antes posible
                            try:
                                QTimer.singleShot(0, self._stop_polling_timers)
                            except Exception:
                                pass
                            best_data = data
                            break
                        # Si no hay cierre, conservar el primero válido para UI (por ejemplo mostrar estado)
                        if best_data is None:
                            best_data = data
                except Exception:
                    pass

            if best_data is not None:
                # Emitir señal hacia el hilo de UI para manejar estado de forma segura
                try:
                    self.statusFetched.emit(best_data)
                except Exception:
                    # Fallback: intentar encolar con QTimer
                    try:
                        QTimer.singleShot(0, lambda d=best_data: self._handle_token_status(d))
                    except Exception:
                        pass
        except Exception:
            # Silencioso, reintenta en próximo tick
            pass
        finally:
            self._polling_inflight = False

    def _handle_token_status(self, data: dict):
        try:
            used = bool(data.get("used"))
            expired = bool(data.get("expired"))
            exists = True if data.get("exists") is None else bool(data.get("exists"))
            try:
                logging.info(
                    f"QRCheckinToast.handle: token={self.token} exists={exists} used={used} expired={expired}"
                )
            except Exception:
                pass
            # Emitir señal si el token deja de existir o expira, para que el main pueda reaccionar
            payload = {"token": self.token, "used": used, "expired": expired}
            try:
                if used or expired or (exists is False):
                    self.tokenProcessed.emit(payload)
                    handler = getattr(self.main_window, 'on_checkin_token_processed', None)
                    if callable(handler):
                        handler(payload)
            except Exception:
                pass

            # Cerrar en éxito; permitir cierre inmediato en expirado o inexistente
            if used:
                try:
                    # Detener timers inmediatamente para evitar más peticiones/contador
                    self._stop_polling_timers()
                    # Mostrar confirmación visible y cerrar luego de una breve pausa
                    self.status_label.setText("✅ Asistencia registrada")
                    self.status_label.setVisible(True)
                    self._allow_close = True
                    try:
                        logging.info("QRCheckinToast.handle: programando cierre en 1200ms tras éxito")
                    except Exception:
                        pass
                    QTimer.singleShot(1200, self.close)
                    # Watchdog adicional: por si el singleShot no dispara, forzar cierre
                    try:
                        if not hasattr(self, '_close_watchdog') or self._close_watchdog is None:
                            self._close_watchdog = QTimer(self)
                            self._close_watchdog.setSingleShot(True)
                            self._close_watchdog.timeout.connect(self.close)
                        self._close_watchdog.start(1600)
                    except Exception:
                        pass
                    # Habilitar botón cerrar tras confirmación
                    try:
                        self.close_btn.setEnabled(True)
                    except Exception:
                        pass
                except Exception:
                    self._allow_close = True
                    self.close()
            else:
                # En expiración o inexistencia, permitir cerrar y (si expiró) autocerrar rápido
                try:
                    if expired:
                        self.status_label.setText("⚠️ Token expirado")
                        self.status_label.setVisible(True)
                        self._allow_close = True
                        self._stop_polling_timers()
                        try:
                            self.close_btn.setEnabled(True)
                        except Exception:
                            pass
                        # Autocerrar tras una breve pausa para no dejar el proceso abierto
                        try:
                            if not hasattr(self, '_close_watchdog') or self._close_watchdog is None:
                                self._close_watchdog = QTimer(self)
                                self._close_watchdog.setSingleShot(True)
                                self._close_watchdog.timeout.connect(self.close)
                            QTimer.singleShot(1200, self.close)
                            self._close_watchdog.start(1600)
                        except Exception:
                            QTimer.singleShot(1200, self.close)
                    elif exists is False:
                        # Token no encontrado: habilitar cierre manual y mostrar estado
                        self.status_label.setText("⏳ Esperando confirmación")
                        self.status_label.setVisible(True)
                        self._allow_close = True
                        try:
                            self.close_btn.setEnabled(True)
                        except Exception:
                            pass
                except Exception:
                    pass
        except Exception:
            pass

    def closeEvent(self, event):
        try:
            # Bloquear cierre si aún no hay éxito o timeout
            if not getattr(self, '_allow_close', False) and self.seconds_left > 0:
                try:
                    logging.info("QRCheckinToast.closeEvent: intento de cierre bloqueado")
                except Exception:
                    pass
                event.ignore()
                return
            # Detener timers para evitar polling residual
            # Marcar finalizado y detener timers de forma centralizada
            self._token_finalized = True
            self._stop_polling_timers()
            try:
                logging.info("QRCheckinToast.closeEvent: cierre del toast, timers detenidos")
            except Exception:
                pass
        except Exception:
            pass
        super().closeEvent(event)

    def show_toast(self):
        try:
            self.show()
            self.raise_()
        except Exception:
            self.show()

    def _stop_polling_timers(self):
        """Detiene de forma segura los timers de polling y de countdown."""
        try:
            if hasattr(self, 'poll_timer') and self.poll_timer:
                try:
                    self.poll_timer.stop()
                except Exception:
                    pass
            if hasattr(self, 'countdown_timer') and self.countdown_timer:
                try:
                    self.countdown_timer.stop()
                except Exception:
                    pass
            # Marcar no en curso cualquier polling
            self._polling_inflight = False
        except Exception:
            pass