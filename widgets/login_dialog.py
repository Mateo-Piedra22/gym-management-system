import sys
import os
import json
from PyQt6.QtWidgets import (QDialog, QVBoxLayout, QTabWidget, QWidget,
                             QFormLayout, QLineEdit, QPushButton, QComboBox,
                             QMessageBox, QLabel, QHBoxLayout, QFrame,
                             QScrollArea, QSizePolicy, QGraphicsDropShadowEffect, QApplication, QToolButton)
from PyQt6.QtCore import Qt, pyqtSignal, QPropertyAnimation, QEasingCurve, QSequentialAnimationGroup, QPoint, QRegularExpression, QSize, QUrl, QThread, QTimer
from PyQt6.QtGui import QIcon, QPixmap, QFont, QColor, QAction, QKeySequence, QShortcut, QRegularExpressionValidator, QPainter, QPen, QBrush, QDesktopServices
from typing import List, Optional, Dict
from models import Usuario
from database import DatabaseManager
from utils_modules.async_runner import TaskThread
from utils import resource_path, get_public_tunnel_enabled, get_webapp_base_url, read_gym_data
import uuid
import socket
import logging
import time
try:
    from audit_logger import set_audit_context
except Exception:
    def set_audit_context(*args, **kwargs):
        pass

class LoginDialog(QDialog):
    def __init__(self, db_manager: DatabaseManager, parent=None):
        super().__init__(parent)
        self.db_manager = db_manager
        self.logged_in_role = None
        self.logged_in_user = None
        self.gym_data = {}
        self.branding_config = {}
        # Cachés en memoria para acelerar cargas repetidas durante el login
        try:
            self._mem_cache = getattr(self.db_manager, 'cache', None)
        except Exception:
            self._mem_cache = None

        # Cargar datos del gimnasio y branding de forma perezosa y con caché
        self._load_gym_data_cached()
        self.load_branding_config()

        gym_name = self.gym_data.get('gym_name', 'Gimnasio')
        self.setWindowTitle(f"Iniciar Sesión - {gym_name}")
        for candidate in ["assets/gym_logo.ico", "assets/gym_icon.ico", "assets/icon.png"]:
            icon_path = resource_path(candidate)
            if os.path.exists(icon_path):
                self.setWindowIcon(QIcon(icon_path))
                break
        self.setMinimumSize(900, 360)
        self.setModal(True) # Bloquea la ventana principal
        # Habilitar botón de minimizar además del de cerrar
        try:
            self.setWindowFlag(Qt.WindowType.WindowMinimizeButtonHint, True)
            self.setWindowFlag(Qt.WindowType.WindowCloseButtonHint, True)
            self.setWindowFlag(Qt.WindowType.WindowTitleHint, True)
        except Exception:
            pass
        # Desactivar el agarre de tamaño; permitir redimensionar desde las esquinas
        try:
            self.setSizeGripEnabled(False)
        except Exception:
            pass
        # Ajustar límites según la pantalla disponible
        try:
            screen = QApplication.primaryScreen()
            if screen:
                geom = screen.availableGeometry()
                screen_w = geom.width()
                screen_h = geom.height()
                # Limitar la altura máxima para no superar la pantalla en equipos pequeños
                self.setMaximumHeight(max(360, screen_h - 40))
                # Asegurar una altura mínima razonable sin exceder la máxima
                self.setMinimumHeight(360)
                # Ajustar tamaño inicial proporcional
                target_w = min(max(900, int(screen_w * 0.6)), int(screen_w * 0.9))
                target_h = min(max(360, int(screen_h * 0.6)), self.maximumHeight())
                self.resize(target_w, target_h)
        except Exception:
            pass

        # Habilitar sistema de CSS dinámico para el diálogo
        self.setProperty("dynamic_css", "true")

        # Inicializar con configuración de branding por defecto antes de construir UI
        self.branding_config = {
            'primary_color': '#3498db',
            'secondary_color': '#2ecc71',
            'accent_color': '#e74c3c',
            'background_color': '#ffffff',
            'alt_background_color': '#f8f9fa',
            'text_color': '#2c3e50',
            'main_font': 'Arial',
            'main_logo_path': None,
            'show_logo': True,
            'logo_size': 100
        }

        self.setup_ui()
        self.apply_modern_styling()
        # Lanzar carga asíncrona de branding y profesores para no bloquear la UI
        try:
            self._load_branding_config_async(timeout_ms=5000)
        except Exception:
            pass
        try:
            self._load_profesores_async(timeout_ms=8000)
        except Exception:
            pass
        try:
            QTimer.singleShot(50, self._prefetch_critical_data_async)
        except Exception:
            pass

    def setup_ui(self):
        """Configura la interfaz de usuario moderna con diseño de dos columnas"""
        main_layout = QHBoxLayout(self)
        main_layout.setSpacing(20)
        main_layout.setContentsMargins(24, 24, 24, 24)

        # === COLUMNA IZQUIERDA - LOGIN ===
        left_frame = QFrame()
        left_frame.setObjectName("login_left_panel")
        left_frame.setMinimumWidth(450)
        left_frame.setMaximumWidth(500)
        # Habilitar CSS dinámico en panel izquierdo
        left_frame.setProperty("dynamic_css", "true")
        # Sombra sutil para elevar el panel izquierdo
        left_shadow = QGraphicsDropShadowEffect(self)
        left_shadow.setBlurRadius(28)
        left_shadow.setXOffset(0)
        left_shadow.setYOffset(8)
        left_shadow.setColor(QColor(0, 0, 0, 60))
        left_frame.setGraphicsEffect(left_shadow)
        left_layout = QVBoxLayout(left_frame)
        left_layout.setSpacing(15)
        left_layout.setContentsMargins(30, 25, 30, 25)

        # Título de bienvenida
        welcome_label = QLabel("Bienvenido")
        welcome_label.setObjectName("welcome_title")
        welcome_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        # Evitar estilos globales que reduzcan el tamaño del título
        try:
            welcome_font = welcome_label.font()
            try:
                welcome_font.setPixelSize(36)
            except Exception:
                welcome_font.setPointSize(24)
            welcome_font.setWeight(QFont.Weight.Bold)
            welcome_label.setFont(welcome_font)
        except Exception:
            pass
        left_layout.addWidget(welcome_label)

        subtitle_label = QLabel("Inicia sesión para continuar")
        subtitle_label.setObjectName("welcome_subtitle")
        subtitle_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        left_layout.addWidget(subtitle_label)
        # Barra de acento bajo el título
        accent_line = QFrame()
        accent_line.setObjectName("accent_bar")
        accent_line.setFixedHeight(3)
        accent_line.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Fixed)
        left_layout.addWidget(accent_line, 0, Qt.AlignmentFlag.AlignHCenter)

        left_layout.addSpacing(15)

        # Tabs de login
        self.tabs = QTabWidget()
        self.tabs.setObjectName("login_tabs")
        # Habilitar CSS dinámico en tabs
        self.tabs.setProperty("dynamic_css", "true")
        
        # Recursos de iconos para toggles de visibilidad
        self._eye_icon_path = resource_path(os.path.join("assets", "eye.svg"))
        self._eye_off_icon_path = resource_path(os.path.join("assets", "eye_off.svg"))
        if os.path.exists(self._eye_icon_path):
            self._eye_icon = QIcon(self._eye_icon_path)
        else:
            self._eye_icon = self._create_fallback_eye_icon(False)
        if os.path.exists(self._eye_off_icon_path):
            self._eye_off_icon = QIcon(self._eye_off_icon_path)
        else:
            self._eye_off_icon = self._create_fallback_eye_icon(True)
        # Pestaña de Dueño
        owner_widget = QWidget()
        owner_widget.setObjectName("login_card")
        # Guardar referencia para aplicar estilos específicos sin sobreescribir el QSS global
        self.owner_widget = owner_widget
        owner_widget.setProperty("dynamic_css", "true")
        owner_layout = QVBoxLayout(owner_widget)
        owner_layout.setSpacing(15)
        owner_layout.setContentsMargins(15, 15, 15, 15)
        owner_title = QLabel("👑 Acceso de Dueño")
        owner_title.setObjectName("tab_title")
        owner_title.setAlignment(Qt.AlignmentFlag.AlignCenter)
        owner_layout.addWidget(owner_title)
        # Divisor sutil bajo el título de la pestaña Dueño
        owner_divider = QFrame()
        owner_divider.setObjectName("section_divider")
        owner_divider.setFrameShape(QFrame.Shape.HLine)
        owner_divider.setFrameShadow(QFrame.Shadow.Sunken)
        owner_divider.setProperty("dynamic_css", "true")
        owner_layout.addWidget(owner_divider)
        
        self.owner_password_input = QLineEdit()
        self.owner_password_input.setEchoMode(QLineEdit.EchoMode.Password)
        self.owner_password_input.setPlaceholderText("Contraseña de Dueño")
        self.owner_password_input.setObjectName("login_input")
        self.owner_password_input.setClearButtonEnabled(True)
        # Toggle de visibilidad para contraseña de dueño
        self._owner_password_visible = False
        self._owner_toggle_action = self.owner_password_input.addAction(self._eye_off_icon, QLineEdit.ActionPosition.TrailingPosition)
        self._owner_toggle_action.triggered.connect(lambda: self._toggle_password_visibility(self.owner_password_input, 'owner'))
        owner_label = QLabel("🔒 Contraseña:")
        owner_label.setProperty("class", "panel_label")
        owner_label.setProperty("dynamic_css", "true")
        owner_label.setAlignment(Qt.AlignmentFlag.AlignLeft)
        owner_layout.addWidget(owner_label)
        owner_layout.addWidget(self.owner_password_input)
        # Enlace para cambiar contraseña del Dueño
        owner_change_label = QLabel('<a href="#">Cambiar contraseña de Dueño</a>')
        owner_change_label.setObjectName("link_label")
        owner_change_label.setAlignment(Qt.AlignmentFlag.AlignLeft)
        try:
            owner_change_label.setTextInteractionFlags(Qt.TextInteractionFlag.LinksAccessibleByMouse)
        except Exception:
            pass
        owner_change_label.setOpenExternalLinks(False)
        owner_change_label.linkActivated.connect(self._show_owner_password_change_dialog)
        owner_layout.addWidget(owner_change_label)
        
        self.owner_login_button = QPushButton("🔑 Iniciar Sesión como Dueño")
        self.owner_login_button.setObjectName("primary_login_button")
        # Clasificar botón como primario para estilos globales
        self.owner_login_button.setProperty("class", "update-button")
        self.owner_login_button.setProperty("dynamic_css", "true")
        owner_layout.addWidget(self.owner_login_button)
        
        owner_layout.addStretch()

        # Icono de acceso web (HTTPS) en esquina inferior izquierda usando SVG
        try:
            svg_path = resource_path(os.path.join('assets', 'web-icon.svg'))
            icon = QIcon(svg_path)
            # Fallback a PNG si el SVG no puede cargarse
            if icon.isNull():
                png_path = resource_path(os.path.join('assets', 'web-icon.png'))
                icon = QIcon(png_path)

            web_icon_button = QToolButton()
            web_icon_button.setIcon(icon)
            web_icon_button.setIconSize(QSize(70, 70))
            web_icon_button.setAutoRaise(True)
            if get_public_tunnel_enabled():
                web_icon_button.setToolTip("Abrir Dashboard Web (público)")
                # Abrir directamente la URL pública configurada (Railway)
                web_icon_button.clicked.connect(lambda: QDesktopServices.openUrl(QUrl(get_webapp_base_url())))
            else:
                web_icon_button.setEnabled(False)
                web_icon_button.setToolTip("Túnel público deshabilitado por configuración")
            owner_layout.addWidget(web_icon_button, 0, Qt.AlignmentFlag.AlignLeft)
        except Exception:
            # Fallback: crear botón deshabilitado si ocurre un error
            web_icon_button = QToolButton()
            web_icon_button.setEnabled(False)
            web_icon_button.setToolTip("Icono web no disponible")
            owner_layout.addWidget(web_icon_button, 0, Qt.AlignmentFlag.AlignLeft)

        # Pestaña de Profesor
        profesor_widget = QWidget()
        profesor_widget.setObjectName("login_card")
        profesor_widget.setProperty("dynamic_css", "true")
        profesor_layout = QVBoxLayout(profesor_widget)
        profesor_layout.setSpacing(15)
        profesor_layout.setContentsMargins(15, 15, 15, 15)
        profesor_title = QLabel("🎓 Acceso de Profesor")
        profesor_title.setObjectName("tab_title")
        profesor_title.setAlignment(Qt.AlignmentFlag.AlignCenter)
        profesor_layout.addWidget(profesor_title)
        # Divisor sutil bajo el título de la pestaña Profesor
        profesor_divider = QFrame()
        profesor_divider.setObjectName("section_divider")
        profesor_divider.setFrameShape(QFrame.Shape.HLine)
        profesor_divider.setFrameShadow(QFrame.Shadow.Sunken)
        profesor_divider.setProperty("dynamic_css", "true")
        profesor_layout.addWidget(profesor_divider)
        
        self.profesor_selector = QComboBox()
        self.profesor_selector.setObjectName("login_combo")
        profesor_selector_label = QLabel("👤 Seleccionar Profesor:")
        profesor_selector_label.setProperty("class", "panel_label")
        profesor_selector_label.setProperty("dynamic_css", "true")
        profesor_selector_label.setAlignment(Qt.AlignmentFlag.AlignLeft)
        profesor_selector_label.setWordWrap(True)
        profesor_selector_label.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Fixed)
        profesor_selector_label.setStyleSheet("margin: 6px 0 2px 2px; padding: 0; font-weight: 700;")
        profesor_selector_label.setVisible(True)
        try:
            profesor_selector_label.setContentsMargins(0, 6, 0, 8)
            profesor_selector_label.setMinimumHeight(profesor_selector_label.fontMetrics().height() + 12)
        except Exception:
            pass
        profesor_layout.addWidget(profesor_selector_label)
        profesor_layout.addWidget(self.profesor_selector)
        
        self.profesor_pin_input = QLineEdit()
        self.profesor_pin_input.setEchoMode(QLineEdit.EchoMode.Password)
        self.profesor_pin_input.setMaxLength(4)
        self.profesor_pin_input.setPlaceholderText("PIN de 4 dígitos")
        self.profesor_pin_input.setObjectName("login_input")
        self.profesor_pin_input.setClearButtonEnabled(True)
        pin_regex = QRegularExpression("^[0-9]{0,4}$")
        self.profesor_pin_input.setValidator(QRegularExpressionValidator(pin_regex, self))
        # Toggle de visibilidad para PIN de profesor
        self._profesor_pin_visible = False
        self._profesor_toggle_action = self.profesor_pin_input.addAction(self._eye_off_icon, QLineEdit.ActionPosition.TrailingPosition)
        self._profesor_toggle_action.triggered.connect(lambda: self._toggle_password_visibility(self.profesor_pin_input, 'profesor'))
        profesor_pin_label = QLabel("🔢 PIN:")
        profesor_pin_label.setProperty("class", "panel_label")
        profesor_pin_label.setProperty("dynamic_css", "true")
        profesor_pin_label.setAlignment(Qt.AlignmentFlag.AlignLeft)
        profesor_pin_label.setWordWrap(False)
        profesor_pin_label.setSizePolicy(QSizePolicy.Policy.Preferred, QSizePolicy.Policy.Fixed)
        profesor_pin_label.setStyleSheet("margin: 10px 0 2px 2px; padding: 0; font-weight: 700;")
        profesor_pin_label.setVisible(True)
        profesor_layout.addWidget(profesor_pin_label)
        profesor_layout.addWidget(self.profesor_pin_input)
        
        self.profesor_login_button = QPushButton("🔑 Iniciar Sesión como Profesor")
        self.profesor_login_button.setObjectName("primary_login_button")
        # Clasificar botón como primario para estilos globales
        self.profesor_login_button.setProperty("class", "update-button")
        self.profesor_login_button.setProperty("dynamic_css", "true")
        profesor_layout.addWidget(self.profesor_login_button)
        
        profesor_layout.addStretch()

        self.tabs.addTab(owner_widget, "👑 Dueño")
        self.tabs.addTab(profesor_widget, "🎓 Profesor")
        # Ajustar botón por defecto y foco según la pestaña activa
        self.tabs.currentChanged.connect(self._on_tab_changed)
        self._on_tab_changed(self.tabs.currentIndex())
        left_layout.addWidget(self.tabs)
        left_layout.addStretch()

        # === COLUMNA DERECHA - INFORMACIÓN DEL GIMNASIO ===
        right_frame = QFrame()
        right_frame.setObjectName("gym_info_panel")
        # Habilitar CSS dinámico en panel derecho
        right_frame.setProperty("dynamic_css", "true")
        # Sombra sutil para elevar el panel derecho
        right_shadow = QGraphicsDropShadowEffect(self)
        right_shadow.setBlurRadius(22)
        right_shadow.setXOffset(0)
        right_shadow.setYOffset(8)
        right_shadow.setColor(QColor(0, 0, 0, 40))
        right_frame.setGraphicsEffect(right_shadow)
        right_layout = QVBoxLayout(right_frame)
        right_layout.setSpacing(12)
        right_layout.setContentsMargins(30, 25, 30, 25)
        # Asegurar que el QFrame pinte su propio fondo para respetar el borde redondeado
        right_frame.setAttribute(Qt.WidgetAttribute.WA_StyledBackground, True)

        self.create_gym_info_section(right_layout)

        # Agregar columnas al layout principal
        main_layout.addWidget(left_frame, 1)
        # Envolver el panel derecho en un QScrollArea para permitir achicar la ventana
        right_scroll = QScrollArea()
        right_scroll.setWidgetResizable(True)
        right_scroll.setFrameShape(QFrame.Shape.NoFrame)
        right_scroll.setVerticalScrollBarPolicy(Qt.ScrollBarPolicy.ScrollBarAsNeeded)
        right_scroll.setHorizontalScrollBarPolicy(Qt.ScrollBarPolicy.ScrollBarAlwaysOff)
        right_scroll.setObjectName("gym_info_scroll")
        # Guardar referencia para aplicar estilos específicos sin sobreescribir el QSS global
        self.right_scroll = right_scroll
        right_scroll.setProperty("dynamic_css", "true")
        # Transparentar el viewport para evitar picos negros en las esquinas redondeadas
        try:
            right_scroll.viewport().setAttribute(Qt.WidgetAttribute.WA_TranslucentBackground, True)
            right_scroll.viewport().setAutoFillBackground(False)
        except Exception:
            pass
        right_scroll.setWidget(right_frame)
        main_layout.addWidget(right_scroll, 1)

        self.load_profesores()
        self.connect_signals()

    def load_gym_data(self):
        """Carga los datos del gimnasio priorizando DB con caché y fallback a archivo."""
        try:
            data = read_gym_data()
            if isinstance(data, dict):
                self.gym_data = dict(data)
                return
        except Exception as e:
            try:
                logging.warning(f"LoginDialog: error leyendo gym data desde utils/DB: {e}")
            except Exception:
                pass
        # Datos por defecto si falla
        self.gym_data = {
            'gym_name': 'Gimnasio',
            'gym_slogan': 'Tu mejor versión te espera',
            'gym_address': 'Dirección no disponible',
            'gym_phone': 'Teléfono no disponible',
            'gym_email': 'Email no disponible',
            'gym_website': 'Website no disponible',
            'facebook': '@gym',
            'instagram': '@gym',
            'twitter': '@gym'
        }

    def _load_gym_data_cached(self):
        """Carga gym_data usando caché en memoria si disponible, con fallback a disco."""
        try:
            cache_key = 'config:gym_data'
            if self._mem_cache:
                cached = None
                try:
                    cached = self._mem_cache.get(cache_key)  # type: ignore
                except Exception:
                    cached = None
                if cached:
                    self.gym_data = dict(cached) if isinstance(cached, dict) else {}
                    return
            # Si no hay caché, cargar y cachear
            self.load_gym_data()
            if self._mem_cache and self.gym_data:
                try:
                    # Guardar por 30 minutos aprox si CacheManager soporta expiración
                    self._mem_cache.set(cache_key, dict(self.gym_data), ttl=1800)  # type: ignore
                except Exception:
                    pass
        except Exception:
            # Fallback silencioso a carga original
            try:
                self.load_gym_data()
            except Exception:
                pass

    def load_branding_config(self):
        """Carga la configuración de branding desde la base de datos (sin bloquear)."""
        # Mantener compatibilidad: ahora delega en la versión asíncrona
        try:
            self._load_branding_config_async(timeout_ms=5000)
        except Exception:
            pass

    def _load_branding_config_async(self, timeout_ms: int = 5000):
        """Obtiene branding_config en un hilo y actualiza estilos al completar.

        Si no completa dentro del timeout, se mantiene la configuración por defecto.
        """
        # Respetar Circuit Breaker: si está abierto, evitar golpear la DB y usar defaults
        try:
            if hasattr(self.db_manager, 'is_circuit_open') and self.db_manager.is_circuit_open():
                state = self.db_manager.get_circuit_state() if hasattr(self.db_manager, 'get_circuit_state') else {}
                try:
                    logging.warning(f"LoginDialog: circuito abierto, omitiendo carga de branding; estado={state}")
                except Exception:
                    pass
                # Mantener configuración por defecto y reintentar luego de un corto delay
                from PyQt6.QtCore import QTimer
                QTimer.singleShot(min(timeout_ms, 3000), lambda: self._load_branding_config_async(timeout_ms))
                return
        except Exception:
            pass
        # Primer intento: intentar leer desde caché en memoria del DatabaseManager
        try:
            cache_key = 'config:branding_config'
            if self._mem_cache:
                cached = None
                try:
                    cached = self._mem_cache.get(cache_key)  # type: ignore
                except Exception:
                    cached = None
                if cached and isinstance(cached, dict):
                    self.branding_config = cached
                    try:
                        logging.info("LoginDialog: branding aplicado desde caché en memoria")
                    except Exception:
                        pass
                    try:
                        self.apply_modern_styling()
                    except Exception:
                        pass
                    # Programar una actualización en background para refrescar caché sin bloquear
                    try:
                        QTimer.singleShot(2000, lambda: self._refresh_branding_cache_background())
                    except Exception:
                        pass
                    return
        except Exception:
            pass
        class _BrandingThread(QThread):
            def __init__(self, db_manager):
                super().__init__()
                self.db_manager = db_manager
                self.result = None
                self.error = None
            def run(self):
                try:
                    branding_json = self.db_manager.obtener_configuracion('branding_config')
                    self.result = json.loads(branding_json) if branding_json else None
                except Exception as e:
                    self.error = e

        try:
            start_ts = time.time()
            self._branding_thread = _BrandingThread(self.db_manager)
            try:
                self._branding_thread.startTime = start_ts
            except Exception:
                pass
            self._branding_thread.finished.connect(self._on_branding_loaded)
            self._branding_thread.start()

            # Establecer timeout para no bloquear ni esperar indefinidamente
            def _branding_timeout():
                try:
                    if self._branding_thread and self._branding_thread.isRunning():
                        # No interrumpimos el hilo; solo dejamos defaults y desconectamos
                        try:
                            self._branding_thread.finished.disconnect(self._on_branding_loaded)
                        except Exception:
                            pass
                except Exception:
                    pass
            QTimer.singleShot(timeout_ms, _branding_timeout)
            try:
                logging.info("LoginDialog: carga asíncrona de branding inicializada")
            except Exception:
                pass
        except Exception:
            pass

    def _refresh_branding_cache_background(self):
        """Actualiza la caché de branding en segundo plano sin bloquear la UI."""
        try:
            class _RefreshThread(QThread):
                def __init__(self, db_manager, cache, key):
                    super().__init__()
                    self.db_manager = db_manager
                    self.cache = cache
                    self.key = key
                def run(self):
                    try:
                        branding_json = self.db_manager.obtener_configuracion('branding_config')
                        if branding_json:
                            data = json.loads(branding_json)
                            if self.cache:
                                try:
                                    self.cache.set(self.key, data, ttl=1800)
                                except Exception:
                                    pass
                    except Exception:
                        pass
            cache_key = 'config:branding_config'
            _thr = _RefreshThread(self.db_manager, self._mem_cache, cache_key)
            _thr.start()
        except Exception:
            pass

    def _on_branding_loaded(self):
        try:
            thread = getattr(self, '_branding_thread', None)
            if thread and thread.result:
                try:
                    logging.info("LoginDialog: branding cargado en %.0f ms", (time.time() - thread.startTime) * 1000 if hasattr(thread, 'startTime') else -1)
                except Exception:
                    pass
                self.branding_config = thread.result
                # Reaplicar estilos con la nueva configuración
                try:
                    self.apply_modern_styling()
                except Exception:
                    pass
        except Exception:
            pass

    def create_gym_info_section(self, layout):
        """Crea la sección de información del gimnasio"""
        # Logo del gimnasio (con logo por defecto si corresponde)
        if self.branding_config.get('show_logo', True):
            logo_label = QLabel()
            logo_label.setObjectName("gym_logo")
            logo_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
            # Habilitar CSS dinámico en logo
            logo_label.setProperty("dynamic_css", "true")
            try:
                pixmap = None
                branding_path = self.branding_config.get('main_logo_path')
                if branding_path:
                    tmp = QPixmap(branding_path)
                    if not tmp.isNull():
                        pixmap = tmp
                
                if pixmap is None:
                    fallback_path = resource_path(os.path.join('assets', 'gym_logo.png'))
                    if os.path.exists(fallback_path):
                        tmp = QPixmap(fallback_path)
                        if not tmp.isNull():
                            pixmap = tmp
                
                if pixmap is not None:
                    logo_size = self.branding_config.get('logo_size', 100)
                    scaled_size = int(120 * (logo_size / 100))
                    scaled_pixmap = pixmap.scaled(
                        scaled_size, scaled_size,
                        Qt.AspectRatioMode.KeepAspectRatio,
                        Qt.TransformationMode.SmoothTransformation
                    )
                    logo_label.setPixmap(scaled_pixmap)
                    # Sombra sutil para el logo
                    logo_shadow = QGraphicsDropShadowEffect(self)
                    logo_shadow.setBlurRadius(18)
                    logo_shadow.setXOffset(0)
                    logo_shadow.setYOffset(4)
                    logo_shadow.setColor(QColor(0, 0, 0, 50))
                    logo_label.setGraphicsEffect(logo_shadow)
                    layout.addWidget(logo_label)
            except Exception as e:
                print(f"Error cargando logo: {e}")

        # Nombre del gimnasio 🏋️ 
        gym_name = QLabel(f"{self.gym_data.get('gym_name', 'Gimnasio')}")
        gym_name.setObjectName("gym_name_display")
        gym_name.setAlignment(Qt.AlignmentFlag.AlignCenter)
        gym_name.setProperty("dynamic_css", "true")
        try:
            gym_font = gym_name.font()
            try:
                gym_font.setPixelSize(34)
            except Exception:
                gym_font.setPointSize(22)
            gym_font.setWeight(QFont.Weight.Bold)
            gym_name.setFont(gym_font)
        except Exception:
            pass
        gym_name.setStyleSheet("font-size: 26px; font-weight: 800; margin: 12px 0 8px 0; padding: 0;")
        layout.addWidget(gym_name)

        # Slogan
        gym_slogan = QLabel(self.gym_data.get('gym_slogan', 'Tu mejor versión te espera'))
        gym_slogan.setObjectName("gym_slogan_display")
        gym_slogan.setAlignment(Qt.AlignmentFlag.AlignCenter)
        gym_slogan.setProperty("dynamic_css", "true")
        layout.addWidget(gym_slogan)

        layout.addSpacing(20)

        # Divisor sutil antes de Información de Contacto
        top_info_divider = QFrame()
        top_info_divider.setObjectName("section_divider")
        top_info_divider.setFrameShape(QFrame.Shape.HLine)
        top_info_divider.setFrameShadow(QFrame.Shadow.Sunken)
        top_info_divider.setProperty("dynamic_css", "true")
        layout.addWidget(top_info_divider)

        # Información de contacto
        contact_title = QLabel("Información de Contacto")
        contact_title.setObjectName("section_title")
        contact_title.setAlignment(Qt.AlignmentFlag.AlignLeft)
        contact_title.setProperty("dynamic_css", "true")
        contact_title.setProperty("class", "panel_label")
        layout.addWidget(contact_title)

        # Dirección
        address_label = QLabel(f"📍 {self.gym_data.get('gym_address', 'Dirección no disponible')}")
        address_label.setObjectName("contact_info")
        address_label.setWordWrap(True)
        address_label.setProperty("dynamic_css", "true")
        layout.addWidget(address_label)

        # Teléfono
        phone_label = QLabel(f"📞 {self.gym_data.get('gym_phone', 'Teléfono no disponible')}")
        phone_label.setObjectName("contact_info")
        phone_label.setProperty("dynamic_css", "true")
        layout.addWidget(phone_label)

        # Email
        email_label = QLabel(f"✉️ {self.gym_data.get('gym_email', 'Email no disponible')}")
        email_label.setObjectName("contact_info")
        email_label.setProperty("dynamic_css", "true")
        layout.addWidget(email_label)

        # Website
        website_label = QLabel(f"🌐 {self.gym_data.get('gym_website', 'Website no disponible')}")
        website_label.setObjectName("contact_info")
        website_label.setProperty("dynamic_css", "true")
        layout.addWidget(website_label)

        layout.addSpacing(8)

        # Divisor sutil entre Contacto y Redes Sociales
        middle_divider = QFrame()
        middle_divider.setObjectName("section_divider")
        middle_divider.setFrameShape(QFrame.Shape.HLine)
        middle_divider.setFrameShadow(QFrame.Shadow.Sunken)
        middle_divider.setProperty("dynamic_css", "true")
        layout.addWidget(middle_divider)

        # Redes sociales
        social_title = QLabel("Redes sociales")
        social_title.setObjectName("section_title")
        social_title.setAlignment(Qt.AlignmentFlag.AlignLeft)
        social_title.setProperty("dynamic_css", "true")
        social_title.setProperty("class", "panel_label")
        layout.addWidget(social_title)

        social_layout = QVBoxLayout()
        social_layout.setSpacing(6)

        # Facebook
        facebook_label = QLabel(f"📘 Facebook: {self.gym_data.get('facebook', '@gym')}")
        facebook_label.setObjectName("social_info")
        facebook_label.setProperty("dynamic_css", "true")
        social_layout.addWidget(facebook_label)

        # Instagram
        instagram_label = QLabel(f"📷 Instagram: {self.gym_data.get('instagram', '@gym')}")
        instagram_label.setObjectName("social_info")
        instagram_label.setProperty("dynamic_css", "true")
        social_layout.addWidget(instagram_label)

        # Twitter
        twitter_label = QLabel(f"🐦 Twitter: {self.gym_data.get('twitter', '@gym')}")
        twitter_label.setObjectName("social_info")
        twitter_label.setProperty("dynamic_css", "true")
        social_layout.addWidget(twitter_label)

        layout.addLayout(social_layout)
        layout.addSpacing(10)
        layout.addStretch()

    def apply_modern_styling(self):
        """Aplica estilos modernos usando el sistema de branding dinámico (versión del login antiguo)."""
        primary_color = self.branding_config.get('primary_color', '#3498db')
        secondary_color = self.branding_config.get('secondary_color', '#2ecc71')
        accent_color = self.branding_config.get('accent_color', '#e74c3c')
        background_color = self.branding_config.get('background_color', '#ffffff')
        alt_background_color = self.branding_config.get('alt_background_color', '#f8f9fa')
        text_color = self.branding_config.get('text_color', '#2c3e50')
        main_font = self.branding_config.get('main_font', 'Arial')

        # Generar colores hover automáticamente
        def darken_color(color_hex, factor=0.8):
            try:
                color = QColor(color_hex)
                h, s, v, a = color.getHsv()
                v = int(v * factor)
                color.setHsv(h, s, v, a)
                return color.name()
            except Exception:
                return color_hex

        # Helpers de contraste y ajuste de brillo
        def get_contrasting_text_color(color_hex: str) -> str:
            """Elige el color de texto (blanco/negro) con mayor contraste perceptual."""
            try:
                def _rel_lum(hex_col: str) -> float:
                    q = QColor(hex_col)
                    def _ch(c: int) -> float:
                        c = c / 255.0
                        return (c / 12.92) if (c <= 0.03928) else (((c + 0.055) / 1.055) ** 2.4)
                    r, g, b, _ = q.getRgb()
                    return 0.2126 * _ch(r) + 0.7152 * _ch(g) + 0.0722 * _ch(b)
                L_bg = _rel_lum(color_hex)
                L_white = _rel_lum("#FFFFFF")
                L_black = _rel_lum("#000000")
                def _contrast(L1: float, L2: float) -> float:
                    hi, lo = (L1, L2) if L1 > L2 else (L2, L1)
                    return (hi + 0.05) / (lo + 0.05)
                c_white = _contrast(L_bg, L_white)
                c_black = _contrast(L_bg, L_black)
                return "#FFFFFF" if c_white >= c_black else "#000000"
            except Exception:
                return "#FFFFFF"

        def adjust_brightness(color_hex: str, factor: float) -> str:
            try:
                c = QColor(color_hex)
                h, s, v, a = c.getHsv()
                v = max(0, min(255, int(v * factor)))
                c.setHsv(h, s, v, a)
                return c.name()
            except Exception:
                return color_hex

        # Primero determinar contraste y modo (claro/oscuro) antes de usar is_dark_theme
        auto_text_on_bg = get_contrasting_text_color(background_color)
        auto_text_on_alt = get_contrasting_text_color(alt_background_color)
        auto_text_on_primary = get_contrasting_text_color(primary_color)
        ui_text_color = self.branding_config.get('ui_text_color') or auto_text_on_bg
        is_dark_theme = (auto_text_on_bg == "#FFFFFF")

        # Usar colores de hover del branding si existen; si no, oscurecer ligeramente
        primary_hover = self.branding_config.get('primary_hover_color', darken_color(primary_color))
        secondary_hover = self.branding_config.get('secondary_hover_color', darken_color(secondary_color))
        accent_hover = self.branding_config.get('accent_hover_color', darken_color(accent_color))
        # El texto en hover y pressed debe calcularse contra el fondo real de hover/pressed
        auto_text_on_primary_hover = get_contrasting_text_color(primary_hover)
        # Colores de foco ligeramente más brillantes para dar feedback en hover+focus
        primary_focus = adjust_brightness(primary_color, 1.08 if not is_dark_theme else 1.04)
        secondary_focus = adjust_brightness(secondary_color, 1.08 if not is_dark_theme else 1.04)
        is_alt_dark = (auto_text_on_alt == "#FFFFFF")
        border_color = adjust_brightness(alt_background_color, 1.2 if is_alt_dark else 0.85)
        primary_pressed = adjust_brightness(primary_color, 0.8 if is_dark_theme else 0.6)
        auto_text_on_primary_pressed = get_contrasting_text_color(primary_pressed)
        tertiary_bg = adjust_brightness(alt_background_color, 0.9 if is_dark_theme else 1.1)
        auto_text_on_tertiary = get_contrasting_text_color(tertiary_bg)
        # Definir quaternary_bg para evitar NameError en reemplazos
        quaternary_bg = adjust_brightness(tertiary_bg, 0.95 if is_dark_theme else 1.05)

        # Colores adicionales usados por el QSS global
        warning_color = self.branding_config.get('warning_color', '#EBCB8B')
        info_color = self.branding_config.get('info_color', '#88C0D0')
        muted_color = self.branding_config.get('muted_color') or auto_text_on_alt

        # Guardar variables clave para reinyectar estilos al mostrarse
        try:
            self._primary_color = primary_color
            self._primary_hover = primary_hover
            self._primary_pressed = primary_pressed
            self._auto_text_on_primary = auto_text_on_primary
            self._auto_text_on_primary_hover = auto_text_on_primary_hover
            self._auto_text_on_primary_pressed = auto_text_on_primary_pressed
        except Exception:
            pass

        # Ruta para flecha del QComboBox
        arrow_down_path = resource_path(os.path.join("assets", "arrow_down.svg"))
        if not os.path.exists(arrow_down_path):
            arrow_down_path = ""
        arrow_down_path = arrow_down_path.replace("\\", "/")

        modern_style = f"""
        /* Estilo general del diálogo */
        QDialog {{
            background: qlineargradient(x1:0, y1:0, x2:1, y2:1, 
                       stop:0 {background_color}, stop:1 {alt_background_color});
            font-family: '{main_font}', Arial, sans-serif;
            color: {ui_text_color};
        }}
        
        /* Panel izquierdo - Login */
        QFrame#login_left_panel {{
            background: qlineargradient(x1:0, y1:0, x2:0, y2:1, 
                       stop:0 {background_color}, stop:1 {alt_background_color});
            border: 1px solid {border_color};
            border-radius: 16px;
        }}
        
        /* Panel derecho - Información del gimnasio */
        QFrame#gym_info_panel {{
            background: {alt_background_color};
            color: {auto_text_on_alt};
            border-radius: 16px;
            border: 1px solid {border_color};
        }}
        
        /* Scroll area del panel derecho para respetar esquinas redondeadas */
        QScrollArea#gym_info_scroll {{
            background: transparent;
            border: none;
        }}
        QScrollArea#gym_info_scroll > QWidget#qt_scrollarea_viewport {{
            background: transparent;
            border: none;
        }}
        
        /* Títulos de bienvenida */
        QLabel#welcome_title {{
            font-size: 36px;
            font-weight: bold;
            color: {primary_color};
            margin: 10px 0;
            padding: 10px;
        }}
        
        QLabel#welcome_subtitle {{
            font-size: 14px;
            color: {ui_text_color};
            margin-bottom: 8px;
            opacity: 0.8;
        }}
        
        /* Barra de acento bajo el título */
        QFrame#accent_bar {{
            background: {primary_color};
            min-height: 3px;
            max-height: 3px;
            border-radius: 2px;
            margin: 2px 0 12px 0;
        }}
        
        /* Divisores sutiles de sección */
        QFrame#section_divider {{
            background: transparent;
            border: none;
            border-top: 1px solid {border_color};
            margin: 10px 0 14px 0;
            min-height: 1px;
            max-height: 1px;
        }}
        
        /* Tabs de login */
        QTabWidget#login_tabs {{
            background: transparent;
            border: none;
        }}
        
        QTabWidget#login_tabs::pane {{
            border: 2px solid {primary_color};
            border-radius: 14px;
            background: {background_color};
            margin-top: 10px;
        }}
        
        QTabWidget#login_tabs QTabBar::tab {{
            background: qlineargradient(x1:0, y1:0, x2:0, y2:1, 
                       stop:0 {alt_background_color}, stop:1 {background_color});
            border: 2px solid {primary_color};
            border-bottom: none;
            border-radius: 8px 8px 0 0;
            padding: 12px 20px;
            margin-right: 2px;
            font-weight: bold;
            color: {auto_text_on_bg};
            min-width: 120px;
        }}
        
        QTabWidget#login_tabs QTabBar::tab:selected {{
            background: qlineargradient(x1:0, y1:0, x2:0, y2:1, 
                       stop:0 {primary_color}, stop:1 {secondary_color});
            color: white;
            border-bottom: 2px solid {primary_color};
        }}

        QTabWidget#login_tabs QTabBar::tab:hover:!selected {{
            background: qlineargradient(x1:0, y1:0, x2:0, y2:1, 
                       stop:0 {primary_hover}, stop:1 {alt_background_color});
            color: {auto_text_on_bg};
            border-color: {primary_hover};
        }}
        /* Suavizar feedback al pasar sobre la pestaña activa */
        QTabWidget#login_tabs QTabBar::tab:selected:hover {{
            background: qlineargradient(x1:0, y1:0, x2:0, y2:1,
                       stop:0 {primary_focus}, stop:1 {secondary_focus});
            color: white;
            border-bottom: 2px solid {primary_focus};
        }}
        /* Estado deshabilitado para consistencia visual */
        QTabWidget#login_tabs QTabBar::tab:disabled {{
            background: {tertiary_bg};
            color: {auto_text_on_tertiary};
            border-color: {border_color};
        }}
        
        /* Títulos de las pestañas */
        QLabel#tab_title {{
            font-size: 16px;
            font-weight: bold;
            color: {primary_color};
            margin: 10px 0;
            padding: 6px;
        }}
        
        /* Inputs de login */
        QLineEdit#login_input {{
            padding: 12px 15px;
            border: 2px solid {alt_background_color};
            border-radius: 8px;
            font-size: 14px;
            background-color: {background_color};
            color: {auto_text_on_bg};
            selection-background-color: {primary_color};
        }}

        QLineEdit#login_input:focus {{
             border: 2px solid {primary_color};
             background-color: {background_color};
         }}
         
         QLineEdit#login_input:hover {{
             border: 2px solid {primary_hover};
             background-color: {background_color};
             color: {auto_text_on_bg};
         }}
         
         /* Combinación hover+focus (ligeramente más brillante) */
         QLineEdit#login_input:hover:focus {{
             border: 2px solid {primary_focus};
         }}
         
         /* Estado deshabilitado */
         QLineEdit#login_input:disabled {{
             background-color: {tertiary_bg};
             color: {auto_text_on_tertiary};
             border: 2px dashed {border_color};
         }}
        
        /* ComboBox */
        QComboBox#login_combo {{
            padding: 12px 15px;
            border: 2px solid {alt_background_color};
            border-radius: 8px;
            font-size: 14px;
            background-color: {background_color};
            color: {auto_text_on_bg};
            min-height: 20px;
        }}
        
        QComboBox#login_combo:focus {{
            border: 2px solid {primary_color};
        }}
        
        QComboBox#login_combo:hover {{
            border: 2px solid {primary_hover};
            background-color: {background_color};
            color: {auto_text_on_bg};
        }}
        
        /* Combinación hover+focus */
        QComboBox#login_combo:hover:focus {{
            border: 2px solid {primary_focus};
        }}
        
        /* Estado deshabilitado */
        QComboBox#login_combo:disabled {{
            background-color: {tertiary_bg};
            color: {auto_text_on_tertiary};
            border: 2px dashed {border_color};
        }}
        
        QComboBox#login_combo QAbstractItemView {{
            background-color: {background_color};
            color: {auto_text_on_bg};
            selection-background-color: {primary_color};
            selection-color: {auto_text_on_primary};
            border: 1px solid {border_color};
        }}
        
        QComboBox#login_combo::drop-down {{
            border: none;
            width: 30px;
        }}
        
        QComboBox#login_combo::down-arrow {{
            image: url("{arrow_down_path}");
            border: none;
            width: 16px;
            height: 16px;
        }}
        
        /* Botones de login */
        QPushButton#primary_login_button {{
            background: qlineargradient(x1:0, y1:0, x2:0, y2:1, 
                       stop:0 {primary_color}, stop:1 {secondary_color});
            color: {auto_text_on_primary};
            border: none;
            border-radius: 10px;
            padding: 15px 25px;
            font-size: 16px;
            font-weight: bold;
            margin: 10px 0;
        }}
        
        QPushButton#primary_login_button:hover {{
             background: qlineargradient(x1:0, y1:0, x2:0, y2:1, 
                        stop:0 {primary_hover}, stop:1 {secondary_hover});
             color: {auto_text_on_primary_hover};
         }}
         
         QPushButton#primary_login_button:pressed {{
             background: qlineargradient(x1:0, y1:0, x2:0, y2:1, 
                        stop:0 {secondary_color}, stop:1 {primary_color});
             color: {auto_text_on_primary_pressed};
         }}
         
         QPushButton#primary_login_button:disabled {{
             background: {quaternary_bg};
             color: {auto_text_on_tertiary};
         }}

        /* Información del gimnasio */
        QLabel#gym_name_display {{
            font-size: 34px;
            font-weight: bold;
            color: {primary_color};
            margin: 12px 0 8px 0;
            padding: 0;
            text-align: center;
        }}
        
        QLabel#gym_slogan_display {{
            font-size: 14px;
            color: {auto_text_on_bg};
            font-style: italic;
            margin-bottom: 18px;
            opacity: 0.8;
            text-align: center;
        }}
        
        QLabel#section_title {{
            font-size: 16px;
            font-weight: 700;
            color: {primary_color};
            margin: 18px 0 8px 0;
            padding: 8px 12px;
            background: qlineargradient(x1:0, y1:0, x2:1, y2:0, stop:0 {alt_background_color}, stop:1 {tertiary_bg});
            border-radius: 10px;
            border-left: 4px solid {primary_color};
            border-bottom: none;
        }}
        
        QLabel#contact_info, QLabel#social_info {{
            font-size: 13px;
            color: {ui_text_color};
            margin: 6px 0;
            padding: 10px 14px;
            background: {background_color};
            border-radius: 10px;
            border-left: 3px solid {primary_color};
            border: 1px solid {border_color};
        }}
        
        QLabel#gym_logo {{
            margin: 12px 0;
            padding: 0px;
            background: transparent;
            border: none;
        }}
        
        /* Labels generales */
        QLabel {{
            color: {ui_text_color};
            font-family: '{main_font}', Arial, sans-serif;
        }}
        
        /* Labels en el panel derecho */
        QFrame#gym_info_panel QLabel {{
            color: {auto_text_on_alt};
        }}
        
        QFrame#gym_info_panel QLabel#gym_name_display {{
            color: {primary_color};
        }}
        
        QFrame#gym_info_panel QLabel#section_title {{
            color: {primary_color};
        }}
        """

        # Estilos adicionales para hover/pressed que usan variables dinámicas del branding
        extra_style = f"""
        /* Botones tipo herramienta (QToolButton) */
        QToolButton {{
            background-color: transparent;
            color: {ui_text_color};
            border: 1px solid transparent;
            border-radius: 6px;
            padding: 4px;
        }}
        QToolButton:hover {{
            background-color: {alt_background_color};
            color: {primary_color};
            border-color: {primary_color};
        }}
        QToolButton:pressed {{
            background-color: {tertiary_bg};
            color: {primary_pressed};
            border-color: {primary_pressed};
        }}
        QToolButton:disabled {{
            color: {auto_text_on_tertiary};
            border-color: transparent;
        }}

        /* Labels tipo enlace */
        QLabel#link_label {{
            color: {primary_color};
        }}
        QLabel#link_label:hover {{
            color: {primary_hover};
        }}

        /* Campos de texto: hover y focus */
        QLineEdit {{
            border: 1px solid {border_color};
            border-radius: 6px;
        }}
        QLineEdit:hover {{
            border-color: {primary_color};
        }}
        QLineEdit:focus {{
            border-color: {primary_hover};
        }}

        /* CheckBox con estados dinámicos */
        QCheckBox {{
            color: {auto_text_on_bg};
        }}
        QCheckBox::indicator {{
            border: 1px solid {border_color};
            border-radius: 3px;
            background: {alt_background_color};
        }}
        QCheckBox::indicator:hover {{
            border-color: {primary_color};
        }}
        QCheckBox::indicator:checked {{
            background: {primary_color};
            border-color: {primary_color};
        }}
        QCheckBox::indicator:pressed {{
            background: {primary_pressed};
            border-color: {primary_pressed};
        }}
        """

        # Estilos adicionales específicos para la tarjeta de login
        login_card_style = f"""
        /* Tarjeta de login */
        QWidget#login_card {{
            background: {background_color};
            border: 1px solid {border_color};
            border-radius: 12px;
            padding: 10px;
        }}
        
        /* Reducir tamaño de labels dentro del recuadro de login, manteniendo negrita */
        QWidget#login_card QLabel[class="panel_label"] {{
            font-size: 15px;
            font-weight: bold;
            padding: 2px 0 4px 0;
        }}
        """

        # Cargar y procesar el QSS base para que el LoginDialog respete el sistema global
        try:
            base_dir = os.path.dirname(os.path.dirname(__file__))
            qss_path = os.path.join(base_dir, 'styles', 'style.qss')
            base_qss = ''
            if os.path.exists(qss_path):
                with open(qss_path, 'r', encoding='utf-8') as f:
                    base_qss = f.read()

            replacements = {
                'VAR_BG_PRIMARY': background_color,
                'VAR_BG_SECONDARY': alt_background_color,
                'VAR_BG_TERTIARY': background_color,
                'VAR_BG_QUATERNARY': background_color,
                'VAR_PRIMARY_COLOR': primary_color,
                'VAR_PRIMARY_HOVER_COLOR': primary_hover,
                'VAR_SECONDARY_HOVER_COLOR': secondary_hover,
                'VAR_ACCENT_HOVER_COLOR': accent_hover,
                'VAR_PRIMARY_COLOR_PRESSED': primary_pressed,
                'VAR_BORDER_PRIMARY': border_color,
                'VAR_TEXT_PRIMARY': ui_text_color,
                'VAR_TEXT_SECONDARY': auto_text_on_alt,
                'VAR_TEXT_TERTIARY': auto_text_on_bg,
                'VAR_TEXT_MUTED': muted_color,
                'VAR_TEXT_ON_BRAND': auto_text_on_primary,
                'VAR_ACCENT_SUCCESS': secondary_color,
                'VAR_ACCENT_DANGER': accent_color,
                'VAR_ACCENT_WARNING': warning_color,
                'VAR_ACCENT_INFO': info_color,
                'VAR_FONT_FAMILY': main_font,
                '"Segoe UI"': f'"{main_font}"',
                'Segoe UI': main_font,
                '#252A35': background_color,
                '#2E3440': alt_background_color,
                '#3B4252': background_color,
                '#434C5E': background_color,
                '#5E81AC': primary_color,
                '#81A1C1': primary_hover,
                '#4C6A94': primary_pressed,
                '#ECEFF4': auto_text_on_bg,
                '#D8DEE9': auto_text_on_alt,
                '#B8C5D1': auto_text_on_bg,
                '#4C566A': border_color,
            }
            processed_qss = base_qss
            try:
                processed_qss = processed_qss.replace('VAR_PRIMARY_COLOR_PRESSED', primary_pressed)
            except Exception:
                pass
            for k, v in replacements.items():
                try:
                    processed_qss = processed_qss.replace(k, v)
                except Exception:
                    pass
            complete_css = processed_qss + "\n" + modern_style + "\n" + extra_style + "\n" + login_card_style
            self.setStyleSheet(complete_css)
        except Exception:
            # Si algo falla, aplicar al menos el estilo moderno local
            self.setStyleSheet(modern_style + "\n" + login_card_style)

        # Refuerzo de estilo específico en los botones de login para evitar overrides accidentales
        try:
            explicit_btn_style = f"""
            QPushButton#primary_login_button {{
                background-color: {primary_color};
                color: {auto_text_on_primary};
                border: 1px solid {primary_color};
            }}
            QPushButton#primary_login_button:hover {{
                background-color: {primary_hover};
                color: {auto_text_on_primary_hover};
                border: 1px solid {primary_hover};
            }}
            QPushButton#primary_login_button:pressed {{
                background-color: {primary_pressed};
                color: {auto_text_on_primary_pressed};
                border: 1px solid {primary_pressed};
            }}
            """
            self.owner_login_button.setStyleSheet(explicit_btn_style)
            self.profesor_login_button.setStyleSheet(explicit_btn_style)
        except Exception:
            pass

    def _apply_login_button_styles(self):
        """Reaplica el estilo de los botones de login en el showEvent para asegurar prioridad."""
        try:
            pc = getattr(self, '_primary_color', None)
            ph = getattr(self, '_primary_hover', None)
            pp = getattr(self, '_primary_pressed', None)
            tp = getattr(self, '_auto_text_on_primary', None)
            tph = getattr(self, '_auto_text_on_primary_hover', tp)
            tpp = getattr(self, '_auto_text_on_primary_pressed', tp)
            if not (pc and ph and pp and tp):
                return
            explicit_btn_style = f"""
            QPushButton#primary_login_button {{
                background-color: {pc};
                color: {tp};
            }}
            QPushButton#primary_login_button:hover {{
                background-color: {ph};
                color: {tph};
            }}
            QPushButton#primary_login_button:pressed {{
                background-color: {pp};
                color: {tpp};
            }}
            """
            if hasattr(self, 'owner_login_button'):
                self.owner_login_button.setStyleSheet(explicit_btn_style)
            if hasattr(self, 'profesor_login_button'):
                self.profesor_login_button.setStyleSheet(explicit_btn_style)
        except Exception:
            pass

    def _create_fallback_eye_icon(self, obscured: bool) -> QIcon:
        try:
            size = 24
            pix = QPixmap(size, size)
            pix.fill(Qt.GlobalColor.transparent)
            painter = QPainter(pix)
            painter.setRenderHint(QPainter.RenderHint.Antialiasing, True)
            eye_color = QColor("#4C566A")
            pupil_color = QColor("#2E3440")
            pen = QPen(eye_color)
            pen.setWidth(2)
            painter.setPen(pen)
            painter.drawEllipse(4, 8, 16, 8)
            painter.setBrush(QBrush(pupil_color))
            painter.drawEllipse(10, 10, 6, 6)
            if obscured:
                slash_pen = QPen(QColor("#e74c3c"))
                slash_pen.setWidth(2)
                painter.setPen(slash_pen)
                painter.drawLine(6, 18, 18, 6)
            painter.end()
            return QIcon(pix)
        except Exception:
            return QIcon()

    def connect_signals(self):
        self.owner_login_button.clicked.connect(self.handle_owner_login)
        self.profesor_login_button.clicked.connect(self.handle_profesor_login)
        self.owner_password_input.returnPressed.connect(self.handle_owner_login)
        self.profesor_pin_input.returnPressed.connect(self.handle_profesor_login)
        # Atajo de teclado: Ctrl+Enter para iniciar sesión según pestaña activa
        self._submit_shortcut = QShortcut(QKeySequence("Ctrl+Return"), self)
        self._submit_shortcut.activated.connect(self._trigger_current_login)
        # Limpiar PIN al cambiar de profesor (evita confusiones)
        try:
            self.profesor_selector.currentIndexChanged.connect(lambda _: self.profesor_pin_input.clear())
        except Exception:
            pass

    def _trigger_current_login(self):
        try:
            current = self.tabs.currentIndex() if hasattr(self, 'tabs') else 0
            if current == 0:
                self.handle_owner_login()
            else:
                self.handle_profesor_login()
        except Exception:
            try:
                w = self.owner_password_input if (hasattr(self, 'tabs') and self.tabs.currentIndex() == 0) else self.profesor_pin_input
                self._shake_widget(w)
            except Exception:
                pass

    def _toggle_password_visibility(self, line_edit: QLineEdit, role: str):
        try:
            if role == 'owner':
                self._owner_password_visible = not getattr(self, '_owner_password_visible', False)
                if self._owner_password_visible:
                    line_edit.setEchoMode(QLineEdit.EchoMode.Normal)
                    if hasattr(self, '_owner_toggle_action'):
                        self._owner_toggle_action.setIcon(self._eye_icon)
                else:
                    line_edit.setEchoMode(QLineEdit.EchoMode.Password)
                    if hasattr(self, '_owner_toggle_action'):
                        self._owner_toggle_action.setIcon(self._eye_off_icon)
            else:
                self._profesor_pin_visible = not getattr(self, '_profesor_pin_visible', False)
                if self._profesor_pin_visible:
                    line_edit.setEchoMode(QLineEdit.EchoMode.Normal)
                    if hasattr(self, '_profesor_toggle_action'):
                        self._profesor_toggle_action.setIcon(self._eye_icon)
                else:
                    line_edit.setEchoMode(QLineEdit.EchoMode.Password)
                    if hasattr(self, '_profesor_toggle_action'):
                        self._profesor_toggle_action.setIcon(self._eye_off_icon)
        except Exception:
            pass

    def _on_tab_changed(self, index: int):
        try:
            if hasattr(self, 'owner_login_button') and hasattr(self, 'profesor_login_button'):
                self.owner_login_button.setDefault(index == 0)
                self.profesor_login_button.setDefault(index != 0)
            if index == 0 and hasattr(self, 'owner_password_input'):
                self.owner_password_input.setFocus()
            else:
                # Prefer focus on PIN if selector is populated
                if hasattr(self, 'profesor_selector') and self.profesor_selector.count() > 0:
                    if hasattr(self, 'profesor_pin_input'):
                        self.profesor_pin_input.setFocus()
                elif hasattr(self, 'profesor_selector'):
                    self.profesor_selector.setFocus()
        except Exception:
            pass

    def _shake_widget(self, widget):
        try:
            if widget is None:
                return
            start_pos = widget.pos()
            seq = QSequentialAnimationGroup(self)
            for dx in (-8, 8, -6, 6, -4, 4, 0):
                anim = QPropertyAnimation(widget, b"pos", self)
                anim.setDuration(40)
                anim.setEasingCurve(QEasingCurve.Type.OutQuad)
                anim.setStartValue(start_pos + QPoint(-dx, 0))
                anim.setEndValue(start_pos + QPoint(dx, 0))
                seq.addAnimation(anim)
            def restore():
                widget.move(start_pos)
            seq.finished.connect(restore)
            seq.start()
        except Exception:
            pass

    def showEvent(self, event):
        # Recalcular límites al mostrarse en función de la pantalla
        try:
            screen = QApplication.primaryScreen()
            if screen:
                geom = screen.availableGeometry()
                screen_h = geom.height()
                # Limitar altura máxima para no sobrepasar la pantalla
                self.setMaximumHeight(max(360, screen_h - 40))
                # Tomar tamaño natural (sizeHint) y limitar por la altura disponible
                try:
                    hint = self.sizeHint()
                    new_h = min(hint.height(), self.maximumHeight())
                    new_w = max(hint.width(), self.minimumWidth())
                    self.resize(new_w, new_h)
                except Exception:
                    pass
                if self.height() > self.maximumHeight():
                    self.resize(self.width(), self.maximumHeight())
                # Mover al borde superior y centrar horizontalmente
                try:
                    x = geom.x() + (geom.width() - self.width()) // 2
                    y = geom.y()
                    self.move(x, y)
                except Exception:
                    pass
        except Exception:
            pass
        # Reinyectar estilos de botones para asegurar hover correcto
        try:
            self._apply_login_button_styles()
        except Exception:
            pass
        return super().showEvent(event)

    def load_profesores(self):
        """Interfaz compat: ahora inicia carga asíncrona con placeholder."""
        try:
            self._load_profesores_async(timeout_ms=8000)
        except Exception as e:
            try:
                QMessageBox.critical(self, "Error", f"No se pudo iniciar la carga de profesores: {e}")
            except Exception:
                pass

    def _load_profesores_async(self, timeout_ms: int = 8000):
        self.profesor_selector.clear()
        # Placeholder inmediato para evitar bloqueo visual
        try:
            self.profesor_selector.addItem("Cargando profesores...")
            self.profesor_selector.setEnabled(False)
            self.profesor_pin_input.setEnabled(False)
            self.profesor_login_button.setEnabled(False)
        except Exception:
            pass
        # Fallback inmediato: usar lista cacheada si existe para evitar bloqueo inicial
        try:
            cached = None
            cm = getattr(self.db_manager, 'cache_manager', None)
            if cm is not None and hasattr(cm, 'get'):
                cached = cm.get('profesores_basico')
            if cached:
                self.profesor_selector.clear()
                for prof in cached:
                    try:
                        self.profesor_selector.addItem(prof.get('nombre', 'Profesor'), userData=prof)
                    except Exception:
                        self.profesor_selector.addItem(str(prof))
                self.profesor_selector.setEnabled(True)
                self.profesor_pin_input.setEnabled(True)
                self.profesor_login_button.setEnabled(True)
                try:
                    logging.info("LoginDialog: profesores cargados desde caché")
                except Exception:
                    pass
                return
        except Exception:
            pass
        # Evitar golpear la DB durante la inicialización del DatabaseManager
        try:
            if hasattr(self.db_manager, '_initializing') and self.db_manager._initializing:
                from PyQt6.QtCore import QTimer
                QTimer.singleShot(1000, lambda: self._load_profesores_async(timeout_ms))
                try:
                    logging.info("LoginDialog: inicializando DB, diferimos carga de profesores 1s")
                except Exception:
                    pass
                return
        except Exception:
            pass
        # Respetar Circuit Breaker: si está abierto, diferir la carga sin bloquear la UI
        try:
            if hasattr(self.db_manager, 'is_circuit_open') and self.db_manager.is_circuit_open():
                state = self.db_manager.get_circuit_state() if hasattr(self.db_manager, 'get_circuit_state') else {}
                try:
                    logging.warning(f"LoginDialog: circuito abierto, diferiendo carga de profesores; estado={state}")
                except Exception:
                    pass
                # Mostrar mensaje y, si hay caché, usarla; si no, dejar placeholder
                try:
                    self.profesor_selector.clear()
                    cm = getattr(self.db_manager, 'cache_manager', None)
                    cached = cm.get('profesores_basico') if (cm is not None and hasattr(cm, 'get')) else None
                    if cached:
                        for prof in cached:
                            try:
                                self.profesor_selector.addItem(prof.get('nombre', 'Profesor'), userData=prof)
                            except Exception:
                                self.profesor_selector.addItem(str(prof))
                        self.profesor_selector.setEnabled(True)
                        self.profesor_pin_input.setEnabled(True)
                        self.profesor_login_button.setEnabled(True)
                        try:
                            logging.info("LoginDialog: circuito abierto pero se usó caché de profesores")
                        except Exception:
                            pass
                    else:
                        self.profesor_selector.addItem("Base de datos ocupada; reintentando...")
                except Exception:
                    pass
                from PyQt6.QtCore import QTimer
                delay_ms = min(timeout_ms, 2000)
                QTimer.singleShot(delay_ms, lambda: self._load_profesores_async(timeout_ms))
                return
        except Exception:
            pass

        class _ProfesThread(QThread):
            def __init__(self, db_manager):
                super().__init__()
                self.db_manager = db_manager
                self.result = None
                self.error = None
            def run(self):
                try:
                    # Preferir método ligero si está disponible
                    if hasattr(self.db_manager, 'obtener_profesores_basico'):
                        self.result = self.db_manager.obtener_profesores_basico()
                    else:
                        self.result = self.db_manager.obtener_profesores()
                except Exception as e:
                    self.error = e

        try:
            start_ts = time.time()
            self._profes_thread = _ProfesThread(self.db_manager)
            try:
                self._profes_thread.startTime = start_ts
            except Exception:
                pass
            self._profes_thread.finished.connect(self._on_profes_loaded)
            self._profes_thread.start()

            def _profes_timeout():
                try:
                    if self._profes_thread and self._profes_thread.isRunning():
                        try:
                            self._profes_thread.finished.disconnect(self._on_profes_loaded)
                        except Exception:
                            pass
                        # Mantener placeholder, se actualizará cuando termine el hilo
                    else:
                        pass
                except Exception:
                    pass
            QTimer.singleShot(timeout_ms, _profes_timeout)
            try:
                logging.info("LoginDialog: carga asíncrona de profesores inicializada")
            except Exception:
                pass
        except Exception:
            pass

    def _on_profes_loaded(self):
        try:
            thread = getattr(self, '_profes_thread', None)
            if thread:
                try:
                    logging.info("LoginDialog: profesores cargados en %.0f ms", (time.time() - thread.startTime) * 1000 if hasattr(thread, 'startTime') else -1)
                except Exception:
                    pass
                profesores = thread.result or []
                self.profesor_selector.clear()
                if not profesores:
                    self.profesor_selector.addItem("No hay profesores registrados")
                    self.profesor_selector.setEnabled(False)
                    self.profesor_pin_input.setEnabled(False)
                    self.profesor_login_button.setEnabled(False)
                else:
                    self.profesor_selector.setEnabled(True)
                    self.profesor_pin_input.setEnabled(True)
                    self.profesor_login_button.setEnabled(True)
                    for prof in profesores:
                        try:
                            self.profesor_selector.addItem(prof['nombre'], userData=prof)
                        except Exception:
                            self.profesor_selector.addItem(str(prof.get('nombre', 'Profesor')))
        except Exception:
            # Mantener placeholder si hubo error
            try:
                self.profesor_selector.clear()
                self.profesor_selector.addItem("Error cargando profesores")
                self.profesor_selector.setEnabled(False)
                self.profesor_pin_input.setEnabled(False)
                self.profesor_login_button.setEnabled(False)
            except Exception:
                pass
    
    def _load_profesores_delayed(self):
        """Carga los profesores después de que la inicialización termine"""
        try:
            # Verificar nuevamente si la inicialización terminó
            if hasattr(self.db_manager, '_initializing') and self.db_manager._initializing:
                # Si aún está inicializando, intentar de nuevo en 500ms
                from PyQt6.QtCore import QTimer
                QTimer.singleShot(500, self._load_profesores_delayed)
                return
            
            # Limpiar y cargar profesores
            self.profesor_selector.clear()
            profesores = self.db_manager.obtener_profesores()
            if not profesores:
                self.profesor_selector.addItem("No hay profesores registrados")
                self.profesor_selector.setEnabled(False)
                self.profesor_pin_input.setEnabled(False)
                self.profesor_login_button.setEnabled(False)
            else:
                self.profesor_selector.setEnabled(True)
                self.profesor_pin_input.setEnabled(True)
                self.profesor_login_button.setEnabled(True)
                for prof in profesores:
                    self.profesor_selector.addItem(prof['nombre'], userData=prof)
        except Exception as e:
            # Si hay error, mostrar mensaje y deshabilitar la pestaña de profesor
            self.profesor_selector.clear()
            self.profesor_selector.addItem("Error cargando profesores")
            self.profesor_selector.setEnabled(False)
            self.profesor_pin_input.setEnabled(False)
            self.profesor_login_button.setEnabled(False)

    def handle_owner_login(self):
        # Validar exclusivamente contra la base de datos; usar DEV como último respaldo
        try:
            input_pwd = (self.owner_password_input.text() or "").strip()
            if not input_pwd:
                QMessageBox.warning(self, "Acceso Denegado", "Ingrese la contraseña.")
                return

            # 1) Base de datos (fuente de verdad) con verificación segura
            db_pwd = None
            try:
                if hasattr(self, 'db_manager') and self.db_manager and hasattr(self.db_manager, 'obtener_configuracion'):
                    db_pwd = self.db_manager.obtener_configuracion('owner_password')
            except Exception:
                db_pwd = None

            verified = False
            if db_pwd:
                try:
                    db_pwd_str = str(db_pwd).strip()
                    # Si es bcrypt, verificar de forma segura; si no, retrocompatibilidad texto plano
                    if db_pwd_str.startswith('$2b$') or db_pwd_str.startswith('$2a$'):
                        from security_utils import SecurityUtils
                        verified = SecurityUtils.verify_password(input_pwd, db_pwd_str)
                    else:
                        verified = (input_pwd == db_pwd_str)
                except Exception:
                    verified = False

            # 2) Fallback desarrollador sólo si DB no verifica y no está desactivado
            if not verified:
                try:
                    import os
                    dev_login_disabled = str(os.getenv('DISABLE_DEV_LOGIN', '')).strip().lower() in ('1', 'true', 'yes', 'on')
                except Exception:
                    dev_login_disabled = False
                if not dev_login_disabled:
                    try:
                        from managers import DeveloperManager
                        dev_pwd = getattr(DeveloperManager, 'DEV_PASSWORD', None)
                    except Exception:
                        dev_pwd = None
                    if dev_pwd:
                        verified = (input_pwd == str(dev_pwd).strip())

            if verified:
                self.logged_in_role = "dueño"
                try:
                    self.logged_in_user = {'id': 1, 'rol': 'dueño', 'nombre': 'Dueño'}
                except Exception:
                    self.logged_in_user = None
                try:
                    self.session_id = str(uuid.uuid4())
                    ip_address = socket.gethostbyname(socket.gethostname())
                    user_agent = "GymMS Desktop"
                    user_id = None
                    if isinstance(self.logged_in_user, dict):
                        user_id = self.logged_in_user.get('id') or self.logged_in_user.get('usuario_id')
                    else:
                        user_id = getattr(self.logged_in_user, 'id', None)
                    set_audit_context(user_id=user_id, session_id=self.session_id, ip_address=ip_address, user_agent=user_agent)
                except Exception:
                    pass
                try:
                    QTimer.singleShot(2500, self._resolve_owner_user_post_init)
                except Exception:
                    pass
                try:
                    self._shutdown_threads_before_accept()
                except Exception:
                    pass
                self.accept()
            else:
                QMessageBox.warning(self, "Acceso Denegado", "Contraseña de administrador incorrecta.")
                self.owner_password_input.clear()
        except Exception:
            QMessageBox.warning(self, "Acceso Denegado", "Error al validar la contraseña.")
            try:
                self.owner_password_input.clear()
            except Exception:
                pass

    def _show_owner_password_change_dialog(self):
        """Muestra un diálogo para cambiar la contraseña del Dueño."""
        dlg = QDialog(self)
        dlg.setWindowTitle("Cambiar contraseña de Dueño")
        layout = QVBoxLayout(dlg)
        info = QLabel("Ingrese la contraseña actual y la nueva.")
        layout.addWidget(info)
        old_input = QLineEdit()
        old_input.setEchoMode(QLineEdit.EchoMode.Password)
        old_input.setPlaceholderText("Contraseña actual")
        layout.addWidget(old_input)
        new_input = QLineEdit()
        new_input.setEchoMode(QLineEdit.EchoMode.Password)
        new_input.setPlaceholderText("Nueva contraseña")
        layout.addWidget(new_input)

        confirm_input = QLineEdit()
        confirm_input.setEchoMode(QLineEdit.EchoMode.Password)
        confirm_input.setPlaceholderText("Confirmar nueva contraseña")
        layout.addWidget(confirm_input)
        buttons = QHBoxLayout()
        save_btn = QPushButton("Guardar")
        cancel_btn = QPushButton("Cancelar")
        buttons.addWidget(save_btn)
        buttons.addWidget(cancel_btn)
        layout.addLayout(buttons)

        # Edición permitida: la contraseña se controla en la base de datos (sin bloqueo por ENV)

        def do_save():
            try:
                # Validaciones rápidas en UI
                new_pwd = new_input.text().strip()
                confirm_pwd = confirm_input.text().strip()
                if new_pwd != confirm_pwd:
                    QMessageBox.warning(self, "Error", "La confirmación no coincide.")
                    return

                # Validación de fortaleza de contraseña
                try:
                    from security_utils import SecurityUtils
                    strength = SecurityUtils.validate_password_strength(new_pwd)
                    if not (isinstance(strength, dict) and strength.get('valid')):
                        QMessageBox.warning(
                            self,
                            "Error",
                            "La nueva contraseña es débil. Debe tener mínimo 8 caracteres y combinar mayúsculas, minúsculas, dígitos y símbolos."
                        )
                        return
                except Exception:
                    # Si falla la validación, evitar continuar por seguridad
                    QMessageBox.warning(self, "Error", "No se pudo validar la fortaleza de la contraseña.")
                    return

                old_pwd = old_input.text()

                # Deshabilitar botones mientras corre la operación
                save_btn.setEnabled(False)
                cancel_btn.setEnabled(False)

                def _change_owner_password():
                    # Obtener contraseña actual y actualizar en ambas bases si coincide
                    try:
                        # Fuente de verdad: base de datos. DEV_PASSWORD sólo como último respaldo para leer (si no está desactivado).
                        current = None
                        if hasattr(self, 'db_manager') and self.db_manager:
                            current = self.db_manager.obtener_configuracion('owner_password')
                        if not current:
                            try:
                                import os
                                dev_login_disabled = str(os.getenv('DISABLE_DEV_LOGIN', '')).strip().lower() in ('1', 'true', 'yes', 'on')
                                if not dev_login_disabled:
                                    from managers import DeveloperManager
                                    current = DeveloperManager.DEV_PASSWORD
                            except Exception:
                                current = None
                        if not current:
                            return {'ok': False, 'msg': 'No se pudo obtener la contraseña actual.'}
                        
                        # Verificar contraseña actual usando bcrypt o texto plano (retrocompatibilidad)
                        is_valid = False
                        
                        # Primero intentar verificar con bcrypt
                        if current.startswith('$2b$') or current.startswith('$2a$'):
                            # Es un hash bcrypt
                            from security_utils import SecurityUtils
                            is_valid = SecurityUtils.verify_password(old_pwd, current)
                        else:
                            # Es texto plano (retrocompatibilidad)
                            is_valid = (old_pwd == current)
                        
                        if not is_valid:
                            return {'ok': False, 'msg': 'La contraseña actual no coincide.'}

                        # Hashear la nueva contraseña antes de guardarla
                        from security_utils import SecurityUtils
                        hashed_pwd = SecurityUtils.hash_password(new_pwd)
                        
                        # Actualizar únicamente en la base local de forma segura
                        local_ok = False
                        if hasattr(self, 'db_manager') and self.db_manager:
                            try:
                                local_ok = bool(self.db_manager.actualizar_configuracion('owner_password', hashed_pwd))
                                # Refrescar cachés locales inmediatamente para evitar valores antiguos
                                try:
                                    if hasattr(self.db_manager, 'prefetch_owner_credentials_async'):
                                        self.db_manager.prefetch_owner_credentials_async(ttl_seconds=0)
                                except Exception:
                                    pass
                            except Exception:
                                local_ok = False

                        if not local_ok:
                            return {'ok': False, 'msg': 'No se pudo actualizar la contraseña.'}

                        return {'ok': True}
                    except Exception as e:
                        return {'ok': False, 'msg': f'Error interno: {e}'}

                def _on_done(res):
                    try:
                        if res.get('ok'):
                            QMessageBox.information(self, "Éxito", "Contraseña actualizada.")
                            dlg.accept()
                        else:
                            QMessageBox.warning(self, "Error", res.get('msg', 'No se pudo actualizar la contraseña.'))
                    finally:
                        save_btn.setEnabled(True)
                        cancel_btn.setEnabled(True)

                def _on_error(err):
                    try:
                        QMessageBox.critical(self, "Error", f"Ocurrió un error: {err}")
                    finally:
                        save_btn.setEnabled(True)
                        cancel_btn.setEnabled(True)

                TaskThread(_change_owner_password, on_success=_on_done, on_error=_on_error, parent=self).start()
            except Exception as e:
                QMessageBox.critical(self, "Error", f"Ocurrió un error: {e}")

        save_btn.clicked.connect(do_save)
        cancel_btn.clicked.connect(dlg.reject)
        dlg.exec()

    # Métodos auxiliares movidos fuera del diálogo de cambio de contraseña
    def _resolve_owner_user_post_init(self):
        """Resuelve el usuario 'dueño' de forma no bloqueante usando caché con TTL."""
        try:
            if hasattr(self, 'db_manager') and self.db_manager and hasattr(self.db_manager, 'get_owner_user_cached'):
                user = self.db_manager.get_owner_user_cached(ttl_seconds=600, timeout_ms=1200)
                if user:
                    self.logged_in_user = user
        except Exception:
            pass

    def _shutdown_threads_before_accept(self):
        """Apaga explícitamente los hilos activos del diálogo antes de aceptar."""
        try:
            # Referencia anterior a _proxy_watchdog eliminada
            if hasattr(self, '_branding_thread') and self._branding_thread:
                try:
                    if self._branding_thread.isRunning():
                        try:
                            self._branding_thread.finished.disconnect(self._on_branding_loaded)
                        except Exception:
                            pass
                        try:
                            self._branding_thread.requestInterruption()
                        except Exception:
                            pass
                        try:
                            self._branding_thread.quit()
                        except Exception:
                            pass
                        try:
                            self._branding_thread.wait(800)
                        except Exception:
                            pass
                except Exception:
                    pass
            if hasattr(self, '_profes_thread') and self._profes_thread:
                try:
                    if self._profes_thread.isRunning():
                        try:
                            self._profes_thread.finished.disconnect(self._on_profes_loaded)
                        except Exception:
                            pass
                        try:
                            self._profes_thread.requestInterruption()
                        except Exception:
                            pass
                        try:
                            self._profes_thread.quit()
                        except Exception:
                            pass
                        try:
                            self._profes_thread.wait(800)
                        except Exception:
                            pass
                except Exception:
                    pass
            if hasattr(self, '_refresh_thread') and self._refresh_thread:
                try:
                    if self._refresh_thread.isRunning():
                        try:
                            self._refresh_thread.requestInterruption()
                        except Exception:
                            pass
                        try:
                            self._refresh_thread.quit()
                        except Exception:
                            pass
                        try:
                            self._refresh_thread.wait(800)
                        except Exception:
                            pass
                except Exception:
                    pass
        except Exception:
            pass


    def handle_profesor_login(self):
        import sys
        import logging

        selected_prof_index = self.profesor_selector.currentIndex()
        
        if selected_prof_index < 0:
            QMessageBox.warning(self, "Datos Incompletos", "Seleccione un profesor e ingrese el PIN.")
            return

        selected_prof = self.profesor_selector.itemData(selected_prof_index)
        pin_ingresado = self.profesor_pin_input.text()

        if not selected_prof or not pin_ingresado:
            QMessageBox.warning(self, "Datos Incompletos", "Seleccione un profesor e ingrese el PIN.")
            return

        if self.db_manager.verificar_pin_usuario(selected_prof['usuario_id'], pin_ingresado):
            self.logged_in_role = "profesor"
            self.logged_in_user = selected_prof
            # Establecer contexto de auditoría con el usuario y sesión
            try:
                self.session_id = str(uuid.uuid4())
                ip_address = socket.gethostbyname(socket.gethostname())
                user_agent = "GymMS Desktop"
                set_audit_context(user_id=selected_prof.get('usuario_id'), session_id=self.session_id, ip_address=ip_address, user_agent=user_agent)
            except Exception:
                pass
            
            # Iniciar sesión de trabajo solo para profesores de musculación
            try:
                # Obtener información del profesor para verificar su tipo
                profesor_info = self.db_manager.obtener_profesor_por_usuario_id(selected_prof['usuario_id'])
                
                # Normalizar el tipo para comparación (manejar tildes y mayúsculas)
                import unicodedata
                # Acceder al atributo tipo_profesor del objeto Usuario
                tipo_profesor = getattr(profesor_info, 'tipo_profesor', '') if profesor_info else ''
                tipo_normalizado = unicodedata.normalize('NFD', tipo_profesor.lower()).encode('ascii', 'ignore').decode('ascii')
                
                if profesor_info and tipo_normalizado == 'musculacion':
                    # Obtener el profesor_id correcto (no el ID del usuario)
                    profesor_id = getattr(profesor_info, 'profesor_id', None)
                    
                    if not profesor_id:
                        QMessageBox.warning(self, "Error", "No se pudo obtener el ID del profesor")
                        return
                    
                    # Verificar reinicio mensual automático
                    reinicio_info = self.db_manager.verificar_reinicio_mensual_horas(profesor_id)
                    if reinicio_info.get('success') and reinicio_info.get('necesita_reinicio'):
                        estadisticas = reinicio_info.get('estadisticas_mes_anterior')
                        if estadisticas:
                            QMessageBox.information(
                                self, "Nuevo Mes Detectado",
                                f"🗓️ Nuevo mes iniciado!\n\n"
                                f"Resumen del mes anterior:\n"
                                f"• Total de sesiones: {estadisticas['total_sesiones']}\n"
                                f"• Horas trabajadas: {estadisticas['total_horas']:.2f}h\n"
                                f"• Período: {estadisticas['primera_fecha']} - {estadisticas['ultima_fecha']}\n\n"
                                f"El conteo de horas se ha reiniciado para este mes."
                            )
                    
                    # Verificar si hay una sesión activa para este profesor
                    sesion_activa = self.db_manager.obtener_sesion_activa_profesor(profesor_id)
                    
                    if sesion_activa.get('success') and sesion_activa.get('datos'):
                        # Hay una sesión activa, preguntar qué hacer
                        sesion_data = sesion_activa['datos']
                        hora_inicio = sesion_data.get('hora_inicio', 'Desconocida')
                        
                        respuesta = QMessageBox.question(
                            self, "Sesión Activa Detectada",
                            f"Ya tienes una sesión de trabajo activa desde las {hora_inicio}.\n\n"
                            f"¿Qué deseas hacer?",
                            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
                            QMessageBox.StandardButton.Yes
                        )
                        
                        if respuesta == QMessageBox.StandardButton.Yes:
                            # Continuar con la sesión existente
                            QMessageBox.information(
                                self, "Sesión Continuada", 
                                f"✅ Continuando con la sesión activa desde las {hora_inicio}"
                            )
                        else:
                            # Finalizar la sesión actual e iniciar una nueva
                            resultado_fin = self.db_manager.finalizar_sesion_trabajo_profesor(profesor_id)
                            if resultado_fin.get('success'):
                                resultado_inicio = self.db_manager.iniciar_sesion_trabajo_profesor(profesor_id)
                                if resultado_inicio.get('success'):
                                    QMessageBox.information(
                                        self, "Nueva Sesión", 
                                        "✅ Sesión anterior finalizada y nueva sesión iniciada"
                                    )
                                else:
                                    QMessageBox.warning(
                                        self, "Error", 
                                        f"Error al iniciar nueva sesión: {resultado_inicio.get('mensaje', 'Error desconocido')}"
                                    )
                    else:
                        # No hay sesión activa, iniciar una nueva
                        resultado_inicio = self.db_manager.iniciar_sesion_trabajo_profesor(profesor_id)
                        
                        if resultado_inicio.get('success'):
                            QMessageBox.information(
                                self, "Sesión Iniciada", 
                                f"✅ Sesión de trabajo iniciada para {selected_prof['nombre']}"
                            )
                        else:
                            logging.warning(f"Error al iniciar la sesión de trabajo: {resultado_inicio.get('mensaje', 'Error desconocido')}")
                            QMessageBox.warning(
                                self, "Error", 
                                f"No se pudo iniciar la sesión de trabajo: {resultado_inicio.get('mensaje', 'Error desconocido')}"
                            )
                        
                # Para otros tipos de profesores (clases grupales, personal trainer) no se inicia sesión de trabajo
                else:
                    logging.info("Profesor NO es de musculación - No se inicia sesión de trabajo")
                    if profesor_info:
                        logging.info(f"Tipo detectado: '{profesor_info.get('tipo', 'N/A')}'")
                    
            except Exception as e:
                # Si hay error al iniciar la sesión de trabajo, mostrar advertencia pero permitir login
                logging.exception(f"Excepción en gestión de sesión de trabajo: {str(e)}")
                QMessageBox.warning(self, "Advertencia", 
                                   f"Error al gestionar sesión de trabajo: {str(e)}")
            
            logging.info("Finalizando login de profesor (aceptado)")
            self.accept()
        else:
            logging.warning("PIN incorrecto")
            QMessageBox.warning(self, "Acceso Denegado", "PIN incorrecto.")
            self.profesor_pin_input.clear()
        
        logging.info("Fin del proceso de login de profesor")
    
    def _prefetch_critical_data_async(self):
        """Prefetch de lecturas críticas sin bloquear la UI (usuarios, dueño)."""
        try:
            import threading
            def _run():
                try:
                    # Warm-up preferente de caché si está disponible para minimizar coste
                    if hasattr(self.db_manager, 'obtener_usuarios_con_cache'):
                        self.db_manager.obtener_usuarios_con_cache()
                    else:
                        self.db_manager.obtener_todos_usuarios()
                except Exception:
                    pass
                try:
                    # Resolver usuario dueño vía caché TTL para evitar bloqueos/RLS
                    _ = self.db_manager.get_owner_user_cached(ttl_seconds=600)
                except Exception:
                    pass
            threading.Thread(target=_run, daemon=True).start()
        except Exception:
            pass

    def closeEvent(self, event):
        # Asegurar cierre seguro de hilos en segundo plano para evitar "QThread: Destroyed..."
        try:
            # Branding thread
            if hasattr(self, '_branding_thread') and self._branding_thread:
                try:
                    if self._branding_thread.isRunning():
                        try:
                            self._branding_thread.finished.disconnect(self._on_branding_loaded)
                        except Exception:
                            pass
                        # Solicitar interrupción y cierre ordenado
                        try:
                            self._branding_thread.requestInterruption()
                        except Exception:
                            pass
                        try:
                            self._branding_thread.quit()
                        except Exception:
                            pass
                        # Espera breve para permitir que finalice sin forzar terminación
                        try:
                            self._branding_thread.wait(800)
                        except Exception:
                            pass
                except Exception:
                    pass
            # Profesores thread
            if hasattr(self, '_profes_thread') and self._profes_thread:
                try:
                    if self._profes_thread.isRunning():
                        try:
                            self._profes_thread.finished.disconnect(self._on_profes_loaded)
                        except Exception:
                            pass
                        # Solicitar interrupción y cierre ordenado
                        try:
                            self._profes_thread.requestInterruption()
                        except Exception:
                            pass
                        try:
                            self._profes_thread.quit()
                        except Exception:
                            pass
                        # Espera breve; las consultas usan timeouts, debería terminar rápido
                        try:
                            self._profes_thread.wait(1500)
                        except Exception:
                            pass
                except Exception:
                    pass
            # Refresh thread (cache/branding refresco en background)
            if hasattr(self, '_refresh_thread') and self._refresh_thread:
                try:
                    if self._refresh_thread.isRunning():
                        try:
                            self._refresh_thread.requestInterruption()
                        except Exception:
                            pass
                        try:
                            self._refresh_thread.quit()
                        except Exception:
                            pass
                        try:
                            self._refresh_thread.wait(1200)
                        except Exception:
                            pass
                except Exception:
                    pass
        except Exception:
            pass

        # Evitar que se cierre con la 'X' si no se ha logueado
        if self.result() != QDialog.DialogCode.Accepted:
            reply = QMessageBox.question(
                self,
                "Confirmar Salida",
                "¿Está seguro de que desea cerrar el programa?",
                QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
                QMessageBox.StandardButton.No,
            )
            if reply == QMessageBox.StandardButton.Yes:
                # Marcar cierre confirmado y rechazar el diálogo para que main maneje la limpieza
                try:
                    setattr(self, "_closing_confirmed", True)
                except Exception:
                    pass
                event.accept()
                self.reject()
            else:
                event.ignore()

