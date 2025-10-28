import sys
import os

# First run setup
try:
    from first_run_setup import first_run_setup
    first_run_setup()
except Exception:
    pass  # Continue even if first run setup fails
import logging
import json
"""
Headless/Server guard for container environments (e.g., Railway):
- Detects headless Linux or explicit HEADLESS flag and starts the FastAPI app
  directly via uvicorn, avoiding any PyQt imports that require libGL.
- Guarantees that running this file in a container will never crash due to
  missing GUI libraries; it will serve the web app instead.
"""

def _is_headless_env() -> bool:
    try:
        # Explicit override
        if os.getenv("HEADLESS") == "1":
            return True
        # Common container signals
        if os.getenv("RAILWAY") or os.getenv("RAILWAY_ENVIRONMENT") or os.getenv("PORT"):
            # If running on Linux without a display, treat as headless
            if sys.platform.startswith("linux") and not os.getenv("DISPLAY") and not os.getenv("WAYLAND_DISPLAY"):
                return True
        # Generic Linux headless detection
        if sys.platform.startswith("linux") and not os.getenv("DISPLAY") and not os.getenv("WAYLAND_DISPLAY"):
            return True
    except Exception:
        pass
    return False

# Guardado: si estamos en entorno headless (Railway/contenerizado Linux), arrancar la webapp y evitar importar PyQt
try:
    if _is_headless_env():
        try:
            # Importación dinámica para evitar que empaquetadores incluyan dependencias web en builds de escritorio
            m_uvicorn = __import__("uvicorn")
            webapp_server = __import__("webapp.server", fromlist=["app"])  # type: ignore
            app = getattr(webapp_server, "app")
            host = os.getenv("HOST", "0.0.0.0").strip() or "0.0.0.0"
            try:
                port = int(os.getenv("PORT", "8000"))
            except Exception:
                port = 8000
            log_level = os.getenv("LOG_LEVEL", "info").strip() or "info"
            m_uvicorn.run(  # type: ignore
                app,
                host=host,
                port=port,
                log_level=log_level,
                proxy_headers=(os.getenv("PROXY_HEADERS_ENABLED", "1").strip() in ("1", "true", "yes")),
            )
        except Exception as e:
            try:
                logging.exception(f"Headless startup failed: {e}")
            except Exception:
                pass
            # Propagar fallo para que el supervisor lo pueda ver
            raise
        # No continuar cargando la app de escritorio
        raise SystemExit(0)
except Exception:
    # No bloquear la ejecución en escritorio; continuar con importación de PyQt
    pass

# Auto-bootstrap de prerequisitos en primer arranque (Windows/desktop)
try:
    from utils_modules.prerequisites import ensure_prerequisites  # type: ignore
    try:
        from device_id import get_device_id  # type: ignore
        _dev_id = str(get_device_id())
    except Exception:
        _dev_id = os.getenv("DEVICE_ID") or "unknown"
    try:
        # Ejecuta idempotentemente: instala PostgreSQL, outbox, tareas, red/VPN, replicación
        _pr_res = ensure_prerequisites(_dev_id)
        try:
            logging.info("ensure_prerequisites: %s", json.dumps(_pr_res, ensure_ascii=False))
        except Exception:
            pass
    except Exception as _e_pr:
        try:
            logging.warning("Fallo ensure_prerequisitos: %s", _e_pr)
        except Exception:
            pass
except Exception:
    # No bloquear si prerequisitos fallan; continuar
    pass

# Desktop nunca arranca servidores locales; Railway gestiona la webapp.
from PyQt6.QtWidgets import (
    QApplication, QMainWindow, QTabWidget, QVBoxLayout, QWidget,
    QHBoxLayout, QLabel, QPushButton, QCheckBox, QMessageBox, QFrame, QSplitter,
    QSizePolicy, QSpacerItem, QMenuBar, QGraphicsDropShadowEffect,
    QDialog, QProgressBar, QProgressDialog
)
from PyQt6.QtCore import Qt, QTimer, pyqtSignal, QThread, QPropertyAnimation, QEasingCurve
from PyQt6.QtGui import QFont, QIcon, QKeySequence, QShortcut, QAction, QCloseEvent, QPixmap, QColor
from typing import Callable

# --- NUEVA IMPORTACIÓN ---
from widgets.custom_style import CustomProxyStyle

from utils import resource_path, terminate_tunnel_processes, get_public_tunnel_enabled, safe_get
from logger_config import setup_logging
setup_logging()
import threading
from datetime import datetime

from database import DatabaseManager
from payment_manager import PaymentManager
from export_manager import ExportManager
from search_manager import SearchManager
from widgets.user_tab_widget import UserTabWidget
from widgets.payments_tab_widget import PaymentsTabWidget
from widgets.reports_tab_widget import ReportsTabWidget
from widgets.config_tab_widget import ConfigTabWidget
from widgets.routines_tab_widget import RoutinesTabWidget
from widgets.classes_tab_widget import ClassesTabWidget
from widgets.professors_tab_widget import ProfessorsTabWidget
from widgets.global_search_widget import GlobalSearchWidget
from widgets.login_dialog import LoginDialog
from widgets.alerts_widget import AlertsWidget

from utils_modules.alert_system import alert_manager, AlertLevel, AlertCategory
from utils_modules.network_health_monitor import (
    start_network_health_monitor,
    stop_network_health_monitor,
    test_networks_and_restart,
)
from utils_modules.preload_manager import PreloadManager
from utils_modules.ui_profiler import profile
from utils_modules.async_runner import TaskThread
# Replicación lógica PostgreSQL: sin motores externos iniciados por la app.

class StartupProgressDialog(QDialog):
    """Diálogo mejorado para el arranque con progreso determinista y acciones."""
    def __init__(self):
        super().__init__(None)
        try:
            self.setWindowTitle("Cargando aplicación…")
            self.setModal(False)
            self.setWindowFlag(Qt.WindowType.FramelessWindowHint, True)
            self.setWindowFlag(Qt.WindowType.WindowStaysOnTopHint, True)
            self.setAttribute(Qt.WidgetAttribute.WA_TranslucentBackground, True)
            # Evitar tomar foco/activación para no bloquear la interacción al inicio
            self.setFocusPolicy(Qt.FocusPolicy.NoFocus)
            try:
                self.setAttribute(Qt.WidgetAttribute.WA_ShowWithoutActivating, True)
            except Exception:
                pass
        except Exception:
            pass

        outer = QVBoxLayout(self)
        outer.setContentsMargins(12, 12, 12, 12)
        outer.setSpacing(0)

        self.card = QFrame(self)
        self.card.setObjectName("startup_card")
        card_layout = QVBoxLayout(self.card)
        card_layout.setContentsMargins(16, 14, 16, 14)
        card_layout.setSpacing(10)

        try:
            # Sombra eliminada para reducir costo de pintura y evitar bloqueos iniciales
            self.card.setGraphicsEffect(None)
        except Exception:
            pass

        header = QHBoxLayout()
        header.setSpacing(8)
        # Logo del gimnasio a la izquierda del título (si existe)
        self.logo = QLabel()
        try:
            lp = resource_path("assets/gym_logo.png")
            if os.path.exists(lp):
                pm = QPixmap(lp).scaled(28, 28, Qt.AspectRatioMode.KeepAspectRatio, Qt.TransformationMode.SmoothTransformation)
                self.logo.setPixmap(pm)
        except Exception:
            pass
        try:
            if self.logo.pixmap() is None:
                self.logo.setFixedWidth(0)
        except Exception:
            pass
        header.addWidget(self.logo)
        self.label = QLabel("Preparando inicio…")
        try:
            f = self.label.font()
            f.setPointSize(max(f.pointSize() - 1, 11))
            f.setBold(True)
            self.label.setFont(f)
        except Exception:
            pass
        header.addWidget(self.label, 1)

        self.detail_label = QLabel("")
        self.detail_label.setWordWrap(True)
        try:
            f2 = self.detail_label.font()
            f2.setPointSize(max(f2.pointSize() - 2, 10))
            self.detail_label.setFont(f2)
        except Exception:
            pass

        self.bar = QProgressBar()
        try:
            self.bar.setRange(0, 100)
        except Exception:
            pass
        self.bar.setTextVisible(False)

        actions = QHBoxLayout()
        actions.setSpacing(6)
        self.hide_btn = QPushButton("Ocultar")
        self.hide_btn.setObjectName("startup_hide_btn")
        actions.addStretch(1)
        actions.addWidget(self.hide_btn)

        card_layout.addLayout(header)
        card_layout.addWidget(self.detail_label)
        card_layout.addWidget(self.bar)
        card_layout.addLayout(actions)
        outer.addWidget(self.card)

        try:
            self.setStyleSheet("""
                #startup_card {
                    background-color: rgba(36, 36, 36, 240);
                    border-radius: 12px;
                }
                #startup_card QLabel {
                    color: #f0f0f0;
                    background-color: transparent;
                }
                #startup_hide_btn {
                    padding: 4px 12px;
                    color: #f0f0f0;
                    background-color: #303030;
                    border: 1px solid #404040;
                    border-radius: 6px;
                }
                QProgressBar {
                    background-color: #202020;
                    border: 1px solid #404040;
                    border-radius: 4px;
                    height: 8px;
                }
                QProgressBar::chunk {
                    background-color: #3b82f6;
                    border-radius: 4px;
                }
            """)
        except Exception:
            pass
        try:
            self.setFixedSize(540, 200)
        except Exception:
            pass

        self._fade = None
        try:
            self.setWindowOpacity(0.0)
        except Exception:
            pass

    def fade_in(self, duration_ms: int = 220):
        try:
            # Mostrar sin activar para evitar bloqueos de foco al inicio
            self.show()
            anim = QPropertyAnimation(self, b"windowOpacity")
            anim.setDuration(int(duration_ms))
            try:
                anim.setEasingCurve(QEasingCurve.Type.InOutQuad)
            except Exception:
                pass
            anim.setStartValue(0.0)
            anim.setEndValue(1.0)
            self._fade = anim
            anim.start()
        except Exception:
            try:
                self.show()
            except Exception:
                pass

    def fade_out(self, duration_ms: int = 180, on_finished: Callable | None = None):
        try:
            anim = QPropertyAnimation(self, b"windowOpacity")
            anim.setDuration(int(duration_ms))
            try:
                anim.setEasingCurve(QEasingCurve.Type.InOutQuad)
            except Exception:
                pass
            anim.setStartValue(self.windowOpacity())
            anim.setEndValue(0.0)
            def _done():
                try:
                    self.hide()
                    self.setWindowOpacity(1.0)
                except Exception:
                    pass
                if on_finished:
                    try:
                        on_finished()
                    except Exception:
                        pass
            try:
                anim.finished.connect(_done)
            except Exception:
                _done()
            self._fade = anim
            anim.start()
        except Exception:
            try:
                self.hide()
            except Exception:
                pass
# Diálogo de progreso de arranque: definir antes de su uso
# Eliminado: clase duplicada StartupProgressDialog (definida arriba)

class SyncProgressDialog(QDialog):
    """Diálogo mejorado para la sincronización inicial con progreso y acciones."""
    def __init__(self):
        super().__init__(None)
        try:
            self.setWindowTitle("Sincronizando datos…")
            self.setModal(False)
            self.setWindowFlag(Qt.WindowType.FramelessWindowHint, True)
            self.setWindowFlag(Qt.WindowType.WindowStaysOnTopHint, True)
            # Fondo translúcido para animación suave y sombra
            self.setAttribute(Qt.WidgetAttribute.WA_TranslucentBackground, True)
        except Exception:
            pass

        outer = QVBoxLayout(self)
        outer.setContentsMargins(12, 12, 12, 12)
        outer.setSpacing(0)

        # Tarjeta central con sombra
        self.card = QFrame(self)
        self.card.setObjectName("sync_card")
        card_layout = QVBoxLayout(self.card)
        card_layout.setContentsMargins(16, 14, 16, 14)
        card_layout.setSpacing(10)

        try:
            # Eliminar sombra para consistencia y rendimiento
            self.card.setGraphicsEffect(None)
        except Exception:
            pass

        # Encabezado con icono y título
        header = QHBoxLayout()
        header.setSpacing(8)
        self.icon = QLabel()
        try:
            pm = None
            for candidate in ["assets/gym_logo.png", "assets/standard_icon.png"]:
                icon_path = resource_path(candidate)
                if os.path.exists(icon_path):
                    pm = QPixmap(icon_path)
                    break
            if pm and not pm.isNull():
                pm = pm.scaled(26, 26, Qt.AspectRatioMode.KeepAspectRatio, Qt.TransformationMode.SmoothTransformation)
                self.icon.setPixmap(pm)
        except Exception:
            pass
        self.label = QLabel("Sincronización inicial en curso…")
        try:
            f = self.label.font()
            f.setPointSize(max(f.pointSize() - 1, 11))
            f.setBold(True)
            self.label.setFont(f)
        except Exception:
            pass
        header.addWidget(self.icon)
        header.addWidget(self.label, 1)

        # Detalle pequeño
        self.detail_label = QLabel("")
        self.detail_label.setWordWrap(True)
        try:
            f2 = self.detail_label.font()
            f2.setPointSize(max(f2.pointSize() - 2, 10))
            self.detail_label.setFont(f2)
        except Exception:
            pass

        # Barra de progreso (indeterminada por defecto)
        self.bar = QProgressBar()
        try:
            self.bar.setRange(0, 0)
        except Exception:
            pass
        self.bar.setTextVisible(False)

        # Acciones
        actions = QHBoxLayout()
        actions.setSpacing(6)
        self.hide_btn = QPushButton("Ocultar")
        self.hide_btn.setObjectName("sync_hide_btn")
        self.dnd_checkbox = QCheckBox("No molestar")
        self.dnd_checkbox.setObjectName("sync_dnd_checkbox")
        actions.addStretch(1)
        actions.addWidget(self.dnd_checkbox)
        actions.addWidget(self.hide_btn)

        # Ensamblar tarjeta
        card_layout.addLayout(header)
        card_layout.addWidget(self.detail_label)
        card_layout.addWidget(self.bar)
        card_layout.addLayout(actions)
        outer.addWidget(self.card)

        # Estilos
        try:
            self.setStyleSheet("""
                #sync_card {
                    background-color: rgba(36, 36, 36, 240);
                    border-radius: 12px;
                }
                #sync_card QLabel {
                    color: #f0f0f0;
                    background: transparent;
                }
                #sync_hide_btn {
                    padding: 6px 12px;
                    color: #eaeaea;
                    background: rgba(255, 255, 255, 0.06);
                    border: 1px solid rgba(255, 255, 255, 0.12);
                    border-radius: 8px;
                }
                #sync_hide_btn:hover {
                    background: rgba(255, 255, 255, 0.10);
                }
                #sync_hide_btn:pressed {
                    background: rgba(255, 255, 255, 0.08);
                }
                QProgressBar {
                    background: rgba(255, 255, 255, 0.08);
                    border: 1px solid rgba(255, 255, 255, 0.12);
                    border-radius: 6px;
                    padding: 2px;
                }
                QProgressBar::chunk {
                    background: qlineargradient(x1:0,y1:0,x2:1,y2:0, stop:0 #4caf50, stop:1 #66bb6a);
                    border-radius: 4px;
                }
            """)
        except Exception:
            pass

        try:
            self.setFixedSize(540, 200)
        except Exception:
            pass

        # Animación de entrada/salida
        self._fade = None
        try:
            self.setWindowOpacity(0.0)
        except Exception:
            pass

    def fade_in(self, duration_ms: int = 220):
        try:
            self.show()
            anim = QPropertyAnimation(self, b"windowOpacity")
            anim.setDuration(int(duration_ms))
            try:
                anim.setEasingCurve(QEasingCurve.Type.InOutQuad)
            except Exception:
                pass
            anim.setStartValue(0.0)
            anim.setEndValue(1.0)
            self._fade = anim
            anim.start()
        except Exception:
            try:
                self.show()
            except Exception:
                pass

    def fade_out(self, duration_ms: int = 180, on_finished: Callable | None = None):
        try:
            anim = QPropertyAnimation(self, b"windowOpacity")
            anim.setDuration(int(duration_ms))
            try:
                anim.setEasingCurve(QEasingCurve.Type.InOutQuad)
            except Exception:
                pass
            anim.setStartValue(self.windowOpacity())
            anim.setEndValue(0.0)
            def _done():
                try:
                    self.hide()
                    self.setWindowOpacity(1.0)
                except Exception:
                    pass
                if on_finished:
                    try:
                        on_finished()
                    except Exception:
                        pass
            try:
                anim.finished.connect(_done)
            except Exception:
                _done()
            self._fade = anim
            anim.start()
        except Exception:
            try:
                self.hide()
            except Exception:
                pass

class MainWindow(QMainWindow):
    monthly_hours_ready = pyqtSignal(int, int, bool)
    initialized = pyqtSignal()
    # Progreso determinista de arranque: (step_index, total_steps, label)
    startup_progress_step = pyqtSignal(int, int, str)
    # Guardia global para QThreads: registro y estado de instalación
    _QTHREAD_REGISTRY = set()
    _QTHREAD_GUARD_INSTALLED = False
    _orig_qthread_start = None
    def __init__(self, user_role: str, db_manager: DatabaseManager, logged_in_user=None):
        super().__init__()
        self.user_role = user_role
        self.logged_in_role = user_role  # Alias para compatibilidad
        self.logged_in_user = logged_in_user
        self.db_manager = db_manager  # Usar la instancia existente
        # Estado para evitar ejecuciones concurrentes del cálculo de horas
        self._hours_worker_running = False
        # Bandera para evitar ejecuciones simultáneas de automatización pesada
        self._wa_auto_running = False
        # Debounce para refrescos por replicación
        self._inbound_debounce_ms = 2000
        self._last_inbound_refresh_ts = 0
        # Overlays de arranque y sincronización
        self._startup_overlay: StartupProgressDialog | None = None
        self._sync_overlay: SyncProgressDialog | None = None
        self._sync_dnd: bool = False
        try:
            logging.info(f"Iniciando la aplicación con el rol: {self.user_role}...")
            # Instalar guardia de QThreads para evitar destrucción de hilos activos
            try:
                self._install_qthread_guard()
            except Exception:
                pass
            # Conectar señal para actualizar horas de forma segura en el hilo de UI
            try:
                self.monthly_hours_ready.connect(self._apply_monthly_hours_result)
            except Exception:
                pass
            
            # Obtener nombre del gimnasio dinámicamente
            self.gym_name = self.get_gym_name()
            
            # Configurar título inicial
            self.update_window_title()
            
            # Establecer icono de ventana con fallback
            for candidate in ["assets/gym_logo.ico", "assets/gym_icon.ico", "assets/icon.png"]:
                icon_path = resource_path(candidate)
                if os.path.exists(icon_path):
                    self.setWindowIcon(QIcon(icon_path))
                    break
            # Ajuste responsivo mínimo de la ventana
            try:
                self._apply_responsive_window_constraints()
            except Exception:
                self.resize(1600, 1000)
                self.setMinimumSize(1400, 900)
            self.showMaximized()  # Iniciar maximizada por defecto
            
            # Configurar menú de la aplicación
            self.setup_menu_bar()
            # Programar indexación en segundo plano para no bloquear el arranque
            try:
                QTimer.singleShot(5000, self._start_background_indexing)
            except Exception:
                pass

            # Scheduler de WhatsApp: enviar todo cada 5 minutos en hilos dedicados
            try:
                self.whatsapp_sendall_timer = QTimer(self)
                self.whatsapp_sendall_timer.setInterval(5 * 60 * 1000)  # 5 minutos
                self.whatsapp_sendall_timer.timeout.connect(self._trigger_whatsapp_sendall)
                self.whatsapp_sendall_timer.start()
                # Primer envío temprano (a los 15s) para drenar pendientes tras el arranque
                QTimer.singleShot(15000, self._trigger_whatsapp_sendall)
                logging.info("Scheduler de WhatsApp activado (cada 5 minutos)")
            except Exception as e:
                logging.debug(f"No se pudo iniciar scheduler de WhatsApp: {e}")
            
            # --- Lógica de carga de estilos mejorada ---
            try:
                style_path = resource_path("styles/style.qss")
                with open(style_path, "r", encoding="utf-8") as f:
                    self.app_style = f.read()
            except FileNotFoundError:
                logging.warning("No se encontró el archivo de estilos 'styles/style.qss'. Se usarán los estilos por defecto.")
                self.app_style = "" # Asegura que no haya error si el archivo no existe
            self.payment_manager = PaymentManager(self.db_manager)
            self.export_manager = ExportManager(self.db_manager)
            self.search_manager = SearchManager(self.db_manager)
            
            # Inicializar y vincular WhatsApp Manager utilizando el creado por PaymentManager
            try:
                self.whatsapp_manager = getattr(self.payment_manager, 'whatsapp_manager', None)
                if self.whatsapp_manager:
                    # Vincular referencias cruzadas
                    try:
                        setattr(self.whatsapp_manager, 'payment_manager', self.payment_manager)
                    except Exception:
                        pass
                    logging.info("WhatsApp Manager vinculado desde PaymentManager")
                else:
                    from whatsapp_manager import WhatsAppManager
                    self.whatsapp_manager = WhatsAppManager(self.db_manager)
                    try:
                        setattr(self.whatsapp_manager, 'payment_manager', self.payment_manager)
                    except Exception:
                        pass
                    logging.info("WhatsApp Manager inicializado y vinculado correctamente")
            except ImportError as e:
                logging.warning(f"WhatsApp Manager no disponible: {e}")
                self.whatsapp_manager = None
            except Exception as e:
                logging.error(f"Error al inicializar o vincular WhatsApp Manager: {e}")
                self.whatsapp_manager = None

            # Eliminado: inicialización del gestor de sincronización legacy y proxy local

            # Configuración inicial de fuente eliminada

            # Eliminado flujo antiguo de desactivación por 90 días sin pago.
            # La desactivación ahora se realiza únicamente vía contador de 3 cuotas vencidas.

            # Widget central
            central_widget = QWidget()
            self.setCentralWidget(central_widget)
            # Micro-yield para permitir repintado inicial
            try:
                QApplication.processEvents()
            except Exception:
                pass
            # Asegurar reglas QSS dinámicas descendentes
            central_widget.setProperty("dynamic_css", "true")
            self.setProperty("dynamic_css", "true")
            
            # Layout principal con tamaños fijos
            main_layout = QVBoxLayout(central_widget)
            main_layout.setContentsMargins(10, 10, 10, 10)  # Márgenes fijos
            main_layout.setSpacing(6)  # Espaciado fijo

            # ===== Barra de progreso superpuesta arriba de la ventana (lógicamente) =====
            try:
                self.startupProgressBar = QProgressBar()
                self.startupProgressBar.setObjectName("startup_progress_top")
                self.startupProgressBar.setTextVisible(False)
                self.startupProgressBar.setFixedHeight(6)
                # Rango inicial indeterminado; se volverá determinista al recibir señales
                self.startupProgressBar.setRange(0, 0)
                # Estilo ligero para que se integre con el QSS global
                self.startupProgressBar.setProperty("dynamic_css", "true")
                # Insertar la barra arriba, antes del contenido principal
                main_layout.addWidget(self.startupProgressBar)
                # Conectar actualización determinista
                try:
                    def _on_startup_step(i: int, total: int, label: str):
                        try:
                            if total <= 0:
                                total = 1
                            self.startupProgressBar.setRange(0, total)
                            self.startupProgressBar.setValue(max(0, min(i, total)))
                            # Ocultar automáticamente al completar
                            if i >= total:
                                self.startupProgressBar.hide()
                            else:
                                self.startupProgressBar.show()
                        except Exception:
                            pass
                        # Actualizar overlay mejorado en paralelo
                        try:
                            self.update_startup_overlay(i, total, label)
                            if i >= total:
                                self.hide_startup_overlay()
                        except Exception:
                            pass
                    self.startup_progress_step.connect(_on_startup_step)
                except Exception:
                    pass
            except Exception:
                # No bloquear si falla la creación de la barra
                pass
            
            # TabWidget con barra de búsqueda integrada
            self.tabWidget = QTabWidget()
            self.setup_search_bar_in_tabs()
            main_layout.addWidget(self.tabWidget)
            # Micro-yield tras crear el contenedor de pestañas
            try:
                QApplication.processEvents()
            except Exception:
                pass
            
            # Crear botón flotante de cerrar sesión (posicionamiento absoluto)
            self.create_floating_logout_button()
            
            self.tabs = {}
            self.notification_counts = {}  # Para almacenar contadores de notificaciones
            
            # Configurar barra de estado con alertas
            self.setup_status_bar()
            
            # Cargar configuración de branding ANTES de crear las pestañas
            # para que los gráficos se inicialicen con los colores correctos
            self.load_branding_configuration()
            
            # Diferir tareas pesadas de configuración de pestañas y estilos
            try:
                # Mostrar overlay de arranque inmediatamente
                self.show_startup_overlay("Cargando aplicación…", "Preparando inicio…", 0)
                QTimer.singleShot(0, self._run_deferred_startup)
            except Exception:
                # Fallback: ejecutar directamente si QTimer falla
                try:
                    self.setup_tabs()
                except Exception:
                    pass
                try:
                    self.apply_role_permissions()
                except Exception:
                    pass
                try:
                    self.apply_main_tabs_visibility_by_role()
                except Exception:
                    pass
                try:
                    self.apply_complete_styling()
                except Exception:
                    pass
                # Fallback: actualizar y ocultar overlay
                try:
                    self.update_startup_overlay(1, 1, "Inicio completado")
                    self.hide_startup_overlay()
                except Exception:
                    pass

            # Garantizar que la pestaña Configuración siempre sea visible
            try:
                if 'configuracion' in self.tab_indices:
                    self.tabWidget.setTabVisible(self.tab_indices['configuracion'], True)
            except Exception:
                pass

            # Arrancar watchdog de UI para detectar bloqueos del event loop
            try:
                self._ui_watchdog_last_tick = datetime.now()
                self._ui_watchdog_timer = QTimer(self)
                self._ui_watchdog_timer.setInterval(1000)
                self._ui_watchdog_timer.timeout.connect(self._ui_watchdog_tick)
                self._ui_watchdog_timer.start()
                logging.info("UI Watchdog iniciado")
            except Exception as e:
                logging.debug(f"No se pudo iniciar UI Watchdog: {e}")

            # Reducir márgenes internos del contenido de pestañas a 1px globalmente
            self._apply_global_tab_layout_margins(1)
            
            self.setup_notification_timer()
            
            # Conectar señales de búsqueda
            self.connect_search_signals()
            
            # Estilos completos se aplicarán dentro de _run_deferred_startup
            
            # Inicializar gestor de teclado global (atajos como Ctrl+Enter)
            try:
        # Eliminado: inicialización de keyboard_manager; atajos se manejan localmente en los widgets
                logging.info("KeyboardManager inicializado desde MainWindow")
            except Exception as e:
                logging.warning(f"No se pudo inicializar KeyboardManager: {e}")
            
            # Inicializar sistema de alertas
            self.setup_alert_system()
            
            # MEJORA: Aplazar tareas pesadas para evitar bloquear la UI
            try:
                QTimer.singleShot(0, lambda: self._update_charts_branding(self.branding_config))
                # Inicialización de WhatsApp en background tras mostrar la UI
                QTimer.singleShot(0, lambda: getattr(self, 'payment_manager', None) and self.payment_manager.start_whatsapp_initialization(background=True, delay_seconds=2.0))
                QTimer.singleShot(200, self.update_monthly_hours)
            except Exception:
                # Fallback si QTimer falla: ejecutar de forma directa
                try:
                    self._update_charts_branding(self.branding_config)
                except Exception:
                    pass
                try:
                    self.update_monthly_hours()
                except Exception:
                    pass
            
            logging.info("Aplicación inicializando en segundo plano (deferred startup).")
        except Exception as e:
            # No cerrar ni abortar: continuar en modo degradado para evitar crash silencioso
            logging.critical(f"Fallo crítico durante la inicialización de MainWindow: {e}", exc_info=True)
            try:
                from PyQt6.QtWidgets import QMessageBox
                QMessageBox.critical(self, "Error al inicializar", f"Ocurrió un error inicializando la ventana principal. Algunas funciones pueden no estar disponibles.\n\nDetalle: {e}")
            except Exception:
                pass
            try:
                logging.error("Continuando en modo degradado; algunas funciones pueden no estar disponibles.")
            except Exception:
                pass
            # Importante: no cerrar la ventana y no re-lanzar la excepción
            
        # Bandera para distinguir entre logout y cierre de aplicación
        self.is_logout = False

        # Configurar timer para actualizar título con contador de sesiones
        self.setup_title_update_timer()

    # Eliminado: stub legacy de inicialización de OfflineSyncManager

    def show_startup_overlay(self, title: str = "Cargando aplicación…", detail: str = "", percent: int | None = None):
        try:
            if self._startup_overlay is None:
                self._startup_overlay = StartupProgressDialog()
                try:
                    self._startup_overlay.hide_btn.clicked.connect(self.hide_startup_overlay)
                except Exception:
                    pass
            # Textos
            try:
                self._startup_overlay.label.setText(title)
            except Exception:
                pass
            try:
                self._startup_overlay.detail_label.setText(detail or "")
            except Exception:
                pass
            # Progreso
            try:
                if isinstance(percent, int):
                    self._startup_overlay.bar.setRange(0, 100)
                    self._startup_overlay.bar.setValue(max(0, min(100, percent)))
                else:
                    # Si no hay porcentaje, mantener determinista al recibir señales
                    self._startup_overlay.bar.setRange(0, 100)
                
            except Exception:
                pass
            # Centrar sobre la ventana principal
            try:
                geo = self.geometry()
                dlg_w = self._startup_overlay.width()
                dlg_h = self._startup_overlay.height()
                x = geo.x() + (geo.width() - dlg_w) // 2
                y = geo.y() + (geo.height() - dlg_h) // 3
                self._startup_overlay.move(max(0, x), max(0, y))
            except Exception:
                pass
            # Mostrar con fade-in
            try:
                self._startup_overlay.fade_in()
            except Exception:
                try:
                    # Mostrar sin activar para evitar robar foco y bloquear interacción
                    self._startup_overlay.show()
                except Exception:
                    pass
        except Exception:
            pass

    def update_startup_overlay(self, i: int, total: int, label: str):
        try:
            if not self._startup_overlay:
                return
            # Actualizar barra como porcentaje determinista
            pct = 0
            try:
                pct = int((i / max(total, 1)) * 100)
            except Exception:
                pct = i
            try:
                self._startup_overlay.bar.setRange(0, 100)
                self._startup_overlay.bar.setValue(max(0, min(100, pct)))
            except Exception:
                pass
            # Detalle descriptivo
            try:
                self._startup_overlay.detail_label.setText(label or "")
            except Exception:
                pass
        except Exception:
            pass

    def hide_startup_overlay(self):
        try:
            if self._startup_overlay:
                try:
                    self._startup_overlay.fade_out(on_finished=lambda: setattr(self, "_startup_overlay", None))
                except Exception:
                    self._startup_overlay.close()
                    self._startup_overlay = None
        except Exception:
            pass

    def show_sync_overlay(self, message: str = "Sincronizando datos iniciales…", ready: int | None = None, total: int | None = None, detail: str | None = None):
        try:
            if getattr(self, "_sync_dnd", False):
                return
            if self._sync_overlay is None:
                self._sync_overlay = SyncProgressDialog()
                try:
                    self._sync_overlay.hide_btn.clicked.connect(self.hide_sync_overlay)
                except Exception:
                    pass
                try:
                    self._sync_overlay.dnd_checkbox.stateChanged.connect(self._on_sync_dnd_changed)
                except Exception:
                    pass
            # Actualizar textos
            try:
                self._sync_overlay.label.setText(message or "Sincronizando datos…")
            except Exception:
                pass
            try:
                if detail and detail.strip():
                    self._sync_overlay.detail_label.setText(detail)
                    self._sync_overlay.detail_label.show()
                else:
                    self._sync_overlay.detail_label.hide()
            except Exception:
                pass
            # Progreso (ready/total) o indeterminado
            try:
                if isinstance(ready, int) and isinstance(total, int) and total > 0:
                    self._sync_overlay.bar.setRange(0, int(total))
                    self._sync_overlay.bar.setValue(max(0, min(int(ready), int(total))))
                else:
                    self._sync_overlay.bar.setRange(0, 0)
            except Exception:
                pass
            # Centrar sobre la ventana principal
            try:
                geo = self.frameGeometry()
                center = geo.center()
                dlg_geo = self._sync_overlay.frameGeometry()
                dlg_geo.moveCenter(center)
                self._sync_overlay.move(dlg_geo.topLeft())
            except Exception:
                pass
            # Mostrar con fade-in
            try:
                self._sync_overlay.fade_in()
            except Exception:
                self._sync_overlay.show()
                self._sync_overlay.raise_()
                self._sync_overlay.activateWindow()
        except Exception:
            pass

    def update_sync_overlay(self, message: str | None = None, ready: int | None = None, total: int | None = None, detail: str | None = None):
        try:
            if not self._sync_overlay or getattr(self, "_sync_dnd", False):
                return
            if message is not None:
                try:
                    self._sync_overlay.label.setText(message or "")
                except Exception:
                    pass
            if detail is not None:
                try:
                    if detail.strip():
                        self._sync_overlay.detail_label.setText(detail)
                        self._sync_overlay.detail_label.show()
                    else:
                        self._sync_overlay.detail_label.hide()
                except Exception:
                    pass
            if (ready is not None) or (total is not None):
                try:
                    if isinstance(ready, int) and isinstance(total, int) and int(total) > 0:
                        self._sync_overlay.bar.setRange(0, int(total))
                        self._sync_overlay.bar.setValue(max(0, min(int(ready or 0), int(total))))
                    else:
                        self._sync_overlay.bar.setRange(0, 0)
                except Exception:
                    pass
        except Exception:
            pass

    def hide_sync_overlay(self):
        try:
            if self._sync_overlay:
                try:
                    self._sync_overlay.fade_out(on_finished=lambda: setattr(self, "_sync_overlay", None))
                except Exception:
                    self._sync_overlay.close()
                    self._sync_overlay = None
        except Exception:
            pass

    def _on_sync_dnd_changed(self, state: int):
        try:
            self._sync_dnd = (int(state) == int(Qt.CheckState.Checked))
            if self._sync_dnd:
                self.hide_sync_overlay()
        except Exception:
            pass
    def _apply_responsive_window_constraints(self):
        try:
            screen = QApplication.primaryScreen()
            if not screen:
                self.resize(1600, 1000)
                self.setMinimumSize(1400, 900)
                return
            geom = screen.availableGeometry()
            sw = max(geom.width(), 1)
            sh = max(geom.height(), 1)
            target_w = min(1600, int(sw * 0.9))
            target_h = min(1000, int(sh * 0.9))
            min_w = min(1400, int(sw * 0.8))
            min_h = min(900, int(sh * 0.8))
            min_w = max(1024, min_w)
            min_h = max(700, min_h)
            target_w = max(min_w, target_w)
            target_h = max(min_h, target_h)
            self.setMinimumSize(min_w, min_h)
            self.resize(target_w, target_h)
        except Exception:
            self.resize(1600, 1000)
            self.setMinimumSize(1400, 900)
    def _log_window_constraints_after_styling(self):
        try:
            screen = QApplication.primaryScreen()
            sw = sh = None
            if screen:
                geom = screen.availableGeometry()
                sw, sh = geom.width(), geom.height()
            min_sz = self.minimumSize()
            cur_sz = self.size()
            logging.info(
                f"Post-style constraints: min=({min_sz.width()}x{min_sz.height()}), "
                f"current=({cur_sz.width()}x{cur_sz.height()}), "
                f"screen=({sw}x{sh})"
            )
        except Exception as e:
            logging.debug(f"No se pudo registrar constraints de ventana post-estilos: {e}")
    def _run_deferred_startup(self):
        """Ejecuta pasos pesados de arranque en serie usando QTimer, con progreso."""
        try:
            try:
                logging.info("Deferred startup: iniciando")
            except Exception:
                pass
            steps: list[tuple[str, callable]] = [
                ("Verificando prerequisitos (PostgreSQL)…", self.verify_and_install_prereqs_step),
                ("Configurando pestañas…", self.setup_tabs),
                ("Aplicando permisos por rol…", self.apply_role_permissions),
                ("Ajustando visibilidad de pestañas…", self.apply_main_tabs_visibility_by_role),
                ("Aplicando estilos completos…", self.apply_complete_styling),
                ("Verificando constraints de ventana…", self._log_window_constraints_after_styling),
                # Ejecutar horas mensuales en hilo de fondo para evitar bloquear UI
                ("Actualizando horas del mes…", lambda: QTimer.singleShot(0, self.update_monthly_hours)),
            ]

            total = len(steps)
            try:
                logging.info(f"Deferred startup: {total} pasos")
            except Exception:
                pass

            def _run_step(i: int):
                if i >= total:
                    try:
                        self.startup_progress_step.emit(total, total, "Inicio completado")
                    except Exception:
                        pass
                    # Emitir initialized al completar todos los pasos
                    try:
                        QTimer.singleShot(0, self.initialized.emit)
                    except Exception:
                        pass
                    try:
                        logging.info("Deferred startup: completado")
                    except Exception:
                        pass
                    return
                label, fn = steps[i]
                try:
                    try:
                        logging.info(f"Deferred startup: paso {i+1}/{total} - {label}")
                    except Exception:
                        pass
                    with profile(f"startup step: {label}"):
                        fn()
                except Exception as e:
                    try:
                        logging.error(f"Error en paso de arranque '{label}': {e}")
                    except Exception:
                        pass
                # Micro-yield tras cada paso
                try:
                    QApplication.processEvents()
                except Exception:
                    pass
                try:
                    self.startup_progress_step.emit(i + 1, total, label)
                except Exception:
                    pass
                # Espaciar pasos para evitar bloqueos largos del event loop
                QTimer.singleShot(25, lambda: _run_step(i + 1))

            # Emitir inicio y arrancar la secuencia
            try:
                self.startup_progress_step.emit(0, total, "Preparando inicio…")
            except Exception:
                pass
            QTimer.singleShot(0, lambda: _run_step(0))
        except Exception as e:
            try:
                logging.error(f"Fallo en deferred startup: {e}")
            except Exception:
                pass
    def verify_and_install_prereqs_step(self):
        """Lanza verificación/instalación de prerequisitos (PostgreSQL) en background para no bloquear la UI."""
        try:
            from utils_modules.async_utils import run_in_background
            from utils_modules.prerequisites import ensure_prerequisites
            from device_id import get_device_id

            device = None
            try:
                device = get_device_id()
            except Exception:
                device = "unknown"

            def _work():
                try:
                    return ensure_prerequisites(device)
                except Exception as e:
                    return {"error": str(e)}

            def _on_success(result: object):
                try:
                    if isinstance(result, dict):
                        pg_ok = bool(result.get("postgresql", {}).get("installed"))
                        db_created = bool(result.get("postgresql", {}).get("db_created"))
                        marked = bool(result.get("marked"))
                        summary = []
                        summary.append(f"PostgreSQL 17: {'OK' if pg_ok else 'Falta'}")
                        if pg_ok:
                            summary.append(f"DB: {'creada' if db_created else 'existente/no creada'}")
                        if marked:
                            summary.append("(marcado este equipo)")
                        text = "✅ " + ", ".join(summary) if pg_ok else "⚠️ " + ", ".join(summary)
                        try:
                            if hasattr(self, 'system_status_label') and self.system_status_label:
                                self.system_status_label.setText(text)
                                self.system_status_label.setToolTip("Auto-verificación de PostgreSQL ejecutada en background")
                        except Exception:
                            pass
                        try:
                            logging.info(f"Prerequisitos resumen: {text}")
                        except Exception:
                            pass
                except Exception:
                    pass

            def _on_error(message: str):
                try:
                    if hasattr(self, 'system_status_label') and self.system_status_label:
                        self.system_status_label.setText("⚠️ Prerequisitos: error")
                        self.system_status_label.setToolTip(message or "Error desconocido en verificación de prerequisitos")
                except Exception:
                    pass

            run_in_background(_work, _on_success, _on_error, parent=self)
        except Exception as e:
            try:
                logging.debug(f"No se pudo iniciar verificación de prerequisitos: {e}")
            except Exception:
                pass
    # Replicación lógica PostgreSQL: no hay arranque de motor externo
    def _start_background_indexing(self):
        """Lanza ensure_indexes en un hilo en background para reducir el tiempo de inicio."""
        try:
            threading.Thread(target=self.db_manager.ensure_indexes, daemon=True).start()
            logging.info("Indexación concurrente programada en segundo plano")
        except Exception as e:
            logging.warning(f"No se pudo programar indexación: {e}")

        # Eliminado: arranque de OfflineSyncManager legacy

        # Pequeño watchdog de UI para asegurar que no hay bloqueos
        try:
            QTimer.singleShot(1000, self._ui_watchdog_tick)
        except Exception:
            pass

    def _trigger_whatsapp_sendall(self):
        """Dispara procesamiento de pendientes WhatsApp en hilo dedicado sin bloquear UI."""
        try:
            def _process():
                try:
                    if hasattr(self, 'whatsapp_manager') and self.whatsapp_manager:
                        # Fallback: intentar una rutina de envío si existe
                        try:
                            fn = getattr(self.whatsapp_manager, 'process_pending_sends', None)
                            if callable(fn):
                                fn()
                        except Exception:
                            pass
                except Exception as e:
                    logging.debug(f"Error en procesamiento periódico de WhatsApp: {e}")

            thr = TaskThread(_process)
            # Establecer prioridad cuando el hilo esté corriendo para evitar errores
            try:
                thr.started.connect(lambda: thr.setPriority(QThread.Priority.HighPriority))
            except Exception:
                pass
            thr.finished.connect(thr.deleteLater)
            thr.start()
        except Exception:
            pass

    # ===== Orquestación de hilos con prioridades =====
    def run_background_task(self, func):
        """Ejecuta tarea poco relevante en hilo con prioridad baja."""
        try:
            thr = TaskThread(func)
            # Fijar prioridad sólo después de que el hilo arranque
            try:
                thr.started.connect(lambda: thr.setPriority(QThread.Priority.LowestPriority))
            except Exception:
                pass
            thr.finished.connect(thr.deleteLater)
            thr.start()
            return thr
        except Exception:
            return None

    def run_important_task(self, func):
        """Ejecuta operación importante/cálculo pesado en hilo de alta prioridad."""
        try:
            thr = TaskThread(func)
            # Evitar 'Cannot set priority, thread is not running'
            try:
                thr.started.connect(lambda: thr.setPriority(QThread.Priority.TimeCriticalPriority))
            except Exception:
                pass
            thr.finished.connect(thr.deleteLater)
            thr.start()
            return thr
        except Exception:
            return None
        # Eliminado: inicialización de OfflineSyncManager legacy en este punto

    def _ui_watchdog_tick(self):
        """Detecta si el event loop estuvo bloqueado y registra la latencia."""
        try:
            now = datetime.now()
            last = getattr(self, '_ui_watchdog_last_tick', now)
            delta = (now - last).total_seconds()
            self._ui_watchdog_last_tick = now
            if delta > 2.0:
                logging.warning(f"UI watchdog: event loop bloqueado ~{delta:.2f}s")
                try:
                    os.makedirs('logs', exist_ok=True)
                    with open(os.path.join('logs', 'ui_watchdog.log'), 'a', encoding='utf-8') as f:
                        f.write(f"{now.isoformat()} delay={delta:.2f}s\n")
                except Exception:
                    pass
        except Exception:
            pass

    # === Check-in por QR: flujo movido desde KeyboardManager ===
    def _resolve_selected_user_id_from_tab(self, current_tab) -> int | None:
        """Intenta resolver el usuario seleccionado según la pestaña activa."""
        try:
            # 1) Método explícito en la pestaña
            if current_tab and hasattr(current_tab, 'get_current_user_id'):
                uid = current_tab.get_current_user_id()
                if isinstance(uid, int) and uid > 0:
                    return uid
            # 2) Atributo selected_user con id
            if current_tab and hasattr(current_tab, 'selected_user'):
                su = getattr(current_tab, 'selected_user')
                uid = getattr(su, 'id', None) or (su.get('id') if isinstance(su, dict) else None)
                if isinstance(uid, int) and uid > 0:
                    return uid
            # 3) Combobox de usuarios común en pagos
            if current_tab and hasattr(current_tab, 'user_combobox'):
                combo = current_tab.user_combobox
                data = combo.currentData()
                uid = None
                if isinstance(data, dict):
                    uid = data.get('id') or data.get('usuario_id')
                else:
                    uid = getattr(data, 'id', None) or getattr(data, 'usuario_id', None)
                if isinstance(uid, int) and uid > 0:
                    return uid
        except Exception:
            pass
        return None

    def generate_checkin_qr_for_selected_user(self, current_tab):
        """Genera token temporal, muestra toast con QR y conecta señal al Main."""
        try:
            from widgets.qr_checkin_toast import QRCheckinToast
        except Exception:
            QRCheckinToast = None

        try:
            # Aclaración: el check-in es para asistencia general desde pestaña Usuarios
            try:
                usuarios_tab = self.tabs.get('usuarios') if hasattr(self, 'tabs') else None
                if usuarios_tab is not None and current_tab is not usuarios_tab:
                    from PyQt6.QtWidgets import QMessageBox
                    QMessageBox.information(
                        self,
                        "Check-in (Usuarios)",
                        "El check-in de sala se gestiona desde la pestaña Usuarios.\nCambie a esa pestaña y seleccione un socio."
                    )
                    return
            except Exception:
                pass

            user_id = self._resolve_selected_user_id_from_tab(current_tab)
            if not user_id:
                QMessageBox.information(self, "Sin selección", "Seleccione un socio antes de generar el QR de check-in.")
                return

            import uuid
            token = str(uuid.uuid4())
            expires_minutes = 3
            try:
                self.db_manager.crear_checkin_token(user_id, token, expires_minutes)
            except Exception as e:
                QMessageBox.critical(self, "Error", f"No se pudo crear el token de check-in: {e}")
                return

            # Empujar subida inmediata del outbox para acortar ventana de sincronización
            try:
                svc = getattr(self, 'sync_service', None)
                if svc is not None and hasattr(svc, 'flush_outbox_once_bg'):
                    svc.flush_outbox_once_bg(delay_ms=0)
            except Exception:
                pass

            try:
                if QRCheckinToast is None:
                    QMessageBox.information(self, "QR generado", f"Token: {token}\nInstale dependencia de QR para vista avanzada.")
                    return
                toast = QRCheckinToast(self, token, expires_minutes)
                try:
                    toast.tokenProcessed.connect(self.on_checkin_token_processed)
                except Exception:
                    pass
                toast.show_toast()
            except Exception as e:
                QMessageBox.warning(self, "Advertencia", f"Token creado, pero no se pudo mostrar el toast: {e}\nToken: {token}")
        except Exception as e:
            logging.error(f"Error generando QR de check-in: {e}")

    def on_checkin_token_processed(self, info: dict):
        """Señal al procesarse un token de check-in: registra asistencia local y refresca UI."""
        try:
            used = bool(info.get('used'))
            expired = bool(info.get('expired'))
            msg = "Check-in registrado" if used else ("Token expirado" if expired else "Token procesado")
            if hasattr(self, 'status_bar'):
                self.status_bar.showMessage(msg, 5000)
        except Exception:
            pass

        try:
            if used:
                token = str(info.get('token') or '').strip()
                usuario_id = None
                try:
                    if token and hasattr(self, 'db_manager') and hasattr(self.db_manager, 'obtener_checkin_por_token'):
                        row = self.db_manager.obtener_checkin_por_token(token)
                        if isinstance(row, dict) and row.get('usuario_id') is not None:
                            usuario_id = int(row.get('usuario_id'))
                except Exception:
                    usuario_id = None
                if usuario_id:
                    try:
                        from datetime import date
                        if hasattr(self.db_manager, 'registrar_asistencia'):
                            try:
                                self.db_manager.registrar_asistencia(usuario_id, date.today())
                            except ValueError:
                                pass
                        elif hasattr(self.db_manager, 'registrar_asistencia_comun'):
                            try:
                                self.db_manager.registrar_asistencia_comun(usuario_id, date.today())
                            except ValueError:
                                pass
                    except Exception:
                        pass
                    try:
                        if token and hasattr(self.db_manager, 'marcar_checkin_usado'):
                            self.db_manager.marcar_checkin_usado(token)
                    except Exception:
                        pass
                    try:
                        svc = getattr(self, 'sync_service', None)
                        if svc is not None and hasattr(svc, 'flush_outbox_once_bg'):
                            svc.flush_outbox_once_bg(delay_ms=0)
                    except Exception:
                        pass
        except Exception:
            pass

        try:
            self.update_tab_notifications()
        except Exception:
            pass

        try:
            usuarios_tab = self.tabs.get('usuarios')
            if usuarios_tab:
                if hasattr(usuarios_tab, 'load_users'):
                    try:
                        usuarios_tab.load_users(usar_cache=False)
                    except TypeError:
                        usuarios_tab.load_users()
                    try:
                        if hasattr(usuarios_tab, 'usuarios_modificados'):
                            usuarios_tab.usuarios_modificados.emit()
                    except Exception:
                        pass
                for m in ['refresh_data', 'actualizar_lista_usuarios', 'update_users_list', 'update_attendance_display']:
                    if hasattr(usuarios_tab, m):
                        getattr(usuarios_tab, m)()
                        break
        except Exception:
            pass

        try:
            reportes_tab = self.tabs.get('reportes')
            if reportes_tab:
                for m in ['refresh_all_reports', 'update_attendance_display', 'reload_kpis']:
                    if hasattr(reportes_tab, m):
                        getattr(reportes_tab, m)()
                        break
        except Exception:
            pass
            
    def get_gym_name(self) -> str:
        """Obtiene el nombre del gimnasio desde gym_data.txt"""
        try:
            from utils import _resolve_gym_data_path
            gym_data_path = _resolve_gym_data_path()
            if os.path.exists(gym_data_path):
                with open(gym_data_path, 'r', encoding='utf-8') as f:
                    for line in f:
                        if line.startswith('gym_name='):
                            return line.split('=', 1)[1].strip()
            # Fallback al nombre por defecto
            return "Gimnasio"
        except Exception as e:
            logging.error(f"Error obteniendo nombre del gimnasio: {e}")
            return "Gimnasio"
    
    def update_window_title(self):
        """Actualiza el título de la ventana con información dinámica"""
        try:
            # Obtener nombre del profesor logueado
            professor_name = ""
            profesor_id_real = None

            # Obtener nombre del usuario logueado
            if self.logged_in_user and hasattr(self.logged_in_user, 'get'):
                professor_name = self.logged_in_user.get('nombre', '')
            elif hasattr(self, 'logged_in_user') and self.logged_in_user:
                professor_name = str(self.logged_in_user)

            # Resolver profesor_id real a partir de usuario_id
            if self.user_role == 'profesor':
                try:
                    usuario_id_para_buscar = safe_get(self.logged_in_user, 'usuario_id') or safe_get(self.logged_in_user, 'id')
                    info = self.db_manager.obtener_profesor_por_usuario_id(usuario_id_para_buscar)
                    if info and hasattr(info, 'profesor_id'):
                        profesor_id_real = info.profesor_id
                except Exception as e:
                    logging.debug(f"No se pudo resolver profesor_id: {e}")
            
            # Construir título dinámico
            title_parts = [f"Sistema de Gestión de {self.gym_name}"]
            
            # Agregar información del modo y profesor
            if professor_name:
                title_parts.append(f"Modo: {self.user_role.capitalize()} ({professor_name})")
            else:
                title_parts.append(f"Modo: {self.user_role.capitalize()}")
            
            # Agregar duración de sesión en tiempo real si es profesor
            if self.user_role == 'profesor' and profesor_id_real:
                try:
                    duracion_info = self.db_manager.obtener_duracion_sesion_actual_profesor(profesor_id_real)
                    if duracion_info.get('success') and duracion_info.get('tiene_sesion_activa'):
                        tiempo_formateado = duracion_info.get('tiempo_formateado', '0h 0m')
                        title_parts.append(f"Sesión: {tiempo_formateado}")
                    else:
                        title_parts.append("Sesión: Sin sesión activa")
                except Exception as e:
                    logging.warning(f"Error obteniendo duración de sesión: {e}")
                    title_parts.append("Sesión: Error")
            
            # Establecer título completo
            full_title = " - ".join(title_parts)
            self.setWindowTitle(full_title)
            
        except Exception as e:
            logging.error(f"Error actualizando título de ventana: {e}")
            # Título de fallback
            self.setWindowTitle(f"Sistema de Gestión de {self.gym_name} - Modo: {self.user_role.capitalize()}")
    
    
    def setup_title_update_timer(self):
        """Configura el timer para actualizar el título periódicamente"""
        self.title_timer = QTimer()
        self.title_timer.timeout.connect(self.update_window_title)
        self.title_timer.start(60000)  # Actualizar cada 60 segundos para reducir carga en BD
            
    def _apply_global_tab_layout_margins(self, margin: int = 1):
        """Aplica márgenes de contenido = margin a todos los layouts dentro de cada pestaña."""
        try:
            from PyQt6.QtWidgets import QWidget

            def adjust_layouts_recursively(widget: QWidget):
                layout = widget.layout()
                if layout is not None:
                    layout.setContentsMargins(margin, margin, margin, margin)
                for child in widget.findChildren(QWidget):
                    child_layout = child.layout()
                    if child_layout is not None:
                        child_layout.setContentsMargins(margin, margin, margin, margin)

            for key, tab_widget in getattr(self, 'tabs', {}).items():
                adjust_layouts_recursively(tab_widget)
        except Exception as e:
            logging.warning(f"Error aplicando márgenes globales a layouts de pestañas: {e}")

    def setup_tabs(self):
        self.tabWidget.clear()
        # Carga inmediata de todas las pestañas (sin diferir)
        self.deferred_tab_constructors = {}
        
        try:
            with profile("setup_tabs: create usuarios"):
                self.tabs['usuarios'] = UserTabWidget(self.db_manager, self.payment_manager)
        except Exception as e:
            logging.error(f"Error creando UserTabWidget: {e}", exc_info=True)
            
        try:
            with profile("setup_tabs: create pagos"):
                self.tabs['pagos'] = PaymentsTabWidget(self.db_manager, self.payment_manager)
        except Exception as e:
            logging.error(f"Error creando PaymentsTabWidget: {e}", exc_info=True)
            
        try:
            with profile("setup_tabs: create reportes"):
                # Cargar dashboard de reportes sin diferir la inicialización interna
                self.tabs['reportes'] = ReportsTabWidget(self.db_manager, self.payment_manager, self.export_manager, defer_initial_load=False)
            if hasattr(self.tabs['reportes'], 'set_main_window'):
                self.tabs['reportes'].set_main_window(self)
            else:
                setattr(self.tabs['reportes'], 'main_window', self)
        except Exception as e:
            logging.error(f"Error creando ReportsTabWidget: {e}", exc_info=True)

        # Crear inmediatamente Rutinas, Clases, Profesores, Configuración
        try:
            with profile("setup_tabs: create rutinas"):
                self.tabs['rutinas'] = RoutinesTabWidget(self.db_manager, self.user_role)
        except Exception as e:
            logging.error(f"Error creando RoutinesTabWidget: {e}", exc_info=True)

        try:
            with profile("setup_tabs: create clases"):
                self.tabs['clases'] = ClassesTabWidget(self.db_manager, self.user_role)
        except Exception as e:
            logging.error(f"Error creando ClassesTabWidget: {e}", exc_info=True)

        try:
            with profile("setup_tabs: create profesores"):
                self.tabs['profesores'] = ProfessorsTabWidget(self.db_manager)
        except Exception as e:
            logging.error(f"Error creando ProfessorsTabWidget: {e}", exc_info=True)

        try:
            with profile("setup_tabs: create configuracion"):
                self.tabs['configuracion'] = ConfigTabWidget(self.db_manager, self.export_manager)
        except Exception as e:
            logging.error(f"Error creando ConfigTabWidget: {e}", exc_info=True)
        
        # Reorganización de pestañas según requerimiento: Usuarios, Pagos, Rutinas, Clases, Profesores, Dashboard, Configuración
        self.tab_indices = {}
        
        # Solo agregar pestañas que se crearon exitosamente
        if 'usuarios' in self.tabs:
            with profile("setup_tabs: add usuarios"):
                self.tab_indices['usuarios'] = self.tabWidget.addTab(self.tabs['usuarios'], QIcon(resource_path("assets/users.png")), "👥 Usuarios y Asistencias")
            try:
                QApplication.processEvents()
            except Exception:
                pass
        if 'pagos' in self.tabs:
            with profile("setup_tabs: add pagos"):
                self.tab_indices['pagos'] = self.tabWidget.addTab(self.tabs['pagos'], QIcon(resource_path("assets/money.png")), "💰 Pagos")
            try:
                QApplication.processEvents()
            except Exception:
                pass
        # Agregar el resto de pestañas creadas
        if 'rutinas' in self.tabs:
            with profile("setup_tabs: add rutinas"):
                self.tab_indices['rutinas'] = self.tabWidget.addTab(self.tabs['rutinas'], QIcon(resource_path("assets/routines.png")), "🏋️ Rutinas")
            try:
                QApplication.processEvents()
            except Exception:
                pass
        if 'clases' in self.tabs:
            with profile("setup_tabs: add clases"):
                self.tab_indices['clases'] = self.tabWidget.addTab(self.tabs['clases'], QIcon(resource_path("assets/classes.png")), "📅 Clases")
            try:
                QApplication.processEvents()
            except Exception:
                pass
        if 'profesores' in self.tabs:
            with profile("setup_tabs: add profesores"):
                self.tab_indices['profesores'] = self.tabWidget.addTab(self.tabs['profesores'], QIcon(resource_path("assets/student_icon.png")), "👨‍🏫 Profesores")
            try:
                QApplication.processEvents()
            except Exception:
                pass
        if 'reportes' in self.tabs:
            with profile("setup_tabs: add reportes"):
                self.tab_indices['reportes'] = self.tabWidget.addTab(self.tabs['reportes'], QIcon(resource_path("assets/payment_rate.png")), "📊 Dashboard de Reportes")
            try:
                QApplication.processEvents()
            except Exception:
                pass
        if 'configuracion' in self.tabs:
            with profile("setup_tabs: add configuracion"):
                self.tab_indices['configuracion'] = self.tabWidget.addTab(self.tabs['configuracion'], QIcon(resource_path("assets/gear.png")), "⚙️ Configuración")
        self.setup_keyboard_shortcuts()
        self.connect_signals()
        self.update_tab_notifications()  # Actualizar notificaciones iniciales

        # Salvaguarda: eliminar duplicados de pestañas por texto (p. ej. "💰 Pagos")
        try:
            def _remove_duplicate_tabs_by_text(text: str):
                indices = []
                for i in range(self.tabWidget.count()):
                    try:
                        if self.tabWidget.tabText(i) == text:
                            indices.append(i)
                    except Exception:
                        pass
                if len(indices) > 1:
                    for idx in sorted(indices[1:], reverse=True):
                        try:
                            self.tabWidget.removeTab(idx)
                        except Exception:
                            pass
            _remove_duplicate_tabs_by_text("💰 Pagos")
            # Re-sincronizar el mapa de índices tras posibles cambios
            try:
                for key, widget in self.tabs.items():
                    idx = self.tabWidget.indexOf(widget)
                    if idx >= 0:
                        self.tab_indices[key] = idx
            except Exception:
                pass
        except Exception:
            pass

        # Preload opcional (mantener en background, ya no se usa carga diferida de tabs)
        try:
            QTimer.singleShot(0, self._start_preload_manager)
        except Exception:
            pass

    def _start_preload_manager(self):
        """Inicializa y arranca PreloadManager en segundo plano."""
        try:
            search_mgr = getattr(self, 'search_manager', None)
            self.preload_manager = PreloadManager(self.db_manager, getattr(self, 'payment_manager', None), search_manager=search_mgr)
            self.preload_manager.start()
            logging.info("PreloadManager iniciado en segundo plano")
        except Exception as e:
            logging.debug(f"No se pudo iniciar PreloadManager: {e}")
    
    def setup_keyboard_shortcuts(self):
        """Configura los shortcuts de teclado Ctrl+1-8 para navegación rápida entre pestañas"""
        shortcuts = [
            ('usuarios', 'Ctrl+1'),
            ('pagos', 'Ctrl+2'),
            ('rutinas', 'Ctrl+3'),
            ('clases', 'Ctrl+4'),
            ('profesores', 'Ctrl+5'),
            ('reportes', 'Ctrl+6'),
            ('configuracion', 'Ctrl+7')
        ]
        
        for tab_key, shortcut_key in shortcuts:
            if tab_key in self.tab_indices:
                shortcut = QShortcut(QKeySequence(shortcut_key), self)
                shortcut.activated.connect(lambda tk=tab_key: self.switch_to_tab(tk))
                # Actualizar tooltip de la pestaña con el shortcut
                current_text = self.tabWidget.tabText(self.tab_indices[tab_key])
                self.tabWidget.setTabToolTip(self.tab_indices[tab_key], f"{current_text} ({shortcut_key})")
        
        # Shortcut para activar búsqueda global
        search_shortcut = QShortcut(QKeySequence('Ctrl+F'), self)
        search_shortcut.activated.connect(self.activate_search)
    
    def switch_to_tab(self, tab_key: str):
        """Cambia a la pestaña especificada si está visible"""
        if tab_key in self.tab_indices:
            tab_index = self.tab_indices[tab_key]
            if not self.tabWidget.isTabVisible(tab_index):
                return  # No cambiar si la pestaña no está visible
            self.tabWidget.setCurrentIndex(tab_index)
    
    def setup_search_bar_in_tabs(self):
        """Configura la barra de búsqueda integrada en el área de pestañas con el nombre del gimnasio"""
        # Crear un widget personalizado para el área de pestañas con nombre del gimnasio y búsqueda
        tab_header_widget = QWidget()
        tab_header_layout = QHBoxLayout(tab_header_widget)
        # Ajustar márgenes para perfecta alineación con las pestañas
        tab_header_layout.setContentsMargins(12, 4, 16, 4)  # Márgenes balanceados
        tab_header_layout.setSpacing(16)  # Espaciado consistente
        
        # Contenedor del nombre del gimnasio
        gym_name_container = QWidget()
        gym_name_container.setObjectName("gym_name_container")
        gym_name_layout = QHBoxLayout(gym_name_container)
        gym_name_layout.setContentsMargins(0, 0, 0, 0)
        gym_name_layout.setSpacing(6)  # Espaciado entre ícono y texto
        
        # Ícono del gimnasio (reemplazar emoji por ícono real)
        gym_icon_label = QLabel()
        gym_icon_label.setObjectName("gym_icon")
        gym_icon_label.setAlignment(Qt.AlignmentFlag.AlignVCenter | Qt.AlignmentFlag.AlignCenter)
        gym_icon_label.setFixedSize(28, 28)
        try:
            gym_pixmap = QPixmap(resource_path("assets/gym_logo.png"))
            if not gym_pixmap.isNull():
                gym_icon_label.setPixmap(gym_pixmap.scaled(24, 24, Qt.AspectRatioMode.KeepAspectRatio, Qt.TransformationMode.SmoothTransformation))
        except Exception:
            pass
        gym_icon_label.setStyleSheet("""
            QLabel#gym_icon {
                background-color: transparent;
                border: none;
                margin: 0px;
                padding: 0px;
            }
        """)
        
        # Nombre del gimnasio con tipografía elegante y efectos visuales mejorados
        self.gym_name_label = QLabel("Gimnasio")
        # Establecer el nombre real del gimnasio al iniciar (evitar fallback)
        try:
            initial_gym_name = getattr(self, 'gym_name', None) or self.get_gym_name()
            if initial_gym_name:
                self.gym_name_label.setText(initial_gym_name)
        except Exception:
            # Mantener fallback en caso de error
            pass
        self.gym_name_label.setObjectName("gym_name_tab")
        self.gym_name_label.setFont(QFont("Segoe UI", 14, QFont.Weight.Bold))  # Tamaño aumentado para mayor visibilidad
        self.gym_name_label.setAlignment(Qt.AlignmentFlag.AlignVCenter | Qt.AlignmentFlag.AlignLeft)
        # Establecer altura consistente para alineación perfecta
        # Altura aumentada para acomodar padding y borde del QSS
        self.gym_name_label.setFixedHeight(34)
        # Usar el QSS dinámico global en lugar de estilos inline y aplicar marco
        # Esto permite que los colores y el borde se ajusten según el tema
        self.gym_name_label.setProperty("dynamic_css", "true")
        self.gym_name_label.setProperty("class", "panel_title")
        
        gym_name_layout.addWidget(gym_icon_label)
        gym_name_layout.addWidget(self.gym_name_label)
        gym_name_container.setFixedHeight(34)  # Altura fija del contenedor (acomoda borde)
        
        # Espaciador flexible para separar nombre del gimnasio y búsqueda
        tab_header_layout.addWidget(gym_name_container)
        tab_header_layout.addStretch()
        
        # Widget de búsqueda con tamaño ajustado
        self.search_widget = GlobalSearchWidget(self.search_manager)
        self.search_widget.setObjectName("global_search_widget")
        self.search_widget.setMaximumWidth(280)  # Ancho optimizado
        # Altura aumentada para acomodar padding y borde del QSS
        self.search_widget.setFixedHeight(32)  # Altura consistente con borde
        # Habilitar QSS dinámico para que tome borde y colores del tema
        self.search_widget.setProperty("dynamic_css", "true")
        
        # Contenedor para la búsqueda con estilo integrado y mejor alineación
        search_container = QFrame()
        search_container.setStyleSheet("""
            QFrame {
                background-color: transparent;
                border: none;
                margin: 0px;
                padding: 0px;
            }
        """)
        
        search_container_layout = QHBoxLayout(search_container)
        search_container_layout.setContentsMargins(0, 0, 0, 0)
        search_container_layout.setSpacing(10)  # Espaciado optimizado entre búsqueda y ayuda
        search_container_layout.addWidget(self.search_widget)
        
        # Label de ayuda con mejor alineación
        help_label = QLabel("Ctrl+F")
        help_label.setObjectName("search_help_label")
        help_label.setAlignment(Qt.AlignmentFlag.AlignVCenter | Qt.AlignmentFlag.AlignCenter)
        help_label.setFixedHeight(28)  # Altura consistente
        help_label.setStyleSheet("""
            QLabel#search_help_label {
                color: #88C0D0;
                font-size: 9px;
                font-weight: 500;
                padding: 2px 6px;
                background-color: rgba(136, 192, 208, 0.1);
                border-radius: 3px;
                border: 1px solid rgba(136, 192, 208, 0.3);
            }
        """)
        search_container_layout.addWidget(help_label)
        
        # Establecer altura fija del contenedor de búsqueda (evitar recortes de borde)
        search_container.setFixedHeight(32)
        tab_header_layout.addWidget(search_container)
        
        # Establecer altura fija del widget principal para alineación perfecta
        tab_header_widget.setFixedHeight(32)  # Altura reducida y consistente
        tab_header_widget.setStyleSheet("""
            QWidget {
                background-color: transparent;
                border: none;
                margin: 0px;
                padding: 0px;
            }
        """)
        
        # Establecer el widget personalizado como corner widget del TabWidget
        self.tabWidget.setCornerWidget(tab_header_widget, Qt.Corner.TopRightCorner)
    
    # Método setup_header eliminado - funcionalidad movida al área de pestañas
    
    def update_gym_header(self):
        """Actualiza el nombre del gimnasio en el área de pestañas con la información de branding actual"""
        if hasattr(self, 'branding_config') and self.branding_config:
            # Actualizar nombre del gimnasio en el área de pestañas
            gym_name = self.branding_config.get('gym_name', 'Gimnasio')
            if hasattr(self, 'gym_name_label'):
                self.gym_name_label.setText(gym_name)
        else:
            # Valor por defecto
            if hasattr(self, 'gym_name_label'):
                self.gym_name_label.setText("Gimnasio")
    
    def connect_search_signals(self):
        """Conecta las señales de búsqueda"""
        self.search_widget.user_selected.connect(self.navigate_to_user)
        self.search_widget.payment_selected.connect(self.navigate_to_payment)
        self.search_widget.class_selected.connect(self.navigate_to_class)
        self.search_widget.routine_selected.connect(self.navigate_to_routine)
    
    def navigate_to_user(self, user_id: int):
        """Navega a la pestaña de usuarios y selecciona el usuario"""
        self.tabWidget.setCurrentWidget(self.tabs['usuarios'])
        self.tabs['usuarios'].select_user(user_id)
    
    def navigate_to_payment(self, payment_id: int):
        """Navega a la pestaña de pagos y selecciona el pago"""
        self.tabWidget.setCurrentWidget(self.tabs['pagos'])
        self.tabs['pagos'].select_payment(payment_id)
    
    def navigate_to_class(self, class_id: int):
        """Navega a la pestaña de clases y selecciona la clase"""
        self.tabWidget.setCurrentWidget(self.tabs['clases'])
        self.tabs['clases'].select_class(class_id)
    
    def navigate_to_routine(self, routine_id: int):
        """Navega a la pestaña de rutinas y selecciona la rutina"""
        self.tabWidget.setCurrentWidget(self.tabs['rutinas'])
        self.tabs['rutinas'].select_routine(routine_id)
    
    def activate_search(self):
        """Activa la barra de búsqueda global"""
        self.search_widget.set_focus()
    
    def setup_status_bar(self):
        """Configura la barra de estado con indicadores de alertas (solo para pestaña Configuración)"""
        self.status_bar = self.statusBar()
        
        # Contador de horas mensuales para profesores
        self.monthly_hours_label = QLabel("⏰ Horas del mes: --")
        self.monthly_hours_label.setToolTip("Horas trabajadas en el mes actual")
        self.monthly_hours_label.setStyleSheet(
            "QLabel { color: #2ecc71; font-weight: bold; padding: 2px 8px; }"
        )
        
        # Indicador de alertas críticas
        self.critical_alerts_label = QLabel("🚨 0")
        self.critical_alerts_label.setToolTip("Alertas críticas")
        self.critical_alerts_label.setStyleSheet(
            "QLabel { color: #e74c3c; font-weight: bold; padding: 2px 8px; }"
        )
        
        # Indicador de advertencias
        self.warning_alerts_label = QLabel("⚠️ 0")
        self.warning_alerts_label.setToolTip("Advertencias")
        self.warning_alerts_label.setStyleSheet(
            "QLabel { color: #f39c12; font-weight: bold; padding: 2px 8px; }"
        )
        
        # Botón para abrir panel de alertas
        self.alerts_button = QPushButton("📊 Ver Alertas")
        self.alerts_button.setToolTip("Abrir panel de alertas")
        self.alerts_button.clicked.connect(self.show_alerts_panel)
        # Migrado al sistema CSS dinámico - usar objectName para aplicar estilos automáticamente
        self.alerts_button.setObjectName("alerts_button")
        self.alerts_button.setProperty("class", "secondary")
        
        # Estado del sistema
        self.system_status_label = QLabel("✅ Sistema OK")
        self.system_status_label.setToolTip("Estado del sistema")

        # Indicador de conectividad en tiempo real
        self.connectivity_label = QLabel("🟡 Conectividad…")
        self.connectivity_label.setToolTip("Estado de internet, base de datos y WhatsApp")
        self.connectivity_label.setStyleSheet(
            "QLabel { font-weight: bold; padding: 2px 8px; }"
        )

        # Resumen breve de replicación en barra de estado
        self.replication_status_label = QLabel("🛰️ Replicación: N/A")
        self.replication_status_label.setToolTip("Estado de replicación lógica (PostgreSQL) — suscripción y pequeño lag")
        self.replication_status_label.setStyleSheet(
            "QLabel { color: #607D8B; font-weight: bold; padding: 2px 8px; }"
        )

        # Advertencia visual separada para pendientes no accionables (ej. WhatsApp)
        self.whatsapp_pending_label = QLabel("")
        self.whatsapp_pending_label.setObjectName("whatsapp_pending_label")
        self.whatsapp_pending_label.setToolTip("Pendientes no accionables – requieren conexión/cliente WhatsApp")
        self.whatsapp_pending_label.setStyleSheet(
            "QLabel { color: #e67e22; font-weight: bold; padding: 2px 8px; }"
        )
        self.whatsapp_pending_label.hide()

        # Indicador de operaciones programadas por backoff
        self.scheduled_pending_label = QLabel("")
        self.scheduled_pending_label.setObjectName("scheduled_pending_label")
        self.scheduled_pending_label.setToolTip("Operaciones programadas por backoff (reintentos)")
        self.scheduled_pending_label.setStyleSheet(
            "QLabel { color: #3498db; font-weight: bold; padding: 2px 8px; }"
        )
        self.scheduled_pending_label.hide()
        
        # Agregar widgets a la barra de estado (con contador de horas)
        self.status_bar.addPermanentWidget(self.monthly_hours_label)
        self.status_bar.addPermanentWidget(self.critical_alerts_label)
        self.status_bar.addPermanentWidget(self.warning_alerts_label)
        self.status_bar.addPermanentWidget(self.alerts_button)
        self.status_bar.addPermanentWidget(self.system_status_label)
        self.status_bar.addPermanentWidget(self.replication_status_label)
        self.status_bar.addPermanentWidget(self.connectivity_label)
        self.status_bar.addPermanentWidget(self.whatsapp_pending_label)
        self.status_bar.addPermanentWidget(self.scheduled_pending_label)
        # Botón pequeño para testear y reiniciar redes automáticamente si fallan
        try:
            self.test_networks_btn = QPushButton("🧪 Test redes")
            self.test_networks_btn.setObjectName("test_networks_btn")
            self.test_networks_btn.setProperty("class", "secondary")
            try:
                self.test_networks_btn.setFixedHeight(24)
            except Exception:
                pass
            self.test_networks_btn.setToolTip("Probar salud local y túnel público; reiniciar si fallan")
            self.test_networks_btn.clicked.connect(self._test_networks_from_status_bar)
            self.status_bar.addPermanentWidget(self.test_networks_btn)
        except Exception:
            logging.warning("No se pudo agregar el botón de test de redes a la barra de estado")
        
        # Mensaje inicial
        self.status_bar.showMessage("Sistema iniciado correctamente")
        
        # Timer para actualizar horas mensuales en tiempo real
        self.hours_timer = QTimer()
        self.hours_timer.timeout.connect(self.update_monthly_hours)
        self.hours_timer.start(30000)  # Actualizar cada 30 segundos
        
        # Actualizar horas inicialmente
        self.update_monthly_hours()
        
        # Mantener UX actual: ocultar barra de estado por defecto
        self.status_bar.hide()

        # Timer para actualizar conectividad en tiempo real
        self.connectivity_timer = QTimer()
        self.connectivity_timer.timeout.connect(self.update_connectivity_indicator)
        self.connectivity_timer.start(5000)  # cada 5 segundos
        self.update_connectivity_indicator()

        # Intentar auto-configurar replicación en background (idempotente)
        try:
            QTimer.singleShot(1500, self._start_replication_setup_thread)
        except Exception:
            pass

        # Instalar triggers de outbox en background (idempotente)
        try:
            QTimer.singleShot(2500, self._start_outbox_trigger_install_thread)
        except Exception:
            pass

        # Servicio de sincronización: observar cola local y refrescar UI al cambiar
        try:
            from utils_modules.sync_service import SyncService
            self.sync_service = SyncService(poll_interval_ms=3000, db_manager=self.db_manager)
            # Cuando cambian los pendientes, refrescar el indicador de conectividad
            self.sync_service.on_pending_change = lambda count: self.update_connectivity_indicator()
            # Cuando la cola queda vacía, actualizar indicadores de pestañas
            self.sync_service.on_queue_empty = lambda: self.update_tab_notifications()
            # Adjuntar estado del OutboxPoller para refrescar conectividad
            try:
                self.sync_service.attach_outbox_status_callback(lambda status: self.update_connectivity_indicator())
            except Exception:
                pass
            self.sync_service.start()
            try:
                # Parada limpia cuando se destruya la ventana
                self.destroyed.connect(self.sync_service.stop)
            except Exception:
                pass
        except Exception as e:
            try:
                logging.warning(f"No se pudo iniciar SyncService: {e}")
            except Exception:
                pass

        # Observador de replicación: refrescar UI cuando lleguen cambios entrantes
        try:
            from utils_modules.replication_observer import ReplicationObserver
            self.replication_observer = ReplicationObserver(db_manager=self.db_manager, poll_interval_ms=5000)
            self.replication_observer.on_inbound_change = self._on_replication_inbound_change
            # Registrar callback de estado para usar métricas en el indicador de conectividad
            try:
                self._last_replication_metrics = {}
            except Exception:
                pass
            try:
                self.replication_observer.on_status_update = self._on_replication_status_update
            except Exception:
                pass
            self.replication_observer.start()
            try:
                # Parada limpia cuando se destruya la ventana
                self.destroyed.connect(self.replication_observer.stop)
            except Exception:
                pass
        except Exception as e:
            try:
                logging.warning(f"No se pudo iniciar ReplicationObserver: {e}")
            except Exception:
                pass

    def _test_networks_from_status_bar(self):
        """Prueba salud de red local y pública y reinicia automáticamente si fallan."""
        try:
            host = getattr(self, 'web_host', os.getenv("WEBAPP_HOST", "127.0.0.1"))
            port = int(getattr(self, 'web_port', int(os.getenv("WEBAPP_PORT", "8000"))))
            public_url = getattr(self, 'public_url', None)
            # Preparar callbacks de reinicio seguros
            def _restart_server():
                try:
                    from webapp.server import start_web_server
                    start_web_server(self.db_manager, host=host, port=port)
                except Exception:
                    pass

            def _restart_tunnel():
                try:
                    from webapp.server import start_public_tunnel
                    start_public_tunnel(local_port=port)
                except Exception:
                    pass

            res = test_networks_and_restart(
                host=host,
                port=port,
                public_url=public_url,
                restart_server_cb=_restart_server,
                restart_tunnel_cb=_restart_tunnel,
            )
            # Feedback en barra de estado
            local_ok = res.get("local_ok", False)
            public_ok = res.get("public_ok", False)
            msg = []
            msg.append(f"Local: {'OK' if local_ok else 'REINICIADA' if res.get('server_restarted') else 'ERROR'}")
            # Solo mostrar pública si está habilitado el túnel
            try:
                if get_public_tunnel_enabled():
                    msg.append(f"Público: {'OK' if public_ok else 'REINICIADA' if res.get('tunnel_restarted') else 'ERROR'}")
            except Exception:
                pass
            self.status_bar.showMessage(" | ".join(msg), 8000)
        except Exception as e:
            logging.error(f"Error al testear/reiniciar redes desde barra de estado: {e}")

    def update_connectivity_indicator(self):
        """Actualiza el indicador de conectividad considerando replicación lógica (PostgreSQL)."""
        try:
            # Estado de internet (ligero): asumir OK para no bloquear UI
            internet_ok = True

            # Estado de base de datos
            db_ok = False
            try:
                with self.db_manager.get_connection_context() as conn:
                    conn.cursor().execute("SELECT 1")
                db_ok = True
            except Exception:
                db_ok = False

            # Estado de WhatsApp
            whatsapp_ok = False
            try:
                whatsapp_ok = getattr(self, 'whatsapp_manager', None) is not None and getattr(self.whatsapp_manager, 'wa_client', None) is not None
            except Exception:
                whatsapp_ok = False

            # Pendientes de replicación lógica (PostgreSQL) - obtener desde sync_client si disponible
            try:
                from sync_client import get_pending_count  # type: ignore
                pending_ops = get_pending_count()
            except Exception:
                pending_ops = None

            # Desglose mínimo para la UI (sin sistema legacy)
            pending_breakdown = {
                'actionable_whatsapp': 0,
                'whatsapp': 0,
                'scheduled': 0,
                'scheduled_db': 0,
                'scheduled_whatsapp': 0,
            }
            actionable_whatsapp = 0
            total_whatsapp = 0
            scheduled_total = 0
            scheduled_db = 0
            scheduled_wa = 0

            # Determinar estado general
            all_ok = internet_ok and db_ok and (whatsapp_ok or not hasattr(self, 'whatsapp_manager'))

            if all_ok and (pending_ops in (0, None)):
                self.connectivity_label.setText("🟢 Conectado")
                self.connectivity_label.setStyleSheet("QLabel { color: #2ecc71; font-weight: bold; padding: 2px 8px; }")
            elif db_ok or internet_ok:
                status_text = "🟡 Parcial"
                if isinstance(pending_ops, int) and pending_ops:
                    status_text += f" • cola {pending_ops}"
                self.connectivity_label.setText(status_text)
                self.connectivity_label.setStyleSheet("QLabel { color: #f1c40f; font-weight: bold; padding: 2px 8px; }")
            else:
                status_text = "🔴 Sin conexión"
                if isinstance(pending_ops, int) and pending_ops:
                    status_text += f" • encoladas {pending_ops}"
                self.connectivity_label.setText(status_text)
                self.connectivity_label.setStyleSheet("QLabel { color: #e74c3c; font-weight: bold; padding: 2px 8px; }")

            # Métricas de replicación lógica obtenidas del observador
            repl = getattr(self, '_last_replication_metrics', {}) or {}
            has_sub = bool(repl.get('has_subscription'))
            apply_lag = repl.get('max_apply_lag_s')
            sync_states = repl.get('sync_states') or []
            lag_text = (f"{float(apply_lag):.1f}s" if isinstance(apply_lag, (int, float)) else "N/A")
            sync_text = ", ".join(sync_states) if sync_states else "N/A"

            # Razón técnica si no hay suscripción (de auto-setup)
            setup_res = getattr(self, '_replication_setup_result', {}) or {}
            remote_checks = None
            try:
                for step in setup_res.get('steps', []):
                    if 'remote_checks' in step:
                        remote_checks = step['remote_checks']
                        break
            except Exception:
                remote_checks = None
            reason_text = ''
            if not has_sub and remote_checks and not remote_checks.get('ok'):
                wl = remote_checks.get('wal_level')
                slots = remote_checks.get('max_replication_slots')
                senders = remote_checks.get('max_wal_senders')
                reason_text = f" (remoto: wal_level={wl}, slots={slots}, senders={senders})"

            # Tooltip detallado
            tooltip = (
                f"Internet: {'OK' if internet_ok else 'FALLA'}\n"
                f"Base de datos: {'OK' if db_ok else 'FALLA'}\n"
                f"WhatsApp: {'OK' if whatsapp_ok else 'FALLA'}\n"
                f"Replicación: {'OK' if has_sub else 'SIN SUSCRIPCIÓN'} | Lag: {lag_text} | Estado: {sync_text}{reason_text}\n"
                f"Cola local (pendientes): {'N/A' if pending_ops is None else pending_ops}\n"
                f"Programados (backoff): {scheduled_total}"
            )
            self.connectivity_label.setToolTip(tooltip)

            # Resumen breve en la barra de estado (no intrusivo)
            try:
                short = f"Rep: {'OK' if has_sub else 'SIN SUB'} • lag {lag_text} • {sync_text}"
                self.replication_status_label.setText(f"🛰️ {short}")
                self.replication_status_label.setStyleSheet(
                    "QLabel { color: %s; font-weight: bold; padding: 2px 8px; }" % ("#2ecc71" if has_sub else "#e74c3c")
                )
                # Tooltip específico de replicación con razón técnica si aplica
                self.replication_status_label.setToolTip(
                    f"Replicación: {'OK' if has_sub else 'SIN SUSCRIPCIÓN'} • lag {lag_text} • {sync_text}{reason_text}"
                )
            except Exception:
                pass

            # Overlay de sincronización inicial: mostrar mientras haya tablas no listas
            try:
                if has_sub and not getattr(self, '_sync_dnd', False):
                    prog = self._get_initial_sync_progress_counts()
                    if prog and int(prog.get('total', 0)) > 0 and int(prog.get('ready', 0)) < int(prog.get('total', 0)):
                        ready = int(prog.get('ready', 0))
                        total = int(prog.get('total', 0))
                        detail = f"Tablas listas: {ready}/{total} • lag {lag_text}"
                        self.show_sync_overlay("Sincronizando datos iniciales…", ready=ready, total=total, detail=detail)
                    else:
                        self.hide_sync_overlay()
                else:
                    self.hide_sync_overlay()
            except Exception:
                # No romper si falla la detección
                pass

            # Actualizar advertencia separada para pendientes no accionables de WhatsApp
            try:
                if total_whatsapp and actionable_whatsapp == 0 and not whatsapp_ok:
                    self.whatsapp_pending_label.setText(f"⚠️ WhatsApp pendiente: {total_whatsapp}")
                    # Tooltip con detalle de pendientes
                    try:
                        breakdown_parts = [
                            f"Total: {pending_breakdown.get('total', 0)}",
                            f"DB: {pending_breakdown.get('db', 0)}",
                            f"WhatsApp: {pending_breakdown.get('whatsapp', 0)}",
                            f"Accionables (DB): {pending_breakdown.get('actionable_db', 0)}",
                            f"Accionables (WhatsApp): {pending_breakdown.get('actionable_whatsapp', 0)}",
                        ]
                        breakdown_text = "\n".join(breakdown_parts)
                        hint = "Requiere internet y sesión de WhatsApp para drenar."
                        self.whatsapp_pending_label.setToolTip(f"Desglose cola offline:\n{breakdown_text}\n\n{hint}")
                    except Exception:
                        pass
                    self.whatsapp_pending_label.show()
                else:
                    self.whatsapp_pending_label.hide()

            except Exception:
                # No romper UI por errores en etiqueta de WhatsApp
                try:
                    self.whatsapp_pending_label.hide()
                except Exception:
                    pass

            # Mostrar indicador de operaciones programadas
            try:
                if scheduled_total > 0:
                    self.scheduled_pending_label.setText(
                        f"🕘 Programados: {scheduled_total} (DB {scheduled_db}, WA {scheduled_wa})"
                    )
                    self.scheduled_pending_label.show()
                else:
                    self.scheduled_pending_label.hide()
            except Exception:
                # No romper UI por errores en etiqueta secundaria
                self.scheduled_pending_label.hide()

        except Exception as e:
            # No romper UI por errores en el indicador
            logging.debug(f"Error actualizando indicador de conectividad: {e}")

    def _on_replication_status_update(self, metrics: dict):
        """Callback de estado del observador de replicación para refrescar la UI."""
        try:
            self._last_replication_metrics = metrics or {}
            # Refrescar indicador para reflejar métricas recientes
            self.update_connectivity_indicator()
        except Exception:
            pass

    def _is_initial_sync_in_progress(self) -> bool:
        """Detecta si hay sincronización inicial en curso consultando pg_subscription_rel."""
        try:
            with self.db_manager.get_connection_context() as conn:
                cur = conn.cursor()
                # Contar tablas no listas (estados distintos de 'r' ready)
                try:
                    cur.execute("SELECT COUNT(*) FROM pg_subscription_rel WHERE srsubstate <> 'r'")
                except Exception:
                    # Fallback si columna/tabla no existe (versiones antiguas)
                    return False
                row = cur.fetchone()
                pending = int(row[0]) if row and row[0] is not None else 0
                return pending > 0
        except Exception:
            return False

    def _get_initial_sync_progress_counts(self) -> dict | None:
        """Devuelve dict con 'ready' y 'total' consultando pg_subscription_rel, o None si no disponible."""
        try:
            with self.db_manager.get_connection_context() as conn:
                cur = conn.cursor()
                try:
                    cur.execute("""
                        SELECT
                            SUM(CASE WHEN srsubstate = 'r' THEN 1 ELSE 0 END) AS ready,
                            COUNT(*) AS total
                        FROM pg_subscription_rel
                    """)
                except Exception:
                    return None
                row = cur.fetchone() or (0, 0)
                ready = int(row[0] or 0)
                total = int(row[1] or 0)
                return {"ready": ready, "total": total}
        except Exception:
            return None

    def _start_replication_setup_thread(self):
        """Inicia hilo ligero para auto-setup de replicación sin bloquear la UI."""
        try:
            import threading
            t = threading.Thread(target=self._auto_setup_logical_replication, daemon=True)
            t.start()
            try:
                self._replication_setup_thread = t
            except Exception:
                pass
        except Exception:
            pass

    def _auto_setup_logical_replication(self):
        """Asegura PUBLICATION/SUBSCRIPTION y siembra keyring usando config.json."""
        try:
            from pathlib import Path
            from utils_modules.replication_setup import ensure_logical_replication_from_config_path

            base_dir = Path(__file__).resolve().parent
            cfg_path = base_dir / 'config' / 'config.json'
            res = ensure_logical_replication_from_config_path(cfg_path)
            try:
                self._replication_setup_result = res
            except Exception:
                pass
            # Si la suscripción fue creada, mostrar overlay de sincronización inicial
            try:
                for step in (res.get('steps') or []):
                    sub = step.get('subscription') if isinstance(step, dict) else None
                    if sub and sub.get('changed'):
                        QTimer.singleShot(0, lambda: self.show_sync_overlay("Sincronizando datos iniciales…"))
                        try:
                            QTimer.singleShot(500, self._start_initial_reconciliation_thread)
                        except Exception:
                            pass
                        break
            except Exception:
                pass
            # Feedback no bloqueante en la barra de estado
            try:
                def _update_msg():
                    ok = bool(res.get('ok'))
                    msg = f"Replicación {'OK' if ok else 'FALLÓ'}"
                    self.status_bar.showMessage(msg, 5000)
                    # Refrescar indicador con métricas actuales
                    self.update_connectivity_indicator()
                QTimer.singleShot(0, _update_msg)
            except Exception:
                pass
        except Exception as e:
            try:
                logging.warning(f"Auto-setup de replicación falló: {e}")
            except Exception:
                pass

    def _start_outbox_trigger_install_thread(self):
        try:
            import threading
            t = threading.Thread(target=self._auto_install_outbox_triggers, daemon=True)
            t.start()
            try:
                self._outbox_install_thread = t
            except Exception:
                pass
        except Exception:
            pass

    def _auto_install_outbox_triggers(self):
        """Instala tabla/función/índices del outbox y triggers idempotentes."""
        try:
            from scripts.install_outbox_triggers import run as install_outbox
            res = install_outbox()
            try:
                self._outbox_install_result = res
            except Exception:
                pass
            # Notificar sin bloquear
            try:
                def _update_msg():
                    # run() imprime; no devuelve dict, asumimos OK si no hubo excepción
                    msg = "Outbox instalado"
                    self.status_bar.showMessage(msg, 5000)
                    self.update_connectivity_indicator()
                QTimer.singleShot(0, _update_msg)
            except Exception:
                pass
        except Exception as e:
            try:
                logging.warning(f"Auto-instalar outbox falló: {e}")
            except Exception:
                pass
    
    def _start_initial_reconciliation_thread(self):
        try:
            import threading
            t = threading.Thread(target=self._auto_initial_reconciliation, daemon=True)
            t.start()
            try:
                self._initial_recon_thread = t
            except Exception:
                pass
        except Exception:
            pass

    def _auto_initial_reconciliation(self):
        """Ejecuta reconciliación inicial bidireccional sin bloquear la UI.
        - Usa nombres de suscripción/publicación desde config/replication.
        - Resuelve tablas dinámicamente desde config/sync_tables.json.
        - Soporta PK compuestas y actualiza filas con updated_at.
        """
        try:
            QTimer.singleShot(0, lambda: self.show_sync_overlay("Reconciliando datos históricos…", detail="Preparando reconciliación local⇄remoto"))
        except Exception:
            pass
        # Local→Remoto para tablas transaccionales
        try:
            from pathlib import Path
            import json
            from scripts import reconcile_local_remote_once as L2R
            cfg = L2R.load_config()
            rep_cfg = (cfg.get('replication') or {})
            subname = rep_cfg.get('subscription_name') or 'gym_sub'
            local_params = L2R.build_conn_params('local', cfg)
            remote_params = L2R.build_conn_params('remote', cfg)
            local_conn = L2R.connect(local_params)
            remote_conn = L2R.connect(remote_params)
            try:
                L2R.disable_subscription(local_conn, subname)
            except Exception:
                pass
            try:
                tables_to_process = list(getattr(L2R, 'DEFAULT_TABLES', []))
                try:
                    sync_path = Path(__file__).resolve().parent / 'config' / 'sync_tables.json'
                    if sync_path.exists():
                        with open(sync_path, 'r', encoding='utf-8') as f:
                            sync_cfg = json.load(f) or {}
                        uploads = sync_cfg.get('uploads_local_to_remote') or []
                        if uploads:
                            tables_to_process = uploads
                except Exception:
                    pass
                total_inserted = 0
                total_updated = 0
                for table in tables_to_process:
                    try:
                        pk_cols = L2R.get_pk_columns(local_conn, 'public', table)
                        # Inserciones: claves presentes en local y faltantes en remoto
                        missing = L2R.fetch_missing_pks(local_conn, remote_conn, 'public', table, pk_cols)
                        if missing:
                            rows = L2R.fetch_rows_by_pk(local_conn, 'public', table, pk_cols, missing)
                            inserted = L2R.insert_rows_remote(remote_conn, 'public', table, rows, pk_cols, dry_run=False)
                            total_inserted += int(inserted or 0)
                        # Actualizaciones: filas existentes con updated_at más reciente en local
                        updated = L2R.reconcile_updates_remote(local_conn, remote_conn, 'public', table, pk_cols, dry_run=False)
                        total_updated += int(updated or 0)
                    except Exception:
                        pass
                try:
                    # Feedback discreto
                    QTimer.singleShot(0, lambda: self.status_bar.showMessage(f"Local→Remoto: insertadas {total_inserted}, actualizadas {total_updated}", 5000))
                except Exception:
                    pass
            finally:
                try:
                    L2R.enable_subscription(local_conn, subname)
                except Exception:
                    pass
                try:
                    local_conn.close()
                except Exception:
                    pass
                try:
                    remote_conn.close()
                except Exception:
                    pass
        except Exception as e:
            try:
                logging.warning(f"Reconciliación local→remoto falló: {e}")
            except Exception:
                pass
        # Remoto→Local para tablas puntuales excluidas de publicación
        try:
            from pathlib import Path
            import json
            from scripts import reconcile_remote_to_local_once as R2L
            cfg = R2L.load_config()
            rep_cfg = (cfg.get('replication') or {})
            subname = rep_cfg.get('subscription_name') or 'gym_sub'
            local_params = R2L.build_conn_params('local', cfg)
            remote_params = R2L.build_conn_params('remote', cfg)
            local_conn = R2L.connect(local_params)
            remote_conn = R2L.connect(remote_params)
            try:
                R2L.disable_subscription(local_conn, subname)
            except Exception:
                pass
            try:
                tables_to_process = list(getattr(R2L, 'DEFAULT_TABLES', []))
                try:
                    sync_path = Path(__file__).resolve().parent / 'config' / 'sync_tables.json'
                    if sync_path.exists():
                        with open(sync_path, 'r', encoding='utf-8') as f:
                            sync_cfg = json.load(f) or {}
                        publishes = sync_cfg.get('publishes_remote_to_local') or []
                        if publishes:
                            tables_to_process = publishes
                except Exception:
                    pass
                total_inserted = 0
                total_updated = 0
                for table in tables_to_process:
                    try:
                        pk_cols = R2L.get_pk_columns(local_conn, 'public', table)
                        # Inserciones: claves presentes en remoto y faltantes en local
                        missing = R2L.fetch_missing_pks(remote_conn, local_conn, 'public', table, pk_cols)
                        if missing:
                            rows = R2L.fetch_rows_by_pk(remote_conn, 'public', table, pk_cols, missing)
                            inserted = R2L.insert_rows_local(local_conn, 'public', table, rows, pk_cols, dry_run=False)
                            total_inserted += int(inserted or 0)
                        # Actualizaciones: filas existentes con updated_at más reciente en remoto
                        updated = R2L.reconcile_updates_local(remote_conn, local_conn, 'public', table, pk_cols, dry_run=False)
                        total_updated += int(updated or 0)
                    except Exception:
                        pass
                try:
                    QTimer.singleShot(0, lambda: self.status_bar.showMessage(f"Remoto→Local: insertadas {total_inserted}, actualizadas {total_updated}", 5000))
                except Exception:
                    pass
            finally:
                try:
                    R2L.enable_subscription(local_conn, subname)
                except Exception:
                    pass
                try:
                    local_conn.close()
                except Exception:
                    pass
                try:
                    remote_conn.close()
                except Exception:
                    pass
        except Exception as e:
            try:
                logging.warning(f"Reconciliación remoto→local falló: {e}")
            except Exception:
                pass
        # Actualizar UI y lanzar subida inmediata de outbox
        try:
            QTimer.singleShot(0, lambda: self.status_bar.showMessage("Reconciliación inicial completada", 7000))
        except Exception:
            pass
        try:
            svc = getattr(self, 'sync_service', None)
            if svc is not None and hasattr(svc, 'flush_outbox_once_bg'):
                svc.flush_outbox_once_bg(delay_ms=0)
        except Exception:
            pass
        try:
            QTimer.singleShot(0, self.hide_sync_overlay)
        except Exception:
            pass

    def setup_alert_system(self):
        """Configura el sistema de alertas y sus conexiones"""
        try:
            # Conectar señales del gestor de alertas
            alert_manager.alert_generated.connect(self.on_alert_generated)
            alert_manager.alert_acknowledged.connect(self.update_alert_indicators)
            alert_manager.alert_resolved.connect(self.update_alert_indicators)
            
            # Timer para verificar alertas periódicamente
            self.alert_check_timer = QTimer()
            self.alert_check_timer.timeout.connect(self.check_system_alerts)
            self.alert_check_timer.start(60000)  # Verificar cada minuto
            
            # Verificación inicial
            QTimer.singleShot(5000, self.check_system_alerts)  # Verificar después de 5 segundos
            
            # Actualizar indicadores iniciales
            self.update_alert_indicators()
            
            logging.info("Sistema de alertas configurado correctamente")
            
        except Exception as e:
            logging.error(f"Error configurando sistema de alertas: {e}")
    
    def on_alert_generated(self, alert):
        """Maneja la generación de nuevas alertas"""
        try:
            # Actualizar indicadores
            self.update_alert_indicators()
            
            # Mostrar notificación en barra de estado para alertas críticas
            if alert.level == AlertLevel.CRITICAL:
                self.status_bar.showMessage(f"🚨 ALERTA CRÍTICA: {alert.title}", 10000)
                
                # Opcional: mostrar mensaje emergente para alertas críticas
                if hasattr(self, 'show_critical_alert_popup'):
                    QTimer.singleShot(100, lambda: self.show_critical_alert_popup(alert))
            
            elif alert.level == AlertLevel.WARNING:
                self.status_bar.showMessage(f"⚠️ Advertencia: {alert.title}", 5000)
                
        except Exception as e:
            logging.error(f"Error manejando alerta generada: {e}")
    
    def update_alert_indicators(self):
        """Actualiza los indicadores de alertas en la barra de estado"""
        try:
            counts = alert_manager.get_alert_counts()
            
            # Actualizar contador de alertas críticas
            critical_count = counts.get('critical', 0)
            self.critical_alerts_label.setText(f"🚨 {critical_count}")
            
            # Actualizar contador de advertencias
            warning_count = counts.get('warning', 0)
            self.warning_alerts_label.setText(f"⚠️ {warning_count}")
            
            # Actualizar estado del sistema
            unresolved_count = counts.get('unresolved', 0)
            if critical_count > 0:
                self.system_status_label.setText("🔴 Crítico")
                self.system_status_label.setStyleSheet("QLabel { color: #e74c3c; font-weight: bold; }")
            elif warning_count > 0:
                self.system_status_label.setText("🟡 Advertencias")
                self.system_status_label.setStyleSheet("QLabel { color: #f39c12; font-weight: bold; }")
            elif unresolved_count > 0:
                self.system_status_label.setText("🟠 Pendientes")
                self.system_status_label.setStyleSheet("QLabel { color: #e67e22; font-weight: bold; }")
            else:
                self.system_status_label.setText("✅ Sistema OK")
                self.system_status_label.setStyleSheet("QLabel { color: #27ae60; font-weight: bold; }")
                
        except Exception as e:
            logging.error(f"Error actualizando indicadores de alertas: {e}")
    
    def check_system_alerts(self):
        """Verifica el estado del sistema y genera alertas si es necesario"""
        try:
            # Verificar alertas de mantenimiento
            self.check_maintenance_alerts()
            
            # Verificar alertas de base de datos
            self.check_database_alerts()
            
            # Verificar alertas de sistema
            self.check_system_performance_alerts()
            
        except Exception as e:
            logging.error(f"Error verificando alertas del sistema: {e}")
    
    def check_maintenance_alerts(self):
        """Verifica alertas relacionadas con mantenimiento"""
        try:
            from datetime import datetime, timedelta
            
            # Verificar si hay muchos pagos vencidos
            overdue_count = self.count_overdue_payments()
            if overdue_count > 10:
                alert_manager.generate_alert(
                    AlertLevel.WARNING,
                    AlertCategory.MAINTENANCE,
                    "Muchos pagos vencidos",
                    f"Hay {overdue_count} usuarios con pagos vencidos que requieren atención.",
                    "MainWindow"
                )
            
            # Verificar usuarios inactivos
            inactive_count = self.count_inactive_users()
            if inactive_count > 20:
                alert_manager.generate_alert(
                    AlertLevel.WARNING,
                    AlertCategory.MAINTENANCE,
                    "Muchos usuarios inactivos",
                    f"Hay {inactive_count} usuarios sin asistencia reciente.",
                    "MainWindow"
                )
                
        except Exception as e:
            logging.error(f"Error verificando alertas de mantenimiento: {e}")
    
    def check_database_alerts(self):
        """Verifica alertas relacionadas con la base de datos"""
        try:
            import os
            
            # Verificar tamaño de la base de datos
            db_path = self.db_manager.db_path
            if os.path.exists(db_path):
                db_size_mb = os.path.getsize(db_path) / (1024 * 1024)
                
                if db_size_mb > 500:  # Mayor a 500 MB
                    alert_manager.generate_alert(
                        AlertLevel.WARNING,
                        AlertCategory.DATABASE,
                        "Base de datos grande",
                        f"La base de datos tiene {db_size_mb:.1f} MB. Considera optimizar.",
                        "DatabaseManager"
                    )
                elif db_size_mb > 1000:  # Mayor a 1 GB
                    alert_manager.generate_alert(
                        AlertLevel.CRITICAL,
                        AlertCategory.DATABASE,
                        "Base de datos muy grande",
                        f"La base de datos tiene {db_size_mb:.1f} MB. Optimización urgente requerida.",
                        "DatabaseManager"
                    )
                    
        except Exception as e:
            logging.error(f"Error verificando alertas de base de datos: {e}")
    
    def check_system_performance_alerts(self):
        """Verifica alertas relacionadas con el rendimiento del sistema"""
        try:
            import psutil
            
            # Verificar uso de CPU
            cpu_percent = psutil.cpu_percent(interval=1)
            if cpu_percent > 80:
                alert_manager.generate_alert(
                    AlertLevel.WARNING,
                    AlertCategory.PERFORMANCE,
                    "Alto uso de CPU",
                    f"El uso de CPU está al {cpu_percent:.1f}%",
                    "SystemMonitor"
                )
            
            # Verificar uso de memoria
            memory = psutil.virtual_memory()
            if memory.percent > 85:
                alert_manager.generate_alert(
                    AlertLevel.WARNING,
                    AlertCategory.PERFORMANCE,
                    "Alto uso de memoria",
                    f"El uso de memoria está al {memory.percent:.1f}%",
                    "SystemMonitor"
                )
            
            # Verificar espacio en disco
            disk = psutil.disk_usage('/')
            if disk.percent > 90:
                alert_manager.generate_alert(
                    AlertLevel.CRITICAL,
                    AlertCategory.SYSTEM,
                    "Poco espacio en disco",
                    f"El disco está al {disk.percent:.1f}% de capacidad",
                    "SystemMonitor"
                )
                
        except ImportError:
            # psutil no está disponible, omitir verificaciones de rendimiento
            pass
        except Exception as e:
            logging.error(f"Error verificando alertas de rendimiento: {e}")
    
    def show_alerts_panel(self):
        """Muestra el panel de alertas en una ventana separada"""
        try:
            if not hasattr(self, 'alerts_window') or not self.alerts_window.isVisible():
                self.alerts_window = QWidget()
                self.alerts_window.setWindowTitle("🚨 Panel de Alertas del Sistema")
                self.alerts_window.setWindowIcon(self.windowIcon())
                self.alerts_window.resize(1000, 700)
                
                layout = QVBoxLayout(self.alerts_window)
                self.alerts_widget = AlertsWidget()
                layout.addWidget(self.alerts_widget)
                
                # Centrar la ventana
                self.alerts_window.move(
                    self.x() + (self.width() - self.alerts_window.width()) // 2,
                    self.y() + (self.height() - self.alerts_window.height()) // 2
                )
            
            self.alerts_window.show()
            self.alerts_window.raise_()
            self.alerts_window.activateWindow()
            
        except Exception as e:
            logging.error(f"Error mostrando panel de alertas: {e}")
            QMessageBox.critical(self, "Error", f"No se pudo abrir el panel de alertas: {e}")
    
    def show_critical_alert_popup(self, alert):
        """Muestra un popup para alertas críticas"""
        try:
            msg = QMessageBox(self)
            msg.setIcon(QMessageBox.Icon.Critical)
            msg.setWindowTitle("🚨 Alerta Crítica")
            msg.setText(alert.title)
            msg.setDetailedText(alert.message)
            msg.setStandardButtons(QMessageBox.StandardButton.Ok)
            msg.exec()
            
        except Exception as e:
            logging.error(f"Error mostrando popup de alerta crítica: {e}")
    
    def setup_notification_timer(self):
        """Configura un timer para actualizar las notificaciones periódicamente"""
        self.notification_timer = QTimer()
        self.notification_timer.timeout.connect(self.update_tab_notifications)
        self.notification_timer.start(30000)  # Actualizar cada 30 segundos
        
        # Timer para procesamiento automático de WhatsApp (recordatorios y cuotas vencidas)
        self.whatsapp_timer = QTimer()
        self.whatsapp_timer.timeout.connect(self.process_whatsapp_automation)
        self.whatsapp_timer.start(3600000)  # Ejecutar cada hora (3600000 ms)
        
        # Ejecutar procesamiento inicial con diferimiento y opción de deshabilitar por entorno
        try:
            disable_initial = os.getenv("DISABLE_AUTOPROCESS_ON_START") == "1"
            if disable_initial:
                logging.info("Automatización WhatsApp inicial deshabilitada por configuración (DISABLE_AUTOPROCESS_ON_START=1)")
            else:
                # Diferir más el arranque para evitar competir con el login/carga inicial
                QTimer.singleShot(60000, self.process_whatsapp_automation)  # 60s
        except Exception:
            # Fallback seguro
            QTimer.singleShot(60000, self.process_whatsapp_automation)
    
    def process_whatsapp_automation(self):
        """Lanza el procesamiento automático en segundo plano para no bloquear la UI."""
        import threading
        if getattr(self, '_wa_auto_running', False):
            logging.info("Automatización WhatsApp ya en ejecución; se omite disparo simultáneo")
            return
        self._wa_auto_running = True

        def _worker():
            try:
                if not self.payment_manager:
                    logging.info("Payment Manager no disponible para procesamiento automático")
                    return

                logging.info("Iniciando procesamiento automático de pagos y estados (background)")

                # 1. Procesar recordatorios de próximos vencimientos (3 días antes)
                try:
                    recordatorios_enviados = self.payment_manager.procesar_recordatorios_proximos_vencimientos()
                except Exception as e:
                    logging.error(f"Error en recordatorios de vencimientos: {e}")
                    recordatorios_enviados = 0
                logging.info(f"Recordatorios de próximos vencimientos enviados: {recordatorios_enviados}")

                # 2. Procesar usuarios morosos y cuotas vencidas
                try:
                    morosos_procesados = self.payment_manager.procesar_usuarios_morosos()
                except Exception as e:
                    logging.error(f"Error procesando usuarios morosos: {e}")
                    morosos_procesados = 0
                logging.info(f"Usuarios morosos procesados: {morosos_procesados}")

                # 3. Procesar vencimientos automáticos (incrementar contadores y desactivar usuarios)
                try:
                    resultados_vencimientos = self.db_manager.procesar_vencimientos_automaticos()
                except Exception as e:
                    logging.error(f"Error en vencimientos automáticos: {e}")
                    resultados_vencimientos = {'usuarios_desactivados': 0, 'errores': 1, 'detalles_errores': [str(e)]}
                logging.info(f"Vencimientos automáticos procesados: {resultados_vencimientos}")

                # Generar alerta si hay actividad significativa
                total_actividad = (
                    (recordatorios_enviados or 0) +
                    (morosos_procesados or 0) +
                    int(resultados_vencimientos.get('usuarios_desactivados', 0) or 0)
                )
                if total_actividad > 0:
                    try:
                        alert_manager.generate_alert(
                            AlertLevel.INFO,
                            AlertCategory.SYSTEM,
                            "Procesamiento automático WhatsApp completado",
                            f"Recordatorios: {recordatorios_enviados}, Morosos: {morosos_procesados}, "
                            f"Usuarios desactivados: {resultados_vencimientos.get('usuarios_desactivados', 0)}",
                            "MainWindow"
                        )
                    except Exception:
                        pass
            except Exception as e:
                logging.error(f"Error en procesamiento automático de WhatsApp (background): {e}")
                try:
                    alert_manager.generate_alert(
                        AlertLevel.ERROR,
                        AlertCategory.SYSTEM,
                        "Error en procesamiento automático WhatsApp",
                        f"Error: {str(e)}",
                        "MainWindow"
                    )
                except Exception:
                    pass
            finally:
                self._wa_auto_running = False

        try:
            threading.Thread(target=_worker, name="WA-AutoWorker", daemon=True).start()
        except Exception as e:
            # Fallback: ejecutar inline si no se pudo crear el hilo (no recomendado)
            logging.warning(f"No se pudo iniciar hilo de automatización WhatsApp: {e}. Ejecutando inline.")
            _worker()
    
    def update_tab_notifications(self):
        """Actualiza los indicadores de notificación en las pestañas"""
        try:
            # Contar pagos vencidos
            overdue_payments = self.count_overdue_payments()
            self.notification_counts['pagos'] = overdue_payments
            
            # Contar usuarios inactivos (sin asistencia en 30 días)
            inactive_users = self.count_inactive_users()
            self.notification_counts['usuarios'] = inactive_users
            
            # Contar clases con pocos inscritos
            low_enrollment_classes = self.count_low_enrollment_classes()
            self.notification_counts['clases'] = low_enrollment_classes
            
            # Actualizar texto de las pestañas
            self.update_tab_text_with_notifications()
            
        except Exception as e:
            logging.error(f"Error actualizando notificaciones: {e}")

    def _on_replication_inbound_change(self):
        """Callback de observador de replicación: refresca UI ante cambios entrantes."""
        # Debounce para evitar ráfagas de refresco si hay avances rápidos
        try:
            import time
            now = time.time()
            last = getattr(self, '_last_inbound_refresh_ts', 0)
            debounce_ms = int(getattr(self, '_inbound_debounce_ms', 0))
            if debounce_ms > 0 and (now - float(last)) * 1000 < debounce_ms:
                return
            self._last_inbound_refresh_ts = now
        except Exception:
            pass
        try:
            self.update_tab_notifications()
        except Exception:
            pass
        try:
            # Emitir señal para forzar re-render en pestañas dependientes
            usuarios_tab = self.tabs.get('usuarios')
            if usuarios_tab and hasattr(usuarios_tab, 'usuarios_modificados'):
                usuarios_tab.usuarios_modificados.emit()
        except Exception:
            pass
        try:
            self.update_monthly_hours()
        except Exception:
            pass
        # Intentar vaciar outbox local tras cambios entrantes (no bloquea UI)
        try:
            from PyQt5.QtCore import QTimer
            QTimer.singleShot(500, self._flush_sync_outbox_once_bg)
        except Exception:
            try:
                self._flush_sync_outbox_once_bg()
            except Exception:
                pass
    
    def _flush_sync_outbox_once_bg(self):
        """Vacía la outbox de sincronización en background sin bloquear la UI."""
        try:
            if getattr(self, '_sync_uploader_running', False):
                return
            self._sync_uploader_running = True
            def _worker():
                try:
                    from sync_uploader import SyncUploader
                    uploader = SyncUploader()
                    sent, deleted = uploader.flush_once()
                    try:
                        logging.info(f"SyncUploader flush: enviadas={sent} borradas={deleted}")
                    except Exception:
                        pass
                    try:
                        from PyQt5.QtCore import QTimer
                        # Refrescar indicador de conectividad en el hilo principal de Qt
                        QTimer.singleShot(0, self.update_connectivity_indicator)
                    except Exception:
                        try:
                            # Fallback si Qt no está disponible
                            self.update_connectivity_indicator()
                        except Exception:
                            pass
                    try:
                        # Feedback opcional en barra de estado desde el hilo principal
                        if hasattr(self, 'status_bar'):
                            from PyQt5.QtCore import QTimer
                            QTimer.singleShot(0, lambda: self.status_bar.showMessage(
                                f"Outbox sincronizada: {sent} env., {deleted} elim.", 4000
                            ))
                    except Exception:
                        pass
                except Exception as e:
                    try:
                        logging.warning(f"Falló flush SyncUploader: {e}")
                    except Exception:
                        pass
                finally:
                    try:
                        self._sync_uploader_running = False
                    except Exception:
                        pass
            try:
                import threading
                threading.Thread(target=_worker, name="SyncUploaderFlush", daemon=True).start()
            except Exception:
                _worker()
        except Exception:
            pass

    def count_overdue_payments(self) -> int:
        """Cuenta los pagos vencidos"""
        try:
            from datetime import datetime, timedelta
            cutoff_date = datetime.now() - timedelta(days=30)
            
            # Obtener usuarios activos (socios y profesores)
            usuarios_activos = (
                self.db_manager.obtener_usuarios_por_rol('socio') +
                self.db_manager.obtener_usuarios_por_rol('profesor')
            )
            usuarios_activos = [u for u in usuarios_activos if u.activo]
            overdue_count = 0
            
            for usuario in usuarios_activos:
                # Considerar usuarios SIN pagos: su cuota vence igual
                ultimo_pago = self.payment_manager.obtener_ultimo_pago_usuario(usuario.id)

                # Usar fecha de próximo vencimiento si está disponible en el usuario
                fpv = getattr(usuario, 'fecha_proximo_vencimiento', None)
                fecha_ref = None

                if fpv:
                    # Normalizar fecha_proximo_vencimiento a objeto date/datetime
                    try:
                        if isinstance(fpv, str):
                            from datetime import date
                            try:
                                # Intentar ISO
                                fecha_ref = datetime.fromisoformat(fpv)
                            except Exception:
                                # Intentar formato dd/mm/YYYY
                                fecha_ref = datetime.strptime(fpv, '%d/%m/%Y')
                        else:
                            fecha_ref = fpv
                    except Exception:
                        fecha_ref = None

                if not fecha_ref:
                    # Si no hay fpv, caer al último pago (si existe)
                    if ultimo_pago:
                        fecha_ref = ultimo_pago.fecha_pago
                        try:
                            if isinstance(fecha_ref, str):
                                fecha_ref = datetime.fromisoformat(fecha_ref)
                        except Exception:
                            pass
                    else:
                        # Sin pagos y sin fpv: considerar como vencido por defecto
                        # para no excluirlos de los contadores
                        fecha_ref = datetime.now() - timedelta(days=31)

                # Si la fecha de referencia (vencimiento o último pago) es anterior al corte, contar como vencido
                try:
                    # Convertir a datetime si es date
                    from datetime import date
                    if isinstance(fecha_ref, date) and not isinstance(fecha_ref, datetime):
                        fecha_ref = datetime.combine(fecha_ref, datetime.min.time())
                except Exception:
                    pass

                if fecha_ref < cutoff_date:
                    overdue_count += 1
                
            return overdue_count
        except Exception as e:
            logging.error(f"Error contando pagos vencidos: {e}")
            return 0
    
    def count_inactive_users(self) -> int:
        """Cuenta usuarios sin asistencia reciente"""
        try:
            from datetime import datetime, timedelta, date
            cutoff_date = datetime.now() - timedelta(days=30)
            
            # Obtener usuarios activos (rol socio)
            usuarios_activos = self.db_manager.obtener_usuarios_por_rol('socio')
            usuarios_activos = [u for u in usuarios_activos if u.activo]
            inactive_count = 0
            
            # Verificar asistencias recientes usando método existente
            with self.db_manager.get_connection_context() as conn:
                cursor = conn.cursor()
                for usuario in usuarios_activos:
                    cursor.execute(
                        "SELECT COUNT(*) FROM asistencias WHERE usuario_id = %s AND fecha >= %s",
                        (usuario.id, cutoff_date.date().isoformat())
                    )
                    result = cursor.fetchone()
                    asistencias_count = result[0] if result and len(result) > 0 else 0
                    if asistencias_count == 0:
                        inactive_count += 1
                    
            return inactive_count
        except Exception as e:
            logging.error(f"Error contando usuarios inactivos: {e}")
            return 0
    
    def count_low_enrollment_classes(self) -> int:
        """Cuenta clases con baja inscripción"""
        try:
            # Obtener todas las clases
            clases = self.db_manager.obtener_clases()
            low_enrollment_count = 0
            
            with self.db_manager.get_connection_context() as conn:
                cursor = conn.cursor()
                for clase in clases:
                    # Obtener horarios de la clase y contar inscritos
                    horarios = self.db_manager.obtener_horarios_de_clase(clase.id)
                    for horario in horarios:
                        cursor.execute(
                            "SELECT COUNT(*) FROM clase_usuarios WHERE clase_horario_id = %s",
                            (horario.id,)
                        )
                        result = cursor.fetchone()
                        inscritos = result[0] if result and len(result) > 0 else 0
                        if inscritos < 3:  # Menos de 3 inscritos se considera bajo
                            low_enrollment_count += 1
                            break  # Solo contar la clase una vez
                    
            return low_enrollment_count
        except Exception as e:
            logging.error(f"Error contando clases con baja inscripción: {e}")
            return 0
    
    def update_tab_text_with_notifications(self):
        """Actualiza el texto de las pestañas con indicadores de notificación"""
        base_texts = {
            'usuarios': "Usuarios y Asistencias",
            'pagos': "Pagos",
            'reportes': "Dashboard de Reportes",
            'rutinas': "Rutinas",
            'clases': "Clases",
            'profesores': "Profesores",
            'configuracion': "Configuración"
        }
        
        for tab_key, base_text in base_texts.items():
            if tab_key in self.tab_indices:
                count = self.notification_counts.get(tab_key, 0)
                if count > 0:
                    new_text = f"{base_text} ({count})"
                    # Cambiar color del texto para indicar notificación
                    self.tabWidget.setTabText(self.tab_indices[tab_key], new_text)
                    # Aplicar estilo de notificación
                    self.apply_notification_style(self.tab_indices[tab_key], True)
                else:
                    self.tabWidget.setTabText(self.tab_indices[tab_key], base_text)
                    self.apply_notification_style(self.tab_indices[tab_key], False)
    
    def apply_notification_style(self, tab_index: int, has_notification: bool):
        """Aplica estilo visual para indicar notificaciones"""
        try:
            if has_notification:
                # Aplicar estilo de notificación (color rojo/naranja)
                self.tabWidget.tabBar().setTabTextColor(tab_index, self.tabWidget.palette().color(self.tabWidget.palette().ColorRole.Highlight))
            else:
                # Restaurar color normal
                self.tabWidget.tabBar().setTabTextColor(tab_index, self.tabWidget.palette().color(self.tabWidget.palette().ColorRole.WindowText))
        except Exception as e:
            logging.error(f"Error aplicando estilo de notificación: {e}")
    
    # Métodos responsivos eliminados - usando tamaños fijos para ventana maximizada

    def connect_signals(self):
        self.tabWidget.currentChanged.connect(self.tab_changed)
        
        # Conectar señales solo si las pestañas existen
        if 'usuarios' in self.tabs and 'pagos' in self.tabs:
            self.tabs['usuarios'].request_payment_for_user.connect(self.switch_to_payments_tab)
            self.tabs['usuarios'].request_routine_for_user.connect(self.switch_to_routines_tab)
            self.tabs['usuarios'].usuarios_modificados.connect(self.tabs['pagos'].load_initial_data)
            self.tabs['pagos'].pagos_modificados.connect(self.tabs['usuarios'].load_users)
        
        if 'usuarios' in self.tabs and 'reportes' in self.tabs:
            self.tabs['usuarios'].usuarios_modificados.connect(self.tabs['reportes'].actualizar_reportes)
            
        if 'usuarios' in self.tabs and 'profesores' in self.tabs:
            self.tabs['usuarios'].usuarios_modificados.connect(self.tabs['profesores'].cargar_profesores)
            
        if 'pagos' in self.tabs and 'reportes' in self.tabs:
            self.tabs['pagos'].pagos_modificados.connect(self.tabs['reportes'].actualizar_reportes)
            
        if 'profesores' in self.tabs and 'usuarios' in self.tabs:
            self.tabs['profesores'].profesor_guardado.connect(self.tabs['usuarios'].load_users)
            
        if 'configuracion' in self.tabs and 'pagos' in self.tabs:
            self.tabs['configuracion'].precio_actualizado.connect(self.tabs['pagos'].load_defaults)
            
        if 'configuracion' in self.tabs:
            self.tabs['configuracion'].feature_toggled.connect(self.update_tab_visibility)
            # Configuración de fuente eliminada
            if 'usuarios' in self.tabs:
                self.tabs['configuracion'].usuarios_modificados.connect(self.tabs['usuarios'].load_users)
            
            # Conectar señal de branding si existe el widget de branding
            if hasattr(self.tabs['configuracion'], 'branding_widget'):
                self.tabs['configuracion'].branding_widget.branding_changed.connect(self.apply_branding_changes)

    def tab_changed(self, index):
        widget = self.tabWidget.widget(index)
        # Inicializar perezosamente si la pestaña seleccionada es un placeholder diferido
        try:
            for key, tab_index in self.tab_indices.items():
                if tab_index == index and key in getattr(self, 'deferred_tab_constructors', {}) and key not in self.tabs:
                    constructor = self.deferred_tab_constructors.get(key)
                    if constructor:
                        real_widget = constructor()
                        self.tabs[key] = real_widget
                        # Reemplazar el placeholder con el widget real
                        self.tabWidget.removeTab(index)
                        # Volver a insertar en la misma posición con el mismo icono/título
                        title_map = {
                            'rutinas': "🏋️ Rutinas",
                            'clases': "📅 Clases",
                            'profesores': "👨‍🏫 Profesores",
                            'configuracion': "⚙️ Configuración",
                        }
                        icon_map = {
                            'rutinas': QIcon(resource_path("assets/routines.png")),
                            'clases': QIcon(resource_path("assets/classes.png")),
                            'profesores': QIcon(resource_path("assets/student_icon.png")),
                            'configuracion': QIcon(resource_path("assets/gear.png")),
                        }
                        new_index = self.tabWidget.insertTab(index, real_widget, icon_map.get(key), title_map.get(key, key))
                        self.tab_indices[key] = new_index
                        self.tabWidget.setCurrentIndex(new_index)
                        # Conectar señales específicas para la pestaña creada
                        try:
                            self._connect_new_tab_signals(key)
                        except Exception:
                            pass
                        widget = real_widget
                        break
        except Exception as e:
            logging.warning(f"No se pudo inicializar pestaña diferida: {e}")
        
        # Controlar visibilidad del footer de alertas
        # Solo mostrar en la pestaña de Configuración
        if widget == self.tabs.get('configuracion'):
            self.status_bar.show()
        else:
            self.status_bar.hide()
        
        # Lógica existente para cada pestaña
        if widget == self.tabs.get('rutinas'):
            self.tabs['rutinas'].load_preset_routines()
        elif widget == self.tabs.get('clases'):
            self.tabs['clases'].load_initial_data()
        elif widget == self.tabs.get('profesores'):
            self.tabs['profesores'].cargar_profesores()
        elif widget == self.tabs.get('reportes'):
            # Inicializar carga pesada al abrir la pestaña de reportes
            try:
                reports_tab = self.tabs['reportes']
                if hasattr(reports_tab, 'initialize_dashboard_data'):
                    reports_tab.initialize_dashboard_data()
                else:
                    reports_tab.actualizar_reportes()
            except Exception as e:
                logging.warning(f"Error inicializando reportes al abrir pestaña: {e}")
        elif widget == self.tabs.get('configuracion'):
             if self.tabs['configuracion'].dev_manager.is_dev_mode_active:
                 self.tabs['configuracion'].on_dev_tab_changed(self.tabs['configuracion'].dev_tabs.currentIndex())

    def _connect_new_tab_signals(self, tab_key: str):
        """Conecta señales específicas cuando se instancian pestañas diferidas."""
        try:
            if tab_key == 'configuracion':
                if 'configuracion' in self.tabs:
                    self.tabs['configuracion'].feature_toggled.connect(self.update_tab_visibility)
                    if 'usuarios' in self.tabs:
                        self.tabs['configuracion'].usuarios_modificados.connect(self.tabs['usuarios'].load_users)
                    if hasattr(self.tabs['configuracion'], 'branding_widget'):
                        self.tabs['configuracion'].branding_widget.branding_changed.connect(self.apply_branding_changes)
            elif tab_key == 'profesores':
                if 'profesores' in self.tabs and 'usuarios' in self.tabs:
                    self.tabs['profesores'].profesor_guardado.connect(self.tabs['usuarios'].load_users)
            elif tab_key == 'clases':
                # Nada adicional por ahora, carga inicial al cambiar a la pestaña
                pass
            elif tab_key == 'rutinas':
                # Nada adicional por ahora
                pass
        except Exception as e:
            logging.warning(f"Error conectando señales de pestaña '{tab_key}': {e}")

    def show_deactivation_message(self, deactivated_users):
        user_names = ", ".join([user['nombre'] for user in deactivated_users])
        logging.info(f"Se desactivaron {len(deactivated_users)} usuarios: {user_names}")
        QMessageBox.information(self, "Revisión Automática", f"Se han desactivado {len(deactivated_users)} socio(s) automáticamente por no registrar pagos en los últimos 3 meses.")
        # Verificar que las pestañas estén inicializadas antes de cargar usuarios
        if hasattr(self, 'tabs') and 'usuarios' in self.tabs:
            self.tabs['usuarios'].load_users()

    def switch_to_payments_tab(self, user_id: int):
        user = self.db_manager.obtener_usuario(user_id)
        if user:
            self.tabWidget.setCurrentWidget(self.tabs['pagos'])
            self.tabs['pagos'].set_user_for_payment(user.id)

    def switch_to_routines_tab(self, user_id: int):
        user = self.db_manager.obtener_usuario(user_id)
        if user:
            self.tabWidget.setCurrentWidget(self.tabs['rutinas'])
            self.tabs['rutinas'].set_user_for_routine(user)

    def apply_role_permissions(self):
        if self.user_role == "profesor":
            # Los profesores no pueden ver reportes, auditoría ni gestionar otros profesores
            # PERO SÍ pueden ver configuración con funcionalidades limitadas
            if 'reportes' in self.tab_indices: self.tabWidget.setTabVisible(self.tab_indices['reportes'], False)
            if 'profesores' in self.tab_indices: self.tabWidget.setTabVisible(self.tab_indices['profesores'], False)
            if 'auditoria' in self.tab_indices: self.tabWidget.setTabVisible(self.tab_indices['auditoria'], False)
            
            # Configurar la pestaña de configuración para profesores (con funcionalidades limitadas)
            if 'configuracion' in self.tab_indices and 'configuracion' in self.tabs:
                # La pestaña de configuración permanece visible para profesores
                # pero con funcionalidades limitadas (esto se maneja dentro del ConfigTabWidget)
                self.tabs['configuracion'].set_user_role(self.user_role)

        elif self.user_role == "empleado":
            # Los empleados pueden ver profesores pero con funcionalidad limitada, no pueden ver auditoría
            if 'profesores' in self.tab_indices:
                # Aquí podrías agregar lógica para limitar funcionalidades específicas
                pass
            if 'auditoria' in self.tab_indices: self.tabWidget.setTabVisible(self.tab_indices['auditoria'], False)
            
            # Configurar la pestaña de configuración para empleados
            if 'configuracion' in self.tab_indices and 'configuracion' in self.tabs:
                self.tabs['configuracion'].set_user_role(self.user_role)

    def update_tab_visibility(self, feature_states: dict):
        if self.user_role != "dueño": return
        for key, is_visible in feature_states.items():
            if key in self.tab_indices:
                self.tabWidget.setTabVisible(self.tab_indices[key], is_visible)

    # Método set_global_font_size eliminado - configuración de fuente removida

    # Método set_initial_font_size eliminado - configuración de fuente removida

    def apply_main_tabs_visibility_by_role(self):
        """Aplica la visibilidad de las pestañas principales según la configuración persistida por rol.
        Compatible con configuraciones anteriores (sin 'main_tabs').
        """
        try:
            # Regla profesional: el Dueño siempre ve TODO, ignorar cualquier configuración persistida
            if getattr(self, 'user_role', None) == 'dueño':
                try:
                    for idx in self.tab_indices.values():
                        self.tabWidget.setTabVisible(idx, True)
                except Exception:
                    pass
                return
            raw = None
            try:
                raw = self.db_manager.obtener_configuracion('developer_tab_visibility_by_role')
            except Exception:
                raw = None
            cfg = {}
            if raw:
                try:
                    cfg = json.loads(raw)
                except Exception:
                    cfg = {}
            role_cfg = cfg.get(self.user_role, {})
            # Compatibilidad hacia atrás: si no existe 'main_tabs', dejar todas visibles por defecto
            main_cfg = role_cfg.get('main_tabs', {})
            for key, idx in self.tab_indices.items():
                visible = bool(main_cfg.get(key, True))
                try:
                    self.tabWidget.setTabVisible(idx, visible)
                except Exception:
                    pass
        except Exception:
            pass
    
    def load_branding_configuration(self):
        """Carga la configuración de branding desde la base de datos"""
        try:
            branding_json = self.db_manager.obtener_configuracion('branding_config')
            if branding_json:
                self.branding_config = json.loads(branding_json)
                logging.info("Configuración de branding cargada exitosamente")
            else:
                # Configuración por defecto
                self.branding_config = {
                    'primary_color': '#3498db',
                    'secondary_color': '#2ecc71',
                    'accent_color': '#e74c3c',
                    'background_color': '#ffffff',
                    'alt_background_color': '#f8f9fa',
                    'text_color': '#2c3e50',
                    'main_font': 'Arial',
                    'heading_font': 'Arial'
                }
                logging.info("Usando configuración de branding por defecto")
        except Exception as e:
            logging.error(f"Error cargando configuración de branding: {e}")
            self.branding_config = {}
    
    def generate_dynamic_css(self):
        """Genera CSS dinámico exhaustivo con detección automática de temas y contraste mejorado"""
        if not self.branding_config:
            return ""
        
        try:
            primary_color = self.branding_config.get('primary_color', '#3498db')
            secondary_color = self.branding_config.get('secondary_color', '#2ecc71')
            accent_color = self.branding_config.get('accent_color', '#e74c3c')
            background_color = self.branding_config.get('background_color', '#ffffff')
            alt_background_color = self.branding_config.get('alt_background_color', '#f8f9fa')
            text_color = self.branding_config.get('text_color', '#2c3e50')
            ui_text_color = self.branding_config.get('ui_text_color')
            main_font = self.branding_config.get('main_font', 'Arial')
            
            # Detección automática de tema claro/oscuro y ajuste de contraste
            is_dark_theme = self._is_dark_color(background_color)
            auto_text_color = self._get_contrasting_text_color(background_color)
            auto_alt_text_color = self._get_contrasting_text_color(alt_background_color)
            
            # MEJORA: Usar siempre color de texto automático para máximo contraste
            final_text_color = ui_text_color or auto_text_color
            final_alt_text_color = auto_alt_text_color
            
            # MEJORA: Usar colores hover personalizados del branding si están disponibles
            hover_color = self.branding_config.get('primary_hover_color', self._calculate_smart_hover_color(primary_color, is_dark_theme))
            pressed_color = self._calculate_smart_pressed_color(primary_color, is_dark_theme)
            secondary_hover_color = self.branding_config.get('secondary_hover_color', self._calculate_smart_hover_color(secondary_color, is_dark_theme))
            accent_hover_color = self.branding_config.get('accent_hover_color', self._calculate_smart_hover_color(accent_color, is_dark_theme))
            
            # Debug: Verificar colores en generate_dynamic_css
            print(f"DEBUG generate_dynamic_css - pressed_color: {pressed_color}")
            print(f"DEBUG generate_dynamic_css - primary_color: {primary_color}")
            print(f"DEBUG generate_dynamic_css - is_dark_theme: {is_dark_theme}")
            
            border_color = self._adjust_color_brightness(primary_color, 0.9)
            disabled_color = self._adjust_color_brightness(background_color, 0.7 if is_dark_theme else 1.3)
            selection_color = self._adjust_color_brightness(primary_color, 0.3)
            
            # Colores para estados específicos del sistema de branding
            success_color = self.branding_config.get('success_color', '#A3BE8C')
            error_color = self.branding_config.get('error_color', '#BF616A')
            warning_color = self.branding_config.get('warning_color', '#EBCB8B')
            info_color = self.branding_config.get('info_color', '#88C0D0')
            muted_color = self.branding_config.get('muted_color', '#4C566A')
            
            # Colores de texto automáticos para cada estado
            auto_success_text_color = self._get_contrasting_text_color(success_color)
            auto_error_text_color = self._get_contrasting_text_color(error_color)
            auto_warning_text_color = self._get_contrasting_text_color(warning_color)
            auto_info_text_color = self._get_contrasting_text_color(info_color)
            auto_muted_text_color = self._get_contrasting_text_color(muted_color)
            
            # Colores hover para estados
            success_hover_color = self._adjust_color_brightness(success_color, 1.2 if not is_dark_theme else 0.8)
            error_hover_color = self._adjust_color_brightness(error_color, 1.2 if not is_dark_theme else 0.8)
            warning_hover_color = self._adjust_color_brightness(warning_color, 1.2 if not is_dark_theme else 0.8)
            
            # MEJORA: Colores de texto automáticos para máximo contraste WCAG 2.1 AAA
            auto_primary_text_color = self._get_contrasting_text_color(primary_color, require_aaa=True)
            auto_secondary_text_color = self._get_contrasting_text_color(secondary_color, require_aaa=True)
            auto_accent_text_color = self._get_contrasting_text_color(accent_color, require_aaa=True)
            auto_hover_text_color = self._get_contrasting_text_color(hover_color, require_aaa=True)
            auto_selection_text_color = self._get_contrasting_text_color(selection_color, require_aaa=True)
            auto_pressed_text_color = self._get_contrasting_text_color(pressed_color, require_aaa=True)
            
            # Variables adicionales para compatibilidad con CSS existente
            auto_border_color = border_color
            auto_bg_color = background_color
            auto_alt_bg_color = alt_background_color
            auto_text_color = final_text_color
            auto_alt_text_color = final_alt_text_color
            
            # Variables adicionales para elementos específicos del CSS
            card_background = self._adjust_color_brightness(alt_background_color, 1.02 if is_dark_theme else 0.98)
            hover_background = self._adjust_color_brightness(card_background, 1.05 if is_dark_theme else 0.95)
            input_background = self._adjust_color_brightness(background_color, 1.02 if is_dark_theme else 0.98)
            focus_background = self._adjust_color_brightness(alt_background_color, 1.1 if is_dark_theme else 0.95)
            scroll_background = self._adjust_color_brightness(alt_background_color, 0.95 if is_dark_theme else 1.05)
            scroll_handle = self._adjust_color_brightness(primary_color, 0.8)
            scroll_handle_hover = self._adjust_color_brightness(primary_color, 1.1)
            secondary_text_color = self._adjust_color_brightness(ui_text_color or text_color, 0.7)
            primary_hover_color = hover_color
            
            # Variables para elementos de advertencia y estado
            warning_background = self._adjust_color_brightness(warning_color, 1.3 if is_dark_theme else 0.9)
            warning_text_color = auto_warning_text_color
            warning_border_color = self._adjust_color_brightness(warning_color, 0.8)
            primary_light = self._adjust_color_brightness(primary_color, 1.4 if is_dark_theme else 0.8)
            
            dynamic_css = f"""
        /* === CSS DINÁMICO DE BRANDING - COMPLEMENTARIO AL CSS BASE === */
        
        /* VARIABLES DE BRANDING PERSONALIZADAS */
        /* Solo sobrescribir colores específicos de branding, no estructura */
        
        /* COLORES PRIMARIOS DE BRANDING - Solo para elementos específicos */
        QTabBar::tab:selected {{
            background-color: {primary_color};
            color: {auto_primary_text_color};
            border-color: {primary_color};
        }}
        
        QTabBar::tab:hover:!selected {{
            background-color: {hover_color};
            border-color: {hover_color};
        }}
        
        /* BOTONES PRIMARIOS CON BRANDING Y CONTRASTE AUTOMÁTICO */
        QPushButton[class="primary"], QPushButton[objectName*="primary"] {{
            background-color: {primary_color};
            color: {auto_primary_text_color};
            border-color: {primary_color};
        }}
        
        QPushButton[class="primary"]:hover, QPushButton[objectName*="primary"]:hover {{
            background-color: {hover_color};
            color: {auto_hover_text_color};
            border-color: {hover_color};
        }}
        
        QPushButton[class="primary"]:pressed, QPushButton[objectName*="primary"]:pressed {{
            background-color: {pressed_color};
            color: {auto_primary_text_color};
            border-color: {pressed_color};
        }}
        
        /* BOTONES SECUNDARIOS CON BRANDING Y CONTRASTE AUTOMÁTICO */
        QPushButton[class="secondary"], QPushButton[objectName*="secondary"] {{
            background-color: {secondary_color};
            color: {auto_secondary_text_color};
            border-color: {secondary_color};
        }}
        
        QPushButton[class="secondary"]:hover, QPushButton[objectName*="secondary"]:hover {{
            background-color: {secondary_hover_color};
            color: {auto_secondary_text_color};
            border-color: {secondary_hover_color};
        }}
        
        /* ELEMENTOS DE ACENTO */
        QGroupBox::title {{
            color: {primary_color};
        }}
        
        /* CAMPOS DE ENTRADA CON TEMA ADAPTATIVO COMPLETO */
        QLineEdit:focus, QTextEdit:focus, QSpinBox:focus, QDoubleSpinBox:focus, QComboBox:focus {{
            border-color: {primary_color};
            background-color: {alt_background_color};
            color: {final_alt_text_color};
        }}
        
        QLineEdit:hover, QTextEdit:hover, QSpinBox:hover, QDoubleSpinBox:hover, QComboBox:hover {{
            border-color: {hover_color};
            background-color: {alt_background_color};
            color: {final_alt_text_color};
        }}
        
        /* COMBOBOX DROPDOWN ADAPTATIVO */
        QComboBox::drop-down {{
            border-left: 2px solid {border_color};
            width: 24px;
            border-top-right-radius: 6px;
            border-bottom-right-radius: 6px;
            background-color: {primary_color};
        }}
        
        QComboBox::drop-down:hover {{
            background-color: {hover_color};
        }}
        
        QComboBox::drop-down:pressed {{
            background-color: {pressed_color};
        }}
        
        QComboBox QAbstractItemView {{
            background-color: {alt_background_color};
            border: 2px solid {primary_color};
            border-radius: 6px;
            padding: 4px;
            selection-background-color: {primary_color};
            selection-color: {auto_primary_text_color};
            color: {final_alt_text_color};
        }}
        
        QComboBox QAbstractItemView::item {{
            padding: 8px 12px;
            border: none;
            color: {final_alt_text_color};
        }}
        
        QComboBox QAbstractItemView::item:hover {{
            background-color: {hover_color};
            color: {auto_hover_text_color};
        }}
        
        QComboBox QAbstractItemView::item:selected {{
            background-color: {primary_color};
            color: {auto_primary_text_color};
        }}
        
        /* SPINBOX ADAPTATIVO */
        QSpinBox::up-button, QSpinBox::down-button, QDoubleSpinBox::up-button, QDoubleSpinBox::down-button {{
            background-color: {primary_color};
            border-left: 2px solid {border_color};
            width: 24px;
            color: {auto_primary_text_color};
        }}
        
        QSpinBox::up-button:hover, QSpinBox::down-button:hover, QDoubleSpinBox::up-button:hover, QDoubleSpinBox::down-button:hover {{
            background-color: {hover_color};
            color: {auto_hover_text_color};
        }}
        
        QSpinBox::up-button:pressed, QSpinBox::down-button:pressed, QDoubleSpinBox::up-button:pressed, QDoubleSpinBox::down-button:pressed {{
            background-color: {pressed_color};
            color: {auto_pressed_text_color};
        }}
        
        /* SELECCIÓN DE TEXTO */
        QLineEdit, QTextEdit, QSpinBox, QDoubleSpinBox, QComboBox {{
            selection-background-color: {selection_color};
            selection-color: {auto_selection_text_color};
        }}
        
        /* TABLAS Y LISTAS CON CONTRASTE AUTOMÁTICO */
        QTableWidget, QListWidget, QTreeWidget {{
            background-color: {alt_background_color};
            color: {final_alt_text_color};
            border: 2px solid {border_color};
            border-radius: 6px;
            gridline-color: {border_color};
            selection-background-color: {primary_color};
            selection-color: {auto_primary_text_color};
            font-family: {main_font};
        }}
        
        QTableWidget::item, QListWidget::item, QTreeWidget::item {{
            padding: 6px;
            border-bottom: 1px solid {border_color};
        }}
        
        QTableWidget::item:selected, QListWidget::item:selected, QTreeWidget::item:selected {{
            background-color: {primary_color};
            color: {auto_primary_text_color};
        }}
        
        /* HOVER DINÁMICO PARA TABLAS */
        QTableView::item:hover, QTableWidget::item:hover {{
            background-color: {hover_color};
            color: {auto_hover_text_color};
        }}
        
        QTableView::item:selected:hover, QTableWidget::item:selected:hover {{
            background-color: {hover_color};
            color: {auto_hover_text_color};
        }}
        
        QListWidget::item:hover, QTreeWidget::item:hover {{
            background-color: {hover_color};
            color: {auto_hover_text_color};
            border-left: 3px solid {primary_color};
        }}
        
        QHeaderView::section {{
            background-color: {alt_background_color};
            color: {final_text_color};
            padding: 6px 8px;
            border: 1px solid {border_color};
            border-bottom: 2px solid {primary_color};
            font-weight: 600;
            font-family: {main_font};
        }}
        
        QHeaderView::section:hover {{
            background-color: {hover_color};
            color: {auto_hover_text_color};
        }}
        
        QHeaderView::section:pressed {{
            background-color: {primary_color};
            color: {auto_primary_text_color};
        }}
        
        /* SCROLLBARS */
        QScrollBar:vertical {{
            background-color: {alt_background_color};
            width: 12px;
            border-radius: 6px;
        }}
        
        QScrollBar::handle:vertical {{
            background-color: {primary_color};
            border-radius: 6px;
            min-height: 20px;
        }}
        
        QScrollBar::handle:vertical:hover {{
            background-color: {hover_color};
        }}
        
        QScrollBar:horizontal {{
            background-color: {alt_background_color};
            height: 12px;
            border-radius: 6px;
        }}
        
        QScrollBar::handle:horizontal {{
            background-color: {primary_color};
            border-radius: 6px;
            min-width: 20px;
        }}
        
        QScrollBar::handle:horizontal:hover {{
            background-color: {hover_color};
        }}
        
        /* CHECKBOXES Y RADIOBUTTONS */
        QCheckBox::indicator, QRadioButton::indicator {{
            width: 16px;
            height: 16px;
            border: 2px solid {border_color};
            border-radius: 3px;
            background-color: {alt_background_color};
        }}
        
        QCheckBox::indicator:checked, QRadioButton::indicator:checked {{
            background-color: {primary_color};
            border-color: {primary_color};
        }}
        
        QCheckBox::indicator:hover, QRadioButton::indicator:hover {{
            border-color: {hover_color};
        }}
        
        /* SLIDERS */
        QSlider::groove:horizontal {{
            border: 1px solid {border_color};
            height: 6px;
            background-color: {alt_background_color};
            border-radius: 3px;
        }}
        
        QSlider::handle:horizontal {{
            background-color: {primary_color};
            border: 2px solid {border_color};
            width: 16px;
            margin: -6px 0;
            border-radius: 8px;
        }}
        
        QSlider::handle:horizontal:hover {{
            background-color: {accent_hover_color};
        }}
        
        /* ELEMENTOS DE ACENTO CON HOVER */
        QCheckBox::indicator:checked:hover {{
            background-color: {accent_hover_color};
            border-color: {accent_hover_color};
        }}
        
        QRadioButton::indicator:checked:hover {{
            background-color: {accent_hover_color};
            border-color: {accent_hover_color};
        }}
        
        QScrollBar::handle:hover {{
            background-color: {accent_hover_color};
        }}
        
        QTabBar::tab:selected:hover {{
            background-color: {accent_hover_color};
            border-color: {accent_hover_color};
        }}
        
        /* ELEMENTOS DE ESTADO CON BRANDING */
        QLabel[class="success"], QLabel[objectName*="success"] {{
            color: {success_color};
        }}
        
        QLabel[class="error"], QLabel[objectName*="error"] {{
            color: {error_color};
        }}
        
        QLabel[class="warning"], QLabel[objectName*="warning"] {{
            color: {warning_color};
        }}
        
        QLabel[class="info"], QLabel[objectName*="info"] {{
            color: {info_color};
        }}
        
        /* BOTONES DE ESTADO */
        QPushButton[class="success"] {{
            background-color: {success_color};
            color: {auto_success_text_color};
            border-color: {success_color};
        }}
        
        QPushButton[class="success"]:hover {{
            background-color: {success_hover_color};
        }}
        
        QPushButton[class="error"] {{
            background-color: {error_color};
            color: {auto_error_text_color};
            border-color: {error_color};
        }}
        
        QPushButton[class="error"]:hover {{
            background-color: {error_hover_color};
        }}
        
        /* Alias: map danger to error palette */
        QPushButton[class="danger"], QPushButton[objectName*="danger"] {{
            background-color: {error_color};
            color: {auto_error_text_color};
            border-color: {error_color};
        }}
        
        QPushButton[class="danger"]:hover, QPushButton[objectName*="danger"]:hover {{
            background-color: {error_hover_color};
        }}
        
        QPushButton[class="warning"] {{
            background-color: {warning_color};
            color: {auto_warning_text_color};
            border-color: {warning_color};
        }}
        
        QPushButton[class="warning"]:hover {{
            background-color: {warning_hover_color};
        }}
        
        /* PERSONALIZACIÓN DE FUENTE PRINCIPAL Y CONTRASTE GLOBAL */
        QWidget {{
            font-family: {main_font};
        }}
        
        /* COLORES DE FONDO PERSONALIZADOS CON CONTRASTE AUTOMÁTICO */
        QMainWindow[objectName="MainWindow"] {{
            background-color: {background_color};
            color: {final_text_color};
        }}
        
        /* MEJORA: Contraste automático para todos los elementos de texto */
        QLabel, QLineEdit, QTextEdit, QComboBox, QSpinBox, QDoubleSpinBox {{
            color: {final_text_color};
        }}
        
        QGroupBox {{
            color: {final_text_color};
        }}
        
        /* MEJORA: Estados hover mejorados para todos los botones */
        QPushButton:hover {{
            background-color: {hover_color};
            color: {auto_hover_text_color};
            border-color: {hover_color};
        }}
        
        /* MEJORA: Elementos con fondo alternativo */
        QFrame[objectName*="background"], QWidget[class*="alt-bg"] {{
            background-color: {alt_background_color};
            color: {final_alt_text_color};
        }}
        
        /* ELEMENTOS ESPECÍFICOS DE BRANDING */
        QWidget[objectName*="branding"], QWidget[class*="branding"] {{
            background-color: {primary_color};
            color: {auto_primary_text_color};
        }}
        
        /* ELEMENTOS ESPECÍFICOS CON IDENTIFICADORES DE BRANDING */
        QLabel[objectName*="brand"], QFrame[objectName*="brand"] {{
            background-color: {primary_color};
            color: {auto_primary_text_color};
        }}
        
        /* HEADER DEL GIMNASIO - DISEÑO MODERNO Y ESTÉTICO */
        QWidget[objectName="gym_header"] {{
            background: qlineargradient(x1: 0, y1: 0, x2: 1, y2: 0,
                stop: 0 {background_color}, stop: 1 {alt_background_color});
            border-bottom: 3px solid {primary_color};
            padding: 0px;
            margin: 0px;
        }}
        
        QWidget[objectName="logo_container"] {{
            background-color: transparent;
            border: none;
            margin: 0px;
            padding: 4px;
        }}
        
        QLabel[objectName="gym_logo"] {{
            background-color: {alt_background_color};
            border: 3px solid {primary_color};
            border-radius: 36px;
            padding: 8px;
            margin: 0px;
        }}
        
        QWidget[objectName="info_container"] {{
            background-color: transparent;
            border: none;
            margin: 0px;
            padding: 0px;
        }}
        
        QLabel[objectName="gym_name"] {{
            color: {primary_color};
            font-family: "Segoe UI", "Inter", sans-serif;
            font-weight: bold;
            background-color: transparent;
            margin: 0px;
            padding: 0px 0px 4px 0px;
            text-shadow: 1px 1px 2px rgba(0, 0, 0, 0.3);
        }}
        
        QLabel[objectName="gym_slogan"] {{
            color: {final_text_color};
            font-family: "Segoe UI", "Inter", sans-serif;
            font-style: italic;
            background-color: transparent;
            margin: 0px;
            padding: 0px 0px 8px 0px;
            opacity: 0.9;
        }}
        
        QFrame[objectName="decorative_line"] {{
            background-color: {primary_color};
            border: none;
            margin: 0px;
            padding: 0px;
            border-radius: 1px;
        }}
        
        /* RESPONSIVE DESIGN PARA EL HEADER */
        QWidget[objectName="gym_header"][minimumWidth="800"] QLabel[objectName="gym_name"] {{
            font-size: 28px;
        }}
        
        QWidget[objectName="gym_header"][minimumWidth="800"] QLabel[objectName="gym_slogan"] {{
            font-size: 14px;
        }}
        
        QWidget[objectName="gym_header"][maximumWidth="799"] QLabel[objectName="gym_name"] {{
            font-size: 22px;
        }}
        
        QWidget[objectName="gym_header"][maximumWidth="799"] QLabel[objectName="gym_slogan"] {{
            font-size: 12px;
        }}
        
        QWidget[objectName="gym_header"][maximumWidth="600"] QLabel[objectName="gym_logo"] {{
            border-radius: 30px;
            padding: 6px;
        }}
        
        QWidget[objectName="gym_header"][maximumWidth="600"] QLabel[objectName="gym_name"] {{
            font-size: 18px;
        }}
        
        QWidget[objectName="gym_header"][maximumWidth="600"] QLabel[objectName="gym_slogan"] {{
            font-size: 11px;
        }}
        
        /* INDICADORES DE PROGRESO Y BARRAS */
        QProgressBar {{
            border: 1px solid {border_color};
            background-color: {alt_background_color};
        }}
        
        QProgressBar::chunk {{
            background-color: {primary_color};
        }}
        
        /* TOOLTIPS PERSONALIZADOS */
        QToolTip {{
            background-color: {primary_color};
            color: {auto_primary_text_color};
            border: 1px solid {border_color};
        }}
        
        /* MENÚS Y MENÚS CONTEXTUALES */
        QMenu {{
            background-color: {alt_background_color};
            color: {final_text_color};
            border: 2px solid {border_color};
            border-radius: 6px;
            padding: 4px;
        }}
        
        QMenu::item {{
            padding: 6px 12px;
            border-radius: 4px;
        }}
        
        QMenu::item:selected {{
            background-color: {primary_color};
            color: {auto_primary_text_color};
        }}
        
        /* SPLITTERS */
        QSplitter::handle {{
            background-color: {border_color};
        }}
        
        QSplitter::handle:hover {{
            background-color: {primary_color};
        }}
        
        /* METRIC CARDS Y GRÁFICOS CON BORDES DINÁMICOS */
        QFrame.metric-card {{
            background-color: {alt_background_color};
            border: 2px solid {border_color};
            border-radius: 8px;
            padding: 12px;
            margin: 4px;
            color: {final_alt_text_color};
        }}
        
        QFrame.metric-card:hover {{
            border-color: {primary_color};
            background-color: {hover_color};
            color: {auto_hover_text_color};
        }}
        
        /* GRÁFICOS CON ESTILO METRIC-CARD */
        QFrame[class="metric-card"] {{
            background-color: {alt_background_color};
            border: 2px solid {border_color};
            border-radius: 8px;
            padding: 8px;
            margin: 4px;
            color: {final_alt_text_color};
        }}
        
        QFrame[class="metric-card"]:hover {{
            border-color: {primary_color};
            background-color: {hover_color};
            color: {auto_hover_text_color};
        }}
        
        /* BARRA DE BÚSQUEDA GLOBAL FLOTANTE Y MEJORADA */
        QFrame#search_frame {{
            background-color: {alt_background_color};
            border: 2px solid {border_color};
            border-radius: 8px;
            padding: 4px 8px;
            margin: 2px;
            min-height: 32px;
            max-height: 32px;
        }}
        
        QFrame#search_frame:hover {{
            border-color: {primary_color};
            background-color: {hover_color};
        }}
        
        QLineEdit#search_input {{
            background-color: transparent;
            border: none;
            color: {final_alt_text_color};
            font-size: 13px;
            padding: 4px 8px;
            min-height: 20px;
            border-radius: 4px;
        }}
        
        QLineEdit#search_input:focus {{
            background-color: {background_color};
            color: {final_text_color};
            border: 1px solid {primary_color};
        }}
        
        QPushButton#search_clear_button {{
            background-color: {border_color};
            color: {final_text_color};
            border: none;
            border-radius: 6px;
            font-size: 10px;
            font-weight: bold;
            min-width: 12px;
            max-width: 12px;
            min-height: 12px;
            max-height: 12px;
        }}
        
        QPushButton#search_clear_button:hover {{
            background-color: {error_color};
            color: {auto_error_text_color};
        }}
        
        /* PANEL DE RESULTADOS FLOTANTE */
        QFrame#results_frame {{
            background-color: {alt_background_color};
            border: 2px solid {primary_color};
            border-radius: 8px;
            padding: 8px;
            margin: 0px;
            min-width: 280px;
            max-width: 400px;
        }}
        
        QScrollArea#search_results_scroll {{
            background-color: transparent;
            border: none;
            border-radius: 6px;
        }}
        
        /* CONTENEDOR DE RESULTADOS DE BÚSQUEDA */
        QWidget#search_results_container {{
            background-color: {background_color};
            border: none;
        }}
        
        /* ELEMENTOS DE RESULTADO DE BÚSQUEDA */
        QFrame[objectName*="search_result"] {{
            background-color: {background_color};
            border: 1px solid {border_color};
            border-radius: 6px;
            padding: 8px 12px;
            margin: 2px 0px;
            min-height: 40px;
            color: {final_text_color};
        }}
        
        QFrame[objectName*="search_result"]:hover {{
            background-color: {hover_color};
            border-color: {primary_color};
            color: {auto_hover_text_color};
        }}
        
        QLabel#search_result_type_label {{
            background-color: {primary_color};
            color: {auto_primary_text_color};
            border-radius: 4px;
            padding: 2px 6px;
            font-size: 9px;
            font-weight: bold;
            min-width: 40px;
            max-height: 18px;
        }}
        
        /* NOMBRE DEL GIMNASIO EN PESTAÑAS - ESTILO PROFESIONAL Y MODERNO */
        QLabel#gym_name_tab {{
            color: {primary_color};
            font-family: "Segoe UI", "Inter", "Roboto", sans-serif;
            font-weight: 700;
            font-size: 14px;
            background: qlineargradient(x1: 0, y1: 0, x2: 1, y2: 0,
                stop: 0 rgba(255, 255, 255, 0.05), stop: 1 rgba(255, 255, 255, 0.02));
            border: 2px solid {primary_color};
            border-radius: 8px;
            padding: 6px 12px;
            margin: 2px;
            text-shadow: 0px 1px 2px rgba(0, 0, 0, 0.3);
            min-height: 20px;
        }}
        
        QLabel#gym_name_tab:hover {{
            background: qlineargradient(x1: 0, y1: 0, x2: 1, y2: 0,
                stop: 0 {primary_color}, stop: 1 {hover_color});
            color: {auto_primary_text_color};
            border-color: {hover_color};
            text-shadow: 0px 1px 3px rgba(0, 0, 0, 0.5);
            transform: scale(1.02);
        }}
        
        /* CONTENEDOR DEL NOMBRE DEL GIMNASIO */
        QWidget#gym_name_container {{
            background-color: transparent;
            border: none;
            margin: 0px;
            padding: 0px;
        }}
        
        QWidget#gym_name_container:hover {{
            background: qlineargradient(x1: 0, y1: 0, x2: 0, y2: 1,
                stop: 0 rgba(255, 255, 255, 0.03), stop: 1 rgba(255, 255, 255, 0.01));
            border-radius: 6px;
        }}
        
        /* ESTILOS DE ACCESIBILIDAD - TEMAS DE ALTO CONTRASTE */
        
        /* Tema de alto contraste negro */
        QWidget[objectName="high_contrast_black_widget"] {{
            background-color: #000000 !important;
            color: #FFFFFF !important;
        }}
        
        QWidget[objectName="high_contrast_black_widget"] QLabel {{
            background-color: #000000 !important;
            color: #FFFFFF !important;
            border-color: #FFFFFF !important;
        }}
        
        QWidget[objectName="high_contrast_black_widget"] QPushButton {{
            background-color: #000000 !important;
            color: #FFFFFF !important;
            border: 2px solid #FFFFFF !important;
            font-weight: bold !important;
        }}
        
        QWidget[objectName="high_contrast_black_widget"] QPushButton:hover {{
            background-color: #FFFFFF !important;
            color: #000000 !important;
            border: 2px solid #FFFFFF !important;
        }}
        
        QWidget[objectName="high_contrast_black_widget"] QLineEdit,
        QWidget[objectName="high_contrast_black_widget"] QTextEdit,
        QWidget[objectName="high_contrast_black_widget"] QComboBox {{
            background-color: #000000 !important;
            color: #FFFFFF !important;
            border: 2px solid #FFFFFF !important;
        }}
        
        QWidget[objectName="high_contrast_black_widget"] QGroupBox {{
            background-color: #000000 !important;
            color: #FFFFFF !important;
            border: 2px solid #FFFFFF !important;
            font-weight: bold !important;
        }}
        
        QWidget[objectName="high_contrast_black_widget"] QTableWidget {{
            background-color: #000000 !important;
            color: #FFFFFF !important;
            gridline-color: #FFFFFF !important;
            selection-background-color: #FFFFFF !important;
            selection-color: #000000 !important;
        }}
        
        QWidget[objectName="high_contrast_black_widget"] QHeaderView::section {{
            background-color: #000000 !important;
            color: #FFFFFF !important;
            border: 1px solid #FFFFFF !important;
            font-weight: bold !important;
        }}
        
        /* Tema de alto contraste blanco */
        QWidget[objectName="high_contrast_white_widget"] {{
            background-color: #FFFFFF !important;
            color: #000000 !important;
        }}
        
        QWidget[objectName="high_contrast_white_widget"] QLabel {{
            background-color: #FFFFFF !important;
            color: #000000 !important;
            border-color: #000000 !important;
        }}
        
        QWidget[objectName="high_contrast_white_widget"] QPushButton {{
            background-color: #FFFFFF !important;
            color: #000000 !important;
            border: 2px solid #000000 !important;
            font-weight: bold !important;
        }}
        
        QWidget[objectName="high_contrast_white_widget"] QPushButton:hover {{
            background-color: #000000 !important;
            color: #FFFFFF !important;
            border: 2px solid #000000 !important;
        }}
        
        QWidget[objectName="high_contrast_white_widget"] QLineEdit,
        QWidget[objectName="high_contrast_white_widget"] QTextEdit,
        QWidget[objectName="high_contrast_white_widget"] QComboBox {{
            background-color: #FFFFFF !important;
            color: #000000 !important;
            border: 2px solid #000000 !important;
        }}
        
        QWidget[objectName="high_contrast_white_widget"] QGroupBox {{
            background-color: #FFFFFF !important;
            color: #000000 !important;
            border: 2px solid #000000 !important;
            font-weight: bold !important;
        }}
        
        QWidget[objectName="high_contrast_white_widget"] QTableWidget {{
            background-color: #FFFFFF !important;
            color: #000000 !important;
            gridline-color: #000000 !important;
            selection-background-color: #000000 !important;
            selection-color: #FFFFFF !important;
        }}
        
        QWidget[objectName="high_contrast_white_widget"] QHeaderView::section {{
            background-color: #FFFFFF !important;
            color: #000000 !important;
            border: 1px solid #000000 !important;
            font-weight: bold !important;
        }}
        
        /* Tema azul-amarillo para daltonismo */
        QWidget[objectName="blue_yellow_widget"] {{
            background-color: #003366 !important;
            color: #FFFF00 !important;
        }}
        
        QWidget[objectName="blue_yellow_widget"] QLabel {{
            background-color: #003366 !important;
            color: #FFFF00 !important;
            border-color: #FFFF00 !important;
        }}
        
        QWidget[objectName="blue_yellow_widget"] QPushButton {{
            background-color: #0066CC !important;
            color: #FFFF00 !important;
            border: 2px solid #FFFF00 !important;
            font-weight: bold !important;
        }}
        
        QWidget[objectName="blue_yellow_widget"] QPushButton:hover {{
            background-color: #FFFF00 !important;
            color: #003366 !important;
            border: 2px solid #0066CC !important;
        }}
        
        QWidget[objectName="blue_yellow_widget"] QLineEdit,
        QWidget[objectName="blue_yellow_widget"] QTextEdit,
        QWidget[objectName="blue_yellow_widget"] QComboBox {{
            background-color: #003366 !important;
            color: #FFFF00 !important;
            border: 2px solid #FFFF00 !important;
        }}
        
        QWidget[objectName="blue_yellow_widget"] QGroupBox {{
            background-color: #003366 !important;
            color: #FFFF00 !important;
            border: 2px solid #FFFF00 !important;
            font-weight: bold !important;
        }}
        
        QWidget[objectName="blue_yellow_widget"] QTableWidget {{
            background-color: #003366 !important;
            color: #FFFF00 !important;
            gridline-color: #FFFF00 !important;
            selection-background-color: #FFFF00 !important;
            selection-color: #003366 !important;
        }}
        
        QWidget[objectName="blue_yellow_widget"] QHeaderView::section {{
            background-color: #003366 !important;
            color: #FFFF00 !important;
            border: 1px solid #FFFF00 !important;
            font-weight: bold !important;
        }}
        
        /* Estilos para texto grande (accesibilidad) */
        .large-text {{
            font-size: 16px !important;
        }}
        
        .extra-large-text {{
            font-size: 20px !important;
        }}
        
        .accessibility-focus {{
            border: 3px solid #FF6600 !important;
            outline: none !important;
        }}
        
        /* ESTILOS PARA WIDGETS DE ACCESIBILIDAD */
        QLabel#accessibility_value_label {{
                font-size: 12px;
                font-weight: bold;
                color: {primary_color};
            }}
            
            /* Estilos para Etiquetas Widget */
            QLabel#etiquetas_color_preview {{
                border: 2px solid {border_color};
                border-radius: 15px;
                min-width: 30px;
                max-width: 30px;
                min-height: 30px;
                max-height: 30px;
            }}
            
            QPushButton#etiquetas_color_btn {{
                border: 1px solid {border_color};
                border-radius: 15px;
                min-width: 30px;
                max-width: 30px;
                min-height: 30px;
                max-height: 30px;
            }}
            
            QWidget#etiqueta_item_widget {{
                background-color: {card_background};
                border: 1px solid {border_color};
                border-radius: 8px;
                padding: 8px;
                margin: 2px;
            }}
            
            QCheckBox#etiqueta_checkbox {{
                font-size: 12px;
                color: {text_color};
            }}
            
            QLabel#etiqueta_nombre_label {{
                font-weight: bold;
                color: {text_color};
                font-size: 13px;
            }}
            
            QLabel#etiqueta_desc_label {{
                color: {secondary_text_color};
                font-size: 11px;
            }}
            
            QPushButton#etiqueta_editar_btn {{
                background-color: {primary_color};
                color: white;
                border: none;
                border-radius: 4px;
                padding: 4px 8px;
                font-size: 11px;
            }}
            
            QPushButton#etiqueta_editar_btn:hover {{
                background-color: {primary_hover_color};
            }}
            
            QPushButton#etiqueta_eliminar_btn {{
                background-color: #dc3545;
                color: white;
                border: none;
                border-radius: 4px;
                padding: 4px 8px;
                font-size: 11px;
            }}
            
            QPushButton#etiqueta_eliminar_btn:hover {{
                background-color: #c82333;
            }}
            
            QLabel#etiquetas_titulo {{
                font-size: 18px;
                font-weight: bold;
                color: {primary_color};
                padding: 10px 0;
            }}
            
            QGroupBox#etiquetas_filtros_group {{
                font-weight: bold;
                color: {text_color};
                border: 2px solid {border_color};
                border-radius: 8px;
                margin-top: 12px;
                padding-top: 12px;
                background-color: {card_background};
            }}
            
            QLabel#etiquetas_color_label,
            QLabel#etiquetas_texto_label {{
                font-weight: bold;
                color: {text_color};
                font-size: 12px;
            }}
            
            QWidget#etiquetas_controles_widget {{
                background-color: {card_background};
                border: 1px solid {border_color};
                border-radius: 8px;
                padding: 12px;
            }}
            
            QPushButton#etiquetas_crear_btn {{
                background-color: {success_color};
                color: white;
                border: none;
                border-radius: 6px;
                padding: 8px 16px;
                font-weight: bold;
            }}
            
            QPushButton#etiquetas_crear_btn:hover {{
                background-color: {success_hover_color};
            }}
            
            QPushButton#etiquetas_asignar_btn {{
                background-color: {primary_color};
                color: white;
                border: none;
                border-radius: 6px;
                padding: 8px 16px;
                font-weight: bold;
            }}
            
            QPushButton#etiquetas_asignar_btn:hover {{
                background-color: {primary_hover_color};
            }}
            
            QGroupBox#etiquetas_usuario_section,
            QGroupBox#etiquetas_todas_section {{
                font-weight: bold;
                font-size: 14px;
                color: {text_color};
                border: 2px solid {border_color};
                border-radius: 10px;
                margin-top: 12px;
                padding-top: 12px;
                background-color: {card_background};
            }}
            
            QGroupBox#etiquetas_usuario_section::title,
            QGroupBox#etiquetas_todas_section::title {{
                subcontrol-origin: margin;
                left: 12px;
                padding: 0 8px 0 8px;
                color: {primary_color};
            }}
            
            QScrollArea#etiquetas_scroll_area {{
                background-color: {background_color};
                border: 1px solid {border_color};
                border-radius: 8px;
            }}
            
            QScrollArea#etiquetas_scroll_area QScrollBar:vertical {{
                background-color: {scroll_background};
                width: 8px;
                border-radius: 4px;
            }}
            
            QScrollArea#etiquetas_scroll_area QScrollBar::handle:vertical {{
                background-color: {scroll_handle};
                border-radius: 4px;
                min-height: 20px;
            }}
            
            QScrollArea#etiquetas_scroll_area QScrollBar::handle:vertical:hover {{
                background-color: {scroll_handle_hover};
            }}
            
            QLabel#etiquetas_mensaje_vacio {{
                font-style: italic;
                padding: 20px;
                font-size: 12px;
                color: {secondary_text_color};
            }}
            
            /* Estilos para elementos de audit dashboard */
            QFrame#audit_metric_card {{
                border: 2px solid {border_color};
                border-radius: 8px;
                background-color: {card_background};
                padding: 10px;
                margin: 4px;
            }}
            
            QFrame#audit_metric_card:hover {{
                border-color: {primary_color};
                background-color: {hover_background};
            }}
            
            QLabel#audit_metric_title {{
                font-weight: bold;
                color: {primary_color};
                margin-bottom: 5px;
                font-size: 12px;
            }}
            
            QLabel#audit_metric_value {{
                color: {text_color};
                font-size: 18px;
                font-weight: bold;
            }}
            
            QLabel#audit_metric_subtitle {{
                color: {secondary_text_color};
                font-size: 10px;
            }}
            
            QLabel#audit_dashboard_title {{
                color: {text_color};
                font-size: 16px;
                font-weight: bold;
                margin-bottom: 10px;
            }}
            
            QLabel#audit_info_label {{
                background-color: {warning_background};
                color: {warning_text_color};
                padding: 10px;
                border-radius: 5px;
                margin-bottom: 10px;
                border: 1px solid {warning_border_color};
            }}
            
            /* Estilos para filas de severidad en tablas de auditoría */
            QTreeWidget::item[severity="high"] {{
                background-color: rgba(255, 235, 235, 0.8);
            }}
            
            QTreeWidget::item[severity="medium"] {{
                background-color: rgba(255, 248, 220, 0.8);
            }}
            
            QTreeWidget::item[severity="low"] {{
                background-color: rgba(240, 248, 255, 0.8);
            }}
            
            /* Estilos para estados de usuario y asistencia */
            QLabel.status-active {{
                color: {success_color};
                font-weight: bold;
            }}
            
            QLabel.status-inactive {{
                color: {error_color};
                font-weight: bold;
            }}
            
            QLabel.attendance-present {{
                color: {success_color};
                font-weight: bold;
            }}
            
            QLabel.attendance-absent {{
                color: {warning_color};
                font-weight: bold;
            }}
            
            /* Estilos para elementos de búsqueda global */
            QLineEdit#search_input {{
                border: 1px solid {border_color};
                border-radius: 4px;
                padding: 4px 8px;
                background-color: {input_background};
                color: {text_color};
                font-size: 11px;
            }}
            
            QLineEdit#search_input:focus {{
                border-color: {primary_color};
                background-color: {focus_background};
            }}
            
            QPushButton#search_clear_button {{
                border: none;
                background-color: transparent;
                color: {secondary_text_color};
                font-size: 10px;
                border-radius: 6px;
            }}
            
            QPushButton#search_clear_button:hover {{
                background-color: {hover_background};
                color: {text_color};
            }}
            
            QFrame#search_frame {{
                background-color: {card_background};
                border: 1px solid {border_color};
                border-radius: 4px;
            }}
            
            QFrame#results_frame {{
                background-color: {card_background};
                border: 2px solid {border_color};
                border-radius: 6px;
            }}
            
            QLabel#search_results_header {{
                color: {text_color};
                font-weight: bold;
                margin-bottom: 8px;
            }}
            
            QLabel#search_no_results {{
                color: {secondary_text_color};
                padding: 16px;
                font-size: 12px;
                font-style: italic;
            }}
            
            QFrame#search_result_item {{
                background-color: {card_background};
                border: 1px solid {border_color};
                border-radius: 4px;
                margin: 2px 0px;
            }}
            
            QFrame#search_result_item:hover {{
                background-color: {hover_background};
                border-color: {primary_color};
            }}
            
            QLabel#search_result_title {{
                color: {text_color};
                font-weight: bold;
            }}
            
            QLabel#search_result_subtitle {{
                color: {secondary_text_color};
            }}
            
            QLabel#search_result_description {{
                color: {secondary_text_color};
            }}
            
            QLabel#search_result_type_label {{
                color: {primary_color};
                font-weight: bold;
                background-color: {primary_light};
                padding: 2px 6px;
                border-radius: 3px;
            }}
            
            QScrollArea#search_results_scroll {{
                border: none;
                background-color: {card_background};
            }}
            
            QWidget#search_results_container {{
                background-color: {card_background};
            }}
            
            
            /* === ESTILOS DE ACCESIBILIDAD === */
            /* Temas de alto contraste y accesibilidad */
            
            /* Tema de alto contraste negro */
            QWidget[accessibility_theme="high_contrast_black"] {{
                background-color: #000000;
                color: #FFFFFF;
                border: 1px solid #FFFFFF;
            }}
            
            QWidget[accessibility_theme="high_contrast_black"] QPushButton {{
                background-color: #FFFFFF;
                color: #000000;
                border: 2px solid #000000;
                padding: 8px;
            }}
            
            QWidget[accessibility_theme="high_contrast_black"] QPushButton:hover {{
                background-color: #FFFF00;
                color: #000000;
            }}
            
            QWidget[accessibility_theme="high_contrast_black"] QLineEdit,
            QWidget[accessibility_theme="high_contrast_black"] QTextEdit {{
                background-color: #FFFFFF;
                color: #000000;
                border: 2px solid #000000;
            }}
            
            /* Tema de alto contraste blanco */
            QWidget[accessibility_theme="high_contrast_white"] {{
                background-color: #FFFFFF;
                color: #000000;
                border: 2px solid #000000;
            }}
            
            QWidget[accessibility_theme="high_contrast_white"] QPushButton {{
                background-color: #000000;
                color: #FFFFFF;
                border: 2px solid #FFFFFF;
                padding: 8px;
            }}
            
            QWidget[accessibility_theme="high_contrast_white"] QPushButton:hover {{
                background-color: #0000FF;
                color: #FFFFFF;
            }}
            
            /* Tema azul-amarillo para daltonismo */
            QWidget[accessibility_theme="blue_yellow"] {{
                background-color: #000080;
                color: #FFFF00;
            }}
            
            QWidget[accessibility_theme="blue_yellow"] QPushButton {{
                background-color: #FFFF00;
                color: #000080;
                border: 2px solid #FFFFFF;
                padding: 8px;
            }}
            
            /* Tema en escala de grises */
            QWidget[accessibility_theme="grayscale"] {{
                background-color: #808080;
                color: #FFFFFF;
            }}
            
            QWidget[accessibility_theme="grayscale"] QPushButton {{
                background-color: #C0C0C0;
                color: #000000;
                border: 1px solid #000000;
                padding: 8px;
            }}
            
            /* Estilos para elementos específicos de accesibilidad */
            QLabel#accessibility_title {{
                font-size: 18px;
                font-weight: bold;
                color: {primary_color};
                margin-bottom: 10px;
            }}
            
            QGroupBox#accessibility_nav_group,
            QGroupBox#accessibility_shortcuts_group {{
                font-weight: bold;
                color: {text_color};
                border: 2px solid {border_color};
                border-radius: 8px;
                margin-top: 10px;
                padding-top: 10px;
            }}
            
            QCheckBox#accessibility_checkbox {{
                font-size: 12px;
                color: {text_color};
                spacing: 8px;
            }}
            
            QLabel#accessibility_header_label {{
                font-weight: bold;
                color: {primary_color};
                font-size: 14px;
                margin: 5px 0;
            }}
            
            QLabel#accessibility_function_label {{
                font-weight: bold;
                color: {text_color};
                min-width: 120px;
            }}
            
            QLabel#accessibility_description_label {{
                color: {secondary_color};
                font-style: italic;
            }}
            
            QSlider#accessibility_slider {{
                height: 20px;
            }}
            
            QSlider#accessibility_slider::groove:horizontal {{
                border: 1px solid {border_color};
                height: 8px;
                background-color: {alt_background_color};
                border-radius: 4px;
            }}
            
            QSlider#accessibility_slider::handle:horizontal {{
                background-color: {primary_color};
                border: 2px solid {border_color};
                width: 18px;
                margin: -5px 0;
                border-radius: 9px;
            }}
            
            QSlider#accessibility_slider::handle:horizontal:hover {{
                background-color: {hover_color};
            }}
            
            
            /* BOTÓN DE ALERTAS - MIGRADO DEL ESTILO HARDCODEADO */
            QPushButton[objectName="alerts_button"] {{
                padding: 2px 8px;
                border: 1px solid {border_color};
                border-radius: 3px;
                background-color: {alt_background_color};
                color: {final_alt_text_color};
            }}
            
            QPushButton[objectName="alerts_button"]:hover {{
                background-color: {hover_color};
                border-color: {hover_color};
            }}
            """
            
            return dynamic_css
        except NameError as e:
            logging.error(f"Error en generate_dynamic_css - Variable no definida: {e}")
            return ""
        except Exception as e:
            logging.error(f"Error en generate_dynamic_css: {e}")
            return ""
    
    def _adjust_color_brightness(self, hex_color, factor):
        """Ajusta el brillo de un color hexadecimal"""
        try:
            # Remover el # si está presente
            hex_color = hex_color.lstrip('#')
            
            # Convertir a RGB
            r = int(hex_color[0:2], 16)
            g = int(hex_color[2:4], 16)
            b = int(hex_color[4:6], 16)
            
            # Ajustar brillo
            r = max(0, min(255, int(r * factor)))
            g = max(0, min(255, int(g * factor)))
            b = max(0, min(255, int(b * factor)))
            
            # Convertir de vuelta a hex
            return f"#{r:02x}{g:02x}{b:02x}"
        except:
            return hex_color
    
    def _get_relative_luminance(self, hex_color):
        """Calcula la luminancia relativa según las pautas WCAG 2.1"""
        try:
            # Remover el # si está presente
            hex_color = hex_color.lstrip('#')
            
            # Convertir a RGB
            r = int(hex_color[0:2], 16) / 255.0
            g = int(hex_color[2:4], 16) / 255.0
            b = int(hex_color[4:6], 16) / 255.0
            
            # Aplicar corrección gamma según WCAG
            def gamma_correct(c):
                if c <= 0.03928:
                    return c / 12.92
                else:
                    return pow((c + 0.055) / 1.055, 2.4)
            
            r = gamma_correct(r)
            g = gamma_correct(g)
            b = gamma_correct(b)
            
            # Calcular luminancia relativa
            return 0.2126 * r + 0.7152 * g + 0.0722 * b
        except:
            return 0.5
    
    def _calculate_contrast_ratio(self, color1, color2):
        """Calcula la relación de contraste entre dos colores según WCAG"""
        try:
            lum1 = self._get_relative_luminance(color1)
            lum2 = self._get_relative_luminance(color2)
            
            # Asegurar que lum1 sea la luminancia más alta
            if lum1 < lum2:
                lum1, lum2 = lum2, lum1
            
            # Calcular relación de contraste
            return (lum1 + 0.05) / (lum2 + 0.05)
        except:
            return 1.0
    
    def _generate_high_contrast_light_color(self, background_color, min_contrast):
        """Genera un color claro que cumpla con el contraste mínimo requerido"""
        try:
            # Comenzar con blanco y oscurecer gradualmente hasta encontrar contraste suficiente
            for lightness in range(255, 127, -5):
                test_color = f"#{lightness:02x}{lightness:02x}{lightness:02x}"
                if self._calculate_contrast_ratio(background_color, test_color) >= min_contrast:
                    return test_color
            # Si no se encuentra, usar gris claro como último recurso
            return "#E0E0E0"
        except:
            return "#FFFFFF"
    
    def _generate_high_contrast_dark_color(self, background_color, min_contrast):
        """Genera un color oscuro que cumpla con el contraste mínimo requerido"""
        try:
            # Comenzar con negro y aclarar gradualmente hasta encontrar contraste suficiente
            for darkness in range(0, 128, 5):
                test_color = f"#{darkness:02x}{darkness:02x}{darkness:02x}"
                if self._calculate_contrast_ratio(background_color, test_color) >= min_contrast:
                    return test_color
            # Si no se encuentra, usar gris oscuro como último recurso
            return "#202020"
        except:
            return "#000000"
    
    def _is_dark_color(self, hex_color):
        """Determina si un color es oscuro basado en su luminancia relativa mejorada"""
        try:
            luminance = self._get_relative_luminance(hex_color)
            # Usar un umbral más preciso basado en WCAG
            return luminance < 0.179  # Aproximadamente equivale a #777777
        except:
            return False
    
    def _get_contrasting_text_color(self, background_color, require_aaa=False):
        """Obtiene un color de texto que contraste óptimamente con el fondo según WCAG 2.1"""
        try:
            # Calcular contraste con blanco y negro
            contrast_white = self._calculate_contrast_ratio(background_color, "#FFFFFF")
            contrast_black = self._calculate_contrast_ratio(background_color, "#000000")
            
            # WCAG AA requiere al menos 4.5:1 para texto normal, 3:1 para texto grande
            # WCAG AAA requiere al menos 7:1 para texto normal, 4.5:1 para texto grande
            min_contrast = 7.0 if require_aaa else 4.5
            
            # Si ambos cumplen el estándar requerido, elegir el de mayor contraste
            if contrast_white >= min_contrast and contrast_black >= min_contrast:
                return "#FFFFFF" if contrast_white > contrast_black else "#000000"
            
            # Si solo uno cumple el estándar, usar ese
            elif contrast_white >= min_contrast:
                return "#FFFFFF"
            elif contrast_black >= min_contrast:
                return "#000000"
            
            # Si ninguno cumple el estándar mínimo, generar un color alternativo
            else:
                # Intentar con grises más contrastantes
                if contrast_white > contrast_black:
                    # Fondo oscuro, necesitamos texto más claro
                    return self._generate_high_contrast_light_color(background_color, min_contrast)
                else:
                    # Fondo claro, necesitamos texto más oscuro
                    return self._generate_high_contrast_dark_color(background_color, min_contrast)
        except Exception as e:
            logging.error(f"Error calculando color de texto contrastante: {e}")
            # Fallback seguro
            if self._is_dark_color(background_color):
                return "#FFFFFF"
            else:
                return "#000000"
    
    def apply_dynamic_variables_to_qss(self, qss_content):
        """Aplica variables dinámicas de branding al contenido QSS"""
        print("DEBUG - apply_dynamic_variables_to_qss iniciado")
        if not self.branding_config:
            print("DEBUG - No hay branding_config, retornando qss_content original")
            return qss_content
        
        try:
            # Obtener colores de la configuración de branding
            primary_color = self.branding_config.get('primary_color', '#5E81AC')
            secondary_color = self.branding_config.get('secondary_color', '#2ecc71')
            accent_color = self.branding_config.get('accent_color', '#e74c3c')
            background_color = self.branding_config.get('background_color', '#252A35')
            alt_background_color = self.branding_config.get('alt_background_color', '#2E3440')
            text_color = self.branding_config.get('text_color', '#ECEFF4')
            main_font = self.branding_config.get('main_font', 'Segoe UI')
            # Colores de estado adicionales
            warning_color = self.branding_config.get('warning_color', '#EBCB8B')
            info_color = self.branding_config.get('info_color', '#88C0D0')
            
            # Detectar si es tema oscuro
            is_dark_theme = self._is_dark_color(background_color)
            
            # Generar colores derivados
            hover_color = self.branding_config.get('primary_hover_color', self._calculate_smart_hover_color(primary_color, is_dark_theme))
            pressed_color = self._calculate_smart_pressed_color(primary_color, is_dark_theme)
            secondary_hover_color = self.branding_config.get('secondary_hover_color', self._calculate_smart_hover_color(secondary_color, is_dark_theme))
            accent_hover_color = self.branding_config.get('accent_hover_color', self._calculate_smart_hover_color(accent_color, is_dark_theme))
            border_color = self._adjust_color_brightness(primary_color, 0.9)
            tertiary_bg = self._adjust_color_brightness(alt_background_color, 1.1 if not is_dark_theme else 0.9)
            
            # Debug: Verificar valores de colores generados
            print(f"DEBUG - Colores generados:")
            print(f"  primary_color: {primary_color}")
            print(f"  hover_color: {hover_color}")
            print(f"  pressed_color: {pressed_color}")
            print(f"  is_dark_theme: {is_dark_theme}")
            
            # Colores de texto automáticos con contraste WCAG 2.1
            auto_primary_text = self._get_contrasting_text_color(primary_color, require_aaa=True)
            auto_bg_text = self._get_contrasting_text_color(background_color, require_aaa=True)
            auto_alt_bg_text = self._get_contrasting_text_color(alt_background_color, require_aaa=True)
            auto_tertiary_bg_text = self._get_contrasting_text_color(tertiary_bg, require_aaa=True)
            
            # Diccionario de reemplazos para variables CSS
            replacements = {
                # Colores principales
                '#252A35': background_color,  # --bg-primary
                '#2E3440': alt_background_color,  # --bg-secondary
                '#3B4252': tertiary_bg,  # --bg-tertiary
                '#5E81AC': primary_color,  # --accent-primary
                '#81A1C1': hover_color,  # --accent-primary-hover
                '#4C6A94': pressed_color,  # --accent-primary-pressed
                '#88C0D0': self.branding_config.get('muted_color', self._adjust_color_brightness(text_color, 0.8)),  # --text-muted
                '#EBCB8B': warning_color,  # --accent-warning (por compatibilidad en estilos antiguos)
                
                # Colores de texto con contraste automático WCAG 2.1
                '#ECEFF4': self.branding_config.get('ui_text_color', auto_bg_text),  # --text-primary (contraste automático o configurable)
                '#D8DEE9': auto_alt_bg_text,  # --text-secondary (contraste automático con fondo alternativo)
                '#B8C5D1': auto_tertiary_bg_text,  # --text-tertiary (contraste automático con fondo terciario)
                
                # Bordes
                '#4C566A': border_color,  # --border-primary
                
                # Variables VAR_* utilizadas en el CSS
                'VAR_BG_QUATERNARY': hover_color,  # Color hover para elementos
                'VAR_PRIMARY_COLOR': primary_color,  # Color primario
                'VAR_PRIMARY_HOVER_COLOR': hover_color,  # Color hover primario
                'VAR_SECONDARY_HOVER_COLOR': secondary_hover_color,  # Color hover secundario
                'VAR_ACCENT_HOVER_COLOR': accent_hover_color,  # Color hover de acento
                'VAR_PRIMARY_COLOR_PRESSED': pressed_color,  # Color primario presionado
                'VAR_BORDER_PRIMARY': border_color,  # Color de borde primario
                'VAR_BORDER_SECONDARY': self._adjust_color_brightness(border_color, 1.1 if is_dark_theme else 0.9),  # Borde secundario
                'VAR_BG_PRIMARY': background_color,  # Color de fondo primario
                'VAR_BG_SECONDARY': alt_background_color,  # Color de fondo secundario
                'VAR_BG_TERTIARY': tertiary_bg,  # Color de fondo terciario
                'VAR_TEXT_PRIMARY': self.branding_config.get('ui_text_color', auto_bg_text),  # Color de texto primario (configurable)
                'VAR_TEXT_SECONDARY': auto_alt_bg_text,  # Color de texto secundario
                'VAR_TEXT_TERTIARY': auto_tertiary_bg_text,  # Color de texto terciario
                'VAR_TEXT_MUTED': self.branding_config.get('muted_color', auto_alt_bg_text),  # Color de texto muted
                'VAR_TEXT_ON_BRAND': auto_primary_text,  # Texto legible sobre color de marca
                'VAR_ACCENT_SUCCESS': secondary_color,  # Acento de éxito
                'VAR_ACCENT_DANGER': accent_color,  # Acento de peligro
                'VAR_ACCENT_WARNING': warning_color,  # Acento de advertencia
                'VAR_ACCENT_INFO': info_color,  # Acento informativo
                
                # Fuentes
                'VAR_FONT_FAMILY': main_font,
                '"Segoe UI"': f'"{main_font}"',
                'Segoe UI': main_font,
            }
            
            # Reemplazos críticos verificados internamente (sin salida a consola)
            
            # Aplicar reemplazos
            processed_qss = qss_content
            for old_value, new_value in replacements.items():
                if old_value == 'VAR_PRIMARY_COLOR_PRESSED':
                    processed_qss = processed_qss.replace(old_value, new_value)
                    if isinstance(new_value, str) and '_PRESSED' in new_value:
                        logging.error(f"El valor de reemplazo contiene '_PRESSED': {new_value}")
                else:
                    processed_qss = processed_qss.replace(old_value, new_value)
            
            # Validación: detectar ocurrencias no esperadas de '_PRESSED' en el CSS procesado
            if '_PRESSED' in processed_qss:
                lines_with_pressed = [line.strip() for line in processed_qss.split('\n') if '_PRESSED' in line]
                logging.error(f"Encontrado '_PRESSED' en CSS procesado: {len(lines_with_pressed)} ocurrencias")
            
            return processed_qss
            
        except Exception as e:
            logging.error(f"Error aplicando variables dinámicas al QSS: {e}")
            return qss_content
    
    def _needs_contrast_adjustment(self, color1, color2):
        """Verifica si dos colores necesitan ajuste de contraste"""
        try:
            # Ambos colores son oscuros o ambos son claros
            return self._is_dark_color(color1) == self._is_dark_color(color2)
        except:
            return False
    
    def _calculate_smart_hover_color(self, base_color, is_dark_theme):
        """Calcula un color hover inteligente basado en el color base y el tema"""
        try:
            # Para temas oscuros, aclarar el color; para temas claros, oscurecerlo
            if is_dark_theme:
                # En temas oscuros, hacer el hover más brillante
                return self._adjust_color_brightness(base_color, 1.3)
            else:
                # En temas claros, hacer el hover más oscuro
                return self._adjust_color_brightness(base_color, 0.7)
        except:
            return base_color
    
    def _calculate_smart_pressed_color(self, base_color, is_dark_theme):
        """Calcula un color pressed inteligente basado en el color base y el tema"""
        try:
            # Para temas oscuros, oscurecer ligeramente; para temas claros, oscurecer más
            if is_dark_theme:
                return self._adjust_color_brightness(base_color, 0.8)
            else:
                return self._adjust_color_brightness(base_color, 0.6)
        except:
            return base_color

    def _normalize_inline_styles(self, css: str) -> str:
        """Normaliza estilos inline mapeando colores hardcodeados a variables de tema y limpiando residuales."""
        if not css:
            return css
        try:
            import re
            out = css
            # Mapeos comunes de colores a variables dinámicas del tema
            color_map = {
                # Backgrounds
                "#ffffff": "VAR_BG_PRIMARY",
                "white": "VAR_BG_PRIMARY",
                "#f5f7fa": "VAR_BG_SECONDARY",
                "#ecf0f1": "VAR_BG_SECONDARY",
                "#eceff4": "VAR_BG_SECONDARY",
                "#d8dee9": "VAR_BG_TERTIARY",
                "#b8c5d1": "VAR_BG_TERTIARY",
                "#3b4252": "VAR_BG_TERTIARY",
                "#2c3e50": "VAR_PRIMARY_COLOR",
                "#5e81ac": "VAR_PRIMARY_COLOR",
                "#007acc": "VAR_PRIMARY_COLOR",
                "#4caf50": "VAR_ACCENT_SUCCESS",
                "#e74c3c": "VAR_ACCENT_DANGER",
                "#27ae60": "VAR_ACCENT_SUCCESS",
                "#f39c12": "VAR_ACCENT_DANGER",
                "#e67e22": "VAR_ACCENT_DANGER",
                # Texts
                "#000000": "VAR_TEXT_PRIMARY",
                "#000": "VAR_TEXT_PRIMARY",
                "black": "VAR_TEXT_PRIMARY",
                "#333333": "VAR_TEXT_PRIMARY",
                "#495057": "VAR_TEXT_PRIMARY",
                "#444": "VAR_TEXT_PRIMARY",
                "#555": "VAR_TEXT_SECONDARY",
                "#666": "VAR_TEXT_SECONDARY",
                "#666666": "VAR_TEXT_SECONDARY",
                "#6c757d": "VAR_TEXT_SECONDARY",
                "#7f8c8d": "VAR_TEXT_SECONDARY",
                "#868e96": "VAR_TEXT_TERTIARY",
                "#95a5a6": "VAR_TEXT_TERTIARY",
                "#88c0d0": "VAR_TEXT_MUTED",
                "#ffffff_text": "VAR_TEXT_ON_BRAND",
                # Borders
                "#e0e0e0": "VAR_BORDER_PRIMARY",
                "#ddd": "VAR_BORDER_PRIMARY",
                "#dcdfe6": "VAR_BORDER_PRIMARY",
                "#999999": "VAR_BORDER_SECONDARY",
                "#bdc3c7": "VAR_BORDER_SECONDARY",
            }

            # Reemplazar en propiedades específicas
            def replace_prop(prop: str, var_type_hint: str):
                pattern = re.compile(rf"{prop}\\s*:\\s*([^;}}]+)", re.IGNORECASE)
                def repl(m):
                    val = m.group(1).strip().lower()
                    # Caso especial: blanco usado como texto intencional
                    key = val if prop != "color" else (val + "_text" if val in ["#ffffff", "white"] else val)
                    mapped = color_map.get(key) or color_map.get(val)
                    if mapped:
                        return f"{prop}: {mapped}"
                    # Si no se mapea y parece un hex, eliminar para heredar
                    if re.match(r"^#([0-9a-f]{3}|[0-9a-f]{6}|[0-9a-f]{8})$", val):
                        return ""
                    return m.group(0)
                return pattern.sub(repl, out)

            out = replace_prop("background-color", "bg")
            out = replace_prop("background", "bg")
            out = replace_prop("color", "text")
            out = replace_prop("border-color", "border")

            # Manejar propiedad abreviada 'border' reemplazando colores embebidos
            def _replace_border_shorthand(text: str) -> str:
                def repl(m):
                    val = m.group(1).strip()
                    new_val = val
                    # Reemplazar cualquier color hex por variable de borde o mapeo
                    def hex_to_var(hm):
                        k = hm.group(0).lower()
                        return color_map.get(k, "VAR_BORDER_PRIMARY")
                    new_val = re.sub(r"#([0-9a-f]{3}|[0-9a-f]{6}|[0-9a-f]{8})", hex_to_var, new_val, flags=re.IGNORECASE)
                    # Reemplazar colores nombrados presentes en el map
                    for k, v in color_map.items():
                        if k == "#ffffff_text":
                            continue
                        if re.search(re.escape(k), new_val, re.IGNORECASE):
                            new_val = re.sub(re.escape(k), v, new_val, flags=re.IGNORECASE)
                    return f"border: {new_val}"
                return re.sub(r"border\s*:\s*([^;}\n]+)", repl, text, flags=re.IGNORECASE)

            out = _replace_border_shorthand(out)

            # Limpieza de residuales vacíos y dobles separadores
            out = re.sub(r";\s*;", ";", out)
            out = re.sub(r"\{\s*;", "{", out)
            out = re.sub(r";\s*\}", "}", out)
            # Eliminar declaraciones vacías resultantes
            out = re.sub(r"\b(background|background-color|color|border-color)\s*:\s*;", "", out, flags=re.IGNORECASE)
            return out.strip()
        except Exception:
            return css

    def _harmonize_inline_styles(self):
        """Recorre widgets y armoniza estilos inline para respetar el tema actual."""
        try:
            from PyQt6.QtWidgets import QWidget
            for w in self.findChildren(QWidget):
                # Saltar widgets que desean conservar estilos inline específicos
                if bool(w.property("preserve_inline_styles")):
                    continue
                # Garantizar que apliquen reglas dinámicas
                if not w.property("dynamic_css"):
                    w.setProperty("dynamic_css", "true")
                css = w.styleSheet()
                if css:
                    normalized = self._normalize_inline_styles(css)
                    if normalized != css:
                        try:
                            processed = self.apply_dynamic_variables_to_qss(normalized)
                        except Exception:
                            processed = normalized
                        w.setStyleSheet(processed)
        except Exception as e:
            print(f"Aviso: no se pudo armonizar estilos inline: {e}")

    def _apply_card_shadows(self):
        """Aplica sombras sutiles tipo elevación a contenedores 'card'."""
        try:
            is_dark = False
            cfg = getattr(self, 'branding_config', None)
            if isinstance(cfg, dict):
                is_dark = str(cfg.get('theme', 'light')).lower() == 'dark'

            base_alpha = 160 if is_dark else 60
            blur_radius = 22 if is_dark else 18
            offset_y = 3

            shadow_qcolor = QColor(0, 0, 0, base_alpha)

            for frame in self.findChildren(QFrame):
                # Evitar widgets que quieran mantener su estilo inline
                if bool(frame.property('preserve_inline_styles')):
                    continue

                cls = frame.property('class')
                name = frame.objectName() or ""

                is_card_like = False
                if isinstance(cls, str) and ('card' in cls or 'prominent-metric-card' in cls):
                    is_card_like = True
                if ('card' in name) or ('metric' in name):
                    is_card_like = True

                if not is_card_like:
                    continue

                # No duplicar efectos si ya existe uno
                existing_effect = frame.graphicsEffect()
                if isinstance(existing_effect, QGraphicsDropShadowEffect):
                    continue

                effect = QGraphicsDropShadowEffect(self)
                effect.setBlurRadius(blur_radius)
                effect.setOffset(0, offset_y)
                effect.setColor(shadow_qcolor)
                frame.setGraphicsEffect(effect)
        except Exception as e:
            print(f"Aviso: no se pudieron aplicar sombras sutiles: {e}")
    
    def apply_complete_styling(self):
        """Aplica el estilo completo a la aplicación con sistema de plantillas mejorado"""
        print("DEBUG - apply_complete_styling iniciado")
        try:
            # Cargar el archivo QSS base usando rutas compatibles con ejecutables congelados
            qss_path = resource_path('styles/style.qss')
            base_qss = ''
            try:
                with open(qss_path, 'r', encoding='utf-8') as f:
                    base_qss = f.read()
            except Exception:
                dev_qss_path = os.path.join(os.path.dirname(__file__), 'styles', 'style.qss')
                if os.path.exists(dev_qss_path):
                    with open(dev_qss_path, 'r', encoding='utf-8') as f:
                        base_qss = f.read()
            
            # Aplicar variables dinámicas al QSS base
            processed_qss = self.apply_dynamic_variables_to_qss(base_qss)
            
            # Generar CSS dinámico adicional
            dynamic_css = self.generate_dynamic_css()
            
            # Combinar estilos
            complete_css = processed_qss + "\n" + dynamic_css
            
            # Aplicar estilos a la aplicación
            self.setStyleSheet(complete_css)
            
            # Armonizar estilos inline tras aplicar el stylesheet
            if hasattr(self, "_harmonize_inline_styles"):
                self._harmonize_inline_styles()
            # Aplicar elevación sutil a cards
            if hasattr(self, "_apply_card_shadows"):
                self._apply_card_shadows()
            
            print("✓ Estilos aplicados correctamente con variables dinámicas")
            
        except Exception as e:
            print(f"Error aplicando estilos: {e}")
            # Aplicar solo CSS dinámico si falla la carga del QSS
            try:
                dynamic_css = self.generate_dynamic_css()
                self.setStyleSheet(dynamic_css)
                # Armonizar estilos inline también en fallback
                if hasattr(self, "_harmonize_inline_styles"):
                    self._harmonize_inline_styles()
                # Aplicar elevación sutil a cards en fallback
                if hasattr(self, "_apply_card_shadows"):
                    self._apply_card_shadows()
                print("✓ CSS dinámico aplicado como respaldo")
            except Exception as e2:
                print(f"Error aplicando CSS dinámico: {e2}")
    
    def apply_branding_changes(self, branding_config):
        """Aplica cambios de branding personalizados"""
        try:
            # Guardar configuración de branding
            self.branding_config = branding_config
            
            # Guardar configuración en la base de datos para persistencia
            try:
                import json
                branding_json = json.dumps(branding_config, indent=2)
                self.db_manager.actualizar_configuracion('branding_config', branding_json)
                print("✓ Configuración de branding guardada en la base de datos")
            except Exception as e:
                print(f"⚠️ Error guardando configuración de branding: {e}")
            
            # Aplicar estilos completos con la nueva configuración
            self.apply_complete_styling()
            
            # MEJORA: Actualizar gráficos del dashboard de reportes
            self._update_charts_branding(branding_config)
            
            # NUEVO: Actualizar header del gimnasio
            if hasattr(self, 'update_gym_header'):
                self.update_gym_header()
            
            print("✓ Cambios de branding aplicados correctamente")
            
        except Exception as e:
            print(f"Error aplicando cambios de branding: {e}")
    
    def _update_charts_branding(self, branding_config):
        """Actualiza los colores de todos los gráficos en la aplicación"""
        try:
            # Actualizar gráficos en la pestaña de reportes
            if hasattr(self, 'tabs') and 'reportes' in self.tabs:
                reports_tab = self.tabs['reportes']
                
                # Buscar y actualizar todos los widgets de gráficos
                chart_widgets = self._find_chart_widgets(reports_tab)
                print(f"Encontrados {len(chart_widgets)} widgets de gráficos")
                
                for chart_widget in chart_widgets:
                    if hasattr(chart_widget, 'update_colors_from_branding'):
                        print(f"Actualizando colores del gráfico: {chart_widget.objectName()}")
                        chart_widget.update_colors_from_branding(branding_config)
                        
                        # Forzar redibujado inmediato de forma segura
                        if hasattr(chart_widget, 'canvas') and chart_widget.canvas is not None:
                            try:
                                chart_widget.canvas.draw_idle()
                            except RuntimeError as re:
                                if 'has been deleted' in str(re):
                                    pass
                        
                # Forzar actualización completa de los gráficos
                if hasattr(reports_tab, 'update_charts'):
                    reports_tab.update_charts()
                elif hasattr(reports_tab, 'refresh_charts'):
                    reports_tab.refresh_charts()
                    
            # También buscar gráficos en otras pestañas
            for tab_name, tab_widget in self.tabs.items():
                if tab_name != 'reportes':
                    chart_widgets = self._find_chart_widgets(tab_widget)
                    for chart_widget in chart_widgets:
                        if hasattr(chart_widget, 'update_colors_from_branding'):
                            chart_widget.update_colors_from_branding(branding_config)
                            if hasattr(chart_widget, 'canvas') and chart_widget.canvas is not None:
                                try:
                                    chart_widget.canvas.draw_idle()
                                except RuntimeError as re:
                                    if 'has been deleted' in str(re):
                                        pass
                    
            print("✓ Gráficos actualizados con nueva configuración de branding")
            
        except Exception as e:
            print(f"Error actualizando gráficos: {e}")
            import traceback
            traceback.print_exc()
    
    def _find_chart_widgets(self, parent_widget):
        """Encuentra recursivamente todos los widgets de gráficos en un widget padre"""
        chart_widgets = []
        try:
            from widgets.chart_widget import MplChartWidget
            
            # Buscar en el widget actual
            if isinstance(parent_widget, MplChartWidget):
                chart_widgets.append(parent_widget)
            
            # Buscar recursivamente en widgets hijos
            if hasattr(parent_widget, 'children'):
                for child in parent_widget.children():
                    if hasattr(child, 'children'):  # Es un widget
                        chart_widgets.extend(self._find_chart_widgets(child))
                        
        except Exception as e:
            print(f"Error buscando widgets de gráficos: {e}")
            
        return chart_widgets
    
    def create_floating_logout_button(self):
        """Crea un botón flotante de cerrar sesión con posicionamiento absoluto"""
        try:
            # Crear botón flotante
            self.logout_button = QPushButton("🚪 Cerrar sesión", self)
            self.logout_button.setToolTip("Cerrar sesión y volver al login")
            self.logout_button.clicked.connect(self.logout)
            self.logout_button.setObjectName("floating_logout_button")
            
            # Estilo del botón flotante (compatible con Qt)
            self.logout_button.setStyleSheet("""
                QPushButton#floating_logout_button {
                    background-color: #e74c3c;
                    color: white;
                    border: 2px solid #c0392b;
                    padding: 10px 16px;
                    border-radius: 8px;
                    font-weight: bold;
                    font-size: 12px;
                }
                QPushButton#floating_logout_button:hover {
                    background-color: #c0392b;
                    border: 2px solid #a93226;
                }
                QPushButton#floating_logout_button:pressed {
                    background-color: #a93226;
                    border: 2px solid #922b21;
                }
            """)
            
            # Posicionar el botón en la esquina superior derecha
            self.position_floating_button()
            
            # Asegurar que el botón esté siempre visible
            self.logout_button.raise_()
            self.logout_button.show()
            
            logging.info("Botón flotante de logout creado exitosamente")
            
        except Exception as e:
            logging.error(f"Error creando botón flotante: {e}")
    
    def position_floating_button(self):
        """Posiciona el botón flotante en la esquina superior derecha"""
        try:
            # Obtener dimensiones de la ventana
            window_width = self.width()
            button_width = 140  # Ancho aproximado del botón
            button_height = 35  # Alto aproximado del botón
            
            # Posicionar en esquina superior derecha con margen
            x = window_width - button_width - 20  # 20px de margen desde el borde
            y = 10  # 10px desde la parte superior
            
            self.logout_button.setGeometry(x, y, button_width, button_height)
            
        except Exception as e:
            logging.error(f"Error posicionando botón flotante: {e}")
    
    def resizeEvent(self, event):
        """Reposiciona el botón flotante cuando se redimensiona la ventana"""
        super().resizeEvent(event)
        if hasattr(self, 'logout_button') and self.logout_button:
            self.position_floating_button()
    
    def setup_menu_bar(self):
        """Configura la barra de menús de la aplicación"""
        try:
            # Menú removido - botón de cerrar sesión ahora es flotante
            logging.info("Configuración de menú completada")
            
        except Exception as e:
            logging.error(f"Error configurando menú: {e}")

    def _ask_yes_no(self, title: str, text: str) -> bool:
        """Muestra un diálogo de confirmación robusto, modal y siempre visible (solo para logout)."""
        m = QMessageBox(self)
        m.setIcon(QMessageBox.Icon.Question)
        m.setWindowTitle(title)
        m.setText(text)
        m.setStandardButtons(QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No)
        m.setDefaultButton(QMessageBox.StandardButton.No)
        m.setWindowModality(Qt.WindowModality.ApplicationModal)
        m.setWindowFlag(Qt.WindowType.WindowStaysOnTopHint, True)
        m.activateWindow()
        m.raise_()
        ret = m.exec()
        try:
            sb = QMessageBox.StandardButton(ret)
            return sb == QMessageBox.StandardButton.Yes
        except Exception:
            return ret == int(QMessageBox.StandardButton.Yes)
    
    def update_monthly_hours(self):
        """Actualiza el contador de horas mensuales en tiempo real de forma asíncrona"""
        try:
            # Solo mostrar para profesores
            if self.user_role == 'profesor' and self.logged_in_user:
                try:
                    usuario_id_para_buscar = safe_get(self.logged_in_user, 'usuario_id') or safe_get(self.logged_in_user, 'id')
                    info = self.db_manager.obtener_profesor_por_usuario_id(usuario_id_para_buscar)
                    profesor_id = info.profesor_id if info and hasattr(info, 'profesor_id') else None
                except Exception:
                    profesor_id = None

                if profesor_id:
                    # Evitar ejecuciones concurrentes
                    if self._hours_worker_running:
                        return
                    self._hours_worker_running = True
                    import threading
                    threading.Thread(target=self._load_monthly_hours_worker, args=(profesor_id,), daemon=True).start()
                else:
                    self.monthly_hours_label.setVisible(False)
            else:
                self.monthly_hours_label.setVisible(False)
        except Exception as e:
            logging.error(f"Error programando actualización de horas mensuales: {e}")
            self.monthly_hours_label.setText("⏰ Horas del mes: Error")

    def _load_monthly_hours_worker(self, profesor_id: int):
        """Worker en segundo plano para obtener horas del mes actual."""
        try:
            resultado = self.db_manager.obtener_horas_mes_actual_profesor(profesor_id)
            if resultado.get('success'):
                total_minutos = int(resultado.get('total_minutos', 0) or 0)
                horas_display = int(resultado.get('horas_display', total_minutos // 60))
                minutos_display = int(resultado.get('minutos_display', total_minutos % 60))
                try:
                    self.monthly_hours_ready.emit(horas_display, minutos_display, True)
                except Exception:
                    # Fallback: actualizar de forma segura en el hilo principal
                    QTimer.singleShot(0, lambda: self._apply_monthly_hours_result(horas_display, minutos_display, True))
            else:
                try:
                    self.monthly_hours_ready.emit(0, 0, False)
                except Exception:
                    QTimer.singleShot(0, lambda: self._apply_monthly_hours_result(0, 0, False))
        except Exception as e:
            logging.error(f"Error obteniendo horas mensuales en worker: {e}")
            try:
                self.monthly_hours_ready.emit(0, 0, False)
            except Exception:
                QTimer.singleShot(0, lambda: self._apply_monthly_hours_result(0, 0, False))
        finally:
            self._hours_worker_running = False

    def _apply_monthly_hours_result(self, horas_display: int, minutos_display: int, success: bool):
        """Aplica el resultado en la UI de forma segura."""
        try:
            if success:
                self.monthly_hours_label.setText(f"⏰ Mes actual: {horas_display}h {minutos_display}m")
            else:
                self.monthly_hours_label.setText("⏰ Horas del mes: Error")
            self.monthly_hours_label.setVisible(True)
        except Exception as e:
            logging.error(f"Error aplicando resultado de horas mensuales: {e}")

    def _install_qthread_guard(self):
        """Instala un guardia global para registrar QThreads y asegurar cierre limpio."""
        try:
            if MainWindow._QTHREAD_GUARD_INSTALLED:
                return
            MainWindow._QTHREAD_GUARD_INSTALLED = True
            try:
                MainWindow._orig_qthread_start = QThread.start
            except Exception:
                MainWindow._orig_qthread_start = None

            def _guarded_start(thread_self, *args, **kwargs):
                try:
                    MainWindow._QTHREAD_REGISTRY.add(thread_self)
                except Exception:
                    pass
                if MainWindow._orig_qthread_start:
                    return MainWindow._orig_qthread_start(thread_self, *args, **kwargs)
                else:
                    return QThread.start(thread_self, *args, **kwargs)

            try:
                QThread.start = _guarded_start
            except Exception:
                pass
        except Exception:
            pass

    def _shutdown_qthreads_guarded(self):
        """Cierra de forma segura todos los QThreads registrados para evitar crashes."""
        try:
            for thr in list(MainWindow._QTHREAD_REGISTRY):
                try:
                    if thr and thr.isRunning():
                        try:
                            thr.requestInterruption()
                        except Exception:
                            pass
                        try:
                            thr.quit()
                        except Exception:
                            pass
                        try:
                            thr.wait(2000)
                        except Exception:
                            pass
                except Exception:
                    pass
        except Exception:
            pass
    
    def logout(self):
        """Cierra sesión y vuelve al login dialog"""
        import sys
        
        logging.info("Iniciando proceso de logout")
        logging.info(f"Usuario actual: {getattr(self, 'logged_in_user', 'N/A')}")
        logging.info(f"Rol actual: {getattr(self, 'logged_in_role', 'N/A')}")
        try:
            # Evitar fallo si stdout es None en ejecutable
            if getattr(sys, 'stdout', None) and hasattr(sys.stdout, 'flush'):
                sys.stdout.flush()
        except Exception:
            pass
        
        try:
            # Confirmar cierre de sesión
            if not self._ask_yes_no("Cerrar Sesión", "¿Está seguro de que desea cerrar sesión?"):
                return
            
            # Marcar que es un logout (no cierre de aplicación)
            self.is_logout = True
            
            # Cerrar automáticamente la sesión del usuario logueado (si es profesor)
            if hasattr(self, 'logged_in_user') and self.logged_in_user:
                # Verificar si el usuario logueado es un profesor
                try:
                    # Usar usuario_id en lugar de id para buscar el profesor (aceptando dict u objeto Usuario)
                    usuario_id_para_buscar = safe_get(self.logged_in_user, 'usuario_id') or safe_get(self.logged_in_user, 'id')
                    logging.info(f"Verificando si el usuario {usuario_id_para_buscar} es profesor para gestionar su sesión de trabajo")
                    profesor_info = self.db_manager.obtener_profesor_por_usuario_id(usuario_id_para_buscar)
                    if profesor_info and hasattr(profesor_info, 'profesor_id'):
                        profesor_id = profesor_info.profesor_id
                        profesor_nombre = f"{safe_get(self.logged_in_user,'nombre','')} {safe_get(self.logged_in_user,'apellido','')}".strip()
                        logging.info(f"Cerrando sesión del usuario logueado: {profesor_nombre} (ID: {safe_get(self.logged_in_user,'id')}, Profesor ID: {profesor_id})")
                        
                        # Verificar si tiene sesión activa
                        duracion_info = self.db_manager.obtener_duracion_sesion_actual_profesor(profesor_id)
                        if duracion_info.get('success') and duracion_info.get('tiene_sesion_activa'):
                            logging.info("Sesión activa encontrada, finalizando...")
                            
                            resultado = self.db_manager.finalizar_sesion_trabajo_profesor(profesor_id)
                            if resultado.get('success'):
                                # Obtener datos de la sesión finalizada
                                datos_sesion = resultado.get('datos', {})
                                minutos_sesion = datos_sesion.get('minutos_totales', 0) or 0
                                horas_sesion = minutos_sesion / 60.0 if minutos_sesion else 0
                                
                                logging.info(f"Sesión cerrada exitosamente. Duración: {minutos_sesion:.2f} minutos ({horas_sesion:.4f} horas)")
                                
                                # Obtener horas actualizadas del mes
                                try:
                                    horas_mes = self.db_manager.obtener_horas_mes_actual_profesor(profesor_id)
                                    if horas_mes.get('success'):
                                        datos_mes = horas_mes.get('datos', {})
                                        total_horas = datos_mes.get('horas_display', 0)
                                        total_minutos_display = datos_mes.get('minutos_display', 0)
                                        dias_trabajados = datos_mes.get('dias_trabajados', 0)
                                        
                                        logging.info(f"Horas del mes actualizadas: Total {total_horas}h {total_minutos_display}min, Días trabajados {dias_trabajados}")
                                    else:
                                        logging.warning(f"No se pudieron obtener las horas del mes: {horas_mes.get('mensaje', 'Error desconocido')}")
                                except Exception as e:
                                    logging.error(f"Error obteniendo horas del mes: {e}")
                                
                                logging.info(f"Sesión cerrada automáticamente en logout para {profesor_nombre}: {horas_sesion:.4f} horas")
                            else:
                                mensaje_error = resultado.get('mensaje', 'Error desconocido')
                                logging.warning(f"No se pudo cerrar la sesión de {profesor_nombre}: {mensaje_error}")
                        else:
                            logging.info("No hay sesión activa para cerrar")
                    else:
                        logging.info("Usuario logueado no es profesor, no hay sesión que cerrar")
                except Exception as e:
                    logging.error(f"Error verificando sesión del usuario logueado: {e}")
            else:
                logging.warning("No hay usuario logueado identificado")
            
            # También cerrar cualquier otra sesión abierta (por seguridad)
            sesiones_abiertas = self.db_manager.verificar_sesiones_abiertas()
            
            if sesiones_abiertas:
                logging.info(f"Cerrando automáticamente {len(sesiones_abiertas)} sesiones adicionales de trabajo")
                
                # Cerrar todas las sesiones abiertas automáticamente
                for sesion in sesiones_abiertas:
                    try:
                        profesor_id = sesion['profesor_id']
                        profesor_nombre = sesion['profesor_nombre']
                        horas_transcurridas = float(sesion.get('horas_transcurridas', 0))
                        
                        # Información de cierre de sesión (sin salida de depuración)
                        
                        resultado = self.db_manager.finalizar_sesion_trabajo_profesor(profesor_id)
                        if resultado.get('success'):
                            # Obtener datos de la sesión finalizada
                            datos_sesion = resultado.get('datos', {})
                            minutos_sesion = datos_sesion.get('minutos_totales', 0) or 0
                            horas_sesion = minutos_sesion / 60.0 if minutos_sesion else 0
                            
                            logging.info("Sesión cerrada exitosamente")
                            logging.info(f"Duración exacta: {minutos_sesion:.2f} minutos ({horas_sesion:.4f} horas)")
                            
                            # Obtener horas actualizadas del mes
                            try:
                                horas_mes = self.db_manager.obtener_horas_mes_actual_profesor(profesor_id)
                                if horas_mes.get('success'):
                                    datos_mes = horas_mes.get('datos', {})
                                    total_horas = datos_mes.get('horas_display', 0)
                                    total_minutos_display = datos_mes.get('minutos_display', 0)
                                    dias_trabajados = datos_mes.get('dias_trabajados', 0)
                                    
                                    logging.info(f"Horas del mes actualizadas: Total {total_horas}h {total_minutos_display}min, Días trabajados {dias_trabajados}")
                                else:
                                    logging.warning(f"No se pudieron obtener las horas del mes: {horas_mes.get('mensaje', 'Error desconocido')}")
                            except Exception as e:
                                logging.error(f"Error obteniendo horas del mes: {e}")
                            
                            logging.info(f"Sesión cerrada automáticamente para {profesor_nombre}: {horas_sesion:.4f} horas")
                        else:
                            mensaje_error = resultado.get('mensaje', 'Error desconocido')
                            logging.warning(f"No se pudo cerrar la sesión de {profesor_nombre}: {mensaje_error}")
                    except Exception as e:
                        logging.error(f"Error cerrando sesión de {profesor_nombre}: {e}")

            
            # Ocultar la ventana actual
            self.hide()
            
            # Crear nueva instancia del login dialog
            from widgets.login_dialog import LoginDialog
            login_dialog = LoginDialog(self.db_manager)
            
            if login_dialog.exec():
                # Crear nueva ventana principal con el nuevo usuario
                new_window = MainWindow(login_dialog.logged_in_role, self.db_manager, login_dialog.logged_in_user)
                new_window.showMaximized()
                # Cerrar la ventana actual
                self.close()
            else:
                # Si se cancela el login, cerrar la aplicación
                self.close()
                QApplication.quit()
                
        except Exception as e:
            logging.error(f"Error en logout: {e}")
            QMessageBox.critical(self, "Error", f"Error al cerrar sesión: {e}")
    
    def exit_application(self):
        """Cierra completamente la aplicación"""
        try:
            # Usar la lógica existente del closeEvent
            self.close()
        except Exception as e:
            logging.error(f"Error al salir de la aplicación: {e}")
            QApplication.quit()

    def closeEvent(self, event: QCloseEvent):
        """Maneja el evento de cierre de la ventana"""
        try:
            # Si es un logout, no mostrar diálogos de confirmación
            if getattr(self, 'is_logout', False):
                try:
                    terminate_tunnel_processes()
                except Exception:
                    pass
                # Limpiar recursos modernos; referencias legacy eliminadas
                event.accept()
                return
                
            # Verificar sesiones abiertas antes de cerrar
            sesiones_abiertas = self.db_manager.verificar_sesiones_abiertas()
            
            if sesiones_abiertas:
                # Mostrar resumen de sesiones que se van a cerrar
                mensaje = "Se cerrarán las siguientes sesiones de trabajo:\n\n"
                for sesion in sesiones_abiertas:
                    horas_transcurridas = float(sesion.get('horas_transcurridas', 0))
                    mensaje += f"• {sesion['profesor_nombre']}: {horas_transcurridas:.1f} horas\n"
                
                mensaje += "\n¿Desea continuar?"
                
                respuesta = QMessageBox.question(
                    self, "Cerrar Sesiones de Trabajo",
                    mensaje,
                    QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No
                )
                
                if respuesta == QMessageBox.StandardButton.No:
                    event.ignore()
                    return
                
                # Limpieza: eliminar salida de depuración al cerrar sesiones
                
                # Cerrar todas las sesiones abiertas
                for sesion in sesiones_abiertas:
                    try:
                        profesor_id = sesion['profesor_id']
                        profesor_nombre = sesion['profesor_nombre']
                        horas_transcurridas = float(sesion.get('horas_transcurridas', 0))
                        
                        logging.info(f"Cerrando sesión de {profesor_nombre} (ID: {profesor_id}). Tiempo transcurrido: {horas_transcurridas:.2f} horas")
                        
                        # El método finalizar_sesion_trabajo_profesor ahora retorna dict
                        resultado = self.db_manager.finalizar_sesion_trabajo_profesor(profesor_id)
                        if resultado.get('success'):
                            # Obtener datos de la sesión finalizada
                            datos_sesion = resultado.get('datos', {})
                            minutos_sesion = datos_sesion.get('minutos_totales', 0) or 0
                            horas_sesion = minutos_sesion / 60.0 if minutos_sesion else 0
                            
                            logging.info(f"Sesión cerrada exitosamente. Duración: {minutos_sesion:.2f} minutos ({horas_sesion:.4f} horas)")
                            
                            # Obtener horas actualizadas del mes
                            try:
                                horas_mes = self.db_manager.obtener_horas_mes_actual_profesor(profesor_id)
                                if horas_mes.get('success'):
                                    # Usar claves actuales del backend
                                    total_minutos = int(horas_mes.get('total_minutos', 0) or 0)
                                    total_horas = int(horas_mes.get('horas_display', total_minutos // 60))
                                    total_minutos_display = int(horas_mes.get('minutos_display', total_minutos % 60))
                                    dias_trabajados = int(horas_mes.get('total_dias_trabajados', 0) or 0)
                                    
                                    logging.info(f"Horas del mes actualizadas: Total {total_horas}h {total_minutos_display}min, Días trabajados {dias_trabajados}")
                                else:
                                    logging.warning(f"No se pudieron obtener las horas del mes: {horas_mes.get('mensaje', 'Error desconocido')}")
                            except Exception as e:
                                logging.error(f"Error obteniendo horas del mes: {e}")
                            
                            logging.info(f"Sesión cerrada para {profesor_nombre}: {horas_sesion:.4f} horas")
                        else:
                            mensaje_error = resultado.get('mensaje', 'Error desconocido')
                            logging.warning(f"No se pudo cerrar la sesión de {profesor_nombre}: {mensaje_error}")
                    except Exception as e:
                        logging.error(f"Error cerrando sesión de {profesor_nombre}: {e}")

            else:
                # No hay sesiones abiertas, confirmar cierre normal
                if QMessageBox.question(self, 'Confirmar Salida', "¿Está seguro de que desea cerrar el programa?",
                                        QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
                                        QMessageBox.StandardButton.No) != QMessageBox.StandardButton.Yes:
                    event.ignore()
                    return
            
            # Continuar con el cierre normal
            logging.info("Cerrando la aplicación.")
            # Cerrar túnel público (ssh.exe) si estuviera activo
            try:
                terminate_tunnel_processes()
            except Exception:
                pass
            # Referencias legacy a download_sync_worker/proxy_watchdog removidas
            event.accept()
            # Intentar cierre limpio de todos los QThreads activos
            try:
                self._shutdown_qthreads_guarded()
            except Exception:
                pass
            
        except Exception as e:
            logging.error(f"Error en closeEvent: {e}")
            # Permitir cierre aunque haya error
            if QMessageBox.question(self, 'Error al Cerrar', 
                                    f"Ocurrió un error al cerrar la aplicación: {e}\n\n¿Desea forzar el cierre?",
                                    QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
                                    QMessageBox.StandardButton.No) == QMessageBox.StandardButton.Yes:
                event.accept()
            else:
                event.ignore()

def _ensure_windows_app_id():
    """En Windows, establece un AppUserModelID explícito para que la barra de tareas
    use el icono de la aplicación correctamente y agrupe las ventanas.
    Debe llamarse antes de crear cualquier ventana.
    """
    try:
        if os.name == "nt":
            import ctypes
            app_id = os.getenv("APP_USER_MODEL_ID", "GymManagementSystem.App")
            try:
                ctypes.windll.shell32.SetCurrentProcessExplicitAppUserModelID(app_id)
            except Exception:
                # Algunas versiones/registros pueden no permitir establecer AppID
                pass
    except Exception:
        pass

def terminate_ssh_processes(timeout: float = 3.0):
    """Termina de forma segura todos los procesos 'ssh.exe' activos en Windows.
    Primero intenta terminar, espera y fuerza kill si siguen vivos.
    """
    try:
        import psutil
    except Exception:
        return

    targets = []
    try:
        for p in psutil.process_iter(['name']):
            name = str(p.info.get('name', '')).lower()
            if name == 'ssh.exe':
                targets.append(p)
    except Exception:
        pass

    for p in targets:
        try:
            p.terminate()
        except Exception:
            pass

    try:
        psutil.wait_procs(targets, timeout=timeout)
    except Exception:
        pass

    for p in targets:
        try:
            if p.is_running():
                p.kill()
        except Exception:
            pass

def main():
    # Instalar handler de mensajes Qt temprano para capturar errores de plugins
    try:
        from PyQt6.QtCore import qInstallMessageHandler, QtMsgType, QCoreApplication  # type: ignore
        def _default_qt_handler(msg_type, context, message):
            try:
                if msg_type == QtMsgType.QtDebugMsg:
                    logging.debug(message)
                elif msg_type == QtMsgType.QtInfoMsg:
                    logging.info(message)
                elif msg_type == QtMsgType.QtWarningMsg:
                    logging.warning(message)
                elif msg_type in (QtMsgType.QtCriticalMsg, QtMsgType.QtFatalMsg):
                    logging.error(message)
                else:
                    logging.info(message)
            except Exception:
                pass
        try:
            handler = _qt_message_handler  # type: ignore
        except Exception:
            handler = _default_qt_handler
        try:
            qInstallMessageHandler(handler)
        except Exception:
            pass
    except Exception:
        pass

    # Asegurar AppUserModelID en Windows antes de crear la aplicación
    _ensure_windows_app_id()

    # Proteger y forzar resolución de plugins/DLL de Qt en ejecutables (Nuitka/PyInstaller)
    try:
        exe_dir = os.path.dirname(sys.executable) if getattr(sys, "frozen", False) else os.path.dirname(os.path.abspath(__file__))
        meipass = getattr(sys, "_MEIPASS", None)
        plugin_root_candidates = [
            os.path.join(exe_dir, "PyQt6", "Qt6", "plugins"),
            os.path.join(exe_dir, "Qt6", "plugins"),
            os.path.join(exe_dir, "plugins"),
            os.path.join(exe_dir, "lib", "PyQt6", "Qt6", "plugins"),
            os.path.join(exe_dir, "lib", "Qt6", "plugins"),
        ]
        if meipass:
            plugin_root_candidates.extend([
                os.path.join(meipass, "PyQt6", "Qt6", "plugins"),
                os.path.join(meipass, "Qt6", "plugins"),
                os.path.join(meipass, "plugins"),
            ])
        def _valid_dir(path):
            try:
                return os.path.isdir(path)
            except Exception:
                return False
        platform_candidates = []
        styles_candidates = []
        imageformats_candidates = []
        iconengines_candidates = []
        for root in plugin_root_candidates:
            if _valid_dir(root):
                platform_candidates.append(os.path.join(root, "platforms"))
                styles_candidates.append(os.path.join(root, "styles"))
                imageformats_candidates.append(os.path.join(root, "imageformats"))
                iconengines_candidates.append(os.path.join(root, "iconengines"))
        # Candidatos explícitos
        explicit = [
            os.path.join(exe_dir, "PyQt6", "Qt6", "plugins"),
            os.path.join(exe_dir, "Qt6", "plugins"),
            os.path.join(exe_dir, "plugins"),
        ]
        for base in explicit:
            platform_candidates.append(os.path.join(base, "platforms"))
            styles_candidates.append(os.path.join(base, "styles"))
            imageformats_candidates.append(os.path.join(base, "imageformats"))
            iconengines_candidates.append(os.path.join(base, "iconengines"))
        if meipass:
            for base in [
                os.path.join(meipass, "PyQt6", "Qt6", "plugins"),
                os.path.join(meipass, "Qt6", "plugins"),
                os.path.join(meipass, "plugins"),
            ]:
                platform_candidates.append(os.path.join(base, "platforms"))
                styles_candidates.append(os.path.join(base, "styles"))
                imageformats_candidates.append(os.path.join(base, "imageformats"))
                iconengines_candidates.append(os.path.join(base, "iconengines"))

        selected_root = None
        selected_platform = None
        selected_styles = None
        selected_imageformats = None
        selected_iconengines = None
        # Elegir primer ruta válida que contenga plugin de plataforma qwindows
        for plat in platform_candidates:
            try:
                if _valid_dir(plat):
                    has_qwindows = any(fn.lower().startswith("qwindows") for fn in os.listdir(plat))
                    if has_qwindows:
                        selected_platform = plat
                        selected_root = os.path.dirname(plat)
                        break
            except Exception:
                continue
        # Detectar otros plugins útiles
        for sty in styles_candidates:
            if _valid_dir(sty):
                try:
                    has_style = any(fn.lower().startswith("qwindows") or fn.lower().endswith("style.dll") for fn in os.listdir(sty))
                except Exception:
                    has_style = True
                if has_style:
                    selected_styles = sty
                    break
        for img in imageformats_candidates:
            if _valid_dir(img):
                selected_imageformats = img
                break
        for ico in iconengines_candidates:
            if _valid_dir(ico):
                selected_iconengines = ico
                break

        # Forzar variables de entorno y rutas dinámicas
        try:
            from PyQt6.QtCore import QCoreApplication  # type: ignore
        except Exception:
            QCoreApplication = None  # type: ignore
        if selected_root:
            os.environ["QT_PLUGIN_PATH"] = selected_root
            try:
                if QCoreApplication:
                    QCoreApplication.addLibraryPath(selected_root)
            except Exception:
                pass
        if selected_platform:
            os.environ["QT_QPA_PLATFORM_PLUGIN_PATH"] = selected_platform
            try:
                if QCoreApplication:
                    QCoreApplication.addLibraryPath(selected_platform)
            except Exception:
                pass
        for extra in [selected_styles, selected_imageformats, selected_iconengines]:
            if extra:
                try:
                    if QCoreApplication:
                        QCoreApplication.addLibraryPath(extra)
                except Exception:
                    pass
        # Inyectar rutas de DLL Qt en PATH para resolver dependencias
        bin_candidates = [
            os.path.join(exe_dir, "PyQt6", "Qt6", "bin"),
            os.path.join(exe_dir, "Qt6", "bin"),
            os.path.join(exe_dir, "bin"),
        ]
        if meipass:
            bin_candidates.extend([
                os.path.join(meipass, "PyQt6", "Qt6", "bin"),
                os.path.join(meipass, "Qt6", "bin"),
                os.path.join(meipass, "bin"),
            ])
        current_path = os.environ.get("PATH", "")
        for b in bin_candidates:
            try:
                if _valid_dir(b) and b not in current_path:
                    current_path = b + os.pathsep + current_path
            except Exception:
                pass
        os.environ["PATH"] = current_path
        # Activar depuración de plugins para obtener logs útiles durante el arranque
        os.environ["QT_DEBUG_PLUGINS"] = os.environ.get("QT_DEBUG_PLUGINS", "1")
    except Exception:
        pass

    # Crear QApplication con protección ante fallos de plugins
    try:
        app = QApplication(sys.argv)
    except Exception as e:
        # Mostrar mensaje nativo en Windows si no se puede iniciar Qt
        try:
            if os.name == "nt":
                import ctypes
                msg = (
                    "No se pudo inicializar la interfaz Qt.\n\n"
                    "Verifica que el ejecutable contenga los plugins de Qt (platforms/styles) y que no esté bloqueado por antivirus.\n"
                    "Si el problema persiste, reinstala o reconstruye con los plugins incluidos.\n\n"
                    f"Detalle: {e}"
                )
                ctypes.windll.user32.MessageBoxW(None, msg, "Error de inicio", 0x10)
        except Exception:
            pass
        # Registrar y salir con código de error
        try:
            logging.error(f"Fallo al crear QApplication: {e}")
        except Exception:
            pass
        sys.exit(1)
    # Evitar que la app termine si se cierra el último diálogo (p.ej., Login)
    try:
        app.setQuitOnLastWindowClosed(False)
    except Exception:
        pass
    
    # --- APLICACIÓN DEL ESTILO PERSONALIZADO ---
    # Esto asegura que los pequeños íconos (flechas) en los SpinBox y ComboBox
    # se dibujen correctamente sobre el fondo oscuro.
    app.setStyle(CustomProxyStyle())
    # --- CARGA TEMPRANA DE QSS GLOBAL ---
    # Aplicar QSS al inicio para que todas las ventanas iniciales respeten el estilo
    try:
        qss_candidates = [
            'styles/style.qss',
            'assets/style.qss',
        ]
        qss_content = ''
        for candidate in qss_candidates:
            try:
                qss_path = resource_path(candidate)
                if os.path.exists(qss_path):
                    with open(qss_path, 'r', encoding='utf-8') as f:
                        qss_content = f.read()
                    break
            except Exception:
                pass
        if qss_content:
            app.setStyleSheet(qss_content)
            logging.info("QSS global aplicado tempranamente")
    except Exception as e:
        logging.debug(f"No se pudo aplicar QSS global temprano: {e}")

    # Establecer icono de la aplicación a nivel global para la barra de tareas
    try:
        for candidate in ["assets/gym_logo.ico", "assets/gym_icon.ico", "assets/icon.png"]:
            icon_path = resource_path(candidate)
            if os.path.exists(icon_path):
                app.setWindowIcon(QIcon(icon_path))
                break
    except Exception:
        pass

    # Eliminado: aseguramiento y watchdog del proxy local legacy

    # Aplicar timeout de conexión desde configuración antes de crear DB Manager
    try:
        from cdbconfig import _get_current_params  # type: ignore
    except Exception:
        _get_current_params = None  # type: ignore
    try:
        cfg_tmp = (_get_current_params() if _get_current_params else {}) or {}
        ct_tmp = int(cfg_tmp.get('connect_timeout', 10))
        os.environ['PGCONNECT_TIMEOUT'] = str(ct_tmp)
    except Exception:
        pass

    # Verificar e instalar PostgreSQL 17 antes del login (bloqueante)
    try:
        from utils_modules.prerequisites import ensure_prerequisites
        from device_id import get_device_id
        device = get_device_id()
        logging.info("Verificando/instalando PostgreSQL 17 y creando base local…")
        prereq_result = ensure_prerequisites(device)
        pg_ok = bool(prereq_result.get("postgresql", {}).get("installed"))
        db_created = bool(prereq_result.get("postgresql", {}).get("db_created"))
        logging.info(
            f"Prerequisitos: PostgreSQL={'OK' if pg_ok else 'Falta'}, DB={'creada' if db_created else 'existente/no creada'}"
        )
        # Asegurar tareas programadas clave en primer arranque
        try:
            from utils_modules.prerequisites import ensure_scheduled_tasks
            tasks_res = ensure_scheduled_tasks(device)
            logging.info(f"Tareas programadas aseguradas: {tasks_res}")
        except Exception as te:
            logging.warning(f"No se pudieron asegurar tareas programadas: {te}")
    except Exception as e:
        logging.warning(f"No se pudo completar prerequisitos antes del login: {e}")

    db_manager_for_login = DatabaseManager()
    # No iniciar servidor web local desde la app de escritorio
    public_url = None
    try:
        # Importaciones seguras para utilidades relacionadas (sin arrancar servidor)
        from webapp.server import set_public_tunnel_reconnect_callback  # type: ignore
        # Resolver URL pública desde configuración/ENV
        from utils import get_webapp_base_url
        public_url = get_webapp_base_url()
        if public_url:
            logging.info(f"Acceso público configurado: {public_url}")
    except Exception:
        public_url = None
    login_dialog = LoginDialog(db_manager_for_login)
    # Propagar URL pública al diálogo de login sin cambiar su firma
    try:
        login_dialog.web_base = public_url
    except Exception:
        pass
    
    try:
        from PyQt6.QtWidgets import QDialog, QMessageBox
        # En binarios compilados se ha observado cierre prematuro del diálogo.
        # Reintentamos el login una vez antes de cerrar la aplicación.
        login_attempts = 0
        while True:
            result = login_dialog.exec()
            if result == QDialog.DialogCode.Accepted:
                break
            login_attempts += 1
            logging.warning("Login rechazado/cancelado (intento %s).", login_attempts)
            # Si el diálogo indicó cierre explícito, salir sin mostrar confirmación adicional
            try:
                if getattr(login_dialog, "_closing_confirmed", False):
                    logging.info("Login cancelado por el usuario desde el diálogo. Cerrando aplicación.")
                    db_manager_for_login.close_connections()
                    try:
                        wd = getattr(app, 'global_proxy_watchdog', None)
                        if wd:
                            wd.stop()
                    except Exception:
                        pass
                    try:
                        tmr = getattr(app, '_global_proxy_watchdog_timer', None)
                        if tmr:
                            tmr.stop()
                    except Exception:
                        pass
                    try:
                        terminate_tunnel_processes()
                    except Exception:
                        pass
                    try:
                        terminate_ssh_processes()
                    except Exception:
                        pass
                    sys.exit(0)
            except Exception:
                pass
            # Un reintento automático para cubrir cierre inesperado en el .exe
            if login_attempts >= 1:
                reply = QMessageBox.question(
                    None,
                    "Login requerido",
                    "El inicio de sesión fue cancelado. ¿Desea cerrar la aplicación?",
                    QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
                    QMessageBox.StandardButton.No,
                )
                if reply == QMessageBox.StandardButton.Yes:
                    logging.info("Login cancelado por el usuario. Cerrando aplicación.")
                    db_manager_for_login.close_connections()
                    try:
                        wd = getattr(app, 'global_proxy_watchdog', None)
                        if wd:
                            wd.stop()
                    except Exception:
                        pass
                    try:
                        tmr = getattr(app, '_global_proxy_watchdog_timer', None)
                        if tmr:
                            tmr.stop()
                    except Exception:
                        pass
                    try:
                        terminate_tunnel_processes()
                    except Exception:
                        pass
                    try:
                        terminate_ssh_processes()
                    except Exception:
                        pass
                    sys.exit(0)
                else:
                    # Reintentar mostrando nuevamente el diálogo
                    continue
        if True:
            # Mostrar ventana principal inmediatamente; el progreso se gestionará en overlay no bloqueante

            # Crear la ventana principal de forma diferida para que el diálogo se pinte primero
            window = None
            def _create_main_window():
                nonlocal window
                window = MainWindow(login_dialog.logged_in_role, db_manager_for_login, login_dialog.logged_in_user)

                # Conectar progreso de arranque al overlay integrado de MainWindow
                try:
                    def _on_step(i: int, total: int, label: str):
                        try:
                            # Delegar al manejador interno del overlay
                            if hasattr(window, 'update_startup_overlay'):
                                window.update_startup_overlay(i, total, label)
                            QApplication.processEvents()
                        except Exception:
                            pass
                    window.startup_progress_step.connect(_on_step)
                except Exception:
                    pass

                # Reforzar el icono tras el login
                try:
                    icon_candidates = ["assets/gym_logo.ico", "assets/gym_icon.ico", "assets/icon.png"]
                    for candidate in icon_candidates:
                        ip = resource_path(candidate)
                        if os.path.exists(ip):
                            icon = QIcon(ip)
                            QApplication.instance().setWindowIcon(icon)
                            window.setWindowIcon(icon)
                            break
                except Exception:
                    pass
                window.showMaximized()

                # Replicación lógica PostgreSQL: no se inicia ningún proceso externo desde la app
                # La replicación debe ser administrada por el servidor de base de datos

                # Lanzar arranque diferido en segundo plano (hilo) para evitar bloqueo del UI
                try:
                    from PyQt6.QtCore import QThreadPool, QRunnable
                    class _DeferredStartupRunnable(QRunnable):
                        def __init__(self, _window):
                            super().__init__()
                            self._window = _window
                            try:
                                self.setAutoDelete(True)
                            except Exception:
                                pass
                        def run(self):
                            try:
                                self._window._run_deferred_startup()
                            except Exception as e:
                                logging.debug(f"Error en deferred startup: {e}")
                    QThreadPool.globalInstance().start(_DeferredStartupRunnable(window))
                except Exception:
                    # Fallback: ejecutar en el hilo principal si no está disponible QThreadPool
                    try:
                        QTimer.singleShot(0, window._run_deferred_startup)
                    except Exception:
                        pass

                # Sincronización periódica L→R y R→L mientras la app está abierta
                try:
                    from PyQt6.QtCore import QTimer
                    import sys, subprocess, os, threading
                    window._r2l_running = False
                    window._l2r_running = False

                    def _repo_root():
                        try:
                            return os.path.dirname(os.path.abspath(__file__))
                        except Exception:
                            return os.getcwd()

                    def _run_script_async(args: list, flag_attr: str):
                        def _worker():
                            try:
                                subprocess.run(
                                    args,
                                    cwd=_repo_root(),
                                    creationflags=getattr(subprocess, 'CREATE_NO_WINDOW', 0)
                                )
                            except Exception as e:
                                try:
                                    logging.warning(f"Fallo al ejecutar {' '.join(map(str, args))}: {e}")
                                except Exception:
                                    pass
                            finally:
                                try:
                                    setattr(window, flag_attr, False)
                                except Exception:
                                    pass
                        try:
                            if getattr(window, flag_attr, False):
                                return
                            setattr(window, flag_attr, True)
                            threading.Thread(target=_worker, daemon=True).start()
                        except Exception:
                            try:
                                setattr(window, flag_attr, False)
                            except Exception:
                                pass

                    # Ejecución en-proceso de reconciliaciones, compatible con ejecutable
                    def _run_r2l_inprocess(flag_attr: str):
                        def _worker():
                            try:
                                # Preferir importación por nombre para compatibilidad con ejecutables
                                try:
                                    import importlib
                                    mod = importlib.import_module('scripts.reconcile_remote_to_local_once')
                                except Exception:
                                    from importlib.util import spec_from_file_location, module_from_spec
                                    mod_path = os.path.join(_repo_root(), 'scripts', 'reconcile_remote_to_local_once.py')
                                    spec = spec_from_file_location('reconcile_remote_to_local_once', mod_path)
                                    mod = module_from_spec(spec)  # type: ignore
                                    spec.loader.exec_module(mod)  # type: ignore
                                # Ejecutar con gating de 5 minutos
                                try:
                                    mod.run_once(schema='public', tables=None, dry_run=False, threshold_minutes=5, force=False, subscription='gym_sub')
                                except Exception as e:
                                    logging.warning(f"Fallo R→L in-process: {e}")
                            except Exception as e:
                                try:
                                    logging.warning(f"No se pudo cargar módulo R→L: {e}")
                                except Exception:
                                    pass
                            finally:
                                try:
                                    setattr(window, flag_attr, False)
                                except Exception:
                                    pass
                        try:
                            if getattr(window, flag_attr, False):
                                return
                            setattr(window, flag_attr, True)
                            threading.Thread(target=_worker, daemon=True).start()
                        except Exception:
                            try:
                                setattr(window, flag_attr, False)
                            except Exception:
                                pass

                    def _run_l2r_inprocess(flag_attr: str):
                        def _worker():
                            try:
                                # Preferir importación por nombre para compatibilidad con ejecutables
                                try:
                                    import importlib
                                    mod = importlib.import_module('scripts.reconcile_local_remote_once')
                                except Exception:
                                    from importlib.util import spec_from_file_location, module_from_spec
                                    mod_path = os.path.join(_repo_root(), 'scripts', 'reconcile_local_remote_once.py')
                                    spec = spec_from_file_location('reconcile_local_remote_once', mod_path)
                                    mod = module_from_spec(spec)  # type: ignore
                                    spec.loader.exec_module(mod)  # type: ignore
                                try:
                                    mod.run_once(subscription='gym_sub', schema='public', tables=None, dry_run=False)
                                except Exception as e:
                                    logging.warning(f"Fallo L→R in-process: {e}")
                            except Exception as e:
                                try:
                                    logging.warning(f"No se pudo cargar módulo L→R: {e}")
                                except Exception:
                                    pass
                            finally:
                                try:
                                    setattr(window, flag_attr, False)
                                except Exception:
                                    pass
                        try:
                            if getattr(window, flag_attr, False):
                                return
                            setattr(window, flag_attr, True)
                            threading.Thread(target=_worker, daemon=True).start()
                        except Exception:
                            try:
                                setattr(window, flag_attr, False)
                            except Exception:
                                pass

                    def _start_r2l_timer():
                        try:
                            # Remote→Local cada 5 minutos, con gating interno de threshold
                            r2l = QTimer(window)
                            r2l.setInterval(2 * 60 * 1000)
                            r2l.timeout.connect(lambda: _run_r2l_inprocess('_r2l_running'))
                            r2l.start()
                            window._timer_r2l = r2l
                            logging.info("Timer R→L iniciado (cada 5 minutos)")
                        except Exception as e:
                            logging.debug(f"No se pudo iniciar timer R→L: {e}")

                    def _start_l2r_timer():
                        try:
                            # Local→Remote cada 2 minutos
                            l2r = QTimer(window)
                            l2r.setInterval(2 * 60 * 1000)
                            l2r.timeout.connect(lambda: _run_l2r_inprocess('_l2r_running'))
                            l2r.start()
                            window._timer_l2r = l2r
                            logging.info("Timer L→R iniciado (cada 2 minutos)")
                        except Exception as e:
                            logging.debug(f"No se pudo iniciar timer L→R: {e}")

                    # Arrancar ambos timers de forma diferida para no bloquear el render inicial
                    QTimer.singleShot(1500, _start_r2l_timer)
                    QTimer.singleShot(1500, _start_l2r_timer)

                    # Detener timers al salir
                    try:
                        app.aboutToQuit.connect(lambda: (
                            getattr(window, '_timer_r2l', None) and window._timer_r2l.stop(),
                            getattr(window, '_timer_l2r', None) and window._timer_l2r.stop()
                        ))
                    except Exception:
                        pass
                except Exception:
                    pass

                # Definir host/port para escritorio (para monitor y toast)
                try:
                    host = os.getenv("HOST", "127.0.0.1").strip() or "127.0.0.1"
                    try:
                        _port_env = os.getenv("WEBAPP_PORT") or os.getenv("PORT")
                        port = int(_port_env) if _port_env else 8000
                    except Exception:
                        port = 8000
                except Exception:
                    host = "127.0.0.1"
                    port = 8000

                # Integrar monitor de salud de redes en background
                try:
                    try:
                        window.web_host = host
                        window.web_port = port
                        try:
                            window.public_subdomain = None
                        except Exception:
                            pass
                    except Exception:
                        pass

                    def _start_monitor():
                        try:
                            sub = None
                            window.network_monitor = start_network_health_monitor(
                                host=host,
                                port=port,
                                subdomain=sub,
                                public_url=None,
                                restart_server_cb=None,
                                restart_tunnel_cb=None,
                            )
                        except Exception:
                            pass
                    QTimer.singleShot(0, _start_monitor)
                except Exception:
                    pass

            QTimer.singleShot(0, _create_main_window)
            # Eliminar reconexión de túnel: sistema de túnel legacy retirado
            # Mostrar toast de estado del servidor (top-centro) con estados WebApp/DB
            try:
                from widgets.server_status_toast import ServerStatusToast
                import json as _json
                from pathlib import Path as _Path
                # URL local
                local_url = f"http://127.0.0.1:{port}/"
                # URL pública (Railway) desde config si existe
                public_url_val = None
                try:
                    cfg_path = _Path(resource_path('config/config.json'))
                    if not cfg_path.exists():
                        cfg_path = _Path(__file__).resolve().parent / 'config' / 'config.json'
                    if cfg_path.exists():
                        with open(cfg_path, 'r', encoding='utf-8') as f:
                            _cfg = _json.load(f) or {}
                        public_url_val = _cfg.get('UPSTREAM_WEBAPP_BASE_URL') or _cfg.get('webapp_base_url')
                except Exception:
                        public_url_val = None

            except Exception:
                pass

            # Probar salud de WebApp
            webapp_ok = None
            try:
                import requests as _requests  # type: ignore
                r = _requests.get(f"http://127.0.0.1:{port}/healthz", timeout=2.5)
                webapp_ok = (r.status_code == 200)
            except Exception:
                webapp_ok = None

            # Resolver perfiles de DB y probar conexión
            db_local_ok = None
            db_remote_ok = None
            try:
                from database import DatabaseManager as _DBM
                cfg = {}
                cfg_path = _Path(resource_path('config/config.json'))
                if not cfg_path.exists():
                    cfg_path = _Path(__file__).resolve().parent / 'config' / 'config.json'
                if cfg_path.exists():
                    with open(cfg_path, 'r', encoding='utf-8') as f:
                        cfg = _json.load(f) or {}
                local_prof = cfg.get('db_local', {}) or {}
                remote_prof = cfg.get('db_remote', {}) or {}
                db_local_ok = bool(_DBM.test_connection(local_prof)) if local_prof else None
                db_remote_ok = bool(_DBM.test_connection(remote_prof)) if remote_prof else None
            except Exception:
                pass

            try:
                # Identificador externo: no disponible (replicación lógica administrada por PostgreSQL)
                ext_id = None

                toast = ServerStatusToast(
                    window,
                    local_url=local_url,
                    public_url=public_url_val,
                    webapp_ok=webapp_ok,
                    db_local_ok=db_local_ok,
                    db_remote_ok=db_remote_ok,
                    external_id=ext_id,
                )
                toast.show_toast()
            except Exception:
                pass
            except Exception:
                pass
            exit_code = app.exec()
            # Cerrar conexiones al salir
            db_manager_for_login.close_connections()
            # Detener monitor de redes si estaba activo
            try:
                monitor = getattr(window, 'network_monitor', None)
                stop_network_health_monitor(monitor)
            except Exception:
                pass
            # Limpieza moderna: referencias legacy de sync/ProxyWatchdog eliminadas
            # Cerrar túnel público como refuerzo adicional
            try:
                terminate_tunnel_processes()
            except Exception:
                pass
            # Forzar cierre de cualquier proceso ssh.exe residual
            try:
                terminate_ssh_processes()
            except Exception:
                pass
            sys.exit(exit_code)
        else:
            logging.info("Login cancelado. Cerrando aplicación.")
            db_manager_for_login.close_connections()
            # Detener watchdog global del proxy
            try:
                wd = getattr(app, 'global_proxy_watchdog', None)
                if wd:
                    wd.stop()
            except Exception:
                pass
            try:
                tmr = getattr(app, '_global_proxy_watchdog_timer', None)
                if tmr:
                    tmr.stop()
            except Exception:
                pass
            try:
                terminate_tunnel_processes()
            except Exception:
                pass
            try:
                terminate_ssh_processes()
            except Exception:
                pass
            sys.exit(0)
    except Exception as e:
        logging.error(f"Error en función main: {e}")
        try:
            db_manager_for_login.close_connections()
        except:
            pass
        # Detener watchdog global del proxy ante error
        try:
            wd = getattr(app, 'global_proxy_watchdog', None)
            if wd:
                wd.stop()
        except Exception:
            pass
        try:
            tmr = getattr(app, '_global_proxy_watchdog_timer', None)
            if tmr:
                tmr.stop()
        except Exception:
            pass
        try:
            terminate_tunnel_processes()
        except Exception:
            pass
        try:
            terminate_ssh_processes()
        except Exception:
            pass
        sys.exit(1)

if __name__ == "__main__":
    main()
# --- Filtro de mensajes Qt para QSS y consola ---
try:
    from PyQt6.QtCore import qInstallMessageHandler, QtMsgType

    def _qt_message_handler(msg_type, context, message):
        """Suprime warnings de propiedades CSS/QSS no soportadas y redirige el resto a logging."""
        try:
            lower = str(message).lower()
        except Exception:
            lower = ""

        suppress_substrings = (
            "unknown property box-shadow",
            "unknown property transition",
            "unknown property line-height",
            "unknown property letter-spacing",
            "unknown property text-transform",
            "unknown property transform",
            "unknown property opacity",
            "unknown property cursor",
            "declaration dropped",
        )

        if any(s in lower for s in suppress_substrings):
            return

        # Reenvía al sistema de logging según el tipo de mensaje
        try:
            if msg_type == QtMsgType.QtDebugMsg:
                logging.debug(message)
            elif msg_type == QtMsgType.QtInfoMsg:
                logging.info(message)
            elif msg_type == QtMsgType.QtWarningMsg:
                logging.warning(message)
            elif msg_type in (QtMsgType.QtCriticalMsg, QtMsgType.QtFatalMsg):
                logging.error(message)
            else:
                logging.info(message)
        except Exception:
            pass
except Exception:
    # Si no está disponible PyQt6.QtCore, continuamos sin filtro
    def _qt_message_handler(*args, **kwargs):
        return