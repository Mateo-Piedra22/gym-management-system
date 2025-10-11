import os
import sys
import logging
import json
from typing import Optional

from PyQt6.QtWidgets import (
    QApplication, QWidget, QVBoxLayout, QHBoxLayout, QLabel,
    QPushButton, QSystemTrayIcon, QMenu
)
from PyQt6.QtGui import QIcon, QPixmap
from PyQt6.QtCore import Qt

# Utilidades del proyecto
from utils import resource_path, terminate_tunnel_processes, get_gym_name, get_public_tunnel_enabled

# Gestor de base de datos para que la web pueda usar la misma instancia si es necesario
from database import DatabaseManager

# Servidor web y túnel público
from webapp.server import start_web_server, start_public_tunnel
from utils_modules.network_health_monitor import (
    start_network_health_monitor,
    stop_network_health_monitor,
    test_networks_and_restart,
)

# Reutilizar cierre defensivo de procesos SSH adicionales si está disponible
try:
    from main import terminate_ssh_processes  # type: ignore
except Exception:
    def terminate_ssh_processes():
        try:
            # Fallback básico en Windows
            if os.name == 'nt':
                os.system('taskkill /F /IM ssh.exe /T')
        except Exception:
            pass


# Subdominio público: usar función centralizada desde utils


class WebIndicatorWindow(QWidget):
    """Ventana ligera que indica que la web está corriendo y permite abrirla."""

    def __init__(self, local_url: str, public_url: Optional[str], web_port: int):
        super().__init__()
        self.local_url = local_url
        self.public_url = public_url
        self.web_port = int(web_port)
        # Callbacks opcionales de reinicio, configuradas desde main()
        self._restart_server_cb = None
        self._restart_tunnel_cb = None

        # Título con nombre del gimnasio
        gym_name = get_gym_name("Gimnasio")
        self.setWindowTitle(f"GymMS Web – {gym_name}")

        # Icono de la ventana y de la bandeja del sistema
        app_icon = None
        for candidate in [
            "assets/gym_logo.ico",
            "assets/gym_icon.ico",
            "assets/icon.png",
        ]:
            ip = resource_path(candidate)
            if os.path.exists(ip):
                app_icon = QIcon(ip)
                break
        if app_icon:
            self.setWindowIcon(app_icon)

        # Layout principal
        main_layout = QVBoxLayout(self)
        main_layout.setContentsMargins(16, 16, 16, 16)
        main_layout.setSpacing(12)

        # Encabezado con logo y nombre
        header = QHBoxLayout()
        header.setSpacing(10)

        logo_label = QLabel()
        logo_label.setObjectName("gym_logo")
        logo_label.setAlignment(Qt.AlignmentFlag.AlignVCenter | Qt.AlignmentFlag.AlignLeft)
        logo_label.setProperty("dynamic_css", "true")
        try:
            pix = QPixmap(resource_path("assets/gym_logo.png"))
            if not pix.isNull():
                logo_label.setPixmap(pix.scaled(40, 40, Qt.AspectRatioMode.KeepAspectRatio, Qt.TransformationMode.SmoothTransformation))
        except Exception:
            pass

        name_label = QLabel(gym_name)
        name_label.setObjectName("gym_name_tab")
        name_label.setProperty("dynamic_css", "true")
        name_label.setAlignment(Qt.AlignmentFlag.AlignVCenter | Qt.AlignmentFlag.AlignLeft)

        header.addWidget(logo_label)
        header.addWidget(name_label)
        header.addStretch()

        main_layout.addLayout(header)

        # Estado e instrucciones
        status = QLabel("La web está corriendo. Puedes abrirla aquí:")
        status.setObjectName("status_label")
        status.setProperty("dynamic_css", "true")
        main_layout.addWidget(status)

        # Botones para abrir URLs (dueño y check-in)
        buttons = QHBoxLayout()
        buttons.setSpacing(10)

        self.open_owner_btn = QPushButton("Abrir web dueño")
        self.open_owner_btn.setObjectName("open_owner_btn")
        self.open_owner_btn.setProperty("dynamic_css", "true")
        self.open_owner_btn.clicked.connect(self._open_owner_login)
        buttons.addWidget(self.open_owner_btn)

        self.open_checkin_btn = QPushButton("Abrir web check-in")
        self.open_checkin_btn.setObjectName("open_checkin_btn")
        self.open_checkin_btn.setProperty("dynamic_css", "true")
        self.open_checkin_btn.clicked.connect(self._open_checkin)
        buttons.addWidget(self.open_checkin_btn)

        # Botón pequeño para testear y reiniciar redes automáticamente si fallan
        self.test_networks_btn = QPushButton("🧪 Test redes")
        self.test_networks_btn.setObjectName("test_networks_btn")
        self.test_networks_btn.setProperty("class", "secondary")
        try:
            self.test_networks_btn.setFixedHeight(28)
        except Exception:
            pass
        self.test_networks_btn.setToolTip("Probar salud local y túnel público; reiniciar si fallan")
        self.test_networks_btn.clicked.connect(self._test_and_restart_networks)
        buttons.addWidget(self.test_networks_btn)

        main_layout.addLayout(buttons)

        # Indicadores de rutas principales
        preferred_base = self.public_url or self.local_url
        owner_login_url = (preferred_base.rstrip('/') + '/login')
        checkin_url = (preferred_base.rstrip('/') + '/checkin')
        running_label = QLabel(f"Login dueño: {owner_login_url}\nCheck-in: {checkin_url}")
        running_label.setObjectName("running_label")
        running_label.setProperty("dynamic_css", "true")
        main_layout.addWidget(running_label)

        # Crear icono en la bandeja del sistema
        self.tray = QSystemTrayIcon(app_icon or QIcon(), self)
        self.tray.setToolTip(f"GymMS Web – {gym_name}")
        self.tray.setVisible(True)

        tray_menu = QMenu()
        show_action = tray_menu.addAction("Mostrar ventana")
        show_action.triggered.connect(self._show_window)
        open_owner_action = tray_menu.addAction("Abrir web dueño")
        open_owner_action.triggered.connect(self._open_owner_login)
        open_checkin_action = tray_menu.addAction("Abrir web check-in")
        open_checkin_action.triggered.connect(self._open_checkin)
        tray_menu.addSeparator()
        exit_action = tray_menu.addAction("Salir")
        exit_action.triggered.connect(self._exit_app)
        self.tray.setContextMenu(tray_menu)

        # Aplicar estilo (QSS) del programa y conectar al sistema dinámico básico
        try:
            qss_path = resource_path("styles/style.qss")
            if os.path.exists(qss_path):
                with open(qss_path, "r", encoding="utf-8") as f:
                    self.setStyleSheet(f.read())
        except Exception:
            pass

        # Tamaño y flags
        self.setMinimumSize(420, 180)
        self.setWindowFlag(Qt.WindowType.WindowMinimizeButtonHint, True)
        self.setWindowFlag(Qt.WindowType.WindowCloseButtonHint, True)

    def _show_window(self):
        try:
            self.show()
            self.raise_()
            self.activateWindow()
        except Exception:
            pass

    def _open_owner_login(self):
        try:
            import webbrowser
            base = (self.public_url or self.local_url).rstrip('/')
            webbrowser.open(base + "/login")
        except Exception:
            pass

    def _open_checkin(self):
        try:
            import webbrowser
            base = (self.public_url or self.local_url).rstrip('/')
            webbrowser.open(base + "/checkin")
        except Exception:
            pass

    def _exit_app(self):
        try:
            terminate_tunnel_processes()
        except Exception:
            pass
        try:
            terminate_ssh_processes()
        except Exception:
            pass
        QApplication.quit()

    def _test_and_restart_networks(self):
        try:
            public_url = None
            try:
                if get_public_tunnel_enabled():
                    public_url = self.public_url
            except Exception:
                public_url = None
            res = test_networks_and_restart(
                host="127.0.0.1",
                port=self.web_port,
                public_url=public_url,
                restart_server_cb=(self._restart_server_cb or (lambda: None)),
                restart_tunnel_cb=(self._restart_tunnel_cb or (lambda: None)),
            )
            # Feedback mínimo en bandeja
            try:
                msg = (
                    f"Local: {'OK' if res.get('local_ok') else 'REINICIADO' if res.get('server_restarted') else 'FALLA'} | "
                    f"Público: {'OK' if res.get('public_ok') else 'REINICIADO' if res.get('tunnel_restarted') else 'FALLA'}"
                )
                if hasattr(self, 'tray'):
                    self.tray.showMessage("Red GymMSW", msg)
            except Exception:
                pass
        except Exception:
            pass

    def closeEvent(self, event):
        # Al cerrar, finalizar túneles públicos (LocalTunnel por defecto) y SSH residuales
        try:
            terminate_tunnel_processes()
        except Exception:
            pass
        try:
            terminate_ssh_processes()
        except Exception:
            pass
        event.accept()


def main():
    # Configuración de logging básica
    logging.basicConfig(level=logging.INFO, format="[%(levelname)s] %(message)s")

    # Crear aplicación Qt
    app = QApplication(sys.argv)

    # Icono global
    try:
        for candidate in ["assets/gym_logo.ico", "assets/gym_icon.ico", "assets/icon.png"]:
            ip = resource_path(candidate)
            if os.path.exists(ip):
                app.setWindowIcon(QIcon(ip))
                break
    except Exception:
        pass

    # Instanciar DB Manager para que la web pueda acceder si es necesario
    db = None
    try:
        db = DatabaseManager()
    except Exception as e:
        logging.warning(f"No se pudo crear DatabaseManager: {e}")

    # Arrancar servidor web
    host = os.getenv("WEBAPP_HOST", "0.0.0.0")
    try:
        port = int(os.getenv("WEBAPP_PORT", "8000"))
    except Exception:
        port = 8000
    try:
        start_web_server(db_manager=db, host=host, port=port)
        logging.info(f"Servidor web iniciado: http://127.0.0.1:{port}/")
    except Exception as e:
        logging.error(f"No se pudo iniciar el servidor web: {e}")

    # Iniciar túnel público (subdominio desde configuración)
    public_url = None
    if get_public_tunnel_enabled():
        try:
            public_url = start_public_tunnel(local_port=port)
            if public_url:
                logging.info(f"Acceso público: {public_url}")
        except Exception:
            logging.warning("No se pudo iniciar el túnel público automáticamente")
    else:
        logging.info("Túnel público deshabilitado por configuración")

    # Crear ventana indicador
    local_url = f"http://127.0.0.1:{port}/"
    win = WebIndicatorWindow(local_url=local_url, public_url=public_url, web_port=port)
    win.show()

    # Sin contraseña de túnel: Railway no requiere credenciales de acceso públicas

    # Mostrar toast de estado del servidor (top-centro)
    try:
        from widgets.server_status_toast import ServerStatusToast
        toast = ServerStatusToast(win, local_url=local_url, public_url=public_url)
        toast.show_toast()
    except Exception:
        pass

    # Registrar callback de reconexión del túnel (anti-spam gestionado en server.py)
    try:
        from webapp.server import set_public_tunnel_reconnect_callback
        from PyQt6.QtCore import QTimer
        def _on_tunnel_reconnect(url: str):
            # Asegurar creación del toast en el hilo de UI
            try:
                def _show():
                    try:
                        from widgets.server_status_toast import ServerStatusToast
                        toast = ServerStatusToast(win, local_url=local_url, public_url=url, message="Conexión pública reconectada")
                        toast.show_toast()
                    except Exception:
                        pass
                QTimer.singleShot(0, _show)
            except Exception:
                pass
        set_public_tunnel_reconnect_callback(_on_tunnel_reconnect)
    except Exception:
        pass

    # Iniciar monitor de salud de red con callbacks de reinicio
    try:
        # Definir callbacks específicos para reinicio
        win._restart_server_cb = lambda: start_web_server(db_manager=db, host=host, port=port)
        win._restart_tunnel_cb = lambda: start_public_tunnel(local_port=port)
        monitor = start_network_health_monitor(
            host="127.0.0.1",
            port=port,
            subdomain=None,
            public_url=(public_url if get_public_tunnel_enabled() else None),
            restart_server_cb=win._restart_server_cb,
            restart_tunnel_cb=win._restart_tunnel_cb,
        )
    except Exception:
        monitor = None

    # Ejecutar aplicación
    exit_code = app.exec()

    # Limpieza al salir
    try:
        from utils import terminate_tunnel_processes
        terminate_tunnel_processes()
    except Exception:
        pass
    # Detener monitor al salir
    try:
        stop_network_health_monitor(monitor)
    except Exception:
        pass
    try:
        terminate_ssh_processes()
    except Exception:
        pass
    sys.exit(exit_code)


if __name__ == "__main__":
    main()