import os
import sys
from pathlib import Path
import json

from PyQt6.QtWidgets import (
    QApplication, QDialog, QFormLayout, QLineEdit, QSpinBox, QComboBox,
    QHBoxLayout, QVBoxLayout, QPushButton, QLabel, QMessageBox, QInputDialog,
    QCheckBox
)
from PyQt6.QtCore import Qt

import psycopg2
from config import KEYRING_SERVICE_NAME, LEGACY_KEYRING_SERVICE_NAMES
try:
    import keyring  # Almacén seguro de credenciales (Windows Credential Manager)
except Exception:
    keyring = None

try:
    # Importar para leer valores actuales desde database.py
    from database import DatabaseManager  # type: ignore
except Exception:
    DatabaseManager = None  # type: ignore


def _get_current_params() -> dict:
    """Obtiene los parámetros actuales de conexión.
    Prioriza la función existente en database.py para minimizar duplicación.
    Integra lectura de `config/config.json` (incluida la contraseña si existe),
    variables de entorno y almacén seguro (keyring).
    """
    # Directorio base (junto al ejecutable si empaquetado, o al script)
    try:
        base_dir = Path(sys.executable).resolve().parent if getattr(sys, 'frozen', False) else Path(__file__).resolve().parent
    except Exception:
        base_dir = Path(os.getcwd())

    # Intentar cargar de config/config.json
    config_path = base_dir / 'config' / 'config.json'
    cfg = {}
    try:
        if config_path.exists():
            with open(config_path, 'r', encoding='utf-8') as f:
                cfg = json.load(f) or {}
    except Exception:
        cfg = {}

    defaults = {
        'host': str(cfg.get('host', os.getenv('DB_HOST', 'localhost'))),
        'port': int(cfg.get('port', os.getenv('DB_PORT', 5432))),
        'database': str(cfg.get('database', os.getenv('DB_NAME', 'gimnasio'))),
        'user': str(cfg.get('user', os.getenv('DB_USER', 'postgres'))),
        # Primero config.json, luego entorno; keyring más abajo
        'password': str(cfg.get('password', os.getenv('DB_PASSWORD', ''))),
        'sslmode': str(cfg.get('sslmode', os.getenv('DB_SSLMODE', 'prefer'))),
        'connect_timeout': int(cfg.get('connect_timeout', os.getenv('DB_CONNECT_TIMEOUT', 10))),
        'application_name': str(cfg.get('application_name', os.getenv('DB_APPLICATION_NAME', 'gym_management_system'))),
        # URL pública de la webapp (Railway)
        'webapp_base_url': str(cfg.get('webapp_base_url', os.getenv('WEBAPP_BASE_URL', '')))
    }

    # Recuperar contraseña desde almacén seguro si es posible
    if keyring is not None:
        try:
            # Intentar primero por la etiqueta actual
            saved_pwd = keyring.get_password(KEYRING_SERVICE_NAME, defaults['user'])
            # Migración automática desde etiquetas legacy si no existe en la actual
            if not saved_pwd:
                for old_service in LEGACY_KEYRING_SERVICE_NAMES:
                    if not old_service or old_service == KEYRING_SERVICE_NAME:
                        continue
                    try:
                        legacy_pwd = keyring.get_password(old_service, defaults['user'])
                    except Exception:
                        legacy_pwd = None
                    if legacy_pwd:
                        try:
                            keyring.set_password(KEYRING_SERVICE_NAME, defaults['user'], legacy_pwd)
                        except Exception:
                            pass
                        saved_pwd = legacy_pwd
                        break
            if saved_pwd:
                defaults['password'] = saved_pwd
        except Exception:
            pass

    try:
        # Llamar al método existente para asegurar coherencia de defaults
        if DatabaseManager is not None:
            params = DatabaseManager._get_default_connection_params(None)  # type: ignore
            # Asegurar tipos correctos
            params['port'] = int(params.get('port', defaults['port']))
            params['connect_timeout'] = int(params.get('connect_timeout', defaults['connect_timeout']))
            # Si la contraseña no está informada, completar desde defaults (config/env/keyring)
            if not str(params.get('password', '')).strip():
                params['password'] = defaults.get('password', '')
            return params
    except Exception:
        pass

    return defaults


class DBConfigDialog(QDialog):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Configuración de Base de Datos")
        self.setModal(True)
        self.resize(560, 420)

        # Cargar configuración completa del archivo para manejar perfiles
        self.full_cfg = self._load_full_config()
        self.db_local_cfg = self._ensure_db_local_defaults(self.full_cfg.get('db_local'))
        self.db_remote_cfg = self._ensure_db_remote_defaults(self.full_cfg.get('db_remote'))
        # Perfil seleccionado (por archivo o heurística)
        self.selected_profile = str(self.full_cfg.get('db_profile', '') or '').lower()
        if self.selected_profile not in ('local', 'remoto', 'remote'):
            # Heurística: localhost => local; caso contrario => remoto
            top_host = str(self.full_cfg.get('host', ''))
            self.selected_profile = 'local' if top_host in ('', 'localhost', '127.0.0.1') else 'remoto'
        if self.selected_profile == 'remote':
            self.selected_profile = 'remoto'

        self.params = _get_current_params()

        self._build_ui()
        self._load_params()

    def _build_ui(self):
        form = QFormLayout()
        form.setLabelAlignment(Qt.AlignmentFlag.AlignRight)

        # Selector de perfil (Local / Remoto)
        self.profile_combo = QComboBox()
        self.profile_combo.addItems(["Local", "Remoto"])

        self.host_edit = QLineEdit()
        self.port_spin = QSpinBox()
        self.port_spin.setRange(1, 65535)
        self.db_edit = QLineEdit()
        self.user_edit = QLineEdit()
        self.password_edit = QLineEdit()
        self.password_edit.setEchoMode(QLineEdit.EchoMode.Password)
        self.toggle_password_btn = QPushButton("Mostrar")
        self.toggle_password_btn.setCheckable(True)
        self.toggle_password_btn.setToolTip("Mostrar u ocultar la contraseña ingresada")
        pwd_row = QHBoxLayout()
        pwd_row.addWidget(self.password_edit)
        pwd_row.addWidget(self.toggle_password_btn)
        self.ssl_combo = QComboBox()
        self.ssl_combo.addItems(['disable', 'allow', 'prefer', 'require', 'verify-ca', 'verify-full'])
        self.timeout_spin = QSpinBox()
        self.timeout_spin.setRange(1, 600)
        self.app_name_edit = QLineEdit()
        self.store_pwd_checkbox = QCheckBox("Guardar contraseña en archivo (si no hay almacén seguro)")
        self.store_pwd_checkbox.setToolTip("Si está activado, la contraseña también se guarda en config.json como respaldo cuando el almacén seguro no está disponible")

        # Tooltips descriptivos
        self.host_edit.setToolTip("Host de PostgreSQL (por ejemplo, el host de Railway)")
        self.port_spin.setToolTip("Puerto de PostgreSQL (por ejemplo, el puerto asignado por Railway)")
        self.db_edit.setToolTip("Nombre de la base de datos (suele ser 'railway' en Railway)")
        self.user_edit.setToolTip("Usuario de la base de datos (por defecto 'postgres')")
        self.ssl_combo.setToolTip("Modo SSL. Usa 'require' si Railway lo exige")
        self.timeout_spin.setToolTip("Tiempo máximo para intentar conectar (segundos)")
        self.app_name_edit.setToolTip("Nombre de la aplicación para identificar conexiones en PostgreSQL")

        form.addRow("Perfil:", self.profile_combo)
        form.addRow("Host:", self.host_edit)
        form.addRow("Puerto:", self.port_spin)
        form.addRow("Base de datos:", self.db_edit)
        form.addRow("Usuario:", self.user_edit)
        form.addRow("Contraseña:", pwd_row)
        form.addRow("SSL Mode:", self.ssl_combo)
        form.addRow("Connect Timeout (s):", self.timeout_spin)
        form.addRow("Application Name:", self.app_name_edit)
        form.addRow("", self.store_pwd_checkbox)

        # Indicador de estado de conexión
        self.status_label = QLabel("Estado: Sin probar")
        self.status_label.setStyleSheet("color: #666;")
        self.info_label = QLabel("Nota: Las variables de entorno DB_* (si existen) tienen prioridad sobre config.json.")
        self.info_label.setStyleSheet("color: #666; font-size: 11px;")

        # Botones
        self.test_button = QPushButton("Probar conexión")
        self.test_local_button = QPushButton("Probar conexión local")
        self.save_button = QPushButton("Guardar")
        self.show_password_button = QPushButton("Ver contraseña guardada")
        self.clear_password_button = QPushButton("Eliminar contraseña guardada")
        self.cancel_button = QPushButton("Cancelar")

        btns = QHBoxLayout()
        btns.addWidget(self.test_button)
        btns.addWidget(self.test_local_button)
        btns.addWidget(self.show_password_button)
        btns.addWidget(self.clear_password_button)
        btns.addStretch()
        btns.addWidget(self.save_button)
        btns.addWidget(self.cancel_button)

        root = QVBoxLayout()
        root.addLayout(form)
        root.addWidget(self.status_label)
        root.addWidget(self.info_label)
        root.addLayout(btns)
        self.setLayout(root)

        # Señales
        self.test_button.clicked.connect(self._on_test)
        self.test_local_button.clicked.connect(self._on_test_local)
        self.save_button.clicked.connect(self._on_save)
        self.show_password_button.clicked.connect(self._on_show_password)
        self.clear_password_button.clicked.connect(self._on_clear_password)
        self.cancel_button.clicked.connect(self.reject)
        self.toggle_password_btn.toggled.connect(self._on_toggle_password)
        self.profile_combo.currentTextChanged.connect(self._on_profile_changed)

    def _load_params(self):
        # Establecer perfil actual en el combo
        self.profile_combo.setCurrentText("Local" if self.selected_profile == 'local' else "Remoto")
        # Cargar valores del perfil seleccionado (si existen),
        # si no, usar los resueltos por _get_current_params
        if self.selected_profile == 'local' and self.db_local_cfg:
            src = self.db_local_cfg
        elif self.selected_profile in ('remoto', 'remote') and self.db_remote_cfg:
            src = self.db_remote_cfg
        else:
            src = self.params

        self.host_edit.setText(str(src.get('host', self.params.get('host', ''))))
        self.port_spin.setValue(int(src.get('port', self.params.get('port', 5432))))
        self.db_edit.setText(str(src.get('database', self.params.get('database', ''))))
        self.user_edit.setText(str(src.get('user', self.params.get('user', ''))))
        # La contraseña se resuelve por keyring si está vacía
        self.password_edit.setText(str(src.get('password', self.params.get('password', ''))))
        sslmode = str(self.params.get('sslmode', 'prefer'))
        idx = max(0, self.ssl_combo.findText(sslmode))
        self.ssl_combo.setCurrentIndex(idx)
        self.timeout_spin.setValue(int(self.params.get('connect_timeout', 10)))
        self.app_name_edit.setText(str(self.params.get('application_name', 'gym_management_system')))

    def _collect_params(self) -> dict:
        return {
            'host': self.host_edit.text().strip(),
            'port': int(self.port_spin.value()),
            'database': self.db_edit.text().strip(),
            'user': self.user_edit.text().strip(),
            'password': self.password_edit.text(),
            'sslmode': self.ssl_combo.currentText(),
            'connect_timeout': int(self.timeout_spin.value()),
            'application_name': self.app_name_edit.text().strip() or 'gym_management_system',
            'store_password_in_file': bool(self.store_pwd_checkbox.isChecked()),
            'profile': 'local' if self.profile_combo.currentText().lower().startswith('local') else 'remoto',
        }

    def _on_test(self):
        params = self._collect_params()
        ok = self._test_connection(params)
        if ok:
            self.status_label.setText("Estado: Conexión OK")
            self.status_label.setStyleSheet("color: #2ecc71;")
        else:
            self.status_label.setText("Estado: Error de conexión")
            self.status_label.setStyleSheet("color: #e74c3c;")

    def _on_save(self):
        params = self._collect_params()
        # Validaciones amigables
        if not params['host']:
            QMessageBox.warning(self, "Dato requerido", "El host no puede estar vacío.")
            return
        if params['port'] < 1 or params['port'] > 65535:
            QMessageBox.warning(self, "Dato inválido", "El puerto debe estar entre 1 y 65535.")
            return
        if not params['database']:
            QMessageBox.warning(self, "Dato requerido", "Debes indicar el nombre de la base de datos.")
            return
        if not params['user']:
            QMessageBox.warning(self, "Dato requerido", "Debes indicar el usuario de la base de datos.")
            return
        if params['sslmode'] not in ['disable', 'allow', 'prefer', 'require', 'verify-ca', 'verify-full']:
            QMessageBox.warning(self, "Dato inválido", "El modo SSL seleccionado no es válido.")
            return
        if not self._test_connection(params):
            QMessageBox.critical(self, "Conexión fallida", "No se pudo establecer conexión con los parámetros ingresados.")
            return

        try:
            self._write_config_and_password(params)
            QMessageBox.information(self, "Guardado", "Configuración guardada en config/config.json y contraseña en almacén seguro.")
            self.accept()
        except Exception as e:
            QMessageBox.critical(self, "Error al guardar", f"No se pudo guardar la configuración: {e}")

    def _test_connection(self, params: dict) -> bool:
        """Prueba la conexión utilizando la API centralizada de DatabaseManager."""
        try:
            if DatabaseManager is not None and hasattr(DatabaseManager, 'test_connection'):
                # Usar método centralizado para mantener lógica única
                return bool(DatabaseManager.test_connection(params))  # type: ignore[attr-defined]
        except Exception:
            # Fallback conservador: no exponer detalles aquí; UI mostrará error genérico
            pass
        # Si no está disponible DatabaseManager.test_connection, usar fallback local mínimo
        try:
            test_params = {
                'host': params.get('host'),
                'port': params.get('port'),
                'dbname': params.get('database'),
                'user': params.get('user'),
                'password': params.get('password'),
                'sslmode': params.get('sslmode') or 'prefer',
                'connect_timeout': int(params.get('connect_timeout', 5)),
                'application_name': params.get('application_name', 'gym_management_system'),
            }
            # Incluir options si está definido para respetar timeouts de sesión
            if 'options' in params and params.get('options'):
                test_params['options'] = params.get('options')
            conn = psycopg2.connect(**test_params)
            try:
                with conn.cursor() as cur:
                    cur.execute("SELECT 1")
                    cur.fetchone()
            finally:
                try:
                    conn.close()
                except Exception:
                    pass
            return True
        except Exception:
            return False

    def _on_test_local(self):
        try:
            # Construir parámetros locales por defecto con soporte para variables de entorno
            host = os.getenv('DB_HOST', 'localhost')
            try:
                port = int(os.getenv('DB_PORT', 5432))
            except Exception:
                port = 5432
            database = os.getenv('DB_NAME', 'gimnasio')
            user = os.getenv('DB_USER', 'postgres')
            sslmode = os.getenv('DB_SSLMODE', 'prefer')
            connect_timeout = 10
            application_name = 'gym_management_system'

            # Resolver contraseña: campo UI > keyring > entorno > por defecto provisto
            password = self.password_edit.text().strip()
            if not password and keyring is not None:
                try:
                    saved_pwd = keyring.get_password(KEYRING_SERVICE_NAME, user)
                    if saved_pwd:
                        password = saved_pwd
                except Exception:
                    pass
            if not password:
                password = os.getenv('DB_PASSWORD', 'Matute03')

            params = {
                'host': host,
                'port': port,
                'database': database,
                'user': user,
                'password': password,
                'sslmode': sslmode,
                'connect_timeout': connect_timeout,
                'application_name': application_name,
            }

            ok = self._test_connection(params)
            if ok:
                self.status_label.setText("Estado: Conexión local OK")
                self.status_label.setStyleSheet("color: #2ecc71;")
            else:
                self.status_label.setText("Estado: Error con base local")
                self.status_label.setStyleSheet("color: #e74c3c;")
        except Exception:
            self.status_label.setText("Estado: Error al intentar conexión local")
            self.status_label.setStyleSheet("color: #e74c3c;")

    def _write_config_and_password(self, params: dict):
        # Determinar directorio base
        try:
            base_dir = Path(sys.executable).resolve().parent if getattr(sys, 'frozen', False) else Path(__file__).resolve().parent
        except Exception:
            base_dir = Path(os.getcwd())

        # Guardar configuración en config/config.json (fusionando claves existentes)
        config_dir = base_dir / 'config'
        config_dir.mkdir(parents=True, exist_ok=True)
        config_path = config_dir / 'config.json'
        existing_cfg = {}
        try:
            if config_path.exists():
                with open(config_path, 'r', encoding='utf-8') as f:
                    existing_cfg = json.load(f) or {}
        except Exception:
            existing_cfg = {}

        # Actualizar únicamente claves de DB, preservando otras (p.ej., public_tunnel)
        db_cfg = {
            'host': params['host'],
            'port': params['port'],
            'database': params['database'],
            'user': params['user'],
            'sslmode': params['sslmode'],
            'connect_timeout': params['connect_timeout'],
            'application_name': params['application_name'],
        }
        # Incluir contraseña solo si el usuario lo solicita o si no hay keyring
        if params.get('store_password_in_file', False) or keyring is None:
            db_cfg['password'] = params.get('password', '')
        else:
            # Si no queremos persistirla en archivo, eliminar cualquier rastro previo
            if 'password' in existing_cfg:
                try:
                    del existing_cfg['password']
                except Exception:
                    pass

        # Escribir sección del perfil seleccionado y top-level para compatibilidad
        profile = str(params.get('profile', 'local')).lower()
        # Normalizar clave perfil remoto
        profile_key = 'db_local' if profile == 'local' else 'db_remote'
        existing_cfg[profile_key] = db_cfg
        # Mantener la otra sección si ya existe
        if profile_key == 'db_local' and 'db_remote' not in existing_cfg and self.db_remote_cfg:
            existing_cfg['db_remote'] = self.db_remote_cfg
        if profile_key == 'db_remote' and 'db_local' not in existing_cfg and self.db_local_cfg:
            existing_cfg['db_local'] = self.db_local_cfg
        # Establecer perfil activo y duplicar a nivel raíz para que el resto de la app funcione sin cambios
        existing_cfg['db_profile'] = 'local' if profile == 'local' else 'remoto'
        for k in ['host', 'port', 'database', 'user', 'sslmode', 'connect_timeout', 'application_name']:
            existing_cfg[k] = db_cfg.get(k, existing_cfg.get(k))
        if 'password' in db_cfg:
            existing_cfg['password'] = db_cfg['password']

        merged_cfg = existing_cfg
        with open(config_path, 'w', encoding='utf-8') as f:
            json.dump(merged_cfg, f, ensure_ascii=False, indent=2)

        # Guardar contraseña en almacén seguro (Windows Credential Manager)
        if keyring is None:
            # Si no hay keyring, ya quedó guardada en config.json; no fallar la operación
            return
        try:
            keyring.set_password(KEYRING_SERVICE_NAME, params['user'], params['password'])
        except Exception:
            # No interrumpir el guardado si falla el backend de keyring (p.ej., win32timezone faltante)
            pass

    def _on_profile_changed(self, _: str):
        try:
            prof = 'local' if self.profile_combo.currentText().lower().startswith('local') else 'remoto'
            src = self.db_local_cfg if prof == 'local' else self.db_remote_cfg
            # Valores por defecto si no existe configuración del perfil
            if not src:
                src = self._ensure_db_local_defaults(None) if prof == 'local' else self._ensure_db_remote_defaults(None)
            self.host_edit.setText(str(src.get('host', 'localhost' if prof == 'local' else '')))
            self.port_spin.setValue(int(src.get('port', 5432 if prof == 'local' else 5432)))
            self.db_edit.setText(str(src.get('database', 'gimnasio' if prof == 'local' else 'railway')))
            self.user_edit.setText(str(src.get('user', 'postgres')))
            # No sobreescribir contraseña si ya se escribió manualmente
            if not self.password_edit.text().strip():
                self.password_edit.setText(str(src.get('password', '')))
            # sslmode, timeout, app name
            sslmode = str(src.get('sslmode', 'prefer' if prof == 'local' else 'require'))
            idx = max(0, self.ssl_combo.findText(sslmode))
            self.ssl_combo.setCurrentIndex(idx)
            self.timeout_spin.setValue(int(src.get('connect_timeout', 10)))
            self.app_name_edit.setText(str(src.get('application_name', 'gym_management_system')))
            self.status_label.setText("Estado: Sin probar")
            self.status_label.setStyleSheet("color: #666;")
        except Exception:
            pass

    def _load_full_config(self) -> dict:
        try:
            base_dir = Path(sys.executable).resolve().parent if getattr(sys, 'frozen', False) else Path(__file__).resolve().parent
        except Exception:
            base_dir = Path(os.getcwd())
        config_path = base_dir / 'config' / 'config.json'
        try:
            if config_path.exists():
                with open(config_path, 'r', encoding='utf-8') as f:
                    return json.load(f) or {}
        except Exception:
            pass
        return {}

    def _ensure_db_local_defaults(self, cfg: dict | None) -> dict:
        base = cfg.copy() if isinstance(cfg, dict) else {}
        base.setdefault('host', 'localhost')
        base.setdefault('port', 5432)
        base.setdefault('database', 'gimnasio')
        base.setdefault('user', 'postgres')
        base.setdefault('sslmode', 'prefer')
        base.setdefault('connect_timeout', 10)
        base.setdefault('application_name', 'gym_management_system')
        return base

    def _ensure_db_remote_defaults(self, cfg: dict | None) -> dict:
        base = cfg.copy() if isinstance(cfg, dict) else {}
        # No asumimos host/puerto de remoto si no existen
        base.setdefault('host', '')
        base.setdefault('port', 5432)
        base.setdefault('database', 'railway')
        base.setdefault('user', 'postgres')
        base.setdefault('sslmode', 'require')
        base.setdefault('connect_timeout', 10)
        base.setdefault('application_name', 'gym_management_system')
        return base

    def _on_clear_password(self):
        try:
            user = self.user_edit.text().strip()
            if not user:
                QMessageBox.warning(self, "Usuario requerido", "Indica el usuario para eliminar la contraseña guardada.")
                return
            cleared_any = False
            # Eliminar de keyring
            if keyring is not None:
                try:
                    keyring.delete_password(KEYRING_SERVICE_NAME, user)
                    cleared_any = True
                except Exception:
                    # Ignorar si no existe
                    pass
            # Eliminar de config.json
            try:
                base_dir = Path(sys.executable).resolve().parent if getattr(sys, 'frozen', False) else Path(__file__).resolve().parent
            except Exception:
                base_dir = Path(os.getcwd())
            config_path = (base_dir / 'config' / 'config.json')
            try:
                if config_path.exists():
                    with open(config_path, 'r', encoding='utf-8') as f:
                        cfg = json.load(f) or {}
                    if 'password' in cfg:
                        del cfg['password']
                        with open(config_path, 'w', encoding='utf-8') as f:
                            json.dump(cfg, f, ensure_ascii=False, indent=2)
                        cleared_any = True
            except Exception:
                pass
            if cleared_any:
                QMessageBox.information(self, "Hecho", "Contraseña eliminada de almacén seguro y/o config.json.")
            else:
                QMessageBox.information(self, "Sin cambios", "No había contraseña guardada para eliminar.")
        except Exception as e:
            QMessageBox.critical(self, "Error", f"No se pudo eliminar la contraseña: {e}")

    def _on_toggle_password(self, checked: bool):
        try:
            self.password_edit.setEchoMode(QLineEdit.EchoMode.Normal if checked else QLineEdit.EchoMode.Password)
            self.toggle_password_btn.setText("Ocultar" if checked else "Mostrar")
        except Exception:
            pass

    def _on_show_password(self):
        try:
            # Confirmación de acceso antes de mostrar la contraseña
            pwd, ok = QInputDialog.getText(
                self,
                "Confirmación",
                "Contraseña de administrador:",
                QLineEdit.EchoMode.Password,
            )
            if not ok or pwd != "Matute03!?":
                QMessageBox.warning(self, "Denegado", "Contraseña incorrecta.")
                return

            user = self.user_edit.text().strip()
            if not user:
                QMessageBox.warning(self, "Usuario requerido", "Define el usuario para consultar la contraseña.")
                return

            if keyring is None:
                QMessageBox.critical(self, "No disponible", "El almacén seguro no está disponible.")
                return

            # Intentar primero por la etiqueta actual
            saved = keyring.get_password(KEYRING_SERVICE_NAME, user)
            # Migración automática desde etiquetas legacy si no existe en la actual
            if not saved:
                for old_service in LEGACY_KEYRING_SERVICE_NAMES:
                    if not old_service or old_service == KEYRING_SERVICE_NAME:
                        continue
                    try:
                        legacy_pwd = keyring.get_password(old_service, user)
                    except Exception:
                        legacy_pwd = None
                    if legacy_pwd:
                        try:
                            keyring.set_password(KEYRING_SERVICE_NAME, user, legacy_pwd)
                        except Exception:
                            pass
                        saved = legacy_pwd
                        break
            if saved:
                QMessageBox.information(self, "Contraseña guardada", f"La contraseña guardada para '{user}' es:\n\n{saved}")
            else:
                QMessageBox.information(self, "Sin contraseña", "No hay contraseña guardada para el usuario.")
        except Exception as e:
            QMessageBox.critical(self, "Error", f"No se pudo recuperar la contraseña: {e}")


def main():
    app = QApplication(sys.argv)

    # Protección por contraseña antes de mostrar cualquier información
    try:
        pwd, ok = QInputDialog.getText(
            None,
            "Acceso requerido",
            "Contraseña de administrador:",
            QLineEdit.EchoMode.Password,
        )
    except Exception:
        pwd, ok = "", False

    if not ok or pwd != "Matute03!?":
        QMessageBox.critical(None, "Acceso denegado", "Contraseña incorrecta.")
        sys.exit(1)

    dlg = DBConfigDialog()
    dlg.show()
    sys.exit(app.exec())


if __name__ == '__main__':
    main()