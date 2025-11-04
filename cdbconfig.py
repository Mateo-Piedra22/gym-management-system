import os
import sys
from pathlib import Path
import json
import io
import contextlib
import socket
from urllib.parse import urlparse, parse_qs
from datetime import datetime

from PyQt6.QtWidgets import (
    QApplication, QDialog, QFormLayout, QLineEdit, QSpinBox, QComboBox,
    QHBoxLayout, QVBoxLayout, QPushButton, QLabel, QMessageBox, QInputDialog,
    QCheckBox, QTimeEdit, QProgressDialog, QScrollArea, QWidget
)
from PyQt6.QtCore import Qt, QTime

import psycopg2
from psycopg2 import sql
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
        self.resize(680, 700)

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

        # Campo DSN y botón de importación
        self.dsn_edit = QLineEdit()
        self.dsn_edit.setPlaceholderText("postgresql://usuario:contraseña@host:puerto/db?sslmode=require")
        self.dsn_edit.setToolTip("DSN/URL de conexión; si lo ingresas, se rellenan los campos.")
        self.dsn_import_button = QPushButton("Importar desde DSN")
        dsn_row = QHBoxLayout()
        dsn_row.addWidget(self.dsn_edit)
        dsn_row.addWidget(self.dsn_import_button)

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
        form.addRow("DSN (opcional):", dsn_row)
        form.addRow("Host:", self.host_edit)
        form.addRow("Puerto:", self.port_spin)
        form.addRow("Base de datos:", self.db_edit)
        form.addRow("Usuario:", self.user_edit)
        form.addRow("Contraseña:", pwd_row)
        form.addRow("SSL Mode:", self.ssl_combo)
        form.addRow("Connect Timeout (s):", self.timeout_spin)
        form.addRow("Application Name:", self.app_name_edit)
        form.addRow("", self.store_pwd_checkbox)

        # --- Sección de Tareas Programadas ---
        section_label = QLabel("Tareas programadas")
        section_label.setStyleSheet("font-weight: bold; padding-top: 8px;")
        form.addRow("", section_label)

        self.tasks_master_checkbox = QCheckBox("Activar tareas programadas")
        form.addRow("", self.tasks_master_checkbox)

        # Replicación nativa PostgreSQL - sin uploader legacy

        # Reconciliación Remoto→Local (minutos)
        self.reconcile_r2l_enable_checkbox = QCheckBox("Reconciliación Remoto→Local")
        self.reconcile_r2l_interval_spin = QSpinBox()
        self.reconcile_r2l_interval_spin.setRange(1, 60)
        rec_r2l_row = QHBoxLayout()
        rec_r2l_row.addWidget(self.reconcile_r2l_enable_checkbox)
        rec_r2l_row.addStretch()
        rec_r2l_row.addWidget(QLabel("Cada (min):"))
        rec_r2l_row.addWidget(self.reconcile_r2l_interval_spin)
        form.addRow("Reconciliar R→L:", rec_r2l_row)

        # Reconciliación Local→Remoto (diaria)
        self.reconcile_l2r_enable_checkbox = QCheckBox("Reconciliación Local→Remoto")
        self.reconcile_l2r_time_edit = QTimeEdit()
        self.reconcile_l2r_time_edit.setDisplayFormat("HH:mm")
        rec_l2r_row = QHBoxLayout()
        rec_l2r_row.addWidget(self.reconcile_l2r_enable_checkbox)
        rec_l2r_row.addStretch()
        rec_l2r_row.addWidget(QLabel("Hora:"))
        rec_l2r_row.addWidget(self.reconcile_l2r_time_edit)
        form.addRow("Reconciliar L→R:", rec_l2r_row)

        # Reconciliación Bidireccional (diaria)
        self.reconcile_bidirectional_enable_checkbox = QCheckBox("Reconciliación Bidireccional")
        self.reconcile_bidirectional_time_edit = QTimeEdit()
        self.reconcile_bidirectional_time_edit.setDisplayFormat("HH:mm")
        rec_bidir_row = QHBoxLayout()
        rec_bidir_row.addWidget(self.reconcile_bidirectional_enable_checkbox)
        rec_bidir_row.addStretch()
        rec_bidir_row.addWidget(QLabel("Hora:"))
        rec_bidir_row.addWidget(self.reconcile_bidirectional_time_edit)
        form.addRow("Reconciliar Bidireccional:", rec_bidir_row)

        self.cleanup_enable_checkbox = QCheckBox("Limpieza diaria de retención")
        self.cleanup_time_edit = QTimeEdit()
        self.cleanup_time_edit.setDisplayFormat("HH:mm")
        clean_row = QHBoxLayout()
        clean_row.addWidget(self.cleanup_enable_checkbox)
        clean_row.addStretch()
        clean_row.addWidget(QLabel("Hora:"))
        clean_row.addWidget(self.cleanup_time_edit)
        form.addRow("Limpieza:", clean_row)

        self.backup_enable_checkbox = QCheckBox("Backup diario rápido")
        self.backup_time_edit = QTimeEdit()
        self.backup_time_edit.setDisplayFormat("HH:mm")
        bkp_row = QHBoxLayout()
        bkp_row.addWidget(self.backup_enable_checkbox)
        bkp_row.addStretch()
        bkp_row.addWidget(QLabel("Hora:"))
        bkp_row.addWidget(self.backup_time_edit)
        form.addRow("Backup:", bkp_row)

        # Tareas semanales - replicación nativa PostgreSQL

        # Salud de replicación semanal
        self.replication_health_weekly_enable_checkbox = QCheckBox("Salud de replicación semanal")
        self.replication_health_weekly_time_edit = QTimeEdit()
        self.replication_health_weekly_time_edit.setDisplayFormat("HH:mm")
        self.replication_health_weekly_day_combo = QComboBox()
        self.replication_health_weekly_day_combo.addItems(["SUN", "MON", "TUE", "WED", "THU", "FRI", "SAT"]) 
        rep_health_weekly_row = QHBoxLayout()
        rep_health_weekly_row.addWidget(self.replication_health_weekly_enable_checkbox)
        rep_health_weekly_row.addStretch()
        rep_health_weekly_row.addWidget(QLabel("Hora:"))
        rep_health_weekly_row.addWidget(self.replication_health_weekly_time_edit)
        rep_health_weekly_row.addWidget(QLabel("Día:"))
        rep_health_weekly_row.addWidget(self.replication_health_weekly_day_combo)
        form.addRow("Salud replicación:", rep_health_weekly_row)

        # Verificación de publicación semanal
        self.publication_verify_weekly_enable_checkbox = QCheckBox("Verificación de publicación semanal")
        self.publication_verify_weekly_time_edit = QTimeEdit()
        self.publication_verify_weekly_time_edit.setDisplayFormat("HH:mm")
        self.publication_verify_weekly_day_combo = QComboBox()
        self.publication_verify_weekly_day_combo.addItems(["SUN", "MON", "TUE", "WED", "THU", "FRI", "SAT"]) 
        pub_verify_weekly_row = QHBoxLayout()
        pub_verify_weekly_row.addWidget(self.publication_verify_weekly_enable_checkbox)
        pub_verify_weekly_row.addStretch()
        pub_verify_weekly_row.addWidget(QLabel("Hora:"))
        pub_verify_weekly_row.addWidget(self.publication_verify_weekly_time_edit)
        pub_verify_weekly_row.addWidget(QLabel("Día:"))
        pub_verify_weekly_row.addWidget(self.publication_verify_weekly_day_combo)
        form.addRow("Verificación publicación:", pub_verify_weekly_row)

        # --- Sección de Configuración General ---
        gen_label = QLabel("Configuración general")
        gen_label.setStyleSheet("font-weight: bold; padding-top: 8px;")
        form.addRow("", gen_label)

        self.webapp_base_url_edit = QLineEdit()
        self.client_base_url_edit = QLineEdit()
        self.sync_upload_token_edit = QLineEdit()
        self.sync_upload_token_edit.setEchoMode(QLineEdit.EchoMode.Password)
        self.webapp_session_secret_edit = QLineEdit()
        self.webapp_session_secret_edit.setEchoMode(QLineEdit.EchoMode.Password)
        # Túnel público
        self.tunnel_enabled_checkbox = QCheckBox("Activar túnel público")
        self.tunnel_subdomain_edit = QLineEdit()
        tun_row = QHBoxLayout()
        tun_row.addWidget(self.tunnel_enabled_checkbox)
        tun_row.addStretch()
        tun_row.addWidget(QLabel("Subdominio:"))
        tun_row.addWidget(self.tunnel_subdomain_edit)

        form.addRow("Webapp base URL:", self.webapp_base_url_edit)
        form.addRow("Client base URL:", self.client_base_url_edit)
        form.addRow("Token de subida:", self.sync_upload_token_edit)
        form.addRow("Session secret:", self.webapp_session_secret_edit)
        form.addRow("Túnel público:", tun_row)

        # --- Sección de Replicación y Automatización ---
        rep_label = QLabel("Replicación y automatización")
        rep_label.setStyleSheet("font-weight: bold; padding-top: 8px;")
        form.addRow("", rep_label)

        # Nombres de publicación/suscripción
        self.publication_name_edit = QLineEdit()
        self.subscription_name_edit = QLineEdit()
        rep_names_row = QHBoxLayout()
        rep_names_row.addWidget(QLabel("Publication:"))
        rep_names_row.addWidget(self.publication_name_edit)
        rep_names_row.addStretch()
        rep_names_row.addWidget(QLabel("Subscription:"))
        rep_names_row.addWidget(self.subscription_name_edit)
        form.addRow("Replicación:", rep_names_row)

        # Modo + alcance remoto
        self.replication_mode_combo = QComboBox()
        self.replication_mode_combo.addItems(["Remoto → Local", "Bidireccional"])
        self.remote_can_reach_local_checkbox = QCheckBox("Remoto puede acceder al local (VPN/Túnel)")
        mode_row = QHBoxLayout()
        mode_row.addWidget(self.replication_mode_combo)
        mode_row.addStretch()
        mode_row.addWidget(self.remote_can_reach_local_checkbox)
        form.addRow("Modo:", mode_row)

        # Acciones de replicación
        self.setup_replication_button = QPushButton("Configurar replicación")
        self.health_check_button = QPushButton("Verificar salud replicación")
        self.test_remote_button = QPushButton("Probar conexión remota")
        rep_btns = QHBoxLayout()
        rep_btns.addWidget(self.setup_replication_button)
        rep_btns.addWidget(self.health_check_button)
        rep_btns.addWidget(self.test_remote_button)
        form.addRow("", rep_btns)

        # Instaladores (requieren Admin)
        installers_label = QLabel("Instaladores (requieren Admin)")
        installers_label.setStyleSheet("font-weight: bold; padding-top: 8px;")
        form.addRow("", installers_label)
        self.install_wireguard_admin_button = QPushButton("Instalar WireGuard (Admin)")
        self.admin_vpn_postgres_button = QPushButton("VPN + Red PostgreSQL (Admin)")
        self.test_wireguard_button = QPushButton("Probar WireGuard/VPN")
        inst_row = QHBoxLayout()
        inst_row.addWidget(self.install_wireguard_admin_button)
        inst_row.addWidget(self.admin_vpn_postgres_button)
        inst_row.addWidget(self.test_wireguard_button)
        form.addRow("", inst_row)

        # Automatizaciones (operativas) - replicación nativa PostgreSQL
        autom_label = QLabel("Automatizaciones")
        autom_label.setStyleSheet("font-weight: bold; padding-top: 8px;")
        form.addRow("", autom_label)
        self.reconcile_remote_to_local_button = QPushButton("Reconciliar remoto→local (puntual)")
        self.reconcile_local_to_remote_button = QPushButton("Reconciliar local→remoto (puntual)")
        autom_row = QHBoxLayout()
        autom_row.addWidget(self.reconcile_remote_to_local_button)
        autom_row.addWidget(self.reconcile_local_to_remote_button)
        form.addRow("", autom_row)

        # Prerequisitos/Bootstrap
        self.device_id_edit = QLineEdit()
        self.device_id_edit.setPlaceholderText("device_id")
        self.detect_device_button = QPushButton("Detectar")
        dev_row = QHBoxLayout()
        dev_row.addWidget(QLabel("Device ID:"))
        dev_row.addWidget(self.device_id_edit)
        dev_row.addWidget(self.detect_device_button)
        form.addRow("", dev_row)

        self.ensure_prereq_button = QPushButton("Asegurar prerequisitos")
        self.full_bootstrap_button = QPushButton("Forzar instalación completa")
        self.secure_cleanup_button = QPushButton("Limpieza segura (backup + reinit)")
        boot_row = QHBoxLayout()
        boot_row.addWidget(self.ensure_prereq_button)
        boot_row.addWidget(self.full_bootstrap_button)
        boot_row.addWidget(self.secure_cleanup_button)
        form.addRow("", boot_row)

        # Advanced Setup Options
        advanced_label = QLabel("Configuración avanzada")
        advanced_label.setStyleSheet("font-weight: bold; padding-top: 8px;")
        form.addRow("", advanced_label)

        self.force_dependencies_button = QPushButton("Forzar instalación de dependencias")
        self.force_database_init_button = QPushButton("Forzar inicialización de base de datos")
        self.force_replication_button = QPushButton("Forzar configuración de replicación")
        self.force_scheduled_tasks_button = QPushButton("Forzar configuración de tareas programadas")
        
        advanced_row1 = QHBoxLayout()
        advanced_row1.addWidget(self.force_dependencies_button)
        advanced_row1.addWidget(self.force_database_init_button)
        form.addRow("", advanced_row1)
        
        advanced_row2 = QHBoxLayout()
        advanced_row2.addWidget(self.force_replication_button)
        advanced_row2.addWidget(self.force_scheduled_tasks_button)
        form.addRow("", advanced_row2)

        # Seguridad y datos iniciales
        self.secure_owner_local_button = QPushButton("Asegurar DUEÑO (local)")
        self.secure_owner_remote_button = QPushButton("Asegurar DUEÑO (remoto)")
        own_row = QHBoxLayout()
        own_row.addWidget(self.secure_owner_local_button)
        own_row.addWidget(self.secure_owner_remote_button)
        form.addRow("", own_row)

        # Indicador de estado de conexión
        self.status_label = QLabel("Estado: Sin probar")
        self.status_label.setStyleSheet("color: #666;")
        self.info_label = QLabel("Nota: Puedes pegar el DSN de Railway para autocompletar. Los cambios de tareas se aplican al guardar.")
        self.info_label.setStyleSheet("color: #666; font-size: 11px;")

        # Envolver el formulario en un QScrollArea
        scroll_area = QScrollArea()
        scroll_area.setWidgetResizable(True)
        scroll_area.setHorizontalScrollBarPolicy(Qt.ScrollBarPolicy.ScrollBarAlwaysOff)
        scroll_content = QWidget()
        scroll_content.setLayout(form)
        scroll_area.setWidget(scroll_content)

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
        self.cleanup_button = QPushButton("⚠️ Limpiar Bases de Datos…")
        btns.addWidget(self.cleanup_button)
        btns.addStretch()
        btns.addWidget(self.save_button)
        btns.addWidget(self.cancel_button)

        root = QVBoxLayout()
        root.addWidget(scroll_area)
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
        self.cleanup_button.clicked.connect(self._on_cleanup_databases)
        self.cancel_button.clicked.connect(self.reject)
        self.toggle_password_btn.toggled.connect(self._on_toggle_password)
        self.profile_combo.currentTextChanged.connect(self._on_profile_changed)
        self.dsn_import_button.clicked.connect(self._on_import_dsn)
        self.tasks_master_checkbox.toggled.connect(self._on_tasks_master_toggled)
        # Señales de replicación y automatización
        self.setup_replication_button.clicked.connect(self._on_setup_replication)
        self.health_check_button.clicked.connect(self._on_health_check)
        self.test_remote_button.clicked.connect(self._on_test_remote)
        self.detect_device_button.clicked.connect(self._on_detect_device)
        self.ensure_prereq_button.clicked.connect(self._on_ensure_prereq)
        self.full_bootstrap_button.clicked.connect(self._on_full_bootstrap)
        self.secure_owner_local_button.clicked.connect(self._on_secure_owner_local)
        self.secure_owner_remote_button.clicked.connect(self._on_secure_owner_remote)
        # Instaladores
        self.install_wireguard_admin_button.clicked.connect(self._on_install_wireguard_admin)
        self.admin_vpn_postgres_button.clicked.connect(self._on_admin_vpn_postgres)
        self.test_wireguard_button.clicked.connect(self._on_test_wireguard)
        # Automatizaciones - replicación nativa PostgreSQL
        self.reconcile_remote_to_local_button.clicked.connect(self._on_reconcile_remote_to_local_once)
        self.reconcile_local_to_remote_button.clicked.connect(self._on_reconcile_local_to_remote_once)
        self.secure_cleanup_button.clicked.connect(self._on_secure_cleanup)
        
        # Advanced setup connections
        self.force_dependencies_button.clicked.connect(self._on_force_dependencies)
        self.force_database_init_button.clicked.connect(self._on_force_database_init)
        self.force_replication_button.clicked.connect(self._on_force_replication)
        self.force_scheduled_tasks_button.clicked.connect(self._on_force_scheduled_tasks)
        # Mutua exclusión: bidireccional desactiva individuales
        try:
            self.reconcile_bidirectional_enable_checkbox.toggled.connect(self._on_bidir_toggled)
        except Exception:
            pass
        # Mutua exclusión inversa: activar R→L o L→R desactiva bidireccional
        try:
            self.reconcile_r2l_enable_checkbox.toggled.connect(self._on_individual_reconcile_toggled)
            self.reconcile_l2r_enable_checkbox.toggled.connect(self._on_individual_reconcile_toggled)
        except Exception:
            pass

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
        # Cargar configuración de tareas
        self._load_tasks_cfg()
        # Cargar configuración general desde config.json
        try:
            cfg = self.full_cfg if isinstance(self.full_cfg, dict) else {}
            self.webapp_base_url_edit.setText(str(cfg.get('webapp_base_url', '')))
            self.client_base_url_edit.setText(str(cfg.get('client_base_url', '')))
            self.sync_upload_token_edit.setText(str(cfg.get('sync_upload_token', '')))
            self.webapp_session_secret_edit.setText(str(cfg.get('webapp_session_secret', '')))
            public_tunnel = cfg.get('public_tunnel') or {}
            self.tunnel_enabled_checkbox.setChecked(bool(public_tunnel.get('enabled', False)))
            self.tunnel_subdomain_edit.setText(str(public_tunnel.get('subdomain', '')))
        except Exception:
            pass
        # Cargar configuración de replicación
        try:
            rep = (self.full_cfg or {}).get('replication') if isinstance(self.full_cfg, dict) else {}
            rep = rep or {}
            self.publication_name_edit.setText(str(rep.get('publication_name', 'gym_pub')))
            self.subscription_name_edit.setText(str(rep.get('subscription_name', 'gym_sub')))
            self.remote_can_reach_local_checkbox.setChecked(bool(rep.get('remote_can_reach_local', False)))
            # Modo por defecto: Bidireccional
            # Si se requieren pistas para forzar modo unidireccional, pueden añadirse en el futuro.
            mode_idx = 1
            self.replication_mode_combo.setCurrentIndex(mode_idx)
        except Exception:
            pass

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

    def _parse_dsn_to_params(self, dsn: str, defaults: dict) -> dict:
        if not dsn:
            return defaults
        try:
            u = urlparse(dsn)
            host = u.hostname or defaults.get('host', '')
            try:
                port = int(u.port or defaults.get('port', 5432))
            except Exception:
                port = defaults.get('port', 5432)
            database = (u.path or '').lstrip('/') or defaults.get('database', '')
            user = u.username or defaults.get('user', '')
            password = u.password or defaults.get('password', '')
            q = parse_qs(u.query or '')
            sslmode = (q.get('sslmode') or [defaults.get('sslmode', 'prefer')])[0]
            appname = (q.get('application_name') or [defaults.get('application_name', 'gym_management_system')])[0]
            try:
                timeout = int((q.get('connect_timeout') or [defaults.get('connect_timeout', 10)])[0])
            except Exception:
                timeout = defaults.get('connect_timeout', 10)
            return {
                'host': host,
                'port': port,
                'database': database,
                'user': user,
                'password': password,
                'sslmode': sslmode,
                'application_name': appname,
                'connect_timeout': timeout,
            }
        except Exception:
            return defaults

    def _on_import_dsn(self):
        dsn = self.dsn_edit.text().strip()
        if not dsn:
            QMessageBox.warning(self, "DSN requerido", "Ingresa un DSN válido.")
            return
        defaults = self._collect_params()
        newp = self._parse_dsn_to_params(dsn, defaults)
        self.host_edit.setText(str(newp['host']))
        self.port_spin.setValue(int(newp['port']))
        self.db_edit.setText(str(newp['database']))
        self.user_edit.setText(str(newp['user']))
        if newp.get('password'):
            self.password_edit.setText(newp['password'])
        idx = max(0, self.ssl_combo.findText(str(newp['sslmode'])))
        self.ssl_combo.setCurrentIndex(idx)
        self.timeout_spin.setValue(int(newp['connect_timeout']))
        self.app_name_edit.setText(str(newp['application_name']))
        if str(newp['host']).lower() not in ('localhost', '127.0.0.1'):
            self.profile_combo.setCurrentText('Remoto')
        self.status_label.setText("Estado: Sin probar")
        self.status_label.setStyleSheet("color: #666;")

    def _on_test(self):
        try:
            params = self._collect_params()
            progress = QProgressDialog("Probando conexión…", None, 0, 0, self)
            progress.setWindowTitle("Prueba de conexión")
            progress.setWindowModality(Qt.WindowModality.ApplicationModal)
            progress.setMinimumDuration(0)
            progress.setAutoClose(True)
            progress.show()
            QApplication.processEvents()
            ok = self._test_connection(params)
            try:
                progress.close()
            except Exception:
                pass
            if ok:
                self.status_label.setText("Estado: Conexión OK")
                self.status_label.setStyleSheet("color: #2ecc71;")
            else:
                self.status_label.setText("Estado: Error de conexión")
                self.status_label.setStyleSheet("color: #e74c3c;")
        except Exception:
            self.status_label.setText("Estado: Error al intentar conexión")
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
        # Progreso para verificación y guardado
        progress = QProgressDialog("Validando y guardando…", None, 0, 3, self)
        progress.setWindowTitle("Guardar configuración")
        progress.setWindowModality(Qt.WindowModality.ApplicationModal)
        progress.setMinimumDuration(0)
        progress.setAutoClose(True)
        progress.setValue(0)
        progress.show()
        QApplication.processEvents()
        try:
            # Paso 1: Probar conexión
            ok_conn = self._test_connection(params)
            progress.setValue(1)
            QApplication.processEvents()
            if not ok_conn:
                try:
                    progress.close()
                except Exception:
                    pass
                QMessageBox.critical(self, "Conexión fallida", "No se pudo establecer conexión con los parámetros ingresados.")
                return
            # Paso 2: Guardar config y password
            self._write_config_and_password(params)
            progress.setValue(2)
            QApplication.processEvents()
            # Paso 3: Asegurar tareas programadas
            try:
                from utils_modules.prerequisites import ensure_scheduled_tasks
                ensure_scheduled_tasks('local')
            except Exception:
                pass
            progress.setValue(3)
            QApplication.processEvents()
            try:
                progress.close()
            except Exception:
                pass
            QMessageBox.information(self, "Guardado", "Configuración guardada y tareas programadas aplicadas.")
            self.accept()
        except Exception as e:
            try:
                progress.close()
            except Exception:
                pass
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

    def _on_test_remote(self):
        try:
            params = self._get_profile_params('remoto')
            host = str(params.get('host', '')).strip()
            if not host:
                QMessageBox.warning(self, "Config incompleta", "Configura primero el perfil REMOTO (host/puerto/etc.).")
                return
            progress = QProgressDialog("Probando conexión remota…", None, 0, 0, self)
            progress.setWindowTitle("Conexión remota")
            progress.setWindowModality(Qt.WindowModality.ApplicationModal)
            progress.setMinimumDuration(0)
            progress.setAutoClose(True)
            progress.show()
            QApplication.processEvents()
            ok = self._test_connection(params)
            try:
                progress.close()
            except Exception:
                pass
            if ok:
                self.status_label.setText("Estado: Conexión remota OK")
                self.status_label.setStyleSheet("color: #2ecc71;")
            else:
                self.status_label.setText("Estado: Error con base remota")
                self.status_label.setStyleSheet("color: #e74c3c;")
        except Exception:
            self.status_label.setText("Estado: Error al intentar conexión remota")
            self.status_label.setStyleSheet("color: #e74c3c;")

    def _on_setup_replication(self):
        try:
            # Confirmación previa
            resp = QMessageBox.question(
                self,
                "Confirmar replicación",
                (
                    "Esto creará/ajustará publicación y suscripción, y puede modificar objetos\n"
                    "en ambas bases (local/remota). ¿Deseas continuar?"
                ),
                QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
                QMessageBox.StandardButton.No,
            )
            if resp != QMessageBox.StandardButton.Yes:
                return

            from utils_modules.replication_setup import ensure_logical_replication, ensure_bidirectional_replication
            # Cargar config actual desde disco para evitar usar cache desactualizada
            cfg = self._load_full_config() or (self.full_cfg if isinstance(self.full_cfg, dict) else {})
            if not isinstance(cfg, dict):
                cfg = {}
            # Aplicar valores de UI a la sección de replicación
            rep = cfg.get('replication', {}) or {}
            pub = self.publication_name_edit.text().strip() or rep.get('publication_name') or 'gym_pub'
            sub = self.subscription_name_edit.text().strip() or rep.get('subscription_name') or 'gym_sub'
            rep['publication_name'] = pub
            rep['subscription_name'] = sub
            rep['remote_can_reach_local'] = bool(self.remote_can_reach_local_checkbox.isChecked())
            # Pistas para bidireccional
            if self.replication_mode_combo.currentIndex() == 1:
                rep.setdefault('local_publication_name', pub)
                rep.setdefault('remote_subscription_name', sub)
            cfg['replication'] = rep
            # Validaciones ligeras
            if not cfg.get('db_local') or not cfg.get('db_remote'):
                QMessageBox.warning(self, "Config incompleta", "Faltan secciones db_local o db_remote en config/config.json. Guarda y vuelve a intentar.")
                return
            # Orquestar replicación con progreso
            progress = QProgressDialog("Ejecutando replicación…", None, 0, 0, self)
            progress.setWindowTitle("Replicación")
            progress.setWindowModality(Qt.WindowModality.ApplicationModal)
            progress.setMinimumDuration(0)
            progress.setAutoClose(True)
            progress.show()
            QApplication.processEvents()
            # Ejecutar
            if self.replication_mode_combo.currentIndex() == 1:
                out = ensure_bidirectional_replication(cfg)
            else:
                out = ensure_logical_replication(cfg)
            try:
                progress.close()
            except Exception:
                pass
            try:
                pretty = json.dumps(out, ensure_ascii=False, indent=2)
            except Exception:
                pretty = str(out)
            QMessageBox.information(self, "Replicación", pretty)
        except Exception as e:
            QMessageBox.critical(self, "Error en replicación", str(e))

    def _on_health_check(self):
        try:
            # Usar helpers del script verify_replication_health y resolvers de replication_setup
            import importlib
            vmod = importlib.import_module('scripts.verify_replication_health')
            from utils_modules.replication_setup import resolve_local_credentials, resolve_remote_credentials
            cfg = vmod.load_cfg()
            out = {}
            progress = QProgressDialog("Verificando salud de replicación…", None, 0, 0, self)
            progress.setWindowTitle("Salud de replicación")
            progress.setWindowModality(Qt.WindowModality.ApplicationModal)
            progress.setMinimumDuration(0)
            progress.setAutoClose(True)
            progress.show()
            QApplication.processEvents()
            try:
                local_params = resolve_local_credentials(cfg)
                with vmod.connect(local_params) as lconn:
                    out['local_pg_stat_subscription'] = vmod.read_local_subscription(lconn)
            except Exception as e:
                out['local_error'] = str(e)
            try:
                remote_params = resolve_remote_credentials(cfg)
                with vmod.connect(remote_params) as rconn:
                    out['remote_replication'] = vmod.read_remote_replication(rconn)
            except Exception as e:
                out['remote_error'] = str(e)
            try:
                progress.close()
            except Exception:
                pass
            out['ok'] = True
            try:
                pretty = json.dumps(out, ensure_ascii=False, indent=2, default=str)
            except Exception:
                pretty = str(out)
            QMessageBox.information(self, "Salud de replicación", pretty)
        except Exception as e:
            QMessageBox.critical(self, "Error verificando salud", str(e))

    def _on_detect_device(self):
        try:
            try:
                from device_id import get_device_id
                dev = get_device_id()
            except Exception:
                import uuid
                dev = str(uuid.uuid4())
            self.device_id_edit.setText(str(dev))
            QMessageBox.information(self, "Device ID", f"Device ID detectado: {dev}")
        except Exception as e:
            QMessageBox.critical(self, "Error", f"No se pudo detectar el device_id: {e}")

    def _on_ensure_prereq(self):
        try:
            dev = self.device_id_edit.text().strip()
            if not dev:
                try:
                    from device_id import get_device_id
                    dev = get_device_id()
                    self.device_id_edit.setText(str(dev))
                except Exception:
                    pass
            try:
                if dev:
                    from device_id import set_device_id
                    set_device_id(dev)
            except Exception:
                pass
            from utils_modules.prerequisites import ensure_prerequisites
            progress = QProgressDialog("Asegurando prerequisitos…", None, 0, 0, self)
            progress.setWindowTitle("Prerequisitos")
            progress.setWindowModality(Qt.WindowModality.ApplicationModal)
            progress.setMinimumDuration(0)
            progress.setAutoClose(True)
            progress.show()
            QApplication.processEvents()
            res = ensure_prerequisites(dev or "unknown")
            try:
                progress.close()
            except Exception:
                pass
            try:
                pretty = json.dumps(res, ensure_ascii=False, indent=2, default=str)
            except Exception:
                pretty = str(res)
            QMessageBox.information(self, "Prerequisitos", pretty)
        except Exception as e:
            QMessageBox.critical(self, "Error", f"Fallo al asegurar prerequisitos: {e}")

    def _on_secure_owner_local(self):
        try:
            params = self._get_profile_params('local')
            progress = QProgressDialog("Asegurando Dueño (local)…", None, 0, 0, self)
            progress.setWindowTitle("Dueño local")
            progress.setWindowModality(Qt.WindowModality.ApplicationModal)
            progress.setMinimumDuration(0)
            progress.setAutoClose(True)
            progress.show()
            QApplication.processEvents()
            out = self._secure_owner_on_params(params)
            try:
                progress.close()
            except Exception:
                pass
            try:
                pretty = json.dumps(out, ensure_ascii=False, indent=2, default=str)
            except Exception:
                pretty = str(out)
            QMessageBox.information(self, "Asegurar Dueño (Local)", pretty)
        except Exception as e:
            QMessageBox.critical(self, "Error", f"No se pudo asegurar Dueño (local): {e}")

    def _on_secure_owner_remote(self):
        try:
            params = self._get_profile_params('remoto')
            host = str(params.get('host', '')).strip()
            if not host:
                QMessageBox.warning(self, "Config remota faltante", "Configura primero el perfil REMOTO (host/puerto/etc.).")
                return
            progress = QProgressDialog("Asegurando Dueño (remoto)…", None, 0, 0, self)
            progress.setWindowTitle("Dueño remoto")
            progress.setWindowModality(Qt.WindowModality.ApplicationModal)
            progress.setMinimumDuration(0)
            progress.setAutoClose(True)
            progress.show()
            QApplication.processEvents()
            out = self._secure_owner_on_params(params)
            try:
                progress.close()
            except Exception:
                pass
            try:
                pretty = json.dumps(out, ensure_ascii=False, indent=2, default=str)
            except Exception:
                pretty = str(out)
            QMessageBox.information(self, "Asegurar Dueño (Remoto)", pretty)
        except Exception as e:
            QMessageBox.critical(self, "Error", f"No se pudo asegurar Dueño (remoto): {e}")

    def _on_full_bootstrap(self):
        try:
            dev = self.device_id_edit.text().strip()
            if not dev:
                try:
                    from device_id import get_device_id
                    dev = get_device_id()
                    self.device_id_edit.setText(str(dev))
                except Exception:
                    dev = "unknown"
            else:
                try:
                    from device_id import set_device_id
                    set_device_id(dev)
                except Exception:
                    pass
            out = {
                "ok": False,
                "device_id": dev,
                "ensure_prerequisites": None,
                "replication_health": None,
            }
            try:
                from utils_modules.prerequisites import ensure_prerequisites
                progress = QProgressDialog("Asegurando prerequisitos…", None, 0, 0, self)
                progress.setWindowTitle("Bootstrap")
                progress.setWindowModality(Qt.WindowModality.ApplicationModal)
                progress.setMinimumDuration(0)
                progress.setAutoClose(True)
                progress.show()
                QApplication.processEvents()
                prereq_res = ensure_prerequisites(dev)
                try:
                    progress.close()
                except Exception:
                    pass
                out["ensure_prerequisites"] = prereq_res
            except Exception as e:
                out["ensure_prerequisites"] = {"ok": False, "error": str(e)}
            try:
                import importlib
                vmod = importlib.import_module('scripts.verify_replication_health')
                from utils_modules.replication_setup import resolve_local_credentials, resolve_remote_credentials
                cfg = vmod.load_cfg()
                health = {}
                progress2 = QProgressDialog("Verificando salud de replicación…", None, 0, 0, self)
                progress2.setWindowTitle("Bootstrap")
                progress2.setWindowModality(Qt.WindowModality.ApplicationModal)
                progress2.setMinimumDuration(0)
                progress2.setAutoClose(True)
                progress2.show()
                QApplication.processEvents()
                try:
                    local_params = resolve_local_credentials(cfg)
                    with vmod.connect(local_params) as lconn:
                        health['local_pg_stat_subscription'] = vmod.read_local_subscription(lconn)
                except Exception as e:
                    health['local_error'] = str(e)
                try:
                    remote_params = resolve_remote_credentials(cfg)
                    with vmod.connect(remote_params) as rconn:
                        health['remote_replication'] = vmod.read_remote_replication(rconn)
                except Exception as e:
                    health['remote_error'] = str(e)
                try:
                    progress2.close()
                except Exception:
                    pass
                out["replication_health"] = health
            except Exception as e:
                out["replication_health"] = {"error": str(e)}
            out["ok"] = True
            try:
                pretty = json.dumps(out, ensure_ascii=False, indent=2, default=str)
            except Exception:
                pretty = str(out)
            QMessageBox.information(self, "Bootstrap completo", pretty)
        except Exception as e:
            QMessageBox.critical(self, "Error", f"Fallo en bootstrap: {e}")

    def _run_powershell_admin(self, script_rel_path: str) -> tuple[bool, str]:
        try:
            base = Path(sys.executable).resolve().parent if getattr(sys, 'frozen', False) else Path(__file__).resolve().parent
            script_path = (base / script_rel_path).resolve()
            if not script_path.exists():
                return False, f"Script no encontrado: {script_path}"
            # Intento 1: ShellExecuteW runas
            try:
                import ctypes
                ShellExecute = ctypes.windll.shell32.ShellExecuteW
                r = ShellExecute(None, "runas", "powershell.exe", f"-ExecutionPolicy Bypass -File \"{str(script_path)}\"", None, 1)
                # ShellExecuteW returns value >32 on success
                if r > 32:
                    return True, f"Elevado y lanzado: {script_path.name}"
                else:
                    # Continue to fallback
                    pass
            except Exception:
                pass
            # Intento 2: Start-Process -Verb RunAs
            try:
                import subprocess
                cmd = [
                    "powershell", "-NoProfile", "-ExecutionPolicy", "Bypass",
                    "-Command",
                    f"Start-Process PowerShell -Verb RunAs -ArgumentList '-NoProfile','-ExecutionPolicy','Bypass','-File','{str(script_path)}'"
                ]
                subprocess.Popen(cmd, shell=False)
                return True, f"Elevado y lanzado (fallback): {script_path.name}"
            except Exception as e2:
                return False, f"No se pudo elevar: {e2}"
        except Exception as e:
            return False, f"Error: {e}"

    def _on_install_wireguard_admin(self):
        ok, msg = self._run_powershell_admin('scripts/setup_wireguard_client.ps1')
        if ok:
            QMessageBox.information(self, "WireGuard", msg)
        else:
            QMessageBox.critical(self, "WireGuard", msg)

    def _on_admin_vpn_postgres(self):
        # Confirmación previa por operación administrativa
        ret = QMessageBox.question(
            self,
            "Confirmar acción",
            "¿Deseas ejecutar la configuración administrativa de VPN + PostgreSQL?",
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
            QMessageBox.StandardButton.No,
        )
        if ret != QMessageBox.StandardButton.Yes:
            return
        progress = QProgressDialog("Configurando VPN + PostgreSQL…", None, 0, 0, self)
        progress.setWindowTitle("Configuración administrativa")
        progress.setWindowModality(Qt.WindowModality.ApplicationModal)
        progress.setMinimumDuration(0)
        progress.setAutoClose(True)
        progress.show()
        QApplication.processEvents()
        ok, msg = self._run_powershell_admin('scripts/admin_setup_vpn_postgres.ps1')
        try:
            progress.close()
        except Exception:
            pass
        if ok:
            QMessageBox.information(self, "VPN + PostgreSQL", msg)
        else:
            QMessageBox.critical(self, "VPN + PostgreSQL", msg)

    # Función legacy eliminada - replicación nativa PostgreSQL ya configurada

    def _run_python_script(self, script_rel_path: str, args: list[str] | None = None) -> tuple[bool, str, str]:
        try:
            base = Path(sys.executable).resolve().parent if getattr(sys, 'frozen', False) else Path(__file__).resolve().parent
            script_path = (base / script_rel_path).resolve()
            if not script_path.exists():
                return False, "", f"Script no encontrado: {script_path}"
            import subprocess
            py = sys.executable if not getattr(sys, 'frozen', False) else 'python'
            cmd = [py, str(script_path)] + list(args or [])
            proc = subprocess.run(cmd, cwd=str(base), capture_output=True, text=True, shell=False)
            ok = proc.returncode == 0
            return ok, (proc.stdout or '').strip(), (proc.stderr or '').strip()
        except Exception as e:
            return False, "", str(e)

    # Función legacy eliminada - replicación nativa PostgreSQL maneja sincronización automáticamente

    def _on_reconcile_remote_to_local_once(self):
        # Importar módulo y ejecutar run_once capturando stdout para mensajes claros
        try:
            import importlib
            mod = importlib.import_module('scripts.reconcile_remote_to_local_once')
        except Exception as e:
            QMessageBox.critical(self, "Reconciliación remoto→local", f"No se pudo importar script: {e}")
            return
        try:
            cfg = self._load_full_config()
            rep = (cfg.get('replication') or {}) if isinstance(cfg, dict) else {}
            sub = (rep.get('subscription_name') or self.subscription_name_edit.text().strip() or 'gym_sub')
            buf = io.StringIO()
            progress = QProgressDialog("Reconciliando remoto→local…", None, 0, 0, self)
            progress.setWindowTitle("Reconciliación")
            progress.setWindowModality(Qt.WindowModality.ApplicationModal)
            progress.setMinimumDuration(0)
            progress.setAutoClose(True)
            progress.show()
            QApplication.processEvents()
            with contextlib.redirect_stdout(buf):
                # Ejecutar con tablas por defecto y sin dry-run
                mod.run_once(subscription=sub, dry_run=False)
            try:
                progress.close()
            except Exception:
                pass
            text = buf.getvalue().strip() or "Reconciliación R→L completada."
            QMessageBox.information(self, "Reconciliación remoto→local", text)
        except Exception as e:
            QMessageBox.critical(self, "Reconciliación remoto→local", f"Fallo ejecutando: {e}")

    def _on_reconcile_local_to_remote_once(self):
        # Importar módulo y ejecutar run_once capturando stdout para mensajes claros
        try:
            import importlib
            mod = importlib.import_module('scripts.reconcile_local_remote_once')
        except Exception as e:
            QMessageBox.critical(self, "Reconciliación local→remoto", f"No se pudo importar script: {e}")
            return
        try:
            cfg = self._load_full_config()
            rep = (cfg.get('replication') or {}) if isinstance(cfg, dict) else {}
            sub = (rep.get('subscription_name') or self.subscription_name_edit.text().strip() or 'gym_sub')
            buf = io.StringIO()
            progress = QProgressDialog("Reconciliando local→remoto…", None, 0, 0, self)
            progress.setWindowTitle("Reconciliación")
            progress.setWindowModality(Qt.WindowModality.ApplicationModal)
            progress.setMinimumDuration(0)
            progress.setAutoClose(True)
            progress.show()
            QApplication.processEvents()
            with contextlib.redirect_stdout(buf):
                mod.run_once(subscription=sub, dry_run=False)
            try:
                progress.close()
            except Exception:
                pass
            text = buf.getvalue().strip() or "Reconciliación L→R completada."
            QMessageBox.information(self, "Reconciliación local→remoto", text)
        except Exception as e:
            QMessageBox.critical(self, "Reconciliación local→remoto", f"Fallo ejecutando: {e}")

    def _on_test_wireguard(self):
        # Prueba de instalación/adaptadores WireGuard y conectividad hacia remoto (puerto PostgreSQL)
        try:
            results = {}
            # Detectar instalación de WireGuard
            candidates = [
                Path(os.getenv('ProgramFiles', 'C\\Program Files')) / 'WireGuard' / 'wireguard.exe',
                Path(os.getenv('ProgramFiles(x86)', 'C\\Program Files (x86)')) / 'WireGuard' / 'wireguard.exe',
            ]
            results['wireguard_installed'] = any(p.exists() for p in candidates)

            # Enumerar adaptadores WireGuard via PowerShell si disponible
            adapters = []
            try:
                import subprocess
                cmd = [
                    'powershell', '-NoProfile', '-Command',
                    'Get-NetAdapter | Select-Object Name,Status,InterfaceDescription | ConvertTo-Json -Compress'
                ]
                proc = subprocess.run(cmd, capture_output=True, text=True, shell=False)
                if proc.returncode == 0 and proc.stdout.strip():
                    try:
                        arr = json.loads(proc.stdout)
                        if isinstance(arr, dict):
                            arr = [arr]
                        for it in arr or []:
                            desc = str(it.get('InterfaceDescription', ''))
                            name = str(it.get('Name', ''))
                            status = str(it.get('Status', ''))
                            if ('WireGuard' in desc) or ('WireGuard' in name):
                                adapters.append({'name': name, 'status': status})
                    except Exception:
                        pass
            except Exception:
                pass
            results['wireguard_adapters'] = adapters

            # Probar conectividad TCP y conexión DB a remoto
            cfg = self._load_full_config()
            remote = (cfg.get('db_remote') or {}) if isinstance(cfg, dict) else {}
            host = str(remote.get('host') or cfg.get('host') or '').strip()
            try:
                port = int(remote.get('port') or cfg.get('port') or 5432)
            except Exception:
                port = 5432
            tcp_ok = False
            tcp_err = None
            if host:
                try:
                    with socket.create_connection((host, port), timeout=3):
                        tcp_ok = True
                except Exception as e:
                    tcp_err = str(e)
            results['tcp_connect_remote_pg'] = {'host': host, 'port': port, 'ok': tcp_ok, 'error': tcp_err}

            # Conexión DB
            db_ok = False
            db_err = None
            try:
                params = self._get_profile_params('remoto')
                if str(params.get('host', '')).strip():
                    conn = psycopg2.connect(
                        host=params.get('host'),
                        port=int(params.get('port') or 5432),
                        dbname=params.get('database'),
                        user=params.get('user'),
                        password=params.get('password'),
                        sslmode=params.get('sslmode') or 'require',
                        application_name=params.get('application_name', 'cdbconfig_wireguard_test'),
                        connect_timeout=3,
                    )
                    try:
                        with conn.cursor() as cur:
                            cur.execute('SELECT 1')
                            cur.fetchone()
                        db_ok = True
                    finally:
                        try:
                            conn.close()
                        except Exception:
                            pass
            except Exception as e:
                db_err = str(e)
            results['db_connection'] = {'ok': db_ok, 'error': db_err}

            try:
                pretty = json.dumps(results, ensure_ascii=False, indent=2, default=str)
            except Exception:
                pretty = str(results)
            QMessageBox.information(self, 'Prueba WireGuard/VPN', pretty)
        except Exception as e:
            QMessageBox.critical(self, 'Prueba WireGuard/VPN', f'Fallo en prueba: {e}')

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

        # Persistir configuración de tareas programadas desde la UI
        try:
            existing_cfg['scheduled_tasks'] = self._collect_tasks_cfg()
        except Exception:
            pass

        # Persistir configuración general
        try:
            base_url = self.webapp_base_url_edit.text().strip()
            client_url = self.client_base_url_edit.text().strip()
            sync_token = self.sync_upload_token_edit.text().strip()
            session_secret = self.webapp_session_secret_edit.text().strip()
            if base_url:
                existing_cfg['webapp_base_url'] = base_url
            if client_url:
                existing_cfg['client_base_url'] = client_url
            if sync_token:
                existing_cfg['sync_upload_token'] = sync_token
            if session_secret:
                existing_cfg['webapp_session_secret'] = session_secret
            existing_cfg['public_tunnel'] = existing_cfg.get('public_tunnel', {}) or {}
            existing_cfg['public_tunnel']['enabled'] = bool(self.tunnel_enabled_checkbox.isChecked())
            existing_cfg['public_tunnel']['subdomain'] = self.tunnel_subdomain_edit.text().strip()
        except Exception:
            pass

        # Persistir configuración de replicación
        try:
            rep = existing_cfg.get('replication', {}) or {}
            pub = self.publication_name_edit.text().strip() or rep.get('publication_name') or 'gym_pub'
            sub = self.subscription_name_edit.text().strip() or rep.get('subscription_name') or 'gym_sub'
            rep['publication_name'] = pub
            rep['subscription_name'] = sub
            rep['remote_can_reach_local'] = bool(self.remote_can_reach_local_checkbox.isChecked())
            # Guardar pistas de modo bidireccional si corresponde
            if self.replication_mode_combo.currentIndex() == 1:
                rep.setdefault('local_publication_name', pub)
                rep.setdefault('remote_subscription_name', sub)
            existing_cfg['replication'] = rep
        except Exception:
            pass

        merged_cfg = existing_cfg
        with open(config_path, 'w', encoding='utf-8') as f:
            json.dump(merged_cfg, f, ensure_ascii=False, indent=2)

        # Guardar contraseña en almacén seguro (Windows Credential Manager)
        if keyring is None:
            return
        try:
            accounts = []
            if str(params.get('profile', 'local')).lower() == 'local':
                accounts = [
                    params['user'],
                    f"{params['user']}@{params['host']}:{params['port']}",
                ]
            else:
                accounts = [
                    f"{params['user']}@railway",
                    f"{params['user']}@{params['host']}:{params['port']}",
                    params['user'],
                ]
            for acct in accounts:
                try:
                    keyring.set_password(KEYRING_SERVICE_NAME, acct, params['password'])
                except Exception:
                    pass
        except Exception:
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

    # --- NUEVO: Configuración de tareas programadas ---
    def _ensure_tasks_defaults(self, scfg: dict | None) -> dict:
        base = scfg.copy() if isinstance(scfg, dict) else {}
        # Maestro
        base.setdefault('enabled', False)
        # Subtareas con valores por defecto
        uploader = base.get('uploader') if isinstance(base.get('uploader'), dict) else {}
        uploader.setdefault('enabled', False)
        uploader.setdefault('interval_minutes', 15)
        base['uploader'] = uploader
        # Legacy reconcile → nuevos campos
        legacy = base.get('reconcile') if isinstance(base.get('reconcile'), dict) else None
        r2l = base.get('reconcile_r2l') if isinstance(base.get('reconcile_r2l'), dict) else {}
        l2r = base.get('reconcile_l2r') if isinstance(base.get('reconcile_l2r'), dict) else {}
        if legacy and not r2l and not l2r:
            try:
                r2l = {
                    'enabled': bool(legacy.get('enabled', False)),
                    'interval_minutes': int(legacy.get('interval_minutes', 60)),
                }
                l2r = {
                    'enabled': bool(legacy.get('enabled', False)),
                    'time': '02:00',
                }
            except Exception:
                pass
        r2l.setdefault('enabled', False)
        r2l.setdefault('interval_minutes', 60)
        base['reconcile_r2l'] = r2l
        l2r.setdefault('enabled', False)
        l2r.setdefault('time', '02:00')
        base['reconcile_l2r'] = l2r
        # Bidireccional diaria
        bidir = base.get('reconcile_bidirectional') if isinstance(base.get('reconcile_bidirectional'), dict) else {}
        bidir.setdefault('enabled', False)
        bidir.setdefault('time', '02:15')
        base['reconcile_bidirectional'] = bidir
        cleanup = base.get('cleanup') if isinstance(base.get('cleanup'), dict) else {}
        cleanup.setdefault('enabled', False)
        cleanup.setdefault('time', '03:15')
        base['cleanup'] = cleanup
        backup = base.get('backup') if isinstance(base.get('backup'), dict) else {}
        backup.setdefault('enabled', False)
        backup.setdefault('time', '02:30')
        base['backup'] = backup
        # Semanales - replicación nativa PostgreSQL
        rep_health_w = base.get('replication_health_weekly') if isinstance(base.get('replication_health_weekly'), dict) else {}
        rep_health_w.setdefault('enabled', False)
        rep_health_w.setdefault('time', '00:45')
        rep_health_w.setdefault('days', 'SUN')
        base['replication_health_weekly'] = rep_health_w
        pub_verify_w = base.get('publication_verify_weekly') if isinstance(base.get('publication_verify_weekly'), dict) else {}
        pub_verify_w.setdefault('enabled', False)
        pub_verify_w.setdefault('time', '00:30')
        pub_verify_w.setdefault('days', 'SUN')
        base['publication_verify_weekly'] = pub_verify_w
        return base

    def _load_tasks_cfg(self):
        try:
            scfg = self._ensure_tasks_defaults(self.full_cfg.get('scheduled_tasks'))
            self.tasks_master_checkbox.setChecked(bool(scfg.get('enabled', False)))
            # Uploader
            # Replicación nativa PostgreSQL - sin uploader legacy
            # Reconcile R→L
            r2l = scfg.get('reconcile_r2l', {})
            self.reconcile_r2l_enable_checkbox.setChecked(bool(r2l.get('enabled', False)))
            self.reconcile_r2l_interval_spin.setValue(int(r2l.get('interval_minutes', 60)))
            # Reconcile L→R
            l2r = scfg.get('reconcile_l2r', {})
            t_l2r = QTime.fromString(str(l2r.get('time', '02:00')), "HH:mm")
            if not t_l2r.isValid():
                t_l2r = QTime(2, 0)
            self.reconcile_l2r_time_edit.setTime(t_l2r)
            self.reconcile_l2r_enable_checkbox.setChecked(bool(l2r.get('enabled', False)))
            # Reconcile Bidireccional
            bid = scfg.get('reconcile_bidirectional', {})
            t_bid = QTime.fromString(str(bid.get('time', '02:15')), "HH:mm")
            if not t_bid.isValid():
                t_bid = QTime(2, 15)
            self.reconcile_bidirectional_time_edit.setTime(t_bid)
            self.reconcile_bidirectional_enable_checkbox.setChecked(bool(bid.get('enabled', False)))
            # Cleanup time
            t_clean = QTime.fromString(str(scfg['cleanup'].get('time', '03:15')), "HH:mm")
            if not t_clean.isValid():
                t_clean = QTime(3, 15)
            self.cleanup_time_edit.setTime(t_clean)
            self.cleanup_enable_checkbox.setChecked(bool(scfg['cleanup'].get('enabled', False)))
            # Backup time
            t_backup = QTime.fromString(str(scfg['backup'].get('time', '02:30')), "HH:mm")
            if not t_backup.isValid():
                t_backup = QTime(2, 30)
            self.backup_time_edit.setTime(t_backup)
            self.backup_enable_checkbox.setChecked(bool(scfg['backup'].get('enabled', False)))
            # Replicación nativa PostgreSQL - sin outbox legacy
            # Salud replicación semanal
            rep_w = scfg.get('replication_health_weekly', {})
            t_rep_w = QTime.fromString(str(rep_w.get('time', '00:45')), "HH:mm")
            if not t_rep_w.isValid():
                t_rep_w = QTime(0, 45)
            self.replication_health_weekly_time_edit.setTime(t_rep_w)
            self.replication_health_weekly_enable_checkbox.setChecked(bool(rep_w.get('enabled', False)))
            self.replication_health_weekly_day_combo.setCurrentText(str(rep_w.get('days', 'SUN')).upper())
            # Verificación publicación semanal
            pub_w = scfg.get('publication_verify_weekly', {})
            t_pub_w = QTime.fromString(str(pub_w.get('time', '00:30')), "HH:mm")
            if not t_pub_w.isValid():
                t_pub_w = QTime(0, 30)
            self.publication_verify_weekly_time_edit.setTime(t_pub_w)
            self.publication_verify_weekly_enable_checkbox.setChecked(bool(pub_w.get('enabled', False)))
            self.publication_verify_weekly_day_combo.setCurrentText(str(pub_w.get('days', 'SUN')).upper())
            # Aplicar estado master
            self._on_tasks_master_toggled(self.tasks_master_checkbox.isChecked())
            # Aplicar exclusión si bidireccional activo
            self._on_bidir_toggled(bool(self.reconcile_bidirectional_enable_checkbox.isChecked()))
        except Exception:
            pass

    def _collect_tasks_cfg(self) -> dict:
        def fmt_time(qt: QTime) -> str:
            return f"{qt.hour():02d}:{qt.minute():02d}"
        return {
            'enabled': bool(self.tasks_master_checkbox.isChecked()),
            # Replicación nativa PostgreSQL - sin uploader legacy
            'reconcile_r2l': {
                'enabled': bool(self.reconcile_r2l_enable_checkbox.isChecked()),
                'interval_minutes': int(self.reconcile_r2l_interval_spin.value()),
            },
            'reconcile_l2r': {
                'enabled': bool(self.reconcile_l2r_enable_checkbox.isChecked()),
                'time': fmt_time(self.reconcile_l2r_time_edit.time()),
            },
            'reconcile_bidirectional': {
                'enabled': bool(self.reconcile_bidirectional_enable_checkbox.isChecked()),
                'time': fmt_time(self.reconcile_bidirectional_time_edit.time()),
            },
            'cleanup': {
                'enabled': bool(self.cleanup_enable_checkbox.isChecked()),
                'time': fmt_time(self.cleanup_time_edit.time()),
            },
            'backup': {
                'enabled': bool(self.backup_enable_checkbox.isChecked()),
                'time': fmt_time(self.backup_time_edit.time()),
            },
            # Replicación nativa PostgreSQL - sin outbox legacy
            'replication_health_weekly': {
                'enabled': bool(self.replication_health_weekly_enable_checkbox.isChecked()),
                'time': fmt_time(self.replication_health_weekly_time_edit.time()),
                'days': str(self.replication_health_weekly_day_combo.currentText()).upper(),
            },
            'publication_verify_weekly': {
                'enabled': bool(self.publication_verify_weekly_enable_checkbox.isChecked()),
                'time': fmt_time(self.publication_verify_weekly_time_edit.time()),
                'days': str(self.publication_verify_weekly_day_combo.currentText()).upper(),
            },
        }

    def _on_tasks_master_toggled(self, checked: bool):
        try:
            for w in [
                self.reconcile_r2l_enable_checkbox, self.reconcile_r2l_interval_spin,
                self.reconcile_l2r_enable_checkbox, self.reconcile_l2r_time_edit,
                self.reconcile_bidirectional_enable_checkbox, self.reconcile_bidirectional_time_edit,
                self.cleanup_enable_checkbox, self.cleanup_time_edit,
                self.backup_enable_checkbox, self.backup_time_edit,
                self.replication_health_weekly_enable_checkbox, self.replication_health_weekly_time_edit, self.replication_health_weekly_day_combo,
                self.publication_verify_weekly_enable_checkbox, self.publication_verify_weekly_time_edit, self.publication_verify_weekly_day_combo,
            ]:
                w.setEnabled(checked)
            # Si bidireccional está activo, mantén R→L y L→R bloqueados tras cambiar el master
            self._on_bidir_toggled(bool(self.reconcile_bidirectional_enable_checkbox.isChecked()))
        except Exception:
            pass

    def _on_bidir_toggled(self, checked: bool):
        """Cuando se activa Bidireccional, desactiva R→L y L→R y bloquea sus controles."""
        try:
            # Estados
            if checked:
                # Desactivar individuales para evitar solapamientos
                self.reconcile_r2l_enable_checkbox.setChecked(False)
                self.reconcile_l2r_enable_checkbox.setChecked(False)
                # Bloquear controles individuales
                self.reconcile_r2l_enable_checkbox.setEnabled(False)
                self.reconcile_r2l_interval_spin.setEnabled(False)
                self.reconcile_l2r_enable_checkbox.setEnabled(False)
                self.reconcile_l2r_time_edit.setEnabled(False)
                try:
                    QMessageBox.information(self, "Reconciliación Bidireccional", "Se desactivaron las tareas individuales R→L y L→R para evitar solapamientos.")
                except Exception:
                    pass
            else:
                # Rehabilitar controles individuales
                self.reconcile_r2l_enable_checkbox.setEnabled(True)
                self.reconcile_r2l_interval_spin.setEnabled(True)
                self.reconcile_l2r_enable_checkbox.setEnabled(True)
                self.reconcile_l2r_time_edit.setEnabled(True)
        except Exception:
            pass

    def _on_individual_reconcile_toggled(self, checked: bool):
        """Si se activa R→L o L→R, desactiva Bidireccional para mantener exclusión."""
        try:
            if checked and bool(self.reconcile_bidirectional_enable_checkbox.isChecked()):
                # Evitar reentrada de señales al cambiar bidireccional
                prev_block = False
                try:
                    prev_block = self.reconcile_bidirectional_enable_checkbox.blockSignals(True)
                except Exception:
                    pass
                try:
                    self.reconcile_bidirectional_enable_checkbox.setChecked(False)
                finally:
                    try:
                        self.reconcile_bidirectional_enable_checkbox.blockSignals(prev_block)
                    except Exception:
                        pass
                # Aplicar efectos de desactivar bidireccional
                self._on_bidir_toggled(False)
                try:
                    QMessageBox.information(self, "Tareas de reconciliación", "Se desactivó la reconciliación bidireccional al activar R→L/L→R para evitar solapamientos.")
                except Exception:
                    pass
        except Exception:
            pass

    def _on_clear_password(self):
        try:
            user = self.user_edit.text().strip()
            if not user:
                QMessageBox.warning(self, "Usuario requerido", "Indica el usuario para eliminar la contraseña guardada.")
                return
            cleared_any = False
            prof = 'local' if self.profile_combo.currentText().lower().startswith('local') else 'remoto'
            host = self.host_edit.text().strip()
            port = int(self.port_spin.value())
            # Eliminar de keyring variantes
            if keyring is not None:
                variants = [f"{user}@{host}:{port}", user] if prof == 'local' else [f"{user}@railway", f"{user}@{host}:{port}", user]
                for acct in variants:
                    try:
                        keyring.delete_password(KEYRING_SERVICE_NAME, acct)
                        cleared_any = True
                    except Exception:
                        pass
            # Eliminar de config.json (sección correspondiente)
            try:
                base_dir = Path(sys.executable).resolve().parent if getattr(sys, 'frozen', False) else Path(__file__).resolve().parent
            except Exception:
                base_dir = Path(os.getcwd())
            config_path = (base_dir / 'config' / 'config.json')
            try:
                if config_path.exists():
                    with open(config_path, 'r', encoding='utf-8') as f:
                        cfg = json.load(f) or {}
                    key = 'db_local' if prof == 'local' else 'db_remote'
                    if isinstance(cfg.get(key), dict) and 'password' in cfg[key]:
                        del cfg[key]['password']
                        cleared_any = True
                    if 'password' in cfg:
                        del cfg['password']
                        cleared_any = True
                    with open(config_path, 'w', encoding='utf-8') as f:
                        json.dump(cfg, f, ensure_ascii=False, indent=2)
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

            prof = 'local' if self.profile_combo.currentText().lower().startswith('local') else 'remoto'
            host = self.host_edit.text().strip()
            port = int(self.port_spin.value())
            variants = [f"{user}@{host}:{port}", user] if prof == 'local' else [f"{user}@railway", f"{user}@{host}:{port}", user]
            saved = None
            for acct in variants:
                try:
                    saved = keyring.get_password(KEYRING_SERVICE_NAME, acct)
                except Exception:
                    saved = None
                if saved:
                    break
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

    # --- Limpieza peligrosa de bases de datos ---
    def _resolve_password_for(self, params: dict, scope_hint: str) -> dict:
        try:
            if str(params.get('password', '')).strip():
                return params
            if keyring is None:
                return params
            user = str(params.get('user', 'postgres'))
            host = str(params.get('host', ''))
            try:
                port = int(params.get('port', 5432))
            except Exception:
                port = 5432
            variants = [f"{user}@{host}:{port}", user] if scope_hint == 'local' else [f"{user}@railway", f"{user}@{host}:{port}", user]
            for acct in variants:
                try:
                    saved = keyring.get_password(KEYRING_SERVICE_NAME, acct)
                except Exception:
                    saved = None
                if saved:
                    params['password'] = saved
                    break
        except Exception:
            pass
        return params

    def _get_profile_params(self, profile: str) -> dict:
        base = self._ensure_db_local_defaults(self.db_local_cfg) if profile == 'local' else self._ensure_db_remote_defaults(self.db_remote_cfg)
        return self._resolve_password_for(base, profile)

    def _truncate_public_tables(self, params: dict) -> dict:
        conn = None
        try:
            conn = psycopg2.connect(
                host=params.get('host'),
                port=int(params.get('port') or 5432),
                dbname=params.get('database'),
                user=params.get('user'),
                password=params.get('password'),
                sslmode=params.get('sslmode') or 'prefer',
                application_name=params.get('application_name', 'gym_management_system'),
                connect_timeout=int(params.get('connect_timeout') or 10),
            )
            conn.autocommit = False
            cur = conn.cursor()

            # Listado de tablas public (incluye base y particionadas), excluye posibles tablas sym_* legadas
            cur.execute(
                """
                SELECT c.relname
                FROM pg_class c
                JOIN pg_namespace n ON n.oid = c.relnamespace
                WHERE n.nspname='public' AND c.relkind IN ('r','p')
                  AND c.relname NOT LIKE 'sym_%'
                ORDER BY 1
                """
            )
            tables = [r[0] for r in (cur.fetchall() or [])]

            # Excluir tablas de configuración para preservar settings críticos (owner_password, branding, numeración)
            tables = [t for t in tables if t not in ('configuracion', 'configuracion_comprobantes', 'configuraciones')]

            truncated = []
            if tables:
                try:
                    parts = [sql.SQL("{}.{}").format(sql.Identifier('public'), sql.Identifier(t)) for t in tables]
                    stmt = sql.SQL("TRUNCATE TABLE ") + sql.SQL(", ").join(parts) + sql.SQL(" RESTART IDENTITY CASCADE")
                    cur.execute(stmt)
                    truncated = tables
                except Exception:
                    # Fallback conservador: borrar por tabla si TRUNCATE falla
                    for t in tables:
                        try:
                            cur.execute(sql.SQL("DELETE FROM {}.{}").format(sql.Identifier('public'), sql.Identifier(t)))
                        except Exception:
                            pass

            # Reiniciar secuencias adicionales en public
            restarted = []
            try:
                cur.execute(
                    """
                    SELECT sequence_name
                    FROM information_schema.sequences
                    WHERE sequence_schema='public'
                    """
                )
                seqs = [r[0] for r in (cur.fetchall() or [])]
                for s in seqs:
                    try:
                        cur.execute(sql.SQL("ALTER SEQUENCE {}.{} RESTART WITH 1").format(sql.Identifier('public'), sql.Identifier(s)))
                        restarted.append(s)
                    except Exception:
                        pass
            except Exception:
                restarted = []

            conn.commit()
            return {"ok": True, "truncated": truncated, "restarted_sequences": restarted}
        except Exception as e:
            try:
                if conn:
                    conn.rollback()
            except Exception:
                pass
            return {"ok": False, "error": str(e)}
        finally:
            try:
                if conn:
                    conn.close()
            except Exception:
                pass

    def _secure_owner_on_params(self, params: dict) -> dict:
        conn = None
        try:
            conn = psycopg2.connect(
                host=params.get('host'),
                port=int(params.get('port') or 5432),
                dbname=params.get('database'),
                user=params.get('user'),
                password=params.get('password'),
                sslmode=params.get('sslmode') or 'prefer',
                application_name=params.get('application_name', 'gym_management_system'),
                connect_timeout=int(params.get('connect_timeout') or 10),
            )
            conn.autocommit = False
            cur = conn.cursor()

            # 1) Soltar triggers/funciones defensivas si existen para permitir inserción
            try:
                cur.execute("DROP TRIGGER IF EXISTS trg_usuarios_bloquear_ins_upd_dueno ON usuarios")
            except Exception:
                pass
            try:
                cur.execute("DROP TRIGGER IF EXISTS trg_usuarios_bloquear_del_dueno ON usuarios")
            except Exception:
                pass
            try:
                cur.execute("DROP FUNCTION IF EXISTS usuarios_bloquear_dueno_ins_upd()")
            except Exception:
                pass
            try:
                cur.execute("DROP FUNCTION IF EXISTS usuarios_bloquear_dueno_delete()")
            except Exception:
                pass

            # 2) Asegurar tabla usuarios existe
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS usuarios (
                    id SERIAL PRIMARY KEY,
                    nombre VARCHAR(255) NOT NULL,
                    dni VARCHAR(20) UNIQUE,
                    telefono VARCHAR(50) NOT NULL,
                    pin VARCHAR(10) DEFAULT '1234',
                    rol VARCHAR(50) DEFAULT 'socio' NOT NULL,
                    notas TEXT,
                    fecha_registro TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    activo BOOLEAN DEFAULT TRUE,
                    tipo_cuota VARCHAR(100) DEFAULT 'estandar',
                    ultimo_pago DATE,
                    fecha_proximo_vencimiento DATE,
                    cuotas_vencidas INTEGER DEFAULT 0
                )
                """
            )

            # 3) Insertar Dueño si no existe (antes de reinstalar protecciones)
            cur.execute("SELECT 1 FROM usuarios WHERE rol = 'dueño'")
            has_owner = bool(cur.fetchone())
            if not has_owner:
                cur.execute(
                    """INSERT INTO usuarios (nombre, dni, telefono, pin, rol, activo, tipo_cuota)
                           VALUES (%s, %s, %s, %s, %s, %s, %s)""",
                    ("DUEÑO DEL GIMNASIO", "00000000", "N/A", "2203", "dueño", True, "estandar")
                )

            # 4) Reinstalar RLS y políticas bloqueando filas 'dueño'
            cur.execute(
                """
                ALTER TABLE usuarios ENABLE ROW LEVEL SECURITY;
                ALTER TABLE usuarios FORCE ROW LEVEL SECURITY;

                DROP POLICY IF EXISTS usuarios_block_owner_select ON usuarios;
                DROP POLICY IF EXISTS usuarios_block_owner_update ON usuarios;
                DROP POLICY IF EXISTS usuarios_block_owner_delete ON usuarios;
                DROP POLICY IF EXISTS usuarios_block_owner_insert ON usuarios;

                CREATE POLICY usuarios_block_owner_select ON usuarios
                    FOR SELECT
                    USING (rol IS DISTINCT FROM 'dueño');

                CREATE POLICY usuarios_block_owner_update ON usuarios
                    FOR UPDATE
                    USING (rol IS DISTINCT FROM 'dueño')
                    WITH CHECK (rol IS DISTINCT FROM 'dueño');

                CREATE POLICY usuarios_block_owner_delete ON usuarios
                    FOR DELETE
                    USING (rol IS DISTINCT FROM 'dueño');

                CREATE POLICY usuarios_block_owner_insert ON usuarios
                    FOR INSERT
                    WITH CHECK (rol IS DISTINCT FROM 'dueño');
                """
            )

            # 5) Crear funciones y triggers defensivos
            cur.execute(
                """
                CREATE FUNCTION usuarios_bloquear_dueno_ins_upd() RETURNS trigger AS $$
                BEGIN
                    IF NEW.rol = 'dueño' THEN
                        RAISE EXCEPTION 'Operación no permitida: los usuarios con rol "dueño" son inafectables';
                    END IF;
                    RETURN NEW;
                END;
                $$ LANGUAGE plpgsql;

                CREATE FUNCTION usuarios_bloquear_dueno_delete() RETURNS trigger AS $$
                BEGIN
                    IF OLD.rol = 'dueño' THEN
                        RAISE EXCEPTION 'Operación no permitida: los usuarios con rol "dueño" no pueden eliminarse';
                    END IF;
                    RETURN OLD;
                END;
                $$ LANGUAGE plpgsql;

                CREATE TRIGGER trg_usuarios_bloquear_ins_upd_dueno
                BEFORE INSERT OR UPDATE ON usuarios
                FOR EACH ROW EXECUTE FUNCTION usuarios_bloquear_dueno_ins_upd();

                CREATE TRIGGER trg_usuarios_bloquear_del_dueno
                BEFORE DELETE ON usuarios
                FOR EACH ROW EXECUTE FUNCTION usuarios_bloquear_dueno_delete();
                """
            )

            

            # 7) Asegurar tabla configuracion y sembrar owner_password si existe en entorno
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS configuracion (
                    id SERIAL PRIMARY KEY,
                    clave VARCHAR(255) UNIQUE NOT NULL,
                    valor TEXT NOT NULL,
                    tipo VARCHAR(50) DEFAULT 'string',
                    descripcion TEXT
                )
                """
            )
            env_pwd = (os.getenv('WEBAPP_OWNER_PASSWORD', '') or os.getenv('OWNER_PASSWORD', '')).strip()
            if env_pwd:
                cur.execute(
                    """
                    INSERT INTO configuracion (clave, valor, tipo, descripcion)
                    VALUES (%s, %s, 'string', 'Contraseña de acceso del dueño')
                    ON CONFLICT (clave) DO NOTHING
                    """,
                    ('owner_password', env_pwd)
                )

            # Validación básica: existencia de Dueño y triggers/políticas
            cur.execute("SELECT COUNT(*) FROM usuarios WHERE rol = 'dueño'")
            owner_count = int((cur.fetchone() or [0])[0])
            cur.execute("SELECT polname FROM pg_policies WHERE schemaname='public' AND tablename='usuarios'")
            pols = {r[0] for r in (cur.fetchall() or [])}
            cur.execute("SELECT tgname FROM pg_trigger WHERE tgrelid = 'usuarios'::regclass")
            trigs = {r[0] for r in (cur.fetchall() or [])}

            conn.commit()
            return {
                "ok": True,
                "owner_count": owner_count,
                "policies": sorted(list(pols)),
                "triggers": sorted(list(trigs)),
            }
        except Exception as e:
            try:
                if conn:
                    conn.rollback()
            except Exception:
                pass
            return {"ok": False, "error": str(e)}
        finally:
            try:
                if conn:
                    conn.close()
            except Exception:
                pass

    def _on_cleanup_databases(self):
        try:
            # Primera confirmación
            resp = QMessageBox.warning(
                self,
                "Confirmar limpieza peligrosa",
                (
                    "Esta acción TRUNCARÁ todos los datos de las tablas en 'public' "
                    "(RESTART IDENTITY, CASCADE) en las bases configuradas.\n\n"
                    "- Publicación actual incluye 'truncate' para replicación (si hay suscriptores).\n"
                    "- Se reiniciarán secuencias en 'public'.\n\n"
                    "¿Deseas continuar?"
                ),
                QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
                QMessageBox.StandardButton.No,
            )
            if resp != QMessageBox.StandardButton.Yes:
                return

            # Segunda confirmación por frase
            phrase, ok = QInputDialog.getText(
                self,
                "Confirmación final",
                "Escribe 'BORRAR TODO' para confirmar:",
                QLineEdit.EchoMode.Normal,
            )
            if not ok or phrase.strip().upper() != "BORRAR TODO":
                QMessageBox.information(self, "Cancelado", "Operación cancelada.")
                return

            # Requiere contraseña admin
            pwd, ok = QInputDialog.getText(
                self,
                "Autorización",
                "Contraseña de administrador:",
                QLineEdit.EchoMode.Password,
            )
            if not ok or pwd != "Matute03!?":
                QMessageBox.warning(self, "Denegado", "Contraseña incorrecta.")
                return

            # Ejecutar limpieza en local y remoto (si configurado)
            local_params = self._get_profile_params('local')
            remote_params = self._get_profile_params('remoto')
            progress = QProgressDialog("Ejecutando limpieza…", None, 0, 4, self)
            progress.setWindowTitle("Limpieza")
            progress.setWindowModality(Qt.WindowModality.ApplicationModal)
            progress.setMinimumDuration(0)
            progress.setAutoClose(True)
            progress.setValue(0)
            progress.show()
            QApplication.processEvents()

            results = []
            # Local
            try:
                rloc = self._truncate_public_tables(local_params)
                if rloc.get('ok'):
                    rloc_secure = self._secure_owner_on_params(local_params)
                else:
                    rloc_secure = {"ok": False, "error": rloc.get('error') or 'No se pudo truncar'}
                results.append(("LOCAL", rloc, rloc_secure))
                try:
                    progress.setValue(1)
                    QApplication.processEvents()
                except Exception:
                    pass
            except Exception as e:
                results.append(("LOCAL", {"ok": False, "error": str(e)}, {"ok": False, "error": str(e)}))
                try:
                    progress.setValue(1)
                except Exception:
                    pass
            # Remoto si hay host informado
            try:
                if str(remote_params.get('host', '')).strip():
                    rrem = self._truncate_public_tables(remote_params)
                    if rrem.get('ok'):
                        rrem_secure = self._secure_owner_on_params(remote_params)
                    else:
                        rrem_secure = {"ok": False, "error": rrem.get('error') or 'No se pudo truncar'}
                    results.append(("REMOTO", rrem, rrem_secure))
                else:
                    results.append(("REMOTO", {"ok": False, "error": "No configurado"}, {"ok": False, "error": "No configurado"}))
                try:
                    progress.setValue(3)
                    QApplication.processEvents()
                except Exception:
                    pass
            except Exception as e:
                results.append(("REMOTO", {"ok": False, "error": str(e)}, {"ok": False, "error": str(e)}))

            try:
                progress.setValue(4)
                progress.close()
            except Exception:
                pass

            # Mostrar resumen
            lines = []
            for name, trunc_res, sec_res in results:
                if trunc_res.get('ok'):
                    lines.append(f"[{name}] TRUNCATE OK: {len(trunc_res.get('truncated', []))} tablas; {len(trunc_res.get('restarted_sequences', []))} secuencias")
                else:
                    lines.append(f"[{name}] TRUNCATE FALLÓ: {trunc_res.get('error')}")
                if sec_res.get('ok'):
                    lines.append(f"[{name}] Dueño asegurado (conteo={sec_res.get('owner_count', '?')})")
                else:
                    lines.append(f"[{name}] Asegurar Dueño FALLÓ: {sec_res.get('error')}")
            QMessageBox.information(self, "Limpieza completada", "\n".join(lines))
        except Exception as e:
            QMessageBox.critical(self, "Error", f"Fallo en limpieza: {e}")

    def _on_force_dependencies(self):
        """Force installation of dependencies"""
        try:
            from utils_modules.prerequisites import install_dependencies
            progress = QProgressDialog("Instalando dependencias...", None, 0, 0, self)
            progress.setWindowTitle("Dependencias")
            progress.setWindowModality(Qt.WindowModality.ApplicationModal)
            progress.setMinimumDuration(0)
            progress.setAutoClose(True)
            progress.show()
            QApplication.processEvents()
            
            result = install_dependencies()
            
            try:
                progress.close()
            except Exception:
                pass
                
            if result.get("ok"):
                QMessageBox.information(self, "Dependencias", "Dependencias instaladas correctamente.")
            else:
                QMessageBox.critical(self, "Dependencias", f"Error al instalar dependencias: {result.get('error')}")
        except Exception as e:
            QMessageBox.critical(self, "Error", f"Fallo al instalar dependencias: {e}")

    def _on_force_database_init(self):
        """Force database initialization"""
        try:
            from initialize_database import initialize_database
            
            progress = QProgressDialog("Inicializando base de datos...", None, 0, 0, self)
            progress.setWindowTitle("Inicialización")
            progress.setWindowModality(Qt.WindowModality.ApplicationModal)
            progress.setMinimumDuration(0)
            progress.setAutoClose(True)
            progress.show()
            QApplication.processEvents()
            
            result = initialize_database()
            
            try:
                progress.close()
            except Exception:
                pass
                
            if result.get("success"):
                QMessageBox.information(self, "Base de datos", "Base de datos inicializada correctamente.")
            else:
                QMessageBox.critical(self, "Base de datos", f"Error al inicializar base de datos: {result.get('error')}")
        except Exception as e:
            QMessageBox.critical(self, "Error", f"Fallo al inicializar base de datos: {e}")

    def _on_force_replication(self):
        """Force replication setup"""
        try:
            from utils_modules.replication_setup import ensure_bidirectional_replication
            
            # Load current config
            cfg = self._load_full_config()
            
            progress = QProgressDialog("Configurando replicación...", None, 0, 0, self)
            progress.setWindowTitle("Replicación")
            progress.setWindowModality(Qt.WindowModality.ApplicationModal)
            progress.setMinimumDuration(0)
            progress.setAutoClose(True)
            progress.show()
            QApplication.processEvents()
            
            # Bidireccional por defecto
            result = ensure_bidirectional_replication(cfg)
            
            try:
                progress.close()
            except Exception:
                pass
                
            try:
                pretty = json.dumps(result, ensure_ascii=False, indent=2)
            except Exception:
                pretty = str(result)
                
            if result.get("ok"):
                QMessageBox.information(self, "Replicación", f"Replicación bidireccional configurada correctamente:\n{pretty}")
            else:
                QMessageBox.critical(self, "Replicación", f"Error al configurar replicación bidireccional:\n{pretty}")
        except Exception as e:
            QMessageBox.critical(self, "Error", f"Fallo al configurar replicación: {e}")

    def _on_force_scheduled_tasks(self):
        """Force scheduled tasks setup"""
        try:
            from utils_modules.prerequisites import ensure_scheduled_tasks
            
            progress = QProgressDialog("Configurando tareas programadas...", None, 0, 0, self)
            progress.setWindowTitle("Tareas programadas")
            progress.setWindowModality(Qt.WindowModality.ApplicationModal)
            progress.setMinimumDuration(0)
            progress.setAutoClose(True)
            progress.show()
            QApplication.processEvents()
            
            result = ensure_scheduled_tasks('local')
            
            try:
                progress.close()
            except Exception:
                pass
                
            try:
                pretty = json.dumps(result, ensure_ascii=False, indent=2)
            except Exception:
                pretty = str(result)
                
            if result.get("ok"):
                QMessageBox.information(self, "Tareas programadas", f"Tareas programadas configuradas correctamente:\n{pretty}")
            else:
                QMessageBox.critical(self, "Tareas programadas", f"Error al configurar tareas programadas:\n{pretty}")
        except Exception as e:
            QMessageBox.critical(self, "Error", f"Fallo al configurar tareas programadas: {e}")

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
        try:
            app.quit()
        except Exception:
            pass
        return

    dlg = DBConfigDialog()
    dlg.show()
    sys.exit(app.exec())


if __name__ == '__main__':
    main()
    def _on_secure_cleanup(self):
        """Proceso de limpieza segura:
        - Confirmación
        - Backup automático
        - Limpieza y reinicialización forzada
        - Verificación de integridad
        """
        try:
            from PyQt6.QtWidgets import QMessageBox, QProgressDialog, QApplication
        except Exception:
            pass
        # Confirmación
        try:
            ret = QMessageBox.question(self, "Limpieza segura", "¿Desea continuar? Se realizará backup y reinicialización completa.", QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No)
            if ret != QMessageBox.StandardButton.Yes:
                return
        except Exception:
            pass
        # Progreso
        try:
            progress = QProgressDialog("Ejecutando limpieza segura…", None, 0, 0, self)
            progress.setWindowTitle("Limpieza segura")
            progress.setWindowModality(Qt.WindowModality.ApplicationModal)
            progress.setMinimumDuration(0)
            progress.setAutoClose(True)
            progress.show()
            QApplication.processEvents()
        except Exception:
            progress = None
        # Backup
        backup_ok, backup_out, backup_err = self._run_python_script('scripts/quick_backup_database.py')
        # Limpieza
        clean_ok, clean_out, clean_err = self._run_python_script('cleanup_and_reinitialize.py', ['--force', '--full-reset'])
        # Verificación
        verify_ok, verify_out, verify_err = self._run_python_script('verify_system_status.py')
        try:
            if progress:
                progress.close()
        except Exception:
            pass
        # Reporte
        try:
            ok = backup_ok and clean_ok and verify_ok
            details = {
                'backup': {'ok': backup_ok, 'stdout': backup_out, 'stderr': backup_err},
                'cleanup': {'ok': clean_ok, 'stdout': clean_out, 'stderr': clean_err},
                'verify': {'ok': verify_ok, 'stdout': verify_out, 'stderr': verify_err},
            }
            import json
            pretty = json.dumps(details, ensure_ascii=False, indent=2, default=str)
            if ok:
                QMessageBox.information(self, "Limpieza segura", pretty)
            else:
                QMessageBox.warning(self, "Limpieza segura", pretty)
        except Exception:
            pass
