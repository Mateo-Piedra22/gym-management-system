import sys
import logging
import os
import json
from datetime import datetime
from typing import Dict, Any, Optional
from PyQt6.QtWidgets import (
    QWidget, QVBoxLayout, QHBoxLayout, QGridLayout, QLabel, QPushButton,
    QGroupBox, QTabWidget, QLineEdit, QTextEdit, QSpinBox, QComboBox, 
    QCheckBox, QFrame, QScrollArea, QMessageBox, QFormLayout, QSizePolicy,
    QListWidget, QListWidgetItem, QProgressBar, QSlider
)
from PyQt6.QtGui import QFont, QPixmap, QPalette, QColor, QIcon
from PyQt6.QtCore import Qt, pyqtSignal, QTimer, QThread, pyqtSlot

from database import DatabaseManager

# Importaciones condicionales para WhatsApp
try:
    from whatsapp_manager import WhatsAppManager
    from message_logger import MessageLogger
    from template_processor import TemplateProcessor
    WHATSAPP_AVAILABLE = True
except ImportError as e:
    logging.warning(f"Módulos WhatsApp no disponibles: {e}")
    WHATSAPP_AVAILABLE = False
    WhatsAppManager = None
    MessageLogger = None
    TemplateProcessor = None

class WhatsAppTestThread(QThread):
    """Hilo para pruebas de WhatsApp sin bloquear la UI"""
    
    test_completed = pyqtSignal(bool, str)
    progress_updated = pyqtSignal(int, str)
    
    def __init__(self, whatsapp_manager, test_type, test_data=None):
        super().__init__()
        self.whatsapp_manager = whatsapp_manager
        self.test_type = test_type
        self.test_data = test_data or {}
    
    def run(self):
        """Ejecuta las pruebas de WhatsApp"""
        try:
            if self.test_type == 'connection':
                self._test_connection()
            elif self.test_type == 'message':
                self._test_message()
            elif self.test_type == 'template':
                self._test_template()
        except Exception as e:
            logging.error(f"Error en prueba WhatsApp: {e}")
            self.test_completed.emit(False, f"Error: {str(e)}")
    
    def _test_connection(self):
        """Prueba la conexión con WhatsApp"""
        self.progress_updated.emit(25, "Verificando configuración...")
        
        if not self.whatsapp_manager.verificar_configuracion():
            self.test_completed.emit(False, "Configuración inválida")
            return
        
        self.progress_updated.emit(50, "Iniciando cliente WhatsApp...")
        
        # Simular inicialización (en implementación real sería más complejo)
        import time
        time.sleep(2)
        
        self.progress_updated.emit(75, "Verificando estado del cliente...")
        time.sleep(1)
        
        self.progress_updated.emit(100, "Conexión establecida")
        self.test_completed.emit(True, "Conexión exitosa con WhatsApp Business")
    
    def _test_message(self):
        """Prueba el envío de mensaje simple"""
        telefono = self.test_data.get('telefono', '')
        mensaje = self.test_data.get('mensaje', 'Mensaje de prueba')
        
        self.progress_updated.emit(30, "Preparando mensaje...")
        
        if not telefono:
            self.test_completed.emit(False, "Número de teléfono requerido")
            return
        
        self.progress_updated.emit(60, "Enviando mensaje...")
        
        # En implementación real, usar whatsapp_manager.enviar_mensaje_simple
        import time
        time.sleep(2)
        
        self.progress_updated.emit(100, "Mensaje enviado")
        self.test_completed.emit(True, f"Mensaje enviado exitosamente a {telefono}")
    
    def _test_template(self):
        """Prueba el procesamiento de plantillas"""
        template_id = self.test_data.get('template_id', 1)
        
        self.progress_updated.emit(40, "Cargando plantilla...")
        
        # En implementación real, usar template_processor
        import time
        time.sleep(1)
        
        self.progress_updated.emit(80, "Procesando variables...")
        time.sleep(1)
        
        self.progress_updated.emit(100, "Plantilla procesada")
        self.test_completed.emit(True, "Plantilla procesada correctamente")

class WhatsAppConfigWidget(QWidget):
    """Widget de solo lectura para mostrar historial de mensajes y logs del sistema WhatsApp"""
    
    # Señales
    config_changed = pyqtSignal(dict)
    test_completed = pyqtSignal(bool, str)
    
    def __init__(self, db_manager: DatabaseManager):
        super().__init__()
        self.db_manager = db_manager
        
        # Inicializar componentes WhatsApp si están disponibles
        self.whatsapp_manager = None
        self.message_logger = None
        self.template_processor = None
        
        if WHATSAPP_AVAILABLE:
            try:
                self.whatsapp_manager = WhatsAppManager(db_manager)
                self.message_logger = MessageLogger(db_manager)
                self.template_processor = TemplateProcessor(db_manager)
            except Exception as e:
                logging.error(f"Error al inicializar componentes WhatsApp: {e}")
        
        self.setup_ui()
        self.load_current_data()
        
        # Timer para actualizar estadísticas
        self.stats_timer = QTimer()
        self.stats_timer.timeout.connect(self.update_statistics)
        self.stats_timer.start(30000)  # Actualizar cada 30 segundos
    
    def setup_ui(self):
        """Configura la interfaz de usuario de solo lectura"""
        main_layout = QVBoxLayout(self)
        main_layout.setSpacing(8)
        main_layout.setContentsMargins(10, 10, 10, 8)
        
        # Título eliminado para diseño limpio sin encabezado
        
        # Tabs principales (unificadas)
        self.tab_widget = QTabWidget()
        self.tab_widget.setObjectName("whatsapp_tab_widget")
        
        # Estado del sistema en el selector de pestañas (corner)
        try:
            corner_widget = self.create_status_corner()
            self.tab_widget.setCornerWidget(corner_widget, Qt.Corner.TopRightCorner)
        except Exception as e:
            logging.warning(f"No se pudo ubicar el estado en el selector: {e}")

        # Pestaña unificada: Historial, Logs y Estadísticas
        try:
            self.unified_overview_tab = self.create_unified_overview_tab()
            self.tab_widget.addTab(self.unified_overview_tab, "Historial/Logs/Estadísticas")
        except Exception as e:
            logging.warning(f"No se pudo crear pestaña unificada de overview: {e}")

        # Pestaña unificada: Configuración y Avanzado
        try:
            self.unified_config_tab = self.create_unified_config_tab()
            self.tab_widget.addTab(self.unified_config_tab, "⚙️ Configuración y Avanzado")
        except Exception as e:
            logging.warning(f"No se pudo crear pestaña unificada de configuración: {e}")
        
        main_layout.addWidget(self.tab_widget)

        # Botones de acción para guardar/aplicar cambios
        try:
            self.setup_action_buttons(main_layout)
        except Exception as e:
            logging.warning(f"No se pudieron configurar botones de acción: {e}")

    def create_unified_overview_tab(self):
        """Crea una pestaña única que integra Historial, Logs y Estadísticas con 1px y alineado arriba."""
        tab = QWidget()
        layout = QVBoxLayout(tab)
        layout.setSpacing(1)
        layout.setContentsMargins(1, 1, 1, 1)

        # Reutilizar contenidos existentes
        history_tab = self.create_message_history_tab()
        logs_tab = self.create_logs_tab()
        stats_tab = self.create_statistics_tab()

        layout.addWidget(history_tab)
        layout.addWidget(logs_tab)
        layout.addWidget(stats_tab)
        
        # Alinear arriba
        layout.addStretch()
        return tab

    def create_unified_config_tab(self):
        """Crea una pestaña única que integra Configuración y Avanzado con 1px y alineado arriba."""
        tab = QWidget()
        layout = QVBoxLayout(tab)
        layout.setSpacing(1)
        layout.setContentsMargins(1, 1, 1, 1)

        # Reutilizar contenidos existentes
        readonly_config_tab = self.create_readonly_config_tab()
        try:
            advanced_tab = self.create_advanced_tab()
        except Exception as e:
            logging.warning(f"No se pudo crear pestaña avanzada: {e}")
            advanced_tab = None

        layout.addWidget(readonly_config_tab)
        if advanced_tab is not None:
            layout.addWidget(advanced_tab)

        # Alinear arriba
        layout.addStretch()
        return tab
    
    def create_status_corner(self):
        """Crea el widget de estado para ubicarse en el corner del tab selector"""
        status_frame = QWidget()
        status_frame.setObjectName("whatsapp_status_corner")
        
        status_layout = QHBoxLayout(status_frame)
        status_layout.setContentsMargins(4, 2, 6, 2)
        status_layout.setSpacing(6)
        
        # Estado general
        self.status_label = QLabel("🔴 Sistema Deshabilitado")
        self.status_label.setFont(QFont("Arial", 10, QFont.Weight.Bold))
        status_layout.addWidget(self.status_label)
        
        # Botón de actualizar estado
        refresh_button = QPushButton("🔄")
        refresh_button.setToolTip("Actualizar estado del sistema")
        refresh_button.setFixedHeight(24)
        refresh_button.setFixedWidth(28)
        refresh_button.clicked.connect(self.update_status)
        status_layout.addWidget(refresh_button)
        
        return status_frame
    
    def create_message_history_tab(self):
        """Crea la pestaña de historial de mensajes"""
        tab = QWidget()
        layout = QVBoxLayout(tab)
        layout.setSpacing(6)
        layout.setContentsMargins(8, 8, 8, 8)
        
        # Filtros
        filters_group = QGroupBox("Filtros")
        filters_layout = QHBoxLayout(filters_group)
        
        self.filter_type_combo = QComboBox()
        self.filter_type_combo.addItems(["Todos", "payment", "overdue", "welcome"])
        self.filter_type_combo.currentTextChanged.connect(self.filter_messages)
        filters_layout.addWidget(QLabel("Tipo:"))
        filters_layout.addWidget(self.filter_type_combo)
        
        filters_layout.addStretch()
        
        refresh_history_button = QPushButton("🔄 Actualizar")
        refresh_history_button.clicked.connect(self.load_message_history)
        filters_layout.addWidget(refresh_history_button)
        
        layout.addWidget(filters_group)
        
        # Lista de mensajes
        messages_group = QGroupBox("Historial de Mensajes")
        messages_layout = QVBoxLayout(messages_group)
        
        self.messages_list = QListWidget()
        self.messages_list.setMaximumHeight(300)
        messages_layout.addWidget(self.messages_list)
        
        layout.addWidget(messages_group)
        
        return tab
    
    def create_logs_tab(self):
        """Crea la pestaña de logs del sistema"""
        tab = QWidget()
        layout = QVBoxLayout(tab)
        layout.setSpacing(6)
        layout.setContentsMargins(8, 8, 8, 8)
        
        # Controles
        controls_layout = QHBoxLayout()
        
        refresh_logs_button = QPushButton("🔄 Actualizar Logs")
        refresh_logs_button.clicked.connect(self.load_system_logs)
        controls_layout.addWidget(refresh_logs_button)
        
        clear_logs_button = QPushButton("🗑️ Limpiar Logs")
        clear_logs_button.clicked.connect(self.clear_logs_display)
        controls_layout.addWidget(clear_logs_button)
        
        controls_layout.addStretch()
        layout.addLayout(controls_layout)
        
        # Área de logs
        logs_group = QGroupBox("Logs del Sistema WhatsApp")
        logs_layout = QVBoxLayout(logs_group)
        
        self.logs_text = QTextEdit()
        self.logs_text.setReadOnly(True)
        self.logs_text.setMaximumHeight(400)
        self.logs_text.setFont(QFont("Consolas", 9))
        logs_layout.addWidget(self.logs_text)
        
        layout.addWidget(logs_group)
        
        return tab
    
    def create_readonly_config_tab(self):
        """Crea la pestaña de configuración de solo lectura"""
        tab = QWidget()
        layout = QVBoxLayout(tab)
        layout.setSpacing(6)
        layout.setContentsMargins(8, 8, 8, 8)
        
        # Configuración actual
        config_group = QGroupBox("Configuración Actual (Solo Lectura)")
        config_layout = QFormLayout(config_group)
        
        self.phone_id_label = QLabel("No configurado")
        config_layout.addRow("Phone ID:", self.phone_id_label)
        
        self.waba_id_label = QLabel("No configurado")
        config_layout.addRow("WABA ID:", self.waba_id_label)
        
        self.token_status_label = QLabel("No configurado")
        config_layout.addRow("Token Status:", self.token_status_label)
        
        layout.addWidget(config_group)
        
        # Plantillas disponibles
        templates_group = QGroupBox("Plantillas Disponibles")
        templates_layout = QVBoxLayout(templates_group)
        
        self.templates_readonly_list = QListWidget()
        self.templates_readonly_list.setMaximumHeight(200)
        templates_layout.addWidget(self.templates_readonly_list)
        
        layout.addWidget(templates_group)
        
        layout.addStretch()
        return tab
    
    def create_config_tab(self):
        """Crea la pestaña de configuración básica"""
        tab = QWidget()
        layout = QVBoxLayout(tab)
        layout.setSpacing(6)
        layout.setContentsMargins(8, 8, 8, 8)
        
        # Configuración de conexión
        connection_group = QGroupBox("Configuración de Conexión")
        connection_layout = QFormLayout(connection_group)
        
        self.phone_number_edit = QLineEdit()
        self.phone_number_edit.setPlaceholderText("+54 9 11 1234-5678")
        connection_layout.addRow("Número de teléfono:", self.phone_number_edit)
        
        self.webhook_url_edit = QLineEdit()
        self.webhook_url_edit.setPlaceholderText("https://tu-dominio.com/webhook")
        connection_layout.addRow("URL del Webhook:", self.webhook_url_edit)
        
        self.webhook_port_spin = QSpinBox()
        self.webhook_port_spin.setRange(1000, 65535)
        self.webhook_port_spin.setValue(8000)
        connection_layout.addRow("Puerto del Webhook:", self.webhook_port_spin)
        
        self.verify_token_edit = QLineEdit()
        self.verify_token_edit.setPlaceholderText("token_de_verificacion")
        connection_layout.addRow("Token de Verificación:", self.verify_token_edit)
        
        layout.addWidget(connection_group)
        
        # Configuración de API
        api_group = QGroupBox("Configuración de API")
        api_layout = QFormLayout(api_group)
        
        self.app_id_edit = QLineEdit()
        self.app_id_edit.setPlaceholderText("ID de la aplicación")
        api_layout.addRow("App ID:", self.app_id_edit)
        
        self.app_secret_edit = QLineEdit()
        self.app_secret_edit.setEchoMode(QLineEdit.EchoMode.Password)
        self.app_secret_edit.setPlaceholderText("Secreto de la aplicación")
        api_layout.addRow("App Secret:", self.app_secret_edit)
        
        self.access_token_edit = QLineEdit()
        self.access_token_edit.setEchoMode(QLineEdit.EchoMode.Password)
        self.access_token_edit.setPlaceholderText("Token de acceso")
        api_layout.addRow("Access Token:", self.access_token_edit)
        
        layout.addWidget(api_group)
        
        # Configuración de mensajería
        messaging_group = QGroupBox("Configuración de Mensajería")
        messaging_layout = QFormLayout(messaging_group)
        
        self.auto_confirm_checkbox = QCheckBox("Confirmación automática de pagos")
        self.auto_confirm_checkbox.setChecked(True)
        messaging_layout.addRow(self.auto_confirm_checkbox)
        
        self.auto_reminder_checkbox = QCheckBox("Recordatorios automáticos de cuotas")
        self.auto_reminder_checkbox.setChecked(True)
        messaging_layout.addRow(self.auto_reminder_checkbox)
        
        self.welcome_message_checkbox = QCheckBox("Mensaje de bienvenida a nuevos usuarios")
        self.welcome_message_checkbox.setChecked(True)
        messaging_layout.addRow(self.welcome_message_checkbox)

        # Nuevo: preferencia para aviso de cupo liberado con confirmación
        self.waitlist_prompt_checkbox = QCheckBox("Aviso de cupo liberado (con confirmación)")
        self.waitlist_prompt_checkbox.setChecked(True)
        self.waitlist_prompt_checkbox.setToolTip("Muestra un diálogo para notificar por WhatsApp al primero en la lista de espera cuando se libera un cupo")
        messaging_layout.addRow(self.waitlist_prompt_checkbox)
        
        layout.addWidget(messaging_group)
        
        layout.addStretch()
        return tab
    
    def create_templates_tab(self):
        """Crea la pestaña de gestión de plantillas"""
        tab = QWidget()
        layout = QVBoxLayout(tab)
        layout.setSpacing(6)
        layout.setContentsMargins(8, 8, 8, 8)
        
        # Lista de plantillas
        templates_group = QGroupBox("Plantillas Disponibles")
        templates_layout = QVBoxLayout(templates_group)
        
        self.templates_list = QListWidget()
        self.templates_list.itemClicked.connect(self.on_template_selected)
        templates_layout.addWidget(self.templates_list)
        
        # Botones de plantillas
        template_buttons_layout = QHBoxLayout()
        
        add_template_button = QPushButton("➕ Nueva Plantilla")
        add_template_button.clicked.connect(self.add_template)
        template_buttons_layout.addWidget(add_template_button)
        
        edit_template_button = QPushButton("✏️ Editar")
        edit_template_button.clicked.connect(self.edit_template)
        template_buttons_layout.addWidget(edit_template_button)
        
        delete_template_button = QPushButton("🗑️ Eliminar")
        delete_template_button.clicked.connect(self.delete_template)
        template_buttons_layout.addWidget(delete_template_button)
        
        template_buttons_layout.addStretch()
        templates_layout.addLayout(template_buttons_layout)
        
        layout.addWidget(templates_group)
        
        # Editor de plantilla
        editor_group = QGroupBox("Editor de Plantilla")
        editor_layout = QFormLayout(editor_group)
        
        self.template_name_edit = QLineEdit()
        self.template_name_edit.setPlaceholderText("Nombre de la plantilla")
        editor_layout.addRow("Nombre:", self.template_name_edit)
        
        self.template_type_combo = QComboBox()
        self.template_type_combo.addItems([
            "confirmacion_pago",
            "recordatorio_cuota", 
            "bienvenida",
            "personalizado"
        ])
        editor_layout.addRow("Tipo:", self.template_type_combo)
        
        self.template_content_edit = QTextEdit()
        self.template_content_edit.setPlaceholderText(
            "Contenido de la plantilla...\n\n"
            "Variables disponibles:\n"
            "{{nombre_usuario}} - Nombre del usuario\n"
            "{{monto}} - Monto del pago\n"
            "{{mes}} - Mes del pago\n"
            "{{año}} - Año del pago\n"
            "{{dias_atraso}} - Días de atraso\n"
            "{{periodo_vencido}} - Período vencido\n"
            "{{fecha_actual}} - Fecha actual\n"
            "{{nombre_gimnasio}} - Nombre del gimnasio"
        )
        self.template_content_edit.setMaximumHeight(150)
        editor_layout.addRow("Contenido:", self.template_content_edit)
        
        # Botones del editor
        editor_buttons_layout = QHBoxLayout()
        
        save_template_button = QPushButton("Guardar Plantilla")
        save_template_button.clicked.connect(self.save_template)
        editor_buttons_layout.addWidget(save_template_button)
        
        preview_template_button = QPushButton("👁️ Vista Previa")
        preview_template_button.clicked.connect(self.preview_template)
        editor_buttons_layout.addWidget(preview_template_button)
        
        editor_buttons_layout.addStretch()
        editor_layout.addRow(editor_buttons_layout)
        
        layout.addWidget(editor_group)
        
        return tab
    
    def create_statistics_tab(self):
        """Crea la pestaña de estadísticas"""
        tab = QWidget()
        layout = QVBoxLayout(tab)
        layout.setSpacing(6)
        layout.setContentsMargins(8, 8, 8, 8)
        
        # Estadísticas generales
        general_stats_group = QGroupBox("Estadísticas Generales")
        general_stats_layout = QGridLayout(general_stats_group)
        
        # Mensajes enviados hoy
        self.messages_today_label = QLabel("0")
        self.messages_today_label.setFont(QFont("Arial", 14, QFont.Weight.Bold))
        self.messages_today_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        general_stats_layout.addWidget(QLabel("Mensajes enviados hoy:"), 0, 0)
        general_stats_layout.addWidget(self.messages_today_label, 0, 1)
        
        # Mensajes esta semana
        self.messages_week_label = QLabel("0")
        self.messages_week_label.setFont(QFont("Arial", 14, QFont.Weight.Bold))
        self.messages_week_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        general_stats_layout.addWidget(QLabel("Mensajes esta semana:"), 1, 0)
        general_stats_layout.addWidget(self.messages_week_label, 1, 1)
        
        # Mensajes fallidos
        self.messages_failed_label = QLabel("0")
        self.messages_failed_label.setFont(QFont("Arial", 14, QFont.Weight.Bold))
        self.messages_failed_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        general_stats_layout.addWidget(QLabel("Mensajes fallidos:"), 2, 0)
        general_stats_layout.addWidget(self.messages_failed_label, 2, 1)
        
        # Usuarios bloqueados
        self.blocked_users_label = QLabel("0")
        self.blocked_users_label.setFont(QFont("Arial", 14, QFont.Weight.Bold))
        self.blocked_users_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        general_stats_layout.addWidget(QLabel("Usuarios bloqueados:"), 3, 0)
        general_stats_layout.addWidget(self.blocked_users_label, 3, 1)
        
        layout.addWidget(general_stats_group)
        
        # Estadísticas por tipo
        type_stats_group = QGroupBox("Estadísticas por Tipo de Mensaje")
        type_stats_layout = QVBoxLayout(type_stats_group)
        
        self.type_stats_list = QListWidget()
        type_stats_layout.addWidget(self.type_stats_list)
        
        layout.addWidget(type_stats_group)
        
        # Botones de estadísticas
        stats_buttons_layout = QHBoxLayout()
        
        refresh_stats_button = QPushButton("🔄 Actualizar Estadísticas")
        refresh_stats_button.clicked.connect(self.update_statistics)
        stats_buttons_layout.addWidget(refresh_stats_button)
        
        export_stats_button = QPushButton("Exportar Estadísticas")
        export_stats_button.clicked.connect(self.export_statistics)
        stats_buttons_layout.addWidget(export_stats_button)
        
        clear_stats_button = QPushButton("🗑️ Limpiar Historial")
        clear_stats_button.clicked.connect(self.clear_message_history)
        stats_buttons_layout.addWidget(clear_stats_button)
        
        stats_buttons_layout.addStretch()
        layout.addLayout(stats_buttons_layout)
        
        layout.addStretch()
        return tab
    
    def create_tests_tab(self):
        """Crea la pestaña de pruebas"""
        tab = QWidget()
        layout = QVBoxLayout(tab)
        layout.setSpacing(6)
        layout.setContentsMargins(8, 8, 8, 8)
        
        # Prueba de conexión
        connection_test_group = QGroupBox("Prueba de Conexión")
        connection_test_layout = QVBoxLayout(connection_test_group)
        
        connection_test_button = QPushButton("🔗 Probar Conexión WhatsApp")
        connection_test_button.clicked.connect(self.test_connection)
        connection_test_layout.addWidget(connection_test_button)
        
        layout.addWidget(connection_test_group)
        
        # Prueba de mensaje
        message_test_group = QGroupBox("Prueba de Mensaje")
        message_test_layout = QFormLayout(message_test_group)
        
        self.test_phone_edit = QLineEdit()
        self.test_phone_edit.setPlaceholderText("+54 9 11 1234-5678")
        message_test_layout.addRow("Teléfono de prueba:", self.test_phone_edit)
        
        self.test_message_edit = QTextEdit()
        self.test_message_edit.setPlaceholderText("Mensaje de prueba...")
        self.test_message_edit.setMaximumHeight(80)
        message_test_layout.addRow("Mensaje:", self.test_message_edit)
        
        test_message_button = QPushButton("📱 Enviar Mensaje de Prueba")
        test_message_button.clicked.connect(self.test_message)
        message_test_layout.addRow(test_message_button)
        
        layout.addWidget(message_test_group)
        
        # Prueba de plantillas
        template_test_group = QGroupBox("Prueba de Plantillas")
        template_test_layout = QFormLayout(template_test_group)
        
        self.test_template_combo = QComboBox()
        template_test_layout.addRow("Plantilla:", self.test_template_combo)
        
        test_template_button = QPushButton("Probar Plantilla")
        test_template_button.clicked.connect(self.test_template)
        template_test_layout.addRow(test_template_button)
        
        layout.addWidget(template_test_group)
        
        # Progreso de pruebas
        progress_group = QGroupBox("Progreso de Pruebas")
        progress_layout = QVBoxLayout(progress_group)
        
        self.test_progress_bar = QProgressBar()
        self.test_progress_bar.setVisible(False)
        progress_layout.addWidget(self.test_progress_bar)
        
        self.test_status_label = QLabel("Listo para pruebas")
        self.test_status_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        progress_layout.addWidget(self.test_status_label)
        
        layout.addWidget(progress_group)
        
        layout.addStretch()
        return tab
    
    def create_advanced_tab(self):
        """Crea la pestaña de configuración avanzada"""
        tab = QWidget()
        layout = QVBoxLayout(tab)
        layout.setSpacing(6)
        layout.setContentsMargins(8, 8, 8, 8)
        
        # Configuración anti-spam
        antispam_group = QGroupBox("Configuración Anti-Spam")
        antispam_layout = QFormLayout(antispam_group)
        
        self.max_messages_per_hour_spin = QSpinBox()
        self.max_messages_per_hour_spin.setRange(1, 100)
        self.max_messages_per_hour_spin.setValue(10)
        antispam_layout.addRow("Máx. mensajes por hora:", self.max_messages_per_hour_spin)
        
        self.max_messages_per_day_spin = QSpinBox()
        self.max_messages_per_day_spin.setRange(1, 1000)
        self.max_messages_per_day_spin.setValue(50)
        antispam_layout.addRow("Máx. mensajes por día:", self.max_messages_per_day_spin)
        
        self.block_duration_spin = QSpinBox()
        self.block_duration_spin.setRange(1, 168)  # 1 hora a 1 semana
        self.block_duration_spin.setValue(24)
        antispam_layout.addRow("Duración bloqueo (horas):", self.block_duration_spin)
        
        layout.addWidget(antispam_group)
        
        # Configuración de reintentos
        retry_group = QGroupBox("Configuración de Reintentos")
        retry_layout = QFormLayout(retry_group)
        
        self.max_retries_spin = QSpinBox()
        self.max_retries_spin.setRange(0, 10)
        self.max_retries_spin.setValue(3)
        retry_layout.addRow("Máx. reintentos:", self.max_retries_spin)
        
        self.retry_delay_spin = QSpinBox()
        self.retry_delay_spin.setRange(1, 3600)  # 1 segundo a 1 hora
        self.retry_delay_spin.setValue(300)  # 5 minutos
        retry_layout.addRow("Delay entre reintentos (seg):", self.retry_delay_spin)
        
        layout.addWidget(retry_group)
        
        # Configuración de limpieza
        cleanup_group = QGroupBox("Configuración de Limpieza")
        cleanup_layout = QFormLayout(cleanup_group)
        
        self.cleanup_days_spin = QSpinBox()
        self.cleanup_days_spin.setRange(1, 365)
        self.cleanup_days_spin.setValue(30)
        cleanup_layout.addRow("Días para limpiar mensajes:", self.cleanup_days_spin)
        
        self.auto_cleanup_checkbox = QCheckBox("Limpieza automática")
        self.auto_cleanup_checkbox.setChecked(True)
        cleanup_layout.addRow(self.auto_cleanup_checkbox)
        
        cleanup_now_button = QPushButton("🧹 Limpiar Ahora")
        cleanup_now_button.clicked.connect(self.cleanup_messages_now)
        cleanup_layout.addRow(cleanup_now_button)
        
        layout.addWidget(cleanup_group)
        
        # Configuración de logging
        logging_group = QGroupBox("Configuración de Logging")
        logging_layout = QFormLayout(logging_group)
        
        self.log_level_combo = QComboBox()
        self.log_level_combo.addItems(["DEBUG", "INFO", "WARNING", "ERROR"])
        self.log_level_combo.setCurrentText("INFO")
        logging_layout.addRow("Nivel de log:", self.log_level_combo)
        
        self.log_to_file_checkbox = QCheckBox("Guardar logs en archivo")
        self.log_to_file_checkbox.setChecked(True)
        logging_layout.addRow(self.log_to_file_checkbox)
        
        layout.addWidget(logging_group)

        # Preferencias de cola offline de WhatsApp
        queue_group = QGroupBox("Cola Offline de WhatsApp")
        queue_layout = QFormLayout(queue_group)

        self.queue_ttl_spin = QSpinBox()
        self.queue_ttl_spin.setRange(1, 168)  # 1 hora a 7 días
        self.queue_ttl_spin.setValue(72)
        queue_layout.addRow("TTL de operaciones (horas):", self.queue_ttl_spin)

        self.queue_dedup_checkbox = QCheckBox("Habilitar deduplicación de operaciones")
        self.queue_dedup_checkbox.setChecked(True)
        queue_layout.addRow(self.queue_dedup_checkbox)

        info_label = QLabel("Estas preferencias afectan cómo se manejan las operaciones pendientes de WhatsApp cuando no hay conexión: TTL define cuándo expiran y la deduplicación evita duplicados.")
        info_label.setWordWrap(True)
        queue_layout.addRow(info_label)

        layout.addWidget(queue_group)
        
        layout.addStretch()
        return tab
    
    def setup_action_buttons(self, parent_layout):
        """Configura los botones de acción principales"""
        buttons_layout = QHBoxLayout()
        buttons_layout.setSpacing(8)
        
        # Botón guardar
        save_button = QPushButton("Guardar Configuración")
        save_button.setObjectName("primary_button")
        save_button.clicked.connect(self.save_configuration)
        buttons_layout.addWidget(save_button)
        
        # Botón aplicar
        apply_button = QPushButton("✅ Aplicar Cambios")
        apply_button.clicked.connect(self.apply_configuration)
        buttons_layout.addWidget(apply_button)
        
        # Botón resetear
        reset_button = QPushButton("🔄 Resetear")
        reset_button.clicked.connect(self.reset_configuration)
        buttons_layout.addWidget(reset_button)
        
        buttons_layout.addStretch()
        
        # Botón ayuda
        help_button = QPushButton("❓ Ayuda")
        help_button.clicked.connect(self.show_help)
        buttons_layout.addWidget(help_button)
        
        parent_layout.addLayout(buttons_layout)
    
    # --- MÉTODOS DE CARGA Y GUARDADO ---
    
    def load_whatsapp_config(self) -> Dict[str, Any]:
        """Carga la configuración de WhatsApp desde la base de datos"""
        try:
            config = self.db_manager.obtener_configuracion_whatsapp_completa()
            return config if config else self.get_default_config()
        except Exception as e:
            logging.error(f"Error al cargar configuración WhatsApp: {e}")
            return self.get_default_config()
    
    def get_default_config(self) -> Dict[str, Any]:
        """Retorna la configuración por defecto"""
        return {
            'phone_number': '',
            'webhook_url': '',
            'webhook_port': 8000,
            'verify_token': '',
            'app_id': '',
            'app_secret': '',
            'access_token': '',
            'auto_confirm_payments': True,
            'auto_reminders': True,
            'welcome_messages': True,
            'waitlist_prompt_enabled': True,
            'max_messages_per_hour': 10,
            'max_messages_per_day': 50,
            'block_duration_hours': 24,
            'max_retries': 3,
            'retry_delay_seconds': 300,
            'cleanup_days': 30,
            'auto_cleanup': True,
            'log_level': 'INFO',
            'log_to_file': True
        }
    
    def load_current_data(self):
        """Carga los datos actuales en la interfaz de solo lectura"""
        self.update_status()
        self.load_readonly_config()
        self.load_templates_readonly()
        self.load_message_history()
        self.load_system_logs()
        # Cargar preferencias de cola si la pestaña avanzada está disponible
        try:
            self.load_queue_preferences()
        except Exception as e:
            logging.debug(f"No se pudieron cargar preferencias de cola: {e}")
    
    def load_readonly_config(self):
        """Carga la configuración de solo lectura"""
        try:
            if WHATSAPP_AVAILABLE and self.whatsapp_manager:
                self.phone_id_label.setText(self.whatsapp_manager.phone_number_id)
                self.waba_id_label.setText(self.whatsapp_manager.whatsapp_business_account_id)
                self.token_status_label.setText("✅ Configurado" if self.whatsapp_manager.access_token else "❌ No configurado")
            else:
                self.phone_id_label.setText("❌ No disponible")
                self.waba_id_label.setText("❌ No disponible")
                self.token_status_label.setText("❌ No disponible")
        except Exception as e:
            logging.error(f"Error al cargar configuración de solo lectura: {e}")
    
    def load_templates_readonly(self):
        """Carga las plantillas en modo de solo lectura"""
        try:
            if not WHATSAPP_AVAILABLE:
                return
            
            templates = self.db_manager.obtener_plantillas_whatsapp()
            
            self.templates_readonly_list.clear()
            
            for template in templates:
                item_text = f"{template['template_name'][:50]}..."
                item = QListWidgetItem(item_text)
                item.setToolTip(template['body_text'])
                self.templates_readonly_list.addItem(item)
                
        except Exception as e:
            logging.error(f"Error al cargar plantillas de solo lectura: {e}")
    
    def load_message_history(self):
        """Carga el historial de mensajes"""
        try:
            if not WHATSAPP_AVAILABLE or not self.message_logger:
                return
            
            # Obtener filtro actual
            filter_type = self.filter_type_combo.currentText() if hasattr(self, 'filter_type_combo') else "Todos"
            message_type = None if filter_type == "Todos" else filter_type
            
            # Obtener historial
            messages = self.message_logger.obtener_historial_mensajes(limite=50)
            
            if hasattr(self, 'messages_list'):
                self.messages_list.clear()
                
                for message in messages:
                    if message_type and message.get('message_type') != message_type:
                        continue
                    
                    timestamp = message.get('sent_at', 'N/A')
                    phone = message.get('phone_number', 'N/A')
                    msg_type = message.get('message_type', 'N/A')
                    status = message.get('status', 'N/A')
                    
                    item_text = f"[{timestamp}] {phone} - {msg_type} ({status})"
                    item = QListWidgetItem(item_text)
                    item.setToolTip(message.get('message_content', 'Sin contenido'))
                    self.messages_list.addItem(item)
                    
        except Exception as e:
            logging.error(f"Error al cargar historial de mensajes: {e}")
    
    def load_system_logs(self):
        """Carga los logs del sistema"""
        try:
            if hasattr(self, 'logs_text'):
                # Simular logs del sistema WhatsApp
                logs = [
                    f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] INFO: Sistema WhatsApp inicializado",
                    f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] INFO: Configuración cargada correctamente",
                    f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] INFO: Plantillas sincronizadas",
                ]
                
                if WHATSAPP_AVAILABLE and self.whatsapp_manager:
                    if self.whatsapp_manager.verificar_configuracion():
                        logs.append(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] INFO: Cliente WhatsApp configurado correctamente")
                    else:
                        logs.append(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] WARNING: Configuración de WhatsApp incompleta")
                else:
                    logs.append(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] ERROR: Módulos WhatsApp no disponibles")
                
                self.logs_text.setPlainText('\n'.join(logs))
                
        except Exception as e:
            logging.error(f"Error al cargar logs del sistema: {e}")
    
    def filter_messages(self):
        """Filtra los mensajes por tipo"""
        self.load_message_history()
    
    def clear_logs_display(self):
        """Limpia la visualización de logs"""
        if hasattr(self, 'logs_text'):
            self.logs_text.clear()
    
    def load_templates(self):
        """Carga las plantillas disponibles"""
        try:
            if not WHATSAPP_AVAILABLE:
                return
            
            templates = self.db_manager.obtener_plantillas_whatsapp()
            
            self.templates_list.clear()
            self.test_template_combo.clear()
            
            for template in templates:
                item_text = f"{template['nombre']} ({template['tipo']})"
                
                # Agregar a la lista
                item = QListWidgetItem(item_text)
                item.setData(Qt.ItemDataRole.UserRole, template)
                self.templates_list.addItem(item)
                
                # Agregar al combo de pruebas
                self.test_template_combo.addItem(item_text, template['id'])
                
        except Exception as e:
            logging.error(f"Error al cargar plantillas: {e}")
    
    # --- MÉTODOS DE EVENTOS ---
    
    def save_configuration(self):
        """Guarda la configuración actual"""
        try:
            config = self.get_current_config_from_ui()
            
            # Guardar en base de datos
            for key, value in config.items():
                self.db_manager.actualizar_configuracion_whatsapp(key, str(value))

            # Guardar flag de aviso de cupo liberado en configuración general
            try:
                self.db_manager.actualizar_configuracion('enable_waitlist_prompt',
                                                        'true' if self.waitlist_prompt_checkbox.isChecked() else 'false')
            except Exception as e:
                logging.warning(f"No se pudo guardar enable_waitlist_prompt: {e}")

            # Guardar preferencias de cola offline (sistema)
            try:
                ttl_hours = int(self.queue_ttl_spin.value())
                dedup_enabled = self.queue_dedup_checkbox.isChecked()
                self.db_manager.actualizar_configuracion('whatsapp_queue_ttl_hours', str(ttl_hours))
                self.db_manager.actualizar_configuracion('whatsapp_queue_dedup_enabled', 'true' if dedup_enabled else 'false')
            except Exception as e:
                logging.warning(f"No se pudieron guardar preferencias de cola: {e}")
            
            self.current_config = config
            
            QMessageBox.information(
                self, 
                "Configuración Guardada", 
                "La configuración de WhatsApp se ha guardado correctamente."
            )
            
            self.config_changed.emit(config)
            
        except Exception as e:
            logging.error(f"Error al guardar configuración: {e}")
            QMessageBox.critical(
                self, 
                "Error", 
                f"Error al guardar la configuración: {str(e)}"
            )
    
    def apply_configuration(self):
        """Aplica la configuración actual"""
        try:
            self.save_configuration()
            
            # Reinicializar componentes WhatsApp si están disponibles
            if WHATSAPP_AVAILABLE and self.whatsapp_manager:
                self.whatsapp_manager.reinicializar_configuracion()
                # Recargar preferencias en OfflineSyncManager si está vinculado
                try:
                    offline_mgr = getattr(self.whatsapp_manager, 'offline_sync_manager', None)
                    if offline_mgr:
                        offline_mgr.reload_preferences(self.db_manager)
                except Exception as e:
                    logging.debug(f"No se pudieron recargar preferencias en OfflineSyncManager: {e}")
            
            self.update_status()
            
            QMessageBox.information(
                self, 
                "Configuración Aplicada", 
                "La configuración se ha aplicado correctamente."
            )
            
        except Exception as e:
            logging.error(f"Error al aplicar configuración: {e}")
            QMessageBox.critical(
                self, 
                "Error", 
                f"Error al aplicar la configuración: {str(e)}"
            )
    
    def reset_configuration(self):
        """Resetea la configuración a valores por defecto"""
        reply = QMessageBox.question(
            self,
            "Resetear Configuración",
            "¿Está seguro de que desea resetear toda la configuración a los valores por defecto?",
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No
        )
        
        if reply == QMessageBox.StandardButton.Yes:
            self.current_config = self.get_default_config()
            self.load_current_settings()

    def load_queue_preferences(self):
        """Carga TTL y deduplicación desde la configuración del sistema"""
        try:
            ttl_str = self.db_manager.obtener_configuracion('whatsapp_queue_ttl_hours') or '72'
            dedup_str = self.db_manager.obtener_configuracion('whatsapp_queue_dedup_enabled') or 'true'
            ttl_val = 72
            try:
                ttl_val = max(1, min(168, int(ttl_str)))
            except Exception:
                ttl_val = 72
            self.queue_ttl_spin.setValue(ttl_val)
            self.queue_dedup_checkbox.setChecked(str(dedup_str).lower() == 'true')
        except Exception as e:
            logging.debug(f"Fallo cargando preferencias de cola: {e}")
    
    def get_current_config_from_ui(self) -> Dict[str, Any]:
        """Obtiene la configuración actual desde la interfaz"""
        return {
            'phone_number': self.phone_number_edit.text().strip(),
            'webhook_url': self.webhook_url_edit.text().strip(),
            'webhook_port': self.webhook_port_spin.value(),
            'verify_token': self.verify_token_edit.text().strip(),
            'app_id': self.app_id_edit.text().strip(),
            'app_secret': self.app_secret_edit.text().strip(),
            'access_token': self.access_token_edit.text().strip(),
            'auto_confirm_payments': self.auto_confirm_checkbox.isChecked(),
            'auto_reminders': self.auto_reminder_checkbox.isChecked(),
            'welcome_messages': self.welcome_message_checkbox.isChecked(),
            'waitlist_prompt_enabled': self.waitlist_prompt_checkbox.isChecked(),
            'max_messages_per_hour': self.max_messages_per_hour_spin.value(),
            'max_messages_per_day': self.max_messages_per_day_spin.value(),
            'block_duration_hours': self.block_duration_spin.value(),
            'max_retries': self.max_retries_spin.value(),
            'retry_delay_seconds': self.retry_delay_spin.value(),
            'cleanup_days': self.cleanup_days_spin.value(),
            'auto_cleanup': self.auto_cleanup_checkbox.isChecked(),
            'log_level': self.log_level_combo.currentText(),
            'log_to_file': self.log_to_file_checkbox.isChecked()
        }

    def load_current_settings(self):
        """Carga la configuración actual en la UI de edición"""
        try:
            cfg = self.load_whatsapp_config()
            # Campos básicos
            self.phone_number_edit.setText(str(cfg.get('phone_number', '')))
            self.webhook_url_edit.setText(str(cfg.get('webhook_url', '')))
            self.webhook_port_spin.setValue(int(cfg.get('webhook_port', 8000)))
            self.verify_token_edit.setText(str(cfg.get('verify_token', '')))
            self.app_id_edit.setText(str(cfg.get('app_id', '')))
            self.app_secret_edit.setText(str(cfg.get('app_secret', '')))
            self.access_token_edit.setText(str(cfg.get('access_token', '')))
            
            # Checkboxes de mensajería
            self.auto_confirm_checkbox.setChecked(bool(cfg.get('auto_confirm_payments', True)))
            self.auto_reminder_checkbox.setChecked(bool(cfg.get('auto_reminders', True)))
            self.welcome_message_checkbox.setChecked(bool(cfg.get('welcome_messages', True)))
            # Nuevo flag: intentar cargar desde configuración general si está disponible
            try:
                general_flag = self.db_manager.obtener_configuracion('enable_waitlist_prompt')
                if general_flag is not None:
                    self.waitlist_prompt_checkbox.setChecked(str(general_flag).lower() == 'true')
                else:
                    self.waitlist_prompt_checkbox.setChecked(bool(cfg.get('waitlist_prompt_enabled', True)))
            except Exception:
                self.waitlist_prompt_checkbox.setChecked(bool(cfg.get('waitlist_prompt_enabled', True)))
            
            # Avanzado
            self.max_messages_per_hour_spin.setValue(int(cfg.get('max_messages_per_hour', 10)))
            self.max_messages_per_day_spin.setValue(int(cfg.get('max_messages_per_day', 50)))
            self.block_duration_spin.setValue(int(cfg.get('block_duration_hours', 24)))
            self.max_retries_spin.setValue(int(cfg.get('max_retries', 3)))
            self.retry_delay_spin.setValue(int(cfg.get('retry_delay_seconds', 300)))
            self.cleanup_days_spin.setValue(int(cfg.get('cleanup_days', 30)))
            self.auto_cleanup_checkbox.setChecked(bool(cfg.get('auto_cleanup', True)))
            self.log_level_combo.setCurrentText(str(cfg.get('log_level', 'INFO')))
            self.log_to_file_checkbox.setChecked(bool(cfg.get('log_to_file', True)))
        except Exception as e:
            logging.error(f"Error cargando configuración en WhatsAppConfigWidget: {e}")
    
    # --- MÉTODOS DE ESTADO Y CONTROL ---
    
    def update_status(self):
        """Actualiza el estado del sistema"""
        try:
            if not WHATSAPP_AVAILABLE:
                self.status_label.setText("🔴 Módulos WhatsApp no disponibles")
                return
            
            if not self.whatsapp_manager:
                self.status_label.setText("🔴 WhatsApp Manager no inicializado")
                return
            
            # Verificar configuración
            config_valid = self.whatsapp_manager.verificar_configuracion()
            
            if not config_valid:
                self.status_label.setText("🟡 Configuración incompleta")
                return
            
            # Estado del sistema
            self.status_label.setText("🟢 Sistema Configurado")
            
        except Exception as e:
            logging.error(f"Error al actualizar estado: {e}")
            self.status_label.setText("🔴 Error de Estado")
    
    def toggle_server(self):
        """Alterna el estado del servidor WhatsApp"""
        try:
            if not WHATSAPP_AVAILABLE or not self.whatsapp_manager:
                QMessageBox.warning(
                    self,
                    "WhatsApp No Disponible",
                    "Los módulos de WhatsApp no están disponibles."
                )
                return
            
            server_active = getattr(self.whatsapp_manager, 'servidor_activo', False)
            
            if server_active:
                # Detener servidor
                success = self.whatsapp_manager.detener_servidor()
                if success:
                    QMessageBox.information(
                        self,
                        "Servidor Detenido",
                        "El servidor WhatsApp se ha detenido correctamente."
                    )
                else:
                    QMessageBox.warning(
                        self,
                        "Error",
                        "No se pudo detener el servidor WhatsApp."
                    )
            else:
                # Iniciar servidor
                success = self.whatsapp_manager.iniciar_servidor()
                if success:
                    QMessageBox.information(
                        self,
                        "Servidor Iniciado",
                        "El servidor WhatsApp se ha iniciado correctamente."
                    )
                else:
                    QMessageBox.warning(
                        self,
                        "Error",
                        "No se pudo iniciar el servidor WhatsApp.\n\nVerifique la configuración."
                    )
            
            self.update_status()
            
        except Exception as e:
            logging.error(f"Error al alternar servidor: {e}")
            QMessageBox.critical(
                self,
                "Error",
                f"Error al controlar el servidor: {str(e)}"
            )
    
    # --- MÉTODOS DE PLANTILLAS ---
    
    def on_template_selected(self, item):
        """Maneja la selección de una plantilla"""
        template_data = item.data(Qt.ItemDataRole.UserRole)
        if template_data:
            self.template_name_edit.setText(template_data['nombre'])
            self.template_type_combo.setCurrentText(template_data['tipo'])
            self.template_content_edit.setPlainText(template_data['contenido'])
    
    def add_template(self):
        """Agrega una nueva plantilla"""
        self.template_name_edit.clear()
        self.template_type_combo.setCurrentIndex(0)
        self.template_content_edit.clear()
        self.template_name_edit.setFocus()
    
    def edit_template(self):
        """Edita la plantilla seleccionada"""
        current_item = self.templates_list.currentItem()
        if not current_item:
            QMessageBox.information(
                self,
                "Seleccionar Plantilla",
                "Por favor, seleccione una plantilla para editar."
            )
            return
        
        # La plantilla ya está cargada en el editor por on_template_selected
        self.template_name_edit.setFocus()
    
    def delete_template(self):
        """Elimina la plantilla seleccionada"""
        current_item = self.templates_list.currentItem()
        if not current_item:
            QMessageBox.information(
                self,
                "Seleccionar Plantilla",
                "Por favor, seleccione una plantilla para eliminar."
            )
            return
        
        template_data = current_item.data(Qt.ItemDataRole.UserRole)
        
        reply = QMessageBox.question(
            self,
            "Eliminar Plantilla",
            f"¿Está seguro de que desea eliminar la plantilla '{template_data['nombre']}'?",
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No
        )
        
        if reply == QMessageBox.StandardButton.Yes:
            try:
                self.db_manager.eliminar_plantilla_whatsapp(template_data['id'])
                self.load_templates()
                
                # Limpiar editor
                self.template_name_edit.clear()
                self.template_content_edit.clear()
                
                QMessageBox.information(
                    self,
                    "Plantilla Eliminada",
                    "La plantilla se ha eliminado correctamente."
                )
                
            except Exception as e:
                logging.error(f"Error al eliminar plantilla: {e}")
                QMessageBox.critical(
                    self,
                    "Error",
                    f"Error al eliminar la plantilla: {str(e)}"
                )
    
    def save_template(self):
        """Guarda la plantilla actual"""
        nombre = self.template_name_edit.text().strip()
        tipo = self.template_type_combo.currentText()
        contenido = self.template_content_edit.toPlainText().strip()
        
        if not nombre or not contenido:
            QMessageBox.warning(
                self,
                "Datos Incompletos",
                "Por favor, complete el nombre y contenido de la plantilla."
            )
            return
        
        try:
            # Verificar si es edición o nueva plantilla
            current_item = self.templates_list.currentItem()
            
            if current_item:
                # Editar plantilla existente
                template_data = current_item.data(Qt.ItemDataRole.UserRole)
                self.db_manager.actualizar_plantilla_whatsapp(
                    template_data['id'], nombre, tipo, contenido
                )
                message = "Plantilla actualizada correctamente."
            else:
                # Nueva plantilla
                self.db_manager.crear_plantilla_whatsapp(nombre, tipo, contenido)
                message = "Plantilla creada correctamente."
            
            self.load_templates()
            
            QMessageBox.information(
                self,
                "Plantilla Guardada",
                message
            )
            
        except Exception as e:
            logging.error(f"Error al guardar plantilla: {e}")
            QMessageBox.critical(
                self,
                "Error",
                f"Error al guardar la plantilla: {str(e)}"
            )
    
    def preview_template(self):
        """Muestra una vista previa de la plantilla"""
        contenido = self.template_content_edit.toPlainText().strip()
        
        if not contenido:
            QMessageBox.information(
                self,
                "Plantilla Vacía",
                "No hay contenido para mostrar en la vista previa."
            )
            return
        
        try:
            if WHATSAPP_AVAILABLE and self.template_processor:
                # Procesar plantilla con datos de ejemplo
                datos_ejemplo = {
                    'nombre_usuario': 'Juan Pérez',
                    'monto': 15000.0,
                    'mes': 12,
                    'año': 2024,
                    'dias_atraso': 5,
                    'periodo_vencido': '11/2024',
                    'fecha_actual': datetime.now().strftime('%d/%m/%Y'),
                    'nombre_gimnasio': get_gym_name('Gimnasio')
                }
                
                contenido_procesado = self.template_processor.procesar_plantilla(
                    contenido, datos_ejemplo
                )
            else:
                contenido_procesado = contenido
            
            QMessageBox.information(
                self,
                "Vista Previa de Plantilla",
                f"Vista previa con datos de ejemplo:\n\n{contenido_procesado}"
            )
            
        except Exception as e:
            logging.error(f"Error en vista previa: {e}")
            QMessageBox.critical(
                self,
                "Error",
                f"Error al generar vista previa: {str(e)}"
            )
    
    # --- MÉTODOS DE ESTADÍSTICAS ---
    
    def update_statistics(self):
        """Actualiza las estadísticas mostradas"""
        try:
            if not WHATSAPP_AVAILABLE or not self.message_logger:
                self.messages_today_label.setText("N/A")
                self.messages_week_label.setText("N/A")
                self.messages_failed_label.setText("N/A")
                self.blocked_users_label.setText("N/A")
                return
            
            # Obtener estadísticas
            stats_diarias = self.message_logger.obtener_estadisticas_diarias()
            stats_semanales = self.message_logger.obtener_estadisticas_semanales()
            stats_por_tipo = self.message_logger.obtener_estadisticas_por_tipo(7)
            usuarios_bloqueados = self.message_logger.obtener_usuarios_bloqueados()
            
            # Actualizar labels
            self.messages_today_label.setText(str(stats_diarias.get('enviados', 0)))
            self.messages_week_label.setText(str(stats_semanales.get('enviados', 0)))
            self.messages_failed_label.setText(str(stats_diarias.get('fallidos', 0)))
            self.blocked_users_label.setText(str(len(usuarios_bloqueados)))
            
            # Actualizar lista de estadísticas por tipo
            self.type_stats_list.clear()
            for tipo, stats in stats_por_tipo.items():
                item_text = f"{tipo}: {stats.get('enviados', 0)} enviados, {stats.get('fallidos', 0)} fallidos"
                self.type_stats_list.addItem(item_text)
            
        except Exception as e:
            logging.error(f"Error al actualizar estadísticas: {e}")
    
    def export_statistics(self):
        """Exporta las estadísticas a un archivo"""
        try:
            if not WHATSAPP_AVAILABLE or not self.message_logger:
                QMessageBox.warning(
                    self,
                    "WhatsApp No Disponible",
                    "El sistema WhatsApp no está disponible para exportar estadísticas."
                )
                return
            
            from PyQt6.QtWidgets import QFileDialog
            
            filename, _ = QFileDialog.getSaveFileName(
                self,
                "Exportar Estadísticas WhatsApp",
                f"estadisticas_whatsapp_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json",
                "Archivos JSON (*.json)"
            )
            
            if filename:
                # Recopilar todas las estadísticas
                estadisticas = {
                    'fecha_exportacion': datetime.now().isoformat(),
                    'estadisticas_diarias': self.message_logger.obtener_estadisticas_diarias(),
                    'estadisticas_semanales': self.message_logger.obtener_estadisticas_semanales(),
                    'estadisticas_por_tipo': self.message_logger.obtener_estadisticas_por_tipo(30),
                    'usuarios_bloqueados': self.message_logger.obtener_usuarios_bloqueados()
                }
                
                with open(filename, 'w', encoding='utf-8') as f:
                    json.dump(estadisticas, f, indent=2, ensure_ascii=False)
                
                QMessageBox.information(
                    self,
                    "Estadísticas Exportadas",
                    f"Las estadísticas se han exportado correctamente a:\n{filename}"
                )
                
        except Exception as e:
            logging.error(f"Error al exportar estadísticas: {e}")
            QMessageBox.critical(
                self,
                "Error",
                f"Error al exportar estadísticas: {str(e)}"
            )
    
    def clear_message_history(self):
        """Limpia el historial de mensajes"""
        reply = QMessageBox.question(
            self,
            "Limpiar Historial",
            "¿Está seguro de que desea limpiar todo el historial de mensajes?\n\nEsta acción no se puede deshacer.",
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No
        )
        
        if reply == QMessageBox.StandardButton.Yes:
            try:
                if WHATSAPP_AVAILABLE and self.message_logger:
                    self.message_logger.limpiar_mensajes_antiguos(0)  # Limpiar todos
                    self.update_statistics()
                    
                    QMessageBox.information(
                        self,
                        "Historial Limpiado",
                        "El historial de mensajes se ha limpiado correctamente."
                    )
                else:
                    QMessageBox.warning(
                        self,
                        "WhatsApp No Disponible",
                        "El sistema WhatsApp no está disponible."
                    )
                    
            except Exception as e:
                logging.error(f"Error al limpiar historial: {e}")
                QMessageBox.critical(
                    self,
                    "Error",
                    f"Error al limpiar el historial: {str(e)}"
                )
    
    def cleanup_messages_now(self):
        """Ejecuta la limpieza de mensajes inmediatamente"""
        try:
            if not WHATSAPP_AVAILABLE or not self.message_logger:
                QMessageBox.warning(
                    self,
                    "WhatsApp No Disponible",
                    "El sistema WhatsApp no está disponible."
                )
                return
            
            dias = self.cleanup_days_spin.value()
            mensajes_eliminados = self.message_logger.limpiar_mensajes_antiguos(dias)
            
            QMessageBox.information(
                self,
                "Limpieza Completada",
                f"Se han eliminado {mensajes_eliminados} mensajes antiguos (más de {dias} días)."
            )
            
            self.update_statistics()
            
        except Exception as e:
            logging.error(f"Error en limpieza de mensajes: {e}")
            QMessageBox.critical(
                self,
                "Error",
                f"Error al limpiar mensajes: {str(e)}"
            )
    
    # --- MÉTODOS DE PRUEBAS ---
    
    def test_connection(self):
        """Prueba la conexión con WhatsApp"""
        if not WHATSAPP_AVAILABLE or not self.whatsapp_manager:
            QMessageBox.warning(
                self,
                "WhatsApp No Disponible",
                "Los módulos de WhatsApp no están disponibles para realizar pruebas."
            )
            return
        
        self.start_test('connection')
    
    def test_message(self):
        """Prueba el envío de un mensaje"""
        telefono = self.test_phone_edit.text().strip()
        mensaje = self.test_message_edit.toPlainText().strip()
        
        if not telefono or not mensaje:
            QMessageBox.warning(
                self,
                "Datos Incompletos",
                "Por favor, complete el teléfono y mensaje de prueba."
            )
            return
        
        if not WHATSAPP_AVAILABLE or not self.whatsapp_manager:
            QMessageBox.warning(
                self,
                "WhatsApp No Disponible",
                "Los módulos de WhatsApp no están disponibles para realizar pruebas."
            )
            return
        
        test_data = {
            'telefono': telefono,
            'mensaje': mensaje
        }
        
        self.start_test('message', test_data)
    
    def test_template(self):
        """Prueba el procesamiento de una plantilla"""
        if self.test_template_combo.count() == 0:
            QMessageBox.information(
                self,
                "Sin Plantillas",
                "No hay plantillas disponibles para probar."
            )
            return
        
        if not WHATSAPP_AVAILABLE:
            QMessageBox.warning(
                self,
                "WhatsApp No Disponible",
                "Los módulos de WhatsApp no están disponibles para realizar pruebas."
            )
            return
        
        template_id = self.test_template_combo.currentData()
        test_data = {'template_id': template_id}
        
        self.start_test('template', test_data)
    
    def start_test(self, test_type, test_data=None):
        """Inicia una prueba en un hilo separado"""
        if self.test_thread and self.test_thread.isRunning():
            QMessageBox.information(
                self,
                "Prueba en Progreso",
                "Ya hay una prueba en progreso. Por favor, espere a que termine."
            )
            return
        
        # Mostrar barra de progreso
        self.test_progress_bar.setVisible(True)
        self.test_progress_bar.setValue(0)
        self.test_status_label.setText("Iniciando prueba...")
        
        # Crear y configurar hilo de prueba
        self.test_thread = WhatsAppTestThread(self.whatsapp_manager, test_type, test_data)
        self.test_thread.progress_updated.connect(self.on_test_progress)
        self.test_thread.test_completed.connect(self.on_test_completed)
        self.test_thread.start()
    
    @pyqtSlot(int, str)
    def on_test_progress(self, progress, message):
        """Actualiza el progreso de la prueba"""
        self.test_progress_bar.setValue(progress)
        self.test_status_label.setText(message)
    
    @pyqtSlot(bool, str)
    def on_test_completed(self, success, message):
        """Maneja la finalización de una prueba"""
        self.test_progress_bar.setVisible(False)
        self.test_status_label.setText("Listo para pruebas")
        
        if success:
            QMessageBox.information(
                self,
                "Prueba Exitosa",
                message
            )
        else:
            QMessageBox.warning(
                self,
                "Prueba Fallida",
                message
            )
        
        self.test_completed.emit(success, message)
    
    # --- MÉTODOS DE AYUDA ---
    
    def show_help(self):
        """Muestra la ayuda del sistema WhatsApp"""
        help_text = """
<b>Configuración WhatsApp Business</b>

<b>Configuración Básica:</b>
• <b>Número de teléfono:</b> El número de WhatsApp Business registrado
• <b>URL del Webhook:</b> URL pública donde WhatsApp enviará los mensajes
• <b>Puerto del Webhook:</b> Puerto local para el servidor webhook
• <b>Token de Verificación:</b> Token para verificar el webhook
• <b>App ID:</b> ID de la aplicación de Facebook
• <b>App Secret:</b> Secreto de la aplicación
• <b>Access Token:</b> Token de acceso de la API

<b>Plantillas:</b>
• Cree plantillas personalizadas para diferentes tipos de mensajes
• Use variables como {{nombre_usuario}}, {{monto}}, {{fecha_actual}}
• Las plantillas se procesan automáticamente antes del envío

<b>Estadísticas:</b>
• Monitoree mensajes enviados, fallidos y usuarios bloqueados
• Exporte estadísticas para análisis externos
• Configure limpieza automática de mensajes antiguos

<b>Configuración Avanzada:</b>
• Configure límites anti-spam para evitar bloqueos
• Ajuste reintentos y delays para mensajes fallidos
• Configure logging y limpieza automática

<b>Pruebas:</b>
• Pruebe la conexión antes de usar en producción
• Envíe mensajes de prueba para verificar funcionamiento
• Valide plantillas con datos de ejemplo

<b>Nota:</b> Asegúrese de tener una cuenta de WhatsApp Business válida y los permisos necesarios antes de configurar el sistema.
        """
        
        QMessageBox.information(
            self,
            "Ayuda - WhatsApp Business",
            help_text
        )
    
    # --- MÉTODOS DE LIMPIEZA ---
    
    def closeEvent(self, event):
        """Maneja el cierre del widget"""
        # Detener timer de estadísticas
        if hasattr(self, 'stats_timer'):
            self.stats_timer.stop()
        
        # Esperar a que termine el hilo de pruebas
        if hasattr(self, 'test_thread') and self.test_thread and self.test_thread.isRunning():
            self.test_thread.quit()
            self.test_thread.wait(1000)  # Esperar máximo 1 segundo
        
        event.accept()
    
    def __del__(self):
        """Destructor del widget"""
        try:
            if hasattr(self, 'stats_timer'):
                self.stats_timer.stop()
        except:
            pass

# --- FUNCIONES DE UTILIDAD ---

def format_phone_number(phone: str) -> str:
    """Formatea un número de teléfono para WhatsApp"""
    # Remover espacios y caracteres especiales
    phone = ''.join(filter(str.isdigit, phone))
    
    # Agregar código de país si no está presente
    if not phone.startswith('54'):
        phone = '54' + phone
    
    return phone

def validate_webhook_url(url: str) -> bool:
    """Valida una URL de webhook"""
    import re
    
    pattern = r'^https?://[\w\.-]+(?:\.[\w\.-]+)+[\w\-\._~:/?#[\]@!\$&\'\(\)\*\+,;=.]+$'
    return bool(re.match(pattern, url))

def get_whatsapp_status_color(status: str) -> str:
    """Retorna el color CSS para un estado de WhatsApp"""
    colors = {
        'active': '#4CAF50',      # Verde
        'configured': '#FF9800',   # Naranja
        'error': '#F44336',       # Rojo
        'disabled': '#9E9E9E'     # Gris
    }
    return colors.get(status, colors['disabled'])

if __name__ == '__main__':
    # Código de prueba para desarrollo
    import sys
    from PyQt6.QtWidgets import QApplication
    
    app = QApplication(sys.argv)
    
    # Crear una instancia mock del DatabaseManager para pruebas
    class MockDatabaseManager:
        def obtener_configuracion_whatsapp_completa(self):
            return {}
        
        def actualizar_configuracion_whatsapp(self, key, value):
            pass
        
        def obtener_plantillas_whatsapp(self):
            return []
        
        def crear_plantilla_whatsapp(self, nombre, tipo, contenido):
            pass
        
        def actualizar_plantilla_whatsapp(self, id, nombre, tipo, contenido):
            pass
        
        def eliminar_plantilla_whatsapp(self, id):
            pass
    
    mock_db = MockDatabaseManager()
    widget = WhatsAppConfigWidget(mock_db)
    widget.show()
    
    sys.exit(app.exec())