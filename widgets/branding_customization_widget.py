import sys
import logging
import os
import json
import psycopg2.extras
from datetime import datetime
from PyQt6.QtWidgets import (
    QWidget, QVBoxLayout, QHBoxLayout, QGridLayout, QLabel, QPushButton,
    QGroupBox, QTabWidget, QColorDialog, QFileDialog, QLineEdit,
    QTextEdit, QSpinBox, QComboBox, QCheckBox, QFrame, QScrollArea,
    QMessageBox, QSlider, QFormLayout, QSizePolicy, QListWidget, QListWidgetItem,
    QDialog
)
from PyQt6.QtGui import QFont, QPixmap, QPalette, QColor, QIcon, QPainter
from PyQt6.QtCore import Qt, pyqtSignal, QSize
from utils_modules.async_utils import run_in_background

from database import DatabaseManager
from utils import resource_path, get_gym_name
from widgets.schedule_dialog import ScheduleDialog
from widgets.event_dialog import EventDialog

class ColorPreviewWidget(QWidget):
    """Widget para mostrar una vista previa del color seleccionado"""
    
    def __init__(self, color=QColor(255, 255, 255)):
        super().__init__()
        self.color = color
        self.setFixedSize(50, 30)
        self.setObjectName("color_preview_widget")
        self.update_color_style(color)
    
    def set_color(self, color):
        """Actualiza el color mostrado"""
        self.color = color
        self.update_color_style(color)
    
    def update_color_style(self, color):
        """Actualiza el estilo usando el color especificado"""
        # Usar propiedades dinámicas en lugar de estilos hardcodeados
        self.setProperty("previewColor", color.name())
        self.style().unpolish(self)
        self.style().polish(self)

class LogoPreviewWidget(QLabel):
    """Widget para mostrar una vista previa del logo"""
    
    file_dropped = pyqtSignal(str)
    
    def __init__(self):
        super().__init__()
        self.setMinimumSize(200, 120)
        self.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self.setObjectName("logo_preview_widget")
        self.setText("Sin logo")
        self.setAcceptDrops(True)
        self._original_pixmap = None
        self.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Expanding)
    
    def set_logo(self, pixmap):
        """Establece el logo a mostrar"""
        if pixmap and not pixmap.isNull():
            self._original_pixmap = pixmap
            scaled_pixmap = self._original_pixmap.scaled(
                self.size(), 
                Qt.AspectRatioMode.KeepAspectRatio, 
                Qt.TransformationMode.SmoothTransformation
            )
            self.setPixmap(scaled_pixmap)
            self.setText("")
        else:
            self._original_pixmap = None
            self.clear()
            self.setText("Sin logo")
    
    def dragEnterEvent(self, event):
        if event.mimeData().hasUrls():
            for url in event.mimeData().urls():
                if url.isLocalFile():
                    path = url.toLocalFile().lower()
                    if path.endswith((".png", ".jpg", ".jpeg", ".bmp", ".gif", ".svg")):
                        event.acceptProposedAction()
                        return
        event.ignore()
    
    def resizeEvent(self, event):
        if hasattr(self, '_original_pixmap') and self._original_pixmap and not self._original_pixmap.isNull():
            scaled_pixmap = self._original_pixmap.scaled(
                self.size(),
                Qt.AspectRatioMode.KeepAspectRatio,
                Qt.TransformationMode.SmoothTransformation
            )
            self.setPixmap(scaled_pixmap)
        super().resizeEvent(event)

    def dropEvent(self, event):
        if event.mimeData().hasUrls():
            for url in event.mimeData().urls():
                if url.isLocalFile():
                    file_path = url.toLocalFile()
                    pm = QPixmap(file_path)
                    if not pm.isNull():
                        self.set_logo(pm)
                        self.file_dropped.emit(file_path)
                        event.acceptProposedAction()
                        return
        event.ignore()

class BrandingCustomizationWidget(QWidget):
    """Widget para personalización y branding del sistema"""
    
    # Señales para notificar cambios
    branding_changed = pyqtSignal(dict)
    theme_changed = pyqtSignal()
    
    def __init__(self, db_manager: DatabaseManager):
        super().__init__()
        self.db_manager = db_manager
        self.current_branding = self.load_branding_config()
        # Cachés ligeros
        self._themes_cache = None
        self._themes_cache_ts = None
        self._advanced_cfg_cache_ts = None

        # CRÍTICO: Cargar datos del gimnasio desde gym_data.txt INMEDIATAMENTE después de load_branding_config
        # para que estén disponibles cuando se configure la interfaz
        self.sync_gym_data_with_branding()
        
        # DEBUG: Verificar que los datos se cargaron correctamente
        logging.info(f"Datos después de sync_gym_data_with_branding: {self.current_branding}")
        
        # Inicializar configuración avanzada
        self.advanced_config = {
            'spacing': {
                'general': 8,
                'content_margin': 12,
                'widget_spacing': 8
            },
            'iconography': {
                'size': 20,
                'style': 'Outline',
                'color': '#333333'
            },
            'animations': {
                'enabled': True,
                'speed': 250,
                'transition_type': 'Smooth',
                'effects': {
                    'fade': False,
                    'slide': False,
                    'scale': False
                }
            },
            'borders': {
                'radius': 4,
                'width': 1,
                'shadows': {
                    'enabled': True,
                    'intensity': 2
                }
            }
        }
        
        self.setup_ui()
        
        # Cargar configuración actual en la interfaz (los datos del gimnasio ya están cargados)
        self.load_current_settings()
        
        # DEBUG: Verificar que los datos se cargaron en la interfaz
        logging.info(f"Datos cargados en interfaz - Nombre: '{self.gym_name_edit.text()}', Eslogan: '{self.gym_slogan_edit.text()}', Teléfono: '{self.gym_phone_edit.text()}'")
        logging.info(f"Email: '{self.gym_email_edit.text()}', Website: '{self.gym_website_edit.text()}', Facebook: '{self.facebook_edit.text()}'")
        logging.info(f"Instagram: '{self.instagram_edit.text()}', Twitter: '{self.twitter_edit.text()}'")
        logging.info(f"Dirección: '{self.gym_address_edit.toPlainText()}'")
        
        # Cargar temas guardados (asíncrono con timeout/caché)
        self.load_saved_themes_async(timeout=3.0, ttl_seconds=120)
        
        # Cargar configuración avanzada desde la base de datos (asíncrono)
        self.load_advanced_config_from_db_async(timeout=3.0)
    
    def setup_ui(self):
        """Configura la interfaz de usuario"""
        main_layout = QVBoxLayout(self)
        main_layout.setSpacing(8)
        main_layout.setContentsMargins(10, 10, 10, 8)
        
        # Título eliminado para diseño edge-to-edge
        # (Se mantiene la estructura sin encabezado visual)
        
        # Tabs principales
        self.tab_widget = QTabWidget()
        self.tab_widget.setObjectName("branding_tab_widget")
        self.tab_widget.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Preferred)
        
        # Tab de Logos
        self.logos_tab = self.create_logos_tab()
        self.tab_widget.addTab(self.logos_tab, "Logos")
        
        # Tab de Colores
        self.colors_tab = self.create_colors_tab()
        self.tab_widget.addTab(self.colors_tab, "Colores")
        
        # Tab de Información Corporativa
        self.info_tab = self.create_info_tab()
        self.tab_widget.addTab(self.info_tab, "Información")
        
        
        # Tab de Programación Automática
        self.scheduling_tab = self.create_scheduling_tab()
        self.tab_widget.addTab(self.scheduling_tab, "Programación")
        
        main_layout.addWidget(self.tab_widget)
        
        # Botones de acción
        self.setup_action_buttons(main_layout)
    
    def create_logos_tab(self):
        """Crea la pestaña de configuración de logos"""
        tab = QWidget()
        layout = QVBoxLayout(tab)
        layout.setSpacing(6)
        layout.setContentsMargins(8, 8, 8, 8)
        
        # Logo principal
        main_logo_group = QGroupBox("Logo")
        main_logo_group.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Preferred)
        main_logo_layout = QGridLayout(main_logo_group)
        main_logo_layout.setSpacing(4)
        main_logo_layout.setContentsMargins(8, 8, 8, 8)
        
        self.main_logo_preview = LogoPreviewWidget()
        self.main_logo_preview.file_dropped.connect(lambda path: self.on_logo_dropped(path, 'main'))
        main_logo_layout.addWidget(QLabel("Vista previa:"), 0, 0)
        main_logo_layout.addWidget(self.main_logo_preview, 0, 1, 2, 1)
        
        self.select_main_logo_button = QPushButton("📁 Seleccionar Logo")
        self.select_main_logo_button.clicked.connect(lambda: self.select_logo('main'))
        main_logo_layout.addWidget(self.select_main_logo_button, 0, 2)
        
        self.remove_main_logo_button = QPushButton("🗑️ Quitar Logo")
        self.remove_main_logo_button.clicked.connect(lambda: self.remove_logo('main'))
        main_logo_layout.addWidget(self.remove_main_logo_button, 1, 2)
        
        layout.addWidget(main_logo_group)
        
        # Icono / Favicon
        secondary_logo_group = QGroupBox("Icono / Favicon")
        secondary_logo_group.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Preferred)
        secondary_logo_layout = QGridLayout(secondary_logo_group)
        secondary_logo_layout.setSpacing(4)
        secondary_logo_layout.setContentsMargins(8, 8, 8, 8)
        
        self.secondary_logo_preview = LogoPreviewWidget()
        self.secondary_logo_preview.file_dropped.connect(lambda path: self.on_logo_dropped(path, 'secondary'))
        secondary_logo_layout.addWidget(QLabel("Vista previa:"), 0, 0)
        secondary_logo_layout.addWidget(self.secondary_logo_preview, 0, 1, 2, 1)
        
        self.select_secondary_logo_button = QPushButton("📁 Seleccionar Icono")
        self.select_secondary_logo_button.clicked.connect(lambda: self.select_logo('secondary'))
        secondary_logo_layout.addWidget(self.select_secondary_logo_button, 0, 2)
        
        self.remove_secondary_logo_button = QPushButton("🗑️ Quitar Icono")
        self.remove_secondary_logo_button.clicked.connect(lambda: self.remove_logo('secondary'))
        secondary_logo_layout.addWidget(self.remove_secondary_logo_button, 1, 2)
        
        layout.addWidget(secondary_logo_group)
        
        # Configuración de logos
        logo_config_group = QGroupBox("Configuración de Logos")
        logo_config_group.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Preferred)
        logo_config_layout = QFormLayout(logo_config_group)
        logo_config_layout.setSpacing(4)
        logo_config_layout.setContentsMargins(8, 8, 8, 8)
        
        self.logo_size_slider = QSlider(Qt.Orientation.Horizontal)
        self.logo_size_slider.setRange(50, 200)
        self.logo_size_slider.setValue(100)
        self.logo_size_label = QLabel("100%")
        self.logo_size_slider.valueChanged.connect(lambda v: self.logo_size_label.setText(f"{v}%"))
        self.logo_size_slider.valueChanged.connect(self.on_logo_config_changed)
        
        logo_size_layout = QHBoxLayout()
        logo_size_layout.addWidget(self.logo_size_slider)
        logo_size_layout.addWidget(self.logo_size_label)
        
        logo_config_layout.addRow("Tamaño del logo:", logo_size_layout)
        
        self.show_logo_checkbox = QCheckBox("Mostrar logo en la aplicación")
        self.show_logo_checkbox.setChecked(True)
        self.show_logo_checkbox.toggled.connect(self.on_logo_config_changed)
        logo_config_layout.addRow(self.show_logo_checkbox)
        
        layout.addWidget(logo_config_group)
        layout.addStretch()
        
        return tab
    
    def create_colors_tab(self):
        """Crea la pestaña de configuración de colores"""
        tab = QWidget()
        layout = QVBoxLayout(tab)
        layout.setSpacing(6)
        layout.setContentsMargins(8, 8, 8, 8)
        
        # Colores principales
        primary_colors_group = QGroupBox("Colores Principales")
        primary_colors_group.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Preferred)
        primary_colors_layout = QGridLayout(primary_colors_group)
        primary_colors_layout.setSpacing(4)
        primary_colors_layout.setContentsMargins(8, 8, 8, 8)
        
        # Color primario
        primary_colors_layout.addWidget(QLabel("Color primario:"), 0, 0)
        self.primary_color_preview = ColorPreviewWidget(QColor("#3498db"))
        primary_colors_layout.addWidget(self.primary_color_preview, 0, 1)
        self.select_primary_color_button = QPushButton("Seleccionar")
        self.select_primary_color_button.clicked.connect(lambda: self.select_color('primary'))
        primary_colors_layout.addWidget(self.select_primary_color_button, 0, 2)
        
        # Color secundario
        primary_colors_layout.addWidget(QLabel("Color secundario:"), 1, 0)
        self.secondary_color_preview = ColorPreviewWidget(QColor("#2ecc71"))
        primary_colors_layout.addWidget(self.secondary_color_preview, 1, 1)
        self.select_secondary_color_button = QPushButton("Seleccionar")
        self.select_secondary_color_button.clicked.connect(lambda: self.select_color('secondary'))
        primary_colors_layout.addWidget(self.select_secondary_color_button, 1, 2)
        
        # Color de acento
        primary_colors_layout.addWidget(QLabel("Color de acento:"), 2, 0)
        self.accent_color_preview = ColorPreviewWidget(QColor("#e74c3c"))
        primary_colors_layout.addWidget(self.accent_color_preview, 2, 1)
        self.select_accent_color_button = QPushButton("Seleccionar")
        self.select_accent_color_button.clicked.connect(lambda: self.select_color('accent'))
        primary_colors_layout.addWidget(self.select_accent_color_button, 2, 2)
        
        layout.addWidget(primary_colors_group)
        
        # Colores de fondo
        background_colors_group = QGroupBox("Colores de Fondo")
        background_colors_group.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Preferred)
        background_colors_layout = QGridLayout(background_colors_group)
        background_colors_layout.setSpacing(4)
        background_colors_layout.setContentsMargins(8, 8, 8, 8)
        
        # Fondo principal
        background_colors_layout.addWidget(QLabel("Fondo principal:"), 0, 0)
        self.background_color_preview = ColorPreviewWidget(QColor("#ffffff"))
        background_colors_layout.addWidget(self.background_color_preview, 0, 1)
        self.select_background_color_button = QPushButton("Seleccionar")
        self.select_background_color_button.clicked.connect(lambda: self.select_color('background'))
        background_colors_layout.addWidget(self.select_background_color_button, 0, 2)
        
        # Fondo alternativo
        background_colors_layout.addWidget(QLabel("Fondo alternativo:"), 1, 0)
        self.alt_background_color_preview = ColorPreviewWidget(QColor("#f8f9fa"))
        background_colors_layout.addWidget(self.alt_background_color_preview, 1, 1)
        self.select_alt_background_color_button = QPushButton("Seleccionar")
        self.select_alt_background_color_button.clicked.connect(lambda: self.select_color('alt_background'))
        background_colors_layout.addWidget(self.select_alt_background_color_button, 1, 2)
        
        layout.addWidget(background_colors_group)
        
        # Colores de interacción (hover, focus, etc.)
        interaction_colors_group = QGroupBox("Colores de Interacción")
        interaction_colors_group.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Preferred)
        interaction_colors_layout = QGridLayout(interaction_colors_group)
        interaction_colors_layout.setSpacing(4)
        interaction_colors_layout.setContentsMargins(8, 8, 8, 8)
        
        # Color hover primario
        interaction_colors_layout.addWidget(QLabel("Hover primario:"), 0, 0)
        self.primary_hover_color_preview = ColorPreviewWidget(QColor("#2980b9"))
        interaction_colors_layout.addWidget(self.primary_hover_color_preview, 0, 1)
        self.select_primary_hover_color_button = QPushButton("Seleccionar")
        self.select_primary_hover_color_button.clicked.connect(lambda: self.select_color('primary_hover'))
        interaction_colors_layout.addWidget(self.select_primary_hover_color_button, 0, 2)
        
        # Color hover secundario
        interaction_colors_layout.addWidget(QLabel("Hover secundario:"), 1, 0)
        self.secondary_hover_color_preview = ColorPreviewWidget(QColor("#27ae60"))
        interaction_colors_layout.addWidget(self.secondary_hover_color_preview, 1, 1)
        self.select_secondary_hover_color_button = QPushButton("Seleccionar")
        self.select_secondary_hover_color_button.clicked.connect(lambda: self.select_color('secondary_hover'))
        interaction_colors_layout.addWidget(self.select_secondary_hover_color_button, 1, 2)
        
        # Color hover de acento
        interaction_colors_layout.addWidget(QLabel("Hover de acento:"), 2, 0)
        self.accent_hover_color_preview = ColorPreviewWidget(QColor("#c0392b"))
        interaction_colors_layout.addWidget(self.accent_hover_color_preview, 2, 1)
        self.select_accent_hover_color_button = QPushButton("Seleccionar")
        self.select_accent_hover_color_button.clicked.connect(lambda: self.select_color('accent_hover'))
        interaction_colors_layout.addWidget(self.select_accent_hover_color_button, 2, 2)
        
        # Botón para resetear colores hover a automático
        reset_hover_button = QPushButton("🔄 Resetear a Automático")
        reset_hover_button.clicked.connect(self.reset_hover_colors_to_auto)
        interaction_colors_layout.addWidget(reset_hover_button, 3, 0, 1, 3)
        
        layout.addWidget(interaction_colors_group)

        # Colores de estado (warning, info)
        status_colors_group = QGroupBox("Colores de Estado")
        status_colors_group.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Preferred)
        status_colors_layout = QGridLayout(status_colors_group)
        status_colors_layout.setSpacing(4)
        status_colors_layout.setContentsMargins(8, 8, 8, 8)

        # Color de advertencia (warning)
        status_colors_layout.addWidget(QLabel("Advertencia (warning):"), 0, 0)
        self.warning_color_preview = ColorPreviewWidget(QColor(self.current_branding.get('warning_color', "#EBCB8B")))
        status_colors_layout.addWidget(self.warning_color_preview, 0, 1)
        self.select_warning_color_button = QPushButton("Seleccionar")
        self.select_warning_color_button.clicked.connect(lambda: self.select_color('warning'))
        status_colors_layout.addWidget(self.select_warning_color_button, 0, 2)

        # Color de información (info)
        status_colors_layout.addWidget(QLabel("Información (info):"), 1, 0)
        self.info_color_preview = ColorPreviewWidget(QColor(self.current_branding.get('info_color', "#88C0D0")))
        status_colors_layout.addWidget(self.info_color_preview, 1, 1)
        self.select_info_color_button = QPushButton("Seleccionar")
        self.select_info_color_button.clicked.connect(lambda: self.select_color('info'))
        status_colors_layout.addWidget(self.select_info_color_button, 1, 2)

        layout.addWidget(status_colors_group)

        # Temas predefinidos
        themes_group = QGroupBox("Temas Predefinidos")
        themes_group.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Preferred)
        themes_layout = QGridLayout(themes_group)
        themes_layout.setSpacing(4)
        themes_layout.setContentsMargins(8, 8, 8, 8)
        
        theme_buttons = [
            ("🔵 Azul Clásico", self.apply_blue_theme),
            ("🟢 Verde Natura", self.apply_green_theme),
            ("🟣 Púrpura Elegante", self.apply_purple_theme),
            ("🟠 Naranja Energético", self.apply_orange_theme),
            ("⚫ Oscuro Moderno", self.apply_dark_theme),
            ("⚪ Minimalista", self.apply_minimal_theme)
        ]
        
        for i, (text, callback) in enumerate(theme_buttons):
            button = QPushButton(text)
            button.setObjectName("theme_button")
            button.clicked.connect(callback)
            themes_layout.addWidget(button, i // 3, i % 3)
        
        layout.addWidget(themes_group)
        layout.addStretch()
        
        return tab
    
    def create_info_tab(self):
        """Crea la pestaña de información corporativa"""
        tab = QWidget()
        layout = QVBoxLayout(tab)
        layout.setSpacing(6)
        layout.setContentsMargins(8, 8, 8, 8)
        
        # Información básica
        basic_info_group = QGroupBox("Información Básica")
        basic_info_group.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Preferred)
        basic_info_layout = QFormLayout(basic_info_group)
        basic_info_layout.setSpacing(4)
        basic_info_layout.setContentsMargins(8, 8, 8, 8)
        
        self.gym_name_edit = QLineEdit()
        self.gym_name_edit.setPlaceholderText("Nombre del gimnasio")
        basic_info_layout.addRow("Nombre del gimnasio:", self.gym_name_edit)
        
        self.gym_slogan_edit = QLineEdit()
        self.gym_slogan_edit.setPlaceholderText("Eslogan o lema")
        basic_info_layout.addRow("Eslogan:", self.gym_slogan_edit)
        
        self.gym_address_edit = QTextEdit()
        self.gym_address_edit.setMaximumHeight(80)
        self.gym_address_edit.setPlaceholderText("Dirección completa")
        basic_info_layout.addRow("Dirección:", self.gym_address_edit)
        
        layout.addWidget(basic_info_group)
        
        # Información de contacto
        contact_info_group = QGroupBox("Información de Contacto")
        contact_info_group.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Preferred)
        contact_info_layout = QFormLayout(contact_info_group)
        contact_info_layout.setSpacing(4)
        contact_info_layout.setContentsMargins(8, 8, 8, 8)
        
        self.gym_phone_edit = QLineEdit()
        self.gym_phone_edit.setPlaceholderText("+56 9 1234 5678")
        contact_info_layout.addRow("Teléfono:", self.gym_phone_edit)
        
        self.gym_email_edit = QLineEdit()
        self.gym_email_edit.setPlaceholderText("contacto@gimnasio.com")
        contact_info_layout.addRow("Email:", self.gym_email_edit)
        
        self.gym_website_edit = QLineEdit()
        self.gym_website_edit.setPlaceholderText("www.gimnasio.com")
        contact_info_layout.addRow("Sitio web:", self.gym_website_edit)
        
        layout.addWidget(contact_info_group)
        
        # Redes sociales
        social_media_group = QGroupBox("Redes Sociales")
        social_media_group.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Preferred)
        social_media_layout = QFormLayout(social_media_group)
        social_media_layout.setSpacing(4)
        social_media_layout.setContentsMargins(8, 8, 8, 8)
        
        self.facebook_edit = QLineEdit()
        self.facebook_edit.setPlaceholderText("@gimnasio")
        social_media_layout.addRow("Facebook:", self.facebook_edit)
        
        self.instagram_edit = QLineEdit()
        self.instagram_edit.setPlaceholderText("@gimnasio")
        social_media_layout.addRow("Instagram:", self.instagram_edit)
        
        self.twitter_edit = QLineEdit()
        self.twitter_edit.setPlaceholderText("@gimnasio")
        social_media_layout.addRow("Twitter:", self.twitter_edit)
        
        layout.addWidget(social_media_group)
        
        # Conectar cambios en información corporativa para emitir señales
        self.gym_name_edit.textChanged.connect(self.on_corporate_info_changed)
        self.gym_slogan_edit.textChanged.connect(self.on_corporate_info_changed)
        self.gym_address_edit.textChanged.connect(self.on_corporate_info_changed)
        self.gym_phone_edit.textChanged.connect(self.on_corporate_info_changed)
        self.gym_email_edit.textChanged.connect(self.on_corporate_info_changed)
        self.gym_website_edit.textChanged.connect(self.on_corporate_info_changed)
        self.facebook_edit.textChanged.connect(self.on_corporate_info_changed)
        self.instagram_edit.textChanged.connect(self.on_corporate_info_changed)
        self.twitter_edit.textChanged.connect(self.on_corporate_info_changed)
        
        layout.addStretch()
        
        return tab
    
    # Eliminado: pestaña de tipografía no utilizada
    
    def create_scheduling_tab(self):
        """Crea la pestaña de programación automática de temas"""
        tab = QWidget()
        layout = QVBoxLayout(tab)
        layout.setSpacing(6)
        layout.setContentsMargins(8, 8, 8, 8)
        
        # Grupo de programación por horarios
        schedule_group = QGroupBox("Programación por Horarios")
        schedule_group.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Preferred)
        schedule_layout = QVBoxLayout(schedule_group)
        schedule_layout.setSpacing(4)
        schedule_layout.setContentsMargins(8, 8, 8, 8)
        
        # Habilitar programación automática
        self.enable_scheduling_checkbox = QCheckBox("Habilitar programación automática")
        self.enable_scheduling_checkbox.stateChanged.connect(self.on_scheduling_enabled_changed)
        schedule_layout.addWidget(self.enable_scheduling_checkbox)
        
        # Lista de programaciones
        self.schedules_list = QListWidget()
        # Permitir expansión adaptativa de la lista de programaciones
        # self.schedules_list.setMaximumHeight(150)  # Removido para permitir expansión
        schedule_layout.addWidget(QLabel("Programaciones activas:"))
        schedule_layout.addWidget(self.schedules_list)
        
        # Botones de gestión de programaciones
        schedule_buttons_layout = QHBoxLayout()
        
        self.add_schedule_button = QPushButton("➕ Agregar Programación")
        self.add_schedule_button.setProperty("class", "success")
        self.add_schedule_button.clicked.connect(self.add_schedule)
        
        self.edit_schedule_button = QPushButton("✏️ Editar")
        self.edit_schedule_button.setProperty("class", "primary")
        self.edit_schedule_button.clicked.connect(self.edit_schedule)
        
        self.delete_schedule_button = QPushButton("🗑️ Eliminar")
        self.delete_schedule_button.setProperty("class", "danger")
        self.delete_schedule_button.clicked.connect(self.delete_schedule)
        
        schedule_buttons_layout.addWidget(self.add_schedule_button)
        schedule_buttons_layout.addWidget(self.edit_schedule_button)
        schedule_buttons_layout.addWidget(self.delete_schedule_button)
        schedule_buttons_layout.addStretch()
        
        schedule_layout.addLayout(schedule_buttons_layout)
        layout.addWidget(schedule_group)
        
        # Grupo de eventos especiales
        events_group = QGroupBox("Eventos y Temporadas Especiales")
        events_group.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Preferred)
        events_layout = QVBoxLayout(events_group)
        events_layout.setSpacing(4)
        events_layout.setContentsMargins(8, 8, 8, 8)
        
        # Lista de eventos
        self.events_list = QListWidget()
        # Permitir expansión adaptativa de la lista de eventos
        # self.events_list.setMaximumHeight(120)  # Removido para permitir expansión
        events_layout.addWidget(QLabel("Eventos programados:"))
        events_layout.addWidget(self.events_list)
        
        # Botones de gestión de eventos
        events_buttons_layout = QHBoxLayout()
        
        self.add_event_button = QPushButton("🎉 Agregar Evento")
        self.add_event_button.setProperty("class", "warning")
        self.add_event_button.clicked.connect(self.add_event)
        
        self.edit_event_button = QPushButton("✏️ Editar Evento")
        self.edit_event_button.setProperty("class", "primary")
        self.edit_event_button.clicked.connect(self.edit_event)
        
        self.delete_event_button = QPushButton("🗑️ Eliminar Evento")
        self.delete_event_button.setProperty("class", "danger")
        self.delete_event_button.clicked.connect(self.delete_event)
        
        events_buttons_layout.addWidget(self.add_event_button)
        events_buttons_layout.addWidget(self.edit_event_button)
        events_buttons_layout.addWidget(self.delete_event_button)
        events_buttons_layout.addStretch()
        
        events_layout.addLayout(events_buttons_layout)
        layout.addWidget(events_group)
        
        # Estado actual
        status_group = QGroupBox("Estado Actual")
        status_group.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Preferred)
        status_layout = QVBoxLayout(status_group)
        status_layout.setSpacing(4)
        status_layout.setContentsMargins(8, 8, 8, 8)
        
        self.current_schedule_label = QLabel("No hay programación activa")
        self.current_schedule_label.setStyleSheet("""
            QLabel {
                padding: 10px;
                background-color: #ecf0f1;
                border-radius: 5px;
                font-weight: bold;
            }
        """)
        status_layout.addWidget(self.current_schedule_label)
        
        self.next_change_label = QLabel("Próximo cambio: No programado")
        self.next_change_label.setStyleSheet("""
            QLabel {
                padding: 8px;
                color: #7f8c8d;
            }
        """)
        status_layout.addWidget(self.next_change_label)
        
        layout.addWidget(status_group)
        
        layout.addStretch()
        
        # Cargar programaciones existentes
        self.load_schedules()
        self.load_events()
        
        return tab
    
    def create_advanced_customization_tab(self):
        """Crea la pestaña de personalización avanzada de componentes"""
        # Crear scroll area para el contenido
        scroll_area = QScrollArea()
        tab = QWidget()
        layout = QVBoxLayout(tab)
        layout.setSpacing(6)
        layout.setContentsMargins(8, 8, 8, 8)
        
        # Grupo de espaciados y márgenes
        spacing_group = QGroupBox("Espaciados y Márgenes")
        spacing_group.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Preferred)
        spacing_layout = QFormLayout(spacing_group)
        spacing_layout.setSpacing(4)
        spacing_layout.setContentsMargins(8, 8, 8, 8)
        
        # Espaciado general
        self.general_spacing_slider = QSlider(Qt.Orientation.Horizontal)
        self.general_spacing_slider.setRange(5, 30)
        self.general_spacing_slider.setValue(10)
        self.general_spacing_label = QLabel("10px")
        self.general_spacing_slider.valueChanged.connect(lambda v: self.general_spacing_label.setText(f"{v}px"))
        self.general_spacing_slider.valueChanged.connect(self.on_spacing_changed)
        
        spacing_layout_h = QHBoxLayout()
        spacing_layout_h.addWidget(self.general_spacing_slider)
        spacing_layout_h.addWidget(self.general_spacing_label)
        spacing_layout.addRow("Espaciado general:", spacing_layout_h)
        
        # Margen de contenido
        self.content_margin_slider = QSlider(Qt.Orientation.Horizontal)
        self.content_margin_slider.setRange(10, 50)
        self.content_margin_slider.setValue(20)
        self.content_margin_label = QLabel("20px")
        self.content_margin_slider.valueChanged.connect(lambda v: self.content_margin_label.setText(f"{v}px"))
        self.content_margin_slider.valueChanged.connect(self.on_content_margin_changed)
        
        margin_layout_h = QHBoxLayout()
        margin_layout_h.addWidget(self.content_margin_slider)
        margin_layout_h.addWidget(self.content_margin_label)
        spacing_layout.addRow("Margen de contenido:", margin_layout_h)
        
        # Espaciado entre widgets
        self.widget_spacing_slider = QSlider(Qt.Orientation.Horizontal)
        self.widget_spacing_slider.setRange(5, 25)
        self.widget_spacing_slider.setValue(8)
        self.widget_spacing_label = QLabel("8px")
        self.widget_spacing_slider.valueChanged.connect(lambda v: self.widget_spacing_label.setText(f"{v}px"))
        self.widget_spacing_slider.valueChanged.connect(self.on_widget_spacing_changed)
        
        widget_spacing_layout_h = QHBoxLayout()
        widget_spacing_layout_h.addWidget(self.widget_spacing_slider)
        widget_spacing_layout_h.addWidget(self.widget_spacing_label)
        spacing_layout.addRow("Espaciado entre widgets:", widget_spacing_layout_h)
        
        layout.addWidget(spacing_group)
        
        # Grupo de iconografía
        icons_group = QGroupBox("Iconografía del Sistema")
        icons_group.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Preferred)
        icons_layout = QVBoxLayout(icons_group)
        icons_layout.setSpacing(4)
        icons_layout.setContentsMargins(8, 8, 8, 8)
        
        # Tamaño de iconos
        icon_size_layout = QFormLayout()
        
        self.icon_size_slider = QSlider(Qt.Orientation.Horizontal)
        self.icon_size_slider.setRange(12, 32)
        self.icon_size_slider.setValue(16)
        self.icon_size_label = QLabel("16px")
        self.icon_size_slider.valueChanged.connect(lambda v: self.icon_size_label.setText(f"{v}px"))
        self.icon_size_slider.valueChanged.connect(self.on_icon_size_changed)
        
        icon_size_layout_h = QHBoxLayout()
        icon_size_layout_h.addWidget(self.icon_size_slider)
        icon_size_layout_h.addWidget(self.icon_size_label)
        icon_size_layout.addRow("Tamaño de iconos:", icon_size_layout_h)
        
        icons_layout.addLayout(icon_size_layout)
        
        # Estilo de iconos
        icon_style_layout = QHBoxLayout()
        
        self.icon_style_combo = QComboBox()
        self.icon_style_combo.addItems(["Sólido", "Contorno", "Redondeado", "Minimalista"])
        self.icon_style_combo.currentTextChanged.connect(self.on_icon_style_changed)
        icon_style_layout.addWidget(QLabel("Estilo de iconos:"))
        icon_style_layout.addWidget(self.icon_style_combo)
        icon_style_layout.addStretch()
        
        icons_layout.addLayout(icon_style_layout)
        
        # Color de iconos
        icon_color_layout = QHBoxLayout()
        
        self.icon_color_preview = ColorPreviewWidget(QColor("#34495e"))
        self.select_icon_color_button = QPushButton("Seleccionar Color")
        self.select_icon_color_button.clicked.connect(self.select_icon_color)
        
        icon_color_layout.addWidget(QLabel("Color de iconos:"))
        icon_color_layout.addWidget(self.icon_color_preview)
        icon_color_layout.addWidget(self.select_icon_color_button)
        icon_color_layout.addStretch()
        
        icons_layout.addLayout(icon_color_layout)
        
        layout.addWidget(icons_group)
        
        # Grupo de color de textos de UI
        ui_text_group = QGroupBox("Colores de Texto UI")
        ui_text_group.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Preferred)
        ui_text_layout = QHBoxLayout(ui_text_group)
        self.ui_text_color_preview = ColorPreviewWidget(QColor(self.current_branding.get('ui_text_color', '#34495e')))
        self.select_ui_text_color_button = QPushButton("Seleccionar Color")
        self.select_ui_text_color_button.clicked.connect(lambda: self.select_color('ui_text'))
        ui_text_layout.addWidget(QLabel("Color de texto UI:"))
        ui_text_layout.addWidget(self.ui_text_color_preview)
        ui_text_layout.addWidget(self.select_ui_text_color_button)
        ui_text_layout.addStretch()
        layout.addWidget(ui_text_group)
        
        # Grupo de animaciones y transiciones
        animations_group = QGroupBox("Animaciones y Transiciones")
        animations_group.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Preferred)
        animations_layout = QFormLayout(animations_group)
        animations_layout.setSpacing(4)
        animations_layout.setContentsMargins(8, 8, 8, 8)
        
        # Habilitar animaciones
        self.enable_animations_checkbox = QCheckBox("Habilitar animaciones")
        self.enable_animations_checkbox.setChecked(True)
        self.enable_animations_checkbox.toggled.connect(self.on_animations_enabled_changed)
        animations_layout.addRow(self.enable_animations_checkbox)
        
        # Velocidad de animaciones
        self.animation_speed_slider = QSlider(Qt.Orientation.Horizontal)
        self.animation_speed_slider.setRange(100, 1000)
        self.animation_speed_slider.setValue(300)
        self.animation_speed_label = QLabel("300ms")
        self.animation_speed_slider.valueChanged.connect(lambda v: self.animation_speed_label.setText(f"{v}ms"))
        self.animation_speed_slider.valueChanged.connect(self.on_animation_speed_changed)
        
        speed_layout_h = QHBoxLayout()
        speed_layout_h.addWidget(self.animation_speed_slider)
        speed_layout_h.addWidget(self.animation_speed_label)
        animations_layout.addRow("Velocidad de animación:", speed_layout_h)
        
        # Tipo de transición
        self.transition_type_combo = QComboBox()
        self.transition_type_combo.addItems(["Suave", "Rápida", "Elástica", "Rebote"])
        self.transition_type_combo.currentTextChanged.connect(self.on_transition_type_changed)
        animations_layout.addRow("Tipo de transición:", self.transition_type_combo)
        
        # Efectos especiales
        effects_layout = QVBoxLayout()
        
        self.fade_effect_checkbox = QCheckBox("Efecto de desvanecimiento")
        self.fade_effect_checkbox.setChecked(True)
        self.fade_effect_checkbox.toggled.connect(self.on_fade_effect_changed)
        effects_layout.addWidget(self.fade_effect_checkbox)
        
        self.slide_effect_checkbox = QCheckBox("Efecto de deslizamiento")
        self.slide_effect_checkbox.setChecked(False)
        self.slide_effect_checkbox.toggled.connect(self.on_slide_effect_changed)
        effects_layout.addWidget(self.slide_effect_checkbox)
        
        self.scale_effect_checkbox = QCheckBox("Efecto de escalado")
        self.scale_effect_checkbox.setChecked(False)
        self.scale_effect_checkbox.toggled.connect(self.on_scale_effect_changed)
        effects_layout.addWidget(self.scale_effect_checkbox)
        
        animations_layout.addRow("Efectos especiales:", effects_layout)
        
        layout.addWidget(animations_group)
        
        # Grupo de configuración de bordes y sombras
        borders_group = QGroupBox("Bordes y Sombras")
        borders_group.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Preferred)
        borders_layout = QFormLayout(borders_group)
        borders_layout.setSpacing(4)
        borders_layout.setContentsMargins(8, 8, 8, 8)
        
        # Radio de bordes
        self.border_radius_slider = QSlider(Qt.Orientation.Horizontal)
        self.border_radius_slider.setRange(0, 20)
        self.border_radius_slider.setValue(5)
        self.border_radius_label = QLabel("5px")
        self.border_radius_slider.valueChanged.connect(lambda v: self.border_radius_label.setText(f"{v}px"))
        self.border_radius_slider.valueChanged.connect(self.on_border_radius_changed)
        
        radius_layout_h = QHBoxLayout()
        radius_layout_h.addWidget(self.border_radius_slider)
        radius_layout_h.addWidget(self.border_radius_label)
        borders_layout.addRow("Radio de bordes:", radius_layout_h)
        
        # Grosor de bordes
        self.border_width_slider = QSlider(Qt.Orientation.Horizontal)
        self.border_width_slider.setRange(0, 5)
        self.border_width_slider.setValue(1)
        self.border_width_label = QLabel("1px")
        self.border_width_slider.valueChanged.connect(lambda v: self.border_width_label.setText(f"{v}px"))
        self.border_width_slider.valueChanged.connect(self.on_border_width_changed)
        
        width_layout_h = QHBoxLayout()
        width_layout_h.addWidget(self.border_width_slider)
        width_layout_h.addWidget(self.border_width_label)
        borders_layout.addRow("Grosor de bordes:", width_layout_h)
        
        # Habilitar sombras
        self.enable_shadows_checkbox = QCheckBox("Habilitar sombras")
        self.enable_shadows_checkbox.setChecked(True)
        self.enable_shadows_checkbox.toggled.connect(self.on_shadows_enabled_changed)
        borders_layout.addRow(self.enable_shadows_checkbox)
        
        # Intensidad de sombras
        self.shadow_intensity_slider = QSlider(Qt.Orientation.Horizontal)
        self.shadow_intensity_slider.setRange(10, 100)
        self.shadow_intensity_slider.setValue(30)
        self.shadow_intensity_label = QLabel("30%")
        self.shadow_intensity_slider.valueChanged.connect(lambda v: self.shadow_intensity_label.setText(f"{v}%"))
        self.shadow_intensity_slider.valueChanged.connect(self.on_shadow_intensity_changed)
        
        intensity_layout_h = QHBoxLayout()
        intensity_layout_h.addWidget(self.shadow_intensity_slider)
        intensity_layout_h.addWidget(self.shadow_intensity_label)
        borders_layout.addRow("Intensidad de sombras:", intensity_layout_h)
        
        layout.addWidget(borders_group)
        
        # Botones de presets
        presets_group = QGroupBox("Configuraciones Predefinidas")
        presets_group.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Preferred)
        presets_layout = QGridLayout(presets_group)
        presets_layout.setSpacing(4)
        presets_layout.setContentsMargins(8, 8, 8, 8)
        
        preset_buttons = [
            ("🎯 Compacto", self.apply_compact_preset),
            ("📱 Móvil", self.apply_mobile_preset),
            ("🖥️ Escritorio", self.apply_desktop_preset),
            ("♿ Accesible", self.apply_accessible_preset),
            ("⚡ Rápido", self.apply_fast_preset),
            ("🎨 Artístico", self.apply_artistic_preset)
        ]
        
        for i, (text, callback) in enumerate(preset_buttons):
            button = QPushButton(text)
            button.setProperty("class", "secondary")
            button.clicked.connect(callback)
            presets_layout.addWidget(button, i // 3, i % 3)
        
        layout.addWidget(presets_group)
        
        layout.addStretch()
        
        return tab
    
    def setup_action_buttons(self, main_layout):
        """Configura los botones de acción"""
        # Primera fila de botones - Gestión de temas
        theme_buttons_layout = QHBoxLayout()
        
        self.export_button = QPushButton("📤 Exportar Tema")
        self.export_button.setProperty("class", "warning")
        self.export_button.clicked.connect(self.export_theme)
        
        self.import_button = QPushButton("📥 Importar Tema")
        self.import_button.setProperty("class", "primary")
        self.import_button.clicked.connect(self.import_theme)
        
        theme_buttons_layout.addWidget(QLabel("Gestión de Temas:"))
        theme_buttons_layout.addWidget(self.export_button)
        theme_buttons_layout.addWidget(self.import_button)
        theme_buttons_layout.addStretch()
        
        main_layout.addLayout(theme_buttons_layout)
        
        # Segunda fila de botones - Acciones principales
        buttons_layout = QHBoxLayout()
        
        self.preview_button = QPushButton("👁️ Vista Previa")
        self.preview_button.setProperty("class", "primary")
        self.preview_button.clicked.connect(self.preview_changes)
        
        self.save_button = QPushButton("💾 Guardar Cambios")
        self.save_button.setProperty("class", "success")
        self.save_button.clicked.connect(self.save_branding)
        
        self.reset_button = QPushButton("🔄 Restablecer")
        self.reset_button.setProperty("class", "danger")
        self.reset_button.clicked.connect(self.reset_to_defaults)
        
        buttons_layout.addStretch()
        buttons_layout.addWidget(self.preview_button)
        buttons_layout.addWidget(self.save_button)
        buttons_layout.addWidget(self.reset_button)
        
        main_layout.addLayout(buttons_layout)
        
        # Tercera fila - Gestión de múltiples temas
        saved_themes_layout = QHBoxLayout()
        
        self.save_theme_button = QPushButton("💾 Guardar Tema Como...")
        self.save_theme_button.setProperty("class", "success")
        self.save_theme_button.clicked.connect(self.save_custom_theme)
        
        self.themes_combo = QComboBox()
        self.themes_combo.setStyleSheet("""
            QComboBox {
                padding: 8px;
                border: 2px solid #bdc3c7;
                border-radius: 5px;
                background-color: white;
                min-width: 200px;
            }
            QComboBox:hover {
                border-color: #3498db;
            }
        """)
        self.themes_combo.currentTextChanged.connect(self.on_theme_selected)
        
        self.load_theme_button = QPushButton("📂 Cargar Tema")
        self.load_theme_button.setProperty("class", "primary")
        self.load_theme_button.clicked.connect(self.load_selected_theme)
        
        self.delete_theme_button = QPushButton("🗑️ Eliminar Tema")
        self.delete_theme_button.setProperty("class", "danger")
        self.delete_theme_button.clicked.connect(self.delete_selected_theme)
        
        saved_themes_layout.addWidget(QLabel("Temas Guardados:"))
        saved_themes_layout.addWidget(self.save_theme_button)
        saved_themes_layout.addWidget(self.themes_combo)
        saved_themes_layout.addWidget(self.load_theme_button)
        saved_themes_layout.addWidget(self.delete_theme_button)
        saved_themes_layout.addStretch()
        
        main_layout.addLayout(saved_themes_layout)
        
        # Cargar temas guardados al inicializar
        self.load_saved_themes()
    
    def select_logo(self, logo_type):
        """Selecciona un archivo de logo y lo guarda en assets"""
        file_path, _ = QFileDialog.getOpenFileName(
            self, f"Seleccionar {('logo' if logo_type=='main' else 'icono')}", "",
            "Imágenes (*.png *.jpg *.jpeg *.bmp *.gif *.svg *.ico)"
        )
        
        if file_path:
            pm = QPixmap(file_path)
            if pm.isNull():
                return
            try:
                if logo_type == 'main':
                    dest = resource_path(os.path.join('assets', 'gym_logo.png'))
                    pm.save(dest, 'PNG')
                    saved = QPixmap(dest)
                    self.main_logo_preview.set_logo(saved if not saved.isNull() else pm)
                    self.current_branding['main_logo_path'] = os.path.join('assets', 'gym_logo.png')
                    # Generar automáticamente el icono .ico a partir del logo principal
                    ico_pm = pm
                    max_size = 256
                    if ico_pm.width() > max_size or ico_pm.height() > max_size:
                        ico_pm = ico_pm.scaled(max_size, max_size, Qt.AspectRatioMode.KeepAspectRatio, Qt.TransformationMode.SmoothTransformation)
                    dest_ico = resource_path(os.path.join('assets', 'gym_logo.ico'))
                    ico_pm.save(dest_ico, 'ICO')
                    saved_ico = QPixmap(dest_ico)
                    self.secondary_logo_preview.set_logo(saved_ico if not saved_ico.isNull() else ico_pm)
                    self.current_branding['secondary_logo_path'] = os.path.join('assets', 'gym_logo.ico')
                else:
                    max_size = 256
                    if pm.width() > max_size or pm.height() > max_size:
                        pm = pm.scaled(max_size, max_size, Qt.AspectRatioMode.KeepAspectRatio, Qt.TransformationMode.SmoothTransformation)
                    dest = resource_path(os.path.join('assets', 'gym_logo.ico'))
                    pm.save(dest, 'ICO')
                    saved = QPixmap(dest)
                    self.secondary_logo_preview.set_logo(saved if not saved.isNull() else pm)
                    self.current_branding['secondary_logo_path'] = os.path.join('assets', 'gym_logo.ico')
            except Exception:
                if logo_type == 'main':
                    self.main_logo_preview.set_logo(pm)
                    self.current_branding['main_logo_path'] = file_path
                else:
                    self.secondary_logo_preview.set_logo(pm)
                    self.current_branding['secondary_logo_path'] = file_path
            self.branding_changed.emit(self.current_branding)
    
    def remove_logo(self, logo_type):
        """Quita el logo seleccionado"""
        if logo_type == 'main':
            self.main_logo_preview.set_logo(None)
            self.current_branding['main_logo_path'] = None
            # Opcional: si se muestra logo, intenta mostrar fallback en preview
            try:
                if self.show_logo_checkbox.isChecked():
                    fallback_path = resource_path(os.path.join('assets', 'gym_logo.png'))
                    if os.path.exists(fallback_path):
                        pm = QPixmap(fallback_path)
                        if not pm.isNull():
                            self.main_logo_preview.set_logo(pm)
            except Exception:
                pass
        elif logo_type == 'secondary':
            self.secondary_logo_preview.set_logo(None)
            self.current_branding['secondary_logo_path'] = None
        # Emitir señal de cambio para aplicar inmediatamente
        self.branding_changed.emit(self.current_branding)
    
    def on_logo_dropped(self, file_path: str, which: str):
        """Maneja archivos de logo arrastrados y soltados en la vista previa: siempre guarda en assets"""
        if not file_path or not os.path.exists(file_path):
            return
        pm = QPixmap(file_path)
        if pm.isNull():
            return
        try:
            if which == 'main':
                dest = resource_path(os.path.join('assets', 'gym_logo.png'))
                pm.save(dest, 'PNG')
                saved = QPixmap(dest)
                self.main_logo_preview.set_logo(saved if not saved.isNull() else pm)
                self.current_branding['main_logo_path'] = os.path.join('assets', 'gym_logo.png')
                # Generar automáticamente el icono .ico a partir del logo principal
                ico_pm = pm
                max_size = 256
                if ico_pm.width() > max_size or ico_pm.height() > max_size:
                    ico_pm = ico_pm.scaled(max_size, max_size, Qt.AspectRatioMode.KeepAspectRatio, Qt.TransformationMode.SmoothTransformation)
                dest_ico = resource_path(os.path.join('assets', 'gym_logo.ico'))
                ico_pm.save(dest_ico, 'ICO')
                saved_ico = QPixmap(dest_ico)
                self.secondary_logo_preview.set_logo(saved_ico if not saved_ico.isNull() else ico_pm)
                self.current_branding['secondary_logo_path'] = os.path.join('assets', 'gym_logo.ico')
            else:
                max_size = 256
                if pm.width() > max_size or pm.height() > max_size:
                    pm = pm.scaled(max_size, max_size, Qt.AspectRatioMode.KeepAspectRatio, Qt.TransformationMode.SmoothTransformation)
                dest = resource_path(os.path.join('assets', 'gym_logo.ico'))
                pm.save(dest, 'ICO')
                saved = QPixmap(dest)
                self.secondary_logo_preview.set_logo(saved if not saved.isNull() else pm)
                self.current_branding['secondary_logo_path'] = os.path.join('assets', 'gym_logo.ico')
        except Exception:
            if which == 'main':
                self.main_logo_preview.set_logo(pm)
                self.current_branding['main_logo_path'] = file_path
            else:
                self.secondary_logo_preview.set_logo(pm)
                self.current_branding['secondary_logo_path'] = file_path
        # Emitir señal de cambio para aplicar inmediatamente
        self.branding_changed.emit(self.current_branding)
    
    def select_color(self, color_type):
        """Abre el diálogo de selección de color"""
        current_color = getattr(self, f"{color_type}_color_preview").color
        color = QColorDialog.getColor(current_color, self, f"Seleccionar color {color_type}")
        
        if color.isValid():
            getattr(self, f"{color_type}_color_preview").set_color(color)
            self.current_branding[f"{color_type}_color"] = color.name()
            # Emitir señal de cambio inmediato para preview en tiempo real
            self.branding_changed.emit(self.current_branding)
    
    def reset_hover_colors_to_auto(self):
        """Resetea los colores hover a automático (calculados dinámicamente)"""
        # Remover colores hover personalizados del branding
        hover_keys = ['primary_hover_color', 'secondary_hover_color', 'accent_hover_color']
        for key in hover_keys:
            if key in self.current_branding:
                del self.current_branding[key]
        
        # Resetear las previsualizaciones a colores automáticos
        # Calcular colores hover automáticos basados en los colores principales
        primary_color = QColor(self.current_branding.get('primary_color', '#3498db'))
        secondary_color = QColor(self.current_branding.get('secondary_color', '#2ecc71'))
        accent_color = QColor(self.current_branding.get('accent_color', '#e74c3c'))
        
        # Oscurecer ligeramente para el efecto hover
        primary_hover = primary_color.darker(110)
        secondary_hover = secondary_color.darker(110)
        accent_hover = accent_color.darker(110)
        
        self.primary_hover_color_preview.set_color(primary_hover)
        self.secondary_hover_color_preview.set_color(secondary_hover)
        self.accent_hover_color_preview.set_color(accent_hover)
        
        # Emitir señal de cambio
        self.branding_changed.emit(self.current_branding)
    
    def apply_blue_theme(self):
        """Aplica el tema azul clásico"""
        self.primary_color_preview.set_color(QColor("#3498db"))
        self.secondary_color_preview.set_color(QColor("#2980b9"))
        self.accent_color_preview.set_color(QColor("#e74c3c"))
        self.background_color_preview.set_color(QColor("#ffffff"))
        self.alt_background_color_preview.set_color(QColor("#ecf0f1"))
        self.update_branding_colors()
        # Emitir señal de cambio para aplicar tema inmediatamente
        self.branding_changed.emit(self.current_branding)
    
    def apply_green_theme(self):
        """Aplica el tema verde natura"""
        self.primary_color_preview.set_color(QColor("#27ae60"))
        self.secondary_color_preview.set_color(QColor("#2ecc71"))
        self.accent_color_preview.set_color(QColor("#f39c12"))
        self.background_color_preview.set_color(QColor("#ffffff"))
        self.alt_background_color_preview.set_color(QColor("#f8f9fa"))
        self.update_branding_colors()
        # Emitir señal de cambio para aplicar tema inmediatamente
        self.branding_changed.emit(self.current_branding)
    
    def apply_purple_theme(self):
        """Aplica el tema púrpura elegante"""
        self.primary_color_preview.set_color(QColor("#8e44ad"))
        self.secondary_color_preview.set_color(QColor("#9b59b6"))
        self.accent_color_preview.set_color(QColor("#e67e22"))
        self.background_color_preview.set_color(QColor("#ffffff"))
        self.alt_background_color_preview.set_color(QColor("#f4f4f4"))
        self.update_branding_colors()
        # Emitir señal de cambio para aplicar tema inmediatamente
        self.branding_changed.emit(self.current_branding)
    
    def apply_orange_theme(self):
        """Aplica el tema naranja energético"""
        self.primary_color_preview.set_color(QColor("#e67e22"))
        self.secondary_color_preview.set_color(QColor("#f39c12"))
        self.accent_color_preview.set_color(QColor("#e74c3c"))
        self.background_color_preview.set_color(QColor("#ffffff"))
        self.alt_background_color_preview.set_color(QColor("#fdf2e9"))
        self.update_branding_colors()
        # Emitir señal de cambio para aplicar tema inmediatamente
        self.branding_changed.emit(self.current_branding)
    
    def apply_dark_theme(self):
        """Aplica el tema oscuro moderno"""
        self.primary_color_preview.set_color(QColor("#34495e"))
        self.secondary_color_preview.set_color(QColor("#2c3e50"))
        self.accent_color_preview.set_color(QColor("#3498db"))
        self.background_color_preview.set_color(QColor("#2c3e50"))
        self.alt_background_color_preview.set_color(QColor("#34495e"))
        self.update_branding_colors()
        # Emitir señal de cambio para aplicar tema inmediatamente
        self.branding_changed.emit(self.current_branding)
    
    def apply_minimal_theme(self):
        """Aplica el tema minimalista"""
        self.primary_color_preview.set_color(QColor("#2c3e50"))
        self.secondary_color_preview.set_color(QColor("#34495e"))
        self.accent_color_preview.set_color(QColor("#3498db"))
        self.background_color_preview.set_color(QColor("#ffffff"))
        self.alt_background_color_preview.set_color(QColor("#f8f9fa"))
        self.update_branding_colors()
        # Emitir señal de cambio para aplicar tema inmediatamente
        self.branding_changed.emit(self.current_branding)
    
    def update_branding_colors(self):
        """Actualiza los colores en la configuración de branding"""
        self.current_branding['primary_color'] = self.primary_color_preview.color.name()
        self.current_branding['secondary_color'] = self.secondary_color_preview.color.name()
        self.current_branding['accent_color'] = self.accent_color_preview.color.name()
        self.current_branding['background_color'] = self.background_color_preview.color.name()
        self.current_branding['alt_background_color'] = self.alt_background_color_preview.color.name()
        # Color de texto UI (si existe preview)
        if hasattr(self, 'ui_text_color_preview'):
            self.current_branding['ui_text_color'] = self.ui_text_color_preview.color.name()
        # Colores de estado
        if hasattr(self, 'warning_color_preview'):
            self.current_branding['warning_color'] = self.warning_color_preview.color.name()
        if hasattr(self, 'info_color_preview'):
            self.current_branding['info_color'] = self.info_color_preview.color.name()
        
        # Incluir colores hover si están definidos
        if hasattr(self, 'primary_hover_color_preview') and hasattr(self, 'secondary_hover_color_preview') and hasattr(self, 'accent_hover_color_preview'):
            # Solo incluir si no son automáticos (diferentes a los calculados automáticamente)
            primary_auto_hover = QColor(self.current_branding['primary_color']).darker(110)
            secondary_auto_hover = QColor(self.current_branding['secondary_color']).darker(110)
            accent_auto_hover = QColor(self.current_branding['accent_color']).darker(110)
            
            if self.primary_hover_color_preview.color != primary_auto_hover:
                self.current_branding['primary_hover_color'] = self.primary_hover_color_preview.color.name()
            if self.secondary_hover_color_preview.color != secondary_auto_hover:
                self.current_branding['secondary_hover_color'] = self.secondary_hover_color_preview.color.name()
            if self.accent_hover_color_preview.color != accent_auto_hover:
                self.current_branding['accent_hover_color'] = self.accent_hover_color_preview.color.name()
    
    def update_typography_preview(self):
        """Actualiza la vista previa de tipografía (si existen los controles)"""
        # Si el widget de vista previa no existe, no hacer nada
        if not hasattr(self, 'typography_preview') or self.typography_preview is None:
            return

        try:
            main_font = (
                self.main_font_combo.currentText() if hasattr(self, 'main_font_combo') and self.main_font_combo is not None
                else self.current_branding.get('main_font', 'Arial')
            )
            heading_font = (
                self.heading_font_combo.currentText() if hasattr(self, 'heading_font_combo') and self.heading_font_combo is not None
                else self.current_branding.get('heading_font', 'Arial')
            )
            base_size = (
                self.base_font_size_spinbox.value() if hasattr(self, 'base_font_size_spinbox') and self.base_font_size_spinbox is not None
                else self.current_branding.get('base_font_size', 10)
            )
            heading_size = (
                self.heading_font_size_spinbox.value() if hasattr(self, 'heading_font_size_spinbox') and self.heading_font_size_spinbox is not None
                else self.current_branding.get('heading_font_size', 16)
            )

            preview_html = f"""
            <div style="font-family: {main_font}; font-size: {base_size}pt;">
                <h1 style="font-family: {heading_font}; font-size: {heading_size}pt; color: #2c3e50;">
                    {get_gym_name('Gimnasio')}
                </h1>
                <p>Este es un ejemplo de texto normal con la fuente principal seleccionada.
                Aquí puedes ver cómo se verá el contenido regular de la aplicación.</p>
                <h2 style="font-family: {heading_font}; font-size: {heading_size-2}pt; color: #34495e;">
                    Subtítulo de Ejemplo
                </h2>
                <p>Los subtítulos y encabezados utilizarán la fuente de títulos seleccionada,
                mientras que el texto del cuerpo usará la fuente principal.</p>
            </div>
            """

            self.typography_preview.setHtml(preview_html)
        except Exception:
            # Si falta algún control, simplemente ignorar
            pass
    
    def on_typography_changed(self):
        """Maneja cambios en la configuración de tipografía (si existen los controles)"""
        try:
            updated = {}
            if hasattr(self, 'main_font_combo') and self.main_font_combo is not None:
                updated['main_font'] = self.main_font_combo.currentText()
            if hasattr(self, 'heading_font_combo') and self.heading_font_combo is not None:
                updated['heading_font'] = self.heading_font_combo.currentText()
            if hasattr(self, 'base_font_size_spinbox') and self.base_font_size_spinbox is not None:
                updated['base_font_size'] = self.base_font_size_spinbox.value()
            if hasattr(self, 'heading_font_size_spinbox') and self.heading_font_size_spinbox is not None:
                updated['heading_font_size'] = self.heading_font_size_spinbox.value()

            if updated:
                self.current_branding.update(updated)
                self.branding_changed.emit(self.current_branding)
        except Exception:
            pass
    
    def on_corporate_info_changed(self):
        """Maneja cambios en la información corporativa"""
        self.current_branding.update({
            'gym_name': self.gym_name_edit.text(),
            'gym_slogan': self.gym_slogan_edit.text(),
            'gym_address': self.gym_address_edit.toPlainText(),
            'gym_phone': self.gym_phone_edit.text(),
            'gym_email': self.gym_email_edit.text(),
            'gym_website': self.gym_website_edit.text(),
            'facebook': self.facebook_edit.text(),
            'instagram': self.instagram_edit.text(),
            'twitter': self.twitter_edit.text()
        })
        # Emitir señal de cambio para aplicar inmediatamente
        self.branding_changed.emit(self.current_branding)
    
    def on_logo_config_changed(self):
        """Maneja cambios en la configuración de logos"""
        self.current_branding.update({
            'logo_size': self.logo_size_slider.value(),
            'show_logo': self.show_logo_checkbox.isChecked()
        })
        # Emitir señal de cambio para aplicar inmediatamente
        self.branding_changed.emit(self.current_branding)
    
    def load_branding_config(self):
        """Carga la configuración de branding desde la base de datos"""
        # Configuración base por defecto
        base_config = {
            'main_logo_path': None,
            'secondary_logo_path': None,
            'primary_color': '#3498db',
            'secondary_color': '#2ecc71',
            'accent_color': '#e74c3c',
            'background_color': '#ffffff',
            'alt_background_color': '#f8f9fa',
            'main_font': 'Arial',
            'heading_font': 'Arial',
            'base_font_size': 10,
            'heading_font_size': 16,
            'logo_size': 100,
            'show_logo': True
        }
        
        try:
            branding_json = self.db_manager.obtener_configuracion('branding_config')
            if branding_json:
                config = json.loads(branding_json)
                # Remover datos del gimnasio si existen en la configuración guardada
                # Estos datos se cargan EXCLUSIVAMENTE desde gym_data.txt
                gym_data_keys = ['gym_name', 'gym_slogan', 'gym_address', 'gym_phone', 
                               'gym_email', 'gym_website', 'facebook', 'instagram', 'twitter']
                for key in gym_data_keys:
                    config.pop(key, None)
                # Actualizar configuración base con los datos de la BD
                base_config.update(config)
        except Exception as e:
            logging.warning(f"Error cargando configuración de branding: {e}")
        
        return base_config
    
    def load_current_settings(self):
        """Carga la configuración actual en la interfaz"""
        # Desconectar temporalmente las señales para evitar que on_corporate_info_changed
        # sobrescriba current_branding mientras cargamos los datos
        self._disconnect_corporate_info_signals()
        
        try:
            # Logos
            # Logo principal con fallback si no hay ruta válida
            main_path = self.current_branding.get('main_logo_path')
            main_pixmap = QPixmap(main_path) if main_path else QPixmap()
            if main_pixmap.isNull():
                try:
                    fallback_path = resource_path(os.path.join('assets', 'gym_logo.png'))
                    if os.path.exists(fallback_path):
                        main_pixmap = QPixmap(fallback_path)
                except Exception as _e:
                    pass
            self.main_logo_preview.set_logo(main_pixmap if not main_pixmap.isNull() else None)
            
            # Logo secundario con fallback a assets/gym_logo.ico
            sec_path = self.current_branding.get('secondary_logo_path')
            sec_pixmap = QPixmap(sec_path) if sec_path else QPixmap()
            if sec_pixmap.isNull():
                try:
                    fallback_ico = resource_path(os.path.join('assets', 'gym_logo.ico'))
                    if os.path.exists(fallback_ico):
                        sec_pixmap = QPixmap(fallback_ico)
                except Exception as _e:
                    pass
            self.secondary_logo_preview.set_logo(sec_pixmap if not sec_pixmap.isNull() else None)
            
            # Colores
            self.primary_color_preview.set_color(QColor(self.current_branding.get('primary_color', '#3498db')))
            self.secondary_color_preview.set_color(QColor(self.current_branding.get('secondary_color', '#2ecc71')))
            self.accent_color_preview.set_color(QColor(self.current_branding.get('accent_color', '#e74c3c')))
            self.background_color_preview.set_color(QColor(self.current_branding.get('background_color', '#ffffff')))
            self.alt_background_color_preview.set_color(QColor(self.current_branding.get('alt_background_color', '#f8f9fa')))
            
            # Color de texto UI
            if hasattr(self, 'ui_text_color_preview'):
                if 'ui_text_color' in self.current_branding:
                    self.ui_text_color_preview.set_color(QColor(self.current_branding['ui_text_color']))
                else:
                    bg = QColor(self.current_branding.get('background_color', '#ffffff'))
                    r, g, b, _ = bg.getRgb()
                    lum = 0.2126 * r + 0.7152 * g + 0.0722 * b
                    auto = QColor('#000000') if lum > 180 else QColor('#ECEFF4')
                    self.ui_text_color_preview.set_color(auto)
            
            # Colores hover (si están definidos, sino usar automáticos)
            if hasattr(self, 'primary_hover_color_preview'):
                if 'primary_hover_color' in self.current_branding:
                    self.primary_hover_color_preview.set_color(QColor(self.current_branding['primary_hover_color']))
                else:
                    # Usar color automático
                    auto_color = QColor(self.current_branding.get('primary_color', '#3498db')).darker(110)
                    self.primary_hover_color_preview.set_color(auto_color)
            
            if hasattr(self, 'secondary_hover_color_preview'):
                if 'secondary_hover_color' in self.current_branding:
                    self.secondary_hover_color_preview.set_color(QColor(self.current_branding['secondary_hover_color']))
                else:
                    # Usar color automático
                    auto_color = QColor(self.current_branding.get('secondary_color', '#2ecc71')).darker(110)
                    self.secondary_hover_color_preview.set_color(auto_color)
            
            if hasattr(self, 'accent_hover_color_preview'):
                if 'accent_hover_color' in self.current_branding:
                    self.accent_hover_color_preview.set_color(QColor(self.current_branding['accent_hover_color']))
                else:
                    # Usar color automático
                    auto_color = QColor(self.current_branding.get('accent_color', '#e74c3c')).darker(110)
                    self.accent_hover_color_preview.set_color(auto_color)
            
            # Información corporativa
            self.gym_name_edit.setText(self.current_branding.get('gym_name', ''))
            self.gym_slogan_edit.setText(self.current_branding.get('gym_slogan', ''))
            self.gym_address_edit.setPlainText(self.current_branding.get('gym_address', ''))
            self.gym_phone_edit.setText(self.current_branding.get('gym_phone', ''))
            self.gym_email_edit.setText(self.current_branding.get('gym_email', ''))
            self.gym_website_edit.setText(self.current_branding.get('gym_website', ''))
            self.facebook_edit.setText(self.current_branding.get('facebook', ''))
            self.instagram_edit.setText(self.current_branding.get('instagram', ''))
            self.twitter_edit.setText(self.current_branding.get('twitter', ''))
            
            # Tipografía: solo si los controles existen (la pestaña puede haber sido eliminada)
            try:
                if hasattr(self, 'main_font_combo') and self.main_font_combo is not None:
                    main_font = self.current_branding.get('main_font', 'Arial')
                    if main_font in [self.main_font_combo.itemText(i) for i in range(self.main_font_combo.count())]:
                        self.main_font_combo.setCurrentText(main_font)

                if hasattr(self, 'heading_font_combo') and self.heading_font_combo is not None:
                    heading_font = self.current_branding.get('heading_font', 'Arial')
                    if heading_font in [self.heading_font_combo.itemText(i) for i in range(self.heading_font_combo.count())]:
                        self.heading_font_combo.setCurrentText(heading_font)

                if hasattr(self, 'base_font_size_spinbox') and self.base_font_size_spinbox is not None:
                    self.base_font_size_spinbox.setValue(self.current_branding.get('base_font_size', 10))
                if hasattr(self, 'heading_font_size_spinbox') and self.heading_font_size_spinbox is not None:
                    self.heading_font_size_spinbox.setValue(self.current_branding.get('heading_font_size', 16))
            except Exception:
                pass
            
            # Configuración de logos
            self.logo_size_slider.setValue(self.current_branding.get('logo_size', 100))
            self.show_logo_checkbox.setChecked(self.current_branding.get('show_logo', True))
            
        finally:
            # Reconectar las señales después de cargar los datos
            self._connect_corporate_info_signals()
    
    def _disconnect_corporate_info_signals(self):
        """Desconecta temporalmente las señales de información corporativa"""
        try:
            self.gym_name_edit.textChanged.disconnect(self.on_corporate_info_changed)
            self.gym_slogan_edit.textChanged.disconnect(self.on_corporate_info_changed)
            self.gym_address_edit.textChanged.disconnect(self.on_corporate_info_changed)
            self.gym_phone_edit.textChanged.disconnect(self.on_corporate_info_changed)
            self.gym_email_edit.textChanged.disconnect(self.on_corporate_info_changed)
            self.gym_website_edit.textChanged.disconnect(self.on_corporate_info_changed)
            self.facebook_edit.textChanged.disconnect(self.on_corporate_info_changed)
            self.instagram_edit.textChanged.disconnect(self.on_corporate_info_changed)
            self.twitter_edit.textChanged.disconnect(self.on_corporate_info_changed)
        except TypeError:
            # Las señales ya estaban desconectadas
            pass
    
    def _connect_corporate_info_signals(self):
        """Reconecta las señales de información corporativa"""
        self.gym_name_edit.textChanged.connect(self.on_corporate_info_changed)
        self.gym_slogan_edit.textChanged.connect(self.on_corporate_info_changed)
        self.gym_address_edit.textChanged.connect(self.on_corporate_info_changed)
        self.gym_phone_edit.textChanged.connect(self.on_corporate_info_changed)
        self.gym_email_edit.textChanged.connect(self.on_corporate_info_changed)
        self.gym_website_edit.textChanged.connect(self.on_corporate_info_changed)
        self.facebook_edit.textChanged.connect(self.on_corporate_info_changed)
        self.instagram_edit.textChanged.connect(self.on_corporate_info_changed)
        self.twitter_edit.textChanged.connect(self.on_corporate_info_changed)
    
    def collect_current_settings(self):
        """Recopila la configuración actual de la interfaz"""
        updated = {
            'primary_color': self.primary_color_preview.color.name(),
            'secondary_color': self.secondary_color_preview.color.name(),
            'accent_color': self.accent_color_preview.color.name(),
            'background_color': self.background_color_preview.color.name(),
            'alt_background_color': self.alt_background_color_preview.color.name(),
            'ui_text_color': self.ui_text_color_preview.color.name() if hasattr(self, 'ui_text_color_preview') else self.current_branding.get('ui_text_color', ''),
            'gym_name': self.gym_name_edit.text(),
            'gym_slogan': self.gym_slogan_edit.text(),
            'gym_address': self.gym_address_edit.toPlainText(),
            'gym_phone': self.gym_phone_edit.text(),
            'gym_email': self.gym_email_edit.text(),
            'gym_website': self.gym_website_edit.text(),
            'facebook': self.facebook_edit.text(),
            'instagram': self.instagram_edit.text(),
            'twitter': self.twitter_edit.text(),
            'logo_size': self.logo_size_slider.value(),
            'show_logo': self.show_logo_checkbox.isChecked()
        }
        # Tipografía: agregar solo si existen los controles
        try:
            if hasattr(self, 'main_font_combo') and self.main_font_combo is not None:
                updated['main_font'] = self.main_font_combo.currentText()
            if hasattr(self, 'heading_font_combo') and self.heading_font_combo is not None:
                updated['heading_font'] = self.heading_font_combo.currentText()
            if hasattr(self, 'base_font_size_spinbox') and self.base_font_size_spinbox is not None:
                updated['base_font_size'] = self.base_font_size_spinbox.value()
            if hasattr(self, 'heading_font_size_spinbox') and self.heading_font_size_spinbox is not None:
                updated['heading_font_size'] = self.heading_font_size_spinbox.value()
        except Exception:
            pass

        self.current_branding.update(updated)
        
        # Incluir colores hover si están personalizados (diferentes a los automáticos)
        if hasattr(self, 'primary_hover_color_preview'):
            primary_auto_hover = QColor(self.current_branding['primary_color']).darker(110)
            if self.primary_hover_color_preview.color != primary_auto_hover:
                self.current_branding['primary_hover_color'] = self.primary_hover_color_preview.color.name()
        
        if hasattr(self, 'secondary_hover_color_preview'):
            secondary_auto_hover = QColor(self.current_branding['secondary_color']).darker(110)
            if self.secondary_hover_color_preview.color != secondary_auto_hover:
                self.current_branding['secondary_hover_color'] = self.secondary_hover_color_preview.color.name()
        
        if hasattr(self, 'accent_hover_color_preview'):
            accent_auto_hover = QColor(self.current_branding['accent_color']).darker(110)
            if self.accent_hover_color_preview.color != accent_auto_hover:
                self.current_branding['accent_hover_color'] = self.accent_hover_color_preview.color.name()
    
    def preview_changes(self):
        """Muestra una vista previa de los cambios y los aplica temporalmente"""
        self.collect_current_settings()
        
        # Aplicar cambios temporalmente para preview en tiempo real
        self.branding_changed.emit(self.current_branding)
        
        # Crear ventana de vista previa mejorada
        preview_dialog = QMessageBox(self)
        preview_dialog.setWindowTitle("Vista Previa de Branding")
        preview_dialog.setIcon(QMessageBox.Icon.Information)
        
        preview_text = f"""
        <div style="background-color: {self.current_branding.get('background_color', '#FFFFFF')}; padding: 20px; border-radius: 8px;">
            <h2 style="color: {self.current_branding.get('primary_color', '#2c3e50')}; font-family: {self.current_branding.get('heading_font', 'Arial')}; font-size: {self.current_branding.get('heading_font_size', 16)}pt; margin: 0;">
                {self.current_branding.get('gym_name') or 'Gimnasio'}
            </h2>
            <p style="color: {self.current_branding.get('secondary_color', '#34495e')}; font-style: italic; margin: 5px 0;">
                {self.current_branding.get('gym_slogan') or 'Tu eslogan aquí'}
            </p>
            <p style="font-family: {self.current_branding.get('main_font', 'Arial')}; font-size: {self.current_branding.get('base_font_size', 10)}pt; color: {self.current_branding.get('primary_color', '#2c3e50')};">
                Esta es una vista previa de cómo se verá tu branding personalizado en la aplicación.
            </p>
            <div style="background-color: {self.current_branding.get('alt_background_color', '#F8F9FA')}; padding: 10px; border-radius: 4px; margin: 10px 0;">
                <p style="margin: 0; font-family: {self.current_branding.get('main_font', 'Arial')}; font-size: {self.current_branding.get('base_font_size', 10)}pt;"><strong>Configuración actual:</strong></p>
                <ul style="margin: 5px 0; font-family: {self.current_branding.get('main_font', 'Arial')}; font-size: {self.current_branding.get('base_font_size', 10)}pt;">
                    <li>Color Primario: <span style="color: {self.current_branding.get('primary_color', '#2c3e50')};">{self.current_branding.get('primary_color', '#2c3e50')}</span></li>
                    <li>Color Secundario: <span style="color: {self.current_branding.get('secondary_color', '#34495e')};">{self.current_branding.get('secondary_color', '#34495e')}</span></li>
                    <li>Color de Acento: <span style="color: {self.current_branding.get('accent_color', '#e74c3c')};">{self.current_branding.get('accent_color', '#e74c3c')}</span></li>
                    <li>Fuente Principal: {self.current_branding.get('main_font', 'Arial')}</li>
                    <li>Fuente de Títulos: {self.current_branding.get('heading_font', 'Arial')}</li>
                </ul>
            </div>
        </div>
        """
        
        preview_dialog.setText(preview_text)
        preview_dialog.setStandardButtons(QMessageBox.StandardButton.Ok)
        preview_dialog.exec()
    
    def save_branding(self):
        """Guarda la configuración de branding"""
        try:
            self.collect_current_settings()
            
            # Guardar en la base de datos
            branding_json = json.dumps(self.current_branding, indent=2)
            self.db_manager.actualizar_configuracion('branding_config', branding_json)
            
            # Guardar también en gym_data.txt automáticamente
            gym_data_saved = self.save_branding_to_gym_data()
            
            # Emitir señal de cambio
            self.branding_changed.emit(self.current_branding)
            
            success_message = "La configuración de branding ha sido guardada exitosamente.\n\n"
            if gym_data_saved:
                success_message += "✅ Los datos del gimnasio también se guardaron en gym_data.txt\n\n"
            else:
                success_message += "⚠️ Advertencia: No se pudieron guardar los datos en gym_data.txt\n\n"
            
            success_message += "Algunos cambios pueden requerir reiniciar la aplicación para verse completamente."
            
            QMessageBox.information(
                self, "Branding Guardado", 
                success_message
            )
            
        except Exception as e:
            QMessageBox.critical(
                self, "Error", 
                f"No se pudo guardar la configuración de branding:\n{str(e)}"
            )
    
    def reset_to_defaults(self):
        """Restablece la configuración a los valores por defecto"""
        reply = QMessageBox.question(
            self, "Restablecer Configuración",
            "¿Está seguro de que desea restablecer toda la configuración de branding a los valores por defecto?\n\n"
            "Esta acción no se puede deshacer.",
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
            QMessageBox.StandardButton.No
        )
        
        if reply == QMessageBox.StandardButton.Yes:
            # Restablecer a valores por defecto
            self.current_branding = {
                'main_logo_path': None,
                'secondary_logo_path': None,
                'primary_color': '#3498db',
                'secondary_color': '#2ecc71',
                'accent_color': '#e74c3c',
                'background_color': '#ffffff',
                'alt_background_color': '#f8f9fa',
                'gym_name': get_gym_name('Gimnasio'),
                'gym_slogan': '',
                'gym_address': '',
                'gym_phone': '',
                'gym_email': '',
                'gym_website': '',
                'facebook': '',
                'instagram': '',
                'twitter': '',
                'main_font': 'Arial',
                'heading_font': 'Arial',
                'base_font_size': 10,
                'heading_font_size': 16,
                'logo_size': 100,
                'show_logo': True
            }
            
            # Cargar la configuración por defecto en la interfaz
            self.load_current_settings()
            
            # Guardar los valores por defecto en la base de datos
            try:
                branding_json = json.dumps(self.current_branding, indent=2)
                self.db_manager.actualizar_configuracion('branding_config', branding_json)
                
                # Emitir señal de cambio para aplicar inmediatamente
                self.branding_changed.emit(self.current_branding)
                
                QMessageBox.information(self, "Restablecido", "La configuración ha sido restablecida a los valores por defecto y aplicada.")
            except Exception as e:
                QMessageBox.warning(self, "Advertencia", f"La configuración se restableció pero no se pudo guardar: {str(e)}")
    
    def export_theme(self):
        """Exporta la configuración actual de branding a un archivo JSON"""
        try:
            self.collect_current_settings()
            
            # Crear datos de exportación con metadatos
            export_data = {
                'metadata': {
                    'version': '1.0',
                    'export_date': datetime.now().isoformat(),
                    'theme_name': self.current_branding.get('gym_name', 'Tema Personalizado'),
                    'description': f"Tema exportado desde {self.current_branding.get('gym_name', 'Gimnasio')}"
                },
                'branding_config': self.current_branding.copy()
            }
            
            # Abrir diálogo para guardar archivo
            file_path, _ = QFileDialog.getSaveFileName(
                self,
                "Exportar Tema de Branding",
                f"tema_{self.current_branding.get('gym_name', 'personalizado').replace(' ', '_').lower()}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json",
                "Archivos JSON (*.json);;Todos los archivos (*.*)"
            )
            
            if file_path:
                with open(file_path, 'w', encoding='utf-8') as f:
                    json.dump(export_data, f, indent=2, ensure_ascii=False)
                
                QMessageBox.information(
                    self, "Exportación Exitosa",
                    f"El tema ha sido exportado exitosamente a:\n{file_path}"
                )
                
        except Exception as e:
            QMessageBox.critical(
                self, "Error de Exportación",
                f"No se pudo exportar el tema:\n{str(e)}"
            )
    
    def import_theme(self):
        """Importa una configuración de branding desde un archivo JSON"""
        try:
            # Abrir diálogo para seleccionar archivo
            file_path, _ = QFileDialog.getOpenFileName(
                self,
                "Importar Tema de Branding",
                "",
                "Archivos JSON (*.json);;Todos los archivos (*.*)"
            )
            
            if not file_path:
                return
            
            # Leer y validar archivo
            with open(file_path, 'r', encoding='utf-8') as f:
                import_data = json.load(f)
            
            # Validar estructura del archivo
            if not self._validate_import_data(import_data):
                QMessageBox.warning(
                    self, "Archivo Inválido",
                    "El archivo seleccionado no tiene un formato válido de tema de branding."
                )
                return
            
            # Extraer configuración de branding
            if 'branding_config' in import_data:
                imported_config = import_data['branding_config']
            else:
                # Compatibilidad con archivos de configuración directa
                imported_config = import_data
            
            # Mostrar información del tema a importar
            theme_info = ""
            if 'metadata' in import_data:
                metadata = import_data['metadata']
                theme_info = f"""Información del Tema:
• Nombre: {metadata.get('theme_name', 'Sin nombre')}
• Descripción: {metadata.get('description', 'Sin descripción')}
• Fecha de exportación: {metadata.get('export_date', 'Desconocida')}
• Versión: {metadata.get('version', 'Desconocida')}

"""
            
            # Confirmar importación
            reply = QMessageBox.question(
                self, "Confirmar Importación",
                f"{theme_info}¿Desea importar este tema?\n\n"
                "Nota: Se creará un respaldo automático de la configuración actual antes de importar.",
                QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
                QMessageBox.StandardButton.No
            )
            
            if reply == QMessageBox.StandardButton.Yes:
                # Crear respaldo automático
                self._create_backup()
                
                # Aplicar configuración importada
                self.current_branding.update(imported_config)
                
                # Cargar en la interfaz
                self.load_current_settings()
                
                # Guardar en base de datos
                branding_json = json.dumps(self.current_branding, indent=2)
                self.db_manager.actualizar_configuracion('branding_config', branding_json)
                
                # Emitir señal de cambio
                self.branding_changed.emit(self.current_branding)
                
                QMessageBox.information(
                    self, "Importación Exitosa",
                    "El tema ha sido importado y aplicado exitosamente."
                )
                
        except FileNotFoundError:
            QMessageBox.warning(
                self, "Archivo No Encontrado",
                "El archivo seleccionado no existe o no se puede acceder a él."
            )
        except json.JSONDecodeError:
            QMessageBox.warning(
                self, "Archivo Inválido",
                "El archivo seleccionado no es un archivo JSON válido."
            )
        except Exception as e:
            QMessageBox.critical(
                self, "Error de Importación",
                f"No se pudo importar el tema:\n{str(e)}"
            )
    
    def _validate_import_data(self, data):
        """Valida que los datos importados tengan la estructura correcta"""
        try:
            # Si tiene metadata, validar estructura completa
            if 'metadata' in data and 'branding_config' in data:
                config = data['branding_config']
            else:
                # Asumir que es configuración directa
                config = data
            
            # Validar campos esenciales
            required_fields = ['primary_color', 'secondary_color', 'gym_name']
            for field in required_fields:
                if field not in config:
                    return False
            
            # Validar formato de colores
            color_fields = ['primary_color', 'secondary_color', 'accent_color', 'background_color', 'alt_background_color']
            for field in color_fields:
                if field in config:
                    color_value = config[field]
                    if not isinstance(color_value, str) or not color_value.startswith('#'):
                        return False
            
            return True
            
        except Exception:
            return False
    
    def _create_backup(self):
        """Crea un respaldo automático de la configuración actual"""
        try:
            import os
            
            # Crear directorio de respaldos si no existe
            backup_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'backups', 'branding')
            os.makedirs(backup_dir, exist_ok=True)
            
            # Crear archivo de respaldo
            backup_filename = f"branding_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
            backup_path = os.path.join(backup_dir, backup_filename)
            
            backup_data = {
                'metadata': {
                    'backup_date': datetime.now().isoformat(),
                    'backup_type': 'automatic',
                    'description': 'Respaldo automático antes de importar nuevo tema'
                },
                'branding_config': self.current_branding.copy()
            }
            
            with open(backup_path, 'w', encoding='utf-8') as f:
                json.dump(backup_data, f, indent=2, ensure_ascii=False)
            
            return backup_path
            
        except Exception as e:
            print(f"Error creando respaldo: {e}")
            return None
    
    def save_custom_theme(self):
        """Guarda el tema actual como un tema personalizado"""
        try:
            from PyQt6.QtWidgets import QInputDialog
            
            theme_name, ok = QInputDialog.getText(
                self, 
                'Guardar Tema Personalizado', 
                'Ingrese un nombre para el tema:',
                text=f'Mi_Tema_{datetime.now().strftime("%Y%m%d_%H%M")}'
            )
            
            if not ok or not theme_name.strip():
                return
            
            theme_name = theme_name.strip()
            
            # Verificar si el tema ya existe
            if self._theme_exists(theme_name):
                reply = QMessageBox.question(
                    self, 
                    'Tema Existente', 
                    f'Ya existe un tema con el nombre "{theme_name}". ¿Desea sobrescribirlo?',
                    QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No
                )
                if reply != QMessageBox.StandardButton.Yes:
                    return
            
            # Recopilar configuración actual
            self.collect_current_settings()
            
            # Crear estructura del tema
            theme_data = {
                'metadata': {
                    'name': theme_name,
                    'description': f'Tema personalizado creado el {datetime.now().strftime("%d/%m/%Y %H:%M")}',
                    'created_at': datetime.now().isoformat(),
                    'version': '1.0',
                    'type': 'custom_theme'
                },
                'branding_config': self.current_branding.copy()
            }
            
            # Guardar en base de datos
            self._save_theme_to_db(theme_name, theme_data)
            
            # Actualizar lista de temas
            self.load_saved_themes()
            
            # Seleccionar el tema recién guardado
            index = self.themes_combo.findText(theme_name)
            if index >= 0:
                self.themes_combo.setCurrentIndex(index)
            
            QMessageBox.information(
                self, 
                'Éxito', 
                f'Tema "{theme_name}" guardado correctamente.'
            )
            
        except Exception as e:
            QMessageBox.critical(self, "Error", f"Error al guardar el tema: {str(e)}")
    
    def load_saved_themes(self):
        """Carga la lista de temas guardados en el combo box"""
        try:
            self.themes_combo.clear()
            self.themes_combo.addItem("-- Seleccionar tema --")
            
            # Cargar temas desde la base de datos
            themes = self._get_themes_from_db()
            
            for theme_name in sorted(themes.keys()):
                self.themes_combo.addItem(theme_name)
            
        except Exception as e:
            print(f"Error al cargar temas guardados: {e}")

    def load_saved_themes_async(self, timeout: float = 3.0, ttl_seconds: int = 120):
        """Carga temas guardados de forma asíncrona con caché ligera y timeout."""
        try:
            # Usar caché si está fresca
            from time import time
            if self._themes_cache is not None and self._themes_cache_ts is not None:
                if time() - self._themes_cache_ts < ttl_seconds:
                    self._apply_themes_to_combo(self._themes_cache)
                    return

            def worker():
                return self._get_themes_from_db()

            def on_success(themes):
                self._themes_cache = themes
                from time import time as _t
                self._themes_cache_ts = _t()
                self._apply_themes_to_combo(themes)

            def on_error(err):
                logging.warning(f"Fallo cargando temas en background: {err}")
                # Fallback mínimo: mantener combo como está

            run_in_background(
                worker,
                on_success=on_success,
                on_error=on_error,
                parent=self,
                timeout_seconds=timeout,
                description="load_saved_themes"
            )
        except Exception as e:
            logging.error(f"Error disparando carga asíncrona de temas: {e}")

    def _apply_themes_to_combo(self, themes: dict):
        try:
            self.themes_combo.clear()
            self.themes_combo.addItem("-- Seleccionar tema --")
            for theme_name in sorted(themes.keys()):
                self.themes_combo.addItem(theme_name)
        except Exception as e:
            logging.error(f"Error aplicando temas al combo: {e}")
    
    def on_theme_selected(self, theme_name):
        """Maneja la selección de un tema en el combo box"""
        if theme_name and theme_name != "-- Seleccionar tema --":
            self.load_theme_button.setEnabled(True)
            self.delete_theme_button.setEnabled(True)
        else:
            self.load_theme_button.setEnabled(False)
            self.delete_theme_button.setEnabled(False)
    
    def load_selected_theme(self):
        """Carga el tema seleccionado"""
        try:
            theme_name = self.themes_combo.currentText()
            if not theme_name or theme_name == "-- Seleccionar tema --":
                QMessageBox.warning(self, "Advertencia", "Por favor seleccione un tema para cargar.")
                return
            
            # Confirmar carga del tema
            reply = QMessageBox.question(
                self, 
                'Cargar Tema', 
                f'¿Está seguro de que desea cargar el tema "{theme_name}"?\n\nEsto sobrescribirá la configuración actual.',
                QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No
            )
            
            if reply != QMessageBox.StandardButton.Yes:
                return
            
            # Crear backup antes de cargar
            self._create_backup()
            
            # Cargar tema desde la base de datos
            theme_data = self._get_theme_from_db(theme_name)
            
            if theme_data and 'branding_config' in theme_data:
                # Aplicar configuración del tema
                self.current_branding.update(theme_data['branding_config'])
                
                # Actualizar UI
                self.load_current_settings()
                
                # Guardar en la base de datos como configuración actual
                self.save_branding()
                
                QMessageBox.information(
                    self, 
                    'Éxito', 
                    f'Tema "{theme_name}" cargado correctamente.'
                )
            else:
                QMessageBox.critical(self, "Error", "No se pudo cargar el tema seleccionado.")
                
        except Exception as e:
            QMessageBox.critical(self, "Error", f"Error al cargar el tema: {str(e)}")
    
    def delete_selected_theme(self):
        """Elimina el tema seleccionado"""
        try:
            theme_name = self.themes_combo.currentText()
            if not theme_name or theme_name == "-- Seleccionar tema --":
                QMessageBox.warning(self, "Advertencia", "Por favor seleccione un tema para eliminar.")
                return
            
            # Confirmar eliminación
            reply = QMessageBox.question(
                self, 
                'Eliminar Tema', 
                f'¿Está seguro de que desea eliminar el tema "{theme_name}"?\n\nEsta acción no se puede deshacer.',
                QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No
            )
            
            if reply != QMessageBox.StandardButton.Yes:
                return
            
            # Eliminar de la base de datos
            self._delete_theme_from_db(theme_name)
            
            # Actualizar lista de temas
            self.load_saved_themes()
            
            QMessageBox.information(
                self, 
                'Éxito', 
                f'Tema "{theme_name}" eliminado correctamente.'
            )
            
        except Exception as e:
            QMessageBox.critical(self, "Error", f"Error al eliminar el tema: {str(e)}")
    
    def _theme_exists(self, theme_name):
        """Verifica si un tema ya existe"""
        try:
            themes = self._get_themes_from_db()
            return theme_name in themes
        except:
            return False
    
    def _save_theme_to_db(self, theme_name, theme_data):
        """Guarda un tema en la base de datos"""
        try:
            with self.db_manager.get_connection_context() as conn:
                cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            

            
            # Insertar o actualizar tema
            cursor.execute("""
                INSERT INTO custom_themes (name, data, updated_at) ON CONFLICT (name) DO UPDATE SET data = EXCLUDED.data, updated_at = EXCLUDED.updated_at
                VALUES (%s, %s, CURRENT_TIMESTAMP)
            """, (theme_name, json.dumps(theme_data, ensure_ascii=False)))
            
            conn.commit()
            
        except Exception as e:
            print(f"Error al guardar tema en BD: {e}")
            raise
    
    def _get_themes_from_db(self):
        """Obtiene todos los temas de la base de datos"""
        try:
            with self.db_manager.get_connection_context() as conn:
                cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
                cursor.execute("""
                    SELECT name, data FROM custom_themes ORDER BY name
                """)
                result = cursor.fetchall()
                
                themes = {}
                if result:
                    for row in result:
                        try:
                            themes[row[0]] = json.loads(row[1])
                        except json.JSONDecodeError:
                            print(f"Error al decodificar tema: {row[0]}")
                
                return themes
            
        except Exception as e:
            print(f"Error al obtener temas de BD: {e}")
            return {}
    
    def _get_theme_from_db(self, theme_name):
        """Obtiene un tema específico de la base de datos"""
        try:
            with self.db_manager.get_connection_context() as conn:
                cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
                cursor.execute("""
                    SELECT data FROM custom_themes WHERE name = %s
                """, (theme_name,))
                result = cursor.fetchone()
                
                if result:
                    return json.loads(result[0])
                
                return None
            
        except Exception as e:
            print(f"Error al obtener tema de BD: {e}")
            return None
    
    def _delete_theme_from_db(self, theme_name):
        """Elimina un tema de la base de datos"""
        try:
            with self.db_manager.get_connection_context() as conn:
                cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
                cursor.execute("""
                    DELETE FROM custom_themes WHERE name = %s
                """, (theme_name,))
                conn.commit()
            
        except Exception as e:
            print(f"Error al eliminar tema de BD: {e}")
            raise
    
    # Métodos para programación automática de temas
    def on_scheduling_enabled_changed(self, state):
        """Maneja el cambio en la habilitación de programación automática"""
        enabled = state == 2  # Qt.Checked
        
        # Habilitar/deshabilitar controles
        self.add_schedule_button.setEnabled(enabled)
        self.edit_schedule_button.setEnabled(enabled)
        self.delete_schedule_button.setEnabled(enabled)
        self.add_event_button.setEnabled(enabled)
        self.edit_event_button.setEnabled(enabled)
        self.delete_event_button.setEnabled(enabled)
        
        # Guardar estado en configuración
        try:
            with self.db_manager.get_connection_context() as conn:
                cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS scheduling_config (
                    id SERIAL PRIMARY KEY,
                    enabled BOOLEAN DEFAULT 0,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            conn.commit()
            
            cursor.execute("""
                INSERT INTO scheduling_config (id, enabled, updated_at) ON CONFLICT (id) DO UPDATE SET enabled = EXCLUDED.enabled, updated_at = EXCLUDED.updated_at
                VALUES (1, %s, CURRENT_TIMESTAMP)
            """, (enabled,))
            conn.commit()
            
        except Exception as e:
            print(f"Error al guardar configuración de programación: {e}")
    
    def add_schedule(self):
        """Abre el diálogo para agregar una nueva programación"""
        from .schedule_dialog import ScheduleDialog
        
        dialog = ScheduleDialog(self)
        if dialog.exec() == QDialog.DialogCode.Accepted:
            schedule_data = dialog.get_schedule_data()
            self._save_schedule_to_db(schedule_data)
            self.load_schedules()
            self.update_schedule_status()
    
    def edit_schedule(self):
        """Edita la programación seleccionada"""
        current_item = self.schedules_list.currentItem()
        if not current_item:
            QMessageBox.warning(self, "Advertencia", "Por favor seleccione una programación para editar.")
            return
        
        schedule_id = current_item.data(Qt.ItemDataRole.UserRole)
        schedule_data = self._get_schedule_from_db(schedule_id)
        
        if schedule_data:
            from .schedule_dialog import ScheduleDialog
            
            dialog = ScheduleDialog(self, schedule_data)
            if dialog.exec() == QDialog.DialogCode.Accepted:
                updated_data = dialog.get_schedule_data()
                updated_data['id'] = schedule_id
                self._update_schedule_in_db(updated_data)
                self.load_schedules()
                self.update_schedule_status()
    
    def delete_schedule(self):
        """Elimina la programación seleccionada"""
        current_item = self.schedules_list.currentItem()
        if not current_item:
            QMessageBox.warning(self, "Advertencia", "Por favor seleccione una programación para eliminar.")
            return
        
        reply = QMessageBox.question(
            self, 
            'Eliminar Programación', 
            '¿Está seguro de que desea eliminar esta programación?',
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No
        )
        
        if reply == QMessageBox.StandardButton.Yes:
            schedule_id = current_item.data(Qt.ItemDataRole.UserRole)
            self._delete_schedule_from_db(schedule_id)
            self.load_schedules()
            self.update_schedule_status()
    
    def add_event(self):
        """Abre el diálogo para agregar un nuevo evento"""
        from .event_dialog import EventDialog
        
        dialog = EventDialog(self)
        if dialog.exec() == QDialog.DialogCode.Accepted:
            event_data = dialog.get_event_data()
            self._save_event_to_db(event_data)
            self.load_events()
            self.update_schedule_status()
    
    def edit_event(self):
        """Edita el evento seleccionado"""
        current_item = self.events_list.currentItem()
        if not current_item:
            QMessageBox.warning(self, "Advertencia", "Por favor seleccione un evento para editar.")
            return
        
        event_id = current_item.data(Qt.ItemDataRole.UserRole)
        event_data = self._get_event_from_db(event_id)
        
        if event_data:
            from .event_dialog import EventDialog
            
            dialog = EventDialog(self, event_data)
            if dialog.exec() == QDialog.DialogCode.Accepted:
                updated_data = dialog.get_event_data()
                updated_data['id'] = event_id
                self._update_event_in_db(updated_data)
                self.load_events()
                self.update_schedule_status()
    
    def delete_event(self):
        """Elimina el evento seleccionado"""
        current_item = self.events_list.currentItem()
        if not current_item:
            QMessageBox.warning(self, "Advertencia", "Por favor seleccione un evento para eliminar.")
            return
        
        reply = QMessageBox.question(
            self, 
            'Eliminar Evento', 
            '¿Está seguro de que desea eliminar este evento?',
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No
        )
        
        if reply == QMessageBox.StandardButton.Yes:
            event_id = current_item.data(Qt.ItemDataRole.UserRole)
            self._delete_event_from_db(event_id)
            self.load_events()
            self.update_schedule_status()
    
    def load_schedules(self):
        """Carga las programaciones desde la base de datos"""
        try:
            self.schedules_list.clear()
            schedules = self._get_schedules_from_db()
            
            for schedule in schedules:
                item_text = f"{schedule['name']} - {schedule['start_time']} a {schedule['end_time']}"
                if schedule.get('days'):
                    item_text += f" ({', '.join(schedule['days'])})"
                
                item = QListWidgetItem(item_text)
                item.setData(Qt.ItemDataRole.UserRole, schedule['id'])
                self.schedules_list.addItem(item)
                
        except Exception as e:
            print(f"Error al cargar programaciones: {e}")
    
    def load_events(self):
        """Carga los eventos desde la base de datos"""
        try:
            self.events_list.clear()
            events = self._get_events_from_db()
            
            for event in events:
                item_text = f"{event['name']} - {event['start_date']} a {event['end_date']}"
                
                item = QListWidgetItem(item_text)
                item.setData(Qt.ItemDataRole.UserRole, event['id'])
                self.events_list.addItem(item)
                
        except Exception as e:
            print(f"Error al cargar eventos: {e}")
    
    def update_schedule_status(self):
        """Actualiza el estado actual de la programación"""
        try:
            current_time = datetime.now().time()
            current_date = datetime.now().date()
            current_day = datetime.now().strftime('%A').lower()
            
            # Verificar eventos activos
            active_event = self._get_active_event(current_date)
            if active_event:
                self.current_schedule_label.setText(f"Evento activo: {active_event['name']}")
                self.current_schedule_label.setStyleSheet("""
                    QLabel {
                        padding: 10px;
                        background-color: #f39c12;
                        color: white;
                        border-radius: 5px;
                        font-weight: bold;
                    }
                """)
                return
            
            # Verificar programaciones activas
            active_schedule = self._get_active_schedule(current_time, current_day)
            if active_schedule:
                self.current_schedule_label.setText(f"Programación activa: {active_schedule['name']}")
                self.current_schedule_label.setStyleSheet("""
                    QLabel {
                        padding: 10px;
                        background-color: #27ae60;
                        color: white;
                        border-radius: 5px;
                        font-weight: bold;
                    }
                """)
            else:
                self.current_schedule_label.setText("No hay programación activa")
                self.current_schedule_label.setStyleSheet("""
                    QLabel {
                        padding: 10px;
                        background-color: #ecf0f1;
                        border-radius: 5px;
                        font-weight: bold;
                    }
                """)
            
            # Obtener próximo cambio
            next_change = self._get_next_change()
            if next_change:
                self.next_change_label.setText(f"Próximo cambio: {next_change}")
            else:
                self.next_change_label.setText("Próximo cambio: No programado")
                
        except Exception as e:
            print(f"Error al actualizar estado de programación: {e}")
    
    # Métodos auxiliares para base de datos
    def _save_schedule_to_db(self, schedule_data):
        """Guarda una programación en la base de datos"""
        try:
            with self.db_manager.get_connection_context() as conn:
                cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            
            # Convertir lista de días a columnas individuales
            days_dict = {
                'monday': 'monday' in schedule_data['days'],
                'tuesday': 'tuesday' in schedule_data['days'],
                'wednesday': 'wednesday' in schedule_data['days'],
                'thursday': 'thursday' in schedule_data['days'],
                'friday': 'friday' in schedule_data['days'],
                'saturday': 'saturday' in schedule_data['days'],
                'sunday': 'sunday' in schedule_data['days']
            }
            
            cursor.execute("""
                INSERT INTO theme_schedules (name, theme_name, start_time, end_time, 
                                           monday, tuesday, wednesday, thursday, 
                                           friday, saturday, sunday)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                schedule_data['name'],
                schedule_data['theme_name'],
                schedule_data['start_time'],
                schedule_data['end_time'],
                days_dict['monday'],
                days_dict['tuesday'],
                days_dict['wednesday'],
                days_dict['thursday'],
                days_dict['friday'],
                days_dict['saturday'],
                days_dict['sunday']
            ))
            
            conn.commit()
            
        except Exception as e:
            print(f"Error al guardar programación: {e}")
            raise
    
    def _get_schedules_from_db(self):
        """Obtiene todas las programaciones de la base de datos"""
        try:
            with self.db_manager.get_connection_context() as conn:
                cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
                cursor.execute("""
                    SELECT id, name, theme_name, start_time, end_time, 
                           monday, tuesday, wednesday, thursday, friday, saturday, sunday, is_active
                    FROM theme_schedules WHERE is_active = true
                    ORDER BY start_time
                """)
                result = cursor.fetchall()
                
                schedules = []
                if result:
                    for row in result:
                        # Convertir días individuales a lista
                        days = []
                        day_names = ['monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday', 'sunday']
                        for day_name in day_names:
                            if row[day_name]:
                                days.append(day_name)
                        
                        schedules.append({
                            'id': row['id'],
                            'name': row['name'],
                            'theme_name': row['theme_name'],
                            'start_time': row['start_time'],
                            'end_time': row['end_time'],
                            'days': days,
                            'active': row['is_active']
                        })
                
                return schedules
            
        except Exception as e:
            print(f"Error al obtener programaciones: {e}")
            return []
    
    def _save_event_to_db(self, event_data):
        """Guarda un evento en la base de datos"""
        try:
            with self.db_manager.get_connection_context() as conn:
                cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            
            cursor.execute("""
                INSERT INTO theme_events (evento, theme_id, fecha_inicio, fecha_fin, descripcion)
                VALUES (%s, %s, %s, %s, %s)
            """, (
                event_data['name'],
                1,  # theme_id por defecto, debería obtenerse dinámicamente
                event_data['start_date'],
                event_data['end_date'],
                event_data.get('description', '')
            ))
            
            conn.commit()
            
        except Exception as e:
            print(f"Error al guardar evento: {e}")
            raise
    
    def _get_events_from_db(self):
        """Obtiene todos los eventos de la base de datos"""
        try:
            with self.db_manager.get_connection_context() as conn:
                cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
                # Verificar existencia de la tabla para evitar errores cuando no se ha migrado
                cursor.execute("SELECT to_regclass('public.theme_events') AS tbl")
                reg = cursor.fetchone()
                if not reg or not reg.get('tbl'):
                    # Tabla no existe: devolver lista vacía sin error
                    return []
                cursor.execute("""
                    SELECT id, evento, theme_id, fecha_inicio, fecha_fin, activo
                    FROM theme_events WHERE activo = true
                    ORDER BY fecha_inicio
                """)
                result = cursor.fetchall()
                
                events = []
                if result:
                    for row in result:
                        events.append({
                            'id': row['id'],
                            'name': row['evento'],
                            'theme_name': row['theme_id'],  # Debería hacer JOIN para obtener nombre del tema
                            'start_date': row['fecha_inicio'],
                            'end_date': row['fecha_fin'],
                            'active': row['activo']
                        })
                
                return events
            
        except Exception as e:
            print(f"Error al obtener eventos: {e}")
            return []
    
    def _get_active_event(self, current_date):
        """Obtiene el evento activo para la fecha actual"""
        try:
            with self.db_manager.get_connection_context() as conn:
                cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
                # Verificar existencia de la tabla para evitar errores cuando no se ha migrado
                cursor.execute("SELECT to_regclass('public.theme_events') AS tbl")
                reg = cursor.fetchone()
                if not reg or not reg.get('tbl'):
                    return None
                cursor.execute("""
                    SELECT id, evento, theme_id, fecha_inicio, fecha_fin
                    FROM theme_events 
                    WHERE activo = true AND %s BETWEEN fecha_inicio AND fecha_fin
                    ORDER BY fecha_inicio DESC
                    LIMIT 1
                """, (current_date.isoformat(),))
                result = cursor.fetchone()
                
                if result:
                    return {
                        'id': result['id'],
                        'name': result['evento'],
                        'theme_name': result['theme_id'],  # Debería hacer JOIN para obtener nombre del tema
                        'start_date': result['fecha_inicio'],
                        'end_date': result['fecha_fin']
                    }
                
                return None
            
        except Exception as e:
            print(f"Error al obtener evento activo: {e}")
            return None
    
    def _get_active_schedule(self, current_time, current_day):
        """Obtiene la programación activa para el horario actual"""
        try:
            schedules = self._get_schedules_from_db()
            
            for schedule in schedules:
                # Verificar si el día actual está en los días programados
                if current_day in [day.lower() for day in schedule['days']]:
                    start_time = datetime.strptime(schedule['start_time'], '%H:%M').time()
                    end_time = datetime.strptime(schedule['end_time'], '%H:%M').time()
                    
                    if start_time <= current_time <= end_time:
                        return schedule
            
            return None
            
        except Exception as e:
            print(f"Error al obtener programación activa: {e}")
            return None
    
    def _get_next_change(self):
        """Obtiene información sobre el próximo cambio programado"""
        try:
            # Implementar lógica para calcular próximo cambio
            # Por ahora retorna None
            return None
            
        except Exception as e:
            print(f"Error al obtener próximo cambio: {e}")
            return None
    
    def _get_schedule_from_db(self, schedule_id):
        """Obtiene una programación específica de la base de datos"""
        try:
            with self.db_manager.get_connection_context() as conn:
                cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
                cursor.execute("""
                    SELECT id, name, theme_name, start_time, end_time, 
                           monday, tuesday, wednesday, thursday, friday, saturday, sunday, is_active
                    FROM theme_schedules WHERE id = %s
                """, (schedule_id,))
                result = cursor.fetchone()
                
                if result:
                    # Convertir días individuales a lista
                    days = []
                    day_names = ['monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday', 'sunday']
                    for day_name in day_names:
                        if result[day_name]:
                            days.append(day_name)
                    
                    return {
                        'id': result['id'],
                        'name': result['name'],
                        'theme_name': result['theme_name'],
                        'start_time': result['start_time'],
                        'end_time': result['end_time'],
                        'days': days,
                        'active': result['is_active']
                    }
                
                return None
            
        except Exception as e:
            print(f"Error al obtener programación: {e}")
            return None
    
    def _update_schedule_in_db(self, schedule_data):
        """Actualiza una programación en la base de datos"""
        try:
            with self.db_manager.get_connection_context() as conn:
                cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
                
                # Convertir lista de días a columnas individuales
                days_dict = {
                    'monday': 'monday' in schedule_data['days'],
                    'tuesday': 'tuesday' in schedule_data['days'],
                    'wednesday': 'wednesday' in schedule_data['days'],
                    'thursday': 'thursday' in schedule_data['days'],
                    'friday': 'friday' in schedule_data['days'],
                    'saturday': 'saturday' in schedule_data['days'],
                    'sunday': 'sunday' in schedule_data['days']
                }
                
                cursor.execute("""
                    UPDATE theme_schedules 
                    SET name = %s, theme_name = %s, start_time = %s, end_time = %s, 
                        monday = %s, tuesday = %s, wednesday = %s, thursday = %s, 
                        friday = %s, saturday = %s, sunday = %s
                    WHERE id = %s
                """, (
                    schedule_data['name'],
                    schedule_data['theme_name'],
                    schedule_data['start_time'],
                    schedule_data['end_time'],
                    days_dict['monday'],
                    days_dict['tuesday'],
                    days_dict['wednesday'],
                    days_dict['thursday'],
                    days_dict['friday'],
                    days_dict['saturday'],
                    days_dict['sunday'],
                    schedule_data['id']
                ))
                conn.commit()
            
        except Exception as e:
            print(f"Error al actualizar programación: {e}")
            raise
    
    def _delete_schedule_from_db(self, schedule_id):
        """Elimina una programación de la base de datos"""
        try:
            with self.db_manager.get_connection_context() as conn:
                cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
                cursor.execute("""
                    UPDATE theme_schedules SET is_active = false WHERE id = %s
                """, (schedule_id,))
                conn.commit()
            
        except Exception as e:
            print(f"Error al eliminar programación: {e}")
            raise
    
    def _get_event_from_db(self, event_id):
        """Obtiene un evento específico de la base de datos"""
        try:
            with self.db_manager.get_connection_context() as conn:
                cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
                cursor.execute("""
                    SELECT id, name, theme_name, start_date, end_date, is_active
                    FROM theme_events WHERE id = %s
                """, (event_id,))
                result = cursor.fetchone()
                
                if result:
                    return {
                        'id': result[0],
                        'name': result[1],
                        'theme_name': result[2],
                        'start_date': result[3],
                        'end_date': result[4],
                        'active': result[5]
                    }
                
                return None
            
        except Exception as e:
            print(f"Error al obtener evento: {e}")
            return None
    
    def _update_event_in_db(self, event_data):
        """Actualiza un evento en la base de datos"""
        try:
            with self.db_manager.get_connection_context() as conn:
                cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
                cursor.execute("""
                    UPDATE theme_events 
                    SET evento = %s, theme_id = %s, fecha_inicio = %s, fecha_fin = %s, descripcion = %s
                    WHERE id = %s
                """, (
                    event_data['name'],
                    1,  # theme_id por defecto, debería obtenerse dinámicamente
                    event_data['start_date'],
                    event_data['end_date'],
                    event_data.get('description', ''),
                    event_data['id']
                ))
                conn.commit()
            
        except Exception as e:
            print(f"Error al actualizar evento: {e}")
            raise
    
    def _delete_event_from_db(self, event_id):
        """Elimina un evento de la base de datos"""
        try:
            with self.db_manager.get_connection_context() as conn:
                cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
                cursor.execute("""
                    UPDATE theme_events SET activo = false WHERE id = %s
                """, (event_id,))
                conn.commit()
            
        except Exception as e:
            print(f"Error al eliminar evento: {e}")
            raise
    
    # Métodos para personalización avanzada
    def on_spacing_changed(self, value):
        """Callback para cambio de espaciado general"""
        self.advanced_config['spacing']['general'] = value
        self.apply_advanced_config()
    
    def on_content_margin_changed(self, value):
        """Callback para cambio de margen de contenido"""
        self.advanced_config['spacing']['content_margin'] = value
        self.apply_advanced_config()
    
    def on_widget_spacing_changed(self, value):
        """Callback para cambio de espaciado entre widgets"""
        self.advanced_config['spacing']['widget_spacing'] = value
        self.apply_advanced_config()
    
    def on_icon_size_changed(self, value):
        """Callback para cambio de tamaño de iconos"""
        self.advanced_config['iconography']['size'] = value
        self.apply_advanced_config()
    
    def on_icon_style_changed(self, style):
        """Callback para cambio de estilo de iconos"""
        self.advanced_config['iconography']['style'] = style
        self.apply_advanced_config()
    
    def select_icon_color(self):
        """Abre diálogo para seleccionar color de iconos"""
        color = QColorDialog.getColor()
        if color.isValid():
            self.advanced_config['iconography']['color'] = color.name()
            self.apply_advanced_config()
    
    def on_animations_enabled_changed(self, enabled):
        """Callback para habilitar/deshabilitar animaciones"""
        self.advanced_config['animations']['enabled'] = enabled
        self.apply_advanced_config()
    
    def on_animation_speed_changed(self, value):
        """Callback para cambio de velocidad de animación"""
        self.advanced_config['animations']['speed'] = value
        self.apply_advanced_config()
    
    def on_transition_type_changed(self, transition_type):
        """Callback para cambio de tipo de transición"""
        self.advanced_config['animations']['transition_type'] = transition_type
        self.apply_advanced_config()
    
    def on_fade_effect_changed(self, enabled):
        """Callback para efecto de desvanecimiento"""
        self.advanced_config['animations']['effects']['fade'] = enabled
        self.apply_advanced_config()
    
    def on_slide_effect_changed(self, enabled):
        """Callback para efecto de deslizamiento"""
        self.advanced_config['animations']['effects']['slide'] = enabled
        self.apply_advanced_config()
    
    def on_scale_effect_changed(self, enabled):
        """Callback para efecto de escalado"""
        self.advanced_config['animations']['effects']['scale'] = enabled
        self.apply_advanced_config()
    
    def on_border_radius_changed(self, value):
        """Callback para cambio de radio de borde"""
        self.advanced_config['borders']['radius'] = value
        self.apply_advanced_config()
    
    def on_border_width_changed(self, value):
        """Callback para cambio de ancho de borde"""
        self.advanced_config['borders']['width'] = value
        self.apply_advanced_config()
    
    def on_shadows_enabled_changed(self, enabled):
        """Callback para habilitar/deshabilitar sombras"""
        self.advanced_config['borders']['shadows']['enabled'] = enabled
        self.apply_advanced_config()
    
    def on_shadow_intensity_changed(self, value):
        """Callback para cambio de intensidad de sombra"""
        self.advanced_config['borders']['shadows']['intensity'] = value
        self.apply_advanced_config()
    
    def apply_preset_compact(self):
        """Aplica configuración compacta"""
        self.advanced_config.update({
            'spacing': {'general': 4, 'content_margin': 8, 'widget_spacing': 4},
            'iconography': {'size': 16, 'style': 'Minimalist'},
            'animations': {'enabled': False, 'speed': 200},
            'borders': {'radius': 2, 'width': 1, 'shadows': {'enabled': False, 'intensity': 0}}
        })
        self.update_advanced_ui()
        self.apply_advanced_config()
    

    
    def apply_compact_preset(self):
        """Aplica configuración compacta"""
        self.advanced_config.update({
            'spacing': {'general': 4, 'content_margin': 6, 'widget_spacing': 4},
            'iconography': {'size': 16, 'style': 'Minimalist'},
            'animations': {'enabled': True, 'speed': 200, 'transition_type': 'Fast'},
            'borders': {'radius': 2, 'width': 1, 'shadows': {'enabled': False, 'intensity': 0}}
        })
        self.update_advanced_ui()
        self.apply_advanced_config()
    
    def apply_mobile_preset(self):
        """Aplica configuración optimizada para móvil"""
        self.advanced_config.update({
            'spacing': {'general': 6, 'content_margin': 8, 'widget_spacing': 6},
            'iconography': {'size': 24, 'style': 'Solid'},
            'animations': {'enabled': True, 'speed': 200, 'transition_type': 'Fast'},
            'borders': {'radius': 8, 'width': 2, 'shadows': {'enabled': True, 'intensity': 1}}
        })
        self.update_advanced_ui()
        self.apply_advanced_config()
    
    def apply_desktop_preset(self):
        """Aplica configuración para escritorio"""
        self.advanced_config.update({
            'spacing': {'general': 8, 'content_margin': 12, 'widget_spacing': 8},
            'iconography': {'size': 20, 'style': 'Outline'},
            'animations': {'enabled': True, 'speed': 250, 'transition_type': 'Fast'},
            'borders': {'radius': 4, 'width': 1, 'shadows': {'enabled': True, 'intensity': 2}}
        })
        self.update_advanced_ui()
        self.apply_advanced_config()
    
    def apply_accessible_preset(self):
        """Aplica configuración accesible"""
        self.advanced_config.update({
            'spacing': {'general': 16, 'content_margin': 20, 'widget_spacing': 16},
            'iconography': {'size': 28, 'style': 'Solid'},
            'animations': {'enabled': False, 'speed': 500},
            'borders': {'radius': 6, 'width': 3, 'shadows': {'enabled': False, 'intensity': 0}}
        })
        self.update_advanced_ui()
        self.apply_advanced_config()
    
    def apply_fast_preset(self):
        """Aplica configuración rápida"""
        self.advanced_config.update({
            'spacing': {'general': 6, 'content_margin': 10, 'widget_spacing': 6},
            'iconography': {'size': 18, 'style': 'Minimalist'},
            'animations': {'enabled': True, 'speed': 150, 'transition_type': 'Fast'},
            'borders': {'radius': 3, 'width': 1, 'shadows': {'enabled': False, 'intensity': 0}}
        })
        self.update_advanced_ui()
        self.apply_advanced_config()
    
    def apply_artistic_preset(self):
        """Aplica configuración artística"""
        self.advanced_config.update({
            'spacing': {'general': 10, 'content_margin': 15, 'widget_spacing': 10},
            'iconography': {'size': 22, 'style': 'Rounded'},
            'animations': {'enabled': True, 'speed': 400, 'transition_type': 'Elastic',
                         'effects': {'fade': True, 'slide': True, 'scale': True}},
            'borders': {'radius': 12, 'width': 2, 'shadows': {'enabled': True, 'intensity': 5}}
        })
        self.update_advanced_ui()
        self.apply_advanced_config()

    # Métodos duplicados eliminados - se mantienen solo las versiones principales
    
    def update_advanced_ui(self):
        """Actualiza la interfaz de personalización avanzada con los valores actuales"""
        if hasattr(self, 'spacing_slider'):
            self.spacing_slider.setValue(self.advanced_config.get('spacing', {}).get('general', 10))
        if hasattr(self, 'content_margin_slider'):
            self.content_margin_slider.setValue(self.advanced_config.get('spacing', {}).get('content_margin', 15))
        if hasattr(self, 'widget_spacing_slider'):
            self.widget_spacing_slider.setValue(self.advanced_config.get('spacing', {}).get('widget_spacing', 10))
        if hasattr(self, 'icon_size_slider'):
            self.icon_size_slider.setValue(self.advanced_config.get('iconography', {}).get('size', 20))
        if hasattr(self, 'icon_style_combo'):
            style = self.advanced_config.get('iconography', {}).get('style', 'Standard')
            index = self.icon_style_combo.findText(style)
            if index >= 0:
                self.icon_style_combo.setCurrentIndex(index)
        if hasattr(self, 'animations_checkbox'):
            self.animations_checkbox.setChecked(self.advanced_config.get('animations', {}).get('enabled', True))
        if hasattr(self, 'animation_speed_slider'):
            self.animation_speed_slider.setValue(self.advanced_config.get('animations', {}).get('speed', 300))
        if hasattr(self, 'transition_combo'):
            transition = self.advanced_config.get('animations', {}).get('transition_type', 'Standard')
            index = self.transition_combo.findText(transition)
            if index >= 0:
                self.transition_combo.setCurrentIndex(index)
        if hasattr(self, 'border_radius_slider'):
            self.border_radius_slider.setValue(self.advanced_config.get('borders', {}).get('radius', 8))
        if hasattr(self, 'border_width_slider'):
            self.border_width_slider.setValue(self.advanced_config.get('borders', {}).get('width', 2))
        if hasattr(self, 'shadows_checkbox'):
            shadows_config = self.advanced_config.get('borders', {}).get('shadows', {})
            self.shadows_checkbox.setChecked(shadows_config.get('enabled', True))
        if hasattr(self, 'shadow_intensity_slider'):
            shadows_config = self.advanced_config.get('borders', {}).get('shadows', {})
            intensity = shadows_config.get('intensity', 3)  # Valor por defecto
            self.shadow_intensity_slider.setValue(intensity)
    
    def apply_advanced_config(self):
        """Aplica la configuración avanzada al sistema"""
        try:
            # Guardar configuración en la base de datos
            self.save_advanced_config_to_db()
            
            # Aplicar cambios visuales inmediatamente
            self.apply_visual_changes()
            
            # Emitir señal de cambio
            self.theme_changed.emit()
            
        except Exception as e:
            print(f"Error al aplicar configuración avanzada: {e}")
    
    def save_advanced_config_to_db(self):
        """Guarda la configuración avanzada en la base de datos"""
        try:
            config_json = json.dumps(self.advanced_config)
            
            # Verificar si ya existe una configuración
            with self.db_manager.get_connection_context() as conn:
                cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
                cursor.execute("""
                    SELECT id FROM theme_scheduling_config WHERE config_type = 'advanced'
                """)
                existing = cursor.fetchone()
                
                if existing:
                    # Actualizar configuración existente
                    cursor.execute("""
                        UPDATE theme_scheduling_config 
                        SET config_data = %s, updated_at = CURRENT_TIMESTAMP
                        WHERE config_type = 'advanced'
                    """, (config_json,))
                else:
                    # Crear nueva configuración
                    cursor.execute("""
                        INSERT INTO theme_scheduling_config (config_type, config_data)
                        VALUES ('advanced', %s)
                    """, (config_json,))
                conn.commit()
                
        except Exception as e:
            print(f"Error al guardar configuración avanzada: {e}")
    
    def load_advanced_config_from_db(self):
        """Carga la configuración avanzada desde la base de datos (sincrónico)."""
        try:
            with self.db_manager.get_connection_context() as conn:
                cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
                cursor.execute("""
                    SELECT config_data FROM theme_scheduling_config 
                    WHERE config_type = 'advanced'
                """)
                result = cursor.fetchone()
                
                if result and result[0]:
                    loaded_config = json.loads(result[0])
                    self.advanced_config.update(loaded_config)
                    self.update_advanced_ui()
                
        except Exception as e:
            print(f"Error al cargar configuración avanzada: {e}")

    def load_advanced_config_from_db_async(self, timeout: float = 3.0):
        """Carga configuración avanzada en background con timeout ligero."""
        try:
            def worker():
                with self.db_manager.get_connection_context() as conn:
                    cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
                    cursor.execute("""
                        SELECT config_data FROM theme_scheduling_config 
                        WHERE config_type = 'advanced'
                    """)
                    result = cursor.fetchone()
                    if result and result[0]:
                        return json.loads(result[0])
                    return None

            def on_success(loaded):
                if loaded:
                    self.advanced_config.update(loaded)
                    self.update_advanced_ui()

            def on_error(err):
                logging.warning(f"Fallo cargando configuración avanzada: {err}")

            run_in_background(
                worker,
                on_success=on_success,
                on_error=on_error,
                parent=self,
                timeout_seconds=timeout,
                description="load_advanced_config"
            )
        except Exception as e:
            logging.error(f"Error disparando carga avanzada async: {e}")
    
    def apply_visual_changes(self):
        """Aplica los cambios visuales basados en la configuración avanzada"""
        try:
            # Aplicar espaciado
            spacing = self.advanced_config['spacing']['general']
            
            # Aplicar a layouts principales
            if hasattr(self, 'layout'):
                self.layout().setSpacing(spacing)
            
            # Aplicar márgenes de contenido
            margin = self.advanced_config['spacing']['content_margin']
            if hasattr(self, 'layout'):
                self.layout().setContentsMargins(margin, margin, margin, margin)
            
            # Aplicar configuración de bordes y sombras
            self.apply_border_styles()
            
        except Exception as e:
            print(f"Error al aplicar cambios visuales: {e}")
    
    def apply_border_styles(self):
        """Aplica estilos de borde basados en la configuración"""
        try:
            radius = self.advanced_config['borders']['radius']
            width = self.advanced_config['borders']['width']
            
            # Aplicar estilos a widgets principales
            style_sheet = f"""
                QGroupBox {{
                    border: {width}px solid #cccccc;
                    border-radius: {radius}px;
                    margin-top: 10px;
                    padding-top: 10px;
                }}
                
                QPushButton {{
                    border: {width}px solid #cccccc;
                    border-radius: {radius}px;
                    padding: 5px;
                }}
                
                QLineEdit, QTextEdit, QComboBox {{
                    border: {width}px solid #cccccc;
                    border-radius: {radius}px;
                    padding: 3px;
                }}
            """
            
            # Aplicar sombras si están habilitadas (nota: box-shadow no es compatible con PyQt6)
            # Las sombras se pueden simular con bordes más gruesos o efectos alternativos
            if self.advanced_config['borders']['shadows']['enabled']:
                intensity = self.advanced_config['borders']['shadows']['intensity']
                # Simular sombra con borde más grueso y color más oscuro
                shadow_style = f"""
                    QGroupBox, QPushButton {{
                        border-width: {width + intensity}px;
                        border-color: #999999;
                    }}
                """
                style_sheet += shadow_style
            
            self.setStyleSheet(style_sheet)
            
        except Exception as e:
            print(f"Error al aplicar estilos de borde: {e}")
    
    # Eliminado: pestaña de accesibilidad no utilizada
    
    def apply_accessibility_changes(self, settings):
        """Aplica los cambios de accesibilidad al sistema"""
        try:
            # Obtener la ventana principal para aplicar cambios globalmente
            main_window = self.get_main_window()
            
            # 1. Aplicar cambios de fuente globalmente
            if settings.get('large_text', False) or settings.get('font_size_multiplier', 100) != 100:
                self.apply_font_size_changes(settings, main_window)
            
            # 2. Aplicar esquema de alto contraste
            if settings.get('high_contrast', False):
                self.apply_high_contrast_theme(settings.get('color_scheme', 'normal'), main_window)
            else:
                # Restablecer tema normal
                self.apply_high_contrast_theme('normal', main_window)
            
            # 3. Aplicar configuraciones de navegación por teclado
            if settings.get('keyboard_navigation', False):
                self.apply_keyboard_navigation(settings, main_window)
            
            # 4. Regenerar CSS dinámico con nuevas configuraciones
            if main_window and hasattr(main_window, 'apply_dynamic_variables_to_qss'):
                main_window.apply_dynamic_variables_to_qss()
            
            # 5. Mostrar notificación de cambios aplicados
            if settings.get('show_notifications', True):
                self.show_accessibility_notification()
                
        except Exception as e:
            from PyQt6.QtWidgets import QMessageBox
            QMessageBox.warning(
                self, "Error de Accesibilidad", 
                f"No se pudieron aplicar todos los cambios de accesibilidad:\n{str(e)}"
            )
    
    def get_main_window(self):
        """Obtiene la ventana principal de la aplicación"""
        try:
            # Buscar la ventana principal navegando hacia arriba en la jerarquía
            widget = self
            while widget.parent():
                widget = widget.parent()
                if hasattr(widget, 'tabWidget') and hasattr(widget, 'tabs'):
                    return widget
            return None
        except Exception as e:
            import logging
            logging.warning(f"Error obteniendo ventana principal: {e}")
            return None
    
    def apply_font_size_changes(self, settings, main_window):
        """Aplica cambios de tamaño de fuente globalmente"""
        try:
            from PyQt6.QtWidgets import QApplication
            from PyQt6.QtGui import QFont
            
            font_multiplier = settings.get('font_size_multiplier', 100) / 100
            if settings.get('large_text', False):
                font_multiplier *= 1.2  # Incremento adicional para texto grande
            
            # Aplicar a la aplicación completa
            app = QApplication.instance()
            if app:
                current_font = app.font()
                new_size = int(current_font.pointSize() * font_multiplier)
                new_font = QFont(current_font.family(), new_size)
                
                # Aplicar fuente personalizada si está configurada
                if settings.get('font_family'):
                    new_font.setFamily(settings['font_family'])
                
                if settings.get('bold_text', False):
                    new_font.setBold(True)
                
                app.setFont(new_font)
                        
        except Exception as e:
            import logging
            logging.warning(f"Error aplicando cambios de fuente: {e}")
    
    def apply_high_contrast_theme(self, scheme, main_window=None):
        """Aplica un tema de alto contraste al widget principal"""
        try:
            from PyQt6.QtWidgets import QWidget
            
            # Aplicar clase CSS específica para alto contraste
            target_widget = main_window if main_window else self
            
            if scheme == 'high_contrast_black':
                target_widget.setObjectName("high_contrast_black_widget")
            elif scheme == 'high_contrast_white':
                target_widget.setObjectName("high_contrast_white_widget")
            elif scheme == 'blue_yellow':
                target_widget.setObjectName("blue_yellow_widget")
            else:
                # Restablecer al tema normal
                target_widget.setObjectName("")
            
            # Forzar actualización del estilo
            target_widget.style().unpolish(target_widget)
            target_widget.style().polish(target_widget)
            
            # Aplicar también a todos los widgets hijos
            if main_window:
                self.apply_theme_to_children(main_window, scheme)
                
        except Exception as e:
            import logging
            logging.warning(f"Error aplicando tema de alto contraste: {e}")
    
    def apply_theme_to_children(self, parent_widget, scheme):
        """Aplica el tema de accesibilidad a todos los widgets hijos"""
        try:
            from PyQt6.QtWidgets import QWidget
            
            for child in parent_widget.findChildren(QWidget):
                if scheme == 'high_contrast_black':
                    child.setObjectName("high_contrast_black_widget")
                elif scheme == 'high_contrast_white':
                    child.setObjectName("high_contrast_white_widget")
                elif scheme == 'blue_yellow':
                    child.setObjectName("blue_yellow_widget")
                else:
                    child.setObjectName("")
                
                child.style().unpolish(child)
                child.style().polish(child)
        except Exception as e:
            import logging
            logging.warning(f"Error aplicando tema a widgets hijos: {e}")
    
    def apply_keyboard_navigation(self, settings, main_window):
        """Aplica configuraciones de navegación por teclado"""
        try:
            from PyQt6.QtCore import Qt
            from PyQt6.QtWidgets import QWidget
            
            if main_window:
                # Habilitar navegación por teclado mejorada
                main_window.setFocusPolicy(Qt.FocusPolicy.StrongFocus)
                
                # Aplicar estilos de foco mejorados
                for widget in main_window.findChildren(QWidget):
                    if widget.focusPolicy() != Qt.FocusPolicy.NoFocus:
                        widget.setProperty("class", "accessibility-focus")
                        
        except Exception as e:
            import logging
            logging.warning(f"Error aplicando navegación por teclado: {e}")
    
    def show_accessibility_notification(self):
        """Muestra notificación de cambios de accesibilidad aplicados"""
        try:
            from PyQt6.QtWidgets import QMessageBox
            
            QMessageBox.information(
                self, "Accesibilidad", 
                "✅ Los cambios de accesibilidad han sido aplicados correctamente.\n\n"
                "🔄 Algunos cambios pueden requerir reiniciar la aplicación para tomar efecto completo.\n\n"
                "💾 Use 'Guardar Configuración' para hacer los cambios permanentes."
            )
        except Exception as e:
            import logging
            logging.warning(f"Error mostrando notificación de accesibilidad: {e}")
    
    # ==================== FUNCIONES DE MANEJO DE DATOS DEL GIMNASIO ====================
    
    def get_gym_data_file_path(self):
        """Obtiene la ruta del archivo gym_data.txt"""
        try:
            from utils import _resolve_gym_data_path
            return _resolve_gym_data_path()
        except Exception:
            current_dir = os.path.dirname(os.path.abspath(__file__))
            project_root = os.path.dirname(current_dir)
            return os.path.join(project_root, 'gym_data.txt')
    
    def load_gym_data_from_file(self):
        """Carga los datos del gimnasio desde el archivo gym_data.txt"""
        gym_data = {
            'gym_name': 'Gimnasio',
            'gym_slogan': '',
            'gym_address': '',
            'gym_phone': '',
            'gym_email': '',
            'gym_website': '',
            'facebook': '',
            'instagram': '',
            'twitter': ''
        }
        
        try:
            data_file_path = self.get_gym_data_file_path()
            if os.path.exists(data_file_path):
                with open(data_file_path, 'r', encoding='utf-8') as file:
                    for line in file:
                        line = line.strip()
                        # Ignorar líneas vacías y comentarios
                        if line and not line.startswith('#'):
                            if '=' in line:
                                key, value = line.split('=', 1)
                                key = key.strip()
                                value = value.strip()
                                # Manejar saltos de línea en direcciones
                                if key == 'gym_address':
                                    value = value.replace('\\n', '\n')
                                gym_data[key] = value
                logging.info(f"Datos del gimnasio cargados desde {data_file_path}")
            else:
                logging.warning(f"Archivo {data_file_path} no encontrado, usando valores por defecto")
                
        except Exception as e:
            logging.error(f"Error cargando datos del gimnasio: {e}")
            
        return gym_data
    
    def save_gym_data_to_file(self, gym_data):
        """Guarda los datos del gimnasio en el archivo gym_data.txt"""
        try:
            data_file_path = self.get_gym_data_file_path()
            # Crear el directorio si no existe
            os.makedirs(os.path.dirname(data_file_path), exist_ok=True)
            
            with open(data_file_path, 'w', encoding='utf-8') as file:
                file.write("# Información del Gimnasio\n")
                file.write("# Este archivo contiene toda la información básica, de contacto y redes sociales del gimnasio\n")
                file.write("# Formato: clave=valor\n\n")
                
                file.write("# Información Básica\n")
                file.write(f"gym_name={gym_data.get('gym_name', '')}\n")
                file.write(f"gym_slogan={gym_data.get('gym_slogan', '')}\n")
                # Manejar saltos de línea en direcciones
                address = gym_data.get('gym_address', '').replace('\n', '\\n')
                file.write(f"gym_address={address}\n\n")
                
                file.write("# Información de Contacto\n")
                file.write(f"gym_phone={gym_data.get('gym_phone', '')}\n")
                file.write(f"gym_email={gym_data.get('gym_email', '')}\n")
                file.write(f"gym_website={gym_data.get('gym_website', '')}\n\n")
                
                file.write("# Redes Sociales\n")
                file.write(f"facebook={gym_data.get('facebook', '')}\n")
                file.write(f"instagram={gym_data.get('instagram', '')}\n")
                file.write(f"twitter={gym_data.get('twitter', '')}\n")
            
            logging.info(f"Datos del gimnasio guardados en {data_file_path}")
            return True
            
        except Exception as e:
            logging.error(f"Error guardando datos del gimnasio: {e}")
            return False
    
    def update_gym_data_file(self, updates):
        """Actualiza datos específicos del gimnasio en el archivo"""
        current_data = self.load_gym_data_from_file()
        current_data.update(updates)
        return self.save_gym_data_to_file(current_data)
    
    def get_gym_info_from_file(self, key):
        """Obtiene un valor específico de la información del gimnasio desde el archivo"""
        gym_data = self.load_gym_data_from_file()
        return gym_data.get(key)
    
    def gym_data_file_exists(self):
        """Verifica si el archivo de datos del gimnasio existe"""
        return os.path.exists(self.get_gym_data_file_path())
    
    def sync_gym_data_with_branding(self):
        """Sincroniza los datos del archivo gym_data.txt con la configuración de branding"""
        try:
            # Cargar datos del archivo
            gym_data = self.load_gym_data_from_file()
            
            # Actualizar SOLO la configuración de branding actual (NO guardar en base de datos)
            # Los datos del gimnasio se manejan EXCLUSIVAMENTE desde el archivo gym_data.txt
            self.current_branding.update({
                'gym_name': gym_data.get('gym_name', 'Gimnasio'),
                'gym_slogan': gym_data.get('gym_slogan', ''),
                'gym_address': gym_data.get('gym_address', ''),
                'gym_phone': gym_data.get('gym_phone', ''),
                'gym_email': gym_data.get('gym_email', ''),
                'gym_website': gym_data.get('gym_website', ''),
                'facebook': gym_data.get('facebook', ''),
                'instagram': gym_data.get('instagram', ''),
                'twitter': gym_data.get('twitter', '')
            })
            
            logging.info(f"Datos del gimnasio cargados desde archivo: {gym_data}")
            logging.info("Datos del gimnasio sincronizados con la configuración de branding (solo en memoria)")
            return True
            
        except Exception as e:
            logging.error(f"Error sincronizando datos del gimnasio: {e}")
            return False
    
    def save_branding_to_gym_data(self):
        """Guarda la información del gimnasio desde branding al archivo gym_data.txt"""
        try:
            # Recopilar datos actuales de la interfaz
            self.collect_current_settings()
            
            # Extraer solo los datos del gimnasio
            gym_data = {
                'gym_name': self.current_branding.get('gym_name', ''),
                'gym_slogan': self.current_branding.get('gym_slogan', ''),
                'gym_address': self.current_branding.get('gym_address', ''),
                'gym_phone': self.current_branding.get('gym_phone', ''),
                'gym_email': self.current_branding.get('gym_email', ''),
                'gym_website': self.current_branding.get('gym_website', ''),
                'facebook': self.current_branding.get('facebook', ''),
                'instagram': self.current_branding.get('instagram', ''),
                'twitter': self.current_branding.get('twitter', '')
            }
            
            # Guardar en el archivo
            return self.save_gym_data_to_file(gym_data)
            
        except Exception as e:
            logging.error(f"Error guardando branding a gym_data.txt: {e}")
            return False