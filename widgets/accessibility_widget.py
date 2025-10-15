import sys
import logging
import json
from datetime import datetime
from PyQt6.QtWidgets import (
    QWidget, QVBoxLayout, QHBoxLayout, QGridLayout, QLabel, QPushButton,
    QGroupBox, QTabWidget, QCheckBox, QSlider, QComboBox, QSpinBox,
    QTextEdit, QFrame, QScrollArea, QMessageBox, QFormLayout,
    QButtonGroup, QRadioButton, QSizePolicy, QApplication
)
from PyQt6.QtGui import QFont, QPalette, QColor, QKeySequence, QShortcut
from PyQt6.QtCore import Qt, pyqtSignal, QSize, QTimer

from database import DatabaseManager
from utils import resource_path

class AccessibilityWidget(QWidget):
    """Widget para configurar opciones de accesibilidad del sistema"""
    
    # Señales para notificar cambios
    accessibility_changed = pyqtSignal(dict)
    
    def __init__(self, db_manager: DatabaseManager):
        super().__init__()
        self.db_manager = db_manager
        self.current_settings = self.load_accessibility_settings()
        self.setup_ui()
        self.load_current_settings()
        self.setup_keyboard_shortcuts()
    
    def setup_ui(self):
        """Configura la interfaz de usuario"""
        main_layout = QVBoxLayout(self)
        main_layout.setSpacing(12)
        main_layout.setContentsMargins(16, 16, 16, 16)
        
        # Título con mejor estilo
        title_label = QLabel("Configuración de Accesibilidad")
        title_label.setFont(QFont("Arial", 18, QFont.Weight.Bold))
        title_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        title_label.setObjectName("accessibility_title")
        # Estilos aplicados via CSS dinámico
        main_layout.addWidget(title_label)
        
        # Tabs principales con mejor estilo
        self.tab_widget = QTabWidget()
        self.tab_widget.setObjectName("accessibility_tab_widget")
        self.tab_widget.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Expanding)
        # Estilos aplicados via CSS dinámico
        
        # Tab de Navegación por Teclado
        self.keyboard_tab = self.create_keyboard_navigation_tab()
        self.tab_widget.addTab(self.keyboard_tab, "⌨️ Navegación")
        
        # Tab de Contraste y Colores
        self.contrast_tab = self.create_contrast_tab()
        self.tab_widget.addTab(self.contrast_tab, "🎨 Contraste")
        
        # Tab de Texto y Fuentes
        self.text_tab = self.create_text_tab()
        self.tab_widget.addTab(self.text_tab, "📝 Texto")
        
        # Tab de Sonidos y Alertas
        self.audio_tab = self.create_audio_tab()
        self.tab_widget.addTab(self.audio_tab, "🔊 Audio")
        
        main_layout.addWidget(self.tab_widget)
        
        # Botones de acción
        self.setup_action_buttons(main_layout)
    
    def create_keyboard_navigation_tab(self):
        """Crea la pestaña de navegación por teclado"""
        tab = QWidget()
        layout = QVBoxLayout(tab)
        layout.setSpacing(16)
        layout.setContentsMargins(20, 20, 20, 20)
        
        # Configuración general de navegación con mejor estilo
        nav_group = QGroupBox("Configuración de Navegación")
        nav_group.setObjectName("accessibility_nav_group")
        nav_group.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Preferred)
        # Estilos aplicados via CSS dinámico
        nav_layout = QFormLayout(nav_group)
        nav_layout.setSpacing(12)
        nav_layout.setContentsMargins(16, 16, 16, 16)
        
        self.enable_keyboard_nav_checkbox = QCheckBox("Habilitar navegación completa por teclado")
        self.enable_keyboard_nav_checkbox.setObjectName("accessibility_checkbox")
        self.enable_keyboard_nav_checkbox.setChecked(True)
        # Estilos aplicados via CSS dinámico
        nav_layout.addRow(self.enable_keyboard_nav_checkbox)
        
        self.show_focus_indicator_checkbox = QCheckBox("Mostrar indicador de foco visual")
        self.show_focus_indicator_checkbox.setObjectName("accessibility_checkbox")
        self.show_focus_indicator_checkbox.setChecked(True)
        # Estilos aplicados via CSS dinámico
        nav_layout.addRow(self.show_focus_indicator_checkbox)
        
        self.tab_cycling_checkbox = QCheckBox("Permitir navegación cíclica con Tab")
        self.tab_cycling_checkbox.setObjectName("accessibility_checkbox")
        self.tab_cycling_checkbox.setChecked(True)
        # Estilos aplicados via CSS dinámico
        nav_layout.addRow(self.tab_cycling_checkbox)
        
        layout.addWidget(nav_group)
        
        # Atajos de teclado personalizados con mejor estilo
        shortcuts_group = QGroupBox("Atajos de Teclado")
        shortcuts_group.setObjectName("accessibility_shortcuts_group")
        shortcuts_group.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Expanding)
        shortcuts_group.setMinimumHeight(300)  # Altura mínima para evitar cortes
        # Estilos aplicados via CSS dinámico
        
        # Crear un scroll area para el contenido de atajos
        shortcuts_scroll = QScrollArea()
        shortcuts_scroll.setWidgetResizable(True)
        shortcuts_scroll.setHorizontalScrollBarPolicy(Qt.ScrollBarPolicy.ScrollBarAlwaysOff)
        shortcuts_scroll.setVerticalScrollBarPolicy(Qt.ScrollBarPolicy.ScrollBarAsNeeded)
        shortcuts_scroll.setFrameShape(QFrame.Shape.NoFrame)
        
        shortcuts_content = QWidget()
        shortcuts_layout = QGridLayout(shortcuts_content)
        shortcuts_layout.setSpacing(12)  # Aumentar espaciado vertical
        shortcuts_layout.setContentsMargins(16, 20, 16, 20)  # Más margen superior e inferior
        
        # Etiquetas de encabezado con mejor estilo
        function_header = QLabel("Función")
        function_header.setObjectName("accessibility_header_label")
        function_header.setMinimumHeight(32)  # Altura mínima para encabezados
        shortcut_header = QLabel("Atajo Actual")
        shortcut_header.setObjectName("accessibility_header_label")
        shortcut_header.setMinimumHeight(32)  # Altura mínima para encabezados
        description_header = QLabel("Descripción")
        description_header.setObjectName("accessibility_header_label")
        description_header.setMinimumHeight(32)  # Altura mínima para encabezados
        # Estilos aplicados via CSS dinámico
        
        shortcuts_layout.addWidget(function_header, 0, 0)
        shortcuts_layout.addWidget(shortcut_header, 0, 1)
        shortcuts_layout.addWidget(description_header, 0, 2)
        
        # Lista de atajos predefinidos
        self.shortcuts_info = [
            ("Buscar Usuario", "Ctrl+F", "Buscar usuarios en la pestaña de usuarios"),
            ("Nueva Rutina", "Ctrl+N", "Crear una nueva rutina"),
            ("Guardar", "Ctrl+S", "Guardar cambios actuales"),
            ("Modo Desarrollador", "Ctrl+Shift+D", "Activar/desactivar modo desarrollador"),
            ("Exportar Datos", "Ctrl+E", "Exportar datos a Excel"),
            ("Ayuda", "F1", "Mostrar ayuda contextual"),
            ("Pantalla Completa", "F11", "Alternar pantalla completa"),
            ("Cerrar Ventana", "Alt+F4", "Cerrar la aplicación")
        ]
        
        # Estilos aplicados via CSS dinámico
        
        for i, (function, shortcut, description) in enumerate(self.shortcuts_info, 1):
            function_label = QLabel(function)
            function_label.setObjectName("accessibility_function_label")
            function_label.setMinimumHeight(28)  # Altura mínima para evitar cortes
            shortcuts_layout.addWidget(function_label, i, 0)
            
            shortcut_label = QLabel(shortcut)
            shortcut_label.setObjectName("accessibility_shortcut_label")
            shortcut_label.setMinimumHeight(28)  # Altura mínima para evitar cortes
            shortcuts_layout.addWidget(shortcut_label, i, 1)
            
            description_label = QLabel(description)
            description_label.setObjectName("accessibility_description_label")
            description_label.setMinimumHeight(28)  # Altura mínima para evitar cortes
            description_label.setWordWrap(True)  # Permitir ajuste de línea
            shortcuts_layout.addWidget(description_label, i, 2)
        
        # Configurar el scroll area
        shortcuts_scroll.setWidget(shortcuts_content)
        
        # Layout del grupo de atajos
        shortcuts_group_layout = QVBoxLayout(shortcuts_group)
        shortcuts_group_layout.setContentsMargins(8, 20, 8, 8)
        shortcuts_group_layout.addWidget(shortcuts_scroll)
        
        layout.addWidget(shortcuts_group)
        
        # Configuración de velocidad de navegación con mejor estilo
        speed_group = QGroupBox("Velocidad de Navegación")
        speed_group.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Preferred)
        # Usar sistema CSS dinámico en lugar de estilos hardcodeados
        speed_group.setObjectName("accessibility_speed_group")
        speed_group.setProperty("class", "config_group")
        speed_layout = QFormLayout(speed_group)
        speed_layout.setSpacing(12)
        speed_layout.setContentsMargins(16, 16, 16, 16)
        
        self.key_repeat_delay_slider = QSlider(Qt.Orientation.Horizontal)
        self.key_repeat_delay_slider.setObjectName("accessibility_slider")
        self.key_repeat_delay_slider.setRange(100, 1000)
        self.key_repeat_delay_slider.setValue(500)
        # Estilos aplicados via CSS dinámico
        self.key_repeat_delay_label = QLabel("500ms")
        self.key_repeat_delay_label.setObjectName("accessibility_value_label")
        self.key_repeat_delay_slider.valueChanged.connect(lambda v: self.key_repeat_delay_label.setText(f"{v}ms"))
        
        delay_layout = QHBoxLayout()
        delay_layout.addWidget(self.key_repeat_delay_slider)
        delay_layout.addWidget(self.key_repeat_delay_label)
        
        speed_layout.addRow("Retraso de repetición de teclas:", delay_layout)
        
        self.focus_timeout_slider = QSlider(Qt.Orientation.Horizontal)
        self.focus_timeout_slider.setObjectName("accessibility_slider")
        self.focus_timeout_slider.setRange(1000, 10000)
        self.focus_timeout_slider.setValue(3000)
        # Estilos aplicados via CSS dinámico
        self.focus_timeout_label = QLabel("3s")
        self.focus_timeout_label.setObjectName("accessibility_value_label")
        self.focus_timeout_slider.valueChanged.connect(lambda v: self.focus_timeout_label.setText(f"{v//1000}s"))
        
        timeout_layout = QHBoxLayout()
        timeout_layout.addWidget(self.focus_timeout_slider)
        timeout_layout.addWidget(self.focus_timeout_label)
        
        speed_layout.addRow("Tiempo de espera del foco:", timeout_layout)
        
        layout.addWidget(speed_group)
        layout.addStretch()
        return tab
    
    def create_contrast_tab(self):
        """Crea la pestaña de configuración de contraste"""
        tab = QWidget()
        layout = QVBoxLayout(tab)
        layout.setSpacing(6)
        layout.setContentsMargins(8, 8, 8, 8)
        
        # Modo de alto contraste
        contrast_group = QGroupBox("Modo de Alto Contraste")
        contrast_group.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Preferred)
        contrast_layout = QVBoxLayout(contrast_group)
        contrast_layout.setSpacing(4)
        contrast_layout.setContentsMargins(8, 8, 8, 8)
        
        self.high_contrast_checkbox = QCheckBox("Activar modo de alto contraste")
        contrast_layout.addWidget(self.high_contrast_checkbox)
        
        # Esquemas de color predefinidos
        schemes_group = QGroupBox("Esquemas de Color")
        schemes_group.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Preferred)
        schemes_layout = QGridLayout(schemes_group)
        schemes_layout.setSpacing(4)
        schemes_layout.setContentsMargins(8, 8, 8, 8)
        
        self.color_scheme_group = QButtonGroup()
        
        schemes = [
            ("normal", "🌈 Normal", "Colores estándar del sistema"),
            ("high_contrast_black", "⚫ Alto Contraste Negro", "Texto blanco sobre fondo negro"),
            ("high_contrast_white", "⚪ Alto Contraste Blanco", "Texto negro sobre fondo blanco"),
            ("blue_yellow", "🔵 Azul-Amarillo", "Esquema amigable para daltonismo"),
            ("grayscale", "⚫ Escala de Grises", "Solo tonos de gris")
        ]
        
        for i, (scheme_id, name, description) in enumerate(schemes):
            radio = QRadioButton(name)
            radio.setProperty("scheme_id", scheme_id)
            if scheme_id == "normal":
                radio.setChecked(True)
            self.color_scheme_group.addButton(radio)
            
            schemes_layout.addWidget(radio, i, 0)
            schemes_layout.addWidget(QLabel(description), i, 1)
        
        contrast_layout.addWidget(schemes_group)
        layout.addWidget(contrast_group)
        
        # Configuración de contraste personalizada
        custom_contrast_group = QGroupBox("Configuración Personalizada")
        custom_contrast_group.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Preferred)
        custom_layout = QFormLayout(custom_contrast_group)
        custom_layout.setSpacing(4)
        custom_layout.setContentsMargins(8, 8, 8, 8)
        
        self.contrast_level_slider = QSlider(Qt.Orientation.Horizontal)
        self.contrast_level_slider.setRange(50, 200)
        self.contrast_level_slider.setValue(100)
        self.contrast_level_label = QLabel("100%")
        self.contrast_level_slider.valueChanged.connect(lambda v: self.contrast_level_label.setText(f"{v}%"))
        
        contrast_slider_layout = QHBoxLayout()
        contrast_slider_layout.addWidget(self.contrast_level_slider)
        contrast_slider_layout.addWidget(self.contrast_level_label)
        
        custom_layout.addRow("Nivel de contraste:", contrast_slider_layout)
        
        self.brightness_slider = QSlider(Qt.Orientation.Horizontal)
        self.brightness_slider.setRange(50, 150)
        self.brightness_slider.setValue(100)
        self.brightness_label = QLabel("100%")
        self.brightness_slider.valueChanged.connect(lambda v: self.brightness_label.setText(f"{v}%"))
        
        brightness_slider_layout = QHBoxLayout()
        brightness_slider_layout.addWidget(self.brightness_slider)
        brightness_slider_layout.addWidget(self.brightness_label)
        
        custom_layout.addRow("Brillo:", brightness_slider_layout)
        
        layout.addWidget(custom_contrast_group)
        
        # Vista previa
        preview_group = QGroupBox("Vista Previa")
        preview_group.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Preferred)
        preview_layout = QVBoxLayout(preview_group)
        preview_layout.setSpacing(4)
        preview_layout.setContentsMargins(8, 8, 8, 8)
        
        self.contrast_preview = QTextEdit()
        self.contrast_preview.setReadOnly(True)
        # Permitir expansión adaptativa del preview de contraste
        # self.contrast_preview.setMaximumHeight(120)  # Removido para permitir expansión
        self.update_contrast_preview()
        preview_layout.addWidget(self.contrast_preview)
        
        # Conectar cambios para actualizar vista previa
        self.high_contrast_checkbox.stateChanged.connect(self.update_contrast_preview)
        self.color_scheme_group.buttonClicked.connect(self.update_contrast_preview)
        self.contrast_level_slider.valueChanged.connect(self.update_contrast_preview)
        self.brightness_slider.valueChanged.connect(self.update_contrast_preview)
        
        layout.addWidget(preview_group)
        layout.addStretch()
        return tab
    
    def create_text_tab(self):
        """Crea la pestaña de configuración de texto"""
        tab = QWidget()
        layout = QVBoxLayout(tab)
        layout.setSpacing(6)
        layout.setContentsMargins(8, 8, 8, 8)
        
        # Configuración de fuentes
        font_group = QGroupBox("Configuración de Fuentes")
        font_group.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Preferred)
        font_layout = QFormLayout(font_group)
        font_layout.setSpacing(4)
        font_layout.setContentsMargins(8, 8, 8, 8)
        
        self.large_text_checkbox = QCheckBox("Usar texto grande")
        font_layout.addRow(self.large_text_checkbox)
        
        self.font_size_multiplier_slider = QSlider(Qt.Orientation.Horizontal)
        self.font_size_multiplier_slider.setRange(80, 200)
        self.font_size_multiplier_slider.setValue(100)
        self.font_size_multiplier_label = QLabel("100%")
        self.font_size_multiplier_slider.valueChanged.connect(lambda v: self.font_size_multiplier_label.setText(f"{v}%"))
        
        font_size_layout = QHBoxLayout()
        font_size_layout.addWidget(self.font_size_multiplier_slider)
        font_size_layout.addWidget(self.font_size_multiplier_label)
        
        font_layout.addRow("Multiplicador de tamaño:", font_size_layout)
        
        self.font_family_combo = QComboBox()
        self.font_family_combo.addItems(["Arial", "Helvetica", "Verdana", "Tahoma", "Georgia", "Times New Roman", "Courier New"])
        font_layout.addRow("Familia de fuente:", self.font_family_combo)
        
        self.bold_text_checkbox = QCheckBox("Usar texto en negrita")
        font_layout.addRow(self.bold_text_checkbox)
        
        layout.addWidget(font_group)
        
        # Configuración de legibilidad
        readability_group = QGroupBox("Legibilidad")
        readability_group.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Preferred)
        readability_layout = QFormLayout(readability_group)
        readability_layout.setSpacing(4)
        readability_layout.setContentsMargins(8, 8, 8, 8)
        
        self.line_spacing_slider = QSlider(Qt.Orientation.Horizontal)
        self.line_spacing_slider.setRange(100, 200)
        self.line_spacing_slider.setValue(120)
        self.line_spacing_label = QLabel("120%")
        self.line_spacing_slider.valueChanged.connect(lambda v: self.line_spacing_label.setText(f"{v}%"))
        
        line_spacing_layout = QHBoxLayout()
        line_spacing_layout.addWidget(self.line_spacing_slider)
        line_spacing_layout.addWidget(self.line_spacing_label)
        
        readability_layout.addRow("Espaciado entre líneas:", line_spacing_layout)
        
        self.letter_spacing_slider = QSlider(Qt.Orientation.Horizontal)
        self.letter_spacing_slider.setRange(100, 150)
        self.letter_spacing_slider.setValue(100)
        self.letter_spacing_label = QLabel("100%")
        self.letter_spacing_slider.valueChanged.connect(lambda v: self.letter_spacing_label.setText(f"{v}%"))
        
        letter_spacing_layout = QHBoxLayout()
        letter_spacing_layout.addWidget(self.letter_spacing_slider)
        letter_spacing_layout.addWidget(self.letter_spacing_label)
        
        readability_layout.addRow("Espaciado entre letras:", letter_spacing_layout)
        
        self.underline_links_checkbox = QCheckBox("Subrayar enlaces")
        self.underline_links_checkbox.setChecked(True)
        readability_layout.addRow(self.underline_links_checkbox)
        
        layout.addWidget(readability_group)
        
        # Vista previa de texto
        text_preview_group = QGroupBox("Vista Previa de Texto")
        text_preview_group.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Preferred)
        text_preview_layout = QVBoxLayout(text_preview_group)
        text_preview_layout.setSpacing(4)
        text_preview_layout.setContentsMargins(8, 8, 8, 8)
        
        self.text_preview = QTextEdit()
        self.text_preview.setReadOnly(True)
        self.text_preview.setMaximumHeight(150)
        self.update_text_preview()
        text_preview_layout.addWidget(self.text_preview)
        
        # Conectar cambios para actualizar vista previa
        self.large_text_checkbox.stateChanged.connect(self.update_text_preview)
        self.font_size_multiplier_slider.valueChanged.connect(self.update_text_preview)
        self.font_family_combo.currentTextChanged.connect(self.update_text_preview)
        self.bold_text_checkbox.stateChanged.connect(self.update_text_preview)
        self.line_spacing_slider.valueChanged.connect(self.update_text_preview)
        self.letter_spacing_slider.valueChanged.connect(self.update_text_preview)
        
        layout.addWidget(text_preview_group)
        layout.addStretch()
        return tab
    
    def create_audio_tab(self):
        """Crea la pestaña de configuración de audio"""
        tab = QWidget()
        layout = QVBoxLayout(tab)
        layout.setSpacing(6)
        layout.setContentsMargins(8, 8, 8, 8)
        
        # Configuración de sonidos del sistema
        sounds_group = QGroupBox("Sonidos del Sistema")
        sounds_group.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Preferred)
        sounds_layout = QFormLayout(sounds_group)
        sounds_layout.setSpacing(4)
        sounds_layout.setContentsMargins(8, 8, 8, 8)
        
        self.enable_sounds_checkbox = QCheckBox("Habilitar sonidos del sistema")
        self.enable_sounds_checkbox.setChecked(True)
        sounds_layout.addRow(self.enable_sounds_checkbox)
        
        self.error_sounds_checkbox = QCheckBox("Sonido para errores")
        self.error_sounds_checkbox.setChecked(True)
        sounds_layout.addRow(self.error_sounds_checkbox)
        
        self.success_sounds_checkbox = QCheckBox("Sonido para operaciones exitosas")
        self.success_sounds_checkbox.setChecked(True)
        sounds_layout.addRow(self.success_sounds_checkbox)
        
        self.notification_sounds_checkbox = QCheckBox("Sonido para notificaciones")
        self.notification_sounds_checkbox.setChecked(True)
        sounds_layout.addRow(self.notification_sounds_checkbox)
        
        layout.addWidget(sounds_group)
        
        # Configuración de volumen
        volume_group = QGroupBox("Configuración de Volumen")
        volume_group.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Preferred)
        volume_layout = QFormLayout(volume_group)
        volume_layout.setSpacing(4)
        volume_layout.setContentsMargins(8, 8, 8, 8)
        
        self.master_volume_slider = QSlider(Qt.Orientation.Horizontal)
        self.master_volume_slider.setRange(0, 100)
        self.master_volume_slider.setValue(50)
        self.master_volume_label = QLabel("50%")
        self.master_volume_slider.valueChanged.connect(lambda v: self.master_volume_label.setText(f"{v}%"))
        
        master_volume_layout = QHBoxLayout()
        master_volume_layout.addWidget(self.master_volume_slider)
        master_volume_layout.addWidget(self.master_volume_label)
        
        volume_layout.addRow("Volumen maestro:", master_volume_layout)
        
        layout.addWidget(volume_group)
        
        # Alertas visuales
        visual_alerts_group = QGroupBox("Alertas Visuales")
        visual_alerts_group.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Preferred)
        visual_alerts_layout = QFormLayout(visual_alerts_group)
        visual_alerts_layout.setSpacing(4)
        visual_alerts_layout.setContentsMargins(8, 8, 8, 8)
        
        self.flash_screen_checkbox = QCheckBox("Parpadeo de pantalla para alertas")
        visual_alerts_layout.addRow(self.flash_screen_checkbox)
        
        self.show_notifications_checkbox = QCheckBox("Mostrar notificaciones en pantalla")
        self.show_notifications_checkbox.setChecked(True)
        visual_alerts_layout.addRow(self.show_notifications_checkbox)
        
        self.notification_duration_slider = QSlider(Qt.Orientation.Horizontal)
        self.notification_duration_slider.setRange(1, 10)
        self.notification_duration_slider.setValue(5)
        self.notification_duration_label = QLabel("5s")
        self.notification_duration_slider.valueChanged.connect(lambda v: self.notification_duration_label.setText(f"{v}s"))
        
        duration_layout = QHBoxLayout()
        duration_layout.addWidget(self.notification_duration_slider)
        duration_layout.addWidget(self.notification_duration_label)
        
        visual_alerts_layout.addRow("Duración de notificaciones:", duration_layout)
        
        layout.addWidget(visual_alerts_group)
        layout.addStretch()
        return tab
    
    def setup_action_buttons(self, main_layout):
        """Configura los botones de acción"""
        buttons_layout = QHBoxLayout()
        buttons_layout.setSpacing(8)
        buttons_layout.setContentsMargins(0, 8, 0, 0)
        
        self.test_button = QPushButton("🧪 Probar Configuración")
        self.test_button.setObjectName("accessibility_test_button")
        self.test_button.clicked.connect(self.test_accessibility_settings)
        
        self.apply_button = QPushButton("✅ Aplicar Cambios")
        self.apply_button.setObjectName("accessibility_apply_button")
        self.apply_button.clicked.connect(self.apply_accessibility_settings)
        
        self.save_button = QPushButton("💾 Guardar Configuración")
        self.save_button.setObjectName("accessibility_save_button")
        self.save_button.clicked.connect(self.save_accessibility_settings)
        
        self.reset_button = QPushButton("🔄 Restablecer")
        self.reset_button.setObjectName("accessibility_reset_button")
        self.reset_button.clicked.connect(self.reset_to_defaults)
        
        buttons_layout.addStretch()
        buttons_layout.addWidget(self.test_button)
        buttons_layout.addWidget(self.apply_button)
        buttons_layout.addWidget(self.save_button)
        buttons_layout.addWidget(self.reset_button)
        
        main_layout.addLayout(buttons_layout)
    
    def setup_keyboard_shortcuts(self):
        """Configura los atajos de teclado del widget"""
        # Atajo para aplicar cambios rápidamente
        apply_shortcut = QShortcut(QKeySequence("Ctrl+Return"), self)
        apply_shortcut.activated.connect(self.apply_accessibility_settings)
        
        # Atajo para guardar
        save_shortcut = QShortcut(QKeySequence("Ctrl+S"), self)
        save_shortcut.activated.connect(self.save_accessibility_settings)
        
        # Atajo para restablecer
        reset_shortcut = QShortcut(QKeySequence("Ctrl+R"), self)
        reset_shortcut.activated.connect(self.reset_to_defaults)
    
    def update_contrast_preview(self):
        """Actualiza la vista previa de contraste usando CSS dinámico"""
        scheme_id = "normal"
        for button in self.color_scheme_group.buttons():
            if button.isChecked():
                scheme_id = button.property("scheme_id")
                break
        
        contrast_level = self.contrast_level_slider.value()
        brightness = self.brightness_slider.value()
        high_contrast = self.high_contrast_checkbox.isChecked()
        
        # Usar CSS dinámico en lugar de estilos inline hardcodeados
        self.contrast_preview.setObjectName("contrast_preview")
        self.contrast_preview.setProperty("cssClass", "accessibility-contrast-preview")
        self.contrast_preview.setProperty("colorScheme", scheme_id)
        self.contrast_preview.setProperty("contrastLevel", str(contrast_level))
        self.contrast_preview.setProperty("brightnessLevel", str(brightness))
        
        # HTML sin estilos inline - los estilos se aplicarán via CSS dinámico
        preview_html = f"""
        <div class="contrast-preview-content">
            <h2>Vista Previa de Contraste</h2>
            <p>Este es un ejemplo de cómo se verá el texto con la configuración actual.</p>
            <p><strong>Texto en negrita</strong> y <em>texto en cursiva</em> para probar la legibilidad.</p>
            <p>Esquema actual: {scheme_id.replace('_', ' ').title()}</p>
            <p>Contraste: {contrast_level}% | Brillo: {brightness}%</p>
        </div>
        """
        
        self.contrast_preview.setHtml(preview_html)
    
    def update_text_preview(self):
        """Actualiza la vista previa de texto usando CSS dinámico"""
        font_family = self.font_family_combo.currentText()
        font_size_multiplier = self.font_size_multiplier_slider.value()
        line_spacing = self.line_spacing_slider.value()
        letter_spacing = self.letter_spacing_slider.value()
        bold = self.bold_text_checkbox.isChecked()
        large_text = self.large_text_checkbox.isChecked()
        
        base_size = 14 if large_text else 12
        final_size = int(base_size * font_size_multiplier / 100)
        font_weight = "bold" if bold else "normal"
        
        # Usar CSS dinámico en lugar de estilos inline hardcodeados
        self.text_preview.setObjectName("text_preview")
        self.text_preview.setProperty("cssClass", "accessibility-text-preview")
        self.text_preview.setProperty("fontFamily", font_family)
        self.text_preview.setProperty("fontSize", str(final_size))
        self.text_preview.setProperty("lineHeight", str(line_spacing))
        self.text_preview.setProperty("letterSpacing", str(letter_spacing-100))
        self.text_preview.setProperty("fontWeight", font_weight)
        
        # HTML sin estilos inline - los estilos se aplicarán via CSS dinámico
        preview_html = f"""
        <div class="text-preview-content">
            <h2>Vista Previa de Texto</h2>
            <p>Este es un ejemplo de cómo se verá el texto con la configuración actual de fuentes.</p>
            <p>Familia: {font_family} | Tamaño: {final_size}px | Espaciado: {line_spacing}%</p>
            <p>Lorem ipsum dolor sit amet, consectetur adipiscing elit. Sed do eiusmod tempor incididunt ut labore et dolore magna aliqua.</p>
        </div>
        """
        
        self.text_preview.setHtml(preview_html)
    
    def load_accessibility_settings(self):
        """Carga la configuración de accesibilidad desde la base de datos"""
        try:
            settings_json = self.db_manager.obtener_configuracion('accessibility_settings')
            if settings_json:
                return json.loads(settings_json)
        except Exception as e:
            logging.warning(f"Error cargando configuración de accesibilidad: {e}")
        
        # Configuración por defecto
        return {
            'keyboard_navigation_enabled': True,
            'show_focus_indicator': True,
            'tab_cycling': True,
            'key_repeat_delay': 500,
            'focus_timeout': 3000,
            'high_contrast': False,
            'color_scheme': 'normal',
            'contrast_level': 100,
            'brightness': 100,
            'large_text': False,
            'font_size_multiplier': 100,
            'font_family': 'Arial',
            'bold_text': False,
            'line_spacing': 120,
            'letter_spacing': 100,
            'underline_links': True,
            'enable_sounds': True,
            'error_sounds': True,
            'success_sounds': True,
            'notification_sounds': True,
            'master_volume': 50,
            'flash_screen': False,
            'show_notifications': True,
            'notification_duration': 5
        }
    
    def load_current_settings(self):
        """Carga la configuración actual en la interfaz"""
        # Navegación por teclado
        self.enable_keyboard_nav_checkbox.setChecked(self.current_settings.get('keyboard_navigation_enabled', True))
        self.show_focus_indicator_checkbox.setChecked(self.current_settings.get('show_focus_indicator', True))
        self.tab_cycling_checkbox.setChecked(self.current_settings.get('tab_cycling', True))
        self.key_repeat_delay_slider.setValue(self.current_settings.get('key_repeat_delay', 500))
        self.focus_timeout_slider.setValue(self.current_settings.get('focus_timeout', 3000))
        
        # Contraste
        self.high_contrast_checkbox.setChecked(self.current_settings.get('high_contrast', False))
        color_scheme = self.current_settings.get('color_scheme', 'normal')
        for button in self.color_scheme_group.buttons():
            if button.property("scheme_id") == color_scheme:
                button.setChecked(True)
                break
        self.contrast_level_slider.setValue(self.current_settings.get('contrast_level', 100))
        self.brightness_slider.setValue(self.current_settings.get('brightness', 100))
        
        # Texto
        self.large_text_checkbox.setChecked(self.current_settings.get('large_text', False))
        self.font_size_multiplier_slider.setValue(self.current_settings.get('font_size_multiplier', 100))
        font_family = self.current_settings.get('font_family', 'Arial')
        if font_family in [self.font_family_combo.itemText(i) for i in range(self.font_family_combo.count())]:
            self.font_family_combo.setCurrentText(font_family)
        self.bold_text_checkbox.setChecked(self.current_settings.get('bold_text', False))
        self.line_spacing_slider.setValue(self.current_settings.get('line_spacing', 120))
        self.letter_spacing_slider.setValue(self.current_settings.get('letter_spacing', 100))
        self.underline_links_checkbox.setChecked(self.current_settings.get('underline_links', True))
        
        # Audio
        self.enable_sounds_checkbox.setChecked(self.current_settings.get('enable_sounds', True))
        self.error_sounds_checkbox.setChecked(self.current_settings.get('error_sounds', True))
        self.success_sounds_checkbox.setChecked(self.current_settings.get('success_sounds', True))
        self.notification_sounds_checkbox.setChecked(self.current_settings.get('notification_sounds', True))
        self.master_volume_slider.setValue(self.current_settings.get('master_volume', 50))
        self.flash_screen_checkbox.setChecked(self.current_settings.get('flash_screen', False))
        self.show_notifications_checkbox.setChecked(self.current_settings.get('show_notifications', True))
        self.notification_duration_slider.setValue(self.current_settings.get('notification_duration', 5))
    
    def collect_current_settings(self):
        """Recopila la configuración actual de la interfaz"""
        # Obtener esquema de color seleccionado
        color_scheme = "normal"
        for button in self.color_scheme_group.buttons():
            if button.isChecked():
                color_scheme = button.property("scheme_id")
                break
        
        self.current_settings.update({
            'keyboard_navigation_enabled': self.enable_keyboard_nav_checkbox.isChecked(),
            'show_focus_indicator': self.show_focus_indicator_checkbox.isChecked(),
            'tab_cycling': self.tab_cycling_checkbox.isChecked(),
            'key_repeat_delay': self.key_repeat_delay_slider.value(),
            'focus_timeout': self.focus_timeout_slider.value(),
            'high_contrast': self.high_contrast_checkbox.isChecked(),
            'color_scheme': color_scheme,
            'contrast_level': self.contrast_level_slider.value(),
            'brightness': self.brightness_slider.value(),
            'large_text': self.large_text_checkbox.isChecked(),
            'font_size_multiplier': self.font_size_multiplier_slider.value(),
            'font_family': self.font_family_combo.currentText(),
            'bold_text': self.bold_text_checkbox.isChecked(),
            'line_spacing': self.line_spacing_slider.value(),
            'letter_spacing': self.letter_spacing_slider.value(),
            'underline_links': self.underline_links_checkbox.isChecked(),
            'enable_sounds': self.enable_sounds_checkbox.isChecked(),
            'error_sounds': self.error_sounds_checkbox.isChecked(),
            'success_sounds': self.success_sounds_checkbox.isChecked(),
            'notification_sounds': self.notification_sounds_checkbox.isChecked(),
            'master_volume': self.master_volume_slider.value(),
            'flash_screen': self.flash_screen_checkbox.isChecked(),
            'show_notifications': self.show_notifications_checkbox.isChecked(),
            'notification_duration': self.notification_duration_slider.value()
        })
    
    def test_accessibility_settings(self):
        """Prueba la configuración de accesibilidad actual"""
        self.collect_current_settings()
        
        # Aplicar configuraciones temporalmente para la prueba
        self.apply_accessibility_changes_temporarily()
        
        # Mostrar un diálogo de prueba con las configuraciones aplicadas
        test_dialog = QMessageBox(self)
        test_dialog.setWindowTitle("Prueba de Accesibilidad")
        test_dialog.setIcon(QMessageBox.Icon.Information)
        
        test_text = """
        Esta es una prueba de la configuración de accesibilidad.
        
        Configuración actual:
        • Navegación por teclado: {}
        • Alto contraste: {}
        • Esquema de color: {}
        • Texto grande: {}
        • Sonidos habilitados: {}
        
        ¿La configuración se ve y funciona correctamente?
        """.format(
            "Activada" if self.current_settings['keyboard_navigation_enabled'] else "Desactivada",
            "Activado" if self.current_settings['high_contrast'] else "Desactivado",
            self.current_settings['color_scheme'].replace('_', ' ').title(),
            "Activado" if self.current_settings['large_text'] else "Desactivado",
            "Activados" if self.current_settings['enable_sounds'] else "Desactivados"
        )
        
        test_dialog.setText(test_text)
        test_dialog.setStandardButtons(QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No)
        test_dialog.setDefaultButton(QMessageBox.StandardButton.Yes)
        
        # Aplicar configuración de prueba al diálogo
        if self.current_settings['large_text']:
            font = test_dialog.font()
            font.setPointSize(int(font.pointSize() * self.current_settings['font_size_multiplier'] / 100))
            test_dialog.setFont(font)
        
        result = test_dialog.exec()
        if result == QMessageBox.StandardButton.Yes:
            QMessageBox.information(self, "Prueba Exitosa", "¡Excelente! La configuración funciona correctamente.")
        else:
            QMessageBox.information(self, "Ajustar Configuración", "Puede ajustar la configuración según sus necesidades.")
    
    def apply_accessibility_changes_temporarily(self):
        """Aplica los cambios de accesibilidad temporalmente para pruebas"""
        try:
            # Aplicar configuraciones de navegación por teclado
            keyboard_settings = {
                'enabled': self.current_settings['keyboard_navigation_enabled'],
                'tab_navigation': self.current_settings['tab_cycling'],
                'focus_indicator': self.current_settings['show_focus_indicator']
            }
            
            if keyboard_settings['enabled']:
                # Configurar navegación por teclado en la aplicación principal
                app = QApplication.instance()
                if app:
                    app.setProperty('keyboardNavigationEnabled', True)
                    app.setProperty('tabNavigationEnabled', keyboard_settings['tab_navigation'])
                    app.setProperty('focusIndicatorEnabled', keyboard_settings['focus_indicator'])
            
            # Aplicar configuraciones de contraste
            scheme = self.current_settings['color_scheme']
            
            if scheme == 'high_contrast_black':
                self.apply_high_contrast_theme()
            elif scheme == 'high_contrast_white':
                self.apply_high_contrast_white_theme()
            elif scheme == 'blue_yellow':
                self.apply_blue_yellow_theme()
            elif scheme == 'grayscale':
                self.apply_grayscale_theme()
            else:
                self.apply_normal_theme()
            
            # Aplicar configuraciones de texto
            font_size = 12 if not self.current_settings['large_text'] else 16
            font_size = int(font_size * self.current_settings['font_size_multiplier'] / 100)
            font_family = self.current_settings['font_family']
            
            self.apply_font_settings(font_family, font_size)
            
        except Exception as e:
            logging.error(f"Error aplicando configuraciones temporalmente: {e}")
    
    def apply_high_contrast_theme(self):
        """Aplica tema de alto contraste negro usando CSS dinámico"""
        app = QApplication.instance()
        if app:
            # Usar sistema CSS dinámico en lugar de estilos hardcodeados
            app.setProperty('accessibility_theme', 'high_contrast_black')
            # Los estilos se aplicarán automáticamente a través del sistema CSS dinámico
    
    def apply_high_contrast_white_theme(self):
        """Aplica tema de alto contraste blanco usando CSS dinámico"""
        app = QApplication.instance()
        if app:
            # Usar sistema CSS dinámico en lugar de estilos hardcodeados
            app.setProperty('accessibility_theme', 'high_contrast_white')
            # Los estilos se aplicarán automáticamente a través del sistema CSS dinámico
    
    def apply_blue_yellow_theme(self):
        """Aplica tema azul-amarillo para daltonismo usando CSS dinámico"""
        app = QApplication.instance()
        if app:
            # Usar sistema CSS dinámico en lugar de estilos hardcodeados
            app.setProperty('accessibility_theme', 'blue_yellow')
            # Los estilos se aplicarán automáticamente a través del sistema CSS dinámico
    
    def apply_grayscale_theme(self):
        """Aplica tema en escala de grises usando CSS dinámico"""
        app = QApplication.instance()
        if app:
            # Usar sistema CSS dinámico en lugar de estilos hardcodeados
            app.setProperty('accessibility_theme', 'grayscale')
            # Los estilos se aplicarán automáticamente a través del sistema CSS dinámico
    
    def apply_normal_theme(self):
        """Aplica tema normal usando CSS dinámico"""
        app = QApplication.instance()
        if app:
            # Usar sistema CSS dinámico para restaurar tema normal
            app.setProperty('accessibility_theme', 'normal')
            # Los estilos se aplicarán automáticamente a través del sistema CSS dinámico
    
    def apply_font_settings(self, font_family, font_size):
        """Aplica configuraciones de fuente"""
        app = QApplication.instance()
        if app:
            font = QFont(font_family, font_size)
            if self.current_settings['bold_text']:
                font.setBold(True)
            app.setFont(font)
    
    def apply_accessibility_settings(self):
        """Aplica la configuración de accesibilidad temporalmente"""
        self.collect_current_settings()
        
        # Aplicar cambios inmediatamente
        self.apply_accessibility_changes_temporarily()
        
        # Emitir señal de cambio
        self.accessibility_changed.emit(self.current_settings)
        
        QMessageBox.information(
            self, "Configuración Aplicada", 
            "La configuración de accesibilidad ha sido aplicada temporalmente.\n\n"
            "Use 'Guardar Configuración' para hacer los cambios permanentes."
        )
    
    def get_main_window(self):
        """Obtiene la ventana principal para aplicar estilos"""
        parent = self.parent()
        while parent:
            if hasattr(parent, 'setObjectName') and parent.objectName() == "MainWindow":
                return parent
            parent = parent.parent()
        return QApplication.instance().activeWindow()
    
    def save_accessibility_settings(self):
        """Guarda la configuración de accesibilidad"""
        try:
            self.collect_current_settings()
            
            # Aplicar cambios antes de guardar
            self.apply_accessibility_changes_temporarily()
            
            # Guardar en la base de datos
            settings_json = json.dumps(self.current_settings, indent=2)
            self.db_manager.actualizar_configuracion('accessibility_settings', settings_json)
            
            # También guardar en archivo local como respaldo
            self.save_settings_to_file(self.current_settings)
            
            # Emitir señal de cambio
            self.accessibility_changed.emit(self.current_settings)
            
            QMessageBox.information(
                self, "Configuración Guardada", 
                "La configuración de accesibilidad ha sido guardada exitosamente.\n\n"
                "Los cambios se aplicarán inmediatamente y se mantendrán al reiniciar la aplicación."
            )
            
        except Exception as e:
            QMessageBox.critical(
                self, "Error", 
                f"No se pudo guardar la configuración de accesibilidad:\n{str(e)}"
            )
    
    def save_settings_to_file(self, settings):
        """Guarda las configuraciones en un archivo local"""
        import os
        
        try:
            # Crear directorio de configuración si no existe
            config_dir = os.path.join(os.path.expanduser('~'), '.gym_management')
            os.makedirs(config_dir, exist_ok=True)
            
            # Guardar configuraciones en archivo JSON
            config_file = os.path.join(config_dir, 'accessibility_settings.json')
            with open(config_file, 'w', encoding='utf-8') as f:
                json.dump(settings, f, indent=2, ensure_ascii=False)
                
        except Exception as e:
            logging.error(f"Error al guardar configuraciones en archivo: {e}")
    
    def reset_to_defaults(self):
        """Restablece la configuración a los valores por defecto"""
        reply = QMessageBox.question(
            self, "Restablecer Configuración",
            "¿Está seguro de que desea restablecer toda la configuración de accesibilidad a los valores por defecto?\n\n"
            "Esta acción no se puede deshacer.",
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
            QMessageBox.StandardButton.No
        )
        
        if reply == QMessageBox.StandardButton.Yes:
            # Restablecer tema normal primero
            self.apply_normal_theme()
            
            # Restablecer a configuración por defecto
            self.current_settings = {
                'keyboard_navigation_enabled': True,
                'show_focus_indicator': True,
                'tab_cycling': True,
                'key_repeat_delay': 500,
                'focus_timeout': 3000,
                'high_contrast': False,
                'color_scheme': 'normal',
                'contrast_level': 100,
                'brightness': 100,
                'large_text': False,
                'font_size_multiplier': 100,
                'font_family': 'Arial',
                'bold_text': False,
                'line_spacing': 120,
                'letter_spacing': 100,
                'underline_links': True,
                'enable_sounds': True,
                'error_sounds': True,
                'success_sounds': True,
                'notification_sounds': True,
                'master_volume': 50,
                'flash_screen': False,
                'show_notifications': True,
                'notification_duration': 5
            }
            
            # Cargar configuración en la interfaz
            self.load_current_settings()
            self.update_contrast_preview()
            self.update_text_preview()
            
            # Eliminar configuración guardada
            self.clear_saved_settings()
            
            # Aplicar configuración por defecto
            self.apply_font_settings('Arial', 12)
            
            QMessageBox.information(self, "Restablecido", "La configuración ha sido restablecida a los valores por defecto.")
    
    def clear_saved_settings(self):
        """Elimina las configuraciones guardadas"""
        import os
        
        try:
            # Eliminar de la base de datos
            self.db_manager.actualizar_configuracion('accessibility_settings', '')
            
            # Eliminar archivo local
            config_file = os.path.join(os.path.expanduser('~'), '.gym_management', 'accessibility_settings.json')
            if os.path.exists(config_file):
                os.remove(config_file)
                
        except Exception as e:
            logging.error(f"Error eliminando configuraciones guardadas: {e}")
    
    def load_settings(self, settings):
        """Carga configuraciones desde un diccionario externo."""
        try:
            # Actualizar configuraciones actuales
            self.current_settings.update(settings)
            
            # Aplicar a la interfaz
            self.load_current_settings()
            
            # Actualizar vistas previas
            self.update_contrast_preview()
            self.update_text_preview()
            
            logging.info(f"Configuraciones de accesibilidad cargadas: {len(settings)} elementos")
            
        except Exception as e:
            logging.error(f"Error cargando configuraciones en AccessibilityWidget: {e}")
    
    def get_current_settings(self):
        """Obtiene las configuraciones actuales como diccionario."""
        self.collect_current_settings()
        return self.current_settings.copy()
    
    def apply_settings_immediately(self):
        """Aplica las configuraciones actuales inmediatamente sin mostrar diálogos."""
        try:
            self.collect_current_settings()
            self.accessibility_changed.emit(self.current_settings)
            logging.info("Configuraciones de accesibilidad aplicadas inmediatamente")
        except Exception as e:
            logging.error(f"Error aplicando configuraciones inmediatamente: {e}")

