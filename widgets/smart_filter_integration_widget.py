from PyQt6.QtWidgets import (
    QWidget, QVBoxLayout, QHBoxLayout, QPushButton, QFrame,
    QLabel, QButtonGroup, QRadioButton, QStackedWidget,
    QSplitter, QGroupBox, QTabWidget, QDialog
)
from PyQt6.QtCore import Qt, pyqtSignal
from PyQt6.QtGui import QFont, QIcon
from typing import Dict, List, Any
from .advanced_filter_widget import AdvancedFilterWidget, FilterField
from .smart_filter_widget import SmartFilterWidget
from .filter_button_widget import FilterButton

class FilterModeSelector(QWidget):
    """Selector para alternar entre modos de filtro"""
    
    mode_changed = pyqtSignal(str)  # 'traditional' o 'smart'
    
    def __init__(self, parent=None):
        super().__init__(parent)
        self.current_mode = 'traditional'
        self.setup_ui()
    
    def setup_ui(self):
        layout = QHBoxLayout(self)
        layout.setContentsMargins(8, 4, 8, 4)
        layout.setSpacing(12)
        
        # Título
        title = QLabel("🔧 Modo de Filtros:")
        title.setObjectName("filter_mode_title")
        layout.addWidget(title)
        
        # Grupo de botones de radio
        self.button_group = QButtonGroup(self)
        
        # Modo tradicional
        self.traditional_radio = QRadioButton("🔍 Tradicional")
        self.traditional_radio.setChecked(True)
        self.traditional_radio.setObjectName("filter_mode_radio")
        
        # Modo inteligente
        self.smart_radio = QRadioButton("🧠 Inteligente")
        self.smart_radio.setObjectName("filter_mode_radio")
        
        self.button_group.addButton(self.traditional_radio, 0)
        self.button_group.addButton(self.smart_radio, 1)
        
        layout.addWidget(self.traditional_radio)
        layout.addWidget(self.smart_radio)
        
        # Separador
        separator = QFrame()
        separator.setFrameShape(QFrame.Shape.VLine)
        separator.setObjectName("filter_mode_separator")
        layout.addWidget(separator)
        
        # Información del modo actual
        self.mode_info = QLabel("Filtros básicos y avanzados")
        self.mode_info.setObjectName("filter_mode_info")
        layout.addWidget(self.mode_info)
        
        layout.addStretch()
        
        # Conectar señales
        self.button_group.buttonClicked.connect(self.on_mode_changed)
    
    def on_mode_changed(self, button):
        """Maneja el cambio de modo"""
        if button == self.traditional_radio:
            self.current_mode = 'traditional'
            self.mode_info.setText("Filtros básicos y avanzados")
        else:
            self.current_mode = 'smart'
            self.mode_info.setText("Filtros inteligentes con IA y sugerencias")
        
        self.mode_changed.emit(self.current_mode)
    
    def get_current_mode(self) -> str:
        """Obtiene el modo actual"""
        return self.current_mode
    
    def set_mode(self, mode: str):
        """Establece el modo programáticamente"""
        if mode == 'traditional':
            self.traditional_radio.setChecked(True)
        else:
            self.smart_radio.setChecked(True)
        self.on_mode_changed(self.button_group.checkedButton())

class SmartFilterIntegrationWidget(QWidget):
    """Widget integrador que combina filtros tradicionales e inteligentes"""
    
    filters_changed = pyqtSignal(dict)
    filters_cleared = pyqtSignal()
    
    def __init__(self, filter_fields: List[FilterField], parent=None):
        super().__init__(parent)
        self.filter_fields = filter_fields
        self.current_filters = {}
        self.setup_ui()
        self.setup_connections()
    
    def setup_ui(self):
        """Configura la interfaz principal"""
        main_layout = QVBoxLayout(self)
        main_layout.setContentsMargins(0, 0, 0, 0)
        main_layout.setSpacing(8)
        
        # Selector de modo
        self.mode_selector = FilterModeSelector()
        main_layout.addWidget(self.mode_selector)
        
        # Widget apilado para alternar entre modos
        self.stacked_widget = QStackedWidget()
        
        # Página de filtros tradicionales
        traditional_page = self.create_traditional_page()
        self.stacked_widget.addWidget(traditional_page)
        
        # Página de filtros inteligentes
        smart_page = self.create_smart_page()
        self.stacked_widget.addWidget(smart_page)
        
        main_layout.addWidget(self.stacked_widget)
        
        # Panel de estado unificado
        self.status_panel = self.create_status_panel()
        main_layout.addWidget(self.status_panel)
    
    def create_traditional_page(self) -> QWidget:
        """Crea la página de filtros tradicionales"""
        page = QWidget()
        layout = QVBoxLayout(page)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(8)
        
        # Widget de filtros avanzados tradicional
        self.traditional_filter = AdvancedFilterWidget(self.filter_fields)
        layout.addWidget(self.traditional_filter)
        
        # Información sobre el modo tradicional
        info_frame = QFrame()
        info_frame.setObjectName("traditional_info_frame")
        
        info_layout = QHBoxLayout(info_frame)
        info_icon = QLabel("ℹ️")
        info_text = QLabel("Modo Tradicional: Filtros básicos con opciones avanzadas estándar")
        info_text.setObjectName("traditional_mode_info_text")
        
        info_layout.addWidget(info_icon)
        info_layout.addWidget(info_text)
        info_layout.addStretch()
        
        layout.addWidget(info_frame)
        
        return page
    
    def create_smart_page(self) -> QWidget:
        """Crea la página de filtros inteligentes"""
        page = QWidget()
        layout = QVBoxLayout(page)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(8)
        
        # Widget de filtros inteligentes
        self.smart_filter = SmartFilterWidget(self.filter_fields)
        layout.addWidget(self.smart_filter)
        
        # Información sobre el modo inteligente
        info_frame = QFrame()
        info_frame.setObjectName("smart_info_frame")
        
        info_layout = QVBoxLayout(info_frame)
        
        info_header = QHBoxLayout()
        info_icon = QLabel("🧠")
        info_title = QLabel("Modo Inteligente Activado")
        info_title.setObjectName("smart_mode_info_title")
        
        info_header.addWidget(info_icon)
        info_header.addWidget(info_title)
        info_header.addStretch()
        
        features_text = QLabel(
            "• Filtros rápidos predefinidos\n"
            "• Búsqueda inteligente con lenguaje natural\n"
            "• Sugerencias basadas en patrones de uso\n"
            "• Filtros guardados y favoritos\n"
            "• Análisis de comportamiento de filtrado"
        )
        features_text.setObjectName("smart_mode_features_text")
        
        info_layout.addLayout(info_header)
        info_layout.addWidget(features_text)
        
        layout.addWidget(info_frame)
        
        return page
    
    def create_status_panel(self) -> QWidget:
        """Crea el panel de estado unificado"""
        panel = QFrame()
        panel.setObjectName("filter_status_panel")
        
        layout = QHBoxLayout(panel)
        
        # Estado actual
        self.status_icon = QLabel("✅")
        self.status_text = QLabel("Listo para filtrar")
        self.status_text.setObjectName("filter_status_text")
        
        # Contador de filtros
        self.filter_count = QLabel("0 filtros activos")
        self.filter_count.setObjectName("filter_count_display")
        
        # Modo actual
        self.current_mode_label = QLabel("Modo: Tradicional")
        self.current_mode_label.setObjectName("current_mode_display")
        
        # Botones de acción
        self.clear_all_btn = QPushButton("🗑️ Limpiar")
        self.clear_all_btn.setObjectName("filter_clear_button")
        
        
        layout.addWidget(self.status_icon)
        layout.addWidget(self.status_text)
        layout.addWidget(QLabel("|"))
        layout.addWidget(self.filter_count)
        layout.addWidget(QLabel("|"))
        layout.addWidget(self.current_mode_label)
        layout.addStretch()
        layout.addWidget(self.clear_all_btn)
        
        return panel
    
    def setup_connections(self):
        """Configura las conexiones de señales"""
        # Selector de modo
        self.mode_selector.mode_changed.connect(self.on_mode_changed)
        
        # Filtros tradicionales
        self.traditional_filter.filters_changed.connect(self.on_traditional_filters_changed)
        self.traditional_filter.filters_cleared.connect(self.on_filters_cleared)
        
        # Filtros inteligentes
        self.smart_filter.filters_changed.connect(self.on_smart_filters_changed)
        self.smart_filter.filters_cleared.connect(self.on_filters_cleared)
        
        # Botones de acción
        self.clear_all_btn.clicked.connect(self.clear_all_filters)
    
    def on_mode_changed(self, mode: str):
        """Maneja el cambio de modo"""
        if mode == 'traditional':
            self.stacked_widget.setCurrentIndex(0)
            self.current_mode_label.setText("Modo: Tradicional")
        else:
            self.stacked_widget.setCurrentIndex(1)
            self.current_mode_label.setText("Modo: Inteligente")
        
        # Sincronización anterior eliminada; el modo cambia sin replicar filtros
    
    def on_traditional_filters_changed(self, filters: Dict[str, Any]):
        """Maneja cambios en filtros tradicionales"""
        self.current_filters = filters
        self.update_status()
        self.filters_changed.emit(filters)
    
    def on_smart_filters_changed(self, filters: Dict[str, Any]):
        """Maneja cambios en filtros inteligentes"""
        self.current_filters = filters
        self.update_status()
        self.filters_changed.emit(filters)
    
    def on_filters_cleared(self):
        """Maneja la limpieza de filtros"""
        self.current_filters = {}
        self.update_status()
        self.filters_cleared.emit()
    
    def update_status(self):
        """Actualiza el panel de estado"""
        count = len(self.current_filters)
        self.filter_count.setText(f"{count} filtro{'s' if count != 1 else ''} activo{'s' if count != 1 else ''}")
        
        if count > 0:
            self.status_icon.setText("🔍")
            self.status_text.setText("Filtros aplicados")
            self.status_text.setProperty("filterStatus", "active")
        else:
            self.status_icon.setText("✅")
            self.status_text.setText("Listo para filtrar")
            self.status_text.setProperty("filterStatus", "ready")
        
        # Refrescar estilos
        self.status_text.style().unpolish(self.status_text)
        self.status_text.style().polish(self.status_text)
    
    def clear_all_filters(self):
        """Limpia todos los filtros en ambos modos"""
        self.traditional_filter.clear_filters()
        self.smart_filter.clear_all_filters()
        self.current_filters = {}
        self.update_status()
        self.filters_cleared.emit()
    
    # Eliminada: sync_filters (anterior)
    
    def get_current_filters(self) -> Dict[str, Any]:
        """Obtiene los filtros actuales"""
        return self.current_filters.copy()
    
    def set_filters(self, filters: Dict[str, Any]):
        """Establece filtros específicos"""
        self.current_filters = filters.copy()
        
        # Aplicar a ambos modos
        for field_name, value in filters.items():
            self.traditional_filter.set_filter_value(field_name, value)
        
        self.smart_filter.set_filters(filters)
        self.update_status()
        self.filters_changed.emit(self.current_filters)
    
    def get_current_mode(self) -> str:
        """Obtiene el modo actual"""
        return self.mode_selector.get_current_mode()
    
    def set_mode(self, mode: str):
        """Establece el modo programáticamente"""
        self.mode_selector.set_mode(mode)

class SmartFilterButton(QPushButton):
    """Botón mejorado que abre el diálogo de filtros inteligentes"""
    
    filters_changed = pyqtSignal(dict)
    filters_cleared = pyqtSignal()
    
    def __init__(self, filter_fields: List[FilterField], parent=None):
        super().__init__(parent)
        self.filter_fields = filter_fields
        self.active_filters = {}
        self.setup_ui()
        self.setup_connections()
    
    def setup_ui(self):
        """Configura la apariencia del botón"""
        self.setText("🧠 Filtros Inteligentes")
        self.setToolTip("Abrir sistema de filtros inteligentes")
        self.setObjectName("smart_filter_button")
    
    def setup_connections(self):
        """Configura las conexiones"""
        self.clicked.connect(self.open_filter_dialog)
    
    def open_filter_dialog(self):
        """Abre el diálogo de filtros inteligentes"""
        dialog = SmartFilterDialog(self.filter_fields, self)
        
        # Establecer filtros actuales
        if self.active_filters:
            dialog.set_filters(self.active_filters)
        
        if dialog.exec() == dialog.DialogCode.Accepted:
            filters = dialog.get_filters()
            self.active_filters = filters
            self.update_button_appearance()
            
            if filters:
                self.filters_changed.emit(filters)
            else:
                self.filters_cleared.emit()
    
    def update_button_appearance(self):
        """Actualiza la apariencia del botón según si hay filtros activos"""
        has_filters = len(self.active_filters) > 0
        self.setProperty("hasFilters", has_filters)
        self.style().unpolish(self)
        self.style().polish(self)
        
        if has_filters:
            count = len(self.active_filters)
            self.setText(f"🧠 Filtros ({count})")
        else:
            self.setText("🧠 Filtros Inteligentes")
    
    def clear_filters(self):
        """Limpia todos los filtros activos"""
        self.active_filters = {}
        self.update_button_appearance()
        self.filters_cleared.emit()
    
    def get_filters(self) -> Dict[str, Any]:
        """Obtiene los filtros activos"""
        return self.active_filters.copy()
    
    def set_filters(self, filters: Dict[str, Any]):
        """Establece filtros específicos"""
        self.active_filters = filters.copy()
        self.update_button_appearance()

class SmartFilterDialog(QDialog):
    """Diálogo que contiene el sistema de filtros inteligentes"""
    
    def __init__(self, filter_fields: List[FilterField], parent=None):
        super().__init__(parent)
        self.filter_fields = filter_fields
        self.setup_ui()
        self.setModal(True)
        self.resize(800, 600)
    
    def setup_ui(self):
        """Configura la interfaz del diálogo"""
        self.setWindowTitle("🧠 Sistema de Filtros Inteligentes")
        
        layout = QVBoxLayout(self)
        layout.setContentsMargins(16, 16, 16, 16)
        layout.setSpacing(12)
        
        # Widget principal de filtros
        self.filter_widget = SmartFilterIntegrationWidget(self.filter_fields)
        layout.addWidget(self.filter_widget)
        
        # Botones del diálogo
        from PyQt6.QtWidgets import QDialogButtonBox
        
        buttons = QDialogButtonBox(
            QDialogButtonBox.StandardButton.Ok | 
            QDialogButtonBox.StandardButton.Cancel |
            QDialogButtonBox.StandardButton.Reset
        )
        
        buttons.accepted.connect(self.accept)
        buttons.rejected.connect(self.reject)
        buttons.button(QDialogButtonBox.StandardButton.Reset).clicked.connect(
            self.filter_widget.clear_all_filters
        )
        
        layout.addWidget(buttons)
    
    def get_filters(self) -> Dict[str, Any]:
        """Obtiene los filtros del widget"""
        return self.filter_widget.get_current_filters()
    
    def set_filters(self, filters: Dict[str, Any]):
        """Establece filtros en el widget"""
        self.filter_widget.set_filters(filters)

