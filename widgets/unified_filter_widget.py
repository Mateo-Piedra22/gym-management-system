from PyQt6.QtWidgets import (
    QPushButton, QDialog, QVBoxLayout, QHBoxLayout, QDialogButtonBox,
    QLabel, QFrame, QTabWidget, QWidget, QButtonGroup, QRadioButton
)
from PyQt6.QtCore import Qt, pyqtSignal
from PyQt6.QtGui import QFont, QIcon
from .advanced_filter_widget import AdvancedFilterWidget, FilterField
from typing import Dict, List, Any

class UnifiedFilterButton(QPushButton):
    """Botón de filtros unificado que combina filtros avanzados e inteligentes"""
    filters_changed = pyqtSignal(dict)
    
    def __init__(self, filter_fields: List[FilterField], context_name: str = "Datos", parent=None):
        super().__init__("Filtros", parent)
        self.filter_fields = filter_fields
        self.context_name = context_name
        self.active_filters = {}
        self.current_mode = "advanced"  # "advanced" o "smart"
        
        self.setObjectName("unified_filter_button")
        self.setToolTip("Abrir panel de filtros avanzados e inteligentes")
        
        self.clicked.connect(self.show_filter_dialog)
        self.update_button_text()
    
    def update_button_text(self):
        """Actualiza el texto del botón según los filtros activos"""
        active_count = len([v for v in self.active_filters.values() if v is not None and v != ""])
        if active_count > 0:
            self.setText(f"Filtros ({active_count})")
            self.setProperty("hasFilters", "true")
        else:
            self.setText("Filtros")
            self.setProperty("hasFilters", "false")
        
        # Forzar actualización de estilo
        self.style().unpolish(self)
        self.style().polish(self)
    
    def show_filter_dialog(self):
        """Muestra el diálogo de filtros unificado"""
        dialog = UnifiedFilterDialog(self.filter_fields, self.context_name, self.active_filters, self.current_mode, self)
        if dialog.exec():
            self.active_filters = dialog.get_active_filters()
            self.current_mode = dialog.get_current_mode()
            self.update_button_text()
            self.filters_changed.emit(self.active_filters)
    
    def clear_filters(self):
        """Limpia todos los filtros activos"""
        self.active_filters = {}
        self.update_button_text()
        self.filters_changed.emit(self.active_filters)
    
    def get_filters(self) -> Dict[str, Any]:
        """Obtiene los filtros activos del widget"""
        return self.active_filters.copy()

class UnifiedFilterDialog(QDialog):
    """Diálogo simplificado para filtros avanzados"""
    
    def __init__(self, filter_fields: List[FilterField], context_name: str, current_filters: Dict[str, Any], current_mode: str, parent=None):
        super().__init__(parent)
        self.filter_fields = filter_fields
        self.context_name = context_name
        self.current_filters = current_filters.copy()
        
        self.setWindowTitle(f"Filtros - {context_name}")
        self.setModal(True)
        self.resize(800, 600)
        
        self.setup_ui()
        self.load_current_filters()
    
    def setup_ui(self):
        """Configura la interfaz del diálogo"""
        layout = QVBoxLayout(self)
        
        # Título de la sección
        title_label = QLabel("Filtros Avanzados")
        title_label.setFont(QFont("Arial", 12, QFont.Weight.Bold))
        title_label.setObjectName("unified_filter_title")
        layout.addWidget(title_label)
        
        # Widget de filtros avanzados directamente expandido
        self.advanced_widget = AdvancedFilterWidget(self.filter_fields)
        layout.addWidget(self.advanced_widget)
        
        # Botones de acción
        button_layout = QHBoxLayout()
        
        self.clear_button = QPushButton("Limpiar Filtros")
        self.clear_button.clicked.connect(self.clear_all_filters)
        
        button_box = QDialogButtonBox(
            QDialogButtonBox.StandardButton.Ok | QDialogButtonBox.StandardButton.Cancel
        )
        button_box.accepted.connect(self.accept)
        button_box.rejected.connect(self.reject)
        
        button_layout.addWidget(self.clear_button)
        button_layout.addStretch()
        button_layout.addWidget(button_box)
        
        layout.addLayout(button_layout)
        
        # No hay señales adicionales que conectar
    

    
    def load_current_filters(self):
        """Carga los filtros actuales en el widget"""
        if self.current_filters:
            self.advanced_widget.set_filters(self.current_filters)
    
    def clear_all_filters(self):
        """Limpia todos los filtros"""
        self.advanced_widget.clear_filters()
        self.current_filters = {}
    
    def get_active_filters(self) -> Dict[str, Any]:
        """Obtiene los filtros activos del widget avanzado"""
        return self.advanced_widget.get_active_filters()
    
    def get_current_mode(self) -> str:
        """Obtiene el modo actual de filtro (siempre avanzado)"""
        return "advanced"

