from PyQt6.QtWidgets import (
    QPushButton, QDialog, QVBoxLayout, QHBoxLayout, QDialogButtonBox,
    QLabel, QFrame
)
from PyQt6.QtCore import Qt, pyqtSignal
from PyQt6.QtGui import QFont, QIcon
from .advanced_filter_widget import AdvancedFilterWidget, FilterField
from typing import List, Dict, Any

class FilterDialog(QDialog):
    """Diálogo que contiene los filtros avanzados"""
    
    def __init__(self, filter_fields: List[FilterField], parent=None):
        super().__init__(parent)
        self.filter_fields = filter_fields
        self.setup_ui()
        self.setModal(True)
        self.resize(500, 600)
        
    def setup_ui(self):
        """Configura la interfaz del diálogo"""
        self.setWindowTitle("Filtros Avanzados")
        
        layout = QVBoxLayout(self)
        layout.setContentsMargins(16, 16, 16, 16)
        layout.setSpacing(16)
        
        # Título
        title_label = QLabel("Configurar Filtros")
        title_label.setFont(QFont("Segoe UI", 14, QFont.Weight.Bold))
        title_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        title_label.setObjectName("filter_dialog_title")
        layout.addWidget(title_label)
        
        # Widget de filtros avanzados
        self.advanced_filter = AdvancedFilterWidget(self.filter_fields)
        # Forzar que el panel esté expandido
        self.advanced_filter.is_collapsed = False
        self.advanced_filter.filters_panel.setVisible(True)
        self.advanced_filter.toggle_button.setText("▲")
        
        # Ocultar el header del widget de filtros ya que tenemos nuestro propio título
        header_frame = self.advanced_filter.findChild(QFrame, "filter_header")
        if header_frame:
            header_frame.setVisible(False)
            
        layout.addWidget(self.advanced_filter)
        
        # Botones del diálogo
        button_box = QDialogButtonBox(
            QDialogButtonBox.StandardButton.Ok | 
            QDialogButtonBox.StandardButton.Cancel |
            QDialogButtonBox.StandardButton.Reset
        )
        
        # Personalizar botones
        ok_button = button_box.button(QDialogButtonBox.StandardButton.Ok)
        ok_button.setText("Aplicar Filtros")
        
        cancel_button = button_box.button(QDialogButtonBox.StandardButton.Cancel)
        cancel_button.setText("Cancelar")
        
        reset_button = button_box.button(QDialogButtonBox.StandardButton.Reset)
        reset_button.setText("Limpiar Todo")
        
        # Conectar señales
        button_box.accepted.connect(self.accept)
        button_box.rejected.connect(self.reject)
        reset_button.clicked.connect(self.advanced_filter.clear_filters)
        
        layout.addWidget(button_box)
        
        # Aplicar objectName al diálogo
        self.setObjectName("filter_dialog")
    
    def get_filters(self) -> Dict[str, Any]:
        """Obtiene los filtros activos del widget"""
        return self.advanced_filter.get_active_filters()
    
    def set_filters(self, filters: Dict[str, Any]):
        """Establece filtros en el widget"""
        for field_name, value in filters.items():
            self.advanced_filter.set_filter_value(field_name, value)

class FilterButton(QPushButton):
    """Botón compacto con ícono de lupa que abre el diálogo de filtros"""
    
    # Señales
    filters_changed = pyqtSignal(dict)  # Emite los filtros activos
    filters_cleared = pyqtSignal()      # Emite cuando se limpian los filtros
    
    def __init__(self, filter_fields: List[FilterField], parent=None):
        super().__init__(parent)
        self.filter_fields = filter_fields
        self.active_filters = {}
        self.setup_ui()
        self.setup_connections()
        
    def setup_ui(self):
        """Configura la interfaz del botón"""
        # Configurar texto e ícono
        self.setText("Filtros")
        self.setToolTip("Abrir filtros avanzados")
        
        # Configurar tamaño
        self.setFixedSize(100, 32)
        
        # Aplicar objectName
        self.setObjectName("filter_button")
        
    def setup_connections(self):
        """Configura las conexiones de señales"""
        self.clicked.connect(self.open_filter_dialog)
        
    def open_filter_dialog(self):
        """Abre el diálogo de filtros"""
        dialog = FilterDialog(self.filter_fields, self)
        
        # Establecer filtros actuales en el diálogo
        if self.active_filters:
            dialog.set_filters(self.active_filters)
            
        if dialog.exec() == QDialog.DialogCode.Accepted:
            # Obtener nuevos filtros
            new_filters = dialog.get_filters()
            self.active_filters = new_filters
            self.update_button_appearance()
            self.filters_changed.emit(new_filters)
            
    def update_button_appearance(self):
        """Actualiza la apariencia del botón según si hay filtros activos"""
        has_filters = len(self.active_filters) > 0
        self.setProperty("hasFilters", has_filters)
        
        if has_filters:
            count = len(self.active_filters)
            self.setText(f"Filtros ({count})")
            self.setToolTip(f"Filtros activos: {count}. Click para modificar.")
        else:
            self.setText("Filtros")
            self.setToolTip("Abrir filtros avanzados")
            
        # Forzar actualización del estilo
        self.style().unpolish(self)
        self.style().polish(self)
        
    def clear_filters(self):
        """Limpia todos los filtros activos"""
        self.active_filters = {}
        self.update_button_appearance()
        self.filters_cleared.emit()
        
    def get_active_filters(self) -> Dict[str, Any]:
        """Obtiene los filtros activos"""
        return self.active_filters.copy()
        
    def set_filters(self, filters: Dict[str, Any]):
        """Establece filtros específicos"""
        self.active_filters = filters.copy()
        self.update_button_appearance()
        self.filters_changed.emit(self.active_filters)

