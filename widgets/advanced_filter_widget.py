from PyQt6.QtWidgets import (
    QWidget, QVBoxLayout, QHBoxLayout, QLineEdit, QPushButton, 
    QComboBox, QDateEdit, QSpinBox, QDoubleSpinBox, QCheckBox,
    QLabel, QFrame, QGroupBox, QFormLayout, QScrollArea,
    QButtonGroup, QRadioButton, QSlider, QListWidget, QListWidgetItem
)
from PyQt6.QtCore import Qt, pyqtSignal, QDate, QTimer
from PyQt6.QtGui import QFont, QIcon
from datetime import datetime, date, timedelta
from typing import Dict, List, Any, Optional, Callable
import logging

class FilterField:
    """Representa un campo de filtro con sus propiedades"""
    
    def __init__(self, name: str, label: str, field_type: str, 
                 options: List[str] = None, min_value: float = None, 
                 max_value: float = None, default_value: Any = None):
        self.name = name
        self.label = label
        self.field_type = field_type  # 'text', 'combo', 'date', 'number', 'boolean', 'range'
        self.options = options or []
        self.min_value = min_value
        self.max_value = max_value
        self.default_value = default_value

class AdvancedFilterWidget(QWidget):
    """Widget de filtros avanzados reutilizable para tablas"""
    
    # Señales
    filters_changed = pyqtSignal(dict)  # Emite los filtros activos
    filters_cleared = pyqtSignal()      # Emite cuando se limpian los filtros
    
    def __init__(self, filter_fields: List[FilterField], parent=None):
        super().__init__(parent)
        # Convertir diccionarios a objetos FilterField si es necesario
        self.filter_fields = self._convert_to_filter_fields(filter_fields)
        self.filter_widgets = {}  # Almacena los widgets de filtro
        self.active_filters = {}  # Almacena los filtros activos
        self.is_collapsed = False  # Estado del panel (expandido por defecto)
        
        # Timer para búsqueda con delay
        self.filter_timer = QTimer()
        self.filter_timer.setSingleShot(True)
        self.filter_timer.timeout.connect(self._emit_filters)
        self.filter_delay = 300  # ms
        
        self.setup_ui()
        self.setup_connections()
    
    def _convert_to_filter_fields(self, filter_fields):
        """Convierte diccionarios a objetos FilterField si es necesario"""
        converted_fields = []
        for field in filter_fields:
            if isinstance(field, dict):
                # Convertir diccionario a FilterField
                name = field.get('field', '')
                label = field.get('label', name)
                field_type = field.get('type', 'text')
                options = field.get('options', [])
                
                # Manejar opciones anidadas
                if isinstance(options, dict):
                    if 'options' in options:
                        options = options['options']
                    else:
                        options = []
                
                min_value = None
                max_value = None
                if isinstance(field.get('options'), dict):
                    min_value = field['options'].get('min')
                    max_value = field['options'].get('max')
                
                filter_field = FilterField(
                    name=name,
                    label=label,
                    field_type=field_type,
                    options=options,
                    min_value=min_value,
                    max_value=max_value
                )
                converted_fields.append(filter_field)
            else:
                # Ya es un objeto FilterField
                converted_fields.append(field)
        return converted_fields
    
    def setup_ui(self):
        """Configura la interfaz del widget de filtros"""
        main_layout = QVBoxLayout(self)
        main_layout.setContentsMargins(0, 0, 0, 0)
        main_layout.setSpacing(8)
        
        # Header simplificado sin botón de colapsar
        header_frame = QFrame()
        header_frame.setObjectName("advanced_filter_header")
        
        header_layout = QHBoxLayout(header_frame)
        header_layout.setContentsMargins(12, 8, 12, 8)
        
        # Título y contador de filtros activos
        self.title_label = QLabel("Filtros Avanzados")
        self.title_label.setFont(QFont("Segoe UI", 10, QFont.Weight.Bold))
        
        self.filter_count_label = QLabel("")
        self.filter_count_label.setObjectName("advanced_filter_count_label")
        
        # Botón de limpiar filtros
        self.clear_button = QPushButton("Limpiar")
        self.clear_button.setObjectName("advanced_filter_clear_button")
        self.clear_button.setFixedSize(60, 24)
        
        header_layout.addWidget(self.title_label)
        header_layout.addWidget(self.filter_count_label)
        header_layout.addStretch()
        header_layout.addWidget(self.clear_button)
        
        main_layout.addWidget(header_frame)
        
        # Panel de filtros (siempre visible)
        self.filters_panel = QFrame()
        self.filters_panel.setObjectName("advanced_filters_panel")
        self.filters_panel.setVisible(True)
        
        self.setup_filters_panel()
        main_layout.addWidget(self.filters_panel)
    
    def setup_filters_panel(self):
        """Configura el panel de filtros"""
        scroll_area = QScrollArea()
        scroll_area.setWidgetResizable(True)
        scroll_area.setHorizontalScrollBarPolicy(Qt.ScrollBarPolicy.ScrollBarAlwaysOff)
        scroll_area.setVerticalScrollBarPolicy(Qt.ScrollBarPolicy.ScrollBarAsNeeded)
        # Permitir expansión adaptativa del área de scroll de filtros
        # scroll_area.setMaximumHeight(300)  # Removido para permitir expansión
        
        filters_widget = QWidget()
        filters_layout = QFormLayout(filters_widget)
        filters_layout.setSpacing(12)
        filters_layout.setContentsMargins(8, 8, 8, 8)
        
        # Crear widgets para cada campo de filtro
        for field in self.filter_fields:
            widget = self.create_filter_widget(field)
            if widget:
                label = QLabel(field.label)
                label.setObjectName("advanced_filter_label")
                filters_layout.addRow(label, widget)
                self.filter_widgets[field.name] = widget
        
        scroll_area.setWidget(filters_widget)
        
        panel_layout = QVBoxLayout(self.filters_panel)
        panel_layout.setContentsMargins(0, 0, 0, 0)
        panel_layout.addWidget(scroll_area)
    
    def create_filter_widget(self, field: FilterField) -> Optional[QWidget]:
        """Crea el widget apropiado para el tipo de campo"""
        if field.field_type == 'text':
            widget = QLineEdit()
            widget.setPlaceholderText(f"Buscar en {field.label.lower()}...")
            widget.textChanged.connect(self._on_filter_changed)
            if field.default_value:
                widget.setText(str(field.default_value))
            return widget
        
        elif field.field_type == 'combo':
            widget = QComboBox()
            widget.addItem("Todos", None)
            for option in field.options:
                widget.addItem(option, option)
            widget.currentTextChanged.connect(self._on_filter_changed)
            if field.default_value and field.default_value in field.options:
                widget.setCurrentText(field.default_value)
            return widget
        
        elif field.field_type == 'date':
            container = QWidget()
            layout = QHBoxLayout(container)
            layout.setContentsMargins(0, 0, 0, 0)
            layout.setSpacing(8)
            
            # Fecha desde
            from_date = QDateEdit()
            from_date.setDate(QDate.currentDate().addDays(-30))
            from_date.setCalendarPopup(True)
            from_date.dateChanged.connect(self._on_filter_changed)
            
            # Fecha hasta
            to_date = QDateEdit()
            to_date.setDate(QDate.currentDate())
            to_date.setCalendarPopup(True)
            to_date.dateChanged.connect(self._on_filter_changed)
            
            layout.addWidget(QLabel("Desde:"))
            layout.addWidget(from_date)
            layout.addWidget(QLabel("Hasta:"))
            layout.addWidget(to_date)
            
            # Almacenar referencias
            container.from_date = from_date
            container.to_date = to_date
            
            return container
        
        elif field.field_type == 'number':
            container = QWidget()
            layout = QHBoxLayout(container)
            layout.setContentsMargins(0, 0, 0, 0)
            layout.setSpacing(8)
            
            # Valor mínimo
            min_spin = QDoubleSpinBox()
            min_spin.setMinimum(field.min_value or 0)
            min_spin.setMaximum(field.max_value or 999999)
            min_spin.setValue(field.min_value or 0)
            min_spin.valueChanged.connect(self._on_filter_changed)
            
            # Valor máximo
            max_spin = QDoubleSpinBox()
            max_spin.setMinimum(field.min_value or 0)
            max_spin.setMaximum(field.max_value or 999999)
            max_spin.setValue(field.max_value or 999999)
            max_spin.valueChanged.connect(self._on_filter_changed)
            
            layout.addWidget(QLabel("Min:"))
            layout.addWidget(min_spin)
            layout.addWidget(QLabel("Max:"))
            layout.addWidget(max_spin)
            
            # Almacenar referencias
            container.min_spin = min_spin
            container.max_spin = max_spin
            
            return container
        
        elif field.field_type == 'boolean':
            widget = QCheckBox()
            widget.setTristate(True)  # Permite tres estados: True, False, None
            widget.setCheckState(Qt.CheckState.PartiallyChecked)  # Estado inicial "todos"
            widget.stateChanged.connect(self._on_filter_changed)
            return widget
        
        return None
    
    def setup_connections(self):
        """Configura las conexiones de señales"""
        self.clear_button.clicked.connect(self.clear_filters)
    

    
    def _on_filter_changed(self):
        """Maneja cambios en los filtros con delay"""
        self.filter_timer.start(self.filter_delay)
    
    def _emit_filters(self):
        """Emite los filtros activos"""
        self.active_filters = self.get_active_filters()
        self.update_filter_count()
        self.filters_changed.emit(self.active_filters)
    
    def get_active_filters(self) -> Dict[str, Any]:
        """Obtiene los filtros activos"""
        filters = {}
        
        for field in self.filter_fields:
            widget = self.filter_widgets.get(field.name)
            if not widget:
                continue
            
            if field.field_type == 'text':
                text = widget.text().strip()
                if text:
                    filters[field.name] = text
            
            elif field.field_type == 'combo':
                current_data = widget.currentData()
                if current_data is not None:
                    filters[field.name] = current_data
            
            elif field.field_type == 'date':
                from_date = widget.from_date.date().toPyDate()
                to_date = widget.to_date.date().toPyDate()
                filters[f"{field.name}_from"] = from_date
                filters[f"{field.name}_to"] = to_date
            
            elif field.field_type == 'number':
                min_val = widget.min_spin.value()
                max_val = widget.max_spin.value()
                filters[f"{field.name}_min"] = min_val
                filters[f"{field.name}_max"] = max_val
            
            elif field.field_type == 'boolean':
                state = widget.checkState()
                if state != Qt.CheckState.PartiallyChecked:
                    filters[field.name] = state == Qt.CheckState.Checked
        
        return filters
    
    def clear_filters(self):
        """Limpia todos los filtros"""
        for field in self.filter_fields:
            widget = self.filter_widgets.get(field.name)
            if not widget:
                continue
            
            if field.field_type == 'text':
                widget.clear()
            
            elif field.field_type == 'combo':
                widget.setCurrentIndex(0)  # "Todos"
            
            elif field.field_type == 'date':
                widget.from_date.setDate(QDate.currentDate().addDays(-30))
                widget.to_date.setDate(QDate.currentDate())
            
            elif field.field_type == 'number':
                widget.min_spin.setValue(field.min_value or 0)
                widget.max_spin.setValue(field.max_value or 999999)
            
            elif field.field_type == 'boolean':
                widget.setCheckState(Qt.CheckState.PartiallyChecked)
        
        self.active_filters = {}
        self.update_filter_count()
        self.filters_cleared.emit()
    
    def update_filter_count(self):
        """Actualiza el contador de filtros activos"""
        count = len(self.active_filters)
        if count > 0:
            self.filter_count_label.setText(f"({count} filtro{'s' if count != 1 else ''} activo{'s' if count != 1 else ''})")
            self.filter_count_label.setVisible(True)
        else:
            self.filter_count_label.setVisible(False)
    
    def set_filter_value(self, field_name: str, value: Any):
        """Establece el valor de un filtro específico"""
        widget = self.filter_widgets.get(field_name)
        if not widget:
            return
        
        field = next((f for f in self.filter_fields if f.name == field_name), None)
        if not field:
            return
        
        if field.field_type == 'text':
            widget.setText(str(value) if value else "")
        elif field.field_type == 'combo':
            index = widget.findData(value)
            if index >= 0:
                widget.setCurrentIndex(index)
        elif field.field_type == 'boolean':
            if value is None:
                widget.setCheckState(Qt.CheckState.PartiallyChecked)
            else:
                widget.setCheckState(Qt.CheckState.Checked if value else Qt.CheckState.Unchecked)
    
    def get_filter_value(self, field_name: str) -> Any:
        """Obtiene el valor de un filtro específico"""
        return self.active_filters.get(field_name)
    
    def apply_filters_to_data(self, data: List[Dict[str, Any]], 
                             filter_function: Callable[[Dict[str, Any], Dict[str, Any]], bool] = None) -> List[Dict[str, Any]]:
        """Aplica los filtros activos a una lista de datos"""
        if not self.active_filters:
            return data
        
        if filter_function:
            return [item for item in data if filter_function(item, self.active_filters)]
        
        # Filtrado básico por defecto
        filtered_data = []
        for item in data:
            if self._default_filter_match(item, self.active_filters):
                filtered_data.append(item)
        
        return filtered_data
    
    def _default_filter_match(self, item: Dict[str, Any], filters: Dict[str, Any]) -> bool:
        """Función de filtrado por defecto"""
        for filter_name, filter_value in filters.items():
            if filter_name.endswith('_from') or filter_name.endswith('_to'):
                continue  # Manejado por separado
            
            if filter_name.endswith('_min') or filter_name.endswith('_max'):
                continue  # Manejado por separado
            
            item_value = item.get(filter_name)
            if item_value is None:
                continue
            
            # Filtro de texto
            if isinstance(filter_value, str):
                if filter_value.lower() not in str(item_value).lower():
                    return False
            
            # Filtro booleano
            elif isinstance(filter_value, bool):
                if bool(item_value) != filter_value:
                    return False
            
            # Filtro exacto
            else:
                if item_value != filter_value:
                    return False
        
        return True

