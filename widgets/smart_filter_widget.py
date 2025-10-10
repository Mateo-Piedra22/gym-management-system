from PyQt6.QtWidgets import (
    QWidget, QVBoxLayout, QHBoxLayout, QLineEdit, QPushButton, 
    QComboBox, QLabel, QFrame, QGroupBox, QFormLayout, QScrollArea,
    QListWidget, QListWidgetItem, QDialog, QDialogButtonBox,
    QTextEdit, QCheckBox, QTabWidget, QSplitter, QTreeWidget,
    QTreeWidgetItem, QMenu, QMessageBox, QProgressBar, QSlider
)
from PyQt6.QtCore import Qt, pyqtSignal, QTimer, QThread, pyqtSlot
from PyQt6.QtGui import QFont, QIcon, QAction, QPixmap, QPainter, QColor
from datetime import datetime, date, timedelta
from typing import Dict, List, Any, Optional, Callable, Tuple
import json
import logging
from dataclasses import dataclass, asdict
from collections import defaultdict, Counter
import re

@dataclass
class SavedFilter:
    """Representa un filtro guardado"""
    name: str
    description: str
    filters: Dict[str, Any]
    created_at: datetime
    usage_count: int = 0
    is_favorite: bool = False
    tags: List[str] = None
    
    def __post_init__(self):
        if self.tags is None:
            self.tags = []

@dataclass
class FilterSuggestion:
    """Representa una sugerencia de filtro"""
    field: str
    value: str
    confidence: float
    reason: str
    usage_count: int = 0

class FilterAnalyzer:
    """Analizador de patrones de filtros para sugerencias inteligentes"""
    
    def __init__(self):
        self.usage_patterns = defaultdict(Counter)
        self.field_correlations = defaultdict(lambda: defaultdict(int))
        self.temporal_patterns = defaultdict(list)
    
    def record_filter_usage(self, filters: Dict[str, Any]):
        """Registra el uso de filtros para análisis"""
        timestamp = datetime.now()
        
        # Registrar patrones de uso individual
        for field, value in filters.items():
            self.usage_patterns[field][str(value)] += 1
            self.temporal_patterns[field].append((timestamp, str(value)))
        
        # Registrar correlaciones entre campos
        fields = list(filters.keys())
        for i, field1 in enumerate(fields):
            for field2 in fields[i+1:]:
                key = f"{field1}:{filters[field1]}|{field2}:{filters[field2]}"
                self.field_correlations[field1][key] += 1
    
    def get_suggestions(self, current_filters: Dict[str, Any], 
                       available_fields: List[str]) -> List[FilterSuggestion]:
        """Genera sugerencias basadas en patrones de uso"""
        suggestions = []
        
        # Sugerencias basadas en uso frecuente
        for field in available_fields:
            if field not in current_filters:
                most_common = self.usage_patterns[field].most_common(3)
                for value, count in most_common:
                    if count > 2:  # Mínimo de uso
                        confidence = min(count / 10.0, 1.0)
                        suggestions.append(FilterSuggestion(
                            field=field,
                            value=value,
                            confidence=confidence,
                            reason=f"Usado {count} veces",
                            usage_count=count
                        ))
        
        # Sugerencias basadas en correlaciones
        for current_field, current_value in current_filters.items():
            for field in available_fields:
                if field != current_field and field not in current_filters:
                    correlation_key = f"{current_field}:{current_value}|{field}:"
                    correlations = [(k, v) for k, v in self.field_correlations[current_field].items() 
                                  if k.startswith(correlation_key)]
                    
                    for corr_key, count in correlations:
                        if count > 1:
                            suggested_value = corr_key.split('|')[1].split(':')[1]
                            confidence = min(count / 5.0, 0.8)
                            suggestions.append(FilterSuggestion(
                                field=field,
                                value=suggested_value,
                                confidence=confidence,
                                reason=f"Frecuentemente usado con {current_field}={current_value}",
                                usage_count=count
                            ))
        
        # Ordenar por confianza
        suggestions.sort(key=lambda x: x.confidence, reverse=True)
        return suggestions[:5]  # Top 5 sugerencias

class QuickFilterWidget(QWidget):
    """Widget para filtros rápidos predefinidos"""
    
    filter_applied = pyqtSignal(dict)
    
    def __init__(self, parent=None):
        super().__init__(parent)
        self.quick_filters = {
            "Hoy": {"fecha_from": date.today(), "fecha_to": date.today()},
            "Esta semana": {"fecha_from": date.today() - timedelta(days=7), "fecha_to": date.today()},
            "Este mes": {"fecha_from": date.today().replace(day=1), "fecha_to": date.today()},
            "Activos": {"estado": "activo"},
            "Inactivos": {"estado": "inactivo"},
            "Nuevos (30 días)": {"fecha_registro_from": date.today() - timedelta(days=30)}
        }
        self.setup_ui()
    
    def setup_ui(self):
        layout = QHBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(8)
        
        label = QLabel("🚀 Filtros Rápidos:")
        label.setObjectName("quick_filters_label")
        layout.addWidget(label)
        
        for name, filters in self.quick_filters.items():
            btn = QPushButton(name)
            btn.setObjectName("quick_filter_button")
            btn.clicked.connect(lambda checked, f=filters: self.filter_applied.emit(f))
            layout.addWidget(btn)
        
        layout.addStretch()

class SmartSearchWidget(QWidget):
    """Widget de búsqueda inteligente con sugerencias"""
    
    search_changed = pyqtSignal(str)
    filter_suggested = pyqtSignal(dict)
    
    def __init__(self, analyzer: FilterAnalyzer, parent=None):
        super().__init__(parent)
        self.analyzer = analyzer
        self.suggestions_visible = False
        self.setup_ui()
    
    def setup_ui(self):
        layout = QVBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(4)
        
        # Campo de búsqueda
        search_layout = QHBoxLayout()
        
        self.search_input = QLineEdit()
        self.search_input.setObjectName("smart_search_input")
        self.search_input.setPlaceholderText("🔍 Búsqueda inteligente... (ej: 'usuarios activos enero')")
        self.search_input.textChanged.connect(self.on_search_changed)
        
        self.suggestions_btn = QPushButton("💡")
        self.suggestions_btn.setFixedSize(32, 32)
        self.suggestions_btn.setToolTip("Ver sugerencias")
        self.suggestions_btn.clicked.connect(self.toggle_suggestions)
        
        search_layout.addWidget(self.search_input)
        search_layout.addWidget(self.suggestions_btn)
        layout.addLayout(search_layout)
        
        # Panel de sugerencias
        self.suggestions_panel = QFrame()
        self.suggestions_panel.setObjectName("smart_suggestions_panel")
        self.suggestions_panel.setVisible(False)
        
        suggestions_layout = QVBoxLayout(self.suggestions_panel)
        
        self.suggestions_list = QListWidget()
        self.suggestions_list.setObjectName("smart_suggestions_list")
        self.suggestions_list.setMaximumHeight(150)
        self.suggestions_list.itemClicked.connect(self.apply_suggestion)
        
        suggestions_layout.addWidget(QLabel("💡 Sugerencias:"))
        suggestions_layout.addWidget(self.suggestions_list)
        
        layout.addWidget(self.suggestions_panel)
    
    def on_search_changed(self, text: str):
        self.search_changed.emit(text)
        self.update_suggestions(text)
    
    def update_suggestions(self, query: str):
        """Actualiza las sugerencias basadas en la consulta"""
        self.suggestions_list.clear()
        
        if len(query) < 2:
            return
        
        # Análisis simple de consulta en lenguaje natural
        suggestions = self.parse_natural_query(query)
        
        for suggestion in suggestions:
            item = QListWidgetItem(f"🎯 {suggestion['description']}")
            item.setData(Qt.ItemDataRole.UserRole, suggestion['filters'])
            self.suggestions_list.addItem(item)
    
    def parse_natural_query(self, query: str) -> List[Dict[str, Any]]:
        """Parsea consultas en lenguaje natural"""
        suggestions = []
        query_lower = query.lower()
        
        # Patrones de estado
        if 'activo' in query_lower:
            suggestions.append({
                'description': 'Filtrar por usuarios activos',
                'filters': {'estado': 'activo'}
            })
        elif 'inactivo' in query_lower:
            suggestions.append({
                'description': 'Filtrar por usuarios inactivos',
                'filters': {'estado': 'inactivo'}
            })
        
        # Patrones temporales
        if 'hoy' in query_lower:
            suggestions.append({
                'description': 'Filtrar por hoy',
                'filters': {'fecha_from': date.today(), 'fecha_to': date.today()}
            })
        elif 'semana' in query_lower:
            suggestions.append({
                'description': 'Filtrar por esta semana',
                'filters': {'fecha_from': date.today() - timedelta(days=7), 'fecha_to': date.today()}
            })
        elif 'mes' in query_lower:
            suggestions.append({
                'description': 'Filtrar por este mes',
                'filters': {'fecha_from': date.today().replace(day=1), 'fecha_to': date.today()}
            })
        
        # Patrones de monto
        amount_match = re.search(r'(\d+)', query_lower)
        if amount_match and ('pago' in query_lower or 'monto' in query_lower):
            amount = int(amount_match.group(1))
            suggestions.append({
                'description': f'Filtrar por monto mayor a ${amount}',
                'filters': {'monto_min': amount}
            })
        
        return suggestions
    
    def apply_suggestion(self, item: QListWidgetItem):
        """Aplica una sugerencia seleccionada"""
        filters = item.data(Qt.ItemDataRole.UserRole)
        if filters:
            self.filter_suggested.emit(filters)
    
    def toggle_suggestions(self):
        """Alterna la visibilidad del panel de sugerencias"""
        self.suggestions_visible = not self.suggestions_visible
        self.suggestions_panel.setVisible(self.suggestions_visible)

class SavedFiltersWidget(QWidget):
    """Widget para gestionar filtros guardados"""
    
    filter_loaded = pyqtSignal(dict)
    
    def __init__(self, parent=None):
        super().__init__(parent)
        self.saved_filters: List[SavedFilter] = []
        self.setup_ui()
        self.load_saved_filters()
    
    def setup_ui(self):
        layout = QVBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(8)
        
        # Header
        header_layout = QHBoxLayout()
        
        title = QLabel("💾 Filtros Guardados")
        title.setFont(QFont("Segoe UI", 10, QFont.Weight.Bold))
        
        self.save_btn = QPushButton("Guardar Actual")
        self.save_btn.setObjectName("save_filter_button")
        
        header_layout.addWidget(title)
        header_layout.addStretch()
        header_layout.addWidget(self.save_btn)
        layout.addLayout(header_layout)
        
        # Lista de filtros guardados
        self.filters_tree = QTreeWidget()
        self.filters_tree.setHeaderLabels(["Nombre", "Descripción", "Usos", "Favorito"])
        self.filters_tree.setObjectName("saved_filters_tree")
        self.filters_tree.setContextMenuPolicy(Qt.ContextMenuPolicy.CustomContextMenu)
        self.filters_tree.customContextMenuRequested.connect(self.show_context_menu)
        self.filters_tree.itemDoubleClicked.connect(self.load_filter)
        
        layout.addWidget(self.filters_tree)
    
    def save_current_filter(self, filters: Dict[str, Any]):
        """Guarda el filtro actual"""
        dialog = SaveFilterDialog(self)
        if dialog.exec() == QDialog.DialogCode.Accepted:
            name, description, tags = dialog.get_data()
            
            saved_filter = SavedFilter(
                name=name,
                description=description,
                filters=filters,
                created_at=datetime.now(),
                tags=tags
            )
            
            self.saved_filters.append(saved_filter)
            self.save_filters_to_storage()
            self.refresh_tree()
    
    def load_filter(self, item: QTreeWidgetItem):
        """Carga un filtro guardado"""
        filter_name = item.text(0)
        saved_filter = next((f for f in self.saved_filters if f.name == filter_name), None)
        
        if saved_filter:
            saved_filter.usage_count += 1
            self.save_filters_to_storage()
            self.refresh_tree()
            self.filter_loaded.emit(saved_filter.filters)
    
    def show_context_menu(self, position):
        """Muestra el menú contextual"""
        item = self.filters_tree.itemAt(position)
        if not item:
            return
        
        menu = QMenu(self)
        menu.setObjectName("contextMenu")
        menu.setProperty("menuType", "filter")
        
        load_action = QAction("📂 Cargar", self)
        load_action.triggered.connect(lambda: self.load_filter(item))
        menu.addAction(load_action)
        
        favorite_action = QAction("⭐ Marcar como favorito", self)
        favorite_action.triggered.connect(lambda: self.toggle_favorite(item))
        menu.addAction(favorite_action)
        
        delete_action = QAction("🗑️ Eliminar", self)
        delete_action.triggered.connect(lambda: self.delete_filter(item))
        menu.addAction(delete_action)
        
        menu.exec(self.filters_tree.mapToGlobal(position))
    
    def toggle_favorite(self, item: QTreeWidgetItem):
        """Alterna el estado de favorito"""
        filter_name = item.text(0)
        saved_filter = next((f for f in self.saved_filters if f.name == filter_name), None)
        
        if saved_filter:
            saved_filter.is_favorite = not saved_filter.is_favorite
            self.save_filters_to_storage()
            self.refresh_tree()
    
    def delete_filter(self, item: QTreeWidgetItem):
        """Elimina un filtro guardado"""
        filter_name = item.text(0)
        reply = QMessageBox.question(
            self, "Confirmar eliminación",
            f"¿Está seguro de que desea eliminar el filtro '{filter_name}'?",
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No
        )
        
        if reply == QMessageBox.StandardButton.Yes:
            self.saved_filters = [f for f in self.saved_filters if f.name != filter_name]
            self.save_filters_to_storage()
            self.refresh_tree()
    
    def refresh_tree(self):
        """Actualiza el árbol de filtros"""
        self.filters_tree.clear()
        
        # Ordenar por favoritos y uso
        sorted_filters = sorted(
            self.saved_filters,
            key=lambda f: (not f.is_favorite, -f.usage_count)
        )
        
        for saved_filter in sorted_filters:
            item = QTreeWidgetItem([
                saved_filter.name,
                saved_filter.description,
                str(saved_filter.usage_count),
                "⭐" if saved_filter.is_favorite else ""
            ])
            
            if saved_filter.is_favorite:
                item.setBackground(0, QColor("#5E81AC"))
            
            self.filters_tree.addTopLevelItem(item)
    
    def load_saved_filters(self):
        """Carga filtros guardados desde almacenamiento"""
        # Implementar carga desde base de datos o archivo
        pass
    
    def save_filters_to_storage(self):
        """Guarda filtros en almacenamiento"""
        # Implementar guardado en base de datos o archivo
        pass

class SaveFilterDialog(QDialog):
    """Diálogo para guardar un filtro"""
    
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Guardar Filtro")
        self.setModal(True)
        self.resize(400, 300)
        self.setup_ui()
    
    def setup_ui(self):
        layout = QVBoxLayout(self)
        
        # Nombre
        layout.addWidget(QLabel("Nombre del filtro:"))
        self.name_input = QLineEdit()
        self.name_input.setPlaceholderText("Ej: Usuarios activos enero")
        layout.addWidget(self.name_input)
        
        # Descripción
        layout.addWidget(QLabel("Descripción:"))
        self.description_input = QTextEdit()
        self.description_input.setMaximumHeight(80)
        self.description_input.setPlaceholderText("Descripción opcional del filtro...")
        layout.addWidget(self.description_input)
        
        # Tags
        layout.addWidget(QLabel("Tags (separados por comas):"))
        self.tags_input = QLineEdit()
        self.tags_input.setPlaceholderText("Ej: usuarios, activos, enero")
        layout.addWidget(self.tags_input)
        
        # Botones
        buttons = QDialogButtonBox(
            QDialogButtonBox.StandardButton.Ok | QDialogButtonBox.StandardButton.Cancel
        )
        buttons.accepted.connect(self.accept)
        buttons.rejected.connect(self.reject)
        layout.addWidget(buttons)
    
    def get_data(self) -> Tuple[str, str, List[str]]:
        """Obtiene los datos del diálogo"""
        name = self.name_input.text().strip()
        description = self.description_input.toPlainText().strip()
        tags = [tag.strip() for tag in self.tags_input.text().split(',') if tag.strip()]
        return name, description, tags

class SmartFilterWidget(QWidget):
    """Widget principal de filtros inteligentes"""
    
    filters_changed = pyqtSignal(dict)
    filters_cleared = pyqtSignal()
    
    def __init__(self, filter_fields: List, parent=None):
        super().__init__(parent)
        self.filter_fields = filter_fields
        self.current_filters = {}
        self.analyzer = FilterAnalyzer()
        self.setup_ui()
        self.setup_connections()
        
        # Barra de progreso
        self.progress_bar = QProgressBar()
        self.progress_bar.setVisible(False)
        self.progress_bar.setMinimumHeight(30)
        self.progress_bar.setTextVisible(True)
        self.progress_bar.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self.progress_bar.setStyleSheet("""
            QProgressBar {
                border: 2px solid VAR_BG_QUATERNARY;
                border-radius: 5px;
                text-align: center;
                font-weight: bold;
                font-size: 12px;
                margin: 5px 0;
            }
            QProgressBar::chunk {
                background-color: #5E81AC;
                border-radius: 3px;
            }
        """)
        layout = self.layout()
        if layout:
            layout.addWidget(self.progress_bar)
    
    def setup_ui(self):
        """Configura la interfaz principal"""
        layout = QVBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(12)
        
        # Título principal
        title = QLabel("🧠 Sistema de Filtros Inteligentes")
        title.setFont(QFont("Segoe UI", 12, QFont.Weight.Bold))
        title.setObjectName("filter_section_title")
        layout.addWidget(title)
        
        # Pestañas principales
        self.tab_widget = QTabWidget()
        self.tab_widget.setObjectName("smart_filter_tabs")
        
        # Pestaña de filtros rápidos
        quick_tab = QWidget()
        quick_layout = QVBoxLayout(quick_tab)
        
        self.quick_filters = QuickFilterWidget()
        self.smart_search = SmartSearchWidget(self.analyzer)
        
        quick_layout.addWidget(self.quick_filters)
        quick_layout.addWidget(self.smart_search)
        quick_layout.addStretch()
        
        self.tab_widget.addTab(quick_tab, "🚀 Rápidos")
        
        # Pestaña de filtros guardados
        self.saved_filters = SavedFiltersWidget()
        self.tab_widget.addTab(self.saved_filters, "💾 Guardados")
        
        # Pestaña de análisis
        analysis_tab = self.create_analysis_tab()
        self.tab_widget.addTab(analysis_tab, "📊 Análisis")
        
        layout.addWidget(self.tab_widget)
        
        # Panel de estado
        self.status_panel = self.create_status_panel()
        layout.addWidget(self.status_panel)
    
    def create_analysis_tab(self) -> QWidget:
        """Crea la pestaña de análisis de filtros"""
        widget = QWidget()
        layout = QVBoxLayout(widget)
        
        # Estadísticas de uso
        stats_group = QGroupBox("📈 Estadísticas de Uso")
        stats_layout = QFormLayout(stats_group)
        
        self.total_filters_label = QLabel("0")
        self.most_used_label = QLabel("N/A")
        self.avg_filters_label = QLabel("0")
        
        stats_layout.addRow("Total de filtros aplicados:", self.total_filters_label)
        stats_layout.addRow("Filtro más usado:", self.most_used_label)
        stats_layout.addRow("Promedio por sesión:", self.avg_filters_label)
        
        layout.addWidget(stats_group)
        
        # Patrones detectados
        patterns_group = QGroupBox("🔍 Patrones Detectados")
        patterns_layout = QVBoxLayout(patterns_group)
        
        self.patterns_list = QListWidget()
        self.patterns_list.setMaximumHeight(150)
        patterns_layout.addWidget(self.patterns_list)
        
        layout.addWidget(patterns_group)
        
        # Botón de actualización
        refresh_btn = QPushButton("🔄 Actualizar Análisis")
        refresh_btn.clicked.connect(self.update_analysis)
        layout.addWidget(refresh_btn)
        
        layout.addStretch()
        return widget
    
    def create_status_panel(self) -> QWidget:
        """Crea el panel de estado"""
        panel = QFrame()
        panel.setObjectName("smart_filter_status_panel")
        
        layout = QHBoxLayout(panel)
        
        self.status_label = QLabel("✅ Listo para filtrar")
        self.status_label.setObjectName("filter_status_active")
        
        self.filter_count_label = QLabel("0 filtros activos")
        self.filter_count_label.setObjectName("filter_count_label")
        
        clear_btn = QPushButton("🗑️ Limpiar Todo")
        clear_btn.setObjectName("smart_filter_clear_button")
        clear_btn.clicked.connect(self.clear_all_filters)
        
        save_btn = QPushButton("💾 Guardar")
        save_btn.setObjectName("smart_filter_save_button")
        save_btn.clicked.connect(self.save_current_filters)
        
        layout.addWidget(self.status_label)
        layout.addWidget(self.filter_count_label)
        layout.addStretch()
        layout.addWidget(save_btn)
        layout.addWidget(clear_btn)
        
        return panel
    
    def setup_connections(self):
        """Configura las conexiones de señales"""
        self.quick_filters.filter_applied.connect(self.apply_filters)
        self.smart_search.filter_suggested.connect(self.apply_filters)
        self.saved_filters.filter_loaded.connect(self.apply_filters)
        self.saved_filters.save_btn.clicked.connect(self.save_current_filters)
    
    def apply_filters(self, filters: Dict[str, Any]):
        """Aplica filtros y actualiza el estado"""
        self.current_filters.update(filters)
        self.analyzer.record_filter_usage(filters)
        self.update_status()
        self.filters_changed.emit(self.current_filters)
    
    def clear_all_filters(self):
        """Limpia todos los filtros"""
        self.current_filters.clear()
        self.update_status()
        self.filters_cleared.emit()
    
    def save_current_filters(self):
        """Guarda los filtros actuales"""
        if self.current_filters:
            self.saved_filters.save_current_filter(self.current_filters)
        else:
            QMessageBox.information(self, "Información", "No hay filtros activos para guardar.")
    
    def update_status(self):
        """Actualiza el panel de estado"""
        count = len(self.current_filters)
        self.filter_count_label.setText(f"{count} filtro{'s' if count != 1 else ''} activo{'s' if count != 1 else ''}")
        
        if count > 0:
            self.status_label.setText("🔍 Filtros aplicados")
            self.status_label.setObjectName("filter_status_inactive")
        else:
            self.status_label.setText("✅ Listo para filtrar")
            self.status_label.setObjectName("filter_status_active")
    
    def update_analysis(self):
        """Actualiza el análisis de patrones"""
        # Actualizar estadísticas
        total_usage = sum(sum(counter.values()) for counter in self.analyzer.usage_patterns.values())
        self.total_filters_label.setText(str(total_usage))
        
        # Encontrar el filtro más usado
        most_used = ""
        max_count = 0
        for field, counter in self.analyzer.usage_patterns.items():
            for value, count in counter.items():
                if count > max_count:
                    max_count = count
                    most_used = f"{field}: {value} ({count} veces)"
        
        self.most_used_label.setText(most_used or "N/A")
        
        # Actualizar patrones
        self.patterns_list.clear()
        suggestions = self.analyzer.get_suggestions(self.current_filters, 
                                                   [f.name for f in self.filter_fields])
        
        for suggestion in suggestions:
            item_text = f"💡 {suggestion.field}: {suggestion.value} ({suggestion.reason})"
            self.patterns_list.addItem(item_text)
    
    def get_current_filters(self) -> Dict[str, Any]:
        """Obtiene los filtros actuales"""
        return self.current_filters.copy()
    
    def set_filters(self, filters: Dict[str, Any]):
        """Establece filtros específicos"""
        self.current_filters = filters.copy()
        self.update_status()
        self.filters_changed.emit(self.current_filters)

