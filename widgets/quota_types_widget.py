import os
import logging
from datetime import datetime
from typing import List, Optional
from PyQt6.QtWidgets import (
    QWidget, QVBoxLayout, QHBoxLayout, QGroupBox, QPushButton,
    QTableWidget, QTableWidgetItem, QHeaderView, QAbstractItemView,
    QFormLayout, QLineEdit, QDoubleSpinBox, QTextEdit, QCheckBox,
    QMessageBox, QFileDialog, QLabel, QFrame, QComboBox, QSpinBox, QDialog,
    QSizePolicy, QMenu
)
from PyQt6.QtCore import pyqtSignal, Qt, QSize
from PyQt6.QtGui import QIcon, QPixmap, QFont
from database import DatabaseManager
from models import TipoCuota
from utils import resource_path
from widgets.icon_selector_widget import IconSelectorWidget
from utils_modules.icon_manager import IconManager

class QuotaTypesWidget(QWidget):
    """Widget para gestión de tipos de cuota dinámicos"""
    
    # Señales
    tipos_cuota_modificados = pyqtSignal()
    
    def __init__(self, db_manager: DatabaseManager, parent=None):
        super().__init__(parent)
        self.db_manager = db_manager
        self.current_tipo_cuota = None
        self.icon_manager = IconManager()
        self.setup_ui()
        self.load_tipos_cuota()
        self.connect_signals()
        self.enable_form_editing(False)
    
    def setup_ui(self):
        """Configura la interfaz de usuario"""
        main_layout = QVBoxLayout(self)
        main_layout.setSpacing(10)
        main_layout.setContentsMargins(10, 10, 10, 10)
        
        # Título principal
        title_label = QLabel("💳 Gestión de Tipos de Cuota")
        title_label.setObjectName("main_title")
        title_label.setProperty("class", "title")
        title_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        title_label.setVisible(False)
        main_layout.addWidget(title_label)
        
        # Descripción
        desc_label = QLabel(
            "Administra los diferentes tipos de cuota disponibles en el gimnasio. "
            "Puedes crear, editar y configurar precios e iconos para cada tipo."
        )
        desc_label.setProperty("class", "help_text")
        desc_label.setWordWrap(True)
        desc_label.setAlignment(Qt.AlignmentFlag.AlignLeft)
        desc_label.setVisible(False)
        # No se agrega a layout para mantener UI consistente
        
        # Contenedor principal con dos columnas
        main_container = QHBoxLayout()
        main_container.setSpacing(15)
        
        # Panel izquierdo - Lista de tipos de cuota
        self.setup_list_panel(main_container)
        
        # Panel derecho - Formulario de edición
        self.setup_form_panel(main_container)
        
        main_layout.addLayout(main_container)
        
        # Panel de estadísticas
        self.setup_statistics_panel(main_layout)
    
    def setup_list_panel(self, parent_layout):
        """Configura el panel de lista de tipos de cuota"""
        # Grupo contenedor con estilo consistente
        concepts_group = QGroupBox("📋 Tipos de Cuota Existentes")
        concepts_group.setObjectName("config_group")
        concepts_group.setSizePolicy(QSizePolicy.Policy.Preferred, QSizePolicy.Policy.Expanding)
        list_layout = QVBoxLayout(concepts_group)
        list_layout.setSpacing(10)
        list_layout.setContentsMargins(15, 15, 15, 15)
        
        # Tabla de tipos de cuota (movida arriba)
        self.tipos_table = QTableWidget()
        self.tipos_table.setProperty("class", "data_table")
        self.tipos_table.setColumnCount(6)
        self.tipos_table.setHorizontalHeaderLabels([
            "ID", "Nombre", "Precio (ARS)", "Duración", "Estado", "Usuarios"
        ])
        
        # Configurar tabla
        header = self.tipos_table.horizontalHeader()
        header.setSectionResizeMode(0, QHeaderView.ResizeMode.ResizeToContents)  # ID
        header.setSectionResizeMode(1, QHeaderView.ResizeMode.Stretch)  # Nombre (más ancho)
        header.setSectionResizeMode(2, QHeaderView.ResizeMode.Stretch)  # Precio (más ancho)
        header.setSectionResizeMode(3, QHeaderView.ResizeMode.ResizeToContents)  # Duración
        header.setSectionResizeMode(4, QHeaderView.ResizeMode.Fixed)  # Estado (más angosto)
        header.setSectionResizeMode(5, QHeaderView.ResizeMode.ResizeToContents)  # Usuarios
        header.setStretchLastSection(True)
        self.tipos_table.setColumnWidth(4, 80)
        self.tipos_table.setSelectionBehavior(QAbstractItemView.SelectionBehavior.SelectRows)
        self.tipos_table.setSelectionMode(QAbstractItemView.SelectionMode.SingleSelection)
        self.tipos_table.setEditTriggers(QAbstractItemView.EditTrigger.NoEditTriggers)
        self.tipos_table.setAlternatingRowColors(True)
        self.tipos_table.setSortingEnabled(True)
        self.tipos_table.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Expanding)
        self.tipos_table.setMinimumHeight(120)
        self.tipos_table.verticalHeader().setVisible(False)
        self.tipos_table.verticalHeader().setDefaultSectionSize(22)
        
        # Configurar context menu
        self.tipos_table.setContextMenuPolicy(Qt.ContextMenuPolicy.CustomContextMenu)
        self.tipos_table.customContextMenuRequested.connect(self.show_table_context_menu)
        
        list_layout.addWidget(self.tipos_table)
        
        # Botones de acción (abajo)
        buttons_layout = QHBoxLayout()
        buttons_layout.setSpacing(10)
        
        self.refresh_button = QPushButton("🔄 Actualizar")
        self.refresh_button.setObjectName("action_button")
        self.refresh_button.setMinimumHeight(28)
        
        self.add_button = QPushButton("➕ Añadir")
        self.add_button.setObjectName("success_button")
        self.add_button.setMinimumHeight(28)
        
        self.edit_button = QPushButton("✏️ Editar")
        self.edit_button.setObjectName("primary_button")
        self.edit_button.setMinimumHeight(28)
        self.edit_button.setEnabled(False)
        
        self.toggle_button = QPushButton("🔄 Activar/Desactivar")
        self.toggle_button.setObjectName("warning_button")
        self.toggle_button.setMinimumHeight(28)
        self.toggle_button.setEnabled(False)
        
        self.delete_button = QPushButton("🗑️ Eliminar")
        self.delete_button.setObjectName("danger_button")
        self.delete_button.setMinimumHeight(28)
        self.delete_button.setEnabled(False)
        
        buttons_layout.addWidget(self.refresh_button)
        buttons_layout.addWidget(self.add_button)
        buttons_layout.addWidget(self.edit_button)
        buttons_layout.addWidget(self.toggle_button)
        buttons_layout.addWidget(self.delete_button)
        buttons_layout.addStretch()
        
        list_layout.addLayout(buttons_layout)
        
        parent_layout.addWidget(concepts_group, 2)
    
    def setup_form_panel(self, parent_layout):
        """Configura el panel de formulario de edición"""
        # Grupo contenedor con borde y título
        form_group = QGroupBox("➕ Crear/Editar Tipo de Cuota")
        form_group.setObjectName("config_group")
        form_group.setSizePolicy(QSizePolicy.Policy.Preferred, QSizePolicy.Policy.Expanding)
        
        layout = QVBoxLayout(form_group)
        layout.setContentsMargins(15, 15, 15, 15)
        layout.setSpacing(10)
        
        # Formulario
        form_widget = QWidget()
        form_layout = QVBoxLayout(form_widget)
        form_layout.setSpacing(10)
        
        # Campos del formulario
        fields_layout = QFormLayout()
        fields_layout.setSpacing(10)
        
        self.nombre_edit = QLineEdit()
        self.nombre_edit.setPlaceholderText("Ej: Premium, Estudiante, Senior...")
        self.nombre_edit.setObjectName("form_input")
        fields_layout.addRow("📝 Nombre:", self.nombre_edit)
        
        self.descripcion_edit = QTextEdit()
        self.descripcion_edit.setPlaceholderText("Descripción del tipo de cuota, beneficios incluidos, restricciones, etc.")
        self.descripcion_edit.setMaximumHeight(80)
        self.descripcion_edit.setObjectName("form_input")
        fields_layout.addRow("📄 Descripción:", self.descripcion_edit)
        
        self.precio_spinbox = QDoubleSpinBox()
        self.precio_spinbox.setRange(0.0, 999999.99)
        self.precio_spinbox.setDecimals(2)
        self.precio_spinbox.setPrefix("$ ")
        self.precio_spinbox.setSuffix(" ARS")
        self.precio_spinbox.setObjectName("form_input")
        fields_layout.addRow("💰 Precio Mensual:", self.precio_spinbox)
        
        # Campo para duración en días
        self.duracion_spinbox = QSpinBox()
        self.duracion_spinbox.setRange(1, 365)
        self.duracion_spinbox.setValue(30)  # Valor por defecto: 30 días
        self.duracion_spinbox.setSuffix(" días")
        self.duracion_spinbox.setObjectName("form_input")
        self.duracion_spinbox.setToolTip("Duración de la cuota en días (ej: 30 días para mensual, 7 días para semanal)")
        fields_layout.addRow("📅 Duración:", self.duracion_spinbox)
        
        # Selector de iconos
        icon_layout = QHBoxLayout()
        icon_layout.setSpacing(5)
        self.icon_button = QPushButton("💰 Seleccionar Icono")
        self.icon_button.clicked.connect(self.open_icon_selector)
        self.selected_icon = "💰"  # Icono por defecto
        
        self.icon_preview = QLabel()
        self.icon_preview.setFixedSize(32, 32)
        self.icon_preview.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self.icon_preview.setStyleSheet("border: 1px solid gray; border-radius: 4px;")
        self.update_icon_preview()
        
        icon_layout.addWidget(self.icon_button)
        icon_layout.addWidget(self.icon_preview)
        
        fields_layout.addRow("🎨 Icono:", icon_layout)
        
        self.activo_checkbox = QCheckBox("Tipo de cuota disponible para nuevos usuarios")
        self.activo_checkbox.setChecked(True)
        fields_layout.addRow("✅ Estado:", self.activo_checkbox)
        
        form_layout.addLayout(fields_layout)
        
        # Botones del formulario
        form_buttons_layout = QHBoxLayout()
        form_buttons_layout.setSpacing(10)
        
        self.save_button = QPushButton("💾 Guardar")
        self.save_button.setObjectName("success_button")
        form_buttons_layout.addWidget(self.save_button)
        
        self.cancel_button = QPushButton("🧹 Limpiar")
        self.cancel_button.setObjectName("secondary_button")
        form_buttons_layout.addWidget(self.cancel_button)
        
        form_layout.addLayout(form_buttons_layout)
        
        layout.addWidget(form_widget)
        
        parent_layout.addWidget(form_group, 1)
    
    def setup_statistics_panel(self, parent_layout):
        """Configura el panel de estadísticas"""
        # Grupo contenedor de estadísticas
        stats_group = QGroupBox("📊 Estadísticas de Tipos de Cuota")
        stats_group.setObjectName("config_group")
        # Reducir verticalmente el panel de estadísticas y evitar que expanda
        stats_group.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Fixed)
        stats_group.setMinimumHeight(100)
        stats_group.setMaximumHeight(160)
        stats_main_layout = QVBoxLayout(stats_group)
        stats_main_layout.setSpacing(10)
        stats_main_layout.setContentsMargins(15, 15, 15, 15)
        
        # Layout horizontal para las métricas
        stats_layout = QHBoxLayout()
        stats_layout.setSpacing(20)
        stats_layout.setContentsMargins(0, 0, 0, 0)
        
        # Métricas
        self.total_tipos_label = QLabel("0")
        self.total_tipos_label.setObjectName("metric_value")
        self.total_tipos_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        
        self.tipos_activos_label = QLabel("0")
        self.tipos_activos_label.setObjectName("metric_value")
        self.tipos_activos_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        
        self.tipos_inactivos_label = QLabel("0")
        self.tipos_inactivos_label.setObjectName("metric_value")
        self.tipos_inactivos_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        
        self.precio_promedio_label = QLabel("$ 0.00")
        self.precio_promedio_label.setObjectName("metric_value")
        self.precio_promedio_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        
        # Contenedores de métricas
        total_frame = self.create_metric_frame("📋 Total Tipos", self.total_tipos_label)
        activos_frame = self.create_metric_frame("✅ Tipos Activos", self.tipos_activos_label)
        inactivos_frame = self.create_metric_frame("❌ Tipos Inactivos", self.tipos_inactivos_label)
        avg_price_frame = self.create_metric_frame("💵 Precio Promedio", self.precio_promedio_label)
        
        stats_layout.addWidget(total_frame, 1)
        stats_layout.addWidget(activos_frame, 1)
        stats_layout.addWidget(inactivos_frame, 1)
        stats_layout.addWidget(avg_price_frame, 1)
        
        stats_main_layout.addLayout(stats_layout)
        parent_layout.addWidget(stats_group)
    
    def create_metric_frame(self, title: str, value_label: QLabel) -> QFrame:
        """Crea un frame para una métrica"""
        frame = QFrame()
        frame.setFrameStyle(QFrame.Shape.StyledPanel)
        frame.setProperty("class", "metric_frame")
        
        layout = QVBoxLayout(frame)
        layout.setSpacing(5)
        layout.setContentsMargins(10, 10, 10, 10)
        
        title_label = QLabel(title)
        title_label.setObjectName("metric_title")
        title_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        
        layout.addWidget(title_label)
        layout.addWidget(value_label)
        
        return frame
    
    def load_available_icons(self):
        """Carga los iconos disponibles en el combo"""
        # Iconos predefinidos
        default_icons = [
            ("🏃 Estándar", "icons/standard.png"),
            ("🎓 Estudiante", "icons/student.png"),
            ("👑 Premium", "icons/premium.png"),
            ("👴 Senior", "icons/senior.png"),
            ("👨‍👩‍👧‍👦 Familiar", "icons/family.png"),
            ("💼 Corporativo", "icons/corporate.png"),
            ("🏆 VIP", "icons/vip.png"),
            ("⚡ Express", "icons/express.png")
        ]
        
        for name, path in default_icons:
            self.icon_combo.addItem(name, path)
    
    def connect_signals(self):
        """Conecta las señales de los widgets"""
        # Tabla
        self.tipos_table.itemSelectionChanged.connect(self.on_selection_changed)
        
        # Botones de lista
        self.refresh_button.clicked.connect(self.refresh_data)
        self.add_button.clicked.connect(self.add_new_tipo)
        self.edit_button.clicked.connect(self.edit_tipo)
        self.toggle_button.clicked.connect(self.toggle_tipo_activo)
        self.delete_button.clicked.connect(self.delete_tipo)
        
        # Botones de formulario
        self.save_button.clicked.connect(self.save_tipo)
        self.cancel_button.clicked.connect(self.cancel_edit)
        
        # Campos del formulario
        self.nombre_edit.textChanged.connect(self.on_form_changed)
        self.precio_spinbox.valueChanged.connect(self.on_form_changed)
        self.duracion_spinbox.valueChanged.connect(self.on_form_changed)
        self.descripcion_edit.textChanged.connect(self.on_form_changed)
        self.activo_checkbox.toggled.connect(self.on_form_changed)
    
    def load_tipos_cuota(self):
        """Carga los tipos de cuota en la tabla"""
        try:
            tipos = self.db_manager.obtener_tipos_cuota()
            estadisticas = self.db_manager.obtener_estadisticas_tipos_cuota()
            
            # Crear diccionario de estadísticas por ID
            stats_dict = {stat['id']: stat for stat in estadisticas}
            
            self.tipos_table.setRowCount(len(tipos))
            
            for row, tipo in enumerate(tipos):
                # ID
                self.tipos_table.setItem(row, 0, QTableWidgetItem(str(tipo.id)))
                
                # Nombre
                self.tipos_table.setItem(row, 1, QTableWidgetItem(tipo.nombre))
                
                # Precio
                precio_item = QTableWidgetItem(f"$ {tipo.precio:.2f}")
                precio_item.setTextAlignment(Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter)
                self.tipos_table.setItem(row, 2, precio_item)
                
                # Duración
                duracion = getattr(tipo, 'duracion_dias', 30)
                duracion_item = QTableWidgetItem(f"{duracion} días")
                duracion_item.setTextAlignment(Qt.AlignmentFlag.AlignCenter)
                self.tipos_table.setItem(row, 3, duracion_item)
                
                # Estado
                estado_text = "✅ Activo" if tipo.activo else "❌ Inactivo"
                estado_item = QTableWidgetItem(estado_text)
                estado_item.setTextAlignment(Qt.AlignmentFlag.AlignCenter)
                self.tipos_table.setItem(row, 4, estado_item)
                
                # Usuarios
                stat = stats_dict.get(tipo.id, {})
                usuarios_count = stat.get('usuarios_activos', 0)
                usuarios_item = QTableWidgetItem(str(usuarios_count))
                usuarios_item.setTextAlignment(Qt.AlignmentFlag.AlignCenter)
                self.tipos_table.setItem(row, 5, usuarios_item)
            
            self.update_statistics(estadisticas, tipos)
            
        except Exception as e:
            logging.error(f"Error cargando tipos de cuota: {e}")
            QMessageBox.critical(self, "Error", f"Error al cargar tipos de cuota: {e}")
    
    def update_statistics(self, estadisticas: List[dict], tipos: List[TipoCuota]):
        """Actualiza las estadísticas mostradas con 4 métricas"""
        total_tipos = len(tipos)
        tipos_activos = sum(1 for t in tipos if getattr(t, 'activo', False))
        tipos_inactivos = total_tipos - tipos_activos
        avg_price = (sum(getattr(t, 'precio', 0.0) for t in tipos) / total_tipos) if total_tipos > 0 else 0.0
        
        self.total_tipos_label.setText(str(total_tipos))
        self.tipos_activos_label.setText(str(tipos_activos))
        self.tipos_inactivos_label.setText(str(tipos_inactivos))
        self.precio_promedio_label.setText(f"$ {avg_price:.2f}")
    
    def on_selection_changed(self):
        """Maneja el cambio de selección en la tabla"""
        selected_rows = self.tipos_table.selectionModel().selectedRows()
        has_selection = len(selected_rows) > 0
        
        self.edit_button.setEnabled(has_selection)
        self.toggle_button.setEnabled(has_selection)
        self.delete_button.setEnabled(has_selection)
        
        if has_selection:
            row = selected_rows[0].row()
            tipo_id = int(self.tipos_table.item(row, 0).text())
            self.load_tipo_to_form(tipo_id)
    
    def load_tipo_to_form(self, tipo_id: int):
        """Carga un tipo de cuota en el formulario"""
        try:
            tipo = self.db_manager.obtener_tipo_cuota_por_id(tipo_id)
            if tipo:
                self.current_tipo_cuota = tipo
                
                self.nombre_edit.setText(tipo.nombre)
                self.precio_spinbox.setValue(tipo.precio)
                # Cargar duración si existe, sino usar 30 días por defecto
                duracion = getattr(tipo, 'duracion_dias', 30)
                self.duracion_spinbox.setValue(duracion)
                self.descripcion_edit.setPlainText(tipo.descripcion or "")
                self.activo_checkbox.setChecked(tipo.activo)
                
                # Cargar el icono
                if hasattr(tipo, 'icono_path') and tipo.icono_path:
                    self.selected_icon = tipo.icono_path
                else:
                    self.selected_icon = "💰"  # Icono por defecto
                
                self.update_icon_preview()
                # No habilitar edición automáticamente; usar botón Editar
                self.enable_form_editing(False)
                
        except Exception as e:
            logging.error(f"Error cargando tipo de cuota: {e}")
            QMessageBox.critical(self, "Error", f"Error al cargar tipo de cuota: {e}")
    
    def add_new_tipo(self):
        """Inicia la creación de un nuevo tipo de cuota"""
        self.current_tipo_cuota = None
        self.clear_form()
        self.enable_form_editing(True)
        self.nombre_edit.setFocus()
    
    def duplicate_tipo(self):
        """Duplica el tipo de cuota seleccionado"""
        if self.current_tipo_cuota:
            self.current_tipo_cuota = None
            # Mantener los datos actuales pero cambiar el nombre
            current_name = self.nombre_edit.text()
            self.nombre_edit.setText(f"{current_name} (Copia)")
            self.enable_form_editing(True)
            self.nombre_edit.setFocus()
            self.nombre_edit.selectAll()
    
    def edit_tipo(self):
        """Carga el tipo seleccionado en el formulario para edición"""
        selected_rows = self.tipos_table.selectionModel().selectedRows()
        if not selected_rows:
            QMessageBox.information(self, "Editar", "Seleccione un tipo de cuota de la tabla.")
            return
        row = selected_rows[0].row()
        tipo_id = int(self.tipos_table.item(row, 0).text())
        self.load_tipo_to_form(tipo_id)
        self.enable_form_editing(True)
        self.nombre_edit.setFocus()
    
    def toggle_tipo_activo(self):
        """Activa/Desactiva el tipo de cuota seleccionado con validaciones"""
        selected_rows = self.tipos_table.selectionModel().selectedRows()
        if not selected_rows:
            QMessageBox.information(self, "Activar/Desactivar", "Seleccione un tipo de cuota de la tabla.")
            return
        row = selected_rows[0].row()
        tipo_id = int(self.tipos_table.item(row, 0).text())
        try:
            tipo = self.db_manager.obtener_tipo_cuota_por_id(tipo_id)
            if not tipo:
                QMessageBox.warning(self, "Error", "No se pudo obtener el tipo de cuota seleccionado.")
                return
            nuevo_estado = not tipo.activo
            # Validaciones
            todos_tipos = self.db_manager.obtener_tipos_cuota()
            activos = [t for t in todos_tipos if t.activo and t.id != tipo.id]
            if not activos and tipo.activo and not nuevo_estado:
                QMessageBox.warning(self, "No permitido", "No puede desactivar el único tipo de cuota activo.")
                return
            usuarios_usando = self.db_manager.contar_usuarios_por_tipo_cuota(tipo.id)
            if usuarios_usando > 0 and not nuevo_estado:
                QMessageBox.warning(self, "No permitido", f"No puede desactivar este tipo de cuota porque {usuarios_usando} usuario(s) lo están usando.")
                return
            tipo.activo = nuevo_estado
            success = self.db_manager.actualizar_tipo_cuota(tipo)
            if success:
                self.load_tipos_cuota()
                self.tipos_cuota_modificados.emit()
            else:
                QMessageBox.warning(self, "Error", "No se pudo actualizar el estado del tipo de cuota.")
        except Exception as e:
            logging.error(f"Error al togglear estado: {e}")
            QMessageBox.critical(self, "Error", f"Ocurrió un error: {e}")
    
    def delete_tipo(self):
        """Elimina el tipo de cuota seleccionado"""
        if not self.current_tipo_cuota:
            return
        
        # Validar antes de eliminar
        if not self.validate_before_delete(self.current_tipo_cuota):
            return
        
        reply = QMessageBox.question(
            self, "Confirmar Eliminación",
            f"¿Está seguro de que desea eliminar el tipo de cuota '{self.current_tipo_cuota.nombre}'?\n\n"
            "Esta acción no se puede deshacer. Si hay usuarios usando este tipo de cuota, "
            "la eliminación será rechazada.",
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
            QMessageBox.StandardButton.No
        )
        
        if reply == QMessageBox.StandardButton.Yes:
            try:
                success = self.db_manager.eliminar_tipo_cuota(self.current_tipo_cuota.id)
                if success:
                    QMessageBox.information(self, "Éxito", "Tipo de cuota eliminado correctamente.")
                    self.load_tipos_cuota()
                    self.clear_form()
                    self.enable_form_editing(False)
                    self.tipos_cuota_modificados.emit()
                else:
                    QMessageBox.warning(
                        self, "No se puede eliminar",
                        "No se puede eliminar este tipo de cuota. Verifique que no esté siendo usado por usuarios."
                    )
            except Exception as e:
                logging.error(f"Error eliminando tipo de cuota: {e}")
                QMessageBox.critical(self, "Error", f"Error al eliminar tipo de cuota: {e}")
    
    def save_tipo(self):
        """Guarda el tipo de cuota actual"""
        if not self.validate_form():
            return
        
        try:
            # Crear o actualizar tipo de cuota
            if self.current_tipo_cuota:
                # Actualizar existente
                self.current_tipo_cuota.nombre = self.nombre_edit.text().strip()
                self.current_tipo_cuota.precio = self.precio_spinbox.value()
                self.current_tipo_cuota.duracion_dias = self.duracion_spinbox.value()
                self.current_tipo_cuota.descripcion = self.descripcion_edit.toPlainText().strip()
                self.current_tipo_cuota.icono_path = self.selected_icon
                self.current_tipo_cuota.activo = self.activo_checkbox.isChecked()
                
                success = self.db_manager.actualizar_tipo_cuota(self.current_tipo_cuota)
                message = "Tipo de cuota actualizado correctamente."
            else:
                # Crear nuevo
                nuevo_tipo = TipoCuota(
                    id=None,
                    nombre=self.nombre_edit.text().strip(),
                    precio=self.precio_spinbox.value(),
                    duracion_dias=self.duracion_spinbox.value(),
                    icono_path=self.selected_icon,
                    activo=self.activo_checkbox.isChecked(),
                    descripcion=self.descripcion_edit.toPlainText().strip()
                )
                
                tipo_id = self.db_manager.crear_tipo_cuota(nuevo_tipo)
                success = tipo_id > 0
                message = "Tipo de cuota creado correctamente."
            
            if success:
                QMessageBox.information(self, "Éxito", message)
                self.load_tipos_cuota()
                self.enable_form_editing(False)
                self.tipos_cuota_modificados.emit()
            else:
                QMessageBox.warning(self, "Error", "No se pudo guardar el tipo de cuota.")
                
        except Exception as e:
            logging.error(f"Error guardando tipo de cuota: {e}")
            QMessageBox.critical(self, "Error", f"Error al guardar tipo de cuota: {e}")
    
    def cancel_edit(self):
        """Cancela/limpia y prepara formulario para crear nuevo"""
        self.clear_form()
        # Permitir creación de nuevo tipo tras limpiar
        self.enable_form_editing(True)
        self.current_tipo_cuota = None
    
    def validate_form(self) -> bool:
        """Valida el formulario con validaciones completas del sistema dinámico"""
        nombre = self.nombre_edit.text().strip()
        
        # Validación de nombre obligatorio
        if not nombre:
            QMessageBox.warning(self, "Validación", "El nombre del tipo de cuota es obligatorio.")
            self.nombre_edit.setFocus()
            return False
        
        # Validación de longitud mínima
        if len(nombre) < 2:
            QMessageBox.warning(self, "Validación", "El nombre debe tener al menos 2 caracteres.")
            self.nombre_edit.setFocus()
            return False
        
        # Validación de longitud máxima
        if len(nombre) > 50:
            QMessageBox.warning(self, "Validación", "El nombre no puede exceder 50 caracteres.")
            self.nombre_edit.setFocus()
            return False
        
        # Validación de caracteres permitidos
        import re
        if not re.match(r'^[a-zA-ZáéíóúÁÉÍÓÚñÑ0-9\s\-_]+$', nombre):
            QMessageBox.warning(self, "Validación", "El nombre solo puede contener letras, números, espacios, guiones y guiones bajos.")
            self.nombre_edit.setFocus()
            return False
        
        # Validación de precio
        precio = self.precio_spinbox.value()
        if precio <= 0:
            QMessageBox.warning(self, "Validación", "El precio debe ser mayor a 0.")
            self.precio_spinbox.setFocus()
            return False
        
        # Validación de precio máximo razonable
        if precio > 999999.99:
            QMessageBox.warning(self, "Validación", "El precio no puede exceder $999,999.99.")
            self.precio_spinbox.setFocus()
            return False
        
        # Validación de descripción
        descripcion = self.descripcion_edit.toPlainText().strip()
        if len(descripcion) > 500:
            QMessageBox.warning(self, "Validación", "La descripción no puede exceder 500 caracteres.")
            self.descripcion_edit.setFocus()
            return False
        
        # Verificar nombre único (solo para nuevos o si cambió el nombre)
        if not self.current_tipo_cuota or self.current_tipo_cuota.nombre != nombre:
            existing = self.db_manager.obtener_tipo_cuota_por_nombre(nombre)
            if existing:
                QMessageBox.warning(self, "Validación", f"Ya existe un tipo de cuota con el nombre '{nombre}'.")
                self.nombre_edit.setFocus()
                return False
        
        # Validación de icono
        if not self.selected_icon:
            QMessageBox.warning(self, "Validación", "Debe seleccionar un icono para el tipo de cuota.")
            return False
        
        # Validación de integridad del sistema
        if not self.validate_system_integrity():
            return False
        
        return True
    
    def clear_form(self):
        """Limpia el formulario"""
        self.nombre_edit.clear()
        self.precio_spinbox.setValue(0.0)
        self.duracion_spinbox.setValue(30)  # Valor por defecto
        self.descripcion_edit.clear()
        self.activo_checkbox.setChecked(True)
        self.selected_icon = "💰"
        self.update_icon_preview()
        self.current_tipo_cuota = None
    
    def enable_form_editing(self, enabled: bool):
        """Habilita o deshabilita la edición del formulario"""
        self.nombre_edit.setEnabled(enabled)
        self.precio_spinbox.setEnabled(enabled)
        self.duracion_spinbox.setEnabled(enabled)
        self.descripcion_edit.setEnabled(enabled)
        self.icon_button.setEnabled(enabled)
        self.activo_checkbox.setEnabled(enabled)
        
        self.save_button.setEnabled(enabled)
        self.cancel_button.setEnabled(enabled)
    
    def on_form_changed(self):
        """Maneja cambios en el formulario"""
        # Habilitar botones si hay cambios
        pass
    
    def open_icon_selector(self):
        """Abre el selector de iconos"""
        dialog = QDialog(self)
        dialog.setWindowTitle("Seleccionar Icono")
        dialog.setModal(True)
        dialog.resize(500, 600)
        
        layout = QVBoxLayout(dialog)
        
        # Widget selector de iconos
        icon_selector = IconSelectorWidget(self, self.selected_icon)
        layout.addWidget(icon_selector)
        
        # Botones
        buttons_layout = QHBoxLayout()
        
        accept_button = QPushButton("Aceptar")
        accept_button.setObjectName("primary_button")
        accept_button.clicked.connect(dialog.accept)
        
        cancel_button = QPushButton("Cancelar")
        cancel_button.setObjectName("secondary_button")
        cancel_button.clicked.connect(dialog.reject)
        
        buttons_layout.addStretch()
        buttons_layout.addWidget(cancel_button)
        buttons_layout.addWidget(accept_button)
        
        layout.addLayout(buttons_layout)
        
        # Conectar señal de selección
        selected_icon = self.selected_icon
        def on_icon_selected(icon):
            nonlocal selected_icon
            selected_icon = icon
        
        icon_selector.icon_selected.connect(on_icon_selected)
        
        if dialog.exec() == QDialog.DialogCode.Accepted:
            self.selected_icon = selected_icon
            self.update_icon_preview()
    
    def update_icon_preview(self):
        """Actualiza la vista previa del icono seleccionado"""
        if self.selected_icon:
            # Si es un emoji o texto corto, mostrarlo directamente
            if len(self.selected_icon) <= 4 or not self.selected_icon.startswith(('/', 'icons/', 'images/')):
                self.icon_preview.setText(self.selected_icon)
                self.icon_preview.setPixmap(QPixmap())  # Limpiar pixmap
                self.icon_preview.setStyleSheet(
                    "border: 1px solid gray; border-radius: 4px; font-size: 20px; padding: 2px;"
                )
            else:
                # Si es una ruta de archivo, cargar la imagen
                try:
                    # Intentar cargar desde diferentes ubicaciones
                    possible_paths = [
                        self.selected_icon,
                        resource_path(self.selected_icon),
                        os.path.join('icons', os.path.basename(self.selected_icon)),
                        os.path.join('resources', 'icons', os.path.basename(self.selected_icon))
                    ]
                    
                    pixmap_loaded = False
                    for path in possible_paths:
                        try:
                            pixmap = QPixmap(path)
                            if not pixmap.isNull():
                                scaled_pixmap = pixmap.scaled(
                                    28, 28, Qt.AspectRatioMode.KeepAspectRatio, 
                                    Qt.TransformationMode.SmoothTransformation
                                )
                                self.icon_preview.setPixmap(scaled_pixmap)
                                self.icon_preview.setText("")  # Limpiar texto
                                pixmap_loaded = True
                                break
                        except Exception:
                            continue
                    
                    if not pixmap_loaded:
                        # Si no se pudo cargar la imagen, mostrar emoji por defecto
                        self.icon_preview.setText("💰")
                        self.icon_preview.setPixmap(QPixmap())  # Limpiar pixmap
                        
                except Exception as e:
                    logging.warning(f"Error cargando icono {self.selected_icon}: {e}")
                    self.icon_preview.setText("💰")
                    self.icon_preview.setPixmap(QPixmap())  # Limpiar pixmap
                
                self.icon_preview.setStyleSheet(
                    "border: 1px solid gray; border-radius: 4px; padding: 2px;"
                )
    
    def validate_system_integrity(self) -> bool:
        """Valida la integridad del sistema dinámico de tipos de cuota"""
        try:
            # Obtener todos los tipos de cuota
            tipos_cuota = self.db_manager.obtener_tipos_cuota()
            # Verificar que al menos un tipo de cuota esté activo
            tipos_activos = [tipo for tipo in tipos_cuota if tipo.activo]
            
            # Si estamos desactivando el único tipo activo, advertir
            if self.current_tipo_cuota and self.current_tipo_cuota.activo and not self.activo_checkbox.isChecked():
                if len(tipos_activos) == 1 and tipos_activos[0].id == self.current_tipo_cuota.id:
                    reply = QMessageBox.question(
                        self, "Advertencia",
                        "Está desactivando el único tipo de cuota activo. Esto puede causar problemas en el sistema.\n\n¿Está seguro de continuar?",
                        QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
                        QMessageBox.StandardButton.No
                    )
                    if reply == QMessageBox.StandardButton.No:
                        return False
            
            # Verificar que no se eliminen tipos de cuota en uso
            if self.current_tipo_cuota:
                usuarios_usando = self.db_manager.contar_usuarios_por_tipo_cuota(self.current_tipo_cuota.id)
                if usuarios_usando > 0 and not self.activo_checkbox.isChecked():
                    QMessageBox.warning(
                        self, "Validación",
                        f"No se puede desactivar este tipo de cuota porque {usuarios_usando} usuario(s) lo están usando.\n\nPrimero debe cambiar el tipo de cuota de estos usuarios."
                    )
                    return False
            
            return True
            
        except Exception as e:
            logging.error(f"Error validando integridad del sistema: {e}")
            QMessageBox.warning(self, "Error", "Error al validar la integridad del sistema.")
            return False
    
    def validate_before_delete(self, tipo_cuota) -> bool:
        """Valida antes de eliminar un tipo de cuota"""
        try:
            # Obtener todos los tipos de cuota
            todos_tipos = self.db_manager.obtener_tipos_cuota()
            # Verificar que no sea el único tipo activo
            tipos_activos = [tipo for tipo in todos_tipos if tipo.activo and tipo.id != tipo_cuota.id]
            if not tipos_activos and tipo_cuota.activo:
                QMessageBox.warning(
                    self, "No se puede eliminar",
                    "No se puede eliminar el único tipo de cuota activo. Debe crear otro tipo activo primero."
                )
                return False
            
            # Verificar que no haya usuarios usando este tipo
            usuarios_usando = self.db_manager.contar_usuarios_por_tipo_cuota(tipo_cuota.id)
            if usuarios_usando > 0:
                QMessageBox.warning(
                    self, "No se puede eliminar",
                    f"No se puede eliminar este tipo de cuota porque {usuarios_usando} usuario(s) lo están usando.\n\nPrimero debe cambiar el tipo de cuota de estos usuarios."
                )
                return False
            
            return True
            
        except Exception as e:
            logging.error(f"Error validando eliminación: {e}")
            QMessageBox.warning(self, "Error", "Error al validar la eliminación.")
            return False
    
    def show_table_context_menu(self, position):
        """Muestra el menú contextual de la tabla"""
        if self.tipos_table.itemAt(position) is None:
            return
        
        menu = QMenu(self)
        menu.setObjectName("contextMenu")
        menu.setProperty("menuType", "quota_types")
        
        # Acciones del menú
        edit_action = menu.addAction("✏️ Editar Tipo")
        duplicate_action = menu.addAction("📋 Duplicar Tipo")
        toggle_action = menu.addAction("🔄 Activar/Desactivar Tipo")
        menu.addSeparator()
        delete_action = menu.addAction("🗑️ Eliminar Tipo")
        
        # Ejecutar menú y manejar acción seleccionada
        action = menu.exec(self.tipos_table.mapToGlobal(position))
        
        if action == edit_action:
            self.edit_tipo()
        elif action == duplicate_action:
            self.duplicate_tipo()
        elif action == toggle_action:
            self.toggle_tipo_activo()
        elif action == delete_action:
            self.delete_tipo()
    
    def refresh_data(self):
        """Refresca los datos mostrados"""
        self.load_tipos_cuota()

