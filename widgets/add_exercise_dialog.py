from PyQt6.QtWidgets import (
    QDialog, QVBoxLayout, QHBoxLayout, QLabel, QLineEdit, QTableWidget, QTableWidgetItem,
    QPushButton, QDialogButtonBox, QSpinBox, QMessageBox, QGroupBox, QFormLayout,
    QAbstractItemView, QHeaderView, QMenu, QComboBox
)
from PyQt6.QtCore import Qt
from PyQt6.QtGui import QAction
from typing import Optional, Dict, List
from database import DatabaseManager
from models import Ejercicio, RutinaEjercicio
from widgets.exercise_bank_dialog import ExerciseBankDialog

class AddExerciseDialog(QDialog):
    def __init__(self, parent, db_manager: DatabaseManager):
        super().__init__(parent)
        self.db_manager = db_manager
        self.all_exercises: List[Ejercicio] = []
        self.selected_exercise: Optional[Ejercicio] = None
        self.new_rutina_ejercicio: Optional[RutinaEjercicio] = None

        self.setWindowTitle("Añadir/Gestionar Ejercicio")
        self.setMinimumSize(600, 520)
        # Permitir redimensionar desde las esquinas, sin mostrar size grip
        self.setSizeGripEnabled(False)
        try:
            self.setWindowFlag(Qt.WindowType.WindowMinimizeButtonHint, True)
        except AttributeError:
            self.setWindowFlag(Qt.WindowMinimizeButtonHint, True)

        main_layout = QVBoxLayout(self)
        
        search_group = QGroupBox("1. Buscar Ejercicio Existente")
        search_layout = QVBoxLayout(search_group)
        
        # Filtros avanzados
        filters_layout = QHBoxLayout()
        self.search_input = QLineEdit()
        self.search_input.setPlaceholderText("Buscar por nombre...")
        
        self.grupo_filter = QComboBox()
        self.grupo_filter.addItem("Todos los grupos")
        
        self.objetivo_filter = QComboBox()
        self.objetivo_filter.addItem("Todos los objetivos")
        objetivos = ["general", "fuerza", "cardio", "rehabilitación", "flexibilidad", "resistencia", "coordinación", "equilibrio"]
        self.objetivo_filter.addItems(objetivos)
        
        filters_layout.addWidget(QLabel("Buscar:"))
        filters_layout.addWidget(self.search_input, 2)
        filters_layout.addWidget(QLabel("Grupo:"))
        filters_layout.addWidget(self.grupo_filter, 1)
        filters_layout.addWidget(QLabel("Objetivo:"))
        filters_layout.addWidget(self.objetivo_filter, 1)
        
        self.exercise_table = QTableWidget()
        self.exercise_table.setColumnCount(3)
        self.exercise_table.setHorizontalHeaderLabels(["Nombre", "Grupo Muscular", "Objetivo"])
        self.exercise_table.horizontalHeader().setSectionResizeMode(0, QHeaderView.ResizeMode.Stretch)
        self.exercise_table.setSelectionBehavior(QAbstractItemView.SelectionBehavior.SelectRows)
        self.exercise_table.setEditTriggers(QAbstractItemView.EditTrigger.NoEditTriggers)
        # Habilitamos el menú contextual
        self.exercise_table.setContextMenuPolicy(Qt.ContextMenuPolicy.CustomContextMenu)

        # Botones de gestión del banco
        management_layout = QHBoxLayout()
        self.add_new_exercise_button = QPushButton("➕ Añadir Nuevo al Banco")
        self.edit_exercise_button = QPushButton("✏️ Editar Seleccionado")
        self.delete_exercise_button = QPushButton("🗑️ Eliminar Seleccionado")
        management_layout.addWidget(self.add_new_exercise_button)
        management_layout.addWidget(self.edit_exercise_button)
        management_layout.addWidget(self.delete_exercise_button)

        search_layout.addLayout(filters_layout)
        search_layout.addWidget(self.exercise_table)
        search_layout.addLayout(management_layout)
        
        config_group = QGroupBox("2. Configurar Ejercicio para esta Rutina")
        config_layout = QFormLayout(config_group)
        self.series_spinbox = QSpinBox(minimum=1, maximum=20)
        self.reps_input = QLineEdit()
        self.reps_input.setPlaceholderText("Ej: 12, 10-12, al fallo...")
        config_layout.addRow("Series:", self.series_spinbox)
        config_layout.addRow("Repeticiones:", self.reps_input)

        self.button_box = QDialogButtonBox(QDialogButtonBox.StandardButton.Ok | QDialogButtonBox.StandardButton.Cancel)
        self.button_box.button(QDialogButtonBox.StandardButton.Ok).setText("Añadir a la Rutina")
        main_layout.addWidget(search_group)
        main_layout.addWidget(config_group)
        main_layout.addWidget(self.button_box)

        self.connect_signals()
        self.load_all_exercises()

    def connect_signals(self):
        self.search_input.textChanged.connect(self.filter_exercises)
        self.grupo_filter.currentTextChanged.connect(self.filter_exercises)
        self.objetivo_filter.currentTextChanged.connect(self.filter_exercises)
        self.exercise_table.itemClicked.connect(self.on_exercise_selected)
        self.exercise_table.customContextMenuRequested.connect(self.show_context_menu)
        # Conexiones para los nuevos botones
        self.add_new_exercise_button.clicked.connect(self.add_exercise_to_bank)
        self.edit_exercise_button.clicked.connect(self.edit_exercise_in_bank)
        self.delete_exercise_button.clicked.connect(self.delete_exercise_from_bank)
        
        self.button_box.accepted.connect(self.accept_dialog)
        self.button_box.rejected.connect(self.reject)

    def load_all_exercises(self):
        current_selection_id = (self.selected_exercise.get('id') if isinstance(self.selected_exercise, dict) else self.selected_exercise.id) if self.selected_exercise else None
        self.exercise_table.setRowCount(0)
        row_to_select = -1
        try:
            self.all_exercises = self.db_manager.obtener_ejercicios()
            
            # Cargar grupos únicos para el filtro
            grupos_unicos = set()
            for ej in self.all_exercises:
                grupo = ej.get('grupo_muscular') if isinstance(ej, dict) else getattr(ej, 'grupo_muscular', None)
                if grupo:
                    grupos_unicos.add(grupo)
            
            self.grupo_filter.blockSignals(True)
            current_grupo = self.grupo_filter.currentText()
            self.grupo_filter.clear()
            self.grupo_filter.addItem("Todos los grupos")
            for grupo in sorted(grupos_unicos):
                self.grupo_filter.addItem(grupo)
            
            # Restaurar selección si existe
            if current_grupo in [self.grupo_filter.itemText(i) for i in range(self.grupo_filter.count())]:
                self.grupo_filter.setCurrentText(current_grupo)
            self.grupo_filter.blockSignals(False)
            
            self.exercise_table.setRowCount(len(self.all_exercises))
            for row, ej in enumerate(self.all_exercises):
                nombre = ej['nombre'] if isinstance(ej, dict) else getattr(ej, 'nombre', '')
                grupo = ej.get('grupo_muscular', 'N/A') if isinstance(ej, dict) else (getattr(ej, 'grupo_muscular', None) or 'N/A')
                objetivo = ej.get('objetivo', 'general') if isinstance(ej, dict) else (getattr(ej, 'objetivo', None) or 'general')
                ej_id = ej.get('id') if isinstance(ej, dict) else getattr(ej, 'id', None)

                nombre_item = QTableWidgetItem(nombre)
                nombre_item.setData(Qt.ItemDataRole.UserRole, ej)
                self.exercise_table.setItem(row, 0, nombre_item)
                self.exercise_table.setItem(row, 1, QTableWidgetItem(grupo))
                self.exercise_table.setItem(row, 2, QTableWidgetItem(objetivo))
                if ej_id == current_selection_id:
                    row_to_select = row
            
            if row_to_select != -1:
                self.exercise_table.selectRow(row_to_select)
            
            self.filter_exercises()  # Aplicar filtros después de cargar

        except Exception as e:
            QMessageBox.critical(self, "Error", f"No se pudieron cargar los ejercicios: {e}")

    def filter_exercises(self):
        """Filtra ejercicios usando el método mejorado de la base de datos."""
        search_text = self.search_input.text().strip()
        selected_grupo = self.grupo_filter.currentText()
        selected_objetivo = self.objetivo_filter.currentText()
        
        # Preparar parámetros para el filtro
        grupo_param = selected_grupo if selected_grupo != "Todos los grupos" else ""
        objetivo_param = selected_objetivo if selected_objetivo != "Todos los objetivos" else ""
        
        try:
            # Usar el método mejorado de obtener_ejercicios con filtros
            filtered_exercises = self.db_manager.obtener_ejercicios(
                filtro=search_text,
                objetivo=objetivo_param,
                grupo_muscular=grupo_param
            )
            
            # Actualizar la tabla con los ejercicios filtrados
            current_selection_id = (self.selected_exercise.get('id') if isinstance(self.selected_exercise, dict) else self.selected_exercise.id) if self.selected_exercise else None
            self.exercise_table.setRowCount(len(filtered_exercises))
            row_to_select = -1
            
            for row, ej in enumerate(filtered_exercises):
                nombre = ej['nombre'] if isinstance(ej, dict) else getattr(ej, 'nombre', '')
                grupo = ej.get('grupo_muscular', 'N/A') if isinstance(ej, dict) else (getattr(ej, 'grupo_muscular', None) or 'N/A')
                objetivo = ej.get('objetivo', 'general') if isinstance(ej, dict) else (getattr(ej, 'objetivo', None) or 'general')
                ej_id = ej.get('id') if isinstance(ej, dict) else getattr(ej, 'id', None)

                nombre_item = QTableWidgetItem(nombre)
                nombre_item.setData(Qt.ItemDataRole.UserRole, ej)
                self.exercise_table.setItem(row, 0, nombre_item)
                self.exercise_table.setItem(row, 1, QTableWidgetItem(grupo))
                self.exercise_table.setItem(row, 2, QTableWidgetItem(objetivo))
                
                if ej_id == current_selection_id:
                    row_to_select = row
            
            # Restaurar selección si existe
            if row_to_select != -1:
                self.exercise_table.selectRow(row_to_select)
                
        except Exception as e:
            QMessageBox.critical(self, "Error", f"Error al filtrar ejercicios: {e}")

    def on_exercise_selected(self, item):
        self.selected_exercise = self.exercise_table.item(item.row(), 0).data(Qt.ItemDataRole.UserRole)

    def add_exercise_to_bank(self):
        dialog = ExerciseBankDialog(self)
        if dialog.exec():
            new_exercise = dialog.get_ejercicio()
            try:
                self.db_manager.crear_ejercicio(new_exercise)
                self.load_all_exercises() # Recargamos la lista
            except Exception as e:
                QMessageBox.critical(self, "Error", f"No se pudo crear el ejercicio (¿quizás el nombre ya existe?): {e}")

    def edit_exercise_in_bank(self):
        if not self.selected_exercise:
            QMessageBox.warning(self, "Sin selección", "Seleccione un ejercicio de la tabla para editar.")
            return
        
        dialog = ExerciseBankDialog(self, ejercicio=self.selected_exercise)
        if dialog.exec():
            updated_exercise = dialog.get_ejercicio()
            try:
                self.db_manager.actualizar_ejercicio(updated_exercise)
                self.load_all_exercises()
            except Exception as e:
                QMessageBox.critical(self, "Error", f"No se pudo actualizar el ejercicio: {e}")

    def delete_exercise_from_bank(self):
        if not self.selected_exercise:
            QMessageBox.warning(self, "Sin selección", "Seleccione un ejercicio de la tabla para eliminar.")
            return

        # Obtener el nombre del ejercicio según el tipo (dict o objeto)
        nombre_ejercicio = self.selected_exercise.get('nombre') if isinstance(self.selected_exercise, dict) else self.selected_exercise.nombre
        ejercicio_id = self.selected_exercise.get('id') if isinstance(self.selected_exercise, dict) else self.selected_exercise.id

        reply = QMessageBox.question(self, "Confirmar Eliminación", 
                                     f"¿Está seguro de que desea eliminar '{nombre_ejercicio}' del banco de ejercicios?\n"
                                     "Esta acción también lo eliminará de TODAS las rutinas guardadas.",
                                     QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
                                     QMessageBox.StandardButton.No)
        
        if reply == QMessageBox.StandardButton.Yes:
            try:
                self.db_manager.eliminar_ejercicio(ejercicio_id)
                self.selected_exercise = None # Limpiamos la selección
                self.load_all_exercises()
            except Exception as e:
                QMessageBox.critical(self, "Error", f"No se pudo eliminar el ejercicio: {e}")

    def show_context_menu(self, pos):
        if not self.exercise_table.indexAt(pos).isValid(): return
        self.on_exercise_selected(self.exercise_table.itemAt(pos))
        
        menu = QMenu(self)
        menu.setObjectName("contextMenu")
        menu.setProperty("menuType", "exercise_bank")
        edit_action = QAction("✏️ Editar Ejercicio...", self)
        delete_action = QAction("🗑️ Eliminar Ejercicio...", self)
        
        edit_action.triggered.connect(self.edit_exercise_in_bank)
        delete_action.triggered.connect(self.delete_exercise_from_bank)
        
        menu.addAction(edit_action)
        menu.addAction(delete_action)
        menu.exec(self.exercise_table.viewport().mapToGlobal(pos))

    def accept_dialog(self):
        if not self.selected_exercise:
            QMessageBox.warning(self, "Selección Requerida", "Por favor, seleccione un ejercicio de la tabla.")
            return

        # Obtener el ID del ejercicio según el tipo (dict o objeto)
        ejercicio_id = self.selected_exercise.get('id') if isinstance(self.selected_exercise, dict) else self.selected_exercise.id

        self.new_rutina_ejercicio = RutinaEjercicio(
            ejercicio_id=ejercicio_id,
            series=self.series_spinbox.value(),
            repeticiones=self.reps_input.text().strip(),
            ejercicio=self.selected_exercise
        )
        self.accept()

    def get_rutina_ejercicio(self) -> Optional[RutinaEjercicio]:
        return self.new_rutina_ejercicio

