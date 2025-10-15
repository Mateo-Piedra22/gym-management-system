import logging
import re
from typing import Optional, Dict
from PyQt6.QtWidgets import (
    QDialog, QVBoxLayout, QHBoxLayout, QGroupBox, QFormLayout,
    QLineEdit, QSpinBox, QPushButton, QTableWidget, QTableWidgetItem,
    QHeaderView, QLabel, QMessageBox, QTabWidget, QDialogButtonBox,
    QAbstractItemView, QMenu, QWidget, QComboBox
)
from PyQt6.QtCore import Qt
from PyQt6.QtGui import QAction
from database import DatabaseManager
from models import Rutina, RutinaEjercicio
from widgets.add_exercise_dialog import AddExerciseDialog

class TemplateEditorDialog(QDialog):
    # --- CONSTRUCTOR CORREGIDO ---
    def __init__(self, parent, db_manager: DatabaseManager, rutina: Optional[Rutina] = None):
        super().__init__(parent)
        self.db_manager = db_manager
        self.is_new = rutina is None
        # Evitar nombre por defecto que pueda causar duplicados; usar placeholder
        self.current_routine = rutina if rutina else Rutina(nombre_rutina="", dias_semana=3)

        self.setWindowTitle("Editar Plantilla" if not self.is_new else "Crear Nueva Plantilla")
        self.setMinimumSize(880, 660)
        # Permitir redimensionar desde las esquinas, sin mostrar size grip
        self.setSizeGripEnabled(False)
        try:
            self.setWindowFlag(Qt.WindowType.WindowMinimizeButtonHint, True)
        except AttributeError:
            self.setWindowFlag(Qt.WindowMinimizeButtonHint, True)

        self.exercises_by_day: Dict[int, list[RutinaEjercicio]] = {}
        if not self.is_new:
            self.load_exercises_from_routine()

        self.setup_ui()

    def setup_ui(self):
        main_layout = QVBoxLayout(self)

        # Panel de detalles
        details_group = QGroupBox("Detalles de la Plantilla")
        details_layout = QFormLayout(details_group)
        self.routine_name_input = QLineEdit(self.current_routine.nombre_rutina)
        self.routine_name_input.setPlaceholderText("Nueva Plantilla")
        self.routine_days_spinbox = QSpinBox(minimum=1, maximum=7)
        self.routine_days_spinbox.setValue(self.current_routine.dias_semana)
        
        # Campo de categoría
        self.category_combo = QComboBox()
        self.category_combo.addItems(["general", "fuerza", "cardio", "rehabilitación", "flexibilidad", "resistencia", "coordinación", "equilibrio"])
        self.category_combo.setCurrentText(self.current_routine.categoria)
        
        details_layout.addRow("Nombre de la Plantilla:", self.routine_name_input)
        details_layout.addRow("Categoría:", self.category_combo)
        details_layout.addRow("Dividir en (días):", self.routine_days_spinbox)

        # Panel de ejercicios (pestañas por día)
        self.routine_display_tabs = QTabWidget()
        
        # Botones de Aceptar/Cancelar
        self.button_box = QDialogButtonBox(QDialogButtonBox.StandardButton.Save | QDialogButtonBox.StandardButton.Cancel)

        main_layout.addWidget(details_group)
        main_layout.addWidget(self.routine_display_tabs)
        main_layout.addWidget(self.button_box)

        self.connect_signals()
        self.update_day_tabs()

    def connect_signals(self):
        self.routine_days_spinbox.valueChanged.connect(self.update_day_tabs)
        self.button_box.accepted.connect(self.save_and_accept)
        self.button_box.rejected.connect(self.reject)
    
    def load_exercises_from_routine(self):
        """Carga los ejercicios si estamos editando una rutina existente."""
        full_routine = self.db_manager.obtener_rutina_completa(self.current_routine.id)
        if full_routine:
            for ej in full_routine.ejercicios:
                if ej.dia_semana not in self.exercises_by_day:
                    self.exercises_by_day[ej.dia_semana] = []
                self.exercises_by_day[ej.dia_semana].append(ej)

    def update_day_tabs(self):
        num_days = self.routine_days_spinbox.value()
        self.routine_display_tabs.clear()
        for day in range(1, num_days + 1):
            day_widget = QWidget()
            day_layout = QVBoxLayout(day_widget)
            table = QTableWidget()
            table.setSelectionBehavior(QAbstractItemView.SelectionBehavior.SelectRows)
            table.setColumnCount(4)
            table.setHorizontalHeaderLabels(["Ejercicio", "Grupo Muscular", "Series", "Repeticiones"])
            table.horizontalHeader().setSectionResizeMode(0, QHeaderView.ResizeMode.Stretch)
            table.setContextMenuPolicy(Qt.ContextMenuPolicy.CustomContextMenu)
            table.customContextMenuRequested.connect(lambda pos, t=table: self.show_exercise_context_menu(pos, t))
            self.populate_exercise_table(table, day)
            
            buttons_layout = QHBoxLayout()
            add_button = QPushButton("➕ Añadir Ejercicio a este día")
            add_button.clicked.connect(lambda _, d=day, t=table: self.add_exercise_to_day(d, t))
            buttons_layout.addWidget(add_button)
            buttons_layout.addStretch()
            day_layout.addLayout(buttons_layout)
            day_layout.addWidget(table)
            self.routine_display_tabs.addTab(day_widget, f"Día {day}")

    def populate_exercise_table(self, table: QTableWidget, day: int):
        exercises = self.exercises_by_day.get(day, [])
        table.setRowCount(len(exercises))
        for row, ex in enumerate(exercises):
            # Obtener el nombre del ejercicio según el tipo (dict o objeto)
            ejercicio_nombre = ex.ejercicio.get('nombre') if isinstance(ex.ejercicio, dict) else ex.ejercicio.nombre
            ejercicio_grupo = ex.ejercicio.get('grupo_muscular') if isinstance(ex.ejercicio, dict) else ex.ejercicio.grupo_muscular
            
            table.setItem(row, 0, QTableWidgetItem(ejercicio_nombre))
            table.setItem(row, 1, QTableWidgetItem(ejercicio_grupo or "N/A"))
            table.setItem(row, 2, QTableWidgetItem(str(ex.series)))
            table.setItem(row, 3, QTableWidgetItem(ex.repeticiones))
            table.item(row, 0).setData(Qt.ItemDataRole.UserRole, ex)

    def add_exercise_to_day(self, day: int, table: QTableWidget):
        dialog = AddExerciseDialog(self, self.db_manager)
        if dialog.exec():
            new_exercise = dialog.get_rutina_ejercicio()
            if new_exercise:
                if day not in self.exercises_by_day: self.exercises_by_day[day] = []
                new_exercise.dia_semana = day
                self.exercises_by_day[day].append(new_exercise)
                self.populate_exercise_table(table, day)
    
    def remove_exercise_from_day(self, table: QTableWidget):
        selected_items = table.selectedItems()
        if not selected_items: return
        day = self.routine_display_tabs.currentIndex() + 1
        exercise_to_remove: RutinaEjercicio = table.item(table.currentRow(), 0).data(Qt.ItemDataRole.UserRole)
        self.exercises_by_day[day].remove(exercise_to_remove)
        self.populate_exercise_table(table, day)

    def show_exercise_context_menu(self, pos, table: QTableWidget):
        if not table.indexAt(pos).isValid(): return
        menu = QMenu(self)
        menu.setObjectName("contextMenu")
        menu.setProperty("menuType", "template_exercise")
        remove_action = QAction("🗑️ Eliminar Ejercicio", self)
        remove_action.triggered.connect(lambda: self.remove_exercise_from_day(table))
        menu.addAction(remove_action)
        menu.exec(table.viewport().mapToGlobal(pos))
        
    def save_and_accept(self):
        # Datos actuales
        input_name = self.routine_name_input.text().strip()
        dias = self.routine_days_spinbox.value()
        categoria = self.category_combo.currentText()
        if not input_name:
            QMessageBox.warning(self, "Dato Requerido", "El nombre de la plantilla no puede estar vacío.")
            return
        # Normalizar y usar solo el nombre base. Días/categoría NO se agregan al texto guardado.
        try:
            # Normalizar espacios
            norm_name = re.sub(r"\s+", " ", input_name).strip()
            base = norm_name
            # Quitar sufijo numérico duplicado al final: " (n)"
            base = re.sub(r"\s*\(\d+\)\s*$", "", base, flags=re.IGNORECASE)
            # Quitar marcadores de días al final: "(Nd)" o "(N días)"
            base = re.sub(r"\s*\(\d+\s*d\)\s*$", "", base, flags=re.IGNORECASE)
            base = re.sub(r"\s*\(\d+\s*días\)\s*$", "", base, flags=re.IGNORECASE)
            # Quitar categoría al final: " - algo"
            base = re.sub(r"\s*-\s*[^-]+$", "", base)
            base = base.strip()

            # Detectar colisiones SOLO entre plantillas con mismo base y misma combinación de días/categoría
            with self.db_manager.get_connection_context() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("SELECT nombre_rutina, id, dias_semana, categoria FROM rutinas WHERE usuario_id IS NULL")
                    rows = cursor.fetchall() or []

            collisions = []
            for row in rows:
                try:
                    # Soporte para tuplas/dicts/objetos
                    n = row[0] if isinstance(row, tuple) else (row.get('nombre_rutina') if isinstance(row, dict) else getattr(row, 'nombre_rutina', None))
                    rid = row[1] if isinstance(row, tuple) else (row.get('id') if isinstance(row, dict) else getattr(row, 'id', None))
                    d_exist = row[2] if isinstance(row, tuple) else (row.get('dias_semana') if isinstance(row, dict) else getattr(row, 'dias_semana', None))
                    c_exist = row[3] if isinstance(row, tuple) else (row.get('categoria') if isinstance(row, dict) else getattr(row, 'categoria', None))
                except Exception:
                    n = getattr(row, 'nombre_rutina', None); rid = getattr(row, 'id', None)
                    d_exist = getattr(row, 'dias_semana', None); c_exist = getattr(row, 'categoria', None)
                if n is None:
                    continue
                # Ignorar la propia rutina si se edita
                if not self.is_new and rid == self.current_routine.id:
                    continue

                # Solo comparar si días y categoría coinciden
                try:
                    same_days = int(d_exist) == int(dias)
                except Exception:
                    same_days = False
                same_cat = str(c_exist).strip().lower() == str(categoria).strip().lower()
                if not (same_days and same_cat):
                    continue

                # Limpiar el nombre existente a su base para comparar
                n_str = str(n).strip()
                base_exist = n_str
                base_exist = re.sub(r"\s*\(\d+\)\s*$", "", base_exist, flags=re.IGNORECASE)  # quitar sufijo (n)
                base_exist = re.sub(r"\s*\(\d+\s*d\)\s*$", "", base_exist, flags=re.IGNORECASE)  # quitar (Nd)
                base_exist = re.sub(r"\s*\(\d+\s*días\)\s*$", "", base_exist, flags=re.IGNORECASE)  # quitar (N días)
                base_exist = re.sub(r"\s*-\s*[^-]+$", "", base_exist)  # quitar " - categoría"
                base_exist = base_exist.strip()

                if base_exist.lower() == base.lower():
                    # Intentar capturar sufijo existente (n) del nombre original
                    m = re.search(r"\s\((\d+)\)\s*$", n_str)
                    if m:
                        try:
                            collisions.append(int(m.group(1)))
                        except Exception:
                            collisions.append(1)
                    else:
                        collisions.append(1)

            final_name = base
            if collisions:
                next_num = max(collisions) + 1 if max(collisions) > 1 else 2
                final_name = f"{base} ({next_num})"
        except Exception:
            # Si algo falla, usar el nombre tal como está
            final_name = input_name

        # Asignar datos
        self.current_routine.nombre_rutina = final_name
        self.current_routine.dias_semana = dias
        self.current_routine.categoria = categoria
        # Asegurar que las plantillas no están asociadas a un usuario
        try:
            self.current_routine.usuario_id = None
        except Exception:
            pass
            
        try:
            # Si es nueva, la creamos
            if self.is_new:
                new_id = self.db_manager.crear_rutina(self.current_routine)
                self.current_routine.id = new_id
            # Si ya existe, la actualizamos
            else:
                self.db_manager.actualizar_rutina(self.current_routine)

            # Guardamos los ejercicios
            all_exercises = [ex for day_list in self.exercises_by_day.values() for ex in day_list]
            self.db_manager.guardar_ejercicios_de_rutina(self.current_routine.id, all_exercises)
            self.accept()
        except Exception as e:
            logging.error(f"Error guardando plantilla: {e}")
            QMessageBox.critical(self, "Error de Base de Datos", f"No se pudo guardar la plantilla: {e}")

