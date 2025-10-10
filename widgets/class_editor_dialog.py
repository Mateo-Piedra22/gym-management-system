import logging
import re
from PyQt6.QtWidgets import (
    QDialog, QLabel, QVBoxLayout, QHBoxLayout, QGroupBox, QFormLayout,
    QLineEdit, QTextEdit, QListWidget, QListWidgetItem, QPushButton,
    QDialogButtonBox, QMessageBox, QComboBox, QInputDialog, QFileDialog
)
from PyQt6.QtCore import Qt
from typing import Optional, Dict, List
import pandas as pd
from models import Clase, Ejercicio, EjercicioGrupo
from database import DatabaseManager
from export_manager import ExportManager

class ClassEditorDialog(QDialog):
    def __init__(self, parent, db_manager: DatabaseManager, export_manager: ExportManager, clase: Optional[Clase] = None):
        super().__init__(parent)
        self.db_manager = db_manager; self.export_manager = export_manager
        self.is_new = clase is None
        # Evitar nombre por defecto que cause duplicados; usar placeholder en el input
        self.clase = clase if clase else Clase(nombre="")
        if not self.is_new: self.clase.ejercicios = self.db_manager.obtener_ejercicios_de_clase(self.clase.id)
        
        self.setWindowTitle("Editar Tipo de Clase" if not self.is_new else "Crear Nuevo Tipo de Clase")
        self.setMinimumSize(770, 640)
        # Permitir redimensionar desde las esquinas, sin mostrar size grip
        self.setSizeGripEnabled(False)
        try:
            self.setWindowFlag(Qt.WindowType.WindowMinimizeButtonHint, True)
        except AttributeError:
            self.setWindowFlag(Qt.WindowMinimizeButtonHint, True)
        self.setup_ui()

    # Helpers para robustez con dicts u objetos Ejercicio
    def _ej_to_obj(self, ej) -> Ejercicio:
        if isinstance(ej, dict):
            try:
                return Ejercicio(**ej)
            except Exception:
                # Fallback mínimo por si faltan claves opcionales
                return Ejercicio(id=ej.get('id'), nombre=ej.get('nombre', ''), grupo_muscular=ej.get('grupo_muscular'), descripcion=ej.get('descripcion'), objetivo=ej.get('objetivo'))
        return ej

    def _ej_id(self, ej) -> int:
        return ej.get('id') if isinstance(ej, dict) else ej.id

    def _ej_nombre(self, ej) -> str:
        return ej.get('nombre') if isinstance(ej, dict) else ej.nombre

    def _ej_grupo(self, ej) -> Optional[str]:
        return ej.get('grupo_muscular') if isinstance(ej, dict) else ej.grupo_muscular

    def setup_ui(self):
        main_layout = QVBoxLayout(self)
        details_group = QGroupBox("Detalles de la Clase"); details_layout = QFormLayout(details_group)
        self.nombre_input = QLineEdit(self.clase.nombre); self.nombre_input.setPlaceholderText("Nueva Clase")
        self.descripcion_input = QTextEdit(self.clase.descripcion or "")
        details_layout.addRow("Nombre:", self.nombre_input); details_layout.addRow("Descripción:", self.descripcion_input)
        
        exercises_group = QGroupBox("Ejercicios Predefinidos para la Clase"); exercises_layout = QVBoxLayout(exercises_group)
        self.exercise_list = QListWidget()
        
        # --- NUEVOS CONTROLES PARA GRUPOS Y EXPORTACIÓN ---
        ex_management_layout = QHBoxLayout()
        self.add_exercise_button = QPushButton("➕ Añadir del Banco"); self.remove_exercise_button = QPushButton("🗑️ Quitar de la Lista")
        ex_management_layout.addWidget(self.add_exercise_button); ex_management_layout.addWidget(self.remove_exercise_button)

        ex_group_layout = QHBoxLayout(); group_label = QLabel("Cargar Grupo:"); self.group_combo = QComboBox(); self.save_as_group_button = QPushButton("💾 Guardar como Grupo"); self.delete_group_button = QPushButton("🗑️ Eliminar Grupo")
        ex_group_layout.addWidget(group_label); ex_group_layout.addWidget(self.group_combo); ex_group_layout.addWidget(self.save_as_group_button); ex_group_layout.addWidget(self.delete_group_button)

        ex_io_layout = QHBoxLayout(); self.import_button = QPushButton("📥 Importar"); self.export_button = QPushButton("📤 Exportar")
        ex_io_layout.addStretch(); ex_io_layout.addWidget(self.import_button); ex_io_layout.addWidget(self.export_button)

        exercises_layout.addLayout(ex_management_layout); exercises_layout.addWidget(self.exercise_list); exercises_layout.addLayout(ex_group_layout); exercises_layout.addLayout(ex_io_layout)
        
        self.button_box = QDialogButtonBox(QDialogButtonBox.StandardButton.Save | QDialogButtonBox.StandardButton.Cancel)
        main_layout.addWidget(details_group); main_layout.addWidget(exercises_group); main_layout.addWidget(self.button_box)
        
        self.connect_signals(); self.refresh_exercise_list(); self.load_exercise_groups()

    def connect_signals(self):
        self.add_exercise_button.clicked.connect(self.add_exercise); self.remove_exercise_button.clicked.connect(self.remove_exercise)
        self.group_combo.currentIndexChanged.connect(self.load_from_group)
        self.save_as_group_button.clicked.connect(self.save_as_group)
        self.delete_group_button.clicked.connect(self.delete_selected_group)
        self.import_button.clicked.connect(self.import_from_excel); self.export_button.clicked.connect(self.export_to_excel)
        self.button_box.accepted.connect(self.accept_dialog); self.button_box.rejected.connect(self.reject)

    def add_exercise(self):
        from widgets.add_exercise_dialog import AddExerciseDialog
        all_exercises = self.db_manager.obtener_ejercicios()
        current_ids = {self._ej_id(ej) for ej in self.clase.ejercicios}
        # En el diálogo de añadir, no filtramos, mostramos todo. El profesor puede añadir desde ahí.
        dialog = AddExerciseDialog(self, self.db_manager)
        if dialog.exec():
            rutina_ejercicio = dialog.get_rutina_ejercicio()
            if rutina_ejercicio:
                ejercicio_raw = rutina_ejercicio.ejercicio
                ejercicio_id = self._ej_id(ejercicio_raw)
                if ejercicio_id not in current_ids:
                    ejercicio = self._ej_to_obj(ejercicio_raw)
                    self.clase.ejercicios.append(ejercicio)
                    self.refresh_exercise_list()
    
    def remove_exercise(self):
        current_item = self.exercise_list.currentItem()
        if not current_item: QMessageBox.warning(self, "Sin selección", "Seleccione un ejercicio para quitar."); return
        exercise_to_remove = current_item.data(Qt.ItemDataRole.UserRole)
        exercise_to_remove_id = self._ej_id(exercise_to_remove)
        self.clase.ejercicios = [ej for ej in self.clase.ejercicios if self._ej_id(ej) != exercise_to_remove_id]
        self.refresh_exercise_list()
        
    def refresh_exercise_list(self):
        self.exercise_list.clear()
        # Normalizar todos a objetos para mantener consistencia
        self.clase.ejercicios = [self._ej_to_obj(ej) for ej in self.clase.ejercicios]
        self.clase.ejercicios.sort(key=lambda x: self._ej_nombre(x) or "")
        for ej in self.clase.ejercicios:
            nombre = self._ej_nombre(ej) or ""
            grupo = self._ej_grupo(ej) or 'N/A'
            item = QListWidgetItem(f"{nombre} ({grupo})")
            item.setData(Qt.ItemDataRole.UserRole, ej)
            self.exercise_list.addItem(item)
            
    def load_exercise_groups(self):
        self.group_combo.blockSignals(True); self.group_combo.clear(); self.group_combo.addItem("Seleccionar...", userData=None)
        try:
            grupos = self.db_manager.obtener_grupos_ejercicios()
            for g in grupos: self.group_combo.addItem(g.nombre, userData=g)
        except Exception as e: logging.error(f"Error cargando grupos: {e}")
        finally: self.group_combo.blockSignals(False)

    def load_from_group(self, index):
        if index <= 0: return
        grupo = self.group_combo.currentData()
        if grupo:
            ejercicios_del_grupo = self.db_manager.obtener_ejercicios_de_grupo(grupo.id)
            current_ids = {self._ej_id(ej) for ej in self.clase.ejercicios}
            for ej in ejercicios_del_grupo:
                if self._ej_id(ej) not in current_ids:
                    self.clase.ejercicios.append(self._ej_to_obj(ej))
            self.refresh_exercise_list()
        # Mantener el grupo seleccionado para permitir eliminación inmediata
        
    def save_as_group(self):
        if not self.clase.ejercicios: QMessageBox.warning(self, "Vacío", "No hay ejercicios en la lista para guardar como grupo."); return
        nombre_grupo, ok = QInputDialog.getText(self, "Guardar Grupo de Ejercicios", "Nombre para el nuevo grupo:")
        if ok and nombre_grupo:
            try:
                ejercicio_ids = [self._ej_id(ej) for ej in self.clase.ejercicios]
                self.db_manager.crear_grupo_ejercicios(nombre_grupo, ejercicio_ids)
                QMessageBox.information(self, "Éxito", f"Grupo '{nombre_grupo}' guardado correctamente.")
                self.load_exercise_groups()
            except Exception as e: QMessageBox.critical(self, "Error", f"No se pudo guardar el grupo (¿nombre duplicado?): {e}")

    def delete_selected_group(self):
        index = self.group_combo.currentIndex()
        grupo = self.group_combo.currentData()
        if index <= 0 or not grupo:
            QMessageBox.warning(self, "Sin selección", "Seleccione un grupo para eliminar desde el combo.")
            return
        reply = QMessageBox.question(self, "Confirmar eliminación",
                                     f"¿Eliminar el grupo '{grupo.nombre}'? Esta acción no se puede deshacer.",
                                     QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
                                     QMessageBox.StandardButton.No)
        if reply == QMessageBox.StandardButton.Yes:
            try:
                self.db_manager.eliminar_grupo_ejercicios(grupo.id)
                QMessageBox.information(self, "Grupo eliminado", f"El grupo '{grupo.nombre}' fue eliminado.")
                self.load_exercise_groups()
                self.group_combo.setCurrentIndex(0)
            except Exception as e:
                QMessageBox.critical(self, "Error", f"No se pudo eliminar el grupo: {e}")

    def export_to_excel(self):
        if not self.clase.ejercicios: QMessageBox.warning(self, "Vacío", "No hay ejercicios en la lista para exportar."); return
        filepath, _ = QFileDialog.getSaveFileName(self, "Exportar Ejercicios", f"ejercicios_{self.clase.nombre}.xlsx", "Excel Files (*.xlsx)")
        if filepath:
            try: self.export_manager.exportar_ejercicios_a_excel(filepath, self.clase.ejercicios); QMessageBox.information(self, "Éxito", f"Ejercicios exportados a:\n{filepath}")
            except Exception as e: QMessageBox.critical(self, "Error", f"No se pudo exportar: {e}")

    def import_from_excel(self):
        filepath, _ = QFileDialog.getOpenFileName(self, "Importar Ejercicios", "", "Excel Files (*.xlsx)")
        if not filepath: return
        try:
            df = pd.read_excel(filepath)
            required_cols = ['nombre']
            if not all(col in df.columns for col in required_cols):
                QMessageBox.critical(self, "Error de Formato", f"El archivo Excel debe contener la columna 'nombre'."); return
            
            all_bank_exercises = self.db_manager.obtener_ejercicios()
            bank_dict = {ej.nombre.lower(): ej for ej in all_bank_exercises}
            current_ids = {self._ej_id(ej) for ej in self.clase.ejercicios}
            added_count = 0
            for _, row in df.iterrows():
                nombre = str(row['nombre']).strip().lower()
                if nombre in bank_dict:
                    ejercicio = bank_dict[nombre]
                    if self._ej_id(ejercicio) not in current_ids:
                        self.clase.ejercicios.append(self._ej_to_obj(ejercicio)); current_ids.add(self._ej_id(ejercicio)); added_count += 1
            QMessageBox.information(self, "Importación Completa", f"Se han añadido {added_count} ejercicios a la lista desde el Excel.")
            self.refresh_exercise_list()
        except Exception as e: QMessageBox.critical(self, "Error de Lectura", f"No se pudo leer el archivo Excel: {e}")
            
    def accept_dialog(self):
        nombre = self.nombre_input.text().strip()
        if not nombre: QMessageBox.warning(self, "Campo Requerido", "El nombre de la clase es obligatorio."); return
        # Ajuste automático en caso de duplicado: Clase (2), Clase (3), ...
        try:
            all_clases = self.db_manager.obtener_clases() or []
            # Excluir la clase actual si se está editando
            other_names = [c.nombre for c in all_clases if not (hasattr(c, 'id') and not self.is_new and c.id == self.clase.id)]
            base = re.sub(r"\s\((\d+)\)\s*$", "", nombre).strip()
            # Buscar colisiones case-insensitive
            collisions = []
            for n in other_names:
                if n is None:
                    continue
                n_str = str(n)
                if n_str.strip().lower() == base.lower():
                    collisions.append(1)
                else:
                    m = re.match(rf"^{re.escape(base)}\s\((\d+)\)\s*$", n_str.strip(), flags=re.IGNORECASE)
                    if m:
                        try:
                            collisions.append(int(m.group(1)))
                        except Exception:
                            pass
            if collisions:
                next_num = max(collisions) + 1 if max(collisions) > 1 else 2
                nombre = f"{base} ({next_num})"
        except Exception:
            # Si algo falla, continuamos y confiamos en el control de BD
            pass
        self.clase.nombre = nombre; self.clase.descripcion = self.descripcion_input.toPlainText().strip()
        try:
            if self.is_new: self.clase.id = self.db_manager.crear_clase(self.clase)
            else: self.db_manager.actualizar_clase(self.clase)
            ejercicio_ids = [self._ej_id(ej) for ej in self.clase.ejercicios]
            self.db_manager.guardar_ejercicios_para_clase(self.clase.id, ejercicio_ids)
            super().accept()
        except Exception as e: QMessageBox.critical(self, "Error", f"No se pudo guardar la clase: {e}")

