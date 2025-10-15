from PyQt6.QtWidgets import (
    QDialog, QVBoxLayout, QFormLayout, QLineEdit, QTextEdit,
    QDialogButtonBox, QMessageBox, QComboBox
)
from typing import Optional, Dict
from models import Ejercicio

class ExerciseBankDialog(QDialog):
    def __init__(self, parent, ejercicio: Optional[Ejercicio] = None):
        super().__init__(parent)
        self.ejercicio = ejercicio
        self.setWindowTitle("Editar Ejercicio" if self.ejercicio else "Añadir Nuevo Ejercicio")
        
        layout = QFormLayout(self)
        self.nombre_input = QLineEdit()
        self.grupo_muscular_input = QLineEdit()
        self.objetivo_combo = QComboBox()
        self.descripcion_input = QTextEdit()
        
        # Configurar opciones de objetivo
        objetivos = ["general", "fuerza", "cardio", "rehabilitación", "flexibilidad", "resistencia", "coordinación", "equilibrio"]
        self.objetivo_combo.addItems(objetivos)

        layout.addRow("Nombre del Ejercicio:", self.nombre_input)
        layout.addRow("Grupo Muscular:", self.grupo_muscular_input)
        layout.addRow("Objetivo:", self.objetivo_combo)
        layout.addRow("Descripción/Notas:", self.descripcion_input)
        
        self.button_box = QDialogButtonBox(QDialogButtonBox.StandardButton.Ok | QDialogButtonBox.StandardButton.Cancel)
        layout.addRow(self.button_box)

        self.button_box.accepted.connect(self.accept_dialog)
        self.button_box.rejected.connect(self.reject)

        if self.ejercicio:
            self.load_data()

    def load_data(self):
        # Verificar si ejercicio es un diccionario o un objeto
        if isinstance(self.ejercicio, dict):
            self.nombre_input.setText(self.ejercicio.get('nombre', ''))
            self.grupo_muscular_input.setText(self.ejercicio.get('grupo_muscular', ''))
            # Configurar objetivo
            objetivo = self.ejercicio.get('objetivo', 'general')
            index = self.objetivo_combo.findText(objetivo)
            if index >= 0:
                self.objetivo_combo.setCurrentIndex(index)
            self.descripcion_input.setPlainText(self.ejercicio.get('descripcion', ''))
        else:
            # Comportamiento original para objetos Ejercicio
            self.nombre_input.setText(self.ejercicio.nombre)
            self.grupo_muscular_input.setText(self.ejercicio.grupo_muscular or "")
            # Configurar objetivo
            objetivo = getattr(self.ejercicio, 'objetivo', 'general')
            index = self.objetivo_combo.findText(objetivo)
            if index >= 0:
                self.objetivo_combo.setCurrentIndex(index)
            self.descripcion_input.setPlainText(self.ejercicio.descripcion or "")

    def accept_dialog(self):
        nombre = self.nombre_input.text().strip()
        if not nombre:
            QMessageBox.warning(self, "Campo Requerido", "El nombre del ejercicio es obligatorio.")
            return

        # Si es un nuevo ejercicio, lo creamos
        if not self.ejercicio:
            self.ejercicio = Ejercicio()
        
        # Si es dict, convertirlo a objeto Ejercicio antes de actualizar
        if isinstance(self.ejercicio, dict):
            self.ejercicio = Ejercicio(
                id=self.ejercicio.get('id'),
                nombre=self.ejercicio.get('nombre', ''),
                grupo_muscular=self.ejercicio.get('grupo_muscular'),
                descripcion=self.ejercicio.get('descripcion'),
                objetivo=self.ejercicio.get('objetivo', 'general')
            )
        
        # Actualizamos los datos del objeto ejercicio
        self.ejercicio.nombre = nombre
        self.ejercicio.grupo_muscular = self.grupo_muscular_input.text().strip()
        self.ejercicio.objetivo = self.objetivo_combo.currentText()
        self.ejercicio.descripcion = self.descripcion_input.toPlainText().strip()
        
        super().accept()

    def get_ejercicio(self) -> Ejercicio:
        return self.ejercicio

