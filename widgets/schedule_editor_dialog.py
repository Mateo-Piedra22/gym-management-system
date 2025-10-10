from PyQt6.QtWidgets import (
    QDialog, QVBoxLayout, QGridLayout, QComboBox, QTimeEdit,
    QSpinBox, QDialogButtonBox, QMessageBox, QLabel
)
from PyQt6.QtCore import QTime, Qt
from typing import Optional, List
from models import ClaseHorario, Usuario

class ScheduleEditorDialog(QDialog):
    def __init__(self, parent, horario: Optional[ClaseHorario] = None, clase_id: int = 0, profesores: List[Usuario] = []):
        super().__init__(parent)
        self.horario = horario if horario else ClaseHorario(clase_id=clase_id)
        self.is_new = horario is None
        self.profesores = profesores

        self.setWindowTitle("Editar Horario" if not self.is_new else "Añadir Nuevo Horario")
        self.setMinimumWidth(380)
        
        main_layout = QVBoxLayout(self)
        main_layout.setSpacing(15)
        
        # Usamos un Grid Layout para un control perfecto de la alineación
        grid_layout = QGridLayout()
        grid_layout.setContentsMargins(10, 10, 10, 10)
        grid_layout.setVerticalSpacing(15)
        grid_layout.setHorizontalSpacing(10)

        # Creación de Widgets
        self.dia_semana_combo = QComboBox()
        self.dia_semana_combo.addItems(["Lunes", "Martes", "Miércoles", "Jueves", "Viernes", "Sábado", "Domingo"])
        
        self.hora_inicio_edit = QTimeEdit()
        self.hora_inicio_edit.setDisplayFormat("HH:mm")
        
        self.hora_fin_edit = QTimeEdit()
        self.hora_fin_edit.setDisplayFormat("HH:mm")

        self.profesor_combo = QComboBox()
        self.profesor_combo.addItem("Sin asignar", userData=None)
        # Agregar profesores al combo
        for prof in self.profesores:
            # Los profesores siempre vienen como diccionarios desde obtener_profesores()
            nombre = prof.get('nombre', 'Profesor sin nombre')
            self.profesor_combo.addItem(nombre, prof)

        self.cupo_spinbox = QSpinBox(minimum=1, maximum=100)
        
        # Añadir widgets al Grid con alineación correcta
        grid_layout.addWidget(QLabel("Día de la semana:"), 0, 0, Qt.AlignmentFlag.AlignRight)
        grid_layout.addWidget(self.dia_semana_combo, 0, 1)
        grid_layout.addWidget(QLabel("Hora de Inicio:"), 1, 0, Qt.AlignmentFlag.AlignRight)
        grid_layout.addWidget(self.hora_inicio_edit, 1, 1)
        grid_layout.addWidget(QLabel("Hora de Fin:"), 2, 0, Qt.AlignmentFlag.AlignRight)
        grid_layout.addWidget(self.hora_fin_edit, 2, 1)
        grid_layout.addWidget(QLabel("Profesor a cargo:"), 3, 0, Qt.AlignmentFlag.AlignRight)
        grid_layout.addWidget(self.profesor_combo, 3, 1)
        grid_layout.addWidget(QLabel("Cupo Máximo:"), 4, 0, Qt.AlignmentFlag.AlignRight)
        grid_layout.addWidget(self.cupo_spinbox, 4, 1)
        
        # Hacemos que la columna de los inputs se estire
        grid_layout.setColumnStretch(1, 1)
        
        self.button_box = QDialogButtonBox(QDialogButtonBox.StandardButton.Ok | QDialogButtonBox.StandardButton.Cancel)
        
        main_layout.addLayout(grid_layout)
        main_layout.addWidget(self.button_box)

        self.connect_signals()
        
        if not self.is_new:
            self.load_data()
        else:
            self.hora_inicio_edit.setTime(QTime(18, 0))
            self.hora_fin_edit.setTime(QTime(19, 0))
            self.cupo_spinbox.setValue(20)
        
        self.on_start_time_changed(self.hora_inicio_edit.time())

    def load_data(self):
        self.dia_semana_combo.setCurrentText(self.horario.dia_semana)
        
        # Convertir datetime.time a string si es necesario
        hora_inicio_str = self.horario.hora_inicio.strftime("%H:%M") if hasattr(self.horario.hora_inicio, 'strftime') else str(self.horario.hora_inicio)
        hora_fin_str = self.horario.hora_fin.strftime("%H:%M") if hasattr(self.horario.hora_fin, 'strftime') else str(self.horario.hora_fin)
        
        self.hora_inicio_edit.setTime(QTime.fromString(hora_inicio_str, "HH:mm"))
        self.hora_fin_edit.setTime(QTime.fromString(hora_fin_str, "HH:mm"))
        self.cupo_spinbox.setValue(self.horario.cupo_maximo)
        
        if self.horario.profesor_id:
            for i in range(self.profesor_combo.count()):
                profesor_data = self.profesor_combo.itemData(i)
                if profesor_data:
                    # Manejar tanto objetos Usuario como diccionarios
                    profesor_id = profesor_data.get('id') if isinstance(profesor_data, dict) else profesor_data.id
                    if profesor_id == self.horario.profesor_id:
                        self.profesor_combo.setCurrentIndex(i)
                        break
        else:
            self.profesor_combo.setCurrentIndex(0)

    def connect_signals(self):
        self.hora_inicio_edit.timeChanged.connect(self.on_start_time_changed)
        self.button_box.accepted.connect(self.accept_dialog)
        self.button_box.rejected.connect(self.reject)
    
    def on_start_time_changed(self, time: QTime):
        self.hora_fin_edit.setMinimumTime(time)
        if self.hora_fin_edit.time() < time:
            self.hora_fin_edit.setTime(time)

    def accept_dialog(self):
        profesor_data = self.profesor_combo.currentData()
        self.horario.dia_semana = self.dia_semana_combo.currentText()
        self.horario.hora_inicio = self.hora_inicio_edit.time().toString("HH:mm")
        self.horario.hora_fin = self.hora_fin_edit.time().toString("HH:mm")
        # Manejar tanto objetos Usuario como diccionarios
        if profesor_data:
            self.horario.profesor_id = profesor_data.get('id') if isinstance(profesor_data, dict) else profesor_data.id
        else:
            self.horario.profesor_id = None
        self.horario.cupo_maximo = self.cupo_spinbox.value()
        super().accept()

    def get_horario(self) -> Optional[ClaseHorario]:
        return self.horario

