from PyQt6.QtWidgets import (
    QDialog, QVBoxLayout, QHBoxLayout, QLabel, QPushButton,
    QTableWidget, QTableWidgetItem, QCheckBox, QDialogButtonBox,
    QMessageBox, QGroupBox, QHeaderView, QAbstractItemView
)
from PyQt6.QtCore import Qt
from datetime import datetime
from typing import List

class AttendanceDialog(QDialog):
    """Diálogo para registrar la asistencia de usuarios inscritos en una clase"""
    
    def __init__(self, parent, inscriptos, selected_schedule, db_manager):
        super().__init__(parent)
        self.inscriptos = inscriptos
        self.selected_schedule = selected_schedule
        self.db_manager = db_manager
        self.attendance_data = {}
        
        self.setWindowTitle("Registrar Asistencia de Clase")
        self.setMinimumSize(600, 400)
        self.setup_ui()
        self.load_attendance_data()
    
    def setup_ui(self):
        """Configura la interfaz del diálogo"""
        main_layout = QVBoxLayout(self)
        
        # Información de la clase
        info_group = QGroupBox("Información de la Clase")
        info_layout = QVBoxLayout(info_group)
        
        class_info = QLabel(f"Clase: {self.selected_schedule.nombre_clase}")
        schedule_info = QLabel(f"Horario: {self.selected_schedule.dia_semana} {self.selected_schedule.hora_inicio}-{self.selected_schedule.hora_fin}")
        professor_info = QLabel(f"Profesor: {self.selected_schedule.nombre_profesor or 'Sin asignar'}")
        date_info = QLabel(f"Fecha: {datetime.now().strftime('%d/%m/%Y')}")
        
        info_layout.addWidget(class_info)
        info_layout.addWidget(schedule_info)
        info_layout.addWidget(professor_info)
        info_layout.addWidget(date_info)
        
        # Tabla de asistencia
        attendance_group = QGroupBox("Lista de Asistencia")
        attendance_layout = QVBoxLayout(attendance_group)
        
        self.attendance_table = QTableWidget()
        self.attendance_table.setColumnCount(4)
        self.attendance_table.setHorizontalHeaderLabels(["ID", "Nombre", "Presente", "Observaciones"])
        self.attendance_table.horizontalHeader().setSectionResizeMode(1, QHeaderView.ResizeMode.Stretch)
        self.attendance_table.horizontalHeader().setSectionResizeMode(3, QHeaderView.ResizeMode.Stretch)
        self.attendance_table.setSelectionBehavior(QAbstractItemView.SelectionBehavior.SelectRows)
        
        attendance_layout.addWidget(self.attendance_table)
        
        # Botones de acción rápida
        quick_actions_layout = QHBoxLayout()
        self.mark_all_present_button = QPushButton("Marcar Todos Presentes")
        self.mark_all_absent_button = QPushButton("Marcar Todos Ausentes")
        
        quick_actions_layout.addWidget(self.mark_all_present_button)
        quick_actions_layout.addWidget(self.mark_all_absent_button)
        quick_actions_layout.addStretch()
        
        attendance_layout.addLayout(quick_actions_layout)
        
        # Botones del diálogo
        self.button_box = QDialogButtonBox(
            QDialogButtonBox.StandardButton.Save | QDialogButtonBox.StandardButton.Cancel
        )
        
        main_layout.addWidget(info_group)
        main_layout.addWidget(attendance_group)
        main_layout.addWidget(self.button_box)
        
        self.connect_signals()
    
    def connect_signals(self):
        """Conecta las señales de los widgets"""
        self.button_box.accepted.connect(self.save_attendance)
        self.button_box.rejected.connect(self.reject)
        self.mark_all_present_button.clicked.connect(self.mark_all_present)
        self.mark_all_absent_button.clicked.connect(self.mark_all_absent)
    
    def load_attendance_data(self):
        """Carga los datos de los usuarios inscritos en la tabla"""
        self.attendance_table.setRowCount(len(self.inscriptos))
        
        for row, inscripto in enumerate(self.inscriptos):
            # ID del usuario - acceder como diccionario
            user_id = inscripto['usuario_id'] if isinstance(inscripto, dict) else inscripto.usuario_id
            id_item = QTableWidgetItem(str(user_id))
            id_item.setFlags(id_item.flags() & ~Qt.ItemFlag.ItemIsEditable)
            self.attendance_table.setItem(row, 0, id_item)
            
            # Nombre del usuario - acceder como diccionario
            nombre = inscripto['nombre_usuario'] if isinstance(inscripto, dict) else inscripto.nombre_usuario
            name_item = QTableWidgetItem(nombre)
            name_item.setFlags(name_item.flags() & ~Qt.ItemFlag.ItemIsEditable)
            self.attendance_table.setItem(row, 1, name_item)
            
            # Checkbox de presente
            present_checkbox = QCheckBox()
            present_checkbox.setChecked(False)  # Por defecto ausente
            self.attendance_table.setCellWidget(row, 2, present_checkbox)
            
            # Campo de observaciones
            observations_item = QTableWidgetItem("")
            self.attendance_table.setItem(row, 3, observations_item)
            
            # Guardar referencia para acceso posterior
            self.attendance_data[user_id] = {
                'checkbox': present_checkbox,
                'observations': observations_item
            }
    
    def mark_all_present(self):
        """Marca todos los usuarios como presentes"""
        for user_data in self.attendance_data.values():
            user_data['checkbox'].setChecked(True)
    
    def mark_all_absent(self):
        """Marca todos los usuarios como ausentes"""
        for user_data in self.attendance_data.values():
            user_data['checkbox'].setChecked(False)
    
    def save_attendance(self):
        """Guarda la asistencia en la base de datos"""
        try:
            fecha_actual = datetime.now().date()
            
            # Recopilar datos de asistencia
            attendance_records = []
            owner_skipped = False
            for inscripto in self.inscriptos:
                user_id = inscripto['usuario_id'] if isinstance(inscripto, dict) else inscripto.usuario_id
                user_data = self.attendance_data[user_id]
                
                presente = user_data['checkbox'].isChecked()
                observaciones = user_data['observations'].text().strip()
                
                attendance_records.append({
                    'usuario_id': user_id,
                    'horario_id': self.selected_schedule.id,
                    'fecha': fecha_actual,
                    'presente': presente,
                    'observaciones': observaciones if observaciones else None
                })
            
            # Guardar en la base de datos
            for record in attendance_records:
                # Bloquear registro de asistencia para usuario con rol 'dueño'
                try:
                    usuario = None
                    if hasattr(self.db_manager, 'obtener_usuario_por_id'):
                        usuario = self.db_manager.obtener_usuario_por_id(record['usuario_id'])
                    elif hasattr(self.db_manager, 'obtener_usuario'):
                        usuario = self.db_manager.obtener_usuario(record['usuario_id'])
                    if usuario and getattr(usuario, 'rol', None) == 'dueño':
                        owner_skipped = True
                        continue
                except Exception:
                    pass
                estado = 'presente' if record['presente'] else 'ausente'
                self.db_manager.registrar_asistencia_clase_completa(
                    record['horario_id'],
                    record['usuario_id'],
                    record['fecha'].isoformat(),
                    estado,
                    None,  # hora_llegada
                    record['observaciones'],
                    None   # registrado_por
                )
                # Sin encolado manual: la replicación se gestiona por SymmetricDS
            
            if owner_skipped:
                QMessageBox.information(self, "Acción aplicada", "Se ignoró la asistencia para el usuario Dueño.")
            self.accept()
            
        except Exception as e:
            QMessageBox.critical(self, "Error", f"No se pudo guardar la asistencia: {e}")

