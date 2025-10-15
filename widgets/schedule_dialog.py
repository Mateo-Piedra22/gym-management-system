from PyQt6.QtWidgets import (
    QDialog, QVBoxLayout, QHBoxLayout, QFormLayout,
    QLineEdit, QTimeEdit, QCheckBox, QComboBox,
    QPushButton, QLabel, QGroupBox, QMessageBox
)
from PyQt6.QtCore import QTime, Qt
from PyQt6.QtGui import QFont

class ScheduleDialog(QDialog):
    """Diálogo para crear/editar programaciones de temas"""
    
    def __init__(self, parent=None, schedule_data=None):
        super().__init__(parent)
        self.schedule_data = schedule_data
        self.setup_ui()
        
        if schedule_data:
            self.load_schedule_data()
    
    def setup_ui(self):
        """Configura la interfaz del diálogo"""
        self.setWindowTitle("Programación de Tema")
        self.setModal(True)
        self.resize(400, 350)
        
        layout = QVBoxLayout(self)
        
        # Información básica
        info_group = QGroupBox("Información de la Programación")
        info_layout = QFormLayout(info_group)
        
        self.name_edit = QLineEdit()
        self.name_edit.setPlaceholderText("Ej: Tema Matutino")
        info_layout.addRow("Nombre:", self.name_edit)
        
        # Selector de tema
        self.theme_combo = QComboBox()
        self.load_available_themes()
        info_layout.addRow("Tema:", self.theme_combo)
        
        layout.addWidget(info_group)
        
        # Configuración de horario
        time_group = QGroupBox("Horario")
        time_layout = QFormLayout(time_group)
        
        self.start_time = QTimeEdit()
        self.start_time.setTime(QTime(9, 0))  # 9:00 AM por defecto
        self.start_time.setDisplayFormat("HH:mm")
        time_layout.addRow("Hora de inicio:", self.start_time)
        
        self.end_time = QTimeEdit()
        self.end_time.setTime(QTime(17, 0))  # 5:00 PM por defecto
        self.end_time.setDisplayFormat("HH:mm")
        time_layout.addRow("Hora de fin:", self.end_time)
        
        layout.addWidget(time_group)
        
        # Días de la semana
        days_group = QGroupBox("Días de la Semana")
        days_layout = QVBoxLayout(days_group)
        
        self.day_checkboxes = {}
        days = [
            ('monday', 'Lunes'),
            ('tuesday', 'Martes'),
            ('wednesday', 'Miércoles'),
            ('thursday', 'Jueves'),
            ('friday', 'Viernes'),
            ('saturday', 'Sábado'),
            ('sunday', 'Domingo')
        ]
        
        for day_key, day_name in days:
            checkbox = QCheckBox(day_name)
            self.day_checkboxes[day_key] = checkbox
            days_layout.addWidget(checkbox)
        
        layout.addWidget(days_group)
        
        # Botones
        buttons_layout = QHBoxLayout()
        
        self.save_button = QPushButton("Guardar")
        self.save_button.clicked.connect(self.accept)
        # Use standardized class instead of inline stylesheet
        self.save_button.setProperty("class", "primary")
        
        self.cancel_button = QPushButton("Cancelar")
        self.cancel_button.clicked.connect(self.reject)
        # Use standardized class instead of inline stylesheet
        self.cancel_button.setProperty("class", "secondary")
        
        buttons_layout.addWidget(self.cancel_button)
        buttons_layout.addWidget(self.save_button)
        
        layout.addLayout(buttons_layout)
    
    def load_available_themes(self):
        """Carga los temas disponibles en el combo box"""
        # Temas predefinidos
        predefined_themes = [
            "Clásico",
            "Moderno",
            "Oscuro",
            "Claro",
            "Corporativo",
            "Deportivo",
            "Elegante",
            "Minimalista"
        ]
        
        self.theme_combo.addItems(predefined_themes)
        
        # Cargar temas personalizados si están disponibles
        try:
            if hasattr(self.parent(), 'themes_combo'):
                for i in range(self.parent().themes_combo.count()):
                    theme_name = self.parent().themes_combo.itemText(i)
                    if theme_name not in predefined_themes:
                        self.theme_combo.addItem(theme_name)
        except Exception as e:
            print(f"Error al cargar temas personalizados: {e}")
    
    def load_schedule_data(self):
        """Carga los datos de una programación existente"""
        if not self.schedule_data:
            return
        
        self.name_edit.setText(self.schedule_data.get('name', ''))
        
        # Seleccionar tema
        theme_name = self.schedule_data.get('theme_name', '')
        index = self.theme_combo.findText(theme_name)
        if index >= 0:
            self.theme_combo.setCurrentIndex(index)
        
        # Configurar horarios
        start_time_str = self.schedule_data.get('start_time', '09:00')
        end_time_str = self.schedule_data.get('end_time', '17:00')
        
        start_time = QTime.fromString(start_time_str, "HH:mm")
        end_time = QTime.fromString(end_time_str, "HH:mm")
        
        self.start_time.setTime(start_time)
        self.end_time.setTime(end_time)
        
        # Configurar días
        days = self.schedule_data.get('days', [])
        for day_key, checkbox in self.day_checkboxes.items():
            checkbox.setChecked(day_key in days)
    
    def get_schedule_data(self):
        """Obtiene los datos de la programación del diálogo"""
        # Validar campos requeridos
        if not self.name_edit.text().strip():
            QMessageBox.warning(self, "Error", "Por favor ingrese un nombre para la programación.")
            return None
        
        if self.theme_combo.currentText() == "":
            QMessageBox.warning(self, "Error", "Por favor seleccione un tema.")
            return None
        
        # Validar que al menos un día esté seleccionado
        selected_days = [day for day, checkbox in self.day_checkboxes.items() if checkbox.isChecked()]
        if not selected_days:
            QMessageBox.warning(self, "Error", "Por favor seleccione al menos un día de la semana.")
            return None
        
        # Validar horarios
        start_time = self.start_time.time()
        end_time = self.end_time.time()
        
        if start_time >= end_time:
            QMessageBox.warning(self, "Error", "La hora de inicio debe ser anterior a la hora de fin.")
            return None
        
        return {
            'name': self.name_edit.text().strip(),
            'theme_name': self.theme_combo.currentText(),
            'start_time': start_time.toString("HH:mm"),
            'end_time': end_time.toString("HH:mm"),
            'days': selected_days
        }
    
    def accept(self):
        """Valida y acepta el diálogo"""
        if self.get_schedule_data() is not None:
            super().accept()

