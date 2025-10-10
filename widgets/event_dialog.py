from PyQt6.QtWidgets import (
    QDialog, QVBoxLayout, QHBoxLayout, QFormLayout,
    QLineEdit, QDateEdit, QComboBox, QPushButton,
    QLabel, QGroupBox, QMessageBox, QTextEdit
)
from PyQt6.QtCore import QDate, Qt
from PyQt6.QtGui import QFont

class EventDialog(QDialog):
    """Diálogo para crear/editar eventos especiales de temas"""
    
    def __init__(self, parent=None, event_data=None):
        super().__init__(parent)
        self.event_data = event_data
        self.setup_ui()
        
        if event_data:
            self.load_event_data()
    
    def setup_ui(self):
        """Configura la interfaz del diálogo"""
        self.setWindowTitle("Evento Especial")
        self.setModal(True)
        self.resize(400, 400)
        
        layout = QVBoxLayout(self)
        
        # Información básica
        info_group = QGroupBox("Información del Evento")
        info_layout = QFormLayout(info_group)
        
        self.name_edit = QLineEdit()
        self.name_edit.setPlaceholderText("Ej: Navidad, Año Nuevo, Promoción Especial")
        info_layout.addRow("Nombre del Evento:", self.name_edit)
        
        # Selector de tema
        self.theme_combo = QComboBox()
        self.load_available_themes()
        info_layout.addRow("Tema:", self.theme_combo)
        
        layout.addWidget(info_group)
        
        # Configuración de fechas
        dates_group = QGroupBox("Período del Evento")
        dates_layout = QFormLayout(dates_group)
        
        self.start_date = QDateEdit()
        self.start_date.setDate(QDate.currentDate())
        self.start_date.setCalendarPopup(True)
        dates_layout.addRow("Fecha de inicio:", self.start_date)
        
        self.end_date = QDateEdit()
        self.end_date.setDate(QDate.currentDate().addDays(1))
        self.end_date.setCalendarPopup(True)
        dates_layout.addRow("Fecha de fin:", self.end_date)
        
        layout.addWidget(dates_group)
        
        # Descripción opcional
        desc_group = QGroupBox("Descripción (Opcional)")
        desc_layout = QVBoxLayout(desc_group)
        
        self.description_edit = QTextEdit()
        self.description_edit.setMaximumHeight(80)
        self.description_edit.setPlaceholderText("Descripción del evento o notas adicionales...")
        desc_layout.addWidget(self.description_edit)
        
        layout.addWidget(desc_group)
        
        # Información adicional
        info_label = QLabel(
            "<b>Nota:</b> Los eventos tienen prioridad sobre las programaciones regulares. "
            "Durante el período del evento, se aplicará el tema seleccionado independientemente "
            "de las programaciones horarias configuradas."
        )
        info_label.setWordWrap(True)
        info_label.setStyleSheet("""
            QLabel {
                background-color: #e8f4fd;
                border: 1px solid #bee5eb;
                border-radius: 4px;
                padding: 10px;
                color: #0c5460;
            }
        """)
        layout.addWidget(info_label)
        
        # Botones
        buttons_layout = QHBoxLayout()
        
        self.save_button = QPushButton("Guardar Evento")
        self.save_button.clicked.connect(self.accept)
        # Replace inline style with standardized class
        self.save_button.setProperty("class", "success")
        
        self.cancel_button = QPushButton("Cancelar")
        self.cancel_button.clicked.connect(self.reject)
        # Replace inline style with standardized class
        self.cancel_button.setProperty("class", "secondary")
        
        buttons_layout.addWidget(self.cancel_button)
        buttons_layout.addWidget(self.save_button)
        
        layout.addLayout(buttons_layout)
        
        # Conectar señales para validación en tiempo real
        self.start_date.dateChanged.connect(self.validate_dates)
        self.end_date.dateChanged.connect(self.validate_dates)
    
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
            "Minimalista",
            "Navideño",
            "Año Nuevo",
            "San Valentín",
            "Primavera",
            "Verano",
            "Otoño",
            "Invierno"
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
    
    def load_event_data(self):
        """Carga los datos de un evento existente"""
        if not self.event_data:
            return
        
        self.name_edit.setText(self.event_data.get('name', ''))
        
        # Seleccionar tema
        theme_name = self.event_data.get('theme_name', '')
        index = self.theme_combo.findText(theme_name)
        if index >= 0:
            self.theme_combo.setCurrentIndex(index)
        
        # Configurar fechas
        start_date_str = self.event_data.get('start_date', '')
        end_date_str = self.event_data.get('end_date', '')
        
        if start_date_str:
            start_date = QDate.fromString(start_date_str, "yyyy-MM-dd")
            if start_date.isValid():
                self.start_date.setDate(start_date)
        
        if end_date_str:
            end_date = QDate.fromString(end_date_str, "yyyy-MM-dd")
            if end_date.isValid():
                self.end_date.setDate(end_date)
        
        # Cargar descripción si existe
        description = self.event_data.get('description', '')
        if description:
            self.description_edit.setPlainText(description)
    
    def validate_dates(self):
        """Valida que las fechas sean coherentes"""
        start_date = self.start_date.date()
        end_date = self.end_date.date()
        
        if start_date > end_date:
            # Ajustar automáticamente la fecha de fin
            self.end_date.setDate(start_date.addDays(1))
    
    def get_event_data(self):
        """Obtiene los datos del evento del diálogo"""
        # Validar campos requeridos
        if not self.name_edit.text().strip():
            QMessageBox.warning(self, "Error", "Por favor ingrese un nombre para el evento.")
            return None
        
        if self.theme_combo.currentText() == "":
            QMessageBox.warning(self, "Error", "Por favor seleccione un tema.")
            return None
        
        # Validar fechas
        start_date = self.start_date.date()
        end_date = self.end_date.date()
        
        if start_date > end_date:
            QMessageBox.warning(self, "Error", "La fecha de inicio debe ser anterior o igual a la fecha de fin.")
            return None
        
        # Verificar que no sea una fecha muy antigua
        current_date = QDate.currentDate()
        if end_date < current_date:
            reply = QMessageBox.question(
                self,
                "Fecha Pasada",
                "La fecha de fin del evento ya ha pasado. ¿Desea continuar?",
                QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No
            )
            if reply == QMessageBox.StandardButton.No:
                return None
        
        event_data = {
            'name': self.name_edit.text().strip(),
            'theme_name': self.theme_combo.currentText(),
            'start_date': start_date.toString("yyyy-MM-dd"),
            'end_date': end_date.toString("yyyy-MM-dd")
        }
        
        # Agregar descripción si existe
        description = self.description_edit.toPlainText().strip()
        if description:
            event_data['description'] = description
        
        return event_data
    
    def accept(self):
        """Valida y acepta el diálogo"""
        if self.get_event_data() is not None:
            super().accept()

