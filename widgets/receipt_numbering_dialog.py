from PyQt6.QtWidgets import (
    QDialog, QVBoxLayout, QHBoxLayout, QFormLayout, QLabel, 
    QLineEdit, QSpinBox, QComboBox, QPushButton, QGroupBox,
    QCheckBox, QMessageBox
)
from PyQt6.QtCore import Qt
from PyQt6.QtGui import QFont
from database import DatabaseManager

class ReceiptNumberingDialog(QDialog):
    """Diálogo para configurar la numeración automática de comprobantes"""
    
    def __init__(self, db_manager, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Configuración de Numeración de Comprobantes")
        self.setModal(True)
        self.resize(500, 400)
        
        # Referencia al database manager
        self.db_manager = db_manager
        
        # Configuración guardada
        self.config = {}
        
        self.setup_ui()
        self.load_current_config()
        self.update_preview()
        
    def setup_ui(self):
        """Configura la interfaz del diálogo"""
        layout = QVBoxLayout(self)
        
        # Grupo de configuración general
        general_group = QGroupBox("Configuración General")
        general_layout = QFormLayout(general_group)
        
        # Prefijo del comprobante
        self.prefix_edit = QLineEdit()
        self.prefix_edit.setPlaceholderText("Ej: REC, COMP, FAC")
        self.prefix_edit.setMaxLength(10)
        general_layout.addRow("Prefijo:", self.prefix_edit)
        
        # Número inicial
        self.start_number_spin = QSpinBox()
        self.start_number_spin.setRange(1, 999999)
        self.start_number_spin.setValue(1)
        general_layout.addRow("Número inicial:", self.start_number_spin)
        
        # Longitud del número
        self.number_length_spin = QSpinBox()
        self.number_length_spin.setRange(3, 10)
        self.number_length_spin.setValue(6)
        general_layout.addRow("Longitud del número:", self.number_length_spin)
        
        # Separador
        self.separator_edit = QLineEdit()
        self.separator_edit.setPlaceholderText("Ej: -, _, (vacío)")
        self.separator_edit.setMaxLength(3)
        general_layout.addRow("Separador:", self.separator_edit)
        
        layout.addWidget(general_group)
        
        # Grupo de configuración avanzada
        advanced_group = QGroupBox("Configuración Avanzada")
        advanced_layout = QFormLayout(advanced_group)
        
        # Reiniciar numeración anualmente
        self.reset_yearly_check = QCheckBox("Reiniciar numeración cada año")
        advanced_layout.addRow(self.reset_yearly_check)
        
        # Incluir año en el formato
        self.include_year_check = QCheckBox("Incluir año en el formato")
        advanced_layout.addRow(self.include_year_check)
        
        # Incluir mes en el formato
        self.include_month_check = QCheckBox("Incluir mes en el formato")
        advanced_layout.addRow(self.include_month_check)
        
        layout.addWidget(advanced_group)
        
        # Grupo de vista previa
        preview_group = QGroupBox("Vista Previa")
        preview_layout = QVBoxLayout(preview_group)
        
        self.preview_label = QLabel()
        self.preview_label.setStyleSheet("""
            QLabel {
                background-color: #f0f0f0;
                border: 1px solid #ccc;
                padding: 10px;
                font-family: 'Courier New', monospace;
                font-size: 14px;
                font-weight: bold;
            }
        """)
        preview_layout.addWidget(self.preview_label)
        
        layout.addWidget(preview_group)
        
        # Botones
        buttons_layout = QHBoxLayout()
        
        self.test_button = QPushButton("Probar Formato")
        self.test_button.clicked.connect(self.update_preview)
        buttons_layout.addWidget(self.test_button)
        
        buttons_layout.addStretch()
        
        self.cancel_button = QPushButton("Cancelar")
        self.cancel_button.clicked.connect(self.reject)
        buttons_layout.addWidget(self.cancel_button)
        
        self.save_button = QPushButton("Guardar")
        self.save_button.clicked.connect(self.save_config)
        self.save_button.setDefault(True)
        buttons_layout.addWidget(self.save_button)
        
        layout.addLayout(buttons_layout)
        
        # Conectar señales para actualizar vista previa automáticamente
        self.prefix_edit.textChanged.connect(self.update_preview)
        self.start_number_spin.valueChanged.connect(self.update_preview)
        self.number_length_spin.valueChanged.connect(self.update_preview)
        self.separator_edit.textChanged.connect(self.update_preview)
        self.reset_yearly_check.toggled.connect(self.update_preview)
        self.include_year_check.toggled.connect(self.update_preview)
        self.include_month_check.toggled.connect(self.update_preview)
        
    def load_current_config(self):
        """Carga la configuración actual desde la base de datos"""
        try:
            config = self.db_manager.get_receipt_numbering_config()
            if config:
                self.prefix_edit.setText(config.get('prefijo', 'REC'))
                self.start_number_spin.setValue(config.get('numero_inicial', 1))
                self.number_length_spin.setValue(config.get('longitud_numero', 6))
                self.separator_edit.setText(config.get('separador', '-'))
                self.reset_yearly_check.setChecked(config.get('reiniciar_anual', False))
                self.include_year_check.setChecked(config.get('incluir_año', True))
                self.include_month_check.setChecked(config.get('incluir_mes', False))
            else:
                # Valores por defecto
                self.prefix_edit.setText('REC')
                self.separator_edit.setText('-')
                self.include_year_check.setChecked(True)
                
            self.update_preview()
            
        except Exception as e:
            QMessageBox.warning(self, "Error", f"Error al cargar configuración: {str(e)}")
    
    def update_preview(self):
        """Actualiza la vista previa del formato"""
        try:
            format_str = self.generate_format_preview()
            self.preview_label.setText(f"Próximo comprobante: {format_str}")
        except Exception as e:
            self.preview_label.setText(f"Error en formato: {str(e)}")
    
    def generate_format_preview(self):
        """Genera una vista previa del formato de numeración"""
        from datetime import datetime
        
        prefix = self.prefix_edit.text().strip()
        separator = self.separator_edit.text()
        number_length = self.number_length_spin.value()
        start_number = self.start_number_spin.value()
        
        # Construir el formato
        parts = []
        
        if prefix:
            parts.append(prefix)
        
        if self.include_year_check.isChecked():
            parts.append(str(datetime.now().year))
        
        if self.include_month_check.isChecked():
            parts.append(f"{datetime.now().month:02d}")
        
        # Número con padding
        number_str = str(start_number).zfill(number_length)
        parts.append(number_str)
        
        return separator.join(parts)
    
    def save_config(self):
        """Guarda la configuración en la base de datos"""
        try:
            # Validaciones
            if not self.prefix_edit.text().strip():
                QMessageBox.warning(self, "Error", "El prefijo no puede estar vacío")
                return
            
            config = {
                'prefijo': self.prefix_edit.text().strip().upper(),
                'numero_inicial': self.start_number_spin.value(),
                'longitud_numero': self.number_length_spin.value(),
                'separador': self.separator_edit.text(),
                'reiniciar_anual': self.reset_yearly_check.isChecked(),
                'incluir_año': self.include_year_check.isChecked(),
                'incluir_mes': self.include_month_check.isChecked()
            }
            
            # Guardar en base de datos
            self.db_manager.save_receipt_numbering_config(config)
            
            QMessageBox.information(self, "Éxito", "Configuración guardada correctamente")
            self.accept()
            
        except Exception as e:
            QMessageBox.critical(self, "Error", f"Error al guardar configuración: {str(e)}")
    
    def get_next_receipt_number(self):
        """Obtiene el próximo número de comprobante según la configuración actual"""
        try:
            return self.db_manager.get_next_receipt_number()
        except Exception as e:
            return f"Error: {str(e)}"

