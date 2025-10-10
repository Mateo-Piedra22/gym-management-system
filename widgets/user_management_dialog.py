from PyQt6.QtWidgets import (
    QDialog, QVBoxLayout, QHBoxLayout, QTabWidget, QLabel, QPushButton,
    QMessageBox, QFrame, QSizePolicy
)
from PyQt6.QtCore import Qt, pyqtSignal
from PyQt6.QtGui import QFont
from typing import Optional

from models import Usuario
from database import DatabaseManager
from widgets.notas_widget import NotasWidget
from widgets.etiquetas_widget import EtiquetasWidget
from widgets.estados_widget import EstadosWidget


class UserManagementDialog(QDialog):
    """Ventana de gestión completa para Notas, Etiquetas y Estados de un usuario."""
    
    # Señales
    datos_actualizados = pyqtSignal()  # Señal cuando se actualizan los datos
    
    def __init__(self, parent, usuario: Usuario, db_manager: DatabaseManager):
        super().__init__(parent)
        self.db_manager = db_manager
        self.usuario = usuario
        self.setup_ui()
        self.conectar_señales()
        self.cargar_datos_usuario()
    
    def setup_ui(self):
        """Configura la interfaz de usuario."""
        self.setWindowTitle(f"Gestión de Usuario - {self.usuario.nombre}")
        self.setModal(True)
        self.resize(900, 380)  # Reducido de 550 a 440 (110px menos)
        
        layout = QVBoxLayout(self)
        layout.setContentsMargins(6, 6, 6, 6)  # Reducido aún más los márgenes
        layout.setSpacing(4)  # Reducido espaciado entre elementos
        
        # Encabezado con información del usuario
        self.crear_encabezado(layout)
        
        # Pestañas principales
        self.tab_widget = QTabWidget()
        # Política de tamaño compacta para el TabWidget
        self.tab_widget.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Preferred)
        self.tab_widget.setTabPosition(QTabWidget.TabPosition.North)
        
        # Pestaña de Notas
        self.notas_widget = NotasWidget(self.db_manager)
        self.tab_widget.addTab(self.notas_widget, "Notas")
        
        # Pestaña de Etiquetas
        self.etiquetas_widget = EtiquetasWidget(self.db_manager)
        self.tab_widget.addTab(self.etiquetas_widget, "🏷️ Etiquetas")
        
        # Pestaña de Estados
        self.estados_widget = EstadosWidget(self.db_manager)
        self.tab_widget.addTab(self.estados_widget, "Estados")
        
        layout.addWidget(self.tab_widget)
        
        # Botones de acción
        self.crear_botones_accion(layout)
    
    def crear_encabezado(self, layout):
        """Crea el encabezado con información del usuario."""
        encabezado_frame = QFrame()
        encabezado_frame.setFrameStyle(QFrame.Shape.StyledPanel)
        encabezado_frame.setObjectName("user_info_header")
        
        encabezado_layout = QVBoxLayout(encabezado_frame)
        encabezado_layout.setContentsMargins(6, 4, 6, 4)  # Padding mínimo
        encabezado_layout.setSpacing(2)  # Espaciado mínimo
        
        # Título principal
        titulo = QLabel(f"👤 {self.usuario.nombre}")
        titulo.setObjectName("user_title_label")
        titulo_font = QFont()
        titulo_font.setBold(True)
        titulo_font.setPointSize(10)  # Reducido de 12 a 10 para compactar
        titulo.setFont(titulo_font)
        encabezado_layout.addWidget(titulo)
        
        # Información adicional
        info_layout = QHBoxLayout()
        info_layout.setSpacing(8)  # Espaciado mínimo entre elementos
        
        if self.usuario.dni:
            dni_label = QLabel(f"DNI: {self.usuario.dni}")
            dni_label.setObjectName("user_info_label")
            info_layout.addWidget(dni_label)
        
        if self.usuario.telefono:
            telefono_label = QLabel(f"Teléfono: {self.usuario.telefono}")
            telefono_label.setObjectName("user_info_label")
            info_layout.addWidget(telefono_label)
        
        tipo_cuota_label = QLabel(f"Tipo de Cuota: {self.usuario.tipo_cuota}")
        tipo_cuota_label.setObjectName("user_info_label")
        info_layout.addWidget(tipo_cuota_label)
        
        info_layout.addStretch()
        encabezado_layout.addLayout(info_layout)
        
        layout.addWidget(encabezado_frame)
    
    def crear_botones_accion(self, layout):
        """Crea los botones de acción en la parte inferior."""
        botones_layout = QHBoxLayout()
        botones_layout.setContentsMargins(0, 2, 0, 0)  # Margen mínimo superior
        
        # Botón de actualizar
        self.actualizar_btn = QPushButton("🔄 Actualizar")
        self.actualizar_btn.clicked.connect(self.actualizar_datos)
        botones_layout.addWidget(self.actualizar_btn)
        
        botones_layout.addStretch()
        
        # Botón de cerrar
        self.cerrar_btn = QPushButton("✖️ Cerrar")
        self.cerrar_btn.clicked.connect(self.accept)
        botones_layout.addWidget(self.cerrar_btn)
        
        layout.addLayout(botones_layout)
    
    def conectar_señales(self):
        """Conecta las señales de los widgets."""
        # Conectar señales de cambios en los widgets
        self.notas_widget.notas_changed.connect(self.on_datos_cambiados)
        self.etiquetas_widget.etiquetas_changed.connect(self.on_datos_cambiados)
        self.estados_widget.estados_changed.connect(self.on_datos_cambiados)
    
    def cargar_datos_usuario(self):
        """Carga los datos del usuario en todos los widgets."""
        try:
            # Establecer usuario en cada widget
            self.notas_widget.set_user(self.usuario)
            self.etiquetas_widget.establecer_usuario(self.usuario)
            self.estados_widget.set_user(self.usuario)
            
        except Exception as e:
            QMessageBox.critical(
                self, "Error", 
                f"Error al cargar datos del usuario: {str(e)}"
            )
    
    def actualizar_datos(self):
        """Actualiza todos los datos del usuario."""
        try:
            self.cargar_datos_usuario()
            QMessageBox.information(
                self, "Actualización", 
                "Datos actualizados correctamente."
            )
        except Exception as e:
            QMessageBox.critical(
                self, "Error", 
                f"Error al actualizar datos: {str(e)}"
            )
    
    def on_datos_cambiados(self):
        """Maneja cuando se cambian los datos en cualquier widget."""
        # Emitir señal para notificar cambios
        self.datos_actualizados.emit()
    
    def closeEvent(self, event):
        """Maneja el evento de cierre de la ventana."""
        # Emitir señal final de actualización
        self.datos_actualizados.emit()
        super().closeEvent(event)

