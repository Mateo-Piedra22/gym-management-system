from PyQt6.QtWidgets import (
    QWidget, QVBoxLayout, QHBoxLayout, QGroupBox, QLineEdit, QPushButton,
    QListWidget, QListWidgetItem, QLabel, QMessageBox, QDialog, QDialogButtonBox,
    QFormLayout, QColorDialog, QFrame, QScrollArea, QCheckBox, QComboBox, QSizePolicy
)
from PyQt6.QtCore import Qt, pyqtSignal, QSize
from PyQt6.QtGui import QFont, QColor, QPalette, QPixmap, QPainter
from typing import List, Optional
from models import Etiqueta, UsuarioEtiqueta
from database import DatabaseManager

class EtiquetaDialog(QDialog):
    """Diálogo para crear/editar etiquetas."""
    
    def __init__(self, parent=None, etiqueta: Optional[Etiqueta] = None):
        super().__init__(parent)
        self.etiqueta = etiqueta
        self.color_seleccionado = "#3498db"  # Color por defecto
        self.setup_ui()
        
        if etiqueta:
            self.cargar_etiqueta()
    
    def setup_ui(self):
        self.setWindowTitle("Crear Etiqueta" if not self.etiqueta else "Editar Etiqueta")
        self.setModal(True)
        self.resize(400, 300)
        
        layout = QVBoxLayout(self)
        
        # Formulario
        form_layout = QFormLayout()
        
        # Nombre
        self.nombre_edit = QLineEdit()
        self.nombre_edit.setPlaceholderText("Nombre de la etiqueta...")
        form_layout.addRow("Nombre:", self.nombre_edit)
        
        # Color
        color_layout = QHBoxLayout()
        self.color_preview = QLabel()
        self.color_preview.setFixedSize(30, 30)
        self.color_preview.setObjectName("color_preview")
        # Usar sistema CSS dinámico en lugar de estilos hardcodeados
        self.color_preview.setProperty("background_color", self.color_seleccionado)
        
        self.color_btn = QPushButton("Seleccionar Color")
        self.color_btn.clicked.connect(self.seleccionar_color)
        
        color_layout.addWidget(self.color_preview)
        color_layout.addWidget(self.color_btn)
        color_layout.addStretch()
        
        form_layout.addRow("Color:", color_layout)
        
        # Descripción
        self.descripcion_edit = QLineEdit()
        self.descripcion_edit.setPlaceholderText("Descripción opcional...")
        form_layout.addRow("Descripción:", self.descripcion_edit)
        
        layout.addLayout(form_layout)
        
        # Colores predefinidos
        colores_group = QGroupBox("Colores Predefinidos")
        colores_layout = QHBoxLayout(colores_group)
        
        colores_predefinidos = [
            "#e74c3c", "#f39c12", "#f1c40f", "#2ecc71", "#3498db",
            "#9b59b6", "#34495e", "#95a5a6", "#e67e22", "#1abc9c"
        ]
        
        for color in colores_predefinidos:
            color_btn = QPushButton()
            color_btn.setFixedSize(25, 25)
            color_btn.setObjectName("predefined_color_btn")
            # Usar sistema CSS dinámico en lugar de estilos hardcodeados
            color_btn.setProperty("background_color", color)
            color_btn.clicked.connect(lambda checked, c=color: self.establecer_color(c))
            colores_layout.addWidget(color_btn)
        
        layout.addWidget(colores_group)
        
        # Botones
        button_box = QDialogButtonBox(QDialogButtonBox.StandardButton.Ok | QDialogButtonBox.StandardButton.Cancel)
        button_box.accepted.connect(self.accept)
        button_box.rejected.connect(self.reject)
        layout.addWidget(button_box)
    
    def cargar_etiqueta(self):
        """Carga los datos de una etiqueta existente."""
        if self.etiqueta:
            # Manejar tanto objetos como diccionarios
            nombre = getattr(self.etiqueta, 'nombre', None) or self.etiqueta.get('nombre', '') if isinstance(self.etiqueta, dict) else getattr(self.etiqueta, 'nombre', '')
            descripcion = getattr(self.etiqueta, 'descripcion', None) or self.etiqueta.get('descripcion', '') if isinstance(self.etiqueta, dict) else getattr(self.etiqueta, 'descripcion', '')
            color = getattr(self.etiqueta, 'color', None) or self.etiqueta.get('color', '') if isinstance(self.etiqueta, dict) else getattr(self.etiqueta, 'color', '')
            
            self.nombre_edit.setText(nombre or "")
            self.descripcion_edit.setText(descripcion or "")
            if color:
                self.establecer_color(color)
    
    def seleccionar_color(self):
        """Abre el diálogo de selección de color."""
        color = QColorDialog.getColor(QColor(self.color_seleccionado), self)
        if color.isValid():
            self.establecer_color(color.name())
    
    def establecer_color(self, color: str):
        """Establece el color seleccionado."""
        self.color_seleccionado = color
        # Usar sistema CSS dinámico en lugar de estilos hardcodeados
        self.color_preview.setProperty("background_color", color)
        self.color_preview.setObjectName("color_preview")
        
        # Aplicar el color de fondo usando styleSheet para que sea visible
        if color and color.strip():
            self.color_preview.setStyleSheet(f"""
                QLabel#color_preview {{
                    background-color: {color};
                    border-radius: 6px;
                    min-width: 24px;
                    min-height: 24px;
                    border: 2px solid #4C566A;
                    margin: 4px;
                }}
            """)
        else:
            # Color por defecto si no hay color especificado
            self.color_preview.setStyleSheet("""
                QLabel#color_preview {
                    background-color: #4C566A;
                    border-radius: 6px;
                    min-width: 24px;
                    min-height: 24px;
                    border: 2px solid #4C566A;
                    margin: 4px;
                }
            """)
        
        # Forzar actualización del widget
        self.color_preview.update()
    
    def obtener_datos(self) -> dict:
        """Obtiene los datos del formulario."""
        return {
            'nombre': self.nombre_edit.text().strip(),
            'color': self.color_seleccionado,
            'descripcion': self.descripcion_edit.text().strip() or None
        }
    
    def validar_datos(self) -> bool:
        """Valida que los datos sean correctos."""
        datos = self.obtener_datos()
        
        if not datos['nombre']:
            QMessageBox.warning(self, "Error", "El nombre es obligatorio.")
            return False
        
        return True
    
    def accept(self):
        if self.validar_datos():
            super().accept()

class EtiquetaItemWidget(QWidget):
    """Widget para mostrar una etiqueta individual."""
    
    etiqueta_seleccionada = pyqtSignal(Etiqueta, bool)  # etiqueta, seleccionada
    editar_etiqueta = pyqtSignal(Etiqueta)
    eliminar_etiqueta = pyqtSignal(Etiqueta)
    
    def __init__(self, etiqueta, seleccionada: bool = False, modo_seleccion: bool = False, parent=None):
        super().__init__(parent)
        # Convertir diccionario a objeto Etiqueta si es necesario
        self.etiqueta = self._convertir_a_etiqueta(etiqueta)
        self.modo_seleccion = modo_seleccion
        self.seleccionada = seleccionada
        self.setup_ui()
    
    def _convertir_a_etiqueta(self, etiqueta):
        """Convierte un diccionario a objeto Etiqueta si es necesario."""
        if isinstance(etiqueta, dict):
            return Etiqueta(
                id=etiqueta.get('id'),
                nombre=etiqueta.get('nombre', ''),
                color=etiqueta.get('color', ''),
                descripcion=etiqueta.get('descripcion')
            )
        return etiqueta
    
    def setup_ui(self):
        # Contenedor principal con estilo
        self.setObjectName("etiqueta_item_widget")
        
        # Aplicar clase CSS para recuadro flotante
        self.setProperty("class", "etiqueta-item-container")
        self.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Fixed)
        self.setMinimumHeight(60)
        
        layout = QHBoxLayout(self)
        layout.setContentsMargins(16, 12, 16, 12)
        layout.setSpacing(12)
        
        # Checkbox para modo selección
        if self.modo_seleccion:
            self.checkbox = QCheckBox()
            self.checkbox.setChecked(self.seleccionada)
            self.checkbox.setFixedSize(20, 20)
            self.checkbox.setObjectName("etiqueta_checkbox")
            self.checkbox.toggled.connect(lambda checked: self.etiqueta_seleccionada.emit(self._convertir_a_etiqueta(self.etiqueta), checked))
            layout.addWidget(self.checkbox)
        
        # Indicador de color
        color_indicator = QLabel()
        color_indicator.setFixedSize(28, 28)
        color_indicator.setSizePolicy(QSizePolicy.Policy.Fixed, QSizePolicy.Policy.Fixed)
        # Usar sistema CSS dinámico en lugar de estilos hardcodeados
        color_indicator.setObjectName("etiqueta_color_indicator")
        
        # Manejar tanto objetos como diccionarios
        color = self.etiqueta.get('color', '') if isinstance(self.etiqueta, dict) else getattr(self.etiqueta, 'color', '')
        nombre = self.etiqueta.get('nombre', '') if isinstance(self.etiqueta, dict) else getattr(self.etiqueta, 'nombre', '')
        
        color_indicator.setProperty("background_color", color)
        color_indicator.setObjectName("etiqueta_color_indicator")
        
        # Aplicar el color de fondo usando styleSheet para que sea visible
        if color and color.strip():
            color_indicator.setStyleSheet(f"""
                QLabel#etiqueta_color_indicator {{
                    background-color: {color};
                    border-radius: 8px;
                    min-width: 16px;
                    min-height: 16px;
                    max-width: 16px;
                    max-height: 16px;
                    margin: 2px;
                    border: 1px solid #4C566A;
                }}
            """)
        else:
            # Color por defecto si no hay color especificado
            color_indicator.setStyleSheet("""
                QLabel#etiqueta_color_indicator {
                    background-color: #4C566A;
                    border-radius: 8px;
                    min-width: 16px;
                    min-height: 16px;
                    max-width: 16px;
                    max-height: 16px;
                    margin: 2px;
                    border: 1px solid #4C566A;
                }
            """)
        
        layout.addWidget(color_indicator)
        
        # Nombre
        nombre_label = QLabel(nombre)
        nombre_font = QFont()
        nombre_font.setBold(True)
        nombre_font.setPointSize(14)
        nombre_label.setFont(nombre_font)
        nombre_label.setProperty("class", "primary-text")
        nombre_label.setObjectName("etiqueta_nombre_label")
        nombre_label.setSizePolicy(QSizePolicy.Policy.Preferred, QSizePolicy.Policy.Fixed)
        layout.addWidget(nombre_label)
        
        # Descripción
        descripcion = self.etiqueta.get('descripcion', '') if isinstance(self.etiqueta, dict) else getattr(self.etiqueta, 'descripcion', '')
        if descripcion:
            desc_label = QLabel(f"- {descripcion}")
            desc_label.setProperty("class", "secondary-text")
            desc_label.setObjectName("etiqueta_desc_label")
            desc_label.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Fixed)
            layout.addWidget(desc_label)
        
        layout.addStretch()
        
        # Botones de acción (solo si no está en modo selección)
        if not self.modo_seleccion:
            # Contenedor de botones
            botones_layout = QHBoxLayout()
            botones_layout.setSpacing(8)
            
            editar_btn = QPushButton("Editar")
            editar_btn.setFixedSize(70, 32)
            editar_btn.setToolTip("Editar etiqueta")
            editar_btn.setProperty("class", "primary")
            editar_btn.setObjectName("etiqueta_editar_btn")
            editar_btn.setSizePolicy(QSizePolicy.Policy.Fixed, QSizePolicy.Policy.Fixed)
            editar_btn.clicked.connect(lambda: self.editar_etiqueta.emit(self._convertir_a_etiqueta(self.etiqueta)))
            botones_layout.addWidget(editar_btn)
            
            eliminar_btn = QPushButton("Eliminar")
            eliminar_btn.setFixedSize(70, 32)
            eliminar_btn.setToolTip("Eliminar etiqueta")
            eliminar_btn.setProperty("class", "danger")
            eliminar_btn.setObjectName("etiqueta_eliminar_btn")
            eliminar_btn.setSizePolicy(QSizePolicy.Policy.Fixed, QSizePolicy.Policy.Fixed)
            eliminar_btn.clicked.connect(lambda: self.eliminar_etiqueta.emit(self._convertir_a_etiqueta(self.etiqueta)))
            botones_layout.addWidget(eliminar_btn)
            
            layout.addLayout(botones_layout)

class AsignarEtiquetasDialog(QDialog):
    """Diálogo para asignar etiquetas a un usuario."""
    
    def __init__(self, db_manager: DatabaseManager, usuario_id: int, parent=None):
        super().__init__(parent)
        self.db_manager = db_manager
        self.usuario_id = usuario_id
        self.etiquetas_disponibles = []
        self.etiquetas_usuario = []
        self.etiquetas_seleccionadas = set()
        self.setup_ui()
        self.cargar_datos()
    
    def setup_ui(self):
        self.setWindowTitle("Asignar Etiquetas")
        self.setModal(True)
        self.resize(500, 400)
        
        layout = QVBoxLayout(self)
        
        # Título
        titulo = QLabel("Seleccione las etiquetas para el usuario:")
        titulo_font = QFont()
        titulo_font.setBold(True)
        titulo.setFont(titulo_font)
        layout.addWidget(titulo)
        
        # Lista de etiquetas
        self.scroll_area = QScrollArea()
        self.scroll_area.setWidgetResizable(True)
        
        self.etiquetas_container = QWidget()
        self.etiquetas_layout = QVBoxLayout(self.etiquetas_container)
        self.etiquetas_layout.setAlignment(Qt.AlignmentFlag.AlignTop)
        
        self.scroll_area.setWidget(self.etiquetas_container)
        layout.addWidget(self.scroll_area)
        
        # Botones
        button_box = QDialogButtonBox(QDialogButtonBox.StandardButton.Ok | QDialogButtonBox.StandardButton.Cancel)
        button_box.accepted.connect(self.accept)
        button_box.rejected.connect(self.reject)
        layout.addWidget(button_box)
    
    def cargar_datos(self):
        """Carga las etiquetas disponibles y las del usuario."""
        try:
            self.etiquetas_disponibles = self.db_manager.obtener_etiquetas()
            # obtener_etiquetas_usuario devuelve objetos Etiqueta directamente
            etiquetas_usuario = self.db_manager.obtener_etiquetas_usuario(self.usuario_id)
            etiquetas_usuario_ids = [e.id if hasattr(e, 'id') else e.get('id') for e in etiquetas_usuario]
            self.etiquetas_seleccionadas = set(etiquetas_usuario_ids)
            self.mostrar_etiquetas()
        except Exception as e:
            QMessageBox.critical(self, "Error", f"Error al cargar etiquetas: {str(e)}")
    
    def mostrar_etiquetas(self):
        """Muestra las etiquetas en la interfaz."""
        # Limpiar layout
        while self.etiquetas_layout.count():
            child = self.etiquetas_layout.takeAt(0)
            if child.widget():
                child.widget().deleteLater()
        
        if not self.etiquetas_disponibles:
            mensaje = QLabel("No hay etiquetas disponibles. Cree algunas primero.")
            mensaje.setAlignment(Qt.AlignmentFlag.AlignCenter)
            # Usar sistema CSS dinámico en lugar de estilos hardcodeados
            mensaje.setProperty("class", "muted-text")
            mensaje.setObjectName("etiquetas_mensaje_vacio")
            self.etiquetas_layout.addWidget(mensaje)
            return
        
        for etiqueta in self.etiquetas_disponibles:
            etiqueta_id = etiqueta.id if hasattr(etiqueta, 'id') else etiqueta.get('id')
            seleccionada = etiqueta_id in self.etiquetas_seleccionadas
            etiqueta_widget = EtiquetaItemWidget(etiqueta, seleccionada, modo_seleccion=True)
            etiqueta_widget.etiqueta_seleccionada.connect(self.on_etiqueta_seleccionada)
            self.etiquetas_layout.addWidget(etiqueta_widget)
    
    def on_etiqueta_seleccionada(self, etiqueta: Etiqueta, seleccionada: bool):
        """Maneja la selección/deselección de etiquetas."""
        etiqueta_id = etiqueta.id if hasattr(etiqueta, 'id') else etiqueta.get('id')
        if seleccionada:
            self.etiquetas_seleccionadas.add(etiqueta_id)
        else:
            self.etiquetas_seleccionadas.discard(etiqueta_id)
    
    def obtener_etiquetas_seleccionadas(self) -> List[int]:
        """Obtiene la lista de IDs de etiquetas seleccionadas."""
        return list(self.etiquetas_seleccionadas)

class EtiquetasWidget(QWidget):
    """Widget principal para gestión de etiquetas."""
    
    # Señales
    etiquetas_changed = pyqtSignal()  # Señal general para cambios
    
    def __init__(self, db_manager: DatabaseManager, parent=None):
        super().__init__(parent)
        self.db_manager = db_manager
        self.usuario_actual = None
        self.etiquetas_disponibles = []
        self.etiquetas_usuario = []
        self.main_window = None
        self.setup_ui()
        self.conectar_señales()
        self.connect_accessibility_signals()
        self.cargar_etiquetas_disponibles()
    
    def setup_ui(self):
        layout = QVBoxLayout(self)
        layout.setContentsMargins(20, 20, 20, 20)
        layout.setSpacing(16)
        
        # Configurar política de tamaño del widget principal
        self.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Expanding)
        
        # Título
        titulo = QLabel("🏷️ Gestión de Etiquetas")
        titulo_font = QFont()
        titulo_font.setBold(True)
        titulo_font.setPointSize(16)
        titulo.setFont(titulo_font)
        titulo.setSizePolicy(QSizePolicy.Policy.Preferred, QSizePolicy.Policy.Fixed)
        titulo.setObjectName("etiquetas_titulo")
        layout.addWidget(titulo)
        
        # Filtros
        filtros_group = QGroupBox("Filtros")
        filtros_group.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Expanding)
        filtros_group.setMinimumHeight(120)
        filtros_group.setMaximumHeight(150)
        filtros_group.setObjectName("etiquetas_filtros_group")
        filtros_layout = QHBoxLayout(filtros_group)
        filtros_layout.setContentsMargins(16, 25, 16, 16)
        filtros_layout.setSpacing(20)
        
        # Filtro por color
        color_label = QLabel("Color:")
        color_label.setObjectName("etiquetas_filter_label")
        self.color_filtro = QComboBox()
        self.color_filtro.setMinimumWidth(130)
        self.color_filtro.addItems(["Todos", "Rojo", "Verde", "Azul", "Amarillo", "Naranja", "Morado", "Rosa"])
        self.color_filtro.setCurrentIndex(0)
        filtros_layout.addWidget(color_label)
        filtros_layout.addWidget(self.color_filtro)
        
        # Filtro por texto
        texto_label = QLabel("Buscar:")
        texto_label.setObjectName("etiquetas_filter_label")
        self.texto_filtro = QLineEdit()
        self.texto_filtro.setPlaceholderText("Buscar etiquetas...")
        self.texto_filtro.setMinimumWidth(200)
        filtros_layout.addWidget(texto_label)
        filtros_layout.addWidget(self.texto_filtro)
        
        filtros_layout.addStretch()
        layout.addWidget(filtros_group)
        
        # Controles superiores en widget contenedor
        controles_widget = QWidget()
        controles_widget.setFixedHeight(50)
        controles_widget.setObjectName("etiquetas_controles_widget")
        controles_layout = QHBoxLayout(controles_widget)
        controles_layout.setContentsMargins(16, 8, 16, 8)
        controles_layout.setSpacing(12)
        
        # Gestión de etiquetas
        self.crear_etiqueta_btn = QPushButton("➕ Crear Etiqueta")
        self.crear_etiqueta_btn.setMinimumHeight(34)
        self.crear_etiqueta_btn.setSizePolicy(QSizePolicy.Policy.Fixed, QSizePolicy.Policy.Fixed)
        self.crear_etiqueta_btn.setObjectName("etiquetas_crear_btn")
        controles_layout.addWidget(self.crear_etiqueta_btn)
        
        controles_layout.addStretch()
        
        # Asignar etiquetas al usuario
        self.asignar_etiquetas_btn = QPushButton("🏷️ Asignar Etiquetas")
        self.asignar_etiquetas_btn.setEnabled(False)
        self.asignar_etiquetas_btn.setMinimumHeight(34)
        self.asignar_etiquetas_btn.setSizePolicy(QSizePolicy.Policy.Fixed, QSizePolicy.Policy.Fixed)
        self.asignar_etiquetas_btn.setObjectName("etiquetas_asignar_btn")
        controles_layout.addWidget(self.asignar_etiquetas_btn)
        
        layout.addWidget(controles_widget)
        
        
        usuario_section = QGroupBox("Etiquetas del Usuario")
        usuario_section.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Preferred)
        usuario_section.setObjectName("etiquetas_usuario_section")
        usuario_layout = QVBoxLayout(usuario_section)
        usuario_layout.setContentsMargins(16, 20, 16, 16)
        usuario_layout.setSpacing(12)
        
        self.etiquetas_usuario_scroll = QScrollArea()
        self.etiquetas_usuario_scroll.setWidgetResizable(True)
        self.etiquetas_usuario_scroll.setMinimumHeight(100)
        self.etiquetas_usuario_scroll.setMaximumHeight(200)
        self.etiquetas_usuario_scroll.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Preferred)
        self.etiquetas_usuario_scroll.setHorizontalScrollBarPolicy(Qt.ScrollBarPolicy.ScrollBarAlwaysOff)
        self.etiquetas_usuario_scroll.setVerticalScrollBarPolicy(Qt.ScrollBarPolicy.ScrollBarAsNeeded)
        self.etiquetas_usuario_scroll.setObjectName("etiquetas_scroll_area")
        
        self.etiquetas_usuario_container = QWidget()
        self.etiquetas_usuario_container.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Fixed)
        self.etiquetas_usuario_layout = QVBoxLayout(self.etiquetas_usuario_container)
        self.etiquetas_usuario_layout.setAlignment(Qt.AlignmentFlag.AlignTop)
        self.etiquetas_usuario_layout.setSpacing(6)
        self.etiquetas_usuario_layout.setContentsMargins(8, 8, 8, 8)
        
        self.etiquetas_usuario_scroll.setWidget(self.etiquetas_usuario_container)
        usuario_layout.addWidget(self.etiquetas_usuario_scroll)
        
        self.mensaje_usuario_vacio = QLabel("Seleccione un usuario para ver sus etiquetas")
        self.mensaje_usuario_vacio.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self.mensaje_usuario_vacio.setProperty("class", "muted-text")
        self.mensaje_usuario_vacio.setObjectName("etiquetas_mensaje_vacio")
        self.mensaje_usuario_vacio.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Preferred)
        usuario_layout.addWidget(self.mensaje_usuario_vacio)
        
        layout.addWidget(usuario_section)
        
        
        todas_section = QGroupBox("Todas las Etiquetas")
        todas_section.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Expanding)
        todas_section.setObjectName("etiquetas_todas_section")
        todas_layout = QVBoxLayout(todas_section)
        todas_layout.setContentsMargins(16, 20, 16, 16)
        todas_layout.setSpacing(12)
        
        self.todas_etiquetas_scroll = QScrollArea()
        self.todas_etiquetas_scroll.setWidgetResizable(True)
        # Permitir expansión adaptativa del área de scroll de etiquetas
        # self.todas_etiquetas_scroll.setMinimumHeight(150)  # Removido para permitir expansión
        # self.todas_etiquetas_scroll.setMaximumHeight(250)  # Removido para permitir expansión
        self.todas_etiquetas_scroll.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Expanding)
        self.todas_etiquetas_scroll.setHorizontalScrollBarPolicy(Qt.ScrollBarPolicy.ScrollBarAlwaysOff)
        self.todas_etiquetas_scroll.setVerticalScrollBarPolicy(Qt.ScrollBarPolicy.ScrollBarAsNeeded)
        self.todas_etiquetas_scroll.setObjectName("etiquetas_scroll_area")
        
        self.todas_etiquetas_container = QWidget()
        self.todas_etiquetas_container.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Fixed)
        self.todas_etiquetas_layout = QVBoxLayout(self.todas_etiquetas_container)
        self.todas_etiquetas_layout.setAlignment(Qt.AlignmentFlag.AlignTop)
        self.todas_etiquetas_layout.setSpacing(6)
        self.todas_etiquetas_layout.setContentsMargins(8, 8, 8, 8)
        
        self.todas_etiquetas_scroll.setWidget(self.todas_etiquetas_container)
        todas_layout.addWidget(self.todas_etiquetas_scroll)
        
        layout.addWidget(todas_section)
        
        # Inicialmente ocultar scroll de usuario
        self.etiquetas_usuario_scroll.hide()
    
    def conectar_señales(self):
        """Conecta las señales de los widgets."""
        self.crear_etiqueta_btn.clicked.connect(self.crear_etiqueta)
        self.asignar_etiquetas_btn.clicked.connect(self.asignar_etiquetas)
        
        # Conectar filtros
        self.color_filtro.currentTextChanged.connect(self.aplicar_filtros)
        self.texto_filtro.textChanged.connect(self.aplicar_filtros)
    
    def establecer_usuario(self, usuario):
        """Establece el usuario actual y carga sus etiquetas."""
        self.usuario_actual = usuario
        # Bloquear asignación de etiquetas para usuario 'dueño'
        enabled = usuario is not None and getattr(usuario, 'rol', None) != 'dueño'
        self.asignar_etiquetas_btn.setEnabled(enabled)
        try:
            if usuario and getattr(usuario, 'rol', None) == 'dueño':
                self.asignar_etiquetas_btn.setToolTip("Bloqueado para usuario Dueño")
            else:
                self.asignar_etiquetas_btn.setToolTip("")
        except Exception:
            pass
        
        if usuario:
            self.cargar_etiquetas_usuario()
            self.mensaje_usuario_vacio.hide()
            self.etiquetas_usuario_scroll.show()
        else:
            self.limpiar_etiquetas_usuario()
            self.etiquetas_usuario_scroll.hide()
            self.mensaje_usuario_vacio.show()
    
    def cargar_etiquetas_disponibles(self):
        """Carga todas las etiquetas disponibles."""
        try:
            self.etiquetas_disponibles = self.db_manager.obtener_etiquetas()
            self.mostrar_todas_etiquetas()
        except Exception as e:
            QMessageBox.critical(self, "Error", f"Error al cargar etiquetas: {str(e)}")
    
    def cargar_etiquetas_usuario(self):
        """Carga las etiquetas del usuario actual."""
        if not self.usuario_actual:
            return
        
        try:
            # obtener_etiquetas_usuario devuelve objetos Etiqueta directamente
            self.etiquetas_usuario = self.db_manager.obtener_etiquetas_usuario(self.usuario_actual.id)
            self.mostrar_etiquetas_usuario()
        except Exception as e:
            QMessageBox.critical(self, "Error", f"Error al cargar etiquetas del usuario: {str(e)}")
    
    def mostrar_todas_etiquetas(self):
        """Muestra todas las etiquetas disponibles."""
        self.limpiar_todas_etiquetas()
        
        if not self.etiquetas_disponibles:
            mensaje = QLabel("No hay etiquetas creadas")
            mensaje.setAlignment(Qt.AlignmentFlag.AlignCenter)
            mensaje.setProperty("class", "muted-text")
            mensaje.setObjectName("etiquetas_mensaje_vacio")
            self.todas_etiquetas_layout.addWidget(mensaje)
            return
        
        for etiqueta in self.etiquetas_disponibles:
            etiqueta_widget = EtiquetaItemWidget(etiqueta, modo_seleccion=False)
            etiqueta_widget.editar_etiqueta.connect(self.editar_etiqueta)
            etiqueta_widget.eliminar_etiqueta.connect(self.eliminar_etiqueta)
            self.todas_etiquetas_layout.addWidget(etiqueta_widget)
    
    def mostrar_etiquetas_usuario(self):
        """Muestra las etiquetas del usuario actual."""
        self.limpiar_etiquetas_usuario()
        
        if not self.etiquetas_usuario:
            mensaje = QLabel("El usuario no tiene etiquetas asignadas")
            mensaje.setAlignment(Qt.AlignmentFlag.AlignCenter)
            mensaje.setProperty("class", "muted-text")
            mensaje.setObjectName("etiquetas_mensaje_vacio")
            self.etiquetas_usuario_layout.addWidget(mensaje)
            return
        
        for etiqueta in self.etiquetas_usuario:
            etiqueta_widget = EtiquetaItemWidget(etiqueta, modo_seleccion=False)
            self.etiquetas_usuario_layout.addWidget(etiqueta_widget)
    
    def limpiar_todas_etiquetas(self):
        """Limpia la lista de todas las etiquetas."""
        while self.todas_etiquetas_layout.count():
            child = self.todas_etiquetas_layout.takeAt(0)
            if child.widget():
                child.widget().deleteLater()
    
    def limpiar_etiquetas_usuario(self):
        """Limpia la lista de etiquetas del usuario."""
        while self.etiquetas_usuario_layout.count():
            child = self.etiquetas_usuario_layout.takeAt(0)
            if child.widget():
                child.widget().deleteLater()
    
    def aplicar_filtros(self):
        """Aplica los filtros seleccionados a las etiquetas."""
        color_filtro = self.color_filtro.currentText()
        texto_filtro = self.texto_filtro.text().lower().strip()
        
        # Filtrar etiquetas disponibles
        etiquetas_filtradas = []
        for etiqueta in self.etiquetas_disponibles:
            # Manejar tanto objetos como diccionarios
            color = etiqueta.get('color', '') if isinstance(etiqueta, dict) else getattr(etiqueta, 'color', '')
            nombre = etiqueta.get('nombre', '') if isinstance(etiqueta, dict) else getattr(etiqueta, 'nombre', '')
            
            # Filtro por color
            if color_filtro != "Todos" and color.lower() != color_filtro.lower():
                continue
            
            # Filtro por texto
            if texto_filtro and texto_filtro not in nombre.lower():
                continue
            
            etiquetas_filtradas.append(etiqueta)
        
        # Mostrar etiquetas filtradas
        self.limpiar_todas_etiquetas()
        
        if not etiquetas_filtradas:
            mensaje = QLabel("No se encontraron etiquetas que coincidan con los filtros")
            mensaje.setAlignment(Qt.AlignmentFlag.AlignCenter)
            mensaje.setProperty("class", "muted-text")
            mensaje.setObjectName("etiquetas_mensaje_vacio")
            self.todas_etiquetas_layout.addWidget(mensaje)
            return
        
        for etiqueta in etiquetas_filtradas:
            etiqueta_widget = EtiquetaItemWidget(etiqueta, modo_seleccion=False)
            etiqueta_widget.editar_etiqueta.connect(self.editar_etiqueta)
            etiqueta_widget.eliminar_etiqueta.connect(self.eliminar_etiqueta)
            self.todas_etiquetas_layout.addWidget(etiqueta_widget)
    
    def crear_etiqueta(self):
        """Abre el diálogo para crear una nueva etiqueta."""
        dialog = EtiquetaDialog(self)
        if dialog.exec() == QDialog.DialogCode.Accepted:
            datos = dialog.obtener_datos()
            
            nueva_etiqueta = Etiqueta(
                nombre=datos['nombre'],
                color=datos['color'],
                descripcion=datos['descripcion']
            )
            
            try:
                etiqueta_id = self.db_manager.crear_etiqueta(nueva_etiqueta)
                nueva_etiqueta.id = etiqueta_id
                QMessageBox.information(self, "Éxito", "Etiqueta creada correctamente.")
                self.cargar_etiquetas_disponibles()
                self.etiquetas_changed.emit()
            except Exception as e:
                QMessageBox.critical(self, "Error", f"Error al crear etiqueta: {str(e)}")
    
    def editar_etiqueta(self, etiqueta: Etiqueta):
        """Abre el diálogo para editar una etiqueta."""
        dialog = EtiquetaDialog(self, etiqueta=etiqueta)
        if dialog.exec() == QDialog.DialogCode.Accepted:
            datos = dialog.obtener_datos()
            
            # Manejar tanto objetos como diccionarios
            if isinstance(etiqueta, dict):
                etiqueta['nombre'] = datos['nombre']
                etiqueta['color'] = datos['color']
                etiqueta['descripcion'] = datos['descripcion']
            else:
                etiqueta.nombre = datos['nombre']
                etiqueta.color = datos['color']
                etiqueta.descripcion = datos['descripcion']
            
            try:
                self.db_manager.actualizar_etiqueta(etiqueta)
                QMessageBox.information(self, "Éxito", "Etiqueta actualizada correctamente.")
                self.cargar_etiquetas_disponibles()
                if self.usuario_actual:
                    self.cargar_etiquetas_usuario()
                self.etiquetas_changed.emit()
            except Exception as e:
                QMessageBox.critical(self, "Error", f"Error al actualizar etiqueta: {str(e)}")
    
    def eliminar_etiqueta(self, etiqueta: Etiqueta):
        """Elimina una etiqueta después de confirmar."""
        respuesta = QMessageBox.question(
            self, "Confirmar eliminación",
            f"¿Está seguro de que desea eliminar la etiqueta '{etiqueta.nombre}'?\n\n"
            "Esto también eliminará todas las asignaciones de esta etiqueta a usuarios.",
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
            QMessageBox.StandardButton.No
        )
        
        if respuesta == QMessageBox.StandardButton.Yes:
            try:
                self.db_manager.eliminar_etiqueta(etiqueta.id)
                QMessageBox.information(self, "Éxito", "Etiqueta eliminada correctamente.")
                self.cargar_etiquetas_disponibles()
                if self.usuario_actual:
                    self.cargar_etiquetas_usuario()
                self.etiquetas_changed.emit()
            except Exception as e:
                QMessageBox.critical(self, "Error", f"Error al eliminar etiqueta: {str(e)}")
    
    def asignar_etiquetas(self):
        """Abre el diálogo para asignar etiquetas al usuario actual."""
        if not self.usuario_actual:
            return
        
        dialog = AsignarEtiquetasDialog(self.db_manager, self.usuario_actual.id, self)
        if dialog.exec() == QDialog.DialogCode.Accepted:
            etiquetas_seleccionadas = dialog.obtener_etiquetas_seleccionadas()
            
            try:
                # Primero desasignar todas las etiquetas del usuario
                etiquetas_actuales = self.db_manager.obtener_etiquetas_usuario(self.usuario_actual.id)
                for etiqueta in etiquetas_actuales:
                    self.db_manager.desasignar_etiqueta_usuario(self.usuario_actual.id, etiqueta.id)
                
                # Luego asignar las nuevas etiquetas
                for etiqueta_id in etiquetas_seleccionadas:
                    self.db_manager.asignar_etiqueta_usuario(self.usuario_actual.id, etiqueta_id)
                
                QMessageBox.information(self, "Éxito", "Etiquetas asignadas correctamente.")
                self.cargar_etiquetas_usuario()
                self.etiquetas_changed.emit()
            except Exception as e:
                QMessageBox.critical(self, "Error", f"Error al asignar etiquetas: {str(e)}")
    
    def set_user(self, usuario):
        """Establece el usuario actual (método requerido por UserTabWidget)."""
        self.establecer_usuario(usuario)
    
    def clear(self):
        """Limpia el widget (método requerido por UserTabWidget)."""
        self.establecer_usuario(None)
    
    def set_main_window(self, main_window):
        """Establece la referencia a la ventana principal."""
        self.main_window = main_window
    
    def connect_accessibility_signals(self):
        """Conecta las señales de accesibilidad con la ventana principal."""
        # Se conectarán cuando se establezca main_window
        pass
    
    def apply_branding(self, theme_config: dict):
        """Aplica el branding automático al widget usando el sistema CSS dinámico."""
        if not theme_config:
            return
        
        try:
            # Configurar propiedades para el sistema CSS dinámico
            if 'colors' in theme_config:
                colors = theme_config['colors']
                
                # Establecer propiedades de tema en lugar de estilos hardcodeados
                self.setProperty('branding_theme', 'etiquetas')
                self.setProperty('primary_color', colors.get('primary', '#007bff'))
                self.setProperty('background_color', colors.get('background_color', '#ffffff'))
                self.setProperty('text_color', colors.get('text', '#000000'))
                
                # Aplicar el sistema CSS dinámico
                main_window = self.get_main_window()
                if main_window and hasattr(main_window, 'apply_dynamic_styles'):
                    main_window.apply_dynamic_styles()
                
        except Exception as e:
            print(f"Error aplicando branding a EtiquetasWidget: {e}")
    
    def get_main_window(self):
        """Obtiene la ventana principal para aplicar estilos dinámicos."""
        parent = self.parent()
        while parent:
            if hasattr(parent, 'objectName') and parent.objectName() == 'MainWindow':
                return parent
            parent = parent.parent()
        
        # Fallback: buscar en la aplicación
        from PyQt6.QtWidgets import QApplication
        app = QApplication.instance()
        if app:
            for widget in app.topLevelWidgets():
                if hasattr(widget, 'objectName') and widget.objectName() == 'MainWindow':
                    return widget
        return None

