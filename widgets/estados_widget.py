from PyQt6.QtWidgets import (
    QWidget, QVBoxLayout, QHBoxLayout, QGroupBox, QLineEdit, QPushButton,
    QListWidget, QListWidgetItem, QLabel, QMessageBox, QDialog, QDialogButtonBox,
    QFormLayout, QDateEdit, QFrame, QScrollArea, QCheckBox, QComboBox, QTextEdit,
    QSizePolicy
)
from PyQt6.QtCore import Qt, pyqtSignal, QDate, QDateTime, QTimer
from PyQt6.QtGui import QFont, QColor, QPalette
from datetime import datetime, timedelta
from typing import List, Optional
from models import UsuarioEstado
from database import DatabaseManager

class EstadoDialog(QDialog):
    """Diálogo para crear/editar estados temporales de usuario."""
    
    def __init__(self, parent=None, estado: Optional[UsuarioEstado] = None, usuario_id: int = None):
        super().__init__(parent)
        self.estado = estado
        self.usuario_id = usuario_id
        self.setup_ui()
        
        if estado:
            self.cargar_estado()
    
    def setup_ui(self):
        self.setWindowTitle("Crear Estado" if not self.estado else "Editar Estado")
        self.setModal(True)
        self.resize(450, 350)
        
        layout = QVBoxLayout(self)
        
        # Formulario
        form_layout = QFormLayout()
        
        # Nombre del estado
        self.nombre_edit = QLineEdit()
        self.nombre_edit.setPlaceholderText("Ej: Suspendido, En observación, VIP...")
        form_layout.addRow("Estado:", self.nombre_edit)
        
        # Descripción
        self.descripcion_edit = QTextEdit()
        self.descripcion_edit.setPlaceholderText("Descripción del estado y motivo...")
        self.descripcion_edit.setMaximumHeight(80)
        form_layout.addRow("Descripción:", self.descripcion_edit)
        
        # Fecha de inicio
        self.fecha_inicio_edit = QDateEdit()
        self.fecha_inicio_edit.setDate(QDate.currentDate())
        self.fecha_inicio_edit.setCalendarPopup(True)
        form_layout.addRow("Fecha Inicio:", self.fecha_inicio_edit)
        
        # Fecha de fin
        fecha_fin_layout = QHBoxLayout()
        self.fecha_fin_edit = QDateEdit()
        self.fecha_fin_edit.setDate(QDate.currentDate().addDays(30))  # 30 días por defecto
        self.fecha_fin_edit.setCalendarPopup(True)
        
        self.sin_vencimiento_check = QCheckBox("Sin vencimiento")
        self.sin_vencimiento_check.toggled.connect(self.toggle_fecha_fin)
        
        fecha_fin_layout.addWidget(self.fecha_fin_edit)
        fecha_fin_layout.addWidget(self.sin_vencimiento_check)
        
        form_layout.addRow("Fecha Fin:", fecha_fin_layout)
        
        # Duración rápida
        duracion_layout = QHBoxLayout()
        
        duracion_label = QLabel("Duración rápida:")
        duracion_layout.addWidget(duracion_label)
        
        # Botones de duración predefinida
        duraciones = [
            ("1 día", 1),
            ("1 semana", 7),
            ("1 mes", 30),
            ("3 meses", 90)
        ]
        
        for texto, dias in duraciones:
            btn = QPushButton(texto)
            btn.clicked.connect(lambda checked, d=dias: self.establecer_duracion(d))
            duracion_layout.addWidget(btn)
        
        duracion_layout.addStretch()
        form_layout.addRow("", duracion_layout)
        
        layout.addLayout(form_layout)
        
        # Información adicional
        info_group = QGroupBox("Información")
        info_layout = QVBoxLayout(info_group)
        
        self.info_label = QLabel()
        self.info_label.setProperty("class", "muted-text")
        self.info_label.setStyleSheet("font-size: 9px;")
        self.actualizar_info()
        info_layout.addWidget(self.info_label)
        
        layout.addWidget(info_group)
        
        # Conectar señales para actualizar info
        self.fecha_inicio_edit.dateChanged.connect(self.actualizar_info)
        self.fecha_fin_edit.dateChanged.connect(self.actualizar_info)
        self.sin_vencimiento_check.toggled.connect(self.actualizar_info)
        
        # Botones
        button_box = QDialogButtonBox(QDialogButtonBox.StandardButton.Ok | QDialogButtonBox.StandardButton.Cancel)
        button_box.accepted.connect(self.accept)
        button_box.rejected.connect(self.reject)
        layout.addWidget(button_box)
    
    def cargar_estado(self):
        """Carga los datos de un estado existente."""
        if self.estado:
            # Manejar tanto objetos como diccionarios
            estado_nombre = getattr(self.estado, 'estado', '')
            descripcion = getattr(self.estado, 'descripcion', '')
            fecha_inicio = getattr(self.estado, 'fecha_inicio', None)
            fecha_vencimiento = getattr(self.estado, 'fecha_vencimiento', None)
            
            self.nombre_edit.setText(estado_nombre or "")
            self.descripcion_edit.setPlainText(descripcion or "")
            
            if fecha_inicio:
                try:
                    fecha_inicio_str = fecha_inicio.replace('Z', '+00:00') if isinstance(fecha_inicio, str) else str(fecha_inicio)
                    fecha_inicio_dt = datetime.fromisoformat(fecha_inicio_str)
                    self.fecha_inicio_edit.setDate(QDate(fecha_inicio_dt.year, fecha_inicio_dt.month, fecha_inicio_dt.day))
                except:
                    pass
            
            if fecha_vencimiento:
                try:
                    fecha_vencimiento_str = fecha_vencimiento.replace('Z', '+00:00') if isinstance(fecha_vencimiento, str) else str(fecha_vencimiento)
                    fecha_vencimiento_dt = datetime.fromisoformat(fecha_vencimiento_str)
                    self.fecha_fin_edit.setDate(QDate(fecha_vencimiento_dt.year, fecha_vencimiento_dt.month, fecha_vencimiento_dt.day))
                except:
                    pass
            else:
                self.sin_vencimiento_check.setChecked(True)
    
    def toggle_fecha_fin(self, sin_vencimiento: bool):
        """Habilita/deshabilita la fecha de fin."""
        self.fecha_fin_edit.setEnabled(not sin_vencimiento)
    
    def establecer_duracion(self, dias: int):
        """Establece la fecha de fin basada en la duración."""
        fecha_inicio = self.fecha_inicio_edit.date()
        fecha_fin = fecha_inicio.addDays(dias)
        self.fecha_fin_edit.setDate(fecha_fin)
        self.sin_vencimiento_check.setChecked(False)
    
    def actualizar_info(self):
        """Actualiza la información mostrada."""
        if self.sin_vencimiento_check.isChecked():
            info_text = "Estado permanente (sin fecha de vencimiento)"
        else:
            fecha_inicio = self.fecha_inicio_edit.date()
            fecha_fin = self.fecha_fin_edit.date()
            dias = fecha_inicio.daysTo(fecha_fin)
            
            if dias < 0:
                info_text = "⚠️ La fecha de fin es anterior a la fecha de inicio"
            elif dias == 0:
                info_text = "Estado válido solo por hoy"
            else:
                info_text = f"Duración: {dias} día{'s' if dias != 1 else ''}"
        
        self.info_label.setText(info_text)
    
    def obtener_datos(self) -> dict:
        """Obtiene los datos del formulario."""
        fecha_inicio = self.fecha_inicio_edit.date().toPyDate()
        fecha_fin = None if self.sin_vencimiento_check.isChecked() else self.fecha_fin_edit.date().toPyDate()
        
        return {
            'estado': self.nombre_edit.text().strip(),
            'descripcion': self.descripcion_edit.toPlainText().strip() or None,
            'fecha_inicio': fecha_inicio.isoformat(),
            'fecha_fin': fecha_fin.isoformat() if fecha_fin else None
        }
    
    def validar_datos(self) -> bool:
        """Valida que los datos sean correctos."""
        datos = self.obtener_datos()
        
        if not datos['estado']:
            QMessageBox.warning(self, "Error", "El nombre del estado es obligatorio.")
            return False
        
        if datos['fecha_fin']:
            fecha_inicio = datetime.fromisoformat(datos['fecha_inicio'])
            fecha_fin = datetime.fromisoformat(datos['fecha_fin'])
            
            if fecha_fin <= fecha_inicio:
                QMessageBox.warning(self, "Error", "La fecha de fin debe ser posterior a la fecha de inicio.")
                return False
        
        return True
    
    def accept(self):
        if self.validar_datos():
            super().accept()

class EstadoItemWidget(QWidget):
    """Widget para mostrar un estado individual."""
    
    estado_seleccionado = pyqtSignal(UsuarioEstado)
    editar_estado = pyqtSignal(UsuarioEstado)
    eliminar_estado = pyqtSignal(UsuarioEstado)
    
    def __init__(self, estado: UsuarioEstado, parent=None):
        super().__init__(parent)
        self.estado = self._convertir_a_usuario_estado(estado)
        self.setup_ui()
        
        # Timer para actualizar el estado de vencimiento
        self.timer = QTimer()
        self.timer.timeout.connect(self.actualizar_estado_vencimiento)
        self.timer.start(60000)  # Actualizar cada minuto
    
    def _convertir_a_usuario_estado(self, estado):
        """Convierte un diccionario a objeto UsuarioEstado si es necesario."""
        if isinstance(estado, dict):
            return UsuarioEstado(
                id=estado.get('id'),
                usuario_id=estado.get('usuario_id', 0),
                estado=estado.get('estado', ''),
                descripcion=estado.get('descripcion'),
                fecha_inicio=estado.get('fecha_inicio'),
                fecha_vencimiento=estado.get('fecha_vencimiento'),
                activo=estado.get('activo', True),
                creado_por=estado.get('creado_por')
            )
        return estado
    
    def setup_ui(self):
        layout = QVBoxLayout(self)
        layout.setContentsMargins(8, 8, 8, 8)
        layout.setSpacing(6)
        
        # Configurar el contenedor principal
        self.setProperty("class", "estado-item-container")
        self.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Fixed)
        self.setMinimumHeight(100)
        # estilos movidos a QSS global
        self.setStyleSheet("""
            QWidget[class="estado-item-container"] {
                background-color: #ffffff;
                border: 1px solid #e9ecef;
                border-radius: 6px;
                margin: 2px;
            }
            QWidget[class="estado-item-container"]:hover {
                background-color: #f8f9fa;
                border-color: #007bff;
            }
        """)
        
        # Header con estado y indicador
        header_layout = QHBoxLayout()
        header_layout.setContentsMargins(0, 0, 0, 0)
        header_layout.setSpacing(8)
        
        # Indicador de estado (círculo de color)
        self.indicador_estado = QLabel()
        self.indicador_estado.setFixedSize(12, 12)
        self.indicador_estado.setProperty("class", "status-indicator")
        # estilos movidos a QSS global
        self.indicador_estado.setStyleSheet("""
            QLabel[class="status-indicator"] {
                border-radius: 6px;
                border: 1px solid #ffffff;
            }
        """)
        header_layout.addWidget(self.indicador_estado)
        
        # Nombre del estado
        estado_nombre = getattr(self.estado, 'estado', '')
        self.estado_label = QLabel(estado_nombre or "")
        estado_font = QFont()
        estado_font.setBold(True)
        estado_font.setPointSize(10)
        self.estado_label.setFont(estado_font)
        self.estado_label.setProperty("class", "primary-text")
        # estilos movidos a QSS global
        self.estado_label.setStyleSheet("color: #2c3e50; font-weight: bold;")
        header_layout.addWidget(self.estado_label)
        
        header_layout.addStretch()
        
        # Badge de estado
        self.estado_badge = QLabel()
        self.estado_badge.setProperty("class", "status-badge")
        self.estado_badge.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self.estado_badge.setFixedHeight(18)
        self.estado_badge.setMinimumWidth(50)
        # estilos movidos a QSS global
        self.estado_badge.setStyleSheet("""
            QLabel[class="status-badge"] {
                border-radius: 9px;
                font-size: 8px;
                font-weight: bold;
                padding: 2px 6px;
            }
        """)
        header_layout.addWidget(self.estado_badge)
        
        layout.addLayout(header_layout)
        
        # Descripción
        descripcion = getattr(self.estado, 'descripcion', '')
        if descripcion:
            self.descripcion_label = QLabel(descripcion)
            self.descripcion_label.setWordWrap(True)
            self.descripcion_label.setProperty("class", "secondary-text")
            descripcion_font = QFont()
            descripcion_font.setPointSize(8)
            self.descripcion_label.setFont(descripcion_font)
            self.descripcion_label.setStyleSheet("color: #6c757d; margin: 2px 0px;")
            layout.addWidget(self.descripcion_label)
        
        # Fechas
        fechas_layout = QHBoxLayout()
        fechas_layout.setContentsMargins(0, 0, 0, 0)
        fechas_layout.setSpacing(12)
        
        fecha_inicio = getattr(self.estado, 'fecha_inicio', None)
        fecha_vencimiento = getattr(self.estado, 'fecha_vencimiento', None)
        
        if fecha_inicio:
            inicio_label = QLabel(f"📅 Inicio: {self.formato_fecha(fecha_inicio)}")
            inicio_label.setProperty("class", "muted-text")
            inicio_font = QFont()
            inicio_font.setPointSize(7)
            inicio_label.setFont(inicio_font)
            # estilos movidos a QSS global
            inicio_label.setStyleSheet("color: #868e96; font-size: 7px;")
            fechas_layout.addWidget(inicio_label)
        
        if fecha_vencimiento:
            vencimiento_label = QLabel(f"⏰ Vence: {self.formato_fecha(fecha_vencimiento)}")
            vencimiento_label.setProperty("class", "muted-text")
            vencimiento_font = QFont()
            vencimiento_font.setPointSize(7)
            vencimiento_label.setFont(vencimiento_font)
            # estilos movidos a QSS global
            vencimiento_label.setStyleSheet("color: #868e96; font-size: 7px;")
            fechas_layout.addWidget(vencimiento_label)
        
        fechas_layout.addStretch()
        layout.addLayout(fechas_layout)
        
        # Botones de acción
        botones_layout = QHBoxLayout()
        botones_layout.setContentsMargins(0, 4, 0, 0)
        botones_layout.setSpacing(6)
        botones_layout.addStretch()
        
        self.editar_btn = QPushButton("✏️ Editar")
        self.editar_btn.setProperty("class", "primary")
        self.editar_btn.setFixedSize(60, 22)

        botones_layout.addWidget(self.editar_btn)
        
        self.eliminar_btn = QPushButton("🗑️ Eliminar")
        self.eliminar_btn.setProperty("class", "danger")
        self.eliminar_btn.setFixedSize(70, 22)

        botones_layout.addWidget(self.eliminar_btn)
        
        layout.addLayout(botones_layout)
        
        # Actualizar el estado visual
        self.actualizar_estado_vencimiento()
        
        # Conectar señales
        self.editar_btn.clicked.connect(lambda: self.editar_estado.emit(self._convertir_a_usuario_estado(self.estado)))
        self.eliminar_btn.clicked.connect(lambda: self.eliminar_estado.emit(self._convertir_a_usuario_estado(self.estado)))
        
        # Hacer clickeable


    
    def formato_fecha(self, fecha_str):
        """Formatea una fecha ISO a formato legible."""
        try:
            fecha = datetime.fromisoformat(fecha_str.replace('Z', '+00:00'))
            return fecha.strftime("%d/%m/%Y")
        except:
            return fecha_str
    
    def mousePressEvent(self, event):
        """Maneja el evento de clic en el widget."""
        self.estado_seleccionado.emit(self._convertir_a_usuario_estado(self.estado))
        super().mousePressEvent(event)
    
    def actualizar_estado_vencimiento(self):
        """Actualiza el indicador visual del estado de vencimiento."""
        ahora = datetime.now().date()
        
        try:
            # Verificar si el estado está activo
            activo = True
            es_futuro = False
            
            fecha_inicio = getattr(self.estado, 'fecha_inicio', None)
            fecha_vencimiento = getattr(self.estado, 'fecha_vencimiento', None)
            
            # Verificar fecha de inicio
            if fecha_inicio:
                fecha_inicio_dt = datetime.fromisoformat(fecha_inicio.replace('Z', '+00:00')).date()
                if ahora < fecha_inicio_dt:
                    activo = False
                    es_futuro = True
            
            # Verificar fecha de vencimiento
            if fecha_vencimiento:
                fecha_vencimiento_dt = datetime.fromisoformat(fecha_vencimiento.replace('Z', '+00:00')).date()
                if ahora > fecha_vencimiento_dt:
                    activo = False
                    es_futuro = False
            
            # Definir estado y texto del badge usando propiedades dinámicas
            status_value = ""
            badge_text = ""
            if es_futuro:
                status_value = "future"
                badge_text = "FUTURO"
            elif activo:
                if fecha_vencimiento:
                    fecha_vencimiento_dt = datetime.fromisoformat(fecha_vencimiento.replace('Z', '+00:00')).date()
                    dias_restantes = (fecha_vencimiento_dt - ahora).days
                    if dias_restantes <= 3:
                        status_value = "future"
                        badge_text = "PRÓXIMO"
                    else:
                        status_value = "active"
                        badge_text = "ACTIVO"
                else:
                    status_value = "active"
                    badge_text = "ACTIVO"
            else:
                status_value = "expired"
                badge_text = "VENCIDO"

            # Aplicar propiedades y repulir para que el QSS reaccione
            self.indicador_estado.setProperty("status", status_value)
            # Mapear badge a estados binarios (activo/inactivo) para estilos existentes
            badge_status = "active" if status_value == "active" else "inactive"
            self.estado_badge.setProperty("status", badge_status)
            self.estado_badge.setText(badge_text)

            self.indicador_estado.style().unpolish(self.indicador_estado)
            self.indicador_estado.style().polish(self.indicador_estado)
            self.estado_badge.style().unpolish(self.estado_badge)
            self.estado_badge.style().polish(self.estado_badge)
            self.update()
            
        except Exception as e:
            # Fallback ante errores: marcar como vencido
            self.indicador_estado.setProperty("status", "expired")
            self.estado_badge.setProperty("status", "inactive")
            self.estado_badge.setText("ERROR")
            self.indicador_estado.style().unpolish(self.indicador_estado)
            self.indicador_estado.style().polish(self.indicador_estado)
            self.estado_badge.style().unpolish(self.estado_badge)
            self.estado_badge.style().polish(self.estado_badge)
            self.update()

class EstadosWidget(QWidget):
    """Widget principal para gestión de estados temporales de usuarios."""
    
    # Señales
    estados_changed = pyqtSignal()  # Señal general para cambios
    
    def __init__(self, db_manager: DatabaseManager, parent=None):
        super().__init__(parent)
        self.db_manager = db_manager
        self.usuario_actual = None
        self.estados_usuario = []
        self.main_window = None
        
        # Señales
        self.estado_creado = pyqtSignal()
        self.estado_actualizado = pyqtSignal()
        self.estado_eliminado = pyqtSignal()
        
        self.setup_ui()
        self.conectar_señales()
        self.connect_accessibility_signals()
        
        # Timer para limpiar estados vencidos
        self.timer_limpieza = QTimer()
        self.timer_limpieza.timeout.connect(self.limpiar_estados_vencidos)
        self.timer_limpieza.start(3600000)  # Cada hora
    
    def setup_ui(self):
        layout = QVBoxLayout(self)
        layout.setContentsMargins(8, 8, 8, 8)
        layout.setSpacing(6)
        
        # Configurar política de tamaño del widget principal
        self.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Expanding)
        self.setObjectName("estados_widget")
        
        # Título
        titulo = QLabel("⏱️ Estados Temporales")
        titulo_font = QFont()
        titulo_font.setBold(True)
        titulo_font.setPointSize(11)
        titulo.setFont(titulo_font)
        titulo.setSizePolicy(QSizePolicy.Policy.Preferred, QSizePolicy.Policy.Fixed)
        titulo.setProperty("class", "panel_label")
        layout.addWidget(titulo)
        
        # Controles superiores
        controles_widget = QWidget()
        controles_widget.setFixedHeight(50)
        controles_layout = QHBoxLayout(controles_widget)
        controles_layout.setContentsMargins(0, 0, 0, 0)
        controles_layout.setSpacing(8)
        
        # Filtros
        filtros_group = QGroupBox("Filtros")
        filtros_group.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Expanding)
        filtros_group.setMinimumHeight(120)
        filtros_group.setMaximumHeight(150)
        # estilos movidos a QSS global
        filtros_layout = QHBoxLayout(filtros_group)
        filtros_layout.setContentsMargins(16, 25, 16, 16)
        filtros_layout.setSpacing(20)
        
        # Filtro por texto
        texto_label = QLabel("Buscar:")
        texto_label.setProperty("class", "muted-text")
        self.texto_filtro = QLineEdit()
        self.texto_filtro.setPlaceholderText("Buscar por título o descripción...")
        # estilos movidos a QSS global
        self.texto_filtro.setStyleSheet("""
            QLineEdit {
                font-size: 9px;
                padding: 3px 6px;
                border: 1px solid #ccc;
                border-radius: 3px;
                min-width: 150px;
            }
        """)
        filtros_layout.addWidget(texto_label)
        filtros_layout.addWidget(self.texto_filtro)
        
        # Filtro por estado
        estado_label = QLabel("Estado:")
        estado_label.setProperty("class", "muted-text")
        self.estado_filtro = QComboBox()
        self.estado_filtro.addItems(["Todos", "Activos", "Vencidos", "Futuros"])
        self.estado_filtro.setCurrentIndex(0)  # Establecer "Todos" por defecto
        # estilos movidos a QSS global
        self.estado_filtro.setStyleSheet("""
            QComboBox {
                font-size: 9px;
                padding: 3px 6px;
                border: 1px solid #ccc;
                border-radius: 3px;
                min-width: 80px;
            }
        """)
        filtros_layout.addWidget(estado_label)
        filtros_layout.addWidget(self.estado_filtro)
        
        controles_layout.addWidget(filtros_group)
        controles_layout.addStretch()
        
        # Botones
        self.agregar_btn = QPushButton("➕ Agregar Estado")
        self.agregar_btn.setEnabled(False)
        self.agregar_btn.setProperty("class", "primary")
        self.agregar_btn.setFixedSize(60, 22)
        self.agregar_btn.setStyleSheet("""
            QPushButton {
                font-size: 9px;
                font-weight: bold;
                padding: 6px 12px;
                background-color: #007bff;
                color: white;
                border: none;
                border-radius: 4px;
            }
            QPushButton:hover {
                background-color: #0056b3;
            }
            QPushButton:disabled {
                background-color: #6c757d;
                color: #adb5bd;
            }
        """)
        controles_layout.addWidget(self.agregar_btn)
        
        self.limpiar_vencidos_btn = QPushButton("🧹 Limpiar Vencidos")
        self.limpiar_vencidos_btn.setProperty("class", "danger")
        self.limpiar_vencidos_btn.setFixedSize(70, 22)
        self.limpiar_vencidos_btn.setStyleSheet("""
            QPushButton {
                font-size: 9px;
                font-weight: bold;
                padding: 6px 12px;
                background-color: #dc3545;
                color: white;
                border: none;
                border-radius: 4px;
            }
            QPushButton:hover {
                background-color: #c82333;
            }
        """)
        controles_layout.addWidget(self.limpiar_vencidos_btn)
        
        layout.addWidget(controles_widget)
        
        # Lista de estados
        self.scroll_area = QScrollArea()
        self.scroll_area.setWidgetResizable(True)
        self.scroll_area.setVerticalScrollBarPolicy(Qt.ScrollBarPolicy.ScrollBarAsNeeded)
        self.scroll_area.setHorizontalScrollBarPolicy(Qt.ScrollBarPolicy.ScrollBarAlwaysOff)
        self.scroll_area.setMinimumHeight(150)
        self.scroll_area.setMaximumHeight(250)
        self.scroll_area.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Expanding)
        self.scroll_area.setStyleSheet("""
            QScrollArea {
                border: 1px solid #dee2e6;
                border-radius: 6px;
                background-color: #f8f9fa;
            }
        """)
        
        self.estados_container = QWidget()
        self.estados_container.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Preferred)
        self.estados_layout = QVBoxLayout(self.estados_container)
        self.estados_layout.setSpacing(8)
        self.estados_layout.setContentsMargins(8, 8, 8, 8)
        self.estados_layout.setAlignment(Qt.AlignmentFlag.AlignTop)
        
        self.scroll_area.setWidget(self.estados_container)
        layout.addWidget(self.scroll_area)
        
        # Mensaje cuando no hay estados
        self.mensaje_vacio = QLabel("No hay estados para mostrar")
        self.mensaje_vacio.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self.mensaje_vacio.setProperty("class", "empty-state")
        self.mensaje_vacio.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Preferred)
        self.mensaje_vacio.hide()
        layout.addWidget(self.mensaje_vacio)
        
        self.scroll_area.hide()
    
    def conectar_señales(self):
        """Conecta las señales de los widgets."""
        self.agregar_btn.clicked.connect(self.agregar_estado)
        self.limpiar_vencidos_btn.clicked.connect(self.limpiar_estados_vencidos)
        self.estado_filtro.currentTextChanged.connect(self.aplicar_filtros)
        self.texto_filtro.textChanged.connect(self.aplicar_filtros)
    
    def establecer_usuario(self, usuario):
        """Establece el usuario actual y carga sus estados."""
        self.usuario_actual = usuario
        # Bloquear asignación de estados para usuario 'dueño'
        self.agregar_btn.setEnabled(usuario is not None and getattr(usuario, 'rol', None) != 'dueño')
        try:
            if usuario and getattr(usuario, 'rol', None) == 'dueño':
                self.agregar_btn.setToolTip("Bloqueado para usuario Dueño")
            else:
                self.agregar_btn.setToolTip("")
        except Exception:
            pass
        
        if usuario:
            self.cargar_estados()
            self.mensaje_vacio.hide()
            self.scroll_area.show()
        else:
            self.limpiar_estados()
            self.scroll_area.hide()
            self.mensaje_vacio.show()
    
    def cargar_estados(self):
        """Carga los estados del usuario actual."""
        if not self.usuario_actual:
            return
        
        try:
            self.estados_usuario = self.db_manager.obtener_estados_usuario(self.usuario_actual.id)
            self.mostrar_estados()
        except Exception as e:
            QMessageBox.critical(self, "Error", f"Error al cargar estados: {str(e)}")
    
    def mostrar_estados(self):
        """Muestra los estados en la interfaz."""
        self.limpiar_estados()
        
        estados_filtrados = self.filtrar_estados()
        
        if not estados_filtrados:
            mensaje = QLabel("No hay estados que coincidan con los filtros")
            mensaje.setAlignment(Qt.AlignmentFlag.AlignCenter)
            mensaje.setProperty("class", "muted-text")
            mensaje.setStyleSheet("font-style: italic; padding: 20px;")
            self.estados_layout.addWidget(mensaje)
            return
        
        for estado in estados_filtrados:
            estado_widget = EstadoItemWidget(estado)
            estado_widget.editar_estado.connect(self.editar_estado)
            estado_widget.eliminar_estado.connect(self.eliminar_estado)
            self.estados_layout.addWidget(estado_widget)
    
    def limpiar_estados(self):
        """Limpia la lista de estados."""
        while self.estados_layout.count():
            child = self.estados_layout.takeAt(0)
            if child.widget():
                child.widget().deleteLater()
    
    def filtrar_estados(self) -> List[UsuarioEstado]:
        """Aplica los filtros a los estados."""
        estados_filtrados = self.estados_usuario.copy()
        
        # Filtro por texto
        texto = self.texto_filtro.text().strip().lower()
        if texto:
            estados_filtrados = [e for e in estados_filtrados if 
                               texto in (e.get('estado', '') if isinstance(e, dict) else getattr(e, 'estado', '')).lower() or 
                               texto in (e.get('descripcion', '') if isinstance(e, dict) else getattr(e, 'descripcion', '') or "").lower()]
        
        # Filtro por estado (activo/vencido/futuro)
        filtro = self.estado_filtro.currentText()
        
        if filtro == "Todos":
            return estados_filtrados
        
        ahora = datetime.now().date()
        
        if filtro == "Activos":
            return [e for e in estados_filtrados if self.es_estado_activo(e, ahora)]
        elif filtro == "Vencidos":
            return [e for e in estados_filtrados if self.es_estado_vencido(e, ahora)]
        elif filtro == "Futuros":
            return [e for e in estados_filtrados if self.es_estado_futuro(e, ahora)]
        
        return estados_filtrados
    
    def es_estado_activo(self, estado: UsuarioEstado, fecha_actual) -> bool:
        """Verifica si un estado está activo."""
        try:
            fecha_inicio = estado.get('fecha_inicio') if isinstance(estado, dict) else getattr(estado, 'fecha_inicio', None)
            fecha_vencimiento = estado.get('fecha_vencimiento') if isinstance(estado, dict) else getattr(estado, 'fecha_vencimiento', None)
            
            if fecha_inicio:
                fecha_inicio_dt = datetime.fromisoformat(fecha_inicio.replace('Z', '+00:00')).date()
                if fecha_actual < fecha_inicio_dt:
                    return False
            
            if fecha_vencimiento:
                fecha_vencimiento_dt = datetime.fromisoformat(fecha_vencimiento.replace('Z', '+00:00')).date()
                if fecha_actual > fecha_vencimiento_dt:
                    return False
            
            return True
        except:
            return False
    
    def es_estado_vencido(self, estado: UsuarioEstado, fecha_actual) -> bool:
        """Verifica si un estado está vencido."""
        try:
            fecha_vencimiento = estado.get('fecha_vencimiento') if isinstance(estado, dict) else getattr(estado, 'fecha_vencimiento', None)
            if fecha_vencimiento:
                fecha_vencimiento_dt = datetime.fromisoformat(fecha_vencimiento.replace('Z', '+00:00')).date()
                return fecha_actual > fecha_vencimiento_dt
            return False
        except:
            return False
    
    def es_estado_futuro(self, estado: UsuarioEstado, fecha_actual) -> bool:
        """Verifica si un estado es futuro."""
        try:
            fecha_inicio = estado.get('fecha_inicio') if isinstance(estado, dict) else getattr(estado, 'fecha_inicio', None)
            if fecha_inicio:
                fecha_inicio_dt = datetime.fromisoformat(fecha_inicio.replace('Z', '+00:00')).date()
                return fecha_actual < fecha_inicio_dt
            return False
        except:
            return False
    
    def aplicar_filtros(self):
        """Aplica los filtros y actualiza la vista."""
        self.mostrar_estados()
    
    def agregar_estado(self):
        """Abre el diálogo para agregar un nuevo estado."""
        if not self.usuario_actual:
            return
        
        dialog = EstadoDialog(self, usuario_id=self.usuario_actual.id)
        if dialog.exec() == QDialog.DialogCode.Accepted:
            datos = dialog.obtener_datos()
            
            nuevo_estado = UsuarioEstado(
                usuario_id=self.usuario_actual.id,
                estado=datos['estado'],
                descripcion=datos['descripcion'],
                fecha_inicio=datos['fecha_inicio'],
                fecha_vencimiento=datos['fecha_fin'],
                creado_por=self.get_current_user_id()  # Usuario actual del sistema
            )
            
            try:
                motivo = f"Creación de nuevo estado '{nuevo_estado.estado}' desde interfaz de gestión"
                estado_id = self.db_manager.crear_estado_usuario(nuevo_estado, motivo, "127.0.0.1")
                nuevo_estado.id = estado_id
                QMessageBox.information(self, "Éxito", "Estado creado correctamente.")
                self.cargar_estados()
                self.estados_changed.emit()
            except Exception as e:
                QMessageBox.critical(self, "Error", f"Error al crear estado: {str(e)}")
    
    def editar_estado(self, estado: UsuarioEstado):
        """Abre el diálogo para editar un estado."""
        dialog = EstadoDialog(self, estado=estado)
        if dialog.exec() == QDialog.DialogCode.Accepted:
            datos = dialog.obtener_datos()
            
            # Obtener valores del estado (dict o objeto)
            if isinstance(estado, dict):
                estado['estado'] = datos['estado']
                estado['descripcion'] = datos['descripcion']
                estado['fecha_inicio'] = datos['fecha_inicio']
                estado['fecha_vencimiento'] = datos['fecha_fin']
                estado_id = estado.get('id')
            else:
                estado.estado = datos['estado']
                estado.descripcion = datos['descripcion']
                estado.fecha_inicio = datos['fecha_inicio']
                estado.fecha_vencimiento = datos['fecha_fin']
                estado_id = estado.id
            
            try:
                usuario_modificador = self.get_current_user_id()
                motivo = f"Modificación de estado '{datos['estado']}' desde interfaz de gestión"
                self.db_manager.actualizar_estado_usuario(estado, usuario_modificador, motivo, "127.0.0.1")
                QMessageBox.information(self, "Éxito", "Estado actualizado correctamente.")
                self.cargar_estados()
                self.estados_changed.emit()
            except Exception as e:
                QMessageBox.critical(self, "Error", f"Error al actualizar estado: {str(e)}")
    
    def eliminar_estado(self, estado: UsuarioEstado):
        """Elimina un estado después de confirmar."""
        # Obtener valores del estado (dict o objeto)
        estado_nombre = estado.get('estado', '') if isinstance(estado, dict) else getattr(estado, 'estado', '')
        estado_id = estado.get('id') if isinstance(estado, dict) else getattr(estado, 'id', None)
        
        respuesta = QMessageBox.question(
            self, "Confirmar eliminación",
            f"¿Está seguro de que desea eliminar el estado '{estado_nombre}'?",
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
            QMessageBox.StandardButton.No
        )
        
        if respuesta == QMessageBox.StandardButton.Yes:
            try:
                usuario_modificador = self.get_current_user_id()
                motivo = f"Eliminación de estado '{estado_nombre}' desde interfaz de gestión"
                self.db_manager.eliminar_estado_usuario(estado_id, usuario_modificador, motivo, "127.0.0.1")
                QMessageBox.information(self, "Éxito", "Estado eliminado correctamente.")
                self.cargar_estados()
                self.estados_changed.emit()
            except Exception as e:
                QMessageBox.critical(self, "Error", f"Error al eliminar estado: {str(e)}")
    
    def set_main_window(self, main_window):
        """Establece la referencia a la ventana principal"""
        self.main_window = main_window
        # Conectar señales de accesibilidad/branding y aplicar branding de inmediato
        try:
            self.connect_accessibility_signals()
        except Exception as e:
            print(f"Aviso: no se pudieron conectar señales de accesibilidad en EstadosWidget: {e}")
        try:
            self.apply_branding()
        except Exception:
            pass
    
    def get_current_user_id(self):
        """Obtiene el ID del usuario actual del sistema"""
        if hasattr(self, 'main_window') and self.main_window and hasattr(self.main_window, 'logged_in_user'):
            if self.main_window.logged_in_user:
                return self.main_window.logged_in_user.id
        return 1  # Fallback al usuario administrador por defecto
    
    def connect_accessibility_signals(self):
        """Conecta las señales de accesibilidad si están disponibles"""
        if hasattr(self, 'main_window') and self.main_window:
            # Conectar a la señal branding_changed del widget de branding (acepta cualquier firma)
            if hasattr(self.main_window, 'tabs') and 'configuracion' in self.main_window.tabs:
                config_tab = self.main_window.tabs['configuracion']
                if hasattr(config_tab, 'branding_widget') and hasattr(config_tab.branding_widget, 'branding_changed'):
                    try:
                        config_tab.branding_widget.branding_changed.connect(lambda *args, **kwargs: self.apply_branding())
                    except Exception:
                        pass
            # Conectar actualización de contraste/tema
            if hasattr(self.main_window, 'contrast_changed'):
                try:
                    self.main_window.contrast_changed.connect(lambda *args, **kwargs: self.apply_branding())
                except Exception:
                    pass
    
    def apply_branding(self):
        """Aplica el branding dinámico basado en la configuración del tema
        Nota: ahora se apoya en el stylesheet global (styles/style.qss) con variables dinámicas.
        Esta función solo fuerza el re-polish para que las reglas globales se apliquen.
        """
        # Asegurar que el objectName correcto esté establecido para las reglas ID-based
        try:
            if self.objectName() != "estados_widget":
                self.setObjectName("estados_widget")
        except Exception:
            pass

        # Marcar este widget y sus hijos para usar CSS dinámico
        try:
            from PyQt6.QtWidgets import QWidget as _QW
            self.setProperty("dynamic_css", "true")
            for w in self.findChildren(_QW):
                if not w.property("dynamic_css"):
                    w.setProperty("dynamic_css", "true")
        except Exception:
            pass

        # Re-aplicar estilos globales (sin establecer CSS inline)
        try:
            st = self.style()
            if st:
                st.unpolish(self)
                st.polish(self)
            self.update()
        except Exception:
            pass
    
    def limpiar_estados_vencidos(self):
        """Limpia automáticamente los estados vencidos."""
        try:
            estados_eliminados = self.db_manager.limpiar_estados_vencidos()
            if estados_eliminados > 0:
                QMessageBox.information(
                    self, "Limpieza completada",
                    f"Se eliminaron {estados_eliminados} estado(s) vencido(s)."
                )
                if self.usuario_actual:
                    self.cargar_estados()
            else:
                QMessageBox.information(self, "Limpieza completada", "No hay estados vencidos para eliminar.")
        except Exception as e:
            QMessageBox.critical(self, "Error", f"Error al limpiar estados vencidos: {str(e)}")
    
    def set_user(self, usuario):
        """Establece el usuario actual (método requerido por UserTabWidget)."""
        self.establecer_usuario(usuario)
    
    def clear(self):
        """Limpia el widget (método requerido por UserTabWidget)."""
        self.establecer_usuario(None)

