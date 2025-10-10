from PyQt6.QtWidgets import (
    QWidget, QVBoxLayout, QHBoxLayout, QGroupBox, QLineEdit, QPushButton,
    QListWidget, QListWidgetItem, QLabel, QMessageBox, QDialog, QDialogButtonBox,
    QFormLayout, QTextEdit, QComboBox, QFrame, QScrollArea, QSizePolicy
)
from PyQt6.QtCore import Qt, pyqtSignal, QDate
from PyQt6.QtGui import QFont, QColor, QPalette
from datetime import datetime
from typing import List, Optional
from models import UsuarioNota
from database import DatabaseManager

class NotaDialog(QDialog):
    """Diálogo para crear/editar notas de usuario."""
    
    def __init__(self, parent=None, nota: Optional[UsuarioNota] = None, usuario_id: int = None):
        super().__init__(parent)
        self.nota = nota
        self.usuario_id = usuario_id
        self.setup_ui()
        
        if nota:
            self.cargar_nota()
    
    def setup_ui(self):
        self.setWindowTitle("Agregar Nota" if not self.nota else "Editar Nota")
        self.setModal(True)
        self.resize(500, 400)
        
        layout = QVBoxLayout(self)
        
        # Formulario
        form_layout = QFormLayout()
        
        # Título
        self.titulo_edit = QLineEdit()
        self.titulo_edit.setPlaceholderText("Título de la nota...")
        form_layout.addRow("Título:", self.titulo_edit)
        
        # Categoría
        self.categoria_combo = QComboBox()
        self.categoria_combo.addItems(["general", "medica", "administrativa", "comportamiento"])
        form_layout.addRow("Categoría:", self.categoria_combo)
        
        # Importancia
        self.importancia_combo = QComboBox()
        self.importancia_combo.addItems(["baja", "normal", "alta", "critica"])
        self.importancia_combo.setCurrentText("normal")
        form_layout.addRow("Importancia:", self.importancia_combo)
        
        layout.addLayout(form_layout)
        
        # Contenido
        content_label = QLabel("Contenido:")
        layout.addWidget(content_label)
        
        self.contenido_edit = QTextEdit()
        self.contenido_edit.setPlaceholderText("Escriba el contenido de la nota aquí...")
        layout.addWidget(self.contenido_edit)
        
        # Botones
        button_box = QDialogButtonBox(QDialogButtonBox.StandardButton.Ok | QDialogButtonBox.StandardButton.Cancel)
        button_box.accepted.connect(self.accept)
        button_box.rejected.connect(self.reject)
        layout.addWidget(button_box)
    
    def cargar_nota(self):
        """Carga los datos de una nota existente."""
        if self.nota:
            self.titulo_edit.setText(self.nota.titulo)
            self.categoria_combo.setCurrentText(self.nota.categoria)
            self.importancia_combo.setCurrentText(self.nota.importancia)
            self.contenido_edit.setPlainText(self.nota.contenido)
    
    def obtener_datos(self) -> dict:
        """Obtiene los datos del formulario."""
        return {
            'titulo': self.titulo_edit.text().strip(),
            'categoria': self.categoria_combo.currentText(),
            'importancia': self.importancia_combo.currentText(),
            'contenido': self.contenido_edit.toPlainText().strip()
        }
    
    def validar_datos(self) -> bool:
        """Valida que los datos sean correctos."""
        datos = self.obtener_datos()
        
        if not datos['titulo']:
            QMessageBox.warning(self, "Error", "El título es obligatorio.")
            return False
        
        if not datos['contenido']:
            QMessageBox.warning(self, "Error", "El contenido es obligatorio.")
            return False
        
        return True
    
    def accept(self):
        if self.validar_datos():
            super().accept()

class NotaItemWidget(QWidget):
    """Widget personalizado para mostrar una nota en la lista."""
    
    nota_seleccionada = pyqtSignal(UsuarioNota)
    editar_nota = pyqtSignal(UsuarioNota)
    eliminar_nota = pyqtSignal(UsuarioNota)
    
    def __init__(self, nota, parent=None):
        super().__init__(parent)
        # Convertir diccionario a objeto UsuarioNota si es necesario
        if isinstance(nota, dict):
            self.nota = self._convertir_a_nota(nota)
        else:
            self.nota = nota
        self.setup_ui()
    
    def _convertir_a_nota(self, nota_dict):
        """Convierte un diccionario a objeto UsuarioNota."""
        return UsuarioNota(
            id=nota_dict.get('id'),
            usuario_id=nota_dict.get('usuario_id', 0),
            categoria=nota_dict.get('categoria', 'general'),
            titulo=nota_dict.get('titulo', ''),
            contenido=nota_dict.get('contenido', ''),
            importancia=nota_dict.get('importancia', 'normal'),
            fecha_creacion=nota_dict.get('fecha_creacion'),
            fecha_modificacion=nota_dict.get('fecha_modificacion'),
            activa=nota_dict.get('activa', True),
            autor_id=nota_dict.get('autor_id')
        )
        
    def setup_ui(self):
        """Configura la interfaz del item de nota."""
        layout = QVBoxLayout(self)
        layout.setContentsMargins(16, 12, 16, 12)
        layout.setSpacing(10)
        
        # Header con título e importancia
        header_layout = QHBoxLayout()
        header_layout.setSpacing(12)
        
        # Título
        titulo_text = getattr(self.nota, 'titulo', 'Sin título') if hasattr(self.nota, 'titulo') else self.nota.get('titulo', 'Sin título')
        self.titulo_label = QLabel(titulo_text)
        self.titulo_label.setObjectName("nota_titulo")
        self.titulo_label.setProperty("class", "nota-title")
        header_layout.addWidget(self.titulo_label)
        
        header_layout.addStretch()
        
        # Importancia
        importancia_text = getattr(self.nota, 'importancia', 'normal') if hasattr(self.nota, 'importancia') else self.nota.get('importancia', 'normal')
        self.importancia_label = QLabel(importancia_text.upper())
        color_importancia = {
            'baja': '#28a745',
            'normal': '#17a2b8', 
            'alta': '#ffc107',
            'critica': '#dc3545'
        }.get(importancia_text, '#6c757d')
        
        text_color = 'white' if importancia_text != 'alta' else 'black'
        
        self.importancia_label.setObjectName("nota_importancia")
        self.importancia_label.setProperty("class", f"importance-{importancia_text}")
        self.importancia_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        header_layout.addWidget(self.importancia_label)
        
        layout.addLayout(header_layout)
        
        # Contenido (preview)
        contenido_text = getattr(self.nota, 'contenido', 'Sin contenido') if hasattr(self.nota, 'contenido') else self.nota.get('contenido', 'Sin contenido')
        contenido_preview = contenido_text[:120] + "..." if len(contenido_text) > 120 else contenido_text
        self.contenido_label = QLabel(contenido_preview)
        self.contenido_label.setWordWrap(True)
        self.contenido_label.setObjectName("nota_contenido")
        self.contenido_label.setProperty("class", "nota-content")
        layout.addWidget(self.contenido_label)
        
        # Footer con categoría y fecha
        footer_layout = QHBoxLayout()
        footer_layout.setSpacing(16)
        
        # Categoría
        categoria_text = getattr(self.nota, 'categoria', 'General') if hasattr(self.nota, 'categoria') else self.nota.get('categoria', 'General')
        self.categoria_label = QLabel(f"📁 {categoria_text.title()}")
        self.categoria_label.setObjectName("nota_categoria")
        self.categoria_label.setProperty("class", "nota-category")
        footer_layout.addWidget(self.categoria_label)
        
        footer_layout.addStretch()
        
        # Fecha
        fecha_creacion = getattr(self.nota, 'fecha_creacion', None) if hasattr(self.nota, 'fecha_creacion') else self.nota.get('fecha_creacion', None)
        if fecha_creacion:
            try:
                if hasattr(fecha_creacion, 'strftime'):
                    fecha_str = fecha_creacion.strftime("%d/%m/%Y %H:%M")
                else:
                    fecha = datetime.fromisoformat(str(fecha_creacion).replace('Z', '+00:00'))
                    fecha_str = fecha.strftime("%d/%m/%Y %H:%M")
            except:
                fecha_str = str(fecha_creacion)
        else:
            fecha_str = "Sin fecha"
        
        self.fecha_label = QLabel(f"🕒 {fecha_str}")
        self.fecha_label.setObjectName("nota_fecha")
        self.fecha_label.setProperty("class", "nota-date")
        footer_layout.addWidget(self.fecha_label)
        
        layout.addLayout(footer_layout)
        
        # Botones de acción
        botones_layout = QHBoxLayout()
        botones_layout.setSpacing(8)
        botones_layout.addStretch()
        
        self.editar_btn = QPushButton("✏️ Editar")
        self.editar_btn.setFixedHeight(32)
        self.editar_btn.setMinimumWidth(80)
        self.editar_btn.setToolTip("Editar nota")
        self.editar_btn.setObjectName("nota_editar_btn")
        self.editar_btn.setProperty("class", "btn-outline-primary")
        self.editar_btn.clicked.connect(lambda: self.editar_nota.emit(self.nota))
        botones_layout.addWidget(self.editar_btn)
        
        self.eliminar_btn = QPushButton("🗑️ Eliminar")
        self.eliminar_btn.setFixedHeight(32)
        self.eliminar_btn.setMinimumWidth(90)
        self.eliminar_btn.setToolTip("Eliminar nota")
        self.eliminar_btn.setObjectName("nota_eliminar_btn")
        self.eliminar_btn.setProperty("class", "btn-outline-danger")
        self.eliminar_btn.clicked.connect(lambda: self.eliminar_nota.emit(self.nota))
        botones_layout.addWidget(self.eliminar_btn)
        
        layout.addLayout(botones_layout)
        
        # Estilo del contenedor migrado al sistema CSS dinámico
        self.setObjectName("nota_item_widget")
        self.setProperty("class", "nota-item-container")
        
        # Hacer clickeable
        self.mousePressEvent = lambda event: self.nota_seleccionada.emit(self.nota)

class NotasWidget(QWidget):
    """Widget principal para gestión de notas de usuarios."""
    
    # Señales
    nota_creada = pyqtSignal(UsuarioNota)
    nota_actualizada = pyqtSignal(UsuarioNota)
    nota_eliminada = pyqtSignal(int)
    notas_changed = pyqtSignal()  # Señal general para cambios
    
    def __init__(self, db_manager: DatabaseManager, parent=None):
        super().__init__(parent)
        self.db_manager = db_manager
        self.usuario_actual = None
        self.notas_usuario = []
        self.main_window = None
        self.setup_ui()
        self.conectar_señales()
        self.connect_accessibility_signals()
    
    def setup_ui(self):
        layout = QVBoxLayout(self)
        layout.setContentsMargins(8, 8, 8, 8)
        layout.setSpacing(6)
        
        # Configurar política de tamaño del widget principal
        self.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Expanding)
        
        # Título
        titulo = QLabel("📝 Notas")
        titulo_font = QFont()
        titulo_font.setBold(True)
        titulo_font.setPointSize(11)
        titulo.setFont(titulo_font)
        titulo.setSizePolicy(QSizePolicy.Policy.Preferred, QSizePolicy.Policy.Fixed)
        titulo.setObjectName("notas_titulo")
        titulo.setProperty("class", "section-title")
        layout.addWidget(titulo)
        
        
        filtros_group = QGroupBox("Filtros")
        filtros_group.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Fixed)
        filtros_group.setMinimumHeight(120)
        filtros_group.setMaximumHeight(150)
        filtros_group.setObjectName("filtros_group")
        filtros_group.setProperty("class", "config_group")
        filtros_layout = QHBoxLayout(filtros_group)
        filtros_layout.setContentsMargins(16, 25, 16, 16)
        filtros_layout.setSpacing(20)
        
        # Filtro por categoría
        categoria_label = QLabel("Categoría:")
        categoria_label.setObjectName("categoria_label")
        categoria_label.setProperty("class", "filter-label")
        self.categoria_filtro = QComboBox()
        self.categoria_filtro.addItems(["Todas", "general", "medica", "administrativa", "comportamiento"])
        self.categoria_filtro.setCurrentIndex(0)
        self.categoria_filtro.setObjectName("categoria_filtro")
        self.categoria_filtro.setProperty("class", "filter-combo")
        filtros_layout.addWidget(categoria_label)
        filtros_layout.addWidget(self.categoria_filtro)
        
        # Filtro por importancia
        importancia_label = QLabel("Importancia:")
        importancia_label.setObjectName("importancia_label")
        importancia_label.setProperty("class", "filter-label")
        self.importancia_filtro = QComboBox()
        self.importancia_filtro.addItems(["Todas", "baja", "normal", "alta", "critica"])
        self.importancia_filtro.setCurrentIndex(0)
        self.importancia_filtro.setObjectName("importancia_filtro")
        self.importancia_filtro.setProperty("class", "filter-combo")
        filtros_layout.addWidget(importancia_label)
        filtros_layout.addWidget(self.importancia_filtro)
        
        # Filtro de texto
        texto_label = QLabel("Buscar:")
        texto_label.setObjectName("texto_label")
        texto_label.setProperty("class", "filter-label")
        self.texto_filtro = QLineEdit()
        self.texto_filtro.setPlaceholderText("Buscar en título o contenido...")
        self.texto_filtro.setObjectName("texto_filtro")
        self.texto_filtro.setProperty("class", "filter-input")
        filtros_layout.addWidget(texto_label)
        filtros_layout.addWidget(self.texto_filtro)
        
        filtros_layout.addStretch()
        layout.addWidget(filtros_group)
        
        # Controles
        controles_widget = QWidget()
        controles_widget.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Fixed)
        controles_widget.setFixedHeight(50)
        controles_layout = QHBoxLayout(controles_widget)
        controles_layout.setContentsMargins(0, 0, 0, 0)
        controles_layout.setSpacing(8)
        
        controles_layout.addStretch()
        
        # Botón agregar
        self.agregar_btn = QPushButton("➕ Agregar Nota")
        self.agregar_btn.setEnabled(False)
        self.agregar_btn.setObjectName("agregar_nota_btn")
        self.agregar_btn.setProperty("class", "btn-primary")
        controles_layout.addWidget(self.agregar_btn)
        
        layout.addWidget(controles_widget)
        
        
        notas_usuario_group = QGroupBox("Mis Notas")
        notas_usuario_group.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Expanding)
        notas_usuario_group.setObjectName("notas_usuario_group")
        notas_usuario_group.setProperty("class", "config_group")
        notas_usuario_layout = QVBoxLayout(notas_usuario_group)
        notas_usuario_layout.setContentsMargins(16, 25, 16, 16)
        notas_usuario_layout.setSpacing(8)
        
        # Scroll area para notas
        self.scroll_area = QScrollArea()
        self.scroll_area.setWidgetResizable(True)
        self.scroll_area.setVerticalScrollBarPolicy(Qt.ScrollBarPolicy.ScrollBarAsNeeded)
        self.scroll_area.setHorizontalScrollBarPolicy(Qt.ScrollBarPolicy.ScrollBarAlwaysOff)
        self.scroll_area.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Expanding)
        self.scroll_area.setMinimumHeight(120)
        self.scroll_area.setMaximumHeight(250)
        self.scroll_area.setObjectName("notas_scroll_area")
        self.scroll_area.setProperty("class", "config_scroll")
        
        self.notas_container = QWidget()
        self.notas_container.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Preferred)
        self.notas_layout = QVBoxLayout(self.notas_container)
        self.notas_layout.setSpacing(8)
        self.notas_layout.setContentsMargins(8, 8, 8, 8)
        self.notas_layout.setAlignment(Qt.AlignmentFlag.AlignTop)
        
        self.scroll_area.setWidget(self.notas_container)
        notas_usuario_layout.addWidget(self.scroll_area)
        
        # Mensaje cuando no hay notas
        self.mensaje_vacio = QLabel("No hay notas para mostrar")
        self.mensaje_vacio.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self.mensaje_vacio.setProperty("class", "muted-text")
        self.mensaje_vacio.setObjectName("mensaje_vacio")
        self.mensaje_vacio.setProperty("class", "empty-message")
        self.mensaje_vacio.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Preferred)
        self.mensaje_vacio.hide()
        notas_usuario_layout.addWidget(self.mensaje_vacio)
        
        layout.addWidget(notas_usuario_group)
        
        self.scroll_area.hide()
    
    def conectar_señales(self):
        """Conecta las señales de los widgets."""
        self.agregar_btn.clicked.connect(self.agregar_nota)
        self.categoria_filtro.currentTextChanged.connect(self.aplicar_filtros)
        self.importancia_filtro.currentTextChanged.connect(self.aplicar_filtros)
        self.texto_filtro.textChanged.connect(self.aplicar_filtros)
        
        # Conectar señales específicas a la señal general
        self.nota_creada.connect(lambda: self.notas_changed.emit())
        self.nota_actualizada.connect(lambda: self.notas_changed.emit())
        self.nota_eliminada.connect(lambda: self.notas_changed.emit())
    
    def establecer_usuario(self, usuario):
        """Establece el usuario actual y carga sus notas."""
        self.usuario_actual = usuario
        # Bloquear creación de notas para usuario 'dueño'
        self.agregar_btn.setEnabled(usuario is not None and getattr(usuario, 'rol', None) != 'dueño')
        try:
            if usuario and getattr(usuario, 'rol', None) == 'dueño':
                self.agregar_btn.setToolTip("Bloqueado para usuario Dueño")
            else:
                self.agregar_btn.setToolTip("")
        except Exception:
            pass
        
        if usuario:
            self.cargar_notas()
            self.mensaje_vacio.hide()
            self.scroll_area.show()
        else:
            self.limpiar_notas()
            self.scroll_area.hide()
            self.mensaje_vacio.show()
    
    def cargar_notas(self):
        """Carga las notas del usuario actual."""
        if not self.usuario_actual:
            return
        
        try:
            self.notas_usuario = self.db_manager.obtener_notas_usuario(self.usuario_actual.id)
            self.mostrar_notas()
        except Exception as e:
            QMessageBox.critical(self, "Error", f"Error al cargar notas: {str(e)}")
    
    def mostrar_notas(self):
        """Muestra las notas en la interfaz."""
        self.limpiar_notas()
        
        notas_filtradas = self.filtrar_notas()
        
        if not notas_filtradas:
            mensaje = QLabel("No hay notas que coincidan con los filtros")
            mensaje.setAlignment(Qt.AlignmentFlag.AlignCenter)
            mensaje.setObjectName("mensaje_filtros")
            mensaje.setProperty("class", "muted-text italic")
            self.notas_layout.addWidget(mensaje)
            return
        
        for nota in notas_filtradas:
            nota_widget = NotaItemWidget(nota)
            nota_widget.editar_nota.connect(self.editar_nota)
            nota_widget.eliminar_nota.connect(self.eliminar_nota)
            self.notas_layout.addWidget(nota_widget)
    
    def limpiar_notas(self):
        """Limpia la lista de notas."""
        while self.notas_layout.count():
            child = self.notas_layout.takeAt(0)
            if child.widget():
                child.widget().deleteLater()
    
    def clear(self):
        """Alias para limpiar_notas - mantiene compatibilidad."""
        self.limpiar_notas()
        self.usuario_actual = None
        self.agregar_btn.setEnabled(False)
        self.scroll_area.hide()
        self.mensaje_vacio.show()
    
    def set_user(self, usuario):
        """Alias para establecer_usuario - mantiene compatibilidad."""
        self.establecer_usuario(usuario)
    
    def filtrar_notas(self) -> List:
        """Aplica los filtros a las notas."""
        notas_filtradas = self.notas_usuario.copy()
        
        # Filtro por categoría
        categoria_filtro = self.categoria_filtro.currentText()
        if categoria_filtro != "Todas":
            notas_filtradas = [n for n in notas_filtradas if 
                             (n.get('categoria') if isinstance(n, dict) else getattr(n, 'categoria', '')) == categoria_filtro]
        
        # Filtro por importancia
        importancia_filtro = self.importancia_filtro.currentText()
        if importancia_filtro != "Todas":
            notas_filtradas = [n for n in notas_filtradas if 
                             (n.get('importancia') if isinstance(n, dict) else getattr(n, 'importancia', '')) == importancia_filtro]
        
        # Filtro por texto
        texto = self.texto_filtro.text().strip().lower()
        if texto:
            notas_filtradas = [n for n in notas_filtradas if 
                             texto in (n.get('titulo', '') if isinstance(n, dict) else getattr(n, 'titulo', '')).lower() or 
                             texto in (n.get('contenido', '') if isinstance(n, dict) else getattr(n, 'contenido', '') or "").lower()]
        
        return notas_filtradas
    
    def aplicar_filtros(self):
        """Aplica los filtros y actualiza la vista."""
        self.mostrar_notas()
    
    def agregar_nota(self):
        """Abre el diálogo para agregar una nueva nota."""
        if not self.usuario_actual:
            return
        
        dialog = NotaDialog(self, usuario_id=self.usuario_actual.id)
        if dialog.exec() == QDialog.DialogCode.Accepted:
            datos = dialog.obtener_datos()
            
            # Crear nueva nota
            nueva_nota = UsuarioNota(
                usuario_id=self.usuario_actual.id,
                categoria=datos['categoria'],
                titulo=datos['titulo'],
                contenido=datos['contenido'],
                importancia=datos['importancia'],
                autor_id=self.get_current_user_id()  # Usuario actual del sistema
            )
            
            try:
                nota_id = self.db_manager.crear_nota_usuario(nueva_nota)
                nueva_nota.id = nota_id
                QMessageBox.information(self, "Éxito", "Nota creada correctamente.")
                self.cargar_notas()
                self.nota_creada.emit(nueva_nota)
            except Exception as e:
                QMessageBox.critical(self, "Error", f"Error al crear nota: {str(e)}")
    
    def editar_nota(self, nota: UsuarioNota):
        """Abre el diálogo para editar una nota."""
        dialog = NotaDialog(self, nota=nota)
        if dialog.exec() == QDialog.DialogCode.Accepted:
            datos = dialog.obtener_datos()
            
            # Actualizar nota
            nota.categoria = datos['categoria']
            nota.titulo = datos['titulo']
            nota.contenido = datos['contenido']
            nota.importancia = datos['importancia']
            
            try:
                self.db_manager.actualizar_nota_usuario(nota)
                QMessageBox.information(self, "Éxito", "Nota actualizada correctamente.")
                self.cargar_notas()
                self.nota_actualizada.emit(nota)
            except Exception as e:
                QMessageBox.critical(self, "Error", f"Error al actualizar nota: {str(e)}")
    
    def eliminar_nota(self, nota: UsuarioNota):
        """Elimina una nota después de confirmar."""
        respuesta = QMessageBox.question(
            self, "Confirmar eliminación",
            f"¿Está seguro de que desea eliminar la nota '{nota.titulo}'?",
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
            QMessageBox.StandardButton.No
        )
        
        if respuesta == QMessageBox.StandardButton.Yes:
            try:
                self.db_manager.eliminar_nota_usuario(nota.id)
                QMessageBox.information(self, "Éxito", "Nota eliminada correctamente.")
                self.cargar_notas()
                self.nota_eliminada.emit(nota.id)
            except Exception as e:
                QMessageBox.critical(self, "Error", f"Error al eliminar nota: {str(e)}")
    
    def set_main_window(self, main_window):
        """Establece la referencia a la ventana principal."""
        self.main_window = main_window
    
    def get_current_user_id(self):
        """Obtiene el ID del usuario actual del sistema"""
        if hasattr(self, 'main_window') and self.main_window and hasattr(self.main_window, 'logged_in_user'):
            if self.main_window.logged_in_user:
                return self.main_window.logged_in_user.id
        return 1  # Fallback al usuario administrador por defecto
    
    def connect_accessibility_signals(self):
        """Conecta las señales de accesibilidad con la ventana principal."""
        # Se conectarán cuando se establezca main_window
        pass
    
    def apply_branding(self, primary_color, secondary_color, accent_color):
        """Aplica los colores del branding al widget"""
        try:
            # Los estilos ahora se manejan a través del sistema CSS dinámico
            # Los colores se aplicarán automáticamente según las propiedades establecidas
            pass
        except Exception as e:
            print(f"Error aplicando branding a NotasWidget: {e}")

