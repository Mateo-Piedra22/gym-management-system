from PyQt6.QtWidgets import (
    QWidget, QVBoxLayout, QHBoxLayout, QGridLayout, QPushButton, 
    QLabel, QFileDialog, QMessageBox, QScrollArea, QFrame,
    QButtonGroup, QGroupBox, QSizePolicy, QLineEdit
)
from PyQt6.QtCore import Qt, pyqtSignal, QSize
from PyQt6.QtGui import QPixmap, QIcon
from utils_modules.icon_manager import IconManager
import os

class IconSelectorWidget(QWidget):
    """Widget para seleccionar iconos predefinidos o personalizados"""
    
    icon_selected = pyqtSignal(str)
    
    def __init__(self, parent=None, current_icon=None):
        super().__init__(parent)
        self.icon_manager = IconManager()
        self.selected_icon = current_icon or "💰"
        self.button_group = QButtonGroup()
        self.setup_ui()
        self.load_icons()
    
    def setup_ui(self):
        """Configura la interfaz de usuario"""
        layout = QVBoxLayout(self)
        layout.setSpacing(15)
        layout.setContentsMargins(10, 10, 10, 10)
        
        # Título
        title_label = QLabel("Seleccionar Icono")
        title_label.setProperty("class", "section_title")
        title_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        layout.addWidget(title_label)
        
        # Icono seleccionado actual
        self.current_selection_frame = QFrame()
        self.current_selection_frame.setFrameStyle(QFrame.Shape.Box)
        self.current_selection_frame.setFixedHeight(80)
        current_layout = QHBoxLayout(self.current_selection_frame)
        
        current_layout.addWidget(QLabel("Seleccionado:"))
        self.current_icon_label = QLabel()
        self.current_icon_label.setFixedSize(48, 48)
        self.current_icon_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self.current_icon_label.setStyleSheet("border: 2px solid #007ACC; border-radius: 4px;")
        current_layout.addWidget(self.current_icon_label)
        
        self.current_icon_name = QLabel(self.selected_icon)
        current_layout.addWidget(self.current_icon_name)
        current_layout.addStretch()
        
        layout.addWidget(self.current_selection_frame)
        
        # Campo de búsqueda
        search_layout = QHBoxLayout()
        search_label = QLabel("Buscar:")
        self.search_input = QLineEdit()
        self.search_input.setPlaceholderText("Buscar íconos por nombre (ej: fitness, money, student...)")
        self.search_input.textChanged.connect(self.filter_icons)
        search_layout.addWidget(search_label)
        search_layout.addWidget(self.search_input)
        layout.addLayout(search_layout)
        
        # Iconos predefinidos
        predefined_group = QGroupBox("Iconos Predefinidos")
        predefined_layout = QVBoxLayout(predefined_group)
        
        # Scroll area para iconos predefinidos
        predefined_scroll = QScrollArea()
        predefined_scroll.setWidgetResizable(True)
        predefined_scroll.setFixedHeight(200)
        
        self.predefined_widget = QWidget()
        self.predefined_grid = QGridLayout(self.predefined_widget)
        self.predefined_grid.setSpacing(5)
        
        predefined_scroll.setWidget(self.predefined_widget)
        predefined_layout.addWidget(predefined_scroll)
        
        layout.addWidget(predefined_group)
        
        # Iconos personalizados
        custom_group = QGroupBox("Iconos Personalizados")
        custom_layout = QVBoxLayout(custom_group)
        
        # Botones para gestión de iconos personalizados
        custom_buttons_layout = QHBoxLayout()
        
        self.load_custom_button = QPushButton("📁 Cargar Icono")
        self.load_custom_button.setProperty("class", "primary")
        self.load_custom_button.clicked.connect(self.load_custom_icon)
        
        self.delete_custom_button = QPushButton("🗑️ Eliminar")
        self.delete_custom_button.setProperty("class", "danger")
        self.delete_custom_button.clicked.connect(self.delete_custom_icon)
        self.delete_custom_button.setEnabled(False)
        
        custom_buttons_layout.addWidget(self.load_custom_button)
        custom_buttons_layout.addWidget(self.delete_custom_button)
        custom_buttons_layout.addStretch()
        
        custom_layout.addLayout(custom_buttons_layout)
        
        # Scroll area para iconos personalizados
        custom_scroll = QScrollArea()
        custom_scroll.setWidgetResizable(True)
        custom_scroll.setFixedHeight(150)
        
        self.custom_widget = QWidget()
        self.custom_grid = QGridLayout(self.custom_widget)
        self.custom_grid.setSpacing(5)
        
        custom_scroll.setWidget(self.custom_widget)
        custom_layout.addWidget(custom_scroll)
        
        layout.addWidget(custom_group)
        
        # Actualizar icono seleccionado
        self.update_current_icon_display()
    
    def filter_icons(self, search_term: str):
        """Filtra los iconos basado en el término de búsqueda"""
        self.load_predefined_icons(search_term.strip())
    
    def load_icons(self):
        """Carga todos los iconos disponibles"""
        self.load_predefined_icons()
        self.load_custom_icons()
    
    def load_predefined_icons(self, search_term: str = ""):
        """Carga los iconos predefinidos con filtro opcional"""
        # Limpiar grid existente
        self.clear_layout(self.predefined_grid)
        
        # Obtener iconos filtrados si hay término de búsqueda
        if search_term:
            predefined_icons = self.icon_manager.search_icons_by_name(search_term)
        else:
            predefined_icons = self.icon_manager.get_predefined_icons_list()
        
        if not predefined_icons:
            no_results_label = QLabel("No se encontraron íconos")
            no_results_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
            no_results_label.setStyleSheet("color: gray; font-style: italic;")
            self.predefined_grid.addWidget(no_results_label, 0, 0)
            return
        
        row, col = 0, 0
        max_cols = 8
        
        for emoji in predefined_icons:
            # Crear botón con tooltip que muestre el nombre
            button = QPushButton(emoji)
            button.setFixedSize(40, 40)
            button.setToolTip(f"{emoji} - {self.icon_manager.get_icon_name(emoji)}")
            button.setProperty("icon_identifier", emoji)
            button.clicked.connect(lambda checked, icon=emoji: self.select_icon(icon))
            
            # Marcar como seleccionado si es el icono actual
            if emoji == self.selected_icon:
                button.setProperty("class", "selected")
                button.setStyleSheet("background-color: #007ACC; color: white;")
            
            self.button_group.addButton(button)
            self.predefined_grid.addWidget(button, row, col)
            
            col += 1
            if col >= max_cols:
                col = 0
                row += 1
    
    def load_custom_icons(self):
        """Carga los iconos personalizados"""
        # Limpiar grid existente
        self.clear_layout(self.custom_grid)
        
        custom_icons = self.icon_manager.get_custom_icons()
        
        if not custom_icons:
            no_icons_label = QLabel("No hay iconos personalizados")
            no_icons_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
            no_icons_label.setStyleSheet("color: gray; font-style: italic;")
            self.custom_grid.addWidget(no_icons_label, 0, 0)
            return
        
        row, col = 0, 0
        max_cols = 6
        
        for icon_path in custom_icons:
            button = QPushButton()
            button.setFixedSize(48, 48)
            button.setProperty("icon_identifier", icon_path)
            button.clicked.connect(lambda checked, icon=icon_path: self.select_icon(icon))
            
            # Cargar imagen
            pixmap = QPixmap(icon_path)
            if not pixmap.isNull():
                scaled_pixmap = pixmap.scaled(40, 40, Qt.AspectRatioMode.KeepAspectRatio, Qt.TransformationMode.SmoothTransformation)
                button.setIcon(QIcon(scaled_pixmap))
                button.setIconSize(QSize(40, 40))
            
            # Marcar como seleccionado si es el icono actual
            if icon_path == self.selected_icon:
                button.setProperty("class", "selected")
                button.setStyleSheet("background-color: #007ACC;")
            
            self.button_group.addButton(button)
            self.custom_grid.addWidget(button, row, col)
            
            col += 1
            if col >= max_cols:
                col = 0
                row += 1
    
    def clear_layout(self, layout):
        """Limpia todos los widgets de un layout"""
        while layout.count():
            child = layout.takeAt(0)
            if child.widget():
                child.widget().deleteLater()
    
    def select_icon(self, icon_identifier: str):
        """Selecciona un icono"""
        self.selected_icon = icon_identifier
        self.update_current_icon_display()
        self.update_button_selection()
        self.icon_selected.emit(icon_identifier)
        
        # Habilitar botón de eliminar solo para iconos personalizados
        is_custom = not icon_identifier in self.icon_manager.get_predefined_icons_list()
        self.delete_custom_button.setEnabled(is_custom)
    

    
    def update_current_icon_display(self):
        """Actualiza la visualización del icono seleccionado"""
        icon = self.icon_manager.create_qicon(self.selected_icon, QSize(48, 48))
        self.current_icon_label.setPixmap(icon.pixmap(48, 48))
        
        # Mostrar nombre del icono
        if self.selected_icon in self.icon_manager.get_predefined_icons_list():
            self.current_icon_name.setText(f"Emoji: {self.selected_icon}")
        else:
            filename = os.path.basename(self.selected_icon) if self.selected_icon else "Ninguno"
            self.current_icon_name.setText(f"Archivo: {filename}")
    
    def update_button_selection(self):
        """Actualiza la apariencia de los botones para mostrar la selección"""
        # Resetear todos los botones
        for button in self.button_group.buttons():
            button.setStyleSheet("")
            button.setProperty("class", "")
        
        # Marcar el botón seleccionado
        for button in self.button_group.buttons():
            if button.property("icon_identifier") == self.selected_icon:
                if self.selected_icon in self.icon_manager.get_predefined_icons_list():
                    button.setStyleSheet("background-color: #007ACC; color: white;")
                else:
                    button.setStyleSheet("background-color: #007ACC; border: 2px solid #005a9e;")
                break
    
    def load_custom_icon(self):
        """Carga un icono personalizado desde archivo"""
        file_path, _ = QFileDialog.getOpenFileName(
            self,
            "Seleccionar Icono",
            "",
            "Archivos de Imagen (*.png *.jpg *.jpeg *.gif *.bmp *.svg)"
        )
        
        if file_path:
            # Validar el archivo
            if not self.icon_manager.validate_icon(file_path):
                QMessageBox.warning(self, "Error", "El archivo seleccionado no es una imagen válida.")
                return
            
            # Obtener nombre para el icono
            filename = os.path.basename(file_path)
            name_without_ext = os.path.splitext(filename)[0]
            
            # Guardar el icono
            saved_path = self.icon_manager.save_custom_icon(file_path, name_without_ext)
            
            if saved_path:
                # Recargar iconos personalizados
                self.load_custom_icons()
                # Seleccionar el nuevo icono
                self.select_icon(saved_path)
                QMessageBox.information(self, "Éxito", "Icono personalizado cargado correctamente.")
            else:
                QMessageBox.warning(self, "Error", "No se pudo guardar el icono personalizado.")
    
    def delete_custom_icon(self):
        """Elimina el icono personalizado seleccionado"""
        if not self.selected_icon or self.selected_icon in self.icon_manager.get_predefined_icons_list():
            return
        
        reply = QMessageBox.question(
            self,
            "Confirmar Eliminación",
            f"¿Está seguro de que desea eliminar el icono personalizado?\n{os.path.basename(self.selected_icon)}",
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No
        )
        
        if reply == QMessageBox.StandardButton.Yes:
            if self.icon_manager.delete_custom_icon(self.selected_icon):
                # Seleccionar icono por defecto
                self.select_icon("💰")
                # Recargar iconos personalizados
                self.load_custom_icons()
                QMessageBox.information(self, "Éxito", "Icono eliminado correctamente.")
            else:
                QMessageBox.warning(self, "Error", "No se pudo eliminar el icono.")
    
    def get_selected_icon(self) -> str:
        """Retorna el icono seleccionado"""
        return self.selected_icon
    
    def set_selected_icon(self, icon_identifier: str):
        """Establece el icono seleccionado"""
        self.selected_icon = icon_identifier
        self.update_current_icon_display()
        self.update_button_selection()

