from PyQt6.QtWidgets import (
    QDialog, QVBoxLayout, QLineEdit, QListWidget, QListWidgetItem,
    QDialogButtonBox, QMessageBox
)
from PyQt6.QtCore import Qt
from typing import List, Optional
from models import Usuario

class UserSelectionDialog(QDialog):
    def __init__(self, parent, usuarios: List[Usuario]):
        super().__init__(parent)
        self.setWindowTitle("Seleccionar Alumno para Inscribir")
        self.setMinimumWidth(400)
        self.selected_user: Optional[Usuario] = None
        
        layout = QVBoxLayout(self)
        self.search_input = QLineEdit()
        self.search_input.setPlaceholderText("Buscar por nombre o DNI...")
        self.user_list = QListWidget()
        
        for user in usuarios:
            item = QListWidgetItem(f"{user.nombre} (DNI: {user.dni or 'N/A'})")
            item.setData(Qt.ItemDataRole.UserRole, user)
            self.user_list.addItem(item)
            
        self.button_box = QDialogButtonBox(QDialogButtonBox.StandardButton.Ok | QDialogButtonBox.StandardButton.Cancel)

        layout.addWidget(self.search_input)
        layout.addWidget(self.user_list)
        layout.addWidget(self.button_box)

        self.connect_signals()

    def connect_signals(self):
        self.search_input.textChanged.connect(self.filter_users)
        self.user_list.itemDoubleClicked.connect(self.accept_dialog)
        self.button_box.accepted.connect(self.accept_dialog)
        self.button_box.rejected.connect(self.reject)
    
    def filter_users(self, text: str):
        text_lower = text.lower()
        for i in range(self.user_list.count()):
            item = self.user_list.item(i)
            item.setHidden(text_lower not in item.text().lower())
            
    def accept_dialog(self):
        current_item = self.user_list.currentItem()
        if not current_item:
            QMessageBox.warning(self, "Sin Selección", "Por favor, seleccione un alumno de la lista.")
            return
        self.selected_user = current_item.data(Qt.ItemDataRole.UserRole)
        self.accept()
        
    def get_selected_user(self) -> Optional[Usuario]:
        return self.selected_user

