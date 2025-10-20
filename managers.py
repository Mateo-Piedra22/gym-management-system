import os
import shutil
import psutil
import logging
from datetime import datetime
from PyQt6.QtWidgets import (
    QInputDialog, QMessageBox, QLineEdit, QFileDialog, QDialog, QVBoxLayout,
    QTabWidget, QPushButton, QHBoxLayout, QLabel
)
from PyQt6.QtCore import Qt
from PyQt6.QtGui import QFont

# Widgets de diagnóstico y mantenimiento integrados en el Panel de Control de Dueño

class DeveloperManager:
    """Gestiona herramientas administrativas del Dueño y utilidades avanzadas."""
    
    # Contraseña del Dueño (acceso administrativo)
    DEV_PASSWORD = "Matute03"
    
    def __init__(self, parent_widget, db_manager):
        self.parent = parent_widget
        self.db_manager = db_manager
        self.is_dev_mode_active = False

    def check_password(self):
        """Solicita la contraseña del Dueño para acciones administrativas."""
        password, ok = QInputDialog.getText(
            self.parent, 
            "Acceso de Dueño", 
            "Ingrese la contraseña de Dueño:", 
            echo=QLineEdit.EchoMode.Password
        )
        
        if ok and password == self.DEV_PASSWORD:
            self.is_dev_mode_active = True
            QMessageBox.information(self.parent, "Éxito", "Acceso de Dueño concedido.")
            return True
        elif ok:
            QMessageBox.warning(self.parent, "Acceso Denegado", "Contraseña incorrecta.")
            return False
        return False
    
    # Método eliminado - funcionalidad migrada al Panel de Control de Dueño

    def create_database_backup(self):
        """
        Permite al usuario guardar una copia de seguridad de la base de datos.
        """
        backup_dir = "backups"
        os.makedirs(backup_dir, exist_ok=True)
        
        timestamp = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
        default_filename = os.path.join(backup_dir, f"backup_{timestamp}.db")

        backup_path, _ = QFileDialog.getSaveFileName(
            self.parent, 
            "Guardar Copia de Seguridad", 
            default_filename, 
            "Database Files (*.db)"
        )
        
        if backup_path:
            try:
                shutil.copyfile(self.db_manager.db_path, backup_path)
                QMessageBox.information(self.parent, "Éxito", f"Copia de seguridad creada en:\n{os.path.abspath(backup_path)}")
            except Exception as e:
                QMessageBox.critical(self.parent, "Error", f"No se pudo crear la copia de seguridad: {e}")

    def clean_old_data(self):
        """
        Elimina registros antiguos de pagos y asistencias de la base de datos.
        """
        years, ok = QInputDialog.getInt(self.parent, "Limpiar Datos", 
                                        "Eliminar registros con más de (años):", 
                                        2, 1, 10)
        
        if ok:
            confirm = QMessageBox.warning(
                self.parent, 
                "Confirmación de Seguridad", 
                f"Esta acción eliminará permanentemente pagos y asistencias de hace más de {years} año(s).\n"
                "Esta operación no se puede deshacer.\n\n¿Está absolutamente seguro?",
                QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.Cancel,
                QMessageBox.StandardButton.Cancel
            )
            
            if confirm == QMessageBox.StandardButton.Yes:
                try:
                    pagos_eliminados, asistencias_eliminadas = self.db_manager.limpiar_datos_antiguos(years)
                    QMessageBox.information(self.parent, "Limpieza Completada", 
                                            f"Se eliminaron {pagos_eliminados} registros de pagos y "
                                            f"{asistencias_eliminadas} registros de asistencias.")
                except Exception as e:
                    QMessageBox.critical(self.parent, "Error", f"No se pudo completar la limpieza: {e}")

# Exponer DEV_PASSWORD para uso por componentes web (server.py)
DEV_PASSWORD = DeveloperManager.DEV_PASSWORD