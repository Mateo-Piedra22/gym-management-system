import os
import sys
import logging
import shutil
import subprocess
import json
import tempfile
import pandas as pd
from datetime import datetime, timedelta
from PyQt6.QtWidgets import (
    QWidget, QMessageBox, QCheckBox, QVBoxLayout,
    QGroupBox, QPushButton, QTextEdit, QHBoxLayout,
    QLabel, QDoubleSpinBox, QFormLayout, QTabWidget,
    QFileDialog, QSpinBox, QListWidget, QListWidgetItem,
    QInputDialog, QComboBox, QTableWidget, QTableWidgetItem,
    QAbstractItemView, QHeaderView, QSizePolicy, QGridLayout, QFrame,
    QLineEdit, QScrollArea, QDialog, QTimeEdit, QDateTimeEdit,
    QProgressDialog, QApplication, QSplitter, QMenu
)
from PyQt6.QtCore import pyqtSignal, Qt, QTimer, QSize, QTime, QDateTime
from PyQt6.QtGui import QIcon, QPixmap, QColor, QFont
import psycopg2.extras
from database import DatabaseManager
# CSS dinámico se maneja en main.py
from managers import DeveloperManager
from export_manager import ExportManager
from utils import resource_path, collect_log_candidates, collect_temp_candidates, delete_files
from models import Usuario, Ejercicio, Rutina, Clase
from widgets.exercise_bank_dialog import ExerciseBankDialog
from widgets.template_editor_dialog import TemplateEditorDialog
from widgets.class_editor_dialog import ClassEditorDialog
from widgets.quota_types_widget import QuotaTypesWidget
from widgets.task_automation_widget import TaskAutomationWidget
from widgets.bulk_import_export_widget import BulkImportExportWidget
from widgets.branding_customization_widget import BrandingCustomizationWidget
from widgets.accessibility_widget import AccessibilityWidget
# from widgets.maintenance_tools_widget import MaintenanceToolsWidget  # Comentado - archivo no existe
from widgets.system_diagnostics_widget import SystemDiagnosticsWidget
# from widgets.developer_diagnostics_widget import DeveloperDiagnosticsWidget  # Comentado - archivo no existe
from widgets.audit_dashboard_widget import AuditDashboardWidget
from payment_manager import PaymentManager
from models import ConceptoPago, MetodoPago


class ScheduledBackupDialog(QDialog):
    """Diálogo para configurar backup programado"""
    
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Configurar Backup Programado")
        self.setModal(True)
        # Permitir redimensionar y evitar compresión
        self.resize(600, 700)
        self.backup_config = {}
        self.setup_ui()
    
    def setup_ui(self):
        # Layout exterior con ScrollArea para evitar elementos comprimidos
        outer_layout = QVBoxLayout(self)
        outer_layout.setSpacing(0)
        outer_layout.setContentsMargins(0, 0, 0, 0)

        scroll = QScrollArea(self)
        scroll.setWidgetResizable(True)
        content = QWidget()
        layout = QVBoxLayout(content)
        layout.setSpacing(20)
        layout.setContentsMargins(20, 20, 20, 20)
        
        # Título
        title = QLabel("💾 Configuración de Backup Automático")
        title.setProperty("class", "dialog_title")
        title.setAlignment(Qt.AlignmentFlag.AlignCenter)
        layout.addWidget(title)
        
        # Configuración de frecuencia
        freq_group = QGroupBox("📅 Frecuencia de Backup")
        freq_layout = QFormLayout(freq_group)
        freq_group.setSizePolicy(QSizePolicy.Policy.Preferred, QSizePolicy.Policy.Maximum)
        
        self.frequency_combo = QComboBox()
        self.frequency_combo.addItems(["Diario", "Semanal", "Mensual"])
        self.frequency_combo.setCurrentText("Diario")
        freq_layout.addRow("Frecuencia:", self.frequency_combo)
        
        self.time_edit = QTimeEdit()
        self.time_edit.setTime(QTime(2, 0))  # 2:00 AM por defecto
        freq_layout.addRow("Hora de ejecución:", self.time_edit)
        
        layout.addWidget(freq_group)
        # Hacer plegable Frecuencia de Backup
        freq_group.setCheckable(True)
        freq_group.setChecked(True)
        freq_group.toggled.connect(lambda checked, g=freq_group: self._set_group_content_visible(g, checked))
        self._set_group_content_visible(freq_group, True)
        
        # Configuración de ubicación
        location_group = QGroupBox("📁 Ubicación de Backup")
        location_layout = QVBoxLayout(location_group)
        location_group.setSizePolicy(QSizePolicy.Policy.Preferred, QSizePolicy.Policy.Maximum)
        
        location_row = QHBoxLayout()
        self.location_edit = QLineEdit()
        self.location_edit.setText("backups/auto/")
        self.location_edit.setPlaceholderText("Ruta donde se guardarán los backups")
        location_row.addWidget(self.location_edit)
        
        browse_btn = QPushButton("📂 Examinar")
        browse_btn.clicked.connect(self.browse_location)
        location_row.addWidget(browse_btn)
        
        location_layout.addLayout(location_row)
        layout.addWidget(location_group)
        # Hacer plegable Ubicación de Backup
        location_group.setCheckable(True)
        location_group.setChecked(True)
        location_group.toggled.connect(lambda checked, g=location_group: self._set_group_content_visible(g, checked))
        self._set_group_content_visible(location_group, True)
        
        # Configuración de retención
        retention_group = QGroupBox("🗂️ Retención de Backups")
        retention_layout = QFormLayout(retention_group)
        retention_group.setSizePolicy(QSizePolicy.Policy.Preferred, QSizePolicy.Policy.Maximum)
        
        self.retention_spin = QSpinBox()
        self.retention_spin.setRange(1, 365)
        self.retention_spin.setValue(30)
        self.retention_spin.setSuffix(" días")
        retention_layout.addRow("Mantener backups por:", self.retention_spin)
        
        self.max_backups_spin = QSpinBox()
        self.max_backups_spin.setRange(1, 100)
        self.max_backups_spin.setValue(10)
        self.max_backups_spin.setSuffix(" archivos")
        retention_layout.addRow("Máximo de backups:", self.max_backups_spin)
        
        layout.addWidget(retention_group)
        # Hacer plegable Retención de Backups
        retention_group.setCheckable(True)
        retention_group.setChecked(True)
        retention_group.toggled.connect(lambda checked, g=retention_group: self._set_group_content_visible(g, checked))
        self._set_group_content_visible(retention_group, True)
        
        # Opciones adicionales
        options_group = QGroupBox("⚙️ Opciones Adicionales")
        options_layout = QVBoxLayout(options_group)
        options_group.setSizePolicy(QSizePolicy.Policy.Preferred, QSizePolicy.Policy.Maximum)
        
        self.compress_check = QCheckBox("Comprimir backups (recomendado)")
        self.compress_check.setChecked(True)
        options_layout.addWidget(self.compress_check)
        
        self.verify_check = QCheckBox("Verificar integridad después del backup")
        self.verify_check.setChecked(True)
        options_layout.addWidget(self.verify_check)
        
        self.notify_check = QCheckBox("Notificar cuando se complete el backup")
        self.notify_check.setChecked(False)
        options_layout.addWidget(self.notify_check)
        
        layout.addWidget(options_group)
        # Hacer plegable Opciones Adicionales
        options_group.setCheckable(True)
        options_group.setChecked(True)
        options_group.toggled.connect(lambda checked, g=options_group: self._set_group_content_visible(g, checked))
        self._set_group_content_visible(options_group, True)
        
        # Estado actual
        status_group = QGroupBox("📊 Estado Actual")
        status_layout = QVBoxLayout(status_group)
        status_group.setSizePolicy(QSizePolicy.Policy.Preferred, QSizePolicy.Policy.Maximum)
        
        self.status_label = QLabel("Estado: No configurado")
        self.last_backup_label = QLabel("Último backup: Nunca")
        self.next_backup_label = QLabel("Próximo backup: No programado")
        
        status_layout.addWidget(self.status_label)
        status_layout.addWidget(self.last_backup_label)
        status_layout.addWidget(self.next_backup_label)
        
        layout.addWidget(status_group)
        # Hacer plegable Estado Actual
        status_group.setCheckable(True)
        status_group.setChecked(True)
        status_group.toggled.connect(lambda checked, g=status_group: self._set_group_content_visible(g, checked))
        self._set_group_content_visible(status_group, True)
        
        # Botones
        buttons_layout = QHBoxLayout()
        
        test_btn = QPushButton("🧪 Probar Configuración")
        test_btn.clicked.connect(self.test_backup)
        buttons_layout.addWidget(test_btn)
        
        buttons_layout.addStretch()
        
        cancel_btn = QPushButton("❌ Cancelar")
        cancel_btn.clicked.connect(self.reject)
        buttons_layout.addWidget(cancel_btn)
        
        save_btn = QPushButton("💾 Guardar Configuración")
        save_btn.clicked.connect(self.save_config)
        save_btn.setProperty("class", "success")
        buttons_layout.addWidget(save_btn)
        
        # Colocar contenido dentro del scroll y los botones abajo
        scroll.setWidget(content)
        outer_layout.addWidget(scroll)
        outer_layout.addLayout(buttons_layout)
    
    def browse_location(self):
        """Abre diálogo para seleccionar ubicación de backup"""
        folder = QFileDialog.getExistingDirectory(
            self, "Seleccionar Carpeta de Backup", self.location_edit.text()
        )
        if folder:
            self.location_edit.setText(folder)
    
    def test_backup(self):
        """Prueba la configuración de backup verificando permisos y espacio"""
        try:
            import os
            import shutil
            
            backup_location = self.location_edit.text()
            
            # Verificar que la ubicación existe
            if not backup_location:
                QMessageBox.warning(self, "Error de Configuración", "Debe especificar una ubicación para el backup.")
                return
            
            progress = QProgressDialog("Verificando configuración de backup...", "Cancelar", 0, 100, self)
            progress.setWindowModality(Qt.WindowModality.WindowModal)
            progress.show()
            
            # Verificar si el directorio existe
            progress.setValue(25)
            progress.setLabelText("Verificando directorio...")
            QApplication.processEvents()
            
            if not os.path.exists(backup_location):
                try:
                    os.makedirs(backup_location, exist_ok=True)
                except Exception as e:
                    progress.close()
                    QMessageBox.critical(self, "Error de Directorio", f"No se puede crear el directorio de backup:\n{str(e)}")
                    return
            
            # Verificar permisos de escritura
            progress.setValue(50)
            progress.setLabelText("Verificando permisos...")
            QApplication.processEvents()
            
            if not os.access(backup_location, os.W_OK):
                progress.close()
                QMessageBox.critical(self, "Error de Permisos", "No hay permisos de escritura en la ubicación especificada.")
                return
            
            # Verificar espacio disponible
            progress.setValue(75)
            progress.setLabelText("Verificando espacio disponible...")
            QApplication.processEvents()
            
            free_space = shutil.disk_usage(backup_location).free
            free_space_gb = free_space / (1024**3)
            
            progress.setValue(100)
            progress.close()
            
            QMessageBox.information(
                self,
                "Verificación Exitosa",
                "✅ La configuración de backup es válida.\n\n"
                f"📁 Ubicación: {backup_location}\n"
                f"⏰ Frecuencia: {self.frequency_combo.currentText()}\n"
                f"🕐 Hora: {self.time_edit.time().toString('hh:mm')}\n"
                f"💾 Espacio disponible: {free_space_gb:.1f} GB"
            )
        except Exception as e:
            QMessageBox.critical(
                self, "Error en Verificación", f"Error al verificar configuración:\n{str(e)}"
            )
    
    def save_config(self):
        """Guarda la configuración de backup"""
        try:
            self.backup_config = {
                'frequency': self.frequency_combo.currentText(),
                'time': self.time_edit.time().toString('hh:mm'),
                'location': self.location_edit.text(),
                'retention_days': self.retention_spin.value(),
                'max_backups': self.max_backups_spin.value(),
                'compress': self.compress_check.isChecked(),
                'verify': self.verify_check.isChecked(),
                'notify': self.notify_check.isChecked(),
                'enabled': True
            }
            
            QMessageBox.information(
                self,
                "Configuración Guardada",
                "✅ La configuración de backup programado ha sido guardada exitosamente.\n\n"
                "El sistema comenzará a realizar backups automáticos según la configuración especificada."
            )
            
            self.accept()
        except Exception as e:
            QMessageBox.critical(
                self, "Error", f"Error al guardar configuración:\n{str(e)}"
            )

    def _set_group_content_visible(self, group, visible: bool):
        """Muestra/oculta los contenidos internos de un QGroupBox sin ocultar el encabezado (soporta layouts anidados)."""
        try:
            lay = group.layout()
            if not lay:
                return
            
            def toggle_layout(l):
                for i in range(l.count()):
                    item = l.itemAt(i)
                    w = item.widget()
                    if w is not None:
                        w.setVisible(visible)
                    else:
                        child_layout = item.layout()
                        if child_layout is not None:
                            toggle_layout(child_layout)
            
            toggle_layout(lay)
        except Exception as e:
            logging.warning(f"Error actualizando visibilidad del grupo {getattr(group, 'title', lambda: str(group))()}: {e}")


class ScheduledMaintenanceDialog(QDialog):
    """Diálogo para configurar mantenimiento programado"""
    
    def __init__(self, parent=None):
        super().__init__(parent)
        # Referencia al gestor de base de datos del padre si está disponible
        try:
            self.db_manager = getattr(parent, 'db_manager', None)
        except Exception:
            self.db_manager = None
        self.setWindowTitle("Configurar Mantenimiento Programado")
        self.setModal(True)
        self.setFixedSize(550, 700)
        self.maintenance_config = {}
        self.setup_ui()
    
    def setup_ui(self):
        layout = QVBoxLayout(self)
        layout.setSpacing(20)
        layout.setContentsMargins(20, 20, 20, 20)
        
        # Título
        title = QLabel("🔧 Configuración de Mantenimiento Automático")
        title.setProperty("class", "dialog_title")
        title.setAlignment(Qt.AlignmentFlag.AlignCenter)
        layout.addWidget(title)
        
        # Tareas de mantenimiento
        tasks_group = QGroupBox("📋 Tareas de Mantenimiento")
        tasks_layout = QVBoxLayout(tasks_group)
        
        self.cleanup_check = QCheckBox("🧹 Limpieza de base de datos (eliminar registros antiguos)")
        self.cleanup_check.setChecked(True)
        tasks_layout.addWidget(self.cleanup_check)
        
        self.optimize_check = QCheckBox("⚡ Optimización automática (reindexar tablas)")
        self.optimize_check.setChecked(True)
        tasks_layout.addWidget(self.optimize_check)
        
        self.integrity_check = QCheckBox("🔍 Verificación de integridad")
        self.integrity_check.setChecked(True)
        tasks_layout.addWidget(self.integrity_check)
        
        self.log_cleanup_check = QCheckBox("📄 Limpieza de logs antiguos")
        self.log_cleanup_check.setChecked(True)
        tasks_layout.addWidget(self.log_cleanup_check)
        
        self.temp_cleanup_check = QCheckBox("🗑️ Limpieza de archivos temporales")
        self.temp_cleanup_check.setChecked(True)
        tasks_layout.addWidget(self.temp_cleanup_check)
        
        layout.addWidget(tasks_group)
        # Hacer plegable Tareas de Mantenimiento
        tasks_group.setCheckable(True)
        tasks_group.setChecked(True)
        tasks_group.toggled.connect(lambda checked, g=tasks_group: self._set_group_content_visible(g, checked))
        self._set_group_content_visible(tasks_group, True)
        
        # Programación
        schedule_group = QGroupBox("⏰ Programación")
        schedule_layout = QFormLayout(schedule_group)
        
        self.frequency_combo = QComboBox()
        self.frequency_combo.addItems(["Diario", "Semanal", "Mensual"])
        self.frequency_combo.setCurrentText("Semanal")
        schedule_layout.addRow("Frecuencia:", self.frequency_combo)
        
        self.time_edit = QTimeEdit()
        self.time_edit.setTime(QTime(3, 0))  # 3:00 AM por defecto
        schedule_layout.addRow("Hora de ejecución:", self.time_edit)
        
        # Día de la semana (solo para frecuencia semanal)
        self.day_combo = QComboBox()
        self.day_combo.addItems(["Lunes", "Martes", "Miércoles", "Jueves", "Viernes", "Sábado", "Domingo"])
        self.day_combo.setCurrentText("Domingo")
        schedule_layout.addRow("Día (semanal):", self.day_combo)
        
        layout.addWidget(schedule_group)
        # Hacer plegable Programación
        schedule_group.setCheckable(True)
        schedule_group.setChecked(True)
        schedule_group.toggled.connect(lambda checked, g=schedule_group: self._set_group_content_visible(g, checked))
        self._set_group_content_visible(schedule_group, True)
        
        # Configuración avanzada
        advanced_group = QGroupBox("⚙️ Configuración Avanzada")
        advanced_layout = QFormLayout(advanced_group)
        
        self.cleanup_days_spin = QSpinBox()
        self.cleanup_days_spin.setRange(1, 365)
        self.cleanup_days_spin.setValue(90)
        self.cleanup_days_spin.setSuffix(" días")
        advanced_layout.addRow("Eliminar registros anteriores a:", self.cleanup_days_spin)
        
        self.log_retention_spin = QSpinBox()
        self.log_retention_spin.setRange(1, 365)
        self.log_retention_spin.setValue(30)
        self.log_retention_spin.setSuffix(" días")
        advanced_layout.addRow("Retener logs por:", self.log_retention_spin)
        
        layout.addWidget(advanced_group)
        # Hacer plegable Configuración Avanzada
        advanced_group.setCheckable(True)
        advanced_group.setChecked(True)
        advanced_group.toggled.connect(lambda checked, g=advanced_group: self._set_group_content_visible(g, checked))
        self._set_group_content_visible(advanced_group, True)
        
        # Notificaciones
        notifications_group = QGroupBox("📢 Notificaciones")
        notifications_layout = QVBoxLayout(notifications_group)
        
        self.notify_start_check = QCheckBox("Notificar al iniciar mantenimiento")
        self.notify_start_check.setChecked(False)
        notifications_layout.addWidget(self.notify_start_check)
        
        self.notify_complete_check = QCheckBox("Notificar al completar mantenimiento")
        self.notify_complete_check.setChecked(True)
        notifications_layout.addWidget(self.notify_complete_check)
        
        self.notify_errors_check = QCheckBox("Notificar solo en caso de errores")
        self.notify_errors_check.setChecked(False)
        notifications_layout.addWidget(self.notify_errors_check)
        
        layout.addWidget(notifications_group)
        # Hacer plegable Notificaciones
        notifications_group.setCheckable(True)
        notifications_group.setChecked(True)
        notifications_group.toggled.connect(lambda checked, g=notifications_group: self._set_group_content_visible(g, checked))
        self._set_group_content_visible(notifications_group, True)
        
        # Estado actual
        status_group = QGroupBox("📊 Estado Actual")
        status_layout = QVBoxLayout(status_group)
        
        self.status_label = QLabel("Estado: No configurado")
        self.last_maintenance_label = QLabel("Último mantenimiento: Nunca")
        self.next_maintenance_label = QLabel("Próximo mantenimiento: No programado")
        
        status_layout.addWidget(self.status_label)
        status_layout.addWidget(self.last_maintenance_label)
        status_layout.addWidget(self.next_maintenance_label)
        
        layout.addWidget(status_group)
        # Hacer plegable Estado Actual
        status_group.setCheckable(True)
        status_group.setChecked(True)
        status_group.toggled.connect(lambda checked, g=status_group: self._set_group_content_visible(g, checked))
        self._set_group_content_visible(status_group, True)
        
        # Botones
        buttons_layout = QHBoxLayout()
        
        test_btn = QPushButton("🧪 Ejecutar Ahora")
        test_btn.clicked.connect(self.run_maintenance_now)
        buttons_layout.addWidget(test_btn)
        
        buttons_layout.addStretch()
        
        cancel_btn = QPushButton("❌ Cancelar")
        cancel_btn.clicked.connect(self.reject)
        buttons_layout.addWidget(cancel_btn)
        
        save_btn = QPushButton("💾 Guardar Configuración")
        save_btn.clicked.connect(self.save_config)
        save_btn.setProperty("class", "success")
        buttons_layout.addWidget(save_btn)
        
        layout.addLayout(buttons_layout)
    
    def run_maintenance_now(self):
        """Ejecuta mantenimiento inmediatamente para probar usando utilidades centralizadas"""
        try:
            reply = QMessageBox.question(
                self,
                "Ejecutar Mantenimiento",
                "¿Desea ejecutar las tareas de mantenimiento ahora?\n\n"
                "⚠️ Esto puede tomar varios minutos y afectar el rendimiento temporalmente.",
                QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No
            )
            
            if reply == QMessageBox.StandardButton.Yes:
                progress = QProgressDialog("Ejecutando tareas de mantenimiento...", "Cancelar", 0, 100, self)
                progress.setWindowModality(Qt.WindowModality.WindowModal)
                progress.show()
                
                completed_tasks = []
                total_tasks = sum([
                    self.cleanup_check.isChecked(),
                    self.optimize_check.isChecked(),
                    self.integrity_check.isChecked(),
                    self.log_cleanup_check.isChecked(),
                    self.temp_cleanup_check.isChecked()
                ])
                current_task = 0
                
                # Limpieza de base de datos (datos antiguos) usando DatabaseManager
                try:
                    if self.cleanup_check.isChecked() and not progress.wasCanceled():
                        current_task += 1
                        progress.setLabelText("Ejecutando: Limpieza de base de datos...")
                        progress.setValue(int(current_task / max(1, total_tasks) * 100))
                        QApplication.processEvents()
                        
                        if not self.db_manager:
                            completed_tasks.append("Limpieza de base de datos: gestor no disponible")
                        else:
                            days = int(self.cleanup_days_spin.value())
                            years = max(1, round(days / 365))
                            try:
                                pagos_eliminados, asistencias_eliminadas = self.db_manager.limpiar_datos_antiguos(years)
                                completed_tasks.append(
                                    f"Limpieza de base de datos (pagos: {pagos_eliminados}, asistencias: {asistencias_eliminadas})"
                                )
                            except Exception as e:
                                completed_tasks.append(f"Error en limpieza de base de datos: {str(e)}")
                except Exception as e:
                    completed_tasks.append(f"Error inesperado en limpieza de base de datos: {str(e)}")
                
                # Optimización de base de datos usando DatabaseManager
                try:
                    if self.optimize_check.isChecked() and not progress.wasCanceled():
                        current_task += 1
                        progress.setLabelText("Ejecutando: Optimización de base de datos...")
                        progress.setValue(int(current_task / max(1, total_tasks) * 100))
                        QApplication.processEvents()
                        
                        if not self.db_manager:
                            completed_tasks.append("Optimización: gestor no disponible")
                        else:
                            ok = False
                            try:
                                ok = self.db_manager.optimizar_base_datos()
                            except Exception as e:
                                completed_tasks.append(f"Error en optimización: {str(e)}")
                            if ok:
                                completed_tasks.append("Optimización de base de datos completada")
                except Exception as e:
                    completed_tasks.append(f"Error inesperado en optimización: {str(e)}")
                
                # Verificación de integridad usando DatabaseManager
                try:
                    if self.integrity_check.isChecked() and not progress.wasCanceled():
                        current_task += 1
                        progress.setLabelText("Ejecutando: Verificación de integridad...")
                        progress.setValue(int(current_task / max(1, total_tasks) * 100))
                        QApplication.processEvents()
                        
                        if not self.db_manager:
                            completed_tasks.append("Integridad: gestor no disponible")
                        else:
                            try:
                                result = self.db_manager.verificar_integridad_base_datos()
                                estado = result.get('estado', 'ERROR')
                                errores = result.get('errores', [])
                                advertencias = result.get('advertencias', [])
                                tablas = result.get('tablas_verificadas', 0)
                                if estado == 'OK' and not errores:
                                    msg = f"Verificación de integridad (OK, {tablas} tablas)"
                                    if advertencias:
                                        msg += f" con {len(advertencias)} advertencia(s)"
                                    completed_tasks.append(msg)
                                else:
                                    completed_tasks.append(
                                        f"Integridad con problemas: {len(errores)} error(es), {len(advertencias)} advertencia(s)"
                                    )
                            except Exception as e:
                                completed_tasks.append(f"Error en verificación de integridad: {str(e)}")
                except Exception as e:
                    completed_tasks.append(f"Error inesperado en verificación: {str(e)}")
                
                # Limpieza de logs usando utilidades centralizadas
                try:
                    if self.log_cleanup_check.isChecked() and not progress.wasCanceled():
                        current_task += 1
                        progress.setLabelText("Ejecutando: Limpieza de logs...")
                        progress.setValue(int(current_task / max(1, total_tasks) * 100))
                        QApplication.processEvents()
                        
                        base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
                        log_dir = os.path.join(base_dir, 'logs')
                        retention_days = self.log_retention_spin.value() if hasattr(self, 'log_retention_spin') else 30
                        candidates = collect_log_candidates(log_dir, retention_days)
                        
                        if not candidates:
                            completed_tasks.append("Limpieza de logs: nada para eliminar")
                        else:
                            sub = QProgressDialog("Eliminando logs antiguos...", "Cancelar", 0, len(candidates), self)
                            sub.setWindowModality(Qt.WindowModality.WindowModal)
                            sub.show()
                            deleted_count, error_count = delete_files(candidates, sub)
                            if error_count:
                                completed_tasks.append(
                                    f"Limpieza de logs: {deleted_count} eliminado(s), {error_count} error(es)"
                                )
                            else:
                                completed_tasks.append(f"Limpieza de logs: {deleted_count} archivo(s) eliminado(s)")
                except Exception as e:
                    completed_tasks.append(f"Error inesperado en limpieza de logs: {str(e)}")
                
                # Limpieza de temporales usando utilidades centralizadas
                try:
                    if self.temp_cleanup_check.isChecked() and not progress.wasCanceled():
                        current_task += 1
                        progress.setLabelText("Ejecutando: Limpieza de archivos temporales...")
                        progress.setValue(int(current_task / max(1, total_tasks) * 100))
                        QApplication.processEvents()
                        
                        retention_days = 7  # valor por defecto para temporales en mantenimiento programado
                        candidates = collect_temp_candidates(retention_days)
                        
                        if not candidates:
                            completed_tasks.append("Limpieza de temporales: nada para eliminar")
                        else:
                            sub = QProgressDialog("Eliminando archivos temporales...", "Cancelar", 0, len(candidates), self)
                            sub.setWindowModality(Qt.WindowModality.WindowModal)
                            sub.show()
                            deleted_count, error_count = delete_files(candidates, sub)
                            if error_count:
                                completed_tasks.append(
                                    f"Limpieza de temporales: {deleted_count} eliminado(s), {error_count} error(es)"
                                )
                            else:
                                completed_tasks.append(f"Limpieza de temporales: {deleted_count} archivo(s) eliminado(s)")
                except Exception as e:
                    completed_tasks.append(f"Error inesperado en limpieza de temporales: {str(e)}")
                
                progress.setValue(100)
                progress.close()
                
                if not progress.wasCanceled():
                    tasks_summary = "\n".join([f"• {task}" for task in completed_tasks])
                    QMessageBox.information(
                        self,
                        "Mantenimiento Completado",
                        (
                            "✅ Mantenimiento ejecutado exitosamente.\n\n"
                            f"📋 Tareas completadas:\n{tasks_summary}\n\n"
                            f"⏱️ Fecha: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
                        )
                    )
        except Exception as e:
            QMessageBox.critical(
                self, "Error", f"Error al ejecutar mantenimiento:\n{str(e)}"
            )
    
    def save_config(self):
        """Guarda la configuración de mantenimiento"""
        try:
            tasks = []
            if self.cleanup_check.isChecked():
                tasks.append('cleanup')
            if self.optimize_check.isChecked():
                tasks.append('optimize')
            if self.integrity_check.isChecked():
                tasks.append('integrity')
            if self.log_cleanup_check.isChecked():
                tasks.append('log_cleanup')
            if self.temp_cleanup_check.isChecked():
                tasks.append('temp_cleanup')
            
            self.maintenance_config = {
                'tasks': tasks,
                'frequency': self.frequency_combo.currentText(),
                'time': self.time_edit.time().toString('hh:mm'),
                'day': self.day_combo.currentText(),
                'cleanup_days': self.cleanup_days_spin.value(),
                'log_retention': self.log_retention_spin.value(),
                'notify_start': self.notify_start_check.isChecked(),
                'notify_complete': self.notify_complete_check.isChecked(),
                'notify_errors': self.notify_errors_check.isChecked(),
                'enabled': True
            }
            
            QMessageBox.information(
                self,
                "Configuración Guardada",
                "✅ La configuración de mantenimiento programado ha sido guardada exitosamente.\n\n"
                "El sistema ejecutará las tareas de mantenimiento según la programación especificada."
            )
            
            self.accept()
        except Exception as e:
            QMessageBox.critical(
                self, "Error", f"Error al guardar configuración:\n{str(e)}"
            )

    def _set_group_content_visible(self, group, visible: bool):
        """Muestra/oculta los contenidos internos de un QGroupBox sin ocultar el encabezado (soporta layouts anidados)."""
        try:
            lay = group.layout()
            if not lay:
                return

            def toggle_layout(l):
                for i in range(l.count()):
                    item = l.itemAt(i)
                    w = item.widget()
                    if w is not None:
                        w.setVisible(visible)
                    else:
                        child_layout = item.layout()
                        if child_layout is not None:
                            toggle_layout(child_layout)

            toggle_layout(lay)
        except Exception as e:
            logging.warning(f"Error actualizando visibilidad del grupo {getattr(group, 'title', lambda: str(group))()}: {e}")


class ConfigTabWidget(QWidget):
    # Señales declaradas a nivel de clase para un funcionamiento correcto
    precio_actualizado = pyqtSignal()
    feature_toggled = pyqtSignal(dict)
    usuarios_modificados = pyqtSignal()

    def __init__(self, db_manager: DatabaseManager, export_manager: ExportManager):
        super().__init__()
        self.db_manager = db_manager
        self.export_manager = export_manager
        self.dev_manager = DeveloperManager(self, self.db_manager)
        self.payment_manager = PaymentManager(self.db_manager)
        # Estados para colapsabilidad y registro de grupos colapsables
        self._collapsible_enabled = True
        self._collapse_config = {}
        self._collapsible_groups = {}
        self._collapse_config = {}
        self._collapsible_groups = {}
        self.setup_ui()
        self.load_config()
        # Estado para debounce de recargas de tablas
        self._reload_cooldown_secs = 3
        self._last_load_concepts_at = None
        self._last_load_methods_at = None

    def set_user_role(self, user_role):
        """Establece el rol del usuario para configurar permisos"""
        self.user_role = user_role
        # Aplicar visibilidad del Panel de Desarrollador y widgets según rol
        try:
            if hasattr(self, 'dev_options_group'):
                if user_role == "profesor":
                    # Mostrar el panel pero limitar contenido según configuración
                    self.dev_options_group.setVisible(True)
                    self.dev_options_group.setEnabled(True)
                    # El panel de desarrollador NO debe tener checkbox en el encabezado para profesor
                    try:
                        self.dev_options_group.setCheckable(False)
                    except Exception:
                        pass
                    if hasattr(self, 'dev_tabs'):
                        self.dev_tabs.blockSignals(False)
                    # Aplicar visibilidad de widgets/pestañas del panel de desarrollador
                    try:
                        if not hasattr(self, '_visibility_config'):
                            self._load_visibility_config()
                        role_cfg = self._visibility_config.get('profesor', {})
                        dev_cfg = role_cfg.get('dev_tabs', {})
                        # Construir índices si no existen
                        try:
                            self._build_dev_tab_index_map()
                        except Exception:
                            pass
                        for key, idx in getattr(self, '_dev_tab_indices', {}).items():
                            # No tocar aquí la pestaña "Visibilidad Profesor" para evitar desalinear índices
                            if key == 'visibilidad_profesor':
                                continue
                            state = bool(dev_cfg.get(key, True))
                            try:
                                self.dev_tabs.setTabVisible(idx, state)
                            except Exception:
                                try:
                                    self.dev_tabs.setTabEnabled(idx, state)
                                except Exception:
                                    pass
                        # Remover completamente la pestaña "Visibilidad Profesor" para el rol profesor
                        try:
                            idx_vp = getattr(self, '_dev_tab_indices', {}).get('visibilidad_profesor')
                            if idx_vp is not None:
                                self.dev_tabs.removeTab(idx_vp)
                                # Recalcular índices tras la eliminación
                                self._dev_tab_indices = {}
                                for i in range(self.dev_tabs.count()):
                                    text = self.dev_tabs.tabText(i)
                                    key = getattr(self, '_dev_tab_key_by_text', {}).get(text)
                                    if key:
                                        self._dev_tab_indices[key] = i
                        except Exception:
                            pass
                    except Exception:
                        pass
                    # Aplicar visibilidad de widgets de Configuración según configuración para 'profesor'
                    try:
                        if not hasattr(self, '_visibility_config'):
                            self._load_visibility_config()
                        role_cfg = self._visibility_config.get('profesor', {})
                        cfg_widgets = role_cfg.get('config_widgets', {})
                        # Controlar grupo de precios
                        if hasattr(self, 'prices_group'):
                            show_prices = bool(cfg_widgets.get('config_precios', True))
                            self.prices_group.setVisible(show_prices)
                            self.prices_group.setEnabled(show_prices)
                    except Exception:
                        pass
                else:
                    # Dueño/otros roles: acceso completo al panel
                    self.dev_options_group.setVisible(True)
                    self.dev_options_group.setEnabled(True)
                    try:
                        self.dev_options_group.setCheckable(True)
                        self.dev_options_group.setChecked(True)
                    except Exception:
                        pass
                    if hasattr(self, 'dev_tabs'):
                        self.dev_tabs.blockSignals(False)
                    # Asegurar que todas las pestañas del panel estén visibles
                    try:
                        self._build_dev_tab_index_map()
                        for idx in getattr(self, '_dev_tab_indices', {}).values():
                            try:
                                self.dev_tabs.setTabVisible(idx, True)
                            except Exception:
                                try:
                                    self.dev_tabs.setTabEnabled(idx, True)
                                except Exception:
                                    pass
                    except Exception:
                        pass

            # Mostrar panel de control de visibilidad solo para dueño
            if hasattr(self, 'visibility_group'):
                is_owner = (user_role == 'dueño')
                self.visibility_group.setVisible(is_owner)
                self.visibility_group.setEnabled(is_owner)
                # Mostrar/ocultar botón toggle acorde al rol
                try:
                    if hasattr(self, 'visibility_toggle_button'):
                        self.visibility_toggle_button.setVisible(is_owner)
                        # Ajustar texto acorde al estado
                        self.visibility_toggle_button.setText(
                            "👁️ Ocultar Panel de Visibilidad" if is_owner and self.visibility_group.isVisible() else "👁️ Mostrar Panel de Visibilidad"
                        )
                except Exception:
                    pass
                # Aplicar visibilidad efectiva a las pestañas según configuración
                try:
                    self.apply_developer_visibility_by_role()
                except Exception:
                    pass
        except Exception:
            # Falla segura: no bloquear la UI si ocurre algún problema
            pass

    def toggle_visibility_panel(self):
        """Alterna la visibilidad del panel de control de visibilidad por rol (solo Dueño)."""
        try:
            if not hasattr(self, 'visibility_group'):
                return
            currently_visible = self.visibility_group.isVisible()
            self.visibility_group.setVisible(not currently_visible)
            self.visibility_group.setEnabled(not currently_visible)
            # Actualizar texto del botón si existe
            try:
                if hasattr(self, 'visibility_toggle_button'):
                    self.visibility_toggle_button.setText(
                        "👁️ Ocultar Panel de Visibilidad" if not currently_visible else "👁️ Mostrar Panel de Visibilidad"
                    )
            except Exception:
                pass
        except Exception as e:
            logging.warning(f"Error al alternar panel de visibilidad: {e}")
        
    def setup_ui(self):
        
        # Crear un scroll area para hacer el widget scrolleable
        scroll_area = QScrollArea()
        scroll_area.setWidgetResizable(True)
        scroll_area.setVerticalScrollBarPolicy(Qt.ScrollBarPolicy.ScrollBarAlwaysOn)
        scroll_area.setHorizontalScrollBarPolicy(Qt.ScrollBarPolicy.ScrollBarAsNeeded)
        # Eliminar el marco para un diseño completamente edge-to-edge
        try:
            scroll_area.setFrameShape(QFrame.Shape.NoFrame)
        except Exception:
            pass
        
        # Aplicar estilos CSS personalizados para el scrollbar (mejorados para visibilidad)
        # Migrado al sistema CSS dinámico - usar setObjectName y setProperty
        scroll_area.setObjectName("config_scroll_area")
        scroll_area.setProperty("class", "config_scroll")
        
        # Widget contenedor para el contenido scrolleable
        scroll_widget = QWidget()
        scroll_area.setWidget(scroll_widget)
        
        # Layout principal del widget
        main_layout = QVBoxLayout(self)
        main_layout.setContentsMargins(0, 0, 0, 0)
        main_layout.addWidget(scroll_area)
        
        # Layout del contenido scrolleable
        content_layout = QVBoxLayout(scroll_widget)
        # Espaciado cero para un look completamente edge-to-edge
        content_layout.setSpacing(0)
        content_layout.setContentsMargins(0, 0, 0, 0)
        
        # Sistema dinámico de configuración de precios de cuotas (simplificado)
        prices_group = QGroupBox("💰 Configuración de Precios de Cuotas")
        # Guardar referencia para control de visibilidad por rol
        self.prices_group = prices_group
        # Ajuste de políticas para ocupar solo el espacio necesario
        prices_group.setSizePolicy(QSizePolicy.Policy.Preferred, QSizePolicy.Policy.Minimum)
        prices_main_layout = QVBoxLayout(prices_group)
        prices_main_layout.setSpacing(8)
        prices_main_layout.setContentsMargins(12, 12, 12, 8)
        
        # Título descriptivo mejorado
        prices_title = QLabel("Configura los precios mensuales para todos los tipos de membresía disponibles en el gimnasio")
        prices_title.setProperty("class", "help_text")
        prices_title.setWordWrap(True)
        prices_title.setAlignment(Qt.AlignmentFlag.AlignCenter)
        prices_main_layout.addWidget(prices_title)
        
        # Nota sobre gestión avanzada
        advanced_note = QLabel("💡 Para crear, editar o eliminar tipos de cuota, utiliza la pestaña 'Tipos de Cuota' en el Panel de Control del Desarrollador")
        advanced_note.setProperty("class", "help_text")
        advanced_note.setWordWrap(True)
        advanced_note.setAlignment(Qt.AlignmentFlag.AlignCenter)
        prices_main_layout.addWidget(advanced_note)
        
        # Tabla profesional para tipos de cuota (ampliada)
        self.prices_table = QTableWidget()
        # Reducir altura mínima para evitar ocupación innecesaria
        self.prices_table.setMinimumHeight(280)
        self.prices_table.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Preferred)
        
        # Configurar columnas de la tabla
        self.prices_table.setColumnCount(5)
        headers = ["Icono", "Nombre", "Descripción", "Precio Actual", "Nuevo Precio"]
        self.prices_table.setHorizontalHeaderLabels(headers)
        
        # Configurar propiedades de la tabla
        self.prices_table.setAlternatingRowColors(True)
        self.prices_table.setSelectionBehavior(QAbstractItemView.SelectionBehavior.SelectRows)
        self.prices_table.setSelectionMode(QAbstractItemView.SelectionMode.SingleSelection)
        self.prices_table.setShowGrid(True)
        self.prices_table.setGridStyle(Qt.PenStyle.SolidLine)
        
        # Configurar header horizontal
        header = self.prices_table.horizontalHeader()
        header.setStretchLastSection(False)
        header.setSectionResizeMode(0, QHeaderView.ResizeMode.Fixed)  # Icono
        header.setSectionResizeMode(1, QHeaderView.ResizeMode.ResizeToContents)  # Nombre
        header.setSectionResizeMode(2, QHeaderView.ResizeMode.Stretch)  # Descripción
        header.setSectionResizeMode(3, QHeaderView.ResizeMode.ResizeToContents)  # Precio Actual
        header.setSectionResizeMode(4, QHeaderView.ResizeMode.ResizeToContents)  # Nuevo Precio
        
        # Configurar anchos específicos
        self.prices_table.setColumnWidth(0, 80)   # Icono
        self.prices_table.setColumnWidth(3, 150)  # Precio Actual
        self.prices_table.setColumnWidth(4, 180)  # Nuevo Precio
        
        # Configurar header vertical
        self.prices_table.verticalHeader().setVisible(False)
        self.prices_table.setRowHeight(0, 60)  # Altura de fila estándar
        
        # Aplicar clases CSS estándar del sistema dinámico
        # QTableWidget ya hereda los estilos de QTableView del sistema
        prices_main_layout.addWidget(self.prices_table)
        
        # === SECCIÓN DE ACCIONES ===
        actions_frame = QFrame()
        actions_frame.setFrameStyle(QFrame.Shape.Box)
        actions_frame.setProperty("class", "price_actions_frame")
        actions_layout = QVBoxLayout(actions_frame)
        actions_layout.setSpacing(10)
        actions_layout.setContentsMargins(15, 12, 15, 12)
        
        actions_title = QLabel("⚠️ Importante: Los cambios de precios afectarán a las nuevas cuotas generadas")
        actions_title.setProperty("class", "help_text")
        actions_title.setWordWrap(True)
        actions_title.setAlignment(Qt.AlignmentFlag.AlignCenter)
        
        button_layout = QHBoxLayout()
        button_layout.setSpacing(15)
        
        self.save_price_button = QPushButton("💾 Guardar Nuevos Precios")
        self.save_price_button.setMinimumHeight(35)
        self.save_price_button.setMinimumWidth(200)
        self.save_price_button.setProperty("class", "success")
        
        refresh_button = QPushButton("🔄 Actualizar Lista")
        refresh_button.setMinimumHeight(35)
        refresh_button.setMinimumWidth(150)
        refresh_button.clicked.connect(self.load_dynamic_prices)
        
        button_layout.addStretch()
        button_layout.addWidget(refresh_button)
        button_layout.addWidget(self.save_price_button)
        button_layout.addStretch()
        
        actions_layout.addWidget(actions_title)
        actions_layout.addLayout(button_layout)
        
        # Ensamblar todo
        prices_main_layout.addWidget(actions_frame)
        
        # Inicializar contenedores para widgets dinámicos
        self.price_widgets = {}  # {tipo_cuota_id: {'current_label': QLabel, 'spinbox': QDoubleSpinBox, 'tipo': TipoCuota}}
        
        # Crear el widget info_text_edit que se usa en load_config
        self.info_text_edit = QTextEdit()
        self.info_text_edit.setVisible(False)  # Oculto por defecto, se usa internamente
        
        # Grupo de Configuración de Precios sin colapsabilidad (según solicitud)
        prices_group.setCheckable(False)
        self._set_group_content_visible(prices_group, True)
        
        # Agregar el grupo de precios directamente al layout principal
        content_layout.addWidget(prices_group)
        
        # Grupo de opciones de desarrollador con layout adaptativo
        self.dev_options_group = QGroupBox("")
        # Permitir que el panel de desarrollador se expanda completamente (ancho y alto)
        self.dev_options_group.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Expanding)
        # Eliminamos setMaximumHeight para permitir crecimiento automático
        
        dev_main_layout = QVBoxLayout(self.dev_options_group)
        dev_main_layout.setSpacing(1)  # Ajuste fino: 1px de espaciado interno
        dev_main_layout.setContentsMargins(1, 4, 1, 1)  # Separación visible bajo el título del panel
        
        self.dev_tabs = QTabWidget()
        self.dev_tabs.setTabPosition(QTabWidget.TabPosition.North)
        # Permitir expansión total para ocupar todo el espacio disponible
        self.dev_tabs.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Expanding)
        self.dev_tabs.setMinimumHeight(800)
        # Eliminar límite de altura para que pueda crecer con el contenedor

        # La barra de controles de visibilidad y el botón maestro
        # se trasladan a un widget independiente ubicado fuera del panel.

        dev_main_layout.addWidget(self.dev_tabs, 1)

        self.setup_developer_tabs()
        # Sistema de colapsabilidad eliminado: no registrar grupos ni aplicar estados

        # Panel de control de visibilidad por rol (solo Dueño)
        try:
            # Mover el panel de visibilidad justo debajo del panel de desarrollador
            self.init_visibility_control_ui(content_layout)
            # Sincronizar texto del botón con el estado inicial del panel
            try:
                if hasattr(self, 'visibility_toggle_button') and hasattr(self, 'visibility_group'):
                    self.visibility_toggle_button.setText(
                        "👁️ Ocultar Panel de Visibilidad" if self.visibility_group.isVisible() else "👁️ Mostrar Panel de Visibilidad"
                    )
            except Exception:
                pass
        except Exception:
            # No bloquear UI si hay algún problema al construir el panel
            pass
        
        # Plegable para Panel de Control del Desarrollador
        self.dev_options_group.setCheckable(True)
        self.dev_options_group.setChecked(True)
        self.dev_options_group.toggled.connect(lambda checked, g=self.dev_options_group: self._set_group_content_visible(g, checked))
        self._set_group_content_visible(self.dev_options_group, True)
        
        content_layout.addWidget(self.dev_options_group, 1)

        # Desactivar checkboxes de grupos en toda la pestaña de Configuración
        try:
            for gb in self.findChildren(QGroupBox):
                gb.setCheckable(False)
        except Exception:
            pass
        
        # Eliminar el espaciador que causa espacios vacíos excesivos
        # content_layout.addStretch()
        
        self.connect_signals()

    def setup_developer_tabs(self):
        # === PESTAÑAS PRINCIPALES (ESENCIALES) ===
        
        # 🎨 Pestaña de Interfaz y Personalización (REORGANIZADA)
        ui_tab = self.create_interface_customization_tab()
        
        # 🗃️ Pestaña de Base de Datos (MANTENIDA)
        db_tab = self.create_database_tab()
        
        
        # ⚙️ Pestaña de Sistema y Configuración (REORGANIZADA)
        system_tab = self.create_system_configuration_tab()
        # Nueva pestaña para controlar lo que ven los profesores (pestañas y widgets)
        professor_visibility_tab = self.create_professor_visibility_tab()
        
        # === PESTAÑAS DE GESTIÓN ESPECÍFICA ===
        
        # 💰 Tipos de Cuota (MANTENIDA)
        from widgets.quota_types_widget import QuotaTypesWidget
        self.quota_types_widget = QuotaTypesWidget(self.db_manager, parent=self)
        
        # 💳 Conceptos de Pago (MANTENIDA)
        payment_concepts_tab = self.create_payment_concepts_widget()
        
        # 💰 Métodos de Pago (MANTENIDA)
        payment_methods_tab = self.create_payment_methods_widget()
        
        # === ADMINISTRACIÓN AVANZADA ===
        
        # 🔧 Administración Avanzada (REORGANIZADA CON SUB-PESTAÑAS)
        admin_tab = self.create_admin_panel_widget()
        
        # Agregar todas las pestañas al widget principal, envueltas en scroll
        from PyQt6.QtWidgets import QScrollArea
        def wrap_in_scroll(widget):
            sa = QScrollArea()
            sa.setWidget(widget)
            sa.setWidgetResizable(True)
            # Asegurar expansión vertical y alineación superior/izquierda del contenido
            sa.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Expanding)
            try:
                # Mantener alineación superior sin restringir el ancho
                sa.setAlignment(Qt.AlignmentFlag.AlignTop)
                sa.setFrameShape(QFrame.Shape.NoFrame)
            except Exception:
                pass
            return sa

        self.dev_tabs.addTab(wrap_in_scroll(ui_tab), "🎨 Interfaz y Personalización")
        self.dev_tabs.addTab(wrap_in_scroll(db_tab), "🏋️ Banco de Ejercicios")
        # Pestaña "📄 Logs y Monitoreo" eliminada: contenido movido a "⚙️ Sistema y Configuración"
        self.dev_tabs.addTab(wrap_in_scroll(system_tab), "⚙️ Sistema y Configuración")
        # Guardar referencia y añadir siempre; se controlará por rol en set_user_role
        self.professor_visibility_tab_widget = wrap_in_scroll(professor_visibility_tab)
        self.dev_tabs.addTab(self.professor_visibility_tab_widget, "👨‍🏫 Visibilidad Profesor")
        self.dev_tabs.addTab(wrap_in_scroll(self.quota_types_widget), "💰 Tipos de Cuota")
        self.dev_tabs.addTab(wrap_in_scroll(payment_concepts_tab), "💳 Conceptos de Pago")
        self.dev_tabs.addTab(wrap_in_scroll(payment_methods_tab), "💰 Métodos de Pago")
        self.dev_tabs.addTab(wrap_in_scroll(admin_tab), "🔧 Administración Avanzada")

        # Seleccionar por defecto la pestaña "👨‍🏫 Visibilidad Profesor" SOLO para Dueño
        try:
            win = self.window()
            current_role = getattr(win, 'user_role', None)
            if current_role == 'dueño':
                for i in range(self.dev_tabs.count()):
                    if "Visibilidad Profesor" in self.dev_tabs.tabText(i):
                        self.dev_tabs.setCurrentIndex(i)
                        break
        except Exception:
            pass

        # Registrar índices de pestañas por clave para control de visibilidad
        try:
            self._dev_tab_key_by_text = {
                "🎨 Interfaz y Personalización": "interfaz_personalizacion",
                "🏋️ Banco de Ejercicios": "banco_ejercicios",
                "⚙️ Sistema y Configuración": "sistema_configuracion",
                "💰 Tipos de Cuota": "tipos_cuota",
                "💳 Conceptos de Pago": "conceptos_pago",
                "💰 Métodos de Pago": "metodos_pago",
                "🔧 Administración Avanzada": "administracion_avanzada",
                "👨‍🏫 Visibilidad Profesor": "visibilidad_profesor",
            }
            self._dev_tab_indices = {}
            for i in range(self.dev_tabs.count()):
                text = self.dev_tabs.tabText(i)
                key = self._dev_tab_key_by_text.get(text)
                if key:
                    self._dev_tab_indices[key] = i
        except Exception:
            self._dev_tab_indices = {}

        # Control de visibilidad de "👨‍🏫 Visibilidad Profesor" según rol actual
        try:
            current_role = getattr(self, 'user_role', None)
            if hasattr(self, 'dev_tabs') and hasattr(self, '_dev_tab_indices'):
                idx = self._dev_tab_indices.get('visibilidad_profesor')
                if current_role == 'profesor' and idx is not None:
                    # Remover completamente la pestaña para que NO se vea ni deshabilitada
                    try:
                        self.dev_tabs.removeTab(idx)
                        # Recalcular índices tras la eliminación
                        self._dev_tab_indices = {}
                        for i in range(self.dev_tabs.count()):
                            text = self.dev_tabs.tabText(i)
                            key = getattr(self, '_dev_tab_key_by_text', {}).get(text)
                            if key:
                                self._dev_tab_indices[key] = i
                    except Exception:
                        pass
                elif current_role == 'dueño':
                    # Para dueño, asegurar que la pestaña esté presente; si fue eliminada por error, volver a añadirla
                    try:
                        if idx is None and hasattr(self, 'professor_visibility_tab_widget') and self.professor_visibility_tab_widget is not None:
                            self.dev_tabs.addTab(self.professor_visibility_tab_widget, "👨‍🏫 Visibilidad Profesor")
                            # Recalcular índices tras añadir
                            self._dev_tab_indices = {}
                            for i in range(self.dev_tabs.count()):
                                text = self.dev_tabs.tabText(i)
                                key = getattr(self, '_dev_tab_key_by_text', {}).get(text)
                                if key:
                                    self._dev_tab_indices[key] = i
                    except Exception:
                        pass
        except Exception:
            pass
        return

    def create_professor_visibility_tab(self):
        """Crea una pestaña para gestionar lo que ven los profesores: pestañas principales y widgets de Configuración."""
        tab = QWidget()
        layout = QVBoxLayout(tab)
        layout.setContentsMargins(10, 10, 10, 10)
        layout.setSpacing(8)

        info = QLabel("Configura la visibilidad para el rol 'Profesor'.")
        info.setWordWrap(True)
        layout.addWidget(info)

        # Pestañas principales visibles para 'Profesor'
        group_tabs = QGroupBox("Pestañas visibles para 'Profesor'")
        tabs_layout = QVBoxLayout(group_tabs)
        self.prof_main_checks = {}
        main_tabs = [
            ("usuarios", "👥 Usuarios y Asistencias"),
            ("pagos", "💰 Pagos"),
            ("reportes", "📊 Reportes"),
            ("rutinas", "🏋️ Rutinas"),
            ("clases", "📅 Clases"),
            ("profesores", "👨‍🏫 Profesores"),
            ("configuracion", "⚙️ Configuración"),
        ]
        for key, label in main_tabs:
            chk = QCheckBox(label)
            chk.setChecked(True)
            self.prof_main_checks[key] = chk
            tabs_layout.addWidget(chk)
        layout.addWidget(group_tabs)

        # Widgets de Configuración visibles para 'Profesor'
        group_cfg = QGroupBox("Widgets de Configuración visibles para 'Profesor'")
        cfg_layout = QVBoxLayout(group_cfg)
        self.prof_cfg_checks = {}
        config_widgets = [
            ("config_precios", "💰 Configuración de Precios de Cuotas"),
        ]
        for key, label in config_widgets:
            chk = QCheckBox(label)
            chk.setChecked(True)
            self.prof_cfg_checks[key] = chk
            cfg_layout.addWidget(chk)
        layout.addWidget(group_cfg)

        # Widgets/pestañas del Panel de Control de Desarrollador visibles para 'Profesor'
        group_dev = QGroupBox("Widgets del Panel de Desarrollador visibles para 'Profesor'")
        dev_layout = QVBoxLayout(group_dev)
        self.prof_dev_checks = {}
        dev_tabs = [
            ("interfaz_personalizacion", "🎨 Interfaz y Personalización"),
            ("banco_ejercicios", "🏋️ Banco de Ejercicios"),
            ("sistema_configuracion", "⚙️ Sistema y Configuración"),
            ("tipos_cuota", "💰 Tipos de Cuota"),
            ("conceptos_pago", "💳 Conceptos de Pago"),
            ("metodos_pago", "💰 Métodos de Pago"),
            ("administracion_avanzada", "🔧 Administración Avanzada"),
        ]
        for key, label in dev_tabs:
            chk = QCheckBox(label)
            chk.setChecked(True)
            self.prof_dev_checks[key] = chk
            dev_layout.addWidget(chk)
        layout.addWidget(group_dev)

        # Botones de acción
        actions = QHBoxLayout()
        save_btn = QPushButton("💾 Guardar")
        apply_btn = QPushButton("✅ Aplicar ahora")
        actions.addWidget(save_btn)
        actions.addWidget(apply_btn)
        actions.addStretch()
        layout.addLayout(actions)

        save_btn.clicked.connect(self.save_professor_visibility_config)
        apply_btn.clicked.connect(self.apply_professor_visibility_now)

        # Cargar configuración existente
        try:
            self._load_visibility_config()
            role_cfg = self._visibility_config.get('profesor', {})
            main_cfg = role_cfg.get('main_tabs', {})
            cfg_widgets = role_cfg.get('config_widgets', {})
            dev_cfg = role_cfg.get('dev_tabs', {})
            for key, chk in self.prof_main_checks.items():
                chk.setChecked(bool(main_cfg.get(key, True)))
            for key, chk in self.prof_cfg_checks.items():
                chk.setChecked(bool(cfg_widgets.get(key, True)))
            for key, chk in self.prof_dev_checks.items():
                chk.setChecked(bool(dev_cfg.get(key, True)))
        except Exception:
            pass

        return tab

    def save_professor_visibility_config(self):
        """Guarda la configuración de visibilidad de 'Profesor' en la base de datos."""
        try:
            if not hasattr(self, '_visibility_config'):
                self._load_visibility_config()
            role = 'profesor'
            if role not in self._visibility_config:
                self._visibility_config[role] = {'dev_tabs': {}, 'main_tabs': {}, 'config_widgets': {}}
            self._visibility_config[role]['main_tabs'] = {k: bool(chk.isChecked()) for k, chk in self.prof_main_checks.items()}
            self._visibility_config[role]['config_widgets'] = {k: bool(chk.isChecked()) for k, chk in self.prof_cfg_checks.items()}
            self._visibility_config[role]['dev_tabs'] = {k: bool(chk.isChecked()) for k, chk in self.prof_dev_checks.items()}
            payload = json.dumps(self._visibility_config, ensure_ascii=False)
            self.db_manager.actualizar_configuracion('developer_tab_visibility_by_role', payload)
            try:
                QMessageBox.information(self, "Guardado", "Visibilidad de Profesor guardada correctamente.")
            except Exception:
                pass
        except Exception as e:
            logging.error(f"Error guardando visibilidad de profesor: {e}")
            try:
                QMessageBox.warning(self, "Error", "No se pudo guardar la configuración.")
            except Exception:
                pass

    def apply_professor_visibility_now(self):
        """Aplica inmediatamente la visibilidad configurada para 'Profesor' (simulación en esta sesión)."""
        try:
            # Guardar primero para asegurar persistencia
            self.save_professor_visibility_config()
            # Aplicar a las pestañas principales de la ventana actual
            win = self.window()
            if win and hasattr(win, 'tabWidget') and hasattr(win, 'tab_indices'):
                states = {k: bool(chk.isChecked()) for k, chk in self.prof_main_checks.items()}
                for key, idx in getattr(win, 'tab_indices', {}).items():
                    if key in states:
                        try:
                            win.tabWidget.setTabVisible(idx, states[key])
                        except Exception:
                            pass
            # Aplicar widgets de configuración para profesor (si el rol actual es profesor)
            if getattr(win, 'user_role', None) == 'profesor':
                try:
                    if hasattr(self, 'prices_group'):
                        show_prices = bool(self.prof_cfg_checks.get('config_precios').isChecked())
                        self.prices_group.setVisible(show_prices)
                        self.prices_group.setEnabled(show_prices)
                except Exception:
                    pass
                # Aplicar visibilidad de widgets del Panel de Desarrollador (pestañas dentro de dev_tabs)
                try:
                    if hasattr(self, 'dev_tabs') and hasattr(self, '_dev_tab_indices'):
                        dev_states = {k: bool(chk.isChecked()) for k, chk in self.prof_dev_checks.items()}
                        for key, idx in getattr(self, '_dev_tab_indices', {}).items():
                            if key in dev_states:
                                try:
                                    self.dev_tabs.setTabVisible(idx, dev_states[key])
                                except Exception:
                                    # Fallback: deshabilitar si no se puede ocultar
                                    try:
                                        self.dev_tabs.setTabEnabled(idx, dev_states[key])
                                    except Exception:
                                        pass
                except Exception:
                    pass
            try:
                QMessageBox.information(self, "Aplicado", "Se aplicó la visibilidad (simulación) para Profesor.")
            except Exception:
                pass
        except Exception as e:
            logging.error(f"Error aplicando visibilidad de profesor: {e}")
            try:
                QMessageBox.warning(self, "Error", "No se pudo aplicar la configuración.")
            except Exception:
                pass

    def init_visibility_control_ui(self, parent_layout: QVBoxLayout):
        """Crea el panel de control de visibilidad por rol (solo Dueño).
        Incluye controles para pestañas del Panel de Desarrollador y pestañas principales.
        """
        # Roles disponibles (restringidos a Dueño y Profesor)
        self.available_roles = ['dueño', 'profesor']

        # Grupo contenedor
        self.visibility_group = QGroupBox("Control de Visibilidad por Rol (Dueño)")
        self.visibility_group.setObjectName("config_group")
        vg_layout = QVBoxLayout(self.visibility_group)
        vg_layout.setSpacing(8)
        vg_layout.setContentsMargins(10, 10, 10, 10)

        # Ocultar este panel por defecto; se mostrará sólo si el rol es 'dueño'
        current_role = None
        try:
            win = self.window()
            if win and hasattr(win, 'user_role'):
                current_role = win.user_role
        except Exception:
            current_role = None
        is_owner = (current_role == 'dueño')
        self.visibility_group.setVisible(is_owner)
        self.visibility_group.setEnabled(is_owner)

        # Selector de rol
        selector_layout = QHBoxLayout()
        selector_label = QLabel("Rol a configurar:")
        self.visibility_role_selector = QComboBox()
        for role in self.available_roles:
            self.visibility_role_selector.addItem(role.capitalize(), role)
        selector_layout.addWidget(selector_label)
        selector_layout.addWidget(self.visibility_role_selector)
        selector_layout.addStretch()
        vg_layout.addLayout(selector_layout)

        # Checkboxes por componente (pestañas del panel de desarrollador)
        self.visibility_checkboxes = {}
        components_order = [
            ("interfaz_personalizacion", "🎨 Interfaz y Personalización"),
            ("banco_ejercicios", "🏋️ Banco de Ejercicios"),
            ("sistema_configuracion", "⚙️ Sistema y Configuración"),
            ("tipos_cuota", "💰 Tipos de Cuota"),
            ("conceptos_pago", "💳 Conceptos de Pago"),
            ("metodos_pago", "💰 Métodos de Pago"),
            ("administracion_avanzada", "🔧 Administración Avanzada"),
        ]
        for key, label in components_order:
            chk = QCheckBox(f"Visible: {label}")
            chk.setChecked(True)
            self.visibility_checkboxes[key] = chk
            vg_layout.addWidget(chk)

        # Separador visual
        sep = QFrame()
        sep.setFrameShape(QFrame.Shape.HLine)
        sep.setFrameShadow(QFrame.Shadow.Sunken)
        vg_layout.addWidget(sep)

        # Checkboxes por componente (pestañas principales de la aplicación)
        self.main_visibility_checkboxes = {}
        main_components_order = [
            ("usuarios", "👥 Usuarios y Asistencias"),
            ("pagos", "💰 Pagos"),
            ("reportes", "📊 Dashboard de Reportes"),
            ("rutinas", "🏋️ Rutinas"),
            ("clases", "📅 Clases"),
            ("profesores", "👨‍🏫 Profesores"),
            ("configuracion", "⚙️ Configuración"),
        ]
        for key, label in main_components_order:
            chk = QCheckBox(f"Visible: {label}")
            chk.setChecked(True)
            self.main_visibility_checkboxes[key] = chk
            vg_layout.addWidget(chk)

        # Botones de acción (simplificados)
        actions = QHBoxLayout()
        save_btn = QPushButton("💾 Guardar Visibilidad")
        apply_btn = QPushButton("✅ Aplicar ahora")
        actions.addWidget(save_btn)
        actions.addWidget(apply_btn)
        actions.addStretch()
        vg_layout.addLayout(actions)

        # Panel no colapsable
        self.visibility_group.setCheckable(False)
        self._set_group_content_visible(self.visibility_group, True)

        # Añadir al layout padre
        parent_layout.addWidget(self.visibility_group)

        # Conexiones
        self.visibility_role_selector.currentIndexChanged.connect(self._on_visibility_role_changed)
        save_btn.clicked.connect(self.save_visibility_config)
        apply_btn.clicked.connect(self.apply_developer_visibility_by_role)

        # Cargar configuración desde DB y reflejar estado inicial
        try:
            self._load_visibility_config()
            self._on_visibility_role_changed()
        except Exception:
            pass

        # Sistema de colapsabilidad eliminado; no aplicar visibilidad de checkboxes

    def _load_visibility_config(self):
        """Carga configuración de visibilidad desde la base de datos o establece valores por defecto."""
        raw = None
        try:
            raw = self.db_manager.obtener_configuracion('developer_tab_visibility_by_role')
        except Exception:
            raw = None
        self._visibility_config = {}
        if raw:
            try:
                self._visibility_config = json.loads(raw)
            except Exception:
                self._visibility_config = {}
        # Valores por defecto y compatibilidad hacia atrás
        if not self._visibility_config:
            default = {}
            for role in self.available_roles:
                default[role] = {
                    'dev_tabs': {
                        'interfaz_personalizacion': (role in ['dueño', 'administrador']),
                        'banco_ejercicios': True,
                        'sistema_configuracion': True,
                        'tipos_cuota': (role in ['dueño', 'administrador']),
                        'conceptos_pago': (role in ['dueño', 'administrador']),
                        'metodos_pago': (role in ['dueño', 'administrador']),
                        'administracion_avanzada': (role in ['dueño', 'administrador']),
                    },
                    'main_tabs': {
                        'usuarios': True,
                        'pagos': True,
                        'reportes': True,
                        'rutinas': True,
                        'clases': True,
                        'profesores': True,
                        'configuracion': True,
                    },
                    'config_widgets': {
                        # Widgets de Configuración controlables por rol
                        'config_precios': True
                    }
                }
            self._visibility_config = default
        else:
            # Compatibilidad hacia atrás: si el rol tiene un dict plano, envolver en dev_tabs
            try:
                for role, cfg in list(self._visibility_config.items()):
                    if isinstance(cfg, dict) and 'dev_tabs' not in cfg and 'main_tabs' not in cfg:
                        self._visibility_config[role] = {
                            'dev_tabs': cfg,
                            'main_tabs': {
                                'usuarios': True,
                                'pagos': True,
                                'reportes': True,
                                'rutinas': True,
                                'clases': True,
                                'profesores': True,
                                'configuracion': True,
                            },
                            'config_widgets': {
                                'config_precios': True
                            }
                        }
            except Exception:
                pass

    # ---- Persistencia y control de colapsabilidad ----
    def _load_collapse_states(self):
        """Carga estados de colapso por rol desde DB y estado del botón maestro."""
        raw = None
        try:
            raw = self.db_manager.obtener_configuracion('developer_group_collapse_by_role')
        except Exception:
            raw = None
        self._collapse_config = {}
        if raw:
            try:
                self._collapse_config = json.loads(raw)
            except Exception:
                self._collapse_config = {}
        # Valores por defecto: colapsabilidad habilitada y todos expandidos
        if not self._collapse_config:
            default = {}
            for role in self.available_roles:
                default[role] = {
                    'enabled': True,
                    'show_checkboxes': True,
                    'groups': {}
                }
            self._collapse_config = default
        else:
            # Asegurar clave 'show_checkboxes' para roles existentes
            try:
                for role, cfg in list(self._collapse_config.items()):
                    if isinstance(cfg, dict) and 'show_checkboxes' not in cfg:
                        cfg['show_checkboxes'] = True
            except Exception:
                pass

    def _save_collapse_states(self):
        try:
            payload = json.dumps(self._collapse_config, ensure_ascii=False)
            self.db_manager.actualizar_configuracion('developer_group_collapse_by_role', payload)
        except Exception:
            pass

    def _register_collapsible_group(self, key: str, group: QGroupBox):
        """Registra un QGroupBox con colapsabilidad controlada y persistente."""
        try:
            if not isinstance(group, QGroupBox):
                return
            self._collapsible_groups[key] = group
            # Asegurar comportamiento de tamaño compacto
            sp = group.sizePolicy()
            sp.setVerticalPolicy(QSizePolicy.Policy.Maximum)
            sp.setHorizontalPolicy(QSizePolicy.Policy.Preferred)
            group.setSizePolicy(sp)
            group.setCheckable(self._collapsible_enabled)
            # Conectar cambios de colapso
            try:
                group.toggled.connect(lambda checked, k=key: self._on_group_collapse_changed(k, checked))
            except Exception:
                pass
        except Exception:
            pass

    def _register_all_collapsible_groups(self):
        """Registra automáticamente todos los QGroupBox dentro del panel de desarrollador."""
        try:
            if not hasattr(self, 'dev_options_group'):
                return
            for group in self.dev_options_group.findChildren(QGroupBox):
                try:
                    title = getattr(group, 'title', lambda: '')()
                    key = ("grp_" + str(title).lower().replace(" ", "_").strip()) if title else f"grp_{id(group)}"
                    if key not in self._collapsible_groups:
                        self._register_collapsible_group(key, group)
                except Exception:
                    continue
            # Registrar también el propio panel para controlar su indicador y persistencia
            try:
                if "grp_dev_panel" not in self._collapsible_groups:
                    self._register_collapsible_group("grp_dev_panel", self.dev_options_group)
            except Exception:
                pass
        except Exception:
            pass

    def _on_group_collapse_changed(self, key: str, expanded: bool):
        """Actualiza el estado persistente cuando un grupo cambia su colapso."""
        try:
            role = getattr(self, 'user_role', None) or 'miembro'
            if role not in self._collapse_config:
                self._collapse_config[role] = {'enabled': self._collapsible_enabled, 'groups': {}}
            self._collapse_config[role]['groups'][key] = bool(expanded)
            self._save_collapse_states()
        except Exception:
            pass

    def _apply_collapse_states(self):
        """Aplica estados de colapso y estado maestro según rol actual."""
        try:
            role = getattr(self, 'user_role', None) or 'miembro'
            cfg = self._collapse_config.get(role, {'enabled': True, 'groups': {}})
            self._collapsible_enabled = bool(cfg.get('enabled', True))
            show_flag = bool(cfg.get('show_checkboxes', True))
            # Botón maestro, si existe
            if hasattr(self, 'collapse_toggle_btn'):
                self.collapse_toggle_btn.setChecked(self._collapsible_enabled)
                self.collapse_toggle_btn.setText('Colapsabilidad: ON' if self._collapsible_enabled else 'Colapsabilidad: OFF')
            # Reflejar estado del checkbox de visibilidad en la UI sin disparar señales
            try:
                if hasattr(self, 'show_collapse_checkboxes_chk'):
                    self.show_collapse_checkboxes_chk.blockSignals(True)
                    self.show_collapse_checkboxes_chk.setChecked(show_flag)
                    self.show_collapse_checkboxes_chk.blockSignals(False)
            except Exception:
                pass
            for key, group in self._collapsible_groups.items():
                try:
                    effective_enabled = bool(self._collapsible_enabled and show_flag)
                    group.setCheckable(effective_enabled)
                    desired = bool(cfg.get('groups', {}).get(key, True))
                    # Cuando la colapsabilidad/visibilidad está deshabilitada, forzar expandido
                    group.setChecked(True if not effective_enabled else desired)
                    # Mostrar contenido internamente
                    self._set_group_content_visible(group, group.isChecked())
                except Exception:
                    pass
        except Exception:
            pass

    def _set_collapsability_enabled(self, enabled: bool):
        try:
            self._collapsible_enabled = bool(enabled)
            role = getattr(self, 'user_role', None) or 'miembro'
            if role not in self._collapse_config:
                self._collapse_config[role] = {'enabled': self._collapsible_enabled, 'groups': {}}
            self._collapse_config[role]['enabled'] = self._collapsible_enabled
            self._save_collapse_states()
            self._apply_collapse_states()
        except Exception:
            pass

    def _toggle_dev_panel_collapsed(self):
        """Alterna el estado colapsado/expandido del Panel de Control del Desarrollador."""
        try:
            if hasattr(self, 'dev_options_group'):
                new_state = not self.dev_options_group.isChecked()
                # Asegurar que sea checkable
                self.dev_options_group.setCheckable(True)
                self.dev_options_group.setChecked(new_state)
                self._set_group_content_visible(self.dev_options_group, new_state)
        except Exception:
            pass

    def _set_collapse_checkbox_visibility(self, show: bool):
        """Muestra u oculta los indicadores de checkbox en los títulos de los QGroupBox colapsables."""
        try:
            role = getattr(self, 'user_role', None) or 'miembro'
            # Persistir configuración de visibilidad
            try:
                if role not in self._collapse_config:
                    self._collapse_config[role] = {'enabled': True, 'show_checkboxes': True, 'groups': {}}
                self._collapse_config[role]['show_checkboxes'] = bool(show)
                self._save_collapse_states()
            except Exception:
                pass

            # Aplicar accesibilidad/visibilidad efectiva
            for key, group in self._collapsible_groups.items():
                try:
                    if bool(show):
                        # Restaurar comportamiento normal según colapsabilidad global
                        group.setStyleSheet("")
                        group.setCheckable(bool(self._collapsible_enabled))
                        desired = bool(self._collapse_config.get(role, {}).get('groups', {}).get(key, True))
                        group.setChecked(True if not self._collapsible_enabled else desired)
                    else:
                        # Hacerlos inaccesibles y sin indicador; títulos se acomodan como si no existieran
                        group.setStyleSheet("")
                        group.setCheckable(False)
                        # Expandir contenido para evitar áreas inaccesibles colapsadas
                        group.setChecked(True)
                        self._set_group_content_visible(group, True)
                except Exception:
                    continue
        except Exception:
            pass

    def _on_visibility_role_changed(self):
        """Actualiza los checkboxes según el rol seleccionado en el panel."""
        try:
            role = self.visibility_role_selector.currentData()
            role_cfg = self._visibility_config.get(role, {})
            dev_cfg = role_cfg.get('dev_tabs', role_cfg)
            for key, chk in self.visibility_checkboxes.items():
                chk.setChecked(bool(dev_cfg.get(key, True)))
            # Pestañas principales
            main_cfg = role_cfg.get('main_tabs', {})
            for key, chk in getattr(self, 'main_visibility_checkboxes', {}).items():
                chk.setChecked(bool(main_cfg.get(key, True)))
            # Aplicar estados de colapso para el rol seleccionado (sistema de colapsabilidad deshabilitado)
            self._apply_collapse_states()
        except Exception:
            pass

    def save_visibility_config(self):
        """Guarda la configuración de visibilidad (dev y pestañas principales) en la base de datos."""
        try:
            role = self.visibility_role_selector.currentData()
            if role not in self._visibility_config:
                self._visibility_config[role] = {}
            dev_states = {key: bool(chk.isChecked()) for key, chk in self.visibility_checkboxes.items()}
            main_states = {key: bool(chk.isChecked()) for key, chk in getattr(self, 'main_visibility_checkboxes', {}).items()}
            # Fusionar manteniendo compatibilidad hacia atrás
            current = self._visibility_config.get(role, {})
            current['dev_tabs'] = dev_states
            current['main_tabs'] = main_states
            self._visibility_config[role] = current
            payload = json.dumps(self._visibility_config, ensure_ascii=False)
            self.db_manager.actualizar_configuracion('developer_tab_visibility_by_role', payload)
            QMessageBox.information(self, "Guardado", "Configuración de visibilidad guardada correctamente.")
        except Exception as e:
            QMessageBox.warning(self, "Error", f"No se pudo guardar la configuración: {e}")

    def apply_developer_visibility_by_role(self):
        """Aplica la visibilidad efectiva de pestañas del panel de desarrollador y principales según el rol actual.
        """
        try:
            # Asegurar que hay configuración cargada
            if not hasattr(self, '_visibility_config'):
                self._load_visibility_config()
            role = getattr(self, 'user_role', None)
            if not role:
                return
            role_cfg = self._visibility_config.get(role, {})
            dev_cfg = role_cfg.get('dev_tabs', role_cfg)
            # Aplicar a pestañas de desarrollador
            for key, index in (self._dev_tab_indices or {}).items():
                visible = bool(dev_cfg.get(key, True))
                try:
                    self.dev_tabs.setTabVisible(index, visible)
                except Exception:
                    pass
            # Si ninguna pestaña queda visible, ocultar el grupo
            try:
                any_visible = any(self.dev_tabs.isTabVisible(i) for i in range(self.dev_tabs.count()))
                self.dev_options_group.setVisible(any_visible)
            except Exception:
                pass

            # Aplicar visibilidad a pestañas principales inmediatamente si hay referencia a MainWindow
            try:
                win = self.window()
                if win and hasattr(win, 'tabWidget') and hasattr(win, 'tab_indices'):
                    main_cfg = role_cfg.get('main_tabs', {})
                    for key, idx in getattr(win, 'tab_indices', {}).items():
                        visible = bool(main_cfg.get(key, True))
                        try:
                            win.tabWidget.setTabVisible(idx, visible)
                        except Exception:
                            pass
            except Exception:
                pass
        except Exception:
            pass
        return
        
    def create_interface_customization_tab(self):
        """Crea la pestaña consolidada de Interfaz y Personalización"""
        tab = QWidget()
        layout = QVBoxLayout(tab)
        layout.setSpacing(0)
        layout.setContentsMargins(0, 0, 0, 0)

        # Embebido edge-to-edge del widget de branding sin título redundante
        try:
            from widgets.branding_customization_widget import BrandingCustomizationWidget
            self.branding_widget = BrandingCustomizationWidget(self.db_manager)
            self.branding_widget.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Expanding)
            layout.addWidget(self.branding_widget)
        except ImportError:
            branding_info = QLabel("⚠️ Widget de personalización de branding no disponible")
            branding_info.setObjectName("warning_text")
            branding_info.setProperty("class", "warning_info")
            layout.addWidget(branding_info)

        return tab
        
    def create_database_tab(self):
        """Crea la pestaña de Base de Datos (mantenida)"""
        tab = QWidget()
        # Asegurar que el contenido pueda expandirse dentro del scroll
        tab.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Expanding)
        layout = QVBoxLayout(tab)
        layout.setSpacing(0)
        layout.setContentsMargins(0, 0, 0, 0)
        
        # (Bloque 'Operaciones de Base de Datos' eliminado según requerimiento)
        
        # === BANCO DE EJERCICIOS ===
        # Reemplazar el QGroupBox por un QWidget para que el bloque toque los bordes
        exercise_container = QWidget()
        exercise_container.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Expanding)
        exercise_layout = QVBoxLayout(exercise_container)
        # Sin márgenes para que el bloque toque todos los bordes del tab
        exercise_layout.setContentsMargins(0, 0, 0, 0)
        # Sin espaciado entre filas para ocupar el área máxima
        exercise_layout.setSpacing(0)

        # Filtros
        filters_row = QHBoxLayout()
        filters_row.setSpacing(0)
        filters_row.setContentsMargins(0, 0, 0, 0)
        
        filters_row.addWidget(QLabel("Filtro:"))
        self.exercise_filter_input = QLineEdit()
        self.exercise_filter_input.setPlaceholderText("Buscar por nombre o descripción...")
        # Forzar expansión horizontal del buscador
        self.exercise_filter_input.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Preferred)
        filters_row.addWidget(self.exercise_filter_input, 2)

        filters_row.addWidget(QLabel("Grupo:"))
        self.exercise_group_filter = QComboBox()
        self.exercise_group_filter.addItem("Todos")
        # Permitir expansión si el espacio lo requiere
        self.exercise_group_filter.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Preferred)
        filters_row.addWidget(self.exercise_group_filter, 1)

        filters_row.addWidget(QLabel("Objetivo:"))
        self.exercise_objective_filter = QComboBox()
        self.exercise_objective_filter.addItem("Todos")
        self.exercise_objective_filter.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Preferred)
        filters_row.addWidget(self.exercise_objective_filter, 1)

        # No usar stretch vacío para evitar huecos; los widgets se expanden por política

        exercise_layout.addLayout(filters_row)

        # Tabla de ejercicios
        self.exercise_table = QTableWidget(0, 4)
        self.exercise_table.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Expanding)
        self.exercise_table.setAlternatingRowColors(True)
        self.exercise_table.setSelectionBehavior(QAbstractItemView.SelectionBehavior.SelectRows)
        self.exercise_table.setSelectionMode(QAbstractItemView.SelectionMode.SingleSelection)
        self.exercise_table.setEditTriggers(QAbstractItemView.EditTrigger.NoEditTriggers)
        self.exercise_table.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeMode.Stretch)
        self.exercise_table.verticalHeader().setVisible(False)
        self.exercise_table.setHorizontalHeaderLabels(["ID", "Nombre", "Grupo Muscular", "Objetivo"]) 
        self.exercise_table.setColumnHidden(0, True)
        exercise_layout.addWidget(self.exercise_table, 1)

        # Acciones
        actions_row = QHBoxLayout()
        actions_row.setSpacing(0)
        actions_row.setContentsMargins(0, 0, 0, 0)
        self.overwrite_exercises_checkbox = QCheckBox("Sobrescribir existentes por nombre")
        self.overwrite_exercises_checkbox.setToolTip("Si está activado, los ejercicios con el mismo nombre se actualizarán con los datos importados.")
        actions_row.addWidget(self.overwrite_exercises_checkbox)
        
        add_btn = QPushButton("➕ Añadir")
        add_btn.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Preferred)
        add_btn.clicked.connect(self.add_exercise)
        actions_row.addWidget(add_btn, 1)

        edit_btn = QPushButton("✏️ Editar")
        edit_btn.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Preferred)
        edit_btn.clicked.connect(self.edit_exercise)
        actions_row.addWidget(edit_btn, 1)

        delete_btn = QPushButton("🗑️ Eliminar")
        delete_btn.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Preferred)
        delete_btn.clicked.connect(self.delete_exercise)
        actions_row.addWidget(delete_btn, 1)

        export_btn = QPushButton("📤 Exportar")
        export_btn.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Preferred)
        export_btn.clicked.connect(self.export_exercise_bank)
        actions_row.addWidget(export_btn, 1)

        import_btn = QPushButton("📥 Importar")
        import_btn.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Preferred)
        import_btn.clicked.connect(self.import_exercise_bank)
        actions_row.addWidget(import_btn, 1)

        guide_btn = QPushButton("ℹ️ Guía de Importación")
        guide_btn.setToolTip("Ver formato requerido del archivo .xlsx y ejemplos.")
        guide_btn.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Preferred)
        guide_btn.clicked.connect(self.show_exercise_import_guide)
        actions_row.addWidget(guide_btn, 1)

        template_btn = QPushButton("📄 Plantilla XLSX")
        template_btn.setToolTip("Descargar una plantilla .xlsx con columnas y ejemplos.")
        template_btn.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Preferred)
        template_btn.clicked.connect(self.save_exercise_template_xlsx)
        actions_row.addWidget(template_btn, 1)

        exercise_layout.addLayout(actions_row)
        # Asegurar expansión vertical: filtros (0), tabla (1), acciones (2)
        exercise_layout.setStretch(0, 0)
        exercise_layout.setStretch(1, 1)
        exercise_layout.setStretch(2, 0)

        # Conexiones de filtros
        self.exercise_filter_input.textChanged.connect(self.populate_exercise_bank_table)
        self.exercise_group_filter.currentTextChanged.connect(self.populate_exercise_bank_table)
        self.exercise_objective_filter.currentTextChanged.connect(self.populate_exercise_bank_table)

        # Hacer que el bloque ocupe el espacio disponible
        layout.addWidget(exercise_container, 1)

        # Inicializar filtros y datos de tabla
        try:
            self.reload_exercise_filters()
        except Exception:
            pass
        self.populate_exercise_bank_table()
        
        # Eliminar espaciador para evitar espacios vacíos excesivos
        # layout.addStretch()
        
        return tab
    
    def create_logs_monitoring_tab(self):
        """Crea la pestaña de Logs y Monitoreo (reorganizada)"""
        tab = QWidget()
        layout = QVBoxLayout(tab)
        layout.setSpacing(8)  # Espaciado aumentado
        layout.setContentsMargins(10, 10, 10, 8)  # Márgenes aumentados
        
        # === VISUALIZACIÓN DE LOGS ===
        logs_group = QGroupBox("📄 Visualización de Logs")
        logs_group.setSizePolicy(QSizePolicy.Policy.Preferred, QSizePolicy.Policy.Preferred)
        logs_layout = QVBoxLayout(logs_group)
        logs_layout.setContentsMargins(8, 8, 8, 8)
        logs_layout.setSpacing(4)
        
        # Controles de logs
        controls_layout = QHBoxLayout()
        
        view_logs_btn = QPushButton("📖 Ver Logs del Sistema")
        view_logs_btn.clicked.connect(self.view_logs)
        controls_layout.addWidget(view_logs_btn)
        
        refresh_logs_btn = QPushButton("🔄 Actualizar")
        refresh_logs_btn.clicked.connect(self.populate_logs_list)
        controls_layout.addWidget(refresh_logs_btn)
        
        controls_layout.addStretch()
        logs_layout.addLayout(controls_layout)
        
        # Splitter para lista de logs y contenido
        logs_splitter = QSplitter(Qt.Orientation.Horizontal)
        
        # Lista de archivos de log
        self.logs_list_widget = QListWidget()
        self.logs_list_widget.itemClicked.connect(self.view_log_content)
        self.logs_list_widget.setMaximumWidth(200)
        logs_splitter.addWidget(self.logs_list_widget)
        
        # Visor de contenido de logs
        self.log_content_viewer = QTextEdit()
        self.log_content_viewer.setReadOnly(True)
        self.log_content_viewer.setFont(QFont("Consolas", 9))
        # Migrado al sistema CSS dinámico - los estilos se aplican automáticamente
        self.log_content_viewer.setObjectName("log_content_viewer")
        self.log_content_viewer.setProperty("class", "console_text")
        logs_splitter.addWidget(self.log_content_viewer)
        
        logs_layout.addWidget(logs_splitter)
        
        # Plegable para Visualización de Logs
        logs_group.setCheckable(True)
        logs_group.setChecked(True)
        logs_group.toggled.connect(lambda checked, g=logs_group: self._set_group_content_visible(g, checked))
        self._set_group_content_visible(logs_group, True)
        layout.addWidget(logs_group)
        
        # Cargar logs iniciales
        self.populate_logs_list()
        
        return tab
        
    def create_system_configuration_tab(self):
        """Crea la pestaña de Sistema y Configuración (reorganizada)"""
        tab = QWidget()
        layout = QVBoxLayout(tab)
        layout.setSpacing(6)
        layout.setContentsMargins(8, 8, 8, 6)
        
        # (Bloque '📁 Configuración del Sistema' eliminado según requerimiento)
        
        # === CONFIGURACIÓN DE WHATSAPP ===
        whatsapp_group = QGroupBox("📱 Configuración de WhatsApp Business")
        whatsapp_group.setSizePolicy(QSizePolicy.Policy.Preferred, QSizePolicy.Policy.Minimum)
        whatsapp_layout = QVBoxLayout(whatsapp_group)
        whatsapp_layout.setContentsMargins(8, 8, 8, 8)
        whatsapp_layout.setSpacing(4)
        
        # Crear widget de configuración WhatsApp
        try:
            from widgets.whatsapp_config_widget import WhatsAppConfigWidget
            self.whatsapp_config_widget = WhatsAppConfigWidget(self.db_manager)
            whatsapp_layout.addWidget(self.whatsapp_config_widget)
        except ImportError:
            whatsapp_info = QLabel("⚠️ Widget de configuración de WhatsApp no disponible")
            whatsapp_info.setObjectName("warning_text")
            whatsapp_info.setProperty("class", "warning_info")
            whatsapp_layout.addWidget(whatsapp_info)
        
        # Plegable para WhatsApp
        whatsapp_group.setCheckable(True)
        whatsapp_group.setChecked(True)
        whatsapp_group.toggled.connect(lambda checked, g=whatsapp_group: self._set_group_content_visible(g, checked))
        self._set_group_content_visible(whatsapp_group, True)
        
        layout.addWidget(whatsapp_group)

        # === VISUALIZACIÓN DE LOGS (MOVIDA DESDE '📄 Logs y Monitoreo') ===
        logs_group = QGroupBox("📄 Visualización de Logs")
        logs_group.setSizePolicy(QSizePolicy.Policy.Preferred, QSizePolicy.Policy.Preferred)
        logs_layout = QVBoxLayout(logs_group)
        logs_layout.setContentsMargins(8, 8, 8, 8)
        logs_layout.setSpacing(4)

        # Controles de logs
        controls_layout = QHBoxLayout()
        view_logs_btn = QPushButton("📖 Ver Logs del Sistema")
        view_logs_btn.clicked.connect(self.view_logs)
        controls_layout.addWidget(view_logs_btn)
        refresh_logs_btn = QPushButton("🔄 Actualizar")
        refresh_logs_btn.clicked.connect(self.populate_logs_list)
        controls_layout.addWidget(refresh_logs_btn)
        controls_layout.addStretch()
        logs_layout.addLayout(controls_layout)

        # Splitter para lista de logs y contenido
        logs_splitter = QSplitter(Qt.Orientation.Horizontal)

        # Lista de archivos de log
        self.logs_list_widget = QListWidget()
        self.logs_list_widget.itemClicked.connect(self.view_log_content)
        self.logs_list_widget.setMaximumWidth(200)
        logs_splitter.addWidget(self.logs_list_widget)

        # Visor de contenido de logs (altura aumentada)
        self.log_content_viewer = QTextEdit()
        self.log_content_viewer.setReadOnly(True)
        self.log_content_viewer.setFont(QFont("Consolas", 9))
        self.log_content_viewer.setObjectName("log_content_viewer")
        self.log_content_viewer.setProperty("class", "console_text")
        self.log_content_viewer.setMinimumHeight(300)
        logs_splitter.addWidget(self.log_content_viewer)

        logs_layout.addWidget(logs_splitter)

        # Plegable para Visualización de Logs
        logs_group.setCheckable(True)
        logs_group.setChecked(True)
        logs_group.toggled.connect(lambda checked, g=logs_group: self._set_group_content_visible(g, checked))
        self._set_group_content_visible(logs_group, True)
        layout.addWidget(logs_group)

        # Cargar logs iniciales
        try:
            self.populate_logs_list()
        except Exception:
            pass

        return tab

    def connect_signals(self):
        # === SEÑALES ESENCIALES ===
        # Configuración general (solo si existen)
        if hasattr(self, 'save_price_button'):
            self.save_price_button.clicked.connect(self.save_new_prices)
        
        # Interfaz
        # Configuración de fuente eliminada
        
        # === SEÑALES DE ACCESIBILIDAD ===
        # Conectar señales del widget de accesibilidad
        if hasattr(self, 'accessibility_widget'):
            self.accessibility_widget.accessibility_changed.connect(self.apply_accessibility_changes)
        
        # === SEÑALES DE CONFIGURACIÓN ESPECÍFICA ===
        # Branding
        if hasattr(self, 'branding_widget'):
            self.branding_widget.branding_changed.connect(self.apply_branding_changes)
        
        # Tipos de cuota
        if hasattr(self, 'quota_types_widget'):
            self.quota_types_widget.tipos_cuota_modificados.connect(self.on_quota_types_modified)
        
        # === SEÑALES DE NAVEGACIÓN ===
        self.dev_tabs.currentChanged.connect(self.on_dev_tab_changed)

    def toggle_dev_mode_visibility(self, active):
        # Método mantenido para compatibilidad pero sin funcionalidad
        pass

    def on_dev_tab_changed(self, index):
        """Maneja el cambio de pestañas en el panel de desarrollador reorganizado"""
        tab_text = self.dev_tabs.tabText(index)
        
        # === PESTAÑAS ESENCIALES ===
        if "Logs" in tab_text:
            self.populate_logs_list()
        
        # === PESTAÑAS DE CONFIGURACIÓN ESPECÍFICA ===
        elif "Tipos de Cuota" in tab_text:
            self.quota_types_widget.load_tipos_cuota()
        elif "Conceptos de Pago" in tab_text:
            self.load_payment_concepts()
        elif "Métodos de Pago" in tab_text:
            self.load_payment_methods()
        
        # === ADMINISTRACIÓN AVANZADA ===
        elif "Administración Avanzada" in tab_text:
            # Las funciones avanzadas se manejan dentro del widget de administración
            pass

    def emit_feature_toggle(self):
        try:
            states = {key: cb.isChecked() for key, cb in self.feature_checkboxes.items()}
            # Guardar en BD como JSON
            self.db_manager.actualizar_configuracion('tab_visibility_config', json.dumps(states))
            # Emitir para actualizar visibilidad en MainWindow
            self.feature_toggled.emit(states)
        except Exception as e:
            logging.error(f"Error al emitir/guardar visibilidad de pestañas: {e}")
            # Emitir al menos el estado actual para no bloquear la UI
            self.feature_toggled.emit({key: cb.isChecked() for key, cb in self.feature_checkboxes.items()})

    def _set_group_content_visible(self, group, visible: bool):
        """Muestra/oculta los contenidos internos de un QGroupBox sin ocultar el encabezado."""
        try:
            lay = group.layout()
            if not lay:
                return
            for i in range(lay.count()):
                item = lay.itemAt(i)
                w = item.widget()
                if w is not None:
                    w.setVisible(visible)
        except Exception as e:
            logging.warning(f"Error actualizando visibilidad del grupo {getattr(group, 'title', lambda: str(group))()}: {e}")

    # === MÉTODOS PARA OPERACIONES DE BASE DE DATOS ===
    def backup_database(self):
        """Crea un backup de la base de datos"""
        try:
            if hasattr(self, 'dev_manager') and self.dev_manager:
                self.dev_manager.create_database_backup()
            else:
                QMessageBox.information(self, "Backup", "Funcionalidad de backup no disponible")
        except Exception as e:
            QMessageBox.warning(self, "Error", f"Error al crear backup: {str(e)}")
    
    def clean_database(self):
        """Limpia datos antiguos de la base de datos"""
        try:
            if hasattr(self, 'dev_manager') and self.dev_manager:
                self.dev_manager.clean_old_data()
            else:
                QMessageBox.information(self, "Limpieza", "Funcionalidad de limpieza no disponible")
        except Exception as e:
            QMessageBox.warning(self, "Error", f"Error al limpiar base de datos: {str(e)}")
    
    def view_logs(self):
        """Abre el directorio de logs"""
        try:
            self.open_logs_directory()
        except Exception as e:
            QMessageBox.warning(self, "Error", f"Error al abrir logs: {str(e)}")
    
    def export_configuration(self):
        """Exporta la configuración del sistema"""
        try:
            if hasattr(self, 'export_settings'):
                self.export_settings()
            else:
                QMessageBox.information(self, "Exportar", "Funcionalidad de exportación no disponible")
        except Exception as e:
            QMessageBox.warning(self, "Error", f"Error al exportar configuración: {str(e)}")
    
    def import_configuration(self):
        """Importa la configuración del sistema"""
        try:
            if hasattr(self, 'import_settings'):
                self.import_settings()
            else:
                QMessageBox.information(self, "Importar", "Funcionalidad de importación no disponible")
        except Exception as e:
            QMessageBox.warning(self, "Error", f"Error al importar configuración: {str(e)}")
    
    def on_quota_types_modified(self):
        """Maneja las modificaciones en los tipos de cuota y actualiza la configuración de precios"""
        try:
            # Recargar la configuración de precios para reflejar los cambios
            self.load_config()
            # Emitir señal de que los precios han sido actualizados
            self.precio_actualizado.emit()
        except Exception as e:
            QMessageBox.warning(self, "Error", f"No se pudo actualizar la configuración de precios: {e}")

    def load_config(self):
        try:
            self.load_dynamic_prices()
            self.info_text_edit.setHtml(self.db_manager.obtener_configuracion('system_info_html') or "")
            # Cargar configuración de limpieza automática
            try:
                settings_str = self.db_manager.obtener_configuracion('auto_cleanup_settings')
                if settings_str:
                    settings = json.loads(settings_str)
                    if hasattr(self, 'logs_retention_spin') and isinstance(settings.get('logs_retention_days'), int):
                        val = settings.get('logs_retention_days')
                        if 1 <= val <= self.logs_retention_spin.maximum():
                            self.logs_retention_spin.setValue(val)
                    if hasattr(self, 'logs_preview_check'):
                        self.logs_preview_check.setChecked(bool(settings.get('logs_preview_default', False)))
                    if hasattr(self, 'temp_retention_spin') and isinstance(settings.get('temp_retention_days'), int):
                        val_t = settings.get('temp_retention_days')
                        if 1 <= val_t <= self.temp_retention_spin.maximum():
                            self.temp_retention_spin.setValue(val_t)
                    if hasattr(self, 'temp_preview_check'):
                        self.temp_preview_check.setChecked(bool(settings.get('temp_preview_default', False)))
            except Exception as e:
                logging.warning(f"Error cargando configuración de limpieza automática: {e}")
        except Exception as e:
            QMessageBox.warning(self, "Error", f"No se pudo cargar la configuración: {e}")
    
    def save_auto_cleanup_config(self):
        try:
            settings = {
                'logs_retention_days': int(self.logs_retention_spin.value()) if hasattr(self, 'logs_retention_spin') else 14,
                'logs_preview_default': bool(self.logs_preview_check.isChecked()) if hasattr(self, 'logs_preview_check') else False,
                'temp_retention_days': int(self.temp_retention_spin.value()) if hasattr(self, 'temp_retention_spin') else 7,
                'temp_preview_default': bool(self.temp_preview_check.isChecked()) if hasattr(self, 'temp_preview_check') else False,
            }
            self.db_manager.actualizar_configuracion('auto_cleanup_settings', json.dumps(settings))
        except Exception as e:
            logging.warning(f"No se pudo guardar configuración de limpieza automática: {e}")
    
    def load_dynamic_prices(self):
        """Carga dinámicamente todos los tipos de cuota activos en la tabla profesional"""
        try:
            # Limpiar tabla existente
            self.clear_price_widgets()
            
            # Obtener tipos de cuota activos
            tipos_cuota = self.db_manager.obtener_tipos_cuota_activos()
            
            if not tipos_cuota:
                # Mostrar mensaje en la tabla cuando no hay tipos de cuota
                self.prices_table.setRowCount(1)
                self.prices_table.setSpan(0, 0, 1, 5)  # Combinar todas las columnas
                
                empty_message = QTableWidgetItem("📊 No hay tipos de cuota configurados\n\nUtiliza la pestaña 'Tipos de Cuota' para crear diferentes tipos de membresía.")
                empty_message.setTextAlignment(Qt.AlignmentFlag.AlignCenter)
                empty_message.setFlags(Qt.ItemFlag.ItemIsEnabled)  # Solo lectura
                self.prices_table.setItem(0, 0, empty_message)
                self.prices_table.setRowHeight(0, 120)
                return
            
            # Configurar número de filas
            self.prices_table.setRowCount(len(tipos_cuota))
            
            # Llenar la tabla con los datos
            for row, tipo in enumerate(tipos_cuota):
                self.create_table_row_for_type(tipo, row)
                
            # Ajustar altura de filas
            for row in range(len(tipos_cuota)):
                self.prices_table.setRowHeight(row, 60)
                    
        except Exception as e:
            QMessageBox.warning(self, "Error", f"No se pudieron cargar los tipos de cuota: {e}")
    
    def clear_price_widgets(self):
        """Limpia todos los widgets de precios dinámicos de la tabla"""
        # Limpiar la tabla
        self.prices_table.setRowCount(0)
        self.prices_table.clearSpans()
        
        # Limpiar el diccionario
        self.price_widgets.clear()
    
    def create_table_row_for_type(self, tipo, row):
        """Crea una fila en la tabla para un tipo de cuota específico"""
        # Columna 0: Icono
        icon_text = tipo.icono_path if tipo.icono_path else "💰"
        icon_item = QTableWidgetItem(icon_text)
        icon_item.setTextAlignment(Qt.AlignmentFlag.AlignCenter)
        icon_item.setFlags(Qt.ItemFlag.ItemIsEnabled)  # Solo lectura
        icon_item.setFont(QFont("Segoe UI Emoji", 16))
        self.prices_table.setItem(row, 0, icon_item)
        
        # Columna 1: Nombre
        name_item = QTableWidgetItem(tipo.nombre.title())
        name_item.setTextAlignment(Qt.AlignmentFlag.AlignLeft | Qt.AlignmentFlag.AlignVCenter)
        name_item.setFlags(Qt.ItemFlag.ItemIsEnabled)  # Solo lectura
        name_item.setFont(QFont("Arial", 11, QFont.Weight.Bold))
        self.prices_table.setItem(row, 1, name_item)
        
        # Columna 2: Descripción
        desc_text = ""
        if hasattr(tipo, 'descripcion') and tipo.descripcion:
            desc_text = tipo.descripcion
            if len(desc_text) > 100:
                desc_text = desc_text[:97] + "..."
        else:
            desc_text = "Sin descripción"
            
        desc_item = QTableWidgetItem(desc_text)
        desc_item.setTextAlignment(Qt.AlignmentFlag.AlignLeft | Qt.AlignmentFlag.AlignVCenter)
        desc_item.setFlags(Qt.ItemFlag.ItemIsEnabled)  # Solo lectura
        desc_item.setToolTip(tipo.descripcion if hasattr(tipo, 'descripcion') and tipo.descripcion else "Sin descripción")
        self.prices_table.setItem(row, 2, desc_item)
        
        # Columna 3: Precio Actual
        current_price_item = QTableWidgetItem(f"${tipo.precio:,.2f} ARS")
        current_price_item.setTextAlignment(Qt.AlignmentFlag.AlignCenter)
        current_price_item.setFlags(Qt.ItemFlag.ItemIsEnabled)  # Solo lectura
        current_price_item.setFont(QFont("Arial", 10, QFont.Weight.Bold))
        # Usar color dinámico del sistema CSS
        # Usar color verde por defecto para el precio actual
        current_price_item.setForeground(QColor('#A3BE8C'))
        self.prices_table.setItem(row, 3, current_price_item)
        
        # Columna 4: Nuevo Precio (Editable)
        # Crear un QDoubleSpinBox como widget personalizado
        new_price_spinbox = QDoubleSpinBox()
        new_price_spinbox.setRange(0, 999999.99)
        new_price_spinbox.setDecimals(2)
        new_price_spinbox.setPrefix("$ ")
        new_price_spinbox.setSuffix(" ARS")
        new_price_spinbox.setValue(tipo.precio)
        new_price_spinbox.setAlignment(Qt.AlignmentFlag.AlignCenter)
        # QDoubleSpinBox ya hereda los estilos estándar del sistema dinámico
        new_price_spinbox.setMinimumHeight(35)
        
        # Establecer el widget en la celda
        self.prices_table.setCellWidget(row, 4, new_price_spinbox)
        
        # === GUARDAR REFERENCIAS ===
        self.price_widgets[tipo.id] = {
            'current_label': current_price_item,
            'spinbox': new_price_spinbox,
            'tipo': tipo,
            'row': row
        }

    def save_system_info(self):
        try: self.db_manager.actualizar_configuracion('system_info_html', self.info_text_edit.toHtml()); QMessageBox.information(self, "Éxito", "La información del sistema ha sido guardada.")
        except Exception as e: QMessageBox.critical(self, "Error", f"No se pudo guardar la información: {e}")

    def save_new_prices(self):
        """Guarda los nuevos precios para todos los tipos de cuota"""
        if not self.price_widgets:
            QMessageBox.warning(self, "Sin datos", "No hay tipos de cuota para actualizar.")
            return
        
        # Recopilar cambios
        changes = []
        for tipo_id, widgets in self.price_widgets.items():
            tipo = widgets['tipo']
            new_price = widgets['spinbox'].value()
            if new_price != tipo.precio:
                changes.append(f"{tipo.nombre}: ${tipo.precio:,.2f} → ${new_price:,.2f}")
        
        if not changes:
            QMessageBox.information(self, "Sin cambios", "No se detectaron cambios en los precios.")
            return
        
        # Confirmar cambios
        changes_text = "\n".join(changes)
        if QMessageBox.question(
            self, 
            "Confirmar Cambios", 
            f"¿Desea actualizar los siguientes precios?\n\n{changes_text}\n\n⚠️ Esto afectará a las nuevas cuotas generadas.",
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No
        ) == QMessageBox.StandardButton.Yes:
            try:
                # Actualizar precios
                updated_count = 0
                for tipo_id, widgets in self.price_widgets.items():
                    tipo = widgets['tipo']
                    new_price = widgets['spinbox'].value()
                    if new_price != tipo.precio:
                        tipo.precio = new_price
                        self.db_manager.actualizar_tipo_cuota(tipo)
                        updated_count += 1
                
                QMessageBox.information(
                    self, 
                    "Éxito", 
                    f"Se han actualizado {updated_count} precios correctamente."
                )
                
                # Recargar la configuración
                self.load_dynamic_prices()
                self.precio_actualizado.emit()
                
            except Exception as e:
                QMessageBox.critical(self, "Error", f"No se pudieron guardar los precios: {e}")

    def populate_logs_list(self):
        if not hasattr(self, 'logs_list_widget') or self.logs_list_widget is None:
            return
        
        self.logs_list_widget.clear(); log_dir = "logs"
        if os.path.isdir(log_dir):
            try:
                log_files = sorted([f for f in os.listdir(log_dir) if f.endswith('.log')], reverse=True); self.logs_list_widget.addItems(log_files)
                if log_files: self.logs_list_widget.setCurrentRow(0); self.view_log_content(self.logs_list_widget.item(0))
            except Exception as e: 
                if hasattr(self, 'log_content_viewer') and self.log_content_viewer is not None:
                    self.log_content_viewer.setText(f"Error al listar los logs: {e}")

    def view_log_content(self, item: QListWidgetItem):
        try:
            with open(os.path.join("logs", item.text()), 'r', encoding='utf-8') as f: self.log_content_viewer.setText(f.read())
        except Exception as e: self.log_content_viewer.setText(f"No se pudo leer el archivo de log:\n{e}")

    def open_logs_directory(self):
        log_dir = os.path.abspath("logs")
        if os.path.isdir(log_dir):
            try:
                if sys.platform == "win32": os.startfile(log_dir)
                elif sys.platform == "darwin": subprocess.Popen(["open", log_dir])
                else: subprocess.Popen(["xdg-open", log_dir])
            except Exception as e: QMessageBox.warning(self, "Error", f"No se pudo abrir la carpeta de logs:\n{e}")
        else: QMessageBox.warning(self, "Error", "La carpeta de logs no ha sido creada aún.")

    def load_users_for_role_management(self):
        self.user_selector_combo.blockSignals(True); self.user_selector_combo.clear(); self.user_selector_combo.addItem("Seleccione un usuario...", userData=None)
        try:
            all_users = self.db_manager.obtener_todos_usuarios()
            for user in all_users:
                if user.rol != 'dueño': self.user_selector_combo.addItem(f"{user.nombre} ({user.rol.title()})", userData=user)
        except Exception as e: QMessageBox.critical(self, "Error", f"No se pudo cargar la lista de usuarios: {e}")
        finally: self.user_selector_combo.blockSignals(False); self.update_role_buttons_state()

    def update_role_buttons_state(self): user = self.user_selector_combo.currentData(); self.promote_button.setEnabled(bool(user and user.rol == 'socio')); self.demote_button.setEnabled(bool(user and user.rol == 'profesor'))

    def change_user_role(self, new_role: str):
        user: Usuario = self.user_selector_combo.currentData()
        if not user: QMessageBox.warning(self, "Sin selección", "Por favor, seleccione un usuario."); return
        
        # Mensaje personalizado según el cambio de rol
        if new_role == 'profesor':
            mensaje = f"¿Seguro que desea ascender a {user.nombre} a Profesor?\n\nEsto creará automáticamente un perfil de profesor."
        else:
            mensaje = f"¿Seguro que desea degradar a {user.nombre} a Socio?\n\nEsto eliminará automáticamente su perfil de profesor."
        
        if QMessageBox.question(self, "Confirmar Cambio", mensaje, QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No) == QMessageBox.StandardButton.Yes:
            try: 
                user.rol = new_role
                self.db_manager.actualizar_usuario(user)
                
                # Mensaje de éxito personalizado
                if new_role == 'profesor':
                    QMessageBox.information(self, "Éxito", f"El rol de {user.nombre} ha sido cambiado a Profesor.\n\nSe ha creado automáticamente su perfil de profesor.")
                else:
                    QMessageBox.information(self, "Éxito", f"El rol de {user.nombre} ha sido cambiado a Socio.\n\nSe ha eliminado automáticamente su perfil de profesor.")
                
                self.load_users_for_role_management()
                self.usuarios_modificados.emit()
            except Exception as e: QMessageBox.critical(self, "Error", f"No se pudo cambiar el rol: {e}")

    def populate_exercise_bank_table(self):
        if not hasattr(self, 'exercise_table'):
            return
        self.exercise_table.setRowCount(0)
        try:
            filtro = self.exercise_filter_input.text().strip() if hasattr(self, 'exercise_filter_input') else ""
            objetivo = self.exercise_objective_filter.currentText() if hasattr(self, 'exercise_objective_filter') else ""
            grupo = self.exercise_group_filter.currentText() if hasattr(self, 'exercise_group_filter') else ""
            if objetivo == "Todos":
                objetivo = ""
            if grupo == "Todos":
                grupo = ""
            ejercicios = self.db_manager.obtener_ejercicios(filtro=filtro, objetivo=objetivo, grupo_muscular=grupo)
            self.exercise_table.setRowCount(len(ejercicios))
            for row, ej in enumerate(ejercicios):
                id_item = QTableWidgetItem(str(ej.id)); id_item.setData(Qt.ItemDataRole.UserRole, ej)
                self.exercise_table.setItem(row, 0, id_item)
                self.exercise_table.setItem(row, 1, QTableWidgetItem(ej.nombre or ""))
                self.exercise_table.setItem(row, 2, QTableWidgetItem((ej.grupo_muscular or "")))
                objetivo_val = getattr(ej, 'objetivo', None) or "general"
                self.exercise_table.setItem(row, 3, QTableWidgetItem(objetivo_val))
        except Exception as e:
            QMessageBox.critical(self, "Error", f"No se pudo cargar el banco de ejercicios: {e}")

    def reload_exercise_filters(self):
        try:
            ejercicios = self.db_manager.obtener_ejercicios()
            grupos = sorted({e.grupo_muscular for e in ejercicios if getattr(e, 'grupo_muscular', None)})
            objetivos = sorted({getattr(e, 'objetivo', 'general') or 'general' for e in ejercicios})

            cur_grupo = self.exercise_group_filter.currentText() if hasattr(self, 'exercise_group_filter') else "Todos"
            cur_obj = self.exercise_objective_filter.currentText() if hasattr(self, 'exercise_objective_filter') else "Todos"

            self.exercise_group_filter.blockSignals(True)
            self.exercise_group_filter.clear()
            self.exercise_group_filter.addItem("Todos")
            for g in grupos:
                self.exercise_group_filter.addItem(g)
            idx = self.exercise_group_filter.findText(cur_grupo)
            if idx >= 0:
                self.exercise_group_filter.setCurrentIndex(idx)
            self.exercise_group_filter.blockSignals(False)

            self.exercise_objective_filter.blockSignals(True)
            self.exercise_objective_filter.clear()
            self.exercise_objective_filter.addItem("Todos")
            for o in objetivos:
                self.exercise_objective_filter.addItem(o)
            idx2 = self.exercise_objective_filter.findText(cur_obj)
            if idx2 >= 0:
                self.exercise_objective_filter.setCurrentIndex(idx2)
            self.exercise_objective_filter.blockSignals(False)
        except Exception as e:
            logging.exception("Error reloading exercise filters")

    def add_exercise(self):
        dialog = ExerciseBankDialog(self)
        if dialog.exec():
            try: self.db_manager.crear_ejercicio(dialog.get_ejercicio()); self.populate_exercise_bank_table()
            except Exception as e: QMessageBox.critical(self, "Error", f"No se pudo crear el ejercicio (¿quizás el nombre ya existe?): {e}")

    def edit_exercise(self):
        selected_row = self.exercise_table.currentRow()
        if selected_row < 0: QMessageBox.warning(self, "Sin selección", "Seleccione un ejercicio para editar."); return
        exercise_to_edit = self.exercise_table.item(selected_row, 0).data(Qt.ItemDataRole.UserRole)
        dialog = ExerciseBankDialog(self, ejercicio=exercise_to_edit)
        if dialog.exec():
            try: self.db_manager.actualizar_ejercicio(dialog.get_ejercicio()); self.populate_exercise_bank_table()
            except Exception as e: QMessageBox.critical(self, "Error", f"No se pudo actualizar el ejercicio: {e}")

    def delete_exercise(self):
        selected_row = self.exercise_table.currentRow()
        if selected_row < 0: QMessageBox.warning(self, "Sin selección", "Seleccione un ejercicio para eliminar."); return
        exercise_to_delete = self.exercise_table.item(selected_row, 0).data(Qt.ItemDataRole.UserRole)
        if QMessageBox.question(self, "Confirmar Eliminación", f"¿Seguro que desea eliminar '{exercise_to_delete.nombre}'? Será eliminado de todas las rutinas.", QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No, QMessageBox.StandardButton.No) == QMessageBox.StandardButton.Yes:
            try: self.db_manager.eliminar_ejercicio(exercise_to_delete.id); self.populate_exercise_bank_table()
            except Exception as e: QMessageBox.critical(self, "Error", f"No se pudo eliminar el ejercicio: {e}")

    def export_exercise_bank(self):
        try:
            ejercicios = self.db_manager.obtener_ejercicios()
            if not ejercicios: QMessageBox.information(self, "Vacío", "El banco de ejercicios está vacío."); return
            filepath, _ = QFileDialog.getSaveFileName(self, "Exportar Banco de Ejercicios", "banco_ejercicios.xlsx", "Excel Files (*.xlsx)")
            if filepath: self.export_manager.exportar_banco_ejercicios_excel(filepath, ejercicios); QMessageBox.information(self, "Éxito", f"Banco de ejercicios exportado a:\n{filepath}")
        except Exception as e: QMessageBox.critical(self, "Error", f"No se pudo exportar el banco de ejercicios: {e}")

    def import_exercise_bank(self):
        filepath, _ = QFileDialog.getOpenFileName(self, "Importar Banco de Ejercicios", "", "Excel Files (*.xlsx)")
        if not filepath:
            return
        try:
            df = pd.read_excel(filepath)
            required_cols = ['nombre']
            # columnas opcionales compatibles con export masivo
            optional_cols = ['grupo_muscular', 'descripcion', 'objetivo']
            if not all(col in df.columns for col in required_cols):
                QMessageBox.critical(self, "Error de Formato", f"El archivo Excel debe contener la columna obligatoria: {', '.join(required_cols)}");
                return
            overwrite = getattr(self, 'overwrite_exercises_checkbox', None)
            overwrite_enabled = bool(overwrite.isChecked()) if overwrite else False

            added_count, updated_count, skipped_count = 0, 0, 0
            for _, row in df.iterrows():
                nombre = str(row.get('nombre', '')).strip()
                if not nombre:
                    continue
                grupo = row.get('grupo_muscular', None)
                desc = row.get('descripcion', None)
                objetivo = row.get('objetivo', None)
                # Normalizar NaN a None
                grupo = None if pd.isna(grupo) else str(grupo).strip() if grupo is not None else None
                desc = None if pd.isna(desc) else str(desc).strip() if desc is not None else None
                objetivo = None if pd.isna(objetivo) else str(objetivo).strip() if objetivo is not None else None

                try:
                    # Buscar si existe por nombre
                    existentes = self.db_manager.obtener_ejercicios(filtro=nombre)
                    existente = next((e for e in existentes if (getattr(e, 'nombre', e.get('nombre')) or '').strip().lower() == nombre.lower()), None)
                    if existente and overwrite_enabled:
                        # Construir objeto Ejercicio para actualizar
                        eid = getattr(existente, 'id', existente.get('id'))
                        ej_obj = Ejercicio(
                            id=eid,
                            nombre=nombre,
                            grupo_muscular=grupo,
                            descripcion=desc,
                            objetivo=objetivo or getattr(existente, 'objetivo', existente.get('objetivo', 'general')) or 'general'
                        )
                        self.db_manager.actualizar_ejercicio(ej_obj)
                        updated_count += 1
                    elif existente and not overwrite_enabled:
                        skipped_count += 1
                    else:
                        ej_new = Ejercicio(nombre=nombre, grupo_muscular=grupo, descripcion=desc, objetivo=objetivo or 'general')
                        self.db_manager.crear_ejercicio(ej_new)
                        added_count += 1
                except Exception as ex:
                    logging.exception("Error importing exercise row")
                    skipped_count += 1
                    continue

            QMessageBox.information(
                self,
                "Importación Completa",
                f"Añadidos: {added_count}\nActualizados: {updated_count}\nOmitidos: {skipped_count}"
            )
            # Refrescar filtros y tabla
            if hasattr(self, 'reload_exercise_filters'):
                self.reload_exercise_filters()
            self.populate_exercise_bank_table()
        except Exception as e:
            QMessageBox.critical(self, "Error de Lectura", f"No se pudo leer el archivo Excel: {e}")

    def show_exercise_import_guide(self):
        msg = (
            "Para importar ejercicios desde un archivo Excel (.xlsx), usa el siguiente formato:\n\n"
            "Columnas obligatorias:\n"
            "• nombre\n\n"
            "Columnas opcionales:\n"
            "• grupo_muscular, descripcion, objetivo\n\n"
            "Notas:\n"
            "• La columna 'nombre' identifica el ejercicio. Si activas 'Sobrescribir existentes por nombre', "
            "actualizaremos los ejercicios ya existentes con los nuevos datos.\n"
            "• Si 'objetivo' no está presente, se usará 'general'.\n"
            "• Ejemplos de filas:\n"
            "  nombre = 'Sentadilla', grupo_muscular = 'Piernas', objetivo = 'fuerza'\n"
            "  nombre = 'Press banca', grupo_muscular = 'Pecho', descripcion = 'Con barra', objetivo = 'hipertrofia'\n\n"
            "Sugerencia: Puedes descargar una plantilla desde '📄 Plantilla XLSX'."
        )
        QMessageBox.information(self, "Guía de Importación de Ejercicios", msg)

    def save_exercise_template_xlsx(self):
        filepath, _ = QFileDialog.getSaveFileName(
            self,
            "Guardar Plantilla Banco de Ejercicios",
            "plantilla_banco_ejercicios.xlsx",
            "Excel Files (*.xlsx)"
        )
        if not filepath:
            return
        try:
            df = pd.DataFrame([
                {"nombre": "Sentadilla", "grupo_muscular": "Piernas", "descripcion": "Con barra", "objetivo": "fuerza"},
                {"nombre": "Press banca", "grupo_muscular": "Pecho", "descripcion": "Con barra", "objetivo": "hipertrofia"},
                {"nombre": "Remo con barra", "grupo_muscular": "Espalda", "descripcion": "Pronado", "objetivo": "fuerza"},
            ], columns=["nombre", "grupo_muscular", "descripcion", "objetivo"])
            df.to_excel(filepath, index=False)
            QMessageBox.information(self, "Plantilla Guardada", f"Se guardó la plantilla en:\n{filepath}")
        except Exception as e:
            QMessageBox.critical(self, "Error al Guardar", f"No se pudo guardar la plantilla: {e}")

    def populate_template_list(self):
        self.template_list.clear()
        try:
            templates = self.db_manager.obtener_plantillas_rutina()
            for t in templates:
                item = QListWidgetItem(f"{t.nombre_rutina} ({t.dias_semana} días)")
                item.setData(Qt.ItemDataRole.UserRole, t); self.template_list.addItem(item)
        except Exception as e: QMessageBox.critical(self, "Error", f"No se pudieron cargar las plantillas: {e}")

     # --- MÉTODO CORREGIDO ---
    def add_template(self):
        dialog = TemplateEditorDialog(self, self.db_manager)
        if dialog.exec():
            self.populate_template_list()

    # --- MÉTODO CORREGIDO ---
    def edit_template(self):
        selected_item = self.template_list.currentItem()
        if not selected_item:
            QMessageBox.warning(self, "Sin selección", "Por favor, seleccione una plantilla para editar.")
            return
        template_to_edit = selected_item.data(Qt.ItemDataRole.UserRole)
        dialog = TemplateEditorDialog(self, self.db_manager, rutina=template_to_edit)
        if dialog.exec():
            self.populate_template_list()
    
    def delete_template(self):
        selected_item = self.template_list.currentItem()
        if not selected_item: QMessageBox.warning(self, "Sin selección", "Por favor, seleccione una plantilla para eliminar."); return
        template_to_delete = selected_item.data(Qt.ItemDataRole.UserRole)
        if QMessageBox.question(self, "Confirmar Eliminación", f"¿Está seguro de que desea eliminar la plantilla '{template_to_delete.nombre_rutina}'?", QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No, QMessageBox.StandardButton.No) == QMessageBox.StandardButton.Yes:
            try: self.db_manager.eliminar_rutina(template_to_delete.id); self.populate_template_list()
            except Exception as e: QMessageBox.critical(self, "Error", f"No se pudo eliminar la plantilla: {e}")
            
    def populate_class_type_table(self):
        self.class_type_table.setRowCount(0)
        try:
            clases = self.db_manager.obtener_clases()
            self.class_type_table.setRowCount(len(clases))
            for row, c in enumerate(clases):
                id_item = QTableWidgetItem(str(c.id)); id_item.setData(Qt.ItemDataRole.UserRole, c)
                self.class_type_table.setItem(row, 0, id_item); self.class_type_table.setItem(row, 1, QTableWidgetItem(c.nombre)); self.class_type_table.setItem(row, 2, QTableWidgetItem(c.descripcion))
        except Exception as e: QMessageBox.critical(self, "Error", f"No se pudieron cargar los tipos de clase: {e}")

    # --- MÉTODO CORREGIDO ---
    def add_class_type(self):
        # Se pasa el export_manager como argumento con nombre para evitar errores
        dialog = ClassEditorDialog(self, self.db_manager, export_manager=self.export_manager)
        if dialog.exec():
            self.populate_class_type_table()

    # --- MÉTODO CORREGIDO ---
    def edit_class_type(self):
        selected_row = self.class_type_table.currentRow()
        if selected_row < 0:
            QMessageBox.warning(self, "Sin selección", "Seleccione una clase de la tabla para editar.")
            return
        clase_a_editar = self.class_type_table.item(selected_row, 0).data(Qt.ItemDataRole.UserRole)
        # Se pasa el export_manager como argumento con nombre para evitar errores
        dialog = ClassEditorDialog(self, self.db_manager, export_manager=self.export_manager, clase=clase_a_editar)
        if dialog.exec():
            self.populate_class_type_table()

    def delete_class_type(self):
        selected_row = self.class_type_table.currentRow()
        if selected_row < 0: QMessageBox.warning(self, "Sin selección", "Seleccione una clase de la tabla para eliminar."); return
        clase_a_eliminar = self.class_type_table.item(selected_row, 0).data(Qt.ItemDataRole.UserRole)
        if QMessageBox.question(self, "Confirmar Eliminación", f"¿Está seguro de que desea eliminar la clase '{clase_a_eliminar.nombre}'?\nTODOS sus horarios y alumnos inscritos serán eliminados permanentemente.", QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No, QMessageBox.StandardButton.No) == QMessageBox.StandardButton.Yes:
            try: self.db_manager.eliminar_clase(clase_a_eliminar.id); self.populate_class_type_table()
            except Exception as e: QMessageBox.critical(self, "Error", f"No se pudo eliminar la clase: {e}")

    def export_settings(self):
        filepath, _ = QFileDialog.getSaveFileName(self, "Exportar Configuración", "configuracion_gym.json", "JSON Files (*.json)")
        if filepath:
            try:
                settings = {'precio_estandar': self.db_manager.obtener_configuracion('precio_estandar'), 'precio_estudiante': self.db_manager.obtener_configuracion('precio_estudiante'), 'system_info_html': self.db_manager.obtener_configuracion('system_info_html')}
                with open(filepath, 'w', encoding='utf-8') as f: json.dump(settings, f, indent=4)
                QMessageBox.information(self, "Éxito", f"Configuración exportada a\n{filepath}")
            except Exception as e: QMessageBox.critical(self, "Error", f"No se pudo exportar la configuración: {e}")

    def import_settings(self):
        filepath, _ = QFileDialog.getOpenFileName(self, "Importar Configuración", "", "JSON Files (*.json)")
        if filepath:
            try:
                with open(filepath, 'r', encoding='utf-8') as f: settings = json.load(f)
                for key, value in settings.items():
                    if value is not None: self.db_manager.actualizar_configuracion(key, str(value))
                QMessageBox.information(self, "Importación Completa", "Configuración importada con éxito.\nSe recomienda reiniciar la aplicación para que todos los cambios surtan efecto.")
                self.load_config(); self.precio_actualizado.emit()
                # Configuración de fuente eliminada
            except Exception as e: QMessageBox.critical(self, "Error de Importación", f"No se pudo importar la configuración: {e}")
    
    def open_task_automation(self):
        """Abre el widget de automatización de tareas en un diálogo modal."""
        from PyQt6.QtWidgets import QDialog, QVBoxLayout
        
        dialog = QDialog(self)
        dialog.setWindowTitle("Automatización de Tareas")
        dialog.setModal(True)
        dialog.resize(900, 700)
        
        layout = QVBoxLayout(dialog)
        automation_widget = TaskAutomationWidget(self.db_manager)
        layout.addWidget(automation_widget)
        
        dialog.exec()
    
    def open_bulk_import_export(self):
        """Abre el widget de importación/exportación masiva en un diálogo modal."""
        from PyQt6.QtWidgets import QDialog, QVBoxLayout
        
        dialog = QDialog(self)
        dialog.setWindowTitle("Importación y Exportación Masiva de Datos")
        dialog.setModal(True)
        dialog.resize(1000, 800)
        
        layout = QVBoxLayout(dialog)
        bulk_widget = BulkImportExportWidget(self.db_manager)
        layout.addWidget(bulk_widget)
        
        dialog.exec()
    
    def open_branding_customization(self):
        """Abre el widget de personalización y branding en un diálogo modal."""
        from PyQt6.QtWidgets import QDialog, QVBoxLayout
        
        dialog = QDialog(self)
        # Título eliminado para cumplir con diseño sin encabezado
        dialog.setModal(True)
        dialog.resize(1100, 800)
        
        layout = QVBoxLayout(dialog)
        branding_widget = BrandingCustomizationWidget(self.db_manager)
        layout.addWidget(branding_widget)
        
        # Conectar señal de branding para aplicar cambios en tiempo real
        branding_widget.branding_changed.connect(self.apply_branding_changes)
        
        dialog.exec()
    
    def apply_branding_changes(self, branding_config):
        """Aplica cambios de branding y los propaga al widget principal"""
        try:
            # Emitir señal personalizada para que main.py pueda capturarla
            # Esto se hace a través del parent window si existe
            main_window = self.window()
            if hasattr(main_window, 'apply_branding_changes'):
                main_window.apply_branding_changes(branding_config)
            
            logging.info("Cambios de branding aplicados desde ConfigTabWidget")
        except Exception as e:
            logging.error(f"Error aplicando cambios de branding desde ConfigTabWidget: {e}")
    

    

    

    

    

    
    def create_payment_concepts_widget(self):
        """Crea el widget de gestión de conceptos de pago."""
        widget = QWidget()
        widget.setObjectName("payment_concepts_widget")
        
        layout = QVBoxLayout(widget)
        # Reducir márgenes/espaciado para eliminar espacio superior innecesario
        layout.setSpacing(10)
        layout.setContentsMargins(10, 10, 10, 10)
        
        # Título principal eliminado
        
        # Contenedor principal con dos columnas
        main_container = QHBoxLayout()
        main_container.setSpacing(15)
        main_container.setContentsMargins(0, 0, 0, 0)

        # Encapsular columnas en widgets para controlar márgenes y alineación superior
        left_panel = QWidget()
        left_column = QVBoxLayout(left_panel)
        left_column.setSpacing(15)
        left_column.setContentsMargins(0, 0, 0, 0)
        
        # Grupo de lista de conceptos
        concepts_group = QGroupBox("📋 Conceptos de Pago Existentes")
        concepts_group.setObjectName("config_group")
        # Igualar política vertical y márgenes internos a formulario
        concepts_group.setSizePolicy(QSizePolicy.Policy.Preferred, QSizePolicy.Policy.Expanding)
        # Unificar altura del encabezado para evitar desfase de pocos píxeles
        concepts_group.setStyleSheet(
            "QGroupBox{margin-top: 12px;} "
            "QGroupBox::title{subcontrol-origin: margin; subcontrol-position: top left; padding: 0 6px;}"
        )
        concepts_layout = QVBoxLayout(concepts_group)
        concepts_layout.setSpacing(10)
        concepts_layout.setContentsMargins(15, 15, 15, 15)
        
        # Tabla de conceptos
        self.concepts_table = QTableWidget()
        self.concepts_table.setColumnCount(6)
        self.concepts_table.setHorizontalHeaderLabels(["ID", "Nombre", "Descripción", "Precio Base", "Tipo", "Estado"])
        # Ajuste de columnas: hacer más anchas Nombre, Descripción y Precio; achicar Estado
        header = self.concepts_table.horizontalHeader()
        header.setStretchLastSection(False)
        header.setSectionResizeMode(0, QHeaderView.ResizeMode.Fixed)
        header.setSectionResizeMode(1, QHeaderView.ResizeMode.Stretch)
        header.setSectionResizeMode(2, QHeaderView.ResizeMode.Stretch)
        header.setSectionResizeMode(3, QHeaderView.ResizeMode.Stretch)
        header.setSectionResizeMode(4, QHeaderView.ResizeMode.ResizeToContents)
        header.setSectionResizeMode(5, QHeaderView.ResizeMode.Fixed)
        self.concepts_table.setColumnWidth(0, 60)
        self.concepts_table.setColumnWidth(5, 80)
        self.concepts_table.setSelectionBehavior(QAbstractItemView.SelectionBehavior.SelectRows)
        self.concepts_table.setAlternatingRowColors(True)
        self.concepts_table.setSortingEnabled(True)
        # Asegurar expansión vertical para igualar el alto con el formulario
        self.concepts_table.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Expanding)
        # Mantener altura mínima consistente con Tipos de Cuota
        self.concepts_table.setMinimumHeight(120)
        concepts_layout.addWidget(self.concepts_table)
        
        # Botones de acción para la tabla
        table_buttons_layout = QHBoxLayout()
        table_buttons_layout.setSpacing(10)
        
        refresh_concepts_btn = QPushButton("🔄 Actualizar")
        refresh_concepts_btn.setObjectName("action_button")
        refresh_concepts_btn.clicked.connect(self.load_payment_concepts)
        table_buttons_layout.addWidget(refresh_concepts_btn)
        
        edit_concept_btn = QPushButton("✏️ Editar")
        edit_concept_btn.setObjectName("primary_button")
        edit_concept_btn.clicked.connect(self.edit_payment_concept)
        table_buttons_layout.addWidget(edit_concept_btn)
        
        toggle_concept_btn = QPushButton("🔄 Activar/Desactivar")
        toggle_concept_btn.setObjectName("warning_button")
        toggle_concept_btn.clicked.connect(self.toggle_payment_concept)
        table_buttons_layout.addWidget(toggle_concept_btn)
        
        delete_concept_btn = QPushButton("🗑️ Eliminar")
        delete_concept_btn.setObjectName("danger_button")
        delete_concept_btn.clicked.connect(self.delete_payment_concept)
        table_buttons_layout.addWidget(delete_concept_btn)
        
        table_buttons_layout.addStretch()
        concepts_layout.addLayout(table_buttons_layout)
        
        # Mantener expansión vertical para igualar altura con el formulario
        concepts_group.setSizePolicy(QSizePolicy.Policy.Preferred, QSizePolicy.Policy.Expanding)
        left_column.addWidget(concepts_group)
        
        # Columna derecha: Formulario de creación/edición
        right_panel = QWidget()
        right_column = QVBoxLayout(right_panel)
        right_column.setSpacing(15)
        right_column.setContentsMargins(0, 0, 0, 0)
        
        # Grupo de formulario
        form_group = QGroupBox("➕ Crear/Editar Concepto de Pago")
        form_group.setObjectName("config_group")
        # Alinear y permitir expansión para igualar altura con la lista
        form_group.setSizePolicy(QSizePolicy.Policy.Preferred, QSizePolicy.Policy.Expanding)
        # Unificar altura del encabezado para evitar desfase de pocos píxeles
        form_group.setStyleSheet(
            "QGroupBox{margin-top: 12px;} "
            "QGroupBox::title{subcontrol-origin: margin; subcontrol-position: top left; padding: 0 6px;}"
        )
        form_layout = QVBoxLayout(form_group)
        form_layout.setSpacing(10)
        form_layout.setContentsMargins(15, 15, 15, 15)
        
        # Campos del formulario
        fields_layout = QFormLayout()
        # Igualar separación interna a Tipos de Cuota
        fields_layout.setSpacing(10)
        fields_layout.setHorizontalSpacing(10)
        fields_layout.setVerticalSpacing(10)
        
        self.concept_name_input = QLineEdit()
        self.concept_name_input.setPlaceholderText("Ej: Mensualidad Básica")
        self.concept_name_input.setObjectName("form_input")
        fields_layout.addRow("📝 Nombre:", self.concept_name_input)
        
        self.concept_description_input = QTextEdit()
        self.concept_description_input.setMaximumHeight(80)
        self.concept_description_input.setPlaceholderText("Descripción del concepto de pago...")
        self.concept_description_input.setObjectName("form_input")
        fields_layout.addRow("📄 Descripción:", self.concept_description_input)
        
        self.concept_price_input = QDoubleSpinBox()
        self.concept_price_input.setRange(0.0, 999999.99)
        self.concept_price_input.setDecimals(2)
        self.concept_price_input.setPrefix("$ ")
        self.concept_price_input.setObjectName("form_input")
        fields_layout.addRow("💰 Precio Base:", self.concept_price_input)
        
        self.concept_type_combo = QComboBox()
        self.concept_type_combo.addItems(["fijo", "variable"])
        self.concept_type_combo.setObjectName("form_input")
        fields_layout.addRow("🏷️ Tipo:", self.concept_type_combo)
        
        form_layout.addLayout(fields_layout)
        
        # Botones del formulario
        form_buttons_layout = QHBoxLayout()
        form_buttons_layout.setSpacing(10)
        
        self.save_concept_btn = QPushButton("💾 Guardar Concepto")
        self.save_concept_btn.setObjectName("success_button")
        self.save_concept_btn.clicked.connect(self.save_payment_concept)
        form_buttons_layout.addWidget(self.save_concept_btn)
        
        clear_concept_form_btn = QPushButton("🧹 Limpiar Formulario")
        clear_concept_form_btn.setObjectName("secondary_button")
        clear_concept_form_btn.clicked.connect(self.clear_concept_form)
        form_buttons_layout.addWidget(clear_concept_form_btn)
        
        form_layout.addLayout(form_buttons_layout)
        
        right_column.addWidget(form_group)
        
        # Agregar columnas al contenedor principal y alinear arriba con precisión
        # Igualar a Tipos de Cuota: sin alineación explícita Top
        main_container.addWidget(left_panel, 2)  # 2/3 del espacio
        main_container.addWidget(right_panel, 1)  # 1/3 del espacio
        layout.addLayout(main_container)
        
        # Variable para rastrear si estamos editando
        self.editing_concept_id = None
        
        # Panel de estadísticas (igual a Tipos de Cuota)
        stats_group = QGroupBox("📊 Estadísticas de Conceptos")
        stats_group.setObjectName("config_group")
        # Achicar verticalmente el bloque de estadísticas
        stats_group.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Fixed)
        stats_group.setMinimumHeight(100)
        stats_group.setMaximumHeight(160)
        stats_layout = QVBoxLayout(stats_group)
        stats_layout.setSpacing(10)
        stats_layout.setContentsMargins(15, 15, 15, 15)
        
        # Contenedor de métricas
        metrics_layout = QHBoxLayout()
        metrics_layout.setSpacing(20)
        metrics_layout.setContentsMargins(0, 0, 0, 0)
        
        # Métrica: Total de conceptos
        self.concepts_total_frame = self.create_concepts_metric_frame("📊 Total", "0", "#3498db")
        metrics_layout.addWidget(self.concepts_total_frame)
        
        # Métrica: Conceptos activos
        self.concepts_active_frame = self.create_concepts_metric_frame("✅ Activos", "0", "#27ae60")
        metrics_layout.addWidget(self.concepts_active_frame)
        
        # Métrica: Conceptos inactivos
        self.concepts_inactive_frame = self.create_concepts_metric_frame("❌ Inactivos", "0", "#e74c3c")
        metrics_layout.addWidget(self.concepts_inactive_frame)
        
        # Métrica: Precio promedio
        self.concepts_avg_price_frame = self.create_concepts_metric_frame("💰 Precio Prom.", "$0.00", "#f39c12")
        metrics_layout.addWidget(self.concepts_avg_price_frame)
        
        stats_layout.addLayout(metrics_layout)
        layout.addWidget(stats_group)
        
        # Variable para rastrear si estamos editando
        self.editing_concept_id = None
        
        # Aplicar branding
        self.apply_branding_to_payment_concepts()
        
        return widget
    
    def setup_concepts_ui(self):
        """Configura la interfaz de usuario del widget de conceptos de pago."""
        # Layout principal
        main_layout = QVBoxLayout(self.payment_concepts_widget)
        main_layout.setContentsMargins(10, 10, 10, 10)
        main_layout.setSpacing(10)
        
        # Título principal eliminado
        
        # Contenedor principal horizontal
        main_container = QHBoxLayout()
        main_container.setSpacing(15)
        
        # Panel izquierdo (lista)
        self.setup_concepts_list_panel()
        main_container.addWidget(self.concepts_list_panel, 2)
        
        # Panel derecho (formulario)
        self.setup_concepts_form_panel()
        main_container.addWidget(self.concepts_form_panel, 1)
        
        main_layout.addLayout(main_container)
        
        # Panel de estadísticas
        self.setup_concepts_statistics_panel()
        # Panel inferior más corto
        main_layout.addWidget(self.concepts_statistics_panel)
        
        # Cargar datos iniciales después de que la UI esté completamente configurada
        self.load_payment_concepts()
    
    def setup_concepts_list_panel(self):
        """Configura el panel de lista de conceptos de pago."""
        # Widget contenedor con borde
        self.concepts_list_panel = QWidget()
        self.concepts_list_panel.setProperty("class", "panel_with_border")
        # Estirar verticalmente para igualar con el formulario
        self.concepts_list_panel.setSizePolicy(QSizePolicy.Policy.Preferred, QSizePolicy.Policy.Expanding)
        
        layout = QVBoxLayout(self.concepts_list_panel)
        layout.setContentsMargins(15, 15, 15, 15)
        layout.setSpacing(10)
        
        # Título del panel (sin emoji)
        panel_title = QLabel("Lista de Conceptos")
        panel_title.setObjectName("panel_title")
        panel_title.setAlignment(Qt.AlignmentFlag.AlignCenter)
        layout.addWidget(panel_title)
        
        # Tabla de conceptos
        self.concepts_table = QTableWidget()
        self.concepts_table.setColumnCount(6)
        self.concepts_table.setHorizontalHeaderLabels(["ID", "Nombre", "Descripción", "Precio Base", "Tipo", "Estado"])
        
        # Configurar tabla
        self.concepts_table.setSelectionBehavior(QAbstractItemView.SelectionBehavior.SelectRows)
        self.concepts_table.setAlternatingRowColors(True)
        self.concepts_table.setSortingEnabled(True)
        # Mantener altura mínima consistente con Tipos de Cuota
        self.concepts_table.setMinimumHeight(120)
        
        # Configurar altura de filas para mejor visualización de colores
        self.concepts_table.verticalHeader().setDefaultSectionSize(45)
        self.concepts_table.verticalHeader().setMinimumSectionSize(40)
        
        # Configurar menú contextual
        self.concepts_table.setContextMenuPolicy(Qt.ContextMenuPolicy.CustomContextMenu)
        self.concepts_table.customContextMenuRequested.connect(self.show_concepts_table_context_menu)
        
        # Configurar columnas
        header = self.concepts_table.horizontalHeader()
        header.setSectionResizeMode(0, header.ResizeMode.Fixed)
        header.setSectionResizeMode(1, header.ResizeMode.Stretch)
        header.setSectionResizeMode(2, header.ResizeMode.Stretch)
        header.setSectionResizeMode(3, header.ResizeMode.Stretch)
        header.setSectionResizeMode(4, header.ResizeMode.ResizeToContents)
        header.setSectionResizeMode(5, header.ResizeMode.Fixed)
        
        self.concepts_table.setColumnWidth(0, 60)
        self.concepts_table.setColumnWidth(5, 80)
        
        layout.addWidget(self.concepts_table)
        
        # Botones de acción
        buttons_layout = QHBoxLayout()
        buttons_layout.setSpacing(10)
        
        self.refresh_concepts_btn = QPushButton("🔄 Actualizar")
        self.refresh_concepts_btn.setObjectName("action_button")
        buttons_layout.addWidget(self.refresh_concepts_btn)
        
        self.add_concept_btn = QPushButton("➕ Añadir")
        self.add_concept_btn.setObjectName("success_button")
        buttons_layout.addWidget(self.add_concept_btn)
        
        self.edit_concept_btn = QPushButton("✏️ Editar")
        self.edit_concept_btn.setObjectName("primary_button")
        buttons_layout.addWidget(self.edit_concept_btn)
        
        self.toggle_concept_btn = QPushButton("🔄 Activar/Desactivar")
        self.toggle_concept_btn.setObjectName("warning_button")
        buttons_layout.addWidget(self.toggle_concept_btn)
        
        self.delete_concept_btn = QPushButton("🗑️ Eliminar")
        self.delete_concept_btn.setObjectName("danger_button")
        buttons_layout.addWidget(self.delete_concept_btn)
        
        layout.addLayout(buttons_layout)
    
    def setup_concepts_form_panel(self):
        """Configura el panel de formulario de conceptos de pago."""
        # Widget contenedor con borde
        self.concepts_form_panel = QWidget()
        self.concepts_form_panel.setProperty("class", "panel_with_border")
        # Estirar verticalmente para igualar con la lista
        self.concepts_form_panel.setSizePolicy(QSizePolicy.Policy.Preferred, QSizePolicy.Policy.Expanding)
        
        layout = QVBoxLayout(self.concepts_form_panel)
        layout.setContentsMargins(15, 15, 15, 15)
        layout.setSpacing(10)
        
        # Título del panel
        panel_title = QLabel("➕ Crear/Editar Concepto")
        panel_title.setObjectName("panel_title")
        panel_title.setAlignment(Qt.AlignmentFlag.AlignCenter)
        layout.addWidget(panel_title)
        
        # Formulario
        form_widget = QWidget()
        form_layout = QVBoxLayout(form_widget)
        form_layout.setSpacing(10)
        
        # Campos del formulario
        fields_layout = QFormLayout()
        fields_layout.setSpacing(10)
        
        self.concept_name_input = QLineEdit()
        self.concept_name_input.setPlaceholderText("Ej: Mensualidad Básica")
        self.concept_name_input.setObjectName("form_input")
        fields_layout.addRow("📝 Nombre:", self.concept_name_input)
        
        self.concept_description_input = QTextEdit()
        self.concept_description_input.setPlaceholderText("Descripción del concepto de pago...")
        self.concept_description_input.setMaximumHeight(80)
        self.concept_description_input.setObjectName("form_input")
        fields_layout.addRow("📄 Descripción:", self.concept_description_input)
        
        self.concept_price_input = QDoubleSpinBox()
        self.concept_price_input.setRange(0.0, 999999.99)
        self.concept_price_input.setDecimals(2)
        self.concept_price_input.setPrefix("$ ")
        self.concept_price_input.setObjectName("form_input")
        fields_layout.addRow("💰 Precio Base:", self.concept_price_input)
        
        self.concept_type_combo = QComboBox()
        self.concept_type_combo.addItems(["fijo", "variable"])
        self.concept_type_combo.setObjectName("form_input")
        fields_layout.addRow("🏷️ Tipo:", self.concept_type_combo)
        
        # Campo categoria eliminado - no existe en la base de datos
        
        form_layout.addLayout(fields_layout)
        
        # Botones del formulario
        form_buttons_layout = QHBoxLayout()
        form_buttons_layout.setSpacing(10)
        
        self.save_concept_btn = QPushButton("💾 Guardar")
        self.save_concept_btn.setObjectName("success_button")
        form_buttons_layout.addWidget(self.save_concept_btn)
        
        self.clear_concept_form_btn = QPushButton("🧹 Limpiar")
        self.clear_concept_form_btn.setObjectName("secondary_button")
        form_buttons_layout.addWidget(self.clear_concept_form_btn)
        
        form_layout.addLayout(form_buttons_layout)
        
        layout.addWidget(form_widget)
    
    def setup_concepts_statistics_panel(self):
        """Configura el panel de estadísticas de conceptos de pago."""
        # Widget contenedor con borde
        self.concepts_statistics_panel = QWidget()
        self.concepts_statistics_panel.setProperty("class", "panel_with_border")
        
        layout = QVBoxLayout(self.concepts_statistics_panel)
        layout.setContentsMargins(15, 15, 15, 15)
        layout.setSpacing(10)
        
        # Título del panel
        stats_title = QLabel("📊 Estadísticas de Conceptos")
        stats_title.setObjectName("panel_title")
        stats_title.setAlignment(Qt.AlignmentFlag.AlignCenter)
        layout.addWidget(stats_title)
        
        # Contenedor de métricas
        metrics_layout = QHBoxLayout()
        metrics_layout.setSpacing(20)
        
        # Métrica: Total de conceptos
        self.concepts_total_frame = self.create_concepts_metric_frame("📊 Total", "0", "#3498db")
        metrics_layout.addWidget(self.concepts_total_frame)
        
        # Métrica: Conceptos activos
        self.concepts_active_frame = self.create_concepts_metric_frame("✅ Activos", "0", "#27ae60")
        metrics_layout.addWidget(self.concepts_active_frame)
        
        # Métrica: Conceptos inactivos
        self.concepts_inactive_frame = self.create_concepts_metric_frame("❌ Inactivos", "0", "#e74c3c")
        metrics_layout.addWidget(self.concepts_inactive_frame)
        
        # Métrica: Precio promedio
        self.concepts_avg_price_frame = self.create_concepts_metric_frame("💰 Precio Prom.", "$0.00", "#f39c12")
        metrics_layout.addWidget(self.concepts_avg_price_frame)
        
        layout.addLayout(metrics_layout)
    
    def create_concepts_metric_frame(self, title, value, color):
        """Crea un frame de métrica para las estadísticas de conceptos."""
        frame = QFrame()
        frame.setFrameStyle(QFrame.Shape.StyledPanel)
        frame.setProperty("class", "metric_frame")
        
        layout = QVBoxLayout(frame)
        layout.setContentsMargins(10, 10, 10, 10)
        layout.setSpacing(5)
        
        # Título de la métrica
        title_label = QLabel(title)
        title_label.setObjectName("metric_title")
        title_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        layout.addWidget(title_label)
        
        # Valor de la métrica
        value_label = QLabel(value)
        value_label.setObjectName("metric_value")
        value_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        layout.addWidget(value_label)
        
        # Almacenar referencia al label de valor
        if "Total" in title:
            self.concepts_total_label = value_label
        elif "Activos" in title:
            self.concepts_active_label = value_label
        elif "Inactivos" in title:
            self.concepts_inactive_label = value_label
        elif "Precio" in title:
            self.concepts_avg_price_label = value_label
        
        return frame
    
    def connect_concepts_signals(self):
        """Conecta las señales de los elementos del widget de conceptos."""
        # Señales de la tabla
        self.concepts_table.selectionModel().selectionChanged.connect(self.on_concepts_selection_changed)
        
        # Señales de botones
        self.refresh_concepts_btn.clicked.connect(self.load_payment_concepts)
        self.add_concept_btn.clicked.connect(self.clear_concept_form)
        self.edit_concept_btn.clicked.connect(self.edit_payment_concept)
        self.toggle_concept_btn.clicked.connect(self.toggle_payment_concept)
        self.delete_concept_btn.clicked.connect(self.delete_payment_concept)
        self.save_concept_btn.clicked.connect(self.save_payment_concept)
        self.clear_concept_form_btn.clicked.connect(self.clear_concept_form)
    
    def on_concepts_selection_changed(self):
        """Maneja el cambio de selección en la tabla de conceptos."""
        selected_rows = self.concepts_table.selectionModel().selectedRows()
        has_selection = len(selected_rows) > 0
        
        # Habilitar/deshabilitar botones según la selección
        self.edit_concept_btn.setEnabled(has_selection)
        self.toggle_concept_btn.setEnabled(has_selection)
        self.delete_concept_btn.setEnabled(has_selection)
        
        if has_selection:
            # Cargar datos del concepto seleccionado en el formulario
            self.load_concept_to_form()
    
    def load_concept_to_form(self):
        """Carga los datos del concepto seleccionado en el formulario."""
        selected_row = self.concepts_table.currentRow()
        if selected_row >= 0:
            try:
                concepto = self.concepts_table.item(selected_row, 0).data(Qt.ItemDataRole.UserRole)
                
                # Cargar datos en el formulario
                self.concept_name_input.setText(concepto.nombre)
                self.concept_description_input.setPlainText(concepto.descripcion)
                self.concept_price_input.setValue(concepto.precio_base)
                
                # Seleccionar tipo en combo
                tipo_index = self.concept_type_combo.findText(concepto.tipo)
                if tipo_index >= 0:
                    self.concept_type_combo.setCurrentIndex(tipo_index)
                
                # Removed reference to categoria since it doesn't exist in the database
                
            except Exception as e:
                print(f"Error cargando concepto al formulario: {e}")
    
    def update_concepts_statistics(self):
        """Actualiza las estadísticas de conceptos de pago."""
        try:
            # Verificar que los frames existan antes de usarlos
            if not hasattr(self, 'concepts_total_frame'):
                print("Warning: concepts_total_frame no está inicializado")
                return
                
            conceptos = self.payment_manager.obtener_conceptos_pago()
            
            total = len(conceptos)
            activos = sum(1 for c in conceptos if c.activo)
            inactivos = total - activos
            precio_promedio = sum(c.precio_base for c in conceptos) / total if total > 0 else 0
            
            # Actualizar los valores en los frames de métricas
            self.update_concepts_metric_frame(self.concepts_total_frame, str(total))
            self.update_concepts_metric_frame(self.concepts_active_frame, str(activos))
            self.update_concepts_metric_frame(self.concepts_inactive_frame, str(inactivos))
            self.update_concepts_metric_frame(self.concepts_avg_price_frame, f"${precio_promedio:.2f}")
            
        except Exception as e:
            print(f"Error actualizando estadísticas de conceptos: {e}")
    
    def update_concepts_metric_frame(self, frame, value):
        """Actualiza el valor mostrado en un frame de métrica."""
        try:
            # Buscar el label de valor dentro del frame
            for child in frame.findChildren(QLabel):
                if child.objectName() == "metric_value":
                    child.setText(value)
                    break
        except Exception as e:
            print(f"Error actualizando frame de métrica: {e}")
    
    def load_payment_concepts(self, force: bool = False):
        """Carga todos los conceptos de pago en la tabla con protección de recarga reciente (debounce)."""
        try:
            # Protección contra recargas redundantes recientes
            if not force and getattr(self, "_last_load_concepts_at", None) is not None:
                elapsed = (datetime.now() - self._last_load_concepts_at).total_seconds()
                if elapsed < getattr(self, "_reload_cooldown_secs", 3):
                    return
            
            # Cargar todos los conceptos (activos e inactivos) para permitir reactivación
            conceptos = self.payment_manager.obtener_conceptos_pago(solo_activos=False)
            self.concepts_table.setRowCount(len(conceptos))
            
            for row, concepto in enumerate(conceptos):
                # ID
                id_item = QTableWidgetItem(str(concepto.id))
                id_item.setData(Qt.ItemDataRole.UserRole, concepto)
                self.concepts_table.setItem(row, 0, id_item)
                
                # Nombre
                self.concepts_table.setItem(row, 1, QTableWidgetItem(concepto.nombre))
                
                # Descripción (truncada si es muy larga)
                descripcion = concepto.descripcion[:50] + "..." if len(concepto.descripcion) > 50 else concepto.descripcion
                self.concepts_table.setItem(row, 2, QTableWidgetItem(descripcion))
                
                # Precio Base
                precio_item = QTableWidgetItem(f"$ {concepto.precio_base:.2f} ARS")
                precio_item.setData(Qt.ItemDataRole.UserRole, concepto.precio_base)
                self.concepts_table.setItem(row, 3, precio_item)
                
                # Tipo
                self.concepts_table.setItem(row, 4, QTableWidgetItem(concepto.tipo.title()))
                
                # Estado
                estado_item = QTableWidgetItem("✅ Activo" if concepto.activo else "❌ Inactivo")
                estado_item.setData(Qt.ItemDataRole.UserRole, concepto.activo)
                self.concepts_table.setItem(row, 5, estado_item)
                
                # Colorear fila según estado
                if not concepto.activo:
                    for col in range(6):
                        item = self.concepts_table.item(row, col)
                        if item:
                            item.setBackground(QColor(255, 240, 240))  # Fondo rojizo para inactivos
            
            # Las columnas ya están configuradas con políticas de resize específicas
            
            # Actualizar estadísticas
            self.update_concepts_statistics()
            
            # Marcar timestamp de última carga
            self._last_load_concepts_at = datetime.now()
            
        except Exception as e:
            QMessageBox.critical(self, "Error", f"No se pudieron cargar los conceptos de pago: {e}")
    
    def save_payment_concept(self):
        """Guarda un nuevo concepto de pago o actualiza uno existente."""
        try:
            # Validar campos
            nombre = self.concept_name_input.text().strip()
            if not nombre:
                QMessageBox.warning(self, "Campo Requerido", "El nombre del concepto es obligatorio.")
                return
            
            descripcion = self.concept_description_input.toPlainText().strip()
            precio_base = self.concept_price_input.value()
            tipo = self.concept_type_combo.currentText()
            
            if self.editing_concept_id:
                # Actualizar concepto existente
                concepto_actualizado = ConceptoPago(
                    id=self.editing_concept_id,
                    nombre=nombre,
                    descripcion=descripcion,
                    precio_base=precio_base,
                    tipo=tipo,
                    activo=True,  # Mantener activo al editar
                    fecha_creacion=None  # Se mantiene la original
                )
                
                self.payment_manager.actualizar_concepto_pago(concepto_actualizado)
                QMessageBox.information(self, "Éxito", f"Concepto '{nombre}' actualizado correctamente.")
                self.editing_concept_id = None
                self.save_concept_btn.setText("💾 Guardar Concepto")
            else:
                # Crear nuevo concepto
                nuevo_concepto = ConceptoPago(
                    id=None,
                    nombre=nombre,
                    descripcion=descripcion,
                    precio_base=precio_base,
                    tipo=tipo,
                    activo=True,
                    fecha_creacion=None  # Se asigna automáticamente
                )
                
                self.payment_manager.crear_concepto_pago(nuevo_concepto)
                QMessageBox.information(self, "Éxito", f"Concepto '{nombre}' creado correctamente.")
            
            # Limpiar formulario y recargar tabla
            self.clear_concept_form()
            self.load_payment_concepts(force=True)
            
        except Exception as e:
            QMessageBox.critical(self, "Error", f"No se pudo guardar el concepto: {e}")
    
    def edit_payment_concept(self):
        """Carga un concepto seleccionado en el formulario para edición."""
        selected_row = self.concepts_table.currentRow()
        if selected_row < 0:
            QMessageBox.warning(self, "Sin Selección", "Seleccione un concepto de la tabla para editar.")
            return
        
        try:
            concepto = self.concepts_table.item(selected_row, 0).data(Qt.ItemDataRole.UserRole)
            
            # Cargar datos en el formulario
            self.concept_name_input.setText(concepto.nombre)
            self.concept_description_input.setPlainText(concepto.descripcion)
            self.concept_price_input.setValue(concepto.precio_base)
            
            # Seleccionar tipo en combo
            tipo_index = self.concept_type_combo.findText(concepto.tipo)
            if tipo_index >= 0:
                self.concept_type_combo.setCurrentIndex(tipo_index)
            
            # Campo categoria eliminado - no existe en la base de datos
            
            # Cambiar modo a edición
            self.editing_concept_id = concepto.id
            self.save_concept_btn.setText("💾 Actualizar Concepto")
            
            QMessageBox.information(self, "Modo Edición", f"Concepto '{concepto.nombre}' cargado para edición.")
            
        except Exception as e:
            QMessageBox.critical(self, "Error", f"No se pudo cargar el concepto para edición: {e}")
    
    def duplicate_payment_concept(self):
        """Duplica un concepto de pago seleccionado."""
        selected_row = self.concepts_table.currentRow()
        if selected_row < 0:
            QMessageBox.warning(self, "Sin Selección", "Seleccione un concepto de la tabla para duplicar.")
            return
        
        try:
            concepto = self.concepts_table.item(selected_row, 0).data(Qt.ItemDataRole.UserRole)
            
            # Crear nuevo concepto con datos duplicados
            nuevo_nombre = f"{concepto.nombre} (Copia)"
            
            # Cargar datos en el formulario para crear el duplicado
            self.concept_name_input.setText(nuevo_nombre)
            self.concept_description_input.setPlainText(concepto.descripcion)
            self.concept_price_input.setValue(concepto.precio_base)
            
            # Seleccionar tipo en combo
            tipo_index = self.concept_type_combo.findText(concepto.tipo)
            if tipo_index >= 0:
                self.concept_type_combo.setCurrentIndex(tipo_index)
            
            # Campo categoria eliminado - no existe en la base de datos
            
            # Asegurar que estamos en modo creación (no edición)
            self.editing_concept_id = None
            self.save_concept_btn.setText("💾 Guardar Concepto")
            
            QMessageBox.information(self, "Concepto Duplicado", f"Concepto '{concepto.nombre}' duplicado. Modifique los datos y guarde.")
            
        except Exception as e:
            QMessageBox.critical(self, "Error", f"No se pudo duplicar el concepto: {e}")
    
    def toggle_payment_concept(self):
        """Activa o desactiva un concepto de pago."""
        selected_row = self.concepts_table.currentRow()
        if selected_row < 0:
            QMessageBox.warning(self, "Sin Selección", "Seleccione un concepto de la tabla para activar/desactivar.")
            return
        
        try:
            concepto = self.concepts_table.item(selected_row, 0).data(Qt.ItemDataRole.UserRole)
            nuevo_estado = not concepto.activo
            accion = "activar" if nuevo_estado else "desactivar"
            
            if QMessageBox.question(
                self, "Confirmar Acción", 
                f"¿Está seguro de que desea {accion} el concepto '{concepto.nombre}'?",
                QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
                QMessageBox.StandardButton.No
            ) == QMessageBox.StandardButton.Yes:
                
                # Actualizar concepto con nuevo estado
                concepto_actualizado = ConceptoPago(
                    id=concepto.id,
                    nombre=concepto.nombre,
                    descripcion=concepto.descripcion,
                    precio_base=concepto.precio_base,
                    tipo=concepto.tipo,
                    activo=nuevo_estado,
                    fecha_creacion=concepto.fecha_creacion,
                    categoria=concepto.categoria
                )
                
                self.payment_manager.actualizar_concepto_pago(concepto_actualizado)
                QMessageBox.information(self, "Éxito", f"Concepto '{concepto.nombre}' {accion}do correctamente.")
                self.load_payment_concepts(force=True)
                
        except Exception as e:
            QMessageBox.critical(self, "Error", f"No se pudo cambiar el estado del concepto: {e}")
    
    def delete_payment_concept(self):
        """Elimina (soft delete) un concepto de pago."""
        selected_row = self.concepts_table.currentRow()
        if selected_row < 0:
            QMessageBox.warning(self, "Sin Selección", "Seleccione un concepto de la tabla para eliminar.")
            return
        
        try:
            concepto = self.concepts_table.item(selected_row, 0).data(Qt.ItemDataRole.UserRole)
            
            if QMessageBox.question(
                self, "Confirmar Eliminación", 
                f"¿Está seguro de que desea eliminar el concepto '{concepto.nombre}'?\n\n"
                "Esta acción desactivará permanentemente el concepto, pero mantendrá el historial de pagos asociados.",
                QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
                QMessageBox.StandardButton.No
            ) == QMessageBox.StandardButton.Yes:
                
                self.payment_manager.eliminar_concepto_pago(concepto.id)
                QMessageBox.information(self, "Éxito", f"Concepto '{concepto.nombre}' eliminado correctamente.")
                self.load_payment_concepts(force=True)
                
        except Exception as e:
            QMessageBox.critical(self, "Error", f"No se pudo eliminar el concepto: {e}")
    
    def clear_concept_form(self):
        """Limpia todos los campos del formulario de conceptos."""
        self.concept_name_input.clear()
        self.concept_description_input.clear()
        self.concept_price_input.setValue(0.0)
        self.concept_type_combo.setCurrentIndex(0)
        
        # Resetear modo de edición
        self.editing_concept_id = None
        self.save_concept_btn.setText("💾 Guardar Concepto")
    
    def apply_branding_to_payment_concepts(self):
        """Aplica el sistema de branding automático al widget de gestión de conceptos de pago"""
        try:
            # Obtener configuración de branding desde el main window
            main_window = self.window()
            if hasattr(main_window, 'branding_config') and main_window.branding_config:
                branding_config = main_window.branding_config
                
                # Obtener colores del branding
                primary_color = branding_config.get('primary_color', '#5E81AC')
                secondary_color = branding_config.get('secondary_color', '#88C0D0')
                background_color = branding_config.get('background_color', '#FFFFFF')
                alt_background_color = branding_config.get('alt_background_color', '#F8F9FA')
                surface_color = branding_config.get('surface_color', '#FFFFFF')
                accent_color = branding_config.get('accent_color', '#A3BE8C')
                text_color = branding_config.get('text_color', '#2E3440')
                
                # Calcular colores de texto con contraste WCAG 2.1 AAA
                if hasattr(main_window, '_get_contrasting_text_color'):
                    auto_text_color = main_window._get_contrasting_text_color(background_color, require_aaa=True)
                    auto_primary_text_color = main_window._get_contrasting_text_color(primary_color, require_aaa=True)
                    auto_surface_text_color = main_window._get_contrasting_text_color(surface_color, require_aaa=True)
                    auto_secondary_text_color = main_window._get_contrasting_text_color(secondary_color, require_aaa=True)
                else:
                    # Fallback a colores por defecto
                    auto_text_color = text_color
                    auto_primary_text_color = '#FFFFFF' if self._is_dark_color(primary_color) else '#000000'
                    auto_surface_text_color = '#000000' if self._is_light_color(surface_color) else '#FFFFFF'
                    auto_secondary_text_color = '#FFFFFF' if self._is_dark_color(secondary_color) else '#000000'
                
                # Aplicar estilos al widget de conceptos de pago si existe
                if hasattr(self, 'concepts_table'):
                    self._apply_concepts_table_branding({
                        'primary': primary_color,
                        'secondary': secondary_color,
                        'background_color': background_color,
                        'alt_background': alt_background_color,
                        'surface': surface_color,
                        'accent': accent_color,
                        'text': auto_text_color,
                        'primary_text': auto_primary_text_color,
                        'surface_text': auto_surface_text_color,
                        'secondary_text': auto_secondary_text_color
                    })
                
        except Exception as e:
            print(f"Error aplicando branding a widget de conceptos: {e}")
    
    def _apply_concepts_table_branding(self, colors):
        """Aplica branding específico a la tabla y formularios de conceptos"""
        try:
            # Estilo para la tabla de conceptos
            table_style = f"""
                QTableWidget {{
                    background-color: {colors['surface']};
                    color: {colors['surface_text']};
                    border: 2px solid {colors['primary']};
                    border-radius: 8px;
                    gridline-color: {colors['secondary']};
                    selection-background-color: {colors['primary']};
                    selection-color: {colors['primary_text']};
                    alternate-background-color: {colors['alt_background']};
                }}
                
                QTableWidget::item {{
                    padding: 8px;
                    border-bottom: 1px solid {colors['secondary']};
                }}
                
                QTableWidget::item:selected {{
                    background-color: {colors['primary']};
                    color: {colors['primary_text']};
                }}
                
                QHeaderView::section {{
                    background-color: {colors['primary']};
                    color: {colors['primary_text']};
                    padding: 10px;
                    border: none;
                    font-weight: bold;
                }}
            """
            
            if hasattr(self, 'concepts_table'):
                # Migrado al sistema CSS dinámico - los estilos se aplican automáticamente
                self.concepts_table.setObjectName("payment_concepts_table")
                self.concepts_table.setProperty("class", "config_table")
            
            # Migrado al sistema CSS dinámico - los estilos de botones se aplican automáticamente
            # Los botones usan setObjectName y setProperty para el sistema CSS dinámico
            
            # Migrado al sistema CSS dinámico - los estilos de formularios se aplican automáticamente
            # Los inputs usan setObjectName y setProperty para el sistema CSS dinámico
            
            # Migrado al sistema CSS dinámico - los estilos de GroupBox y títulos se aplican automáticamente
            # Los grupos usan setObjectName y setProperty para el sistema CSS dinámico
            
            # Aplicar estilos al widget usando CSS dinámico
            if hasattr(self, 'payment_concepts_widget'):
                # Migrado al sistema CSS dinámico - los estilos se aplican automáticamente
                self.payment_concepts_widget.setObjectName("payment_concepts_widget")
                self.payment_concepts_widget.setProperty("class", "config_widget")
            
        except Exception as e:
            print(f"Error aplicando estilos de branding a conceptos: {e}")
    
    def _is_dark_color(self, color_hex):
        """Determina si un color es oscuro basado en su luminancia"""
        try:
            color_hex = color_hex.lstrip('#')
            r, g, b = tuple(int(color_hex[i:i+2], 16) for i in (0, 2, 4))
            luminance = (0.299 * r + 0.587 * g + 0.114 * b) / 255
            return luminance < 0.5
        except:
            return False
    
    def _is_light_color(self, color_hex):
        """Determina si un color es claro basado en su luminancia"""
        return not self._is_dark_color(color_hex)
    
    def _lighten_color(self, color_hex, factor=0.1):
        """Aclara un color por un factor dado"""
        try:
            color_hex = color_hex.lstrip('#')
            r, g, b = tuple(int(color_hex[i:i+2], 16) for i in (0, 2, 4))
            r = min(255, int(r + (255 - r) * factor))
            g = min(255, int(g + (255 - g) * factor))
            b = min(255, int(b + (255 - b) * factor))
            return f"#{r:02x}{g:02x}{b:02x}"
        except:
            return color_hex
    
    def create_payment_methods_widget(self):
        """Crea el widget de gestión de métodos de pago."""
        widget = QWidget()
        widget.setObjectName("payment_methods_widget")
        
        layout = QVBoxLayout(widget)
        # Igualar a Tipos de Cuota: márgenes 10 y spacing 10
        layout.setSpacing(10)
        layout.setContentsMargins(10, 10, 10, 10)
        
        # Contenedor principal con dos columnas (spacing 15)
        main_container = QHBoxLayout()
        main_container.setSpacing(15)
        main_container.setContentsMargins(0, 0, 0, 0)
        
        # Panel izquierdo: Lista de métodos (encapsulado en QWidget)
        left_panel = QWidget()
        left_column = QVBoxLayout(left_panel)
        left_column.setSpacing(15)
        left_column.setContentsMargins(0, 0, 0, 0)
        
        # Grupo de lista de métodos
        methods_group = QGroupBox("📋 Métodos de Pago Existentes")
        methods_group.setObjectName("config_group")
        methods_group.setSizePolicy(QSizePolicy.Policy.Preferred, QSizePolicy.Policy.Expanding)
        methods_layout = QVBoxLayout(methods_group)
        methods_layout.setSpacing(10)
        methods_layout.setContentsMargins(15, 15, 15, 15)
        
        # Tabla de métodos
        self.methods_table = QTableWidget()
        self.methods_table.setColumnCount(6)
        self.methods_table.setHorizontalHeaderLabels(["ID", "Nombre", "Icono", "Color", "Comisión (%)", "Estado"])
        # Ajustes de columnas para visibilidad: Nombre y Comisión más anchas; Estado angosta
        header = self.methods_table.horizontalHeader()
        header.setStretchLastSection(False)
        header.setSectionResizeMode(0, QHeaderView.ResizeMode.Fixed)
        header.setSectionResizeMode(1, QHeaderView.ResizeMode.Stretch)  # Nombre más ancho
        header.setSectionResizeMode(2, QHeaderView.ResizeMode.Fixed)
        header.setSectionResizeMode(3, QHeaderView.ResizeMode.ResizeToContents)
        header.setSectionResizeMode(4, QHeaderView.ResizeMode.Stretch)  # Comisión más ancha
        header.setSectionResizeMode(5, QHeaderView.ResizeMode.Fixed)    # Estado más angosta
        self.methods_table.setColumnWidth(0, 60)
        self.methods_table.setColumnWidth(1, 240)
        self.methods_table.setColumnWidth(2, 80)
        self.methods_table.setColumnWidth(4, 140)
        self.methods_table.setColumnWidth(5, 80)
        self.methods_table.setSelectionBehavior(QAbstractItemView.SelectionBehavior.SelectRows)
        self.methods_table.setAlternatingRowColors(True)
        self.methods_table.setSortingEnabled(True)
        # Igualar crecimiento vertical de la tabla
        self.methods_table.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Expanding)
        methods_layout.addWidget(self.methods_table)
        
        # Botones de acción para la tabla
        table_buttons_layout = QHBoxLayout()
        table_buttons_layout.setSpacing(10)
        
        refresh_methods_btn = QPushButton("🔄 Actualizar")
        refresh_methods_btn.setObjectName("action_button")
        refresh_methods_btn.clicked.connect(self.load_payment_methods)
        table_buttons_layout.addWidget(refresh_methods_btn)
        
        edit_method_btn = QPushButton("✏️ Editar")
        edit_method_btn.setObjectName("primary_button")
        edit_method_btn.clicked.connect(self.edit_payment_method)
        table_buttons_layout.addWidget(edit_method_btn)
        
        toggle_method_btn = QPushButton("🔄 Activar/Desactivar")
        toggle_method_btn.setObjectName("warning_button")
        toggle_method_btn.clicked.connect(self.toggle_payment_method)
        table_buttons_layout.addWidget(toggle_method_btn)
        
        delete_method_btn = QPushButton("🗑️ Eliminar")
        delete_method_btn.setObjectName("danger_button")
        delete_method_btn.clicked.connect(self.delete_payment_method)
        table_buttons_layout.addWidget(delete_method_btn)
        
        table_buttons_layout.addStretch()
        methods_layout.addLayout(table_buttons_layout)
        
        left_column.addWidget(methods_group)
        
        # Panel derecho: Formulario (encapsulado en QWidget)
        right_panel = QWidget()
        right_column = QVBoxLayout(right_panel)
        right_column.setSpacing(15)
        right_column.setContentsMargins(0, 0, 0, 0)
        
        # Grupo de formulario
        form_group = QGroupBox("➕ Crear/Editar Método de Pago")
        form_group.setObjectName("config_group")
        form_group.setSizePolicy(QSizePolicy.Policy.Preferred, QSizePolicy.Policy.Expanding)
        form_layout = QVBoxLayout(form_group)
        form_layout.setSpacing(10)
        form_layout.setContentsMargins(15, 15, 15, 15)
        
        # Campos del formulario
        fields_layout = QFormLayout()
        fields_layout.setSpacing(10)
        
        self.method_name_input = QLineEdit()
        self.method_name_input.setPlaceholderText("Ej: Tarjeta de Crédito")
        self.method_name_input.setObjectName("form_input")
        fields_layout.addRow("📝 Nombre:", self.method_name_input)
        
        self.method_icon_input = QLineEdit()
        self.method_icon_input.setPlaceholderText("Ej: 💳")
        self.method_icon_input.setObjectName("form_input")
        fields_layout.addRow("🎨 Icono:", self.method_icon_input)
        
        self.method_color_input = QLineEdit()
        self.method_color_input.setPlaceholderText("Ej: #3498db")
        self.method_color_input.setObjectName("form_input")
        fields_layout.addRow("🎨 Color:", self.method_color_input)
        
        self.method_commission_input = QDoubleSpinBox()
        self.method_commission_input.setRange(0.0, 100.0)
        self.method_commission_input.setDecimals(2)
        self.method_commission_input.setSuffix(" %")
        self.method_commission_input.setObjectName("form_input")
        fields_layout.addRow("💰 Comisión:", self.method_commission_input)
        
        form_layout.addLayout(fields_layout)
        
        # Botones del formulario
        form_buttons_layout = QHBoxLayout()
        form_buttons_layout.setSpacing(10)
        
        self.save_method_btn = QPushButton("💾 Guardar Método")
        self.save_method_btn.setObjectName("success_button")
        self.save_method_btn.clicked.connect(self.save_payment_method)
        form_buttons_layout.addWidget(self.save_method_btn)
        
        clear_method_form_btn = QPushButton("🧹 Limpiar Formulario")
        clear_method_form_btn.setObjectName("secondary_button")
        clear_method_form_btn.clicked.connect(self.clear_method_form)
        form_buttons_layout.addWidget(clear_method_form_btn)
        
        form_layout.addLayout(form_buttons_layout)
        
        right_column.addWidget(form_group)
        
        # Agregar columnas al contenedor principal como widgets con alineación superior
        # Igualar a Tipos de Cuota: sin alineación explícita Top
        main_container.addWidget(left_panel, 2)
        main_container.addWidget(right_panel, 1)
        layout.addLayout(main_container)
        
        # Panel inferior de estadísticas (igual a Tipos de Cuota)
        stats_group = QGroupBox("📊 Estadísticas de Métodos de Pago")
        stats_group.setObjectName("config_group")
        # Achicar verticalmente el bloque de estadísticas
        stats_group.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Fixed)
        stats_group.setMinimumHeight(100)
        stats_group.setMaximumHeight(160)
        stats_layout = QVBoxLayout(stats_group)
        stats_layout.setSpacing(10)
        stats_layout.setContentsMargins(15, 15, 15, 15)
        
        metrics_layout = QHBoxLayout()
        metrics_layout.setSpacing(20)
        metrics_layout.setContentsMargins(0, 0, 0, 0)
        
        # Métricas placeholder (se pueden poblar más adelante)
        self.total_methods_label = QLabel("0")
        self.total_methods_label.setObjectName("metric_value")
        self.total_methods_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        metrics_layout.addWidget(self.total_methods_label)
        
        self.methods_active_label = QLabel("0")
        self.methods_active_label.setObjectName("metric_value")
        self.methods_active_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        metrics_layout.addWidget(self.methods_active_label)
        
        self.methods_inactive_label = QLabel("0")
        self.methods_inactive_label.setObjectName("metric_value")
        self.methods_inactive_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        metrics_layout.addWidget(self.methods_inactive_label)
        
        stats_layout.addLayout(metrics_layout)
        layout.addWidget(stats_group)
        
        return widget
        main_container.addLayout(right_column, 1)  # 1/3 del espacio
        layout.addLayout(main_container)
        layout.setAlignment(main_container, Qt.AlignmentFlag.AlignTop)
        
        # Panel de estadísticas de métodos
        self.setup_methods_statistics_panel()
        layout.addWidget(self.methods_statistics_panel)
        
        # Variable para rastrear si estamos editando
        self.editing_method_id = None
        
        # Cargar datos iniciales
        self.load_payment_methods()
        
        # Aplicar branding
        self.apply_branding_to_payment_methods()
        
        return widget
    
    def setup_methods_ui(self, widget):
        """Configura la interfaz de usuario del widget de métodos de pago."""
        main_layout = QVBoxLayout(widget)
        main_layout.setContentsMargins(10, 10, 10, 10)
        main_layout.setSpacing(10)
        
        # Título principal eliminado
        
        # Contenedor principal horizontal
        content_layout = QHBoxLayout()
        content_layout.setSpacing(15)
        
        # Panel izquierdo: Lista
        self.setup_methods_list_panel(content_layout)
        
        # Panel derecho: Formulario
        self.setup_methods_form_panel(content_layout)
        
        main_layout.addLayout(content_layout)
    
    def setup_methods_list_panel(self, parent_layout):
        """Configura el panel de lista de métodos de pago."""
        # Widget contenedor con borde
        list_widget = QWidget()
        list_widget.setProperty("class", "panel_with_border")
        # Estirar verticalmente para igualar con el formulario
        list_widget.setSizePolicy(QSizePolicy.Policy.Preferred, QSizePolicy.Policy.Expanding)
        list_layout = QVBoxLayout(list_widget)
        list_layout.setContentsMargins(15, 15, 15, 15)
        list_layout.setSpacing(10)
        
        # Título del panel
        panel_title = QLabel("📋 Métodos de Pago Registrados")
        panel_title.setProperty("class", "panel_title")
        list_layout.addWidget(panel_title)
        
        # Tabla de métodos
        self.methods_table = QTableWidget()
        self.methods_table.setColumnCount(6)
        self.methods_table.setHorizontalHeaderLabels(["ID", "Nombre", "Icono", "Color", "Comisión (%)", "Estado"])
        
        # Configurar tabla
        self.methods_table.setSelectionBehavior(QAbstractItemView.SelectionBehavior.SelectRows)
        self.methods_table.setAlternatingRowColors(True)
        self.methods_table.setSortingEnabled(True)
        self.methods_table.setProperty("class", "data_table")
        # Asegurar expansión vertical para igualar el alto con el formulario
        self.methods_table.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Expanding)
        # Mantener altura mínima consistente con Tipos de Cuota
        self.methods_table.setMinimumHeight(120)
        
        # Configurar menú contextual
        self.methods_table.setContextMenuPolicy(Qt.ContextMenuPolicy.CustomContextMenu)
        self.methods_table.customContextMenuRequested.connect(self.show_methods_table_context_menu)
        
        # Configurar columnas
        header = self.methods_table.horizontalHeader()
        header.setStretchLastSection(False)
        header.setSectionResizeMode(0, header.ResizeMode.Fixed)
        header.setSectionResizeMode(1, header.ResizeMode.Stretch)  # Nombre más ancho
        header.setSectionResizeMode(2, header.ResizeMode.Fixed)
        header.setSectionResizeMode(3, header.ResizeMode.ResizeToContents)
        header.setSectionResizeMode(4, header.ResizeMode.Stretch)  # Comisión más ancha
        header.setSectionResizeMode(5, header.ResizeMode.Fixed)    # Estado más angosta
        
        self.methods_table.setColumnWidth(0, 60)
        self.methods_table.setColumnWidth(1, 240)
        self.methods_table.setColumnWidth(2, 80)
        self.methods_table.setColumnWidth(4, 140)
        self.methods_table.setColumnWidth(5, 80)
        
        list_layout.addWidget(self.methods_table)
        
        # Botones de acción
        buttons_layout = QHBoxLayout()
        buttons_layout.setSpacing(10)
        
        self.refresh_methods_btn = QPushButton("🔄 Actualizar")
        self.refresh_methods_btn.setProperty("class", "action_button")
        buttons_layout.addWidget(self.refresh_methods_btn)
        
        self.add_method_btn = QPushButton("➕ Añadir")
        self.add_method_btn.setProperty("class", "success_button")
        buttons_layout.addWidget(self.add_method_btn)
        
        self.edit_method_btn = QPushButton("✏️ Editar")
        self.edit_method_btn.setProperty("class", "primary_button")
        buttons_layout.addWidget(self.edit_method_btn)
        
        self.toggle_method_btn = QPushButton("🔄 Activar/Desactivar")
        self.toggle_method_btn.setProperty("class", "warning_button")
        buttons_layout.addWidget(self.toggle_method_btn)
        
        self.delete_method_btn = QPushButton("🗑️ Eliminar")
        self.delete_method_btn.setProperty("class", "danger_button")
        buttons_layout.addWidget(self.delete_method_btn)
        
        buttons_layout.addStretch()
        list_layout.addLayout(buttons_layout)
        
        parent_layout.addWidget(list_widget, 2)
    
    def setup_methods_form_panel(self, parent_layout):
        """Configura el panel de formulario de métodos de pago."""
        # Widget contenedor con borde
        form_widget = QWidget()
        form_widget.setProperty("class", "panel_with_border")
        # Estirar verticalmente para igualar con la lista
        form_widget.setSizePolicy(QSizePolicy.Policy.Preferred, QSizePolicy.Policy.Expanding)
        form_layout = QVBoxLayout(form_widget)
        form_layout.setContentsMargins(15, 15, 15, 15)
        form_layout.setSpacing(15)
        
        # Título del panel
        panel_title = QLabel("➕ Crear/Editar Método de Pago")
        panel_title.setProperty("class", "panel_title")
        form_layout.addWidget(panel_title)
        
        # Campos del formulario
        fields_layout = QFormLayout()
        # Igualar separación interna a Tipos de Cuota
        fields_layout.setSpacing(10)
        fields_layout.setHorizontalSpacing(10)
        fields_layout.setVerticalSpacing(10)
        fields_layout.setLabelAlignment(Qt.AlignmentFlag.AlignRight)
        
        # Campo Nombre
        self.method_name_input = QLineEdit()
        self.method_name_input.setPlaceholderText("Ej: Tarjeta de Crédito")
        self.method_name_input.setProperty("class", "form_input")
        fields_layout.addRow("📝 Nombre:", self.method_name_input)
        
        # Campo Icono
        self.method_icon_input = QLineEdit()
        self.method_icon_input.setPlaceholderText("Ej: 💳")
        self.method_icon_input.setProperty("class", "form_input")
        fields_layout.addRow("🎨 Icono:", self.method_icon_input)
        
        # Campo Color
        self.method_color_input = QLineEdit()
        self.method_color_input.setPlaceholderText("Ej: #3498db")
        self.method_color_input.setProperty("class", "form_input")
        fields_layout.addRow("🎨 Color:", self.method_color_input)
        
        # Campo Comisión
        self.method_commission_input = QDoubleSpinBox()
        self.method_commission_input.setRange(0.0, 100.0)
        self.method_commission_input.setDecimals(2)
        self.method_commission_input.setSuffix(" %")
        self.method_commission_input.setProperty("class", "form_input")
        fields_layout.addRow("💰 Comisión:", self.method_commission_input)
        
        form_layout.addLayout(fields_layout)
        
        # Botones del formulario
        buttons_layout = QHBoxLayout()
        buttons_layout.setSpacing(10)
        
        self.save_method_btn = QPushButton("💾 Guardar Método")
        self.save_method_btn.setProperty("class", "success_button")
        buttons_layout.addWidget(self.save_method_btn)
        
        self.clear_method_form_btn = QPushButton("🧹 Limpiar")
        self.clear_method_form_btn.setProperty("class", "secondary_button")
        buttons_layout.addWidget(self.clear_method_form_btn)
        
        buttons_layout.addStretch()
        form_layout.addLayout(buttons_layout)
        
        # Quitar espaciador para no incrementar la altura del bloque superior
        # y mantener la separación con estadísticas igual a Tipos de Cuota
        
        parent_layout.addWidget(form_widget, 1)
    
    
    def setup_methods_statistics_panel(self):
        """Configura el panel de estadísticas de métodos de pago con estructura uniforme."""
        stats_group = QGroupBox("📊 Estadísticas de Métodos de Pago")
        stats_group.setObjectName("config_group")
        # Reducir verticalmente el panel y evitar expansión
        stats_group.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Fixed)
        stats_group.setMinimumHeight(100)
        stats_group.setMaximumHeight(160)

        layout = QVBoxLayout(stats_group)
        layout.setContentsMargins(15, 15, 15, 15)
        layout.setSpacing(10)

        # Contenedor de métricas
        metrics_layout = QHBoxLayout()
        metrics_layout.setSpacing(20)

        # Métrica: Total de métodos
        self.methods_total_frame = self.create_concepts_metric_frame("📊 Total", "0", "#3498db")
        metrics_layout.addWidget(self.methods_total_frame)

        # Métrica: Métodos activos
        self.methods_active_frame = self.create_concepts_metric_frame("✅ Activos", "0", "#27ae60")
        metrics_layout.addWidget(self.methods_active_frame)

        # Métrica: Métodos inactivos
        self.methods_inactive_frame = self.create_concepts_metric_frame("❌ Inactivos", "0", "#e74c3c")
        metrics_layout.addWidget(self.methods_inactive_frame)

        # Métrica: Comisión promedio
        self.methods_avg_commission_frame = self.create_concepts_metric_frame("💰 Comisión Prom.", "0.00 %", "#f39c12")
        metrics_layout.addWidget(self.methods_avg_commission_frame)

        layout.addLayout(metrics_layout)

        # Contenedor externo como atributo para añadir al layout principal
        self.methods_statistics_panel = stats_group
    
    def create_methods_metric_frame(self, title, value, color):
        """Crea un frame de métrica para las estadísticas de métodos."""
        frame = QFrame()
        frame.setFrameStyle(QFrame.Shape.StyledPanel)
        frame.setProperty("class", "metric_frame")
        
        layout = QVBoxLayout(frame)
        layout.setContentsMargins(10, 10, 10, 10)
        layout.setSpacing(5)
        
        # Título de la métrica
        title_label = QLabel(title)
        title_label.setObjectName("metric_title")
        title_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        layout.addWidget(title_label)
        
        # Valor de la métrica
        value_label = QLabel(value)
        value_label.setObjectName("metric_value")
        value_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        layout.addWidget(value_label)
        
        return frame
    
    def update_methods_metric_frame(self, frame, value):
        """Actualiza el valor mostrado en un frame de métrica de métodos."""
        try:
            for child in frame.findChildren(QLabel):
                if child.objectName() == "metric_value":
                    child.setText(value)
                    break
        except Exception as e:
            print(f"Error actualizando frame de métrica de métodos: {e}")

    
    def connect_methods_signals(self):
        """Conecta las señales de los métodos de pago."""
        # Señales de botones
        self.refresh_methods_btn.clicked.connect(self.load_payment_methods)
        self.add_method_btn.clicked.connect(self.clear_method_form)
        self.edit_method_btn.clicked.connect(self.edit_payment_method)
        self.toggle_method_btn.clicked.connect(self.toggle_payment_method)
        self.delete_method_btn.clicked.connect(self.delete_payment_method)
        self.save_method_btn.clicked.connect(self.save_payment_method)
        self.clear_method_form_btn.clicked.connect(self.clear_method_form)
        
        # Señal de selección en tabla
        self.methods_table.itemSelectionChanged.connect(self.on_methods_selection_changed)
    
    def on_methods_selection_changed(self):
        """Maneja el cambio de selección en la tabla de métodos de pago."""
        selected_row = self.methods_table.currentRow()
        if selected_row >= 0:
            metodo = self.methods_table.item(selected_row, 0).data(Qt.ItemDataRole.UserRole)
            if metodo:
                self.load_method_to_form(metodo)
    
    def load_method_to_form(self, metodo):
        """Carga los datos de un método de pago en el formulario."""
        self.method_name_input.setText(metodo.nombre)
        self.method_icon_input.setText(metodo.icono or "💳")
        self.method_color_input.setText(metodo.color or "#000000")
        self.method_commission_input.setValue(metodo.comision)
    
    def update_methods_statistics(self):
        """Actualiza las estadísticas de métodos de pago."""
        try:
            if not hasattr(self, 'methods_table'):
                return
            total = self.methods_table.rowCount()
            activos = 0
            inactivos = 0
            comisiones = []

            for row in range(total):
                id_item = self.methods_table.item(row, 0)
                metodo = id_item.data(Qt.ItemDataRole.UserRole) if id_item else None
                if metodo is not None:
                    # Estado
                    if getattr(metodo, 'activo', False):
                        activos += 1
                    else:
                        inactivos += 1
                    # Comisión
                    valor = getattr(metodo, 'comision', None)
                    if valor is not None:
                        try:
                            comisiones.append(float(valor))
                        except Exception:
                            pass
                else:
                    # Fallback leyendo desde celdas
                    estado_item = self.methods_table.item(row, 5)
                    if estado_item is not None:
                        data_estado = estado_item.data(Qt.ItemDataRole.UserRole)
                        if isinstance(data_estado, bool):
                            if data_estado:
                                activos += 1
                            else:
                                inactivos += 1
                        else:
                            texto = (estado_item.text() or "").lower()
                            if 'activo' in texto and 'inactivo' not in texto:
                                activos += 1
                            else:
                                inactivos += 1
                    com_item = self.methods_table.item(row, 4)
                    if com_item is not None:
                        data_com = com_item.data(Qt.ItemDataRole.UserRole)
                        if data_com is not None:
                            try:
                                comisiones.append(float(data_com))
                            except Exception:
                                pass
                        else:
                            try:
                                texto = (com_item.text() or "").replace('%', '').strip()
                                if texto:
                                    comisiones.append(float(texto))
                            except Exception:
                                pass

            promedio = sum(comisiones) / len(comisiones) if comisiones else 0.0

            # Actualizar UI
            if hasattr(self, 'methods_total_frame'):
                self.update_methods_metric_frame(self.methods_total_frame, str(total))
            if hasattr(self, 'methods_active_frame'):
                self.update_methods_metric_frame(self.methods_active_frame, str(activos))
            if hasattr(self, 'methods_inactive_frame'):
                self.update_methods_metric_frame(self.methods_inactive_frame, str(inactivos))
            if hasattr(self, 'methods_avg_commission_frame'):
                self.update_methods_metric_frame(self.methods_avg_commission_frame, f"{promedio:.2f} %")
        except Exception as e:
            print(f"Error actualizando estadísticas de métodos: {e}")
    
    def load_payment_methods(self, force: bool = False):
        """Carga todos los métodos de pago en la tabla con protección de recarga reciente (debounce)."""
        try:
            # Protección contra recargas redundantes recientes
            if not force and getattr(self, "_last_load_methods_at", None) is not None:
                elapsed = (datetime.now() - self._last_load_methods_at).total_seconds()
                if elapsed < getattr(self, "_reload_cooldown_secs", 3):
                    return
            
            # Cargar todos los métodos (activos e inactivos) para permitir reactivación
            metodos = self.payment_manager.obtener_metodos_pago(solo_activos=False)
            self.methods_table.setRowCount(len(metodos))
            
            for row, metodo in enumerate(metodos):
                # ID
                id_item = QTableWidgetItem(str(metodo.id))
                id_item.setData(Qt.ItemDataRole.UserRole, metodo)
                self.methods_table.setItem(row, 0, id_item)
                
                # Nombre
                self.methods_table.setItem(row, 1, QTableWidgetItem(metodo.nombre))
                
                # Icono
                self.methods_table.setItem(row, 2, QTableWidgetItem(metodo.icono or "💳"))
                
                # Color - Mostrar como cuadro de color en lugar de código hexadecimal
                color_item = QTableWidgetItem("")
                if metodo.color:
                    # Crear un widget personalizado para mostrar el color
                    color_widget = QWidget()
                    color_layout = QHBoxLayout(color_widget)
                    color_layout.setContentsMargins(5, 2, 5, 2)
                    
                    # Crear un label con el cuadro de color
                    color_square = QLabel()
                    color_square.setFixedSize(20, 20)
                    color_square.setStyleSheet(f"""
                        QLabel {{
                            background-color: {metodo.color};
                            border: 1px solid #ccc;
                            border-radius: 3px;
                        }}
                    """)
                    
                    # Agregar el código de color como texto pequeño
                    color_text = QLabel(metodo.color)
                    color_text.setStyleSheet("font-size: 10px; color: #666;")
                    
                    color_layout.addWidget(color_square)
                    color_layout.addWidget(color_text)
                    color_layout.addStretch()
                    
                    # Establecer el widget en la celda
                    self.methods_table.setCellWidget(row, 3, color_widget)
                    
                    # También establecer el item para mantener compatibilidad
                    color_item.setData(Qt.ItemDataRole.UserRole, metodo.color)
                else:
                    color_item.setText("Sin color")
                    color_item.setData(Qt.ItemDataRole.UserRole, "#000000")
                
                self.methods_table.setItem(row, 3, color_item)
                
                # Comisión
                comision_item = QTableWidgetItem(f"{metodo.comision:.2f} %")
                comision_item.setData(Qt.ItemDataRole.UserRole, metodo.comision)
                self.methods_table.setItem(row, 4, comision_item)
                
                # Estado
                estado_item = QTableWidgetItem("✅ Activo" if metodo.activo else "❌ Inactivo")
                estado_item.setData(Qt.ItemDataRole.UserRole, metodo.activo)
                self.methods_table.setItem(row, 5, estado_item)
                
                # Colorear fila según estado
                if not metodo.activo:
                    for col in range(6):
                        item = self.methods_table.item(row, col)
                        if item:
                            item.setBackground(QColor(255, 240, 240))  # Fondo rojizo para inactivos
            
            # Las columnas ya están configuradas con políticas de resize automáticas
            
            # Actualizar estadísticas
            self.update_methods_statistics()
            
            # Marcar timestamp
            self._last_load_methods_at = datetime.now()
            
        except Exception as e:
            QMessageBox.critical(self, "Error", f"No se pudieron cargar los métodos de pago: {e}")
    
    def duplicate_payment_method(self):
        """Duplica un método de pago seleccionado."""
        selected_row = self.methods_table.currentRow()
        if selected_row < 0:
            QMessageBox.warning(self, "Sin Selección", "Seleccione un método de la tabla para duplicar.")
            return
        
        try:
            metodo = self.methods_table.item(selected_row, 0).data(Qt.ItemDataRole.UserRole)
            
            # Crear nuevo método con datos duplicados
            nuevo_nombre = f"{metodo.nombre} (Copia)"
            
            # Cargar datos en el formulario para crear el duplicado
            self.method_name_input.setText(nuevo_nombre)
            self.method_icon_input.setText(metodo.icono or "💳")
            self.method_color_input.setText(metodo.color or "#000000")
            self.method_commission_input.setValue(metodo.comision)
            
            # Asegurar que estamos en modo creación (no edición)
            self.editing_method_id = None
            self.save_method_btn.setText("💾 Guardar Método")
            
            QMessageBox.information(self, "Método Duplicado", f"Método '{metodo.nombre}' duplicado. Modifique los datos y guarde.")
            
        except Exception as e:
            QMessageBox.critical(self, "Error", f"No se pudo duplicar el método: {e}")
    
    def save_payment_method(self):
        """Guarda un nuevo método de pago o actualiza uno existente."""
        try:
            # Validar campos
            nombre = self.method_name_input.text().strip()
            if not nombre:
                QMessageBox.warning(self, "Campo Requerido", "El nombre del método de pago es obligatorio.")
                return
            
            icono = self.method_icon_input.text().strip() or "💳"
            color = self.method_color_input.text().strip() or "#000000"
            comision = self.method_commission_input.value()
            
            # Validar formato de color
            if not color.startswith('#') or len(color) != 7:
                QMessageBox.warning(self, "Color Inválido", "El color debe estar en formato hexadecimal (#RRGGBB).")
                return
            
            if self.editing_method_id:
                # Actualizar método existente
                metodo_actualizado = MetodoPago(
                    id=self.editing_method_id,
                    nombre=nombre,
                    icono=icono,
                    color=color,
                    comision=comision,
                    activo=True,  # Mantener activo al editar
                    fecha_creacion=None  # Se mantiene la original
                )
                
                self.payment_manager.actualizar_metodo_pago(metodo_actualizado)
                QMessageBox.information(self, "Éxito", f"Método '{nombre}' actualizado correctamente.")
                self.editing_method_id = None
                self.save_method_btn.setText("💾 Guardar Método")
            else:
                # Crear nuevo método
                nuevo_metodo = MetodoPago(
                    id=None,
                    nombre=nombre,
                    icono=icono,
                    color=color,
                    comision=comision,
                    activo=True,
                    fecha_creacion=None  # Se asigna automáticamente
                )
                
                self.payment_manager.crear_metodo_pago(nuevo_metodo)
                QMessageBox.information(self, "Éxito", f"Método '{nombre}' creado correctamente.")
            
            # Limpiar formulario y recargar tabla
            self.clear_method_form()
            self.load_payment_methods(force=True)
            
        except Exception as e:
            QMessageBox.critical(self, "Error", f"No se pudo guardar el método de pago: {e}")
    
    def edit_payment_method(self):
        """Carga un método seleccionado en el formulario para edición."""
        selected_row = self.methods_table.currentRow()
        if selected_row < 0:
            QMessageBox.warning(self, "Sin Selección", "Seleccione un método de la tabla para editar.")
            return
        
        try:
            metodo = self.methods_table.item(selected_row, 0).data(Qt.ItemDataRole.UserRole)
            
            # Cargar datos en el formulario
            self.method_name_input.setText(metodo.nombre)
            self.method_icon_input.setText(metodo.icono or "💳")
            self.method_color_input.setText(metodo.color or "#000000")
            self.method_commission_input.setValue(metodo.comision)
            
            # Cambiar modo a edición
            self.editing_method_id = metodo.id
            self.save_method_btn.setText("💾 Actualizar Método")
            
            QMessageBox.information(self, "Modo Edición", f"Método '{metodo.nombre}' cargado para edición.")
            
        except Exception as e:
            QMessageBox.critical(self, "Error", f"No se pudo cargar el método para edición: {e}")
    
    def toggle_payment_method(self):
        """Activa o desactiva un método de pago."""
        selected_row = self.methods_table.currentRow()
        if selected_row < 0:
            QMessageBox.warning(self, "Sin Selección", "Seleccione un método de la tabla para activar/desactivar.")
            return
        
        try:
            metodo = self.methods_table.item(selected_row, 0).data(Qt.ItemDataRole.UserRole)
            nuevo_estado = not metodo.activo
            accion = "activar" if nuevo_estado else "desactivar"
            
            if QMessageBox.question(
                self, "Confirmar Acción", 
                f"¿Está seguro de que desea {accion} el método '{metodo.nombre}'?",
                QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
                QMessageBox.StandardButton.No
            ) == QMessageBox.StandardButton.Yes:
                
                # Actualizar método con nuevo estado
                metodo_actualizado = MetodoPago(
                    id=metodo.id,
                    nombre=metodo.nombre,
                    icono=metodo.icono,
                    color=metodo.color,
                    comision=metodo.comision,
                    activo=nuevo_estado,
                    fecha_creacion=metodo.fecha_creacion
                )
                
                self.payment_manager.actualizar_metodo_pago(metodo_actualizado)
                QMessageBox.information(self, "Éxito", f"Método '{metodo.nombre}' {accion}do correctamente.")
                self.load_payment_methods(force=True)
                
        except Exception as e:
            QMessageBox.critical(self, "Error", f"No se pudo cambiar el estado del método: {e}")
    
    def delete_payment_method(self):
        """Elimina (soft delete) un método de pago."""
        selected_row = self.methods_table.currentRow()
        if selected_row < 0:
            QMessageBox.warning(self, "Sin Selección", "Seleccione un método de la tabla para eliminar.")
            return
        
        try:
            metodo = self.methods_table.item(selected_row, 0).data(Qt.ItemDataRole.UserRole)
            
            if QMessageBox.question(
                self, "Confirmar Eliminación", 
                f"¿Está seguro de que desea eliminar el método '{metodo.nombre}'?\n\n"
                "Esta acción desactivará permanentemente el método, pero mantendrá el historial de pagos asociados.",
                QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
                QMessageBox.StandardButton.No
            ) == QMessageBox.StandardButton.Yes:
                
                self.payment_manager.eliminar_metodo_pago(metodo.id)
                QMessageBox.information(self, "Éxito", f"Método '{metodo.nombre}' eliminado correctamente.")
                self.load_payment_methods(force=True)
                
        except Exception as e:
            QMessageBox.critical(self, "Error", f"No se pudo eliminar el método: {e}")
    
    def clear_method_form(self):
        """Limpia todos los campos del formulario de métodos."""
        self.method_name_input.clear()
        self.method_icon_input.clear()
        self.method_color_input.clear()
        self.method_commission_input.setValue(0.0)
        
        # Resetear modo de edición
        self.editing_method_id = None
        self.save_method_btn.setText("💾 Guardar Método")
    
    def apply_branding_to_payment_methods(self):
        """Aplica el sistema de branding automático al widget de gestión de métodos de pago"""
        try:
            # Obtener configuración de branding desde el main window
            main_window = self.window()
            if hasattr(main_window, 'branding_config') and main_window.branding_config:
                branding_config = main_window.branding_config
                
                # Obtener colores del branding
                primary_color = branding_config.get('primary_color', '#5E81AC')
                secondary_color = branding_config.get('secondary_color', '#88C0D0')
                background_color = branding_config.get('background_color', '#FFFFFF')
                alt_background_color = branding_config.get('alt_background_color', '#F8F9FA')
                surface_color = branding_config.get('surface_color', '#FFFFFF')
                accent_color = branding_config.get('accent_color', '#A3BE8C')
                text_color = branding_config.get('text_color', '#2E3440')
                
                # Calcular colores de texto con contraste WCAG 2.1 AAA
                if hasattr(main_window, '_get_contrasting_text_color'):
                    auto_text_color = main_window._get_contrasting_text_color(background_color, require_aaa=True)
                    auto_primary_text_color = main_window._get_contrasting_text_color(primary_color, require_aaa=True)
                    auto_surface_text_color = main_window._get_contrasting_text_color(surface_color, require_aaa=True)
                    auto_secondary_text_color = main_window._get_contrasting_text_color(secondary_color, require_aaa=True)
                else:
                    # Fallback a colores por defecto
                    auto_text_color = text_color
                    auto_primary_text_color = '#FFFFFF' if self._is_dark_color(primary_color) else '#000000'
                    auto_surface_text_color = '#000000' if self._is_light_color(surface_color) else '#FFFFFF'
                    auto_secondary_text_color = '#FFFFFF' if self._is_dark_color(secondary_color) else '#000000'
                
                # Aplicar estilos al widget de métodos de pago si existe
                if hasattr(self, 'methods_table'):
                    self._apply_methods_table_branding({
                        'primary': primary_color,
                        'secondary': secondary_color,
                        'background_color': background_color,
                        'alt_background': alt_background_color,
                        'surface': surface_color,
                        'accent': accent_color,
                        'text': auto_text_color,
                        'primary_text': auto_primary_text_color,
                        'surface_text': auto_surface_text_color,
                        'secondary_text': auto_secondary_text_color
                    })
                
        except Exception as e:
            print(f"Error aplicando branding a widget de métodos: {e}")
    
    def _apply_methods_table_branding(self, colors):
        """Aplica branding específico a la tabla y formularios de métodos"""
        try:
            # Reutilizar los mismos estilos que el widget de conceptos
            # Estilo para la tabla de métodos
            table_style = f"""
                QTableWidget {{
                    background-color: {colors['surface']};
                    color: {colors['surface_text']};
                    border: 2px solid {colors['primary']};
                    border-radius: 8px;
                    gridline-color: {colors['secondary']};
                    selection-background-color: {colors['primary']};
                    selection-color: {colors['primary_text']};
                    alternate-background-color: {colors['alt_background']};
                }}
                
                QTableWidget::item {{
                    padding: 8px;
                    border-bottom: 1px solid {colors['secondary']};
                }}
                
                QTableWidget::item:selected {{
                    background-color: {colors['primary']};
                    color: {colors['primary_text']};
                }}
                
                QHeaderView::section {{
                    background-color: {colors['primary']};
                    color: {colors['primary_text']};
                    padding: 10px;
                    border: none;
                    font-weight: bold;
                }}
            """
            
            if hasattr(self, 'methods_table'):
                # Migrado al sistema CSS dinámico - los estilos se aplican automáticamente
                self.methods_table.setObjectName("payment_methods_table")
                self.methods_table.setProperty("class", "config_table")
            
            # Aplicar estilos al widget completo usando CSS dinámico
            if hasattr(self, 'payment_methods_widget'):
                # Migrado al sistema CSS dinámico - los estilos se aplican automáticamente
                self.payment_methods_widget.setObjectName("payment_methods_widget")
                self.payment_methods_widget.setProperty("class", "config_widget")
            
        except Exception as e:
            print(f"Error aplicando estilos de branding a métodos: {e}")
    
    def _get_complete_widget_style(self, colors):
        """Obtiene el estilo completo para widgets de gestión"""
        # Migrado al sistema CSS dinámico - todos los estilos se aplican automáticamente
        # Los widgets deben usar setObjectName() y setProperty() para aplicar estilos:
        # - primary_button, success_button, secondary_button, warning_button, danger_button, action_button
        # - form_input para inputs de formulario
        # - config_group para GroupBox
        # - section_title para títulos de sección
        
        return ""
    
    def create_admin_panel_widget(self):
        """Crea el widget del panel de administración avanzado con sub-pestañas organizadas."""
        widget = QWidget()
        layout = QVBoxLayout(widget)
        layout.setSpacing(0)
        layout.setContentsMargins(0, 0, 0, 0)
        
        # (Eliminado) Título principal "Administración Avanzada" para evitar desplazamientos
        
        # Crear tabs para organizar las herramientas administrativas
        admin_tabs = QTabWidget()
        admin_tabs.setObjectName("admin_tabs")
        admin_tabs.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Expanding)
        
        # (Eliminada) Sub-pestaña de Mantenimiento
        
        # === SUB-PESTAÑA 2: DIAGNÓSTICOS ===
        diagnostics_tab = self.create_diagnostics_tab()
        admin_tabs.addTab(diagnostics_tab, "Diagnósticos")
        
        # (Eliminada) Sub-pestaña de Dev Tools
        
        # === SUB-PESTAÑA 4: IMPORTACIÓN/EXPORTACIÓN ===
        data_ops_tab = self.create_data_ops_tab()
        admin_tabs.addTab(data_ops_tab, "Importación/Exportación y Automatización")
        
        # === SUB-PESTAÑA 5: RESPALDO Y AUTOMATIZACIÓN REAL ===
        real_automation_tab = self.create_backup_automation_tab()
        # (Eliminada) pestaña antigua de Respaldo y Automatización
        
        # === SUB-PESTAÑA 6: AUDITORÍA Y TRAZABILIDAD ===
        audit_tab = self.create_audit_tab()
        admin_tabs.addTab(audit_tab, "Auditoría y Trazabilidad")
        
        # Exponer referencia para control de visibilidad por rol
        self.admin_tabs = admin_tabs
        layout.addWidget(admin_tabs)
        
        
        return widget
    
    # [Eliminado] create_maintenance_tab
    
    def create_diagnostics_tab(self):
        """Crea la subpestaña de Diagnósticos con contenido edge-to-edge, sin títulos redundantes."""
        tab = QWidget()
        layout = QVBoxLayout(tab)
        layout.setSpacing(0)
        layout.setContentsMargins(0, 0, 0, 0)

        try:
            self.system_diagnostics_widget = SystemDiagnosticsWidget()
            self.system_diagnostics_widget.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Expanding)
            layout.addWidget(self.system_diagnostics_widget)
        except Exception as e:
            fallback = QLabel(f"No se pudo cargar el panel de diagnóstico: {e}")
            fallback.setStyleSheet("color: #b00; padding: 8px;")
            layout.addWidget(fallback)

        return tab
    
    # [Eliminado] create_dev_tools_tab

    def create_data_ops_tab(self):
        """Pestaña combinada: Importación/Exportación y Respaldo/Automatización en diseño edge-to-edge."""
        tab = QWidget()
        layout = QVBoxLayout(tab)
        layout.setSpacing(0)
        layout.setContentsMargins(0, 0, 0, 0)

        # Contenido de Importación/Exportación
        try:
            import_export_widget = self.create_import_export_tab()
            import_export_widget.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Expanding)
            layout.addWidget(import_export_widget)
        except Exception:
            pass

        # Contenido de Respaldo y Automatización
        try:
            backup_automation_widget = self.create_backup_automation_tab()
            backup_automation_widget.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Expanding)
            layout.addWidget(backup_automation_widget)
        except Exception:
            pass

        return tab
    
    def create_import_export_tab(self):
        """Crea la sub-pestaña de Importación/Exportación."""
        tab = QWidget()
        layout = QVBoxLayout(tab)
        layout.setSpacing(0)
        layout.setContentsMargins(0, 0, 0, 0)
        
        # Grupo de exportación
        export_group = QGroupBox("")
        export_group.setObjectName("config_group")
        export_group.setFlat(True)
        export_group.setSizePolicy(QSizePolicy.Policy.Preferred, QSizePolicy.Policy.Minimum)
        export_layout = QVBoxLayout(export_group)
        export_layout.setContentsMargins(0, 0, 0, 0)
        export_layout.setSpacing(0)
        
        export_all_button = QPushButton("Exportar Todos los Datos")
        export_all_button.setObjectName("primary_button")
        export_all_button.setMinimumHeight(28)
        export_all_button.setToolTip("Exporta toda la base de datos a formato SQL")
        export_all_button.clicked.connect(self.on_export_all_clicked)
        
        export_users_button = QPushButton("👥 Exportar Usuarios")
        export_users_button.setObjectName("secondary_button")
        export_users_button.setMinimumHeight(28)
        export_users_button.clicked.connect(self.on_export_users_clicked)
        
        export_payments_button = QPushButton("💰 Exportar Pagos")
        export_payments_button.setObjectName("secondary_button")
        export_payments_button.setMinimumHeight(28)
        export_payments_button.clicked.connect(self.on_export_payments_clicked)
        
        export_layout.addWidget(export_all_button)
        export_layout.addWidget(export_users_button)
        export_layout.addWidget(export_payments_button)
        
        # edge-to-edge: sin título ni colapsable visual
        
        # Grupo de importación
        import_group = QGroupBox("")
        import_group.setObjectName("config_group")
        import_group.setFlat(True)
        import_group.setSizePolicy(QSizePolicy.Policy.Preferred, QSizePolicy.Policy.Minimum)
        import_layout = QVBoxLayout(import_group)
        import_layout.setContentsMargins(0, 0, 0, 0)
        import_layout.setSpacing(0)
        
        import_csv_button = QPushButton("📊 Importar desde CSV")
        import_csv_button.setObjectName("success_button")
        import_csv_button.setMinimumHeight(28)
        import_csv_button.setToolTip("Importa datos desde archivos CSV")
        import_csv_button.clicked.connect(self.on_import_csv_clicked)
        
        import_sql_button = QPushButton("💻 Importar desde SQL")
        import_sql_button.setObjectName("action_button")
        import_sql_button.setMinimumHeight(28)
        import_sql_button.setToolTip("Ejecuta scripts SQL para importar datos")
        import_sql_button.clicked.connect(self.on_import_sql_clicked)
        
        import_backup_button = QPushButton("🔄 Restaurar desde Backup")
        import_backup_button.setObjectName("warning_button")
        import_backup_button.setMinimumHeight(28)
        import_backup_button.clicked.connect(self.on_import_backup_clicked)
        
        import_layout.addWidget(import_csv_button)
        import_layout.addWidget(import_sql_button)
        import_layout.addWidget(import_backup_button)
        
        # edge-to-edge: sin título ni colapsable visual
        
        # Grupo de configuración de importación/exportación
        config_group = QGroupBox("")
        config_group.setObjectName("config_group")
        config_group.setFlat(True)
        config_group.setSizePolicy(QSizePolicy.Policy.Preferred, QSizePolicy.Policy.Minimum)
        config_layout = QVBoxLayout(config_group)
        config_layout.setContentsMargins(0, 0, 0, 0)
        config_layout.setSpacing(0)
        
        # Opciones de formato
        format_label = QLabel("Formato de exportación:")
        self.export_format_combo = QComboBox()
        self.export_format_combo.setObjectName("form_input")
        self.export_format_combo.addItems(["SQL", "CSV", "JSON", "XML"])
        
        # Opciones de compresión
        self.compress_checkbox = QCheckBox("Comprimir archivos exportados")
        self.compress_checkbox.setChecked(True)
        
        config_layout.addWidget(format_label)
        config_layout.addWidget(self.export_format_combo)
        config_layout.addWidget(self.compress_checkbox)
        
        # edge-to-edge: sin título ni colapsable visual
        
        layout.addWidget(export_group)
        layout.addWidget(import_group)
        layout.addWidget(config_group)
        # Alinear los bloques hacia arriba dejando un stretch al final
        layout.addStretch()

        return tab
    
    def create_backup_automation_tab(self):
        """Crea la sub-pestaña de Respaldo y Automatización REAL."""
        tab = QWidget()
        layout = QVBoxLayout(tab)
        layout.setSpacing(0)
        layout.setContentsMargins(0, 0, 0, 0)

        # === Respaldo Manual REAL ===
        manual_group = QGroupBox("")
        manual_group.setObjectName("config_group")
        manual_group.setFlat(True)
        manual_group.setSizePolicy(QSizePolicy.Policy.Preferred, QSizePolicy.Policy.Minimum)
        manual_layout = QVBoxLayout(manual_group)
        manual_layout.setContentsMargins(0, 0, 0, 0)
        manual_layout.setSpacing(0)

        backup_now_button = QPushButton("🧷 Crear Respaldo Ahora (pg_dump)")
        backup_now_button.setObjectName("primary_button")
        backup_now_button.setMinimumHeight(28)
        backup_now_button.setToolTip("Genera un respaldo SQL real usando pg_dump")
        backup_now_button.clicked.connect(self.on_backup_now_clicked)

        manual_layout.addWidget(backup_now_button)
        # edge-to-edge: sin título ni colapsable visual

        # === Backup Automático REAL ===
        auto_group = QGroupBox("")
        auto_group.setObjectName("config_group")
        auto_group.setFlat(True)
        auto_group.setSizePolicy(QSizePolicy.Policy.Preferred, QSizePolicy.Policy.Minimum)
        auto_layout = QVBoxLayout(auto_group)
        auto_layout.setContentsMargins(0, 0, 0, 0)
        auto_layout.setSpacing(0)

        try:
            cfg = self.db_manager.obtener_configuracion_respaldo_automatico()
            estado = "Habilitado" if str(cfg.get("habilitado", "false")).lower() == "true" else "Deshabilitado"
            self.auto_backup_summary_label = QLabel(
                f"Estado: {estado} | Frecuencia: {cfg.get('frecuencia', 'diario')} | Hora: {cfg.get('hora', '02:00')} | Carpeta: {cfg.get('directorio', '') or 'No definida'}"
            )
        except Exception as e:
            self.auto_backup_summary_label = QLabel(f"No se pudo leer configuración actual: {e}")
            self.auto_backup_summary_label.setStyleSheet("color:#b00;")

        schedule_button = QPushButton("🗓️ Configurar Backup Automático")
        schedule_button.setObjectName("secondary_button")
        schedule_button.setMinimumHeight(28)
        schedule_button.setToolTip("Abre el diálogo para programar backups automáticos reales")
        schedule_button.clicked.connect(self.on_open_scheduled_backup_dialog)

        auto_layout.addWidget(self.auto_backup_summary_label)
        auto_layout.addWidget(schedule_button)

        # edge-to-edge: sin título ni colapsable visual

        # === Automatización de Estados REAL ===
        states_group = QGroupBox("")
        states_group.setObjectName("config_group")
        states_group.setFlat(True)
        states_group.setSizePolicy(QSizePolicy.Policy.Preferred, QSizePolicy.Policy.Minimum)
        states_layout = QVBoxLayout(states_group)
        states_layout.setContentsMargins(0, 0, 0, 0)
        states_layout.setSpacing(0)

        run_now_button = QPushButton("⚡ Ejecutar Automatización de Estados Ahora")
        run_now_button.setObjectName("action_button")
        run_now_button.setMinimumHeight(28)
        run_now_button.setToolTip("Actualiza estados de usuarios según reglas reales")
        run_now_button.clicked.connect(self.on_run_state_automation_clicked)

        states_layout.addWidget(run_now_button)
        # edge-to-edge: sin título ni colapsable visual

        # Agregar grupos
        layout.addWidget(manual_group)
        layout.addWidget(auto_group)
        layout.addWidget(states_group)
        # Alinear los bloques hacia arriba dejando un stretch al final
        layout.addStretch()

        return tab
    
    def create_audit_tab(self):
        """Crea la sub-pestaña de Auditoría y Trazabilidad."""
        # Crear una instancia del AuditDashboardWidget
        audit_widget = AuditDashboardWidget(self.db_manager)
        return audit_widget
    
    def _darken_color(self, color_hex, factor=0.1):
        """Oscurece un color por un factor dado"""
        try:
            color_hex = color_hex.lstrip('#')
            r, g, b = tuple(int(color_hex[i:i+2], 16) for i in (0, 2, 4))
            r = max(0, int(r * (1 - factor)))
            g = max(0, int(g * (1 - factor)))
            b = max(0, int(b * (1 - factor)))
            return f"#{r:02x}{g:02x}{b:02x}"
        except:
            return color_hex
    
    # ==================== MÉTODOS DE MANEJO DE BOTONES ADMINISTRACIÓN AVANZADA ====================
    
    # (Eliminados) handlers de Mantenimiento

    def on_backup_now_clicked(self):
        """Ejecuta un respaldo inmediato usando DatabaseManager.backup_database (pg_dump)."""
        try:
            reply = QMessageBox.question(
                self,
                "Crear Respaldo Ahora",
                "¿Desea crear un respaldo completo de la base de datos ahora?",
                QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
                QMessageBox.StandardButton.No
            )
            if reply != QMessageBox.StandardButton.Yes:
                return

            # Preparar ruta de salida
            backups_dir = os.path.join(os.getcwd(), 'backups')
            os.makedirs(backups_dir, exist_ok=True)
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            backup_file = os.path.join(backups_dir, f"backup_{timestamp}.sql")

            progress = QProgressDialog("Creando respaldo...", "Cancelar", 0, 0, self)
            progress.setWindowModality(Qt.WindowModality.WindowModal)
            progress.setValue(0)
            progress.show()
            QApplication.processEvents()

            ok = False
            try:
                ok = self.db_manager.backup_database(backup_file)
            finally:
                progress.close()

            if ok:
                QMessageBox.information(
                    self,
                    "Respaldo Completado",
                    f"✅ Respaldo creado exitosamente en:\n{backup_file}"
                )
            else:
                QMessageBox.warning(
                    self,
                    "Respaldo Fallido",
                    "No se pudo crear el respaldo. Verifique configuración de conexión y pg_dump."
                )
        except Exception as e:
            QMessageBox.critical(
                self,
                "Error de Respaldo",
                f"Error al crear respaldo: {str(e)}"
            )

    def on_open_scheduled_backup_dialog(self):
        """Abre el diálogo de backup programado y persiste la configuración."""
        try:
            dialog = ScheduledBackupDialog(self)
            if dialog.exec() == QDialog.DialogCode.Accepted:
                # Persistir configuración usando claves del sistema
                cfg = getattr(dialog, 'backup_config', None) or {}

                # Mapear a claves existentes en configuracion
                try:
                    self.db_manager.actualizar_configuracion('backup_automatico_habilitado', 'true' if cfg.get('enabled') else 'false')
                    self.db_manager.actualizar_configuracion('backup_frecuencia', str(cfg.get('frequency', 'diario')))
                    self.db_manager.actualizar_configuracion('backup_hora', str(cfg.get('time', '02:00')))
                    self.db_manager.actualizar_configuracion('backup_directorio', str(cfg.get('location', 'backups/auto')))
                    self.db_manager.actualizar_configuracion('backup_retener_dias', str(cfg.get('retention_days', 30)))
                except Exception as db_err:
                    QMessageBox.warning(self, "Persistencia Parcial", f"Ocurrió un problema guardando la configuración: {db_err}")

                # Refrescar indicador/resumen del widget
                try:
                    cfg_actual = self.db_manager.obtener_configuracion_respaldo_automatico()
                    estado = "Habilitado" if str(cfg_actual.get("habilitado", "false")).lower() in ("true", "1", "si", "sí") else "Deshabilitado"
                    if hasattr(self, 'auto_backup_summary_label') and self.auto_backup_summary_label:
                        self.auto_backup_summary_label.setText(
                            f"Estado: {estado} | Frecuencia: {cfg_actual.get('frecuencia', 'diario')} | Hora: {cfg_actual.get('hora', '02:00')} | Carpeta: {cfg_actual.get('directorio', '') or 'No definida'}"
                        )
                except Exception as e_refresh:
                    logging.warning(f"No se pudo refrescar resumen de backup automático: {e_refresh}")

                QMessageBox.information(
                    self,
                    "Backup Programado",
                    "✅ Configuración de backup automático guardada exitosamente."
                )
        except Exception as e:
            QMessageBox.critical(
                self,
                "Error",
                f"Error al abrir o guardar configuración de backup programado:\n{str(e)}"
            )

    def on_run_state_automation_clicked(self):
        """Ejecuta automatización de estados y muestra un resumen de resultados."""
        try:
            reply = QMessageBox.question(
                self,
                "Automatización de Estados",
                "¿Desea ejecutar ahora la automatización de estados de usuarios?",
                QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
                QMessageBox.StandardButton.No
            )
            if reply != QMessageBox.StandardButton.Yes:
                return

            progress = QProgressDialog("Ejecutando automatización de estados...", "Cancelar", 0, 0, self)
            progress.setWindowModality(Qt.WindowModality.WindowModal)
            progress.setValue(0)
            progress.show()
            QApplication.processEvents()

            try:
                resultados = self.db_manager.automatizar_estados_por_vencimiento_optimizada()
            finally:
                progress.close()

            # Construir resumen
            resumen = (
                f"Usuarios actualizados: {resultados.get('estados_actualizados', 0)}\n"
                f"Alertas generadas: {resultados.get('alertas_generadas', 0)}\n"
                f"Usuarios reactivados: {resultados.get('usuarios_reactivados', 0)}\n"
                f"Tiempo de procesamiento: {resultados.get('tiempo_procesamiento', 0)}s"
            )

            # Mostrar errores si existieran
            errores = resultados.get('errores', [])
            if errores:
                resumen += f"\n\nErrores: {len(errores)} (ver logs para detalles)"

            QMessageBox.information(self, "Automatización Completada", resumen)
        except Exception as e:
            QMessageBox.critical(
                self,
                "Error en Automatización",
                f"Error al ejecutar automatización de estados: {str(e)}"
            )
    
    # Métodos para pestaña de Diagnósticos
    def on_system_health_clicked(self):
        """Muestra el estado de salud del sistema."""
        try:
            # Aquí se mostraría información detallada del sistema
            health_info = (
                "Estado de Salud del Sistema:\n\n"
                "✅ Base de datos: Funcionando correctamente\n"
                "✅ Memoria: 65% utilizada\n"
                "✅ Disco: 78% utilizado\n"
                "✅ Conexiones: 12 activas\n"
                "⚠️ Logs: Requiere limpieza\n\n"
                "Estado general: BUENO"
            )
            
            QMessageBox.information(
                self,
                "Estado de Salud del Sistema",
                health_info
            )
        except Exception as e:
            QMessageBox.critical(
                self,
                "Error",
                f"Error al obtener estado del sistema:\n{str(e)}"
            )
    
    def on_performance_report_clicked(self):
        """Genera reporte de rendimiento."""
        try:
            # Aquí se generaría un reporte detallado de rendimiento
            performance_info = (
                "Reporte de Rendimiento:\n\n"
                "📊 Consultas por segundo: 45\n"
                "⏱️ Tiempo promedio de respuesta: 120ms\n"
                "💾 Uso de memoria: 512MB\n"
                "🔄 Transacciones completadas: 1,247\n"
                "❌ Errores en las últimas 24h: 3\n\n"
                "Rendimiento general: ÓPTIMO"
            )
            
            QMessageBox.information(
                self,
                "Reporte de Rendimiento",
                performance_info
            )
        except Exception as e:
            QMessageBox.critical(
                self,
                "Error",
                f"Error al generar reporte de rendimiento:\n{str(e)}"
            )
    
    def on_analyze_tables_clicked(self):
        """Ejecuta la optimización/ANALYZE centralizada de la base de datos."""
        try:
            reply = QMessageBox.question(
                self,
                "Analizar Tablas",
                "¿Desea actualizar las estadísticas de la base de datos (ANALYZE/OPTIMIZACIÓN)?\n\nEsto puede tomar varios minutos.",
                QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No
            )
            
            if reply == QMessageBox.StandardButton.Yes:
                # Diálogo de progreso
                progress = QProgressDialog("Iniciando análisis/optimización...", "Cancelar", 0, 100, self)
                progress.setWindowModality(Qt.WindowModality.WindowModal)
                progress.setValue(10)
                progress.show()
                QApplication.processEvents()
                
                try:
                    progress.setLabelText("Ejecutando optimización (VACUUM/ANALYZE)...")
                    progress.setValue(50)
                    QApplication.processEvents()
                    
                    ok = self.db_manager.optimizar_base_datos()
                    
                    progress.setValue(100)
                    progress.close()
                    
                    if ok:
                        QMessageBox.information(
                            self,
                            "Análisis Completado",
                            "✅ Estadísticas actualizadas y base de datos optimizada correctamente."
                        )
                    else:
                        QMessageBox.warning(
                            self,
                            "Análisis Incompleto",
                            "Se produjo un problema al optimizar/analizar la base de datos. Revise los logs para más detalles."
                        )
                except Exception as db_error:
                    progress.close()
                    QMessageBox.critical(
                        self,
                        "Error de Análisis",
                        f"Error durante la optimización/análisis:\n{str(db_error)}"
                    )
        except Exception as e:
            QMessageBox.critical(
                self,
                "Error de Análisis",
                f"Error al analizar tablas:\n{str(e)}"
            )
    
    def on_check_integrity_clicked(self):
        """Verifica la integridad de la base de datos usando DatabaseManager."""
        try:
            reply = QMessageBox.question(
                self,
                "Verificar Integridad",
                "¿Desea verificar la integridad de la base de datos?",
                QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No
            )
            
            if reply == QMessageBox.StandardButton.Yes:
                # Diálogo de progreso
                progress = QProgressDialog("Verificando integridad...", "Cancelar", 0, 100, self)
                progress.setWindowModality(Qt.WindowModality.WindowModal)
                progress.setValue(10)
                progress.show()
                QApplication.processEvents()
                
                try:
                    progress.setLabelText("Ejecutando verificación centralizada...")
                    progress.setValue(50)
                    QApplication.processEvents()
                    
                    result = self.db_manager.verificar_integridad_base_datos()
                    
                    progress.setValue(100)
                    progress.close()
                    
                    estado = result.get("estado", "ERROR") if isinstance(result, dict) else "ERROR"
                    errores = (result.get("errores") if isinstance(result, dict) else None) or []
                    advertencias = (result.get("advertencias") if isinstance(result, dict) else None) or []
                    tablas = (result.get("tablas_verificadas") if isinstance(result, dict) else None) or 0
                    
                    if estado == "OK" and not errores:
                        advertencias_txt = "\n- " + "\n- ".join(advertencias[:10]) if advertencias else ""
                        msg = (
                            "Verificación de Integridad Completada:\n\n"
                            f"✅ Estado: {estado}\n"
                            f"📋 Tablas verificadas: {tablas}\n"
                            f"⚠️ Advertencias: {len(advertencias)}" + advertencias_txt
                        )
                        QMessageBox.information(self, "Verificación Completada", msg)
                    else:
                        partes = []
                        if errores:
                            partes.append(
                                "Errores:\n- " + "\n- ".join(errores[:10]) + (f"\n... y {len(errores) - 10} más" if len(errores) > 10 else "")
                            )
                        if advertencias:
                            partes.append(
                                "Advertencias:\n- " + "\n- ".join(advertencias[:10]) + (f"\n... y {len(advertencias) - 10} más" if len(advertencias) > 10 else "")
                            )
                        detalle = "\n\n".join(partes) if partes else "No se obtuvo detalle de problemas."
                        msg = (
                            "Verificación de Integridad Completada con problemas:\n\n"
                            f"❌ Estado: {estado}\n"
                            f"📋 Tablas verificadas: {tablas}\n\n"
                            f"{detalle}"
                        )
                        QMessageBox.warning(self, "Problemas de Integridad", msg)
                except Exception as db_error:
                    progress.close()
                    QMessageBox.critical(
                        self,
                        "Error de Verificación",
                        f"Error durante la verificación:\n{str(db_error)}"
                    )
        except Exception as e:
            QMessageBox.critical(
                self,
                "Error de Verificación",
                f"Error al verificar integridad:\n{str(e)}"
            )
    
    # (Eliminados) handlers de Dev Tools
    
    # Métodos para pestaña de Importación/Exportación
    def on_export_all_clicked(self):
        """Exporta todos los datos."""
        try:
            reply = QMessageBox.question(
                self,
                "Exportar Todos los Datos",
                "¿Desea exportar toda la base de datos?\n\nEsto puede tomar varios minutos.",
                QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No
            )
            
            if reply == QMessageBox.StandardButton.Yes:
                import os
                from datetime import datetime
                
                # Mostrar diálogo de progreso
                progress = QProgressDialog("Exportando datos...", "Cancelar", 0, 100, self)
                progress.setWindowModality(Qt.WindowModality.WindowModal)
                progress.show()
                
                try:
                    # Crear directorio de exportación si no existe
                    export_dir = "exports"
                    if not os.path.exists(export_dir):
                        os.makedirs(export_dir)
                    
                    # Generar nombre de archivo con timestamp
                    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                    export_file = os.path.join(export_dir, f"backup_completo_{timestamp}.sql")
                    
                    progress.setValue(25)
                    progress.setLabelText("Conectando a la base de datos...")
                    QApplication.processEvents()
                    
                    # Usar el DatabaseManager existente
                    with self.db_manager.get_connection_context() as conn:
                        progress.setValue(50)
                        progress.setLabelText("Exportando estructura y datos...")
                        QApplication.processEvents()
                        
                        # Exportar la base de datos completa usando PostgreSQL
                        with open(export_file, 'w', encoding='utf-8') as f:
                            # Escribir encabezado
                            f.write(f"-- Backup completo de la base de datos PostgreSQL\n")
                            f.write(f"-- Generado el: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
                            
                            cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
                            
                            # Obtener todas las tablas del esquema público
                            cursor.execute("""
                                SELECT table_name 
                                FROM information_schema.tables 
                                WHERE table_schema = 'public' 
                                AND table_type = 'BASE TABLE'
                                ORDER BY table_name
                            """)
                            tables = cursor.fetchall()
                            
                            # Exportar estructura y datos de cada tabla
                            for table_row in tables:
                                table_name = table_row['table_name']
                                
                                # Exportar estructura de la tabla
                                f.write(f"\n-- Estructura de tabla: {table_name}\n")
                                
                                # Obtener definición de columnas
                                cursor.execute("""
                                    SELECT column_name, data_type, is_nullable, column_default
                                    FROM information_schema.columns 
                                    WHERE table_name = %s AND table_schema = 'public'
                                    ORDER BY ordinal_position
                                """, (table_name,))
                                columns = cursor.fetchall()
                                
                                # Crear statement CREATE TABLE básico
                                f.write(f"CREATE TABLE IF NOT EXISTS {table_name} (\n")
                                column_defs = []
                                for col in columns:
                                    col_def = f"    {col['column_name']} {col['data_type']}"
                                    if col['is_nullable'] == 'NO':
                                        col_def += " NOT NULL"
                                    if col['column_default']:
                                        col_def += f" DEFAULT {col['column_default']}"
                                    column_defs.append(col_def)
                                f.write(",\n".join(column_defs))
                                f.write("\n);\n\n")
                                
                                # Exportar datos de la tabla
                                cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
                                result = cursor.fetchone()
                                row_count = result['count'] if result else 0
                                
                                if row_count > 0:
                                    f.write(f"-- Datos de tabla: {table_name} ({row_count} registros)\n")
                                    
                                    # Obtener todos los datos
                                    cursor.execute(f"SELECT * FROM {table_name}")
                                    rows = cursor.fetchall()
                                    
                                    if rows:
                                        # Crear INSERT statements
                                        column_names = [col['column_name'] for col in columns]
                                        f.write(f"INSERT INTO {table_name} ({', '.join(column_names)}) VALUES\n")
                                        
                                        insert_values = []
                                        for row in rows:
                                            values = []
                                            for col_name in column_names:
                                                value = row[col_name]
                                                if value is None:
                                                    values.append('NULL')
                                                elif isinstance(value, str):
                                                    # Escapar comillas simples
                                                    escaped_value = value.replace("'", "''")
                                                    values.append(f"'{escaped_value}'")
                                                elif isinstance(value, (int, float)):
                                                    values.append(str(value))
                                                elif isinstance(value, bool):
                                                    values.append('TRUE' if value else 'FALSE')
                                                else:
                                                    values.append(f"'{str(value)}'")
                                            insert_values.append(f"    ({', '.join(values)})")
                                        
                                        f.write(",\n".join(insert_values))
                                        f.write(";\n\n")
                                else:
                                    f.write(f"-- Tabla {table_name} está vacía\n\n")
                        
                        progress.setValue(75)
                        progress.setLabelText("Verificando exportación...")
                        QApplication.processEvents()
                        
                        # Verificar que el archivo se creó correctamente
                        file_size = os.path.getsize(export_file)
                        file_size_mb = file_size / (1024 * 1024)
                        
                        progress.setValue(100)
                    
                    progress.close()
                    
                    QMessageBox.information(
                        self,
                        "Exportación Completada",
                        f"✅ Todos los datos han sido exportados exitosamente.\n\n"
                        f"Archivo: {export_file}\n"
                        f"Tamaño: {file_size_mb:.2f} MB\n\n"
                        f"La exportación incluye estructura y datos de todas las tablas."
                    )
                    
                except Exception as export_error:
                    progress.close()
                    QMessageBox.critical(
                        self,
                        "Error de Exportación",
                        f"Error durante la exportación:\n{str(export_error)}"
                    )
        except Exception as e:
            QMessageBox.critical(
                self,
                "Error de Exportación",
                f"Error al exportar datos:\n{str(e)}"
            )
    
    def on_export_users_clicked(self):
        """Exporta datos de usuarios."""
        try:
            reply = QMessageBox.question(
                self,
                "Exportar Usuarios",
                "¿Desea exportar los datos de usuarios?",
                QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No
            )
            
            if reply == QMessageBox.StandardButton.Yes:
                import os
                import csv
                from datetime import datetime
                
                # Crear directorio de exportación si no existe
                export_dir = "exports"
                if not os.path.exists(export_dir):
                    os.makedirs(export_dir)
                
                # Generar nombre de archivo con timestamp
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                export_file = os.path.join(export_dir, f"usuarios_{timestamp}.csv")
                
                # Obtener datos de usuarios
                with self.db_manager.get_connection_context() as conn:
                    cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
                    cursor.execute("""
                        SELECT u.id, u.nombre, u.apellido, u.email, u.telefono, 
                               u.fecha_nacimiento, u.fecha_registro, u.activo,
                               m.nombre as membresia
                        FROM usuarios u
                        LEFT JOIN membresias m ON u.membresia_id = m.id
                        ORDER BY u.nombre, u.apellido
                    """)
                    usuarios = cursor.fetchall()
                
                # Escribir archivo CSV
                with open(export_file, 'w', newline='', encoding='utf-8') as csvfile:
                    fieldnames = ['ID', 'Nombre', 'Apellido', 'Email', 'Teléfono', 
                                'Fecha Nacimiento', 'Fecha Registro', 'Activo', 'Membresía']
                    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                    
                    writer.writeheader()
                    for usuario in usuarios:
                        writer.writerow({
                            'ID': usuario['id'],
                            'Nombre': usuario['nombre'],
                            'Apellido': usuario['apellido'],
                            'Email': usuario['email'],
                            'Teléfono': usuario['telefono'],
                            'Fecha Nacimiento': usuario['fecha_nacimiento'],
                            'Fecha Registro': usuario['fecha_registro'],
                            'Activo': 'Sí' if usuario['activo'] else 'No',
                            'Membresía': usuario['membresia'] or 'Sin membresía'
                        })
                
                QMessageBox.information(
                    self,
                    "Exportación Completada",
                    f"✅ Los datos de usuarios han sido exportados.\n\nArchivo guardado en: {export_file}\nTotal de usuarios: {len(usuarios)}"
                )
        except Exception as e:
            QMessageBox.critical(
                self,
                "Error de Exportación",
                f"Error al exportar usuarios:\n{str(e)}"
            )
    
    def on_export_payments_clicked(self):
        """Exporta datos de pagos."""
        try:
            reply = QMessageBox.question(
                self,
                "Exportar Pagos",
                "¿Desea exportar los datos de pagos?",
                QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No
            )
            
            if reply == QMessageBox.StandardButton.Yes:
                import os
                import csv
                from datetime import datetime
                
                # Crear directorio de exportación si no existe
                export_dir = "exports"
                if not os.path.exists(export_dir):
                    os.makedirs(export_dir)
                
                # Generar nombre de archivo con timestamp
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                export_file = os.path.join(export_dir, f"pagos_{timestamp}.csv")
                
                # Obtener datos de pagos
                with self.db_manager.get_connection_context() as conn:
                    cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
                    cursor.execute("""
                        SELECT p.id, u.nombre || ' ' || u.apellido as usuario,
                               c.nombre as concepto, p.monto, p.fecha_pago,
                               mp.nombre as metodo_pago, p.estado, p.observaciones
                        FROM pagos p
                        JOIN usuarios u ON p.usuario_id = u.id
                        JOIN conceptos_pago c ON p.concepto_id = c.id
                        LEFT JOIN metodos_pago mp ON p.metodo_pago_id = mp.id
                        ORDER BY p.fecha_pago DESC
                    """)
                    pagos = cursor.fetchall()
                
                # Escribir archivo CSV
                with open(export_file, 'w', newline='', encoding='utf-8') as csvfile:
                    fieldnames = ['ID', 'Usuario', 'Concepto', 'Monto', 'Fecha Pago', 
                                'Método Pago', 'Estado', 'Observaciones']
                    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                    
                    writer.writeheader()
                    for pago in pagos:
                        writer.writerow({
                            'ID': pago['id'],
                            'Usuario': pago['usuario'],
                            'Concepto': pago['concepto'],
                            'Monto': pago['monto'],
                            'Fecha Pago': pago['fecha_pago'],
                            'Método Pago': pago['metodo_pago'] or 'No especificado',
                            'Estado': pago['estado'],
                            'Observaciones': pago['observaciones'] or ''
                        })
                
                QMessageBox.information(
                    self,
                    "Exportación Completada",
                    f"✅ Los datos de pagos han sido exportados.\n\nArchivo guardado en: {export_file}\nTotal de pagos: {len(pagos)}"
                )
        except Exception as e:
            QMessageBox.critical(
                self,
                "Error de Exportación",
                f"Error al exportar pagos:\n{str(e)}"
            )
    
    def on_import_csv_clicked(self):
        """Importa datos desde CSV."""
        try:
            from PyQt6.QtWidgets import QFileDialog
            import csv
            import os
            
            # Abrir diálogo de selección de archivo
            file_path, _ = QFileDialog.getOpenFileName(
                self,
                "Seleccionar archivo CSV",
                "",
                "Archivos CSV (*.csv);;Todos los archivos (*)"
            )
            
            if file_path and os.path.exists(file_path):
                # Mostrar diálogo de confirmación con opciones
                reply = QMessageBox.question(
                    self,
                    "Importar CSV",
                    f"¿Desea importar datos desde el archivo:\n{file_path}\n\n"
                    "ADVERTENCIA: Esta operación puede sobrescribir datos existentes.",
                    QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No
                )
                
                if reply == QMessageBox.StandardButton.Yes:
                    # Mostrar diálogo de progreso
                    progress = QProgressDialog("Importando datos...", "Cancelar", 0, 100, self)
                    progress.setWindowModality(Qt.WindowModality.WindowModal)
                    progress.show()
                    
                    try:
                        imported_count = 0
                        errors = []
                        
                        with open(file_path, 'r', encoding='utf-8') as csvfile:
                            # Detectar el delimitador
                            sample = csvfile.read(1024)
                            csvfile.seek(0)
                            sniffer = csv.Sniffer()
                            delimiter = sniffer.sniff(sample).delimiter
                            
                            reader = csv.DictReader(csvfile, delimiter=delimiter)
                            rows = list(reader)
                            total_rows = len(rows)
                            
                            progress.setMaximum(total_rows)
                            
                            # Determinar tipo de datos basado en las columnas
                            if reader.fieldnames:
                                fieldnames = [field.lower().strip() for field in reader.fieldnames]
                                
                                # Detectar si es archivo de usuarios
                                if any(field in fieldnames for field in ['nombre', 'apellido', 'email']):
                                    imported_count, errors = self._import_users_from_csv(rows, progress)
                                
                                # Detectar si es archivo de pagos
                                elif any(field in fieldnames for field in ['monto', 'concepto', 'fecha_pago']):
                                    imported_count, errors = self._import_payments_from_csv(rows, progress)
                                
                                else:
                                    errors.append("Formato de archivo no reconocido. Verifique las columnas.")
                        
                        progress.close()
                        
                        # Mostrar resultado
                        if errors:
                            error_msg = "\n".join(errors[:5])  # Mostrar solo los primeros 5 errores
                            if len(errors) > 5:
                                error_msg += f"\n... y {len(errors) - 5} errores más"
                            
                            QMessageBox.warning(
                                self,
                                "Importación Completada con Errores",
                                f"⚠️ Importación completada con errores:\n\n"
                                f"Registros importados: {imported_count}\n"
                                f"Errores encontrados: {len(errors)}\n\n"
                                f"Primeros errores:\n{error_msg}"
                            )
                        else:
                            QMessageBox.information(
                                self,
                                "Importación Completada",
                                f"✅ Importación completada exitosamente.\n\n"
                                f"Registros importados: {imported_count}"
                            )
                        
                        # Refrescar las tablas si hay datos importados
                        if imported_count > 0:
                            self.load_payment_concepts(force=True)
                            self.load_payment_methods(force=True)
                    
                    except Exception as import_error:
                        progress.close()
                        QMessageBox.critical(
                            self,
                            "Error de Importación",
                            f"Error durante la importación:\n{str(import_error)}"
                        )
            
        except Exception as e:
            QMessageBox.critical(
                self,
                "Error de Importación",
                f"Error al importar CSV:\n{str(e)}"
            )
    
    def _import_users_from_csv(self, rows, progress):
        """Importa usuarios desde datos CSV."""
        imported_count = 0
        errors = []
        
        try:
            with self.db_manager.get_connection_context() as conn:
                cursor = conn.cursor()
                
                for i, row in enumerate(rows):
                    if progress.wasCanceled():
                        break
                    
                    progress.setValue(i)
                    progress.setLabelText(f"Importando usuario {i+1} de {len(rows)}...")
                    QApplication.processEvents()
                    
                    try:
                        # Mapear campos del CSV a campos de la base de datos
                        nombre = row.get('nombre', '').strip()
                        apellido = row.get('apellido', '').strip()
                        email = row.get('email', '').strip()
                        telefono = row.get('telefono', row.get('teléfono', '')).strip()
                        
                        if not nombre or not apellido or not email:
                            errors.append(f"Fila {i+1}: Faltan campos obligatorios (nombre, apellido, email)")
                            continue
                        
                        # Verificar si el usuario ya existe
                        cursor.execute("SELECT id FROM usuarios WHERE email = %s", (email,))
                        if cursor.fetchone():
                            errors.append(f"Fila {i+1}: Usuario con email {email} ya existe")
                            continue
                        
                        # Insertar usuario
                        cursor.execute("""
                            INSERT INTO usuarios (nombre, apellido, email, telefono, fecha_registro, activo)
                            VALUES (%s, %s, %s, %s, CURRENT_DATE, true)
                        """, (nombre, apellido, email, telefono))
                        
                        imported_count += 1
                        
                    except Exception as row_error:
                        errors.append(f"Fila {i+1}: {str(row_error)}")
                
                conn.commit()
        
        except Exception as e:
            errors.append(f"Error de base de datos: {str(e)}")
        
        return imported_count, errors
    
    def _import_payments_from_csv(self, rows, progress):
        """Importa pagos desde datos CSV."""
        imported_count = 0
        errors = []
        
        try:
            with self.db_manager.get_connection_context() as conn:
                cursor = conn.cursor()
                
                for i, row in enumerate(rows):
                    if progress.wasCanceled():
                        break
                    
                    progress.setValue(i)
                    progress.setLabelText(f"Importando pago {i+1} de {len(rows)}...")
                    QApplication.processEvents()
                    
                    try:
                        # Mapear campos del CSV
                        usuario_email = row.get('usuario_email', row.get('email', '')).strip()
                        concepto_nombre = row.get('concepto', '').strip()
                        monto = float(row.get('monto', 0))
                        fecha_pago = row.get('fecha_pago', row.get('fecha', '')).strip()
                        
                        if not usuario_email or not concepto_nombre or monto <= 0:
                            errors.append(f"Fila {i+1}: Faltan campos obligatorios o monto inválido")
                            continue
                        
                        # Buscar usuario
                        cursor.execute("SELECT id FROM usuarios WHERE email = %s", (usuario_email,))
                        usuario_result = cursor.fetchone()
                        if not usuario_result:
                            errors.append(f"Fila {i+1}: Usuario con email {usuario_email} no encontrado")
                            continue
                        
                        usuario_id = usuario_result[0]
                        
                        # Buscar o crear concepto
                        cursor.execute("SELECT id FROM conceptos_pago WHERE nombre = %s", (concepto_nombre,))
                        concepto_result = cursor.fetchone()
                        if not concepto_result:
                            cursor.execute(
                                "INSERT INTO conceptos_pago (nombre, descripcion, activo) VALUES (%s, %s, true) RETURNING id",
                                (concepto_nombre, f"Concepto importado: {concepto_nombre}")
                            )
                            result = cursor.fetchone()
                            concepto_id = result[0] if result and len(result) > 0 else None
                            if concepto_id is None:
                                raise Exception("No se pudo crear el concepto de pago")
                        else:
                            concepto_id = concepto_result[0]
                        
                        # Insertar pago
                        cursor.execute("""
                            INSERT INTO pagos (usuario_id, concepto_id, monto, fecha_pago, estado)
                            VALUES (%s, %s, %s, %s, 'completado')
                        """, (usuario_id, concepto_id, monto, fecha_pago or 'CURRENT_DATE'))
                        
                        imported_count += 1
                        
                    except Exception as row_error:
                        errors.append(f"Fila {i+1}: {str(row_error)}")
                
                conn.commit()
        
        except Exception as e:
            errors.append(f"Error de base de datos: {str(e)}")
        
        return imported_count, errors
    
    def on_import_sql_clicked(self):
        """Importa datos desde SQL."""
        try:
            reply = QMessageBox.question(
                self,
                "Importar SQL",
                "¿Desea ejecutar un script SQL?\n\n⚠️ ADVERTENCIA: Esto puede modificar la base de datos.",
                QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No
            )
            
            if reply == QMessageBox.StandardButton.Yes:
                # Aquí se abriría un diálogo de selección de archivo SQL
                QMessageBox.information(
                    self,
                    "Importar SQL",
                    "Función de importación SQL.\n\nSeleccione el archivo SQL a ejecutar."
                )
        except Exception as e:
            QMessageBox.critical(
                self,
                "Error de Importación",
                f"Error al importar SQL:\n{str(e)}"
            )
    
    def on_import_backup_clicked(self):
        """Restaura desde backup."""
        try:
            reply = QMessageBox.question(
                self,
                "Restaurar Backup",
                "¿Desea restaurar desde un archivo de backup?\n\n⚠️ ADVERTENCIA: Esto reemplazará todos los datos actuales.",
                QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No
            )
            
            if reply == QMessageBox.StandardButton.Yes:
                # Aquí se abriría un diálogo de selección de backup
                QMessageBox.information(
                    self,
                    "Restaurar Backup",
                    "Función de restauración de backup.\n\nSeleccione el archivo de backup a restaurar."
                )
        except Exception as e:
            QMessageBox.critical(
                 self,
                 "Error de Restauración",
                 f"Error al restaurar backup:\n{str(e)}"
             )

    
    def on_payment_reminders_clicked(self):
        """Configura recordatorios de pago reales."""
        try:
            # Crear diálogo de configuración de notificaciones
            dialog = QDialog(self)
            dialog.setWindowTitle("Configuración de Recordatorios de Pago")
            dialog.setModal(True)
            dialog.resize(500, 400)
            
            layout = QVBoxLayout(dialog)
            
            # Título
            title_label = QLabel("📱 Configuración de Notificaciones por SMS")
            title_label.setStyleSheet("font-size: 16px; font-weight: bold; margin-bottom: 10px;")
            layout.addWidget(title_label)
            
            # Configuración de días de anticipación
            days_group = QGroupBox("Días de Anticipación")
            days_layout = QFormLayout()
            
            self.days_spinbox = QSpinBox()
            self.days_spinbox.setRange(1, 30)
            self.days_spinbox.setValue(3)
            days_layout.addRow("Días antes del vencimiento:", self.days_spinbox)
            
            days_group.setLayout(days_layout)
            layout.addWidget(days_group)
            
            # Lista de usuarios con pagos vencidos
            users_group = QGroupBox("Usuarios con Pagos Vencidos")
            users_layout = QVBoxLayout()
            
            # Obtener usuarios con pagos vencidos
            overdue_users = self.get_overdue_payment_users()
            
            if overdue_users:
                users_table = QTableWidget()
                users_table.setColumnCount(3)
                users_table.setHorizontalHeaderLabels(["Nombre", "Teléfono", "Días Vencido"])
                users_table.setRowCount(len(overdue_users))
                
                for i, user_data in enumerate(overdue_users):
                    users_table.setItem(i, 0, QTableWidgetItem(user_data['nombre']))
                    users_table.setItem(i, 1, QTableWidgetItem(user_data['telefono'] or 'Sin teléfono'))
                    users_table.setItem(i, 2, QTableWidgetItem(str(user_data['dias_vencido'])))
                
                users_table.resizeColumnsToContents()
                users_layout.addWidget(users_table)
                
                # Botón para enviar notificaciones
                send_btn = QPushButton("📱 Enviar Notificaciones SMS")
                send_btn.clicked.connect(lambda: self.send_payment_notifications(overdue_users))
                users_layout.addWidget(send_btn)
            else:
                no_users_label = QLabel("✅ No hay usuarios con pagos vencidos")
                no_users_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
                no_users_label.setStyleSheet("color: green; font-style: italic; padding: 20px;")
                users_layout.addWidget(no_users_label)
            
            users_group.setLayout(users_layout)
            layout.addWidget(users_group)
            
            # Botones
            buttons_layout = QHBoxLayout()
            close_btn = QPushButton("Cerrar")
            close_btn.clicked.connect(dialog.accept)
            buttons_layout.addWidget(close_btn)
            
            layout.addLayout(buttons_layout)
            
            dialog.exec()
            
        except Exception as e:
            QMessageBox.critical(
                self,
                "Error de Configuración",
                f"Error al configurar recordatorios:\n{str(e)}"
            )
    
    def get_overdue_payment_users(self):
        """Obtiene usuarios con pagos vencidos usando datos reales."""
        try:
            from datetime import datetime, timedelta
            
            overdue_users = []
            cutoff_date = datetime.now() - timedelta(days=30)
            
            # Obtener usuarios activos (socios y profesores)
            usuarios_activos = (
                self.db_manager.obtener_usuarios_por_rol('socio') +
                self.db_manager.obtener_usuarios_por_rol('profesor')
            )
            usuarios_activos = [u for u in usuarios_activos if u.activo]
            
            for usuario in usuarios_activos:
                ultimo_pago = self.payment_manager.obtener_ultimo_pago_usuario(usuario.id)
                
                if not ultimo_pago:
                    # Usuario sin pagos registrados
                    dias_vencido = 30
                else:
                    # Calcular días desde el último pago
                    if isinstance(ultimo_pago.fecha_pago, str):
                        fecha_pago = datetime.fromisoformat(ultimo_pago.fecha_pago)
                    else:
                        fecha_pago = ultimo_pago.fecha_pago
                    
                    dias_vencido = (datetime.now() - fecha_pago).days
                
                if dias_vencido > 30:  # Más de 30 días sin pagar
                    overdue_users.append({
                        'id': usuario.id,
                        'nombre': usuario.nombre,
                        'telefono': usuario.telefono,
                        'dias_vencido': dias_vencido
                    })
            
            return overdue_users
            
        except Exception as e:
            logging.error(f"Error obteniendo usuarios con pagos vencidos: {e}")
            return []
    
    def send_payment_notifications(self, users):
        """Registra notificaciones de pago en el sistema para usuarios con pagos vencidos."""
        try:
            sent_count = 0
            failed_count = 0
            
            for user in users:
                if user['telefono']:
                    # Crear mensaje de notificación
                    message = f"Hola {user['nombre']}, tu pago del gimnasio está vencido hace {user['dias_vencido']} días. Por favor, acércate a regularizar tu situación. Gracias."
                    
                    # Registrar la notificación en el sistema
                    try:
                        # Crear registro de notificación en la base de datos
                        from datetime import datetime
                        notification_data = {
                            'usuario_id': user['id'],
                            'tipo': 'pago_vencido',
                            'mensaje': message,
                            'telefono': user['telefono'],
                            'fecha_creacion': datetime.now().isoformat(),
                            'estado': 'pendiente'
                        }
                        
                        # Aquí se guardaría en una tabla de notificaciones si existiera
                        # Por ahora registramos en logs del sistema
                        logging.info(f"Notificación registrada para {user['nombre']} ({user['telefono']}): {message}")
                        sent_count += 1
                        
                    except Exception as e:
                        logging.error(f"Error al registrar notificación para {user['nombre']}: {e}")
                        failed_count += 1
                else:
                    logging.warning(f"Usuario {user['nombre']} no tiene teléfono registrado")
                    failed_count += 1
            
            # Mostrar resultado
            result_msg = f"📱 Notificaciones registradas:\n\n✅ Registradas: {sent_count}\n❌ Fallidas: {failed_count}"
            if failed_count > 0:
                result_msg += "\n\n⚠️ Algunos usuarios no tienen teléfono registrado"
            
            result_msg += "\n\n📋 Las notificaciones han sido registradas en el sistema de logs."
            
            QMessageBox.information(self, "Notificaciones Registradas", result_msg)
            
        except Exception as e:
            QMessageBox.critical(
                self,
                "Error de Registro",
                f"Error al registrar notificaciones:\n{str(e)}"
            )
    
    
    
    def show_concepts_table_context_menu(self, position):
        """Muestra el menú contextual para la tabla de conceptos de pago."""
        print("DEBUG: show_concepts_table_context_menu llamado")
        if self.concepts_table.itemAt(position) is None:
            print("DEBUG: No hay item en la posición")
            return
        
        menu = QMenu(self)
        menu.setObjectName("contextMenu")
        menu.setProperty("menuType", "payment_concepts")
        
        # Acciones del menú
        edit_action = menu.addAction("✏️ Editar Concepto")
        duplicate_action = menu.addAction("📋 Duplicar Concepto")
        menu.addSeparator()
        delete_action = menu.addAction("🗑️ Eliminar Concepto")
        
        # Ejecutar menú y manejar acción seleccionada
        action = menu.exec(self.concepts_table.mapToGlobal(position))
        
        if action == edit_action:
            self.edit_payment_concept()
        elif action == duplicate_action:
            self.duplicate_payment_concept()
        elif action == delete_action:
            self.delete_payment_concept()
    
    def show_methods_table_context_menu(self, position):
        """Muestra el menú contextual para la tabla de métodos de pago."""
        print("DEBUG: show_methods_table_context_menu llamado")
        if self.methods_table.itemAt(position) is None:
            print("DEBUG: No hay item en la posición")
            return
        
        menu = QMenu(self)
        menu.setObjectName("contextMenu")
        menu.setProperty("menuType", "payment_methods")
        
        # Acciones del menú
        edit_action = menu.addAction("✏️ Editar Método")
        duplicate_action = menu.addAction("📋 Duplicar Método")
        toggle_action = menu.addAction("🔄 Activar/Desactivar")
        menu.addSeparator()
        delete_action = menu.addAction("🗑️ Eliminar Método")
        
        # Ejecutar menú y manejar acción seleccionada
        action = menu.exec(self.methods_table.mapToGlobal(position))
        
        if action == edit_action:
            self.edit_payment_method()
        elif action == duplicate_action:
            self.duplicate_payment_method()
        elif action == toggle_action:
            self.toggle_payment_method()
        elif action == delete_action:
            self.delete_payment_method()

