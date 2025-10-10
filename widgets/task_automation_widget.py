import sys
import logging
import os
import shutil

from datetime import datetime, timedelta
from PyQt6.QtWidgets import (
    QWidget, QVBoxLayout, QHBoxLayout, QGridLayout, QLabel, QPushButton,
    QGroupBox, QTableWidget, QTableWidgetItem, QTabWidget, QDateEdit,
    QComboBox, QSpinBox, QTextEdit, QFrame, QScrollArea, QMessageBox,
    QHeaderView, QAbstractItemView, QProgressBar, QSplitter, QCheckBox,
    QTimeEdit, QFileDialog, QListWidget, QListWidgetItem
)
from PyQt6.QtGui import QFont, QPixmap, QPalette, QColor, QIcon
from PyQt6.QtCore import Qt, QDate, QTimer, QThread, pyqtSignal, QTime

from database import DatabaseManager
from utils import resource_path, collect_temp_candidates, delete_files
import json
import psycopg2.extras

class AutomationTask:
    """Clase para representar una tarea de automatización"""
    
    def __init__(self, task_id, name, description, task_type, schedule, enabled=True, last_run=None):
        self.task_id = task_id
        self.name = name
        self.description = description
        self.task_type = task_type  # 'backup', 'cleanup', 'maintenance', 'report'
        self.schedule = schedule  # {'type': 'daily/weekly/monthly', 'time': 'HH:MM', 'day': 'monday'}
        self.enabled = enabled
        self.last_run = last_run
        self.next_run = self._calculate_next_run()
    
    def _calculate_next_run(self):
        """Calcula la próxima ejecución de la tarea"""
        if not self.enabled:
            return None
        
        now = datetime.now()
        schedule_time = datetime.strptime(self.schedule.get('time', '02:00'), '%H:%M').time()
        
        if self.schedule['type'] == 'daily':
            next_run = datetime.combine(now.date(), schedule_time)
            if next_run <= now:
                next_run += timedelta(days=1)
        elif self.schedule['type'] == 'weekly':
            # Implementar lógica semanal
            next_run = datetime.combine(now.date(), schedule_time) + timedelta(days=7)
        elif self.schedule['type'] == 'monthly':
            # Implementar lógica mensual
            next_run = datetime.combine(now.date(), schedule_time) + timedelta(days=30)
        else:
            next_run = None
        
        return next_run
    
    def should_run(self):
        """Verifica si la tarea debe ejecutarse ahora"""
        if not self.enabled or not self.next_run:
            return False
        
        return datetime.now() >= self.next_run
    
    def to_dict(self):
        """Convierte la tarea a diccionario para serialización"""
        return {
            'task_id': self.task_id,
            'name': self.name,
            'description': self.description,
            'task_type': self.task_type,
            'schedule': self.schedule,
            'enabled': self.enabled,
            'last_run': self.last_run.isoformat() if self.last_run else None
        }
    
    @classmethod
    def from_dict(cls, data):
        """Crea una tarea desde un diccionario"""
        last_run = None
        if data.get('last_run'):
            last_run = datetime.fromisoformat(data['last_run'])
        
        return cls(
            task_id=data['task_id'],
            name=data['name'],
            description=data['description'],
            task_type=data['task_type'],
            schedule=data['schedule'],
            enabled=data.get('enabled', True),
            last_run=last_run
        )

class TaskExecutorThread(QThread):
    """Hilo para ejecutar tareas de automatización"""
    
    task_started = pyqtSignal(str)  # task_id
    task_completed = pyqtSignal(str, bool, str)  # task_id, success, message
    progress_updated = pyqtSignal(str, int)  # task_id, progress
    
    def __init__(self, db_manager, task, parent=None):
        super().__init__(parent)
        self.db_manager = db_manager
        self.task = task
        self._stop_requested = False
    
    def stop(self):
        """Solicita detener la ejecución"""
        self._stop_requested = True
    
    def run(self):
        """Ejecuta la tarea"""
        try:
            self.task_started.emit(self.task.task_id)
            
            if self.task.task_type == 'backup':
                success, message = self._execute_backup()
            elif self.task.task_type == 'cleanup':
                success, message = self._execute_cleanup()
            elif self.task.task_type == 'maintenance':
                success, message = self._execute_maintenance()
            elif self.task.task_type == 'report':
                success, message = self._execute_report()
            else:
                success, message = False, f"Tipo de tarea desconocido: {self.task.task_type}"
            
            self.task_completed.emit(self.task.task_id, success, message)
            
        except Exception as e:
            logging.error(f"Error ejecutando tarea {self.task.task_id}: {e}")
            self.task_completed.emit(self.task.task_id, False, str(e))
    
    def _execute_backup(self):
        """Ejecuta backup de la base de datos PostgreSQL"""
        try:
            self.progress_updated.emit(self.task.task_id, 10)
            
            # Crear directorio de backups si no existe
            backup_dir = os.path.join(os.getcwd(), 'backups')
            os.makedirs(backup_dir, exist_ok=True)
            
            self.progress_updated.emit(self.task.task_id, 30)
            
            # Generar nombre del archivo de backup
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            backup_filename = f"gym_backup_{timestamp}.sql"
            backup_path = os.path.join(backup_dir, backup_filename)
            
            self.progress_updated.emit(self.task.task_id, 50)
            
            # Realizar backup usando PostgreSQL pg_dump
            try:
                # Usar el método backup_database del DatabaseManager
                success = self.db_manager.backup_database(backup_path)
                
                if success:
                    self.progress_updated.emit(self.task.task_id, 80)
                    
                    # Comprimir el backup (opcional)
                    import zipfile
                    zip_path = backup_path.replace('.sql', '.zip')
                    with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
                        zipf.write(backup_path, backup_filename)
                    
                    # Eliminar archivo sin comprimir
                    os.remove(backup_path)
                    
                    self.progress_updated.emit(self.task.task_id, 100)
                    
                    return True, f"Backup creado exitosamente: {zip_path}"
                else:
                    return False, "Error al crear backup de PostgreSQL"
                    
            except Exception as backup_error:
                # Fallback: crear backup usando consultas SQL directas
                return self._create_sql_backup(backup_path)
                
        except Exception as e:
            return False, f"Error en backup: {str(e)}"
    
    def _create_sql_backup(self, backup_path):
        """Crea backup usando consultas SQL directas como fallback"""
        try:
            with self.db_manager.get_connection_context() as conn:
                cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
                
                with open(backup_path, 'w', encoding='utf-8') as f:
                    f.write("-- Backup de base de datos PostgreSQL\n")
                    f.write(f"-- Generado el: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
                    
                    # Obtener lista de tablas
                    cursor.execute("""
                        SELECT tablename FROM pg_tables 
                        WHERE schemaname = 'public' 
                        ORDER BY tablename
                    """)
                    tables = [row['tablename'] for row in cursor.fetchall()]
                    
                    # Exportar estructura y datos de cada tabla
                    for table in tables:
                        f.write(f"\n-- Tabla: {table}\n")
                        
                        # Exportar datos
                        cursor.execute(f"SELECT * FROM {table}")
                        rows = cursor.fetchall()
                        
                        if rows:
                            # Obtener nombres de columnas
                            columns = [desc[0] for desc in cursor.description]
                            
                            for row in rows:
                                values = []
                                for value in row:
                                    if value is None:
                                        values.append('NULL')
                                    elif isinstance(value, str):
                                        values.append(f"'{value.replace("'", "''")}'")
                                    elif isinstance(value, datetime):
                                        values.append(f"'{value.isoformat()}'")
                                    else:
                                        values.append(str(value))
                                
                                f.write(f"INSERT INTO {table} ({', '.join(columns)}) VALUES ({', '.join(values)});\n")
                
                self.progress_updated.emit(self.task.task_id, 80)
                
                # Comprimir el backup
                import zipfile
                zip_path = backup_path.replace('.sql', '.zip')
                with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
                    zipf.write(backup_path, os.path.basename(backup_path))
                
                # Eliminar archivo sin comprimir
                os.remove(backup_path)
                
                self.progress_updated.emit(self.task.task_id, 100)
                
                return True, f"Backup SQL creado exitosamente: {zip_path}"
                
        except Exception as e:
            return False, f"Error creando backup SQL: {str(e)}"
    
    def _execute_cleanup(self):
        """Ejecuta limpieza de datos antiguos"""
        try:
            self.progress_updated.emit(self.task.task_id, 10)
            
            with self.db_manager.get_connection_context() as conn:
                cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
                
                # Limpiar logs antiguos (más de 90 días)
                cursor.execute(
                    "DELETE FROM logs WHERE fecha < NOW() - INTERVAL '90 days'"
                )
                logs_deleted = cursor.rowcount
                
                self.progress_updated.emit(self.task.task_id, 40)
                
                # Limpiar asistencias muy antiguas (más de 2 años)
                cursor.execute(
                    "DELETE FROM asistencias WHERE fecha < NOW() - INTERVAL '2 years'"
                )
                attendance_deleted = cursor.rowcount
                
                self.progress_updated.emit(self.task.task_id, 70)
                
                # Limpiar archivos temporales utilizando utilidades centralizadas
                temp_files_deleted = self._cleanup_temp_files()
                
                self.progress_updated.emit(self.task.task_id, 90)
                
                # Optimizar base de datos (PostgreSQL no requiere VACUUM manual frecuente)
                # En PostgreSQL, el autovacuum se encarga automáticamente
                pass
                
                self.progress_updated.emit(self.task.task_id, 100)
                
                message = f"Limpieza completada: {logs_deleted} logs, {attendance_deleted} asistencias, {temp_files_deleted} archivos temporales eliminados"
                return True, message
                
        except Exception as e:
            return False, f"Error en limpieza: {str(e)}"
    
    def _cleanup_temp_files(self):
        """Limpia archivos temporales utilizando utilidades centralizadas"""
        try:
            candidates = collect_temp_candidates(retention_days=7)
            deleted_count, error_count = delete_files(candidates)
            return deleted_count
        except Exception as e:
            logging.error(f"Error limpiando archivos temporales: {e}")
            return 0
    
    def _execute_maintenance(self):
        """Ejecuta tareas de mantenimiento"""
        try:
            self.progress_updated.emit(self.task.task_id, 20)
    
            # Verificación de integridad centralizada
            try:
                integrity = self.db_manager.verificar_integridad_base_datos()
            except Exception as e:
                logging.error(f"Error verificando integridad: {e}")
                integrity = {"estado": "ERROR", "errores": [str(e)], "advertencias": [], "tablas_verificadas": 0}
    
            self.progress_updated.emit(self.task.task_id, 50)
    
            # Optimización centralizada (VACUUM/ANALYZE según corresponda)
            ok_opt = False
            try:
                ok_opt = self.db_manager.optimizar_base_datos()
            except Exception as e:
                logging.error(f"Error optimizando base de datos: {e}")
                ok_opt = False
    
            self.progress_updated.emit(self.task.task_id, 80)
            self.progress_updated.emit(self.task.task_id, 100)
    
            estado = integrity.get("estado", "ERROR") if isinstance(integrity, dict) else "ERROR"
            errores = (integrity.get("errores") if isinstance(integrity, dict) else None) or []
            advertencias = (integrity.get("advertencias") if isinstance(integrity, dict) else None) or []
    
            if estado == "OK" and not errores and ok_opt:
                return True, "Mantenimiento completado. Base de datos íntegra y optimizada."
            else:
                partes = []
                if not ok_opt:
                    partes.append("Optimización fallida")
                if errores:
                    preview = ", ".join(errores[:3]) + (f" y {len(errores) - 3} más" if len(errores) > 3 else "")
                    partes.append(f"Errores: {preview}")
                elif advertencias:
                    preview = ", ".join(advertencias[:3]) + (f" y {len(advertencias) - 3} más" if len(advertencias) > 3 else "")
                    partes.append(f"Advertencias: {preview}")
                detalle = "; ".join(partes) if partes else "Problemas no especificados"
                return False, f"Problemas de mantenimiento: {detalle}"
    
        except Exception as e:
            return False, f"Error en mantenimiento: {str(e)}"
    
    def _execute_report(self):
        """Ejecuta generación de reportes automáticos"""
        try:
            self.progress_updated.emit(self.task.task_id, 25)
            
            # Crear directorio de reportes si no existe
            reports_dir = os.path.join(os.getcwd(), 'reports', 'automated')
            os.makedirs(reports_dir, exist_ok=True)
            
            self.progress_updated.emit(self.task.task_id, 50)
            
            # Generar reporte de resumen diario
            timestamp = datetime.now().strftime('%Y%m%d')
            report_filename = f"reporte_diario_{timestamp}.txt"
            report_path = os.path.join(reports_dir, report_filename)
            
            with self.db_manager.get_connection_context() as conn:
                cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
                
                # Obtener estadísticas del día
                cursor.execute(
                    "SELECT COUNT(*) FROM asistencias WHERE fecha = CURRENT_DATE"
                )
                result = cursor.fetchone()
                asistencias_hoy = result[0] if result and len(result) > 0 else 0
                
                cursor.execute(
                    "SELECT COUNT(*) FROM usuarios WHERE fecha_registro = CURRENT_DATE"
                )
                result = cursor.fetchone()
                nuevos_usuarios = result[0] if result and len(result) > 0 else 0
                
                cursor.execute(
                    "SELECT SUM(monto) FROM pagos WHERE fecha_pago = CURRENT_DATE"
                )
                result = cursor.fetchone()
                ingresos_hoy = result[0] if result and len(result) > 0 else 0
            
            self.progress_updated.emit(self.task.task_id, 75)
            
            # Escribir reporte
            with open(report_path, 'w', encoding='utf-8') as f:
                f.write(f"REPORTE DIARIO - {datetime.now().strftime('%d/%m/%Y')}\n")
                f.write("=" * 50 + "\n\n")
                f.write(f"Asistencias del día: {asistencias_hoy}\n")
                f.write(f"Nuevos usuarios: {nuevos_usuarios}\n")
                f.write(f"Ingresos del día: ${ingresos_hoy:,.2f}\n")
                f.write(f"\nReporte generado automáticamente a las {datetime.now().strftime('%H:%M:%S')}\n")
            
            self.progress_updated.emit(self.task.task_id, 100)
            
            return True, f"Reporte generado: {report_path}"
            
        except Exception as e:
            return False, f"Error generando reporte: {str(e)}"

class TaskAutomationWidget(QWidget):
    """Widget para gestión de automatización de tareas"""
    
    def __init__(self, db_manager, parent=None):
        super().__init__(parent)
        self.db_manager = db_manager
        self.tasks = []
        self.executor_threads = {}
        self.automation_timer = QTimer()
        self.automation_timer.timeout.connect(self.check_scheduled_tasks)
        self.automation_timer.start(60000)  # Verificar cada minuto
        
        self.setup_ui()
        self.load_default_tasks()
        self.load_tasks_from_config()
    
    def setup_ui(self):
        """Configura la interfaz del widget"""
        layout = QVBoxLayout(self)
        layout.setContentsMargins(15, 15, 15, 15)
        layout.setSpacing(10)
        
        # Título
        title = QLabel("🤖 Automatización de Tareas")
        title.setFont(QFont("Segoe UI", 16, QFont.Weight.Bold))
        title.setObjectName("task_automation_title")
        layout.addWidget(title)
        
        # Tabs para diferentes secciones
        self.tab_widget = QTabWidget()
        
        # Tab 1: Tareas Programadas
        self.scheduled_tab = self.create_scheduled_tab()
        self.tab_widget.addTab(self.scheduled_tab, "📅 Tareas Programadas")
        
        # Tab 2: Ejecución Manual
        self.manual_tab = self.create_manual_tab()
        self.tab_widget.addTab(self.manual_tab, "▶️ Ejecución Manual")
        
        # Tab 3: Historial
        self.history_tab = self.create_history_tab()
        self.tab_widget.addTab(self.history_tab, "Historial")
        
        # Tab 4: Configuración
        self.config_tab = self.create_config_tab()
        self.tab_widget.addTab(self.config_tab, "⚙️ Configuración")
        
        layout.addWidget(self.tab_widget)
        
        # Aplicar estilo
        self.setStyleSheet("""
            QTabWidget::pane {
                border: 1px solid #D8DEE9;
                background-color: #ECEFF4;
            }
            QTabBar::tab {
                background-color: #E5E9F0;
                color: #2E3440;
                padding: 8px 16px;
                margin-right: 2px;
                border-top-left-radius: 4px;
                border-top-right-radius: 4px;
            }
            QTabBar::tab:selected {
                background-color: #5E81AC;
                color: white;
            }
            QTabBar::tab:hover {
                background-color: #D8DEE9;
            }
        """)
    
    def create_scheduled_tab(self):
        """Crea la pestaña de tareas programadas"""
        tab = QWidget()
        layout = QVBoxLayout(tab)
        
        # Tabla de tareas programadas
        self.scheduled_table = QTableWidget()
        self.scheduled_table.setColumnCount(7)
        self.scheduled_table.setHorizontalHeaderLabels([
            "Nombre", "Tipo", "Programación", "Próxima Ejecución", "Estado", "Última Ejecución", "Acciones"
        ])
        self.scheduled_table.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeMode.Stretch)
        
        layout.addWidget(self.scheduled_table)
        
        # Botones de control
        buttons_layout = QHBoxLayout()
        
        self.add_task_button = QPushButton("➕ Agregar Tarea")
        self.add_task_button.clicked.connect(self.add_new_task)
        
        self.edit_task_button = QPushButton("✏️ Editar")
        self.edit_task_button.clicked.connect(self.edit_selected_task)
        
        self.delete_task_button = QPushButton("🗑️ Eliminar")
        self.delete_task_button.clicked.connect(self.delete_selected_task)
        
        buttons_layout.addWidget(self.add_task_button)
        buttons_layout.addWidget(self.edit_task_button)
        buttons_layout.addWidget(self.delete_task_button)
        buttons_layout.addStretch()
        
        layout.addLayout(buttons_layout)
        
        return tab
    
    def create_manual_tab(self):
        """Crea la pestaña de ejecución manual"""
        tab = QWidget()
        layout = QVBoxLayout(tab)
        
        # Grupos de tareas manuales
        backup_group = QGroupBox("Backup y Respaldo")
        backup_layout = QGridLayout(backup_group)
        
        self.backup_db_button = QPushButton("Backup Base de Datos")
        self.backup_db_button.clicked.connect(lambda: self.execute_manual_task('backup'))
        
        self.export_data_button = QPushButton("Exportar Datos")
        self.export_data_button.clicked.connect(self.export_data_manual)
        
        backup_layout.addWidget(self.backup_db_button, 0, 0)
        backup_layout.addWidget(self.export_data_button, 0, 1)
        
        # Grupo de limpieza
        cleanup_group = QGroupBox("🧹 Limpieza y Mantenimiento")
        cleanup_layout = QGridLayout(cleanup_group)
        
        self.cleanup_button = QPushButton("Limpiar Datos Antiguos")
        self.cleanup_button.clicked.connect(lambda: self.execute_manual_task('cleanup'))
        
        self.maintenance_button = QPushButton("Mantenimiento BD")
        self.maintenance_button.clicked.connect(lambda: self.execute_manual_task('maintenance'))
        
        cleanup_layout.addWidget(self.cleanup_button, 0, 0)
        cleanup_layout.addWidget(self.maintenance_button, 0, 1)
        
        # Grupo de reportes
        reports_group = QGroupBox("Reportes")
        reports_layout = QGridLayout(reports_group)
        
        self.generate_report_button = QPushButton("Generar Reporte Diario")
        self.generate_report_button.clicked.connect(lambda: self.execute_manual_task('report'))
        
        reports_layout.addWidget(self.generate_report_button, 0, 0)
        
        # Área de progreso
        progress_group = QGroupBox("📈 Progreso de Ejecución")
        progress_layout = QVBoxLayout(progress_group)
        
        self.progress_bar = QProgressBar()
        self.progress_bar.setMinimumHeight(30)
        self.progress_bar.setTextVisible(True)
        self.progress_bar.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self.progress_bar.setStyleSheet("""
            QProgressBar {
                border: 2px solid VAR_BG_QUATERNARY;
                border-radius: 5px;
                text-align: center;
                font-weight: bold;
                font-size: 12px;
                margin: 5px 0;
            }
            QProgressBar::chunk {
                background-color: #5E81AC;
                border-radius: 3px;
            }
        """)
        self.progress_label = QLabel("Listo para ejecutar tareas")
        
        progress_layout.addWidget(self.progress_label)
        progress_layout.addWidget(self.progress_bar)
        
        # Agregar grupos al layout
        layout.addWidget(backup_group)
        layout.addWidget(cleanup_group)
        layout.addWidget(reports_group)
        layout.addWidget(progress_group)
        layout.addStretch()
        
        return tab
    
    def create_history_tab(self):
        """Crea la pestaña de historial"""
        tab = QWidget()
        layout = QVBoxLayout(tab)
        
        # Lista de historial
        self.history_list = QListWidget()
        layout.addWidget(self.history_list)
        
        # Botones de control
        buttons_layout = QHBoxLayout()
        
        self.refresh_history_button = QPushButton("🔄 Actualizar")
        self.refresh_history_button.clicked.connect(self.refresh_history)
        
        self.clear_history_button = QPushButton("🗑️ Limpiar Historial")
        self.clear_history_button.clicked.connect(self.clear_history)
        
        buttons_layout.addWidget(self.refresh_history_button)
        buttons_layout.addWidget(self.clear_history_button)
        buttons_layout.addStretch()
        
        layout.addLayout(buttons_layout)
        
        return tab
    
    def create_config_tab(self):
        """Crea la pestaña de configuración"""
        tab = QWidget()
        layout = QVBoxLayout(tab)
        
        # Configuración general
        general_group = QGroupBox("⚙️ Configuración General")
        general_layout = QGridLayout(general_group)
        
        general_layout.addWidget(QLabel("Intervalo de verificación (minutos):"), 0, 0)
        self.check_interval_spin = QSpinBox()
        self.check_interval_spin.setRange(1, 60)
        self.check_interval_spin.setValue(1)
        general_layout.addWidget(self.check_interval_spin, 0, 1)
        
        general_layout.addWidget(QLabel("Retener backups (días):"), 1, 0)
        self.backup_retention_spin = QSpinBox()
        self.backup_retention_spin.setRange(1, 365)
        self.backup_retention_spin.setValue(30)
        general_layout.addWidget(self.backup_retention_spin, 1, 1)
        
        # Configuración de notificaciones
        notifications_group = QGroupBox("🔔 Notificaciones")
        notifications_layout = QVBoxLayout(notifications_group)
        
        self.notify_success_check = QCheckBox("Notificar ejecuciones exitosas")
        self.notify_errors_check = QCheckBox("Notificar errores")
        self.notify_errors_check.setChecked(True)
        
        notifications_layout.addWidget(self.notify_success_check)
        notifications_layout.addWidget(self.notify_errors_check)
        
        # Botones de configuración
        config_buttons_layout = QHBoxLayout()
        
        self.save_config_button = QPushButton("Guardar Configuración")
        self.save_config_button.clicked.connect(self.save_configuration)
        
        self.reset_config_button = QPushButton("🔄 Restaurar Predeterminados")
        self.reset_config_button.clicked.connect(self.reset_configuration)
        
        config_buttons_layout.addWidget(self.save_config_button)
        config_buttons_layout.addWidget(self.reset_config_button)
        config_buttons_layout.addStretch()
        
        layout.addWidget(general_group)
        layout.addWidget(notifications_group)
        layout.addLayout(config_buttons_layout)
        layout.addStretch()
        
        return tab
    
    def load_default_tasks(self):
        """Carga las tareas predeterminadas"""
        default_tasks = [
            AutomationTask(
                task_id="backup_daily",
                name="Backup Diario",
                description="Backup automático de la base de datos",
                task_type="backup",
                schedule={'type': 'daily', 'time': '02:00'}
            ),
            AutomationTask(
                task_id="cleanup_weekly",
                name="Limpieza Semanal",
                description="Limpieza de datos antiguos y archivos temporales",
                task_type="cleanup",
                schedule={'type': 'weekly', 'time': '03:00', 'day': 'sunday'}
            ),
            AutomationTask(
                task_id="maintenance_monthly",
                name="Mantenimiento Mensual",
                description="Optimización y mantenimiento de la base de datos",
                task_type="maintenance",
                schedule={'type': 'monthly', 'time': '01:00', 'day': '1'}
            ),
            AutomationTask(
                task_id="report_daily",
                name="Reporte Diario",
                description="Generación automática de reporte diario",
                task_type="report",
                schedule={'type': 'daily', 'time': '23:30'}
            )
        ]
        
        self.tasks.extend(default_tasks)
    
    def load_tasks_from_config(self):
        """Carga tareas desde archivo de configuración"""
        try:
            config_path = os.path.join(os.getcwd(), 'config', 'automation_tasks.json')
            if os.path.exists(config_path):
                with open(config_path, 'r', encoding='utf-8') as f:
                    tasks_data = json.load(f)
                
                # Cargar tareas personalizadas
                for task_data in tasks_data.get('custom_tasks', []):
                    task = AutomationTask.from_dict(task_data)
                    self.tasks.append(task)
        
        except Exception as e:
            logging.error(f"Error cargando configuración de tareas: {e}")
        
        self.refresh_scheduled_table()
    
    def save_tasks_to_config(self):
        """Guarda tareas en archivo de configuración"""
        try:
            config_dir = os.path.join(os.getcwd(), 'config')
            os.makedirs(config_dir, exist_ok=True)
            
            config_path = os.path.join(config_dir, 'automation_tasks.json')
            
            # Separar tareas predeterminadas de personalizadas
            default_ids = ['backup_daily', 'cleanup_weekly', 'maintenance_monthly', 'report_daily']
            custom_tasks = [task for task in self.tasks if task.task_id not in default_ids]
            
            config_data = {
                'custom_tasks': [task.to_dict() for task in custom_tasks],
                'last_updated': datetime.now().isoformat()
            }
            
            with open(config_path, 'w', encoding='utf-8') as f:
                json.dump(config_data, f, indent=2, ensure_ascii=False)
        
        except Exception as e:
            logging.error(f"Error guardando configuración de tareas: {e}")
    
    def refresh_scheduled_table(self):
        """Actualiza la tabla de tareas programadas"""
        self.scheduled_table.setRowCount(len(self.tasks))
        
        for i, task in enumerate(self.tasks):
            self.scheduled_table.setItem(i, 0, QTableWidgetItem(task.name))
            self.scheduled_table.setItem(i, 1, QTableWidgetItem(task.task_type.title()))
            
            schedule_text = f"{task.schedule['type'].title()} a las {task.schedule['time']}"
            self.scheduled_table.setItem(i, 2, QTableWidgetItem(schedule_text))
            
            next_run_text = task.next_run.strftime('%d/%m/%Y %H:%M') if task.next_run else 'N/A'
            self.scheduled_table.setItem(i, 3, QTableWidgetItem(next_run_text))
            
            status_text = "Activa" if task.enabled else "Inactiva"
            self.scheduled_table.setItem(i, 4, QTableWidgetItem(status_text))
            
            last_run_text = task.last_run.strftime('%d/%m/%Y %H:%M') if task.last_run else 'Nunca'
            self.scheduled_table.setItem(i, 5, QTableWidgetItem(last_run_text))
            
            # Botón de acción
            action_button = QPushButton("▶️ Ejecutar")
            action_button.clicked.connect(lambda checked, t=task: self.execute_task_now(t))
            self.scheduled_table.setCellWidget(i, 6, action_button)
    
    def check_scheduled_tasks(self):
        """Verifica y ejecuta tareas programadas"""
        for task in self.tasks:
            if task.should_run() and task.task_id not in self.executor_threads:
                self.execute_task_now(task)
    
    def execute_task_now(self, task):
        """Ejecuta una tarea inmediatamente"""
        if task.task_id in self.executor_threads:
            QMessageBox.warning(
                self,
                "Tarea en Ejecución",
                f"La tarea '{task.name}' ya se está ejecutando."
            )
            return
        
        # Crear y configurar hilo de ejecución
        executor = TaskExecutorThread(self.db_manager, task, self)
        executor.task_started.connect(self.on_task_started)
        executor.task_completed.connect(self.on_task_completed)
        executor.progress_updated.connect(self.on_progress_updated)
        
        self.executor_threads[task.task_id] = executor
        executor.start()
    
    def execute_manual_task(self, task_type):
        """Ejecuta una tarea manual"""
        # Crear tarea temporal para ejecución manual
        temp_task = AutomationTask(
            task_id=f"manual_{task_type}_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
            name=f"Manual {task_type.title()}",
            description=f"Ejecución manual de {task_type}",
            task_type=task_type,
            schedule={'type': 'manual', 'time': 'now'}
        )
        
        self.execute_task_now(temp_task)
    
    def on_task_started(self, task_id):
        """Maneja el inicio de una tarea"""
        self.progress_label.setText(f"Ejecutando tarea: {task_id}")
        self.progress_bar.setValue(0)
        
        # Agregar al historial
        timestamp = datetime.now().strftime('%H:%M:%S')
        item = QListWidgetItem(f"[{timestamp}] Iniciando: {task_id}")
        self.history_list.addItem(item)
        self.history_list.scrollToBottom()
    
    def on_progress_updated(self, task_id, progress):
        """Actualiza el progreso de una tarea"""
        self.progress_bar.setValue(progress)
    
    def on_task_completed(self, task_id, success, message):
        """Maneja la finalización de una tarea"""
        # Limpiar hilo
        if task_id in self.executor_threads:
            del self.executor_threads[task_id]
        
        # Actualizar UI
        self.progress_bar.setValue(100 if success else 0)
        status = "✅ Completada" if success else "❌ Error"
        self.progress_label.setText(f"{status}: {message}")
        
        # Agregar al historial
        timestamp = datetime.now().strftime('%H:%M:%S')
        item = QListWidgetItem(f"[{timestamp}] {status}: {task_id} - {message}")
        if success:
            item.setBackground(QColor(200, 255, 200))  # Verde claro
        else:
            item.setBackground(QColor(255, 200, 200))  # Rojo claro
        
        self.history_list.addItem(item)
        self.history_list.scrollToBottom()
        
        # Actualizar última ejecución de la tarea
        for task in self.tasks:
            if task.task_id == task_id:
                task.last_run = datetime.now()
                task.next_run = task._calculate_next_run()
                break
        
        self.refresh_scheduled_table()
        
        # Mostrar notificación si está habilitada
        if (success and self.notify_success_check.isChecked()) or (not success and self.notify_errors_check.isChecked()):
            QMessageBox.information(
                self,
                "Tarea Completada" if success else "Error en Tarea",
                f"Tarea: {task_id}\n{message}"
            )
    
    def add_new_task(self):
        """Agrega una nueva tarea personalizada"""
        dialog = self._crear_dialogo_nueva_tarea()
        if dialog.exec() == QDialog.DialogCode.Accepted:
            task_data = dialog.get_task_data()
            if task_data:
                try:
                    # Crear nueva tarea
                    new_task = AutomationTask(
                        task_id=f"custom_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
                        name=task_data['name'],
                        description=task_data['description'],
                        task_type=task_data['task_type'],
                        schedule=task_data['schedule'],
                        enabled=task_data['enabled']
                    )
                    
                    # Agregar a la lista
                    self.tasks.append(new_task)
                    self.refresh_scheduled_table()
                    self.save_tasks_to_config()
                    
                    QMessageBox.information(
                        self,
                        "Tarea Creada",
                        f"La tarea '{new_task.name}' se ha creado exitosamente."
                    )
                    
                except Exception as e:
                    QMessageBox.critical(
                        self,
                        "Error",
                        f"Error al crear la tarea: {str(e)}"
                    )
    
    def edit_selected_task(self):
        """Edita la tarea seleccionada"""
        current_row = self.scheduled_table.currentRow()
        if current_row < 0:
            QMessageBox.warning(
                self,
                "Selección Requerida",
                "Por favor, seleccione una tarea para editar."
            )
            return
        
        task = self.tasks[current_row]
        
        # Verificar si la tarea se puede editar (no es una tarea del sistema)
        if not task.task_id.startswith('custom_'):
            QMessageBox.warning(
                self,
                "Tarea No Editable",
                "Solo se pueden editar tareas personalizadas."
            )
            return
        
        dialog = self._crear_dialogo_nueva_tarea(task)
        if dialog.exec() == QDialog.DialogCode.Accepted:
            task_data = dialog.get_task_data()
            if task_data:
                try:
                    # Actualizar tarea existente
                    task.name = task_data['name']
                    task.description = task_data['description']
                    task.task_type = task_data['task_type']
                    task.schedule = task_data['schedule']
                    task.enabled = task_data['enabled']
                    task.next_run = task._calculate_next_run()
                    
                    self.refresh_scheduled_table()
                    self.save_tasks_to_config()
                    
                    QMessageBox.information(
                        self,
                        "Tarea Actualizada",
                        f"La tarea '{task.name}' se ha actualizado exitosamente."
                    )
                    
                except Exception as e:
                    QMessageBox.critical(
                        self,
                        "Error",
                        f"Error al actualizar la tarea: {str(e)}"
                    )
    
    def delete_selected_task(self):
        """Elimina la tarea seleccionada"""
        current_row = self.scheduled_table.currentRow()
        if current_row >= 0:
            task = self.tasks[current_row]
            reply = QMessageBox.question(
                self,
                "Confirmar Eliminación",
                f"¿Está seguro de que desea eliminar la tarea '{task.name}'?",
                QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No
            )
            
            if reply == QMessageBox.StandardButton.Yes:
                self.tasks.pop(current_row)
                self.refresh_scheduled_table()
                self.save_tasks_to_config()
    
    def export_data_manual(self):
        """Exporta datos manualmente"""
        from PyQt6.QtWidgets import QFileDialog
        import csv
        import os
        
        # Seleccionar directorio de destino
        directory = QFileDialog.getExistingDirectory(
            self,
            "Seleccionar directorio para exportación",
            "",
            QFileDialog.Option.ShowDirsOnly
        )
        
        if not directory:
            return
        
        try:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            
            # Exportar historial de tareas
            self._export_task_history(directory, timestamp)
            
            # Exportar configuración de tareas
            self._export_task_configuration(directory, timestamp)
            
            # Exportar estadísticas del sistema
            self._export_system_stats(directory, timestamp)
            
            QMessageBox.information(
                self,
                "Exportación Completada",
                f"Los datos se han exportado exitosamente a:\n{directory}"
            )
            
        except Exception as e:
            QMessageBox.critical(
                self,
                "Error de Exportación",
                f"Error al exportar los datos: {str(e)}"
            )
    
    def refresh_history(self):
        """Actualiza el historial"""
        # El historial se actualiza automáticamente
        pass
    
    def clear_history(self):
        """Limpia el historial"""
        reply = QMessageBox.question(
            self,
            "Limpiar Historial",
            "¿Está seguro de que desea limpiar todo el historial?",
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No
        )
        
        if reply == QMessageBox.StandardButton.Yes:
            self.history_list.clear()
    
    def save_configuration(self):
        """Guarda la configuración"""
        # Actualizar intervalo del timer
        interval_minutes = self.check_interval_spin.value()
        self.automation_timer.setInterval(interval_minutes * 60000)
        
        QMessageBox.information(
            self,
            "Configuración Guardada",
            "La configuración se ha guardado exitosamente."
        )
    
    def reset_configuration(self):
        """Restaura la configuración predeterminada"""
        self.check_interval_spin.setValue(1)
        self.backup_retention_spin.setValue(30)
        self.notify_success_check.setChecked(False)
        self.notify_errors_check.setChecked(True)
        
        QMessageBox.information(
            self,
            "Configuración Restaurada",
            "La configuración se ha restaurado a los valores predeterminados."
        )
    
    def _crear_dialogo_nueva_tarea(self, task=None):
        """Crea el diálogo para nueva tarea o edición"""
        from PyQt6.QtWidgets import (
            QDialog, QVBoxLayout, QHBoxLayout, QFormLayout,
            QLineEdit, QTextEdit, QComboBox, QSpinBox,
            QCheckBox, QPushButton, QGroupBox, QRadioButton, QLabel
        )
        
        dialog = QDialog(self)
        dialog.setWindowTitle("Editar Tarea" if task else "Nueva Tarea")
        dialog.setModal(True)
        dialog.resize(500, 600)
        
        layout = QVBoxLayout(dialog)
        
        # Información básica
        info_group = QGroupBox("Información Básica")
        info_layout = QFormLayout(info_group)
        
        name_edit = QLineEdit()
        if task:
            name_edit.setText(task.name)
        info_layout.addRow("Nombre:", name_edit)
        
        description_edit = QTextEdit()
        description_edit.setMaximumHeight(80)
        if task:
            description_edit.setPlainText(task.description)
        info_layout.addRow("Descripción:", description_edit)
        
        # Tipo de tarea
        type_combo = QComboBox()
        type_combo.addItems(["backup", "cleanup", "optimization", "report", "custom"])
        if task:
            index = type_combo.findText(task.task_type)
            if index >= 0:
                type_combo.setCurrentIndex(index)
        info_layout.addRow("Tipo:", type_combo)
        
        layout.addWidget(info_group)
        
        # Programación
        schedule_group = QGroupBox("Programación")
        schedule_layout = QVBoxLayout(schedule_group)
        
        # Tipo de programación
        daily_radio = QRadioButton("Diario")
        weekly_radio = QRadioButton("Semanal")
        monthly_radio = QRadioButton("Mensual")
        
        if task and task.schedule.get('type') == 'weekly':
            weekly_radio.setChecked(True)
        elif task and task.schedule.get('type') == 'monthly':
            monthly_radio.setChecked(True)
        else:
            daily_radio.setChecked(True)
        
        schedule_layout.addWidget(daily_radio)
        schedule_layout.addWidget(weekly_radio)
        schedule_layout.addWidget(monthly_radio)
        
        # Hora
        time_layout = QHBoxLayout()
        hour_spin = QSpinBox()
        hour_spin.setRange(0, 23)
        minute_spin = QSpinBox()
        minute_spin.setRange(0, 59)
        
        if task and 'time' in task.schedule:
            time_parts = task.schedule['time'].split(':')
            if len(time_parts) == 2:
                hour_spin.setValue(int(time_parts[0]))
                minute_spin.setValue(int(time_parts[1]))
        
        time_layout.addWidget(QLabel("Hora:"))
        time_layout.addWidget(hour_spin)
        time_layout.addWidget(QLabel(":"))
        time_layout.addWidget(minute_spin)
        time_layout.addStretch()
        
        schedule_layout.addLayout(time_layout)
        layout.addWidget(schedule_group)
        
        # Estado
        enabled_check = QCheckBox("Tarea habilitada")
        if task:
            enabled_check.setChecked(task.enabled)
        else:
            enabled_check.setChecked(True)
        layout.addWidget(enabled_check)
        
        # Botones
        button_layout = QHBoxLayout()
        ok_button = QPushButton("Aceptar")
        cancel_button = QPushButton("Cancelar")
        
        ok_button.clicked.connect(dialog.accept)
        cancel_button.clicked.connect(dialog.reject)
        
        button_layout.addStretch()
        button_layout.addWidget(ok_button)
        button_layout.addWidget(cancel_button)
        
        layout.addLayout(button_layout)
        
        # Método para obtener datos
        def get_task_data():
            if not name_edit.text().strip():
                QMessageBox.warning(dialog, "Error", "El nombre es requerido.")
                return None
            
            schedule_type = 'daily'
            if weekly_radio.isChecked():
                schedule_type = 'weekly'
            elif monthly_radio.isChecked():
                schedule_type = 'monthly'
            
            return {
                'name': name_edit.text().strip(),
                'description': description_edit.toPlainText().strip(),
                'task_type': type_combo.currentText(),
                'schedule': {
                    'type': schedule_type,
                    'time': f"{hour_spin.value():02d}:{minute_spin.value():02d}"
                },
                'enabled': enabled_check.isChecked()
            }
        
        dialog.get_task_data = get_task_data
        return dialog
    
    def _export_task_history(self, directory, timestamp):
        """Exporta el historial de tareas a CSV"""
        import csv
        import os
        
        filename = f"task_history_{timestamp}.csv"
        filepath = os.path.join(directory, filename)
        
        with open(filepath, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(['Timestamp', 'Evento', 'Detalles'])
            
            # Obtener historial de la lista
            for i in range(self.history_list.count()):
                item = self.history_list.item(i)
                if item:
                    text = item.text()
                    # Parsear el texto del historial
                    if '] ' in text:
                        timestamp_part, event_part = text.split('] ', 1)
                        timestamp_part = timestamp_part.replace('[', '')
                        writer.writerow([timestamp_part, event_part, ''])
                    else:
                        writer.writerow(['', text, ''])
    
    def _export_task_configuration(self, directory, timestamp):
        """Exporta la configuración de tareas a CSV"""
        import csv
        import os
        import json
        
        filename = f"task_configuration_{timestamp}.csv"
        filepath = os.path.join(directory, filename)
        
        with open(filepath, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow([
                'ID', 'Nombre', 'Descripción', 'Tipo', 'Habilitada',
                'Programación', 'Última Ejecución', 'Próxima Ejecución'
            ])
            
            for task in self.tasks:
                schedule_str = json.dumps(task.schedule, ensure_ascii=False)
                last_run = task.last_run.strftime('%Y-%m-%d %H:%M:%S') if task.last_run else 'Nunca'
                next_run = task.next_run.strftime('%Y-%m-%d %H:%M:%S') if task.next_run else 'No programada'
                
                writer.writerow([
                    task.task_id,
                    task.name,
                    task.description,
                    task.task_type,
                    'Sí' if task.enabled else 'No',
                    schedule_str,
                    last_run,
                    next_run
                ])
    
    def _export_system_stats(self, directory, timestamp):
        """Exporta estadísticas del sistema a CSV"""
        import csv
        import os
        import psutil
        
        filename = f"system_stats_{timestamp}.csv"
        filepath = os.path.join(directory, filename)
        
        with open(filepath, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(['Métrica', 'Valor', 'Unidad'])
            
            # Estadísticas del sistema
            try:
                cpu_percent = psutil.cpu_percent(interval=1)
                memory = psutil.virtual_memory()
                disk = psutil.disk_usage('/')
                
                writer.writerow(['CPU Usage', f'{cpu_percent:.1f}', '%'])
                writer.writerow(['Memory Total', f'{memory.total / (1024**3):.2f}', 'GB'])
                writer.writerow(['Memory Used', f'{memory.used / (1024**3):.2f}', 'GB'])
                writer.writerow(['Memory Available', f'{memory.available / (1024**3):.2f}', 'GB'])
                writer.writerow(['Disk Total', f'{disk.total / (1024**3):.2f}', 'GB'])
                writer.writerow(['Disk Used', f'{disk.used / (1024**3):.2f}', 'GB'])
                writer.writerow(['Disk Free', f'{disk.free / (1024**3):.2f}', 'GB'])
                
            except Exception as e:
                writer.writerow(['Error', f'No se pudieron obtener estadísticas: {str(e)}', ''])
            
            # Estadísticas de tareas
            total_tasks = len(self.tasks)
            enabled_tasks = sum(1 for task in self.tasks if task.enabled)
            custom_tasks = sum(1 for task in self.tasks if task.task_id.startswith('custom_'))
            
            writer.writerow(['Total Tasks', str(total_tasks), 'count'])
            writer.writerow(['Enabled Tasks', str(enabled_tasks), 'count'])
            writer.writerow(['Custom Tasks', str(custom_tasks), 'count'])
            writer.writerow(['System Tasks', str(total_tasks - custom_tasks), 'count'])
    
    def closeEvent(self, event):
        """Maneja el cierre del widget"""
        # Detener todos los hilos en ejecución
        for executor in self.executor_threads.values():
            executor.stop()
            executor.wait()
        
        # Detener timer
        self.automation_timer.stop()
        
        # Guardar configuración
        self.save_tasks_to_config()
        
        event.accept()

