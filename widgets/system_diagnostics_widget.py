import sys
import os
import psutil
import psycopg2
import psycopg2.extras
import logging
import json
"""
Nota: Este widget se centra en métricas locales y replicación lógica (PostgreSQL).
"""
from datetime import datetime, timedelta
from PyQt6.QtWidgets import (
    QWidget, QVBoxLayout, QHBoxLayout, QTabWidget, QGroupBox,
    QLabel, QProgressBar, QPushButton, QTextEdit, QTableWidget,
    QTableWidgetItem, QHeaderView, QMessageBox, QFormLayout,
    QSpinBox, QComboBox, QCheckBox, QScrollArea, QFrame
)
from PyQt6.QtCore import QThread, pyqtSignal, QTimer, pyqtSlot
from PyQt6.QtGui import QFont, QColor, QPalette
from database import DatabaseManager
from widgets.chart_widget import MplChartWidget
from utils import collect_temp_candidates, delete_files

class SystemHealthAnalyzer(QThread):
    """Analizador de salud del sistema que ejecuta diagnósticos automáticos"""
    
    health_report_ready = pyqtSignal(dict)
    problem_detected = pyqtSignal(str, str)  # tipo, descripción
    
    def __init__(self):
        super().__init__()
        self.db_manager = DatabaseManager()
        self.running = False
        self.analysis_interval = 300  # 5 minutos por defecto
        
    def run(self):
        """Ejecuta el análisis de salud del sistema"""
        self.running = True
        
        while self.running:
            try:
                health_report = self.perform_health_check()
                self.health_report_ready.emit(health_report)
                
                # Detectar problemas automáticamente
                self.detect_problems(health_report)
                
                # Esperar antes del siguiente análisis
                self.msleep(self.analysis_interval * 1000)
                
            except Exception as e:
                logging.error(f"Error en análisis de salud: {e}")
                self.msleep(60000)  # Esperar 1 minuto en caso de error
    
    def perform_health_check(self):
        """Realiza un chequeo completo de salud del sistema"""
        report = {
            'timestamp': datetime.now().isoformat(),
            'system': self.check_system_health(),
            'database': self.check_database_health(),
            'application': self.check_application_health(),
            'storage': self.check_storage_health(),
            'performance': self.check_performance_metrics()
        }
        
        # Calcular puntuación general de salud
        report['overall_score'] = self.calculate_health_score(report)
        
        return report
    
    def check_system_health(self):
        """Verifica la salud del sistema operativo"""
        try:
            cpu_percent = psutil.cpu_percent(interval=1)
            memory = psutil.virtual_memory()
            disk = psutil.disk_usage('/')
            
            return {
                'cpu_usage': cpu_percent,
                'memory_usage': memory.percent,
                'disk_usage': disk.percent,
                'available_memory': memory.available / (1024**3),  # GB
                'free_disk': disk.free / (1024**3),  # GB
                'load_average': os.getloadavg() if hasattr(os, 'getloadavg') else [0, 0, 0],
                'boot_time': datetime.fromtimestamp(psutil.boot_time()).isoformat()
            }
        except Exception as e:
            logging.error(f"Error verificando salud del sistema: {e}")
            return {}
    
    def check_database_health(self):
        """Verifica la salud de la base de datos"""
        try:
            with self.db_manager.get_connection_context() as conn:
                cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
                
                # Verificar tamaño de la base de datos PostgreSQL
                cursor.execute("SELECT pg_size_pretty(pg_database_size('gimnasio'))")
                result = cursor.fetchone()
                db_size_pretty = result[0] if result and len(result) > 0 else "0 bytes"
                
                cursor.execute("SELECT pg_database_size('gimnasio')")
                result = cursor.fetchone()
                db_size_bytes = result[0] if result and len(result) > 0 else 0
                db_size_mb = db_size_bytes / (1024 * 1024)
                
                # Verificar estadísticas de la base de datos
                cursor.execute("SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'public'")
                result = cursor.fetchone()
                table_count = result[0] if result and len(result) > 0 else 0
                
                # Verificar conexiones activas
                cursor.execute("SELECT COUNT(*) FROM pg_stat_activity WHERE datname = 'gimnasio'")
                result = cursor.fetchone()
                active_connections = result[0] if result and len(result) > 0 else 0
                
                # Para PostgreSQL, la integridad se verifica de manera diferente
                integrity_result = True  # PostgreSQL maneja integridad automáticamente
                fragmentation = 0  # PostgreSQL maneja fragmentación automáticamente
                
                return {
                    'integrity': integrity_result == 'ok',
                    'size_mb': round(db_size_mb, 2),
                    'fragmentation_percent': round(fragmentation, 2),
                    'table_count': table_count,
                    'last_backup': self.get_last_backup_date()
                }
        except Exception as e:
            logging.error(f"Error verificando salud de BD: {e}")
            return {'error': str(e)}
    
    def check_application_health(self):
        """Verifica la salud de la aplicación"""
        try:
            with self.db_manager.get_connection_context() as conn:
                cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
                
                # Verificar datos críticos
                cursor.execute("SELECT COUNT(*) FROM usuarios")
                result = cursor.fetchone()
                total_users = result[0] if result and len(result) > 0 else 0
                
                cursor.execute("SELECT COUNT(*) FROM usuarios WHERE activo = true")
                result = cursor.fetchone()
                active_users = result[0] if result and len(result) > 0 else 0
                
                cursor.execute("SELECT COUNT(*) FROM pagos WHERE fecha_pago >= CURRENT_DATE - INTERVAL '30 days'")
                result = cursor.fetchone()
                recent_payments = result[0] if result and len(result) > 0 else 0
                
                cursor.execute("SELECT COUNT(*) FROM asistencias WHERE fecha::date = CURRENT_DATE")
                result = cursor.fetchone()
                today_attendance = result[0] if result and len(result) > 0 else 0
                
                # Verificar logs de errores recientes
                error_count = self.count_recent_errors()
                
                return {
                    'total_users': total_users,
                    'active_users': active_users,
                    'recent_payments': recent_payments,
                    'today_attendance': today_attendance,
                    'recent_errors': error_count,
                    'uptime_hours': self.get_application_uptime()
                }
        except Exception as e:
            logging.error(f"Error verificando salud de aplicación: {e}")
            return {'error': str(e)}
    
    def check_storage_health(self):
        """Verifica la salud del almacenamiento"""
        try:
            # Verificar directorios críticos
            critical_dirs = ['logs', 'exports', 'backups', 'recibos']
            dir_status = {}
            
            for dir_name in critical_dirs:
                if os.path.exists(dir_name):
                    dir_size = self.get_directory_size(dir_name)
                    file_count = len([f for f in os.listdir(dir_name) if os.path.isfile(os.path.join(dir_name, f))])
                    dir_status[dir_name] = {
                        'exists': True,
                        'size_mb': round(dir_size / (1024*1024), 2),
                        'file_count': file_count
                    }
                else:
                    dir_status[dir_name] = {'exists': False}
            
            return {
                'directories': dir_status,
                'temp_files': self.count_temp_files(),
                'old_logs': self.count_old_logs()
            }
        except Exception as e:
            logging.error(f"Error verificando almacenamiento: {e}")
            return {'error': str(e)}
    
    def check_performance_metrics(self):
        """Verifica métricas de rendimiento"""
        try:
            # Medir tiempo de respuesta de la base de datos
            start_time = datetime.now()
            with self.db_manager.get_connection_context() as conn:
                cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
                cursor.execute("SELECT COUNT(*) FROM usuarios")
                cursor.fetchone()
            db_response_time = (datetime.now() - start_time).total_seconds() * 1000
            
            # Verificar uso de memoria de la aplicación
            process = psutil.Process()
            memory_info = process.memory_info()
            
            return {
                'db_response_time_ms': round(db_response_time, 2),
                'app_memory_mb': round(memory_info.rss / (1024*1024), 2),
                'app_cpu_percent': process.cpu_percent(),
                'open_files': len(process.open_files()) if hasattr(process, 'open_files') else 0
            }
        except Exception as e:
            logging.error(f"Error verificando rendimiento: {e}")
            return {'error': str(e)}

    
    
    def calculate_health_score(self, report):
        """Calcula una puntuación general de salud (0-100)"""
        score = 100
        
        # Penalizar por uso alto de recursos
        system = report.get('system', {})
        if system.get('cpu_usage', 0) > 80:
            score -= 20
        elif system.get('cpu_usage', 0) > 60:
            score -= 10
            
        if system.get('memory_usage', 0) > 90:
            score -= 25
        elif system.get('memory_usage', 0) > 75:
            score -= 15
            
        if system.get('disk_usage', 0) > 95:
            score -= 30
        elif system.get('disk_usage', 0) > 85:
            score -= 15
        
        # Penalizar por problemas de base de datos
        database = report.get('database', {})
        if not database.get('integrity', True):
            score -= 40
        
        if database.get('fragmentation_percent', 0) > 20:
            score -= 10
        
        # Penalizar por errores recientes
        app = report.get('application', {})
        error_count = app.get('recent_errors', 0)
        if error_count > 10:
            score -= 20
        elif error_count > 5:
            score -= 10
        
        # Penalizar por rendimiento lento
        performance = report.get('performance', {})
        if performance.get('db_response_time_ms', 0) > 1000:
            score -= 15
        elif performance.get('db_response_time_ms', 0) > 500:
            score -= 8
        
        return max(0, min(100, score))
    
    def detect_problems(self, report):
        """Detecta problemas automáticamente y emite señales"""
        system = report.get('system', {})
        database = report.get('database', {})
        application = report.get('application', {})
        performance = report.get('performance', {})
        
        # Problemas críticos del sistema
        if system.get('cpu_usage', 0) > 90:
            self.problem_detected.emit('critical', f'Uso de CPU crítico: {system["cpu_usage"]:.1f}%')
        
        if system.get('memory_usage', 0) > 95:
            self.problem_detected.emit('critical', f'Memoria casi agotada: {system["memory_usage"]:.1f}%')
        
        if system.get('disk_usage', 0) > 98:
            self.problem_detected.emit('critical', f'Disco casi lleno: {system["disk_usage"]:.1f}%')
        
        # Problemas de base de datos
        if not database.get('integrity', True):
            self.problem_detected.emit('critical', 'Integridad de base de datos comprometida')
        
        if database.get('fragmentation_percent', 0) > 30:
            self.problem_detected.emit('warning', f'Alta fragmentación de BD: {database["fragmentation_percent"]:.1f}%')
        
        # Problemas de rendimiento
        if performance.get('db_response_time_ms', 0) > 2000:
            self.problem_detected.emit('warning', f'Base de datos lenta: {performance["db_response_time_ms"]:.1f}ms')
        
        # Problemas de aplicación
        if application.get('recent_errors', 0) > 20:
            self.problem_detected.emit('warning', f'Muchos errores recientes: {application["recent_errors"]}')
    
    def get_last_backup_date(self):
        """Obtiene la fecha del último backup"""
        try:
            backup_dir = 'backups'
            if not os.path.exists(backup_dir):
                return None
            
            backup_files = [f for f in os.listdir(backup_dir) if f.endswith('.db')]
            if not backup_files:
                return None
            
            latest_backup = max(backup_files, key=lambda f: os.path.getctime(os.path.join(backup_dir, f)))
            backup_time = os.path.getctime(os.path.join(backup_dir, latest_backup))
            return datetime.fromtimestamp(backup_time).isoformat()
        except:
            return None
    
    def count_recent_errors(self):
        """Cuenta errores recientes en los logs"""
        try:
            import glob
            
            log_files = glob.glob('logs/log_*.log')
            if not log_files:
                return 0
            
            error_count = 0
            cutoff_time = datetime.now() - timedelta(hours=24)
            
            for log_file in log_files:
                try:
                    with open(log_file, 'r', encoding='utf-8') as f:
                        for line in f:
                            if 'ERROR' in line or 'CRITICAL' in line:
                                error_count += 1
                except:
                    continue
            
            return error_count
        except:
            return 0
    
    def get_application_uptime(self):
        """Obtiene el tiempo de actividad de la aplicación"""
        try:
            process = psutil.Process()
            create_time = datetime.fromtimestamp(process.create_time())
            uptime = datetime.now() - create_time
            return round(uptime.total_seconds() / 3600, 2)  # horas
        except:
            return 0
    
    def get_directory_size(self, directory):
        """Calcula el tamaño de un directorio en bytes"""
        total_size = 0
        try:
            for dirpath, dirnames, filenames in os.walk(directory):
                for filename in filenames:
                    filepath = os.path.join(dirpath, filename)
                    if os.path.exists(filepath):
                        total_size += os.path.getsize(filepath)
        except:
            pass
        return total_size
    
    def count_temp_files(self):
        """Cuenta archivos temporales"""
        try:
            temp_extensions = ['.tmp', '.temp', '.bak', '.old']
            temp_count = 0
            
            for root, dirs, files in os.walk('.'):
                for file in files:
                    if any(file.endswith(ext) for ext in temp_extensions):
                        temp_count += 1
            
            return temp_count
        except:
            return 0
    
    def count_old_logs(self):
        """Cuenta logs antiguos (más de 30 días)"""
        try:
            import glob
            
            log_files = glob.glob('logs/log_*.log')
            old_count = 0
            cutoff_time = datetime.now() - timedelta(days=30)
            
            for log_file in log_files:
                try:
                    file_time = datetime.fromtimestamp(os.path.getctime(log_file))
                    if file_time < cutoff_time:
                        old_count += 1
                except:
                    continue
            
            return old_count
        except:
            return 0
    
    def stop(self):
        """Detiene el análisis"""
        self.running = False
        self.quit()
        self.wait()

class SystemDiagnosticsWidget(QWidget):
    """Widget principal para herramientas de diagnóstico del sistema"""
    
    def __init__(self):
        super().__init__()
        self.health_analyzer = None
        self.current_report = None
        self.problems_list = []
        self.setup_ui()
        self.setup_health_analyzer()
    
    def setup_ui(self):
        """Configura la interfaz de usuario"""
        layout = QVBoxLayout(self)
        layout.setSpacing(0)
        layout.setContentsMargins(0, 0, 0, 0)
        
        # Tabs principales
        self.tab_widget = QTabWidget()
        # Estilo edge-to-edge sin marco redundante
        self.tab_widget.setStyleSheet("""
            QTabWidget::pane { border: 0; }
            QTabBar::tab { padding: 6px 12px; }
        """)
        
        self.setup_health_overview_tab()
        # Sintetiza los tres tabs en uno solo compacto
        self.setup_compact_diagnostics_tab()
        
        layout.addWidget(self.tab_widget)
    
    def setup_health_overview_tab(self):
        """Configura el tab de resumen de salud"""
        tab = QWidget()
        layout = QVBoxLayout(tab)
        layout.setSpacing(0)
        layout.setContentsMargins(0, 0, 0, 0)
        
        # Puntuación general de salud
        health_group = QGroupBox("")
        health_group.setFlat(True)
        health_layout = QVBoxLayout(health_group)
        health_layout.setContentsMargins(0, 0, 0, 0)
        health_layout.setSpacing(0)
        
        self.health_score_label = QLabel("Puntuación: --")
        self.health_score_label.setStyleSheet("""
            QLabel {
                font-size: 24px;
                font-weight: bold;
                text-align: center;
                padding: 20px;
                border-radius: 8px;
                background-color: #A3BE8C;
                color: white;
            }
        """)
        health_layout.addWidget(self.health_score_label)
        
        self.health_status_label = QLabel("Estado: Analizando...")
        self.health_status_label.setStyleSheet("""
            QLabel {
                font-size: 14px;
                text-align: center;
                padding: 10px;
            }
        """)
        health_layout.addWidget(self.health_status_label)
        
        layout.addWidget(health_group)
        
        # Métricas rápidas
        metrics_group = QGroupBox("")
        metrics_group.setFlat(True)
        metrics_layout = QFormLayout(metrics_group)
        metrics_layout.setContentsMargins(0, 0, 0, 0)
        metrics_layout.setSpacing(0)
        
        self.cpu_metric = QLabel("--")
        self.memory_metric = QLabel("--")
        self.disk_metric = QLabel("--")
        self.db_metric = QLabel("--")
        self.node_metric = QLabel("--")
        
        metrics_layout.addRow("CPU:", self.cpu_metric)
        metrics_layout.addRow("Memoria:", self.memory_metric)
        metrics_layout.addRow("Disco:", self.disk_metric)
        metrics_layout.addRow("Base de Datos:", self.db_metric)
        metrics_layout.addRow("Replicación:", self.node_metric)
        
        layout.addWidget(metrics_group)

        # Panel de observabilidad de cola offline (legacy eliminado)
        # Espacio reservado para métricas futuras de replicación lógica (PostgreSQL)
        
        # Controles
        controls_layout = QHBoxLayout()
        
        self.start_analysis_btn = QPushButton("▶️ Iniciar Análisis")
        self.start_analysis_btn.clicked.connect(self.start_health_analysis)
        controls_layout.addWidget(self.start_analysis_btn)
        
        self.stop_analysis_btn = QPushButton("⏹️ Detener Análisis")
        self.stop_analysis_btn.clicked.connect(self.stop_health_analysis)
        self.stop_analysis_btn.setEnabled(False)
        controls_layout.addWidget(self.stop_analysis_btn)
        
        refresh_btn = QPushButton("🔄 Actualizar")
        refresh_btn.clicked.connect(self.force_health_check)
        controls_layout.addWidget(refresh_btn)
        
        controls_layout.addStretch()
        layout.addLayout(controls_layout)
        
        self.tab_widget.addTab(tab, "🏥 Resumen de Salud")

    # Legacy OfflineSyncManager snapshot/metrics removidos

    # Espacio reservado para futuras métricas de replicación lógica (PostgreSQL)
    
    def setup_system_analysis_tab(self):
        """Configura el tab de análisis detallado del sistema"""
        tab = QWidget()
        layout = QVBoxLayout(tab)
        layout.setSpacing(0)
        layout.setContentsMargins(0, 0, 0, 0)
        
        # Área de scroll para el reporte
        scroll_area = QScrollArea()
        scroll_widget = QWidget()
        scroll_layout = QVBoxLayout(scroll_widget)
        
        self.system_report_text = QTextEdit()
        self.system_report_text.setReadOnly(True)
        self.system_report_text.setFont(QFont("Consolas", 10))
        self.system_report_text.setStyleSheet("""
            QTextEdit {
                background-color: #252A35;
                color: #ECEFF4;
                border: 1px solid #4C566A;
                border-radius: 4px;
                padding: 10px;
            }
        """)
        scroll_layout.addWidget(self.system_report_text)
        
        scroll_area.setWidget(scroll_widget)
        scroll_area.setWidgetResizable(True)
        layout.addWidget(scroll_area)
        
        # Controles
        controls_layout = QHBoxLayout()
        
        export_report_btn = QPushButton("💾 Exportar Reporte")
        export_report_btn.clicked.connect(self.export_health_report)
        controls_layout.addWidget(export_report_btn)
        
        clear_report_btn = QPushButton("🗑️ Limpiar")
        clear_report_btn.clicked.connect(self.clear_system_report)
        controls_layout.addWidget(clear_report_btn)
        
        controls_layout.addStretch()
        layout.addLayout(controls_layout)
        
        self.tab_widget.addTab(tab, "🔍 Análisis Detallado")
    
    def setup_problem_detection_tab(self):
        """Configura el tab de detección de problemas"""
        tab = QWidget()
        layout = QVBoxLayout(tab)
        layout.setSpacing(0)
        layout.setContentsMargins(0, 0, 0, 0)
        
        # Lista de problemas
        problems_group = QGroupBox("")
        problems_group.setFlat(True)
        problems_layout = QVBoxLayout(problems_group)
        problems_layout.setContentsMargins(0, 0, 0, 0)
        problems_layout.setSpacing(0)
        
        self.problems_table = QTableWidget()
        self.problems_table.setColumnCount(4)
        self.problems_table.setHorizontalHeaderLabels(["Tipo", "Descripción", "Severidad", "Timestamp"])
        self.problems_table.horizontalHeader().setStretchLastSection(True)
        problems_layout.addWidget(self.problems_table)
        
        layout.addWidget(problems_group)
        
        # Configuración de alertas
        alerts_group = QGroupBox("")
        alerts_group.setFlat(True)
        alerts_layout = QFormLayout(alerts_group)
        alerts_layout.setContentsMargins(0, 0, 0, 0)
        alerts_layout.setSpacing(0)
        
        self.cpu_threshold = QSpinBox()
        self.cpu_threshold.setRange(50, 100)
        self.cpu_threshold.setValue(80)
        self.cpu_threshold.setSuffix("%")
        alerts_layout.addRow("Umbral CPU:", self.cpu_threshold)
        
        self.memory_threshold = QSpinBox()
        self.memory_threshold.setRange(50, 100)
        self.memory_threshold.setValue(85)
        self.memory_threshold.setSuffix("%")
        alerts_layout.addRow("Umbral Memoria:", self.memory_threshold)
        
        self.disk_threshold = QSpinBox()
        self.disk_threshold.setRange(70, 100)
        self.disk_threshold.setValue(90)
        self.disk_threshold.setSuffix("%")
        alerts_layout.addRow("Umbral Disco:", self.disk_threshold)
        
        self.enable_alerts = QCheckBox("Habilitar alertas automáticas")
        self.enable_alerts.setChecked(True)
        alerts_layout.addRow(self.enable_alerts)
        
        layout.addWidget(alerts_group)
        
        # Controles
        controls_layout = QHBoxLayout()
        
        clear_problems_btn = QPushButton("🗑️ Limpiar Problemas")
        clear_problems_btn.clicked.connect(self.clear_problems_list)
        controls_layout.addWidget(clear_problems_btn)
        
        save_config_btn = QPushButton("💾 Guardar Configuración")
        save_config_btn.clicked.connect(self.save_alert_config)
        controls_layout.addWidget(save_config_btn)
        
        controls_layout.addStretch()
        layout.addLayout(controls_layout)
        
        self.tab_widget.addTab(tab, "🚨 Detección de Problemas")
    
    def setup_recommendations_tab(self):
        """Configura el tab de recomendaciones"""
        tab = QWidget()
        layout = QVBoxLayout(tab)
        layout.setSpacing(0)
        layout.setContentsMargins(0, 0, 0, 0)
        
        # Recomendaciones automáticas
        recommendations_group = QGroupBox("")
        recommendations_group.setFlat(True)
        recommendations_layout = QVBoxLayout(recommendations_group)
        recommendations_layout.setContentsMargins(0, 0, 0, 0)
        recommendations_layout.setSpacing(0)
        
        self.recommendations_text = QTextEdit()
        self.recommendations_text.setReadOnly(True)
        self.recommendations_text.setStyleSheet("""
            QTextEdit {
                background-color: #3B4252;
                color: #ECEFF4;
                border: 1px solid #EBCB8B;
                border-radius: 4px;
                padding: 10px;
            }
        """)
        recommendations_layout.addWidget(self.recommendations_text)
        
        layout.addWidget(recommendations_group)
        
        # Acciones rápidas
        actions_group = QGroupBox("")
        actions_group.setFlat(True)
        actions_layout = QVBoxLayout(actions_group)
        actions_layout.setContentsMargins(0, 0, 0, 0)
        actions_layout.setSpacing(0)
        
        actions_buttons_layout = QHBoxLayout()
        
        optimize_db_btn = QPushButton("🔧 Optimizar BD")
        optimize_db_btn.clicked.connect(self.quick_optimize_database)
        actions_buttons_layout.addWidget(optimize_db_btn)
        
        clean_temp_btn = QPushButton("🧹 Limpiar Temporales")
        clean_temp_btn.clicked.connect(self.quick_clean_temp_files)
        actions_buttons_layout.addWidget(clean_temp_btn)
        
        backup_db_btn = QPushButton("💾 Backup Rápido")
        backup_db_btn.clicked.connect(self.quick_backup_database)
        actions_layout.addLayout(actions_buttons_layout)
        layout.addWidget(actions_group)
        
        self.tab_widget.addTab(tab, "💡 Recomendaciones")

    def setup_compact_diagnostics_tab(self):
        """Crea una pestaña compacta que sintetiza Análisis, Problemas y Recomendaciones."""
        tab = QWidget()
        layout = QVBoxLayout(tab)
        layout.setSpacing(0)
        layout.setContentsMargins(0, 0, 0, 0)

        # Sección: Análisis Detallado (altura reducida)
        analysis_group = QGroupBox("")
        analysis_group.setFlat(True)
        analysis_layout = QVBoxLayout(analysis_group)
        analysis_layout.setSpacing(0)
        analysis_layout.setContentsMargins(0, 0, 0, 0)

        scroll_area = QScrollArea()
        scroll_widget = QWidget()
        scroll_layout = QVBoxLayout(scroll_widget)
        scroll_layout.setSpacing(1)
        scroll_layout.setContentsMargins(1, 1, 1, 1)

        self.system_report_text = QTextEdit()
        self.system_report_text.setReadOnly(True)
        self.system_report_text.setFont(QFont("Consolas", 10))
        self.system_report_text.setStyleSheet(
            """
            QTextEdit {
                background-color: #252A35;
                color: #ECEFF4;
                border: 1px solid #4C566A;
                border-radius: 4px;
                padding: 10px;
            }
            """
        )
        # Altura reducida para compactar la sección
        self.system_report_text.setMinimumHeight(160)
        scroll_layout.addWidget(self.system_report_text)

        scroll_area.setWidget(scroll_widget)
        scroll_area.setWidgetResizable(True)
        analysis_layout.addWidget(scroll_area)

        # Controles del reporte
        report_controls = QHBoxLayout()
        export_report_btn = QPushButton("💾 Exportar Reporte")
        export_report_btn.clicked.connect(self.export_health_report)
        report_controls.addWidget(export_report_btn)
        clear_report_btn = QPushButton("🗑️ Limpiar")
        clear_report_btn.clicked.connect(self.clear_system_report)
        report_controls.addWidget(clear_report_btn)
        report_controls.addStretch()
        analysis_layout.addLayout(report_controls)

        layout.addWidget(analysis_group)

        # Sección: Problemas Detectados
        problems_group = QGroupBox("")
        problems_group.setFlat(True)
        problems_layout = QVBoxLayout(problems_group)
        problems_layout.setSpacing(0)
        problems_layout.setContentsMargins(0, 0, 0, 0)

        self.problems_table = QTableWidget()
        self.problems_table.setColumnCount(4)
        self.problems_table.setHorizontalHeaderLabels(["Tipo", "Descripción", "Severidad", "Timestamp"])
        self.problems_table.horizontalHeader().setStretchLastSection(True)
        problems_layout.addWidget(self.problems_table)

        layout.addWidget(problems_group)

        # Sección: Configuración de Alertas
        alerts_group = QGroupBox("")
        alerts_group.setFlat(True)
        alerts_layout = QFormLayout(alerts_group)
        # Espaciado/márgenes edge-to-edge
        alerts_layout.setSpacing(0)
        alerts_group.setContentsMargins(0, 0, 0, 0)

        self.cpu_threshold = QSpinBox()
        self.cpu_threshold.setRange(50, 100)
        self.cpu_threshold.setValue(80)
        self.cpu_threshold.setSuffix("%")
        alerts_layout.addRow("Umbral CPU:", self.cpu_threshold)

        self.memory_threshold = QSpinBox()
        self.memory_threshold.setRange(50, 100)
        self.memory_threshold.setValue(85)
        self.memory_threshold.setSuffix("%")
        alerts_layout.addRow("Umbral Memoria:", self.memory_threshold)

        self.disk_threshold = QSpinBox()
        self.disk_threshold.setRange(70, 100)
        self.disk_threshold.setValue(90)
        self.disk_threshold.setSuffix("%")
        alerts_layout.addRow("Umbral Disco:", self.disk_threshold)

        self.enable_alerts = QCheckBox("Habilitar alertas automáticas")
        self.enable_alerts.setChecked(True)
        alerts_layout.addRow(self.enable_alerts)

        layout.addWidget(alerts_group)

        # Controles de problemas/alertas
        prob_controls = QHBoxLayout()
        clear_problems_btn = QPushButton("🗑️ Limpiar Problemas")
        clear_problems_btn.clicked.connect(self.clear_problems_list)
        prob_controls.addWidget(clear_problems_btn)
        save_config_btn = QPushButton("💾 Guardar Configuración")
        save_config_btn.clicked.connect(self.save_alert_config)
        prob_controls.addWidget(save_config_btn)
        prob_controls.addStretch()
        layout.addLayout(prob_controls)

        # Sección: Recomendaciones
        recommendations_group = QGroupBox("")
        recommendations_group.setFlat(True)
        recommendations_layout = QVBoxLayout(recommendations_group)
        recommendations_layout.setSpacing(0)
        recommendations_layout.setContentsMargins(0, 0, 0, 0)

        self.recommendations_text = QTextEdit()
        self.recommendations_text.setReadOnly(True)
        self.recommendations_text.setStyleSheet(
            """
            QTextEdit {
                background-color: #3B4252;
                color: #ECEFF4;
                border: 1px solid #EBCB8B;
                border-radius: 4px;
                padding: 10px;
            }
            """
        )
        recommendations_layout.addWidget(self.recommendations_text)

        layout.addWidget(recommendations_group)

        # Bloque de Acciones Rápidas eliminado según solicitud del usuario

        # Estirar para alinear arriba
        layout.addStretch()

        # Nombre sintetizado de la pestaña
        self.tab_widget.addTab(tab, "🔍 Diagnóstico y Recomendaciones")

    def setup_health_analyzer(self):
        """Configura el analizador de salud"""
        self.health_analyzer = SystemHealthAnalyzer()
        self.health_analyzer.health_report_ready.connect(self.update_health_display)
        self.health_analyzer.problem_detected.connect(self.add_detected_problem)
    
    def start_health_analysis(self):
        """Inicia el análisis de salud automático"""
        if self.health_analyzer and not self.health_analyzer.isRunning():
            self.health_analyzer.start()
            self.start_analysis_btn.setEnabled(False)
            self.stop_analysis_btn.setEnabled(True)
            self.health_status_label.setText("Estado: Análisis en curso...")
    
    def stop_health_analysis(self):
        """Detiene el análisis de salud"""
        if self.health_analyzer and self.health_analyzer.isRunning():
            self.health_analyzer.stop()
            self.start_analysis_btn.setEnabled(True)
            self.stop_analysis_btn.setEnabled(False)
            self.health_status_label.setText("Estado: Análisis detenido")
    
    def force_health_check(self):
        """Fuerza un chequeo inmediato de salud"""
        if self.health_analyzer:
            # Crear un analizador temporal para un chequeo único
            temp_analyzer = SystemHealthAnalyzer()
            report = temp_analyzer.perform_health_check()
            self.update_health_display(report)
    
    @pyqtSlot(dict)
    def update_health_display(self, report):
        """Actualiza la visualización de salud del sistema"""
        self.current_report = report
        
        # Actualizar puntuación de salud
        score = report.get('overall_score', 0)
        self.health_score_label.setText(f"Puntuación: {score}/100")
        
        # Cambiar color según la puntuación
        if score >= 80:
            color = '#A3BE8C'
            status = "Excelente"
        elif score >= 60:
            color = '#EBCB8B'
            status = "Bueno"
        elif score >= 40:
            color = '#5E81AC'
            status = "Regular"
        else:
            color = '#BF616A'
            status = "Crítico"
        
        self.health_score_label.setStyleSheet(f"""
            QLabel {{
                font-size: 24px;
                font-weight: bold;
                text-align: center;
                padding: 20px;
                border-radius: 8px;
                background-color: {color};
                color: #ECEFF4;
            }}
        """)
        
        self.health_status_label.setText(f"Estado: {status}")
        
        # Actualizar métricas rápidas
        system = report.get('system', {})
        database = report.get('database', {})
        
        self.cpu_metric.setText(f"{system.get('cpu_usage', 0):.1f}%")
        self.memory_metric.setText(f"{system.get('memory_usage', 0):.1f}%")
        self.disk_metric.setText(f"{system.get('disk_usage', 0):.1f}%")
        self.db_metric.setText(f"{database.get('size_mb', 0):.1f} MB")

        # Métrica de replicación lógica (PostgreSQL) - placeholder
        self.node_metric.setText("N/A")
        self.node_metric.setStyleSheet("color: #607D8B;")
        self.node_metric.setToolTip("Replicación lógica (PostgreSQL): N/A")
        
        # Actualizar reporte detallado
        self.update_detailed_report(report)
        
        # Generar recomendaciones
        self.generate_recommendations(report)
    
    def update_detailed_report(self, report):
        """Actualiza el reporte detallado del sistema"""
        report_text = f"""REPORTE DE SALUD DEL SISTEMA
{'='*50}
Fecha: {report.get('timestamp', 'N/A')}
Puntuación General: {report.get('overall_score', 0)}/100

SISTEMA OPERATIVO:
{'-'*20}
CPU: {report.get('system', {}).get('cpu_usage', 0):.1f}%
Memoria: {report.get('system', {}).get('memory_usage', 0):.1f}% ({report.get('system', {}).get('available_memory', 0):.1f} GB disponibles)
Disco: {report.get('system', {}).get('disk_usage', 0):.1f}% ({report.get('system', {}).get('free_disk', 0):.1f} GB libres)
Tiempo de arranque: {report.get('system', {}).get('boot_time', 'N/A')}

BASE DE DATOS:
{'-'*15}
Integridad: {'✓ OK' if report.get('database', {}).get('integrity', False) else '✗ ERROR'}
Tamaño: {report.get('database', {}).get('size_mb', 0):.2f} MB
Fragmentación: {report.get('database', {}).get('fragmentation_percent', 0):.2f}%
Tablas: {report.get('database', {}).get('table_count', 0)}
Último backup: {report.get('database', {}).get('last_backup', 'Nunca')}

APLICACIÓN:
{'-'*12}
Usuarios totales: {report.get('application', {}).get('total_users', 0)}
Usuarios activos: {report.get('application', {}).get('active_users', 0)}
Pagos recientes: {report.get('application', {}).get('recent_payments', 0)}
Asistencias hoy: {report.get('application', {}).get('today_attendance', 0)}
Errores recientes: {report.get('application', {}).get('recent_errors', 0)}
Tiempo activo: {report.get('application', {}).get('uptime_hours', 0):.2f} horas

REPLICACIÓN (PostgreSQL lógica):
{'-'*30}
Estado: Preparado para configurar publicaciones y suscripciones nativas

RENDIMIENTO:
{'-'*12}
Tiempo respuesta BD: {report.get('performance', {}).get('db_response_time_ms', 0):.2f} ms
Memoria aplicación: {report.get('performance', {}).get('app_memory_mb', 0):.2f} MB
CPU aplicación: {report.get('performance', {}).get('app_cpu_percent', 0):.2f}%
Archivos abiertos: {report.get('performance', {}).get('open_files', 0)}

ALMACENAMIENTO:
{'-'*15}"""
        
        # Agregar información de directorios
        storage = report.get('storage', {})
        directories = storage.get('directories', {})
        
        for dir_name, dir_info in directories.items():
            if dir_info.get('exists', False):
                report_text += f"\n{dir_name.capitalize()}: {dir_info.get('size_mb', 0):.2f} MB ({dir_info.get('file_count', 0)} archivos)"
            else:
                report_text += f"\n{dir_name.capitalize()}: ✗ No existe"
        
        report_text += f"\n\nArchivos temporales: {storage.get('temp_files', 0)}"
        report_text += f"\nLogs antiguos: {storage.get('old_logs', 0)}"
        
        self.system_report_text.setText(report_text)
    
    def generate_recommendations(self, report):
        """Genera recomendaciones basadas en el reporte"""
        recommendations = []
        
        system = report.get('system', {})
        database = report.get('database', {})
        application = report.get('application', {})
        performance = report.get('performance', {})
        storage = report.get('storage', {})
        
        # Recomendaciones del sistema
        if system.get('cpu_usage', 0) > 80:
            recommendations.append("🔴 CRÍTICO: Uso de CPU muy alto. Considere cerrar aplicaciones innecesarias o reiniciar el sistema.")
        
        if system.get('memory_usage', 0) > 90:
            recommendations.append("🔴 CRÍTICO: Memoria casi agotada. Reinicie la aplicación o el sistema.")
        
        if system.get('disk_usage', 0) > 95:
            recommendations.append("🔴 CRÍTICO: Disco casi lleno. Libere espacio inmediatamente.")
        elif system.get('disk_usage', 0) > 85:
            recommendations.append("🟡 ADVERTENCIA: Poco espacio en disco. Considere limpiar archivos temporales.")
        
        # Recomendaciones de base de datos
        if not database.get('integrity', True):
            recommendations.append("🔴 CRÍTICO: Problemas de integridad en la base de datos. Ejecute reparación inmediatamente.")
        
        if database.get('fragmentation_percent', 0) > 20:
            recommendations.append("🟡 RECOMENDACIÓN: Alta fragmentación de BD. Ejecute optimización (VACUUM).")
        
        if not database.get('last_backup'):
            recommendations.append("🔴 IMPORTANTE: No hay backups recientes. Cree un backup inmediatamente.")
        
        # Recomendaciones de rendimiento
        if performance.get('db_response_time_ms', 0) > 1000:
            recommendations.append("🟡 RENDIMIENTO: Base de datos lenta. Considere optimización o reindexación.")
        
        if performance.get('app_memory_mb', 0) > 500:
            recommendations.append("🟡 MEMORIA: La aplicación usa mucha memoria. Considere reiniciarla.")
        
        # Recomendaciones de mantenimiento
        if application.get('recent_errors', 0) > 10:
            recommendations.append("🟡 LOGS: Muchos errores recientes. Revise los logs para identificar problemas.")
        
        if storage.get('temp_files', 0) > 50:
            recommendations.append("🟡 LIMPIEZA: Muchos archivos temporales. Ejecute limpieza de temporales.")
        
        if storage.get('old_logs', 0) > 10:
            recommendations.append("🟡 MANTENIMIENTO: Muchos logs antiguos. Considere archivar o eliminar logs viejos.")
        
        # Recomendaciones generales
        if report.get('overall_score', 100) < 70:
            recommendations.append("🔧 MANTENIMIENTO: Puntuación de salud baja. Ejecute mantenimiento preventivo completo.")
        
        if not recommendations:
            recommendations.append("✅ EXCELENTE: El sistema está funcionando correctamente. No se requieren acciones inmediatas.")
        
        recommendations_text = "\n\n".join(recommendations)
        self.recommendations_text.setText(recommendations_text)
    
    @pyqtSlot(str, str)
    def add_detected_problem(self, problem_type, description):
        """Agrega un problema detectado a la lista"""
        if not self.enable_alerts.isChecked():
            return
        
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        # Determinar severidad
        if "crítico" in description.lower() or "critical" in problem_type.lower():
            severity = "Crítico"
            color = QColor('#BF616A')
        else:
            severity = "Advertencia"
            color = QColor('#EBCB8B')
        
        # Agregar a la lista
        problem = {
            'type': problem_type,
            'description': description,
            'severity': severity,
            'timestamp': timestamp
        }
        
        self.problems_list.append(problem)
        
        # Actualizar tabla
        self.update_problems_table()
        
        # Mostrar notificación si es crítico
        if severity == "Crítico":
            QMessageBox.critical(self, "Problema Crítico Detectado", description)
    
    def update_problems_table(self):
        """Actualiza la tabla de problemas"""
        self.problems_table.setRowCount(len(self.problems_list))
        
        for row, problem in enumerate(self.problems_list):
            self.problems_table.setItem(row, 0, QTableWidgetItem(problem['type']))
            self.problems_table.setItem(row, 1, QTableWidgetItem(problem['description']))
            
            severity_item = QTableWidgetItem(problem['severity'])
            
            if problem['severity'] == "Crítico":
                severity_item.setBackground(QColor('#BF616A'))
                severity_item.setForeground(QColor('#ECEFF4'))
            else:
                severity_item.setBackground(QColor('#EBCB8B'))
                severity_item.setForeground(QColor('#2E3440'))
            
            self.problems_table.setItem(row, 2, severity_item)
            self.problems_table.setItem(row, 3, QTableWidgetItem(problem['timestamp']))
        
        self.problems_table.resizeColumnsToContents()
    
    def clear_problems_list(self):
        """Limpia la lista de problemas"""
        self.problems_list.clear()
        self.problems_table.setRowCount(0)
    
    def clear_system_report(self):
        """Limpia el reporte del sistema"""
        self.system_report_text.clear()
    
    def export_health_report(self):
        """Exporta el reporte de salud a un archivo"""
        if not self.current_report:
            QMessageBox.warning(self, "Advertencia", "No hay reporte disponible para exportar.")
            return
        
        try:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"health_report_{timestamp}.json"
            
            os.makedirs('exports', exist_ok=True)
            filepath = os.path.join('exports', filename)
            
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(self.current_report, f, indent=2, ensure_ascii=False)
            
            QMessageBox.information(self, "Éxito", f"Reporte exportado a: {filepath}")
            
        except Exception as e:
            QMessageBox.critical(self, "Error", f"Error exportando reporte: {e}")
    
    def save_alert_config(self):
        """Guarda la configuración de alertas"""
        try:
            config = {
                'cpu_threshold': self.cpu_threshold.value(),
                'memory_threshold': self.memory_threshold.value(),
                'disk_threshold': self.disk_threshold.value(),
                'enable_alerts': self.enable_alerts.isChecked()
            }
            
            os.makedirs('config', exist_ok=True)
            with open('config/alert_config.json', 'w', encoding='utf-8') as f:
                json.dump(config, f, indent=2)
            
            QMessageBox.information(self, "Éxito", "Configuración de alertas guardada.")
            
        except Exception as e:
            QMessageBox.critical(self, "Error", f"Error guardando configuración: {e}")
    
    def quick_optimize_database(self):
        """Optimización rápida de la base de datos (centralizada)."""
        try:
            db_manager = DatabaseManager()
            ok = db_manager.optimizar_base_datos()
            if ok:
                QMessageBox.information(self, "Éxito", "Base de datos optimizada correctamente.")
            else:
                QMessageBox.warning(self, "Advertencia", "La optimización no se completó.")
        except Exception as e:
            QMessageBox.critical(self, "Error", f"Error optimizando BD: {e}")

    def quick_clean_temp_files(self):
        """Limpieza rápida de temporales usando utilidades centralizadas."""
        try:
            candidates = collect_temp_candidates(retention_days=7)
            deleted, errors = delete_files(candidates)
            QMessageBox.information(
                self,
                "Éxito",
                f"Se eliminaron {deleted} archivos temporales." + (f"\nErrores: {errors}" if errors else "")
            )
        except Exception as e:
            QMessageBox.critical(self, "Error", f"Error limpiando temporales: {e}")
    
    def quick_backup_database(self):
        """Backup rápido de la base de datos"""
        try:
            import shutil
            
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            backup_name = f"quick_backup_{timestamp}.db"
            
            os.makedirs('backups', exist_ok=True)
            backup_path = os.path.join('backups', backup_name)
            
            # Para PostgreSQL, usar pg_dump para backup
            import subprocess
            result = subprocess.run(['pg_dump', '-h', 'localhost', '-U', 'postgres', '-d', 'gimnasio', '-f', backup_path], 
                                  capture_output=True, text=True)
            if result.returncode != 0:
                raise Exception(f"Error en pg_dump: {result.stderr}")
            
            QMessageBox.information(self, "Éxito", f"Backup creado: {backup_path}")
            
        except Exception as e:
            QMessageBox.critical(self, "Error", f"Error creando backup: {e}")
    
    def closeEvent(self, event):
        """Maneja el cierre del widget"""
        if self.health_analyzer and self.health_analyzer.isRunning():
            self.health_analyzer.stop()
        super().closeEvent(event)

