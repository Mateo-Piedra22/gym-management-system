import json
import csv
from datetime import datetime, timedelta
import psycopg2.extras
from typing import Dict, List, Optional
from PyQt6.QtWidgets import (
    QWidget, QVBoxLayout, QHBoxLayout, QGridLayout, QGroupBox,
    QLabel, QPushButton, QTableWidget, QTableWidgetItem, QTabWidget,
    QDateEdit, QComboBox, QTextEdit, QProgressBar, QMessageBox,
    QFileDialog, QCheckBox, QSpinBox, QFrame, QScrollArea, QSizePolicy
)
from PyQt6.QtCore import QThread, pyqtSignal, QTimer, Qt, QDate
from PyQt6.QtGui import QColor, QFont, QPalette
import logging
from utils_modules.users_loader import load_users_cached_async
from utils_modules.ui_constants import (
    PLACEHOLDER_LOADING_USERS,
    PLACEHOLDER_NO_USERS,
)

class AuditAnalysisThread(QThread):
    """Hilo para análisis de auditoría en segundo plano"""
    
    progress_updated = pyqtSignal(int)
    analysis_complete = pyqtSignal(dict)
    
    def __init__(self, db_manager, filters):
        super().__init__()
        self.db_manager = db_manager
        self.filters = filters
        
    def run(self):
        """Ejecuta el análisis de auditoría"""
        try:
            results = {}
            
            # Progreso inicial
            self.progress_updated.emit(10)
            
            # Análisis de actividad por usuario
            results['user_activity'] = self._analyze_user_activity()
            self.progress_updated.emit(25)
            
            # Análisis de actividad por módulo
            results['module_activity'] = self._analyze_module_activity()
            self.progress_updated.emit(40)
            
            # Patrones de acceso
            results['access_patterns'] = self._analyze_access_patterns()
            self.progress_updated.emit(60)
            
            # Actividades inusuales
            results['unusual_activities'] = self._detect_unusual_activities()
            self.progress_updated.emit(80)
            
            # Estadísticas generales
            results['general_stats'] = self._get_general_stats()
            self.progress_updated.emit(100)
            
            self.analysis_complete.emit(results)
            
        except Exception as e:
            logging.error(f"Error en análisis de auditoría: {e}")
            self.analysis_complete.emit({})
            
    def _analyze_user_activity(self) -> List[Dict]:
        """Analiza la actividad por usuario"""
        try:
            with self.db_manager.get_connection_context() as conn:
                cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
                
                # Verificar si existe la tabla audit_logs y tiene datos
                cursor.execute("SELECT COUNT(*) as count FROM audit_logs")
                result = cursor.fetchone()
                
                if not result or result['count'] == 0:
                    logging.warning("No hay datos de auditoría disponibles")
                    return []
                
                # Obtener actividad por usuario en el período especificado
                query = """
                    SELECT 
                        al.user_id,
                        COALESCE(u.nombre, 'Usuario desconocido') as usuario,
                        COUNT(*) as total_acciones,
                        COUNT(CASE WHEN al.action = 'CREATE' THEN 1 END) as creaciones,
                        COUNT(CASE WHEN al.action = 'UPDATE' THEN 1 END) as modificaciones,
                        COUNT(CASE WHEN al.action = 'DELETE' THEN 1 END) as eliminaciones,
                        COUNT(CASE WHEN al.action = 'READ' THEN 1 END) as lecturas,
                        MIN(al.timestamp) as primera_actividad,
                        MAX(al.timestamp) as ultima_actividad
                    FROM audit_logs al
                    LEFT JOIN usuarios u ON al.user_id = u.id
                    WHERE al.timestamp >= %s AND al.timestamp <= %s
                    GROUP BY al.user_id, u.nombre
                    ORDER BY total_acciones DESC
                """
                
                start_date = self.filters.get('start_date', (datetime.now() - timedelta(days=30)).isoformat())
                end_date = self.filters.get('end_date', datetime.now().isoformat())
                
                cursor.execute(query, (start_date, end_date))
                columns = [desc[0] for desc in cursor.description]
                return [dict(zip(columns, row)) for row in cursor.fetchall()]
                
        except Exception as e:
            logging.error(f"Error analizando actividad de usuarios: {e}")
            return []
            
    def _analyze_module_activity(self) -> List[Dict]:
        """Analiza la actividad por módulo/tabla"""
        try:
            with self.db_manager.get_connection_context() as conn:
                cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
                
                # Verificar si hay datos en audit_logs
                cursor.execute("SELECT COUNT(*) as count FROM audit_logs")
                result = cursor.fetchone()
                if not result or result['count'] == 0:
                    logging.warning("No hay datos en audit_logs para analizar actividad de módulos")
                    return []
                
                query = """
                    SELECT 
                        COALESCE(table_name, 'Sin especificar') as modulo,
                        COUNT(*) as total_operaciones,
                        COUNT(CASE WHEN action = 'CREATE' THEN 1 END) as creaciones,
                        COUNT(CASE WHEN action = 'UPDATE' THEN 1 END) as modificaciones,
                        COUNT(CASE WHEN action = 'DELETE' THEN 1 END) as eliminaciones,
                        COUNT(DISTINCT user_id) as usuarios_unicos,
                        AVG(CASE WHEN action = 'CREATE' THEN 1 
                                WHEN action = 'UPDATE' THEN 2 
                                WHEN action = 'DELETE' THEN 3 
                                ELSE 1 END) as complejidad_promedio
                    FROM audit_logs
                    WHERE timestamp >= %s AND timestamp <= %s
                    GROUP BY table_name
                    ORDER BY total_operaciones DESC
                """
                
                start_date = self.filters.get('start_date', (datetime.now() - timedelta(days=30)).isoformat())
                end_date = self.filters.get('end_date', datetime.now().isoformat())
                
                cursor.execute(query, (start_date, end_date))
                return [dict(row) for row in cursor.fetchall()]
                
        except Exception as e:
            logging.error(f"Error analizando actividad de módulos: {e}")
            return []
            
    def _analyze_access_patterns(self) -> Dict:
        """Analiza patrones de acceso"""
        try:
            with self.db_manager.get_connection_context() as conn:
                cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
                
                # Verificar si hay datos en audit_logs
                cursor.execute("SELECT COUNT(*) as count FROM audit_logs")
                result = cursor.fetchone()
                if not result or result['count'] == 0:
                    logging.warning("No hay datos en audit_logs para analizar patrones de acceso")
                    return {
                        'by_hour': {},
                        'by_weekday': {},
                        'by_ip': []
                    }
                
                patterns = {}
                
                # Obtener fechas de filtro
                start_date_str = self.filters.get('start_date')
                end_date_str = self.filters.get('end_date')
                
                if start_date_str and end_date_str:
                    if isinstance(start_date_str, str) and isinstance(end_date_str, str):
                        try:
                            start_date = datetime.fromisoformat(start_date_str)
                            end_date = datetime.fromisoformat(end_date_str)
                        except ValueError:
                            end_date = datetime.now()
                            start_date = end_date - timedelta(days=30)
                    else:
                        end_date = datetime.now()
                        start_date = end_date - timedelta(days=30)
                else:
                    end_date = datetime.now()
                    start_date = end_date - timedelta(days=30)
                
                # Patrones por hora del día
                cursor.execute("""
                    SELECT 
                        EXTRACT(HOUR FROM timestamp) as hora,
                        COUNT(*) as actividad
                    FROM audit_logs
                    WHERE timestamp >= %s AND timestamp <= %s
                    GROUP BY EXTRACT(HOUR FROM timestamp)
                    ORDER BY hora
                """, (start_date, end_date))
                
                rows = cursor.fetchall()
                patterns['by_hour'] = {int(row['hora']): row['actividad'] for row in rows}
                
                # Patrones por día de la semana
                cursor.execute("""
                    SELECT 
                        CASE EXTRACT(DOW FROM timestamp)
                            WHEN 0 THEN 'Domingo'
                            WHEN 1 THEN 'Lunes'
                            WHEN 2 THEN 'Martes'
                            WHEN 3 THEN 'Miércoles'
                            WHEN 4 THEN 'Jueves'
                            WHEN 5 THEN 'Viernes'
                            WHEN 6 THEN 'Sábado'
                        END as dia_semana,
                        COUNT(*) as actividad
                    FROM audit_logs
                    WHERE timestamp >= %s AND timestamp <= %s
                    GROUP BY EXTRACT(DOW FROM timestamp)
                    ORDER BY EXTRACT(DOW FROM timestamp)
                """, (start_date, end_date))
                
                rows = cursor.fetchall()
                patterns['by_weekday'] = {row['dia_semana']: row['actividad'] for row in rows}
                
                # Patrones por IP (si disponible)
                cursor.execute("""
                    SELECT 
                        CASE 
                            WHEN ip_address IS NULL THEN 'IP no disponible'
                            ELSE CAST(ip_address AS TEXT)
                        END as ip_address,
                        COUNT(*) as accesos,
                        COUNT(DISTINCT user_id) as usuarios_distintos
                    FROM audit_logs
                    WHERE timestamp >= %s AND timestamp <= %s
                    GROUP BY ip_address
                    ORDER BY accesos DESC
                    LIMIT 20
                """, (start_date, end_date))
                
                rows = cursor.fetchall()
                patterns['by_ip'] = [dict(row) for row in rows]
                
                return patterns
                
        except Exception as e:
            logging.error(f"Error analizando patrones de acceso: {e}")
            return {
                'by_hour': {},
                'by_weekday': {},
                'by_ip': []
            }
            
    def _detect_unusual_activities(self) -> List[Dict]:
        """Detecta actividades inusuales"""
        try:
            unusual = []
            
            with self.db_manager.get_connection_context() as conn:
                cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
                
                # Actividad fuera de horario normal (antes de 6 AM o después de 10 PM)
                cursor.execute("""
                    SELECT 
                        al.user_id,
                        u.nombre as usuario,
                        al.timestamp,
                        al.action,
                        al.table_name,
                        'Actividad fuera de horario' as tipo_anomalia
                    FROM audit_logs al
                    LEFT JOIN usuarios u ON al.user_id = u.id
                    WHERE (EXTRACT(HOUR FROM al.timestamp) < 6 OR EXTRACT(HOUR FROM al.timestamp) > 22)
                    AND al.timestamp >= %s AND al.timestamp <= %s
                    ORDER BY al.timestamp DESC
                    LIMIT 50
                """, (self.filters.get('start_date'), self.filters.get('end_date')))
                
                unusual.extend([dict(row) for row in cursor.fetchall()])
                
                # Múltiples eliminaciones en poco tiempo
                cursor.execute("""
                    SELECT 
                        al.user_id,
                        u.nombre as usuario,
                        COUNT(*) as eliminaciones,
                        MIN(al.timestamp) as inicio,
                        MAX(al.timestamp) as fin,
                        'Múltiples eliminaciones' as tipo_anomalia
                    FROM audit_logs al
                    LEFT JOIN usuarios u ON al.user_id = u.id
                    WHERE al.action = 'DELETE'
                    AND al.timestamp >= %s AND al.timestamp <= %s
                    GROUP BY al.user_id, u.nombre, 
                             DATE_TRUNC('hour', al.timestamp)
                    HAVING COUNT(*) >= 5
                    ORDER BY eliminaciones DESC
                """, (self.filters.get('start_date'), self.filters.get('end_date')))
                
                unusual.extend([dict(row) for row in cursor.fetchall()])
                
                # Acceso desde múltiples IPs por el mismo usuario
                cursor.execute("""
                    SELECT 
                        al.user_id,
                        u.nombre as usuario,
                        COUNT(DISTINCT al.ip_address) as ips_distintas,
                        STRING_AGG(DISTINCT 
                            CASE 
                                WHEN al.ip_address IS NULL THEN 'IP no disponible'
                                ELSE CAST(al.ip_address AS TEXT)
                            END, ', ') as ips,
                        'Acceso desde múltiples IPs' as tipo_anomalia
                    FROM audit_logs al
                    LEFT JOIN usuarios u ON al.user_id = u.id
                    WHERE al.timestamp >= %s AND al.timestamp <= %s
                    GROUP BY al.user_id, u.nombre
                    HAVING COUNT(DISTINCT al.ip_address) >= 2
                    ORDER BY ips_distintas DESC
                """, (self.filters.get('start_date'), self.filters.get('end_date')))
                
                unusual.extend([dict(row) for row in cursor.fetchall()])
                
                return unusual
                
        except Exception as e:
            logging.error(f"Error detectando actividades inusuales: {e}")
            return []
            
    def _get_general_stats(self) -> Dict:
        """Obtiene estadísticas generales"""
        try:
            with self.db_manager.get_connection_context() as conn:
                cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
                
                # Verificar si existe la tabla audit_logs y tiene datos
                cursor.execute("""
                    SELECT COUNT(*) as count 
                    FROM information_schema.tables 
                    WHERE table_name = 'audit_logs' AND table_schema = 'public'
                """)
                result = cursor.fetchone()
                table_exists = result and result['count'] > 0
                
                if not table_exists:
                    logging.warning("Tabla audit_logs no existe")
                    return {
                        'total_operations': 0,
                        'active_users': 0,
                        'top_modules': [],
                        'avg_operations_per_day': 0
                    }
                
                stats = {}
                
                # Obtener fechas de filtro
                start_date_str = self.filters.get('start_date')
                end_date_str = self.filters.get('end_date')
                
                if start_date_str and end_date_str:
                    if isinstance(start_date_str, str) and isinstance(end_date_str, str):
                        try:
                            start_date = datetime.fromisoformat(start_date_str)
                            end_date = datetime.fromisoformat(end_date_str)
                        except ValueError:
                            end_date = datetime.now()
                            start_date = end_date - timedelta(days=30)
                    else:
                        end_date = datetime.now()
                        start_date = end_date - timedelta(days=30)
                else:
                    end_date = datetime.now()
                    start_date = end_date - timedelta(days=30)
                
                # Total de operaciones
                cursor.execute("""
                    SELECT COUNT(*) as total
                    FROM audit_logs
                    WHERE timestamp >= %s AND timestamp <= %s
                """, (start_date, end_date))
                
                result = cursor.fetchone()
                stats['total_operations'] = result['total'] if result else 0
                
                # Usuarios únicos activos
                cursor.execute("""
                    SELECT COUNT(DISTINCT user_id) as usuarios_activos
                    FROM audit_logs
                    WHERE timestamp >= %s AND timestamp <= %s
                """, (start_date, end_date))
                
                result = cursor.fetchone()
                stats['active_users'] = result['usuarios_activos'] if result else 0
                
                # Módulos más utilizados
                cursor.execute("""
                    SELECT COALESCE(table_name, 'Sin especificar') as table_name, COUNT(*) as operaciones
                    FROM audit_logs
                    WHERE timestamp >= %s AND timestamp <= %s
                    GROUP BY table_name
                    ORDER BY operaciones DESC
                    LIMIT 5
                """, (start_date, end_date))
                
                stats['top_modules'] = [dict(row) for row in cursor.fetchall()]
                
                # Promedio de operaciones por día
                days_diff = (end_date - start_date).days + 1
                stats['avg_operations_per_day'] = stats['total_operations'] / days_diff if days_diff > 0 else 0
                
                return stats
                
        except Exception as e:
            logging.error(f"Error obteniendo estadísticas generales: {e}")
            return {
                'total_operations': 0,
                'active_users': 0,
                'top_modules': [],
                'avg_operations_per_day': 0
            }

class MetricCard(QFrame):
    """Tarjeta de métrica para el dashboard"""
    
    def __init__(self, title: str, value: str, subtitle: str = "", color: str = "#3498db"):
        super().__init__()
        self.setFrameStyle(QFrame.Shape.Box)
        self.setObjectName("audit_metric_card")
        self.color = color
        
        layout = QVBoxLayout(self)
        
        # Título
        title_label = QLabel(title)
        title_label.setFont(QFont("Arial", 10, QFont.Weight.Bold))
        title_label.setObjectName("audit_metric_title")
        title_label.setProperty("color", color)
        layout.addWidget(title_label)
        
        # Valor principal
        value_label = QLabel(value)
        value_label.setFont(QFont("Arial", 18, QFont.Weight.Bold))
        value_label.setObjectName("audit_metric_value")
        layout.addWidget(value_label)
        
        # Subtítulo
        if subtitle:
            subtitle_label = QLabel(subtitle)
            subtitle_label.setFont(QFont("Arial", 8))
            subtitle_label.setObjectName("audit_metric_subtitle")
            layout.addWidget(subtitle_label)

class AuditDashboardWidget(QWidget):
    """Widget principal del dashboard de auditoría"""
    
    def __init__(self, db_manager, parent=None):
        super().__init__(parent)
        self.db_manager = db_manager
        self.filters = {}  # Inicializar filters
        self.analysis_results = {}
        self.auto_refresh_timer = QTimer()
        self.auto_refresh_timer.timeout.connect(self.refresh_dashboard)
        # Estado interno
        self._analysis_in_progress = False
        # Marcar destrucción y limpieza segura
        self._destroyed = False
        try:
            self.destroyed.connect(self._cleanup_on_destroy)
        except Exception:
            pass
        self.setup_ui()

    def _cleanup_on_destroy(self):
        """Detiene timers y marca el widget como destruido para evitar tareas tardías."""
        try:
            self._destroyed = True
            if hasattr(self, 'auto_refresh_timer') and self.auto_refresh_timer is not None:
                self.auto_refresh_timer.stop()
        except Exception:
            pass
        
    def setup_ui(self):
        """Configura la interfaz de usuario"""
        layout = QVBoxLayout(self)
        layout.setSpacing(8)
        layout.setContentsMargins(8, 8, 8, 8)
        
        # Título principal eliminado para diseño sin encabezado
        
        # Controles superiores
        self.create_controls_section(layout)
        
        # Métricas principales
        self.create_metrics_section(layout)
        
        # Pestañas de análisis
        self.create_analysis_tabs(layout)
        
        # Cargar datos iniciales
        self.load_initial_data()
        
    def create_controls_section(self, parent_layout):
        """Crea la sección de controles"""
        controls_group = QGroupBox("⚙️ Controles de Análisis")
        controls_group.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Preferred)
        controls_layout = QHBoxLayout(controls_group)
        controls_layout.setContentsMargins(8, 8, 8, 8)
        controls_layout.setSpacing(6)
        
        # Filtros de fecha
        controls_layout.addWidget(QLabel("Desde:"))
        self.start_date = QDateEdit()
        self.start_date.setDate(QDate.currentDate().addDays(-30))
        self.start_date.setCalendarPopup(True)
        controls_layout.addWidget(self.start_date)
        
        controls_layout.addWidget(QLabel("Hasta:"))
        self.end_date = QDateEdit()
        self.end_date.setDate(QDate.currentDate())
        self.end_date.setCalendarPopup(True)
        controls_layout.addWidget(self.end_date)
        
        # Filtro de usuario
        controls_layout.addWidget(QLabel("Usuario:"))
        self.user_filter = QComboBox()
        self.user_filter.addItem("Todos los usuarios")
        self.load_users_filter()
        controls_layout.addWidget(self.user_filter)
        
        # Botones de acción
        self.analyze_btn = QPushButton("🔍 Analizar")
        self.analyze_btn.clicked.connect(self.start_analysis)
        controls_layout.addWidget(self.analyze_btn)
        
        self.export_btn = QPushButton("📊 Exportar")
        self.export_btn.clicked.connect(self.export_analysis)
        self.export_btn.setEnabled(False)
        controls_layout.addWidget(self.export_btn)
        
        # Auto-refresh
        self.auto_refresh_check = QCheckBox("Auto-actualizar")
        self.auto_refresh_check.toggled.connect(self.toggle_auto_refresh)
        controls_layout.addWidget(self.auto_refresh_check)
        
        self.refresh_interval = QSpinBox()
        self.refresh_interval.setRange(1, 60)
        self.refresh_interval.setValue(5)
        self.refresh_interval.setSuffix(" min")
        controls_layout.addWidget(self.refresh_interval)
        
        controls_layout.addStretch()
        
        # Barra de progreso
        self.progress_bar = QProgressBar()
        self.progress_bar.setVisible(False)
        controls_layout.addWidget(self.progress_bar)
        
        parent_layout.addWidget(controls_group)
        
    def create_metrics_section(self, parent_layout):
        """Crea la sección de métricas principales"""
        metrics_group = QGroupBox("📊 Métricas Principales")
        metrics_group.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Preferred)
        metrics_layout = QGridLayout(metrics_group)
        metrics_layout.setContentsMargins(8, 8, 8, 8)
        metrics_layout.setSpacing(6)
        
        # Tarjetas de métricas (se llenarán dinámicamente)
        self.metrics_cards = []
        
        parent_layout.addWidget(metrics_group)
        self.metrics_layout = metrics_layout
        
    def create_analysis_tabs(self, parent_layout):
        """Crea las pestañas de análisis"""
        self.tab_widget = QTabWidget()
        
        # Pestaña de actividad por usuario
        self.user_activity_tab = self.create_user_activity_tab()
        self.tab_widget.addTab(self.user_activity_tab, "👥 Actividad por Usuario")
        
        # Pestaña de actividad por módulo
        self.module_activity_tab = self.create_module_activity_tab()
        self.tab_widget.addTab(self.module_activity_tab, "📦 Actividad por Módulo")
        
        # Pestaña de patrones de acceso
        self.access_patterns_tab = self.create_access_patterns_tab()
        self.tab_widget.addTab(self.access_patterns_tab, "🕒 Patrones de Acceso")
        
        # Pestaña de actividades inusuales
        self.unusual_activities_tab = self.create_unusual_activities_tab()
        self.tab_widget.addTab(self.unusual_activities_tab, "⚠️ Actividades Inusuales")
        
        parent_layout.addWidget(self.tab_widget)
        
    def create_user_activity_tab(self):
        """Crea la pestaña de actividad por usuario"""
        tab = QWidget()
        layout = QVBoxLayout(tab)
        layout.setContentsMargins(8, 8, 8, 8)
        layout.setSpacing(6)
        
        # Tabla de actividad por usuario
        self.user_activity_table = QTableWidget()
        self.user_activity_table.setColumnCount(8)
        self.user_activity_table.setHorizontalHeaderLabels([
            "Usuario", "Total Acciones", "Creaciones", "Modificaciones", 
            "Eliminaciones", "Lecturas", "Primera Actividad", "Última Actividad"
        ])
        self.user_activity_table.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Expanding)
        layout.addWidget(self.user_activity_table)
        
        return tab
        
    def create_module_activity_tab(self):
        """Crea la pestaña de actividad por módulo"""
        tab = QWidget()
        layout = QVBoxLayout(tab)
        layout.setContentsMargins(8, 8, 8, 8)
        layout.setSpacing(6)
        
        # Tabla de actividad por módulo
        self.module_activity_table = QTableWidget()
        self.module_activity_table.setColumnCount(7)
        self.module_activity_table.setHorizontalHeaderLabels([
            "Módulo", "Total Operaciones", "Creaciones", "Modificaciones", 
            "Eliminaciones", "Usuarios Únicos", "Complejidad Promedio"
        ])
        self.module_activity_table.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Expanding)
        layout.addWidget(self.module_activity_table)
        
        return tab
        
    def create_access_patterns_tab(self):
        """Crea la pestaña de patrones de acceso"""
        tab = QWidget()
        layout = QVBoxLayout(tab)
        layout.setContentsMargins(8, 8, 8, 8)
        layout.setSpacing(6)
        
        
        hour_group = QGroupBox("📅 Actividad por Hora del Día")
        hour_group.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Expanding)
        hour_layout = QVBoxLayout(hour_group)
        hour_layout.setContentsMargins(8, 8, 8, 8)
        hour_layout.setSpacing(4)
        
        self.hour_patterns_table = QTableWidget()
        self.hour_patterns_table.setColumnCount(2)
        self.hour_patterns_table.setHorizontalHeaderLabels(["Hora", "Actividad"])
        self.hour_patterns_table.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Expanding)
        self.hour_patterns_table.setMaximumHeight(200)
        hour_layout.addWidget(self.hour_patterns_table)
        
        layout.addWidget(hour_group)
        
        
        weekday_group = QGroupBox("📆 Actividad por Día de la Semana")
        weekday_group.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Expanding)
        weekday_layout = QVBoxLayout(weekday_group)
        weekday_layout.setContentsMargins(8, 8, 8, 8)
        weekday_layout.setSpacing(4)
        
        self.weekday_patterns_table = QTableWidget()
        self.weekday_patterns_table.setColumnCount(2)
        self.weekday_patterns_table.setHorizontalHeaderLabels(["Día", "Actividad"])
        self.weekday_patterns_table.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Expanding)
        self.weekday_patterns_table.setMaximumHeight(200)
        weekday_layout.addWidget(self.weekday_patterns_table)
        
        layout.addWidget(weekday_group)
        
        
        ip_group = QGroupBox("🌐 Actividad por Dirección IP")
        ip_group.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Expanding)
        ip_layout = QVBoxLayout(ip_group)
        ip_layout.setContentsMargins(8, 8, 8, 8)
        ip_layout.setSpacing(4)
        
        self.ip_patterns_table = QTableWidget()
        self.ip_patterns_table.setColumnCount(3)
        self.ip_patterns_table.setHorizontalHeaderLabels(["IP", "Accesos", "Usuarios Distintos"])
        self.ip_patterns_table.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Expanding)
        self.ip_patterns_table.setMaximumHeight(200)
        ip_layout.addWidget(self.ip_patterns_table)
        
        layout.addWidget(ip_group)
        
        return tab
        
    def create_unusual_activities_tab(self):
        """Crea la pestaña de actividades inusuales"""
        tab = QWidget()
        layout = QVBoxLayout(tab)
        layout.setContentsMargins(8, 8, 8, 8)
        layout.setSpacing(6)
        
        # Información sobre detección
        info_label = QLabel(
            "⚠️ Esta sección muestra actividades que podrían requerir atención:\n"
            "• Actividad fuera de horario normal (antes de 6 AM o después de 10 PM)\n"
            "• Múltiples eliminaciones en poco tiempo\n"
            "• Acceso desde múltiples direcciones IP por el mismo usuario"
        )
        info_label.setObjectName("audit_info_label")
        layout.addWidget(info_label)
        
        # Tabla de actividades inusuales
        self.unusual_activities_table = QTableWidget()
        self.unusual_activities_table.setColumnCount(6)
        self.unusual_activities_table.setHorizontalHeaderLabels([
            "Usuario", "Tipo de Anomalía", "Timestamp", "Acción", "Módulo", "Detalles"
        ])
        self.unusual_activities_table.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Expanding)
        layout.addWidget(self.unusual_activities_table)
        
        return tab
        
    def load_users_filter(self):
        """Carga los usuarios en el filtro en hilo para evitar bloquear la UI."""
        try:
            # Placeholder mientras carga
            self.user_filter.clear()
            self.user_filter.addItem(PLACEHOLDER_LOADING_USERS, None)
            def _on_success(usuarios):
                activos = [u for u in usuarios if getattr(u, 'activo', True)]
                items = [(getattr(u, 'nombre', str(u)), getattr(u, 'id', None)) for u in activos]
                self._populate_user_filter(items)
            load_users_cached_async(
                self.db_manager,
                on_success=_on_success,
                on_error=lambda msg: self._populate_user_filter([], error=msg),
                parent=self,
            )
        except Exception as e:
            logging.error(f"Error iniciando carga asíncrona de usuarios en filtro: {e}")
            self.user_filter.clear()
            self.user_filter.addItem(PLACEHOLDER_NO_USERS, None)

    def _populate_user_filter(self, items, error: str | None = None):
        try:
            self.user_filter.clear()
            if error:
                logging.error(f"Fallo al cargar usuarios del filtro: {error}")
            if not items:
                self.user_filter.addItem(PLACEHOLDER_NO_USERS, None)
                return
            for nombre, uid in items:
                self.user_filter.addItem(nombre, uid)
        except Exception as e:
            logging.error(f"Error poblando filtro de usuarios: {e}")
            self.user_filter.clear()
            self.user_filter.addItem(PLACEHOLDER_NO_USERS, None)
            
    def load_initial_data(self):
        """Carga los datos iniciales del dashboard"""
        self.start_analysis()
        
    def start_analysis(self):
        """Inicia el análisis de auditoría"""
        # Evitar solapamiento de análisis
        if getattr(self, '_analysis_in_progress', False):
            return
        self._analysis_in_progress = True
        filters = {
            'start_date': self.start_date.date().toString(Qt.DateFormat.ISODate),
            'end_date': self.end_date.date().toString(Qt.DateFormat.ISODate)
        }
        
        if self.user_filter.currentData():
            filters['user_id'] = self.user_filter.currentData()
            
        # Configurar UI para análisis
        self.analyze_btn.setEnabled(False)
        self.progress_bar.setVisible(True)
        self.progress_bar.setValue(0)
        
        # Iniciar hilo de análisis
        self.analysis_thread = AuditAnalysisThread(self.db_manager, filters)
        self.analysis_thread.progress_updated.connect(self.progress_bar.setValue)
        self.analysis_thread.analysis_complete.connect(self.on_analysis_complete)
        self.analysis_thread.start()
        
    def on_analysis_complete(self, results):
        """Maneja la finalización del análisis"""
        # Marcar fin de análisis para permitir nuevos disparos
        self._analysis_in_progress = False
        try:
            self.analysis_results = results
            
            # Restaurar UI
            self.analyze_btn.setEnabled(True)
            self.progress_bar.setVisible(False)
            self.export_btn.setEnabled(True)
            
            # Actualizar métricas principales
            self.update_main_metrics(results)
            
            # Actualizar tablas
            self.update_user_activity_table(results.get('user_activity', []))
            self.update_module_activity_table(results.get('module_activity', []))
            self.update_access_patterns_tables(results.get('access_patterns', {}))
            self.update_unusual_activities_table(results.get('unusual_activities', []))
            
        except Exception as e:
            logging.error(f"Error procesando resultados de análisis: {e}")
            # Evitar mostrar diálogos si el widget ya no es visible o fue destruido
            try:
                if getattr(self, '_destroyed', False):
                    return
                if hasattr(self, 'isVisible') and self.isVisible():
                    QMessageBox.critical(self, "Error", f"Error procesando resultados: {e}")
            except RuntimeError:
                # Ignorar si el objeto Qt ya fue eliminado
                pass
            
    def update_main_metrics(self, results):
        """Actualiza las métricas principales"""
        # Limpiar tarjetas existentes
        for card in self.metrics_cards:
            card.deleteLater()
        self.metrics_cards.clear()
        
        # Obtener estadísticas generales
        stats = results.get('general_stats', {})
        
        # Crear nuevas tarjetas
        cards_data = [
            ("Total Operaciones", str(stats.get('total_operations', 0)), "En el período seleccionado", "#3498db"),
            ("Usuarios Activos", str(stats.get('active_users', 0)), "Usuarios únicos", "#2ecc71"),
            ("Promedio Diario", f"{stats.get('avg_operations_per_day', 0):.1f}", "Operaciones por día", "#f39c12"),
            ("Actividades Inusuales", str(len(results.get('unusual_activities', []))), "Requieren atención", "#e74c3c")
        ]
        
        for i, (title, value, subtitle, color) in enumerate(cards_data):
            card = MetricCard(title, value, subtitle, color)
            self.metrics_cards.append(card)
            row, col = divmod(i, 2)
            self.metrics_layout.addWidget(card, row, col)
            
    def update_user_activity_table(self, user_activity):
        """Actualiza la tabla de actividad por usuario"""
        self.user_activity_table.setRowCount(len(user_activity))
        
        for row, activity in enumerate(user_activity):
            self.user_activity_table.setItem(row, 0, QTableWidgetItem(activity.get('usuario', 'N/A')))
            self.user_activity_table.setItem(row, 1, QTableWidgetItem(str(activity.get('total_acciones', 0))))
            self.user_activity_table.setItem(row, 2, QTableWidgetItem(str(activity.get('creaciones', 0))))
            self.user_activity_table.setItem(row, 3, QTableWidgetItem(str(activity.get('modificaciones', 0))))
            self.user_activity_table.setItem(row, 4, QTableWidgetItem(str(activity.get('eliminaciones', 0))))
            self.user_activity_table.setItem(row, 5, QTableWidgetItem(str(activity.get('lecturas', 0))))
            # Convertir datetime a string si es necesario
            primera_actividad = activity.get('primera_actividad', 'N/A')
            if hasattr(primera_actividad, 'strftime'):
                primera_actividad = primera_actividad.strftime('%Y-%m-%d %H:%M:%S')
            elif primera_actividad != 'N/A':
                primera_actividad = str(primera_actividad)
                
            ultima_actividad = activity.get('ultima_actividad', 'N/A')
            if hasattr(ultima_actividad, 'strftime'):
                ultima_actividad = ultima_actividad.strftime('%Y-%m-%d %H:%M:%S')
            elif ultima_actividad != 'N/A':
                ultima_actividad = str(ultima_actividad)
                
            self.user_activity_table.setItem(row, 6, QTableWidgetItem(str(primera_actividad)))
            self.user_activity_table.setItem(row, 7, QTableWidgetItem(str(ultima_actividad)))
            
        self.user_activity_table.resizeColumnsToContents()
        
    def update_module_activity_table(self, module_activity):
        """Actualiza la tabla de actividad por módulo"""
        self.module_activity_table.setRowCount(len(module_activity))
        
        for row, activity in enumerate(module_activity):
            self.module_activity_table.setItem(row, 0, QTableWidgetItem(activity.get('modulo', 'N/A')))
            self.module_activity_table.setItem(row, 1, QTableWidgetItem(str(activity.get('total_operaciones', 0))))
            self.module_activity_table.setItem(row, 2, QTableWidgetItem(str(activity.get('creaciones', 0))))
            self.module_activity_table.setItem(row, 3, QTableWidgetItem(str(activity.get('modificaciones', 0))))
            self.module_activity_table.setItem(row, 4, QTableWidgetItem(str(activity.get('eliminaciones', 0))))
            self.module_activity_table.setItem(row, 5, QTableWidgetItem(str(activity.get('usuarios_unicos', 0))))
            self.module_activity_table.setItem(row, 6, QTableWidgetItem(f"{activity.get('complejidad_promedio', 0):.2f}"))
            
        self.module_activity_table.resizeColumnsToContents()
        
    def update_access_patterns_tables(self, patterns):
        """Actualiza las tablas de patrones de acceso"""
        # Patrones por hora
        hour_data = patterns.get('by_hour', {})
        self.hour_patterns_table.setRowCount(len(hour_data))
        
        for row, (hour, activity) in enumerate(sorted(hour_data.items())):
            self.hour_patterns_table.setItem(row, 0, QTableWidgetItem(f"{hour}:00"))
            self.hour_patterns_table.setItem(row, 1, QTableWidgetItem(str(activity)))
            
        # Patrones por día de la semana
        weekday_data = patterns.get('by_weekday', {})
        self.weekday_patterns_table.setRowCount(len(weekday_data))
        
        for row, (day, activity) in enumerate(weekday_data.items()):
            self.weekday_patterns_table.setItem(row, 0, QTableWidgetItem(day))
            self.weekday_patterns_table.setItem(row, 1, QTableWidgetItem(str(activity)))
            
        # Patrones por IP
        ip_data = patterns.get('by_ip', [])
        self.ip_patterns_table.setRowCount(len(ip_data))
        
        for row, ip_info in enumerate(ip_data):
            self.ip_patterns_table.setItem(row, 0, QTableWidgetItem(ip_info.get('ip_address', 'N/A')))
            self.ip_patterns_table.setItem(row, 1, QTableWidgetItem(str(ip_info.get('accesos', 0))))
            self.ip_patterns_table.setItem(row, 2, QTableWidgetItem(str(ip_info.get('usuarios_distintos', 0))))
            
        # Redimensionar columnas
        self.hour_patterns_table.resizeColumnsToContents()
        self.weekday_patterns_table.resizeColumnsToContents()
        self.ip_patterns_table.resizeColumnsToContents()
        
    def update_unusual_activities_table(self, unusual_activities):
        """Actualiza la tabla de actividades inusuales"""
        self.unusual_activities_table.setRowCount(len(unusual_activities))
        
        for row, activity in enumerate(unusual_activities):
            self.unusual_activities_table.setItem(row, 0, QTableWidgetItem(activity.get('usuario', 'N/A')))
            self.unusual_activities_table.setItem(row, 1, QTableWidgetItem(activity.get('tipo_anomalia', 'N/A')))
            
            # Timestamp o rango de tiempo
            if 'timestamp' in activity:
                timestamp = activity['timestamp']
                if hasattr(timestamp, 'strftime'):
                    timestamp = timestamp.strftime('%Y-%m-%d %H:%M:%S')
                else:
                    timestamp = str(timestamp)
            elif 'inicio' in activity and 'fin' in activity:
                inicio = activity['inicio']
                fin = activity['fin']
                if hasattr(inicio, 'strftime'):
                    inicio = inicio.strftime('%Y-%m-%d %H:%M:%S')
                else:
                    inicio = str(inicio)
                if hasattr(fin, 'strftime'):
                    fin = fin.strftime('%Y-%m-%d %H:%M:%S')
                else:
                    fin = str(fin)
                timestamp = f"{inicio} - {fin}"
            else:
                timestamp = 'N/A'
            self.unusual_activities_table.setItem(row, 2, QTableWidgetItem(str(timestamp)))
            
            self.unusual_activities_table.setItem(row, 3, QTableWidgetItem(activity.get('action', 'N/A')))
            self.unusual_activities_table.setItem(row, 4, QTableWidgetItem(activity.get('table_name', 'N/A')))
            
            # Detalles adicionales
            details = ""
            if 'eliminaciones' in activity:
                details = f"Eliminaciones: {activity['eliminaciones']}"
            elif 'ips' in activity:
                details = f"IPs: {activity['ips']}"
            
            self.unusual_activities_table.setItem(row, 5, QTableWidgetItem(details))
            
            # Aplicar estilos según el tipo de anomalía usando CSS dinámico
            severity = "high"
            if activity.get('tipo_anomalia') == 'Actividad fuera de horario':
                severity = "medium"
            elif activity.get('tipo_anomalia') == 'Múltiples eliminaciones':
                severity = "high"
            elif activity.get('tipo_anomalia') == 'Acceso desde múltiples IPs':
                severity = "low"
                
            for col in range(6):
                if self.unusual_activities_table.item(row, col):
                    # Usar propiedades CSS dinámicas en lugar de colores hardcodeados
                    self.unusual_activities_table.item(row, col).setData(Qt.ItemDataRole.UserRole, f"{severity}_severity")
                    # El color se aplicará a través del sistema CSS dinámico basado en la propiedad severity
                    
        self.unusual_activities_table.resizeColumnsToContents()
        
    def export_analysis(self):
        """Exporta los resultados del análisis"""
        if not self.analysis_results:
            QMessageBox.warning(self, "Advertencia", "No hay resultados para exportar")
            return
            
        file_path, _ = QFileDialog.getSaveFileName(
            self, "Exportar Análisis de Auditoría", 
            f"auditoria_dashboard_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json",
            "JSON Files (*.json);;CSV Files (*.csv)"
        )
        
        if file_path:
            try:
                if file_path.endswith('.json'):
                    with open(file_path, 'w', encoding='utf-8') as f:
                        json.dump(self.analysis_results, f, indent=2, ensure_ascii=False)
                elif file_path.endswith('.csv'):
                    self._export_to_csv(file_path)
                    
                QMessageBox.information(self, "Éxito", f"Análisis exportado a {file_path}")
                
            except Exception as e:
                QMessageBox.critical(self, "Error", f"Error exportando análisis: {e}")
                
    def _export_to_csv(self, file_path):
        """Exporta los resultados a CSV"""
        with open(file_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            
            # Estadísticas generales
            writer.writerow(['=== ESTADÍSTICAS GENERALES ==='])
            stats = self.analysis_results.get('general_stats', {})
            for key, value in stats.items():
                writer.writerow([key, value])
                
            writer.writerow([])
            
            # Actividad por usuario
            writer.writerow(['=== ACTIVIDAD POR USUARIO ==='])
            writer.writerow(['Usuario', 'Total Acciones', 'Creaciones', 'Modificaciones', 'Eliminaciones', 'Lecturas'])
            for activity in self.analysis_results.get('user_activity', []):
                writer.writerow([
                    activity.get('usuario', ''),
                    activity.get('total_acciones', 0),
                    activity.get('creaciones', 0),
                    activity.get('modificaciones', 0),
                    activity.get('eliminaciones', 0),
                    activity.get('lecturas', 0)
                ])
                
            writer.writerow([])
            
            # Actividad por módulo
            writer.writerow(['=== ACTIVIDAD POR MÓDULO ==='])
            writer.writerow(['Módulo', 'Total Operaciones', 'Creaciones', 'Modificaciones', 'Eliminaciones', 'Usuarios Únicos'])
            for activity in self.analysis_results.get('module_activity', []):
                writer.writerow([
                    activity.get('modulo', ''),
                    activity.get('total_operaciones', 0),
                    activity.get('creaciones', 0),
                    activity.get('modificaciones', 0),
                    activity.get('eliminaciones', 0),
                    activity.get('usuarios_unicos', 0)
                ])
                
            writer.writerow([])
            
            # Actividades inusuales
            writer.writerow(['=== ACTIVIDADES INUSUALES ==='])
            writer.writerow(['Usuario', 'Tipo Anomalía', 'Timestamp', 'Acción', 'Módulo'])
        for activity in self.analysis_results.get('unusual_activities', []):
            timestamp = activity.get('timestamp', activity.get('inicio', 'N/A'))
            writer.writerow([
                activity.get('usuario', ''),
                activity.get('tipo_anomalia', ''),
                timestamp,
                activity.get('action', ''),
                activity.get('table_name', '')
            ])
                
    def toggle_auto_refresh(self, enabled):
        """Activa/desactiva la actualización automática"""
        if enabled:
            interval_ms = self.refresh_interval.value() * 60 * 1000  # Convertir minutos a milisegundos
            self.auto_refresh_timer.start(interval_ms)
        else:
            self.auto_refresh_timer.stop()
            
    def refresh_dashboard(self):
        """Actualiza el dashboard automáticamente"""
        try:
            # Evitar actualizaciones si el widget no está visible o destruido
            if getattr(self, '_destroyed', False):
                return
            if hasattr(self, 'isVisible') and not self.isVisible():
                return
            # Evitar solapamientos de análisis en curso
            if getattr(self, '_analysis_in_progress', False):
                return
            self.start_analysis()
        except Exception as e:
            logging.error(f"Error refrescando dashboard: {e}")
from utils_modules.async_runner import TaskThread

