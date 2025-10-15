import sys
import logging
from datetime import datetime, timedelta
import psycopg2.extras
from PyQt6.QtWidgets import (
    QWidget, QVBoxLayout, QHBoxLayout, QGridLayout, QLabel, QPushButton,
    QGroupBox, QTableWidget, QTableWidgetItem, QTabWidget, QDateEdit,
    QComboBox, QSpinBox, QTextEdit, QFrame, QScrollArea, QMessageBox,
    QHeaderView, QAbstractItemView, QProgressBar, QSplitter, QCheckBox
)
from PyQt6.QtGui import QFont, QPixmap, QPalette, QColor
from PyQt6.QtCore import Qt, QDate, QTimer, QThread, pyqtSignal

from database import DatabaseManager
from widgets.chart_widget import MplChartWidget
from widgets.cache_metrics_dialog import CacheMetricsDialog
from utils import resource_path
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from collections import defaultdict
import os

class OperationalReportsThread(QThread):
    """Hilo para generar reportes operativos sin bloquear la UI"""
    
    report_ready = pyqtSignal(dict)
    error_occurred = pyqtSignal(str)
    
    def __init__(self, db_manager, report_type, params=None):
        super().__init__()
        self.db_manager = db_manager
        self.report_type = report_type
        self.params = params or {}
    
    def run(self):
        """Ejecuta la generación del reporte en segundo plano"""
        try:
            if self.report_type == 'daily_attendance':
                data = self._generate_daily_attendance_report()
            elif self.report_type == 'peak_hours':
                data = self._generate_peak_hours_report()
            elif self.report_type == 'class_occupancy':
                data = self._generate_class_occupancy_report()
            elif self.report_type == 'payment_due':
                data = self._generate_payment_due_report()
            elif self.report_type == 'professor_efficiency':
                data = self._generate_professor_efficiency_report()
            else:
                data = {}
            
            self.report_ready.emit(data)
            
        except Exception as e:
            logging.error(f"Error generando reporte {self.report_type}: {e}")
            self.error_occurred.emit(str(e))
    
    def _generate_daily_attendance_report(self):
        """Genera reporte de asistencia diaria"""
        try:
            with self.db_manager.get_connection_context() as conn:
                cursor = conn.cursor()
                # Aplicar timeouts de solo lectura a todas las consultas del reporte
                try:
                    self.db_manager._apply_readonly_timeouts(cursor)
                except Exception:
                    pass
                
                # Asistencias por día en los últimos 30 días
                cursor.execute("""
                    SELECT fecha::date as dia, COUNT(*) as asistencias
                    FROM asistencias 
                    WHERE fecha >= CURRENT_DATE - INTERVAL '30 days'
                    GROUP BY fecha::date
                    ORDER BY dia DESC
                """)
                
                daily_data = cursor.fetchall()
                
                # Asistencias por hora del día (últimos 7 días)
                cursor.execute("""
                    SELECT 
                        CASE 
                            WHEN EXTRACT(HOUR FROM hora_registro) BETWEEN 6 AND 8 THEN '06:00-09:00'
                        WHEN EXTRACT(HOUR FROM hora_registro) BETWEEN 9 AND 11 THEN '09:00-12:00'
                        WHEN EXTRACT(HOUR FROM hora_registro) BETWEEN 12 AND 14 THEN '12:00-15:00'
                        WHEN EXTRACT(HOUR FROM hora_registro) BETWEEN 15 AND 17 THEN '15:00-18:00'
                        WHEN EXTRACT(HOUR FROM hora_registro) BETWEEN 18 AND 20 THEN '18:00-21:00'
                            ELSE 'Otros'
                        END as franja_horaria,
                        COUNT(*) as asistencias
                    FROM asistencias 
                    WHERE fecha >= CURRENT_DATE - INTERVAL '7 days' AND hora_registro IS NOT NULL
                    GROUP BY franja_horaria
                    ORDER BY asistencias DESC
                """)
                
                hourly_data = cursor.fetchall()
                
                # Estadísticas generales correctas
                cursor.execute("""
                    SELECT 
                        COUNT(*) as total_asistencias,
                        COUNT(DISTINCT usuario_id) as usuarios_unicos
                    FROM asistencias 
                    WHERE fecha >= CURRENT_DATE - INTERVAL '30 days'
                """)
                stats = cursor.fetchone()
                total_asistencias = stats[0] if stats else 0
                usuarios_unicos = stats[1] if stats else 0
                
                cursor.execute("""
                    SELECT AVG(cnt) FROM (
                        SELECT COUNT(*) AS cnt
                        FROM asistencias
                        WHERE fecha >= CURRENT_DATE - INTERVAL '30 days'
                        GROUP BY fecha::date
                    ) s
                """)
                avg_row = cursor.fetchone()
                promedio_diario = float(avg_row[0]) if avg_row and avg_row[0] is not None else 0.0
                
                return {
                    'daily_attendance': daily_data,
                    'hourly_distribution': hourly_data,
                    'total_attendance': total_asistencias,
                    'unique_users': usuarios_unicos,
                    'daily_average': promedio_diario
                }
                
        except Exception as e:
            logging.error(f"Error en reporte de asistencia diaria: {e}")
            return {}
    
    def _generate_peak_hours_report(self):
        """Genera reporte de horas pico"""
        try:
            with self.db_manager.get_connection_context() as conn:
                cursor = conn.cursor()
                try:
                    self.db_manager._apply_readonly_timeouts(cursor)
                except Exception:
                    pass
                
                # Análisis de horas pico por día de la semana
                cursor.execute("""
                    SELECT 
                        CASE EXTRACT(DOW FROM fecha)
                            WHEN 0 THEN 'Domingo'
                            WHEN 1 THEN 'Lunes'
                            WHEN 2 THEN 'Martes'
                            WHEN 3 THEN 'Miércoles'
                            WHEN 4 THEN 'Jueves'
                            WHEN 5 THEN 'Viernes'
                            WHEN 6 THEN 'Sábado'
                        END as dia_semana,
                        CASE 
                            WHEN EXTRACT(HOUR FROM hora_registro) BETWEEN 6 AND 8 THEN '06:00-09:00'
                        WHEN EXTRACT(HOUR FROM hora_registro) BETWEEN 9 AND 11 THEN '09:00-12:00'
                        WHEN EXTRACT(HOUR FROM hora_registro) BETWEEN 12 AND 14 THEN '12:00-15:00'
                        WHEN EXTRACT(HOUR FROM hora_registro) BETWEEN 15 AND 17 THEN '15:00-18:00'
                        WHEN EXTRACT(HOUR FROM hora_registro) BETWEEN 18 AND 20 THEN '18:00-21:00'
                            ELSE 'Otros'
                        END as franja_horaria,
                        COUNT(*) as asistencias
                    FROM asistencias 
                    WHERE fecha >= CURRENT_DATE - INTERVAL '30 days' AND hora_registro IS NOT NULL
                    GROUP BY dia_semana, franja_horaria
                    ORDER BY asistencias DESC
                """)
                
                peak_data = cursor.fetchall()
                
                return {'peak_hours_analysis': peak_data}
                
        except Exception as e:
            logging.error(f"Error en reporte de horas pico: {e}")
            return {}
    
    def _generate_class_occupancy_report(self):
        """Genera reporte de ocupación de clases"""
        try:
            with self.db_manager.get_connection_context() as conn:
                cursor = conn.cursor()
                try:
                    self.db_manager._apply_readonly_timeouts(cursor)
                except Exception:
                    pass
                
                # Ocupación por clase
                cursor.execute("""
                    SELECT 
                        c.nombre as clase,
                        ch.dia_semana,
                        ch.hora_inicio,
                        ch.cupo_maximo,
                        COUNT(cu.usuario_id) as inscritos,
                        ROUND((COUNT(cu.usuario_id) * 100.0 / ch.cupo_maximo), 2) as porcentaje_ocupacion
                    FROM clases c
                    JOIN clases_horarios ch ON c.id = ch.clase_id
                    LEFT JOIN clase_usuarios cu ON ch.id = cu.clase_horario_id
                    WHERE ch.cupo_maximo > 0
                    GROUP BY c.id, ch.id
                    ORDER BY porcentaje_ocupacion DESC
                """)
                
                occupancy_data = cursor.fetchall()
                
                # Estadísticas de ocupación globales correctas
                cursor.execute("""
                    SELECT 
                        AVG(occ) AS ocupacion_promedio,
                        MAX(occ) AS ocupacion_maxima,
                        MIN(occ) AS ocupacion_minima
                    FROM (
                        SELECT (COUNT(cu.usuario_id)::float / ch.cupo_maximo * 100) AS occ
                        FROM clases_horarios ch
                        LEFT JOIN clase_usuarios cu ON ch.id = cu.clase_horario_id
                        WHERE ch.cupo_maximo > 0
                        GROUP BY ch.id
                    ) t
                """)
                
            stats = cursor.fetchone()
                
            return {
                    'class_occupancy': occupancy_data,
                    'avg_occupancy': stats[0] if stats else 0,
                    'max_occupancy': stats[1] if stats else 0,
                    'min_occupancy': stats[2] if stats else 0
                }
                
        except Exception as e:
            logging.error(f"Error en reporte de ocupación: {e}")
            return {}
    
    def _generate_payment_due_report(self):
        """Genera reporte de pagos pendientes"""
        try:
            with self.db_manager.get_connection_context() as conn:
                cursor = conn.cursor()
                try:
                    self.db_manager._apply_readonly_timeouts(cursor)
                except Exception:
                    pass
                
                # Usuarios con pagos pendientes
                mes_actual = datetime.now().month
                año_actual = datetime.now().year
                
                cursor.execute("""
                    SELECT 
                        u.nombre,
                        u.telefono,
                        u.tipo_membresia,
                        CASE 
                            WHEN p.id IS NULL THEN 'Sin pago este mes'
                            ELSE 'Pago registrado'
                        END as estado_pago,
                        u.fecha_registro
                    FROM usuarios u
                    LEFT JOIN pagos p ON u.id = p.usuario_id AND EXTRACT(MONTH FROM p.fecha_pago) = %s AND EXTRACT(YEAR FROM p.fecha_pago) = %s
                    WHERE u.activo = true AND u.rol IN ('socio','profesor')
                    ORDER BY u.nombre
                """, (mes_actual, año_actual))
                
                payment_data = cursor.fetchall()
                
                # Estadísticas de pagos
                cursor.execute("""
                    SELECT 
                        COUNT(*) as total_socios,
                        COUNT(p.id) as pagos_realizados,
                        (COUNT(*) - COUNT(p.id)) as pagos_pendientes
                    FROM usuarios u
                    LEFT JOIN pagos p ON u.id = p.usuario_id AND EXTRACT(MONTH FROM p.fecha_pago) = %s AND EXTRACT(YEAR FROM p.fecha_pago) = %s
                    WHERE u.activo = true AND u.rol IN ('socio','profesor')
                """, (mes_actual, año_actual))
                
                stats = cursor.fetchone()
                
                return {
                    'payment_status': payment_data,
                    'total_members': stats[0] if stats else 0,
                    'payments_made': stats[1] if stats else 0,
                    'payments_pending': stats[2] if stats else 0
                }
                
        except Exception as e:
            logging.error(f"Error en reporte de pagos: {e}")
            return {}
    
    def _generate_professor_efficiency_report(self):
        """Genera reporte de eficiencia de profesores"""
        try:
            with self.db_manager.get_connection_context() as conn:
                cursor = conn.cursor()
                try:
                    self.db_manager._apply_readonly_timeouts(cursor)
                except Exception:
                    pass
                
                # Eficiencia por profesor (corrige agregaciones)
                cursor.execute("""
                    WITH occ_per_schedule AS (
                        SELECT ch.id AS ch_id,
                               (COUNT(cu.usuario_id)::float / NULLIF(ch.cupo_maximo, 0) * 100) AS occ
                        FROM clases_horarios ch
                        LEFT JOIN clase_usuarios cu ON ch.id = cu.clase_horario_id
                        WHERE ch.cupo_maximo > 0
                        GROUP BY ch.id
                    )
                    SELECT 
                        p.nombre AS profesor,
                        p.especialidad,
                        COUNT(DISTINCT c.id) AS clases_asignadas,
                        COUNT(DISTINCT ch.id) AS horarios_totales,
                        COUNT(DISTINCT cu.usuario_id) AS estudiantes_totales,
                        COALESCE(AVG(ops.occ), 0) AS ocupacion_promedio
                    FROM profesores p
                    LEFT JOIN clases c ON p.id = c.profesor_id
                    LEFT JOIN clases_horarios ch ON c.id = ch.clase_id
                    LEFT JOIN occ_per_schedule ops ON ops.ch_id = ch.id
                    LEFT JOIN clase_usuarios cu ON ch.id = cu.clase_horario_id
                    WHERE p.activo = true
                    GROUP BY p.id, p.nombre, p.especialidad
                    ORDER BY ocupacion_promedio DESC
                """)
                
                professor_data = cursor.fetchall()
                
                return {'professor_efficiency': professor_data}
                
        except Exception as e:
            logging.error(f"Error en reporte de profesores: {e}")
            return {}

class OperationalReportsWidget(QWidget):
    """Widget para reportes operativos mejorados"""
    
    def __init__(self, db_manager, parent=None):
        super().__init__(parent)
        self.db_manager = db_manager
        self.report_thread = None
        self.setup_ui()
        
    def setup_ui(self):
        """Configura la interfaz del widget"""
        layout = QVBoxLayout(self)
        layout.setContentsMargins(15, 15, 15, 15)
        layout.setSpacing(10)
        
        # Título
        title = QLabel("Reportes Operativos Avanzados")
        title.setFont(QFont("Segoe UI", 16, QFont.Weight.Bold))
        title.setObjectName("operational_reports_title")
        layout.addWidget(title)
        
        # Tabs para diferentes tipos de reportes
        self.tab_widget = QTabWidget()
        
        # Tab 1: Asistencia Diaria
        self.attendance_tab = self.create_attendance_tab()
        self.tab_widget.addTab(self.attendance_tab, "📅 Asistencia Diaria")
        
        # Tab 2: Análisis de Horas Pico
        self.peak_hours_tab = self.create_peak_hours_tab()
        self.tab_widget.addTab(self.peak_hours_tab, "Horas Pico")
        
        # Tab 3: Ocupación de Clases
        self.occupancy_tab = self.create_occupancy_tab()
        self.tab_widget.addTab(self.occupancy_tab, "Ocupación Clases")
        
        # Tab 4: Pagos Pendientes
        self.payments_tab = self.create_payments_tab()
        self.tab_widget.addTab(self.payments_tab, "💰 Pagos Pendientes")
        
        # Tab 5: Eficiencia Profesores
        self.professors_tab = self.create_professors_tab()
        self.tab_widget.addTab(self.professors_tab, "👨‍🏫 Eficiencia Profesores")
        
        layout.addWidget(self.tab_widget)

        # Control de autoload para evitar consultas pesadas al iniciar
        self.auto_load_checkbox = QCheckBox("Auto cargar al cambiar pestaña")
        self.auto_load_checkbox.setChecked(False)
        layout.addWidget(self.auto_load_checkbox)
        
        # Barra de progreso
        self.progress_bar = QProgressBar()
        self.progress_bar.setVisible(False)
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
        layout.addWidget(self.progress_bar)
        
        # Botones de control
        buttons_layout = QHBoxLayout()
        
        self.refresh_button = QPushButton("🔄 Actualizar Reportes")
        self.refresh_button.clicked.connect(self.refresh_all_reports)
        self.refresh_button.setObjectName("refresh_reports_button")

        self.export_button = QPushButton("📤 Exportar Reportes")
        self.export_button.clicked.connect(self.export_reports)
        self.export_button.setObjectName("export_reports_button")

        # Botón de métricas de caché
        self.cache_metrics_button = QPushButton("📊 Métricas Caché")
        self.cache_metrics_button.clicked.connect(self.open_cache_metrics)
        self.cache_metrics_button.setObjectName("cache_metrics_button")
        
        buttons_layout.addStretch()
        buttons_layout.addWidget(self.cache_metrics_button)
        buttons_layout.addWidget(self.export_button)
        buttons_layout.addWidget(self.refresh_button)
        
        layout.addLayout(buttons_layout)
        
        # Aplicar objectNames para estilos
        self.tab_widget.setObjectName("operational_reports_tabs")
        
        # No cargar datos automáticamente al iniciar; diferir hasta interacción del usuario
        try:
            self.tab_widget.currentChanged.connect(self._on_tab_changed)
        except Exception:
            pass

    def _on_tab_changed(self, index: int):
        """Carga diferida según preferencia de autoload."""
        try:
            if self.auto_load_checkbox.isChecked():
                self.refresh_all_reports()
        except Exception:
            pass

    def open_cache_metrics(self):
        """Abre el diálogo de métricas de caché."""
        try:
            dlg = CacheMetricsDialog(self.db_manager, self)
            dlg.exec()
        except Exception:
            pass
    
    def create_attendance_tab(self):
        """Crea la pestaña de asistencia diaria"""
        tab = QWidget()
        layout = QVBoxLayout(tab)
        
        # Gráfico de asistencia diaria
        self.attendance_chart = MplChartWidget(figsize=(10, 6), enable_toolbar=False)
        layout.addWidget(self.attendance_chart)
        
        # Tabla de estadísticas
        stats_group = QGroupBox("Estadísticas de Asistencia")
        stats_layout = QGridLayout(stats_group)
        
        self.total_attendance_label = QLabel("Total: 0")
        self.unique_users_label = QLabel("Usuarios únicos: 0")
        self.daily_average_label = QLabel("Promedio diario: 0")
        
        stats_layout.addWidget(self.total_attendance_label, 0, 0)
        stats_layout.addWidget(self.unique_users_label, 0, 1)
        stats_layout.addWidget(self.daily_average_label, 0, 2)
        
        layout.addWidget(stats_group)
        
        # Tabla de asistencia por día (para exportar y ver detalle)
        self.attendance_table = QTableWidget()
        self.attendance_table.setColumnCount(2)
        self.attendance_table.setHorizontalHeaderLabels(["Fecha", "Asistencias"])
        self.attendance_table.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeMode.Stretch)
        layout.addWidget(self.attendance_table)
        
        return tab
    
    def create_peak_hours_tab(self):
        """Crea la pestaña de análisis de horas pico"""
        tab = QWidget()
        layout = QVBoxLayout(tab)
        
        # Gráfico de horas pico
        self.peak_hours_chart = MplChartWidget(figsize=(10, 6), enable_toolbar=False)
        layout.addWidget(self.peak_hours_chart)
        
        # Tabla de análisis detallado
        self.peak_hours_table = QTableWidget()
        self.peak_hours_table.setColumnCount(4)
        self.peak_hours_table.setHorizontalHeaderLabels(["Día", "Franja Horaria", "Asistencias", "% del Total"])
        self.peak_hours_table.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeMode.Stretch)
        
        layout.addWidget(self.peak_hours_table)
        
        return tab
    
    def create_occupancy_tab(self):
        """Crea la pestaña de ocupación de clases"""
        tab = QWidget()
        layout = QVBoxLayout(tab)
        
        # Gráfico de ocupación
        self.occupancy_chart = MplChartWidget(figsize=(10, 6), enable_toolbar=False)
        layout.addWidget(self.occupancy_chart)
        
        # Tabla de ocupación por clase
        self.occupancy_table = QTableWidget()
        self.occupancy_table.setColumnCount(6)
        self.occupancy_table.setHorizontalHeaderLabels([
            "Clase", "Día", "Hora", "Cupo Máximo", "Inscritos", "% Ocupación"
        ])
        self.occupancy_table.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeMode.Stretch)
        
        layout.addWidget(self.occupancy_table)
        
        return tab
    
    def create_payments_tab(self):
        """Crea la pestaña de pagos pendientes"""
        tab = QWidget()
        layout = QVBoxLayout(tab)
        
        # Estadísticas de pagos
        stats_group = QGroupBox("Resumen de Pagos")
        stats_layout = QGridLayout(stats_group)
        
        self.total_members_label = QLabel("Total socios: 0")
        self.payments_made_label = QLabel("Pagos realizados: 0")
        self.payments_pending_label = QLabel("Pagos pendientes: 0")
        
        stats_layout.addWidget(self.total_members_label, 0, 0)
        stats_layout.addWidget(self.payments_made_label, 0, 1)
        stats_layout.addWidget(self.payments_pending_label, 0, 2)
        
        layout.addWidget(stats_group)
        
        # Tabla de pagos pendientes
        self.payments_table = QTableWidget()
        self.payments_table.setColumnCount(5)
        self.payments_table.setHorizontalHeaderLabels([
            "Nombre", "Teléfono", "Membresía", "Estado", "Fecha Registro"
        ])
        self.payments_table.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeMode.Stretch)
        
        layout.addWidget(self.payments_table)
        
        return tab
    
    def create_professors_tab(self):
        """Crea la pestaña de eficiencia de profesores"""
        tab = QWidget()
        layout = QVBoxLayout(tab)
        
        # Gráfico de eficiencia
        self.professors_chart = MplChartWidget(figsize=(10, 6), enable_toolbar=False)
        layout.addWidget(self.professors_chart)
        
        # Tabla de eficiencia por profesor
        self.professors_table = QTableWidget()
        self.professors_table.setColumnCount(6)
        self.professors_table.setHorizontalHeaderLabels([
            "Profesor", "Especialidad", "Clases", "Horarios", "Estudiantes", "% Ocupación"
        ])
        self.professors_table.horizontalHeader().setSectionResizeMode(QHeaderView.ResizeMode.Stretch)
        
        layout.addWidget(self.professors_table)
        
        return tab
    
    def refresh_all_reports(self):
        """Actualiza todos los reportes"""
        current_tab = self.tab_widget.currentIndex()
        
        if current_tab == 0:  # Asistencia
            self.load_attendance_report()
        elif current_tab == 1:  # Horas pico
            self.load_peak_hours_report()
        elif current_tab == 2:  # Ocupación
            self.load_occupancy_report()
        elif current_tab == 3:  # Pagos
            self.load_payments_report()
        elif current_tab == 4:  # Profesores
            self.load_professors_report()
    
    def _start_loading(self):
        try:
            self.progress_bar.setVisible(True)
            self.progress_bar.setRange(0, 0)  # indeterminate
            self.refresh_button.setEnabled(False)
            self.export_button.setEnabled(False)
        except Exception:
            pass
        
    def _stop_loading(self):
        try:
            self.progress_bar.setVisible(False)
            self.progress_bar.setRange(0, 100)
            self.refresh_button.setEnabled(True)
            self.export_button.setEnabled(True)
        except Exception:
            pass
    
    def load_attendance_report(self):
        """Carga el reporte de asistencia diaria"""
        if self.report_thread and self.report_thread.isRunning():
            return
        
        self._start_loading()
        self.report_thread = OperationalReportsThread(self.db_manager, 'daily_attendance')
        self.report_thread.report_ready.connect(self.update_attendance_display)
        self.report_thread.error_occurred.connect(self.handle_report_error)
        self.report_thread.start()
    
    def update_attendance_display(self, data):
        """Actualiza la visualización de asistencia"""
        try:
            # Actualizar estadísticas
            self.total_attendance_label.setText(f"Total: {data.get('total_attendance', 0)}")
            self.unique_users_label.setText(f"Usuarios únicos: {data.get('unique_users', 0)}")
            self.daily_average_label.setText(f"Promedio diario: {data.get('daily_average', 0):.1f}")
            
            # Actualizar gráfico y tabla
            daily_data = data.get('daily_attendance', [])
            if daily_data:
                dates = [row[0] for row in daily_data]
                attendance = [row[1] for row in daily_data]
                
                self.attendance_chart.plot_line_chart(
                    dates, attendance,
                    title="Asistencia Diaria (Últimos 30 días)",
                    x_label="Fecha",
                    y_label="Asistencias"
                )
                
                # Llenar tabla
                self.attendance_table.setRowCount(len(daily_data))
                for i, (dia, asistencias) in enumerate(daily_data):
                    self.attendance_table.setItem(i, 0, QTableWidgetItem(str(dia)))
                    self.attendance_table.setItem(i, 1, QTableWidgetItem(str(asistencias)))
            else:
                self.attendance_table.setRowCount(0)
            
        except Exception as e:
            logging.error(f"Error actualizando display de asistencia: {e}")
        finally:
            self._stop_loading()
    
    def load_peak_hours_report(self):
        """Carga el reporte de horas pico"""
        if self.report_thread and self.report_thread.isRunning():
            return
        
        self._start_loading()
        self.report_thread = OperationalReportsThread(self.db_manager, 'peak_hours')
        self.report_thread.report_ready.connect(self.update_peak_hours_display)
        self.report_thread.error_occurred.connect(self.handle_report_error)
        self.report_thread.start()
    
    def update_peak_hours_display(self, data):
        """Actualiza la visualización de horas pico"""
        try:
            peak_data = data.get('peak_hours_analysis', [])
            
            # Actualizar tabla
            self.peak_hours_table.setRowCount(len(peak_data))
            total_attendance = sum(row[2] for row in peak_data) if peak_data else 0
            if total_attendance == 0:
                total_attendance = 1
            
            for i, (day, hour, attendance) in enumerate(peak_data):
                percentage = (attendance / total_attendance) * 100
                
                self.peak_hours_table.setItem(i, 0, QTableWidgetItem(day))
                self.peak_hours_table.setItem(i, 1, QTableWidgetItem(hour))
                self.peak_hours_table.setItem(i, 2, QTableWidgetItem(str(attendance)))
                self.peak_hours_table.setItem(i, 3, QTableWidgetItem(f"{percentage:.1f}%"))
            
            # Actualizar gráfico
            if peak_data:
                # Agrupar por franja horaria
                hourly_totals = defaultdict(int)
                for day, hour, attendance in peak_data:
                    hourly_totals[hour] += attendance
                
                hours = list(hourly_totals.keys())
                totals = list(hourly_totals.values())
                
                self.peak_hours_chart.plot_bar_chart(
                    hours, totals,
                    title="Distribución de Asistencias por Franja Horaria",
                    x_label="Franja Horaria",
                    y_label="Total Asistencias"
                )
            
        except Exception as e:
            logging.error(f"Error actualizando display de horas pico: {e}")
        finally:
            self._stop_loading()
    
    def load_occupancy_report(self):
        """Carga el reporte de ocupación de clases"""
        if self.report_thread and self.report_thread.isRunning():
            return
        
        self._start_loading()
        self.report_thread = OperationalReportsThread(self.db_manager, 'class_occupancy')
        self.report_thread.report_ready.connect(self.update_occupancy_display)
        self.report_thread.error_occurred.connect(self.handle_report_error)
        self.report_thread.start()
    
    def update_occupancy_display(self, data):
        """Actualiza la visualización de ocupación"""
        try:
            occupancy_data = data.get('class_occupancy', [])
            
            # Actualizar tabla
            self.occupancy_table.setRowCount(len(occupancy_data))
            
            for i, (clase, dia, hora, cupo, inscritos, porcentaje) in enumerate(occupancy_data):
                self.occupancy_table.setItem(i, 0, QTableWidgetItem(clase))
                self.occupancy_table.setItem(i, 1, QTableWidgetItem(dia))
                self.occupancy_table.setItem(i, 2, QTableWidgetItem(hora))
                self.occupancy_table.setItem(i, 3, QTableWidgetItem(str(cupo)))
                self.occupancy_table.setItem(i, 4, QTableWidgetItem(str(inscritos)))
                self.occupancy_table.setItem(i, 5, QTableWidgetItem(f"{porcentaje:.1f}%"))
            
            # Actualizar gráfico
            if occupancy_data:
                classes = [f"{row[0]} ({row[1]} {row[2]})" for row in occupancy_data[:10]]  # Top 10
                percentages = [row[5] for row in occupancy_data[:10]]
                
                self.occupancy_chart.plot_bar_chart(
                    classes, percentages,
                    title="Top 10 Clases por % de Ocupación",
                    x_label="Clase",
                    y_label="% Ocupación"
                )
            
        except Exception as e:
            logging.error(f"Error actualizando display de ocupación: {e}")
        finally:
            self._stop_loading()
    
    def load_payments_report(self):
        """Carga el reporte de pagos pendientes"""
        if self.report_thread and self.report_thread.isRunning():
            return
        
        self._start_loading()
        self.report_thread = OperationalReportsThread(self.db_manager, 'payment_due')
        self.report_thread.report_ready.connect(self.update_payments_display)
        self.report_thread.error_occurred.connect(self.handle_report_error)
        self.report_thread.start()
    
    def update_payments_display(self, data):
        """Actualiza la visualización de pagos"""
        try:
            # Actualizar estadísticas
            self.total_members_label.setText(f"Total socios: {data.get('total_members', 0)}")
            self.payments_made_label.setText(f"Pagos realizados: {data.get('payments_made', 0)}")
            self.payments_pending_label.setText(f"Pagos pendientes: {data.get('payments_pending', 0)}")
            
            # Actualizar tabla
            payment_data = data.get('payment_status', [])
            self.payments_table.setRowCount(len(payment_data))
            
            for i, (nombre, telefono, membresia, estado, fecha) in enumerate(payment_data):
                self.payments_table.setItem(i, 0, QTableWidgetItem(str(nombre)))
                self.payments_table.setItem(i, 1, QTableWidgetItem(str(telefono or "")))
                self.payments_table.setItem(i, 2, QTableWidgetItem(str(membresia or "")))
                self.payments_table.setItem(i, 3, QTableWidgetItem(str(estado)))
                self.payments_table.setItem(i, 4, QTableWidgetItem(str(fecha)))
                
                # Colorear filas según estado
                if estado == "Sin pago este mes":
                    for j in range(5):
                        item = self.payments_table.item(i, j)
                        if item:
                            item.setBackground(QColor(255, 200, 200))  # Rojo claro
            
        except Exception as e:
            logging.error(f"Error actualizando display de pagos: {e}")
        finally:
            self._stop_loading()
    
    def load_professors_report(self):
        """Carga el reporte de eficiencia de profesores"""
        if self.report_thread and self.report_thread.isRunning():
            return
        
        self._start_loading()
        self.report_thread = OperationalReportsThread(self.db_manager, 'professor_efficiency')
        self.report_thread.report_ready.connect(self.update_professors_display)
        self.report_thread.error_occurred.connect(self.handle_report_error)
        self.report_thread.start()
    
    def update_professors_display(self, data):
        """Actualiza la visualización de profesores"""
        try:
            professor_data = data.get('professor_efficiency', [])
            
            # Actualizar tabla
            self.professors_table.setRowCount(len(professor_data))
            
            for i, (nombre, especialidad, clases, horarios, estudiantes, ocupacion) in enumerate(professor_data):
                self.professors_table.setItem(i, 0, QTableWidgetItem(nombre))
                self.professors_table.setItem(i, 1, QTableWidgetItem(especialidad or ""))
                self.professors_table.setItem(i, 2, QTableWidgetItem(str(clases or 0)))
                self.professors_table.setItem(i, 3, QTableWidgetItem(str(horarios or 0)))
                self.professors_table.setItem(i, 4, QTableWidgetItem(str(estudiantes or 0)))
                self.professors_table.setItem(i, 5, QTableWidgetItem(f"{ocupacion or 0:.1f}%"))
            
            # Actualizar gráfico
            if professor_data:
                professors = [row[0] for row in professor_data]
                occupancy_rates = [row[5] or 0 for row in professor_data]
                
                self.professors_chart.plot_bar_chart(
                    professors, occupancy_rates,
                    title="Eficiencia de Profesores (% Ocupación Promedio)",
                    x_label="Profesor",
                    y_label="% Ocupación"
                )
            
        except Exception as e:
            logging.error(f"Error actualizando display de profesores: {e}")
        finally:
            self._stop_loading()
    
    def handle_report_error(self, error_message):
        """Maneja errores en la generación de reportes"""
        try:
            # Evitar mostrar diálogos si el widget ya no es visible o fue destruido
            if hasattr(self, 'isVisible') and self.isVisible():
                QMessageBox.warning(
                    self,
                    "Error en Reporte",
                    f"Error al generar el reporte: {error_message}"
                )
        except RuntimeError:
            # Ignorar si el objeto Qt ya fue eliminado
            pass
        finally:
            self._stop_loading()
    
    def export_reports(self):
        """Exporta los reportes actuales"""
        from PyQt6.QtWidgets import QFileDialog, QDialog, QVBoxLayout, QHBoxLayout, QCheckBox, QDialogButtonBox, QLabel
        import csv
        import json
        from datetime import datetime
        import os
        
        # Crear diálogo de selección de reportes
        dialog = QDialog(self)
        dialog.setWindowTitle("Exportar Reportes")
        dialog.setModal(True)
        dialog.resize(400, 300)
        
        layout = QVBoxLayout(dialog)
        
        # Título
        title_label = QLabel("Seleccione los reportes a exportar:")
        title_label.setProperty("class", "section-title")
        layout.addWidget(title_label)
        
        # Checkboxes para cada reporte
        self.export_attendance_check = QCheckBox("Reporte de Asistencia Diaria")
        self.export_peak_hours_check = QCheckBox("Reporte de Horas Pico")
        self.export_occupancy_check = QCheckBox("Reporte de Ocupación de Clases")
        self.export_payments_check = QCheckBox("Reporte de Pagos Pendientes")
        self.export_professors_check = QCheckBox("Reporte de Eficiencia de Profesores")
        
        # Marcar todos por defecto
        self.export_attendance_check.setChecked(True)
        self.export_peak_hours_check.setChecked(True)
        self.export_occupancy_check.setChecked(True)
        self.export_payments_check.setChecked(True)
        self.export_professors_check.setChecked(True)
        
        layout.addWidget(self.export_attendance_check)
        layout.addWidget(self.export_peak_hours_check)
        layout.addWidget(self.export_occupancy_check)
        layout.addWidget(self.export_payments_check)
        layout.addWidget(self.export_professors_check)
        
        # Botones
        button_box = QDialogButtonBox(QDialogButtonBox.StandardButton.Ok | QDialogButtonBox.StandardButton.Cancel)
        button_box.accepted.connect(dialog.accept)
        button_box.rejected.connect(dialog.reject)
        layout.addWidget(button_box)
        
        if dialog.exec() == QDialog.DialogCode.Accepted:
            # Seleccionar directorio de destino
            export_dir = QFileDialog.getExistingDirectory(
                self,
                "Seleccionar directorio de exportación",
                os.path.expanduser("~/Desktop")
            )
            
            if export_dir:
                try:
                    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                    exported_files = []
                    
                    # Exportar cada reporte seleccionado
                    if self.export_attendance_check.isChecked():
                        file_path = self._export_attendance_report(export_dir, timestamp)
                        if file_path:
                            exported_files.append(file_path)
                    
                    if self.export_peak_hours_check.isChecked():
                        file_path = self._export_peak_hours_report(export_dir, timestamp)
                        if file_path:
                            exported_files.append(file_path)
                    
                    if self.export_occupancy_check.isChecked():
                        file_path = self._export_occupancy_report(export_dir, timestamp)
                        if file_path:
                            exported_files.append(file_path)
                    
                    if self.export_payments_check.isChecked():
                        file_path = self._export_payments_report(export_dir, timestamp)
                        if file_path:
                            exported_files.append(file_path)
                    
                    if self.export_professors_check.isChecked():
                        file_path = self._export_professors_report(export_dir, timestamp)
                        if file_path:
                            exported_files.append(file_path)
                    
                    if exported_files:
                        files_list = "\n".join([os.path.basename(f) for f in exported_files])
                        try:
                            if hasattr(self, 'isVisible') and self.isVisible():
                                QMessageBox.information(
                                    self,
                                    "Exportación Exitosa",
                                    f"Reportes exportados exitosamente:\n\n{files_list}\n\nUbicación: {export_dir}"
                                )
                        except RuntimeError:
                            pass
                    else:
                        try:
                            if hasattr(self, 'isVisible') and self.isVisible():
                                QMessageBox.warning(
                                    self,
                                    "Sin Datos",
                                    "No se encontraron datos para exportar en los reportes seleccionados."
                                )
                        except RuntimeError:
                            pass
                        
                except Exception as e:
                    try:
                        if hasattr(self, 'isVisible') and self.isVisible():
                            QMessageBox.critical(
                                 self,
                                 "Error de Exportación",
                                 f"Error al exportar reportes: {str(e)}"
                            )
                    except RuntimeError:
                        pass
    
    def _export_attendance_report(self, export_dir, timestamp):
        """Exporta el reporte de asistencia diaria"""
        try:
            import csv
            file_path = os.path.join(export_dir, f"reporte_asistencia_{timestamp}.csv")
            
            with open(file_path, 'w', newline='', encoding='utf-8') as csvfile:
                writer = csv.writer(csvfile)
                writer.writerow(['Fecha', 'Asistencias'])
                
                # Obtener datos de asistencia (tabla de resumen diario)
                for row in range(self.attendance_table.rowCount()):
                    row_data = []
                    for col in range(self.attendance_table.columnCount()):
                        item = self.attendance_table.item(row, col)
                        row_data.append(item.text() if item else '')
                    writer.writerow(row_data)
            
            return file_path
        except Exception as e:
            print(f"Error exportando reporte de asistencia: {e}")
            return None
    
    def _export_peak_hours_report(self, export_dir, timestamp):
        """Exporta el reporte de horas pico"""
        try:
            import csv
            file_path = os.path.join(export_dir, f"reporte_horas_pico_{timestamp}.csv")
            
            with open(file_path, 'w', newline='', encoding='utf-8') as csvfile:
                writer = csv.writer(csvfile)
                writer.writerow(['Día', 'Franja Horaria', 'Asistencias', '% del Total'])
                
                # Obtener datos de horas pico
                for row in range(self.peak_hours_table.rowCount()):
                    row_data = []
                    for col in range(self.peak_hours_table.columnCount()):
                        item = self.peak_hours_table.item(row, col)
                        row_data.append(item.text() if item else '')
                    writer.writerow(row_data)
            
            return file_path
        except Exception as e:
            print(f"Error exportando reporte de horas pico: {e}")
            return None
    
    def _export_occupancy_report(self, export_dir, timestamp):
        """Exporta el reporte de ocupación de clases"""
        try:
            import csv
            file_path = os.path.join(export_dir, f"reporte_ocupacion_clases_{timestamp}.csv")
            
            with open(file_path, 'w', newline='', encoding='utf-8') as csvfile:
                writer = csv.writer(csvfile)
                writer.writerow(['Clase', 'Día', 'Hora', 'Cupo Máximo', 'Inscritos', '% Ocupación'])
                
                # Obtener datos de ocupación
                for row in range(self.occupancy_table.rowCount()):
                    row_data = []
                    for col in range(self.occupancy_table.columnCount()):
                        item = self.occupancy_table.item(row, col)
                        row_data.append(item.text() if item else '')
                    writer.writerow(row_data)
            
            return file_path
        except Exception as e:
            print(f"Error exportando reporte de ocupación: {e}")
            return None
    
    def _export_payments_report(self, export_dir, timestamp):
        """Exporta el reporte de pagos pendientes"""
        try:
            import csv
            file_path = os.path.join(export_dir, f"reporte_pagos_pendientes_{timestamp}.csv")
            
            with open(file_path, 'w', newline='', encoding='utf-8') as csvfile:
                writer = csv.writer(csvfile)
                writer.writerow(['Nombre', 'Teléfono', 'Membresía', 'Estado', 'Fecha Registro'])
                
                # Obtener datos de pagos
                for row in range(self.payments_table.rowCount()):
                    row_data = []
                    for col in range(self.payments_table.columnCount()):
                        item = self.payments_table.item(row, col)
                        row_data.append(item.text() if item else '')
                    writer.writerow(row_data)
            
            return file_path
        except Exception as e:
            print(f"Error exportando reporte de pagos: {e}")
            return None
    
    def _export_professors_report(self, export_dir, timestamp):
        """Exporta el reporte de eficiencia de profesores"""
        try:
            import csv
            file_path = os.path.join(export_dir, f"reporte_eficiencia_profesores_{timestamp}.csv")
            
            with open(file_path, 'w', newline='', encoding='utf-8') as csvfile:
                writer = csv.writer(csvfile)
                writer.writerow(['Profesor', 'Especialidad', 'Clases', 'Horarios', 'Estudiantes', '% Ocupación'])
                
                # Obtener datos de profesores
                for row in range(self.professors_table.rowCount()):
                    row_data = []
                    for col in range(self.professors_table.columnCount()):
                        item = self.professors_table.item(row, col)
                        row_data.append(item.text() if item else '')
                    writer.writerow(row_data)
            
            return file_path
        except Exception as e:
            print(f"Error exportando reporte de profesores: {e}")
            return None

