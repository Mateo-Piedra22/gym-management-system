from PyQt6.QtWidgets import (
    QDialog, QVBoxLayout, QHBoxLayout, QGridLayout, QLabel, QComboBox,
    QDateEdit, QPushButton, QTableWidget, QTableWidgetItem, QGroupBox,
    QLineEdit, QMessageBox, QHeaderView, QFrame, QSplitter, QWidget,
    QProgressBar, QTextEdit, QCheckBox
)
from PyQt6.QtCore import Qt, QDate, pyqtSignal, QTimer, QThread, pyqtSlot
from PyQt6.QtGui import QFont, QPalette, QColor, QPainter, QPen
import matplotlib.pyplot as plt
from matplotlib.backends.backend_qt5agg import FigureCanvasQTAgg as FigureCanvas
from matplotlib.figure import Figure
import csv
import os
from datetime import datetime, timedelta
from collections import defaultdict
import pandas as pd
from database import DatabaseManager
from PyQt6 import sip

class AttendanceViewerDialog(QDialog):
    def __init__(self, db_manager, parent=None):
        super().__init__(parent)
        self.db_manager = db_manager
        self.current_user_id = None
        self.attendance_data = []
        self._closing = False
        
        self.setWindowTitle("Visor de Asistencias")
        self.setModal(True)
        self.resize(1200, 800)
        
        # Usar estilos globales desde styles/style.qss (variables y temas)
        # Se eliminó el setStyleSheet inline para permitir branding unificado.
        
        self.setup_ui()
        self.connect_signals()
        self.load_users()
        
    def setup_ui(self):
        main_layout = QVBoxLayout(self)
        
        # Crear splitter principal
        splitter = QSplitter(Qt.Orientation.Horizontal)
        
        # Panel izquierdo - Filtros y controles
        left_panel = self.create_left_panel()
        splitter.addWidget(left_panel)
        
        # Panel derecho - Tabla y estadísticas
        right_panel = self.create_right_panel()
        splitter.addWidget(right_panel)
        
        # Configurar proporciones del splitter
        splitter.setSizes([350, 850])
        
        main_layout.addWidget(splitter)
        
        # Botones de acción
        button_layout = QHBoxLayout()
        
        self.export_csv_btn = QPushButton("Exportar CSV")
        self.export_excel_btn = QPushButton("Exportar Excel")
        self.refresh_btn = QPushButton("Actualizar")
        self.close_btn = QPushButton("Cerrar")
        
        # Aplicar clases de estilo globales
        self.export_csv_btn.setProperty("class", "secondary")
        self.export_excel_btn.setProperty("class", "secondary")
        self.refresh_btn.setProperty("class", "update-button")
        self.close_btn.setProperty("class", "secondary")
        
        button_layout.addWidget(self.export_csv_btn)
        button_layout.addWidget(self.export_excel_btn)
        button_layout.addStretch()
        button_layout.addWidget(self.refresh_btn)
        button_layout.addWidget(self.close_btn)
        
        main_layout.addLayout(button_layout)
        
    def create_left_panel(self):
        left_widget = QWidget()
        left_layout = QVBoxLayout(left_widget)
        
        # Grupo de filtros de usuario
        user_group = QGroupBox("Filtros de Usuario")
        user_layout = QGridLayout(user_group)
        
        user_layout.addWidget(QLabel("Usuario:"), 0, 0)
        self.user_combo = QComboBox()
        self.user_combo.setEditable(True)
        self.user_combo.setInsertPolicy(QComboBox.InsertPolicy.NoInsert)
        user_layout.addWidget(self.user_combo, 0, 1)
        
        self.all_users_check = QCheckBox("Todos los usuarios")
        user_layout.addWidget(self.all_users_check, 1, 0, 1, 2)
        
        left_layout.addWidget(user_group)
        
        # Grupo de filtros de fecha
        date_group = QGroupBox("Filtros de Fecha")
        date_layout = QGridLayout(date_group)
        
        date_layout.addWidget(QLabel("Desde:"), 0, 0)
        self.start_date = QDateEdit()
        self.start_date.setDate(QDate.currentDate().addDays(-30))
        self.start_date.setCalendarPopup(True)
        date_layout.addWidget(self.start_date, 0, 1)
        
        date_layout.addWidget(QLabel("Hasta:"), 1, 0)
        self.end_date = QDateEdit()
        self.end_date.setDate(QDate.currentDate())
        self.end_date.setCalendarPopup(True)
        date_layout.addWidget(self.end_date, 1, 1)
        
        left_layout.addWidget(date_group)
        
        # Filtros rápidos
        quick_group = QGroupBox("Filtros Rápidos")
        quick_layout = QVBoxLayout(quick_group)
        
        self.today_btn = QPushButton("Hoy")
        self.week_btn = QPushButton("Esta Semana")
        self.month_btn = QPushButton("Este Mes")
        self.last30_btn = QPushButton("Últimos 30 días")
        
        quick_layout.addWidget(self.today_btn)
        quick_layout.addWidget(self.week_btn)
        quick_layout.addWidget(self.month_btn)
        quick_layout.addWidget(self.last30_btn)
        
        left_layout.addWidget(quick_group)
        
        # Botón de búsqueda
        self.search_btn = QPushButton("Buscar")
        # Conectar señales se realiza en connect_signals para evitar duplicados
        # (antes se conectaba a un método inexistente perform_search)
        # self.search_btn.clicked.connect(self.perform_search)
        # Use standardized class instead of inline stylesheet
        self.search_btn.setProperty("class", "primary")
        left_layout.addWidget(self.search_btn)
        
        left_layout.addStretch()
        
        return left_widget
        
    def create_right_panel(self):
        right_widget = QWidget()
        right_layout = QVBoxLayout(right_widget)
        
        # Panel de estadísticas
        stats_group = QGroupBox("Estadísticas")
        stats_layout = QHBoxLayout(stats_group)
        
        self.total_label = QLabel("Total: 0")
        self.avg_label = QLabel("Promedio/día: 0")
        self.period_label = QLabel("Período: -")
        
        stats_layout.addWidget(self.total_label)
        stats_layout.addWidget(self.avg_label)
        stats_layout.addWidget(self.period_label)
        stats_layout.addStretch()
        
        right_layout.addWidget(stats_group)
        
        # Tabla de asistencias
        table_group = QGroupBox("Registro de Asistencias")
        table_layout = QVBoxLayout(table_group)
        
        self.attendance_table = QTableWidget()
        self.attendance_table.setColumnCount(4)
        self.attendance_table.setHorizontalHeaderLabels(["Fecha", "Usuario", "Hora Registro", "Tipo"])
        
        # Configurar tabla
        header = self.attendance_table.horizontalHeader()
        header.setSectionResizeMode(0, QHeaderView.ResizeMode.ResizeToContents)
        header.setSectionResizeMode(1, QHeaderView.ResizeMode.Stretch)
        header.setSectionResizeMode(2, QHeaderView.ResizeMode.ResizeToContents)
        header.setSectionResizeMode(3, QHeaderView.ResizeMode.ResizeToContents)
        
        self.attendance_table.setAlternatingRowColors(True)
        self.attendance_table.setSelectionBehavior(QTableWidget.SelectionBehavior.SelectRows)
        
        table_layout.addWidget(self.attendance_table)
        right_layout.addWidget(table_group)
        
        # Gráfico simple (placeholder)
        chart_group = QGroupBox("Gráfico de Asistencias")
        chart_layout = QVBoxLayout(chart_group)
        
        # Gráfico de asistencias usando matplotlib
        self.figure = Figure(figsize=(10, 4))
        self.canvas = FigureCanvas(self.figure)
        self.canvas.setMaximumHeight(200)
        chart_layout.addWidget(self.canvas)
        
        right_layout.addWidget(chart_group)
        
        return right_widget
        
    def connect_signals(self):
        # Conectar señales de botones
        self.search_btn.clicked.connect(self.search_attendances)
        self.refresh_btn.clicked.connect(self.search_attendances)
        self.close_btn.clicked.connect(self.close)
        
        # Filtros rápidos
        self.today_btn.clicked.connect(self.set_today_filter)
        self.week_btn.clicked.connect(self.set_week_filter)
        self.month_btn.clicked.connect(self.set_month_filter)
        self.last30_btn.clicked.connect(self.set_last30_filter)
        
        # Exportar
        self.export_csv_btn.clicked.connect(self.export_to_csv)
        self.export_excel_btn.clicked.connect(self.export_to_excel)
        
        # Checkbox todos los usuarios
        self.all_users_check.toggled.connect(self.toggle_user_selection)
        
    def load_users(self):
        """Cargar lista de usuarios en el combobox"""
        try:
            users = self.db_manager.obtener_todos_usuarios()
            self.user_combo.clear()
            self.user_combo.addItem("Seleccionar usuario...", None)
            
            for user in users:
                display_name = user.nombre
                self.user_combo.addItem(display_name, user.id)
                
        except Exception as e:
            QMessageBox.warning(self, "Error", f"Error al cargar usuarios: {str(e)}")
            
    def toggle_user_selection(self, checked):
        """Habilitar/deshabilitar selector de usuario"""
        self.user_combo.setEnabled(not checked)
        
    def set_today_filter(self):
        """Establecer filtro para hoy"""
        today = QDate.currentDate()
        self.start_date.setDate(today)
        self.end_date.setDate(today)
        
    def set_week_filter(self):
        """Establecer filtro para esta semana"""
        today = QDate.currentDate()
        start_of_week = today.addDays(-today.dayOfWeek() + 1)
        self.start_date.setDate(start_of_week)
        self.end_date.setDate(today)
        
    def set_month_filter(self):
        """Establecer filtro para este mes"""
        today = QDate.currentDate()
        start_of_month = QDate(today.year(), today.month(), 1)
        self.start_date.setDate(start_of_month)
        self.end_date.setDate(today)
        
    def set_last30_filter(self):
        """Establecer filtro para últimos 30 días"""
        today = QDate.currentDate()
        thirty_days_ago = today.addDays(-30)
        self.start_date.setDate(thirty_days_ago)
        self.end_date.setDate(today)
        
    def search_attendances(self):
        """Buscar asistencias según los filtros establecidos"""
        try:
            start_date_str = self.start_date.date().toString("yyyy-MM-dd")
            end_date_str = self.end_date.date().toString("yyyy-MM-dd")
            
            if self.all_users_check.isChecked():
                # Obtener todas las asistencias en el rango de fechas
                attendances = self.get_attendances_by_date_range(start_date_str, end_date_str)
            else:
                # Obtener asistencias de usuario específico
                user_id = self.user_combo.currentData()
                if user_id is None:
                    QMessageBox.warning(self, "Advertencia", "Por favor seleccione un usuario.")
                    return
                    
                attendances = self.get_user_attendances(user_id, start_date_str, end_date_str)
            
            self.attendance_data = attendances
            self.populate_table(attendances)
            self.update_statistics(attendances, start_date_str, end_date_str)
            self.update_chart(attendances)
            
        except Exception as e:
            QMessageBox.critical(self, "Error", f"Error al buscar asistencias: {str(e)}")
            
    def get_attendances_by_date_range(self, start_date, end_date):
        """Obtener todas las asistencias en un rango de fechas"""
        attendances = []
        current_date = datetime.strptime(start_date, "%Y-%m-%d")
        end_date_obj = datetime.strptime(end_date, "%Y-%m-%d")
        
        while current_date <= end_date_obj:
            daily_attendances = self.db_manager.obtener_asistencias_por_fecha(current_date)
            
            for attendance in daily_attendances:
                attendances.append({
                    'fecha': attendance['fecha'],
                    'usuario_id': attendance['usuario_id'],
                    'usuario_nombre': attendance['nombre_usuario'],
                    'hora_registro': attendance.get('hora_registro', 'N/A'),
                    'tipo': 'Regular'
                })
            
            current_date += timedelta(days=1)
            
        return attendances
        
    def get_user_attendances(self, user_id, start_date, end_date):
        """Obtener asistencias de un usuario específico"""
        attendances = []
        
        # Usar obtener_asistencias_usuario que consulta la tabla asistencias general
        user_attendances = self.db_manager.obtener_asistencias_usuario(user_id)
        
        user_info = self.db_manager.obtener_usuario_por_id(user_id)
        user_name = user_info.nombre if user_info else "Usuario desconocido"
        
        # Convertir fechas de filtro a objetos datetime para comparación
        start_date_obj = datetime.strptime(start_date, "%Y-%m-%d").date()
        end_date_obj = datetime.strptime(end_date, "%Y-%m-%d").date()
        
        for attendance in user_attendances:
            # El método obtener_asistencias_usuario devuelve: {'id': row[0], 'fecha': row[1], 'hora_registro': row[2]}
            attendance_date = attendance['fecha']
            
            # Convertir fecha de asistencia a objeto date para comparación
            if isinstance(attendance_date, str):
                try:
                    attendance_date_obj = datetime.strptime(attendance_date, "%Y-%m-%d").date()
                except ValueError:
                    # Intentar otros formatos de fecha si es necesario
                    try:
                        attendance_date_obj = datetime.strptime(attendance_date, "%d/%m/%Y").date()
                    except ValueError:
                        continue  # Saltar esta asistencia si no se puede parsear la fecha
            else:
                # Si ya es un objeto date o datetime
                attendance_date_obj = attendance_date if hasattr(attendance_date, 'year') else attendance_date.date()
            
            # Filtrar por rango de fechas
            if start_date_obj <= attendance_date_obj <= end_date_obj:
                attendances.append({
                    'fecha': attendance_date_obj.strftime("%Y-%m-%d"),
                    'usuario_id': user_id,
                    'usuario_nombre': user_name,
                    'hora_registro': attendance.get('hora_registro', 'N/A'),
                    'tipo': 'Regular'
                })
            
        return attendances
        
    def populate_table(self, attendances):
        """Poblar la tabla con los datos de asistencias"""
        self.attendance_table.setRowCount(len(attendances))
        
        for row, attendance in enumerate(attendances):
            self.attendance_table.setItem(row, 0, QTableWidgetItem(str(attendance['fecha'])))
            self.attendance_table.setItem(row, 1, QTableWidgetItem(attendance['usuario_nombre']))
            self.attendance_table.setItem(row, 2, QTableWidgetItem(str(attendance['hora_registro'])))
            self.attendance_table.setItem(row, 3, QTableWidgetItem(attendance['tipo']))
            
    def update_statistics(self, attendances, start_date, end_date):
        """Actualizar estadísticas"""
        total = len(attendances)
        
        # Calcular días en el período
        start_obj = datetime.strptime(start_date, "%Y-%m-%d")
        end_obj = datetime.strptime(end_date, "%Y-%m-%d")
        days = (end_obj - start_obj).days + 1
        
        avg_per_day = total / days if days > 0 else 0
        
        self.total_label.setText(f"Total: {total}")
        self.avg_label.setText(f"Promedio/día: {avg_per_day:.1f}")
        self.period_label.setText(f"Período: {start_date} a {end_date}")
        
    def update_chart(self, attendances):
        """Actualizar gráfico de asistencias usando matplotlib"""
        try:
            # Evitar actualizaciones si el diálogo se está cerrando o el canvas fue eliminado
            if self._closing or not self.isVisible() or self.canvas is None or sip.isdeleted(self.canvas):
                return
            # Limpiar gráfico anterior
            self.figure.clear()
            
            # Agrupar asistencias por fecha
            date_counts = {}
            for attendance in attendances:
                date = attendance['fecha']
                date_counts[date] = date_counts.get(date, 0) + 1
            
            if not date_counts:
                ax = self.figure.add_subplot(111)
                ax.text(0.5, 0.5, 'Sin datos para mostrar', 
                       horizontalalignment='center', verticalalignment='center',
                       transform=ax.transAxes, fontsize=12)
                ax.set_xticks([])
                ax.set_yticks([])
                if not sip.isdeleted(self.canvas):
                    try:
                        self.canvas.draw()
                    except RuntimeError as re:
                        if 'has been deleted' in str(re):
                            return
                        raise
                return
            
            # Crear gráfico
            ax = self.figure.add_subplot(111)
            
            # Ordenar fechas y crear gráfico de líneas
            sorted_dates = sorted(date_counts.keys())
            counts = [date_counts[date] for date in sorted_dates]
            
            ax.plot(range(len(sorted_dates)), counts, 'o-', color='#0078d4', linewidth=2, markersize=6)
            ax.set_title(f'Asistencias por Día ({len(sorted_dates)} días)', fontsize=12, fontweight='bold')
            ax.set_xlabel('Días', fontsize=10)
            ax.set_ylabel('Asistencias', fontsize=10)
            ax.grid(True, alpha=0.3)
            
            # Configurar etiquetas del eje X (mostrar solo algunas fechas)
            if len(sorted_dates) <= 10:
                ax.set_xticks(range(len(sorted_dates)))
                # Convertir fechas a string si son objetos datetime.date
                date_labels = []
                for date in sorted_dates:
                    if isinstance(date, str):
                        date_labels.append(date.split('-')[2] + '/' + date.split('-')[1])
                    else:
                        # Es un objeto datetime.date
                        date_labels.append(f"{date.day:02d}/{date.month:02d}")
                ax.set_xticklabels(date_labels, rotation=45)
            else:
                # Mostrar solo algunas fechas si hay muchas
                step = len(sorted_dates) // 5
                indices = range(0, len(sorted_dates), step)
                ax.set_xticks(indices)
                # Convertir fechas a string si son objetos datetime.date
                date_labels = []
                for i in indices:
                    date = sorted_dates[i]
                    if isinstance(date, str):
                        date_labels.append(date.split('-')[2] + '/' + date.split('-')[1])
                    else:
                        # Es un objeto datetime.date
                        date_labels.append(f"{date.day:02d}/{date.month:02d}")
                ax.set_xticklabels(date_labels, rotation=45)
            
            # Ajustar layout
            self.figure.tight_layout()
            if not sip.isdeleted(self.canvas):
                try:
                    self.canvas.draw()
                except RuntimeError as re:
                    if 'has been deleted' in str(re):
                        return
                    raise
            
        except Exception as e:
            print(f"Error al actualizar gráfico: {e}")
            
    def export_to_csv(self):
        """Exportar datos a CSV"""
        if not self.attendance_data:
            QMessageBox.warning(self, "Advertencia", "No hay datos para exportar.")
            return
            
        try:
            # Crear carpeta exports si no existe
            exports_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), "exports")
            os.makedirs(exports_dir, exist_ok=True)
            
            filename = f"asistencias_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
            filepath = os.path.join(exports_dir, filename)
            
            with open(filepath, 'w', newline='', encoding='utf-8') as csvfile:
                writer = csv.writer(csvfile)
                writer.writerow(["Fecha", "Usuario", "Hora Registro", "Tipo"])
                
                for attendance in self.attendance_data:
                    writer.writerow([
                        attendance['fecha'],
                        attendance['usuario_nombre'],
                        attendance['hora_registro'],
                        attendance['tipo']
                    ])
            
            QMessageBox.information(self, "Éxito", f"Datos exportados a: {filepath}")
            
        except Exception as e:
            QMessageBox.critical(self, "Error", f"Error al exportar CSV: {str(e)}")
            
    def export_to_excel(self):
        """Exportar datos a Excel"""
        if not self.attendance_data:
            QMessageBox.warning(self, "Advertencia", "No hay datos para exportar.")
            return
            
        try:
            # Crear carpeta exports si no existe
            exports_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), "exports")
            os.makedirs(exports_dir, exist_ok=True)
            
            filename = f"asistencias_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
            filepath = os.path.join(exports_dir, filename)
            
            # Crear DataFrame
            df_data = []
            for attendance in self.attendance_data:
                df_data.append({
                    'Fecha': attendance['fecha'],
                    'Usuario': attendance['usuario_nombre'],
                    'Hora Registro': attendance['hora_registro'],
                    'Tipo': attendance['tipo']
                })
            
            df = pd.DataFrame(df_data)
            df.to_excel(filepath, index=False)
            
            QMessageBox.information(self, "Éxito", f"Datos exportados a: {filepath}")
            
        except Exception as e:
            QMessageBox.critical(self, "Error", f"Error al exportar Excel: {str(e)}")

    def perform_search(self):
        """Compatibilidad: wrapper para ejecutar la búsqueda desde la UI."""
        self.search_attendances()

    def set_selected_user(self, user_id, auto_search: bool = False):
        """Permite preseleccionar un usuario en el combo por su ID.
        Si no se encuentra el usuario, no realiza cambios.
        Desactiva el checkbox de 'Todos los usuarios'.
        Si auto_search=True, ejecuta la búsqueda automáticamente.
        """
        try:
            # Asegurar que el selector esté habilitado
            self.all_users_check.setChecked(False)
            self.user_combo.setEnabled(True)

            # Buscar el índice con el user_id como data
            target_index = -1
            for i in range(self.user_combo.count()):
                if self.user_combo.itemData(i) == user_id:
                    target_index = i
                    break
            if target_index != -1:
                self.user_combo.setCurrentIndex(target_index)
                self.current_user_id = user_id
                if auto_search:
                    self.search_attendances()
        except Exception as e:
            # Evitar que un error en la preselección rompa el diálogo
            print(f"Error en set_selected_user: {e}")

    def closeEvent(self, event):
        """Marcar cierre y evitar actualizaciones posteriores del gráfico."""
        try:
            self._closing = True
        except Exception:
            pass
        return super().closeEvent(event)