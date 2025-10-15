from PyQt6.QtWidgets import (
    QWidget, QVBoxLayout, QHBoxLayout, QTabWidget, QTableWidget, QTableWidgetItem,
    QPushButton, QLabel, QLineEdit, QComboBox, QDateEdit, QTextEdit, QGroupBox,
    QSplitter, QHeaderView, QMessageBox, QProgressBar, QCheckBox, QSpinBox,
    QFileDialog, QFrame
)
from PyQt6.QtCore import Qt, QDate, QTimer, QThread, pyqtSignal
from PyQt6.QtGui import QFont, QColor, QPalette
from datetime import datetime, timedelta
import json
import csv
import logging
from collections import defaultdict, Counter
import re

class LogAnalysisThread(QThread):
    """Hilo para análisis de logs en segundo plano"""
    analysis_complete = pyqtSignal(dict)
    progress_updated = pyqtSignal(int)
    
    def __init__(self, db_manager, filters=None):
        super().__init__()
        self.db_manager = db_manager
        self.filters = filters or {}
        
    def run(self):
        """Ejecuta el análisis de logs"""
        try:
            results = {
                'total_logs': 0,
                'by_level': {},
                'by_category': {},
                'by_source': {},
                'by_hour': {},
                'by_day': {},
                'error_patterns': {},
                'performance_metrics': {},
                'recent_critical': []
            }
            
            # Obtener logs con filtros
            query = "SELECT * FROM audit_logs WHERE 1=1"
            params = []
            
            if self.filters.get('start_date'):
                query += " AND timestamp >= %s"
                params.append(self.filters['start_date'])
                
            if self.filters.get('end_date'):
                query += " AND timestamp <= %s"
                params.append(self.filters['end_date'])
                
            if self.filters.get('level'):
                query += " AND level = %s"
                params.append(self.filters['level'])
                
            if self.filters.get('category'):
                query += " AND category = %s"
                params.append(self.filters['category'])
                
            query += " ORDER BY timestamp DESC"
            
            logs = self.db_manager.execute_query(query, params)
            results['total_logs'] = len(logs)
            
            self.progress_updated.emit(20)
            
            # Análisis por nivel
            level_counts = Counter(log[3] for log in logs)  # level es columna 3
            results['by_level'] = dict(level_counts)
            
            self.progress_updated.emit(40)
            
            # Análisis por categoría
            category_counts = Counter(log[4] for log in logs)  # category es columna 4
            results['by_category'] = dict(category_counts)
            
            # Análisis por fuente
            source_counts = Counter(log[6] for log in logs)  # source es columna 6
            results['by_source'] = dict(source_counts)
            
            self.progress_updated.emit(60)
            
            # Análisis temporal
            hour_counts = defaultdict(int)
            day_counts = defaultdict(int)
            
            for log in logs:
                timestamp = datetime.fromisoformat(log[2])  # timestamp es columna 2
                hour_key = timestamp.strftime('%H:00')
                day_key = timestamp.strftime('%Y-%m-%d')
                hour_counts[hour_key] += 1
                day_counts[day_key] += 1
                
            results['by_hour'] = dict(hour_counts)
            results['by_day'] = dict(day_counts)
            
            self.progress_updated.emit(80)
            
            # Análisis de patrones de error
            error_logs = [log for log in logs if log[3] in ['ERROR', 'CRITICAL']]
            error_messages = [log[5] for log in error_logs]  # message es columna 5
            
            # Buscar patrones comunes en errores
            error_patterns = {}
            for message in error_messages:
                # Extraer patrones básicos
                if 'database' in message.lower():
                    error_patterns['Database Errors'] = error_patterns.get('Database Errors', 0) + 1
                elif 'connection' in message.lower():
                    error_patterns['Connection Errors'] = error_patterns.get('Connection Errors', 0) + 1
                elif 'permission' in message.lower():
                    error_patterns['Permission Errors'] = error_patterns.get('Permission Errors', 0) + 1
                elif 'timeout' in message.lower():
                    error_patterns['Timeout Errors'] = error_patterns.get('Timeout Errors', 0) + 1
                else:
                    error_patterns['Other Errors'] = error_patterns.get('Other Errors', 0) + 1
                    
            results['error_patterns'] = error_patterns
            
            # Logs críticos recientes (últimas 24 horas)
            recent_critical = [
                {
                    'timestamp': log[2],
                    'level': log[3],
                    'category': log[4],
                    'message': log[5],
                    'source': log[6]
                }
                for log in logs
                if log[3] == 'CRITICAL' and 
                datetime.fromisoformat(log[2]) > datetime.now() - timedelta(days=1)
            ][:10]  # Máximo 10
            
            results['recent_critical'] = recent_critical
            
            self.progress_updated.emit(100)
            self.analysis_complete.emit(results)
            
        except Exception as e:
            logging.error(f"Error en análisis de logs: {e}")
            self.analysis_complete.emit({})

class LogAnalysisWidget(QWidget):
    """Widget para análisis avanzado de logs del sistema"""
    
    def __init__(self, db_manager):
        super().__init__()
        self.db_manager = db_manager
        self.analysis_thread = None
        self.setup_ui()
        
    def setup_ui(self):
        """Configura la interfaz de usuario"""
        layout = QVBoxLayout(self)
        layout.setContentsMargins(8, 8, 8, 8)
        layout.setSpacing(6)
        
        # Título
        title = QLabel("Análisis de Logs del Sistema")
        title.setFont(QFont("Arial", 16, QFont.Weight.Bold))
        title.setAlignment(Qt.AlignmentFlag.AlignCenter)
        layout.addWidget(title)
        
        # Panel de filtros
        self.setup_filters_panel(layout)
        
        # Splitter principal
        main_splitter = QSplitter(Qt.Orientation.Horizontal)
        main_splitter.setSizePolicy(QWidget.SizePolicy.Expanding, QWidget.SizePolicy.Expanding)
        layout.addWidget(main_splitter)
        
        # Panel izquierdo - Controles y resumen
        left_panel = self.setup_left_panel()
        left_panel.setSizePolicy(QWidget.SizePolicy.Preferred, QWidget.SizePolicy.Expanding)
        main_splitter.addWidget(left_panel)
        
        # Panel derecho - Resultados detallados
        right_panel = self.setup_right_panel()
        right_panel.setSizePolicy(QWidget.SizePolicy.Expanding, QWidget.SizePolicy.Expanding)
        main_splitter.addWidget(right_panel)
        
        # Configurar proporciones del splitter
        main_splitter.setSizes([300, 700])
        
    def setup_filters_panel(self, layout):
        """Configura el panel de filtros"""
        filters_group = QGroupBox("Filtros de Análisis")
        filters_group.setSizePolicy(QWidget.SizePolicy.Expanding, QWidget.SizePolicy.Fixed)
        filters_layout = QHBoxLayout(filters_group)
        filters_layout.setContentsMargins(8, 8, 8, 8)
        filters_layout.setSpacing(6)
        
        # Rango de fechas
        filters_layout.addWidget(QLabel("Desde:"))
        self.start_date = QDateEdit()
        self.start_date.setDate(QDate.currentDate().addDays(-30))
        self.start_date.setCalendarPopup(True)
        filters_layout.addWidget(self.start_date)
        
        filters_layout.addWidget(QLabel("Hasta:"))
        self.end_date = QDateEdit()
        self.end_date.setDate(QDate.currentDate())
        self.end_date.setCalendarPopup(True)
        filters_layout.addWidget(self.end_date)
        
        # Nivel de log
        filters_layout.addWidget(QLabel("Nivel:"))
        self.level_combo = QComboBox()
        self.level_combo.addItems(["Todos", "DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"])
        filters_layout.addWidget(self.level_combo)
        
        # Categoría
        filters_layout.addWidget(QLabel("Categoría:"))
        self.category_combo = QComboBox()
        self.category_combo.addItems(["Todas", "SYSTEM", "DATABASE", "MAINTENANCE", "SECURITY", "PERFORMANCE", "BACKUP"])
        filters_layout.addWidget(self.category_combo)
        
        # Botón de análisis
        self.analyze_btn = QPushButton("Analizar Logs")
        self.analyze_btn.clicked.connect(self.start_analysis)
        filters_layout.addWidget(self.analyze_btn)
        
        filters_layout.addStretch()
        layout.addWidget(filters_group)
        
    def setup_left_panel(self):
        """Configura el panel izquierdo con controles y resumen"""
        left_widget = QWidget()
        left_layout = QVBoxLayout(left_widget)
        left_layout.setContentsMargins(8, 8, 8, 8)
        left_layout.setSpacing(6)
        
        # Progreso del análisis
        progress_group = QGroupBox("📈 Progreso")
        progress_group.setSizePolicy(QWidget.SizePolicy.Expanding, QWidget.SizePolicy.Fixed)
        progress_layout = QVBoxLayout(progress_group)
        progress_layout.setContentsMargins(8, 8, 8, 8)
        progress_layout.setSpacing(4)
        
        self.progress_bar = QProgressBar()
        self.progress_bar.setVisible(False)
        progress_layout.addWidget(self.progress_bar)
        
        self.status_label = QLabel("Listo para analizar")
        progress_layout.addWidget(self.status_label)
        
        left_layout.addWidget(progress_group)
        
        # Resumen estadístico
        stats_group = QGroupBox("Resumen Estadístico")
        stats_group.setSizePolicy(QWidget.SizePolicy.Expanding, QWidget.SizePolicy.Expanding)
        stats_layout = QVBoxLayout(stats_group)
        stats_layout.setContentsMargins(8, 8, 8, 8)
        stats_layout.setSpacing(4)
        
        self.stats_text = QTextEdit()
        self.stats_text.setMaximumHeight(200)
        self.stats_text.setReadOnly(True)
        self.stats_text.setSizePolicy(QWidget.SizePolicy.Expanding, QWidget.SizePolicy.Expanding)
        stats_layout.addWidget(self.stats_text)
        
        left_layout.addWidget(stats_group)
        
        # Acciones
        actions_group = QGroupBox("⚡ Acciones")
        actions_group.setSizePolicy(QWidget.SizePolicy.Expanding, QWidget.SizePolicy.Fixed)
        actions_layout = QVBoxLayout(actions_group)
        actions_layout.setContentsMargins(8, 8, 8, 8)
        actions_layout.setSpacing(4)
        
        self.export_btn = QPushButton("📄 Exportar Resultados")
        self.export_btn.clicked.connect(self.export_results)
        self.export_btn.setEnabled(False)
        actions_layout.addWidget(self.export_btn)
        
        self.clear_old_logs_btn = QPushButton("🗑️ Limpiar Logs Antiguos")
        self.clear_old_logs_btn.clicked.connect(self.clear_old_logs)
        actions_layout.addWidget(self.clear_old_logs_btn)
        
        self.refresh_btn = QPushButton("🔄 Actualizar")
        self.refresh_btn.clicked.connect(self.refresh_analysis)
        actions_layout.addWidget(self.refresh_btn)
        
        left_layout.addWidget(actions_group)
        
        left_layout.addStretch()
        return left_widget
        
    def setup_right_panel(self):
        """Configura el panel derecho con resultados detallados"""
        self.results_tabs = QTabWidget()
        
        # Tab de distribución por nivel
        self.level_table = QTableWidget()
        self.level_table.setColumnCount(2)
        self.level_table.setHorizontalHeaderLabels(["Nivel", "Cantidad"])
        self.results_tabs.addTab(self.level_table, "Por Nivel")
        
        # Tab de distribución por categoría
        self.category_table = QTableWidget()
        self.category_table.setColumnCount(2)
        self.category_table.setHorizontalHeaderLabels(["Categoría", "Cantidad"])
        self.results_tabs.addTab(self.category_table, "📂 Por Categoría")
        
        # Tab de análisis temporal
        self.temporal_table = QTableWidget()
        self.temporal_table.setColumnCount(3)
        self.temporal_table.setHorizontalHeaderLabels(["Período", "Hora", "Cantidad"])
        self.results_tabs.addTab(self.temporal_table, "Análisis Temporal")
        
        # Tab de patrones de error
        self.error_patterns_table = QTableWidget()
        self.error_patterns_table.setColumnCount(2)
        self.error_patterns_table.setHorizontalHeaderLabels(["Patrón de Error", "Frecuencia"])
        self.results_tabs.addTab(self.error_patterns_table, "🚨 Patrones de Error")
        
        # Tab de logs críticos recientes
        self.critical_logs_table = QTableWidget()
        self.critical_logs_table.setColumnCount(5)
        self.critical_logs_table.setHorizontalHeaderLabels(["Timestamp", "Nivel", "Categoría", "Mensaje", "Fuente"])
        self.results_tabs.addTab(self.critical_logs_table, "🔴 Críticos Recientes")
        
        # Configurar tablas
        for i in range(self.results_tabs.count()):
            table = self.results_tabs.widget(i)
            if isinstance(table, QTableWidget):
                table.horizontalHeader().setStretchLastSection(True)
                table.setAlternatingRowColors(True)
                table.setSelectionBehavior(QTableWidget.SelectionBehavior.SelectRows)
                table.setSizePolicy(QWidget.SizePolicy.Expanding, QWidget.SizePolicy.Expanding)
        
        self.results_tabs.setSizePolicy(QWidget.SizePolicy.Expanding, QWidget.SizePolicy.Expanding)
        return self.results_tabs
        
    def start_analysis(self):
        """Inicia el análisis de logs"""
        if self.analysis_thread and self.analysis_thread.isRunning():
            return
            
        # Preparar filtros
        filters = {
            'start_date': self.start_date.date().toString(Qt.DateFormat.ISODate),
            'end_date': self.end_date.date().toString(Qt.DateFormat.ISODate)
        }
        
        if self.level_combo.currentText() != "Todos":
            filters['level'] = self.level_combo.currentText()
            
        if self.category_combo.currentText() != "Todas":
            filters['category'] = self.category_combo.currentText()
        
        # Configurar UI para análisis
        self.analyze_btn.setEnabled(False)
        self.progress_bar.setVisible(True)
        self.progress_bar.setValue(0)
        self.status_label.setText("Analizando logs...")
        
        # Iniciar hilo de análisis
        self.analysis_thread = LogAnalysisThread(self.db_manager, filters)
        self.analysis_thread.progress_updated.connect(self.progress_bar.setValue)
        self.analysis_thread.analysis_complete.connect(self.on_analysis_complete)
        self.analysis_thread.start()
        
    def on_analysis_complete(self, results):
        """Maneja la finalización del análisis"""
        try:
            self.analysis_results = results
            
            # Actualizar UI
            self.analyze_btn.setEnabled(True)
            self.progress_bar.setVisible(False)
            self.status_label.setText(f"Análisis completado - {results.get('total_logs', 0)} logs procesados")
            self.export_btn.setEnabled(True)
            
            # Actualizar resumen estadístico
            self.update_stats_summary(results)
            
            # Actualizar tablas
            self.update_level_table(results.get('by_level', {}))
            self.update_category_table(results.get('by_category', {}))
            self.update_temporal_table(results.get('by_hour', {}), results.get('by_day', {}))
            self.update_error_patterns_table(results.get('error_patterns', {}))
            self.update_critical_logs_table(results.get('recent_critical', []))
            
        except Exception as e:
            logging.error(f"Error procesando resultados de análisis: {e}")
            self.status_label.setText("Error en el análisis")
            
    def update_stats_summary(self, results):
        """Actualiza el resumen estadístico"""
        summary = f"""📊 RESUMEN DEL ANÁLISIS

📈 Total de Logs: {results.get('total_logs', 0)}

🎯 Distribución por Nivel:
"""
        
        for level, count in results.get('by_level', {}).items():
            percentage = (count / results.get('total_logs', 1)) * 100
            summary += f"  • {level}: {count} ({percentage:.1f}%)\n"
            
        summary += "\n📂 Top Categorías:\n"
        categories = sorted(results.get('by_category', {}).items(), key=lambda x: x[1], reverse=True)[:5]
        for category, count in categories:
            summary += f"  • {category}: {count}\n"
            
        summary += "\n🚨 Patrones de Error:\n"
        for pattern, count in results.get('error_patterns', {}).items():
            summary += f"  • {pattern}: {count}\n"
            
        self.stats_text.setPlainText(summary)
        
    def update_level_table(self, level_data):
        """Actualiza la tabla de distribución por nivel"""
        self.level_table.setRowCount(len(level_data))
        
        for row, (level, count) in enumerate(sorted(level_data.items(), key=lambda x: x[1], reverse=True)):
            self.level_table.setItem(row, 0, QTableWidgetItem(level))
            self.level_table.setItem(row, 1, QTableWidgetItem(str(count)))
            
            # Colorear según el nivel
            if level == 'CRITICAL':
                color = QColor(231, 76, 60)  # Rojo
            elif level == 'ERROR':
                color = QColor(230, 126, 34)  # Naranja
            elif level == 'WARNING':
                color = QColor(241, 196, 15)  # Amarillo
            else:
                color = QColor(52, 152, 219)  # Azul
                
            self.level_table.item(row, 0).setBackground(color)
            
    def update_category_table(self, category_data):
        """Actualiza la tabla de distribución por categoría"""
        self.category_table.setRowCount(len(category_data))
        
        for row, (category, count) in enumerate(sorted(category_data.items(), key=lambda x: x[1], reverse=True)):
            self.category_table.setItem(row, 0, QTableWidgetItem(category))
            self.category_table.setItem(row, 1, QTableWidgetItem(str(count)))
            
    def update_temporal_table(self, hour_data, day_data):
        """Actualiza la tabla de análisis temporal"""
        total_rows = len(hour_data) + len(day_data)
        self.temporal_table.setRowCount(total_rows)
        
        row = 0
        
        # Datos por hora
        for hour, count in sorted(hour_data.items()):
            self.temporal_table.setItem(row, 0, QTableWidgetItem("Hora"))
            self.temporal_table.setItem(row, 1, QTableWidgetItem(str(hour)))
            self.temporal_table.setItem(row, 2, QTableWidgetItem(str(count)))
            row += 1
            
        # Datos por día (últimos 7 días)
        recent_days = sorted(day_data.items(), reverse=True)[:7]
        for day, count in recent_days:
            self.temporal_table.setItem(row, 0, QTableWidgetItem("Día"))
            self.temporal_table.setItem(row, 1, QTableWidgetItem(str(day)))
            self.temporal_table.setItem(row, 2, QTableWidgetItem(str(count)))
            row += 1
            
    def update_error_patterns_table(self, patterns_data):
        """Actualiza la tabla de patrones de error"""
        self.error_patterns_table.setRowCount(len(patterns_data))
        
        for row, (pattern, count) in enumerate(sorted(patterns_data.items(), key=lambda x: x[1], reverse=True)):
            self.error_patterns_table.setItem(row, 0, QTableWidgetItem(pattern))
            self.error_patterns_table.setItem(row, 1, QTableWidgetItem(str(count)))
            
    def update_critical_logs_table(self, critical_logs):
        """Actualiza la tabla de logs críticos recientes"""
        self.critical_logs_table.setRowCount(len(critical_logs))
        
        for row, log in enumerate(critical_logs):
            self.critical_logs_table.setItem(row, 0, QTableWidgetItem(str(log['timestamp'])))
            self.critical_logs_table.setItem(row, 1, QTableWidgetItem(log['level']))
            self.critical_logs_table.setItem(row, 2, QTableWidgetItem(log['category']))
            self.critical_logs_table.setItem(row, 3, QTableWidgetItem(log['message'][:100] + "..." if len(log['message']) > 100 else log['message']))
            self.critical_logs_table.setItem(row, 4, QTableWidgetItem(log['source']))
            
            # Colorear filas críticas
            for col in range(5):
                self.critical_logs_table.item(row, col).setBackground(QColor(255, 235, 235))
                
    def export_results(self):
        """Exporta los resultados del análisis"""
        if not hasattr(self, 'analysis_results'):
            QMessageBox.warning(self, "Advertencia", "No hay resultados para exportar")
            return
            
        file_path, _ = QFileDialog.getSaveFileName(
            self, "Exportar Análisis de Logs", 
            f"analisis_logs_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json",
            "JSON Files (*.json);;CSV Files (*.csv)"
        )
        
        if file_path:
            try:
                if file_path.endswith('.json'):
                    with open(file_path, 'w', encoding='utf-8') as f:
                        json.dump(self.analysis_results, f, indent=2, ensure_ascii=False)
                elif file_path.endswith('.csv'):
                    with open(file_path, 'w', newline='', encoding='utf-8') as f:
                        writer = csv.writer(f)
                        writer.writerow(['Métrica', 'Valor'])
                        writer.writerow(['Total Logs', self.analysis_results.get('total_logs', 0)])
                        
                        for level, count in self.analysis_results.get('by_level', {}).items():
                            writer.writerow([f'Nivel {level}', count])
                            
                QMessageBox.information(self, "Éxito", f"Resultados exportados a {file_path}")
                
            except Exception as e:
                QMessageBox.critical(self, "Error", f"Error exportando resultados: {e}")
                
    def clear_old_logs(self):
        """Limpia logs antiguos del sistema"""
        reply = QMessageBox.question(
            self, "Confirmar", 
            "¿Desea eliminar logs anteriores a 90 días?\n\nEsta acción no se puede deshacer.",
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No
        )
        
        if reply == QMessageBox.StandardButton.Yes:
            try:
                cutoff_date = (datetime.now() - timedelta(days=90)).isoformat()
                query = "DELETE FROM audit_logs WHERE timestamp < %s"
                result = self.db_manager.execute_query(query, [cutoff_date])
                
                QMessageBox.information(self, "Éxito", "Logs antiguos eliminados correctamente")
                self.refresh_analysis()
                
            except Exception as e:
                QMessageBox.critical(self, "Error", f"Error eliminando logs antiguos: {e}")
                
    def refresh_analysis(self):
        """Actualiza el análisis con los datos actuales"""
        if hasattr(self, 'analysis_results'):
            self.start_analysis()

