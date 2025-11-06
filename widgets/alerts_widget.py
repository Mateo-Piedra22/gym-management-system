# -*- coding: utf-8 -*-
"""
Widget de Alertas del Sistema
Proporciona una interfaz gráfica para visualizar y gestionar alertas del sistema.
"""

import logging
from datetime import datetime, timedelta
from PyQt6.QtWidgets import (
    QWidget, QVBoxLayout, QHBoxLayout, QTableWidget, QTableWidgetItem,
    QPushButton, QLabel, QGroupBox, QComboBox, QCheckBox, QTextEdit,
    QSplitter, QHeaderView, QAbstractItemView, QMessageBox, QProgressBar,
    QFrame, QGridLayout, QScrollArea, QSizePolicy
)
from PyQt6.QtCore import Qt, pyqtSignal, QTimer, pyqtSlot, QThread
from PyQt6.QtGui import QColor, QFont, QIcon

from utils_modules.alert_system import alert_manager, AlertLevel, AlertCategory, Alert


class AlertsLoadThread(QThread):
    """Hilo para cargar alertas aplicando filtros sin bloquear la UI"""
    alerts_ready = pyqtSignal(list)
    load_error = pyqtSignal(str)

    def __init__(self, parent_widget, level=None, category=None, unresolved_only=False):
        super().__init__()
        self._parent_widget = parent_widget
        self._level = level
        self._category = category
        self._unresolved_only = unresolved_only

    def run(self):
        try:
            # Obtener alertas del sistema general (posible acceso a DB interno del gestor)
            alerts = alert_manager.get_alerts(
                level=self._level,
                category=self._category,
                unresolved_only=self._unresolved_only
            )

            # Agregar alertas de membresías si están disponibles en la ventana principal
            try:
                main_window = self._parent_widget.window()
                if hasattr(main_window, 'tab_widget'):
                    for i in range(main_window.tab_widget.count()):
                        widget = main_window.tab_widget.widget(i)
                        if hasattr(widget, 'obtener_alertas_membresías'):
                            membership_alerts = widget.obtener_alertas_membresías(self._unresolved_only)
                            # Aplicar filtros en el hilo para evitar trabajo en UI
                            if self._level or self._category:
                                filtered_membership = []
                                for alert in membership_alerts:
                                    if self._level and alert.level != self._level:
                                        continue
                                    if self._category and alert.category != self._category:
                                        continue
                                    filtered_membership.append(alert)
                                membership_alerts = filtered_membership
                            alerts.extend(membership_alerts)
                            break
            except Exception as e:
                # No bloquear por errores secundarios, reportar y continuar
                logging.warning(f"Error al obtener alertas de membresías: {e}")

            self.alerts_ready.emit(alerts)
        except Exception as e:
            self.load_error.emit(str(e))


class AlertSummaryWidget(QFrame):
    """Widget de resumen de alertas"""
    
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setFrameStyle(QFrame.Shape.StyledPanel)
        self.setup_ui()
        self.update_summary()
        
        # Conectar señales del gestor de alertas
        alert_manager.alert_generated.connect(self.update_summary)
        alert_manager.alert_resolved.connect(self.update_summary)
    
    def setup_ui(self):
        """Configura la interfaz del resumen"""
        layout = QGridLayout(self)
        layout.setSpacing(6)
        layout.setContentsMargins(8, 8, 8, 8)
        
        # Título
        title = QLabel("Resumen de Alertas")
        title.setFont(QFont("Arial", 12, QFont.Weight.Bold))
        title.setAlignment(Qt.AlignmentFlag.AlignCenter)
        layout.addWidget(title, 0, 0, 1, 4)
        
        # Contadores de alertas
        self.critical_count = QLabel("0")
        self.warning_count = QLabel("0")
        self.error_count = QLabel("0")
        self.unresolved_count = QLabel("0")
        
        # Configurar estilos de contadores
        self.critical_count.setStyleSheet(
            "QLabel { background-color: #e74c3c; color: white; "
            "padding: 10px; border-radius: 5px; font-weight: bold; font-size: 14px; }"
        )
        self.warning_count.setStyleSheet(
            "QLabel { background-color: #f39c12; color: white; "
            "padding: 10px; border-radius: 5px; font-weight: bold; font-size: 14px; }"
        )
        self.error_count.setStyleSheet(
            "QLabel { background-color: #e67e22; color: white; "
            "padding: 10px; border-radius: 5px; font-weight: bold; font-size: 14px; }"
        )
        self.unresolved_count.setStyleSheet(
            "QLabel { background-color: #34495e; color: white; "
            "padding: 10px; border-radius: 5px; font-weight: bold; font-size: 14px; }"
        )
        
        # Etiquetas
        layout.addWidget(QLabel("🚨 Críticas:"), 1, 0)
        layout.addWidget(self.critical_count, 1, 1)
        layout.addWidget(QLabel("⚠️ Advertencias:"), 1, 2)
        layout.addWidget(self.warning_count, 1, 3)
        
        layout.addWidget(QLabel("❌ Errores:"), 2, 0)
        layout.addWidget(self.error_count, 2, 1)
        layout.addWidget(QLabel("🔄 Sin resolver:"), 2, 2)
        layout.addWidget(self.unresolved_count, 2, 3)
    
    @pyqtSlot()
    def update_summary(self):
        """Actualiza el resumen de alertas"""
        counts = alert_manager.get_alert_counts()
        
        self.critical_count.setText(str(counts.get('critical', 0)))
        self.warning_count.setText(str(counts.get('warning', 0)))
        self.error_count.setText(str(counts.get('error', 0)))
        self.unresolved_count.setText(str(counts.get('unresolved', 0)))


class AlertsTableWidget(QTableWidget):
    """Tabla personalizada para mostrar alertas"""
    
    alert_selected = pyqtSignal(object)  # Alert
    
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setup_table()
        self.alerts_data = []
        
        # Conectar señales
        self.itemSelectionChanged.connect(self._on_selection_changed)
        alert_manager.alert_generated.connect(self.refresh_alerts)
        alert_manager.alert_acknowledged.connect(self.refresh_alerts)
        alert_manager.alert_resolved.connect(self.refresh_alerts)
    
    def setup_table(self):
        """Configura la tabla de alertas"""
        self.setColumnCount(7)
        self.setHorizontalHeaderLabels([
            "Nivel", "Categoría", "Título", "Mensaje", "Fuente", "Hora", "Estado"
        ])
        
        # Configurar comportamiento
        self.setSelectionBehavior(QAbstractItemView.SelectionBehavior.SelectRows)
        self.setAlternatingRowColors(True)
        self.setSortingEnabled(True)
        
        # Ajustar columnas
        header = self.horizontalHeader()
        header.setStretchLastSection(True)
        header.setSectionResizeMode(0, QHeaderView.ResizeMode.ResizeToContents)
        header.setSectionResizeMode(1, QHeaderView.ResizeMode.ResizeToContents)
        header.setSectionResizeMode(2, QHeaderView.ResizeMode.Stretch)
        header.setSectionResizeMode(3, QHeaderView.ResizeMode.Stretch)
        header.setSectionResizeMode(4, QHeaderView.ResizeMode.ResizeToContents)
        header.setSectionResizeMode(5, QHeaderView.ResizeMode.ResizeToContents)
        header.setSectionResizeMode(6, QHeaderView.ResizeMode.ResizeToContents)
    
    def load_alerts(self, alerts):
        """Carga alertas en la tabla"""
        self.alerts_data = alerts
        self.setRowCount(len(alerts))
        
        for row, alert in enumerate(alerts):
            # Nivel
            level_item = QTableWidgetItem(alert.level.value.upper())
            level_item.setData(Qt.ItemDataRole.UserRole, alert)
            self._set_level_color(level_item, alert.level)
            self.setItem(row, 0, level_item)
            
            # Categoría
            category_item = QTableWidgetItem(alert.category.value.title())
            self.setItem(row, 1, category_item)
            
            # Título
            title_item = QTableWidgetItem(alert.title)
            self.setItem(row, 2, title_item)
            
            # Mensaje
            message_item = QTableWidgetItem(alert.message[:100] + "..." if len(alert.message) > 100 else alert.message)
            message_item.setToolTip(alert.message)
            self.setItem(row, 3, message_item)
            
            # Fuente
            source_item = QTableWidgetItem(alert.source)
            self.setItem(row, 4, source_item)
            
            # Hora
            time_item = QTableWidgetItem(alert.timestamp.strftime("%H:%M:%S"))
            time_item.setToolTip(alert.timestamp.strftime("%Y-%m-%d %H:%M:%S"))
            self.setItem(row, 5, time_item)
            
            # Estado
            status = "Resuelto" if alert.resolved else ("Reconocido" if alert.acknowledged else "Nuevo")
            status_item = QTableWidgetItem(status)
            self._set_status_color(status_item, alert)
            self.setItem(row, 6, status_item)
            
            # Colorear fila según estado
            if alert.resolved:
                for col in range(7):
                    item = self.item(row, col)
                    if item:
                        item.setBackground(QColor(240, 255, 240))  # Verde claro
            elif alert.acknowledged:
                for col in range(7):
                    item = self.item(row, col)
                    if item:
                        item.setBackground(QColor(255, 255, 240))  # Amarillo claro
    
    def _set_level_color(self, item, level):
        """Establece el color según el nivel de alerta"""
        colors = {
            AlertLevel.CRITICAL: QColor(231, 76, 60),    # Rojo
            AlertLevel.ERROR: QColor(230, 126, 34),      # Naranja
            AlertLevel.WARNING: QColor(243, 156, 18),    # Amarillo
            AlertLevel.INFO: QColor(52, 152, 219)        # Azul
        }
        
        color = colors.get(level, QColor(128, 128, 128))
        item.setBackground(color)
        item.setForeground(QColor(255, 255, 255))
    
    def _set_status_color(self, item, alert):
        """Establece el color según el estado de la alerta"""
        if alert.resolved:
            item.setBackground(QColor(46, 204, 113))  # Verde
            item.setForeground(QColor(255, 255, 255))
        elif alert.acknowledged:
            item.setBackground(QColor(241, 196, 15))  # Amarillo
            item.setForeground(QColor(0, 0, 0))
        else:
            item.setBackground(QColor(231, 76, 60))   # Rojo
            item.setForeground(QColor(255, 255, 255))
    
    def _on_selection_changed(self):
        """Maneja el cambio de selección"""
        current_row = self.currentRow()
        if 0 <= current_row < len(self.alerts_data):
            alert = self.alerts_data[current_row]
            self.alert_selected.emit(alert)
    
    @pyqtSlot()
    def refresh_alerts(self):
        """Refresca la tabla de alertas"""
        # Mantener filtros actuales si existen
        if hasattr(self.parent(), 'apply_filters'):
            self.parent().apply_filters()


class AlertDetailsWidget(QFrame):
    """Widget para mostrar detalles de una alerta seleccionada"""
    
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setFrameStyle(QFrame.Shape.StyledPanel)
        self.current_alert = None
        self.setup_ui()
    
    def setup_ui(self):
        """Configura la interfaz de detalles"""
        layout = QVBoxLayout(self)
        layout.setContentsMargins(8, 8, 8, 8)
        layout.setSpacing(6)
        
        # Título
        title = QLabel("Detalles de la Alerta")
        title.setFont(QFont("Arial", 12, QFont.Weight.Bold))
        title.setAlignment(Qt.AlignmentFlag.AlignCenter)
        layout.addWidget(title)
        
        # Área de detalles
        self.details_text = QTextEdit()
        self.details_text.setReadOnly(True)
        self.details_text.setMaximumHeight(180)
        self.details_text.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Expanding)
        layout.addWidget(self.details_text)
        
        # Botones de acción
        buttons_layout = QHBoxLayout()
        
        self.acknowledge_btn = QPushButton("✅ Reconocer")
        self.acknowledge_btn.clicked.connect(self.acknowledge_alert)
        self.acknowledge_btn.setEnabled(False)
        buttons_layout.addWidget(self.acknowledge_btn)
        
        self.resolve_btn = QPushButton("✔️ Resolver")
        self.resolve_btn.clicked.connect(self.resolve_alert)
        self.resolve_btn.setEnabled(False)
        buttons_layout.addWidget(self.resolve_btn)
        
        buttons_layout.addStretch()
        layout.addLayout(buttons_layout)
        
        # Mensaje inicial
        self.details_text.setPlainText("Selecciona una alerta para ver sus detalles.")
    
    def show_alert_details(self, alert: Alert):
        """Muestra los detalles de una alerta"""
        self.current_alert = alert
        
        details = f"""ID: {alert.id}
Nivel: {alert.level.value.upper()}
Categoría: {alert.category.value.title()}
Título: {alert.title}
Mensaje: {alert.message}
Fuente: {alert.source}
Fecha y Hora: {alert.timestamp.strftime('%Y-%m-%d %H:%M:%S')}
Reconocido: {'Sí' if alert.acknowledged else 'No'}
Resuelto: {'Sí' if alert.resolved else 'No'}"""
        
        self.details_text.setPlainText(details)
        
        # Habilitar/deshabilitar botones
        self.acknowledge_btn.setEnabled(not alert.acknowledged and not alert.resolved)
        self.resolve_btn.setEnabled(not alert.resolved)
    
    def acknowledge_alert(self):
        """Reconoce la alerta actual"""
        if self.current_alert:
            alert_manager.acknowledge_alert(self.current_alert.id)
            self.show_alert_details(self.current_alert)  # Actualizar vista
    
    def resolve_alert(self):
        """Resuelve la alerta actual"""
        if self.current_alert:
            alert_manager.resolve_alert(self.current_alert.id)
            self.show_alert_details(self.current_alert)  # Actualizar vista


class AlertsWidget(QWidget):
    """Widget principal de gestión de alertas"""
    
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setup_ui()
        self.setup_connections()
        self._loading_in_progress = False
        self._loading_thread = None
        self._destroyed = False
        self.refresh_timer = QTimer()
        self.refresh_timer.timeout.connect(self.apply_filters)
        self.refresh_timer.start(30000)  # Actualizar cada 30 segundos
        try:
            self.destroyed.connect(self._cleanup_on_destroy)
        except Exception:
            pass
        
        # Cargar alertas iniciales
        self.apply_filters()
    
    def setup_ui(self):
        """Configura la interfaz principal"""
        layout = QVBoxLayout(self)
        layout.setSpacing(8)
        layout.setContentsMargins(8, 8, 8, 8)
        
        # Título principal
        title = QLabel("🚨 Sistema de Alertas")
        title.setFont(QFont("Arial", 16, QFont.Weight.Bold))
        title.setAlignment(Qt.AlignmentFlag.AlignCenter)
        layout.addWidget(title)
        
        # Resumen de alertas
        self.summary_widget = AlertSummaryWidget()
        layout.addWidget(self.summary_widget)
        
        # Filtros
        filters_group = QGroupBox("Filtros")
        filters_group.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Preferred)
        filters_layout = QHBoxLayout(filters_group)
        filters_layout.setContentsMargins(8, 8, 8, 8)
        filters_layout.setSpacing(6)
        
        # Filtro por nivel
        filters_layout.addWidget(QLabel("Nivel:"))
        self.level_filter = QComboBox()
        self.level_filter.addItem("Todos", None)
        for level in AlertLevel:
            self.level_filter.addItem(level.value.title(), level)
        self.level_filter.currentTextChanged.connect(self.apply_filters)
        filters_layout.addWidget(self.level_filter)
        
        # Filtro por categoría
        filters_layout.addWidget(QLabel("Categoría:"))
        self.category_filter = QComboBox()
        self.category_filter.addItem("Todas", None)
        for category in AlertCategory:
            self.category_filter.addItem(category.value.title(), category)
        self.category_filter.currentTextChanged.connect(self.apply_filters)
        filters_layout.addWidget(self.category_filter)
        
        # Filtro por estado
        self.unresolved_only = QCheckBox("Solo sin resolver")
        self.unresolved_only.stateChanged.connect(self.apply_filters)
        filters_layout.addWidget(self.unresolved_only)
        
        # Botón de actualizar
        refresh_btn = QPushButton("🔄 Actualizar")
        refresh_btn.clicked.connect(self.apply_filters)
        filters_layout.addWidget(refresh_btn)
        
        filters_layout.addStretch()
        layout.addWidget(filters_group)
        
        # Splitter para tabla y detalles
        splitter = QSplitter(Qt.Orientation.Horizontal)
        splitter.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Expanding)
        
        # Tabla de alertas
        self.alerts_table = AlertsTableWidget()
        self.alerts_table.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Expanding)
        splitter.addWidget(self.alerts_table)
        
        # Detalles de alerta
        self.details_widget = AlertDetailsWidget()
        self.details_widget.setSizePolicy(QSizePolicy.Policy.Preferred, QSizePolicy.Policy.Expanding)
        splitter.addWidget(self.details_widget)
        
        # Configurar proporciones del splitter
        splitter.setSizes([700, 300])
        layout.addWidget(splitter)
        
        # Botones de acción
        actions_layout = QHBoxLayout()
        actions_layout.setContentsMargins(0, 8, 0, 0)
        actions_layout.setSpacing(6)
        
        clear_old_btn = QPushButton("🧹 Limpiar Antiguas")
        clear_old_btn.clicked.connect(self.clear_old_alerts)
        actions_layout.addWidget(clear_old_btn)
        
        export_btn = QPushButton("📤 Exportar")
        export_btn.clicked.connect(self.export_alerts)
        actions_layout.addWidget(export_btn)
        
        actions_layout.addStretch()
        layout.addLayout(actions_layout)
    
    def setup_connections(self):
        """Configura las conexiones de señales"""
        self.alerts_table.alert_selected.connect(self.details_widget.show_alert_details)
    
    def apply_filters(self):
        """Aplica los filtros seleccionados"""
        # Evitar ejecutar si el widget no está visible o fue destruido
        try:
            if getattr(self, '_destroyed', False) or (hasattr(self, 'isVisible') and not self.isVisible()):
                return
        except RuntimeError:
            return
        # Evitar solapamiento de cargas si el temporizador dispara rápido
        if self._loading_in_progress:
            return

        level = self.level_filter.currentData()
        category = self.category_filter.currentData()
        unresolved_only = self.unresolved_only.isChecked()
        # Lanzar la carga en segundo plano
        self._loading_in_progress = True
        self._loading_thread = AlertsLoadThread(self, level=level, category=category, unresolved_only=unresolved_only)
        self._loading_thread.alerts_ready.connect(self._on_alerts_loaded)
        self._loading_thread.load_error.connect(self._on_alerts_error)
        self._loading_thread.finished.connect(self._on_alerts_finished)
        self._loading_thread.start()

    def _on_alerts_loaded(self, alerts):
        """Actualiza la tabla y el resumen con resultados cargados"""
        try:
            # No actualizar UI si el widget fue destruido o no es visible
            if getattr(self, '_destroyed', False) or (hasattr(self, 'isVisible') and not self.isVisible()):
                return
            self.alerts_table.load_alerts(alerts)
            # Resumen mediante el gestor (puede usar agregados internos)
            self.summary_widget.update_summary()
        except Exception as e:
            logging.error(f"Error actualizando UI de alertas: {e}")

    def _on_alerts_error(self, message):
        """Notifica un error de carga"""
        logging.error(f"Error cargando alertas: {message}")

    def _on_alerts_finished(self):
        """Marca fin de carga para permitir próximas actualizaciones"""
        self._loading_in_progress = False
        self._loading_thread = None

    def _cleanup_on_destroy(self):
        """Detiene el temporizador y marca el widget como destruido para evitar tareas tardías."""
        try:
            self._destroyed = True
            if hasattr(self, 'refresh_timer') and self.refresh_timer is not None:
                self.refresh_timer.stop()
        except Exception:
            pass
    
    def clear_old_alerts(self):
        """Limpia alertas antiguas"""
        reply = QMessageBox.question(
            self,
            "Confirmar Limpieza",
            "¿Deseas eliminar las alertas de más de 30 días?",
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No
        )
        
        if reply == QMessageBox.StandardButton.Yes:
            alert_manager.clear_old_alerts(30)
            self.apply_filters()
            QMessageBox.information(self, "Limpieza Completada", "Las alertas antiguas han sido eliminadas.")
    
    def export_alerts(self):
        """Exporta las alertas actuales"""
        from PyQt6.QtWidgets import QFileDialog
        
        filename, _ = QFileDialog.getSaveFileName(
            self,
            "Exportar Alertas",
            f"alertas_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json",
            "JSON Files (*.json)"
        )
        
        if filename:
            try:
                alert_manager.export_alerts(filename)
                QMessageBox.information(self, "Exportación Exitosa", f"Alertas exportadas a {filename}")
            except Exception as e:
                QMessageBox.critical(self, "Error de Exportación", f"Error al exportar alertas: {e}")

