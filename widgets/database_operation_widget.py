from PyQt6.QtWidgets import (QWidget, QVBoxLayout, QHBoxLayout, QLabel, 
                           QPushButton, QMessageBox, QProgressBar, QTextEdit,
                           QGroupBox, QCheckBox, QSpinBox, QComboBox)
from PyQt6.QtCore import Qt, pyqtSignal, QTimer, QObject
from PyQt6.QtGui import QFont, QIcon
from database import DatabaseManager, DatabaseOperationManager
from widgets.loading_spinner import LoadingOverlay, DatabaseLoadingManager
from utils_modules.async_utils import run_in_background
import logging
from datetime import datetime, timedelta


class DatabaseOperationWidget(QWidget):
    """Widget integrado para operaciones de base de datos con UI de carga"""
    
    # Señales
    operation_completed = pyqtSignal(str, object)
    operation_failed = pyqtSignal(str, str)
    
    def __init__(self, db_manager: DatabaseManager, parent=None):
        super().__init__(parent)
        self.db_manager = db_manager
        self.operation_manager = DatabaseOperationManager(db_manager)
        self.loading_manager = DatabaseLoadingManager(self)
        self.active_operations = {}
        self._stats_loading = False
        # Estado interno y limpieza segura
        self._destroyed = False
        try:
            self.destroyed.connect(self._cleanup_on_destroy)
        except Exception:
            pass
        
        # Configurar UI
        self._setup_ui()
        self._connect_signals()
        
        # Timer para actualizar estadísticas
        self.stats_timer = QTimer(self)
        self.stats_timer.timeout.connect(self._update_stats)
        self.stats_timer.start(5000)  # Actualizar cada 5 segundos
    
    def _setup_ui(self):
        """Configura la interfaz de usuario"""
        layout = QVBoxLayout(self)
        
        # Grupo de controles de operación
        operation_group = QGroupBox("Operaciones de Base de Datos")
        operation_layout = QVBoxLayout(operation_group)
        
        # Selector de operación
        operation_selector_layout = QHBoxLayout()
        operation_selector_layout.addWidget(QLabel("Operación:"))
        
        self.operation_combo = QComboBox()
        self.operation_combo.addItems([
            "Obtener Usuarios",
            "Buscar Usuario por ID", 
            "Obtener Pagos por Usuario",
            "Obtener Asistencias de Hoy",
            "Obtener Asistencias por Usuario",
            "Obtener Clases Activas",
            "Obtener Profesores Activos",
            "Buscar Usuarios",
            "Reporte de Pagos",
            "Reporte de Asistencias"
        ])
        operation_selector_layout.addWidget(self.operation_combo)
        operation_layout.addLayout(operation_selector_layout)
        
        # Parámetros de operación
        params_layout = QHBoxLayout()
        params_layout.addWidget(QLabel("Parámetros:"))
        
        self.param_usuario_id = QSpinBox()
        self.param_usuario_id.setRange(1, 999999)
        self.param_usuario_id.setVisible(False)
        params_layout.addWidget(self.param_usuario_id)
        
        self.param_query = QComboBox()
        self.param_query.setEditable(True)
        self.param_query.setVisible(False)
        params_layout.addWidget(self.param_query)
        
        self.param_limit = QSpinBox()
        self.param_limit.setRange(1, 1000)
        self.param_limit.setValue(50)
        self.param_limit.setVisible(False)
        params_layout.addWidget(self.param_limit)
        
        operation_layout.addLayout(params_layout)
        
        # Botones de control
        control_layout = QHBoxLayout()
        
        self.execute_btn = QPushButton("Ejecutar")
        self.execute_btn.setIcon(QIcon.fromTheme("system-run"))
        self.execute_btn.clicked.connect(self.execute_operation)
        control_layout.addWidget(self.execute_btn)
        
        self.cancel_btn = QPushButton("Cancelar")
        self.cancel_btn.setIcon(QIcon.fromTheme("process-stop"))
        self.cancel_btn.clicked.connect(self.cancel_operation)
        self.cancel_btn.setEnabled(False)
        control_layout.addWidget(self.cancel_btn)
        
        self.refresh_stats_btn = QPushButton("Actualizar Estadísticas")
        self.refresh_stats_btn.clicked.connect(self._update_stats)
        control_layout.addWidget(self.refresh_stats_btn)
        
        operation_layout.addLayout(control_layout)
        
        # Opciones de carga
        options_layout = QHBoxLayout()
        
        self.show_spinner_check = QCheckBox("Mostrar Spinner")
        self.show_spinner_check.setChecked(True)
        options_layout.addWidget(self.show_spinner_check)
        
        self.auto_cancel_check = QCheckBox("Auto-cancelar lento")
        self.auto_cancel_check.setChecked(True)
        options_layout.addWidget(self.auto_cancel_check)
        
        self.timeout_spin = QSpinBox()
        self.timeout_spin.setRange(10, 300)
        self.timeout_spin.setValue(30)
        self.timeout_spin.setSuffix("s")
        options_layout.addWidget(QLabel("Timeout:"))
        options_layout.addWidget(self.timeout_spin)
        
        operation_layout.addLayout(options_layout)
        
        layout.addWidget(operation_group)
        
        # Área de resultados
        results_group = QGroupBox("Resultados")
        results_layout = QVBoxLayout(results_group)
        
        self.results_text = QTextEdit()
        self.results_text.setReadOnly(True)
        self.results_text.setMaximumHeight(200)
        font = QFont("Courier", 9)
        self.results_text.setFont(font)
        results_layout.addWidget(self.results_text)
        
        layout.addWidget(results_group)
        
        # Estadísticas de rendimiento
        stats_group = QGroupBox("Estadísticas de Rendimiento")
        stats_layout = QVBoxLayout(stats_group)
        
        self.stats_label = QLabel("Cargando estadísticas...")
        self.stats_label.setStyleSheet("""
            QLabel {
                background-color: #f8f9fa;
                padding: 10px;
                border-radius: 5px;
                border: 1px solid #dee2e6;
                font-family: monospace;
            }
        """)
        stats_layout.addWidget(self.stats_label)
        
        layout.addWidget(stats_group)
        
        # Barra de progreso
        self.progress_bar = QProgressBar()
        self.progress_bar.setVisible(False)
        layout.addWidget(self.progress_bar)
        
        # Estado de conexión
        self.connection_status = QLabel("Estado: Desconectado")
        self.connection_status.setStyleSheet("color: #dc3545; font-weight: bold;")
        layout.addWidget(self.connection_status)
        
        # Configurar el layout principal
        layout.setStretch(0, 1)  # Operaciones
        layout.setStretch(1, 2)  # Resultados
        layout.setStretch(2, 1)  # Estadísticas
        layout.setStretch(3, 0)  # Progreso
        layout.setStretch(4, 0)  # Estado
    
    def _connect_signals(self):
        """Conecta las señales de la operación"""
        self.operation_manager.operation_started.connect(self._on_operation_started)
        self.operation_manager.operation_finished.connect(self._on_operation_finished)
        self.operation_manager.operation_error.connect(self._on_operation_error)
        self.operation_manager.operation_progress.connect(self._on_operation_progress)
        
        # Conectar señales personalizadas
        self.operation_completed.connect(self._on_custom_operation_completed)
        self.operation_failed.connect(self._on_custom_operation_failed)
        
        # Conectar cambio de operación
        self.operation_combo.currentTextChanged.connect(self._on_operation_changed)
    
    def _on_operation_changed(self, operation_text):
        """Maneja el cambio de operación seleccionada"""
        # Ocultar todos los parámetros primero
        self.param_usuario_id.setVisible(False)
        self.param_query.setVisible(False)
        self.param_limit.setVisible(False)
        
        # Mostrar parámetros según la operación
        if "ID" in operation_text:
            self.param_usuario_id.setVisible(True)
        elif "Buscar Usuarios" in operation_text:
            self.param_query.setVisible(True)
            self.param_limit.setVisible(True)
        elif "Pagos" in operation_text or "Asistencias" in operation_text:
            self.param_usuario_id.setVisible(True)
            self.param_limit.setVisible(True)
    
    def execute_operation(self):
        """Ejecuta la operación seleccionada"""
        # Evitar ejecuciones si el widget no está visible o está destruido
        if getattr(self, '_destroyed', False):
            return
        if hasattr(self, 'isVisible') and not self.isVisible():
            return
        operation_text = self.operation_combo.currentText()
        operation_key = self._get_operation_key(operation_text)
        
        if not operation_key:
            QMessageBox.warning(self, "Advertencia", "Operación no soportada")
            return
        
        # Preparar parámetros
        params = self._get_operation_params(operation_key)
        
        # Mostrar loading si está habilitado
        if self.show_spinner_check.isChecked():
            message = f"Ejecutando: {operation_text}..."
            self.loading_manager.show_loading(
                operation_key, 
                message,
                spinner_type="circular",
                background_opacity=0.5
            )
        
        # Configurar timeout
        if self.auto_cancel_check.isChecked():
            QTimer.singleShot(self.timeout_spin.value() * 1000, 
                            lambda: self._timeout_operation(operation_key))
        
        # Ejecutar operación
        worker_id = self.operation_manager.execute_operation(operation_key, params)
        self.active_operations[operation_key] = worker_id
        
        # Actualizar UI
        self.execute_btn.setEnabled(False)
        self.cancel_btn.setEnabled(True)
        self.progress_bar.setVisible(True)
        self.progress_bar.setRange(0, 0)  # Modo indeterminado
        
        # Log
        logging.info(f"Operación iniciada: {operation_key} con parámetros: {params}")
    
    def cancel_operation(self):
        """Cancela la operación actual"""
        if getattr(self, '_destroyed', False):
            return
        current_operation = self.operation_combo.currentText()
        operation_key = self._get_operation_key(current_operation)
        
        if operation_key in self.active_operations:
            worker_id = self.active_operations[operation_key]
            self.operation_manager.cancel_operation(worker_id)
            
            self.loading_manager.hide_loading(operation_key)
            self._reset_ui_state()
            
            logging.info(f"Operación cancelada: {operation_key}")
    
    def _get_operation_key(self, operation_text):
        """Convierte texto de operación a clave"""
        mapping = {
            "Obtener Usuarios": "get_usuarios",
            "Buscar Usuario por ID": "get_usuario_by_id",
            "Obtener Pagos por Usuario": "get_pagos_by_usuario",
            "Obtener Asistencias de Hoy": "get_asistencias_today",
            "Obtener Asistencias por Usuario": "get_asistencias_by_usuario",
            "Obtener Clases Activas": "get_clases_activas",
            "Obtener Profesores Activos": "get_profesores_activos",
            "Buscar Usuarios": "search_usuarios",
            "Reporte de Pagos": "get_reporte_pagos",
            "Reporte de Asistencias": "get_reporte_asistencias"
        }
        return mapping.get(operation_text)
    
    def _get_operation_params(self, operation_key):
        """Obtiene los parámetros para la operación"""
        params = {}
        
        if operation_key == "get_usuario_by_id":
            params["usuario_id"] = self.param_usuario_id.value()
        elif operation_key == "get_pagos_by_usuario":
            params["usuario_id"] = self.param_usuario_id.value()
            params["limit"] = self.param_limit.value()
        elif operation_key == "get_asistencias_by_usuario":
            params["usuario_id"] = self.param_usuario_id.value()
            params["fecha_inicio"] = datetime.now() - timedelta(days=30)
            params["fecha_fin"] = datetime.now()
        elif operation_key == "search_usuarios":
            params["query"] = self.param_query.currentText()
            params["limit"] = self.param_limit.value()
        elif operation_key == "get_reporte_pagos":
            now = datetime.now()
            params["mes"] = now.month
            params["anio"] = now.year
        elif operation_key == "get_reporte_asistencias":
            params["dias"] = 30
        
        return params
    
    def _on_operation_started(self, message):
        """Maneja el inicio de una operación"""
        self.results_text.append(f"[{datetime.now().strftime('%H:%M:%S')}] {message}")
    
    def _on_operation_finished(self, operation, result):
        """Maneja la finalización de una operación"""
        if getattr(self, '_destroyed', False):
            return
        operation_key = operation.split(":")[0] if ":" in operation else operation
        
        # Ocultar loading
        self.loading_manager.hide_loading(operation_key)
        
        # Mostrar resultados
        if isinstance(result, list):
            count = len(result)
            self.results_text.append(f"✓ Operación completada: {count} registros encontrados")
            if count > 0:
                self.results_text.append(f"Primer registro: {result[0] if result else 'N/A'}")
        elif isinstance(result, dict):
            self.results_text.append(f"✓ Operación completada: {result}")
        else:
            self.results_text.append(f"✓ Operación completada: {type(result)}")
        
        self._reset_ui_state()
        logging.info(f"Operación completada: {operation}")
    
    def _on_operation_error(self, operation, error):
        """Maneja errores de operación"""
        if getattr(self, '_destroyed', False):
            return
        operation_key = operation.split(":")[0] if ":" in operation else operation
        
        # Ocultar loading
        self.loading_manager.hide_loading(operation_key)
        
        # Mostrar error
        self.results_text.append(f"✗ Error en operación {operation}: {error}")
        QMessageBox.critical(self, "Error de Base de Datos", 
                           f"Error en {operation}: {error}")
        
        self._reset_ui_state()
        logging.error(f"Error en operación {operation}: {error}")
    
    def _on_operation_progress(self, worker_id, progress):
        """Maneja el progreso de la operación"""
        if getattr(self, '_destroyed', False):
            return
        self.progress_bar.setValue(progress)
        self.results_text.append(f"Progreso: {progress}%")
    
    def _timeout_operation(self, operation_key):
        """Maneja el timeout de una operación"""
        if getattr(self, '_destroyed', False):
            return
        if operation_key in self.active_operations:
            self.cancel_operation()
            QMessageBox.warning(self, "Timeout", 
                              f"La operación {operation_key} tardó demasiado tiempo y fue cancelada.")
    
    def _reset_ui_state(self):
        """Restablece el estado de la UI"""
        self.execute_btn.setEnabled(True)
        self.cancel_btn.setEnabled(False)
        self.progress_bar.setVisible(False)
        self.progress_bar.setRange(0, 100)
        self.progress_bar.setValue(0)
        
        # Limpiar operaciones activas
        self.active_operations.clear()
    
    def _update_stats(self):
        """Actualiza las estadísticas de rendimiento en segundo plano evitando solapamientos"""
        # Evitar actualizaciones si el widget no está visible o destruido
        if getattr(self, '_destroyed', False):
            return
        if hasattr(self, 'isVisible') and not self.isVisible():
            return
        if self._stats_loading:
            return
        self._stats_loading = True

        def _collect():
            # Recolectar métricas y estado de conexión en background
            stats = self.db_manager.get_query_performance_stats()
            connected = False
            try:
                with self.db_manager.readonly_session() as conn:
                    with conn.cursor() as cur:
                        cur.execute("SELECT 1")
                        connected = True
            except Exception:
                connected = False
            return {"stats": stats, "connected": connected}

        def _on_success(data):
            try:
                # Evitar actualizar UI si el widget fue destruido o está oculto
                if getattr(self, '_destroyed', False):
                    return
                if hasattr(self, 'isVisible') and not self.isVisible():
                    return
                stats = data.get("stats", {}) if isinstance(data, dict) else {}
                stats_text = (
                    f"""
            <b>Estadísticas de Rendimiento:</b><br>
            • Consultas totales: {stats.get('total_queries', 0):,}<br>
            • Consultas lentas: {stats.get('slow_queries', 0):,}<br>
            • Porcentaje de consultas lentas: {stats.get('slow_query_percentage', 0):.1f}%<br>
            • Tiempo promedio de consulta: {stats.get('average_query_time', 0):.2f}s<br>
            • Ratio de caché: {stats.get('cache_hit_ratio', 0):.1f}%<br>
            • Última actualización: {datetime.now().strftime('%H:%M:%S')}
            """
                )
                self.stats_label.setText(stats_text)

                if data.get("connected"):
                    self.connection_status.setText("Estado: Conectado a São Paulo ✓")
                    self.connection_status.setStyleSheet("color: #28a745; font-weight: bold;")
                else:
                    self.connection_status.setText("Estado: Error de conexión ✗")
                    self.connection_status.setStyleSheet("color: #dc3545; font-weight: bold;")
            finally:
                self._stats_loading = False

        def _on_error(err):
            try:
                if getattr(self, '_destroyed', False):
                    return
                self.stats_label.setText(f"Error al obtener estadísticas: {err}")
                self.connection_status.setText("Estado: Error de conexión ✗")
                self.connection_status.setStyleSheet("color: #dc3545; font-weight: bold;")
                logging.error(f"Error actualizando estadísticas: {err}")
            finally:
                self._stats_loading = False

        try:
            run_in_background(
                _collect,
                on_success=_on_success,
                on_error=_on_error,
                parent=self,
                timeout_ms=4500,
                description="Actualizar estadísticas de BD",
            )
        except Exception as e:
            try:
                self.stats_label.setText(f"Error al iniciar actualización de estadísticas: {e}")
                logging.error(f"Error arrancando actualización de estadísticas: {e}")
            finally:
                self._stats_loading = False

    def _cleanup_on_destroy(self):
        """Detiene timers, marca destrucción y limpia cargas activas de forma segura"""
        try:
            self._destroyed = True
            if hasattr(self, 'stats_timer') and isinstance(self.stats_timer, QTimer):
                self.stats_timer.stop()
        except Exception:
            pass
        # Ocultar overlays de carga activos de forma defensiva
        try:
            overlays = getattr(self.loading_manager, 'overlays', {})
            for key in list(overlays.keys()):
                try:
                    self.loading_manager.hide_loading(key)
                except Exception:
                    pass
        except Exception:
            pass
        # Cancelar operaciones activas si es posible
        try:
            if hasattr(self.operation_manager, 'cancel_operation'):
                for op_key, worker_id in list(self.active_operations.items()):
                    try:
                        self.operation_manager.cancel_operation(worker_id)
                    except Exception:
                        pass
                self.active_operations.clear()
        except Exception:
            pass
    
    def _on_custom_operation_completed(self, operation, result):
        """Maneja operaciones personalizadas completadas"""
        self.operation_completed.emit(operation, result)
    
    def _on_custom_operation_failed(self, operation, error):
        """Maneja operaciones personalizadas fallidas"""
        self.operation_failed.emit(operation, error)


# Clase auxiliar para integrar con widgets existentes
class AsyncDatabaseHelper(QObject):
    """Helper para integrar operaciones asíncronas en widgets existentes"""
    
    def __init__(self, db_manager: DatabaseManager, parent_widget=None):
        super().__init__()
        self.db_manager = db_manager
        self.parent_widget = parent_widget
        self.loading_manager = DatabaseLoadingManager(parent_widget)
        self.operation_manager = DatabaseOperationManager(db_manager)
        
        # Conectar señales
        self.operation_manager.operation_finished.connect(self._on_operation_finished)
        self.operation_manager.operation_error.connect(self._on_operation_error)
    
    def execute_async(self, operation: str, params: dict = None, 
                     loading_message="Cargando...", operation_id="default", timeout_ms=30000):
        """Ejecuta una operación de base de datos de manera asíncrona"""
        # Mostrar loading
        self.loading_manager.show_loading(
            operation_id, 
            loading_message,
            background_opacity=0.3
        )
        
        # Ejecutar operación
        worker_id = self.operation_manager.execute_operation(operation, params)
        
        # Configurar timeout para auto-cierre
        QTimer.singleShot(timeout_ms, lambda: self._timeout_operation(operation_id, worker_id))
        
        return worker_id
    
    def _on_operation_finished(self, operation: str, result):
        """Maneja operación terminada"""
        # Ocultar loading
        operation_id = operation.split(":")[0] if ":" in operation else operation
        self.loading_manager.hide_loading(operation_id)
        
        # Emitir señal o manejar resultado
        logging.info(f"Operación asíncrona completada: {operation}")
    
    def _on_operation_error(self, operation: str, error: str):
        """Maneja error en operación"""
        # Ocultar loading
        operation_id = operation.split(":")[0] if ":" in operation else operation
        self.loading_manager.hide_loading(operation_id)
        
        # Mostrar error al usuario
        if self.parent_widget:
            from PyQt6.QtWidgets import QMessageBox
            QMessageBox.critical(
                self.parent_widget,
                "Error de Base de Datos",
                f"Error en {operation}: {error}"
            )
        
        logging.error(f"Error en operación asíncrona {operation}: {error}")
    
    def _timeout_operation(self, operation_id="default", worker_id=None):
        """Maneja el timeout de una operación - cierra el loading si aún está activo"""
        # Verificar si el loading sigue activo
        if operation_id in self.loading_manager.overlays:
            logging.warning(f"Timeout alcanzado para operación {operation_id}, cerrando loading")
            self.loading_manager.hide_loading(operation_id)
            
            # Cancelar el worker si existe
            if worker_id and hasattr(self.operation_manager, 'cancel_operation'):
                self.operation_manager.cancel_operation(worker_id)
    
    def hide_loading(self, operation_id="default"):
        """Oculta el loading"""
        self.loading_manager.hide_loading(operation_id)