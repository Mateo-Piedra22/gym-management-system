import sys
import logging
import os
import pandas as pd
from datetime import datetime, timedelta
from PyQt6.QtWidgets import (
    QWidget, QVBoxLayout, QHBoxLayout, QGridLayout, QLabel, QPushButton,
    QGroupBox, QTableWidget, QTableWidgetItem, QTabWidget, QDateEdit,
    QComboBox, QSpinBox, QTextEdit, QFrame, QScrollArea, QMessageBox,
    QHeaderView, QAbstractItemView, QProgressBar, QSplitter, QCheckBox,
    QFileDialog, QListWidget, QListWidgetItem, QRadioButton, QButtonGroup
)
from PyQt6.QtGui import QFont, QPixmap, QPalette, QColor, QIcon
from PyQt6.QtCore import Qt, QDate, QTimer, QThread, pyqtSignal

from database import DatabaseManager
from models import Usuario, Ejercicio, Clase, Pago
from utils import resource_path

class BulkImportExportThread(QThread):
    """Hilo para operaciones de importación/exportación masiva en segundo plano"""
    progress_updated = pyqtSignal(int)
    status_updated = pyqtSignal(str)
    operation_completed = pyqtSignal(bool, str)  # success, message
    
    def __init__(self, operation_type, data_type, file_path, db_manager, options=None):
        super().__init__()
        self.operation_type = operation_type  # 'import' or 'export'
        self.data_type = data_type  # 'usuarios', 'ejercicios', 'clases', 'pagos', 'all'
        self.file_path = file_path
        self.db_manager = db_manager
        self.options = options or {}
        
    def run(self):
        try:
            if self.operation_type == 'export':
                self._perform_export()
            else:
                self._perform_import()
        except Exception as e:
            logging.exception(f"Error en operación {self.operation_type}")
            self.operation_completed.emit(False, str(e))
    
    def _perform_export(self):
        """Realiza la exportación de datos"""
        self.status_updated.emit("Iniciando exportación...")
        
        if self.data_type == 'all':
            self._export_all_data()
        elif self.data_type == 'usuarios':
            self._export_usuarios()
        elif self.data_type == 'ejercicios':
            self._export_ejercicios()
        elif self.data_type == 'clases':
            self._export_clases()
        elif self.data_type == 'pagos':
            self._export_pagos()
            
        self.operation_completed.emit(True, "Exportación completada exitosamente")
    
    def _perform_import(self):
        """Realiza la importación de datos"""
        self.status_updated.emit("Iniciando importación...")
        
        if self.data_type == 'usuarios':
            self._import_usuarios()
        elif self.data_type == 'ejercicios':
            self._import_ejercicios()
        elif self.data_type == 'clases':
            self._import_clases()
        elif self.data_type == 'pagos':
            self._import_pagos()
            
        self.operation_completed.emit(True, "Importación completada exitosamente")
    
    def _export_all_data(self):
        """Exporta todos los datos del sistema"""
        with pd.ExcelWriter(self.file_path, engine='openpyxl') as writer:
            # Usuarios
            self.status_updated.emit("Exportando usuarios...")
            usuarios = self.db_manager.obtener_todos_usuarios()
            usuarios_data = [{
                'id': u.id, 'nombre': u.nombre, 'telefono': u.telefono,
                'fecha_registro': u.fecha_registro, 'activo': u.activo, 'rol': u.rol
            } for u in usuarios]
            pd.DataFrame(usuarios_data).to_excel(writer, sheet_name='Usuarios', index=False)
            self.progress_updated.emit(25)
            
            # Ejercicios
            self.status_updated.emit("Exportando ejercicios...")
            ejercicios = self.db_manager.obtener_ejercicios()
            ejercicios_data = [{
                'id': e.id, 'nombre': e.nombre, 'grupo_muscular': e.grupo_muscular,
                'descripcion': e.descripcion, 'objetivo': getattr(e, 'objetivo', None)
            } for e in ejercicios]
            pd.DataFrame(ejercicios_data).to_excel(writer, sheet_name='Ejercicios', index=False)
            self.progress_updated.emit(50)
            
            # Clases
            self.status_updated.emit("Exportando clases...")
            clases = self.db_manager.obtener_clases()
            clases_data = [{
                'id': c.id, 'nombre': c.nombre, 'descripcion': c.descripcion,
                'capacidad_maxima': c.capacidad_maxima
            } for c in clases]
            pd.DataFrame(clases_data).to_excel(writer, sheet_name='Clases', index=False)
            self.progress_updated.emit(75)
            
            # Pagos (últimos 12 meses)
            self.status_updated.emit("Exportando pagos...")
            fecha_limite = datetime.now() - timedelta(days=365)
            pagos = self.db_manager.obtener_pagos_desde_fecha(fecha_limite)
            pagos_data = []
            for p in pagos:
                usuario = self.db_manager.obtener_usuario(p.usuario_id)
                pagos_data.append({
                    'id': p.id, 'usuario_id': p.usuario_id, 'usuario_nombre': usuario.nombre if usuario else 'N/A',
                    'monto': p.monto, 'fecha_pago': p.fecha_pago, 'metodo_pago': p.metodo_pago,
                    'mes_pagado': p.mes_pagado, 'año_pagado': p.año_pagado
                })
            pd.DataFrame(pagos_data).to_excel(writer, sheet_name='Pagos', index=False)
            self.progress_updated.emit(100)
    
    def _export_usuarios(self):
        """Exporta solo usuarios"""
        usuarios = self.db_manager.obtener_todos_usuarios()
        usuarios_data = [{
            'nombre': u.nombre, 'telefono': u.telefono,
            'activo': u.activo, 'rol': u.rol
        } for u in usuarios]
        pd.DataFrame(usuarios_data).to_excel(self.file_path, index=False)
        self.progress_updated.emit(100)
    
    def _export_ejercicios(self):
        """Exporta solo ejercicios"""
        ejercicios = self.db_manager.obtener_ejercicios()
        ejercicios_data = [{
            'nombre': e.nombre, 'grupo_muscular': e.grupo_muscular,
            'descripcion': e.descripcion, 'objetivo': getattr(e, 'objetivo', None)
        } for e in ejercicios]
        pd.DataFrame(ejercicios_data).to_excel(self.file_path, index=False)
        self.progress_updated.emit(100)
    
    def _export_clases(self):
        """Exporta solo clases"""
        clases = self.db_manager.obtener_clases()
        clases_data = [{
            'nombre': c.nombre, 'descripcion': c.descripcion,
            'capacidad_maxima': c.capacidad_maxima
        } for c in clases]
        pd.DataFrame(clases_data).to_excel(self.file_path, index=False)
        self.progress_updated.emit(100)
    
    def _export_pagos(self):
        """Exporta pagos del período seleccionado"""
        fecha_inicio = self.options.get('fecha_inicio', datetime.now() - timedelta(days=365))
        fecha_fin = self.options.get('fecha_fin', datetime.now())
        
        pagos = self.db_manager.obtener_pagos_periodo(fecha_inicio, fecha_fin)
        pagos_data = []
        for p in pagos:
            usuario = self.db_manager.obtener_usuario(p.usuario_id)
            pagos_data.append({
                'usuario_nombre': usuario.nombre if usuario else 'N/A',
                'monto': p.monto, 'fecha_pago': p.fecha_pago,
                'metodo_pago': p.metodo_pago, 'mes_pagado': p.mes_pagado,
                'año_pagado': p.año_pagado
            })
        pd.DataFrame(pagos_data).to_excel(self.file_path, index=False)
        self.progress_updated.emit(100)
    
    def _import_usuarios(self):
        """Importa usuarios desde Excel en lotes (chunky)"""
        df = pd.read_excel(self.file_path)
        required_cols = ['nombre']

        if not all(col in df.columns for col in required_cols):
            raise ValueError(f"El archivo debe contener las columnas: {', '.join(required_cols)}")

        total_rows = len(df)
        if total_rows == 0:
            self.status_updated.emit("No hay filas para importar")
            return

        items: list[dict] = []
        for _, row in df.iterrows():
            try:
                item = {
                    'nombre': str(row['nombre']).strip(),
                    'dni': (str(row.get('dni')).strip() if row.get('dni') is not None else None),
                    'telefono': (str(row.get('telefono')).strip() if row.get('telefono') is not None else None),
                    'pin': (str(row.get('pin')).strip() if row.get('pin') is not None else None),
                    'rol': (str(row.get('rol', 'socio')).strip().lower() or 'socio'),
                    'activo': bool(row.get('activo', True)),
                    'tipo_cuota': row.get('tipo_cuota'),
                    'notas': row.get('notas'),
                }
                items.append(item)
            except Exception as e:
                logging.warning(f"Error preparando usuario: {e}")

        if not items:
            self.status_updated.emit("No se pudieron preparar usuarios para importar")
            return

        opts = self.options or {}
        page_size = int(opts.get('page_size', 250))
        skip_duplicates = bool(opts.get('skip_duplicates', True))

        total_insertados = 0
        total_actualizados = 0
        omitidos_total = []

        for start in range(0, len(items), page_size):
            chunk = items[start:start + page_size]
            try:
                result = self.db_manager.registrar_usuarios_batch(chunk, skip_duplicates=skip_duplicates, validate_data=True)
                total_insertados += len(result.get('insertados', []))
                total_actualizados += len(result.get('actualizados', []))
                omitidos_total.extend(result.get('omitidos', []))
            except Exception as e:
                logging.warning(f"Error importando lote de usuarios: {e}")

            progress = int(min(100, (start + len(chunk)) / len(items) * 100))
            self.progress_updated.emit(progress)
            self.status_updated.emit(f"Importando usuarios... {start + len(chunk)}/{len(items)}")

        self.status_updated.emit(
            f"Importación completada: {total_insertados} insertados, {total_actualizados} actualizados, {len(omitidos_total)} omitidos"
        )
    
    def _import_ejercicios(self):
        """Importa ejercicios desde Excel en lotes (chunky)"""
        df = pd.read_excel(self.file_path)
        required_cols = ['nombre']

        if not all(col in df.columns for col in required_cols):
            raise ValueError(f"El archivo debe contener las columnas: {', '.join(required_cols)}")

        total_rows = len(df)
        if total_rows == 0:
            self.status_updated.emit("No hay filas para importar")
            return

        # Preparar items para batch
        items: list[dict] = []
        for _, row in df.iterrows():
            try:
                item = {
                    'nombre': str(row['nombre']).strip(),
                    'grupo_muscular': (str(row.get('grupo_muscular')).strip() if row.get('grupo_muscular') is not None else None),
                    'descripcion': (str(row.get('descripcion')).strip() if row.get('descripcion') is not None else None),
                }
                items.append(item)
            except Exception as e:
                logging.warning(f"Error preparando ejercicio: {e}")

        if not items:
            self.status_updated.emit("No se pudieron preparar ejercicios para importar")
            return

        # Chunking y batch
        opts = self.options or {}
        page_size = int(opts.get('page_size', 250))
        skip_duplicates = bool(opts.get('skip_duplicates', True))

        total_insertados = 0
        total_actualizados = 0
        omitidos_total = []

        for start in range(0, len(items), page_size):
            chunk = items[start:start + page_size]
            try:
                result = self.db_manager.registrar_ejercicios_batch(chunk, skip_duplicates=skip_duplicates, validate_data=True)
                total_insertados += len(result.get('insertados', []))
                total_actualizados += len(result.get('actualizados', []))
                omitidos_total.extend(result.get('omitidos', []))
            except Exception as e:
                logging.warning(f"Error importando lote de ejercicios: {e}")

            progress = int(min(100, (start + len(chunk)) / len(items) * 100))
            self.progress_updated.emit(progress)
            self.status_updated.emit(f"Importando ejercicios... {start + len(chunk)}/{len(items)}")

        self.status_updated.emit(
            f"Importación completada: {total_insertados} insertados, {total_actualizados} actualizados, {len(omitidos_total)} omitidos"
        )
    
    def _import_clases(self):
        """Importa clases desde Excel en lotes (chunky)"""
        df = pd.read_excel(self.file_path)
        required_cols = ['nombre']

        if not all(col in df.columns for col in required_cols):
            raise ValueError(f"El archivo debe contener las columnas: {', '.join(required_cols)}")

        total_rows = len(df)
        if total_rows == 0:
            self.status_updated.emit("No hay filas para importar")
            return

        items: list[dict] = []
        for _, row in df.iterrows():
            try:
                cap_raw = row.get('capacidad_maxima')
                capacidad = int(cap_raw) if pd.notna(cap_raw) else 20
                item = {
                    'nombre': str(row['nombre']).strip(),
                    'descripcion': (str(row.get('descripcion')).strip() if row.get('descripcion') is not None else None),
                    'capacidad_maxima': capacidad,
                }
                items.append(item)
            except Exception as e:
                logging.warning(f"Error preparando clase: {e}")

        if not items:
            self.status_updated.emit("No se pudieron preparar clases para importar")
            return

        opts = self.options or {}
        page_size = int(opts.get('page_size', 250))
        skip_duplicates = bool(opts.get('skip_duplicates', True))

        total_insertados = 0
        total_actualizados = 0
        omitidos_total = []

        for start in range(0, len(items), page_size):
            chunk = items[start:start + page_size]
            try:
                result = self.db_manager.registrar_clases_batch(chunk, skip_duplicates=skip_duplicates, validate_data=True)
                total_insertados += len(result.get('insertados', []))
                total_actualizados += len(result.get('actualizados', []))
                omitidos_total.extend(result.get('omitidos', []))
            except Exception as e:
                logging.warning(f"Error importando lote de clases: {e}")

            progress = int(min(100, (start + len(chunk)) / len(items) * 100))
            self.progress_updated.emit(progress)
            self.status_updated.emit(f"Importando clases... {start + len(chunk)}/{len(items)}")

        self.status_updated.emit(
            f"Importación completada: {total_insertados} insertados, {total_actualizados} actualizados, {len(omitidos_total)} omitidos"
        )
    
    def _import_pagos(self):
        """Importa pagos desde Excel usando inserción/actualización en lote"""
        df = pd.read_excel(self.file_path)
        required_cols = ['usuario_id', 'monto', 'fecha_pago']

        if not all(col in df.columns for col in required_cols):
            raise ValueError(f"El archivo debe contener las columnas: {', '.join(required_cols)}")

        total_rows = len(df)
        if total_rows == 0:
            self.status_updated.emit("No hay filas para importar")
            return

        # Construir items de importación compatibles con registrar_pagos_batch
        items: list[dict] = []
        for _, row in df.iterrows():
            try:
                fecha_raw = row['fecha_pago']
                # to_datetime maneja múltiples formatos
                fecha_dt = pd.to_datetime(fecha_raw, errors='coerce')
                if pd.isna(fecha_dt):
                    fecha_dt = pd.Timestamp.now()

                item = {
                    'usuario_id': int(row['usuario_id']),
                    'monto': float(row['monto']),
                    'fecha_pago': fecha_dt.to_pydatetime(),
                }
                # Soportar columnas opcionales
                if 'metodo_pago_id' in df.columns and pd.notna(row.get('metodo_pago_id')):
                    try:
                        item['metodo_pago_id'] = int(row.get('metodo_pago_id'))
                    except Exception:
                        pass
                if 'metodo_pago' in df.columns and pd.notna(row.get('metodo_pago')):
                    item['metodo_pago'] = str(row.get('metodo_pago')).strip()
                # Alias para mes/año
                if 'mes' in df.columns and pd.notna(row.get('mes')):
                    item['mes'] = int(row.get('mes'))
                elif 'mes_pagado' in df.columns and pd.notna(row.get('mes_pagado')):
                    item['mes'] = int(row.get('mes_pagado'))
                if 'año' in df.columns and pd.notna(row.get('año')):
                    item['año'] = int(row.get('año'))
                elif 'año_pagado' in df.columns and pd.notna(row.get('año_pagado')):
                    item['año'] = int(row.get('año_pagado'))

                items.append(item)
            except Exception as e:
                logging.warning(f"Error preparando pago: {e}")

        if not items:
            self.status_updated.emit("No se pudieron preparar pagos válidos")
            return

        # Ejecutar en chunks para grandes volúmenes
        chunk_size = int(self.options.get('chunk_size', 500))
        skip_dup = bool(self.options.get('skip_duplicates', False))
        validate_data = bool(self.options.get('validate_data', True))

        imported_count = 0
        omitted_count = 0
        total_chunks = max(1, (len(items) + chunk_size - 1) // chunk_size)

        for i in range(0, len(items), chunk_size):
            chunk = items[i:i + chunk_size]
            try:
                result = self.db_manager.registrar_pagos_batch(chunk, skip_duplicates=skip_dup, validate_data=validate_data)
                imported_count += int(result.get('count') or 0)
                omitted_count += len(result.get('omitidos') or [])
            except Exception as e:
                logging.exception(f"Error importando chunk de pagos: {e}")
                # contar este chunk como omitido completo
                omitted_count += len(chunk)

            progress = int(((i // chunk_size) + 1) / total_chunks * 100)
            self.progress_updated.emit(progress)
            self.status_updated.emit(f"Importando pagos... {imported_count}/{total_rows}")

        self.status_updated.emit(f"Importación completada: {imported_count} pagos importados, {omitted_count} omitidos")

class BulkImportExportWidget(QWidget):
    """Widget para importación y exportación masiva de datos"""
    
    def __init__(self, db_manager: DatabaseManager):
        super().__init__()
        self.db_manager = db_manager
        self.current_thread = None
        self.setup_ui()
    
    def setup_ui(self):
        """Configura la interfaz de usuario"""
        main_layout = QVBoxLayout(self)
        
        # Título
        title_label = QLabel("Importación y Exportación Masiva de Datos")
        title_label.setFont(QFont("Arial", 16, QFont.Weight.Bold))
        title_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        title_label.setObjectName("bulk_import_export_title")
        main_layout.addWidget(title_label)
        
        # Tabs principales
        self.tab_widget = QTabWidget()
        self.tab_widget.setStyleSheet("""
            QTabWidget::pane {
                border: 1px solid #bdc3c7;
                background-color: white;
            }
            QTabBar::tab {
                background-color: #ecf0f1;
                padding: 8px 16px;
                margin-right: 2px;
                border-top-left-radius: 4px;
                border-top-right-radius: 4px;
            }
            QTabBar::tab:selected {
                background-color: #3498db;
                color: white;
            }
        """)
        
        # Tab de Exportación
        self.export_tab = self.create_export_tab()
        self.tab_widget.addTab(self.export_tab, "📤 Exportar Datos")
        
        # Tab de Importación
        self.import_tab = self.create_import_tab()
        self.tab_widget.addTab(self.import_tab, "📥 Importar Datos")
        
        # Tab de Plantillas
        self.templates_tab = self.create_templates_tab()
        self.tab_widget.addTab(self.templates_tab, "📋 Plantillas")
        
        main_layout.addWidget(self.tab_widget)
        
        # Barra de progreso y estado
        self.setup_progress_section(main_layout)
    
    def create_export_tab(self):
        """Crea la pestaña de exportación"""
        tab = QWidget()
        layout = QVBoxLayout(tab)
        
        # Selección de tipo de datos
        data_group = QGroupBox("Seleccionar Datos a Exportar")
        data_layout = QVBoxLayout(data_group)
        
        self.export_data_group = QButtonGroup()
        self.export_all_radio = QRadioButton("Todos los datos del sistema")
        self.export_usuarios_radio = QRadioButton("Solo usuarios")
        self.export_ejercicios_radio = QRadioButton("Solo ejercicios")
        self.export_clases_radio = QRadioButton("Solo clases")
        self.export_pagos_radio = QRadioButton("Solo pagos")
        
        self.export_all_radio.setChecked(True)
        
        for radio in [self.export_all_radio, self.export_usuarios_radio, 
                     self.export_ejercicios_radio, self.export_clases_radio, self.export_pagos_radio]:
            self.export_data_group.addButton(radio)
            data_layout.addWidget(radio)
        
        layout.addWidget(data_group)
        
        # Opciones adicionales para pagos
        self.export_options_group = QGroupBox("Opciones de Exportación")
        options_layout = QGridLayout(self.export_options_group)
        
        options_layout.addWidget(QLabel("Fecha inicio (para pagos):"), 0, 0)
        self.export_fecha_inicio = QDateEdit()
        self.export_fecha_inicio.setDate(QDate.currentDate().addYears(-1))
        options_layout.addWidget(self.export_fecha_inicio, 0, 1)
        
        options_layout.addWidget(QLabel("Fecha fin (para pagos):"), 1, 0)
        self.export_fecha_fin = QDateEdit()
        self.export_fecha_fin.setDate(QDate.currentDate())
        options_layout.addWidget(self.export_fecha_fin, 1, 1)
        
        layout.addWidget(self.export_options_group)
        
        # Botón de exportación
        self.export_button = QPushButton("🗂️ Exportar Datos")
        self.export_button.setStyleSheet("""
            QPushButton {
                background-color: #27ae60;
                color: white;
                border: none;
                padding: 12px 24px;
                border-radius: 6px;
                font-weight: bold;
                font-size: 14px;
            }
            QPushButton:hover {
                background-color: #2ecc71;
            }
            QPushButton:pressed {
                background-color: #1e8449;
            }
        """)
        self.export_button.clicked.connect(self.start_export)
        layout.addWidget(self.export_button)
        
        layout.addStretch()
        return tab
    
    def create_import_tab(self):
        """Crea la pestaña de importación"""
        tab = QWidget()
        layout = QVBoxLayout(tab)
        
        # Selección de tipo de datos
        data_group = QGroupBox("Seleccionar Tipo de Datos a Importar")
        data_layout = QVBoxLayout(data_group)
        
        self.import_data_group = QButtonGroup()
        self.import_usuarios_radio = QRadioButton("Usuarios")
        self.import_ejercicios_radio = QRadioButton("Ejercicios")
        self.import_clases_radio = QRadioButton("Clases")
        self.import_pagos_radio = QRadioButton("Pagos")
        
        self.import_usuarios_radio.setChecked(True)
        
        for radio in [self.import_usuarios_radio, self.import_ejercicios_radio, 
                     self.import_clases_radio, self.import_pagos_radio]:
            self.import_data_group.addButton(radio)
            data_layout.addWidget(radio)
        
        layout.addWidget(data_group)
        
        # Selección de archivo
        file_group = QGroupBox("Archivo a Importar")
        file_layout = QHBoxLayout(file_group)
        
        self.import_file_path = QLabel("Ningún archivo seleccionado")
        self.import_file_path.setObjectName("bulk_import_file_path")
        file_layout.addWidget(self.import_file_path)
        
        self.select_file_button = QPushButton("📁 Seleccionar Archivo")
        self.select_file_button.clicked.connect(self.select_import_file)
        file_layout.addWidget(self.select_file_button)
        
        layout.addWidget(file_group)
        
        # Opciones de importación
        import_options_group = QGroupBox("Opciones de Importación")
        import_options_layout = QVBoxLayout(import_options_group)
        
        self.skip_duplicates_checkbox = QCheckBox("Omitir registros duplicados")
        self.skip_duplicates_checkbox.setChecked(True)
        import_options_layout.addWidget(self.skip_duplicates_checkbox)
        
        self.validate_data_checkbox = QCheckBox("Validar datos antes de importar")
        self.validate_data_checkbox.setChecked(True)
        import_options_layout.addWidget(self.validate_data_checkbox)
        
        layout.addWidget(import_options_group)
        
        # Botón de importación
        self.import_button = QPushButton("📥 Importar Datos")
        self.import_button.setStyleSheet("""
            QPushButton {
                background-color: #3498db;
                color: white;
                border: none;
                padding: 12px 24px;
                border-radius: 6px;
                font-weight: bold;
                font-size: 14px;
            }
            QPushButton:hover {
                background-color: #5dade2;
            }
            QPushButton:pressed {
                background-color: #2980b9;
            }
        """)
        self.import_button.clicked.connect(self.start_import)
        self.import_button.setEnabled(False)
        layout.addWidget(self.import_button)
        
        layout.addStretch()
        return tab
    
    def create_templates_tab(self):
        """Crea la pestaña de plantillas"""
        tab = QWidget()
        layout = QVBoxLayout(tab)
        
        # Información sobre plantillas
        info_group = QGroupBox("Plantillas de Importación")
        info_layout = QVBoxLayout(info_group)
        
        info_text = QTextEdit()
        info_text.setReadOnly(True)
        info_text.setMaximumHeight(200)
        info_text.setHtml("""
        <h3>Formatos de Plantillas para Importación</h3>
        <p><b>Usuarios:</b> nombre, telefono, activo, rol</p>
        <p><b>Ejercicios:</b> nombre, grupo_muscular, descripcion</p>
        <p><b>Clases:</b> nombre, descripcion, capacidad_maxima</p>
        <p><b>Pagos:</b> usuario_id, monto, fecha_pago, metodo_pago, mes_pagado, año_pagado</p>
        <br>
        <p><i>Las columnas marcadas en negrita son obligatorias.</i></p>
        """)
        info_layout.addWidget(info_text)
        
        layout.addWidget(info_group)
        
        # Botones para descargar plantillas
        templates_group = QGroupBox("Descargar Plantillas")
        templates_layout = QGridLayout(templates_group)
        
        template_buttons = [
            ("👥 Plantilla Usuarios", self.download_usuarios_template),
            ("💪 Plantilla Ejercicios", self.download_ejercicios_template),
            ("🧘 Plantilla Clases", self.download_clases_template),
            ("💰 Plantilla Pagos", self.download_pagos_template)
        ]
        
        for i, (text, callback) in enumerate(template_buttons):
            button = QPushButton(text)
            button.setStyleSheet("""
                QPushButton {
                    background-color: #9b59b6;
                    color: white;
                    border: none;
                    padding: 10px 16px;
                    border-radius: 4px;
                    font-weight: bold;
                }
                QPushButton:hover {
                    background-color: #af7ac5;
                }
            """)
            button.clicked.connect(callback)
            templates_layout.addWidget(button, i // 2, i % 2)
        
        layout.addWidget(templates_group)
        layout.addStretch()
        return tab
    
    def setup_progress_section(self, main_layout):
        """Configura la sección de progreso"""
        progress_group = QGroupBox("Estado de la Operación")
        progress_layout = QVBoxLayout(progress_group)
        
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
        progress_layout.addWidget(self.progress_bar)
        
        self.status_label = QLabel("Listo para operaciones")
        self.status_label.setObjectName("bulk_import_status_label")
        progress_layout.addWidget(self.status_label)
        
        main_layout.addWidget(progress_group)
    
    def select_import_file(self):
        """Selecciona el archivo para importar"""
        file_path, _ = QFileDialog.getOpenFileName(
            self, "Seleccionar archivo para importar", "", 
            "Excel Files (*.xlsx *.xls);;CSV Files (*.csv)"
        )
        
        if file_path:
            self.import_file_path.setText(file_path)
            self.import_button.setEnabled(True)
    
    def start_export(self):
        """Inicia el proceso de exportación"""
        # Determinar tipo de datos
        if self.export_all_radio.isChecked():
            data_type = 'all'
        elif self.export_usuarios_radio.isChecked():
            data_type = 'usuarios'
        elif self.export_ejercicios_radio.isChecked():
            data_type = 'ejercicios'
        elif self.export_clases_radio.isChecked():
            data_type = 'clases'
        elif self.export_pagos_radio.isChecked():
            data_type = 'pagos'
        
        # Seleccionar archivo de destino
        default_name = f"export_{data_type}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
        file_path, _ = QFileDialog.getSaveFileName(
            self, "Guardar exportación", default_name, "Excel Files (*.xlsx)"
        )
        
        if not file_path:
            return
        
        # Opciones adicionales
        options = {}
        if data_type == 'pagos':
            options['fecha_inicio'] = self.export_fecha_inicio.date().toPython()
            options['fecha_fin'] = self.export_fecha_fin.date().toPython()
        
        # Iniciar hilo de exportación
        self.current_thread = BulkImportExportThread(
            'export', data_type, file_path, self.db_manager, options
        )
        self.connect_thread_signals()
        self.current_thread.start()
        
        self.export_button.setEnabled(False)
        self.progress_bar.setVisible(True)
        self.progress_bar.setValue(0)
    
    def start_import(self):
        """Inicia el proceso de importación"""
        file_path = self.import_file_path.text()
        if file_path == "Ningún archivo seleccionado":
            QMessageBox.warning(self, "Error", "Debe seleccionar un archivo para importar")
            return
        
        # Determinar tipo de datos
        if self.import_usuarios_radio.isChecked():
            data_type = 'usuarios'
        elif self.import_ejercicios_radio.isChecked():
            data_type = 'ejercicios'
        elif self.import_clases_radio.isChecked():
            data_type = 'clases'
        elif self.import_pagos_radio.isChecked():
            data_type = 'pagos'
        
        # Confirmar importación
        reply = QMessageBox.question(
            self, "Confirmar Importación",
            f"¿Está seguro de que desea importar {data_type} desde el archivo seleccionado?\n\n"
            f"Esta operación puede agregar muchos registros a la base de datos.",
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No
        )
        
        if reply != QMessageBox.StandardButton.Yes:
            return
        
        # Opciones de importación
        options = {
            'skip_duplicates': self.skip_duplicates_checkbox.isChecked(),
            'validate_data': self.validate_data_checkbox.isChecked()
        }
        
        # Iniciar hilo de importación
        self.current_thread = BulkImportExportThread(
            'import', data_type, file_path, self.db_manager, options
        )
        self.connect_thread_signals()
        self.current_thread.start()
        
        self.import_button.setEnabled(False)
        self.progress_bar.setVisible(True)
        self.progress_bar.setValue(0)
    
    def connect_thread_signals(self):
        """Conecta las señales del hilo"""
        self.current_thread.progress_updated.connect(self.progress_bar.setValue)
        self.current_thread.status_updated.connect(self.status_label.setText)
        self.current_thread.operation_completed.connect(self.on_operation_completed)
    
    def on_operation_completed(self, success, message):
        """Maneja la finalización de la operación"""
        self.progress_bar.setVisible(False)
        self.export_button.setEnabled(True)
        self.import_button.setEnabled(True)
        
        if success:
            QMessageBox.information(self, "Operación Completada", message)
            self.status_label.setText("Operación completada exitosamente")
            self.status_label.setProperty("operationStatus", "success")
        else:
            QMessageBox.critical(self, "Error en Operación", f"Error: {message}")
            self.status_label.setText(f"Error: {message}")
            self.status_label.setProperty("operationStatus", "error")
        
        # Refrescar estilos
        self.status_label.style().unpolish(self.status_label)
        self.status_label.style().polish(self.status_label)
    
    def download_usuarios_template(self):
        """Descarga plantilla de usuarios"""
        file_path, _ = QFileDialog.getSaveFileName(
            self, "Guardar plantilla de usuarios", "plantilla_usuarios.xlsx", "Excel Files (*.xlsx)"
        )
        
        if file_path:
            template_data = pd.DataFrame({
                'nombre': ['Juan Pérez', 'María García'],
                'telefono': ['123456789', '987654321'],
                'activo': [True, True],
                'rol': ['socio', 'socio']
            })
            template_data.to_excel(file_path, index=False)
            QMessageBox.information(self, "Plantilla Creada", f"Plantilla guardada en:\n{file_path}")
    
    def download_ejercicios_template(self):
        """Descarga plantilla de ejercicios"""
        file_path, _ = QFileDialog.getSaveFileName(
            self, "Guardar plantilla de ejercicios", "plantilla_ejercicios.xlsx", "Excel Files (*.xlsx)"
        )
        
        if file_path:
            template_data = pd.DataFrame({
                'nombre': ['Press de banca', 'Sentadillas'],
                'grupo_muscular': ['Pecho', 'Piernas'],
                'descripcion': ['Ejercicio para pecho y tríceps', 'Ejercicio para cuádriceps y glúteos']
            })
            template_data.to_excel(file_path, index=False)
            QMessageBox.information(self, "Plantilla Creada", f"Plantilla guardada en:\n{file_path}")
    
    def download_clases_template(self):
        """Descarga plantilla de clases"""
        file_path, _ = QFileDialog.getSaveFileName(
            self, "Guardar plantilla de clases", "plantilla_clases.xlsx", "Excel Files (*.xlsx)"
        )
        
        if file_path:
            template_data = pd.DataFrame({
                'nombre': ['Yoga', 'Spinning'],
                'descripcion': ['Clase de yoga para relajación', 'Clase de ciclismo indoor'],
                'capacidad_maxima': [15, 20]
            })
            template_data.to_excel(file_path, index=False)
            QMessageBox.information(self, "Plantilla Creada", f"Plantilla guardada en:\n{file_path}")
    
    def download_pagos_template(self):
        """Descarga plantilla de pagos"""
        file_path, _ = QFileDialog.getSaveFileName(
            self, "Guardar plantilla de pagos", "plantilla_pagos.xlsx", "Excel Files (*.xlsx)"
        )
        
        if file_path:
            template_data = pd.DataFrame({
                'usuario_id': [1, 2],
                'monto': [50000, 40000],
                'fecha_pago': ['2024-01-15', '2024-01-20'],
                'metodo_pago': ['efectivo', 'tarjeta'],
                'mes_pagado': [1, 1],
                'año_pagado': [2024, 2024]
            })
            template_data.to_excel(file_path, index=False)
            QMessageBox.information(self, "Plantilla Creada", f"Plantilla guardada en:\n{file_path}")

