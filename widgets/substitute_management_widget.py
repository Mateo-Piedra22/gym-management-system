from PyQt6.QtWidgets import (
    QWidget, QVBoxLayout, QHBoxLayout, QTableWidget, QTableWidgetItem,
    QPushButton, QLabel, QGroupBox, QFormLayout, QComboBox, QDateEdit,
    QTimeEdit, QTextEdit, QMessageBox, QHeaderView, QSplitter,
    QFrame, QGridLayout, QListWidget, QListWidgetItem, QCheckBox,
    QAbstractItemView, QSizePolicy, QMenu, QScrollArea
)
from PyQt6.QtCore import Qt, QDate, QTime, pyqtSignal
from PyQt6.QtGui import QFont, QColor, QAction
from datetime import datetime, date
from typing import Optional, Dict, List
import time
from utils_modules.async_utils import run_in_background

class SubstituteManagementWidget(QWidget):
    """Widget para gestionar suplencias de profesores"""
    
    suplente_asignado = pyqtSignal(dict)
    suplencia_cancelada = pyqtSignal(dict)
    suplencia_confirmada = pyqtSignal(dict)
    conflicto_detectado = pyqtSignal(dict)
    disponibilidad_actualizada = pyqtSignal()
    
    def __init__(self, db_manager):
        super().__init__()
        self.db_manager = db_manager
        self.selected_substitute_id = None
        # TTL caches para evitar consultas repetidas en poco tiempo
        self._ttl_seconds_classes = 60
        self._ttl_seconds_substitutes = 20
        self._ttl_seconds_prof_schedules = 25
        self._classes_cache = {"data": None, "ts": 0}
        self._substitutes_cache = {"key": None, "data": None, "ts": 0}
        self._prof_schedules_cache = {}
        self.setup_ui()
        self.connect_signals()
        self.load_data()
    
    def setup_ui(self):
        """Configura la interfaz de usuario"""
        layout = QVBoxLayout(self)
        
        # Panel principal dividido
        splitter = QSplitter(Qt.Orientation.Horizontal)
        
        # Panel izquierdo - Lista de suplencias
        left_panel = self.create_substitutes_panel()
        
        # Panel derecho - Gestión y detalles (envuelto en scroll para evitar compresión)
        right_panel = self.create_management_panel()

        splitter.addWidget(left_panel)
        splitter.addWidget(right_panel)
        splitter.setSizes([500, 400])
        
        layout.addWidget(splitter)
    
    def create_substitutes_panel(self):
        """Crea el panel de suplencias"""
        panel = QWidget()
        layout = QVBoxLayout(panel)
        
        # Filtros y búsqueda
        filters_frame = QFrame()
        filters_layout = QHBoxLayout(filters_frame)
        
        self.status_filter = QComboBox()
        self.status_filter.addItems(["Todas", "Pendiente", "Asignado", "Confirmado", "Cancelado"])
        self.status_filter.setSizeAdjustPolicy(QComboBox.SizeAdjustPolicy.AdjustToContents)
        
        self.date_filter = QDateEdit()
        self.date_filter.setDate(QDate.currentDate())
        self.date_filter.setDisplayFormat("yyyy-MM-dd")
        self.date_filter.setCalendarPopup(True)
        
        filters_layout.addWidget(QLabel("Estado:"))
        filters_layout.addWidget(self.status_filter)
        filters_layout.addWidget(QLabel("Fecha:"))
        filters_layout.addWidget(self.date_filter)
        filters_layout.addStretch()
        
        layout.addWidget(filters_frame)
        
        # Tabla de suplencias
        self.substitutes_table = QTableWidget()
        self.substitutes_table.setColumnCount(7)
        self.substitutes_table.setHorizontalHeaderLabels([
            "ID", "Fecha", "Hora", "Ámbito", "Profesor Original", "Suplente", "Estado"
        ])
        
        # Configurar tabla
        header = self.substitutes_table.horizontalHeader()
        header.setSectionResizeMode(QHeaderView.ResizeMode.Stretch)
        header.setStretchLastSection(True)
        header.setMinimumSectionSize(110)
        self.substitutes_table.setSelectionBehavior(QAbstractItemView.SelectionBehavior.SelectRows)
        self.substitutes_table.setAlternatingRowColors(True)
        self.substitutes_table.verticalHeader().setVisible(False)
        self.substitutes_table.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Expanding)
        self.substitutes_table.setMinimumHeight(320)
        
        # Context menu
        self.substitutes_table.setContextMenuPolicy(Qt.ContextMenuPolicy.CustomContextMenu)
        
        layout.addWidget(self.substitutes_table)
        
        # Botones de acción
        buttons_layout = QHBoxLayout()
        
        self.new_substitute_btn = QPushButton("Nueva Suplencia")
        self.edit_substitute_btn = QPushButton("Editar")
        self.cancel_substitute_btn = QPushButton("Cancelar")
        self.refresh_btn = QPushButton("Actualizar")
        
        self.edit_substitute_btn.setEnabled(False)
        self.cancel_substitute_btn.setEnabled(False)
        
        buttons_layout.addWidget(self.new_substitute_btn)
        buttons_layout.addWidget(self.edit_substitute_btn)
        buttons_layout.addWidget(self.cancel_substitute_btn)
        buttons_layout.addStretch()
        buttons_layout.addWidget(self.refresh_btn)
        
        layout.addLayout(buttons_layout)
        
        return panel
    
    def create_management_panel(self):
        """Crea el panel de gestión y lo envuelve en un QScrollArea"""
        content = QWidget()
        content.setMinimumWidth(520)
        layout = QVBoxLayout(content)
        layout.setContentsMargins(12, 12, 12, 12)
        layout.setSpacing(12)

        # Formulario de nueva suplencia
        self.form_group = QGroupBox("Nueva/Editar Suplencia")
        # Evitar que el formulario se comprima/expanda excesivamente
        self.form_group.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Fixed)
        form_layout = QFormLayout(self.form_group)
        form_layout.setHorizontalSpacing(12)
        form_layout.setVerticalSpacing(8)
        form_layout.setFieldGrowthPolicy(QFormLayout.FieldGrowthPolicy.AllNonFixedFieldsGrow)

        # Campos del formulario
        # Selector de modo
        self.mode_selector = QComboBox()
        self.mode_selector.addItems(["General", "Clases"])
        self.current_mode = "General"
        form_layout.addRow("Modo:", self.mode_selector)

        self.original_professor_combo = QComboBox()
        self.original_professor_combo.setSizeAdjustPolicy(QComboBox.SizeAdjustPolicy.AdjustToContents)
        self.original_professor_combo.setMinimumWidth(240)
        self.substitute_professor_combo = QComboBox()
        self.substitute_professor_combo.setSizeAdjustPolicy(QComboBox.SizeAdjustPolicy.AdjustToContents)
        self.substitute_professor_combo.setMinimumWidth(240)
        self.class_combo = QComboBox()
        self.class_combo.setSizeAdjustPolicy(QComboBox.SizeAdjustPolicy.AdjustToContents)
        self.class_combo.setMinimumWidth(240)
        self.class_combo.setEnabled(False)
        # Nuevo: selector de horario del profesor
        self.professor_schedule_combo = QComboBox()
        self.professor_schedule_combo.setSizeAdjustPolicy(QComboBox.SizeAdjustPolicy.AdjustToContents)
        self.professor_schedule_combo.setMinimumWidth(320)
        self.substitute_date = QDateEdit()
        self.substitute_date.setDate(QDate.currentDate())
        self.substitute_date.setDisplayFormat("yyyy-MM-dd")
        self.substitute_date.setCalendarPopup(True)
        
        self.start_time = QTimeEdit()
        self.start_time.setDisplayFormat("HH:mm")
        self.start_time.setTime(QTime(9, 0))
        self.end_time = QTimeEdit()
        self.end_time.setDisplayFormat("HH:mm")
        self.end_time.setTime(QTime(10, 0))
        
        self.reason_edit = QTextEdit()
        self.reason_edit.setMaximumHeight(80)
        
        self.status_combo = QComboBox()
        self.status_combo.addItems(["Pendiente", "Asignado", "Confirmado", "Cancelado"])
        self.status_combo.setSizeAdjustPolicy(QComboBox.SizeAdjustPolicy.AdjustToContents)
        
        form_layout.addRow("Profesor Original:", self.original_professor_combo)
        form_layout.addRow("Profesor Suplente:", self.substitute_professor_combo)
        form_layout.addRow("Ámbito:", self.class_combo)
        form_layout.addRow("Fecha:", self.substitute_date)
        form_layout.addRow("Horario del profesor:", self.professor_schedule_combo)
        # Checkbox para edición manual de horas
        self.manual_time_checkbox = QCheckBox("Editar horario manualmente")
        self.manual_time_checkbox.setChecked(False)
        form_layout.addRow("", self.manual_time_checkbox)
        form_layout.addRow("Hora Inicio:", self.start_time)
        form_layout.addRow("Hora Fin:", self.end_time)
        form_layout.addRow("Motivo:", self.reason_edit)
        form_layout.addRow("Estado:", self.status_combo)
        
        layout.addWidget(self.form_group)
        
        # Botones del formulario
        form_buttons_layout = QHBoxLayout()
        
        self.save_substitute_btn = QPushButton("Guardar")
        self.clear_form_btn = QPushButton("Limpiar")
        
        form_buttons_layout.addWidget(self.save_substitute_btn)
        form_buttons_layout.addWidget(self.clear_form_btn)
        form_buttons_layout.addStretch()
        
        layout.addLayout(form_buttons_layout)
        
        # Información de la suplencia seleccionada
        self.info_group = QGroupBox("Información de Suplencia")
        info_layout = QVBoxLayout(self.info_group)
        
        self.info_label = QLabel("Seleccione una suplencia para ver detalles")
        self.info_label.setWordWrap(True)
        info_layout.addWidget(self.info_label)
        
        layout.addWidget(self.info_group)
        
        # Profesores disponibles
        self.available_group = QGroupBox("Profesores Disponibles")
        available_layout = QVBoxLayout(self.available_group)
        
        self.available_list = QListWidget()
        self.available_list.setMinimumHeight(140)
        available_layout.addWidget(self.available_list)
        
        self.check_availability_btn = QPushButton("Verificar Disponibilidad")
        available_layout.addWidget(self.check_availability_btn)
        
        layout.addWidget(self.available_group)
        
        # Envolver en scroll para mejorar la experiencia en pantallas pequeñas
        scroll = QScrollArea()
        scroll.setWidgetResizable(True)
        scroll.setWidget(content)
        return scroll
    
    def connect_signals(self):
        """Conecta las señales"""
        # Tabla
        self.substitutes_table.itemSelectionChanged.connect(self.on_substitute_selected)
        self.substitutes_table.customContextMenuRequested.connect(self.show_context_menu)
        
        # Botones principales
        self.new_substitute_btn.clicked.connect(self.new_substitute)
        self.edit_substitute_btn.clicked.connect(self.edit_substitute)
        self.cancel_substitute_btn.clicked.connect(self.cancel_substitute)
        self.refresh_btn.clicked.connect(self.load_data)
        
        # Formulario
        self.save_substitute_btn.clicked.connect(self.save_substitute)
        self.clear_form_btn.clicked.connect(self.clear_form)
        
        # Filtros
        self.status_filter.currentTextChanged.connect(self.filter_substitutes)
        self.date_filter.dateChanged.connect(self.filter_substitutes)
        
        # Disponibilidad
        self.check_availability_btn.clicked.connect(self.check_professor_availability)
        
        # Cambios en formulario
        self.substitute_date.dateChanged.connect(self.on_date_changed)
        self.start_time.timeChanged.connect(self.on_time_changed)
        self.end_time.timeChanged.connect(self.on_time_changed)
        # Toggling edición manual
        try:
            self.manual_time_checkbox.toggled.connect(self.on_manual_edit_toggled)
        except Exception:
            pass
        # Cambios de profesor y horario
        self.original_professor_combo.currentIndexChanged.connect(self.on_original_professor_changed)
        self.professor_schedule_combo.currentIndexChanged.connect(self.on_professor_schedule_changed)
        # Cambio de modo
        try:
            self.mode_selector.currentTextChanged.connect(self.on_mode_changed)
        except Exception:
            pass
    
    def load_data(self):
        """Carga todos los datos"""
        self.load_substitutes()
        self.load_professors()
        self.load_classes()

    def set_profesor_id(self, profesor_id: int):
        """Establece el profesor seleccionado para el formulario de suplencia"""
        try:
            self.profesor_id = profesor_id
            # Asegurar que los combos estén poblados
            self.load_professors()
            # Preseleccionar el profesor original en el formulario
            self.set_combo_by_data(self.original_professor_combo, profesor_id)
            # Cargar horarios del profesor
            try:
                self.load_professor_schedules(profesor_id)
            except Exception:
                pass
            # Evitar seleccionar el mismo en suplente
            if self.substitute_professor_combo.currentData() == profesor_id:
                for i in range(self.substitute_professor_combo.count()):
                    if self.substitute_professor_combo.itemData(i) != profesor_id:
                        self.substitute_professor_combo.setCurrentIndex(i)
                        break
        except Exception as e:
            print(f"Error estableciendo profesor en suplencias: {e}")

    def on_mode_changed(self, mode_text: str):
        """Alterna entre modo General y Clases"""
        self.current_mode = mode_text or "General"
        # Ajustar combo de ámbito
        try:
            self.load_classes()
        except Exception:
            pass
        # Recargar horarios según el profesor actual
        try:
            current_prof_id = self.original_professor_combo.currentData()
            if current_prof_id:
                self.load_professor_schedules(current_prof_id)
        except Exception:
            pass
        # Recargar listado
        try:
            self.load_substitutes()
        except Exception:
            pass

    def on_original_professor_changed(self, index: int):
        """Carga horarios cuando cambia el profesor original"""
        try:
            prof_id = self.original_professor_combo.itemData(index)
            if prof_id:
                self.load_professor_schedules(prof_id)
        except Exception as e:
            print(f"Error cargando horarios del profesor: {e}")

    def load_professor_schedules(self, profesor_id: int):
        """Carga el combo de horarios del profesor según modo"""
        try:
            self.professor_schedule_combo.clear()
            mode = getattr(self, 'current_mode', 'General')
            cache_key = f"{mode}:{profesor_id}"
            now = time.time()
            cached = self._prof_schedules_cache.get(cache_key)
            if cached and (now - cached.get('ts', 0)) < self._ttl_seconds_prof_schedules:
                schedules = cached.get('data', [])
                self._populate_professor_schedules_combo(mode, schedules)
            else:
                def _load():
                    if mode == 'General':
                        return self.db_manager.obtener_horarios_disponibilidad_profesor(profesor_id)
                    return self.db_manager.obtener_horarios_profesor(profesor_id)

                def _on_success(schedules):
                    try:
                        self._prof_schedules_cache[cache_key] = {"data": schedules or [], "ts": time.time()}
                        self._populate_professor_schedules_combo(mode, schedules or [])
                    except Exception as e:
                        QMessageBox.warning(self, "Advertencia", f"Error al aplicar horarios: {str(e)}")

                def _on_error(err):
                    QMessageBox.warning(self, "Advertencia", f"Error al cargar horarios del profesor: {str(err)}")

                run_in_background(_load, on_success=_on_success, on_error=_on_error, parent=self, timeout_ms=5000)
            # Guardar todos los ítems para aplicar filtros sin perder datos
            try:
                self._all_schedule_items = []
                for i in range(self.professor_schedule_combo.count()):
                    self._all_schedule_items.append((
                        self.professor_schedule_combo.itemText(i),
                        self.professor_schedule_combo.itemData(i)
                    ))
            except Exception:
                self._all_schedule_items = []
            # Aplicar filtro por día de la fecha seleccionada
            try:
                self._apply_schedule_date_filter()
            except Exception:
                pass
            if self.professor_schedule_combo.count() > 0:
                self.professor_schedule_combo.setCurrentIndex(0)
                self.on_professor_schedule_changed(0)
        except Exception as e:
            print(f"Error cargando horarios del profesor {profesor_id}: {e}")

    def _populate_professor_schedules_combo(self, mode: str, schedules: List[Dict]):
        try:
            for sc in schedules:
                dia = sc.get('dia_semana') or sc.get('dia')
                hi = sc.get('hora_inicio')
                hf = sc.get('hora_fin')
                if mode == 'General':
                    item_text = f"{dia} {hi} - {hf}"
                    self.professor_schedule_combo.addItem(item_text, {
                        'tipo': 'disponibilidad',
                        'horario_profesor_id': sc.get('id'),
                        'dia_semana': dia,
                        'hora_inicio': hi,
                        'hora_fin': hf,
                    })
                else:
                    clase_nombre = sc.get('nombre_clase') or sc.get('clase_nombre')
                    item_text = f"{dia} {hi} - {hf} · {clase_nombre}"
                    self.professor_schedule_combo.addItem(item_text, {
                        'tipo': 'clase',
                        'clase_horario_id': sc.get('id'),
                        'clase_id': sc.get('clase_id'),
                        'dia_semana': dia,
                        'hora_inicio': hi,
                        'hora_fin': hf,
                    })
        except Exception:
            pass

    def on_professor_schedule_changed(self, index: int):
        """Sincroniza Hora Inicio/Fin y clase con el horario elegido"""
        try:
            data = self.professor_schedule_combo.itemData(index)
            if not data:
                return
            qt_start = self._parse_qtime(data.get('hora_inicio'))
            qt_end = self._parse_qtime(data.get('hora_fin'))
            if qt_start and qt_start.isValid():
                self.start_time.setTime(qt_start)
            if qt_end and qt_end.isValid():
                self.end_time.setTime(qt_end)
            # Bloquear edición si no está habilitada la edición manual
            try:
                if hasattr(self, 'manual_time_checkbox') and not self.manual_time_checkbox.isChecked():
                    self.start_time.setReadOnly(True)
                    self.end_time.setReadOnly(True)
                else:
                    self.start_time.setReadOnly(False)
                    self.end_time.setReadOnly(False)
            except Exception:
                pass
            if data.get('tipo') == 'clase' and data.get('clase_id') is not None:
                try:
                    self.set_combo_by_data(self.class_combo, data.get('clase_id'))
                except Exception:
                    pass
        except Exception as e:
            print(f"Error sincronizando horario de profesor: {e}")
    
    def load_substitutes(self):
        """Carga las suplencias"""
        try:
            # Construir clave de caché por modo/filtros
            mode = getattr(self, 'current_mode', 'General')
            selected_status = self.status_filter.currentText()
            selected_qdate = self.date_filter.date()
            date_str = selected_qdate.toString("yyyy-MM-dd") if selected_qdate else ""
            cache_key = f"{mode}|{selected_status}|{date_str}"

            now = time.time()
            if (
                self._substitutes_cache["key"] == cache_key
                and self._substitutes_cache["data"] is not None
                and (now - self._substitutes_cache["ts"]) < self._ttl_seconds_substitutes
            ):
                substitutes = self._substitutes_cache["data"]
                self._populate_substitutes_table(substitutes)
                return

            def _load():
                if mode == "General":
                    base_query = (
                        """
                        SELECT sg.id, sg.fecha, sg.motivo, sg.estado, sg.fecha_creacion,
                               u1.nombre as original_professor,
                               u2.nombre as substitute_professor,
                               'General' as scope_name,
                               sg.hora_inicio, sg.hora_fin
                        FROM profesor_suplencias_generales sg
                        LEFT JOIN profesores p1 ON sg.profesor_original_id = p1.id
                        LEFT JOIN usuarios u1 ON p1.usuario_id = u1.id
                        LEFT JOIN profesores p2 ON sg.profesor_suplente_id = p2.id
                        LEFT JOIN usuarios u2 ON p2.usuario_id = u2.id
                        """
                    )
                else:
                    base_query = (
                        """
                        SELECT ps.id, ps.fecha_clase AS fecha, ps.motivo, ps.estado, ps.fecha_creacion,
                               uo.nombre AS original_professor,
                               us.nombre AS substitute_professor,
                               c.nombre AS scope_name,
                               ch.hora_inicio, ch.hora_fin
                        FROM profesor_suplencias ps
                        JOIN profesor_clase_asignaciones pca ON ps.asignacion_id = pca.id
                        JOIN clases_horarios ch ON pca.clase_horario_id = ch.id
                        JOIN clases c ON ch.clase_id = c.id
                        JOIN profesores p1 ON pca.profesor_id = p1.id
                        JOIN usuarios uo ON p1.usuario_id = uo.id
                        LEFT JOIN profesores p2 ON ps.profesor_suplente_id = p2.id
                        LEFT JOIN usuarios us ON p2.usuario_id = us.id
                        """
                    )

                conditions = []
                params = []
                if selected_status != "Todas":
                    alias = "sg" if mode == "General" else "ps"
                    conditions.append(f"{alias}.estado = %s")
                    params.append(selected_status)
                if selected_qdate:
                    if mode == "General":
                        conditions.append("sg.fecha = %s")
                        params.append(date_str)
                    else:
                        conditions.append("ps.fecha_clase = %s")
                        params.append(date_str)

                query = base_query
                if conditions:
                    query += " WHERE " + " AND ".join(conditions)
                if mode == "General":
                    query += " ORDER BY sg.fecha DESC"
                else:
                    query += " ORDER BY ps.fecha_clase DESC"

                with self.db_manager.get_connection_context() as conn:
                    cursor = conn.cursor()
                    if params:
                        cursor.execute(query, tuple(params))
                    else:
                        cursor.execute(query)
                    return cursor.fetchall()

            def _on_success(substitutes):
                try:
                    self._substitutes_cache = {"key": cache_key, "data": substitutes, "ts": time.time()}
                    self._populate_substitutes_table(substitutes or [])
                except Exception as e:
                    QMessageBox.warning(self, "Advertencia", f"Error al aplicar suplencias: {str(e)}")

            def _on_error(err):
                QMessageBox.warning(self, "Advertencia", f"Error al cargar suplencias: {str(err)}")

            run_in_background(_load, on_success=_on_success, on_error=_on_error, parent=self, timeout_ms=6000)
        except Exception as e:
            print(f"Error cargando suplencias: {e}")
            # Crear tabla si no existe
            self.create_substitutes_table()

    def _populate_substitutes_table(self, substitutes):
        try:
            self.substitutes_table.setRowCount(len(substitutes))
            for row, substitute in enumerate(substitutes):
                id_txt = str(substitute[0]) if substitute and substitute[0] is not None else ""
                fecha_txt = self._fmt_date(substitute[1]) if len(substitute) > 1 else ""
                hora_inicio = self._fmt_time(substitute[8]) if len(substitute) > 8 else None
                hora_fin = self._fmt_time(substitute[9]) if len(substitute) > 9 else None
                horario_txt = f"{hora_inicio} - {hora_fin}" if hora_inicio and hora_fin else "Horario por definir"
                clase_txt = substitute[7] if len(substitute) > 7 and substitute[7] else ("General" if getattr(self, 'current_mode', 'General') == "General" else "Clase")
                orig_txt = substitute[5] if len(substitute) > 5 and substitute[5] else "Sin profesor"
                supl_txt = substitute[6] if len(substitute) > 6 and substitute[6] else "Sin suplente"
                estado_txt = str(substitute[3]) if len(substitute) > 3 and substitute[3] is not None else ""

                self.substitutes_table.setItem(row, 0, QTableWidgetItem(id_txt))
                self.substitutes_table.setItem(row, 1, QTableWidgetItem(fecha_txt))
                self.substitutes_table.setItem(row, 2, QTableWidgetItem(horario_txt))
                self.substitutes_table.setItem(row, 3, QTableWidgetItem(clase_txt))
                self.substitutes_table.setItem(row, 4, QTableWidgetItem(orig_txt))
                self.substitutes_table.setItem(row, 5, QTableWidgetItem(supl_txt))

                status_item = QTableWidgetItem(estado_txt)
                if estado_txt == "Confirmado":
                    status_item.setBackground(QColor(144, 238, 144))
                elif estado_txt == "Cancelado":
                    status_item.setBackground(QColor(255, 182, 193))
                elif estado_txt == "Pendiente":
                    status_item.setBackground(QColor(255, 255, 224))
                elif estado_txt == "Asignado":
                    status_item.setBackground(QColor(173, 216, 230))

                self.substitutes_table.setItem(row, 6, status_item)
        except Exception:
            pass
    
    def load_professors(self):
        """Carga los profesores en los combos"""
        try:
            # Si el circuito está abierto, intentar rellenar con caché y reintentar luego
            try:
                if hasattr(self.db_manager, 'is_circuit_open') and self.db_manager.is_circuit_open():
                    cached = None
                    try:
                        cached = self.db_manager.cache.get('profesores', 'basico_con_ids')
                    except Exception:
                        cached = None
                    self.original_professor_combo.clear()
                    self.substitute_professor_combo.clear()
                    if cached:
                        for prof in cached:
                            nombre = prof.get('nombre', 'Profesor')
                            pid = prof.get('id')
                            self.original_professor_combo.addItem(nombre, pid)
                            self.substitute_professor_combo.addItem(nombre, pid)
                    else:
                        self.original_professor_combo.addItem("Base de datos ocupada; reintentando...", None)
                        self.substitute_professor_combo.addItem("Base de datos ocupada; reintentando...", None)
                    from PyQt6.QtCore import QTimer
                    QTimer.singleShot(1500, self.load_professors)
                    return
            except Exception:
                pass

            # TTL caché: usar si está fresca
            try:
                if (time.time() - self._professors_cache.get("ts", 0)) < self._ttl_seconds_professors and self._professors_cache.get("data"):
                    self.original_professor_combo.clear()
                    self.substitute_professor_combo.clear()
                    for pid, nombre in self._professors_cache["data"]:
                        self.original_professor_combo.addItem(nombre, pid)
                        self.substitute_professor_combo.addItem(nombre, pid)
                    return
            except Exception:
                pass

            def _load():
                # Intento rápido: método optimizado
                professors = []
                try:
                    if hasattr(self.db_manager, 'obtener_profesores_basico_con_ids'):
                        professors = self.db_manager.obtener_profesores_basico_con_ids() or []
                except Exception:
                    professors = []
                # Normalizar a lista de tuplas (id, nombre)
                normalized = []
                if professors:
                    for prof in professors:
                        try:
                            normalized.append((prof.get('id'), prof.get('nombre', 'Profesor')))
                        except Exception:
                            pass
                if normalized:
                    return normalized
                # Fallback directo con timeouts endurecidos
                query = (
                    """
                    SELECT p.id, u.nombre
                    FROM profesores p
                    JOIN usuarios u ON p.usuario_id = u.id
                    WHERE u.activo = true
                    ORDER BY u.nombre
                    """
                )
                with self.db_manager.get_connection_context() as conn:
                    cursor = conn.cursor()
                    try:
                        if hasattr(self.db_manager, '_apply_readonly_timeouts'):
                            self.db_manager._apply_readonly_timeouts(cursor, lock_ms=800, statement_ms=2000, idle_s=2)
                        else:
                            cursor.execute("SET LOCAL lock_timeout = '800ms'")
                            cursor.execute("SET LOCAL statement_timeout = '2000ms'")
                            cursor.execute("SET LOCAL default_transaction_read_only = on")
                    except Exception:
                        pass
                    cursor.execute(query)
                    return cursor.fetchall()

            def _on_success(rows):
                try:
                    self._professors_cache = {"data": rows or [], "ts": time.time()}
                    self.original_professor_combo.clear()
                    self.substitute_professor_combo.clear()
                    for pid, nombre in rows or []:
                        self.original_professor_combo.addItem(nombre, pid)
                        self.substitute_professor_combo.addItem(nombre, pid)
                except Exception as e:
                    QMessageBox.warning(self, "Advertencia", f"Error al aplicar profesores: {str(e)}")

            def _on_error(err):
                QMessageBox.warning(self, "Advertencia", f"Error cargando profesores: {err}")

            run_in_background(
                _load,
                on_success=_on_success,
                on_error=_on_error,
                parent=self,
                timeout_seconds=5,
                description="load_professors"
            )
        except Exception as e:
            print(f"Error cargando profesores: {e}")
    
    def load_classes(self):
        """Carga el combo de Ámbito según modo"""
        try:
            if getattr(self, 'current_mode', 'General') == "General":
                self.class_combo.clear()
                self.class_combo.addItem("General", None)
                self.class_combo.setEnabled(False)
            else:
                self.class_combo.setEnabled(True)
                # Usar caché si está fresca
                now = time.time()
                if self._classes_cache["data"] is not None and (now - self._classes_cache["ts"]) < self._ttl_seconds_classes:
                    classes = self._classes_cache["data"]
                    self.class_combo.clear()
                    for cls in classes:
                        try:
                            name = cls[1]; cid = cls[0]
                        except Exception:
                            name = str(cls)
                            cid = getattr(cls, 'id', None)
                        self.class_combo.addItem(name, cid)
                    return

                def _load():
                    with self.db_manager.get_connection_context() as conn:
                        cursor = conn.cursor()
                        cursor.execute("SELECT id, nombre FROM clases ORDER BY nombre")
                        return cursor.fetchall()

                def _on_success(classes):
                    try:
                        self._classes_cache = {"data": classes, "ts": time.time()}
                        self.class_combo.clear()
                        for cls in classes or []:
                            try:
                                name = cls[1]; cid = cls[0]
                            except Exception:
                                name = str(cls)
                                cid = getattr(cls, 'id', None)
                            self.class_combo.addItem(name, cid)
                    except Exception as e:
                        QMessageBox.warning(self, "Advertencia", f"Error al aplicar clases: {str(e)}")

                def _on_error(err):
                    QMessageBox.warning(self, "Advertencia", f"Error al cargar clases: {str(err)}")

                run_in_background(_load, on_success=_on_success, on_error=_on_error, parent=self, timeout_ms=5000)
        except Exception as e:
            print(f"Error cargando ámbito/clases: {e}")
    
    def create_substitutes_table(self):
        """Crea la tabla de suplencias si no existe - usando tabla existente profesor_suplencias"""
        # La tabla profesor_suplencias ya existe en la base de datos
        # No necesitamos crear una nueva tabla - ya existe profesor_suplencias
        pass
    
    def on_substitute_selected(self):
        """Maneja la selección de una suplencia"""
        current_row = self.substitutes_table.currentRow()
        
        if current_row >= 0:
            self.selected_substitute_id = int(self.substitutes_table.item(current_row, 0).text())
            self.edit_substitute_btn.setEnabled(True)
            self.cancel_substitute_btn.setEnabled(True)
            
            # Cargar información detallada
            self.load_substitute_info()
        else:
            self.selected_substitute_id = None
            self.edit_substitute_btn.setEnabled(False)
            self.cancel_substitute_btn.setEnabled(False)
    
    def load_substitute_info(self):
        """Carga la información detallada de la suplencia seleccionada en background con TTL y timeout"""
        if not self.selected_substitute_id:
            return

        try:
            mode = getattr(self, 'current_mode', 'General')
            cache_key = f"{mode}|{self.selected_substitute_id}"
            # Usar caché si aún está fresca
            try:
                if (
                    self._substitute_info_cache.get("key") == cache_key and
                    (time.time() - self._substitute_info_cache.get("ts", 0)) < self._ttl_seconds_substitute_info and
                    self._substitute_info_cache.get("data") is not None
                ):
                    self._apply_substitute_info_to_form(self._substitute_info_cache.get("data"))
                    return
            except Exception:
                pass

            def _load():
                if mode == "General":
                    query = (
                        """
                        SELECT 
                            sg.id,
                            sg.fecha,
                            sg.motivo,
                            sg.estado,
                            sg.profesor_original_id,
                            sg.profesor_suplente_id,
                            sg.horario_profesor_id,
                            sg.hora_inicio,
                            sg.hora_fin
                        FROM profesor_suplencias_generales sg
                        WHERE sg.id = %s
                        """
                    )
                else:
                    query = (
                        """
                        SELECT 
                            ps.id,
                            ps.fecha_clase,
                            ps.motivo,
                            ps.estado,
                            pca.profesor_id AS profesor_original_id,
                            ps.profesor_suplente_id,
                            ch.id AS clase_horario_id,
                            ch.hora_inicio,
                            ch.hora_fin,
                            ch.clase_id
                        FROM profesor_suplencias ps
                        JOIN profesor_clase_asignaciones pca ON ps.asignacion_id = pca.id
                        JOIN clases_horarios ch ON pca.clase_horario_id = ch.id
                        WHERE ps.id = %s
                        """
                    )
                with self.db_manager.get_connection_context() as conn:
                    cursor = conn.cursor()
                    cursor.execute(query, (self.selected_substitute_id,))
                    return cursor.fetchone()

            def _on_success(substitute):
                try:
                    self._substitute_info_cache = {"key": cache_key, "data": substitute, "ts": time.time()}
                    if substitute:
                        self._apply_substitute_info_to_form(substitute)
                except Exception as e:
                    QMessageBox.warning(self, "Advertencia", f"Error al aplicar detalles: {str(e)}")

            def _on_error(err):
                QMessageBox.critical(self, "Error", f"Error cargando suplencia: {err}")

            run_in_background(
                _load,
                on_success=_on_success,
                on_error=_on_error,
                parent=self,
                timeout_seconds=5,
                description="load_substitute_info"
            )
        except Exception as e:
            QMessageBox.critical(self, "Error", f"Error cargando suplencia: {e}")

    def _apply_substitute_info_to_form(self, substitute):
        """Aplica la información de la suplencia a los controles del formulario"""
        try:
            # indices modo general: 0 id, 1 fecha, 2 motivo, 3 estado, 4 original_prof_id, 5 suplente_id, 6 horario_profesor_id, 7 hora_inicio, 8 hora_fin
            # indices modo clases: 0 id, 1 fecha_clase, 2 motivo, 3 estado, 4 original_prof_id, 5 suplente_id, 6 clase_horario_id, 7 inicio, 8 fin, 9 clase_id
            self.set_combo_by_data(self.original_professor_combo, substitute[4])
            self.set_combo_by_data(self.substitute_professor_combo, substitute[5])
            # ámbito
            if getattr(self, 'current_mode', 'General') == "General":
                if self.class_combo.count() > 0:
                    self.class_combo.setCurrentIndex(0)
            else:
                clase_id = substitute[9] if len(substitute) > 9 else None
                if clase_id is not None:
                    self.set_combo_by_data(self.class_combo, clase_id)

            # Fecha
            if substitute[1]:
                qd = self._parse_qdate(substitute[1])
                if qd and qd.isValid():
                    self.substitute_date.setDate(qd)
            # Horas
            if len(substitute) > 7 and substitute[7]:
                qt_start = self._parse_qtime(substitute[7])
                if qt_start and qt_start.isValid():
                    self.start_time.setTime(qt_start)
            if len(substitute) > 8 and substitute[8]:
                qt_end = self._parse_qtime(substitute[8])
                if qt_end and qt_end.isValid():
                    self.end_time.setTime(qt_end)
            self.reason_edit.setPlainText(substitute[2] or "")
            if substitute[3]:
                self.status_combo.setCurrentText(str(substitute[3]))
        except Exception as e:
            QMessageBox.warning(self, "Advertencia", f"No se pudo aplicar datos: {e}")
    
    def set_combo_by_data(self, combo: QComboBox, data_value):
        """Establece el valor de un combo por su data"""
        if data_value is None:
            return
        for i in range(combo.count()):
            if combo.itemData(i) == data_value:
                combo.setCurrentIndex(i)
                break
    
    def save_substitute(self):
        """Guarda la suplencia"""
        try:
            last_action = "inicio"
            # Validar datos
            if not self.original_professor_combo.currentData():
                QMessageBox.warning(self, "Error", "Debe seleccionar un profesor original")
                return

            if self.start_time.time() >= self.end_time.time():
                QMessageBox.warning(self, "Error", "La hora de inicio debe ser menor que la hora de fin")
                return

            # Recopilar datos
            original_professor_id = self.original_professor_combo.currentData()
            substitute_professor_id = self.substitute_professor_combo.currentData()
            class_id = self.class_combo.currentData()
            qdate = self.substitute_date.date()
            substitute_date_py = qdate.toPyDate()
            start_time = self.start_time.time().toString("hh:mm")
            end_time = self.end_time.time().toString("hh:mm")
            reason = self.reason_edit.toPlainText()
            status = self.status_combo.currentText()
 
            # Crear o actualizar usando métodos del db_manager según modo
            dias_semana = {
                1: 'Lunes', 2: 'Martes', 3: 'Miércoles', 4: 'Jueves',
                5: 'Viernes', 6: 'Sábado', 7: 'Domingo'
            }
            dia_semana = dias_semana[qdate.dayOfWeek()]

            if getattr(self, 'current_mode', 'General') == "General":
                # Preferir el horario seleccionado salvo override manual
                horario_profesor_id = None
                manual_override = getattr(self, 'manual_time_checkbox', None) and self.manual_time_checkbox.isChecked()
                try:
                    if not manual_override:
                        sd = self.professor_schedule_combo.itemData(self.professor_schedule_combo.currentIndex())
                        horario_profesor_id = sd.get('horario_profesor_id') if sd else None
                except Exception:
                    pass
                # Fallback: resolver por consulta si no hay selección válida
                if not horario_profesor_id:
                    try:
                        with self.db_manager.get_connection_context() as conn:
                            cursor = conn.cursor()
                            cursor.execute(
                                """
                                SELECT id FROM horarios_profesores
                                WHERE profesor_id = %s AND dia_semana = %s AND hora_inicio = %s AND hora_fin = %s AND disponible = TRUE
                                LIMIT 1
                                """,
                                (original_professor_id, dia_semana, start_time, end_time)
                            )
                            row = cursor.fetchone()
                            if row:
                                horario_profesor_id = row[0] if isinstance(row, (list, tuple)) else row.get('id')
                    except Exception as e:
                        print(f"Aviso: no se pudo resolver horario_profesor_id: {e}")

                if not horario_profesor_id:
                    if manual_override:
                        QMessageBox.warning(self, "Atención", "Las horas no coinciden con ningún horario de disponibilidad del profesor en ese día.")
                    else:
                        QMessageBox.warning(self, "Atención", "Debe seleccionar un horario del profesor para la suplencia.")
                    return

                if not self.selected_substitute_id:
                    last_action = "crear suplencia (general)"
                    new_id = self.db_manager.crear_suplencia_general(
                        horario_profesor_id=horario_profesor_id,
                        profesor_original_id=original_professor_id,
                        fecha=substitute_date_py,
                        hora_inicio=start_time,
                        hora_fin=end_time,
                        motivo=reason,
                        profesor_suplente_id=substitute_professor_id if substitute_professor_id else None,
                        notas=None
                    )
                    if not new_id:
                        raise Exception("La creación de la suplencia retornó un ID inválido")
                    self.selected_substitute_id = new_id

                    if status == "Asignado":
                        if not substitute_professor_id:
                            QMessageBox.warning(self, "Atención", "Para marcar como Asignado debe seleccionar un profesor suplente.")
                        else:
                            last_action = "asignar suplente (general)"
                            self.db_manager.asignar_suplente_general(new_id, substitute_professor_id, notas=reason)
                    elif status == "Confirmado":
                        if not substitute_professor_id:
                            QMessageBox.warning(self, "Atención", "Para confirmar, primero asigne un profesor suplente.")
                        else:
                            last_action = "asignar suplente (general)"
                            self.db_manager.asignar_suplente_general(new_id, substitute_professor_id, notas=reason)
                            last_action = "confirmar suplencia (general)"
                            self.db_manager.confirmar_suplencia_general(new_id)
                    elif status == "Cancelado":
                        last_action = "cancelar suplencia (general)"
                        self.db_manager.cancelar_suplencia_general(new_id, motivo=reason)
                else:
                    if status == "Asignado":
                        if not substitute_professor_id:
                            QMessageBox.warning(self, "Atención", "Para marcar como Asignado debe seleccionar un profesor suplente.")
                            return
                        last_action = "asignar suplente (general)"
                        self.db_manager.asignar_suplente_general(self.selected_substitute_id, substitute_professor_id, notas=reason)
                    elif status == "Confirmado":
                        if substitute_professor_id:
                            last_action = "asignar suplente (general)"
                            self.db_manager.asignar_suplente_general(self.selected_substitute_id, substitute_professor_id, notas=reason)
                        last_action = "confirmar suplencia (general)"
                        self.db_manager.confirmar_suplencia_general(self.selected_substitute_id)
                    elif status == "Cancelado":
                        last_action = "cancelar suplencia (general)"
                        self.db_manager.cancelar_suplencia_general(self.selected_substitute_id, motivo=reason)
                    else:
                        QMessageBox.information(self, "Info", "No hay cambios de estado aplicables.")
            else:
                # Preferir el horario de clase seleccionado salvo override manual
                clase_horario_id = None
                manual_override = getattr(self, 'manual_time_checkbox', None) and self.manual_time_checkbox.isChecked()
                try:
                    if not manual_override:
                        sd = self.professor_schedule_combo.itemData(self.professor_schedule_combo.currentIndex())
                        clase_horario_id = sd.get('clase_horario_id') if sd else None
                except Exception:
                    pass
                # Fallback: resolver por consulta si no hay selección válida
                if not clase_horario_id:
                    try:
                        with self.db_manager.get_connection_context() as conn:
                            cursor = conn.cursor()
                            cursor.execute(
                                """
                                SELECT id FROM clases_horarios
                                WHERE clase_id = %s AND dia_semana = %s AND hora_inicio = %s AND hora_fin = %s
                                LIMIT 1
                                """,
                                (class_id, dia_semana, start_time, end_time)
                            )
                            row = cursor.fetchone()
                            if row:
                                clase_horario_id = row[0]
                    except Exception as e:
                        print(f"Aviso: no se pudo resolver clase_horario_id: {e}")

                if not clase_horario_id:
                    if manual_override:
                        QMessageBox.warning(self, "Atención", "Las horas no coinciden con ningún horario de clase del profesor en ese día.")
                    else:
                        QMessageBox.warning(self, "Atención", "Debe seleccionar un horario de clase del profesor para la suplencia.")
                    return

                if not self.selected_substitute_id:
                    last_action = "crear suplencia (clase)"
                    new_id = self.db_manager.crear_suplencia(
                        clase_horario_id=clase_horario_id,
                        profesor_original_id=original_professor_id,
                        fecha_clase=substitute_date_py,
                        motivo=reason,
                        profesor_suplente_id=substitute_professor_id if substitute_professor_id else None,
                        notas=None
                    )
                    if not new_id:
                        raise Exception("La creación de la suplencia (clase) retornó un ID inválido")
                    self.selected_substitute_id = new_id

                    if status == "Asignado":
                        if not substitute_professor_id:
                            QMessageBox.warning(self, "Atención", "Para marcar como Asignado debe seleccionar un profesor suplente.")
                        else:
                            last_action = "asignar suplente (clase)"
                            self.db_manager.asignar_suplente(new_id, substitute_professor_id, notas=reason)
                    elif status == "Confirmado":
                        if not substitute_professor_id:
                            QMessageBox.warning(self, "Atención", "Para confirmar, primero asigne un profesor suplente.")
                        else:
                            last_action = "asignar suplente (clase)"
                            self.db_manager.asignar_suplente(new_id, substitute_professor_id, notas=reason)
                            last_action = "confirmar suplencia (clase)"
                            self.db_manager.confirmar_suplencia(new_id)
                    elif status == "Cancelado":
                        last_action = "cancelar suplencia (clase)"
                        self.db_manager.cancelar_suplencia(new_id, motivo=reason)
                else:
                    if status == "Asignado":
                        if not substitute_professor_id:
                            QMessageBox.warning(self, "Atención", "Para marcar como Asignado debe seleccionar un profesor suplente.")
                            return
                        last_action = "asignar suplente (clase)"
                        self.db_manager.asignar_suplente(self.selected_substitute_id, substitute_professor_id, notas=reason)
                    elif status == "Confirmado":
                        if substitute_professor_id:
                            last_action = "asignar suplente (clase)"
                            self.db_manager.asignar_suplente(self.selected_substitute_id, substitute_professor_id, notas=reason)
                        last_action = "confirmar suplencia (clase)"
                        self.db_manager.confirmar_suplencia(self.selected_substitute_id)
                    elif status == "Cancelado":
                        last_action = "cancelar suplencia (clase)"
                        self.db_manager.cancelar_suplencia(self.selected_substitute_id, motivo=reason)
                    else:
                        QMessageBox.information(self, "Info", "No hay cambios de estado aplicables.")

            QMessageBox.information(self, "Éxito", "Suplencia guardada correctamente")
 
            # Emitir señal
            substitute_data = {
                'substitute_name': self.substitute_professor_combo.currentText(),
                'class_name': self.class_combo.currentText(),
                'date': self.substitute_date.date().toString("yyyy-MM-dd"),
                'time': f"{start_time} - {end_time}"
            }
            self.suplente_asignado.emit(substitute_data)
 
            self.load_data()
            self.clear_form()
 
        except Exception as e:
            try:
                import traceback
                tb = traceback.format_exc()
                print(f"Error guardando suplencia ({last_action}): {e}\n{tb}")
            except Exception:
                pass
            QMessageBox.critical(self, "Error", f"Error guardando suplencia durante {last_action}: {e}")
    
    def cancel_substitute(self):
        """Cancela la suplencia seleccionada"""
        if not self.selected_substitute_id:
            return

        reply = QMessageBox.question(
            self, "Confirmar",
            "¿Está seguro de cancelar esta suplencia?",
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No
        )

        if reply == QMessageBox.StandardButton.Yes:
            try:
                ok = self.db_manager.cancelar_suplencia(self.selected_substitute_id, motivo=None)
                if not ok:
                    raise Exception("No se pudo cancelar la suplencia")

                QMessageBox.information(self, "Éxito", "Suplencia cancelada")

                # Emitir señal
                substitute_data = {'id': self.selected_substitute_id}
                self.suplencia_cancelada.emit(substitute_data)

                self.load_data()

            except Exception as e:
                QMessageBox.critical(self, "Error", f"Error cancelando suplencia: {e}")
    
    def clear_form(self):
        """Limpia el formulario"""
        self.original_professor_combo.setCurrentIndex(0)
        self.substitute_professor_combo.setCurrentIndex(0)
        self.class_combo.setCurrentIndex(0)
        self.substitute_date.setDate(QDate.currentDate())
        self.start_time.setTime(QTime(9, 0))
        self.end_time.setTime(QTime(10, 0))
        self.reason_edit.clear()
        self.status_combo.setCurrentIndex(0)
        self.selected_substitute_id = None

    def new_substitute(self):
        """Prepara el formulario para crear una nueva suplencia"""
        # Limpiar selección en la tabla y formulario
        try:
            self.substitutes_table.clearSelection()
        except Exception:
            pass
        self.clear_form()
        # Ajustar títulos/estado de botones
        try:
            self.form_group.setTitle("Nueva/Editar Suplencia")
            self.info_label.setText("Creando nueva suplencia. Complete el formulario y presione Guardar.")
        except Exception:
            pass
        self.edit_substitute_btn.setEnabled(False)
        self.cancel_substitute_btn.setEnabled(False)
        # Enfocar primer control
        self.original_professor_combo.setFocus()

    def edit_substitute(self):
        """Carga en el formulario la suplencia seleccionada para su edición"""
        if not self.selected_substitute_id:
            QMessageBox.warning(self, "Atención", "Seleccione una suplencia de la lista para editar.")
            return
        # Asegurar que los datos estén cargados en el formulario
        try:
            self.load_substitute_info()
        except Exception:
            pass
        # Ajustar títulos/estado de botones
        try:
            self.form_group.setTitle(f"Nueva/Editar Suplencia")
            self.info_label.setText(f"Editando suplencia #{self.selected_substitute_id}. Modifique los campos y presione Guardar.")
        except Exception:
            pass
        self.edit_substitute_btn.setEnabled(True)
        self.cancel_substitute_btn.setEnabled(True)
        # Enfocar un control del formulario para comodidad
        self.substitute_professor_combo.setFocus()
    
    def filter_substitutes(self):
        """Filtra las suplencias según los criterios seleccionados"""
        # Implementar filtrado por estado y fecha
        self.load_data()
    
    def check_professor_availability(self):
        """Verifica la disponibilidad de profesores para la fecha y hora seleccionadas"""
        self.available_list.clear()

        try:
            selected_date_py = self.substitute_date.date().toPyDate()
            start_time = self.start_time.time().toString("hh:mm")
            end_time = self.end_time.time().toString("hh:mm")

            original_prof_id = self.original_professor_combo.currentData()

            disponibles = []
            for i in range(self.substitute_professor_combo.count()):
                prof_id = self.substitute_professor_combo.itemData(i)
                prof_name = self.substitute_professor_combo.itemText(i)
                if not prof_id or prof_id == original_prof_id:
                    continue
                try:
                    info = self.db_manager.verificar_disponibilidad_profesor_fecha(
                        profesor_id=prof_id,
                        fecha=selected_date_py,
                        hora_inicio=start_time,
                        hora_fin=end_time,
                    )
                    if info.get('disponible', False) and len(info.get('conflictos', [])) == 0:
                        disponibles.append((prof_id, prof_name))
                except Exception as e:
                    print(f"Error verificando disponibilidad de profesor {prof_id}: {e}")

            if disponibles:
                for prof_id, prof_name in disponibles:
                    item = QListWidgetItem(f"{prof_name} (ID: {prof_id})")
                    item.setData(Qt.ItemDataRole.UserRole, prof_id)
                    self.available_list.addItem(item)
            else:
                item = QListWidgetItem("No hay profesores disponibles")
                item.setFlags(Qt.ItemFlag.NoItemFlags)
                self.available_list.addItem(item)

        except Exception as e:
            print(f"Error verificando disponibilidad: {e}")
    
    def on_date_changed(self):
        """Maneja el cambio de fecha"""
        self.available_list.clear()
        try:
            self._apply_schedule_date_filter()
        except Exception:
            pass
    
    def on_time_changed(self):
        """Maneja el cambio de hora"""
        self.available_list.clear()
        # Restaurar hora desde horario si la edición manual no está activa
        try:
            if hasattr(self, 'manual_time_checkbox') and not self.manual_time_checkbox.isChecked():
                idx = self.professor_schedule_combo.currentIndex()
                data = self.professor_schedule_combo.itemData(idx)
                if data:
                    qt_start = self._parse_qtime(data.get('hora_inicio'))
                    qt_end = self._parse_qtime(data.get('hora_fin'))
                    if qt_start and qt_start.isValid():
                        self.start_time.setTime(qt_start)
                    if qt_end and qt_end.isValid():
                        self.end_time.setTime(qt_end)
        except Exception:
            pass
    
    def show_context_menu(self, position):
        """Muestra el menú contextual"""
        if self.substitutes_table.itemAt(position):
            menu = QMenu(self)
            
            edit_action = QAction("Editar", self)
            edit_action.triggered.connect(self.edit_substitute)
            menu.addAction(edit_action)
            
            cancel_action = QAction("Cancelar", self)
            cancel_action.triggered.connect(self.cancel_substitute)
            menu.addAction(cancel_action)
            
            menu.addSeparator()
            
            confirm_action = QAction("Confirmar", self)
            confirm_action.triggered.connect(self.confirm_substitute)
            menu.addAction(confirm_action)
            
            menu.exec(self.substitutes_table.mapToGlobal(position))
    
    def confirm_substitute(self):
        """Confirma la suplencia seleccionada"""
        if not self.selected_substitute_id:
            return

        try:
            ok = self.db_manager.confirmar_suplencia(self.selected_substitute_id)
            if not ok:
                raise Exception("No se pudo confirmar la suplencia")

            QMessageBox.information(self, "Éxito", "Suplencia confirmada")

            # Emitir señal
            substitute_data = {'id': self.selected_substitute_id}
            self.suplencia_confirmada.emit(substitute_data)

            self.load_data()

        except Exception as e:
            QMessageBox.critical(self, "Error", f"Error confirmando suplencia: {e}")

    # --- Utilidades internas de formateo/parsing ---
    def _fmt_date(self, value) -> str:
        if value is None:
            return ""
        if isinstance(value, datetime):
            return value.strftime("%Y-%m-%d")
        if isinstance(value, date):
            return value.strftime("%Y-%m-%d")
        return str(value)

    def _fmt_time(self, value) -> Optional[str]:
        if value is None:
            return None
        if isinstance(value, (datetime,)):
            return value.strftime("%H:%M")
        # Si viene como 'HH:MM:SS', recortar segundos
        s = str(value)
        if len(s) >= 5 and s[2] == ':':
            return s[:5]
        return s

    def _parse_qdate(self, value) -> Optional[QDate]:
        try:
            if isinstance(value, date):
                return QDate(value.year, value.month, value.day)
            if isinstance(value, datetime):
                return QDate(value.year, value.month, value.day)
            # String
            qd = QDate.fromString(str(value), "yyyy-MM-dd")
            if not qd.isValid():
                # Intentar otros formatos comunes
                for fmt in ("dd/MM/yyyy", "MM/dd/yyyy"):
                    qd = QDate.fromString(str(value), fmt)
                    if qd.isValid():
                        break
            return qd
        except Exception:
            return None

    def _parse_qtime(self, value) -> Optional[QTime]:
        try:
            if isinstance(value, datetime):
                return QTime(value.hour, value.minute)
            s = str(value)
            qt = QTime.fromString(s, "HH:mm:ss")
            if not qt.isValid():
                qt = QTime.fromString(s, "HH:mm")
            if not qt.isValid():
                qt = QTime.fromString(s, "hh:mm:ss")
            if not qt.isValid():
                qt = QTime.fromString(s, "hh:mm")
            return qt
        except Exception:
            return None

    # --- Auxiliares de filtrado y edición ---
    def _get_day_name_from_qdate(self, qdate: QDate) -> str:
        try:
            dias_semana = {
                1: 'Lunes', 2: 'Martes', 3: 'Miércoles', 4: 'Jueves',
                5: 'Viernes', 6: 'Sábado', 7: 'Domingo'
            }
            return dias_semana.get(qdate.dayOfWeek(), '')
        except Exception:
            return ''

    def _apply_schedule_date_filter(self):
        """Filtra el combo de horarios según el día de la fecha seleccionada"""
        try:
            dia = self._get_day_name_from_qdate(self.substitute_date.date())
            if not dia:
                return
            source_items: List[tuple] = getattr(self, '_all_schedule_items', [])
            items: List[tuple] = []
            for text, data in source_items:
                if not data:
                    continue
                if (data.get('dia_semana') or '').strip() == dia:
                    items.append((text, data))
            self.professor_schedule_combo.blockSignals(True)
            self.professor_schedule_combo.clear()
            for text, data in items:
                self.professor_schedule_combo.addItem(text, data)
            self.professor_schedule_combo.blockSignals(False)
            if self.professor_schedule_combo.count() > 0:
                self.professor_schedule_combo.setCurrentIndex(0)
                self.on_professor_schedule_changed(0)
        except Exception:
            pass

    def on_manual_edit_toggled(self, checked: bool):
        """Activa o desactiva la edición manual de horas"""
        try:
            self.start_time.setReadOnly(not checked)
            self.end_time.setReadOnly(not checked)
            if not checked:
                # Reaplicar el horario seleccionado
                idx = self.professor_schedule_combo.currentIndex()
                self.on_professor_schedule_changed(idx)
        except Exception:
            pass