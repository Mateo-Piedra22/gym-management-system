from PyQt6.QtWidgets import (
    QWidget, QVBoxLayout, QHBoxLayout, QGroupBox, QGridLayout, # Cambio aquí
    QComboBox, QPushButton, QListWidget, QListWidgetItem, QTableWidget, QTableWidgetItem,
    QHeaderView, QLabel, QMessageBox, QAbstractItemView, QSplitter, QLineEdit
)
from PyQt6.QtCore import Qt
import logging
import time
from config import ENABLE_WAITLIST_PROMPT
from datetime import datetime, timedelta
from typing import Optional
from database import DatabaseManager
from export_manager import ExportManager
from models import Clase, ClaseHorario, Usuario
from widgets.schedule_editor_dialog import ScheduleEditorDialog
from widgets.user_selection_dialog import UserSelectionDialog
from utils_modules.async_runner import TaskThread
from utils_modules.async_utils import run_in_background
from widgets.class_editor_dialog import ClassEditorDialog
from widgets.unified_filter_widget import UnifiedFilterButton

class ClassesTabWidget(QWidget):
    # ... (__init__ sin cambios)
    def __init__(self, db_manager: DatabaseManager, user_role: str):
        super().__init__()
        self.db_manager = db_manager; self.user_role = user_role; self.profesores = []
        self.export_manager = ExportManager(db_manager)
        self.selected_class_id: Optional[int] = None; self.selected_schedule: Optional[ClaseHorario] = None
        self.setup_ui()
    def setup_ui(self):
        main_layout = QHBoxLayout(self)
        left_splitter = QSplitter(Qt.Orientation.Vertical); left_splitter.setFixedWidth(450)
        self.class_type_group = QGroupBox("Gestión de Tipos de Clase")
        class_type_layout = QVBoxLayout(self.class_type_group)
        self.class_list_widget = QListWidget(); self.class_list_widget.setAlternatingRowColors(True); self.class_list_widget.setObjectName("panel_control")
        ct_buttons_layout = QHBoxLayout(); self.add_class_button = QPushButton("➕ Crear"); self.edit_class_button = QPushButton("✏️ Editar"); self.delete_class_button = QPushButton("🗑️ Eliminar")
        ct_buttons_layout.addWidget(self.add_class_button); ct_buttons_layout.addWidget(self.edit_class_button); ct_buttons_layout.addWidget(self.delete_class_button)
        class_type_layout.addWidget(self.class_list_widget); class_type_layout.addLayout(ct_buttons_layout)
        self.class_type_group.setVisible(self.user_role in ['dueño', 'profesor'])
        
        class_selection_group = QGroupBox("Selección de Clase y Horario")
        # --- CAMBIO A QGRIDLAYOUT ---
        selection_grid_layout = QGridLayout(class_selection_group)
        self.class_combo = QComboBox(); self.class_combo.setPlaceholderText("Seleccione una clase..."); self.class_combo.setObjectName("panel_control")
        self.schedule_list = QListWidget(); self.schedule_list.setAlternatingRowColors(True); self.schedule_list.setObjectName("panel_control")
        schedule_buttons_layout = QHBoxLayout(); self.add_schedule_button = QPushButton("➕ Añadir Horario"); self.edit_schedule_button = QPushButton("✏️ Editar Horario"); self.delete_schedule_button = QPushButton("🗑️ Eliminar Horario")
        schedule_buttons_layout.addWidget(self.add_schedule_button); schedule_buttons_layout.addWidget(self.edit_schedule_button); schedule_buttons_layout.addWidget(self.delete_schedule_button)
        
        # --- Asignación a las celdas del grid ---
        clase_label = QLabel("Clase:"); clase_label.setProperty("class", "panel_label")
        horarios_label = QLabel("Horarios:"); horarios_label.setProperty("class", "panel_label")
        selection_grid_layout.addWidget(clase_label, 0, 0); selection_grid_layout.addWidget(self.class_combo, 0, 1)
        selection_grid_layout.addWidget(horarios_label, 1, 0, Qt.AlignmentFlag.AlignTop); selection_grid_layout.addWidget(self.schedule_list, 1, 1)
        selection_grid_layout.addLayout(schedule_buttons_layout, 2, 1)
        selection_grid_layout.setColumnStretch(1, 1)

        left_splitter.addWidget(self.class_type_group); left_splitter.addWidget(class_selection_group)
        # Proporciones optimizadas para ventana maximizada: tipos y selección
        left_splitter.setSizes([300, 700]); main_layout.addWidget(left_splitter)
        
        right_panel = QWidget(); right_layout = QVBoxLayout(right_panel)
        inscriptions_group = QGroupBox(); inscriptions_layout = QVBoxLayout(inscriptions_group)
        
        # Crear layout horizontal para el título y la barra de búsqueda
        header_layout = QHBoxLayout()
        inscriptions_title = QLabel("Alumnos Inscriptos")
        inscriptions_title.setProperty("class", "panel_label")
        # Barra de búsqueda en reemplazo del botón de filtros
        self.inscription_search = QLineEdit()
        self.inscription_search.setObjectName("inscription_search")
        self.inscription_search.setPlaceholderText("Buscar alumno por nombre o ID...")
        self.inscription_search.textChanged.connect(self.on_inscription_search_text_changed)

        header_layout.addWidget(inscriptions_title)
        header_layout.addStretch()
        header_layout.addWidget(self.inscription_search)
        
        inscriptions_layout.addLayout(header_layout)
        
        self.inscriptions_table = QTableWidget(); self.inscriptions_table.setObjectName("detail_table")
        self.inscriptions_table.setColumnCount(2); self.inscriptions_table.setHorizontalHeaderLabels(["ID Alumno", "Nombre"]); self.inscriptions_table.horizontalHeader().setSectionResizeMode(1, QHeaderView.ResizeMode.Stretch); self.inscriptions_table.setSelectionBehavior(QAbstractItemView.SelectionBehavior.SelectRows)
        inscription_buttons_layout = QHBoxLayout(); self.add_user_button = QPushButton("➕ Inscribir Alumno"); self.remove_user_button = QPushButton("🗑️ Quitar Alumno")
        self.register_attendance_button = QPushButton("Registrar Asistencia")
        inscription_buttons_layout.addStretch(); inscription_buttons_layout.addWidget(self.add_user_button); inscription_buttons_layout.addWidget(self.remove_user_button); inscription_buttons_layout.addWidget(self.register_attendance_button)
        inscriptions_layout.addWidget(self.inscriptions_table); inscriptions_layout.addLayout(inscription_buttons_layout)
        
        # Grupo de Lista de Espera
        waiting_list_group = QGroupBox("")
        waiting_list_layout = QVBoxLayout(waiting_list_group)
        
        # Header para lista de espera
        waiting_header_layout = QHBoxLayout()
        waiting_title = QLabel("")
        waiting_title.setProperty("class", "panel_label")
        # Barra de búsqueda en reemplazo del botón de filtros
        self.waiting_search = QLineEdit()
        self.waiting_search.setObjectName("waiting_search")
        self.waiting_search.setPlaceholderText("Buscar en lista de espera por nombre o ID...")
        self.waiting_search.textChanged.connect(self.on_waiting_search_text_changed)

        waiting_header_layout.addWidget(waiting_title)
        waiting_header_layout.addStretch()
        waiting_header_layout.addWidget(self.waiting_search)
        
        waiting_list_layout.addLayout(waiting_header_layout)
        
        # Tabla de lista de espera
        self.waiting_list_table = QTableWidget()
        self.waiting_list_table.setObjectName("detail_table")
        self.waiting_list_table.setColumnCount(3)
        self.waiting_list_table.setHorizontalHeaderLabels(["ID Alumno", "Nombre", "Posición"])
        self.waiting_list_table.horizontalHeader().setSectionResizeMode(1, QHeaderView.ResizeMode.Stretch)
        self.waiting_list_table.setSelectionBehavior(QAbstractItemView.SelectionBehavior.SelectRows)
        
        # Botones para lista de espera
        waiting_buttons_layout = QHBoxLayout()
        self.remove_from_waiting_button = QPushButton("🗑️ Quitar de Lista")
        self.promote_from_waiting_button = QPushButton("⬆️ Promover a Clase")
        
        waiting_buttons_layout.addStretch()
        waiting_buttons_layout.addWidget(self.promote_from_waiting_button)
        waiting_buttons_layout.addWidget(self.remove_from_waiting_button)
        
        waiting_list_layout.addWidget(self.waiting_list_table)
        waiting_list_layout.addLayout(waiting_buttons_layout)
        
        right_layout.addWidget(inscriptions_group)
        right_layout.addWidget(waiting_list_group)
        main_layout.addWidget(right_panel, 1)
        self.connect_signals(); self.set_initial_state()
    # ... (el resto de la clase permanece igual)
    def set_initial_state(self):
        is_admin = self.user_role in ['dueño', 'profesor']; self.add_schedule_button.setEnabled(False); self.edit_schedule_button.setEnabled(False); self.delete_schedule_button.setEnabled(False)
        self.add_user_button.setEnabled(False); self.remove_user_button.setEnabled(False); self.register_attendance_button.setEnabled(False); self.edit_class_button.setEnabled(False); self.delete_class_button.setEnabled(False)
        self.remove_from_waiting_button.setEnabled(False); self.promote_from_waiting_button.setEnabled(False)
        self.add_schedule_button.setVisible(is_admin); self.edit_schedule_button.setVisible(is_admin); self.delete_schedule_button.setVisible(is_admin)
    def connect_signals(self):
        self.class_combo.currentIndexChanged.connect(self.on_class_selected_in_combo); self.schedule_list.currentItemChanged.connect(self.on_schedule_selected)
        self.add_schedule_button.clicked.connect(self.add_schedule); self.edit_schedule_button.clicked.connect(self.edit_schedule); self.delete_schedule_button.clicked.connect(self.delete_schedule)
        self.add_user_button.clicked.connect(self.add_user_to_class); self.remove_user_button.clicked.connect(self.remove_user_from_class); self.register_attendance_button.clicked.connect(self.register_class_attendance)
        self.remove_from_waiting_button.clicked.connect(self.remove_user_from_waiting_list); self.promote_from_waiting_button.clicked.connect(self.promote_user_from_waiting_list)
        self.class_list_widget.currentItemChanged.connect(self.on_class_type_selected_in_list)
        self.add_class_button.clicked.connect(self.add_class_type); self.edit_class_button.clicked.connect(self.edit_class_type); self.delete_class_button.clicked.connect(self.delete_class_type)
        # Los filtros de lista de espera son manejados por UnifiedFilterButton
        # No necesitan conexiones directas aquí
    def load_initial_data(self):
        """Carga inicial de profesores y clases sin bloquear la UI."""
        try:
            def _load():
                profesores = []
                clases = []
                try:
                    profesores = self.db_manager.obtener_profesores()
                except Exception:
                    profesores = []
                try:
                    clases = self.db_manager.obtener_clases()
                except Exception:
                    clases = []
                return {"profesores": profesores, "clases": clases}

            def _on_success(data):
                try:
                    self.profesores = (data or {}).get("profesores", [])
                    self._populate_class_types((data or {}).get("clases", []))
                except Exception:
                    QMessageBox.warning(self, "Advertencia", "Error aplicando datos iniciales de clases.")

            def _on_error(err):
                try:
                    QMessageBox.warning(self, "Advertencia", f"No se pudo cargar datos iniciales: {err}")
                except Exception:
                    pass
                # Mostrar UI vacía pero estable
                self._populate_class_types([])

            run_in_background(
                _load,
                on_success=_on_success,
                on_error=_on_error,
                parent=self,
                timeout_ms=2000,
                description="Cargar datos iniciales de clases",
            )
        except Exception:
            # Fallback sincrónico seguro
            try:
                self.profesores = self.db_manager.obtener_profesores()
            except Exception:
                self.profesores = []
            try:
                self._populate_class_types(self.db_manager.obtener_clases())
            except Exception:
                self._populate_class_types([])
    def load_class_types(self):
        """Carga tipos de clase en segundo plano y actualiza UI."""
        self.class_combo.blockSignals(True); self.class_list_widget.blockSignals(True)
        try:
            current_class_id = None
            try:
                current = self.class_combo.currentData()
                current_class_id = getattr(current, 'id', None) if current else None
            except Exception:
                current_class_id = None

            self.class_combo.clear(); self.class_list_widget.clear(); self.class_combo.addItem("Seleccione una clase...", userData=None)

            def _load():
                try:
                    return self.db_manager.obtener_clases()
                except Exception:
                    return []

            def _on_success(clases):
                try:
                    self._populate_class_types(clases, current_class_id=current_class_id)
                finally:
                    self.class_combo.blockSignals(False); self.class_list_widget.blockSignals(False)
                    self.on_class_selected_in_combo(self.class_combo.currentIndex())

            def _on_error(err):
                try:
                    QMessageBox.critical(self, "Error", f"No se pudieron cargar las clases: {err}")
                except Exception:
                    pass
                try:
                    self._populate_class_types([])
                finally:
                    self.class_combo.blockSignals(False); self.class_list_widget.blockSignals(False)
                    self.on_class_selected_in_combo(self.class_combo.currentIndex())

            run_in_background(
                _load,
                on_success=_on_success,
                on_error=_on_error,
                parent=self,
                timeout_ms=2000,
                description="Cargar tipos de clase",
            )
        except Exception as e:
            try:
                QMessageBox.critical(self, "Error", f"No se pudieron cargar las clases: {e}")
            except Exception:
                pass
            try:
                self.class_combo.blockSignals(False); self.class_list_widget.blockSignals(False)
            except Exception:
                pass
            self.on_class_selected_in_combo(self.class_combo.currentIndex())

    def _populate_class_types(self, clases, current_class_id: Optional[int] = None):
        """Rellena combo y lista de tipos de clase."""
        try:
            self.class_combo.clear(); self.class_list_widget.clear(); self.class_combo.addItem("Seleccione una clase...", userData=None)
            for c in (clases or []):
                try:
                    self.class_combo.addItem(c.nombre, userData=c)
                    item = QListWidgetItem(c.nombre); item.setData(Qt.ItemDataRole.UserRole, c); self.class_list_widget.addItem(item)
                    if current_class_id and getattr(c, 'id', None) == current_class_id:
                        self.class_combo.setCurrentText(c.nombre)
                except Exception:
                    # Fallback para objetos parciales
                    nombre = getattr(c, 'nombre', str(c))
                    self.class_combo.addItem(nombre, userData=c)
                    item = QListWidgetItem(nombre); item.setData(Qt.ItemDataRole.UserRole, c); self.class_list_widget.addItem(item)
        except Exception:
            pass
    def on_class_selected_in_combo(self, index):
        selected_data = self.class_combo.itemData(index); self.schedule_list.clear(); self.inscriptions_table.setRowCount(0)
        is_admin = self.user_role in ['dueño', 'profesor']; self.add_schedule_button.setEnabled(bool(selected_data) and is_admin); self.edit_schedule_button.setEnabled(False); self.delete_schedule_button.setEnabled(False)
        self.add_user_button.setEnabled(False); self.remove_user_button.setEnabled(False)
        if not selected_data: self.selected_class_id = None; self.selected_schedule = None; return
        self.selected_class_id = selected_data.id; self.load_schedules_for_class(self.selected_class_id)
    def on_class_type_selected_in_list(self, current_item, previous_item):
        has_selection = current_item is not None; self.edit_class_button.setEnabled(has_selection); self.delete_class_button.setEnabled(has_selection)
    def add_class_type(self):
        dialog = ClassEditorDialog(self, self.db_manager, self.export_manager)
        if dialog.exec(): self.load_class_types()
    def edit_class_type(self):
        current_item = self.class_list_widget.currentItem()
        if not current_item: return
        clase_a_editar = current_item.data(Qt.ItemDataRole.UserRole)
        dialog = ClassEditorDialog(self, self.db_manager, self.export_manager, clase=clase_a_editar)
        if dialog.exec(): self.load_class_types()
    def delete_class_type(self):
        current_item = self.class_list_widget.currentItem()
        if not current_item: return
        clase_a_eliminar = current_item.data(Qt.ItemDataRole.UserRole)
        if QMessageBox.question(self, "Confirmar Eliminación", f"¿Seguro que desea eliminar la clase '{clase_a_eliminar.nombre}'?\nTODOS sus horarios y alumnos inscritos serán eliminados.", QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No) == QMessageBox.StandardButton.Yes:
            try: self.db_manager.eliminar_clase(clase_a_eliminar.id); self.load_class_types()
            except Exception as e: QMessageBox.critical(self, "Error", f"No se pudo eliminar la clase: {e}")
    def on_schedule_selected(self, current_item, previous_item):
        is_admin = self.user_role in ['dueño', 'profesor']; has_selection = current_item is not None
        self.edit_schedule_button.setEnabled(has_selection and is_admin); self.delete_schedule_button.setEnabled(has_selection and is_admin); self.add_user_button.setEnabled(has_selection); self.remove_user_button.setEnabled(False)
        self.register_attendance_button.setEnabled(has_selection and is_admin); self.remove_from_waiting_button.setEnabled(False); self.promote_from_waiting_button.setEnabled(False)
        if has_selection: 
            self.selected_schedule = current_item.data(Qt.ItemDataRole.UserRole)
            self.load_users_for_schedule(self.selected_schedule.id)
            self.load_waiting_list_for_schedule(self.selected_schedule.id)
        else: 
            self.selected_schedule = None
            self.inscriptions_table.setRowCount(0)
            self.waiting_list_table.setRowCount(0)
    def load_schedules_for_class(self, clase_id: int):
        """Carga horarios de la clase en segundo plano."""
        selected_schedule_id = self.selected_schedule.id if self.selected_schedule else None; self.schedule_list.clear()
        try:
            def _load():
                try:
                    return self.db_manager.obtener_horarios_de_clase(clase_id)
                except Exception:
                    return []

            def _on_success(horarios):
                try:
                    for h in (horarios or []):
                        prof_name = getattr(h, 'nombre_profesor', None) or "Sin Asignar"
                        hora_inicio = getattr(h, 'hora_inicio', '')
                        hora_fin = getattr(h, 'hora_fin', '')
                        ins = getattr(h, 'inscriptos', 0)
                        cupo = getattr(h, 'cupo_maximo', 0)
                        item_text = f"{getattr(h, 'dia_semana', '')} {hora_inicio}-{hora_fin} (Prof: {prof_name}) [{ins}/{cupo}]"
                        list_item = QListWidgetItem(item_text); list_item.setData(Qt.ItemDataRole.UserRole, h); self.schedule_list.addItem(list_item)
                        if getattr(h, 'id', None) == selected_schedule_id:
                            self.schedule_list.setCurrentItem(list_item)
                except Exception:
                    pass

            def _on_error(err):
                QMessageBox.critical(self, "Error", f"No se pudieron cargar los horarios: {err}")

            run_in_background(
                _load,
                on_success=_on_success,
                on_error=_on_error,
                parent=self,
                timeout_ms=2000,
                description="Cargar horarios de clase",
            )
        except Exception as e:
            QMessageBox.critical(self, "Error", f"No se pudieron cargar los horarios: {e}")
    def load_users_for_schedule(self, horario_id: int):
        """Carga inscriptos del horario en segundo plano."""
        self.inscriptions_table.setRowCount(0); self.remove_user_button.setEnabled(False)
        try:
            def _load():
                try:
                    return self.db_manager.obtener_usuarios_en_clase(horario_id)
                except Exception:
                    return []

            def _on_success(inscriptos):
                try:
                    self.inscriptions_table.setRowCount(len(inscriptos))
                    for row, inscripcion in enumerate(inscriptos or []):
                        id_item = QTableWidgetItem(str(inscripcion.get('usuario_id')))
                        id_item.setData(Qt.ItemDataRole.UserRole, inscripcion)
                        self.inscriptions_table.setItem(row, 0, id_item)
                        self.inscriptions_table.setItem(row, 1, QTableWidgetItem(inscripcion.get('nombre_usuario', '')))
                    self.inscriptions_table.itemSelectionChanged.connect(lambda: self.remove_user_button.setEnabled(bool(self.inscriptions_table.selectedItems())))
                except Exception:
                    pass

            def _on_error(err):
                QMessageBox.critical(self, "Error", f"No se pudieron cargar los alumnos inscriptos: {err}")

            run_in_background(
                _load,
                on_success=_on_success,
                on_error=_on_error,
                parent=self,
                timeout_ms=2000,
                description="Cargar inscriptos del horario",
            )
        except Exception as e:
            QMessageBox.critical(self, "Error", f"No se pudieron cargar los alumnos inscriptos: {e}")
    def add_schedule(self):
        if not self.selected_class_id: return
        dialog = ScheduleEditorDialog(self, clase_id=self.selected_class_id, profesores=self.profesores)
        if dialog.exec():
            try: self.db_manager.crear_horario_clase(dialog.get_horario()); self.load_schedules_for_class(self.selected_class_id)
            except Exception as e: QMessageBox.critical(self, "Error", f"No se pudo crear el horario: {e}")
    def edit_schedule(self):
        current_item = self.schedule_list.currentItem();
        if not current_item: return
        horario_a_editar = current_item.data(Qt.ItemDataRole.UserRole)
        dialog = ScheduleEditorDialog(self, horario=horario_a_editar, clase_id=self.selected_class_id, profesores=self.profesores)
        if dialog.exec():
            try:
                # Actualizar horario y, si hay cupo y lista de espera, disparar aviso
                horario_actualizado = dialog.get_horario()
                self.db_manager.actualizar_horario_clase(horario_actualizado)
                self.load_schedules_for_class(self.selected_class_id)
                try:
                    # Intentar avisar si tras la edición hay cupo disponible y espera
                    if getattr(horario_actualizado, 'id', None):
                        self.check_and_prompt_waitlist_notification(horario_actualizado.id)
                except Exception:
                    # No interrumpir flujo si falla el aviso
                    pass
            except Exception as e: QMessageBox.critical(self, "Error", f"No se pudo actualizar el horario: {e}")
    def delete_schedule(self):
        current_item = self.schedule_list.currentItem()
        if not current_item: return
        horario_a_eliminar = current_item.data(Qt.ItemDataRole.UserRole)
        if QMessageBox.question(self, "Confirmar", f"¿Seguro que desea eliminar el horario de las {horario_a_eliminar.hora_inicio}?", QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No) == QMessageBox.StandardButton.Yes:
            try: self.db_manager.eliminar_horario_clase(horario_a_eliminar.id); self.load_schedules_for_class(self.selected_class_id)
            except Exception as e: QMessageBox.critical(self, "Error", f"No se pudo eliminar el horario: {e}")
    def add_user_to_class(self):
        if not self.selected_schedule:
            QMessageBox.warning(self, "Sin selección", "Por favor, seleccione un horario antes de inscribir un alumno.")
            return

        try:
            self.setCursor(Qt.CursorShape.WaitCursor)

            def _load():
                todos_los_socios = [u for u in self.db_manager.obtener_todos_usuarios() if u.rol == 'socio' and u.activo]
                inscriptos_ids = {u['usuario_id'] for u in self.db_manager.obtener_usuarios_en_clase(self.selected_schedule.id)}
                lista_espera_ids = {u['usuario_id'] for u in self.db_manager.obtener_lista_espera(self.selected_schedule.id)}
                disponibles = [u for u in todos_los_socios if u.id not in inscriptos_ids and u.id not in lista_espera_ids]
                return disponibles

            thread = TaskThread(_load)
            thread.success.connect(self._open_user_selection_dialog_for_class)
            thread.error.connect(lambda msg: QMessageBox.critical(self, "Error", f"Error al cargar usuarios: {msg}"))
            thread.finished.connect(self.unsetCursor)
            thread.finished.connect(thread.deleteLater)
            thread.start()
        except Exception as e:
            self.unsetCursor()
            QMessageBox.critical(self, "Error", f"Error al iniciar carga de usuarios: {e}")

    def _open_user_selection_dialog_for_class(self, disponibles):
        try:
            if not disponibles:
                QMessageBox.information(self, "No hay alumnos", "No hay más alumnos disponibles para inscribir.")
                return

            dialog = UserSelectionDialog(self, disponibles)
            if dialog.exec():
                user_to_add = dialog.get_selected_user()
                if user_to_add:
                    schedule_id_to_refresh = self.selected_schedule.id
                    try:
                        inscrito_directamente = self.db_manager.inscribir_usuario_en_clase(schedule_id_to_refresh, user_to_add.id)

                        if inscrito_directamente:
                            QMessageBox.information(self, "Inscripción Exitosa", f"{user_to_add.nombre} ha sido inscrito en la clase.")
                        else:
                            QMessageBox.information(self, "Agregado a Lista de Espera", f"{user_to_add.nombre} ha sido agregado a la lista de espera.\nSerá notificado automáticamente cuando se libere un cupo.")

                        self.load_schedules_for_class(self.selected_class_id)
                        self.load_users_for_schedule(schedule_id_to_refresh)
                        self.load_waiting_list_for_schedule(schedule_id_to_refresh)

                    except Exception as e:
                        try:
                            logging.error(f"Inscripción fallida: tipo={type(e).__name__}, repr={repr(e)}")
                            logging.exception("Stack trace en add_user_to_class")
                        except Exception:
                            pass
                        QMessageBox.critical(self, "Error", f"No se pudo procesar la inscripción: {e}")
        except Exception as e:
            QMessageBox.critical(self, "Error", f"Error al abrir el diálogo: {e}")
    def remove_user_from_class(self):
        selected_items = self.inscriptions_table.selectedItems()
        if not selected_items:
            QMessageBox.warning(self, "Sin selección", "Seleccione un alumno para quitarlo.")
            return
        inscripcion = selected_items[0].data(Qt.ItemDataRole.UserRole)
        if QMessageBox.question(
            self,
            "Confirmar",
            f"¿Quitar a {inscripcion['nombre_usuario']} de la clase?",
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
        ) == QMessageBox.StandardButton.Yes:
            try:
                self.db_manager.quitar_usuario_de_clase(inscripcion['clase_horario_id'], inscripcion['usuario_id'])
                # Recargar vistas
                self.load_schedules_for_class(self.selected_class_id)
                self.load_users_for_schedule(inscripcion['clase_horario_id'])
                self.load_waiting_list_for_schedule(inscripcion['clase_horario_id'])

                # Al liberar un cupo, ofrecer avisar al primero en lista de espera
                try:
                    self.check_and_prompt_waitlist_notification(inscripcion['clase_horario_id'])
                except Exception:
                    # No interrumpir el flujo si falla el aviso
                    pass
            except Exception as e:
                QMessageBox.critical(self, "Error", f"No se pudo quitar al alumno: {e}")

    def check_and_prompt_waitlist_notification(self, horario_id: int):
        """Si hay cupo y lista de espera, pregunta si avisar al primero y envía WhatsApp sin bloquear la UI."""
        try:
            import logging
            # Verificar configuración global desde BD con fallback a constante
            try:
                flag = self.db_manager.obtener_configuracion('enable_waitlist_prompt')
                use_prompt = (str(flag).lower() == 'true') if flag is not None else ENABLE_WAITLIST_PROMPT
            except Exception:
                use_prompt = ENABLE_WAITLIST_PROMPT
            if not use_prompt:
                return

            # Debounce mejorado para evitar prompts consecutivos
            try:
                now_ts = time.time()
                last_map = getattr(self, '_waitlist_prompt_last_by_schedule', {})
                last_ts = last_map.get(horario_id, 0.0)
                if now_ts - last_ts < 3.0:
                    return
                last_map[horario_id] = now_ts
                self._waitlist_prompt_last_by_schedule = last_map
            except Exception:
                pass

            # Verificar cupo disponible
            if not self.db_manager.verificar_cupo_disponible(horario_id):
                return

            # Cargar lista de espera y usuario principal en hilo para no bloquear
            def _load_waitlist_and_user():
                lista_espera = self.db_manager.obtener_lista_espera(horario_id)
                if not lista_espera:
                    return {'primero': None, 'telefono': ''}
                try:
                    primero = sorted(lista_espera, key=lambda e: e.get('posicion', 999999))[0]
                except Exception:
                    primero = lista_espera[0]
                usuario_id = primero.get('usuario_id')
                try:
                    usuario = self.db_manager.obtener_usuario_por_id(usuario_id)
                except Exception:
                    usuario = None
                telefono = getattr(usuario, 'telefono', '') if usuario else ''
                return {'primero': primero, 'telefono': telefono}

            try:
                self.setCursor(Qt.CursorShape.WaitCursor)
                thread = TaskThread(_load_waitlist_and_user)
                thread.success.connect(lambda data: self._handle_waitlist_prompt_data(horario_id, data))
                thread.error.connect(lambda msg: (self.unsetCursor(), logging.error(f"Error cargando lista de espera/usuario: {msg}")))
                thread.finished.connect(self.unsetCursor)
                thread.finished.connect(thread.deleteLater)
                thread.start()
            except Exception as e:
                try:
                    self.unsetCursor()
                except Exception:
                    pass
                logging.error(f"Error iniciando carga asíncrona de lista de espera: {e}")
        except Exception as e:
            try:
                import logging
                logging.error(f"Error en check_and_prompt_waitlist_notification: {e}")
            except Exception:
                pass

    def _handle_waitlist_prompt_data(self, horario_id: int, data: dict):
        try:
            import logging
            primero = (data or {}).get('primero')
            telefono = (data or {}).get('telefono') or '—'
            if not primero:
                return
            usuario_id = primero.get('usuario_id')
            nombre_usuario = primero.get('nombre_usuario', '')

            # Preguntar si avisar
            texto = (
                "Se liberó un cupo en esta clase.\n\n"
                "¿Avisar a la primera persona en lista de espera?\n\n"
                f"Alumno: {nombre_usuario}\n"
                f"Teléfono: {telefono}"
            )
            respuesta = QMessageBox.question(
                self,
                "Cupo disponible",
                texto,
                QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
            )
            if respuesta != QMessageBox.StandardButton.Yes:
                try:
                    self.db_manager.registrar_audit_log(
                        user_id=None,
                        action='WAITLIST_NOTIFY_SKIPPED',
                        table_name='clases_horarios',
                        record_id=horario_id,
                        old_values=None,
                        new_values=f"skip usuario_id={usuario_id}",
                    )
                except Exception:
                    pass
                return

            # Construir clase_info (tipo, fecha próxima, hora)
            clase_info = {}
            try:
                horario = self.db_manager.obtener_horario_por_id(horario_id)
                if horario:
                    tipo_clase = (
                        horario.get('tipo_clase_nombre')
                        or horario.get('clase_nombre')
                        or 'Clase'
                    )
                    dia_semana = horario.get('dia_semana')
                    hora_inicio = horario.get('hora_inicio')

                    dias_map = {
                        'Lunes': 0,
                        'Martes': 1,
                        'Miércoles': 2,
                        'Jueves': 3,
                        'Viernes': 4,
                        'Sábado': 5,
                        'Domingo': 6,
                    }
                    today = datetime.now().date()
                    hoy_idx = today.weekday()  # Lunes=0
                    target_idx = dias_map.get(str(dia_semana), hoy_idx)
                    delta = (target_idx - hoy_idx) % 7
                    fecha = today + timedelta(days=delta)

                    try:
                        hora_str = (
                            hora_inicio.strftime('%H:%M')
                            if hasattr(hora_inicio, 'strftime')
                            else str(hora_inicio)
                        )
                    except Exception:
                        hora_str = str(hora_inicio)

                    clase_info = {
                        'tipo_clase': tipo_clase,
                        'fecha': fecha.strftime('%d/%m/%Y'),
                        'hora': hora_str,
                    }
            except Exception as err:
                logging.warning(f"No se pudo construir clase_info: {err}")

            # Enviar aviso por WhatsApp
            try:
                from whatsapp_manager import WhatsAppManager
                wa_manager = WhatsAppManager(self.db_manager)
                wa_manager.enviar_promocion_lista_espera(usuario_id, clase_info)
                QMessageBox.information(
                    self,
                    "Aviso enviado",
                    f"Se avisó a {nombre_usuario} por WhatsApp.",
                )
                try:
                    self.db_manager.registrar_audit_log(
                        user_id=None,
                        action='WAITLIST_NOTIFY_SENT',
                        table_name='lista_espera',
                        record_id=usuario_id,
                        old_values=None,
                        new_values=f"sent horario_id={horario_id}",
                    )
                except Exception:
                    pass
            except Exception as werr:
                logging.error(
                    f"Error enviando promoción de lista de espera por WhatsApp: {werr}"
                )
                QMessageBox.warning(
                    self,
                    "Aviso no enviado",
                    "No se pudo enviar el aviso por WhatsApp.",
                )
                try:
                    self.db_manager.registrar_audit_log(
                        user_id=None,
                        action='WAITLIST_NOTIFY_FAILED',
                        table_name='lista_espera',
                        record_id=usuario_id,
                        old_values=None,
                        new_values=f"failed horario_id={horario_id}",
                    )
                except Exception:
                    pass
        except Exception as e:
            try:
                import logging
                logging.error(f"Error manejando datos de prompt de lista de espera: {e}")
            except Exception:
                pass
    
    def apply_inscription_filters(self, filters):
        """Aplica filtros avanzados a la tabla de inscripciones"""
        if not self.selected_schedule:
            return
        
        try:
            inscriptos = self.db_manager.obtener_usuarios_en_clase(self.selected_schedule.id)
            
            # Aplicar filtros
            inscriptos_filtrados = []
            for inscripcion in inscriptos:
                # Filtro por nombre
                if filters.get('nombre') and filters['nombre'].lower() not in inscripcion.get('nombre_usuario', '').lower():
                    continue
                
                # Filtro por ID de usuario
                if filters.get('usuario_id') is not None and inscripcion.get('usuario_id') != filters['usuario_id']:
                    continue
                
                inscriptos_filtrados.append(inscripcion)
            
            # Actualizar tabla con inscripciones filtradas
            self.actualizar_tabla_inscripciones(inscriptos_filtrados)
            
        except Exception as e:
            QMessageBox.critical(self, "Error", f"No se pudieron aplicar los filtros: {e}")

    def on_inscription_search_text_changed(self, text: str):
        """Convierte texto en filtros con heurísticas seguras: ID, nombre y etiquetas (#tag)."""
        try:
            import re
            query = (text or "").strip()
            filters = {}
            if query:
                ql = query.lower()
                # Detectar etiqueta con prefijo # (p.ej., #vip) si el backend la soporta
                tag_match = re.search(r"#([\w-]+)", ql)
                if tag_match:
                    filters['etiqueta'] = tag_match.group(1)

                # Detectar patrón ID:123 o solo número
                id_match = re.search(r"\b(?:id[:#\s]?)(\d+)\b", ql)
                if id_match:
                    try:
                        filters['usuario_id'] = int(id_match.group(1))
                    except Exception:
                        pass
                else:
                    try:
                        filters['usuario_id'] = int(query)
                    except ValueError:
                        # Búsqueda por nombre si no es un ID válido
                        filters['nombre'] = query
            self.apply_inscription_filters(filters)
        except Exception:
            # Evitar romper la UI por errores de conversión
            pass
    
    def actualizar_tabla_inscripciones(self, inscriptos):
        """Actualiza la tabla de inscripciones con la lista proporcionada"""
        self.inscriptions_table.setRowCount(len(inscriptos))
        
        for row, inscripcion in enumerate(inscriptos):
            id_item = QTableWidgetItem(str(inscripcion['usuario_id']))
            id_item.setData(Qt.ItemDataRole.UserRole, inscripcion)
            self.inscriptions_table.setItem(row, 0, id_item)
            self.inscriptions_table.setItem(row, 1, QTableWidgetItem(inscripcion['nombre_usuario']))
        
        # Reconectar la señal de selección
        self.inscriptions_table.itemSelectionChanged.connect(
            lambda: self.remove_user_button.setEnabled(bool(self.inscriptions_table.selectedItems()))
        )
    
    def load_waiting_list_for_schedule(self, horario_id: int):
        """Carga la lista de espera para un horario específico sin bloquear la UI"""
        # Estado inicial y feedback
        self.waiting_list_table.setRowCount(0)
        self.remove_from_waiting_button.setEnabled(False)
        self.promote_from_waiting_button.setEnabled(False)
        try:
            self.setCursor(Qt.CursorShape.WaitCursor)
            # Ejecutar en hilo para evitar bloquear la interfaz
            def _load():
                return self.db_manager.obtener_lista_espera(horario_id)

            thread = TaskThread(_load)
            thread.success.connect(lambda lista_espera: self.actualizar_tabla_lista_espera(lista_espera))
            thread.error.connect(lambda msg: QMessageBox.critical(self, "Error", f"No se pudo cargar la lista de espera: {msg}"))
            thread.finished.connect(self.unsetCursor)
            thread.finished.connect(thread.deleteLater)
            thread.start()
        except Exception as e:
            self.unsetCursor()
            QMessageBox.critical(self, "Error", f"Error al iniciar carga de lista de espera: {e}")
    
    def update_waiting_list_buttons(self):
        """Actualiza el estado de los botones de lista de espera"""
        has_selection = bool(self.waiting_list_table.selectedItems())
        self.remove_from_waiting_button.setEnabled(has_selection)
        
        # Solo permitir promover si hay cupo disponible
        if has_selection and self.selected_schedule:
            cupo_disponible = self.db_manager.verificar_cupo_disponible(self.selected_schedule.id)
            self.promote_from_waiting_button.setEnabled(cupo_disponible)
        else:
            self.promote_from_waiting_button.setEnabled(False)
    
    def remove_user_from_waiting_list(self):
        """Quita un usuario de la lista de espera"""
        selected_items = self.waiting_list_table.selectedItems()
        if not selected_items:
            QMessageBox.warning(self, "Sin selección", "Seleccione un alumno para quitar de la lista de espera.")
            return
        
        espera_data = selected_items[0].data(Qt.ItemDataRole.UserRole)
        nombre_usuario = espera_data['nombre_usuario']
        
        if QMessageBox.question(self, "Confirmar", 
                               f"¿Quitar a {nombre_usuario} de la lista de espera?", 
                               QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No) == QMessageBox.StandardButton.Yes:
            try:
                self.db_manager.quitar_de_lista_espera(self.selected_schedule.id, espera_data['usuario_id'])
                self.load_waiting_list_for_schedule(self.selected_schedule.id)
                QMessageBox.information(self, "Éxito", f"{nombre_usuario} ha sido quitado de la lista de espera.")
                # Si hay cupo y aún quedan en lista, ofrecer avisar al siguiente
                try:
                    if self.selected_schedule:
                        self.check_and_prompt_waitlist_notification(self.selected_schedule.id)
                except Exception:
                    pass
            except Exception as e:
                QMessageBox.critical(self, "Error", f"No se pudo quitar al alumno de la lista de espera: {e}")
    
    def promote_user_from_waiting_list(self):
        """Promueve un usuario de la lista de espera a la clase"""
        selected_items = self.waiting_list_table.selectedItems()
        if not selected_items:
            QMessageBox.warning(self, "Sin selección", "Seleccione un alumno para promover a la clase.")
            return
        
        espera_data = selected_items[0].data(Qt.ItemDataRole.UserRole)
        nombre_usuario = espera_data['nombre_usuario']
        # Obtener usuario de forma asíncrona para no bloquear la UI
        try:
            self.setCursor(Qt.CursorShape.WaitCursor)
            thread = TaskThread(lambda: self.db_manager.obtener_usuario_por_id(espera_data['usuario_id']))
            thread.success.connect(lambda usuario: self._continue_promote_user_with_checks(usuario, espera_data, nombre_usuario))
            thread.error.connect(lambda msg: (self.unsetCursor(), QMessageBox.critical(self, "Error", f"Error al obtener usuario: {msg}")))
            thread.finished.connect(thread.deleteLater)
            thread.start()
        except Exception as e:
            self.unsetCursor()
            QMessageBox.critical(self, "Error", f"Error al preparar promoción: {e}")
        return

    def _continue_promote_user_with_checks(self, usuario, espera_data: dict, nombre_usuario: str):
        try:
            if not usuario:
                QMessageBox.warning(self, "Usuario no encontrado", "No se pudo obtener información del usuario seleccionado.")
                return
            if getattr(usuario, 'rol', None) == 'dueño':
                QMessageBox.warning(self, "Acción no permitida", "No se puede promover a la clase al usuario Dueño.")
                return
            if not getattr(usuario, 'activo', True):
                QMessageBox.warning(self, "Usuario Inactivo", f"{nombre_usuario} está inactivo. No se puede promover a la clase.")
                return

            # Verificar que hay cupo disponible
            if not self.db_manager.verificar_cupo_disponible(self.selected_schedule.id):
                QMessageBox.warning(self, "Sin cupo", "No hay cupo disponible en esta clase.")
                return

            # Confirmar promoción
            if QMessageBox.question(
                self,
                "Confirmar",
                f"¿Promover a {nombre_usuario} de la lista de espera a la clase?",
                QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
            ) != QMessageBox.StandardButton.Yes:
                return

            try:
                # Quitar de lista de espera e inscribir en clase
                self.db_manager.quitar_de_lista_espera(self.selected_schedule.id, espera_data['usuario_id'])
                self.db_manager.inscribir_usuario_en_clase(self.selected_schedule.id, espera_data['usuario_id'])

                # Actualizar vistas
                self.load_users_for_schedule(self.selected_schedule.id)
                self.load_waiting_list_for_schedule(self.selected_schedule.id)
                self.load_schedules_for_class(self.selected_class_id)

                QMessageBox.information(self, "Éxito", f"{nombre_usuario} ha sido promovido a la clase.")

                # Integración WhatsApp: opcional según configuración y confirmación
                try:
                    try:
                        flag = self.db_manager.obtener_configuracion('enable_waitlist_prompt')
                        use_prompt = (str(flag).lower() == 'true') if flag is not None else ENABLE_WAITLIST_PROMPT
                    except Exception:
                        use_prompt = ENABLE_WAITLIST_PROMPT

                    enviar_whatsapp = True
                    if use_prompt:
                        resp = QMessageBox.question(
                            self,
                            "Enviar WhatsApp",
                            f"¿Enviar aviso por WhatsApp a {nombre_usuario} informando su promoción?",
                            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
                        )
                        enviar_whatsapp = (resp == QMessageBox.StandardButton.Yes)

                    if enviar_whatsapp:
                        from whatsapp_manager import WhatsAppManager
                        horario = self.db_manager.obtener_horario_por_id(self.selected_schedule.id)
                        clase_info = {}
                        if horario:
                            # Determinar tipo de clase
                            tipo_clase = horario.get('tipo_clase_nombre') or horario.get('clase_nombre') or 'Clase'
                            dia_semana = horario.get('dia_semana')
                            hora_inicio = horario.get('hora_inicio')

                            # Calcular próxima fecha para el día de la semana
                            dias_map = {
                                'Lunes': 0, 'Martes': 1, 'Miércoles': 2, 'Jueves': 3,
                                'Viernes': 4, 'Sábado': 5, 'Domingo': 6
                            }
                        today = datetime.now().date()
                        hoy_idx = today.weekday()  # Lunes=0
                        target_idx = dias_map.get(str(dia_semana), hoy_idx)
                        delta = (target_idx - hoy_idx) % 7
                        fecha = today + timedelta(days=delta)

                        # Formatear hora a HH:MM
                        try:
                            hora_str = hora_inicio.strftime('%H:%M') if hasattr(hora_inicio, 'strftime') else str(hora_inicio)
                        except Exception:
                            hora_str = str(hora_inicio)

                        clase_info = {
                            'tipo_clase': tipo_clase,
                            'fecha': fecha.strftime('%d/%m/%Y'),
                            'hora': hora_str
                        }
                    
                    wa_manager = WhatsAppManager(self.db_manager)
                    # Al promover a lista principal, usar la plantilla específica
                    wa_manager.enviar_promocion_a_lista_principal(espera_data['usuario_id'], clase_info)
                except Exception as werr:
                    import logging
                    logging.error(f"Error enviando aviso de promoción a lista principal por WhatsApp: {werr}")
            except Exception as e:
                QMessageBox.critical(self, "Error", f"No se pudo promover al alumno: {e}")
        finally:
            try:
                self.unsetCursor()
            except Exception:
                pass
    
    def apply_waiting_filters(self, filters):
        """Aplica filtros avanzados a la tabla de lista de espera"""
        if not self.selected_schedule:
            return
        
        try:
            lista_espera = self.db_manager.obtener_lista_espera(self.selected_schedule.id)
            
            # Aplicar filtros
            lista_filtrada = []
            for espera in lista_espera:
                # Filtro por nombre
                if filters.get('nombre') and filters['nombre'].lower() not in espera['nombre_usuario'].lower():
                    continue
                
                # Filtro por ID de usuario
                if filters.get('usuario_id') is not None and espera['usuario_id'] != filters['usuario_id']:
                    continue
                
                lista_filtrada.append(espera)
            
            # Actualizar tabla con lista filtrada
            self.actualizar_tabla_lista_espera(lista_filtrada)
            
        except Exception as e:
            QMessageBox.critical(self, "Error", f"No se pudieron aplicar los filtros: {e}")

    def on_waiting_search_text_changed(self, text: str):
        """Convierte texto en filtros con heurísticas seguras para la lista de espera."""
        try:
            import re
            query = (text or "").strip()
            filters = {}
            if query:
                ql = query.lower()
                # Etiquetas
                tag_match = re.search(r"#([\w-]+)", ql)
                if tag_match:
                    filters['etiqueta'] = tag_match.group(1)

                # ID explícito o número simple
                id_match = re.search(r"\b(?:id[:#\s]?)(\d+)\b", ql)
                if id_match:
                    try:
                        filters['usuario_id'] = int(id_match.group(1))
                    except Exception:
                        pass
                else:
                    try:
                        filters['usuario_id'] = int(query)
                    except ValueError:
                        filters['nombre'] = query
            self.apply_waiting_filters(filters)
        except Exception:
            pass
    
    def actualizar_tabla_lista_espera(self, lista_espera):
        """Actualiza la tabla de lista de espera con la lista proporcionada"""
        self.waiting_list_table.setRowCount(len(lista_espera))
        
        for row, espera in enumerate(lista_espera):
            id_item = QTableWidgetItem(str(espera['usuario_id']))
            id_item.setData(Qt.ItemDataRole.UserRole, espera)
            self.waiting_list_table.setItem(row, 0, id_item)
            self.waiting_list_table.setItem(row, 1, QTableWidgetItem(espera['nombre_usuario']))
            self.waiting_list_table.setItem(row, 2, QTableWidgetItem(str(espera['posicion'])))
        
        # Reconectar la señal de selección
        self.waiting_list_table.itemSelectionChanged.connect(
            lambda: self.update_waiting_list_buttons()
        )
    
    def apply_waiting_list_filters(self):
        """Aplica los filtros de la lista de espera basados en los campos de entrada"""
        if not self.selected_schedule:
            return
        
        filters = {}
        
        # Obtener valores de filtros
        nombre_filter = self.waiting_name_filter.text().strip()
        if nombre_filter:
            filters['nombre'] = nombre_filter
        
        id_filter = self.waiting_id_filter.text().strip()
        if id_filter:
            try:
                filters['usuario_id'] = int(id_filter)
            except ValueError:
                # Si no es un número válido, ignorar el filtro
                pass
        
        # Aplicar filtros
        self.apply_waiting_filters(filters)
    
    def register_class_attendance(self):
        """Abre un diálogo para registrar la asistencia de los usuarios inscritos en la clase sin bloquear la UI"""
        if not self.selected_schedule:
            QMessageBox.warning(self, "Sin selección", "Por favor, seleccione un horario antes de registrar asistencia.")
            return

        try:
            self.setCursor(Qt.CursorShape.WaitCursor)

            def _load():
                inscriptos = self.db_manager.obtener_usuarios_en_clase(self.selected_schedule.id)
                if not inscriptos:
                    return {'inscriptos_activos': [], 'omitidos': 0}
                inscriptos_activos = []
                omitidos = 0
                for ins in inscriptos:
                    usuario_id = ins.get('usuario_id') if isinstance(ins, dict) else ins.usuario_id
                    try:
                        usuario = self.db_manager.obtener_usuario_por_id(usuario_id)
                    except Exception:
                        usuario = None
                    if not usuario:
                        omitidos += 1
                        continue
                    if getattr(usuario, 'rol', None) == 'dueño' or not getattr(usuario, 'activo', True):
                        omitidos += 1
                        continue
                    inscriptos_activos.append(ins)
                return {'inscriptos_activos': inscriptos_activos, 'omitidos': omitidos}

            thread = TaskThread(_load)
            thread.success.connect(lambda data: self._open_attendance_dialog_with_data(data.get('inscriptos_activos', []), data.get('omitidos', 0)))
            thread.error.connect(lambda msg: QMessageBox.critical(self, "Error", f"No se pudo registrar la asistencia: {msg}"))
            thread.finished.connect(self.unsetCursor)
            thread.finished.connect(thread.deleteLater)
            thread.start()
        except Exception as e:
            self.unsetCursor()
            QMessageBox.critical(self, "Error", f"Error al preparar registro de asistencia: {e}")

    def _open_attendance_dialog_with_data(self, inscriptos_activos, omitidos: int):
        try:
            if not inscriptos_activos:
                QMessageBox.information(self, "Sin alumnos activos", "No hay alumnos activos elegibles para registrar asistencia.")
                return

            from widgets.attendance_dialog import AttendanceDialog
            dialog = AttendanceDialog(self, inscriptos_activos, self.selected_schedule, self.db_manager)
            if dialog.exec():
                QMessageBox.information(self, "Éxito", "La asistencia ha sido registrada correctamente.")
                if omitidos:
                    QMessageBox.information(self, "Omitidos", f"{omitidos} usuario(s) inactivos o Dueño fueron omitidos del registro de asistencia.")
        except Exception as e:
            QMessageBox.critical(self, "Error", f"No se pudo abrir el diálogo de asistencia: {e}")

