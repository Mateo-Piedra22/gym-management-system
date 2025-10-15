import logging
from typing import Optional, Dict
from PyQt6.QtWidgets import (
    QWidget, QVBoxLayout, QHBoxLayout, QGroupBox, QFormLayout,
    QComboBox, QLineEdit, QListWidget, QListWidgetItem, QSpinBox, QPushButton,
    QTableWidget, QTableWidgetItem, QHeaderView, QLabel, QMessageBox,
    QTabWidget, QAbstractItemView, QMenu, QFileDialog
)
from PyQt6.QtCore import Qt, QDate
from PyQt6.QtGui import QAction
from database import DatabaseManager
from export_manager import ExportManager 
from pdf_generator import PDFGenerator
from routine_manager import RoutineTemplateManager
from models import Usuario, Rutina, RutinaEjercicio
from widgets.add_exercise_dialog import AddExerciseDialog
from widgets.template_editor_dialog import TemplateEditorDialog
from widgets.filter_button_widget import FilterButton

from widgets.user_selection_dialog import UserSelectionDialog
from utils_modules.async_runner import TaskThread
from utils_modules.users_loader import load_users_cached_async

class RoutinesTabWidget(QWidget):
    def __init__(self, db_manager: DatabaseManager, user_role: str):
        super().__init__()
        self.db_manager = db_manager
        self.user_role = user_role
        self.pdf_generator = None
        self.export_manager = ExportManager(self.db_manager)
        self.routine_manager = RoutineTemplateManager(database_manager=self.db_manager)
        self.selected_user: Optional[Usuario] = None
        self.current_routine: Optional[Rutina] = None
        self.exercises_by_day: Dict[int, list[RutinaEjercicio]] = {}
        self.routine_weeks: int = 4  # Número de semanas para la rutina (valor inicial)
        self.setup_ui()
        self.load_preset_routines()
        self.initialize_pdf_generator()

    def setup_ui(self):
        main_layout = QHBoxLayout(self)
        left_panel = QWidget(); left_layout = QVBoxLayout(left_panel); left_panel.setFixedWidth(450)
        
        self.template_management_group = QGroupBox()
        template_management_layout = QVBoxLayout(self.template_management_group)
        
        # Crear layout horizontal para el título y la barra de búsqueda
        header_layout = QHBoxLayout()
        templates_title = QLabel("Plantillas de Rutina")
        templates_title.setProperty("class", "panel_label")
        # Barra de búsqueda en reemplazo del botón de filtros
        self.template_search = QLineEdit()
        self.template_search.setObjectName("template_search")
        self.template_search.setPlaceholderText("Buscar por nombre, categoría o días...")
        self.template_search.textChanged.connect(self.on_template_search_text_changed)

        header_layout.addWidget(templates_title)
        header_layout.addStretch()
        header_layout.addWidget(self.template_search)
        
        template_management_layout.addLayout(header_layout)
        
        self.template_list = QListWidget()
        self.template_list.setAlternatingRowColors(True)
        template_buttons_layout = QHBoxLayout()
        template_buttons_layout.setSpacing(6)
        template_buttons_layout.setContentsMargins(0, 0, 0, 0)
        self.add_template_button = QPushButton("➕ Crear")
        self.edit_template_button = QPushButton("✏️ Editar")
        self.duplicate_template_button = QPushButton("📋 Duplicar")
        self.delete_template_button = QPushButton("🗑️ Eliminar")
        # Hacer los botones más compactos para evitar desfasajes
        for btn in (
            self.add_template_button,
            self.edit_template_button,
            self.duplicate_template_button,
            self.delete_template_button,
        ):
            btn.setProperty("class", "compact_button")
        template_buttons_layout.addWidget(self.add_template_button)
        template_buttons_layout.addWidget(self.edit_template_button)
        template_buttons_layout.addWidget(self.duplicate_template_button)
        template_buttons_layout.addWidget(self.delete_template_button)
        template_buttons_layout.addStretch()
        template_management_layout.addWidget(self.template_list)
        template_management_layout.addLayout(template_buttons_layout)
        self.template_management_group.setVisible(self.user_role in ['dueño', 'profesor'])
        
        routine_selection_group = QGroupBox("Gestión de Rutina de Usuario")
        routine_selection_layout = QFormLayout(routine_selection_group)
        
        # Layout horizontal para usuario y botón de selección
        user_layout = QHBoxLayout()
        self.user_label = QLabel("No seleccionado")
        self.user_label.setObjectName("routine_user_label")
        self.select_user_button = QPushButton("Seleccionar")
        user_layout.addWidget(self.user_label)
        user_layout.addStretch()
        user_layout.addWidget(self.select_user_button)
        
        self.preset_routines_combo = QComboBox(); self.preset_routines_combo.setPlaceholderText("Cargar desde plantilla...")
        self.user_routines_combo = QComboBox(); self.user_routines_combo.setPlaceholderText("Seleccionar rutina existente...")
        # Ajustar tamaño: barras normales y solo el desplegable se estira
        try:
            # Mantener la barra compacta
            self.preset_routines_combo.setSizeAdjustPolicy(QComboBox.SizeAdjustPolicy.AdjustToMinimumContentsLengthWithIcon)
            self.user_routines_combo.setSizeAdjustPolicy(QComboBox.SizeAdjustPolicy.AdjustToMinimumContentsLengthWithIcon)
            self.preset_routines_combo.setMinimumContentsLength(18)
            self.user_routines_combo.setMinimumContentsLength(20)
            # Ensanchar únicamente el popup del selector
            self.preset_routines_combo.setStyleSheet("QComboBox QAbstractItemView { min-width: 420px; }")
            self.user_routines_combo.setStyleSheet("QComboBox QAbstractItemView { min-width: 420px; }")
        except Exception:
            pass
        self.new_routine_button = QPushButton("➕ Crear Rutina en Blanco")
        routine_selection_layout.addRow("Usuario:", user_layout)
        routine_selection_layout.addRow("Cargar Plantilla:", self.preset_routines_combo)
        routine_selection_layout.addRow("Rutinas Guardadas:", self.user_routines_combo)
        routine_selection_layout.addRow(self.new_routine_button)
        
        self.details_group = QGroupBox("Detalles de la Rutina")
        details_layout = QFormLayout(self.details_group)
        self.routine_name_input = QLineEdit()
        self.routine_days_spinbox = QSpinBox(minimum=2, maximum=5)
        self.routine_weeks_spinbox = QSpinBox(minimum=1, maximum=4)
        self.routine_weeks_spinbox.setValue(4)  # Valor por defecto
        save_delete_layout = QHBoxLayout()
        self.save_routine_button = QPushButton("💾 Guardar")
        self.delete_routine_button = QPushButton("🗑️ Eliminar")
        save_delete_layout.addWidget(self.save_routine_button)
        save_delete_layout.addWidget(self.delete_routine_button)
        details_layout.addRow("Nombre:", self.routine_name_input)
        details_layout.addRow("Días:", self.routine_days_spinbox)
        details_layout.addRow("Semanas:", self.routine_weeks_spinbox)
        details_layout.addRow(save_delete_layout)
        
        left_layout.addWidget(self.template_management_group)
        left_layout.addWidget(routine_selection_group)
        left_layout.addWidget(self.details_group)
        left_layout.addStretch()
        
        export_group = QGroupBox("Exportar")
        export_layout = QHBoxLayout(export_group)
        self.export_pdf_button = QPushButton("Exportar a PDF")
        self.export_excel_button = QPushButton("Exportar a Excel")
        export_layout.addWidget(self.export_pdf_button)
        export_layout.addWidget(self.export_excel_button)
        left_layout.addWidget(export_group)
        
        right_panel = QWidget(); right_layout = QVBoxLayout(right_panel)
        self.routine_display_tabs = QTabWidget()
        self.routine_display_label = QLabel("Seleccione un usuario y cree o cargue una rutina.")
        self.routine_display_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self.routine_display_label.setObjectName("routine_display_label")
        
        right_layout.addWidget(self.routine_display_label)
        right_layout.addWidget(self.routine_display_tabs)
        self.routine_display_tabs.setVisible(False)

        main_layout.addWidget(left_panel)
        main_layout.addWidget(right_panel, 1)

        self.connect_signals()
        self.set_initial_state()

    def connect_signals(self):
        self.add_template_button.clicked.connect(self.add_template)
        self.edit_template_button.clicked.connect(self.edit_template)
        self.duplicate_template_button.clicked.connect(self.duplicate_template)
        self.delete_template_button.clicked.connect(self.delete_template)
        self.template_list.itemSelectionChanged.connect(self.on_template_selection_changed)
        self.select_user_button.clicked.connect(self.open_user_selection_dialog)
        self.new_routine_button.clicked.connect(self.create_new_routine)
        self.routine_days_spinbox.valueChanged.connect(self.update_day_tabs)
        self.routine_weeks_spinbox.valueChanged.connect(self.update_weeks_value)
        self.save_routine_button.clicked.connect(self.save_routine_changes)
        self.delete_routine_button.clicked.connect(self.delete_selected_routine)
        self.user_routines_combo.currentIndexChanged.connect(self.load_selected_routine_from_combo)
        self.preset_routines_combo.currentIndexChanged.connect(self.apply_preset_routine)
        self.export_pdf_button.clicked.connect(self.export_to_pdf)
        self.export_excel_button.clicked.connect(self.export_to_excel)

    def initialize_pdf_generator(self):
        """Inicializa el generador de PDF con la configuración de branding"""
        try:
            # Obtener la configuración de branding desde main_window
            main_window = self.window()
            if hasattr(main_window, 'branding_config') and main_window.branding_config:
                self.pdf_generator = PDFGenerator(main_window.branding_config)
            else:
                # Fallback: inicializar sin configuración de branding
                self.pdf_generator = PDFGenerator()
        except Exception as e:
            logging.error(f"Error al inicializar PDFGenerator con branding: {e}")
            # Fallback: inicializar sin configuración de branding
            self.pdf_generator = PDFGenerator()

    def update_weeks_value(self):
        """Actualiza el número de semanas cuando cambia el spinbox"""
        self.routine_weeks = self.routine_weeks_spinbox.value()
        logging.info(f"Número de semanas actualizado a: {self.routine_weeks}")
        
        # Actualizar las tablas si hay una rutina cargada
        if self.current_routine:
            self.update_day_tabs()

    def set_initial_state(self):
        self.details_group.setVisible(False)
        self.user_routines_combo.setEnabled(False)
        self.new_routine_button.setEnabled(False)
        self.export_pdf_button.setEnabled(False)
        self.export_excel_button.setEnabled(False)
        self.preset_routines_combo.setEnabled(False)
        self.delete_routine_button.setEnabled(False)
        self.select_user_button.setEnabled(True)
        # Deshabilitar botones de plantillas inicialmente
        self.edit_template_button.setEnabled(False)
        self.duplicate_template_button.setEnabled(False)
        self.delete_template_button.setEnabled(False)
    
    def on_template_selection_changed(self):
        """Maneja el cambio de selección en la lista de plantillas"""
        has_selection = bool(self.template_list.currentItem())
        self.edit_template_button.setEnabled(has_selection)
        self.duplicate_template_button.setEnabled(has_selection)
        self.delete_template_button.setEnabled(has_selection)

    def set_user_for_routine(self, user: Usuario):
        self.selected_user = user
        self.user_label.setText(f"{self.selected_user.nombre}")
        # Habilitar/Deshabilitar acciones según estado activo del usuario
        is_active = getattr(self.selected_user, 'activo', True)
        self.new_routine_button.setEnabled(bool(is_active))
        self.preset_routines_combo.setEnabled(bool(is_active))
        # También bloquear la selección de rutinas guardadas si está inactivo
        self.user_routines_combo.setEnabled(bool(is_active))
        self.load_preset_routines()
        self.load_user_routines_into_combobox()

    def load_preset_routines(self):
        self.preset_routines_combo.blockSignals(True)
        self.template_list.clear()
        self.preset_routines_combo.clear()
        self.preset_routines_combo.addItem("Seleccionar para cargar...", userData=None)
        try:
            templates = self.db_manager.obtener_plantillas_rutina()
            for t in templates:
                # Mostrar días y categoría también en el combo de carga de plantilla
                self.preset_routines_combo.addItem(f"{t.nombre_rutina} ({t.dias_semana} días) - {t.categoria}", userData=t)
                item = QListWidgetItem(f"{t.nombre_rutina} ({t.dias_semana} días) - {t.categoria}")
                item.setData(Qt.ItemDataRole.UserRole, t)
                self.template_list.addItem(item)
        except Exception as e:
            logging.error(f"Error cargando plantillas: {e}")
        finally:
            self.preset_routines_combo.blockSignals(False)
    
    def open_user_selection_dialog(self):
         """Abre el diálogo de selección de usuarios sin bloquear la UI (carga en hilo)."""
         try:
             self.setCursor(Qt.CursorShape.WaitCursor)
             load_users_cached_async(
                 self.db_manager,
                 on_success=self._open_user_selection_dialog_with_data,
                 on_error=lambda msg: QMessageBox.critical(self, "Error", f"Error al cargar usuarios: {msg}"),
                 parent=self,
             )
         except Exception as e:
             self.unsetCursor()
             QMessageBox.critical(self, "Error", f"Error al cargar usuarios: {e}")

    def _open_user_selection_dialog_with_data(self, usuarios):
         try:
             if not usuarios:
                 QMessageBox.information(self, "Sin Usuarios", "No hay usuarios registrados en el sistema.")
                 return
             dialog = UserSelectionDialog(self, usuarios)
             dialog.setWindowTitle("Seleccionar Usuario para Rutina")
             if dialog.exec():
                 selected_user = dialog.get_selected_user()
                 if selected_user:
                     self.set_user_for_routine(selected_user)
         except Exception as e:
             QMessageBox.critical(self, "Error", f"Error abriendo diálogo: {e}")
    
    def select_routine(self, routine_id: int):
        """Selecciona una rutina por ID sin bloquear la UI (asíncrono)."""
        try:
            # Feedback visual y evitar interacción mientras carga
            self.setCursor(Qt.CursorShape.WaitCursor)
            self.user_routines_combo.setEnabled(False)
            self.export_pdf_button.setEnabled(False)
            self.export_excel_button.setEnabled(False)
            self.delete_routine_button.setEnabled(False)

            # Ejecutar carga en hilo
            thread = TaskThread(self._get_routine_and_user_blocking, routine_id)
            thread.success.connect(lambda result: self._apply_selected_routine_with_data(result, routine_id))
            thread.error.connect(lambda msg: self._handle_select_routine_error(msg))
            thread.finished.connect(lambda: self._restore_select_routine_ui_state())
            thread.start()
            return True
        except Exception as e:
            self._restore_select_routine_ui_state()
            QMessageBox.critical(self, "Error", f"Error al iniciar selección de rutina: {e}")
            return False

    def _get_routine_and_user_blocking(self, routine_id: int):
        """Operación bloqueante: obtiene rutina y usuario asociado."""
        routine = self.db_manager.obtener_rutina_completa(routine_id)
        if not routine:
            raise Exception(f"Rutina con ID {routine_id} no encontrada")

        user = self.db_manager.obtener_usuario_por_id(routine.usuario_id)
        if not user:
            raise Exception(f"Usuario con ID {routine.usuario_id} no encontrado")

        if not getattr(user, 'activo', True):
            # Lanzar como error para manejar en el hilo de UI
            raise Exception(f"Usuario Inactivo: {getattr(user, 'nombre', 'Desconocido')}")

        return {"routine": routine, "user": user}

    def _apply_selected_routine_with_data(self, data: dict, routine_id: int):
        try:
            routine = data.get("routine")
            user = data.get("user")
            if not routine or not user:
                raise Exception("Datos incompletos al cargar rutina")

            # Establecer usuario y buscar rutina en el combo si existe
            self.set_user_for_routine(user)
            for i in range(self.user_routines_combo.count()):
                combo_routine = self.user_routines_combo.itemData(i)
                if combo_routine and getattr(combo_routine, 'id', None) == routine_id:
                    self.user_routines_combo.setCurrentIndex(i)
                    return

            # Si no se encontró en el combo, aplicar directamente
            self.current_routine = routine
            self.routine_name_input.setText(self.current_routine.nombre_rutina)
            self.routine_days_spinbox.setValue(self.current_routine.dias_semana)
            if not hasattr(self, 'routine_weeks') or self.routine_weeks < 1:
                self.routine_weeks = 4
            self.routine_weeks_spinbox.setValue(self.routine_weeks)
            self.details_group.setVisible(True)

            # Cargar ejercicios en estructura por día
            self.exercises_by_day.clear()
            for ej in getattr(self.current_routine, 'ejercicios', []):
                day = getattr(ej, 'dia_semana', None)
                if day is None:
                    continue
                if day not in self.exercises_by_day:
                    self.exercises_by_day[day] = []
                self.exercises_by_day[day].append(ej)

            self.update_day_tabs()
            self.routine_display_label.setVisible(False)
            self.routine_display_tabs.setVisible(True)
            self.export_pdf_button.setEnabled(True)
            self.export_excel_button.setEnabled(True)
            self.delete_routine_button.setEnabled(True)
        except Exception as e:
            QMessageBox.critical(self, "Error", f"Error aplicando rutina: {e}")

    def _handle_select_routine_error(self, msg: str):
        try:
            if msg.startswith("Usuario Inactivo"):
                QMessageBox.warning(self, "Usuario Inactivo", msg)
            else:
                QMessageBox.critical(self, "Error", f"Error al cargar rutina: {msg}")
        except Exception:
            pass

    def _restore_select_routine_ui_state(self):
        try:
            self.unsetCursor()
            self.user_routines_combo.setEnabled(True)
            # No re-habilitamos exportaciones si no hay rutina cargada
            has_routine = getattr(self, 'current_routine', None) is not None
            self.export_pdf_button.setEnabled(bool(has_routine))
            self.export_excel_button.setEnabled(bool(has_routine))
            self.delete_routine_button.setEnabled(bool(has_routine))
        except Exception:
            pass
    
    def load_user_routines_into_combobox(self):
        self.user_routines_combo.blockSignals(True)
        self.user_routines_combo.clear()
        self.user_routines_combo.addItem("Seleccionar rutina existente...", userData=None)
        if self.selected_user:
            try:
                routines = self.db_manager.obtener_rutinas_por_usuario(self.selected_user.id)
                is_active = getattr(self.selected_user, 'activo', True)
                self.user_routines_combo.setEnabled(bool(routines) and bool(is_active))
                for routine in routines:
                    # routine es un diccionario, no un objeto
                    routine_name = routine.get('nombre_rutina', 'Sin nombre') if isinstance(routine, dict) else routine.nombre_rutina
                    dias_val = routine.get('dias_semana') if isinstance(routine, dict) else getattr(routine, 'dias_semana', None)
                    try:
                        dias_text = f" ({int(dias_val)} días)" if dias_val is not None else ""
                    except Exception:
                        dias_text = ""
                    self.user_routines_combo.addItem(f"{routine_name}{dias_text}", userData=routine)
            except Exception as e:
                logging.error(f"Error al cargar rutinas del usuario: {e}")
        self.user_routines_combo.blockSignals(False)
        self.reset_routine_view()

    def reset_routine_view(self):
        self.current_routine = None
        self.exercises_by_day.clear()
        self.details_group.setVisible(False)
        self.routine_display_tabs.setVisible(False)
        self.routine_display_label.setVisible(True)
        self.export_pdf_button.setEnabled(False)
        self.export_excel_button.setEnabled(False)
        self.delete_routine_button.setEnabled(False)
        if self.preset_routines_combo.count() > 0:
            self.preset_routines_combo.setCurrentIndex(0)

    def create_new_routine(self):
        if not self.selected_user: return
        # Bloquear creación de rutinas para usuario 'dueño'
        if getattr(self.selected_user, 'rol', None) == 'dueño':
            QMessageBox.warning(self, "Acción no permitida", "No se puede crear rutinas para el usuario Dueño.")
            return
        # Bloquear creación de rutinas si el usuario está inactivo
        if not getattr(self.selected_user, 'activo', True):
            QMessageBox.warning(self, "Usuario Inactivo", f"{self.selected_user.nombre} está inactivo. No se puede crear una rutina.")
            return
        base_name = f"Nueva Rutina para {self.selected_user.nombre.split()[0]}"
        existing_names = [self.user_routines_combo.itemText(i) for i in range(self.user_routines_combo.count())]
        final_name = base_name
        counter = 2
        while final_name in existing_names:
            final_name = f"{base_name} {counter}"
            counter += 1
        self.current_routine = Rutina(usuario_id=self.selected_user.id, nombre_rutina=final_name, dias_semana=3)
        self.exercises_by_day.clear()
        self.routine_weeks = 4  # Inicializar semanas para nueva rutina
        self.details_group.setVisible(True)
        self.routine_name_input.setText(self.current_routine.nombre_rutina)
        self.routine_days_spinbox.setValue(self.current_routine.dias_semana)
        self.routine_weeks_spinbox.setValue(self.routine_weeks)
        self.update_day_tabs()
        self.routine_display_label.setVisible(False)
        self.routine_display_tabs.setVisible(True)
        self.export_pdf_button.setEnabled(False)
        self.export_excel_button.setEnabled(False)
        self.delete_routine_button.setEnabled(False)
        QMessageBox.information(self, "Rutina Creada", "Configure los detalles, añada ejercicios y guarde los cambios.")

    def load_selected_routine_from_combo(self, index):
        routine_data = self.user_routines_combo.itemData(index)
        if not routine_data:
            self.reset_routine_view()
            return
        # Bloquear carga de rutinas guardadas para usuario inactivo o 'dueño'
        if self.selected_user:
            if getattr(self.selected_user, 'rol', None) == 'dueño':
                QMessageBox.warning(self, "Acción no permitida", "No se puede cargar rutinas guardadas para el usuario Dueño.")
                self.user_routines_combo.setCurrentIndex(0)
                self.reset_routine_view()
                return
            if not getattr(self.selected_user, 'activo', True):
                QMessageBox.warning(self, "Usuario Inactivo", f"{self.selected_user.nombre} está inactivo. No se puede cargar rutinas guardadas.")
                self.user_routines_combo.setCurrentIndex(0)
                self.reset_routine_view()
                return
        # Cargar rutina seleccionada de forma asíncrona para evitar bloqueos
        def _fetch():
            routine_id_local = routine_data.get('id') if isinstance(routine_data, dict) else routine_data.id
            return self.db_manager.obtener_rutina_completa(routine_id_local)

        def _on_success(rutina):
            try:
                self.current_routine = rutina
                if self.current_routine:
                    self.routine_name_input.setText(self.current_routine.nombre_rutina)
                    self.routine_days_spinbox.setValue(self.current_routine.dias_semana)
                    self.details_group.setVisible(True)
                    self.exercises_by_day.clear()
                    for ej in self.current_routine.ejercicios:
                        if ej.dia_semana not in self.exercises_by_day:
                            self.exercises_by_day[ej.dia_semana] = []
                        # Asegurarnos de agregar el ejercicio al día correspondiente
                        self.exercises_by_day[ej.dia_semana].append(ej)

                self.update_day_tabs()
                self.routine_display_label.setVisible(False)
                self.routine_display_tabs.setVisible(True)
                self.export_pdf_button.setEnabled(True)
                self.export_excel_button.setEnabled(True)
                self.delete_routine_button.setEnabled(True)
            except Exception as e:
                QMessageBox.critical(self, "Error", f"No se pudo aplicar la rutina completa: {e}")

        def _on_error(err):
            QMessageBox.critical(self, "Error", f"No se pudo cargar la rutina completa: {err}")

        TaskThread(_fetch, on_success=_on_success, on_error=_on_error).start()

    def delete_selected_routine(self):
        if not self.current_routine or not self.current_routine.id:
            QMessageBox.warning(self, "Sin selección", "No hay una rutina guardada seleccionada para eliminar.")
            return
        reply = QMessageBox.question(self, "Confirmar Eliminación",
                                     f"¿Está seguro de que desea eliminar permanentemente la rutina '{self.current_routine.nombre_rutina}'?",
                                     QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
                                     QMessageBox.StandardButton.No)
        if reply == QMessageBox.StandardButton.Yes:
            try:
                self.db_manager.eliminar_rutina(self.current_routine.id)
                QMessageBox.information(self, "Éxito", "La rutina ha sido eliminada.")
                self.load_user_routines_into_combobox()
            except Exception as e:
                logging.error(f"Error al eliminar la rutina: {e}")
                QMessageBox.critical(self, "Error de Base de Datos", f"No se pudo eliminar la rutina: {e}")

    def apply_preset_routine(self, index):
        if index <= 0 or not self.selected_user: return
        # Bloquear asignación de plantillas para usuario 'dueño'
        if getattr(self.selected_user, 'rol', None) == 'dueño':
            QMessageBox.warning(self, "Acción no permitida", "No se puede asignar plantillas de rutina al usuario Dueño.")
            self.preset_routines_combo.setCurrentIndex(0)
            return
        # Bloquear asignación de plantillas para usuario inactivo
        if not getattr(self.selected_user, 'activo', True):
            QMessageBox.warning(self, "Usuario Inactivo", f"{self.selected_user.nombre} está inactivo. No se puede cargar plantillas de rutina.")
            self.preset_routines_combo.setCurrentIndex(0)
            return
        template_data = self.preset_routines_combo.itemData(index)
        if not template_data: return
        try:
            full_template = self.db_manager.obtener_rutina_completa(template_data.id)
            if not full_template: return
            
            base_name = f"{full_template.nombre_rutina} para {self.selected_user.nombre.split()[0]}"
            existing_names = [self.user_routines_combo.itemText(i) for i in range(self.user_routines_combo.count())]
            final_name = base_name; counter = 2
            while final_name in existing_names:
                final_name = f"{base_name} {counter}"
                counter += 1

            self.current_routine = Rutina(usuario_id=self.selected_user.id, nombre_rutina=final_name, dias_semana=full_template.dias_semana)
            self.exercises_by_day.clear()
            for ej in full_template.ejercicios:
                if ej.dia_semana not in self.exercises_by_day: self.exercises_by_day[ej.dia_semana] = []
                new_rutina_ejercicio = RutinaEjercicio(ejercicio_id=ej.ejercicio_id, dia_semana=ej.dia_semana, series=ej.series, repeticiones=ej.repeticiones, ejercicio=ej.ejercicio)
                self.exercises_by_day[ej.dia_semana].append(new_rutina_ejercicio)
            
            self.details_group.setVisible(True)
            self.routine_name_input.setText(self.current_routine.nombre_rutina)
            self.routine_days_spinbox.setValue(self.current_routine.dias_semana)
            self.update_day_tabs()
            self.routine_display_label.setVisible(False)
            self.routine_display_tabs.setVisible(True)
            self.export_pdf_button.setEnabled(False)
            self.export_excel_button.setEnabled(False)
            self.delete_routine_button.setEnabled(False)
            QMessageBox.information(self, "Plantilla Cargada", "Se ha cargado la plantilla. Revise los detalles y guarde la rutina para el usuario.")
        except Exception as e:
            QMessageBox.critical(self, "Error", f"No se pudo aplicar la plantilla: {e}")
        finally:
            self.preset_routines_combo.setCurrentIndex(0)

    def update_day_tabs(self):
        if not self.current_routine: return
        num_days = self.routine_days_spinbox.value()
        weeks = getattr(self, 'routine_weeks', 1)
        self.routine_display_tabs.clear()
        for day in range(1, num_days + 1):
            day_widget = QWidget()
            day_layout = QVBoxLayout(day_widget)
            table = QTableWidget()
            table.setObjectName("detail_table") # Asignación de Object Name
            table.setSelectionBehavior(QAbstractItemView.SelectionBehavior.SelectRows)
            
            # Calcular número de columnas: Ejercicio + Grupo Muscular + (Series + Repeticiones) * semanas
            num_columns = 2 + (2 * weeks)
            table.setColumnCount(num_columns)
            
            # Generar headers dinámicos
            headers = ["Ejercicio", "Grupo Muscular"]
            for week in range(1, weeks + 1):
                headers.extend([f"S{week}-Series", f"S{week}-Reps"])
            table.setHorizontalHeaderLabels(headers)
            
            table.horizontalHeader().setSectionResizeMode(0, QHeaderView.ResizeMode.Stretch)
            table.setContextMenuPolicy(Qt.ContextMenuPolicy.CustomContextMenu)
            table.customContextMenuRequested.connect(lambda pos, t=table: self.show_exercise_context_menu(pos, t))
            
            # Conectar evento de cambio de celda para capturar ediciones
            table.itemChanged.connect(lambda item, t=table, d=day: self.on_table_item_changed(item, t, d))
            
            self.populate_exercise_table(table, day)
            buttons_layout = QHBoxLayout()
            add_button = QPushButton("➕ Añadir Ejercicio a este día")
            add_button.clicked.connect(lambda _, d=day, t=table: self.add_exercise_to_day(d, t))
            buttons_layout.addWidget(add_button)
            buttons_layout.addStretch()
            day_layout.addLayout(buttons_layout)
            day_layout.addWidget(table)
            self.routine_display_tabs.addTab(day_widget, f"Día {day}")

    def populate_exercise_table(self, table: QTableWidget, day: int):
        exercises = self.exercises_by_day.get(day, [])
        weeks = getattr(self, 'routine_weeks', 1)
        # Limitar a máximo 8 ejercicios por día
        if len(exercises) > 8:
            exercises = exercises[:8]
            self.exercises_by_day[day] = exercises
        table.setRowCount(len(exercises))
        
        for row, ex in enumerate(exercises):
            # Columnas fijas: Ejercicio y Grupo Muscular
            # Acceso robusto tanto si "ejercicio" es dict como si es objeto
            ejercicio_nombre = (
                ex.ejercicio.get('nombre') if isinstance(ex.ejercicio, dict)
                else getattr(ex.ejercicio, 'nombre', '')
            ) or "Ejercicio"
            grupo_muscular = (
                ex.ejercicio.get('grupo_muscular') if isinstance(ex.ejercicio, dict)
                else getattr(ex.ejercicio, 'grupo_muscular', None)
            ) or "N/A"
            table.setItem(row, 0, QTableWidgetItem(ejercicio_nombre))
            table.setItem(row, 1, QTableWidgetItem(grupo_muscular))
            
            # Hacer las primeras dos columnas no editables
            table.item(row, 0).setFlags(table.item(row, 0).flags() & ~Qt.ItemFlag.ItemIsEditable)
            table.item(row, 1).setFlags(table.item(row, 1).flags() & ~Qt.ItemFlag.ItemIsEditable)
            
            # Parsear valores de series y repeticiones para múltiples semanas
            series_values = self.parse_weekly_values(str(ex.series) if ex.series else "", weeks)
            reps_values = self.parse_weekly_values(ex.repeticiones or "", weeks)
            
            # Llenar columnas por semana
            for week in range(weeks):
                series_col = 2 + (week * 2)  # Columna de series para esta semana
                reps_col = 3 + (week * 2)    # Columna de repeticiones para esta semana
                
                # Series
                series_item = QTableWidgetItem(str(series_values[week]))
                table.setItem(row, series_col, series_item)
                
                # Repeticiones
                reps_item = QTableWidgetItem(str(reps_values[week]))
                table.setItem(row, reps_col, reps_item)
            
            # Guardar referencia al ejercicio en la primera columna
            table.item(row, 0).setData(Qt.ItemDataRole.UserRole, ex)

    def parse_weekly_values(self, value_string: str, weeks: int) -> list:
        """Parsea una cadena de valores separados por comas para múltiples semanas"""
        if not value_string or value_string.strip() == "":
            return [""] * weeks
        
        # Dividir por comas y limpiar espacios
        values = [v.strip() for v in value_string.split(",")]
        
        # Si hay menos valores que semanas, rellenar con vacíos (no repetir último valor)
        while len(values) < weeks:
            values.append("")
        
        # Si hay más valores que semanas, tomar solo los primeros
        return values[:weeks]

    def combine_weekly_values(self, table: QTableWidget, row: int, weeks: int, is_series: bool) -> str:
        """Combina los valores de múltiples semanas en una cadena separada por comas sin recortar vacíos finales"""
        values = []
        for week in range(weeks):
            col = 2 + (week * 2) + (0 if is_series else 1)  # Series o repeticiones
            item = table.item(row, col)
            value = item.text().strip() if item else ""
            values.append(value)
        
        # Retener valores vacíos finales para preservar semanas sin datos
        return ",".join(values)

    def on_table_item_changed(self, item: QTableWidgetItem, table: QTableWidget, day: int):
        """Maneja los cambios en las celdas de la tabla para actualizar los datos del ejercicio"""
        if not item:
            return
        
        row = item.row()
        col = item.column()
        weeks = getattr(self, 'routine_weeks', 1)
        
        # Solo procesar cambios en columnas de series y repeticiones (columnas >= 2)
        if col < 2:
            return
        
        # Obtener el ejercicio de la primera columna
        exercise_item = table.item(row, 0)
        if not exercise_item:
            return
        
        exercise: RutinaEjercicio = exercise_item.data(Qt.ItemDataRole.UserRole)
        if not exercise:
            return
        
        # Actualizar los valores combinados del ejercicio
        exercise.series = self.combine_weekly_values(table, row, weeks, True)  # Series
        exercise.repeticiones = self.combine_weekly_values(table, row, weeks, False)  # Repeticiones
        
        # Marcar que hay cambios sin guardar (opcional)
        # self.has_unsaved_changes = True

    def add_exercise_to_day(self, day: int, table: QTableWidget):
        # Enforce máximo 8 ejercicios por día
        current_count = len(self.exercises_by_day.get(day, []))
        if current_count >= 8:
            QMessageBox.warning(self, "Límite alcanzado", "Máximo 8 ejercicios por día.")
            return
        dialog = AddExerciseDialog(self, self.db_manager)
        if dialog.exec():
            new_exercise = dialog.get_rutina_ejercicio()
            if new_exercise:
                if day not in self.exercises_by_day:
                    self.exercises_by_day[day] = []
                new_exercise.dia_semana = day
                self.exercises_by_day[day].append(new_exercise)
                self.populate_exercise_table(table, day)

    def remove_exercise_from_day(self, table: QTableWidget):
        selected_items = table.selectedItems()
        if not selected_items:
            QMessageBox.warning(self, "Sin selección", "Seleccione el ejercicio que desea eliminar.")
            return
        exercise_to_remove: RutinaEjercicio = table.item(table.currentRow(), 0).data(Qt.ItemDataRole.UserRole)
        day = self.routine_display_tabs.currentIndex() + 1
        self.exercises_by_day[day].remove(exercise_to_remove)
        self.populate_exercise_table(table, day)

    def show_exercise_context_menu(self, pos, table: QTableWidget):
        if not table.indexAt(pos).isValid(): return
        menu = QMenu(self)
        menu.setObjectName("contextMenu")
        menu.setProperty("menuType", "exercise")
        remove_action = QAction("🗑️ Eliminar Ejercicio", self)
        remove_action.triggered.connect(lambda: self.remove_exercise_from_day(table))
        menu.addAction(remove_action)
        menu.exec(table.viewport().mapToGlobal(pos))
        
    def save_routine_changes(self, silent: bool = False):
        if not self.current_routine or not self.selected_user:
            QMessageBox.warning(self, "Sin Rutina Activa", "No hay una rutina activa para guardar.")
            return
        # Bloquear guardado/asignación de rutinas para usuario 'dueño'
        if getattr(self.selected_user, 'rol', None) == 'dueño':
            QMessageBox.warning(self, "Acción no permitida", "No se puede guardar/asignar rutinas al usuario Dueño.")
            return
        self.current_routine.nombre_rutina = self.routine_name_input.text().strip()
        self.current_routine.dias_semana = self.routine_days_spinbox.value()
        if not self.current_routine.nombre_rutina:
            QMessageBox.warning(self, "Dato Requerido", "El nombre de la rutina no puede estar vacío.")
            return
        try:
            nombre_rutina_guardada = self.current_routine.nombre_rutina
            if self.current_routine.id:
                self.db_manager.actualizar_rutina(self.current_routine)
            else:
                self.current_routine.id = self.db_manager.crear_rutina(self.current_routine)
            
            # Persistir ejercicios de todos los días (1..5) sin perder los de días no visibles
            persisted_by_day = {}
            for day, day_exercises in (self.exercises_by_day or {}).items():
                try:
                    d = int(day)
                except Exception:
                    continue
                if 1 <= d <= 5:
                    persisted_by_day[d] = (day_exercises or [])[:8]

            all_exercises_to_save = []
            for d in sorted(persisted_by_day.keys()):
                for ex in persisted_by_day[d]:
                    ex.dia_semana = d
                    ex.orden = len(all_exercises_to_save)
                    all_exercises_to_save.append(ex)
            
            self.db_manager.guardar_ejercicios_de_rutina(self.current_routine.id, all_exercises_to_save)
            if not silent:
                QMessageBox.information(self, "Éxito", "La rutina y sus ejercicios se han guardado correctamente.")
                self.load_user_routines_into_combobox()
                index = self.user_routines_combo.findText(nombre_rutina_guardada)
                if index >= 0:
                    self.user_routines_combo.setCurrentIndex(index)
            
            self.export_pdf_button.setEnabled(True)
            self.export_excel_button.setEnabled(True)
            self.delete_routine_button.setEnabled(True)
        except Exception as e:
            logging.error(f"Error al guardar la rutina: {e}", exc_info=True)
            QMessageBox.critical(self, "Error de Base de Datos", f"No se pudo guardar la rutina: {e}")

    def export_to_pdf(self):
        if not self.current_routine or not self.selected_user:
            QMessageBox.warning(self, "Sin Datos", "Debe haber una rutina activa para exportar.")
            return
        
        # Validar que hay ejercicios
        if not self.exercises_by_day:
            QMessageBox.warning(self, "Sin Ejercicios", "La rutina debe tener al menos un ejercicio para exportar.")
            return

        # Guardar cambios automáticamente (silencioso) antes de exportar
        try:
            self.save_routine_changes(silent=True)
        except Exception:
            pass
        
        default_filename = f"rutina_{self.selected_user.nombre.replace(' ', '_')}_{QDate.currentDate().toString('yyyy-MM-dd')}.pdf"
        filepath, _ = QFileDialog.getSaveFileName(self, "Guardar Rutina en PDF", default_filename, "PDF Files (*.pdf)")
        if filepath:
            try:
                # Usar solo los días visibles actuales para la exportación, sin perder datos ocultos
                visible_days = int(self.routine_days_spinbox.value())
                export_exercises = {}
                for day, day_exercises in (self.exercises_by_day or {}).items():
                    try:
                        d = int(day)
                    except Exception:
                        continue
                    if 1 <= d <= visible_days:
                        export_exercises[d] = (day_exercises or [])[:8]
                # Primero generar Excel (mismo nombre, extensión .xlsx)
                try:
                    from pathlib import Path
                    excel_path = str(Path(filepath).with_suffix('.xlsx'))
                    self.routine_manager.generate_routine_excel(
                        self.current_routine,
                        self.selected_user,
                        export_exercises,
                        excel_path,
                        weeks=self.routine_weeks
                    )
                    # Luego convertir ese Excel a PDF en la ruta elegida
                    final_pdf = self.routine_manager.convert_excel_to_pdf(excel_path, filepath)
                    QMessageBox.information(self, "Éxito", f"Rutina exportada a Excel y PDF:\nExcel: {excel_path}\nPDF: {final_pdf}")
                except Exception:
                    # Fallback: generar PDF directamente
                    final_path = self.routine_manager.generate_routine_pdf(
                        self.current_routine, 
                        self.selected_user, 
                        export_exercises,
                        filepath
                    )
                    QMessageBox.information(self, "Éxito", f"Rutina exportada a PDF en:\n{final_path}")
            except Exception as e:
                logging.exception("Error al exportar rutina a PDF")
                QMessageBox.critical(self, "Error de Exportación", f"No se pudo generar el archivo PDF: {e}")

    def export_to_excel(self):
        if not self.current_routine or not self.selected_user:
            QMessageBox.warning(self, "Sin Datos", "Debe haber una rutina activa para exportar.")
            return
        
        # Validar que hay ejercicios
        if not self.exercises_by_day:
            QMessageBox.warning(self, "Sin Ejercicios", "La rutina debe tener al menos un ejercicio para exportar.")
            return

        # Guardar cambios automáticamente (silencioso) antes de exportar
        try:
            self.save_routine_changes(silent=True)
        except Exception:
            pass
        
        default_filename = f"rutina_{self.selected_user.nombre.replace(' ', '_')}_{QDate.currentDate().toString('yyyy-MM-dd')}.xlsx"
        filepath, _ = QFileDialog.getSaveFileName(self, "Guardar Rutina en Excel", default_filename, "Excel Files (*.xlsx)")
        if filepath:
            try:
                # Usar solo los días visibles actuales para la exportación, sin perder datos ocultos
                visible_days = int(self.routine_days_spinbox.value())
                export_exercises = {}
                for day, day_exercises in (self.exercises_by_day or {}).items():
                    try:
                        d = int(day)
                    except Exception:
                        continue
                    if 1 <= d <= visible_days:
                        export_exercises[d] = (day_exercises or [])[:8]
                # Usar routine_manager para generar Excel
                final_path = self.routine_manager.generate_routine_excel(
                    self.current_routine, 
                    self.selected_user, 
                    export_exercises,
                    filepath,
                    weeks=self.routine_weeks  # Pasar el número de semanas
                )
                QMessageBox.information(self, "Éxito", f"Rutina exportada a Excel en:\n{final_path}")
            except Exception as e:
                logging.exception("Error al exportar rutina a Excel")
                QMessageBox.critical(self, "Error de Exportación", f"No se pudo generar el archivo Excel: {e}")

    def add_template(self):
        dialog = TemplateEditorDialog(self, self.db_manager)
        if dialog.exec():
            self.load_preset_routines()

    def edit_template(self):
        current_item = self.template_list.currentItem()
        if not current_item:
            QMessageBox.warning(self, "Sin selección", "Por favor, seleccione una plantilla de la lista para editar.")
            return
        
        template_to_edit = current_item.data(Qt.ItemDataRole.UserRole)
        dialog = TemplateEditorDialog(self, self.db_manager, rutina=template_to_edit)
        if dialog.exec():
            self.load_preset_routines()
    
    def delete_template(self):
        current_item = self.template_list.currentItem()
        if not current_item:
            QMessageBox.warning(self, "Sin selección", "Por favor, seleccione una plantilla de la lista para eliminar.")
            return
        template_to_delete = current_item.data(Qt.ItemDataRole.UserRole)
        if QMessageBox.question(self, "Confirmar Eliminación",
                                     f"¿Está seguro de que desea eliminar la plantilla '{template_to_delete.nombre_rutina}'?",
                                     QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No) == QMessageBox.StandardButton.Yes:
            try:
                self.db_manager.eliminar_rutina(template_to_delete.id)
                self.load_preset_routines()
            except Exception as e:
                QMessageBox.critical(self, "Error", f"No se pudo eliminar la plantilla: {e}")
    
    def duplicate_template(self):
        """Duplica la plantilla seleccionada con un nuevo nombre"""
        current_item = self.template_list.currentItem()
        if not current_item:
            QMessageBox.warning(self, "Sin selección", "Por favor, seleccione una plantilla de la lista para duplicar.")
            return
        
        template_to_duplicate = current_item.data(Qt.ItemDataRole.UserRole)
        
        try:
            # Obtener la plantilla completa con ejercicios
            full_template = self.db_manager.obtener_rutina_completa(template_to_duplicate.id)
            if not full_template:
                QMessageBox.critical(self, "Error", "No se pudo cargar la plantilla completa.")
                return
            
            # Crear una copia de la plantilla con un nuevo nombre
            new_template = Rutina(
                nombre_rutina=f"Copia de {full_template.nombre_rutina}",
                descripcion=full_template.descripcion,
                dias_semana=full_template.dias_semana,
                categoria=full_template.categoria,
                usuario_id=None  # Las plantillas no tienen usuario específico
            )
            
            # Crear la nueva plantilla en la base de datos
            new_template_id = self.db_manager.crear_rutina(new_template)
            new_template.id = new_template_id
            
            # Duplicar todos los ejercicios
            if full_template.ejercicios:
                new_exercises = []
                for ejercicio in full_template.ejercicios:
                    new_exercise = RutinaEjercicio(
                        rutina_id=new_template_id,
                        ejercicio_id=ejercicio.ejercicio_id,
                        dia_semana=ejercicio.dia_semana,
                        series=ejercicio.series,
                        repeticiones=ejercicio.repeticiones,
                        orden=ejercicio.orden,
                        ejercicio=ejercicio.ejercicio  # Mantener la referencia al ejercicio
                    )
                    new_exercises.append(new_exercise)
                
                # Guardar los ejercicios de la nueva plantilla
                self.db_manager.guardar_ejercicios_de_rutina(new_template_id, new_exercises)
            
            # Recargar la lista de plantillas
            self.load_preset_routines()
            
            QMessageBox.information(self, "Éxito", f"Plantilla duplicada exitosamente como '{new_template.nombre_rutina}'.")
            
        except Exception as e:
            QMessageBox.critical(self, "Error", f"No se pudo duplicar la plantilla: {e}")
            logging.error(f"Error al duplicar plantilla: {e}", exc_info=True)
    
    def apply_template_filters(self, filters):
        """Aplica filtros avanzados a la lista de plantillas"""
        try:
            templates = self.db_manager.obtener_plantillas_rutina()
            
            # Aplicar filtros
            templates_filtradas = []
            for template in templates:
                # Filtro por nombre
                if filters.get('nombre') and filters['nombre'].lower() not in template.nombre_rutina.lower():
                    continue
                
                # Filtro por categoría
                if filters.get('categoria') and filters['categoria'] != 'Todas' and template.categoria != filters['categoria']:
                    continue
                
                # Filtro por días mínimos
                if filters.get('dias_min') is not None and template.dias_semana < filters['dias_min']:
                    continue
                
                # Filtro por días máximos
                if filters.get('dias_max') is not None and template.dias_semana > filters['dias_max']:
                    continue
                
                templates_filtradas.append(template)
            
            # Actualizar lista y combo con plantillas filtradas
            self.actualizar_lista_plantillas(templates_filtradas)
            
        except Exception as e:
            QMessageBox.critical(self, "Error", f"No se pudieron aplicar los filtros: {e}")

    def on_template_search_text_changed(self, text: str):
        """Convierte texto de búsqueda en filtros con heurísticas seguras (categoría/días/nombre)."""
        try:
            import re
            import unicodedata
            query = (text or "").strip()
            filters: Dict = {}
            if query:
                ql = query.lower()
                # Normalizar acentos para comparación robusta
                def normalize(s: str) -> str:
                    return ''.join(c for c in unicodedata.normalize('NFD', s) if unicodedata.category(c) != 'Mn').lower()
                qn = normalize(query)

                # Categorías conocidas + sinónimos comunes
                categorias = {
                    "general": {"general"},
                    "fuerza": {"fuerza", "musculacion", "musculación", "power"},
                    "cardio": {"cardio"},
                    "rehabilitación": {"rehabilitacion", "rehab"},
                    "flexibilidad": {"flexibilidad", "flex"},
                    "resistencia": {"resistencia", "endurance"},
                    "coordinación": {"coordinacion"},
                    "equilibrio": {"equilibrio", "balance"},
                }
                for canon, synonyms in categorias.items():
                    if any(normalize(cat) in qn for cat in synonyms):
                        filters['categoria'] = canon
                        break

                # Detección de días: rangos "3-5" o exactos, solo si se menciona "día/días"
                menciona_dias = any(w in qn for w in ["dia", "dias", "día", "días"])
                if menciona_dias:
                    rango_match = re.search(r"(\d{1,2})\s*-\s*(\d{1,2})", qn)
                    if rango_match:
                        try:
                            d1 = int(rango_match.group(1)); d2 = int(rango_match.group(2))
                            filters['dias_min'] = min(d1, d2)
                            filters['dias_max'] = max(d1, d2)
                        except Exception:
                            pass
                    else:
                        # número único acompañado de palabra día/días
                        num_match = re.search(r"(\d{1,2})", qn)
                        if num_match:
                            try:
                                n = int(num_match.group(1))
                                filters['dias_min'] = n
                                filters['dias_max'] = n
                            except Exception:
                                pass

                # Nombre exacto si va entre comillas
                quoted = re.search(r'"([^"]+)"|\'([^\']+)\'', query)
                if quoted:
                    filters['nombre'] = quoted.group(1) or quoted.group(2)
                else:
                    # Búsqueda general por nombre si no se definió categoría o días explícitos
                    filters.setdefault('nombre', query)

            self.apply_template_filters(filters)
        except Exception:
            # Evitar romper la UI
            pass
    
    def actualizar_lista_plantillas(self, templates):
        """Actualiza la lista y combo de plantillas con las plantillas proporcionadas"""
        self.preset_routines_combo.blockSignals(True)
        self.template_list.clear()
        self.preset_routines_combo.clear()
        self.preset_routines_combo.addItem("Seleccionar para cargar...", userData=None)
        
        for t in templates:
            self.preset_routines_combo.addItem(t.nombre_rutina, userData=t)
            item = QListWidgetItem(f"{t.nombre_rutina} ({t.dias_semana} días) - {t.categoria}")
            item.setData(Qt.ItemDataRole.UserRole, t)
            self.template_list.addItem(item)
        
        self.preset_routines_combo.blockSignals(False)

