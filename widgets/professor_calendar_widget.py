from PyQt6.QtWidgets import (
    QWidget, QVBoxLayout, QHBoxLayout, QCalendarWidget, QLabel,
    QPushButton, QListWidget, QListWidgetItem, QGroupBox,
    QFormLayout, QTimeEdit, QComboBox, QTextEdit, QMessageBox,
    QSplitter, QFrame, QGridLayout, QCheckBox
)
from PyQt6.QtCore import Qt, QDate, QTime, pyqtSignal
from PyQt6.QtGui import QFont, QColor
from datetime import datetime, date, time
from typing import Optional, Dict, List

class ProfessorCalendarWidget(QWidget):
    """Widget para gestionar el calendario y disponibilidad de profesores"""
    
    disponibilidad_actualizada = pyqtSignal()
    conflicto_detectado = pyqtSignal(dict)
    horario_guardado = pyqtSignal()
    
    def __init__(self, db_manager, profesor_id: int = None):
        super().__init__()
        self.db_manager = db_manager
        self.profesor_id = profesor_id
        self.selected_date = QDate.currentDate()
        self.setup_ui()
        self.connect_signals()
        self.load_calendar_data()
    
    def setup_ui(self):
        """Configura la interfaz de usuario"""
        layout = QHBoxLayout(self)
        
        # Panel izquierdo - Calendario
        left_panel = self.create_calendar_panel()
        
        # Panel derecho - Detalles y gestión
        right_panel = self.create_details_panel()
        
        # Splitter para dividir los paneles
        splitter = QSplitter(Qt.Orientation.Horizontal)
        splitter.addWidget(left_panel)
        splitter.addWidget(right_panel)
        splitter.setSizes([400, 300])
        
        layout.addWidget(splitter)
    
    def create_calendar_panel(self):
        """Crea el panel del calendario"""
        panel = QWidget()
        layout = QVBoxLayout(panel)
        
        # Título
        title = QLabel("Calendario de Disponibilidad")
        title.setFont(QFont("Arial", 12, QFont.Weight.Bold))
        layout.addWidget(title)
        
        # Widget de calendario
        self.calendar = QCalendarWidget()
        self.calendar.setGridVisible(True)
        self.calendar.setMinimumDate(QDate.currentDate())
        layout.addWidget(self.calendar)
        
        # Leyenda
        legend_frame = QFrame()
        legend_layout = QGridLayout(legend_frame)
        
        # Colores de leyenda
        self.create_legend_item(legend_layout, 0, "Disponible", "#90EE90")
        self.create_legend_item(legend_layout, 1, "Ocupado", "#FFB6C1")
        self.create_legend_item(legend_layout, 2, "Conflicto", "#FF6B6B")
        self.create_legend_item(legend_layout, 3, "Seleccionado", "#87CEEB")
        
        layout.addWidget(legend_frame)
        
        return panel
    
    def create_legend_item(self, layout, row, text, color):
        """Crea un elemento de la leyenda"""
        color_label = QLabel()
        color_label.setFixedSize(20, 20)
        color_label.setStyleSheet(f"background-color: {color}; border: 1px solid black;")
        
        text_label = QLabel(text)
        
        layout.addWidget(color_label, row, 0)
        layout.addWidget(text_label, row, 1)
    
    def create_details_panel(self):
        """Crea el panel de detalles"""
        panel = QWidget()
        layout = QVBoxLayout(panel)
        
        # Información del día seleccionado
        self.date_info_group = QGroupBox("Información del Día")
        date_info_layout = QVBoxLayout(self.date_info_group)
        
        self.selected_date_label = QLabel("Fecha: No seleccionada")
        self.availability_status_label = QLabel("Estado: -")
        
        date_info_layout.addWidget(self.selected_date_label)
        date_info_layout.addWidget(self.availability_status_label)
        
        layout.addWidget(self.date_info_group)
        
        # Horarios del día
        self.schedules_group = QGroupBox("Horarios del Día")
        schedules_layout = QVBoxLayout(self.schedules_group)
        
        self.schedules_list = QListWidget()
        schedules_layout.addWidget(self.schedules_list)
        
        layout.addWidget(self.schedules_group)
        
        # Gestión de disponibilidad
        self.availability_group = QGroupBox("Gestionar Disponibilidad")
        availability_layout = QFormLayout(self.availability_group)
        
        self.start_time_edit = QTimeEdit()
        self.start_time_edit.setTime(QTime(9, 0))
        self.end_time_edit = QTimeEdit()
        self.end_time_edit.setTime(QTime(17, 0))
        
        self.availability_combo = QComboBox()
        self.availability_combo.addItems(["Disponible", "No Disponible", "Ocupado"])
        
        self.notes_edit = QTextEdit()
        self.notes_edit.setMaximumHeight(80)
        
        availability_layout.addRow("Hora Inicio:", self.start_time_edit)
        availability_layout.addRow("Hora Fin:", self.end_time_edit)
        availability_layout.addRow("Estado:", self.availability_combo)
        availability_layout.addRow("Notas:", self.notes_edit)
        
        layout.addWidget(self.availability_group)
        
        # Botones de acción
        buttons_layout = QHBoxLayout()
        
        self.save_availability_btn = QPushButton("Guardar Disponibilidad")
        self.delete_availability_btn = QPushButton("Eliminar")
        self.refresh_btn = QPushButton("Actualizar")
        
        buttons_layout.addWidget(self.save_availability_btn)
        buttons_layout.addWidget(self.delete_availability_btn)
        buttons_layout.addWidget(self.refresh_btn)
        
        layout.addLayout(buttons_layout)
        
        # Conflictos detectados
        self.conflicts_group = QGroupBox("Conflictos Detectados")
        conflicts_layout = QVBoxLayout(self.conflicts_group)
        
        self.conflicts_list = QListWidget()
        conflicts_layout.addWidget(self.conflicts_list)
        
        layout.addWidget(self.conflicts_group)
        
        return panel
    
    def connect_signals(self):
        """Conecta las señales"""
        self.calendar.selectionChanged.connect(self.on_date_selected)
        self.save_availability_btn.clicked.connect(self.save_availability)
        self.delete_availability_btn.clicked.connect(self.delete_availability)
        self.refresh_btn.clicked.connect(self.refresh_calendar)
    
    def on_date_selected(self):
        """Maneja la selección de una fecha"""
        self.selected_date = self.calendar.selectedDate()
        self.update_date_info()
        self.load_day_schedules()
        self.check_conflicts()
    
    def update_date_info(self):
        """Actualiza la información del día seleccionado"""
        date_str = self.selected_date.toString("dddd, dd MMMM yyyy")
        self.selected_date_label.setText(f"Fecha: {date_str}")
        
        # Obtener estado de disponibilidad
        availability = self.get_availability_for_date(self.selected_date)
        if availability:
            status = availability.get('status', 'Desconocido')
            self.availability_status_label.setText(f"Estado: {status}")
            
            # Cargar datos en el formulario
            start_time = QTime.fromString(availability.get('start_time', '09:00'), 'hh:mm')
            end_time = QTime.fromString(availability.get('end_time', '17:00'), 'hh:mm')
            
            self.start_time_edit.setTime(start_time)
            self.end_time_edit.setTime(end_time)
            self.availability_combo.setCurrentText(status)
            self.notes_edit.setPlainText(availability.get('notes', ''))
        else:
            self.availability_status_label.setText("Estado: Sin definir")
            self.clear_form()
    
    def load_day_schedules(self):
        """Carga los horarios del día seleccionado"""
        self.schedules_list.clear()
        
        if not self.profesor_id:
            return
        
        try:
            # Obtener horarios del profesor desde horarios_profesores
            query = """
                SELECT hora_inicio, hora_fin, dia_semana, disponible
                FROM horarios_profesores
                WHERE profesor_id = %s
            """
            
            with self.db_manager.get_connection_context() as conn:
                cursor = conn.cursor()
                cursor.execute(query, (self.profesor_id,))
                schedules = cursor.fetchall()
            
            # Filtrar por día de la semana (mapear a texto)
            dias_semana = {
                1: 'Lunes', 2: 'Martes', 3: 'Miércoles', 4: 'Jueves',
                5: 'Viernes', 6: 'Sábado', 7: 'Domingo'
            }
            dia_texto = dias_semana.get(self.selected_date.dayOfWeek())
            
            for schedule in schedules:
                if schedule[2] == dia_texto:  # dia_semana (texto)
                    estado = "Disponible" if (schedule[3] in (1, True, '1')) else "No disponible"
                    item_text = f"{schedule[0]} - {schedule[1]}: {estado}"
                    item = QListWidgetItem(item_text)
                    self.schedules_list.addItem(item)
                    
        except Exception as e:
            print(f"Error cargando horarios del día: {e}")
    
    def get_availability_for_date(self, date: QDate) -> Optional[Dict]:
        """Obtiene la disponibilidad para una fecha específica"""
        if not self.profesor_id:
            return None
        
        try:
            date_str = date.toString("yyyy-MM-dd")
            query = """
                SELECT start_time, end_time, status, notes
                FROM professor_availability
                WHERE profesor_id = %s AND date = %s
            """

            # Si hay circuito abierto, usar caché como fallback y evitar acceso DB
            try:
                if hasattr(self.db_manager, 'is_circuit_open') and self.db_manager.is_circuit_open():
                    cached = None
                    try:
                        cached = self.db_manager.cache.get('availability', f"{self.profesor_id}:{date_str}")
                    except Exception:
                        cached = None
                    return cached
            except Exception:
                pass

            # Intentar caché primero
            try:
                cached = self.db_manager.cache.get('availability', f"{self.profesor_id}:{date_str}")
                if cached is not None:
                    return cached
            except Exception:
                pass

            # Consulta endurecida con timeouts de lectura
            with self.db_manager.get_connection_context() as conn:
                cursor = conn.cursor()
                try:
                    if hasattr(self.db_manager, '_apply_readonly_timeouts'):
                        self.db_manager._apply_readonly_timeouts(cursor, lock_ms=800, statement_ms=1500, idle_s=2)
                    else:
                        cursor.execute("SET LOCAL lock_timeout = '800ms'")
                        cursor.execute("SET LOCAL statement_timeout = '2000ms'")
                        cursor.execute("SET LOCAL default_transaction_read_only = on")
                except Exception:
                    pass
                cursor.execute(query, (self.profesor_id, date_str))
            result = cursor.fetchone()

            if result:
                out = {
                    'start_time': result[0],
                    'end_time': result[1],
                    'status': result[2],
                    'notes': result[3]
                }
                try:
                    self.db_manager.cache.set('availability', f"{self.profesor_id}:{date_str}", out)
                except Exception:
                    pass
                return out

        except Exception as e:
            print(f"Error obteniendo disponibilidad: {e}")
        
        return None
    
    def save_availability(self):
        """Guarda la disponibilidad del profesor"""
        if not self.profesor_id:
            QMessageBox.warning(self, "Error", "No hay profesor seleccionado")
            return
        
        try:
            date_str = self.selected_date.toString("yyyy-MM-dd")
            start_time = self.start_time_edit.time().toString("hh:mm")
            end_time = self.end_time_edit.time().toString("hh:mm")
            status = self.availability_combo.currentText()
            notes = self.notes_edit.toPlainText()
            
            # Verificar que la hora de inicio sea menor que la de fin
            if self.start_time_edit.time() >= self.end_time_edit.time():
                QMessageBox.warning(self, "Error", "La hora de inicio debe ser menor que la hora de fin")
                return
            
            # Crear tabla si no existe
            self.create_availability_table()
            
            # Insertar o actualizar disponibilidad
            query = """
                INSERT INTO professor_availability 
                (profesor_id, date, start_time, end_time, status, notes, created_at)
                VALUES (%s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
                ON CONFLICT (profesor_id, date) DO UPDATE SET
                    start_time = EXCLUDED.start_time,
                    end_time = EXCLUDED.end_time,
                    status = EXCLUDED.status,
                    notes = EXCLUDED.notes,
                    created_at = CURRENT_TIMESTAMP
            """
            
            with self.db_manager.get_connection_context() as conn:
                cursor = conn.cursor()
                cursor.execute(query, (self.profesor_id, date_str, start_time, end_time, status, notes))
                conn.commit()
            
            QMessageBox.information(self, "Éxito", "Disponibilidad guardada correctamente")
            self.disponibilidad_actualizada.emit()
            self.refresh_calendar()
            
        except Exception as e:
            QMessageBox.critical(self, "Error", f"Error guardando disponibilidad: {e}")
    
    def delete_availability(self):
        """Elimina la disponibilidad del día seleccionado"""
        if not self.profesor_id:
            return
        
        reply = QMessageBox.question(
            self, "Confirmar", 
            "¿Está seguro de eliminar la disponibilidad para este día?",
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No
        )
        
        if reply == QMessageBox.StandardButton.Yes:
            try:
                date_str = self.selected_date.toString("yyyy-MM-dd")
                
                query = "DELETE FROM professor_availability WHERE profesor_id = %s AND date = %s"
                with self.db_manager.get_connection_context() as conn:
                    cursor = conn.cursor()
                    cursor.execute(query, (self.profesor_id, date_str))
                    conn.commit()
                
                QMessageBox.information(self, "Éxito", "Disponibilidad eliminada")
                self.clear_form()
                self.disponibilidad_actualizada.emit()
                self.refresh_calendar()
                
            except Exception as e:
                QMessageBox.critical(self, "Error", f"Error eliminando disponibilidad: {e}")
    
    def create_availability_table(self):
        """Crea la tabla de disponibilidad si no existe"""
        try:
            query = """
                CREATE TABLE IF NOT EXISTS professor_availability (
                    id SERIAL PRIMARY KEY,
                    profesor_id INTEGER NOT NULL REFERENCES profesores(id) ON DELETE CASCADE,
                    date DATE NOT NULL,
                    start_time TIME NOT NULL,
                    end_time TIME NOT NULL,
                    status VARCHAR(50) NOT NULL,
                    notes TEXT,
                    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(profesor_id, date)
                )
            """
            
            with self.db_manager.get_connection_context() as conn:
                cursor = conn.cursor()
                cursor.execute(query)
                conn.commit()
            
        except Exception as e:
            print(f"Error creando tabla de disponibilidad: {e}")
    
    def check_conflicts(self):
        """Verifica conflictos en la fecha seleccionada"""
        self.conflicts_list.clear()
        
        if not self.profesor_id:
            return
        
        conflicts = self.detect_conflicts_for_date(self.selected_date)
        
        for conflict in conflicts:
            item = QListWidgetItem(conflict['message'])
            item.setData(Qt.ItemDataRole.UserRole, conflict)
            self.conflicts_list.addItem(item)
            
            # Emitir señal de conflicto detectado
            self.conflicto_detectado.emit(conflict)
    
    def detect_conflicts_for_date(self, date: QDate) -> List[Dict]:
        """Detecta conflictos para una fecha específica usando horarios_profesores"""
        conflicts = []
        
        try:
            # Mapear día a texto
            dias_semana = {
                1: 'Lunes', 2: 'Martes', 3: 'Miércoles', 4: 'Jueves',
                5: 'Viernes', 6: 'Sábado', 7: 'Domingo'
            }
            dia_texto = dias_semana.get(date.dayOfWeek())
            
            query = """
                SELECT h1.id, h1.hora_inicio, h1.hora_fin, h1.disponible,
                       h2.id, h2.hora_inicio, h2.hora_fin, h2.disponible
                FROM horarios_profesores h1
                JOIN horarios_profesores h2 ON h1.profesor_id = h2.profesor_id 
                    AND h1.dia_semana = h2.dia_semana 
                    AND h1.id < h2.id
                WHERE h1.profesor_id = %s AND h1.dia_semana = %s
                    AND (
                        (h1.hora_inicio < h2.hora_fin AND h1.hora_fin > h2.hora_inicio)
                    )
            """
            
            with self.db_manager.get_connection_context() as conn:
                cursor = conn.cursor()
                cursor.execute(query, (self.profesor_id, dia_texto))
                overlapping_schedules = cursor.fetchall()
            
            for overlap in overlapping_schedules:
                conflict = {
                    'type': 'schedule_overlap',
                    'date': date.toString("yyyy-MM-dd"),
                    'message': f"Superposición de horarios del profesor: ({overlap[1]}-{overlap[2]}) con ({overlap[5]}-{overlap[6]})",
                    'schedule1_id': overlap[0],
                    'schedule2_id': overlap[4]
                }
                conflicts.append(conflict)
        
        except Exception as e:
            print(f"Error detectando conflictos: {e}")
        
        return conflicts
    
    def clear_form(self):
        """Limpia el formulario"""
        self.start_time_edit.setTime(QTime(9, 0))
        self.end_time_edit.setTime(QTime(17, 0))
        self.availability_combo.setCurrentIndex(0)
        self.notes_edit.clear()
    
    def refresh_calendar(self):
        """Actualiza el calendario"""
        self.load_calendar_data()
        self.update_date_info()
        self.load_day_schedules()
        self.check_conflicts()
    
    def load_calendar_data(self):
        """Carga los datos del calendario"""
        if not self.profesor_id:
            return
        
        # Aquí se podría implementar la lógica para colorear las fechas
        # según la disponibilidad del profesor
        pass
    
    def set_profesor_id(self, profesor_id: int):
        """Establece el ID del profesor"""
        self.profesor_id = profesor_id
        self.refresh_calendar()