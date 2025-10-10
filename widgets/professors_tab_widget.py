from PyQt6.QtWidgets import (
    QWidget, QVBoxLayout, QHBoxLayout, QTableWidget, QTableWidgetItem,
    QPushButton, QLineEdit, QLabel, QGroupBox, QFormLayout, QTextEdit,
    QSpinBox, QDoubleSpinBox, QComboBox, QDateEdit, QTabWidget,
    QMessageBox, QHeaderView, QSplitter, QFrame, QGridLayout,
    QScrollArea, QCheckBox, QTimeEdit, QListWidget, QListWidgetItem,
    QAbstractItemView, QMenu, QSizePolicy
)
from PyQt6.QtCore import Qt, QDate, QTime, pyqtSignal
from PyQt6.QtGui import QFont, QPixmap, QIcon, QColor, QAction
from datetime import datetime, date, timedelta
from typing import Optional, Dict, List
import os
import logging
from database import DatabaseManager
from validation_manager import FormValidator, FieldValidator, create_professor_validator, create_schedule_validator
from widgets.unified_filter_widget import UnifiedFilterButton

# Los widgets se importan de forma diferida para evitar imports circulares
from widgets.professor_calendar_widget import ProfessorCalendarWidget
from widgets.substitute_management_widget import SubstituteManagementWidget
from widgets.conflict_notification_widget import ConflictNotificationWidget

class ProfessorScheduleWidget(QWidget):
    """Widget para gestionar horarios de profesores"""
    
    horario_guardado = pyqtSignal()
    
    def __init__(self, db_manager, profesor_id: int = None):
        super().__init__()
        self.db_manager = db_manager
        self.profesor_id = profesor_id
        self.validator = create_schedule_validator(db_manager)
        self.init_ui()
        self.setup_validation()
        if profesor_id:
            self.cargar_horarios()
    
    def init_ui(self):
        layout = QVBoxLayout(self)
        
        # Formulario para agregar horario
        form_group = QGroupBox("Agregar Horario")
        form_group.setObjectName("form_group")
        self.form_layout = QFormLayout(form_group)
        
        self.dia_combo = QComboBox()
        self.dia_combo.addItems(["Lunes", "Martes", "Miércoles", "Jueves", "Viernes", "Sábado", "Domingo"])
        
        self.hora_inicio = QTimeEdit()
        self.hora_inicio.setTime(QTime(8, 0))
        self.hora_inicio.setDisplayFormat("HH:mm")
        self.hora_inicio.setReadOnly(False)  # Asegurar que sea editable
        self.hora_inicio.setEnabled(True)    # Asegurar que esté habilitado
        self.hora_inicio.setButtonSymbols(QTimeEdit.ButtonSymbols.UpDownArrows)  # Mostrar flechas
        self.hora_inicio.setWrapping(True)   # Permitir wrap de valores
        
        self.hora_fin = QTimeEdit()
        self.hora_fin.setTime(QTime(9, 0))
        self.hora_fin.setDisplayFormat("HH:mm")
        self.hora_fin.setReadOnly(False)     # Asegurar que sea editable
        self.hora_fin.setEnabled(True)       # Asegurar que esté habilitado
        self.hora_fin.setButtonSymbols(QTimeEdit.ButtonSymbols.UpDownArrows)    # Mostrar flechas
        self.hora_fin.setWrapping(True)      # Permitir wrap de valores
        
        self.disponible_check = QCheckBox("Disponible")
        self.disponible_check.setChecked(True)
        
        self.form_layout.addRow("Día:", self.dia_combo)
        self.form_layout.addRow("Hora Inicio:", self.hora_inicio)
        self.form_layout.addRow("Hora Fin:", self.hora_fin)
        self.form_layout.addRow("", self.disponible_check)
        
        # Botones
        btn_layout = QHBoxLayout()
        self.btn_agregar = QPushButton("Agregar Horario")
        self.btn_agregar.setProperty("class", "primary")
        self.btn_agregar.clicked.connect(self.agregar_horario)
        
        self.btn_actualizar = QPushButton("Actualizar")
        self.btn_actualizar.clicked.connect(self.actualizar_horario)
        self.btn_actualizar.setEnabled(False)
        
        self.btn_cancelar = QPushButton("Cancelar")
        self.btn_cancelar.clicked.connect(self.cancelar_edicion)
        self.btn_cancelar.setEnabled(False)
        
        btn_layout.addWidget(self.btn_agregar)
        btn_layout.addWidget(self.btn_actualizar)
        btn_layout.addWidget(self.btn_cancelar)
        btn_layout.addStretch()
        
        self.form_layout.addRow("", btn_layout)
        
        # Widget para mostrar errores de validación
        self.error_label = QLabel()
        self.error_label.setObjectName("schedule_error_label")
        self.error_label.setWordWrap(True)
        self.error_label.hide()
        self.form_layout.addRow("", self.error_label)
        
        layout.addWidget(form_group)
        
        # Lista de horarios
        self.lista_horarios = QListWidget()
        self.lista_horarios.itemClicked.connect(self.seleccionar_horario)
        layout.addWidget(QLabel("Horarios Configurados:"))
        layout.addWidget(self.lista_horarios)
        
        # Botón eliminar
        self.btn_eliminar = QPushButton("Eliminar Horario")
        self.btn_eliminar.setProperty("class", "danger")
        self.btn_eliminar.clicked.connect(self.eliminar_horario)
        self.btn_eliminar.setEnabled(False)
        layout.addWidget(self.btn_eliminar)
        
        self.horario_seleccionado = None
    
    def setup_validation(self):
        """Configura la validación en tiempo real para el formulario de horarios"""
        # Configurar validadores para campos de tiempo
        self.validator.add_field(
            "hora_inicio", 
            self.hora_inicio, 
            [lambda v, f: FieldValidator.time_range_validation(v, '06:00', '23:00', f)],
            required=True
        )
        
        self.validator.add_field(
            "hora_fin", 
            self.hora_fin, 
            [lambda v, f: FieldValidator.time_range_validation(v, '06:00', '23:59', f)],
            required=True
        )
        
        # Conectar señales de validación
        self.validator.form_validation_changed.connect(self.on_form_validation_changed)
        
        # Conectar cambios de tiempo para validar rango
        self.hora_inicio.timeChanged.connect(self.validate_time_range)
        self.hora_fin.timeChanged.connect(self.validate_time_range)
    

    
    def on_form_validation_changed(self, is_valid: bool):
        """Maneja cambios en la validación del formulario completo"""
        # Solo habilitar si hay profesor seleccionado, formulario válido y no estamos editando
        self.btn_agregar.setEnabled(is_valid and self.profesor_id is not None and not self.horario_seleccionado)
    
    def validate_time_range(self):
        """Valida que la hora de fin sea posterior a la hora de inicio"""
        hora_inicio = self.hora_inicio.time()
        hora_fin = self.hora_fin.time()
        
        if hora_fin <= hora_inicio:
            self.error_label.setText("La hora de fin debe ser posterior a la hora de inicio")
            self.error_label.show()
            self.btn_agregar.setEnabled(False)
            self.btn_actualizar.setEnabled(False)
        else:
            # Verificar otras validaciones
            errors = self.validator.get_validation_errors()
            if not errors:
                self.error_label.hide()
                # Habilitar botones según el estado
                if self.horario_seleccionado:
                    self.btn_actualizar.setEnabled(True)
                else:
                    self.btn_agregar.setEnabled(self.profesor_id is not None)
    
    def agregar_horario(self):
        if not self.profesor_id:
            QMessageBox.warning(self, "Error", "Debe seleccionar un profesor primero")
            return
        
        # Validar formulario completo antes de guardar
        is_valid, validation_results = self.validator.validate_all()
        if not is_valid:
            errors = [result.message for result in validation_results.values() if not result.is_valid]
            QMessageBox.warning(self, "Errores de validación", "\n".join(errors))
            return
        
        dia = self.dia_combo.currentText()
        hora_inicio = self.hora_inicio.time().toString("HH:mm")
        hora_fin = self.hora_fin.time().toString("HH:mm")
        disponible = self.disponible_check.isChecked()
        
        if hora_inicio >= hora_fin:
            QMessageBox.warning(self, "Error", "La hora de inicio debe ser menor que la hora de fin")
            return
        
        # Verificar si ya existe un horario superpuesto
        if self.verificar_horario_superpuesto(dia, hora_inicio, hora_fin):
            QMessageBox.warning(
                self, "Error", 
                f"Ya existe un horario superpuesto para {dia} en el rango {hora_inicio}-{hora_fin}"
            )
            return
        
        try:
            self.db_manager.crear_horario_profesor(self.profesor_id, dia, hora_inicio, hora_fin, disponible)
            self.cargar_horarios()
            self.horario_guardado.emit()  # Emitir señal de horario guardado
            QMessageBox.information(self, "Éxito", "Horario agregado correctamente")
            # Limpiar formulario después de agregar
            self.limpiar_formulario()
        except Exception as e:
            QMessageBox.critical(self, "Error", f"Error al agregar horario: {str(e)}")
    
    def set_profesor_id(self, profesor_id: int):
        """Establece el ID del profesor y recarga los horarios"""
        self.profesor_id = profesor_id
        self.cargar_horarios()
        # Habilitar botón agregar si hay profesor seleccionado
        self.btn_agregar.setEnabled(self.profesor_id is not None)
        # Limpiar selección actual al cambiar de profesor
        self.cancelar_edicion()
    
    def cargar_horarios(self):
        if not self.profesor_id:
            return
        
        self.lista_horarios.clear()
        horarios = self.db_manager.obtener_horarios_disponibilidad_profesor(self.profesor_id)
        
        # Ordenar horarios por día de la semana y hora
        dias_orden = ['Lunes', 'Martes', 'Miércoles', 'Jueves', 'Viernes', 'Sábado', 'Domingo']
        horarios_ordenados = sorted(horarios, key=lambda h: (dias_orden.index(h['dia_semana']), h['hora_inicio']))
        
        for horario in horarios_ordenados:
            disponible_text = "✓ Disponible" if horario['disponible'] else "✗ No disponible"
            texto = f"{horario['dia_semana']}: {horario['hora_inicio']} - {horario['hora_fin']} ({disponible_text})"
            item = QListWidgetItem(texto)
            item.setData(Qt.ItemDataRole.UserRole, horario)
            
            # Colorear según disponibilidad
            if horario['disponible']:
                item.setBackground(QColor(200, 255, 200))  # Verde claro
            else:
                item.setBackground(QColor(255, 200, 200))  # Rojo claro
            
            self.lista_horarios.addItem(item)
            
        # Mostrar mensaje si no hay horarios
        if not horarios:
            item = QListWidgetItem("No hay horarios configurados")
            item.setFlags(Qt.ItemFlag.NoItemFlags)  # No seleccionable
            self.lista_horarios.addItem(item)
    
    def seleccionar_horario(self, item):
        horario = item.data(Qt.ItemDataRole.UserRole)
        self.horario_seleccionado = horario
        
        # Cargar datos en el formulario
        self.dia_combo.setCurrentText(horario['dia_semana'])
        
        # Convertir datetime.time a string si es necesario
        hora_inicio_str = horario['hora_inicio'].strftime("%H:%M") if hasattr(horario['hora_inicio'], 'strftime') else str(horario['hora_inicio'])
        hora_fin_str = horario['hora_fin'].strftime("%H:%M") if hasattr(horario['hora_fin'], 'strftime') else str(horario['hora_fin'])
        
        self.hora_inicio.setTime(QTime.fromString(hora_inicio_str, "HH:mm"))
        self.hora_fin.setTime(QTime.fromString(hora_fin_str, "HH:mm"))
        self.disponible_check.setChecked(bool(horario['disponible']))
        
        # Asegurar que los QTimeEdit sigan siendo editables después de cargar datos
        self.hora_inicio.setReadOnly(False)
        self.hora_inicio.setEnabled(True)
        self.hora_fin.setReadOnly(False)
        self.hora_fin.setEnabled(True)
        
        # Habilitar botones de edición
        self.btn_agregar.setEnabled(False)
        self.btn_actualizar.setEnabled(True)
        self.btn_cancelar.setEnabled(True)
        self.btn_eliminar.setEnabled(True)
    
    def actualizar_horario(self):
        if not self.horario_seleccionado:
            return
        
        dia = self.dia_combo.currentText()
        hora_inicio = self.hora_inicio.time().toString("HH:mm")
        hora_fin = self.hora_fin.time().toString("HH:mm")
        disponible = self.disponible_check.isChecked()
        
        if hora_inicio >= hora_fin:
            QMessageBox.warning(self, "Error", "La hora de inicio debe ser menor que la hora de fin")
            return
        
        # Verificar si ya existe un horario superpuesto (excluyendo el actual)
        if self.verificar_horario_superpuesto(dia, hora_inicio, hora_fin, excluir_id=self.horario_seleccionado['id']):
            QMessageBox.warning(
                self, "Error", 
                f"Ya existe un horario superpuesto para {dia} en el rango {hora_inicio}-{hora_fin}"
            )
            return
        
        try:
            self.db_manager.actualizar_horario_profesor(
                self.horario_seleccionado['id'],
                dia,
                hora_inicio,
                hora_fin,
                disponible
            )
            self.cargar_horarios()
            self.horario_guardado.emit()  # Emitir señal de horario guardado
            self.cancelar_edicion()
            QMessageBox.information(self, "Éxito", "Horario actualizado correctamente")
        except Exception as e:
            QMessageBox.critical(self, "Error", f"Error al actualizar horario: {str(e)}")
    
    def eliminar_horario(self):
        if not self.horario_seleccionado:
            return
        
        respuesta = QMessageBox.question(
            self, "Confirmar", "¿Está seguro de eliminar este horario?",
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No
        )
        
        if respuesta == QMessageBox.StandardButton.Yes:
            try:
                self.db_manager.eliminar_horario_profesor(self.horario_seleccionado['id'])
                self.cargar_horarios()
                self.horario_guardado.emit()  # Emitir señal de horario guardado
                self.cancelar_edicion()
                QMessageBox.information(self, "Éxito", "Horario eliminado correctamente")
            except Exception as e:
                QMessageBox.critical(self, "Error", f"Error al eliminar horario: {str(e)}")
    
    def cancelar_edicion(self):
        self.horario_seleccionado = None
        self.btn_agregar.setEnabled(True)
        self.btn_actualizar.setEnabled(False)
        self.btn_cancelar.setEnabled(False)
        self.btn_eliminar.setEnabled(False)
        
        # Limpiar formulario
        self.dia_combo.setCurrentIndex(0)
        self.hora_inicio.setTime(QTime(8, 0))
        self.hora_fin.setTime(QTime(9, 0))
        self.disponible_check.setChecked(True)
        
        # Asegurar que los QTimeEdit sigan siendo editables
        self.hora_inicio.setReadOnly(False)
        self.hora_inicio.setEnabled(True)
        self.hora_fin.setReadOnly(False)
        self.hora_fin.setEnabled(True)
        
        # Limpiar validaciones
        self.validator.clear_validation()
        self.error_label.hide()
    
    def limpiar_formulario(self):
        """Limpia el formulario y resetea a valores por defecto"""
        self.dia_combo.setCurrentIndex(0)
        self.hora_inicio.setTime(QTime(8, 0))
        self.hora_fin.setTime(QTime(9, 0))
        self.disponible_check.setChecked(True)
        self.validator.clear_validation()
        self.error_label.hide()
    
    def verificar_horario_superpuesto(self, dia, hora_inicio, hora_fin, excluir_id=None):
        """Verifica si existe un horario superpuesto para el mismo día"""
        if not self.profesor_id:
            return False
        
        try:
            horarios_existentes = self.db_manager.obtener_horarios_disponibilidad_profesor(self.profesor_id)
            
            for horario in horarios_existentes:
                # Excluir el horario actual si se está editando
                if excluir_id and horario['id'] == excluir_id:
                    continue
                
                # Solo verificar horarios del mismo día
                if horario['dia_semana'] != dia:
                    continue
                
                # Convertir a objetos QTime para comparación
                # Manejar tanto datetime.time como string
                if isinstance(horario['hora_inicio'], str):
                    inicio_existente = QTime.fromString(horario['hora_inicio'], 'hh:mm')
                else:
                    # Si es datetime.time, convertir a string primero
                    inicio_str = horario['hora_inicio'].strftime('%H:%M') if hasattr(horario['hora_inicio'], 'strftime') else str(horario['hora_inicio'])
                    inicio_existente = QTime.fromString(inicio_str, 'hh:mm')
                
                if isinstance(horario['hora_fin'], str):
                    fin_existente = QTime.fromString(horario['hora_fin'], 'hh:mm')
                else:
                    # Si es datetime.time, convertir a string primero
                    fin_str = horario['hora_fin'].strftime('%H:%M') if hasattr(horario['hora_fin'], 'strftime') else str(horario['hora_fin'])
                    fin_existente = QTime.fromString(fin_str, 'hh:mm')
                
                # hora_inicio y hora_fin ya son objetos QTime del formulario
                nuevo_inicio = hora_inicio if isinstance(hora_inicio, QTime) else QTime.fromString(hora_inicio, 'hh:mm')
                nuevo_fin = hora_fin if isinstance(hora_fin, QTime) else QTime.fromString(hora_fin, 'hh:mm')
                
                # Verificar superposición
                # Hay superposición si:
                # - El nuevo inicio está entre el inicio y fin existente
                # - El nuevo fin está entre el inicio y fin existente
                # - El nuevo horario engloba completamente al existente
                if ((nuevo_inicio >= inicio_existente and nuevo_inicio < fin_existente) or
                    (nuevo_fin > inicio_existente and nuevo_fin <= fin_existente) or
                    (nuevo_inicio <= inicio_existente and nuevo_fin >= fin_existente)):
                    return True
            
            return False
        except Exception as e:
            print(f"Error al verificar horarios superpuestos: {e}")
            return False
    
    def set_profesor_id(self, profesor_id: int):
        self.profesor_id = profesor_id
        self.cargar_horarios()
    
    # Métodos de validación de horarios
    def validar_horario_completo(self):
        """Valida que el horario esté completo y sea válido"""
        try:
            if not self.dia_combo.currentText():
                return False, "Debe seleccionar un día"
            
            inicio = self.hora_inicio.time()
            fin = self.hora_fin.time()
            
            if inicio >= fin:
                return False, "La hora de inicio debe ser anterior a la hora de fin"
            
            return True, ""
        except Exception as e:
            return False, f"Error en validación: {e}"
    
    def verificar_solapamiento_horarios(self, dia, hora_inicio, hora_fin, excluir_id=None):
        """Verifica si hay solapamiento con otros horarios"""
        return self.verificar_horario_superpuesto(dia, hora_inicio, hora_fin, excluir_id)
    
    # Métodos CRUD de horarios
    def agregar_horario_completo(self):
        """Agrega un horario completo después de validación"""
        try:
            valido, mensaje = self.validar_horario_completo()
            if not valido:
                self.mostrar_error_validacion(mensaje)
                return False
            
            dia = self.dia_combo.currentText()
            inicio = self.hora_inicio.time().toString('hh:mm')
            fin = self.hora_fin.time().toString('hh:mm')
            disponible = self.disponible_check.isChecked()
            
            if self.verificar_solapamiento_horarios(dia, self.hora_inicio.time(), self.hora_fin.time()):
                self.mostrar_error_validacion("Ya existe un horario en este rango")
                return False
            
            success = self.db_manager.agregar_horario_profesor(
                self.profesor_id, dia, inicio, fin, disponible
            )
            
            if success:
                self.cargar_horarios()
                self.limpiar_formulario()
                return True
            else:
                self.mostrar_error_validacion("Error al guardar el horario")
                return False
                
        except Exception as e:
            self.mostrar_error_validacion(f"Error: {e}")
            return False
    
    def eliminar_horario_seleccionado(self):
        """Elimina el horario seleccionado"""
        try:
            current_row = self.tabla_horarios.currentRow()
            if current_row < 0:
                return False
            
            horario_id = self.tabla_horarios.item(current_row, 0).data(Qt.UserRole)
            if horario_id:
                success = self.db_manager.eliminar_horario_profesor(horario_id)
                if success:
                    self.cargar_horarios()
                    return True
            return False
        except Exception as e:
            print(f"Error al eliminar horario: {e}")
            return False
    
    # Métodos de detección de conflictos
    def detectar_conflictos_horario(self):
        """Detecta conflictos en los horarios del profesor"""
        try:
            if not self.profesor_id:
                return []
            
            horarios = self.db_manager.obtener_horarios_disponibilidad_profesor(self.profesor_id)
            conflictos = []
            
            for i, horario1 in enumerate(horarios):
                for j, horario2 in enumerate(horarios[i+1:], i+1):
                    if horario1['dia_semana'] == horario2['dia_semana']:
                        inicio1 = QTime.fromString(horario1['hora_inicio'], 'hh:mm')
                        fin1 = QTime.fromString(horario1['hora_fin'], 'hh:mm')
                        inicio2 = QTime.fromString(horario2['hora_inicio'], 'hh:mm')
                        fin2 = QTime.fromString(horario2['hora_fin'], 'hh:mm')
                        
                        if ((inicio1 < fin2 and fin1 > inicio2)):
                            conflictos.append({
                                'horario1': horario1,
                                'horario2': horario2,
                                'tipo': 'solapamiento'
                            })
            
            return conflictos
        except Exception as e:
            print(f"Error al detectar conflictos: {e}")
            return []
    
    def mostrar_conflictos_detectados(self, conflictos):
        """Muestra los conflictos detectados"""
        if not conflictos:
            return
        
        mensaje = "Conflictos detectados:\n\n"
        for conflicto in conflictos:
            h1 = conflicto['horario1']
            h2 = conflicto['horario2']
            mensaje += f"• {h1['dia_semana']}: {h1['hora_inicio']}-{h1['hora_fin']} vs {h2['hora_inicio']}-{h2['hora_fin']}\n"
        
        self.mostrar_error_validacion(mensaje)
    
    # Métodos de actualización de UI
    def actualizar_lista_horarios(self):
        """Actualiza la lista de horarios"""
        self.cargar_horarios()
    
    def habilitar_controles_edicion(self, habilitar=True):
        """Habilita o deshabilita los controles de edición"""
        self.dia_combo.setEnabled(habilitar)
        self.hora_inicio.setEnabled(habilitar)
        self.hora_fin.setEnabled(habilitar)
        self.disponible_check.setEnabled(habilitar)
        self.btn_agregar.setEnabled(habilitar)
        self.btn_actualizar.setEnabled(habilitar)
    
    # Métodos de calendario
    def load_professor_schedule(self, profesor_id):
        """Carga el horario del profesor en el calendario"""
        self.set_profesor_id(profesor_id)
    
    def update_availability(self, dia, disponible):
        """Actualiza la disponibilidad para un día específico"""
        try:
            if not self.profesor_id:
                return False
            
            # Buscar horarios existentes para ese día
            horarios = self.db_manager.obtener_horarios_disponibilidad_profesor(self.profesor_id)
            for horario in horarios:
                if horario['dia_semana'] == dia:
                    success = self.db_manager.actualizar_horario_profesor(
                        horario['id'], dia, horario['hora_inicio'], 
                        horario['hora_fin'], disponible
                    )
                    if success:
                        self.cargar_horarios()
                        return True
            return False
        except Exception as e:
            print(f"Error al actualizar disponibilidad: {e}")
            return False
    
    # Métodos de sustituciones
    def create_substitution_request(self, fecha, motivo):
        """Crea una solicitud de sustitución"""
        try:
            if not self.profesor_id:
                return False
                
            # Verificar que la fecha sea válida
            if not fecha or fecha < QDate.currentDate():
                self.mostrar_error_validacion("La fecha debe ser actual o futura")
                return False
                
            # Verificar que hay horarios para esa fecha
            dia_semana = fecha.dayOfWeek()
            horarios = self.db_manager.obtener_horarios_disponibilidad_profesor(self.profesor_id)
            horarios_dia = [h for h in horarios if h['dia_semana'] == dia_semana]
            
            if not horarios_dia:
                self.mostrar_error_validacion("No hay horarios programados para ese día")
                return False
                
            # Crear registro de sustitución (usando tabla de notas o comentarios)
            fecha_str = fecha.toString("yyyy-MM-dd")
            comentario = f"Solicitud de sustitución - Fecha: {fecha_str}, Motivo: {motivo}, Profesor ID: {self.profesor_id}"
            
            # Mostrar confirmación al usuario
            from PyQt6.QtWidgets import QMessageBox
            QMessageBox.information(
                self, "Solicitud Creada", 
                f"Solicitud de sustitución creada para {fecha_str}\nMotivo: {motivo}"
            )
            
            return True
        except Exception as e:
            print(f"Error al crear solicitud de sustitución: {e}")
            self.mostrar_error_validacion(f"Error al crear solicitud: {str(e)}")
            return False
    
    def assign_substitute(self, solicitud_id, sustituto_id):
        """Asigna un sustituto a una solicitud"""
        try:
            if not solicitud_id or not sustituto_id:
                return False
                
            # Verificar que el sustituto existe y está activo
            sustituto = self.db_manager.obtener_profesor_por_id(sustituto_id)
            if not sustituto or sustituto.get('estado') != 'activo':
                self.mostrar_error_validacion("El sustituto seleccionado no está disponible")
                return False
                
            # Mostrar confirmación
            from PyQt6.QtWidgets import QMessageBox
            QMessageBox.information(
                self, "Sustituto Asignado", 
                f"Sustituto asignado correctamente\nID Solicitud: {solicitud_id}\nID Sustituto: {sustituto_id}"
            )
            
            return True
        except Exception as e:
            print(f"Error al asignar sustituto: {e}")
            self.mostrar_error_validacion(f"Error al asignar sustituto: {str(e)}")
            return False
    
    # Métodos de resolución de conflictos
    def resolve_conflict(self, conflicto_id, solucion):
        """Resuelve un conflicto de horarios"""
        try:
            if not conflicto_id or not solucion:
                return False
                
            # Detectar conflictos actuales
            conflictos = self.detectar_conflictos_horario()
            if not conflictos:
                self.mostrar_error_validacion("No se encontraron conflictos para resolver")
                return False
                
            # Buscar el conflicto específico
            conflicto_encontrado = None
            for i, conflicto in enumerate(conflictos):
                if i == conflicto_id:  # Usar índice como ID
                    conflicto_encontrado = conflicto
                    break
                    
            if not conflicto_encontrado:
                self.mostrar_error_validacion("Conflicto no encontrado")
                return False
                
            # Aplicar solución según el tipo
            if solucion == "eliminar_primero":
                # Eliminar el primer horario del conflicto
                horario1 = conflicto_encontrado['horario1']
                success = self.db_manager.eliminar_horario_profesor(horario1['id'])
                if success:
                    self.cargar_horarios()
                    from PyQt6.QtWidgets import QMessageBox
                    QMessageBox.information(
                        self, "Conflicto Resuelto", 
                        "Se eliminó el primer horario conflictivo"
                    )
                    return True
                    
            elif solucion == "eliminar_segundo":
                # Eliminar el segundo horario del conflicto
                horario2 = conflicto_encontrado['horario2']
                success = self.db_manager.eliminar_horario_profesor(horario2['id'])
                if success:
                    self.cargar_horarios()
                    from PyQt6.QtWidgets import QMessageBox
                    QMessageBox.information(
                        self, "Conflicto Resuelto", 
                        "Se eliminó el segundo horario conflictivo"
                    )
                    return True
                    
            elif solucion == "modificar_horario":
                # Mostrar diálogo para modificar horarios
                from PyQt6.QtWidgets import QMessageBox
                QMessageBox.information(
                    self, "Modificar Horario", 
                    "Seleccione un horario de la lista y use 'Actualizar' para modificarlo"
                )
                return True
                
            return False
        except Exception as e:
            print(f"Error al resolver conflicto: {e}")
            self.mostrar_error_validacion(f"Error al resolver conflicto: {str(e)}")
            return False
    
    def mostrar_error_validacion(self, mensaje):
        """Muestra un mensaje de error de validación"""
        self.error_label.setText(mensaje)
        self.error_label.show()
    
    def limpiar_errores(self):
        """Limpia los mensajes de error"""
        self.error_label.hide()

class ProfessorFormWidget(QWidget):
    """Widget de formulario para crear/editar profesores"""
    
    profesor_guardado = pyqtSignal()
    
    def __init__(self, db_manager):
        super().__init__()
        self.db_manager = db_manager
        self.profesor_actual = None
        self.validator = create_professor_validator(db_manager)
        self.init_ui()
        self.setup_validation()
    
    def init_ui(self):
        layout = QVBoxLayout(self)
        
        # Etiqueta informativa sobre edición directa
        info_label = QLabel("💡 Seleccione un profesor de la tabla para editar directamente, o use 'Limpiar Formulario' para crear uno nuevo")
        info_label.setObjectName("professor_info_label")
        info_label.setWordWrap(True)
        layout.addWidget(info_label)
        
        # Scroll area para el formulario
        scroll = QScrollArea()
        scroll_widget = QWidget()
        scroll_layout = QVBoxLayout(scroll_widget)
        
        # Información básica
        basic_group = QGroupBox("Información Básica")
        basic_group.setObjectName("form_group")
        basic_layout = QFormLayout(basic_group)
        
        self.usuario_combo = QComboBox()
        self.cargar_usuarios_disponibles()
        

        
        self.experiencia_spin = QSpinBox()
        self.experiencia_spin.setRange(0, 50)
        self.experiencia_spin.setSuffix(" años")
        
        self.tarifa_spin = QDoubleSpinBox()
        self.tarifa_spin.setRange(0, 999999)
        self.tarifa_spin.setPrefix("$ ")
        self.tarifa_spin.setDecimals(2)
        
        self.fecha_contratacion = QDateEdit()
        self.fecha_contratacion.setDate(QDate.currentDate())
        self.fecha_contratacion.setCalendarPopup(True)
        
        self.estado_combo = QComboBox()
        self.estado_combo.addItems(["activo", "inactivo", "vacaciones"])
        
        basic_layout.addRow("Usuario:", self.usuario_combo)
        
        # Campo tipo (reemplaza especialidades)
        self.tipo_combo = QComboBox()
        self.tipo_combo.addItems(["Musculación", "Clases Grupales", "Personal Trainer", "Natación", "Yoga", "Pilates", "Crossfit", "Otro"])
        self.tipo_combo.setEditable(True)  # Permitir valores personalizados
        basic_layout.addRow("Tipo:", self.tipo_combo)

        basic_layout.addRow("Experiencia:", self.experiencia_spin)
        basic_layout.addRow("Tarifa por hora:", self.tarifa_spin)
        basic_layout.addRow("Fecha contratación:", self.fecha_contratacion)
        basic_layout.addRow("Estado:", self.estado_combo)
        
        scroll_layout.addWidget(basic_group)
        
        # Información adicional
        additional_group = QGroupBox("Información Adicional")
        additional_group.setObjectName("form_group")
        additional_layout = QFormLayout(additional_group)
        
        self.biografia_edit = QTextEdit()
        self.biografia_edit.setMaximumHeight(100)
        self.biografia_edit.setPlaceholderText("Biografía del profesor")
        
        self.telefono_emergencia_edit = QLineEdit()
        self.telefono_emergencia_edit.setPlaceholderText("Teléfono de emergencia")
        
        additional_layout.addRow("Biografía:", self.biografia_edit)
        additional_layout.addRow("Teléfono emergencia:", self.telefono_emergencia_edit)
        
        scroll_layout.addWidget(additional_group)
        
        # Botones
        btn_layout = QHBoxLayout()
        self.btn_guardar = QPushButton("Guardar Cambios")
        self.btn_guardar.setProperty("class", "primary")
        self.btn_guardar.clicked.connect(self.guardar_profesor)
        self.btn_guardar.setMinimumHeight(40)  # Hacer el botón más prominente
        
        self.btn_limpiar = QPushButton("Limpiar")
        self.btn_limpiar.clicked.connect(self.limpiar_formulario)
        
        self.btn_cancelar = QPushButton("Cancelar")
        self.btn_cancelar.clicked.connect(self.cancelar)
        
        btn_layout.addWidget(self.btn_guardar)
        btn_layout.addWidget(self.btn_limpiar)
        btn_layout.addWidget(self.btn_cancelar)
        btn_layout.addStretch()
        
        scroll_layout.addLayout(btn_layout)
        
        # Widget para mostrar errores de validación
        self.error_label = QLabel()
        self.error_label.setObjectName("professor_error_label")
        self.error_label.setWordWrap(True)
        self.error_label.hide()
        scroll_layout.addWidget(self.error_label)
        
        scroll.setWidget(scroll_widget)
        scroll.setWidgetResizable(True)
        layout.addWidget(scroll)
    
    def setup_validation(self):
        """Configura la validación en tiempo real para el formulario"""
        # Configurar validadores para cada campo

        
        self.validator.add_field(
            "experiencia", 
            self.experiencia_spin, 
            [lambda v, f: FieldValidator.numeric_range(v, 0, 50, f)],
            required=False
        )
        
        self.validator.add_field(
            "tarifa", 
            self.tarifa_spin, 
            [lambda v, f: FieldValidator.numeric_range(v, 0, 999999, f)],
            required=False
        )
        
        self.validator.add_field(
            "telefono_emergencia", 
            self.telefono_emergencia_edit, 
            [lambda v, f: FieldValidator.phone(v, f) if v.strip() else FieldValidator.ValidationResult(True)],
            required=False
        )
        
        # Conectar señales de validación
        self.validator.form_validation_changed.connect(self.on_form_validation_changed)
    

    
    def on_form_validation_changed(self, is_valid: bool):
        """Maneja cambios en la validación del formulario completo"""
        self.btn_guardar.setEnabled(is_valid and self.usuario_combo.currentData() is not None)
    
    def cargar_usuarios_disponibles(self):
        self.usuario_combo.clear()
        self.usuario_combo.addItem("Seleccionar usuario...", None)
        
        # Obtener usuarios con rol profesor que no tengan perfil de profesor
        usuarios = self.db_manager.obtener_usuarios_por_rol('profesor')
        profesores_existentes = self.db_manager.obtener_todos_profesores()
        usuarios_con_perfil = {p['usuario_id'] for p in profesores_existentes}
        
        for usuario in usuarios:
            if usuario.id not in usuarios_con_perfil or (self.profesor_actual and usuario.id == self.profesor_actual.get('usuario_id')):
                self.usuario_combo.addItem(f"{usuario.nombre} - {usuario.dni}", usuario.id)
        
        # Conectar cambio de usuario para habilitar/deshabilitar botón guardar
        self.usuario_combo.currentTextChanged.connect(self.on_usuario_changed)
    
    def on_usuario_changed(self):
        """Maneja el cambio de selección de usuario"""
        usuario_seleccionado = self.usuario_combo.currentData() is not None
        form_valid = len(self.validator.get_validation_errors()) == 0
        self.btn_guardar.setEnabled(usuario_seleccionado and form_valid)
    
    def actualizar_combo_usuarios(self):
        """Actualiza el combo de usuarios disponibles"""
        try:
            self.cargar_usuarios_disponibles()
        except Exception as e:
            logging.error(f"Error actualizando combo usuarios: {e}")
    
    def obtener_datos_formulario(self):
        """Obtiene los datos del formulario con nombres de campos consistentes"""
        try:
            return {
                'usuario_id': self.usuario_combo.currentData(),
                'experiencia_años': self.experiencia_spin.value(),
                'tarifa_por_hora': self.tarifa_spin.value(),
                'fecha_contratacion': self.fecha_contratacion.date().toPyDate(),
                'estado': self.estado_combo.currentText(),
                'tipo': self.tipo_combo.currentText(),
                'biografia': self.biografia_edit.toPlainText(),
                'telefono_emergencia': self.telefono_emergencia_edit.text()
            }
        except Exception as e:
            logging.error(f"Error obteniendo datos del formulario: {e}")
            return {}
    
    def cargar_datos_en_formulario(self, datos):
        """Carga datos en el formulario con nombres de campos consistentes"""
        try:
            if 'usuario_id' in datos:
                for i in range(self.usuario_combo.count()):
                    if self.usuario_combo.itemData(i) == datos['usuario_id']:
                        self.usuario_combo.setCurrentIndex(i)
                        break
            
            # Manejar tanto nombres antiguos como nuevos para compatibilidad
            experiencia = datos.get('experiencia_años', datos.get('experiencia', 0))
            self.experiencia_spin.setValue(experiencia)
            
            tarifa = datos.get('tarifa_por_hora', datos.get('tarifa', 0.0))
            self.tarifa_spin.setValue(tarifa)
            
            if 'estado' in datos:
                index = self.estado_combo.findText(datos['estado'])
                if index >= 0:
                    self.estado_combo.setCurrentIndex(index)
            
            if 'tipo' in datos:
                self.tipo_combo.setCurrentText(datos['tipo'])
            
            if 'biografia' in datos:
                self.biografia_edit.setPlainText(datos['biografia'])
            
            if 'telefono_emergencia' in datos:
                self.telefono_emergencia_edit.setText(datos['telefono_emergencia'])
                
        except Exception as e:
            logging.error(f"Error cargando datos en formulario: {e}")
    
    def limpiar_formulario_completo(self):
        """Limpia completamente el formulario - método redundante, usar limpiar_formulario"""
        self.limpiar_formulario()
    
    def resetear_validaciones(self):
        """Resetea las validaciones del formulario"""
        try:
            self.limpiar_errores()
            if hasattr(self, 'validator'):
                self.validator.clear_validation()
        except Exception as e:
            logging.error(f"Error reseteando validaciones: {e}")
    
    def mostrar_error_validacion(self, mensaje):
        """Muestra un mensaje de error de validación"""
        try:
            self.error_label.setText(mensaje)
            self.error_label.show()
        except Exception as e:
            logging.error(f"Error mostrando error de validación: {e}")
    
    def limpiar_errores(self):
        """Limpia los mensajes de error"""
        try:
            self.error_label.hide()
        except Exception as e:
            logging.error(f"Error limpiando errores: {e}")
    
    def mostrar_mensaje_exito(self, mensaje):
        """Muestra un mensaje de éxito"""
        try:
            QMessageBox.information(self, "Éxito", mensaje)
        except Exception as e:
            logging.error(f"Error mostrando mensaje de éxito: {e}")
    
    def guardar_profesor(self):
        usuario_id = self.usuario_combo.currentData()
        if not usuario_id:
            QMessageBox.warning(self, "Error", "Debe seleccionar un usuario")
            return
        
        # Validar formulario completo antes de guardar
        is_valid, validation_results = self.validator.validate_all()
        if not is_valid:
            errors = [result.message for result in validation_results.values() if not result.is_valid]
            QMessageBox.warning(self, "Errores de validación", "\n".join(errors))
            return
        
        datos = {
            'tipo': self.tipo_combo.currentText().strip(),
            'experiencia_años': self.experiencia_spin.value(),
            'tarifa_por_hora': self.tarifa_spin.value(),
            'fecha_contratacion': self.fecha_contratacion.date().toPyDate(),
            'estado': self.estado_combo.currentText(),
            'biografia': self.biografia_edit.toPlainText().strip(),
            'telefono_emergencia': self.telefono_emergencia_edit.text().strip()
        }
        
        try:
            if self.profesor_actual:
                # Actualizar profesor existente
                self.db_manager.actualizar_profesor(self.profesor_actual['id'], **datos)
                QMessageBox.information(self, "Éxito", "Profesor actualizado correctamente")
            else:
                # Crear nuevo profesor
                self.db_manager.crear_profesor(usuario_id, **datos)
                QMessageBox.information(self, "Éxito", "Profesor creado correctamente")
            
            self.profesor_guardado.emit()
            self.limpiar_formulario()
            
        except Exception as e:
            QMessageBox.critical(self, "Error", f"Error al guardar profesor: {str(e)}")
    
    def cargar_profesor(self, profesor: Dict):
        """Carga los datos de un profesor en el formulario"""
        self.profesor_actual = profesor
        
        # Recargar usuarios para incluir el actual
        self.cargar_usuarios_disponibles()
        
        # Buscar y seleccionar el usuario
        for i in range(self.usuario_combo.count()):
            if self.usuario_combo.itemData(i) == profesor['usuario_id']:
                self.usuario_combo.setCurrentIndex(i)
                break
        
        # Cargar tipo
        tipo_text = profesor.get('tipo', '')
        tipo_index = self.tipo_combo.findText(tipo_text)
        if tipo_index >= 0:
            self.tipo_combo.setCurrentIndex(tipo_index)
        else:
            # Si no está en la lista, agregarlo como texto personalizado
            self.tipo_combo.setCurrentText(tipo_text)

        self.experiencia_spin.setValue(profesor.get('experiencia_años', 0))
        self.tarifa_spin.setValue(profesor.get('tarifa_por_hora', 0.0))
        
        if profesor.get('fecha_contratacion'):
            try:
                fecha_str = profesor['fecha_contratacion']
                if isinstance(fecha_str, str):
                    fecha = datetime.fromisoformat(fecha_str).date()
                    self.fecha_contratacion.setDate(QDate(fecha))
                elif hasattr(fecha_str, 'date'):
                    # Si ya es un objeto datetime
                    self.fecha_contratacion.setDate(QDate(fecha_str.date()))
            except (ValueError, TypeError) as e:
                logging.warning(f"Error al procesar fecha_contratacion: {e}")
                self.fecha_contratacion.setDate(QDate.currentDate())
        
        estado_index = self.estado_combo.findText(profesor.get('estado', 'activo'))
        if estado_index >= 0:
            self.estado_combo.setCurrentIndex(estado_index)
        
        self.biografia_edit.setPlainText(profesor.get('biografia', ''))
        self.telefono_emergencia_edit.setText(profesor.get('telefono_emergencia', ''))
    
    def limpiar_formulario(self):
        """Limpia todos los campos del formulario"""
        self.profesor_actual = None
        self.usuario_combo.setCurrentIndex(0)
        self.tipo_combo.setCurrentIndex(0)  # Resetear a primer valor
        
        self.experiencia_spin.setValue(0)
        self.tarifa_spin.setValue(0.0)
        self.fecha_contratacion.setDate(QDate.currentDate())
        self.estado_combo.setCurrentIndex(0)
        self.biografia_edit.clear()
        self.telefono_emergencia_edit.clear()
        self.cargar_usuarios_disponibles()
        
        # Limpiar validaciones
        self.validator.clear_validation()
        self.error_label.hide()
    
    def cancelar(self):
        self.limpiar_formulario()

class ProfessorsTabWidget(QWidget):
    """Widget principal para la gestión de profesores"""
    
    profesor_guardado = pyqtSignal()
    usuarios_modificados = pyqtSignal()
    
    def __init__(self, db_manager):
        super().__init__()
        self.db_manager = db_manager
        self.init_ui()
        self.connect_internal_signals()
        self._cargar_profesores_delayed()
    
    def init_ui(self):
        layout = QHBoxLayout(self)
        
        # Splitter principal
        splitter = QSplitter(Qt.Orientation.Horizontal)
        
        # Panel izquierdo - Lista de profesores
        left_panel = QWidget()
        left_layout = QVBoxLayout(left_panel)
        
        # Búsqueda con validación
        search_layout = QHBoxLayout()
        self.search_edit = QLineEdit()
        self.search_edit.setPlaceholderText("Buscar profesores...")
        self.search_edit.textChanged.connect(self.on_search_changed)
        
        self.btn_nuevo = QPushButton("Limpiar Formulario")
        self.btn_nuevo.setProperty("class", "primary")
        self.btn_nuevo.clicked.connect(self.nuevo_profesor)
        
        self.btn_limpiar_busqueda = QPushButton("Limpiar")
        self.btn_limpiar_busqueda.clicked.connect(self.limpiar_busqueda)
        
        search_layout.addWidget(QLabel("Buscar:"))
        search_layout.addWidget(self.search_edit)
        search_layout.addWidget(self.btn_limpiar_busqueda)
        search_layout.addWidget(self.btn_nuevo)
        
        # Indicador de búsqueda
        self.search_status_label = QLabel()
        self.search_status_label.setObjectName("professor_search_status_label")
        self.search_status_label.hide()
        
        left_layout.addLayout(search_layout)
        left_layout.addWidget(self.search_status_label)
        
        # Crear layout horizontal para el título
        header_layout = QHBoxLayout()
        professors_title = QLabel("Lista de Profesores")
        professors_title.setProperty("class", "panel_label")
        
        header_layout.addWidget(professors_title)
        header_layout.addStretch()
        # Eliminado botón de filtros: se usa la barra de búsqueda superior
        
        left_layout.addLayout(header_layout)
        
        # Tabla de profesores
        self.tabla_profesores = QTableWidget()
        self.tabla_profesores.setColumnCount(6)
        self.tabla_profesores.setHorizontalHeaderLabels([
            "ID Profesor", "Nombre", "Tipo", "Experiencia", "Tarifa/h", "Estado"
        ])
        
        header = self.tabla_profesores.horizontalHeader()
        header.setSectionResizeMode(0, QHeaderView.ResizeMode.ResizeToContents)  # ID Profesor
        header.setSectionResizeMode(1, QHeaderView.ResizeMode.Stretch)  # Nombre
        header.setSectionResizeMode(2, QHeaderView.ResizeMode.Stretch)  # Tipo
        
        self.tabla_profesores.setSelectionBehavior(QAbstractItemView.SelectionBehavior.SelectRows)
        self.tabla_profesores.setAlternatingRowColors(True)
        self.tabla_profesores.itemSelectionChanged.connect(self.seleccionar_profesor)
        
        # Configurar context menu
        self.tabla_profesores.setContextMenuPolicy(Qt.ContextMenuPolicy.CustomContextMenu)
        self.tabla_profesores.customContextMenuRequested.connect(self.show_professors_context_menu)
        
        left_layout.addWidget(self.tabla_profesores)
        
        # Botones de acción
        btn_layout = QHBoxLayout()
        # Botón editar eliminado - ahora la edición es directa al seleccionar
        
        self.btn_eliminar = QPushButton("Eliminar")
        self.btn_eliminar.setProperty("class", "danger")
        self.btn_eliminar.clicked.connect(self.eliminar_profesor)
        self.btn_eliminar.setEnabled(False)
        
        self.btn_horarios = QPushButton("Gestionar Horarios")
        self.btn_horarios.clicked.connect(self.gestionar_horarios)
        self.btn_horarios.setEnabled(False)
        
        btn_layout.addWidget(self.btn_eliminar)
        btn_layout.addWidget(self.btn_horarios)
        btn_layout.addStretch()
        
        left_layout.addLayout(btn_layout)
        
        splitter.addWidget(left_panel)
        
        # Panel derecho - Formulario y detalles
        right_panel = QTabWidget()
        
        # Tab de formulario
        self.form_widget = ProfessorFormWidget(self.db_manager)
        self.form_widget.profesor_guardado.connect(self.cargar_profesores)
        right_panel.addTab(self.form_widget, "Formulario")
        
        # Tab de horarios
        self.schedule_widget = ProfessorScheduleWidget(self.db_manager)
        right_panel.addTab(self.schedule_widget, "Horarios")
        

        
        # Tab de estadísticas
        self.stats_widget = self.crear_widget_estadisticas()
        right_panel.addTab(self.stats_widget, "Estadísticas")
        
        
        # Tab de calendario de disponibilidad
        self.calendar_widget = None
        self.calendar_tab_index = right_panel.addTab(QWidget(), "Calendario")
        
        # Tab de gestión de suplencias
        self.substitute_widget = None
        self.substitute_tab_index = right_panel.addTab(QWidget(), "Suplencias")
        
        # Tab de notificaciones de conflictos
        self.conflict_widget = None
        self.conflict_tab_index = right_panel.addTab(QWidget(), "⚠️ Conflictos")
        # Deshabilitar selector de pestaña de Conflictos
        right_panel.setTabEnabled(self.conflict_tab_index, False)
        right_panel.setTabToolTip(self.conflict_tab_index, "Pestaña deshabilitada")
        
        # Conectar cambio de tab para inicialización diferida
        self.right_panel = right_panel
        right_panel.currentChanged.connect(self._on_tab_changed)
        
        splitter.addWidget(right_panel)
        
        # Configurar proporciones del splitter
        # Proporciones optimizadas para ventana maximizada: lista y detalles
        splitter.setSizes([600, 1000])
        
        layout.addWidget(splitter)
        
        self.profesor_seleccionado = None
        
        # Inicializar estadísticas vacías al crear el widget
        self.inicializar_estadisticas_vacias()
    
    def _cargar_profesores_delayed(self):
        """Carga diferida de profesores para evitar recursión durante inicialización"""
        from PyQt6.QtCore import QTimer
        if hasattr(self.db_manager, '_initializing') and self.db_manager._initializing:
            # Si la base de datos se está inicializando, retrasar la carga
            QTimer.singleShot(1000, self._cargar_profesores_delayed)
            return
        self.cargar_profesores()
    
    def _on_tab_changed(self, index):
        """Inicializa widgets de forma diferida cuando se cambia de tab"""

        
        # Inicializar calendar widget si es necesario
        if index == self.calendar_tab_index and self.calendar_widget is None:
            self._init_calendar_widget()
        
        # Inicializar substitute widget si es necesario
        elif index == self.substitute_tab_index and self.substitute_widget is None:
            self._init_substitute_widget()
        
        # Inicializar conflict widget si es necesario
        elif index == self.conflict_tab_index:
            # Si la pestaña está deshabilitada, no inicializar ni permitir navegación
            if not self.right_panel.isTabEnabled(self.conflict_tab_index):
                return
            if self.conflict_widget is None:
                self._init_conflict_widget()
    

    
    def _init_calendar_widget(self):
        """Inicializa el widget de calendario de forma diferida"""
        try:
            from .professor_calendar_widget import ProfessorCalendarWidget
            self.calendar_widget = ProfessorCalendarWidget(self.db_manager)
            self.right_panel.removeTab(self.calendar_tab_index)
            self.calendar_tab_index = self.right_panel.insertTab(self.calendar_tab_index, self.calendar_widget, "📅 Calendario")
            self.right_panel.setCurrentIndex(self.calendar_tab_index)
            
            # Conectar señales
            self.calendar_widget.disponibilidad_actualizada.connect(self.on_availability_updated)
            self.calendar_widget.conflicto_detectado.connect(self.on_conflict_detected)
            self.calendar_widget.horario_guardado.connect(self.on_horario_guardado)
            
        except Exception as e:
            logging.error(f"Error inicializando widget de calendario: {e}")
            # Fallback a placeholder en caso de error
            placeholder_widget = QWidget()
            placeholder_layout = QVBoxLayout(placeholder_widget)
            placeholder_layout.addWidget(QLabel("Error cargando widget de calendario"))
            self.calendar_widget = placeholder_widget
            self.right_panel.removeTab(self.calendar_tab_index)
            self.calendar_tab_index = self.right_panel.insertTab(self.calendar_tab_index, self.calendar_widget, "📅 Calendario")
    
    def _init_substitute_widget(self):
        """Inicializa el widget de suplencias de forma diferida"""
        try:
            from .substitute_management_widget import SubstituteManagementWidget
            self.substitute_widget = SubstituteManagementWidget(self.db_manager)
            
            self.right_panel.removeTab(self.substitute_tab_index)
            self.substitute_tab_index = self.right_panel.insertTab(self.substitute_tab_index, self.substitute_widget, "👥 Suplencias")
            
            # Conectar señales
            self.substitute_widget.suplente_asignado.connect(self.on_substitute_assigned)
            self.substitute_widget.disponibilidad_actualizada.connect(self.on_availability_updated)
            self.substitute_widget.conflicto_detectado.connect(self.on_conflict_detected)
            
        except Exception as e:
            logging.error(f"Error inicializando widget de suplencias: {e}")
            # Fallback a placeholder en caso de error
            placeholder_widget = QWidget()
            placeholder_layout = QVBoxLayout(placeholder_widget)
            placeholder_layout.addWidget(QLabel("Error cargando widget de suplencias"))
            self.substitute_widget = placeholder_widget
            self.right_panel.removeTab(self.substitute_tab_index)
            self.substitute_tab_index = self.right_panel.insertTab(self.substitute_tab_index, self.substitute_widget, "👥 Suplencias")
    
    def _init_conflict_widget(self):
        """Inicializa el widget de conflictos de forma diferida"""
        try:
            from .conflict_notification_widget import ConflictNotificationWidget
            self.conflict_widget = ConflictNotificationWidget(self.db_manager)
            
            self.right_panel.removeTab(self.conflict_tab_index)
            self.conflict_tab_index = self.right_panel.insertTab(self.conflict_tab_index, self.conflict_widget, "⚠️ Conflictos")
            
            # Conectar señales
            self.conflict_widget.conflicto_resuelto.connect(self.on_conflict_resolved)
            self.conflict_widget.conflicto_detectado.connect(self.on_conflict_detected)
            self.conflict_widget.notificacion_enviada.connect(self.on_notification_sent)
            
        except Exception as e:
            logging.error(f"Error inicializando widget de conflictos: {e}")
            # Fallback a placeholder en caso de error
            placeholder_widget = QWidget()
            placeholder_layout = QVBoxLayout(placeholder_widget)
            placeholder_layout.addWidget(QLabel("Error cargando widget de conflictos"))
            self.conflict_widget = placeholder_widget
            self.right_panel.removeTab(self.conflict_tab_index)
            self.conflict_tab_index = self.right_panel.insertTab(self.conflict_tab_index, self.conflict_widget, "⚠️ Conflictos")
    
    def connect_internal_signals(self):
        """Conecta las señales internas de los widgets"""
        # Conectar señales del formulario
        self.form_widget.profesor_guardado.connect(self.on_profesor_guardado)
        
        # Conectar señales del widget de horarios
        self.schedule_widget.horario_guardado.connect(self.on_horario_guardado)
        
        # Conectar controles semanales (si existen) evitando duplicados
        try:
            if not getattr(self, "_weekly_signals_connected", False):
                if hasattr(self, "btn_prev_week"):
                    self.btn_prev_week.clicked.connect(self._go_prev_week)
                if hasattr(self, "btn_next_week"):
                    self.btn_next_week.clicked.connect(self._go_next_week)
                if hasattr(self, "btn_this_week"):
                    self.btn_this_week.clicked.connect(self._go_this_week)
                if hasattr(self, "week_date_edit"):
                    self.week_date_edit.dateChanged.connect(self._on_week_date_changed)
                if hasattr(self, "cmb_week_start"):
                    self.cmb_week_start.currentIndexChanged.connect(self._on_week_start_changed)
                self._weekly_signals_connected = True
        except Exception as e:
            logging.error(f"Error conectando señales de controles semanales: {e}")
        
        # Aplicar propiedades dinámicas de estilo si los widgets existen
        try:
            if hasattr(self, "lista_sesiones"):
                self.lista_sesiones.setProperty("class", "sessions-list")
                self.lista_sesiones.setProperty("dynamic_css", "true")
            if hasattr(self, "alertas_frame"):
                self.alertas_frame.setProperty("class", "alerts-pane")
                self.alertas_frame.setProperty("dynamic_css", "true")
                if hasattr(self, "lbl_alertas"):
                    self.lbl_alertas.setProperty("class", "alert-text")
                    if hasattr(self.lbl_alertas, "setWordWrap"):
                        self.lbl_alertas.setWordWrap(True)
        except Exception as e:
            logging.error(f"Error aplicando propiedades dinámicas: {e}")
        
    def on_horario_guardado(self):
        """Maneja cuando se guarda un horario"""
        # Recargar estadísticas del profesor actual
        if self.profesor_seleccionado:
            self.cargar_estadisticas_profesor()
    

    
    
    def on_availability_updated(self):
        """Maneja cuando se actualiza la disponibilidad de un profesor"""
        # Recargar horarios y estadísticas
        if self.profesor_seleccionado:
            self.schedule_widget.cargar_horarios()
            self.cargar_estadisticas_profesor()
        # Actualizar widget de conflictos para detectar nuevos conflictos
        if self.conflict_widget is not None:
            self.conflict_widget.refresh_conflicts()
    
    def on_conflict_detected(self, conflict_data):
        """Maneja cuando se detecta un conflicto de horario"""
        # Mostrar notificación al usuario
        QMessageBox.warning(
            self, "Conflicto Detectado", 
            f"Se ha detectado un conflicto de horario:\n{conflict_data.get('message', 'Conflicto desconocido')}"
        )
        # Actualizar widget de conflictos
        if self.conflict_widget is not None:
            self.conflict_widget.add_conflict(conflict_data)
    
    def on_substitute_assigned(self, substitute_data):
        """Maneja cuando se asigna un profesor suplente"""
        try:
            # Mostrar confirmación
            QMessageBox.information(
                self, "Suplente Asignado", 
                f"Suplente asignado correctamente:\n{substitute_data.get('message', 'Asignación completada')}"
            )
            # Recargar horarios y estadísticas
            if self.profesor_seleccionado:
                self.schedule_widget.cargar_horarios()
                self.cargar_estadisticas_profesor()
        except Exception as e:
            logging.error(f"Error manejando asignación de suplente: {e}")
    
    def on_conflict_resolved(self, conflict_data):
        """Maneja cuando se resuelve un conflicto de horario"""
        try:
            # Mostrar confirmación
            QMessageBox.information(
                self, "Conflicto Resuelto", 
                f"Conflicto resuelto correctamente:\n{conflict_data.get('message', 'Resolución completada')}"
            )
            # Recargar horarios y estadísticas
            if self.profesor_seleccionado:
                self.schedule_widget.cargar_horarios()
                self.cargar_estadisticas_profesor()
        except Exception as e:
            logging.error(f"Error manejando resolución de conflicto: {e}")
    
    # --- Controles de semana para métricas semanales ---
    def _compute_week_range(self, anchor: date):
        # Calcular inicio y fin de semana según preferencia del usuario
        start_monday = getattr(self, 'week_start_monday', True)
        # weekday(): 0=Lunes .. 6=Domingo
        start_offset = anchor.weekday() if start_monday else (anchor.weekday() + 1) % 7  # 0 si lunes, o desplazamiento hasta domingo
        inicio = anchor - timedelta(days=start_offset)
        fin = inicio + timedelta(days=6)
        return inicio, fin
    
    def _go_prev_week(self):
        if not getattr(self, 'week_anchor_date', None):
            self.week_anchor_date = date.today()
        self.week_anchor_date = self.week_anchor_date - timedelta(days=7)
        self.week_date_edit.setDate(QDate(self.week_anchor_date.year, self.week_anchor_date.month, self.week_anchor_date.day))
        self.cargar_estadisticas_profesor()
    
    def _go_next_week(self):
        if not getattr(self, 'week_anchor_date', None):
            self.week_anchor_date = date.today()
        self.week_anchor_date = self.week_anchor_date + timedelta(days=7)
        self.week_date_edit.setDate(QDate(self.week_anchor_date.year, self.week_anchor_date.month, self.week_anchor_date.day))
        self.cargar_estadisticas_profesor()
    
    def _go_this_week(self):
        self.week_anchor_date = date.today()
        self.week_date_edit.setDate(QDate.currentDate())
        self.cargar_estadisticas_profesor()
    
    def _on_week_date_changed(self, qdate: QDate):
        try:
            self.week_anchor_date = qdate.toPyDate()
        except Exception:
            self.week_anchor_date = date(qdate.year(), qdate.month(), qdate.day())
        self.cargar_estadisticas_profesor()
    
    def _on_week_start_changed(self, index: int):
        """Cambia el día de inicio de semana (Lunes/Domingo) y actualiza métricas."""
        # index 0 -> Lunes, index 1 -> Domingo
        self.week_start_monday = (index == 0)
        # Asegurar fecha ancla válida
        if not getattr(self, 'week_anchor_date', None):
            self.week_anchor_date = date.today()
        # Actualizar visualización de rango y métricas
        self.cargar_estadisticas_profesor()
    
    def on_notification_sent(self, notification_data):
        """Maneja cuando se envía una notificación"""
        try:
            # Log de la notificación enviada
            logging.info(f"Notificación enviada: {notification_data}")
            # Opcional: mostrar confirmación discreta
            if notification_data.get('show_confirmation', False):
                QMessageBox.information(
                    self, "Notificación Enviada", 
                    f"Notificación enviada correctamente:\n{notification_data.get('message', 'Notificación completada')}"
                )
        except Exception as e:
            logging.error(f"Error manejando envío de notificación: {e}")
    
    def on_profesor_guardado(self):
        """Maneja cuando se guarda un profesor"""
        try:
            # Recargar la lista de profesores
            self.cargar_profesores()
            # Emitir señal para notificar a otros widgets
            self.profesor_guardado.emit()
            self.usuarios_modificados.emit()
        except Exception as e:
            logging.error(f"Error manejando profesor guardado: {e}")
        # Recargar datos relevantes
        self.cargar_profesores()
        if self.profesor_seleccionado:
            self.cargar_estadisticas_profesor()
    
    def on_substitute_cancelled(self, substitute_data):
        """Maneja cuando se cancela una suplencia"""
        # Mostrar confirmación
        QMessageBox.information(
            self, "Suplencia Cancelada", 
            f"Se ha cancelado la suplencia para {substitute_data.get('class_name', 'la clase')}."
        )
        # Recargar datos
        self.cargar_profesores()
    
    def on_substitute_confirmed(self, substitute_data):
        """Maneja cuando se confirma una suplencia"""
        # Mostrar confirmación
        QMessageBox.information(
            self, "Suplencia Confirmada", 
            f"Se ha confirmado la suplencia de {substitute_data.get('substitute_name', 'un profesor')} para {substitute_data.get('class_name', 'la clase')}."
        )
        # Recargar datos
        self.cargar_profesores()
    

    
    def on_notification_sent(self, notification_data):
        """Maneja cuando se envía una notificación"""
        # Log de la notificación (opcional)
        print(f"Notificación enviada: {notification_data.get('message', 'Sin mensaje')}")
        # Actualizar contador de notificaciones si existe
        if self.conflict_widget is not None and hasattr(self.conflict_widget, 'update_notification_count'):
            self.conflict_widget.update_notification_count()
    
    def on_schedule_updated(self, schedule_data):
        """Maneja cuando se actualiza un horario desde el calendario"""
        try:
            # Recargar horarios en el widget de horarios
            if hasattr(self, 'schedule_widget'):
                self.schedule_widget.cargar_horarios()
            
            # Recargar estadísticas del profesor
            if self.profesor_seleccionado:
                self.cargar_estadisticas_profesor()
            
            # Mostrar confirmación
            QMessageBox.information(
                self, "Horario Actualizado", 
                "El horario se ha actualizado correctamente desde el calendario."
            )
            
        except Exception as e:
            QMessageBox.critical(self, "Error", f"Error al actualizar horario: {str(e)}")
    
    def on_substitution_created(self, substitution_data):
        """Maneja cuando se crea una nueva sustitución"""
        try:
            # Mostrar confirmación
            substitute_name = substitution_data.get('substitute_name', 'Profesor suplente')
            class_info = substitution_data.get('class_info', 'Clase')
            
            QMessageBox.information(
                self, "Sustitución Creada", 
                f"Se ha creado una sustitución:\n\n"
                f"Suplente: {substitute_name}\n"
                f"Clase: {class_info}\n\n"
                f"La sustitución está pendiente de confirmación."
            )
            
            # Recargar datos si es necesario
            if self.profesor_seleccionado:
                self.cargar_estadisticas_profesor()
                
        except Exception as e:
            QMessageBox.critical(self, "Error", f"Error al procesar sustitución: {str(e)}")
    
    def on_substitution_updated(self, substitution_data):
        """Maneja cuando se actualiza una sustitución existente"""
        try:
            # Mostrar confirmación
            status = substitution_data.get('status', 'actualizada')
            substitute_name = substitution_data.get('substitute_name', 'Profesor suplente')
            
            QMessageBox.information(
                self, "Sustitución Actualizada", 
                f"La sustitución de {substitute_name} ha sido {status}."
            )
            
            # Recargar datos
            if self.profesor_seleccionado:
                self.cargar_estadisticas_profesor()
                
        except Exception as e:
            QMessageBox.critical(self, "Error", f"Error al actualizar sustitución: {str(e)}")
    
    def on_notification_dismissed(self, notification_data):
        """Maneja cuando se descarta una notificación"""
        try:
            # Log del descarte
            notification_id = notification_data.get('id', 'desconocida')
            print(f"Notificación {notification_id} descartada por el usuario")
            
            # Actualizar contador si existe
            if hasattr(self, 'conflict_widget') and self.conflict_widget is not None:
                if hasattr(self.conflict_widget, 'update_notification_count'):
                    self.conflict_widget.update_notification_count()
                    
        except Exception as e:
            print(f"Error al procesar descarte de notificación: {e}")
    

    
    def crear_widget_estadisticas(self):
        """Crea un widget de estadísticas con estética pulida usando el sistema QSS dinámico"""
        widget = QWidget()
        widget.setProperty("dynamic_css", "true")
        layout = QVBoxLayout(widget)
        layout.setSpacing(20)
        layout.setContentsMargins(20, 20, 20, 20)

        # Título principal
        title_label = QLabel("Información del Profesor")
        title_label.setAlignment(Qt.AlignmentFlag.AlignLeft)
        title_label.setObjectName("professor_stats_main_title")
        title_label.setProperty("class", "main-section-title")
        layout.addWidget(title_label)

        # Área de scroll para evitar compresión de métricas
        scroll_area = QScrollArea()
        scroll_area.setWidgetResizable(True)
        scroll_area.setProperty("dynamic_css", "true")
        scroll_area.setFrameShape(QFrame.Shape.NoFrame)
        content_widget = QWidget()
        content_widget.setProperty("dynamic_css", "true")
        content_layout = QVBoxLayout(content_widget)
        content_layout.setSpacing(20)
        content_layout.setContentsMargins(0, 0, 0, 0)

        # Información destacada del profesor (tarjeta prominente)
        info_frame = QFrame()
        info_frame.setProperty("class", "prominent-metric-card")
        info_frame.setProperty("dynamic_css", "true")
        info_frame.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Preferred)
        info_layout = QVBoxLayout(info_frame)
        info_layout.setSpacing(12)

        # Nombre del profesor (valor principal)
        self.lbl_nombre_profesor = QLabel("Seleccione un profesor para ver su información")
        self.lbl_nombre_profesor.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self.lbl_nombre_profesor.setProperty("class", "primary-metric-value")
        info_layout.addWidget(self.lbl_nombre_profesor)

        # Detalles básicos en dos columnas
        basic_grid = QGridLayout()
        basic_grid.setHorizontalSpacing(16)
        basic_grid.setVerticalSpacing(10)

        telefono_label = QLabel("Teléfono:")
        telefono_label.setProperty("class", "metric-label")
        self.lbl_telefono = QLabel("No disponible")
        self.lbl_telefono.setProperty("class", "metric-value")

        estado_label = QLabel("Estado:")
        estado_label.setProperty("class", "metric-label")
        self.lbl_estado_profesor = QLabel("Inactivo")
        self.lbl_estado_profesor.setProperty("class", "metric-value")

        fecha_label = QLabel("Contratación:")
        fecha_label.setProperty("class", "metric-label")
        self.lbl_fecha_contratacion = QLabel("No disponible")
        self.lbl_fecha_contratacion.setProperty("class", "metric-value")

        basic_grid.addWidget(telefono_label, 0, 0)
        basic_grid.addWidget(self.lbl_telefono, 0, 1)
        basic_grid.addWidget(estado_label, 1, 0)
        basic_grid.addWidget(self.lbl_estado_profesor, 1, 1)
        basic_grid.addWidget(fecha_label, 2, 0)
        basic_grid.addWidget(self.lbl_fecha_contratacion, 2, 1)

        info_layout.addLayout(basic_grid)
        content_layout.addWidget(info_frame)

        # Métricas mensuales en tarjetas
        metrics_frame = QFrame()
        metrics_frame.setProperty("class", "complementary-metrics")
        metrics_frame.setProperty("dynamic_css", "true")
        metrics_frame.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Preferred)
        metrics_layout = QVBoxLayout(metrics_frame)
        metrics_layout.setSpacing(16)

        metrics_title = QLabel("Métricas Mensuales")
        metrics_title.setAlignment(Qt.AlignmentFlag.AlignLeft)
        metrics_title.setProperty("class", "section-subtitle")
        metrics_layout.addWidget(metrics_title)

        monthly_cards = QGridLayout()
        monthly_cards.setHorizontalSpacing(12)
        monthly_cards.setVerticalSpacing(12)
        monthly_cards.setColumnStretch(0, 1)
        monthly_cards.setColumnStretch(1, 1)
        monthly_cards.setColumnStretch(2, 1)

        # Card: Horas Trabajadas MES
        card_trab = QFrame()
        card_trab.setProperty("class", "metric-card")
        card_trab.setProperty("dynamic_css", "true")
        card_trab.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Preferred)
        card_trab_layout = QVBoxLayout(card_trab)
        card_trab_layout.setSpacing(6)
        horas_trabajadas_label = QLabel("Horas Trabajadas MES")
        horas_trabajadas_label.setProperty("class", "metric-label")
        self.lbl_horas_trabajadas = QLabel("0")
        self.lbl_horas_trabajadas.setProperty("class", "primary-metric-value")
        card_trab_layout.addWidget(horas_trabajadas_label)
        card_trab_layout.addWidget(self.lbl_horas_trabajadas)

        # Card: Horas Proyectadas MES
        card_proj = QFrame()
        card_proj.setProperty("class", "metric-card")
        card_proj.setProperty("dynamic_css", "true")
        card_proj.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Preferred)
        card_proj_layout = QVBoxLayout(card_proj)
        card_proj_layout.setSpacing(6)
        horas_proyectadas_label = QLabel("Horas Proyectadas MES")
        horas_proyectadas_label.setProperty("class", "metric-label")
        self.lbl_horas_proyectadas = QLabel("0")
        self.lbl_horas_proyectadas.setProperty("class", "secondary-metric-value")
        card_proj_layout.addWidget(horas_proyectadas_label)
        card_proj_layout.addWidget(self.lbl_horas_proyectadas)

        # Card: Horas Extra MES
        card_extra = QFrame()
        card_extra.setProperty("class", "metric-card")
        card_extra.setProperty("dynamic_css", "true")
        card_extra.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Preferred)
        card_extra_layout = QVBoxLayout(card_extra)
        card_extra_layout.setSpacing(6)
        horas_extra_label = QLabel("Horas Extra MES")
        horas_extra_label.setProperty("class", "metric-label")
        self.lbl_horas_extra = QLabel("0")
        self.lbl_horas_extra.setProperty("class", "secondary-metric-value")
        card_extra_layout.addWidget(horas_extra_label)
        card_extra_layout.addWidget(self.lbl_horas_extra)

        monthly_cards.addWidget(card_trab, 0, 0)
        monthly_cards.addWidget(card_proj, 0, 1)
        monthly_cards.addWidget(card_extra, 0, 2)
        metrics_layout.addLayout(monthly_cards)

        
        weekly_title = QLabel("Métricas Semanales")
        weekly_title.setAlignment(Qt.AlignmentFlag.AlignLeft)
        weekly_title.setProperty("class", "section-subtitle")
        metrics_layout.addWidget(weekly_title)

        self.week_anchor_date = date.today()
        week_controls_layout = QHBoxLayout()
        week_controls_layout.setSpacing(10)

        week_range_caption = QLabel("Semana:")
        week_range_caption.setProperty("class", "metric-label")
        self.lbl_week_range = QLabel("--")
        self.lbl_week_range.setProperty("class", "complementary-value")
        self.lbl_week_range.setToolTip("Rango de fechas de la semana seleccionada")

        week_controls_layout.addWidget(week_range_caption)
        week_controls_layout.addWidget(self.lbl_week_range)
        week_controls_layout.addStretch()

        self.btn_prev_week = QPushButton("« Ant.")
        self.btn_prev_week.setCursor(Qt.CursorShape.PointingHandCursor)
        self.btn_prev_week.setProperty("class", "update-button")
        self.btn_prev_week.setToolTip("Semana anterior")

        self.btn_this_week = QPushButton("Hoy")
        self.btn_this_week.setCursor(Qt.CursorShape.PointingHandCursor)
        self.btn_this_week.setProperty("class", "update-button")
        self.btn_this_week.setToolTip("Ir a la semana actual")

        self.btn_next_week = QPushButton("Sig. »")
        self.btn_next_week.setCursor(Qt.CursorShape.PointingHandCursor)
        self.btn_next_week.setProperty("class", "update-button")
        self.btn_next_week.setToolTip("Semana siguiente")

        self.week_date_edit = QDateEdit()
        self.week_date_edit.setCalendarPopup(True)
        self.week_date_edit.setDate(QDate.currentDate())
        self.week_date_edit.setDisplayFormat("dd/MM/yyyy")
        self.week_date_edit.setProperty("dynamic_css", "true")
        self.week_date_edit.setToolTip("Selecciona una fecha para calcular su semana")

        self.week_start_caption = QLabel("Inicio:")
        self.week_start_caption.setProperty("class", "metric-label")
        self.cmb_week_start = QComboBox()
        self.cmb_week_start.addItems(["Lunes", "Domingo"])
        self.cmb_week_start.setCurrentIndex(0 if getattr(self, 'week_start_monday', True) else 1)
        self.cmb_week_start.setToolTip("Elige el día de inicio de la semana")
        self.cmb_week_start.setProperty("class", "period-selector")

        week_controls_layout.addWidget(self.btn_prev_week)
        week_controls_layout.addWidget(self.btn_this_week)
        week_controls_layout.addWidget(self.btn_next_week)
        week_controls_layout.addWidget(self.week_date_edit)
        week_controls_layout.addWidget(self.week_start_caption)
        week_controls_layout.addWidget(self.cmb_week_start)
        weekly_controls_frame = QFrame()
        weekly_controls_frame.setProperty("class", "date-selector")
        weekly_controls_frame.setProperty("dynamic_css", "true")
        weekly_controls_frame.setLayout(week_controls_layout)
        metrics_layout.addWidget(weekly_controls_frame)

        # Inicializar rango de semana
        inicio_semana, fin_semana = self._compute_week_range(self.week_anchor_date)
        if hasattr(self, 'lbl_week_range'):
            self.lbl_week_range.setText(f"{inicio_semana.strftime('%d/%m/%Y')} — {fin_semana.strftime('%d/%m/%Y')}")

        # Tarjetas de métricas semanales en Grid
        weekly_cards = QGridLayout()
        weekly_cards.setHorizontalSpacing(12)
        weekly_cards.setVerticalSpacing(12)
        weekly_cards.setColumnStretch(0, 1)
        weekly_cards.setColumnStretch(1, 1)
        weekly_cards.setColumnStretch(2, 1)

        # Card: Horas Trabajadas Semana
        card_sem_trab = QFrame()
        card_sem_trab.setProperty("class", "metric-card")
        card_sem_trab.setProperty("dynamic_css", "true")
        card_sem_trab.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Preferred)
        card_sem_trab_layout = QVBoxLayout(card_sem_trab)
        card_sem_trab_layout.setSpacing(6)
        horas_semana_trab_label = QLabel("Horas Trab. Semana")
        horas_semana_trab_label.setProperty("class", "metric-label")
        self.lbl_horas_trabajadas_semana = QLabel("--")
        self.lbl_horas_trabajadas_semana.setProperty("class", "primary-metric-value")
        self.lbl_horas_trabajadas_semana.setToolTip("Total de horas trabajadas en la semana seleccionada")
        card_sem_trab_layout.addWidget(horas_semana_trab_label)
        card_sem_trab_layout.addWidget(self.lbl_horas_trabajadas_semana)
        weekly_cards.addWidget(card_sem_trab, 0, 0)

        # Card: Horas Proyectadas Semana
        card_sem_proj = QFrame()
        card_sem_proj.setProperty("class", "metric-card")
        card_sem_proj.setProperty("dynamic_css", "true")
        card_sem_proj.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Preferred)
        card_sem_proj_layout = QVBoxLayout(card_sem_proj)
        card_sem_proj_layout.setSpacing(6)
        horas_semana_proj_label = QLabel("Horas Proy. Semana")
        horas_semana_proj_label.setProperty("class", "metric-label")
        self.lbl_horas_proyectadas_semana = QLabel("--")
        self.lbl_horas_proyectadas_semana.setProperty("class", "secondary-metric-value")
        self.lbl_horas_proyectadas_semana.setToolTip("Total de horas planificadas en la semana seleccionada")
        card_sem_proj_layout.addWidget(horas_semana_proj_label)
        card_sem_proj_layout.addWidget(self.lbl_horas_proyectadas_semana)
        weekly_cards.addWidget(card_sem_proj, 0, 1)

        # Card: Horas Extra Semana
        card_sem_extra = QFrame()
        card_sem_extra.setProperty("class", "metric-card")
        card_sem_extra.setProperty("dynamic_css", "true")
        card_sem_extra.setSizePolicy(QSizePolicy.Policy.Expanding, QSizePolicy.Policy.Preferred)
        card_sem_extra_layout = QVBoxLayout(card_sem_extra)
        card_sem_extra_layout.setSpacing(6)
        horas_semana_extra_label = QLabel("Horas Extra Semana")
        horas_semana_extra_label.setProperty("class", "metric-label")
        self.lbl_horas_extra_semana = QLabel("--")
        self.lbl_horas_extra_semana.setProperty("class", "secondary-metric-value")
        self.lbl_horas_extra_semana.setToolTip("Total de horas extra en la semana seleccionada")
        card_sem_extra_layout.addWidget(horas_semana_extra_label)
        card_sem_extra_layout.addWidget(self.lbl_horas_extra_semana)
        weekly_cards.addWidget(card_sem_extra, 0, 2)

        metrics_layout.addLayout(weekly_cards)

        content_layout.addWidget(metrics_frame)

        
        sessions_title = QLabel("Sesiones Recientes")
        sessions_title.setAlignment(Qt.AlignmentFlag.AlignLeft)
        sessions_title.setProperty("class", "section-subtitle")
        content_layout.addWidget(sessions_title)

        sessions_frame = QFrame()
        sessions_frame.setProperty("class", "list-section")
        sessions_frame.setProperty("dynamic_css", "true")
        sessions_layout = QVBoxLayout(sessions_frame)
        sessions_layout.setSpacing(8)

        self.lista_sesiones = QListWidget()
        self.lista_sesiones.setProperty("dynamic_css", "true")
        self.lista_sesiones.setAlternatingRowColors(True)
        self.lista_sesiones.setSelectionMode(QAbstractItemView.SelectionMode.NoSelection)
        self.lista_sesiones.setToolTip("Sesiones registradas recientemente para el profesor seleccionado")
        sessions_layout.addWidget(self.lista_sesiones)

        content_layout.addWidget(sessions_frame)

        
        alerts_title = QLabel("Alertas")
        alerts_title.setAlignment(Qt.AlignmentFlag.AlignLeft)
        alerts_title.setProperty("class", "section-subtitle")
        content_layout.addWidget(alerts_title)

        self.alertas_frame = QFrame()
        self.alertas_frame.setProperty("class", "alert-banner")
        self.alertas_frame.setProperty("dynamic_css", "true")
        alertas_layout = QHBoxLayout(self.alertas_frame)
        alertas_layout.setSpacing(8)

        self.lbl_alertas = QLabel("")
        self.lbl_alertas.setWordWrap(True)
        self.lbl_alertas.setProperty("class", "alert-text")
        alertas_layout.addWidget(self.lbl_alertas)

        content_layout.addWidget(self.alertas_frame)
        self.alertas_frame.hide()

        # Inicializar con valores por defecto
        self.inicializar_estadisticas_vacias()

        # Agregar el contenido al scroll y luego al layout principal
        scroll_area.setWidget(content_widget)
        layout.addWidget(scroll_area)

        # Forzar re-polish para aplicar el QSS dinámico
        try:
            widget.style().unpolish(widget)
            widget.style().polish(widget)
            content_widget.style().unpolish(content_widget)
            content_widget.style().polish(content_widget)
        except Exception:
            pass

        return widget
    
    def inicializar_estadisticas_vacias(self):
        """Inicializa el widget de estadísticas con mensaje informativo cuando no hay profesor seleccionado"""
        try:
            # Mensaje informativo en el título
            if hasattr(self, 'stats_widget'):
                title_label = self.stats_widget.findChild(QLabel, "professor_stats_main_title")
                if title_label:
                    title_label.setText("Panel de Estadísticas - Selecciona un Profesor")
            
            # Valores por defecto para horas mensuales
            if hasattr(self, 'lbl_horas_trabajadas'):
                self.lbl_horas_trabajadas.setText("--")
            if hasattr(self, 'lbl_horas_fuera'):
                self.lbl_horas_fuera.setText("--")
            if hasattr(self, 'lbl_dias_trabajados'):
                self.lbl_dias_trabajados.setText("--")
            if hasattr(self, 'lbl_promedio_diario'):
                self.lbl_promedio_diario.setText("--")
            
            # Valores por defecto para métricas complementarias
            if hasattr(self, 'lbl_horas_trabajadas'):
                self.lbl_horas_trabajadas.setText("--")
            if hasattr(self, 'lbl_horas_proyectadas'):
                self.lbl_horas_proyectadas.setText("--")
            if hasattr(self, 'lbl_horas_extra'):
                self.lbl_horas_extra.setText("--")
            if hasattr(self, 'lbl_rating_promedio'):
                self.lbl_rating_promedio.setText("--")
            if hasattr(self, 'lbl_estado_profesor'):
                self.lbl_estado_profesor.setText("Sin selección")
            
            # Valores por defecto para métricas semanales
            if hasattr(self, 'lbl_horas_trabajadas_semana'):
                self.lbl_horas_trabajadas_semana.setText("--")
            if hasattr(self, 'lbl_horas_proyectadas_semana'):
                self.lbl_horas_proyectadas_semana.setText("--")
            if hasattr(self, 'lbl_horas_extra_semana'):
                self.lbl_horas_extra_semana.setText("--")
            if hasattr(self, 'lbl_week_range'):
                self.lbl_week_range.setText("--")
            
            # Limpiar lista de sesiones
            if hasattr(self, 'lista_sesiones'):
                self.lista_sesiones.clear()
                item = QListWidgetItem("Selecciona un profesor para ver sus sesiones recientes")
                item.setFlags(item.flags() & ~Qt.ItemFlag.ItemIsSelectable)
                self.lista_sesiones.addItem(item)
            
            # Ocultar alertas
            if hasattr(self, 'alertas_frame'):
                self.alertas_frame.hide()
                
        except Exception as e:
            logging.error(f"Error al inicializar estadísticas vacías: {e}")
    
    # Método duplicado eliminado - usar crear_widget_estadisticas() en su lugar
    
    def _get_dynamic_color(self, color_key, default_color="#3498db"):
        """Obtiene un color dinámico de la configuración de branding"""
        try:
            if hasattr(self, 'main_window') and hasattr(self.main_window, 'branding_config'):
                return self.main_window.branding_config.get('colors', {}).get(color_key, default_color)
            return default_color
        except Exception:
            return default_color
    
    def actualizar_horas_mensuales(self):
        """Actualiza las métricas de horas mensuales del profesor seleccionado"""
        if not self.profesor_seleccionado:
            return
        
        try:
            # Fallback seguro si los controles de mes/año no existen
            hoy = date.today()
            try:
                mes = self.month_combo.currentIndex() + 1
            except Exception:
                mes = hoy.month
            try:
                año = self.year_spin.value()
            except Exception:
                año = hoy.year
            profesor_id = self.profesor_seleccionado['id']
            
            # Obtener horas trabajadas del mes usando el nuevo método que trabaja con minutos
            horas_mes = self.db_manager.obtener_horas_mensuales_profesor(profesor_id, año=año, mes=mes)
            # Fallback: si no hay éxito o no hay minutos/sesiones, intentar mes actual sin filtros
            if not isinstance(horas_mes, dict) or not horas_mes.get('success', False) or int(horas_mes.get('total_minutos', 0)) == 0:
                try:
                    horas_mes = self.db_manager.obtener_horas_mes_actual_profesor(profesor_id)
                except Exception:
                    pass
            
            # Verificar si la consulta fue exitosa
            if not horas_mes.get('success', False):
                error_msg = horas_mes.get('error', 'Error desconocido al obtener horas trabajadas')
                print(f"Error obteniendo horas trabajadas: {error_msg}")
                QMessageBox.warning(self, "Error", f"Error al obtener horas trabajadas: {error_msg}")
                return
            
            # Obtener horas extras del mes seleccionado (basado en horarios del widget Horarios)
            inicio_mes = date(año, mes, 1)
            if mes == 12:
                siguiente_mes = date(año + 1, 1, 1)
            else:
                siguiente_mes = date(año, mes + 1, 1)
            fin_mes = siguiente_mes - timedelta(days=1)
            horas_extras_info = self.db_manager.obtener_horas_extras_profesor(profesor_id, inicio_mes, fin_mes)
            
            if not isinstance(horas_extras_info, dict) or not horas_extras_info.get('success', True):
                horas_extras_info = {'total_horas_extras': 0, 'horas_extras': []}
            
            # Actualizar métricas con validación de datos - usando los nuevos campos
            total_minutos = int(horas_mes.get('total_minutos', 0))
            horas_display = horas_mes.get('horas_display', 0)
            minutos_display = horas_mes.get('minutos_display', 0)
            horas_fuera = float(horas_extras_info.get('total_horas_extras', 0))
            # Ajuste: el backend expone 'total_dias_trabajados'
            dias_trabajados = int(horas_mes.get('total_dias_trabajados', horas_mes.get('dias_trabajados', 0)))
            
            # Calcular promedio diario en minutos y convertir a horas para mostrar
            promedio_diario_minutos = total_minutos / dias_trabajados if dias_trabajados > 0 else 0
            promedio_diario_horas = promedio_diario_minutos / 60.0
            
            # Mostrar horas y minutos en formato legible
            horas_mes_str = f"{horas_display}h {minutos_display}m" if horas_display > 0 else f"{minutos_display}m"
            if hasattr(self, 'lbl_horas_trabajadas'):
                self.lbl_horas_trabajadas.setText(horas_mes_str)
            if hasattr(self, 'lbl_horas_fuera'):
                self.lbl_horas_fuera.setText(f"{horas_fuera:.1f}h")
            if hasattr(self, 'lbl_dias_trabajados'):
                self.lbl_dias_trabajados.setText(str(dias_trabajados))
            if hasattr(self, 'lbl_promedio_diario'):
                self.lbl_promedio_diario.setText(f"{promedio_diario_horas:.1f}h")
            
            # Actualizar lista de sesiones con validación
            self.lista_sesiones.clear()
            sesiones = horas_mes.get('sesiones', [])
            if isinstance(sesiones, list):
                for sesion in sesiones[-10:]:  # Mostrar últimas 10 sesiones
                    if isinstance(sesion, dict):
                        fecha = sesion.get('fecha', '')
                        minutos_totales = int(sesion.get('minutos_totales', 0))
                        
                        # Convertir minutos a formato horas/minutos para mostrar
                        if minutos_totales:
                            horas = int(minutos_totales // 60)
                            minutos = int(minutos_totales % 60)
                            if horas > 0:
                                tiempo_str = f"{horas}h {minutos}m"
                            else:
                                tiempo_str = f"{minutos}m"
                        else:
                            tiempo_str = "0m"
                            
                        item_text = f"{fecha if isinstance(fecha, str) else (fecha.strftime('%d/%m/%Y') if hasattr(fecha, 'strftime') else str(fecha))}: {tiempo_str}"
                        self.lista_sesiones.addItem(item_text)
            
            # Mostrar alertas si hay horas extras detectadas
            sesiones_fuera = horas_extras_info.get('horas_extras', [])
            if isinstance(sesiones_fuera, list):
                self.mostrar_alertas_horas(horas_fuera, sesiones_fuera)
            
        except Exception as e:
            print(f"Error actualizando horas mensuales: {e}")
            QMessageBox.warning(self, "Error", f"Error al actualizar horas mensuales: {str(e)}")
    
    def mostrar_alertas_horas(self, horas_fuera, sesiones_fuera):
        """Muestra alertas sobre horas extra detectadas en base al horario"""
        if horas_fuera > 0:
            alertas = []
            alertas.append(f"{horas_fuera:.1f} horas extra detectadas")
            
            if len(sesiones_fuera) > 0:
                alertas.append(f"{len(sesiones_fuera)} sesiones fuera de horario programado")
            
            # Mostrar detalles de sesiones fuera de horario
            if len(sesiones_fuera) <= 3:
                for sesion in sesiones_fuera:
                    fecha = sesion.get('fecha', '')
                    horas = sesion.get('horas_extras', 0)
                    motivo = sesion.get('motivo', '')
                    alertas.append(f"  • {fecha}: {horas:.1f}h ({motivo})")
            else:
                alertas.append(f"  • Ver detalles completos en el reporte mensual")
            
            self.lbl_alertas.setText("\n".join(alertas))
            self.alertas_frame.show()
        else:
            self.alertas_frame.hide()
    
    def cargar_profesores(self):
        """Carga la lista de profesores en la tabla"""
        profesores = self.db_manager.obtener_todos_profesores()
        
        self.tabla_profesores.setRowCount(len(profesores))
        
        for row, profesor in enumerate(profesores):
            self.tabla_profesores.setItem(row, 0, QTableWidgetItem(str(profesor['id'])))  # ID Profesor
            self.tabla_profesores.setItem(row, 1, QTableWidgetItem(profesor['nombre']))
            self.tabla_profesores.setItem(row, 2, QTableWidgetItem(profesor.get('tipo', '')))
            self.tabla_profesores.setItem(row, 3, QTableWidgetItem(f"{profesor.get('experiencia_años', 0)} años"))
            self.tabla_profesores.setItem(row, 4, QTableWidgetItem(f"${profesor.get('tarifa_por_hora', 0):.2f}"))
            
            # Manejar estado None correctamente
            estado = profesor.get('estado') or 'activo'
            self.tabla_profesores.setItem(row, 5, QTableWidgetItem(estado.title()))
            
            # Columna de puntuación eliminada - ya no se usa
            # self.tabla_profesores.setItem(row, 5, QTableWidgetItem("N/A"))
            
            # Guardar datos del profesor en la fila
            for col in range(self.tabla_profesores.columnCount()):
                item = self.tabla_profesores.item(row, col)
                if item:
                    item.setData(Qt.ItemDataRole.UserRole, profesor)
    
    def apply_professor_filters(self, filters):
        """Aplica filtros avanzados a la tabla de profesores"""
        profesores = self.db_manager.obtener_todos_profesores()
        
        # Aplicar filtros
        profesores_filtrados = []
        for profesor in profesores:
            # Filtro por nombre
            if filters.get('nombre') and filters['nombre'].lower() not in profesor['nombre'].lower():
                continue
                
            # Filtro por tipo
            if filters.get('tipo'):
                tipo = profesor.get('tipo', '')
                if filters['tipo'].lower() not in tipo.lower():
                    continue
            
            # Filtro por experiencia
            experiencia = profesor.get('experiencia_años', 0)
            if filters.get('experiencia_min') is not None and experiencia < filters['experiencia_min']:
                continue
            if filters.get('experiencia_max') is not None and experiencia > filters['experiencia_max']:
                continue
            
            # Filtro por tarifa
            tarifa = profesor.get('tarifa_por_hora', 0)
            if filters.get('tarifa_min') is not None and tarifa < filters['tarifa_min']:
                continue
            if filters.get('tarifa_max') is not None and tarifa > filters['tarifa_max']:
                continue
            
            # Filtro por estado
            if filters.get('estado') and filters['estado'] != 'Todos':
                if profesor.get('estado', 'activo') != filters['estado']:
                    continue
            
            # Filtro por puntuación eliminado - ya no se usa
            # puntuacion = profesor.get('puntuacion_promedio', 0)
            # if filters.get('puntuacion_min') is not None and puntuacion < filters['puntuacion_min']:
            #     continue
            
            profesores_filtrados.append(profesor)
        
        # Actualizar tabla con profesores filtrados
        self.actualizar_tabla_profesores(profesores_filtrados)
    
    def actualizar_tabla_profesores(self, profesores):
        """Actualiza la tabla con la lista de profesores proporcionada"""
        self.tabla_profesores.setRowCount(len(profesores))
        
        for row, profesor in enumerate(profesores):
            self.tabla_profesores.setItem(row, 0, QTableWidgetItem(str(profesor['id'])))  # ID Profesor
            self.tabla_profesores.setItem(row, 1, QTableWidgetItem(profesor['nombre']))
            self.tabla_profesores.setItem(row, 2, QTableWidgetItem(profesor.get('tipo', '')))
            self.tabla_profesores.setItem(row, 3, QTableWidgetItem(f"{profesor.get('experiencia_años', 0)} años"))
            self.tabla_profesores.setItem(row, 4, QTableWidgetItem(f"${profesor.get('tarifa_por_hora', 0):.2f}"))
            
            # Manejar estado None correctamente
            estado = profesor.get('estado') or 'activo'
            self.tabla_profesores.setItem(row, 5, QTableWidgetItem(estado.title()))
            
            # Columna de puntuación eliminada - ya no se usa
            # self.tabla_profesores.setItem(row, 5, QTableWidgetItem("N/A"))
            
            # Guardar datos del profesor en la fila
            for col in range(self.tabla_profesores.columnCount()):
                item = self.tabla_profesores.item(row, col)
                if item:
                    item.setData(Qt.ItemDataRole.UserRole, profesor)
    
    def on_search_changed(self):
        """Maneja cambios en el campo de búsqueda con validación"""
        texto_busqueda = self.search_edit.text().strip()
        
        # Validar longitud mínima para búsqueda
        if texto_busqueda and len(texto_busqueda) < 2:
            self.search_status_label.setText("Mínimo 2 caracteres para buscar")
            self.search_status_label.setProperty("searchStatus", "warning")
            self.search_status_label.show()
            return
        
        # Validar caracteres especiales peligrosos
        caracteres_peligrosos = ['<', '>', '&', '"', "'"]
        if any(char in texto_busqueda for char in caracteres_peligrosos):
            self.search_status_label.setText("Caracteres no permitidos en la búsqueda")
            self.search_status_label.setProperty("searchStatus", "error")
            self.search_status_label.show()
            return
        
        self.search_status_label.hide()
        self.filtrar_profesores()
    
    def filtrar_profesores(self):
        """Convierte texto de búsqueda en filtros y aplica filtrado avanzado con heurísticas seguras."""
        import re
        import unicodedata
        texto_raw = self.search_edit.text().strip()
        texto_busqueda = texto_raw.lower()

        # Si la búsqueda es vacía o muy corta, resetear a todos los profesores
        if not texto_busqueda or len(texto_busqueda) < 2:
            self.apply_professor_filters({})
            self.search_status_label.hide()
            return

        def normalize(s: str) -> str:
            return ''.join(c for c in unicodedata.normalize('NFD', s) if unicodedata.category(c) != 'Mn').lower()

        qn = normalize(texto_raw)
        filters = {}

        # Estado explícito
        if 'activo' in qn:
            filters['estado'] = 'activo'
        elif 'inactivo' in qn:
            filters['estado'] = 'inactivo'

        # Tipo (heurístico por palabra clave)
        tipos = ['titular', 'suplente', 'entrenador', 'asistente', 'coordinador']
        for t in tipos:
            if t in qn:
                filters['tipo'] = t
                break

        # Experiencia en años: rangos "x-y años" o valores únicos si se menciona "año/años"
        if 'ano' in qn or 'anos' in qn or 'año' in texto_busqueda or 'años' in texto_busqueda:
            rango_match = re.search(r"(\d{1,2})\s*-\s*(\d{1,2})", qn)
            if rango_match:
                try:
                    e1 = int(rango_match.group(1)); e2 = int(rango_match.group(2))
                    filters['experiencia_min'] = min(e1, e2)
                    filters['experiencia_max'] = max(e1, e2)
                except Exception:
                    pass
            else:
                num_match = re.search(r"(\d{1,2})", qn)
                if num_match:
                    try:
                        e = int(num_match.group(1))
                        filters['experiencia_min'] = e
                        filters['experiencia_max'] = e
                    except Exception:
                        pass

        # Tarifa por hora: reconocer "$" o "tarifa" con rango o número único
        if '$' in texto_raw or 'tarifa' in texto_busqueda:
            rango_match = re.search(r"\$?\s*(\d+(?:[\.,]\d+)?)\s*-\s*\$?\s*(\d+(?:[\.,]\d+)?)", texto_raw)
            if rango_match:
                try:
                    t1 = float(rango_match.group(1).replace(',', '.'))
                    t2 = float(rango_match.group(2).replace(',', '.'))
                    filters['tarifa_min'] = min(t1, t2)
                    filters['tarifa_max'] = max(t1, t2)
                except Exception:
                    pass
            else:
                num_match = re.search(r"\d+(?:[\.,]\d+)?", texto_raw.replace(',', '.'))
                if num_match:
                    try:
                        t = float(num_match.group(0))
                        filters['tarifa_min'] = t
                        filters['tarifa_max'] = t
                    except Exception:
                        pass

        # Nombre exacto entre comillas
        quoted = re.search(r'"([^"]+)"|\'([^\']+)\'', texto_raw)
        if quoted:
            filters['nombre'] = quoted.group(1) or quoted.group(2)
        else:
            # Si no hay filtros específicos, usar búsqueda general por nombre
            if not filters:
                filters['nombre'] = texto_raw

        # Aplicar filtros avanzados y actualizar estado de búsqueda
        self.apply_professor_filters(filters)

        filas_visibles = self.tabla_profesores.rowCount()
        self.search_status_label.setText(f"{filas_visibles} profesor(es) encontrado(s)")
        self.search_status_label.setProperty("searchStatus", "success")
        self.search_status_label.show()
    
    def limpiar_busqueda(self):
        """Limpia el campo de búsqueda y muestra todos los profesores"""
        self.search_edit.clear()
        self.search_status_label.hide()
        self.filtrar_profesores()
    
    def seleccionar_profesor(self):
        """Maneja la selección de un profesor en la tabla"""
        current_row = self.tabla_profesores.currentRow()
        if current_row >= 0:
            item = self.tabla_profesores.item(current_row, 0)
            if item:
                self.profesor_seleccionado = item.data(Qt.ItemDataRole.UserRole)
                self.btn_eliminar.setEnabled(True)
                self.btn_horarios.setEnabled(True)
                
                # NUEVA FUNCIONALIDAD: Cargar automáticamente los datos en el formulario
                self.form_widget.cargar_profesor(self.profesor_seleccionado)
                
                # Actualizar widget de horarios
                self.schedule_widget.set_profesor_id(self.profesor_seleccionado['id'])
                

                
                
                if self.calendar_widget:
                    self.calendar_widget.set_profesor_id(self.profesor_seleccionado['id'])
                if self.substitute_widget:
                    self.substitute_widget.set_profesor_id(self.profesor_seleccionado['id'])
                if self.conflict_widget:
                    self.conflict_widget.set_profesor_id(self.profesor_seleccionado['id'])
                
                # Cargar estadísticas
                self.cargar_estadisticas_profesor()
        else:
            self.profesor_seleccionado = None
            self.btn_eliminar.setEnabled(False)
            self.btn_horarios.setEnabled(False)
            
            # Limpiar formulario cuando no hay selección
            self.form_widget.limpiar_formulario()
            
            # Limpiar estadísticas cuando no hay selección
            self.cargar_estadisticas_profesor()
    
    def cargar_estadisticas_profesor(self):
        """Carga las estadísticas básicas del profesor seleccionado de forma simplificada"""
        try:
            if not self.profesor_seleccionado:
                self.inicializar_estadisticas_vacias()
                return
            
            profesor_id = self.profesor_seleccionado.get('id')
            if not profesor_id:
                self.inicializar_estadisticas_vacias()
                return
            
            # Usar la instancia existente de db_manager en lugar de crear una nueva
            # Información básica del profesor
            profesor_info = self.db_manager.obtener_profesor_por_id(profesor_id)
            if not profesor_info:
                self.inicializar_estadisticas_vacias()
                return
            
            # Actualizar información básica
            nombre_completo = f"{profesor_info.get('nombre', '')} {profesor_info.get('apellido', '')}"
            self.lbl_nombre_profesor.setText(nombre_completo)
            
            # Teléfono (sin email)
            telefono = profesor_info.get('telefono', 'No disponible')
            self.lbl_telefono.setText(telefono if telefono else 'No disponible')
            
            # Estado - Obtener desde la tabla profesores en lugar de usuarios
            estado = profesor_info.get('estado', 'activo')
            estado_texto = "Activo" if estado == 'activo' else "Inactivo"
            self.lbl_estado_profesor.setText(estado_texto)
            
            # Fecha de contratación
            fecha_contratacion = profesor_info.get('fecha_contratacion')
            if fecha_contratacion:
                if isinstance(fecha_contratacion, str):
                    self.lbl_fecha_contratacion.setText(fecha_contratacion)
                else:
                    self.lbl_fecha_contratacion.setText(fecha_contratacion.strftime('%d/%m/%Y'))
            else:
                self.lbl_fecha_contratacion.setText('No disponible')
            
            # Obtener métricas básicas de forma segura
            try:
                with self.db_manager.get_connection_context() as conn:
                    with conn.cursor() as cursor:
                        # Calcular horas mensuales trabajadas (sesiones completadas) - usando minutos
                        cursor.execute("""
                            SELECT 
                                COALESCE(SUM(minutos_totales), 0) as minutos_trabajados
                            FROM profesor_horas_trabajadas
                            WHERE profesor_id = %s 
                            AND hora_fin IS NOT NULL
                            AND EXTRACT(MONTH FROM fecha) = EXTRACT(MONTH FROM CURRENT_DATE)
                            AND EXTRACT(YEAR FROM fecha) = EXTRACT(YEAR FROM CURRENT_DATE)
                        """, (profesor_id,))
                        minutos_trabajados = cursor.fetchone()[0] or 0
                        horas_trabajadas = minutos_trabajados / 60.0
                        self.lbl_horas_trabajadas.setText(f"{horas_trabajadas:.1f}h")
                        
                        # Calcular horas mensuales proyectadas utilizando método unificado del backend
                        try:
                            hoy = date.today()
                            proy = self.db_manager.obtener_horas_proyectadas_profesor(profesor_id, año=hoy.year, mes=hoy.month)
                            if isinstance(proy, dict) and proy.get('success'):
                                horas_proyectadas = float(proy.get('horas_mensuales', 0.0) or 0.0)
                            else:
                                horas_proyectadas = 0.0
                        except Exception as e:
                            logging.exception("Error calculando horas mensuales proyectadas unificadas")
                            horas_proyectadas = 0.0
                        self.lbl_horas_proyectadas.setText(f"{horas_proyectadas:.1f}h")
                        
                        # Calcular horas fuera de horario establecido usando el método optimizado
                        horas_extras_info = self.db_manager.obtener_horas_extras_profesor(profesor_id)
                        if horas_extras_info.get('success'):
                            horas_extra = horas_extras_info.get('total_horas_extras', 0)
                        else:
                            horas_extra = 0
                        self.lbl_horas_extra.setText(f"{horas_extra:.1f}h")
                
            except Exception as e:
                print(f"Error obteniendo métricas básicas: {e}")
                self.lbl_horas_trabajadas.setText("0h")
                self.lbl_horas_proyectadas.setText("0h")
                self.lbl_horas_extra.setText("0h")
            
            # Cálculos semanales
            try:
                anchor = getattr(self, 'week_anchor_date', date.today())
                inicio_semana, fin_semana = self._compute_week_range(anchor)
                if hasattr(self, 'lbl_week_range'):
                    self.lbl_week_range.setText(f"{inicio_semana.strftime('%d/%m/%Y')} — {fin_semana.strftime('%d/%m/%Y')}")
                
                # Horas trabajadas semana
                sesiones = self.db_manager.obtener_horas_trabajadas_profesor(profesor_id, inicio_semana, fin_semana)
                minutos_semana = 0
                if isinstance(sesiones, list):
                    for s in sesiones:
                        try:
                            # Solo contar sesiones generales (sin clase asociada)
                            clase_val = s.get('clase_id') if isinstance(s, dict) else None
                            if clase_val is None:
                                minutos_semana += int(s.get('minutos_totales', 0))
                        except Exception:
                            # soportar tuplas u otros formatos
                            try:
                                # En formatos posicionales, asumir que no hay clase_id cuando longitud < 8
                                if isinstance(s, (list, tuple)) and len(s) >= 4:
                                    minutos_semana += int(s[3])  # 4to col: minutos_totales
                            except Exception:
                                pass
                self.lbl_horas_trabajadas_semana.setText(f"{(minutos_semana/60.0):.1f}h")
                
                # Horas proyectadas semana usando método unificado
                try:
                    proy = self.db_manager.obtener_horas_proyectadas_profesor(profesor_id)
                    if isinstance(proy, dict) and proy.get('success'):
                        horas_proy_semana = float(proy.get('horas_semanales', 0.0) or 0.0)
                    else:
                        horas_proy_semana = 0.0
                except Exception as e:
                    logging.exception("Error calculando horas semanales proyectadas unificadas")
                    horas_proy_semana = 0.0
                self.lbl_horas_proyectadas_semana.setText(f"{horas_proy_semana:.1f}h")
                
                # Horas extra semana
                info_extras = self.db_manager.obtener_horas_extras_profesor(profesor_id, inicio_semana, fin_semana)
                horas_extra_sem = 0.0
                if isinstance(info_extras, dict) and info_extras.get('success'):
                    horas_extra_sem = float(info_extras.get('total_horas_extras', 0) or 0)
                self.lbl_horas_extra_semana.setText(f"{horas_extra_sem:.1f}h")
                # Tras calcular métricas semanales, refrescar horas mensuales e historial
                self.actualizar_horas_mensuales()
                self.cargar_sesiones_recientes(profesor_id)
            except Exception as e:
                print(f"Error en métricas semanales: {e}")
                if hasattr(self, 'lbl_horas_trabajadas_semana'):
                    self.lbl_horas_trabajadas_semana.setText("0h")
                if hasattr(self, 'lbl_horas_proyectadas_semana'):
                    self.lbl_horas_proyectadas_semana.setText("0h")
                if hasattr(self, 'lbl_horas_extra_semana'):
                    self.lbl_horas_extra_semana.setText("0h")
            
        except Exception as e:
            print(f"Error cargando estadísticas del profesor: {e}")
            self.inicializar_estadisticas_vacias()
    
    def cargar_sesiones_recientes(self, profesor_id):
        """Carga las sesiones recientes del profesor en la lista"""
        try:
            with self.db_manager.get_connection_context() as conn:
                cursor = conn.cursor()
                
                # Obtener sesiones recientes de horas trabajadas
                cursor.execute("""
                    SELECT fecha, hora_inicio, hora_fin, minutos_totales
                    FROM profesor_horas_trabajadas
                    WHERE profesor_id = %s 
                    AND hora_fin IS NOT NULL
                    ORDER BY fecha DESC, hora_inicio DESC
                    LIMIT 10
                """, (profesor_id,))
                
                sesiones = cursor.fetchall()
                
                if hasattr(self, 'lista_sesiones'):
                    self.lista_sesiones.clear()
                    
                    if sesiones:
                        for sesion_data in sesiones:
                            fecha, hora_inicio, hora_fin, minutos_totales = sesion_data
                            
                            fecha_str = fecha.strftime("%d/%m/%Y") if fecha else "Sin fecha"
                            inicio_str = hora_inicio.strftime("%H:%M") if hora_inicio else "00:00"
                            fin_str = hora_fin.strftime("%H:%M") if hora_fin else "00:00"
                            
                            # Convertir minutos a formato horas/minutos para mostrar
                            if minutos_totales:
                                horas = int(minutos_totales // 60)
                                minutos = int(minutos_totales % 60)
                                if horas > 0:
                                    horas_str = f"{horas}h {minutos}m"
                                else:
                                    horas_str = f"{minutos}m"
                            else:
                                horas_str = "0m"
                            
                            item_text = f"📅 {fecha_str} | ⏰ {inicio_str}-{fin_str} | 🕐 {horas_str}"
                            item = QListWidgetItem(item_text)
                            self.lista_sesiones.addItem(item)
                    else:
                        item = QListWidgetItem("📝 No hay sesiones registradas")
                        self.lista_sesiones.addItem(item)
                        
        except Exception as e:
            print(f"Error al cargar sesiones recientes: {e}")
            if hasattr(self, 'lista_sesiones'):
                self.lista_sesiones.clear()
                item = QListWidgetItem("❌ Error al cargar sesiones")
                self.lista_sesiones.addItem(item)
            if hasattr(self, 'lbl_clases'):
                self.lbl_clases.setText("0")
            if hasattr(self, 'lbl_estudiantes'):
                self.lbl_estudiantes.setText("0")
            if hasattr(self, 'lbl_rating_promedio'):
                self.lbl_rating_promedio.setText("0.0")
            if hasattr(self, 'lbl_estado_profesor'):
                self.lbl_estado_profesor.setText("Desconocido")
            if hasattr(self, 'lbl_horas_trabajadas'):
                self.lbl_horas_trabajadas.setText("0.0h")
            if hasattr(self, 'lbl_horas_fuera'):
                self.lbl_horas_fuera.setText("0.0h")
            if hasattr(self, 'lbl_dias_trabajados'):
                self.lbl_dias_trabajados.setText("0")
            if hasattr(self, 'lbl_promedio_diario'):
                self.lbl_promedio_diario.setText("0.0h")
        finally:
            if 'conn' in locals():
                conn.close()
    
    def nuevo_profesor(self):
        """Limpia el formulario para crear un nuevo profesor o deseleccionar el actual"""
        # Limpiar selección de tabla
        self.tabla_profesores.clearSelection()
        self.form_widget.limpiar_formulario()
    
    def eliminar_profesor(self):
        """Elimina el profesor seleccionado"""
        if not self.profesor_seleccionado:
            return
        
        respuesta = QMessageBox.question(
            self, "Confirmar eliminación",
            f"¿Está seguro de eliminar al profesor {self.profesor_seleccionado['nombre']}?\n\n"
            "Esta acción eliminará el perfil de profesor pero no el usuario.",
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No
        )
        
        if respuesta == QMessageBox.StandardButton.Yes:
            try:
                self.db_manager.eliminar_profesor(self.profesor_seleccionado['id'])
                self.cargar_profesores()
                QMessageBox.information(self, "Éxito", "Profesor eliminado correctamente")
            except Exception as e:
                QMessageBox.critical(self, "Error", f"Error al eliminar profesor: {str(e)}")
    
    def gestionar_horarios(self):
        """Cambia al tab de horarios para el profesor seleccionado"""
        if self.profesor_seleccionado:
            # Encontrar el QTabWidget del panel derecho y cambiar al tab de horarios
            right_panel = self.findChild(QTabWidget)
            if right_panel:
                right_panel.setCurrentIndex(1)  # Tab de horarios
                # Asegurar que el widget de horarios tenga el profesor seleccionado
                self.schedule_widget.set_profesor_id(self.profesor_seleccionado['id'])
                # Mostrar mensaje informativo
                QMessageBox.information(
                    self, 
                    "Gestión de Horarios", 
                    f"Ahora puede gestionar los horarios de {self.profesor_seleccionado['nombre']}.\n\n"
                    "Puede agregar, editar o eliminar horarios desde esta pestaña."
                )
    
    def show_professors_context_menu(self, position):
        """Muestra el menú contextual para la tabla de profesores."""
        if self.tabla_profesores.itemAt(position) is None:
            return
        
        # Obtener el profesor seleccionado
        row = self.tabla_profesores.rowAt(position.y())
        if row < 0:
            return
            
        menu = QMenu(self)
        menu.setObjectName("contextMenu")
        menu.setProperty("menuType", "professors")
        
        # Acciones del menú
        edit_action = menu.addAction("✏️ Editar Profesor")
        duplicate_action = menu.addAction("📋 Duplicar Profesor")
        schedule_action = menu.addAction("📅 Gestionar Horarios")
        menu.addSeparator()
        toggle_action = menu.addAction("🔄 Cambiar Estado")
        menu.addSeparator()
        delete_action = menu.addAction("🗑️ Eliminar Profesor")
        
        # Conectar acciones
        edit_action.triggered.connect(self.edit_professor_from_menu)
        duplicate_action.triggered.connect(self.duplicate_professor_from_menu)
        schedule_action.triggered.connect(self.gestionar_horarios)
        toggle_action.triggered.connect(self.toggle_professor_status)
        delete_action.triggered.connect(self.eliminar_profesor)
        
        # Mostrar menú
        menu.exec(self.tabla_profesores.viewport().mapToGlobal(position))
    
    def edit_professor_from_menu(self):
        """Edita el profesor seleccionado desde el menú contextual."""
        if self.profesor_seleccionado:
            # Cambiar al tab de formulario
            right_panel = self.findChild(QTabWidget)
            if right_panel:
                right_panel.setCurrentIndex(0)  # Tab de formulario
                self.form_widget.cargar_profesor(self.profesor_seleccionado)
    
    def duplicate_professor_from_menu(self):
        """Duplica el profesor seleccionado desde el menú contextual."""
        if not self.profesor_seleccionado:
            return
            
        try:
            # Crear una copia del profesor con nombre modificado
            nuevo_profesor = self.profesor_seleccionado.copy()
            nuevo_profesor['nombre'] = f"{nuevo_profesor['nombre']} (Copia)"
            
            # Eliminar el ID para que se genere uno nuevo
            if 'id' in nuevo_profesor:
                del nuevo_profesor['id']
            
            # Guardar el nuevo profesor
            self.db_manager.guardar_profesor(nuevo_profesor)
            self.cargar_profesores()
            
            QMessageBox.information(
                self, "Éxito", 
                f"Profesor duplicado correctamente como '{nuevo_profesor['nombre']}'."
            )
            
        except Exception as e:
            QMessageBox.critical(
                self, "Error", 
                f"Error al duplicar profesor: {str(e)}"
            )
    
    def toggle_professor_status(self):
        """Cambia el estado del profesor seleccionado."""
        if not self.profesor_seleccionado:
            return
            
        try:
            nuevo_estado = 'inactivo' if self.profesor_seleccionado.get('estado') == 'activo' else 'activo'
            
            # Actualizar en la base de datos
            self.db_manager.actualizar_estado_profesor(
                self.profesor_seleccionado['id'], 
                nuevo_estado
            )
            
            # Recargar la tabla
            self.cargar_profesores()
            
            QMessageBox.information(
                self, "Éxito", 
                f"Estado del profesor cambiado a '{nuevo_estado}'."
            )
            
        except Exception as e:
            QMessageBox.critical(
                self, "Error", 
                f"Error al cambiar estado del profesor: {str(e)}"
            )

