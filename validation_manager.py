from typing import Dict, List, Optional, Tuple, Any
import re
from datetime import datetime, date, time
from PyQt6.QtWidgets import QWidget, QLineEdit, QTextEdit, QSpinBox, QDoubleSpinBox, QComboBox, QDateEdit, QTimeEdit
from PyQt6.QtCore import QObject, pyqtSignal
from PyQt6.QtGui import QPalette

class ValidationResult:
    """Resultado de una validación"""
    
    def __init__(self, is_valid: bool = True, message: str = "", field_name: str = ""):
        self.is_valid = is_valid
        self.message = message
        self.field_name = field_name
    
    def __bool__(self):
        return self.is_valid
    
    def __str__(self):
        return self.message if not self.is_valid else "Válido"

class FieldValidator:
    """Validador para campos individuales"""
    
    @staticmethod
    def required(value: Any, field_name: str = "Campo") -> ValidationResult:
        """Valida que el campo no esté vacío"""
        if value is None or (isinstance(value, str) and not value.strip()):
            return ValidationResult(False, f"{field_name} es requerido", field_name)
        return ValidationResult(True)
    
    @staticmethod
    def min_length(value: str, min_len: int, field_name: str = "Campo") -> ValidationResult:
        """Valida longitud mínima"""
        if len(value.strip()) < min_len:
            return ValidationResult(False, f"{field_name} debe tener al menos {min_len} caracteres", field_name)
        return ValidationResult(True)
    
    @staticmethod
    def max_length(value: str, max_len: int, field_name: str = "Campo") -> ValidationResult:
        """Valida longitud máxima"""
        if len(value.strip()) > max_len:
            return ValidationResult(False, f"{field_name} no puede exceder {max_len} caracteres", field_name)
        return ValidationResult(True)
    
    @staticmethod
    def email(value: str, field_name: str = "Email") -> ValidationResult:
        """Valida formato de email"""
        pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
        if not re.match(pattern, value.strip()):
            return ValidationResult(False, f"{field_name} no tiene un formato válido", field_name)
        return ValidationResult(True)
    
    @staticmethod
    def phone(value: str, field_name: str = "Teléfono") -> ValidationResult:
        """Valida formato de teléfono"""
        # Acepta formatos: +54 11 1234-5678, 011-1234-5678, 1234567890
        pattern = r'^(\+?\d{1,3}[\s-]?)?(\(?\d{2,4}\)?[\s-]?)?\d{3,4}[\s-]?\d{3,4}$'
        cleaned = re.sub(r'[\s()-]', '', value.strip())
        if len(cleaned) < 8 or not re.match(pattern, value.strip()):
            return ValidationResult(False, f"{field_name} no tiene un formato válido", field_name)
        return ValidationResult(True)
    
    @staticmethod
    def dni(value: str, field_name: str = "DNI") -> ValidationResult:
        """Valida formato de DNI argentino"""
        cleaned = re.sub(r'[^\d]', '', value.strip())
        if len(cleaned) < 7 or len(cleaned) > 8 or not cleaned.isdigit():
            return ValidationResult(False, f"{field_name} debe tener entre 7 y 8 dígitos", field_name)
        return ValidationResult(True)
    
    @staticmethod
    def numeric_range(value: float, min_val: float = None, max_val: float = None, field_name: str = "Campo") -> ValidationResult:
        """Valida rango numérico"""
        if min_val is not None and value < min_val:
            return ValidationResult(False, f"{field_name} debe ser mayor o igual a {min_val}", field_name)
        if max_val is not None and value > max_val:
            return ValidationResult(False, f"{field_name} debe ser menor o igual a {max_val}", field_name)
        return ValidationResult(True)
    
    @staticmethod
    def date_range(value: date, min_date: date = None, max_date: date = None, field_name: str = "Fecha") -> ValidationResult:
        """Valida rango de fechas"""
        if min_date and value < min_date:
            return ValidationResult(False, f"{field_name} no puede ser anterior a {min_date.strftime('%d/%m/%Y')}", field_name)
        if max_date and value > max_date:
            return ValidationResult(False, f"{field_name} no puede ser posterior a {max_date.strftime('%d/%m/%Y')}", field_name)
        return ValidationResult(True)
    
    @staticmethod
    def time_range(start_time: time, end_time: time, field_name: str = "Horario") -> ValidationResult:
        """Valida que la hora de inicio sea menor que la de fin"""
        if start_time >= end_time:
            return ValidationResult(False, f"La hora de inicio debe ser menor que la hora de fin", field_name)
        return ValidationResult(True)
    
    @staticmethod
    def time_range_validation(time_str: str, min_time: str, max_time: str, field_name: str = "Hora") -> ValidationResult:
        """Valida que una hora esté dentro de un rango específico"""
        try:
            # Convertir strings a objetos time para comparación
            from datetime import time
            
            # Parsear la hora actual
            if ':' in time_str:
                hour, minute = map(int, time_str.split(':'))
                current_time = time(hour, minute)
            else:
                return ValidationResult(False, f"{field_name} debe tener formato HH:MM", field_name)
            
            # Parsear hora mínima
            min_hour, min_minute = map(int, min_time.split(':'))
            min_time_obj = time(min_hour, min_minute)
            
            # Parsear hora máxima
            max_hour, max_minute = map(int, max_time.split(':'))
            max_time_obj = time(max_hour, max_minute)
            
            # Validar rango
            if current_time < min_time_obj:
                return ValidationResult(False, f"{field_name} debe ser mayor o igual a {min_time}", field_name)
            if current_time > max_time_obj:
                return ValidationResult(False, f"{field_name} debe ser menor o igual a {max_time}", field_name)
            
            return ValidationResult(True)
        except (ValueError, AttributeError) as e:
            return ValidationResult(False, f"{field_name} tiene formato inválido", field_name)
    
    @staticmethod
    def positive_number(value: float, field_name: str = "Campo") -> ValidationResult:
        """Valida que el número sea positivo"""
        if value <= 0:
            return ValidationResult(False, f"{field_name} debe ser un número positivo", field_name)
        return ValidationResult(True)
    
    @staticmethod
    def custom_regex(value: str, pattern: str, message: str, field_name: str = "Campo") -> ValidationResult:
        """Validación con expresión regular personalizada"""
        if not re.match(pattern, value.strip()):
            return ValidationResult(False, message, field_name)
        return ValidationResult(True)

class FormValidator(QObject):
    """Validador de formularios con validación en tiempo real"""
    
    validation_changed = pyqtSignal(str, bool, str)  # field_name, is_valid, message
    form_validation_changed = pyqtSignal(bool)  # is_form_valid
    
    def __init__(self):
        super().__init__()
        self.fields = {}  # field_name -> {'widget': widget, 'validators': [validators], 'required': bool}
        self.validation_results = {}  # field_name -> ValidationResult
        self.real_time_enabled = True
    
    def add_field(self, field_name: str, widget: QWidget, validators: List[callable] = None, required: bool = False):
        """Agrega un campo al validador"""
        if validators is None:
            validators = []
        
        self.fields[field_name] = {
            'widget': widget,
            'validators': validators,
            'required': required
        }
        
        # Conectar eventos para validación en tiempo real
        if self.real_time_enabled:
            self._connect_widget_events(widget, field_name)
    
    def _connect_widget_events(self, widget: QWidget, field_name: str):
        """Conecta eventos del widget para validación en tiempo real"""
        if isinstance(widget, (QLineEdit, QTextEdit)):
            widget.textChanged.connect(lambda: self._validate_field_real_time(field_name))
        elif isinstance(widget, (QSpinBox, QDoubleSpinBox)):
            widget.valueChanged.connect(lambda: self._validate_field_real_time(field_name))
        elif isinstance(widget, QComboBox):
            widget.currentTextChanged.connect(lambda: self._validate_field_real_time(field_name))
        elif isinstance(widget, QDateEdit):
            widget.dateChanged.connect(lambda: self._validate_field_real_time(field_name))
        elif isinstance(widget, QTimeEdit):
            widget.timeChanged.connect(lambda: self._validate_field_real_time(field_name))
    
    def _validate_field_real_time(self, field_name: str):
        """Valida un campo en tiempo real"""
        if not self.real_time_enabled:
            return
        
        result = self.validate_field(field_name)
        self._apply_visual_feedback(field_name, result)
        self.validation_changed.emit(field_name, result.is_valid, result.message)
        
        # Verificar validación completa del formulario
        self._check_form_validation()
    
    def validate_field(self, field_name: str) -> ValidationResult:
        """Valida un campo específico"""
        if field_name not in self.fields:
            return ValidationResult(False, f"Campo {field_name} no encontrado")
        
        field_info = self.fields[field_name]
        widget = field_info['widget']
        validators = field_info['validators']
        required = field_info['required']
        
        # Obtener valor del widget
        value = self._get_widget_value(widget)
        
        # Validar campo requerido
        if required:
            result = FieldValidator.required(value, field_name)
            if not result.is_valid:
                self.validation_results[field_name] = result
                return result
        
        # Si el campo está vacío y no es requerido, es válido
        if not value and not required:
            result = ValidationResult(True)
            self.validation_results[field_name] = result
            return result
        
        # Ejecutar validadores personalizados
        for validator in validators:
            try:
                result = validator(value, field_name)
                if not result.is_valid:
                    self.validation_results[field_name] = result
                    return result
            except Exception as e:
                result = ValidationResult(False, f"Error en validación: {str(e)}", field_name)
                self.validation_results[field_name] = result
                return result
        
        # Si llegamos aquí, el campo es válido
        result = ValidationResult(True)
        self.validation_results[field_name] = result
        return result
    
    def _get_widget_value(self, widget: QWidget) -> Any:
        """Obtiene el valor de un widget"""
        if isinstance(widget, QLineEdit):
            return widget.text()
        elif isinstance(widget, QTextEdit):
            return widget.toPlainText()
        elif isinstance(widget, QSpinBox):
            return widget.value()
        elif isinstance(widget, QDoubleSpinBox):
            return widget.value()
        elif isinstance(widget, QComboBox):
            return widget.currentText()
        elif isinstance(widget, QDateEdit):
            return widget.date().toPyDate()
        elif isinstance(widget, QTimeEdit):
            return widget.time().toString("HH:mm")
        else:
            return None
    
    def _apply_visual_feedback(self, field_name: str, result: ValidationResult):
        """Aplica retroalimentación visual al campo"""
        if field_name not in self.fields:
            return
        
        widget = self.fields[field_name]['widget']
        
        if result.is_valid:
            widget.setProperty("validation_state", "success")
        else:
            widget.setProperty("validation_state", "error")
        
        # Forzar actualización del estilo
        widget.style().unpolish(widget)
        widget.style().polish(widget)
        widget.update()
    
    def validate_all(self) -> Tuple[bool, Dict[str, ValidationResult]]:
        """Valida todos los campos del formulario"""
        all_valid = True
        results = {}
        
        for field_name in self.fields.keys():
            result = self.validate_field(field_name)
            results[field_name] = result
            if not result.is_valid:
                all_valid = False
            
            # Aplicar retroalimentación visual
            self._apply_visual_feedback(field_name, result)
        
        self.form_validation_changed.emit(all_valid)
        return all_valid, results
    
    def _check_form_validation(self):
        """Verifica si todo el formulario es válido"""
        all_valid = all(result.is_valid for result in self.validation_results.values())
        self.form_validation_changed.emit(all_valid)
    
    def clear_validation(self):
        """Limpia todas las validaciones"""
        self.validation_results.clear()
        
        for field_name, field_info in self.fields.items():
            widget = field_info['widget']
            widget.setProperty("validation_state", "")
            widget.style().unpolish(widget)
            widget.style().polish(widget)
            widget.update()
    
    def get_validation_errors(self) -> List[str]:
        """Obtiene lista de errores de validación"""
        errors = []
        for result in self.validation_results.values():
            if not result.is_valid:
                errors.append(result.message)
        return errors
    
    def set_real_time_validation(self, enabled: bool):
        """Habilita/deshabilita validación en tiempo real"""
        self.real_time_enabled = enabled

class BusinessValidator:
    """Validaciones específicas del negocio del gimnasio"""
    
    def __init__(self, db_manager):
        self.db_manager = db_manager
    
    def validate_unique_dni(self, dni: str, exclude_user_id: int = None) -> ValidationResult:
        """Valida que el DNI sea único"""
        usuarios = self.db_manager.obtener_todos_usuarios()
        for usuario in usuarios:
            if usuario['dni'] == dni and (exclude_user_id is None or usuario['id'] != exclude_user_id):
                return ValidationResult(False, "Ya existe un usuario con este DNI", "DNI")
        return ValidationResult(True)
    
    def validate_professor_schedule_conflict(self, profesor_id: int, dia: str, hora_inicio: str, hora_fin: str, exclude_horario_id: int = None) -> ValidationResult:
        """Valida que no haya conflictos de horarios para un profesor"""
        horarios = self.db_manager.obtener_horarios_profesor(profesor_id)
        
        for horario in horarios:
            if exclude_horario_id and horario['id'] == exclude_horario_id:
                continue
            
            if horario['dia_semana'] == dia:
                # Verificar solapamiento de horarios
                if (hora_inicio < horario['hora_fin'] and hora_fin > horario['hora_inicio']):
                    return ValidationResult(
                        False, 
                        f"Conflicto de horario con {horario['hora_inicio']}-{horario['hora_fin']}",
                        "Horario"
                    )
        
        return ValidationResult(True)
    
    def validate_class_capacity(self, clase_id: int, new_enrollments: int = 1) -> ValidationResult:
        """Valida que la clase no exceda su capacidad"""
        try:
            # Obtener información de la clase
            clases = self.db_manager.obtener_todas_clases()
            clase = next((c for c in clases if c['id'] == clase_id), None)
            
            if not clase:
                return ValidationResult(False, "Clase no encontrada", "Clase")
            
            # Obtener inscripciones actuales
            inscripciones = self.db_manager.obtener_inscripciones_clase(clase_id)
            inscripciones_actuales = len(inscripciones)
            
            if inscripciones_actuales + new_enrollments > clase.get('capacidad_maxima', 0):
                return ValidationResult(
                    False, 
                    f"La clase excedería su capacidad máxima ({clase.get('capacidad_maxima', 0)})",
                    "Capacidad"
                )
            
            return ValidationResult(True)
        except Exception as e:
            return ValidationResult(False, f"Error al validar capacidad: {str(e)}", "Capacidad")
    
    def validate_payment_amount(self, usuario_id: int, monto: float) -> ValidationResult:
        """Valida que el monto del pago sea correcto según el tipo de cuota"""
        try:
            usuario = self.db_manager.obtener_usuario_por_id(usuario_id)
            if not usuario:
                return ValidationResult(False, "Usuario no encontrado", "Usuario")
            
            if monto <= 0:
                return ValidationResult(False, "El monto debe ser mayor a cero", "Monto")
            
            # Validar monto según tipo de cuota del usuario
            try:
                precio_esperado = self.db_manager.obtener_precio_cuota(usuario.tipo_cuota)
                if precio_esperado > 0:
                    # Permitir una tolerancia del 5% para variaciones menores
                    tolerancia = precio_esperado * 0.05
                    if abs(monto - precio_esperado) > tolerancia:
                        return ValidationResult(
                            False, 
                            f"El monto ${monto:,.2f} no coincide con el precio esperado para '{usuario.tipo_cuota}': ${precio_esperado:,.2f}",
                            "Monto"
                        )
            except Exception:
                # Si no se puede obtener el precio esperado, solo validar que sea positivo
                pass
            
            return ValidationResult(True)
        except Exception as e:
            return ValidationResult(False, f"Error al validar pago: {str(e)}", "Pago")
    
    def validate_routine_exercises(self, ejercicios: List[Dict]) -> ValidationResult:
        """Valida que una rutina tenga ejercicios válidos"""
        if not ejercicios:
            return ValidationResult(False, "La rutina debe tener al menos un ejercicio", "Ejercicios")
        
        for i, ejercicio in enumerate(ejercicios):
            if not ejercicio.get('ejercicio_id'):
                return ValidationResult(False, f"Ejercicio {i+1}: Debe seleccionar un ejercicio", "Ejercicios")
            
            # Series y repeticiones pueden quedar en blanco para anotar a mano en la rutina exportada
            series_val = ejercicio.get('series', None)
            rep_val = ejercicio.get('repeticiones', None)
            # Aceptar None, cadena vacía o 0 como "no asignado"
            if series_val not in (None, ""):
                try:
                    if int(series_val) <= 0:
                        return ValidationResult(False, f"Ejercicio {i+1}: Las series deben ser mayor a 0 si se informan", "Ejercicios")
                except Exception:
                    return ValidationResult(False, f"Ejercicio {i+1}: Las series deben ser un número entero válido o quedar en blanco", "Ejercicios")
            if rep_val not in (None, ""):
                try:
                    if int(rep_val) <= 0:
                        return ValidationResult(False, f"Ejercicio {i+1}: Las repeticiones deben ser mayor a 0 si se informan", "Ejercicios")
                except Exception:
                    return ValidationResult(False, f"Ejercicio {i+1}: Las repeticiones deben ser un número entero válido o quedar en blanco", "Ejercicios")
        
        return ValidationResult(True)
    
    def validate_quota_type_name(self, nombre: str, exclude_id: int = None) -> ValidationResult:
        """Valida que el nombre del tipo de cuota sea único y válido"""
        # Validar formato del nombre
        if not nombre or not nombre.strip():
            return ValidationResult(False, "El nombre del tipo de cuota es obligatorio", "Nombre")
        
        nombre = nombre.strip()
        
        if len(nombre) < 2:
            return ValidationResult(False, "El nombre debe tener al menos 2 caracteres", "Nombre")
        
        if len(nombre) > 50:
            return ValidationResult(False, "El nombre no puede exceder 50 caracteres", "Nombre")
        
        # Validar caracteres permitidos
        import re
        if not re.match(r'^[a-zA-ZáéíóúÁÉÍÓÚñÑ0-9\s\-_]+$', nombre):
            return ValidationResult(False, "El nombre solo puede contener letras, números, espacios, guiones y guiones bajos", "Nombre")
        
        # Validar unicidad
        try:
            existing = self.db_manager.obtener_tipo_cuota_por_nombre(nombre)
            if existing and (exclude_id is None or existing.id != exclude_id):
                return ValidationResult(False, f"Ya existe un tipo de cuota con el nombre '{nombre}'", "Nombre")
        except Exception as e:
            return ValidationResult(False, f"Error al validar unicidad del nombre: {str(e)}", "Nombre")
        
        return ValidationResult(True)
    
    def validate_quota_type_price(self, precio: float) -> ValidationResult:
        """Valida que el precio del tipo de cuota sea válido"""
        if precio <= 0:
            return ValidationResult(False, "El precio debe ser mayor a 0", "Precio")
        
        if precio > 999999.99:
            return ValidationResult(False, "El precio no puede exceder $999,999.99", "Precio")
        
        return ValidationResult(True)
    
    def validate_quota_type_system_integrity(self, tipo_cuota_id: int = None, activating: bool = True) -> ValidationResult:
        """Valida la integridad del sistema de tipos de cuota"""
        try:
            tipos_cuota = self.db_manager.obtener_tipos_cuota()
            
            if not tipos_cuota:
                return ValidationResult(False, "Debe existir al menos un tipo de cuota en el sistema", "Sistema")
            
            # Contar tipos activos
            tipos_activos = [tipo for tipo in tipos_cuota if tipo.activo]
            
            # Si estamos desactivando un tipo, verificar que no sea el único activo
            if not activating and tipo_cuota_id:
                tipo_actual = next((t for t in tipos_cuota if t.id == tipo_cuota_id), None)
                if tipo_actual and tipo_actual.activo:
                    if len(tipos_activos) == 1 and tipos_activos[0].id == tipo_cuota_id:
                        return ValidationResult(
                            False, 
                            "No se puede desactivar el único tipo de cuota activo. Debe haber al menos un tipo activo en el sistema",
                            "Sistema"
                        )
            
            # Verificar que no se eliminen tipos en uso
            if tipo_cuota_id:
                usuarios_usando = self.db_manager.contar_usuarios_por_tipo_cuota(tipo_cuota_id)
                if usuarios_usando > 0 and not activating:
                    return ValidationResult(
                        False,
                        f"No se puede desactivar este tipo de cuota porque {usuarios_usando} usuario(s) lo están usando",
                        "Sistema"
                    )
            
            return ValidationResult(True)
            
        except Exception as e:
            return ValidationResult(False, f"Error al validar integridad del sistema: {str(e)}", "Sistema")
    
    def validate_quota_type_deletion(self, tipo_cuota_id: int) -> ValidationResult:
        """Valida que un tipo de cuota pueda ser eliminado"""
        try:
            # Verificar que no sea el único tipo activo
            tipos_cuota = self.db_manager.obtener_tipos_cuota()
            tipos_activos = [tipo for tipo in tipos_cuota if tipo.activo and tipo.id != tipo_cuota_id]
            
            tipo_a_eliminar = next((t for t in tipos_cuota if t.id == tipo_cuota_id), None)
            if not tipo_a_eliminar:
                return ValidationResult(False, "Tipo de cuota no encontrado", "Eliminación")
            
            if not tipos_activos and tipo_a_eliminar.activo:
                return ValidationResult(
                    False,
                    "No se puede eliminar el único tipo de cuota activo. Debe crear otro tipo activo primero",
                    "Eliminación"
                )
            
            # Verificar que no haya usuarios usando este tipo
            usuarios_usando = self.db_manager.contar_usuarios_por_tipo_cuota(tipo_cuota_id)
            if usuarios_usando > 0:
                return ValidationResult(
                    False,
                    f"No se puede eliminar este tipo de cuota porque {usuarios_usando} usuario(s) lo están usando",
                    "Eliminación"
                )
            
            return ValidationResult(True)
            
        except Exception as e:
            return ValidationResult(False, f"Error al validar eliminación: {str(e)}", "Eliminación")
    
    def validate_user_quota_type_change(self, usuario_id: int, nuevo_tipo_cuota: str) -> ValidationResult:
        """Valida que se pueda cambiar el tipo de cuota de un usuario"""
        try:
            # Verificar que el nuevo tipo de cuota existe y está activo
            tipo = self.db_manager.obtener_tipo_cuota_por_nombre(nuevo_tipo_cuota)
            if not tipo:
                return ValidationResult(False, f"El tipo de cuota '{nuevo_tipo_cuota}' no existe", "Tipo de Cuota")
            
            if not tipo.activo:
                return ValidationResult(False, f"El tipo de cuota '{nuevo_tipo_cuota}' no está activo", "Tipo de Cuota")
            
            # Verificar que el usuario existe
            usuario = self.db_manager.obtener_usuario_por_id(usuario_id)
            if not usuario:
                return ValidationResult(False, "Usuario no encontrado", "Usuario")
            
            return ValidationResult(True)
            
        except Exception as e:
            return ValidationResult(False, f"Error al validar cambio de tipo de cuota: {str(e)}", "Tipo de Cuota")

# Funciones de utilidad para validaciones comunes
def create_user_validator(db_manager) -> FormValidator:
    """Crea un validador para formularios de usuario"""
    validator = FormValidator()
    business_validator = BusinessValidator(db_manager)
    
    # Validadores comunes para usuarios
    def validate_dni_unique(value, field_name):
        return business_validator.validate_unique_dni(value)
    
    def validate_dni_format(value, field_name):
        return FieldValidator.dni(value, field_name)
    
    def validate_phone_format(value, field_name):
        return FieldValidator.phone(value, field_name)
    
    def validate_email_format(value, field_name):
        return FieldValidator.email(value, field_name)
    
    return validator

def create_professor_validator(db_manager) -> FormValidator:
    """Crea un validador para formularios de profesor"""
    validator = FormValidator()
    
    def validate_positive_experience(value, field_name):
        return FieldValidator.numeric_range(value, 0, 50, field_name)
    
    def validate_positive_rate(value, field_name):
        return FieldValidator.positive_number(value, field_name)
    
    return validator

def create_schedule_validator(db_manager) -> FormValidator:
    """Crea un validador preconfigurado para formularios de horarios"""
    validator = FormValidator()
    business_validator = BusinessValidator(db_manager)
    
    # Agregar validaciones específicas para horarios
    def validate_schedule_conflict(profesor_id, dia, hora_inicio, hora_fin, horario_id=None):
        """Valida que no haya conflictos de horarios para el profesor"""
        try:
            horarios = db_manager.obtener_horarios_profesor(profesor_id)
            for horario in horarios:
                # Saltar el horario actual si estamos editando
                if horario_id and horario['id'] == horario_id:
                    continue
                
                # Verificar si es el mismo día
                if horario['dia_semana'] == dia:
                    # Verificar solapamiento de horarios
                    inicio_existente = horario['hora_inicio']
                    fin_existente = horario['hora_fin']
                    
                    # Convertir a minutos para facilitar comparación
                    def time_to_minutes(time_str):
                        if isinstance(time_str, str):
                            h, m = map(int, time_str.split(':'))
                        else:
                            h, m = time_str.hour, time_str.minute
                        return h * 60 + m
                    
                    inicio_nuevo = time_to_minutes(hora_inicio)
                    fin_nuevo = time_to_minutes(hora_fin)
                    inicio_exist = time_to_minutes(inicio_existente)
                    fin_exist = time_to_minutes(fin_existente)
                    
                    # Verificar solapamiento
                    if not (fin_nuevo <= inicio_exist or inicio_nuevo >= fin_exist):
                        return ValidationResult(
                            False, 
                            f"Conflicto de horario: ya existe un horario de {inicio_existente} a {fin_existente} el {dia}"
                        )
            
            return ValidationResult(True)
        except Exception as e:
            return ValidationResult(False, f"Error al validar horarios: {str(e)}")
    
    def validate_time_range(start_time, end_time):
        return FieldValidator.time_range(start_time, end_time)
    
    # Agregar la función de validación al validador
    validator.validate_schedule_conflict = validate_schedule_conflict
    
    return validator

def create_quota_type_validator(db_manager) -> FormValidator:
    """Crea un validador preconfigurado para formularios de tipos de cuota"""
    validator = FormValidator()
    business_validator = BusinessValidator(db_manager)
    
    # Validadores específicos para tipos de cuota
    def validate_quota_name(value, field_name):
        return business_validator.validate_quota_type_name(value)
    
    def validate_quota_price(value, field_name):
        return business_validator.validate_quota_type_price(value)
    
    def validate_description_length(value, field_name):
        return FieldValidator.max_length(value, 500, field_name)
    
    # Agregar las funciones de validación al validador
    validator.validate_quota_name = validate_quota_name
    validator.validate_quota_price = validate_quota_price
    validator.validate_description_length = validate_description_length
    
    return validator