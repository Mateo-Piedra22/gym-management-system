from dataclasses import dataclass, field
from typing import Optional, Dict, List

@dataclass
class Usuario:
    id: Optional[int] = None; nombre: str = ""; dni: Optional[str] = None; telefono: str = ""; pin: Optional[str] = "1234"; rol: str = "socio"; notas: Optional[str] = None; fecha_registro: Optional[str] = None; activo: bool = True; tipo_cuota: str = "estandar"; fecha_proximo_vencimiento: Optional[str] = None; cuotas_vencidas: int = 0; ultimo_pago: Optional[str] = None
    
    def get(self, key, default=None):
        """Método para compatibilidad con operaciones de diccionario."""
        return getattr(self, key, default)
    
@dataclass
class Pago:
    id: Optional[int] = None; usuario_id: int = 0; monto: float = 0.0; mes: int = 0; año: int = 0; fecha_pago: Optional[str] = None; metodo_pago_id: Optional[int] = None

@dataclass
class Asistencia:
    id: Optional[int] = None
    usuario_id: int = 0
    fecha: Optional[str] = None
    hora_registro: Optional[str] = None

@dataclass
class Ejercicio:
    id: Optional[int] = None; nombre: str = ""; grupo_muscular: Optional[str] = None; descripcion: Optional[str] = None; objetivo: str = "general"

    def get(self, key, default=None):
        """Compatibilidad con acceso tipo diccionario (p. ej., obj.get('campo'))."""
        return getattr(self, key, default)

@dataclass
class Rutina:
    id: Optional[int] = None; usuario_id: Optional[int] = None; nombre_rutina: str = ""; descripcion: Optional[str] = None; dias_semana: int = 1; categoria: str = "general"; fecha_creacion: Optional[str] = None; activa: bool = True; ejercicios: List['RutinaEjercicio'] = field(default_factory=list)

@dataclass
class RutinaEjercicio:
    id: Optional[int] = None; rutina_id: int = 0; ejercicio_id: int = 0; dia_semana: int = 1; series: Optional[int] = None; repeticiones: Optional[str] = None; orden: Optional[int] = None; ejercicio: Optional[Ejercicio] = None

@dataclass
class Clase:
    id: Optional[int] = None; nombre: str = ""; descripcion: Optional[str] = None; activa: bool = True; ejercicios: List[Ejercicio] = field(default_factory=list)
    # Campos opcionales para compatibilidad con DB ampliada
    tipo_clase_id: Optional[int] = None
    tipo_clase_nombre: Optional[str] = None

@dataclass
class ClaseHorario:
    id: Optional[int] = None; clase_id: int = 0; profesor_id: Optional[int] = None; dia_semana: str = ""; hora_inicio: str = ""; hora_fin: str = ""; cupo_maximo: int = 20; activo: bool = True
    nombre_clase: Optional[str] = None; nombre_profesor: Optional[str] = None; inscriptos: int = 0

@dataclass
class ClaseUsuario:
    id: Optional[int] = None; clase_horario_id: int = 0; usuario_id: int = 0; fecha_inscripcion: Optional[str] = None
    nombre_usuario: Optional[str] = None

# --- NUEVOS MODELOS PARA GRUPOS DE EJERCICIOS ---
@dataclass
class EjercicioGrupo:
    id: Optional[int] = None
    nombre: str = ""

@dataclass
class EjercicioGrupoItem:
    grupo_id: int = 0
    ejercicio_id: int = 0

# --- MODELO PARA TIPOS DE CUOTA DINÁMICOS ---
@dataclass
class TipoCuota:
    id: Optional[int] = None
    nombre: str = ""
    precio: float = 0.0
    icono_path: Optional[str] = None
    activo: bool = True
    fecha_creacion: Optional[str] = None
    fecha_modificacion: Optional[str] = None
    descripcion: Optional[str] = None
    duracion_dias: int = 30
    
    def __post_init__(self):
        """Validaciones de negocio"""
        if self.nombre and len(self.nombre.strip()) == 0:
            raise ValueError("El nombre del tipo de cuota no puede estar vacío")
        if self.precio < 0:
            raise ValueError("El precio no puede ser negativo")
        if self.nombre:
            self.nombre = self.nombre.strip().title()

# --- NUEVOS MODELOS PARA FASE 2 ---
@dataclass
class UsuarioNota:
    id: Optional[int] = None
    usuario_id: int = 0
    categoria: str = "general"  # general, medica, administrativa, comportamiento
    titulo: str = ""
    contenido: str = ""
    importancia: str = "normal"  # baja, normal, alta, critica
    fecha_creacion: Optional[str] = None
    fecha_modificacion: Optional[str] = None
    activa: bool = True
    autor_id: Optional[int] = None  # ID del usuario que creó la nota

@dataclass
@dataclass
class Etiqueta:
    id: Optional[int] = None
    nombre: str = ""
    color: str = "#3498db"  # Color en formato hexadecimal
    descripcion: Optional[str] = None
    fecha_creacion: Optional[str] = None
    fecha_modificacion: Optional[str] = None
    activa: bool = True

@dataclass
class UsuarioEtiqueta:
    usuario_id: int = 0
    etiqueta_id: int = 0
    fecha_asignacion: Optional[str] = None
    asignado_por: Optional[int] = None  # ID del usuario que asignó la etiqueta

@dataclass
class UsuarioEstado:
    id: Optional[int] = None
    usuario_id: int = 0
    estado: str = ""
    descripcion: Optional[str] = None
    fecha_inicio: Optional[str] = None
    fecha_vencimiento: Optional[str] = None
    activo: bool = True
    creado_por: Optional[int] = None  # ID del usuario que creó el estado
    
    @property
    def fecha_fin(self):
        """Alias para fecha_vencimiento para compatibilidad."""
        return self.fecha_vencimiento
    
    @fecha_fin.setter
    def fecha_fin(self, value):
        """Setter para fecha_fin que actualiza fecha_vencimiento."""
        self.fecha_vencimiento = value
    
    @property
    def nombre(self):
        """Alias para estado para compatibilidad."""
        return self.estado
    
    @nombre.setter
    def nombre(self, value):
        """Setter para nombre que actualiza estado."""
        self.estado = value

# --- NUEVOS MODELOS PARA FASE 3: MÉTODOS Y CONCEPTOS DE PAGO ---
@dataclass
class MetodoPago:
    id: Optional[int] = None
    nombre: str = ""
    icono: Optional[str] = None  # Nombre del icono o path
    color: str = "#3498db"  # Color en formato hexadecimal
    comision: float = 0.0  # Porcentaje de comisión (0-100)
    activo: bool = True
    fecha_creacion: Optional[str] = None
    descripcion: Optional[str] = None
    
    def __post_init__(self):
        """Validaciones de negocio"""
        if self.nombre and len(self.nombre.strip()) == 0:
            raise ValueError("El nombre del método de pago no puede estar vacío")
        if self.comision < 0 or self.comision > 100:
            raise ValueError("La comisión debe estar entre 0 y 100")
        if self.nombre:
            self.nombre = self.nombre.strip().title()

@dataclass
class ConceptoPago:
    id: Optional[int] = None
    nombre: str = ""
    descripcion: Optional[str] = None
    precio_base: float = 0.0  # Precio base para conceptos fijos
    tipo: str = "fijo"  # fijo, variable
    activo: bool = True
    fecha_creacion: Optional[str] = None
    categoria: str = "general"  # cuota, multa, bonificacion, servicio, producto
    
    def __post_init__(self):
        """Validaciones de negocio"""
        if self.nombre and len(self.nombre.strip()) == 0:
            raise ValueError("El nombre del concepto de pago no puede estar vacío")
        if self.precio_base < 0:
            raise ValueError("El precio base no puede ser negativo")
        if self.tipo not in ["fijo", "variable"]:
            raise ValueError("El tipo debe ser 'fijo' o 'variable'")
        if self.nombre:
            self.nombre = self.nombre.strip().title()

@dataclass
class PagoDetalle:
    id: Optional[int] = None
    pago_id: int = 0
    concepto_id: Optional[int] = None  # Puede ser None para conceptos personalizados
    concepto_nombre: str = ""  # Nombre del concepto (para histórico)
    cantidad: float = 1.0
    precio_unitario: float = 0.0
    subtotal: float = 0.0
    notas: Optional[str] = None
    
    def __post_init__(self):
        """Cálculo automático del subtotal"""
        if self.cantidad and self.precio_unitario:
            self.subtotal = self.cantidad * self.precio_unitario

# --- NUEVOS MODELOS PARA FASE 6: ESPECIALIDADES Y CERTIFICACIONES ---
@dataclass
class Especialidad:
    id: Optional[int] = None
    nombre: str = ""
    descripcion: Optional[str] = None
    categoria: Optional[str] = None  # fitness, yoga, pilates, crossfit, etc.
    activa: bool = True
    fecha_creacion: Optional[str] = None
    
    def __post_init__(self):
        """Validaciones de negocio"""
        if not self.nombre or len(self.nombre.strip()) == 0:
            raise ValueError("El nombre de la especialidad no puede estar vacío")
        if self.nombre:
            self.nombre = self.nombre.strip().title()
        if self.categoria:
            self.categoria = self.categoria.strip().lower()

@dataclass
class ProfesorEspecialidad:
    id: Optional[int] = None
    profesor_id: int = 0
    especialidad_id: int = 0
    nivel_experiencia: str = "principiante"  # principiante, intermedio, avanzado, experto
    años_experiencia: int = 0
    fecha_asignacion: Optional[str] = None
    
    # Campos adicionales para joins
    especialidad_nombre: Optional[str] = None
    especialidad_descripcion: Optional[str] = None
    especialidad_categoria: Optional[str] = None
    
    def __post_init__(self):
        """Validaciones de negocio"""
        if self.nivel_experiencia not in ["principiante", "intermedio", "avanzado", "experto"]:
            raise ValueError("El nivel de experiencia debe ser: principiante, intermedio, avanzado o experto")
        if self.años_experiencia < 0:
            raise ValueError("Los años de experiencia no pueden ser negativos")

@dataclass
class ProfesorCertificacion:
    id: Optional[int] = None
    profesor_id: int = 0
    nombre: str = ""
    institucion_emisora: str = ""
    fecha_obtencion: Optional[str] = None
    fecha_vencimiento: Optional[str] = None
    numero_certificado: Optional[str] = None
    descripcion: Optional[str] = None
    estado: str = "vigente"  # vigente, vencida, por_vencer
    fecha_creacion: Optional[str] = None
    
    def __post_init__(self):
        """Validaciones de negocio"""
        if not self.nombre or len(self.nombre.strip()) == 0:
            raise ValueError("El nombre de la certificación no puede estar vacío")
        if not self.institucion_emisora or len(self.institucion_emisora.strip()) == 0:
            raise ValueError("La institución emisora no puede estar vacía")
        if self.estado not in ["vigente", "vencida", "por_vencer"]:
            raise ValueError("El estado debe ser: vigente, vencida o por_vencer")
        if self.nombre:
            self.nombre = self.nombre.strip().title()
        if self.institucion_emisora:
            self.institucion_emisora = self.institucion_emisora.strip().title()

@dataclass
class HistorialEstado:
    id: Optional[int] = None
    usuario_id: int = 0
    estado_id: Optional[int] = None
    accion: str = ""
    estado_anterior: Optional[str] = None
    estado_nuevo: Optional[str] = None
    fecha_accion: Optional[str] = None
    usuario_modificador: Optional[int] = None
    motivo: Optional[str] = None
    detalles: Optional[str] = None
    ip_origen: Optional[str] = None
    modificador_nombre: Optional[str] = None
    estado_actual_nombre: Optional[str] = None
    
    def get(self, key, default=None):
        """Método get para compatibilidad con diccionarios"""
        return getattr(self, key, default)