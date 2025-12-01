from typing import List, Optional, Any
from datetime import datetime, date, time
from sqlalchemy import (
    Integer, String, Text, Boolean, Date, Time, DateTime, 
    ForeignKey, Numeric, Index, JSON, CheckConstraint, func, text,
    UniqueConstraint
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship
from sqlalchemy.dialects.postgresql import JSONB, INET, UUID, ARRAY
import uuid

class Base(DeclarativeBase):
    pass

# --- Usuarios y Roles ---

class Usuario(Base):
    __tablename__ = 'usuarios'
    
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    nombre: Mapped[str] = mapped_column(String(255), nullable=False)
    dni: Mapped[Optional[str]] = mapped_column(String(20), unique=True)
    telefono: Mapped[str] = mapped_column(String(50), nullable=False)
    pin: Mapped[Optional[str]] = mapped_column(String(10), server_default='1234')
    rol: Mapped[str] = mapped_column(String(50), nullable=False, server_default='socio')
    notas: Mapped[Optional[str]] = mapped_column(Text)
    fecha_registro: Mapped[datetime] = mapped_column(DateTime, server_default=func.current_timestamp())
    activo: Mapped[bool] = mapped_column(Boolean, server_default='true')
    tipo_cuota: Mapped[Optional[str]] = mapped_column(String(100), server_default='estandar')
    ultimo_pago: Mapped[Optional[date]] = mapped_column(Date)
    fecha_proximo_vencimiento: Mapped[Optional[date]] = mapped_column(Date)
    cuotas_vencidas: Mapped[Optional[int]] = mapped_column(Integer, server_default='0')
    
    # Relaciones
    pagos: Mapped[List["Pago"]] = relationship("Pago", back_populates="usuario", cascade="all, delete-orphan")
    asistencias: Mapped[List["Asistencia"]] = relationship("Asistencia", back_populates="usuario", cascade="all, delete-orphan")
    rutinas: Mapped[List["Rutina"]] = relationship("Rutina", back_populates="usuario", cascade="all, delete-orphan")
    usuario_notas: Mapped[List["UsuarioNota"]] = relationship("UsuarioNota", back_populates="usuario", foreign_keys="[UsuarioNota.usuario_id]", cascade="all, delete-orphan")
    usuario_estados: Mapped[List["UsuarioEstado"]] = relationship("UsuarioEstado", back_populates="usuario", foreign_keys="[UsuarioEstado.usuario_id]", cascade="all, delete-orphan")
    usuario_etiquetas: Mapped[List["UsuarioEtiqueta"]] = relationship("UsuarioEtiqueta", back_populates="usuario", foreign_keys="[UsuarioEtiqueta.usuario_id]", cascade="all, delete-orphan")
    historial_estados: Mapped[List["HistorialEstado"]] = relationship("HistorialEstado", back_populates="usuario", foreign_keys="[HistorialEstado.usuario_id]", cascade="all, delete-orphan")
    
    profesor_perfil: Mapped[Optional["Profesor"]] = relationship("Profesor", back_populates="usuario", uselist=False, cascade="all, delete-orphan")
    
    __table_args__ = (
        Index('idx_usuarios_nombre', 'nombre'),
        Index('idx_usuarios_dni', 'dni'),
        Index('idx_usuarios_activo', 'activo'),
        Index('idx_usuarios_rol', 'rol'),
        Index('idx_usuarios_rol_nombre', 'rol', 'nombre'),
        Index('idx_usuarios_activo_rol_nombre', 'activo', 'rol', 'nombre'),
    )

# --- Pagos ---

class Pago(Base):
    __tablename__ = 'pagos'
    
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    usuario_id: Mapped[int] = mapped_column(ForeignKey('usuarios.id', ondelete='CASCADE'), nullable=False)
    monto: Mapped[float] = mapped_column(Numeric(10, 2), nullable=False)
    mes: Mapped[int] = mapped_column(Integer, nullable=False)
    año: Mapped[int] = mapped_column(Integer, nullable=False)
    fecha_pago: Mapped[datetime] = mapped_column(DateTime, server_default=func.current_timestamp())
    metodo_pago_id: Mapped[Optional[int]] = mapped_column(Integer) # Could link to metodos_pago if strict
    concepto: Mapped[Optional[str]] = mapped_column(String(100))
    metodo_pago: Mapped[Optional[str]] = mapped_column(String(50))
    estado: Mapped[Optional[str]] = mapped_column(String(20), server_default='pagado')

    usuario: Mapped["Usuario"] = relationship("Usuario", back_populates="pagos")
    detalles: Mapped[List["PagoDetalle"]] = relationship("PagoDetalle", back_populates="pago", cascade="all, delete-orphan")

    __table_args__ = (
        UniqueConstraint('usuario_id', 'mes', 'año', name='idx_pagos_usuario_mes_año'),
        Index('idx_pagos_usuario_id', 'usuario_id'),
        Index('idx_pagos_fecha', 'fecha_pago'),
        Index('idx_pagos_month_year', text('(EXTRACT(MONTH FROM fecha_pago))'), text('(EXTRACT(YEAR FROM fecha_pago))')),
        Index('idx_pagos_usuario_fecha_desc', 'usuario_id', text('fecha_pago DESC')),
    )

class PagoDetalle(Base):
    __tablename__ = 'pago_detalles'
    
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    pago_id: Mapped[int] = mapped_column(ForeignKey('pagos.id', ondelete='CASCADE'), nullable=False)
    concepto_id: Mapped[Optional[int]] = mapped_column(ForeignKey('conceptos_pago.id', ondelete='SET NULL'))
    descripcion: Mapped[Optional[str]] = mapped_column(Text)
    cantidad: Mapped[float] = mapped_column(Numeric(10, 2), server_default='1')
    precio_unitario: Mapped[float] = mapped_column(Numeric(10, 2), nullable=False)
    subtotal: Mapped[float] = mapped_column(Numeric(10, 2), nullable=False)
    descuento: Mapped[float] = mapped_column(Numeric(10, 2), server_default='0')
    total: Mapped[float] = mapped_column(Numeric(10, 2), nullable=False)
    fecha_creacion: Mapped[datetime] = mapped_column(DateTime, server_default=func.current_timestamp())
    
    pago: Mapped["Pago"] = relationship("Pago", back_populates="detalles")
    concepto_rel: Mapped[Optional["ConceptoPago"]] = relationship("ConceptoPago")

    __table_args__ = (
        Index('idx_pago_detalles_pago_id', 'pago_id'),
        Index('idx_pago_detalles_concepto_id', 'concepto_id'),
    )

class MetodoPago(Base):
    __tablename__ = 'metodos_pago'
    
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    nombre: Mapped[str] = mapped_column(String(100), unique=True, nullable=False)
    icono: Mapped[Optional[str]] = mapped_column(String(10))
    color: Mapped[str] = mapped_column(String(7), server_default='#3498db')
    comision: Mapped[Optional[float]] = mapped_column(Numeric(5, 2), server_default='0.0')
    activo: Mapped[bool] = mapped_column(Boolean, server_default='true')
    fecha_creacion: Mapped[datetime] = mapped_column(DateTime, server_default=func.current_timestamp())
    descripcion: Mapped[Optional[str]] = mapped_column(Text)

class ConceptoPago(Base):
    __tablename__ = 'conceptos_pago'
    
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    nombre: Mapped[str] = mapped_column(String(100), unique=True, nullable=False)
    descripcion: Mapped[Optional[str]] = mapped_column(Text)
    precio_base: Mapped[Optional[float]] = mapped_column(Numeric(10, 2), server_default='0.0')
    tipo: Mapped[str] = mapped_column(String(20), nullable=False, server_default='fijo')
    activo: Mapped[bool] = mapped_column(Boolean, server_default='true')
    fecha_creacion: Mapped[datetime] = mapped_column(DateTime, server_default=func.current_timestamp())

class TipoCuota(Base):
    __tablename__ = 'tipos_cuota'
    
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    nombre: Mapped[str] = mapped_column(String(100), unique=True, nullable=False)
    precio: Mapped[float] = mapped_column(Numeric(10, 2), nullable=False)
    icono_path: Mapped[Optional[str]] = mapped_column(String(255))
    activo: Mapped[bool] = mapped_column(Boolean, server_default='true')
    fecha_creacion: Mapped[datetime] = mapped_column(DateTime, server_default=func.current_timestamp())
    descripcion: Mapped[Optional[str]] = mapped_column(Text)
    duracion_dias: Mapped[Optional[int]] = mapped_column(Integer, server_default='30')
    fecha_modificacion: Mapped[Optional[datetime]] = mapped_column(DateTime, server_default=func.current_timestamp())

# --- Asistencias ---

class Asistencia(Base):
    __tablename__ = 'asistencias'
    
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    usuario_id: Mapped[int] = mapped_column(ForeignKey('usuarios.id', ondelete='CASCADE'), nullable=False)
    fecha: Mapped[date] = mapped_column(Date, server_default=func.current_date())
    hora_registro: Mapped[datetime] = mapped_column(DateTime, server_default=func.current_timestamp())
    hora_entrada: Mapped[Optional[time]] = mapped_column(Time)
    
    usuario: Mapped["Usuario"] = relationship("Usuario", back_populates="asistencias")
    
    __table_args__ = (
        UniqueConstraint('usuario_id', 'fecha', name='asistencias_usuario_id_fecha_key'),
        Index('idx_asistencias_usuario_id', 'usuario_id'),
        Index('idx_asistencias_fecha', 'fecha'),
        Index('idx_asistencias_usuario_fecha', 'usuario_id', 'fecha'),
        Index('idx_asistencias_usuario_fecha_desc', 'usuario_id', text('fecha DESC')),
    )

# --- Clases ---

class Clase(Base):
    __tablename__ = 'clases'
    
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    nombre: Mapped[str] = mapped_column(String(255), unique=True, nullable=False)
    descripcion: Mapped[Optional[str]] = mapped_column(Text)
    activa: Mapped[bool] = mapped_column(Boolean, server_default='true')
    tipo_clase_id: Mapped[Optional[int]] = mapped_column(Integer) # Could be FK if table types_clases exists and we want to enforce
    
    horarios: Mapped[List["ClaseHorario"]] = relationship("ClaseHorario", back_populates="clase", cascade="all, delete-orphan")
    bloques: Mapped[List["ClaseBloque"]] = relationship("ClaseBloque", back_populates="clase", cascade="all, delete-orphan")
    ejercicios: Mapped[List["ClaseEjercicio"]] = relationship("ClaseEjercicio", back_populates="clase", cascade="all, delete-orphan")

    __table_args__ = (
        Index('idx_clases_nombre', 'nombre'),
        Index('idx_clases_activa_true_nombre', 'nombre', postgresql_where=text("activa = TRUE")),
        Index('idx_clases_tipo_clase_id', 'tipo_clase_id'),
    )

class TipoClase(Base):
    __tablename__ = 'tipos_clases'
    
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    nombre: Mapped[str] = mapped_column(String(255), unique=True, nullable=False)
    descripcion: Mapped[Optional[str]] = mapped_column(Text)
    activo: Mapped[bool] = mapped_column(Boolean, server_default='true')

class ClaseHorario(Base):
    __tablename__ = 'clases_horarios'
    
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    clase_id: Mapped[int] = mapped_column(ForeignKey('clases.id', ondelete='CASCADE'), nullable=False)
    dia_semana: Mapped[str] = mapped_column(String(20), nullable=False)
    hora_inicio: Mapped[time] = mapped_column(Time, nullable=False)
    hora_fin: Mapped[time] = mapped_column(Time, nullable=False)
    cupo_maximo: Mapped[Optional[int]] = mapped_column(Integer, server_default='20')
    activo: Mapped[bool] = mapped_column(Boolean, server_default='true')
    
    clase: Mapped["Clase"] = relationship("Clase", back_populates="horarios")
    lista_espera: Mapped[List["ClaseListaEspera"]] = relationship("ClaseListaEspera", back_populates="clase_horario", cascade="all, delete-orphan")
    profesores_asignados: Mapped[List["ProfesorClaseAsignacion"]] = relationship("ProfesorClaseAsignacion", back_populates="clase_horario", cascade="all, delete-orphan")
    clase_usuarios: Mapped[List["ClaseUsuario"]] = relationship("ClaseUsuario", back_populates="clase_horario", cascade="all, delete-orphan")
    notificaciones_cupo: Mapped[List["NotificacionCupo"]] = relationship("NotificacionCupo", back_populates="clase_horario", cascade="all, delete-orphan")

    __table_args__ = (
        Index('idx_clases_horarios_clase_id', 'clase_id'),
    )

class ClaseUsuario(Base):
    __tablename__ = 'clase_usuarios'
    
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    clase_horario_id: Mapped[int] = mapped_column(ForeignKey('clases_horarios.id', ondelete='CASCADE'), nullable=False)
    usuario_id: Mapped[int] = mapped_column(ForeignKey('usuarios.id', ondelete='CASCADE'), nullable=False)
    fecha_inscripcion: Mapped[datetime] = mapped_column(DateTime, server_default=func.current_timestamp())
    
    clase_horario: Mapped["ClaseHorario"] = relationship("ClaseHorario", back_populates="clase_usuarios")
    
    __table_args__ = (
        UniqueConstraint('clase_horario_id', 'usuario_id', name='clase_usuarios_clase_horario_id_usuario_id_key'),
        Index('idx_clase_usuarios_clase_horario_id', 'clase_horario_id'),
        Index('idx_clase_usuarios_usuario_id', 'usuario_id'),
    )

class ClaseListaEspera(Base):
    __tablename__ = 'clase_lista_espera'
    
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    clase_horario_id: Mapped[int] = mapped_column(ForeignKey('clases_horarios.id', ondelete='CASCADE'), nullable=False)
    usuario_id: Mapped[int] = mapped_column(ForeignKey('usuarios.id', ondelete='CASCADE'), nullable=False)
    posicion: Mapped[int] = mapped_column(Integer, nullable=False)
    activo: Mapped[bool] = mapped_column(Boolean, server_default='true')
    fecha_creacion: Mapped[datetime] = mapped_column(DateTime, server_default=func.current_timestamp())
    
    clase_horario: Mapped["ClaseHorario"] = relationship("ClaseHorario", back_populates="lista_espera")
    
    __table_args__ = (
        UniqueConstraint('clase_horario_id', 'usuario_id', name='clase_lista_espera_clase_horario_id_usuario_id_key'),
        Index('idx_clase_lista_espera_clase', 'clase_horario_id'),
        Index('idx_clase_lista_espera_activo', 'activo'),
        Index('idx_clase_lista_espera_posicion', 'posicion'),
    )

class NotificacionCupo(Base):
    __tablename__ = 'notificaciones_cupos'
    
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    usuario_id: Mapped[int] = mapped_column(ForeignKey('usuarios.id', ondelete='CASCADE'), nullable=False)
    clase_horario_id: Mapped[int] = mapped_column(ForeignKey('clases_horarios.id', ondelete='CASCADE'), nullable=False)
    tipo_notificacion: Mapped[str] = mapped_column(String(50), nullable=False)
    mensaje: Mapped[Optional[str]] = mapped_column(Text)
    leida: Mapped[bool] = mapped_column(Boolean, server_default='false')
    activa: Mapped[bool] = mapped_column(Boolean, server_default='true')
    fecha_creacion: Mapped[datetime] = mapped_column(DateTime, server_default=func.current_timestamp())
    fecha_lectura: Mapped[Optional[datetime]] = mapped_column(DateTime)
    
    clase_horario: Mapped["ClaseHorario"] = relationship("ClaseHorario", back_populates="notificaciones_cupo")

    __table_args__ = (
        CheckConstraint("tipo_notificacion IN ('cupo_liberado','promocion','recordatorio')", name='notificaciones_cupos_tipo_notificacion_check'),
        Index('idx_notif_cupos_usuario_activa', 'usuario_id', 'activa'),
        Index('idx_notif_cupos_clase', 'clase_horario_id'),
        Index('idx_notif_cupos_leida', 'leida'),
        Index('idx_notif_cupos_tipo', 'tipo_notificacion'),
    )

# --- Ejercicios y Rutinas ---

class Ejercicio(Base):
    __tablename__ = 'ejercicios'
    
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    nombre: Mapped[str] = mapped_column(String(255), unique=True, nullable=False)
    descripcion: Mapped[Optional[str]] = mapped_column(Text)
    grupo_muscular: Mapped[Optional[str]] = mapped_column(String(100))
    objetivo: Mapped[Optional[str]] = mapped_column(String(100), server_default='general')
    video_url: Mapped[Optional[str]] = mapped_column(String(512))
    video_mime: Mapped[Optional[str]] = mapped_column(String(50))

class Rutina(Base):
    __tablename__ = 'rutinas'
    
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    usuario_id: Mapped[Optional[int]] = mapped_column(ForeignKey('usuarios.id', ondelete='CASCADE'))
    nombre_rutina: Mapped[str] = mapped_column(String(255), nullable=False)
    descripcion: Mapped[Optional[str]] = mapped_column(Text)
    dias_semana: Mapped[Optional[int]] = mapped_column(Integer)
    categoria: Mapped[Optional[str]] = mapped_column(String(100), server_default='general')
    fecha_creacion: Mapped[datetime] = mapped_column(DateTime, server_default=func.current_timestamp())
    activa: Mapped[bool] = mapped_column(Boolean, server_default='true')
    uuid_rutina: Mapped[Optional[str]] = mapped_column(String(36), unique=True)
    
    usuario: Mapped[Optional["Usuario"]] = relationship("Usuario", back_populates="rutinas")
    ejercicios: Mapped[List["RutinaEjercicio"]] = relationship("RutinaEjercicio", back_populates="rutina", cascade="all, delete-orphan")

    __table_args__ = (
        Index('idx_rutinas_uuid_rutina', 'uuid_rutina', unique=True),
        Index('idx_rutinas_usuario_id', 'usuario_id'),
    )

class RutinaEjercicio(Base):
    __tablename__ = 'rutina_ejercicios'
    
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    rutina_id: Mapped[int] = mapped_column(ForeignKey('rutinas.id', ondelete='CASCADE'), nullable=False)
    ejercicio_id: Mapped[int] = mapped_column(ForeignKey('ejercicios.id', ondelete='CASCADE'), nullable=False)
    dia_semana: Mapped[Optional[int]] = mapped_column(Integer)
    series: Mapped[Optional[int]] = mapped_column(Integer)
    repeticiones: Mapped[Optional[str]] = mapped_column(String(50))
    orden: Mapped[Optional[int]] = mapped_column(Integer)
    
    rutina: Mapped["Rutina"] = relationship("Rutina", back_populates="ejercicios")
    ejercicio: Mapped["Ejercicio"] = relationship("Ejercicio")

    __table_args__ = (
        Index('idx_rutina_ejercicios_rutina_id', 'rutina_id'),
        Index('idx_rutina_ejercicios_ejercicio_id', 'ejercicio_id'),
        Index('idx_rutina_ejercicios_rutina_ejercicio', 'rutina_id', 'ejercicio_id'),
    )

class ClaseEjercicio(Base):
    __tablename__ = 'clase_ejercicios'
    
    clase_id: Mapped[int] = mapped_column(ForeignKey('clases.id', ondelete='CASCADE'), primary_key=True)
    ejercicio_id: Mapped[int] = mapped_column(ForeignKey('ejercicios.id', ondelete='CASCADE'), primary_key=True)
    orden: Mapped[Optional[int]] = mapped_column(Integer, server_default='0')
    series: Mapped[Optional[int]] = mapped_column(Integer, server_default='0')
    repeticiones: Mapped[Optional[str]] = mapped_column(String(50), server_default='')
    descanso_segundos: Mapped[Optional[int]] = mapped_column(Integer, server_default='0')
    notas: Mapped[Optional[str]] = mapped_column(Text)
    
    clase: Mapped["Clase"] = relationship("Clase", back_populates="ejercicios")
    ejercicio: Mapped["Ejercicio"] = relationship("Ejercicio")

    __table_args__ = (
        Index('idx_clase_ejercicios_ejercicio_id', 'ejercicio_id'),
        Index('idx_clase_ejercicios_clase_orden', 'clase_id', 'orden'),
    )

class ClaseBloque(Base):
    __tablename__ = 'clase_bloques'
    
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    clase_id: Mapped[int] = mapped_column(ForeignKey('clases.id', ondelete='CASCADE'), nullable=False)
    nombre: Mapped[str] = mapped_column(Text, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.current_timestamp())
    updated_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.current_timestamp())
    
    clase: Mapped["Clase"] = relationship("Clase", back_populates="bloques")
    items: Mapped[List["ClaseBloqueItem"]] = relationship("ClaseBloqueItem", back_populates="bloque", cascade="all, delete-orphan")

    __table_args__ = (
        Index('idx_clase_bloques_clase', 'clase_id'),
        Index('idx_clase_bloques_nombre', 'nombre'),
    )

class ClaseBloqueItem(Base):
    __tablename__ = 'clase_bloque_items'
    
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    bloque_id: Mapped[int] = mapped_column(ForeignKey('clase_bloques.id', ondelete='CASCADE'), nullable=False)
    ejercicio_id: Mapped[int] = mapped_column(ForeignKey('ejercicios.id', ondelete='CASCADE'), nullable=False)
    orden: Mapped[int] = mapped_column(Integer, nullable=False, server_default='0')
    series: Mapped[Optional[int]] = mapped_column(Integer, server_default='0')
    repeticiones: Mapped[Optional[str]] = mapped_column(Text)
    descanso_segundos: Mapped[Optional[int]] = mapped_column(Integer, server_default='0')
    notas: Mapped[Optional[str]] = mapped_column(Text)
    
    bloque: Mapped["ClaseBloque"] = relationship("ClaseBloque", back_populates="items")
    ejercicio: Mapped["Ejercicio"] = relationship("Ejercicio")

    __table_args__ = (
        Index('idx_bloque_items_bloque', 'bloque_id'),
        Index('idx_bloque_items_bloque_orden', 'bloque_id', 'orden'),
    )

class EjercicioGrupo(Base):
    __tablename__ = 'ejercicio_grupos'
    
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    nombre: Mapped[str] = mapped_column(String(255), unique=True, nullable=False)
    
    items: Mapped[List["EjercicioGrupoItem"]] = relationship("EjercicioGrupoItem", back_populates="grupo", cascade="all, delete-orphan")

class EjercicioGrupoItem(Base):
    __tablename__ = 'ejercicio_grupo_items'
    
    grupo_id: Mapped[int] = mapped_column(ForeignKey('ejercicio_grupos.id', ondelete='CASCADE'), primary_key=True)
    ejercicio_id: Mapped[int] = mapped_column(ForeignKey('ejercicios.id', ondelete='CASCADE'), primary_key=True)
    
    grupo: Mapped["EjercicioGrupo"] = relationship("EjercicioGrupo", back_populates="items")
    ejercicio: Mapped["Ejercicio"] = relationship("Ejercicio")

# --- Profesores ---

class Profesor(Base):
    __tablename__ = 'profesores'
    
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    usuario_id: Mapped[int] = mapped_column(ForeignKey('usuarios.id', ondelete='CASCADE'), unique=True, nullable=False)
    tipo: Mapped[Optional[str]] = mapped_column(String(50), server_default='Musculación')
    especialidades: Mapped[Optional[str]] = mapped_column(Text)
    certificaciones: Mapped[Optional[str]] = mapped_column(Text)
    experiencia_años: Mapped[Optional[int]] = mapped_column(Integer, server_default='0')
    tarifa_por_hora: Mapped[Optional[float]] = mapped_column(Numeric(10, 2), server_default='0.0')
    horario_disponible: Mapped[Optional[str]] = mapped_column(Text)
    fecha_contratacion: Mapped[Optional[date]] = mapped_column(Date)
    estado: Mapped[Optional[str]] = mapped_column(String(20), server_default='activo')
    biografia: Mapped[Optional[str]] = mapped_column(Text)
    foto_perfil: Mapped[Optional[str]] = mapped_column(String(255))
    telefono_emergencia: Mapped[Optional[str]] = mapped_column(String(50))
    fecha_creacion: Mapped[datetime] = mapped_column(DateTime, server_default=func.current_timestamp())
    fecha_actualizacion: Mapped[datetime] = mapped_column(DateTime, server_default=func.current_timestamp())
    
    usuario: Mapped["Usuario"] = relationship("Usuario", back_populates="profesor_perfil")
    horarios_asignados: Mapped[List["HorarioProfesor"]] = relationship("HorarioProfesor", back_populates="profesor", cascade="all, delete-orphan")
    disponibilidad_horaria: Mapped[List["ProfesorHorarioDisponibilidad"]] = relationship("ProfesorHorarioDisponibilidad", back_populates="profesor", cascade="all, delete-orphan")
    evaluaciones: Mapped[List["ProfesorEvaluacion"]] = relationship("ProfesorEvaluacion", back_populates="profesor", cascade="all, delete-orphan")
    disponibilidad_especifica: Mapped[List["ProfesorDisponibilidad"]] = relationship("ProfesorDisponibilidad", back_populates="profesor", cascade="all, delete-orphan")
    asignaciones_clase: Mapped[List["ProfesorClaseAsignacion"]] = relationship("ProfesorClaseAsignacion", back_populates="profesor", cascade="all, delete-orphan")
    profesor_especialidades: Mapped[List["ProfesorEspecialidad"]] = relationship("ProfesorEspecialidad", back_populates="profesor", cascade="all, delete-orphan")
    profesor_certificaciones: Mapped[List["ProfesorCertificacion"]] = relationship("ProfesorCertificacion", back_populates="profesor", cascade="all, delete-orphan")
    horas_trabajadas: Mapped[List["ProfesorHoraTrabajada"]] = relationship("ProfesorHoraTrabajada", back_populates="profesor", cascade="all, delete-orphan")

    __table_args__ = (
        CheckConstraint("estado IN ('activo', 'inactivo', 'vacaciones')", name='profesores_estado_check'),
    )

class HorarioProfesor(Base):
    __tablename__ = 'horarios_profesores'
    
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    profesor_id: Mapped[int] = mapped_column(ForeignKey('profesores.id', ondelete='CASCADE'), nullable=False)
    dia_semana: Mapped[str] = mapped_column(String(20), nullable=False)
    hora_inicio: Mapped[time] = mapped_column(Time, nullable=False)
    hora_fin: Mapped[time] = mapped_column(Time, nullable=False)
    disponible: Mapped[bool] = mapped_column(Boolean, server_default='true')
    fecha_creacion: Mapped[datetime] = mapped_column(DateTime, server_default=func.current_timestamp())
    
    profesor: Mapped["Profesor"] = relationship("Profesor", back_populates="horarios_asignados")
    suplencias_generales: Mapped[List["ProfesorSuplenciaGeneral"]] = relationship("ProfesorSuplenciaGeneral", back_populates="horario_profesor")

    __table_args__ = (
        Index('idx_horarios_profesores_profesor_id', 'profesor_id'),
    )

class ProfesorHorarioDisponibilidad(Base):
    __tablename__ = 'profesores_horarios_disponibilidad'
    
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    profesor_id: Mapped[int] = mapped_column(ForeignKey('profesores.id', ondelete='CASCADE'), nullable=False)
    dia_semana: Mapped[int] = mapped_column(Integer, nullable=False)
    hora_inicio: Mapped[time] = mapped_column(Time, nullable=False)
    hora_fin: Mapped[time] = mapped_column(Time, nullable=False)
    disponible: Mapped[bool] = mapped_column(Boolean, server_default='true')
    tipo_disponibilidad: Mapped[Optional[str]] = mapped_column(String(50), server_default='regular')
    fecha_creacion: Mapped[datetime] = mapped_column(DateTime, server_default=func.current_timestamp())
    fecha_actualizacion: Mapped[datetime] = mapped_column(DateTime, server_default=func.current_timestamp())
    
    profesor: Mapped["Profesor"] = relationship("Profesor", back_populates="disponibilidad_horaria")

    __table_args__ = (
        CheckConstraint('dia_semana BETWEEN 0 AND 6', name='profesores_horarios_disponibilidad_dia_semana_check'),
        Index('idx_profesores_horarios_disponibilidad_profesor_id', 'profesor_id'),
    )

class ProfesorEvaluacion(Base):
    __tablename__ = 'profesor_evaluaciones'
    
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    profesor_id: Mapped[int] = mapped_column(ForeignKey('profesores.id', ondelete='CASCADE'), nullable=False)
    usuario_id: Mapped[int] = mapped_column(ForeignKey('usuarios.id', ondelete='CASCADE'), nullable=False)
    puntuacion: Mapped[int] = mapped_column(Integer)
    comentario: Mapped[Optional[str]] = mapped_column(Text)
    fecha_evaluacion: Mapped[datetime] = mapped_column(DateTime, server_default=func.current_timestamp())
    
    profesor: Mapped["Profesor"] = relationship("Profesor", back_populates="evaluaciones")
    usuario: Mapped["Usuario"] = relationship("Usuario")

    __table_args__ = (
        CheckConstraint('puntuacion >= 1 AND puntuacion <= 5', name='profesor_evaluaciones_puntuacion_check'),
        UniqueConstraint('profesor_id', 'usuario_id', name='profesor_evaluaciones_profesor_id_usuario_id_key'),
        Index('idx_profesor_evaluaciones_profesor_id', 'profesor_id'),
        Index('idx_profesor_evaluaciones_usuario_id', 'usuario_id'),
    )

class ProfesorDisponibilidad(Base):
    __tablename__ = 'profesor_disponibilidad'
    
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    profesor_id: Mapped[int] = mapped_column(ForeignKey('profesores.id', ondelete='CASCADE'), nullable=False)
    fecha: Mapped[date] = mapped_column(Date, nullable=False)
    tipo_disponibilidad: Mapped[str] = mapped_column(String(50), nullable=False)
    hora_inicio: Mapped[Optional[time]] = mapped_column(Time)
    hora_fin: Mapped[Optional[time]] = mapped_column(Time)
    notas: Mapped[Optional[str]] = mapped_column(Text)
    fecha_creacion: Mapped[datetime] = mapped_column(DateTime, server_default=func.current_timestamp())
    fecha_modificacion: Mapped[datetime] = mapped_column(DateTime, server_default=func.current_timestamp())
    
    profesor: Mapped["Profesor"] = relationship("Profesor", back_populates="disponibilidad_especifica")

    __table_args__ = (
        CheckConstraint("tipo_disponibilidad IN ('Disponible', 'No Disponible', 'Parcialmente Disponible')", name='profesor_disponibilidad_tipo_disponibilidad_check'),
        UniqueConstraint('profesor_id', 'fecha', name='profesor_disponibilidad_profesor_id_fecha_key'),
        Index('idx_profesor_disponibilidad_profesor_id', 'profesor_id'),
        Index('idx_profesor_disponibilidad_fecha', 'fecha'),
        Index('idx_profesor_disponibilidad_profesor_fecha', 'profesor_id', 'fecha'),
    )

class ProfesorClaseAsignacion(Base):
    __tablename__ = 'profesor_clase_asignaciones'
    
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    clase_horario_id: Mapped[int] = mapped_column(ForeignKey('clases_horarios.id', ondelete='CASCADE'), nullable=False)
    profesor_id: Mapped[int] = mapped_column(ForeignKey('profesores.id', ondelete='CASCADE'), nullable=False)
    fecha_asignacion: Mapped[datetime] = mapped_column(DateTime, server_default=func.current_timestamp())
    activa: Mapped[bool] = mapped_column(Boolean, server_default='true')
    
    clase_horario: Mapped["ClaseHorario"] = relationship("ClaseHorario", back_populates="profesores_asignados")
    profesor: Mapped["Profesor"] = relationship("Profesor", back_populates="asignaciones_clase")
    suplencias: Mapped[List["ProfesorSuplencia"]] = relationship("ProfesorSuplencia", back_populates="asignacion", cascade="all, delete-orphan")

    __table_args__ = (
        UniqueConstraint('clase_horario_id', 'profesor_id', name='profesor_clase_asignaciones_clase_horario_id_profesor_id_key'),
        Index('idx_profesor_clase_asignaciones_profesor_id', 'profesor_id'),
    )

class ProfesorSuplencia(Base):
    __tablename__ = 'profesor_suplencias'
    
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    asignacion_id: Mapped[int] = mapped_column(ForeignKey('profesor_clase_asignaciones.id', ondelete='CASCADE'), nullable=False)
    profesor_suplente_id: Mapped[Optional[int]] = mapped_column(ForeignKey('profesores.id', ondelete='SET NULL'))
    fecha_clase: Mapped[date] = mapped_column(Date, nullable=False)
    motivo: Mapped[str] = mapped_column(Text, nullable=False)
    estado: Mapped[Optional[str]] = mapped_column(String(20), server_default='Pendiente')
    notas: Mapped[Optional[str]] = mapped_column(Text)
    fecha_creacion: Mapped[datetime] = mapped_column(DateTime, server_default=func.current_timestamp())
    fecha_resolucion: Mapped[Optional[datetime]] = mapped_column(DateTime)
    
    asignacion: Mapped["ProfesorClaseAsignacion"] = relationship("ProfesorClaseAsignacion", back_populates="suplencias")
    profesor_suplente: Mapped[Optional["Profesor"]] = relationship("Profesor", foreign_keys=[profesor_suplente_id])

    __table_args__ = (
        CheckConstraint("estado IN ('Pendiente', 'Asignado', 'Confirmado', 'Cancelado')", name='profesor_suplencias_estado_check'),
        Index('idx_profesor_suplencias_asignacion_fecha', 'asignacion_id', 'fecha_clase'),
        Index('idx_profesor_suplencias_asignacion', 'asignacion_id'),
        Index('idx_profesor_suplencias_suplente', 'profesor_suplente_id'),
        Index('idx_profesor_suplencias_estado', 'estado'),
        Index('idx_profesor_suplencias_fecha_clase', 'fecha_clase'),
    )

class ProfesorSuplenciaGeneral(Base):
    __tablename__ = 'profesor_suplencias_generales'
    
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    horario_profesor_id: Mapped[Optional[int]] = mapped_column(ForeignKey('horarios_profesores.id', ondelete='SET NULL'))
    profesor_original_id: Mapped[int] = mapped_column(ForeignKey('profesores.id', ondelete='CASCADE'), nullable=False)
    profesor_suplente_id: Mapped[Optional[int]] = mapped_column(ForeignKey('profesores.id', ondelete='SET NULL'))
    fecha: Mapped[date] = mapped_column(Date, nullable=False)
    hora_inicio: Mapped[time] = mapped_column(Time, nullable=False)
    hora_fin: Mapped[time] = mapped_column(Time, nullable=False)
    motivo: Mapped[str] = mapped_column(Text, nullable=False)
    estado: Mapped[Optional[str]] = mapped_column(String(20), server_default='Pendiente')
    notas: Mapped[Optional[str]] = mapped_column(Text)
    fecha_creacion: Mapped[datetime] = mapped_column(DateTime, server_default=func.current_timestamp())
    fecha_resolucion: Mapped[Optional[datetime]] = mapped_column(DateTime)
    
    horario_profesor: Mapped[Optional["HorarioProfesor"]] = relationship("HorarioProfesor", back_populates="suplencias_generales")
    profesor_original: Mapped["Profesor"] = relationship("Profesor", foreign_keys=[profesor_original_id])
    profesor_suplente: Mapped[Optional["Profesor"]] = relationship("Profesor", foreign_keys=[profesor_suplente_id])

    __table_args__ = (
        CheckConstraint("estado IN ('Pendiente', 'Asignado', 'Confirmado', 'Cancelado')", name='profesor_suplencias_generales_estado_check'),
        Index('idx_profesor_suplencias_generales_fecha', 'fecha'),
        Index('idx_profesor_suplencias_generales_estado', 'estado'),
    )

class Especialidad(Base):
    __tablename__ = 'especialidades'
    
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    nombre: Mapped[str] = mapped_column(String(100), unique=True, nullable=False)
    descripcion: Mapped[Optional[str]] = mapped_column(Text)
    categoria: Mapped[Optional[str]] = mapped_column(String(50))
    activo: Mapped[bool] = mapped_column(Boolean, server_default='true')
    fecha_creacion: Mapped[datetime] = mapped_column(DateTime, server_default=func.current_timestamp())

    __table_args__ = (
        Index('idx_especialidades_nombre', 'nombre'),
        Index('idx_especialidades_activo', 'activo'),
    )

class ProfesorEspecialidad(Base):
    __tablename__ = 'profesor_especialidades'
    
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    profesor_id: Mapped[int] = mapped_column(ForeignKey('profesores.id', ondelete='CASCADE'), nullable=False)
    especialidad_id: Mapped[int] = mapped_column(ForeignKey('especialidades.id', ondelete='CASCADE'), nullable=False)
    fecha_asignacion: Mapped[datetime] = mapped_column(DateTime, server_default=func.current_timestamp())
    activo: Mapped[bool] = mapped_column(Boolean, server_default='true')
    nivel_experiencia: Mapped[Optional[str]] = mapped_column(String(50))
    años_experiencia: Mapped[Optional[int]] = mapped_column(Integer, server_default='0')
    
    profesor: Mapped["Profesor"] = relationship("Profesor", back_populates="profesor_especialidades")
    especialidad: Mapped["Especialidad"] = relationship("Especialidad")

    __table_args__ = (
        UniqueConstraint('profesor_id', 'especialidad_id', name='profesor_especialidades_profesor_id_especialidad_id_key'),
        Index('idx_profesor_especialidades_profesor_id', 'profesor_id'),
        Index('idx_profesor_especialidades_especialidad_id', 'especialidad_id'),
        Index('idx_profesor_especialidades_activo', 'activo'),
    )

class ProfesorCertificacion(Base):
    __tablename__ = 'profesor_certificaciones'
    
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    profesor_id: Mapped[int] = mapped_column(ForeignKey('profesores.id', ondelete='CASCADE'), nullable=False)
    nombre_certificacion: Mapped[str] = mapped_column(String(200), nullable=False)
    institucion_emisora: Mapped[Optional[str]] = mapped_column(String(200))
    fecha_obtencion: Mapped[Optional[date]] = mapped_column(Date)
    fecha_vencimiento: Mapped[Optional[date]] = mapped_column(Date)
    numero_certificado: Mapped[Optional[str]] = mapped_column(String(100))
    archivo_adjunto: Mapped[Optional[str]] = mapped_column(String(500))
    estado: Mapped[Optional[str]] = mapped_column(String(20), server_default='vigente')
    notas: Mapped[Optional[str]] = mapped_column(Text)
    activo: Mapped[bool] = mapped_column(Boolean, server_default='true')
    fecha_creacion: Mapped[datetime] = mapped_column(DateTime, server_default=func.current_timestamp())
    
    profesor: Mapped["Profesor"] = relationship("Profesor", back_populates="profesor_certificaciones")

    __table_args__ = (
        Index('idx_profesor_certificaciones_profesor_id', 'profesor_id'),
        Index('idx_profesor_certificaciones_fecha_vencimiento', 'fecha_vencimiento'),
        Index('idx_profesor_certificaciones_activo', 'activo'),
    )

class ProfesorHoraTrabajada(Base):
    __tablename__ = 'profesor_horas_trabajadas'
    
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    profesor_id: Mapped[int] = mapped_column(ForeignKey('profesores.id', ondelete='CASCADE'), nullable=False)
    fecha: Mapped[date] = mapped_column(Date, nullable=False)
    hora_inicio: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    hora_fin: Mapped[Optional[datetime]] = mapped_column(DateTime)
    minutos_totales: Mapped[Optional[int]] = mapped_column(Integer)
    horas_totales: Mapped[Optional[float]] = mapped_column(Numeric(8, 2))
    tipo_actividad: Mapped[Optional[str]] = mapped_column(String(50))
    clase_id: Mapped[Optional[int]] = mapped_column(ForeignKey('clases.id', ondelete='SET NULL'))
    notas: Mapped[Optional[str]] = mapped_column(Text)
    fecha_creacion: Mapped[datetime] = mapped_column(DateTime, server_default=func.current_timestamp())
    
    profesor: Mapped["Profesor"] = relationship("Profesor", back_populates="horas_trabajadas")
    clase: Mapped[Optional["Clase"]] = relationship("Clase")

    __table_args__ = (
        Index('uniq_sesion_activa_por_profesor', 'profesor_id', unique=True, postgresql_where=text("hora_fin IS NULL")),
        Index('idx_profesor_horas_trabajadas_profesor_id', 'profesor_id'),
        Index('idx_profesor_horas_trabajadas_fecha', 'fecha'),
        Index('idx_profesor_horas_trabajadas_clase_id', 'clase_id'),
    )

# --- Notas, Etiquetas, Estados ---

class UsuarioNota(Base):
    __tablename__ = 'usuario_notas'
    
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    usuario_id: Mapped[int] = mapped_column(ForeignKey('usuarios.id', ondelete='CASCADE'), nullable=False)
    categoria: Mapped[str] = mapped_column(String(50), nullable=False, server_default='general')
    titulo: Mapped[str] = mapped_column(String(255), nullable=False)
    contenido: Mapped[str] = mapped_column(Text, nullable=False)
    importancia: Mapped[str] = mapped_column(String(20), nullable=False, server_default='normal')
    fecha_creacion: Mapped[datetime] = mapped_column(DateTime, server_default=func.current_timestamp())
    fecha_modificacion: Mapped[datetime] = mapped_column(DateTime, server_default=func.current_timestamp())
    activa: Mapped[bool] = mapped_column(Boolean, server_default='true')
    autor_id: Mapped[Optional[int]] = mapped_column(ForeignKey('usuarios.id', ondelete='SET NULL'))
    
    usuario: Mapped["Usuario"] = relationship("Usuario", back_populates="usuario_notas", foreign_keys=[usuario_id])
    autor: Mapped[Optional["Usuario"]] = relationship("Usuario", foreign_keys=[autor_id])

    __table_args__ = (
        CheckConstraint("categoria IN ('general', 'medica', 'administrativa', 'comportamiento')", name='usuario_notas_categoria_check'),
        CheckConstraint("importancia IN ('baja', 'normal', 'alta', 'critica')", name='usuario_notas_importancia_check'),
        Index('idx_usuario_notas_usuario_id', 'usuario_id'),
        Index('idx_usuario_notas_autor_id', 'autor_id'),
    )

class Etiqueta(Base):
    __tablename__ = 'etiquetas'
    
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    nombre: Mapped[str] = mapped_column(String(100), unique=True, nullable=False)
    color: Mapped[str] = mapped_column(String(7), nullable=False, server_default='#3498db')
    descripcion: Mapped[Optional[str]] = mapped_column(Text)
    fecha_creacion: Mapped[datetime] = mapped_column(DateTime, server_default=func.current_timestamp())
    activo: Mapped[bool] = mapped_column(Boolean, server_default='true')

class UsuarioEtiqueta(Base):
    __tablename__ = 'usuario_etiquetas'
    
    usuario_id: Mapped[int] = mapped_column(ForeignKey('usuarios.id', ondelete='CASCADE'), primary_key=True)
    etiqueta_id: Mapped[int] = mapped_column(ForeignKey('etiquetas.id', ondelete='CASCADE'), primary_key=True)
    fecha_asignacion: Mapped[datetime] = mapped_column(DateTime, server_default=func.current_timestamp())
    asignado_por: Mapped[Optional[int]] = mapped_column(ForeignKey('usuarios.id', ondelete='SET NULL'))
    
    usuario: Mapped["Usuario"] = relationship("Usuario", back_populates="usuario_etiquetas", foreign_keys=[usuario_id])
    etiqueta: Mapped["Etiqueta"] = relationship("Etiqueta")
    asignador: Mapped[Optional["Usuario"]] = relationship("Usuario", foreign_keys=[asignado_por])

    __table_args__ = (
        Index('idx_usuario_etiquetas_usuario_id', 'usuario_id'),
        Index('idx_usuario_etiquetas_etiqueta_id', 'etiqueta_id'),
        Index('idx_usuario_etiquetas_asignado_por', 'asignado_por'),
    )

class UsuarioEstado(Base):
    __tablename__ = 'usuario_estados'
    
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    usuario_id: Mapped[int] = mapped_column(ForeignKey('usuarios.id', ondelete='CASCADE'), nullable=False)
    estado: Mapped[str] = mapped_column(String(100), nullable=False)
    descripcion: Mapped[Optional[str]] = mapped_column(Text)
    fecha_inicio: Mapped[datetime] = mapped_column(DateTime, server_default=func.current_timestamp())
    fecha_vencimiento: Mapped[Optional[datetime]] = mapped_column(DateTime)
    activo: Mapped[bool] = mapped_column(Boolean, server_default='true')
    creado_por: Mapped[Optional[int]] = mapped_column(ForeignKey('usuarios.id', ondelete='SET NULL'))
    
    usuario: Mapped["Usuario"] = relationship("Usuario", back_populates="usuario_estados", foreign_keys=[usuario_id])
    creador: Mapped[Optional["Usuario"]] = relationship("Usuario", foreign_keys=[creado_por])

    __table_args__ = (
        Index('idx_usuario_estados_usuario_id', 'usuario_id'),
        Index('idx_usuario_estados_creado_por', 'creado_por'),
    )

class HistorialEstado(Base):
    __tablename__ = 'historial_estados'
    
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    usuario_id: Mapped[int] = mapped_column(ForeignKey('usuarios.id', ondelete='CASCADE'), nullable=False)
    estado_id: Mapped[Optional[int]] = mapped_column(ForeignKey('usuario_estados.id', ondelete='CASCADE'))
    accion: Mapped[str] = mapped_column(String(50), nullable=False)
    fecha_accion: Mapped[datetime] = mapped_column(DateTime, server_default=func.current_timestamp())
    detalles: Mapped[Optional[str]] = mapped_column(Text)
    creado_por: Mapped[Optional[int]] = mapped_column(ForeignKey('usuarios.id', ondelete='SET NULL'))
    
    usuario: Mapped["Usuario"] = relationship("Usuario", back_populates="historial_estados", foreign_keys=[usuario_id])
    estado_rel: Mapped[Optional["UsuarioEstado"]] = relationship("UsuarioEstado")
    creador: Mapped[Optional["Usuario"]] = relationship("Usuario", foreign_keys=[creado_por])

    __table_args__ = (
        Index('idx_historial_estados_usuario_id', 'usuario_id'),
        Index('idx_historial_estados_estado_id', 'estado_id'),
        Index('idx_historial_estados_fecha', 'fecha_accion'),
    )

# --- Themes & Configuration ---

class CustomTheme(Base):
    __tablename__ = 'custom_themes'
    
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    nombre: Mapped[str] = mapped_column(String(100), nullable=False)
    name: Mapped[str] = mapped_column(String(100), nullable=False)
    data: Mapped[Optional[dict]] = mapped_column(JSONB)
    colores: Mapped[dict] = mapped_column(JSONB, nullable=False)
    activo: Mapped[bool] = mapped_column(Boolean, server_default='true')
    fecha_creacion: Mapped[datetime] = mapped_column(DateTime, server_default=func.current_timestamp())
    usuario_creador_id: Mapped[Optional[int]] = mapped_column(ForeignKey('usuarios.id'))
    
    schedules: Mapped[List["ThemeSchedule"]] = relationship("ThemeSchedule", back_populates="theme")

    __table_args__ = (
        Index('idx_custom_themes_activo', 'activo'),
    )

class ThemeSchedule(Base):
    __tablename__ = 'theme_schedules'
    
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    name: Mapped[str] = mapped_column(String(200), nullable=False)
    theme_name: Mapped[str] = mapped_column(String(100), nullable=False)
    theme_id: Mapped[Optional[int]] = mapped_column(ForeignKey('custom_themes.id'))
    start_time: Mapped[time] = mapped_column(Time, nullable=False)
    end_time: Mapped[time] = mapped_column(Time, nullable=False)
    monday: Mapped[Optional[bool]] = mapped_column(Boolean, server_default='false')
    tuesday: Mapped[Optional[bool]] = mapped_column(Boolean, server_default='false')
    wednesday: Mapped[Optional[bool]] = mapped_column(Boolean, server_default='false')
    thursday: Mapped[Optional[bool]] = mapped_column(Boolean, server_default='false')
    friday: Mapped[Optional[bool]] = mapped_column(Boolean, server_default='false')
    saturday: Mapped[Optional[bool]] = mapped_column(Boolean, server_default='false')
    sunday: Mapped[Optional[bool]] = mapped_column(Boolean, server_default='false')
    is_active: Mapped[Optional[bool]] = mapped_column(Boolean, server_default='true')
    fecha_inicio: Mapped[Optional[date]] = mapped_column(Date)
    fecha_fin: Mapped[Optional[date]] = mapped_column(Date)
    activo: Mapped[Optional[bool]] = mapped_column(Boolean, server_default='true')
    fecha_creacion: Mapped[datetime] = mapped_column(DateTime, server_default=func.current_timestamp())
    
    theme: Mapped[Optional["CustomTheme"]] = relationship("CustomTheme", back_populates="schedules")

    __table_args__ = (
        Index('idx_theme_schedules_activo', 'activo'),
        Index('idx_theme_schedules_fechas', 'fecha_inicio', 'fecha_fin'),
        Index('idx_theme_schedules_theme_id', 'theme_id'),
    )

class ThemeSchedulingConfig(Base):
    __tablename__ = 'theme_scheduling_config'
    
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    clave: Mapped[str] = mapped_column(String(100), unique=True, nullable=False)
    valor: Mapped[str] = mapped_column(Text, nullable=False)
    config_data: Mapped[Optional[dict]] = mapped_column(JSONB)
    config_type: Mapped[Optional[str]] = mapped_column(String(50), server_default='general')
    descripcion: Mapped[Optional[str]] = mapped_column(Text)
    fecha_actualizacion: Mapped[datetime] = mapped_column(DateTime, server_default=func.current_timestamp())

class NumeracionComprobante(Base):
    __tablename__ = 'numeracion_comprobantes'
    
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    tipo_comprobante: Mapped[str] = mapped_column(String(50), unique=True, nullable=False)
    prefijo: Mapped[str] = mapped_column(String(10), nullable=False, server_default='')
    numero_inicial: Mapped[int] = mapped_column(Integer, nullable=False, server_default='1')
    separador: Mapped[str] = mapped_column(String(5), nullable=False, server_default='-')
    reiniciar_anual: Mapped[Optional[bool]] = mapped_column(Boolean, server_default='false')
    longitud_numero: Mapped[int] = mapped_column(Integer, nullable=False, server_default='8')
    activo: Mapped[Optional[bool]] = mapped_column(Boolean, server_default='true')
    fecha_creacion: Mapped[datetime] = mapped_column(DateTime, server_default=func.current_timestamp())

class AuditLog(Base):
    __tablename__ = 'audit_logs'
    
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    user_id: Mapped[Optional[int]] = mapped_column(ForeignKey('usuarios.id', ondelete='SET NULL'))
    action: Mapped[str] = mapped_column(String(50), nullable=False)
    table_name: Mapped[str] = mapped_column(String(100), nullable=False)
    record_id: Mapped[Optional[int]] = mapped_column(Integer)
    old_values: Mapped[Optional[str]] = mapped_column(Text)
    new_values: Mapped[Optional[str]] = mapped_column(Text)
    ip_address: Mapped[Optional[str]] = mapped_column(INET)
    user_agent: Mapped[Optional[str]] = mapped_column(Text)
    session_id: Mapped[Optional[str]] = mapped_column(String(255))
    timestamp: Mapped[datetime] = mapped_column(DateTime, server_default=func.current_timestamp())

    __table_args__ = (
        Index('idx_audit_logs_user_id', 'user_id'),
    )

class AccionMasivaPendiente(Base):
    __tablename__ = 'acciones_masivas_pendientes'
    
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    operation_id: Mapped[str] = mapped_column(String(100), unique=True, nullable=False)
    tipo: Mapped[str] = mapped_column(String(50), nullable=False)
    descripcion: Mapped[Optional[str]] = mapped_column(Text)
    usuario_ids: Mapped[List[int]] = mapped_column(ARRAY(Integer), nullable=False)
    parametros: Mapped[Optional[dict]] = mapped_column(JSONB)
    estado: Mapped[Optional[str]] = mapped_column(String(20), server_default='pendiente')
    fecha_programada: Mapped[Optional[datetime]] = mapped_column(DateTime)
    fecha_creacion: Mapped[datetime] = mapped_column(DateTime, server_default=func.current_timestamp())
    fecha_ejecucion: Mapped[Optional[datetime]] = mapped_column(DateTime)
    creado_por: Mapped[Optional[int]] = mapped_column(ForeignKey('usuarios.id', ondelete='SET NULL'))
    resultado: Mapped[Optional[dict]] = mapped_column(JSONB)
    error_message: Mapped[Optional[str]] = mapped_column(Text)
    
    creador: Mapped[Optional["Usuario"]] = relationship("Usuario", foreign_keys=[creado_por])

    __table_args__ = (
        Index('idx_acciones_masivas_estado', 'estado'),
        Index('idx_acciones_masivas_usuario_ids', 'usuario_ids', postgresql_using='gin'),
        Index('idx_acciones_masivas_fecha_programada', 'fecha_programada'),
    )

class CheckinPending(Base):
    __tablename__ = 'checkin_pending'
    
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    usuario_id: Mapped[int] = mapped_column(ForeignKey('usuarios.id', ondelete='CASCADE'), nullable=False)
    token: Mapped[str] = mapped_column(String(64), unique=True, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.current_timestamp())
    expires_at: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    used: Mapped[Optional[bool]] = mapped_column(Boolean, server_default='false')

    __table_args__ = (
        Index('idx_checkin_pending_expires_at', 'expires_at'),
        Index('idx_checkin_pending_used', 'used'),
    )

class GymConfig(Base):
    __tablename__ = 'gym_config'
    
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    gym_name: Mapped[Optional[str]] = mapped_column(Text, server_default='')
    gym_slogan: Mapped[Optional[str]] = mapped_column(Text, server_default='')
    gym_address: Mapped[Optional[str]] = mapped_column(Text, server_default='')
    gym_phone: Mapped[Optional[str]] = mapped_column(Text, server_default='')
    gym_email: Mapped[Optional[str]] = mapped_column(Text, server_default='')
    gym_website: Mapped[Optional[str]] = mapped_column(Text, server_default='')
    facebook: Mapped[Optional[str]] = mapped_column(Text, server_default='')
    instagram: Mapped[Optional[str]] = mapped_column(Text, server_default='')
    twitter: Mapped[Optional[str]] = mapped_column(Text, server_default='')
    logo_url: Mapped[Optional[str]] = mapped_column(Text, server_default='')
    updated_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.current_timestamp())

class Configuracion(Base):
    __tablename__ = 'configuracion'
    
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    clave: Mapped[str] = mapped_column(String(255), unique=True, nullable=False)
    valor: Mapped[str] = mapped_column(Text, nullable=False)
    tipo: Mapped[Optional[str]] = mapped_column(String(50), server_default='string')
    descripcion: Mapped[Optional[str]] = mapped_column(Text)

# --- System Diagnostics & Maintenance ---

class SystemDiagnostics(Base):
    __tablename__ = 'system_diagnostics'
    
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    timestamp: Mapped[datetime] = mapped_column(DateTime, server_default=func.current_timestamp())
    diagnostic_type: Mapped[str] = mapped_column(Text, nullable=False)
    component: Mapped[str] = mapped_column(Text, nullable=False)
    status: Mapped[str] = mapped_column(Text, nullable=False)
    details: Mapped[Optional[str]] = mapped_column(Text)
    metrics: Mapped[Optional[str]] = mapped_column(Text)
    resolved: Mapped[Optional[bool]] = mapped_column(Boolean, server_default='false')
    resolved_at: Mapped[Optional[datetime]] = mapped_column(DateTime)
    resolved_by: Mapped[Optional[int]] = mapped_column(ForeignKey('usuarios.id'))

    __table_args__ = (
        Index('idx_system_diagnostics_timestamp', 'timestamp'),
        Index('idx_system_diagnostics_type', 'diagnostic_type'),
        Index('idx_system_diagnostics_status', 'status'),
    )

class MaintenanceTask(Base):
    __tablename__ = 'maintenance_tasks'
    
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    task_name: Mapped[str] = mapped_column(Text, nullable=False)
    task_type: Mapped[str] = mapped_column(Text, nullable=False)
    description: Mapped[Optional[str]] = mapped_column(Text)
    scheduled_at: Mapped[Optional[datetime]] = mapped_column(DateTime)
    executed_at: Mapped[Optional[datetime]] = mapped_column(DateTime)
    status: Mapped[Optional[str]] = mapped_column(Text, server_default='pending')
    result: Mapped[Optional[str]] = mapped_column(Text)
    error_message: Mapped[Optional[str]] = mapped_column(Text)
    created_by: Mapped[Optional[int]] = mapped_column(ForeignKey('usuarios.id'))
    executed_by: Mapped[Optional[int]] = mapped_column(ForeignKey('usuarios.id'))
    auto_schedule: Mapped[Optional[bool]] = mapped_column(Boolean, server_default='false')
    frequency_days: Mapped[Optional[int]] = mapped_column(Integer)
    next_execution: Mapped[Optional[datetime]] = mapped_column(DateTime)

    __table_args__ = (
        Index('idx_maintenance_tasks_status', 'status'),
        Index('idx_maintenance_tasks_scheduled', 'scheduled_at'),
        Index('idx_maintenance_tasks_next_execution', 'next_execution'),
    )

class ClaseAsistenciaHistorial(Base):
    __tablename__ = 'clase_asistencia_historial'
    
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    clase_horario_id: Mapped[int] = mapped_column(ForeignKey('clases_horarios.id', ondelete='CASCADE'), nullable=False)
    usuario_id: Mapped[int] = mapped_column(ForeignKey('usuarios.id', ondelete='CASCADE'), nullable=False)
    fecha_clase: Mapped[date] = mapped_column(Date, nullable=False)
    estado_asistencia: Mapped[Optional[str]] = mapped_column(String(20), server_default='presente')
    hora_llegada: Mapped[Optional[time]] = mapped_column(Time)
    observaciones: Mapped[Optional[str]] = mapped_column(Text)
    registrado_por: Mapped[Optional[int]] = mapped_column(ForeignKey('usuarios.id', ondelete='SET NULL'))
    fecha_registro: Mapped[datetime] = mapped_column(DateTime, server_default=func.current_timestamp())

    __table_args__ = (
        UniqueConstraint('clase_horario_id', 'usuario_id', 'fecha_clase'),
        Index('idx_clase_asistencia_historial_clase_horario_id', 'clase_horario_id'),
        Index('idx_clase_asistencia_historial_usuario_id', 'usuario_id'),
        Index('idx_clase_asistencia_historial_fecha', 'fecha_clase'),
        Index('idx_clase_asistencia_historial_estado', 'estado_asistencia'),
    )

# --- WhatsApp ---

class WhatsappMessage(Base):
    __tablename__ = 'whatsapp_messages'
    
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    user_id: Mapped[Optional[int]] = mapped_column(ForeignKey('usuarios.id'))
    message_type: Mapped[str] = mapped_column(String(50), nullable=False)
    template_name: Mapped[str] = mapped_column(String(255), nullable=False)
    phone_number: Mapped[str] = mapped_column(String(20), nullable=False)
    message_id: Mapped[Optional[str]] = mapped_column(String(100), unique=True)
    sent_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.current_timestamp())
    status: Mapped[Optional[str]] = mapped_column(String(20), server_default='sent')
    message_content: Mapped[Optional[str]] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.current_timestamp())

    __table_args__ = (
        Index('idx_whatsapp_messages_user_id', 'user_id'),
        Index('idx_whatsapp_messages_type_date', 'message_type', text('sent_at DESC')),
        Index('idx_whatsapp_messages_phone', 'phone_number'),
    )

class WhatsappTemplate(Base):
    __tablename__ = 'whatsapp_templates'
    
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    template_name: Mapped[str] = mapped_column(String(255), unique=True, nullable=False)
    header_text: Mapped[Optional[str]] = mapped_column(String(60))
    body_text: Mapped[str] = mapped_column(Text, nullable=False)
    variables: Mapped[Optional[dict]] = mapped_column(JSONB)
    active: Mapped[bool] = mapped_column(Boolean, server_default='true')
    created_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.current_timestamp())

class ProfesorNotificacion(Base):
    __tablename__ = 'profesor_notificaciones'
    
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    profesor_id: Mapped[int] = mapped_column(ForeignKey('profesores.id', ondelete='CASCADE'))
    mensaje: Mapped[str] = mapped_column(Text)
    leida: Mapped[bool] = mapped_column(Boolean, server_default='false')
    fecha_creacion: Mapped[datetime] = mapped_column(DateTime, server_default=func.current_timestamp())
    fecha_lectura: Mapped[Optional[datetime]] = mapped_column(DateTime)
    
    profesor: Mapped["Profesor"] = relationship("Profesor")
    
    __table_args__ = (
        Index('idx_profesor_notificaciones_profesor', 'profesor_id'),
        Index('idx_profesor_notificaciones_leida', 'leida'),
    )

class WhatsappConfig(Base):
    __tablename__ = 'whatsapp_config'
    
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    phone_id: Mapped[str] = mapped_column(String(50), nullable=False)
    waba_id: Mapped[str] = mapped_column(String(50), nullable=False)
    access_token: Mapped[Optional[str]] = mapped_column(Text)
    active: Mapped[bool] = mapped_column(Boolean, server_default='true')
    created_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.current_timestamp())
