from typing import List, Optional, Dict, Any
from datetime import datetime, date, timedelta, time
import logging
from sqlalchemy import select, update, delete, func, or_, and_
from sqlalchemy.orm import Session
from .base import BaseRepository
from ..orm_models import (
    Profesor, ProfesorHoraTrabajada, HorarioProfesor, Usuario, 
    ProfesorEspecialidad, Especialidad, ProfesorCertificacion
)

class TeacherRepository(BaseRepository):

    def obtener_todos_profesores(self) -> List[Dict]:
        stmt = select(Profesor).order_by(Profesor.id)
        profesores = self.db.scalars(stmt).all()
        result = []
        for p in profesores:
            u = p.usuario
            result.append({
                'id': p.id,
                'usuario_id': p.usuario_id,
                'nombre': u.nombre,
                'dni': u.dni,
                'telefono': u.telefono,
                'activo': u.activo,
                'tipo': p.tipo,
                'especialidades': p.especialidades,
                'certificaciones': p.certificaciones,
                'experiencia_años': p.experiencia_años,
                'tarifa_por_hora': float(p.tarifa_por_hora) if p.tarifa_por_hora else 0.0,
                'fecha_contratacion': p.fecha_contratacion,
                'estado': p.estado,
                'biografia': p.biografia,
                'foto_perfil': p.foto_perfil,
                'telefono_emergencia': p.telefono_emergencia
            })
        return result

    def obtener_profesores(self) -> List[Dict]:
        return self.obtener_todos_profesores()

    def obtener_profesores_basico(self) -> List[Dict]:
        stmt = select(Usuario.id, Usuario.nombre).where(Usuario.rol == 'profesor').order_by(Usuario.nombre)
        return [{'id': r.id, 'nombre': r.nombre} for r in self.db.execute(stmt).all()]

    def obtener_profesores_basico_con_ids(self) -> List[Dict]:
        stmt = select(Profesor.id, Usuario.nombre).join(Usuario).where(Usuario.activo == True).order_by(Usuario.nombre)
        return [{'id': r.id, 'nombre': r.nombre} for r in self.db.execute(stmt).all()]

    def obtener_profesor_por_id(self, profesor_id: int) -> Optional[Dict]:
        p = self.db.get(Profesor, profesor_id)
        if not p:
            return None
        u = p.usuario
        return {
            'id': p.id,
            'usuario_id': p.usuario_id,
            'nombre': u.nombre,
            'dni': u.dni,
            'telefono': u.telefono,
            'activo': u.activo,
            'tipo': p.tipo,
            'especialidades': p.especialidades,
            'certificaciones': p.certificaciones,
            'experiencia_años': p.experiencia_años,
            'tarifa_por_hora': float(p.tarifa_por_hora) if p.tarifa_por_hora else 0.0,
            'fecha_contratacion': p.fecha_contratacion,
            'estado': p.estado,
            'biografia': p.biografia,
            'foto_perfil': p.foto_perfil,
            'telefono_emergencia': p.telefono_emergencia,
            'horario_disponible': p.horario_disponible
        }

    def crear_profesor(self, usuario_id: int, especialidades: str = "", certificaciones: str = "", 
                      experiencia_años: int = 0, tarifa_por_hora: float = 0.0, 
                      fecha_contratacion: date = None, biografia: str = "", 
                      telefono_emergencia: str = "") -> int:
        
        if not fecha_contratacion:
            fecha_contratacion = date.today()
            
        profesor = Profesor(
            usuario_id=usuario_id,
            especialidades=especialidades,
            certificaciones=certificaciones,
            experiencia_años=experiencia_años,
            tarifa_por_hora=tarifa_por_hora,
            fecha_contratacion=fecha_contratacion,
            biografia=biografia,
            telefono_emergencia=telefono_emergencia
        )
        self.db.add(profesor)
        self.db.commit()
        self.db.refresh(profesor)
        return profesor.id

    def actualizar_profesor(self, profesor_id: int, **kwargs) -> bool:
        profesor = self.db.get(Profesor, profesor_id)
        if not profesor:
            raise Exception(f"Profesor con ID {profesor_id} no encontrado")
            
        usuario = profesor.usuario
        
        campos_profesores = {
            'tipo', 'especialidades', 'certificaciones', 'experiencia_años', 
            'tarifa_por_hora', 'fecha_contratacion', 'biografia', 'telefono_emergencia', 'estado'
        }
        campos_usuarios = {
            'nombre', 'apellido', 'email', 'telefono', 'direccion', 'activo'
        }
        
        for k, v in kwargs.items():
            if k in campos_profesores:
                setattr(profesor, k, v)
            elif k in campos_usuarios:
                setattr(usuario, k, v)
                
        self.db.commit()
        return True

    def actualizar_estado_profesor(self, profesor_id: int, nuevo_estado: str) -> bool:
        profesor = self.db.get(Profesor, profesor_id)
        if not profesor:
            return False
        
        profesor.estado = nuevo_estado
        profesor.usuario.activo = (nuevo_estado == 'activo')
        self.db.commit()
        return True

    def actualizar_profesor_sesion(
        self,
        sesion_id: int,
        *,
        fecha: Optional[str] = None,
        hora_inicio: Optional[str] = None,
        hora_fin: Optional[str] = None,
        tipo_actividad: Optional[str] = None,
        minutos_totales: Optional[int] = None,
        timeout_ms: int = 1500,
    ) -> Dict[str, Any]:
        
        sesion = self.db.get(ProfesorHoraTrabajada, sesion_id)
        if not sesion:
             return {"success": False, "error": "Sesión no encontrada"}
             
        if fecha:
            if isinstance(fecha, str):
                sesion.fecha = datetime.strptime(fecha, "%Y-%m-%d").date()
            else:
                sesion.fecha = fecha
                
        if hora_inicio:
            try:
                if isinstance(hora_inicio, str):
                    if 'T' in hora_inicio:
                         dt = datetime.fromisoformat(hora_inicio)
                         sesion.hora_inicio = dt
                    else:
                         t = time.fromisoformat(hora_inicio)
                         sesion.hora_inicio = datetime.combine(sesion.fecha, t)
            except:
                pass

        if hora_fin:
             try:
                if isinstance(hora_fin, str):
                    if 'T' in hora_fin:
                         dt = datetime.fromisoformat(hora_fin)
                         sesion.hora_fin = dt
                    else:
                         t = time.fromisoformat(hora_fin)
                         sesion.hora_fin = datetime.combine(sesion.fecha, t)
             except:
                pass
                
        if sesion.hora_inicio and sesion.hora_fin:
            diff = sesion.hora_fin - sesion.hora_inicio
            sesion.minutos_totales = int(diff.total_seconds() / 60)
            sesion.horas_totales = round(sesion.minutos_totales / 60.0, 2)
            
        if tipo_actividad:
            sesion.tipo_actividad = tipo_actividad
            
        if minutos_totales is not None:
            sesion.minutos_totales = minutos_totales
            sesion.horas_totales = round(minutos_totales / 60.0, 2)

        self.db.commit()
        self.db.refresh(sesion)
        return {"success": True, "updated": {
            'id': sesion.id, 'profesor_id': sesion.profesor_id, 'fecha': sesion.fecha,
            'hora_inicio': sesion.hora_inicio, 'hora_fin': sesion.hora_fin,
            'minutos_totales': sesion.minutos_totales, 'horas_totales': sesion.horas_totales,
            'tipo_actividad': sesion.tipo_actividad
        }}

    def eliminar_profesor_sesion(self, sesion_id: int) -> Dict[str, Any]:
        sesion = self.db.get(ProfesorHoraTrabajada, sesion_id)
        if sesion:
            self.db.delete(sesion)
            self.db.commit()
            return {"success": True, "deleted_id": sesion_id}
        return {"success": False, "error": "Sesión no encontrada"}

    def obtener_minutos_proyectados_profesor_rango(self, profesor_id: int, fecha_inicio: str, fecha_fin: str) -> Dict[str, Any]:
        try:
            start = datetime.strptime(fecha_inicio, "%Y-%m-%d").date()
            end = datetime.strptime(fecha_fin, "%Y-%m-%d").date()
            
            stmt = select(HorarioProfesor).where(HorarioProfesor.profesor_id == profesor_id, HorarioProfesor.disponible == True)
            horarios = self.db.scalars(stmt).all()
            
            minutos_por_dia = {i: 0 for i in range(7)}
            dias_map = {'Lunes': 0, 'Martes': 1, 'Miércoles': 2, 'Jueves': 3, 'Viernes': 4, 'Sábado': 5, 'Domingo': 6}
            
            for h in horarios:
                if h.dia_semana in dias_map:
                    idx = dias_map[h.dia_semana]
                    dummy_date = date(2000, 1, 1)
                    start_dt = datetime.combine(dummy_date, h.hora_inicio)
                    end_dt = datetime.combine(dummy_date, h.hora_fin)
                    if end_dt < start_dt:
                        end_dt += timedelta(days=1)
                    duration = int((end_dt - start_dt).total_seconds() / 60)
                    minutos_por_dia[idx] += duration
            
            total_minutos = 0
            dias_count = {i: 0 for i in range(7)}
            
            curr = start
            while curr <= end:
                idx = curr.weekday()
                dias_count[idx] += 1
                total_minutos += minutos_por_dia[idx]
                curr += timedelta(days=1)
                
            return {
                "success": True,
                "minutos_proyectados": total_minutos,
                "horas_proyectadas": round(total_minutos / 60.0, 2),
                "dias_con_disponibilidad": dias_count
            }
            
        except Exception as e:
            return {"success": False, "error": str(e), "minutos_proyectados": 0}

    # --- Especialidades y Certificaciones ---

    def asignar_especialidad_profesor(self, profesor_id: int, especialidad_id: int, 
                                     nivel_experiencia: str = None, años_experiencia: int = 0) -> int:
        
        existing = self.db.scalar(select(ProfesorEspecialidad).where(
            ProfesorEspecialidad.profesor_id == profesor_id,
            ProfesorEspecialidad.especialidad_id == especialidad_id
        ))
        
        if existing:
            existing.nivel_experiencia = nivel_experiencia
            existing.años_experiencia = años_experiencia
            existing.activo = True
            self.db.commit()
            return existing.id
        
        pe = ProfesorEspecialidad(
            profesor_id=profesor_id, 
            especialidad_id=especialidad_id,
            nivel_experiencia=nivel_experiencia,
            años_experiencia=años_experiencia,
            activo=True
        )
        
        self.db.add(pe)
        self.db.commit()
        self.db.refresh(pe)
        return pe.id

    def quitar_especialidad_profesor(self, profesor_id: int, especialidad_id: int) -> bool:
        stmt = delete(ProfesorEspecialidad).where(
            ProfesorEspecialidad.profesor_id == profesor_id,
            ProfesorEspecialidad.especialidad_id == especialidad_id
        )
        result = self.db.execute(stmt)
        self.db.commit()
        return result.rowcount > 0

    def obtener_especialidades_profesor(self, profesor_id: int) -> List[Dict]:
        stmt = select(ProfesorEspecialidad, Especialidad).join(Especialidad).where(
            ProfesorEspecialidad.profesor_id == profesor_id,
            Especialidad.activo == True
        )
        results = self.db.execute(stmt).all()
        return [
            {
                'id': pe.id, 'profesor_id': pe.profesor_id, 'especialidad_id': pe.especialidad_id,
                'especialidad_nombre': e.nombre, 'especialidad_descripcion': e.descripcion,
                'nivel_experiencia': pe.nivel_experiencia, 'años_experiencia': pe.años_experiencia
            }
            for pe, e in results
        ]

    def crear_certificacion_profesor(self, profesor_id: int, nombre_certificacion: str, 
                                    institucion_emisora: str = None, numero_certificado: str = None,
                                    fecha_obtencion: date = None, fecha_vencimiento: date = None,
                                    archivo_adjunto: str = None, notas: str = None) -> int:
        pc = ProfesorCertificacion(
            profesor_id=profesor_id,
            nombre_certificacion=nombre_certificacion,
            institucion_emisora=institucion_emisora,
            numero_certificado=numero_certificado,
            fecha_obtencion=fecha_obtencion,
            fecha_vencimiento=fecha_vencimiento,
            archivo_adjunto=archivo_adjunto,
            notas=notas
        )
        self.db.add(pc)
        self.db.commit()
        self.db.refresh(pc)
        return pc.id

    def obtener_certificaciones_profesor(self, profesor_id: int, solo_vigentes: bool = False) -> List[Dict]:
        stmt = select(ProfesorCertificacion).where(ProfesorCertificacion.profesor_id == profesor_id)
        if solo_vigentes:
            stmt = stmt.where(ProfesorCertificacion.estado == 'vigente')
        stmt = stmt.order_by(ProfesorCertificacion.fecha_obtencion.desc())
        
        return [
            {
                'id': c.id, 'nombre_certificacion': c.nombre_certificacion, 
                'institucion_emisora': c.institucion_emisora, 'fecha_obtencion': c.fecha_obtencion,
                'fecha_vencimiento': c.fecha_vencimiento, 'estado': c.estado
            }
            for c in self.db.scalars(stmt).all()
        ]

    def actualizar_certificacion_profesor(self, certificacion_id: int, **kwargs) -> bool:
        c = self.db.get(ProfesorCertificacion, certificacion_id)
        if not c:
            return False
            
        for k, v in kwargs.items():
            if hasattr(c, k):
                setattr(c, k, v)
        
        self.db.commit()
        return True

    def eliminar_certificacion_profesor(self, certificacion_id: int) -> bool:
        c = self.db.get(ProfesorCertificacion, certificacion_id)
        if c:
            self.db.delete(c)
            self.db.commit()
            return True
        return False
