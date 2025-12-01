from typing import List, Optional, Dict, Any, Tuple
from sqlalchemy import select, update, insert, delete, func, text, desc, and_
from sqlalchemy.orm import Session, joinedload
from .base import BaseRepository
from ..orm_models import (
    GymConfig, Configuracion, Ejercicio, Rutina, Clase, 
    Pago, Usuario, AccionMasivaPendiente, ClaseHorario, ClaseUsuario,
    ClaseListaEspera, ClaseBloque, ClaseBloqueItem, RutinaEjercicio,
    ClaseEjercicio
)
from datetime import datetime, date, time

class GymRepository(BaseRepository):
    
    # --- Configuration ---
    def obtener_configuracion_gimnasio(self) -> Dict[str, str]:
        conf = self.db.scalar(select(GymConfig).order_by(GymConfig.id))
        if not conf:
             return {'gym_name': 'Gimnasio', 'gym_slogan': '', 'gym_address': '', 'gym_phone': '', 
                     'gym_email': '', 'gym_website': '', 'facebook': '', 'instagram': '', 
                     'twitter': '', 'logo_url': ''}
        
        return {
            'gym_name': conf.gym_name or '',
            'gym_slogan': conf.gym_slogan or '',
            'gym_address': conf.gym_address or '',
            'gym_phone': conf.gym_phone or '',
            'gym_email': conf.gym_email or '',
            'gym_website': conf.gym_website or '',
            'facebook': conf.facebook or '',
            'instagram': conf.instagram or '',
            'twitter': conf.twitter or '',
            'logo_url': conf.logo_url or ''
        }

    def actualizar_configuracion_gimnasio(self, data: dict) -> bool:
        conf = self.db.scalar(select(GymConfig).order_by(GymConfig.id))
        if not conf:
            conf = GymConfig()
            self.db.add(conf)
        
        for key, value in data.items():
            if hasattr(conf, key):
                setattr(conf, key, value)
        
        if 'logo_url' in data:
            logo_url = data['logo_url']
            existing = self.db.scalar(select(Configuracion).where(Configuracion.clave == 'gym_logo_url'))
            if existing:
                existing.valor = logo_url
            else:
                self.db.add(Configuracion(clave='gym_logo_url', valor=logo_url))
                
        self.db.commit()
        self._invalidate_cache('config')
        return True

    def obtener_logo_url(self) -> Optional[str]:
        conf = self.db.scalar(select(GymConfig).order_by(GymConfig.id))
        if conf and conf.logo_url:
            return conf.logo_url
        c = self.db.scalar(select(Configuracion).where(Configuracion.clave == 'gym_logo_url'))
        return c.valor if c else None

    def actualizar_logo_url(self, url: str) -> bool:
        return self.actualizar_configuracion_gimnasio({'logo_url': url})

    # --- Exercises ---
    def obtener_todos_ejercicios(self) -> List[Dict]:
        stmt = select(Ejercicio).order_by(Ejercicio.nombre)
        return [
            {'id': e.id, 'nombre': e.nombre, 'grupo_muscular': e.grupo_muscular, 
             'descripcion': e.descripcion, 'objetivo': e.objetivo, 'video_url': e.video_url}
            for e in self.db.scalars(stmt).all()
        ]

    def obtener_ejercicio(self, ejercicio_id: int) -> Optional[Ejercicio]:
        return self.db.get(Ejercicio, ejercicio_id)

    def crear_ejercicio(self, nombre: str, grupo_muscular: str = None, descripcion: str = None, 
                       objetivo: str = 'general', video_url: str = None) -> int:
        ej = Ejercicio(
            nombre=nombre, grupo_muscular=grupo_muscular, descripcion=descripcion,
            objetivo=objetivo, video_url=video_url
        )
        self.db.add(ej)
        self.db.commit()
        self.db.refresh(ej)
        return ej.id

    def actualizar_ejercicio(self, ejercicio_id: int, **kwargs) -> bool:
        ej = self.db.get(Ejercicio, ejercicio_id)
        if not ej:
            return False
        for k, v in kwargs.items():
            if hasattr(ej, k):
                setattr(ej, k, v)
        self.db.commit()
        return True

    def eliminar_ejercicio(self, ejercicio_id: int) -> bool:
        ej = self.db.get(Ejercicio, ejercicio_id)
        if ej:
            self.db.delete(ej)
            self.db.commit()
            return True
        return False

    # --- Routines ---
    def obtener_todas_rutinas(self) -> List[Dict]:
        stmt = select(Rutina).order_by(Rutina.nombre_rutina)
        return [
            {'id': r.id, 'usuario_id': r.usuario_id, 'nombre_rutina': r.nombre_rutina,
             'descripcion': r.descripcion, 'dias_semana': r.dias_semana, 
             'categoria': r.categoria, 'fecha_creacion': r.fecha_creacion, 'activa': r.activa}
            for r in self.db.scalars(stmt).all()
        ]

    def crear_rutina(self, nombre: str, usuario_id: int = None, descripcion: str = None, 
                    dias_semana: int = None, categoria: str = 'general') -> int:
        rutina = Rutina(
            nombre_rutina=nombre, usuario_id=usuario_id, descripcion=descripcion,
            dias_semana=dias_semana, categoria=categoria, activa=True
        )
        self.db.add(rutina)
        self.db.commit()
        self.db.refresh(rutina)
        return rutina.id

    def agregar_ejercicio_rutina(self, rutina_id: int, ejercicio_id: int, series: int = 3, 
                                repeticiones: str = '10', dia_semana: int = None, orden: int = 0):
        re = RutinaEjercicio(
            rutina_id=rutina_id, ejercicio_id=ejercicio_id, series=series,
            repeticiones=repeticiones, dia_semana=dia_semana, orden=orden
        )
        self.db.add(re)
        self.db.commit()

    def obtener_detalles_rutina(self, rutina_id: int) -> Dict:
        rutina = self.db.get(Rutina, rutina_id)
        if not rutina:
            return {}
        
        stmt = select(RutinaEjercicio, Ejercicio).join(Ejercicio).where(RutinaEjercicio.rutina_id == rutina_id).order_by(RutinaEjercicio.orden)
        ejercicios = []
        for re, ej in self.db.execute(stmt).all():
            ejercicios.append({
                'ejercicio_id': ej.id, 'nombre': ej.nombre, 'series': re.series,
                'repeticiones': re.repeticiones, 'dia_semana': re.dia_semana, 'orden': re.orden
            })
            
        return {
            'id': rutina.id, 'nombre': rutina.nombre_rutina, 'descripcion': rutina.descripcion,
            'ejercicios': ejercicios
        }

    def eliminar_rutina(self, rutina_id: int) -> bool:
        r = self.db.get(Rutina, rutina_id)
        if r:
            self.db.delete(r)
            self.db.commit()
            return True
        return False

    # --- Classes ---
    def obtener_todas_clases(self) -> List[Dict]:
        stmt = select(Clase).order_by(Clase.nombre)
        return [
            {'id': c.id, 'nombre': c.nombre, 'descripcion': c.descripcion, 
             'activa': c.activa, 'tipo_clase_id': c.tipo_clase_id}
            for c in self.db.scalars(stmt).all()
        ]

    def crear_clase(self, nombre: str, descripcion: str = None, tipo_clase_id: int = None) -> int:
        clase = Clase(nombre=nombre, descripcion=descripcion, tipo_clase_id=tipo_clase_id, activa=True)
        self.db.add(clase)
        self.db.commit()
        self.db.refresh(clase)
        return clase.id

    def programar_horario_clase(self, clase_id: int, dia_semana: str, hora_inicio: time, hora_fin: time, cupo_maximo: int = 20) -> int:
        horario = ClaseHorario(
            clase_id=clase_id, dia_semana=dia_semana, 
            hora_inicio=hora_inicio, hora_fin=hora_fin, 
            cupo_maximo=cupo_maximo, activo=True
        )
        self.db.add(horario)
        self.db.commit()
        self.db.refresh(horario)
        return horario.id

    def obtener_horarios_clase(self, clase_id: int) -> List[Dict]:
        stmt = select(ClaseHorario).where(ClaseHorario.clase_id == clase_id, ClaseHorario.activo == True)
        return [
            {'id': h.id, 'dia_semana': h.dia_semana, 'hora_inicio': h.hora_inicio, 
             'hora_fin': h.hora_fin, 'cupo_maximo': h.cupo_maximo}
            for h in self.db.scalars(stmt).all()
        ]

    def inscribir_usuario_clase(self, usuario_id: int, clase_horario_id: int) -> bool:
        horario = self.db.get(ClaseHorario, clase_horario_id)
        if not horario:
            raise ValueError("Horario no encontrado")
            
        inscritos = self.db.scalar(select(func.count(ClaseUsuario.id)).where(ClaseUsuario.clase_horario_id == clase_horario_id))
        if inscritos >= (horario.cupo_maximo or 0):
            posicion = (self.db.scalar(select(func.max(ClaseListaEspera.posicion)).where(ClaseListaEspera.clase_horario_id == clase_horario_id)) or 0) + 1
            wait = ClaseListaEspera(usuario_id=usuario_id, clase_horario_id=clase_horario_id, posicion=posicion, activo=True)
            self.db.add(wait)
            self.db.commit()
            return False 
            
        inscripcion = ClaseUsuario(usuario_id=usuario_id, clase_horario_id=clase_horario_id)
        self.db.add(inscripcion)
        self.db.commit()
        return True 

    def cancelar_inscripcion_clase(self, usuario_id: int, clase_horario_id: int) -> bool:
        stmt = delete(ClaseUsuario).where(
            ClaseUsuario.usuario_id == usuario_id, 
            ClaseUsuario.clase_horario_id == clase_horario_id
        )
        result = self.db.execute(stmt)
        
        stmt_wait = delete(ClaseListaEspera).where(
            ClaseListaEspera.usuario_id == usuario_id, 
            ClaseListaEspera.clase_horario_id == clase_horario_id
        )
        self.db.execute(stmt_wait)
        
        self.db.commit()
        return result.rowcount > 0

    def obtener_inscritos_clase(self, clase_horario_id: int) -> List[Dict]:
        stmt = select(Usuario).join(ClaseUsuario).where(ClaseUsuario.clase_horario_id == clase_horario_id)
        return [{'id': u.id, 'nombre': u.nombre, 'dni': u.dni} for u in self.db.scalars(stmt).all()]

    def crear_bloque_clase(self, clase_id: int, nombre: str) -> int:
        bloque = ClaseBloque(clase_id=clase_id, nombre=nombre)
        self.db.add(bloque)
        self.db.commit()
        self.db.refresh(bloque)
        return bloque.id

    def agregar_item_bloque(self, bloque_id: int, ejercicio_id: int, orden: int, 
                           series: int = 0, repeticiones: str = None, descanso: int = 0):
        item = ClaseBloqueItem(
            bloque_id=bloque_id, ejercicio_id=ejercicio_id, orden=orden,
            series=series, repeticiones=repeticiones, descanso_segundos=descanso
        )
        self.db.add(item)
        self.db.commit()

    # --- Stats & Legacy ---
    def obtener_arpu_y_morosos_mes_actual(self) -> Tuple[float, int]:
        mes = date.today().month
        año = date.today().year
        
        total_activos = self.db.scalar(
            select(func.count(Usuario.id)).where(
                Usuario.activo == True, 
                Usuario.rol.in_(['socio', 'miembro'])
            )
        ) or 0
        
        ingresos = self.db.scalar(
            select(func.sum(Pago.monto)).where(
                func.extract('month', Pago.fecha_pago) == mes,
                func.extract('year', Pago.fecha_pago) == año
            )
        ) or 0.0
        
        arpu = (float(ingresos) / total_activos) if total_activos > 0 else 0.0
        
        subq = select(Pago.usuario_id).where(
            func.extract('month', Pago.fecha_pago) == mes,
            func.extract('year', Pago.fecha_pago) == año
        )
        
        morosos = self.db.scalar(
            select(func.count(Usuario.id)).where(
                Usuario.activo == True,
                Usuario.rol.in_(['socio', 'miembro']),
                Usuario.id.not_in(subq)
            )
        ) or 0
        
        return arpu, morosos

    def dni_existe(self, dni: str, user_id_to_ignore: Optional[int] = None) -> bool:
        stmt = select(func.count(Usuario.id)).where(Usuario.dni == dni)
        if user_id_to_ignore:
            stmt = stmt.where(Usuario.id != user_id_to_ignore)
        return (self.db.scalar(stmt) or 0) > 0

    def registrar_ejercicios_batch(self, ejercicios_items: List[Dict[str, Any]], skip_duplicates: bool = True, validate_data: bool = True) -> Dict[str, Any]:
        result = {'insertados': [], 'actualizados': [], 'omitidos': [], 'count': 0}
        
        for item in ejercicios_items:
            nombre = item.get('nombre')
            if validate_data and not nombre:
                result['omitidos'].append(item)
                continue
                
            existing = self.db.scalar(select(Ejercicio).where(func.lower(Ejercicio.nombre) == str(nombre).lower()))
            
            if existing:
                if not skip_duplicates:
                    existing.grupo_muscular = item.get('grupo_muscular') or existing.grupo_muscular
                    existing.descripcion = item.get('descripcion') or existing.descripcion
                    result['actualizados'].append(existing.nombre)
                else:
                    pass
            else:
                new_ej = Ejercicio(
                    nombre=nombre,
                    grupo_muscular=item.get('grupo_muscular'),
                    descripcion=item.get('descripcion')
                )
                self.db.add(new_ej)
                self.db.flush()
                result['insertados'].append(new_ej.id)
                
        self.db.commit()
        result['count'] = len(result['insertados']) + len(result['actualizados'])
        self._invalidate_cache('ejercicios')
        return result
