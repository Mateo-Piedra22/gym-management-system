from typing import List, Optional, Dict, Any, Set
from datetime import date, datetime, timedelta
import logging
from sqlalchemy import select, update, delete, func, text, or_, and_
from sqlalchemy.orm import Session
from .base import BaseRepository
from ..orm_models import (
    Usuario, Pago, Asistencia, Rutina, ClaseUsuario, ClaseListaEspera,
    UsuarioNota, UsuarioEtiqueta, UsuarioEstado, Profesor, NotificacionCupo,
    AuditLog, CheckinPending, TipoCuota, Etiqueta, UsuarioEtiqueta, HistorialEstado
)

class UserRepository(BaseRepository):
    
    def listar_usuarios_paginados(self, q: Optional[str] = None, limit: int = 50, offset: int = 0) -> List[Dict]:
        stmt = select(Usuario)
        
        if q:
            stmt = stmt.where(
                or_(
                    Usuario.nombre.ilike(f"%{q}%"),
                    Usuario.dni.ilike(f"%{q}%"),
                    Usuario.telefono.ilike(f"%{q}%")
                )
            )
            
        stmt = stmt.order_by(Usuario.nombre.asc()).limit(limit).offset(offset)
        
        users = self.db.scalars(stmt).all()
        return [
            {
                'id': u.id, 
                'nombre': (u.nombre or "").strip(), 
                'dni': u.dni, 
                'telefono': u.telefono, 
                'rol': (u.rol or "").strip().lower(), 
                'tipo_cuota': u.tipo_cuota, 
                'activo': u.activo, 
                'fecha_registro': u.fecha_registro
            }
            for u in users
        ]

    def cambiar_usuario_id(self, current_id: int, new_id: int):
        # This is a dangerous operation, but requested by the user/legacy code.
        # We need to disable foreign key checks or cascade updates if the DB supports it, 
        # or update all related tables manually.
        # PostgreSQL ON UPDATE CASCADE handles this if configured.
        # If not, we might fail.
        
        # Using raw SQL for this specific admin operation
        try:
            self.db.execute(text("UPDATE usuarios SET id = :new_id WHERE id = :old_id"), {"new_id": new_id, "old_id": current_id})
            self.db.commit()
        except Exception as e:
            self.db.rollback()
            raise e

    def obtener_o_crear_etiqueta(self, nombre: str) -> Etiqueta:
        stmt = select(Etiqueta).where(func.lower(Etiqueta.nombre) == nombre.lower().strip())
        etiqueta = self.db.scalar(stmt)
        if not etiqueta:
            etiqueta = Etiqueta(nombre=nombre.strip(), color='#3498db')
            self.db.add(etiqueta)
            self.db.commit()
            self.db.refresh(etiqueta)
        return etiqueta

    # --- Basic CRUD ---
    def obtener_usuario(self, usuario_id: int) -> Optional[Usuario]:
        return self.db.get(Usuario, usuario_id)

    def obtener_usuario_por_id(self, usuario_id: int) -> Optional[Usuario]:
        return self.obtener_usuario(usuario_id)

    def obtener_usuario_por_dni(self, dni: str) -> Optional[Usuario]:
        stmt = select(Usuario).where(Usuario.dni == dni)
        return self.db.scalar(stmt)

    def obtener_todos_usuarios(self) -> List[Usuario]:
        stmt = select(Usuario).order_by(
            func.case(
                (Usuario.rol == 'dueño', 0),
                (Usuario.rol == 'profesor', 1),
                else_=2
            ),
            Usuario.nombre.asc()
        )
        return list(self.db.scalars(stmt).all())

    def crear_usuario(self, usuario: Usuario) -> int:
        if not isinstance(usuario, Usuario):
            data = {k: getattr(usuario, k) for k in ['nombre', 'dni', 'telefono', 'pin', 'rol', 'activo', 'tipo_cuota', 'notas'] if hasattr(usuario, k)}
            usuario = Usuario(**data)
        
        self.db.add(usuario)
        self.db.flush()
        
        if usuario.rol == 'socio':
            usuario.fecha_proximo_vencimiento = date.today() + timedelta(days=30)
        
        if usuario.rol == 'profesor':
            self._crear_profesor_default(usuario.id)
            
        self.db.commit()
        self.db.refresh(usuario)
        self._invalidate_cache('usuarios')
        return usuario.id

    def _crear_profesor_default(self, usuario_id: int):
        profesor = Profesor(
            usuario_id=usuario_id,
            especialidades='',
            certificaciones='',
            experiencia_años=0,
            tarifa_por_hora=0.0,
            fecha_contratacion=date.today(),
            biografia='',
            telefono_emergencia=''
        )
        self.db.add(profesor)

    def actualizar_usuario(self, usuario: Usuario):
        existing = self.db.get(Usuario, usuario.id)
        if not existing:
            return
            
        rol_anterior = existing.rol
        
        existing.nombre = usuario.nombre
        existing.dni = usuario.dni
        existing.telefono = usuario.telefono
        existing.pin = usuario.pin
        existing.rol = usuario.rol
        existing.activo = usuario.activo
        existing.tipo_cuota = usuario.tipo_cuota
        existing.notas = usuario.notas
        
        if rol_anterior != existing.rol:
            if existing.rol == 'profesor' and rol_anterior != 'profesor':
                self._crear_profesor_default(existing.id)
            elif rol_anterior == 'profesor' and existing.rol != 'profesor':
                self.db.execute(delete(Profesor).where(Profesor.usuario_id == existing.id))
        
        self.db.commit()
        self._invalidate_cache('usuarios')

    def eliminar_usuario(self, usuario_id: int):
        user = self.db.get(Usuario, usuario_id)
        if user and user.rol == 'dueño':
            raise PermissionError(f"El usuario con ID {usuario_id} es dueño y no puede ser eliminado.")
            
        if user:
            # Eliminar referencias manuales si necesario (aunque cascade debería manejarlo)
            self.db.delete(user)
            self.db.commit()
            self._invalidate_cache('usuarios')

    # --- Search & Filters ---
    def buscar_usuarios(self, query: str) -> List[Dict]:
        stmt = select(Usuario).where(
            or_(
                Usuario.nombre.ilike(f"%{query}%"),
                Usuario.dni.ilike(f"%{query}%"),
                Usuario.telefono.ilike(f"%{query}%")
            )
        )
        users = self.db.scalars(stmt).all()
        return [
            {'id': u.id, 'nombre': u.nombre, 'dni': u.dni, 'rol': u.rol, 'activo': u.activo}
            for u in users
        ]

    def obtener_usuarios_activos(self) -> List[Usuario]:
        return list(self.db.scalars(select(Usuario).where(Usuario.activo == True)).all())

    def obtener_usuarios_inactivos(self) -> List[Usuario]:
        return list(self.db.scalars(select(Usuario).where(Usuario.activo == False)).all())

    # --- Business Logic ---
    def alternar_estado_activo(self, usuario_id: int) -> bool:
        user = self.db.get(Usuario, usuario_id)
        if user:
            user.activo = not user.activo
            self.db.commit()
            self._invalidate_cache('usuarios')
            return user.activo
        return False

    def cambiar_pin(self, usuario_id: int, nuevo_pin: str) -> bool:
        user = self.db.get(Usuario, usuario_id)
        if user:
            user.pin = nuevo_pin
            self.db.commit()
            return True
        return False

    def actualizar_ultimo_pago(self, usuario_id: int, fecha: date):
        user = self.db.get(Usuario, usuario_id)
        if user:
            user.ultimo_pago = fecha
            self.db.commit()

    # --- Notes Management ---
    def agregar_nota_usuario(self, usuario_id: int, titulo: str, contenido: str, 
                            autor_id: int = None, categoria: str = 'general', importancia: str = 'normal') -> int:
        nota = UsuarioNota(
            usuario_id=usuario_id,
            titulo=titulo,
            contenido=contenido,
            autor_id=autor_id,
            categoria=categoria,
            importancia=importancia,
            activa=True
        )
        self.db.add(nota)
        self.db.commit()
        self.db.refresh(nota)
        return nota.id

    def obtener_notas_usuario(self, usuario_id: int) -> List[Dict]:
        stmt = select(UsuarioNota, Usuario).outerjoin(Usuario, UsuarioNota.autor_id == Usuario.id).where(
            UsuarioNota.usuario_id == usuario_id,
            UsuarioNota.activa == True
        ).order_by(UsuarioNota.fecha_creacion.desc())
        
        results = self.db.execute(stmt).all()
        return [
            {
                'id': n.id, 'titulo': n.titulo, 'contenido': n.contenido,
                'categoria': n.categoria, 'importancia': n.importancia,
                'fecha_creacion': n.fecha_creacion,
                'autor_nombre': u.nombre if u else 'Sistema'
            }
            for n, u in results
        ]

    def eliminar_nota(self, nota_id: int):
        nota = self.db.get(UsuarioNota, nota_id)
        if nota:
            nota.activa = False # Soft delete
            self.db.commit()

    # --- Tags Management ---
    def asignar_etiqueta(self, usuario_id: int, etiqueta_id: int, asignado_por: int = None):
        # Check existing
        exists = self.db.scalar(select(UsuarioEtiqueta).where(
            UsuarioEtiqueta.usuario_id == usuario_id,
            UsuarioEtiqueta.etiqueta_id == etiqueta_id
        ))
        if not exists:
            ue = UsuarioEtiqueta(usuario_id=usuario_id, etiqueta_id=etiqueta_id, asignado_por=asignado_por)
            self.db.add(ue)
            self.db.commit()

    def remover_etiqueta(self, usuario_id: int, etiqueta_id: int):
        stmt = delete(UsuarioEtiqueta).where(
            UsuarioEtiqueta.usuario_id == usuario_id,
            UsuarioEtiqueta.etiqueta_id == etiqueta_id
        )
        self.db.execute(stmt)
        self.db.commit()

    def obtener_etiquetas_usuario(self, usuario_id: int) -> List[Dict]:
        stmt = select(Etiqueta).join(UsuarioEtiqueta).where(UsuarioEtiqueta.usuario_id == usuario_id)
        return [{'id': e.id, 'nombre': e.nombre, 'color': e.color} for e in self.db.scalars(stmt).all()]

    def obtener_todas_etiquetas(self) -> List[Dict]:
        stmt = select(Etiqueta).where(Etiqueta.activo == True)
        return [{'id': e.id, 'nombre': e.nombre, 'color': e.color} for e in self.db.scalars(stmt).all()]

    def crear_etiqueta(self, nombre: str, color: str = '#3498db', descripcion: str = None) -> int:
        etiqueta = Etiqueta(nombre=nombre, color=color, descripcion=descripcion)
        self.db.add(etiqueta)
        self.db.commit()
        self.db.refresh(etiqueta)
        return etiqueta.id

    # --- States History ---
    def registrar_cambio_estado(self, usuario_id: int, nuevo_estado: str, accion: str, 
                               detalles: str = None, creado_por: int = None):
        # Actualizar tabla actual si aplica (aunque UsuarioEstado es mas para estados logicos como 'lesionado')
        # Aquí solo logueamos en historial
        hist = HistorialEstado(
            usuario_id=usuario_id,
            accion=accion,
            detalles=detalles,
            creado_por=creado_por
        )
        self.db.add(hist)
        self.db.commit()

    def obtener_historial_estados(self, usuario_id: int) -> List[Dict]:
        stmt = select(HistorialEstado).where(HistorialEstado.usuario_id == usuario_id).order_by(HistorialEstado.fecha_accion.desc())
        return [
            {'id': h.id, 'accion': h.accion, 'fecha': h.fecha_accion, 'detalles': h.detalles}
            for h in self.db.scalars(stmt).all()
        ]

    # --- Legacy/Bulk Methods ---
    def usuario_id_existe(self, usuario_id: int) -> bool:
        return self.db.scalar(select(func.count(Usuario.id)).where(Usuario.id == usuario_id)) > 0

    def desactivar_usuarios_por_falta_de_pago(self) -> List[Dict]:
        fecha_limite = date.today() - timedelta(days=90)
        subq = select(Pago.usuario_id).where(
            func.make_date(Pago.año, Pago.mes, 1) > fecha_limite
        ).distinct()
        
        stmt = select(Usuario).where(
            Usuario.activo == True,
            Usuario.rol == 'socio',
            Usuario.id.not_in(subq)
        )
        users = self.db.scalars(stmt).all()
        result = []
        
        for user in users:
            user.activo = False
            self.registrar_cambio_estado(user.id, 'inactivo', 'desactivacion_automatica', 'Falta de pago > 90 dias')
            result.append({'id': user.id, 'nombre': user.nombre})
            
        self.db.commit()
        if result:
            self._invalidate_cache('usuarios')
        return result

    def obtener_resumen_referencias_usuario(self, usuario_id: int) -> dict:
        def count_in(model, col_name='usuario_id'):
            return self.db.scalar(select(func.count()).select_from(model).where(getattr(model, col_name) == usuario_id))

        wa_count = 0
        try:
            # Use text for table not yet imported or raw count
            wa_count = self.db.scalar(text("SELECT COUNT(*) FROM whatsapp_messages WHERE user_id = :uid"), {"uid": usuario_id})
        except: pass

        return {
            'pagos': count_in(Pago),
            'asistencias': count_in(Asistencia),
            'rutinas': count_in(Rutina),
            'clase_usuarios': count_in(ClaseUsuario),
            'clase_lista_espera': count_in(ClaseListaEspera),
            'usuario_notas': count_in(UsuarioNota),
            'usuario_etiquetas': count_in(UsuarioEtiqueta),
            'usuario_estados': count_in(UsuarioEstado),
            'profesores': count_in(Profesor),
            'notificaciones_cupos': count_in(NotificacionCupo),
            'audit_logs_user': count_in(AuditLog, 'user_id'),
            'checkin_pending': count_in(CheckinPending),
            'whatsapp_messages': wa_count
        }

    def obtener_usuarios_con_cuotas_por_vencer(self, dias_anticipacion: int = 3) -> List[Dict[str, Any]]:
        fecha_actual = date.today()
        fecha_limite = fecha_actual + timedelta(days=dias_anticipacion)
        
        # Simplified logic using stored fields
        stmt = select(Usuario).where(
            Usuario.activo == True,
            Usuario.rol == 'socio',
            Usuario.fecha_proximo_vencimiento.between(fecha_actual, fecha_limite)
        )
        
        users = self.db.scalars(stmt).all()
        data = []
        for user in users:
            data.append({
                'id': user.id,
                'nombre': user.nombre,
                'telefono': user.telefono,
                'fecha_vencimiento': user.fecha_proximo_vencimiento.strftime('%d/%m/%Y'),
                'dias_para_vencer': (user.fecha_proximo_vencimiento - fecha_actual).days
            })
        return data

    def obtener_usuarios_morosos(self) -> List[Dict[str, Any]]:
        fecha_actual = date.today()
        stmt = select(Usuario).where(
            Usuario.activo == True,
            Usuario.rol == 'socio',
            Usuario.fecha_proximo_vencimiento < fecha_actual
        )
        
        users = self.db.scalars(stmt).all()
        data = []
        for user in users:
            data.append({
                'id': user.id,
                'nombre': user.nombre,
                'telefono': user.telefono,
                'fecha_vencimiento': user.fecha_proximo_vencimiento.strftime('%d/%m/%Y') if user.fecha_proximo_vencimiento else 'N/A',
                'cuotas_vencidas': user.cuotas_vencidas or 0
            })
        return data

    def registrar_usuarios_batch(self, items: List[Dict[str, Any]], *, skip_duplicates: bool = True, validate_data: bool = True) -> Dict[str, Any]:
        result = {'insertados': [], 'actualizados': [], 'omitidos': []}
        
        for item in items:
            try:
                if validate_data and not item.get('nombre'):
                    result['omitidos'].append(item)
                    continue
                    
                dni = item.get('dni')
                existing = None
                if dni:
                    existing = self.db.scalar(select(Usuario).where(Usuario.dni == dni))
                
                if existing:
                    if skip_duplicates:
                        result['omitidos'].append(item)
                    else:
                        for k, v in item.items():
                            if hasattr(existing, k):
                                setattr(existing, k, v)
                        result['actualizados'].append(existing.id)
                else:
                    new_user = Usuario(**{k: v for k, v in item.items() if hasattr(Usuario, k)})
                    self.db.add(new_user)
                    self.db.flush()
                    
                    if new_user.rol == 'socio':
                         new_user.fecha_proximo_vencimiento = date.today() + timedelta(days=30)
                    if new_user.rol == 'profesor':
                        self._crear_profesor_default(new_user.id)
                        
                    result['insertados'].append(new_user.id)
                    
            except Exception as e:
                self.logger.error(f"Error processing batch item: {e}")
                result['omitidos'].append(item)
                
        self.db.commit()
        self._invalidate_cache('usuarios')
        return result
