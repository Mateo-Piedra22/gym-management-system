from typing import List, Optional, Dict, Any, Tuple
from datetime import datetime, date, timedelta
from sqlalchemy import select, update, delete, func, text, desc
from sqlalchemy.orm import Session
from .base import BaseRepository
from ..orm_models import Pago, TipoCuota, MetodoPago, ConceptoPago, Usuario, Configuracion

class PaymentRepository(BaseRepository):
    
    def obtener_todos_pagos(self) -> List[Dict]:
        stmt = select(Pago).order_by(Pago.fecha_pago.desc(), Pago.año.desc(), Pago.mes.desc())
        pagos = self.db.scalars(stmt).all()
        return [
            {'id': p.id, 'usuario_id': p.usuario_id, 'monto': float(p.monto), 
             'mes': p.mes, 'año': p.año, 'fecha_pago': p.fecha_pago, 'metodo_pago_id': p.metodo_pago_id}
            for p in pagos
        ]

    def obtener_metodos_pago(self, solo_activos: bool = True) -> List[Dict]:
        stmt = select(MetodoPago).order_by(MetodoPago.nombre)
        if solo_activos:
            stmt = stmt.where(MetodoPago.activo == True)
        
        return [
            {'id': m.id, 'nombre': m.nombre, 'icono': m.icono, 'color': m.color, 
             'comision': float(m.comision) if m.comision else 0.0, 'activo': m.activo, 
             'fecha_creacion': m.fecha_creacion, 'descripcion': m.descripcion}
            for m in self.db.scalars(stmt).all()
        ]

    def obtener_conceptos_pago(self, solo_activos: bool = True) -> List[Dict]:
        stmt = select(ConceptoPago).order_by(ConceptoPago.nombre)
        if solo_activos:
            stmt = stmt.where(ConceptoPago.activo == True)
            
        return [
            {'id': c.id, 'nombre': c.nombre, 'descripcion': c.descripcion, 
             'precio_base': float(c.precio_base) if c.precio_base else 0.0, 
             'tipo': c.tipo, 'activo': c.activo, 'fecha_creacion': c.fecha_creacion}
            for c in self.db.scalars(stmt).all()
        ]

    def registrar_pagos_batch(self, pagos_items: List[Dict[str, Any]], skip_duplicates: bool = False, validate_data: bool = True, auto_crear_metodos_pago: bool = False) -> Dict[str, Any]:
        result = {'insertados': [], 'actualizados': [], 'omitidos': [], 'count': 0}
        
        # Cache local de metodos
        metodos_cache = {m.nombre.lower(): m for m in self.db.scalars(select(MetodoPago)).all()}
        
        for item in pagos_items:
            try:
                uid = int(item.get('usuario_id'))
                monto = float(item.get('monto'))
                mes = int(item.get('mes') or item.get('mes_pagado') or datetime.now().month)
                año = int(item.get('año') or item.get('año_pagado') or datetime.now().year)
                
                # Resolver fecha
                fecha_pago = item.get('fecha_pago') or datetime.now()
                if isinstance(fecha_pago, str):
                    try:
                        fecha_pago = datetime.fromisoformat(fecha_pago)
                    except:
                        fecha_pago = datetime.now()
                
                # Resolver metodo
                metodo_id = item.get('metodo_pago_id')
                nombre_metodo = item.get('metodo_pago')
                
                if not metodo_id and nombre_metodo:
                    key = str(nombre_metodo).strip().lower()
                    if key in metodos_cache:
                        metodo_id = metodos_cache[key].id
                    elif auto_crear_metodos_pago:
                        # Crear
                        new_m = MetodoPago(nombre=nombre_metodo, icono='💳', color='#9b59b6', comision=0.0, activo=True)
                        self.db.add(new_m)
                        self.db.flush()
                        metodos_cache[key] = new_m
                        metodo_id = new_m.id

                # Check duplicado
                existing = self.db.scalar(
                    select(Pago).where(Pago.usuario_id == uid, Pago.mes == mes, Pago.año == año)
                )
                
                if existing:
                    if skip_duplicates:
                        result['omitidos'].append(item)
                    else:
                        existing.monto = monto
                        existing.fecha_pago = fecha_pago
                        existing.metodo_pago_id = metodo_id
                        result['actualizados'].append((uid, mes, año))
                else:
                    new_pago = Pago(
                        usuario_id=uid,
                        monto=monto,
                        mes=mes,
                        año=año,
                        fecha_pago=fecha_pago,
                        metodo_pago_id=metodo_id
                    )
                    self.db.add(new_pago)
                    self.db.flush()
                    result['insertados'].append(new_pago.id)
                    
                    # Actualizar usuario vencimiento
                    self.actualizar_fecha_proximo_vencimiento(uid, fecha_pago.date() if isinstance(fecha_pago, datetime) else fecha_pago)

            except Exception as e:
                result['omitidos'].append(item)
                self.logger.error(f"Error batch pago: {e}")

        self.db.commit()
        result['count'] = len(result['insertados']) + len(result['actualizados'])
        self._invalidate_cache('pagos')
        return result

    def actualizar_fecha_proximo_vencimiento(self, usuario_id: int, fecha_pago: date = None) -> bool:
        if fecha_pago is None:
            fecha_pago = date.today()
            
        user = self.db.get(Usuario, usuario_id)
        if not user:
            return False
            
        # Obtener duracion
        duracion = 30
        if user.tipo_cuota:
            tc = self.db.scalar(select(TipoCuota).where(TipoCuota.nombre == user.tipo_cuota))
            if tc:
                duracion = tc.duracion_dias or 30
        
        user.fecha_proximo_vencimiento = fecha_pago + timedelta(days=duracion)
        user.cuotas_vencidas = 0
        user.ultimo_pago = fecha_pago
        
        self.db.commit()
        self._invalidate_cache('usuarios', usuario_id)
        return True

    def incrementar_cuotas_vencidas(self, usuario_id: int) -> bool:
        user = self.db.get(Usuario, usuario_id)
        if not user:
            return False
            
        if user.rol in ('profesor', 'dueño', 'owner'):
            return True
            
        user.cuotas_vencidas = (user.cuotas_vencidas or 0) + 1
        self.db.commit()
        self._invalidate_cache('usuarios', usuario_id)
        return True

    def obtener_precio_cuota(self, tipo_cuota: str) -> float:
        tc = self.db.scalar(select(TipoCuota).where(TipoCuota.nombre == tipo_cuota))
        if tc:
            return float(tc.precio)
        
        # Fallback
        tc = self.db.scalar(select(TipoCuota).where(TipoCuota.activo == True))
        return float(tc.precio) if tc else 5000.0

    def actualizar_precio_cuota(self, tipo_cuota: str, nuevo_precio: float):
        tc = self.db.scalar(select(TipoCuota).where(TipoCuota.nombre == tipo_cuota))
        if tc:
            tc.precio = nuevo_precio
            self.db.commit()

    def obtener_tipo_cuota_por_nombre(self, nombre: str) -> Optional[TipoCuota]:
        return self.db.scalar(select(TipoCuota).where(TipoCuota.nombre == nombre))

    def obtener_tipo_cuota_por_id(self, tipo_id: int) -> Optional[TipoCuota]:
        return self.db.get(TipoCuota, tipo_id)

    def actualizar_tipo_cuota(self, tipo_cuota: TipoCuota) -> bool:
        # Assumes tipo_cuota is attached or we fetch it
        existing = self.db.get(TipoCuota, tipo_cuota.id)
        if existing:
            existing.nombre = tipo_cuota.nombre
            existing.precio = tipo_cuota.precio
            existing.descripcion = tipo_cuota.descripcion
            existing.duracion_dias = tipo_cuota.duracion_dias
            existing.activo = tipo_cuota.activo
            existing.icono_path = tipo_cuota.icono_path
            existing.fecha_modificacion = datetime.now()
            self.db.commit()
            return True
        return False

    def obtener_pago(self, pago_id: int) -> Optional[Pago]:
        return self.db.get(Pago, pago_id)

    def obtener_pagos_mes(self, mes: int, año: int) -> List[Pago]:
        stmt = select(Pago).where(
            func.extract('month', Pago.fecha_pago) == mes,
            func.extract('year', Pago.fecha_pago) == año
        ).order_by(Pago.fecha_pago.desc())
        return list(self.db.scalars(stmt).all())

    def eliminar_pago(self, pago_id: int):
        pago = self.db.get(Pago, pago_id)
        if pago:
            self.db.delete(pago)
            self.db.commit()
            self._invalidate_cache('pagos')

    def modificar_pago(self, pago: Pago):
        existing = self.db.get(Pago, pago.id)
        if existing:
            existing.usuario_id = pago.usuario_id
            existing.monto = pago.monto
            existing.fecha_pago = pago.fecha_pago
            existing.metodo_pago_id = pago.metodo_pago_id
            self.db.commit()
            self._invalidate_cache('pagos')

    def verificar_pago_existe(self, usuario_id: int, mes: int, año: int) -> bool:
        stmt = select(func.count(Pago.id)).where(
            Pago.usuario_id == usuario_id,
            func.extract('month', Pago.fecha_pago) == mes,
            func.extract('year', Pago.fecha_pago) == año
        )
        return (self.db.scalar(stmt) or 0) > 0

    def obtener_estadisticas_pagos(self, año: int = None) -> dict:
        if año is None:
            año = datetime.now().year
            
        stmt = select(
            func.count(Pago.id),
            func.sum(Pago.monto),
            func.avg(Pago.monto),
            func.min(Pago.monto),
            func.max(Pago.monto)
        ).where(func.extract('year', Pago.fecha_pago) == año)
        
        row = self.db.execute(stmt).first()
        
        stats = {
            'año': año,
            'total_pagos': row[0] or 0,
            'total_recaudado': float(row[1] or 0),
            'promedio_pago': float(row[2] or 0),
            'pago_minimo': float(row[3] or 0),
            'pago_maximo': float(row[4] or 0),
            'por_mes': {}
        }
        
        stmt_mes = select(
            func.extract('month', Pago.fecha_pago).label('mes'),
            func.count(Pago.id),
            func.sum(Pago.monto)
        ).where(
            func.extract('year', Pago.fecha_pago) == año
        ).group_by('mes').order_by('mes')
        
        for r in self.db.execute(stmt_mes).all():
            stats['por_mes'][int(r[0])] = {'cantidad': r[1], 'total': float(r[2])}
            
        return stats

    def obtener_tipos_cuota_activos(self) -> List[TipoCuota]:
        stmt = select(TipoCuota).where(TipoCuota.activo == True).order_by(TipoCuota.nombre)
        return list(self.db.scalars(stmt).all())
