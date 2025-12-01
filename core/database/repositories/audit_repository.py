from typing import List, Optional, Dict, Any
from datetime import datetime, date, timedelta
from sqlalchemy import select, insert, update, func, text, desc
from .base import BaseRepository
from ..orm_models import AuditLog, Usuario, SystemDiagnostics, MaintenanceTask

class AuditRepository(BaseRepository):
    
    def registrar_audit_log(self, user_id: int, action: str, table_name: str, record_id: int = None, 
                           old_values: str = None, new_values: str = None, ip_address: str = None, 
                           user_agent: str = None, session_id: str = None):
        if user_id is not None and user_id <= 1:
            user_id = None # System/Owner common handling
            
        log = AuditLog(
            user_id=user_id, action=action, table_name=table_name, record_id=record_id,
            old_values=str(old_values) if old_values else None, 
            new_values=str(new_values) if new_values else None, 
            ip_address=ip_address,
            user_agent=user_agent, session_id=session_id
        )
        self.db.add(log)
        try:
            self.db.commit()
            return log.id
        except Exception as e:
            self.logger.error(f"Error registrando audit log: {e}")
            return None

    def obtener_audit_logs(self, limit: int = 100, offset: int = 0, user_id: int = None, 
                          table_name: str = None, action: str = None, fecha_inicio: str = None, 
                          fecha_fin: str = None) -> List[Dict]:
        stmt = select(AuditLog, Usuario).outerjoin(Usuario).order_by(AuditLog.timestamp.desc())
        
        if user_id: stmt = stmt.where(AuditLog.user_id == user_id)
        if table_name: stmt = stmt.where(AuditLog.table_name == table_name)
        if action: stmt = stmt.where(AuditLog.action == action)
        if fecha_inicio: stmt = stmt.where(AuditLog.timestamp >= fecha_inicio)
        if fecha_fin: stmt = stmt.where(AuditLog.timestamp <= fecha_fin)
        
        stmt = stmt.limit(limit).offset(offset)
        
        results = self.db.execute(stmt).all()
        data = []
        for log, user in results:
            d = {k: getattr(log, k) for k in ['id', 'timestamp', 'user_id', 'action', 'table_name', 
                                            'record_id', 'old_values', 'new_values', 'ip_address', 
                                            'user_agent', 'session_id']}
            d['usuario_nombre'] = user.nombre if user else None
            data.append(d)
        return data

    def obtener_estadisticas_auditoria(self, dias: int = 30) -> Dict:
        fecha_limite = datetime.now() - timedelta(days=dias)
        
        # Acciones por tipo
        stmt_acciones = select(AuditLog.action, func.count(AuditLog.id)).where(AuditLog.timestamp >= fecha_limite).group_by(AuditLog.action).order_by(func.count(AuditLog.id).desc())
        acciones = [{'action': r[0], 'count': r[1]} for r in self.db.execute(stmt_acciones).all()]
        
        # Actividad por usuario
        stmt_users = select(Usuario.nombre, func.count(AuditLog.id)).join(Usuario).where(AuditLog.timestamp >= fecha_limite).group_by(Usuario.nombre, AuditLog.user_id).order_by(func.count(AuditLog.id).desc()).limit(10)
        usuarios = [{'nombre': r[0], 'count': r[1]} for r in self.db.execute(stmt_users).all()]
        
        # Tablas modificadas
        stmt_tables = select(AuditLog.table_name, func.count(AuditLog.id)).where(AuditLog.timestamp >= fecha_limite).group_by(AuditLog.table_name).order_by(func.count(AuditLog.id).desc()).limit(10)
        tablas = [{'table_name': r[0], 'count': r[1]} for r in self.db.execute(stmt_tables).all()]
        
        return {
            'acciones_por_tipo': acciones,
            'actividad_por_usuario': usuarios,
            'tablas_modificadas': tablas,
            'periodo_dias': dias
        }

    # Diagnostics
    def registrar_diagnostico(self, diagnostic_type: str, component: str, status: str, 
                             details: str = None, metrics: str = None) -> int:
        diag = SystemDiagnostics(
            diagnostic_type=diagnostic_type, component=component, status=status,
            details=details, metrics=metrics
        )
        self.db.add(diag)
        self.db.commit()
        return diag.id

    def obtener_diagnosticos(self, limit: int = 50, component: str = None, 
                           status: str = None, diagnostic_type: str = None) -> List[Dict]:
        stmt = select(SystemDiagnostics).order_by(SystemDiagnostics.timestamp.desc()).limit(limit)
        if component: stmt = stmt.where(SystemDiagnostics.component == component)
        if status: stmt = stmt.where(SystemDiagnostics.status == status)
        if diagnostic_type: stmt = stmt.where(SystemDiagnostics.diagnostic_type == diagnostic_type)
        
        return [
            {k: getattr(d, k) for k in ['id', 'timestamp', 'diagnostic_type', 'component', 'status', 'details', 'metrics', 'resolved', 'resolved_at', 'resolved_by']}
            for d in self.db.scalars(stmt).all()
        ]

    def resolver_diagnostico(self, diagnostico_id: int, resolved_by: int) -> bool:
        d = self.db.get(SystemDiagnostics, diagnostico_id)
        if d:
            d.resolved = True
            d.resolved_at = datetime.now()
            d.resolved_by = resolved_by
            self.db.commit()
            return True
        return False
