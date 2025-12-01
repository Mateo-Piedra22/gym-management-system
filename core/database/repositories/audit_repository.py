from datetime import datetime, date, timedelta
from typing import List, Optional, Dict, Set, Tuple, Any
import json
import logging
import psycopg2
import psycopg2.extras
from .base import BaseRepository
from ..connection import database_retry

class AuditRepository(BaseRepository):
    pass

    # --- Methods moved from DatabaseManager ---

    def registrar_audit_log(self, user_id: int, action: str, table_name: str, record_id: int = None, 
                           old_values: str = None, new_values: str = None, ip_address: str = None, 
                           user_agent: str = None, session_id: str = None):
        """Registra una entrada en el log de auditoría PostgreSQL"""
        try:
            # Si user_id es 0, 1 (IDs comunes de sistema/dueño) o None, usar NULL
            valid_user_id = user_id
            if user_id is None or user_id <= 1: 
                valid_user_id = None

            with self.get_connection_context() as conn:
                cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
                sql = """
                    INSERT INTO audit_logs 
                    (user_id, action, table_name, record_id, old_values, new_values, ip_address, user_agent, session_id)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    RETURNING id
                """
                cursor.execute(sql, (valid_user_id, action, table_name, record_id, old_values, new_values,
                                   ip_address, user_agent, session_id))
                conn.commit()
                return cursor.fetchone()['id']
        except Exception as e:
            # Log el error pero no interrumpir la operación principal
            logging.error(f"Error registrando audit log: {e}")
            return None
    

    def obtener_audit_logs(self, limit: int = 100, offset: int = 0, user_id: int = None, 
                          table_name: str = None, action: str = None, fecha_inicio: str = None, 
                          fecha_fin: str = None) -> List[Dict]:
        """Obtiene logs de auditoría con filtros opcionales PostgreSQL"""
        with self.get_connection_context() as conn:
            cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            
            sql = """
                SELECT al.*, u.nombre as usuario_nombre 
                FROM audit_logs al
                LEFT JOIN usuarios u ON al.user_id = u.id
            """
            
            filters = {
                "al.user_id": user_id,
                "al.table_name": table_name,
                "al.action": action,
                "al.timestamp >=": fecha_inicio,
                "al.timestamp <=": fecha_fin
            }
            
            where_clauses = []
            params = []
            
            for key, value in filters.items():
                if value is not None:
                    where_clauses.append(f"{key} %s")
                    params.append(value)
            
            if where_clauses:
                sql += " WHERE " + " AND ".join(where_clauses)
            
            sql += " ORDER BY al.timestamp DESC LIMIT %s OFFSET %s"
            params.extend([limit, offset])
            
            cursor.execute(sql, params)
            return [dict(row) for row in cursor.fetchall()]
    

    def obtener_estadisticas_auditoria(self, dias: int = 30) -> Dict:
        """Obtiene estadísticas de auditoría de los últimos días PostgreSQL"""
        with self.get_connection_context() as conn:
            cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            
            # Total de acciones por tipo
            cursor.execute("""
                SELECT action, COUNT(*) as count
                FROM audit_logs 
                WHERE timestamp >= CURRENT_TIMESTAMP - INTERVAL '%s days'
                GROUP BY action
                ORDER BY count DESC
            """, (dias,))
            acciones_por_tipo = [dict(zip([desc[0] for desc in cursor.description], row)) for row in cursor.fetchall()]
            
            # Actividad por usuario
            cursor.execute("""
                SELECT u.nombre, COUNT(*) as count
                FROM audit_logs al
                JOIN usuarios u ON al.user_id = u.id
                WHERE al.timestamp >= CURRENT_TIMESTAMP - INTERVAL '%s days'
                GROUP BY al.user_id, u.nombre
                ORDER BY count DESC
                LIMIT 10
            """, (dias,))
            actividad_por_usuario = [dict(zip([desc[0] for desc in cursor.description], row)) for row in cursor.fetchall()]
            
            # Tablas más modificadas
            cursor.execute("""
                SELECT table_name, COUNT(*) as count
                FROM audit_logs 
                WHERE timestamp >= CURRENT_TIMESTAMP - INTERVAL '%s days'
                GROUP BY table_name
                ORDER BY count DESC
                LIMIT 10
            """, (dias,))
            tablas_modificadas = [dict(zip([desc[0] for desc in cursor.description], row)) for row in cursor.fetchall()]
            
            return {
                'acciones_por_tipo': acciones_por_tipo,
                'actividad_por_usuario': actividad_por_usuario,
                'tablas_modificadas': tablas_modificadas,
                'periodo_dias': dias
            }
    
    # === SYSTEM DIAGNOSTICS METHODS ===
    

    def registrar_diagnostico(self, diagnostic_type: str, component: str, status: str, 
                             details: str = None, metrics: str = None) -> int:
        """Registra un diagnóstico del sistema PostgreSQL"""
        with self.get_connection_context() as conn:
            cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            sql = """
                INSERT INTO system_diagnostics 
                (diagnostic_type, component, status, details, metrics)
                VALUES (%s, %s, %s, %s, %s)
                RETURNING id
            """
            cursor.execute(sql, (diagnostic_type, component, status, details, metrics))
            conn.commit()
            # Manejo seguro de resultado
            result = cursor.fetchone()
            if result and len(result) > 0:
                return result[0]
            raise Exception("No se pudo obtener el ID del diagnóstico registrado")
    

    def obtener_diagnosticos(self, limit: int = 50, component: str = None, 
                           status: str = None, diagnostic_type: str = None) -> List[Dict]:
        """Obtiene diagnósticos del sistema con filtros opcionales PostgreSQL"""
        with self.get_connection_context() as conn:
            cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            
            sql = "SELECT * FROM system_diagnostics WHERE 1=1"
            params = []
            
            if component:
                sql += " AND component = %s"
                params.append(component)
            
            if status:
                sql += " AND status = %s"
                params.append(status)
            
            if diagnostic_type:
                sql += " AND diagnostic_type = %s"
                params.append(diagnostic_type)
            
            sql += " ORDER BY timestamp DESC LIMIT %s"
            params.append(limit)
            
            cursor.execute(sql, params)
            return [dict(zip([desc[0] for desc in cursor.description], row)) for row in cursor.fetchall()]
    

    def resolver_diagnostico(self, diagnostico_id: int, resolved_by: int) -> bool:
        """Marca un diagnóstico como resuelto PostgreSQL"""
        with self.get_connection_context() as conn:
            cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
            sql = """
                UPDATE system_diagnostics 
                SET resolved = true, resolved_at = CURRENT_TIMESTAMP, resolved_by = %s
                WHERE id = %s
            """
            cursor.execute(sql, (resolved_by, diagnostico_id))
            conn.commit()
            return cursor.rowcount > 0
    
    # === MAINTENANCE TASKS METHODS ===
    

    def _migrar_sistema_auditoria(self, cursor):
        """Migración para agregar las tablas del sistema de auditoría y herramientas administrativas avanzadas PostgreSQL"""
        try:
            # Tabla de logs de auditoría
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS audit_logs (
                    id SERIAL PRIMARY KEY,
                    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    user_id INTEGER,
                    action TEXT NOT NULL,
                    table_name TEXT NOT NULL,
                    record_id INTEGER,
                    old_values TEXT,
                    new_values TEXT,
                    ip_address TEXT,
                    user_agent TEXT,
                    session_id TEXT,
                    FOREIGN KEY (user_id) REFERENCES usuarios(id)
                )
            """)
            # Índices para auditoría (sin campos lógicos)
            
            # Tabla de diagnósticos del sistema
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS system_diagnostics (
                    id SERIAL PRIMARY KEY,
                    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    diagnostic_type TEXT NOT NULL,
                    component TEXT NOT NULL,
                    status TEXT NOT NULL,
                    details TEXT,
                    metrics TEXT,
                    resolved BOOLEAN DEFAULT FALSE,
                    resolved_at TIMESTAMP,
                    resolved_by INTEGER,
                    FOREIGN KEY (resolved_by) REFERENCES usuarios(id)
                )
            """)
            
            # Tabla de tareas de mantenimiento
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS maintenance_tasks (
                    id SERIAL PRIMARY KEY,
                    task_name TEXT NOT NULL,
                    task_type TEXT NOT NULL,
                    description TEXT,
                    scheduled_at TIMESTAMP,
                    executed_at TIMESTAMP,
                    status TEXT DEFAULT 'pending',
                    result TEXT,
                    error_message TEXT,
                    created_by INTEGER,
                    executed_by INTEGER,
                    auto_schedule BOOLEAN DEFAULT FALSE,
                    frequency_days INTEGER,
                    next_execution TIMESTAMP,
                    FOREIGN KEY (created_by) REFERENCES usuarios(id),
                    FOREIGN KEY (executed_by) REFERENCES usuarios(id)
                )
            """)
            
            # Índices para optimizar consultas
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_audit_logs_timestamp ON audit_logs(timestamp)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_audit_logs_user_id ON audit_logs(user_id)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_audit_logs_table_name ON audit_logs(table_name)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_audit_logs_action ON audit_logs(action)")
            
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_system_diagnostics_timestamp ON system_diagnostics(timestamp)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_system_diagnostics_type ON system_diagnostics(diagnostic_type)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_system_diagnostics_status ON system_diagnostics(status)")
            
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_maintenance_tasks_status ON maintenance_tasks(status)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_maintenance_tasks_scheduled ON maintenance_tasks(scheduled_at)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_maintenance_tasks_next_execution ON maintenance_tasks(next_execution)")
            
            logging.info("Tablas del sistema de auditoría creadas exitosamente")
            
        except Exception as e:
            logging.error(f"Error en migración del sistema de auditoría: {e}")
            # En caso de error, no interrumpir la inicialización

