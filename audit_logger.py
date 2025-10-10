import json
import functools
import logging
from typing import Any, Dict, Optional
from datetime import datetime

class AuditLogger:
    """Sistema de logging automático para operaciones CRUD."""
    
    def __init__(self, db_manager):
        self.db_manager = db_manager
        self.current_user_id = None
        self.session_id = None
        self.ip_address = None
        self.user_agent = None
    
    def set_context(self, user_id: int = None, session_id: str = None, 
                   ip_address: str = None, user_agent: str = None):
        """Establece el contexto de auditoría para las operaciones."""
        self.current_user_id = user_id
        self.session_id = session_id
        self.ip_address = ip_address
        self.user_agent = user_agent
    
    def log_operation(self, action: str, table_name: str, record_id: int = None,
                     old_values: Dict = None, new_values: Dict = None):
        """Registra una operación en el log de auditoría."""
        try:
            # Convertir diccionarios a JSON para almacenamiento
            old_values_json = json.dumps(old_values, default=str) if old_values else None
            new_values_json = json.dumps(new_values, default=str) if new_values else None
            
            self.db_manager.registrar_audit_log(
                user_id=self.current_user_id,
                action=action,
                table_name=table_name,
                record_id=record_id,
                old_values=old_values_json,
                new_values=new_values_json,
                ip_address=self.ip_address,
                user_agent=self.user_agent,
                session_id=self.session_id
            )
        except Exception as e:
            logging.error(f"Error al registrar log de auditoría: {e}")
    
    def audit_create(self, table_name: str):
        """Decorador para operaciones CREATE."""
        def decorator(func):
            @functools.wraps(func)
            def wrapper(*args, **kwargs):
                try:
                    # Ejecutar la operación original
                    result = func(*args, **kwargs)
                    
                    # Obtener el ID del registro creado
                    record_id = result if isinstance(result, int) else None
                    
                    # Extraer valores del objeto creado
                    new_values = {}
                    if args and hasattr(args[1], '__dict__'):
                        # Segundo argumento suele ser el objeto (después de self)
                        obj = args[1]
                        new_values = {k: v for k, v in obj.__dict__.items() 
                                    if not k.startswith('_')}
                    
                    # Registrar en auditoría
                    self.log_operation(
                        action='CREATE',
                        table_name=table_name,
                        record_id=record_id,
                        new_values=new_values
                    )
                    
                    return result
                except Exception as e:
                    logging.error(f"Error en audit_create para {table_name}: {e}")
                    return func(*args, **kwargs)
            return wrapper
        return decorator
    
    def audit_update(self, table_name: str):
        """Decorador para operaciones UPDATE."""
        def decorator(func):
            @functools.wraps(func)
            def wrapper(*args, **kwargs):
                try:
                    # Obtener valores anteriores si es posible
                    old_values = {}
                    record_id = None
                    
                    if args and hasattr(args[1], 'id'):
                        # Segundo argumento suele ser el objeto (después de self)
                        obj = args[1]
                        record_id = obj.id
                        
                        # Intentar obtener valores anteriores de la base de datos
                        try:
                            if table_name == 'usuarios':
                                old_obj = self.db_manager.obtener_usuario(record_id)
                            elif table_name == 'pagos':
                                old_obj = self.db_manager.obtener_pago(record_id)
                            # Agregar más casos según sea necesario
                            else:
                                old_obj = None
                            
                            if old_obj and hasattr(old_obj, '__dict__'):
                                old_values = {k: v for k, v in old_obj.__dict__.items() 
                                            if not k.startswith('_')}
                        except:
                            pass
                    
                    # Ejecutar la operación original
                    result = func(*args, **kwargs)
                    
                    # Obtener nuevos valores
                    new_values = {}
                    if args and hasattr(args[1], '__dict__'):
                        obj = args[1]
                        new_values = {k: v for k, v in obj.__dict__.items() 
                                    if not k.startswith('_')}
                    
                    # Registrar en auditoría
                    self.log_operation(
                        action='UPDATE',
                        table_name=table_name,
                        record_id=record_id,
                        old_values=old_values,
                        new_values=new_values
                    )
                    
                    return result
                except Exception as e:
                    logging.error(f"Error en audit_update para {table_name}: {e}")
                    return func(*args, **kwargs)
            return wrapper
        return decorator
    
    def audit_delete(self, table_name: str):
        """Decorador para operaciones DELETE."""
        def decorator(func):
            @functools.wraps(func)
            def wrapper(*args, **kwargs):
                try:
                    # Obtener valores anteriores antes de eliminar
                    old_values = {}
                    record_id = None
                    
                    if len(args) > 1:
                        # Segundo argumento suele ser el ID del registro
                        record_id = args[1]
                        
                        # Intentar obtener valores anteriores de la base de datos
                        try:
                            if table_name == 'usuarios':
                                old_obj = self.db_manager.obtener_usuario(record_id)
                            elif table_name == 'pagos':
                                old_obj = self.db_manager.obtener_pago(record_id)
                            # Agregar más casos según sea necesario
                            else:
                                old_obj = None
                            
                            if old_obj and hasattr(old_obj, '__dict__'):
                                old_values = {k: v for k, v in old_obj.__dict__.items() 
                                            if not k.startswith('_')}
                        except:
                            pass
                    
                    # Ejecutar la operación original
                    result = func(*args, **kwargs)
                    
                    # Registrar en auditoría
                    self.log_operation(
                        action='DELETE',
                        table_name=table_name,
                        record_id=record_id,
                        old_values=old_values
                    )
                    
                    return result
                except Exception as e:
                    logging.error(f"Error en audit_delete para {table_name}: {e}")
                    return func(*args, **kwargs)
            return wrapper
        return decorator
    
    def audit_read(self, table_name: str, sensitive: bool = False):
        """Decorador para operaciones READ (solo para datos sensibles)."""
        def decorator(func):
            @functools.wraps(func)
            def wrapper(*args, **kwargs):
                try:
                    # Ejecutar la operación original
                    result = func(*args, **kwargs)
                    
                    # Solo registrar lecturas de datos sensibles
                    if sensitive:
                        record_id = None
                        if len(args) > 1:
                            record_id = args[1]
                        
                        self.log_operation(
                            action='READ',
                            table_name=table_name,
                            record_id=record_id
                        )
                    
                    return result
                except Exception as e:
                    logging.error(f"Error en audit_read para {table_name}: {e}")
                    return func(*args, **kwargs)
            return wrapper
        return decorator

# Instancia global del logger de auditoría
_audit_logger = None

def get_audit_logger(db_manager=None):
    """Obtiene la instancia global del logger de auditoría."""
    global _audit_logger
    if _audit_logger is None and db_manager:
        _audit_logger = AuditLogger(db_manager)
    return _audit_logger

def set_audit_context(user_id: int = None, session_id: str = None, 
                     ip_address: str = None, user_agent: str = None):
    """Establece el contexto de auditoría globalmente."""
    global _audit_logger
    if _audit_logger:
        _audit_logger.set_context(user_id, session_id, ip_address, user_agent)