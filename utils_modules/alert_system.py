# -*- coding: utf-8 -*-
"""
Sistema de Alertas de Mantenimiento
Proporciona un sistema centralizado de alertas para el mantenimiento del sistema.
"""

import logging
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Callable
from enum import Enum
from PyQt6.QtCore import QObject, pyqtSignal, QTimer
from PyQt6.QtWidgets import QMessageBox, QSystemTrayIcon


class AlertLevel(Enum):
    """Niveles de alerta del sistema"""
    INFO = "info"
    WARNING = "warning"
    CRITICAL = "critical"
    ERROR = "error"


class AlertCategory(Enum):
    """Categorías de alertas"""
    SYSTEM = "system"
    DATABASE = "database"
    MAINTENANCE = "maintenance"
    SECURITY = "security"
    PERFORMANCE = "performance"
    BACKUP = "backup"
    MEMBERSHIP = "membership"  # Nueva categoría para alertas de membresías
    PAYMENT = "payment"        # Nueva categoría para alertas de pagos


class Alert:
    """Clase para representar una alerta del sistema"""
    
    def __init__(self, level: AlertLevel, category: AlertCategory, 
                 title: str, message: str, source: str = None, user_id: Optional[int] = None):
        self.id = f"{datetime.now().strftime('%Y%m%d_%H%M%S')}_{hash(message) % 10000}"
        self.level = level
        self.category = category
        self.title = title
        self.message = message
        self.source = source or "Sistema"
        # Timestamp consciente de zona horaria
        self.timestamp = datetime.now().astimezone()
        # Usuario responsable (si está disponible por contexto)
        self.user_id = user_id
        self.acknowledged = False
        self.resolved = False
    
    def to_dict(self) -> Dict:
        """Convierte la alerta a diccionario"""
        return {
            'id': self.id,
            'level': self.level.value,
            'category': self.category.value,
            'title': self.title,
            'message': self.message,
            'source': self.source,
            'timestamp': self.timestamp.isoformat(),
            'user_id': self.user_id,
            'acknowledged': self.acknowledged,
            'resolved': self.resolved
        }
    
    def __str__(self) -> str:
        return f"[{self.timestamp.strftime('%H:%M:%S')}] {self.level.value.upper()}: {self.title} - {self.message}"


class AlertRule:
    """Regla para generar alertas automáticas"""
    
    def __init__(self, name: str, condition: Callable, level: AlertLevel, 
                 category: AlertCategory, title: str, message_template: str,
                 cooldown_minutes: int = 5):
        self.name = name
        self.condition = condition
        self.level = level
        self.category = category
        self.title = title
        self.message_template = message_template
        self.cooldown_minutes = cooldown_minutes
        self.last_triggered = None
    
    def can_trigger(self) -> bool:
        """Verifica si la regla puede dispararse (respeta cooldown)"""
        if self.last_triggered is None:
            return True
        
        time_since_last = datetime.now() - self.last_triggered
        return time_since_last >= timedelta(minutes=self.cooldown_minutes)
    
    def check_and_trigger(self, data: Dict) -> Optional[Alert]:
        """Verifica la condición y genera alerta si es necesario"""
        if not self.can_trigger():
            return None
        
        try:
            if self.condition(data):
                self.last_triggered = datetime.now()
                message = self.message_template.format(**data)
                return Alert(
                    level=self.level,
                    category=self.category,
                    title=self.title,
                    message=message,
                    source=self.name
                )
        except Exception as e:
            logging.error(f"Error evaluando regla de alerta {self.name}: {e}")
        
        return None


class AlertManager(QObject):
    """Gestor centralizado de alertas del sistema"""
    
    # Señales
    alert_generated = pyqtSignal(object)  # Alert
    alert_acknowledged = pyqtSignal(str)  # alert_id
    alert_resolved = pyqtSignal(str)  # alert_id
    
    def __init__(self):
        super().__init__()
        self.alerts: List[Alert] = []
        self.rules: List[AlertRule] = []
        self.handlers: Dict[AlertLevel, List[Callable]] = {
            AlertLevel.INFO: [],
            AlertLevel.WARNING: [],
            AlertLevel.CRITICAL: [],
            AlertLevel.ERROR: []
        }
        self.max_alerts = 1000  # Máximo número de alertas a mantener
        self.auto_check_timer = QTimer()
        self.auto_check_timer.timeout.connect(self._auto_check_rules)
        self.monitoring_data = {}
        
        # Configurar logging
        self.logger = logging.getLogger('AlertSystem')
        # Auditoría (lazy import)
        try:
            from audit_logger import get_audit_logger
            self.audit_logger = get_audit_logger()
        except Exception:
            self.audit_logger = None
        
        # Configurar reglas por defecto
        self._setup_default_rules()
    
    def _setup_default_rules(self):
        """Configura las reglas de alerta por defecto"""
        # Regla de CPU alta
        self.add_rule(AlertRule(
            name="cpu_high",
            condition=lambda data: data.get('cpu_percent', 0) > 80,
            level=AlertLevel.WARNING,
            category=AlertCategory.PERFORMANCE,
            title="Uso de CPU Alto",
            message_template="El uso de CPU es del {cpu_percent:.1f}%",
            cooldown_minutes=5
        ))
        
        # Regla de memoria alta
        self.add_rule(AlertRule(
            name="memory_high",
            condition=lambda data: data.get('memory_percent', 0) > 85,
            level=AlertLevel.WARNING,
            category=AlertCategory.PERFORMANCE,
            title="Uso de Memoria Alto",
            message_template="El uso de memoria es del {memory_percent:.1f}%",
            cooldown_minutes=5
        ))
        
        # Regla de disco lleno
        self.add_rule(AlertRule(
            name="disk_full",
            condition=lambda data: data.get('disk_percent', 0) > 90,
            level=AlertLevel.CRITICAL,
            category=AlertCategory.SYSTEM,
            title="Disco Casi Lleno",
            message_template="El disco está al {disk_percent:.1f}% de capacidad",
            cooldown_minutes=10
        ))
        
        # Regla de base de datos grande
        self.add_rule(AlertRule(
            name="database_large",
            condition=lambda data: data.get('db_size_mb', 0) > 500,
            level=AlertLevel.INFO,
            category=AlertCategory.DATABASE,
            title="Base de Datos Grande",
            message_template="La base de datos tiene {db_size_mb:.1f} MB",
            cooldown_minutes=60
        ))
        
        # Regla de backup pendiente
        self.add_rule(AlertRule(
            name="backup_overdue",
            condition=lambda data: data.get('days_since_backup', 0) > 7,
            level=AlertLevel.WARNING,
            category=AlertCategory.BACKUP,
            title="Backup Pendiente",
            message_template="Han pasado {days_since_backup} días desde el último backup",
            cooldown_minutes=1440  # 24 horas
        ))
    
    def add_rule(self, rule: AlertRule):
        """Agrega una nueva regla de alerta"""
        self.rules.append(rule)
        self.logger.info(f"Regla de alerta agregada: {rule.name}")
    
    def remove_rule(self, rule_name: str):
        """Elimina una regla de alerta"""
        self.rules = [r for r in self.rules if r.name != rule_name]
        self.logger.info(f"Regla de alerta eliminada: {rule_name}")
    
    def add_handler(self, level: AlertLevel, handler: Callable):
        """Agrega un manejador para un nivel de alerta específico"""
        self.handlers[level].append(handler)
    
    def generate_alert(self, level: AlertLevel, category: AlertCategory, 
                      title: str, message: str, source: str = None) -> Alert:
        """Genera una nueva alerta"""
        # Intentar adjuntar user_id desde contexto de auditoría
        user_id = None
        try:
            if self.audit_logger and getattr(self.audit_logger, 'current_user_id', None):
                user_id = self.audit_logger.current_user_id
        except Exception:
            user_id = None

        alert = Alert(level, category, title, message, source, user_id=user_id)
        self.alerts.append(alert)
        
        # Mantener solo las últimas alertas
        if len(self.alerts) > self.max_alerts:
            self.alerts = self.alerts[-self.max_alerts:]
        
        # Emitir señal
        self.alert_generated.emit(alert)
        
        # Ejecutar manejadores
        for handler in self.handlers[level]:
            try:
                handler(alert)
            except Exception as e:
                logging.error(f"Error en manejador de alerta: {str(e)}")
        
        # Log de la alerta
        self.logger.log(
            self._get_log_level(level),
            f"Alerta generada: {alert}"
        )

        # Registrar en auditoría
        try:
            if self.audit_logger:
                self.audit_logger.log_operation(
                    action='ALERT',
                    table_name='alerts',
                    record_id=None,
                    old_values=None,
                    new_values={
                        'id': alert.id,
                        'level': alert.level.value,
                        'category': alert.category.value,
                        'title': alert.title,
                        'message': alert.message,
                        'source': alert.source,
                        'timestamp': alert.timestamp.isoformat(),
                        'user_id': alert.user_id
                    }
                )
        except Exception as e:
            logging.error(f"Error registrando auditoría de alerta: {e}")
        
        return alert
    
    def acknowledge_alert(self, alert_id: str):
        """Marca una alerta como reconocida"""
        for alert in self.alerts:
            if alert.id == alert_id:
                alert.acknowledged = True
                self.alert_acknowledged.emit(alert_id)
                self.logger.info(f"Alerta reconocida: {alert_id}")
                break
    
    def resolve_alert(self, alert_id: str):
        """Marca una alerta como resuelta"""
        for alert in self.alerts:
            if alert.id == alert_id:
                alert.resolved = True
                self.alert_resolved.emit(alert_id)
                self.logger.info(f"Alerta resuelta: {alert_id}")
                break
    
    def get_alerts(self, level: AlertLevel = None, category: AlertCategory = None,
                  unresolved_only: bool = False) -> List[Alert]:
        """Obtiene alertas filtradas"""
        filtered_alerts = self.alerts
        
        if level:
            filtered_alerts = [a for a in filtered_alerts if a.level == level]
        
        if category:
            filtered_alerts = [a for a in filtered_alerts if a.category == category]
        
        if unresolved_only:
            filtered_alerts = [a for a in filtered_alerts if not a.resolved]
        
        return sorted(filtered_alerts, key=lambda x: x.timestamp, reverse=True)
    
    def get_alert_counts(self) -> Dict[str, int]:
        """Obtiene conteos de alertas por nivel"""
        counts = {level.value: 0 for level in AlertLevel}
        unresolved_count = 0
        
        for alert in self.alerts:
            counts[alert.level.value] += 1
            if not alert.resolved:
                unresolved_count += 1
        
        counts['unresolved'] = unresolved_count
        return counts
    
    def update_monitoring_data(self, data: Dict):
        """Actualiza los datos de monitoreo y verifica reglas"""
        self.monitoring_data.update(data)
        self._check_rules()
    
    def start_auto_monitoring(self, interval_seconds: int = 30):
        """Inicia el monitoreo automático"""
        self.auto_check_timer.start(interval_seconds * 1000)
        self.logger.info(f"Monitoreo automático iniciado (intervalo: {interval_seconds}s)")
    
    def stop_auto_monitoring(self):
        """Detiene el monitoreo automático"""
        self.auto_check_timer.stop()
        self.logger.info("Monitoreo automático detenido")
    
    def _auto_check_rules(self):
        """Verifica automáticamente las reglas con los datos actuales"""
        if self.monitoring_data:
            self._check_rules()
    
    def _check_rules(self):
        """Verifica todas las reglas con los datos actuales"""
        for rule in self.rules:
            alert = rule.check_and_trigger(self.monitoring_data)
            if alert:
                self.alerts.append(alert)
                self.alert_generated.emit(alert)
                
                # Ejecutar manejadores
                for handler in self.handlers[alert.level]:
                    try:
                        handler(alert)
                    except Exception as e:
                        logging.error(f"Error en manejador de alerta: {str(e)}")
    
    def _get_log_level(self, alert_level: AlertLevel) -> int:
        """Convierte nivel de alerta a nivel de logging"""
        mapping = {
            AlertLevel.INFO: logging.INFO,
            AlertLevel.WARNING: logging.WARNING,
            AlertLevel.CRITICAL: logging.CRITICAL,
            AlertLevel.ERROR: logging.ERROR
        }
        return mapping.get(alert_level, logging.INFO)
    
    def clear_old_alerts(self, days: int = 30):
        """Elimina alertas antiguas"""
        cutoff_date = datetime.now() - timedelta(days=days)
        initial_count = len(self.alerts)
        self.alerts = [a for a in self.alerts if a.timestamp > cutoff_date]
        removed_count = initial_count - len(self.alerts)
        
        if removed_count > 0:
            self.logger.info(f"Eliminadas {removed_count} alertas antiguas")
    
    def export_alerts(self, filename: str, format: str = 'json'):
        """Exporta alertas a archivo"""
        import json
        
        try:
            alerts_data = [alert.to_dict() for alert in self.alerts]
            
            if format.lower() == 'json':
                with open(filename, 'w', encoding='utf-8') as f:
                    json.dump(alerts_data, f, indent=2, ensure_ascii=False)
            
            self.logger.info(f"Alertas exportadas a {filename}")
            
        except Exception as e:
            logging.error(f"Error exportando alertas: {str(e)}")
            raise
    
    def generate_membership_expiration_alert(self, user_data: dict, days_until_expiration: int) -> Alert:
        """Genera alerta específica para vencimiento de cuota de membresía"""
        if days_until_expiration <= 0:
            level = AlertLevel.CRITICAL
            title = "Cuota de Membresía Vencida"
            message = f"La cuota de {user_data['nombre']} (DNI: {user_data['dni']}) ha vencido hace {abs(days_until_expiration)} días"
        elif days_until_expiration <= 3:
            level = AlertLevel.WARNING
            title = "Cuota Próxima a Vencer"
            message = f"La cuota de {user_data['nombre']} (DNI: {user_data['dni']}) vence en {days_until_expiration} días"
        else:
            level = AlertLevel.INFO
            title = "Recordatorio de Vencimiento"
            message = f"La cuota de {user_data['nombre']} (DNI: {user_data['dni']}) vence en {days_until_expiration} días"
        
        return self.generate_alert(
            level=level,
            category=AlertCategory.PAYMENT,
            title=title,
            message=message,
            source="Sistema de Membresías"
        )
    
    def generate_status_expiration_alert(self, user_data: dict, status_name: str, days_until_expiration: int) -> Alert:
        """Genera alerta específica para vencimiento de estado de usuario"""
        if days_until_expiration <= 0:
            level = AlertLevel.WARNING
            title = "Estado de Usuario Vencido"
            message = f"El estado '{status_name}' de {user_data['nombre']} (DNI: {user_data['dni']}) ha vencido"
        else:
            level = AlertLevel.INFO
            title = "Estado Próximo a Vencer"
            message = f"El estado '{status_name}' de {user_data['nombre']} (DNI: {user_data['dni']}) vence en {days_until_expiration} días"
        
        return self.generate_alert(
            level=level,
            category=AlertCategory.MEMBERSHIP,
            title=title,
            message=message,
            source="Sistema de Estados"
        )
    
    def generate_bulk_membership_alert(self, expired_count: int, expiring_count: int) -> Alert:
        """Genera alerta consolidada para múltiples vencimientos"""
        if expired_count > 0:
            level = AlertLevel.CRITICAL
            title = "Múltiples Cuotas Vencidas"
            message = f"{expired_count} usuarios con cuotas vencidas"
            if expiring_count > 0:
                message += f" y {expiring_count} próximas a vencer"
        elif expiring_count > 0:
            level = AlertLevel.WARNING
            title = "Múltiples Cuotas por Vencer"
            message = f"{expiring_count} usuarios con cuotas próximas a vencer"
        else:
            return None
        
        return self.generate_alert(
            level=level,
            category=AlertCategory.PAYMENT,
            title=title,
            message=message,
            source="Sistema de Membresías"
        )
    
    def get_membership_alerts(self, unresolved_only: bool = True) -> List[Alert]:
        """Obtiene alertas específicas de membresías y pagos"""
        membership_alerts = self.get_alerts(
            category=AlertCategory.MEMBERSHIP,
            unresolved_only=unresolved_only
        )
        payment_alerts = self.get_alerts(
            category=AlertCategory.PAYMENT,
            unresolved_only=unresolved_only
        )
        
        all_alerts = membership_alerts + payment_alerts
        return sorted(all_alerts, key=lambda x: x.timestamp, reverse=True)


# Instancia global del gestor de alertas
alert_manager = AlertManager()


# Manejadores de alerta por defecto
def default_critical_handler(alert: Alert):
    """Manejador por defecto para alertas críticas"""
    # Mostrar mensaje crítico
    QMessageBox.critical(
        None,
        f"Alerta Crítica - {alert.title}",
        alert.message
    )


def default_warning_handler(alert: Alert):
    """Manejador por defecto para alertas de advertencia"""
    # Log de advertencia
    logging.warning(f"ALERTA: {alert.title} - {alert.message}")


# Registrar manejadores por defecto
alert_manager.add_handler(AlertLevel.CRITICAL, default_critical_handler)
alert_manager.add_handler(AlertLevel.WARNING, default_warning_handler)