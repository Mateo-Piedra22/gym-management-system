#!/usr/bin/env python3
"""
Monitoreo de Replicación Nativa - Dashboard de Salud

Proporciona métricas detalladas de la replicación lógica de PostgreSQL
y alertas en tiempo real sobre el estado del sistema.
"""

import psycopg2
import psycopg2.extras
import time
import json
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
import sys

# Configuración de logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class ReplicationHealthMonitor:
    """Monitorea salud de replicación lógica PostgreSQL."""
    
    def __init__(self, local_config: Dict[str, Any], remote_config: Dict[str, Any]):
        self.local_config = local_config
        self.remote_config = remote_config
        self.alerts = []
        self.metrics_history = []
        
    def get_comprehensive_health_report(self) -> Dict[str, Any]:
        """
        Genera reporte completo de salud de replicación.
        
        Returns:
            Dict con métricas detalladas y estado del sistema
        """
        report = {
            "timestamp": datetime.now().isoformat(),
            "overall_status": "healthy",
            "replication_status": {},
            "performance_metrics": {},
            "conflict_resolution": {},
            "alerts": [],
            "recommendations": []
        }
        
        try:
            # 1. Estado de replicación
            report["replication_status"] = self._check_replication_status()
            
            # 2. Métricas de rendimiento
            report["performance_metrics"] = self._get_performance_metrics()
            
            # 3. Resolución de conflictos
            report["conflict_resolution"] = self._check_conflict_resolution()
            
            # 4. Integridad de datos
            report["data_integrity"] = self._verify_data_integrity()
            
            # 5. Generar alertas y recomendaciones
            report["alerts"] = self._generate_alerts(report)
            report["recommendations"] = self._generate_recommendations(report)
            
            # 6. Determinar estado general
            report["overall_status"] = self._determine_overall_status(report)
            
        except Exception as e:
            logger.error(f"❌ Error generando reporte de salud: {e}")
            report["overall_status"] = "error"
            report["alerts"].append({
                "level": "error",
                "message": f"Error generando reporte: {e}",
                "timestamp": datetime.now().isoformat()
            })
            
        return report
    
    def _check_replication_status(self) -> Dict[str, Any]:
        """Verifica estado de replicación."""
        status = {
            "subscriptions": [],
            "publications": [],
            "slots": [],
            "lag_status": "unknown",
            "uptime_seconds": 0
        }
        
        try:
            with psycopg2.connect(**self.remote_config) as conn:
                with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                    # Estado de suscripciones
                    cur.execute("""
                        SELECT 
                            s.subname,
                            s.subenabled,
                            s.subslotname,
                            s.subconninfo,
                            CASE 
                                WHEN s.subenabled THEN 'active'
                                ELSE 'disabled'
                            END as status
                        FROM pg_subscription s
                        ORDER BY s.subname
                    """)
                    status["subscriptions"] = cur.fetchall()
                    
                    # Estado de publicaciones
                    cur.execute("""
                        SELECT 
                            p.pubname,
                            p.pubowner::regrole as owner,
                            p.puballtables,
                            p.pubinsert,
                            p.pubupdate,
                            p.pubdelete
                        FROM pg_publication p
                        ORDER BY p.pubname
                    """)
                    status["publications"] = cur.fetchall()
                    
                    # Estado de slots de replicación
                    cur.execute("""
                        SELECT 
                            slot_name,
                            plugin,
                            slot_type,
                            database,
                            active,
                            restart_lsn,
                            confirmed_flush_lsn,
                            EXTRACT(EPOCH FROM (NOW() - backend_start)) as uptime_seconds
                        FROM pg_replication_slots
                        WHERE slot_type = 'logical'
                        ORDER BY slot_name
                    """)
                    slots = cur.fetchall()
                    status["slots"] = slots
                    
                    # Calcular uptime promedio
                    if slots:
                        status["uptime_seconds"] = sum(slot.get('uptime_seconds', 0) for slot in slots) / len(slots)
                    
        except Exception as e:
            logger.error(f"❌ Error verificando estado de replicación: {e}")
            status["error"] = str(e)
            
        return status
    
    def _get_performance_metrics(self) -> Dict[str, Any]:
        """Obtiene métricas de rendimiento."""
        metrics = {
            "lag_metrics": {},
            "throughput": {},
            "connection_health": {},
            "resource_usage": {}
        }
        
        try:
            with psycopg2.connect(**self.remote_config) as conn:
                with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                    # Métricas de lag
                    cur.execute("""
                        SELECT 
                            application_name,
                            state,
                            sent_lsn,
                            write_lsn,
                            flush_lsn,
                            replay_lsn,
                            write_lag,
                            flush_lag,
                            replay_lag,
                            EXTRACT(EPOCH FROM write_lag) as write_lag_seconds,
                            EXTRACT(EPOCH FROM flush_lag) as flush_lag_seconds,
                            EXTRACT(EPOCH FROM replay_lag) as replay_lag_seconds
                        FROM pg_stat_replication
                        ORDER BY replay_lag DESC
                    """)
                    lag_data = cur.fetchall()
                    
                    # Calcular métricas de lag
                    if lag_data:
                        replay_lags = [r.get('replay_lag_seconds', 0) for r in lag_data if r.get('replay_lag_seconds')]
                        if replay_lags:
                            metrics["lag_metrics"] = {
                                "max_replay_lag_seconds": max(replay_lags),
                                "avg_replay_lag_seconds": sum(replay_lags) / len(replay_lags),
                                "min_replay_lag_seconds": min(replay_lags),
                                "lag_distribution": self._calculate_lag_distribution(replay_lags)
                            }
                    
                    # Métricas de conexión
                    cur.execute("""
                        SELECT 
                            count(*) as total_connections,
                            count(*) FILTER (WHERE state = 'active') as active_connections,
                            count(*) FILTER (WHERE state = 'idle') as idle_connections,
                            avg(EXTRACT(EPOCH FROM (NOW() - backend_start))) as avg_connection_age
                        FROM pg_stat_activity 
                        WHERE datname = current_database()
                    """)
                    connection_metrics = cur.fetchone()
                    metrics["connection_health"] = dict(connection_metrics) if connection_metrics else {}
                    
                    # Uso de recursos
                    cur.execute("""
                        SELECT 
                            numbackends as active_backends,
                            xact_commit as transactions_committed,
                            xact_rollback as transactions_rolled_back,
                            blks_read as blocks_read,
                            blks_hit as blocks_hit,
                            tup_returned as tuples_returned,
                            tup_fetched as tuples_fetched
                        FROM pg_stat_database 
                        WHERE datname = current_database()
                    """)
                    resource_metrics = cur.fetchone()
                    metrics["resource_usage"] = dict(resource_metrics) if resource_metrics else {}
                    
        except Exception as e:
            logger.error(f"❌ Error obteniendo métricas de rendimiento: {e}")
            metrics["error"] = str(e)
            
        return metrics
    
    def _calculate_lag_distribution(self, lags: List[float]) -> Dict[str, int]:
        """Calcula distribución de lag en buckets."""
        distribution = {
            "0-1s": 0,
            "1-5s": 0,
            "5-30s": 0,
            "30s-5m": 0,
            ">5m": 0
        }
        
        for lag in lags:
            if lag <= 1:
                distribution["0-1s"] += 1
            elif lag <= 5:
                distribution["1-5s"] += 1
            elif lag <= 30:
                distribution["5-30s"] += 1
            elif lag <= 300:  # 5 minutos
                distribution["30s-5m"] += 1
            else:
                distribution[">5m"] += 1
                
        return distribution
    
    def _check_conflict_resolution(self) -> Dict[str, Any]:
        """Verifica estado de resolución de conflictos."""
        conflicts = {
            "total_conflicts": 0,
            "resolved_conflicts": 0,
            "pending_conflicts": 0,
            "conflict_types": {},
            "resolution_methods": {}
        }
        
        try:
            # Verificar tablas con columnas de conflicto
            conflict_tables = ["usuarios", "pagos", "asistencias"]
            
            with psycopg2.connect(**self.remote_config) as conn:
                with conn.cursor() as cur:
                    for table in conflict_tables:
                        # Contar conflictos resueltos
                        cur.execute(f"""
                            SELECT COUNT(*) 
                            FROM {table}
                            WHERE conflict_resolved_at IS NOT NULL
                        """)
                        resolved = cur.fetchone()[0]
                        conflicts["resolved_conflicts"] += resolved
                        
                        # Contar conflictos pendientes (registros con conflict_winner)
                        cur.execute(f"""
                            SELECT COUNT(*) 
                            FROM {table}
                            WHERE conflict_winner IS NOT NULL
                        """)
                        pending = cur.fetchone()[0]
                        conflicts["pending_conflicts"] += pending
                        
                        conflicts["conflict_types"][table] = {
                            "resolved": resolved,
                            "pending": pending
                        }
                        
        except Exception as e:
            logger.error(f"❌ Error verificando resolución de conflictos: {e}")
            conflicts["error"] = str(e)
            
        conflicts["total_conflicts"] = conflicts["resolved_conflicts"] + conflicts["pending_conflicts"]
        
        return conflicts
    
    def _verify_data_integrity(self) -> Dict[str, Any]:
        """Verifica integridad de datos entre local y remoto."""
        integrity = {
            "table_counts": {},
            "mismatches": [],
            "sync_percentage": 0,
            "critical_tables": ["usuarios", "pagos", "asistencias"]
        }
        
        try:
            # Comparar conteos de tablas críticas
            total_local = 0
            total_remote = 0
            mismatches = 0
            
            with psycopg2.connect(**self.local_config) as local_conn:
                with psycopg2.connect(**self.remote_config) as remote_conn:
                    
                    for table in integrity["critical_tables"]:
                        # Contar en local
                        with local_conn.cursor() as local_cur:
                            local_cur.execute(f"SELECT COUNT(*) FROM {table}")
                            local_count = local_cur.fetchone()[0]
                        
                        # Contar en remoto
                        with remote_conn.cursor() as remote_cur:
                            remote_cur.execute(f"SELECT COUNT(*) FROM {table}")
                            remote_count = remote_cur.fetchone()[0]
                        
                        diff = abs(local_count - remote_count)
                        
                        integrity["table_counts"][table] = {
                            "local": local_count,
                            "remote": remote_count,
                            "difference": diff,
                            "sync_percentage": 100 - (diff / max(local_count, remote_count, 1) * 100)
                        }
                        
                        total_local += local_count
                        total_remote += remote_count
                        
                        if diff > 0:
                            mismatches += 1
                            integrity["mismatches"].append({
                                "table": table,
                                "local_count": local_count,
                                "remote_count": remote_count,
                                "difference": diff
                            })
            
            # Calcular porcentaje de sincronización general
            total_diff = abs(total_local - total_remote)
            integrity["sync_percentage"] = 100 - (total_diff / max(total_local, total_remote, 1) * 100)
            
        except Exception as e:
            logger.error(f"❌ Error verificando integridad de datos: {e}")
            integrity["error"] = str(e)
            
        return integrity
    
    def _generate_alerts(self, report: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Genera alertas basadas en el reporte."""
        alerts = []
        
        # Alertas de lag crítico
        lag_metrics = report.get("performance_metrics", {}).get("lag_metrics", {})
        max_lag = lag_metrics.get("max_replay_lag_seconds", 0)
        
        if max_lag > 300:  # 5 minutos
            alerts.append({
                "level": "critical",
                "category": "performance",
                "message": f"Lag de replicación crítico: {max_lag:.1f} segundos",
                "recommendation": "Verificar conectividad y rendimiento del servidor"
            })
        elif max_lag > 60:  # 1 minuto
            alerts.append({
                "level": "warning",
                "category": "performance",
                "message": f"Lag de replicación elevado: {max_lag:.1f} segundos",
                "recommendation": "Monitorear tendencia de lag"
            })
        
        # Alertas de conflictos no resueltos
        conflicts = report.get("conflict_resolution", {})
        pending_conflicts = conflicts.get("pending_conflicts", 0)
        
        if pending_conflicts > 10:
            alerts.append({
                "level": "warning",
                "category": "data_integrity",
                "message": f"{pending_conflicts} conflictos de datos pendientes",
                "recommendation": "Revisar y resolver conflictos manualmente"
            })
        
        # Alertas de integridad de datos
        integrity = report.get("data_integrity", {})
        sync_percentage = integrity.get("sync_percentage", 100)
        
        if sync_percentage < 95:
            alerts.append({
                "level": "warning",
                "category": "data_integrity",
                "message": f"Integridad de datos comprometida: {sync_percentage:.1f}% sincronizado",
                "recommendation": "Verificar tablas con diferencias y re-sincronizar si es necesario"
            })
        
        # Alertas de suscripciones inactivas
        repl_status = report.get("replication_status", {})
        subscriptions = repl_status.get("subscriptions", [])
        
        inactive_subs = [s for s in subscriptions if not s.get("subenabled", False)]
        if inactive_subs:
            alerts.append({
                "level": "error",
                "category": "replication",
                "message": f"{len(inactive_subs)} suscripciones inactivas detectadas",
                "recommendation": "Reactivar suscripciones o verificar configuración"
            })
        
        return alerts
    
    def _generate_recommendations(self, report: Dict[str, Any]) -> List[str]:
        """Genera recomendaciones basadas en el reporte."""
        recommendations = []
        
        # Recomendaciones de rendimiento
        lag_metrics = report.get("performance_metrics", {}).get("lag_metrics", {})
        avg_lag = lag_metrics.get("avg_replay_lag_seconds", 0)
        
        if avg_lag > 30:
            recommendations.append("Considerar optimización de red o hardware para reducir lag")
        
        # Recomendaciones de configuración
        repl_status = report.get("replication_status", {})
        slots = repl_status.get("slots", [])
        
        if len(slots) < 2:
            recommendations.append("Considerar configurar réplicas adicionales para alta disponibilidad")
        
        # Recomendaciones de mantenimiento
        conflicts = report.get("conflict_resolution", {})
        total_conflicts = conflicts.get("total_conflicts", 0)
        
        if total_conflicts > 50:
            recommendations.append("Implementar revisión periódica de conflictos para prevenir acumulación")
        
        # Recomendaciones generales
        recommendations.extend([
            "Monitorear replicación cada 5 minutos en producción",
            "Configurar alertas automáticas para lag > 60 segundos",
            "Realizar backup antes de modificaciones en configuración de replicación",
            "Documentar procedimientos de failover para réplicas"
        ])
        
        return recommendations
    
    def _determine_overall_status(self, report: Dict[str, Any]) -> str:
        """Determina el estado general basado en el reporte."""
        alerts = report.get("alerts", [])
        
        critical_alerts = [a for a in alerts if a.get("level") == "critical"]
        error_alerts = [a for a in alerts if a.get("level") == "error"]
        warning_alerts = [a for a in alerts if a.get("level") == "warning"]
        
        if critical_alerts:
            return "critical"
        elif error_alerts:
            return "error"
        elif warning_alerts:
            return "warning"
        else:
            return "healthy"
    
    def monitor_continuous(self, interval_seconds: int = 60):
        """Monitoreo continuo de la replicación."""
        logger.info(f"🔍 Iniciando monitoreo continuo cada {interval_seconds} segundos...")
        
        while True:
            try:
                report = self.get_comprehensive_health_report()
                
                # Log resumen
                status = report["overall_status"]
                alerts_count = len(report["alerts"])
                
                if status == "healthy":
                    logger.info(f"✅ Sistema saludable - {alerts_count} alertas")
                elif status == "warning":
                    logger.warning(f"⚠️ Sistema con advertencias - {alerts_count} alertas")
                else:
                    logger.error(f"❌ Sistema en estado {status} - {alerts_count} alertas")
                
                # Guardar historial
                self.metrics_history.append({
                    "timestamp": report["timestamp"],
                    "status": status,
                    "alerts_count": alerts_count,
                    "max_lag": report.get("performance_metrics", {}).get("lag_metrics", {}).get("max_replay_lag_seconds", 0)
                })
                
                # Mantener solo últimas 1000 entradas
                if len(self.metrics_history) > 1000:
                    self.metrics_history = self.metrics_history[-1000:]
                
                time.sleep(interval_seconds)
                
            except KeyboardInterrupt:
                logger.info("🛑 Monitoreo interrumpido por usuario")
                break
            except Exception as e:
                logger.error(f"❌ Error en monitoreo: {e}")
                time.sleep(interval_seconds)


def main():
    """Función principal de prueba."""
    from secure_config import config as secure_config
    
    local_config = secure_config.get_db_config('local')
    remote_config = secure_config.get_db_config('remote')
    
    print("🔍 Generando reporte de salud de replicación...")
    
    monitor = ReplicationHealthMonitor(local_config, remote_config)
    report = monitor.get_comprehensive_health_report()
    
    print(f"📊 Estado general: {report['overall_status']}")
    print(f"📈 Alertas: {len(report['alerts'])}")
    print(f"💡 Recomendaciones: {len(report['recommendations'])}")
    
    if report['alerts']:
        print("\n⚠️  Alertas detectadas:")
        for alert in report['alerts']:
            print(f"   [{alert['level']}] {alert['message']}")
    
    # Guardar reporte
    report_file = f"replication_health_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    with open(report_file, 'w', encoding='utf-8') as f:
        json.dump(report, f, indent=2, default=str)
    
    print(f"\n📋 Reporte guardado en: {report_file}")
    
    # Opción de monitoreo continuo
    if len(sys.argv) > 1 and sys.argv[1] == "--monitor":
        monitor.monitor_continuous(interval_seconds=30)

if __name__ == "__main__":
    main()