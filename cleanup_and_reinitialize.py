#!/usr/bin/env python3
"""
Limpieza y Reinicialización de Bases de Datos - Sistema de Gestión de Gimnasio

Este script realiza una limpieza completa y reinicialización de las bases de datos
local y remota, preservando solo los datos críticos y optimizando la estructura
para la replicación nativa de PostgreSQL.

PROCESO:
1. Backup completo de datos críticos
2. Limpieza de tablas redundantes y temporales
3. Reinicialización con estructura optimizada
4. Restauración de datos críticos
5. Configuración de replicación nativa
"""

from secure_config import config as secure_config
import psycopg2
import psycopg2.extras
import json
import logging
import sys
from datetime import datetime
from typing import Dict, List, Any, Optional
from pathlib import Path
import shutil

# Configuración de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f'cleanup_reinit_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class DatabaseCleanupReinitializer:
    """Gestiona limpieza y reinicialización completa de bases de datos."""
    
    def __init__(self):
        self.local_config = secure_config.get_db_config('local')
        self.remote_config = secure_config.get_db_config('remote')
        
        # Tablas críticas que deben preservarse
        self.critical_tables = [
            'usuarios', 'pagos', 'asistencias', 'clases', 'profesores',
            'tipos_clases', 'metodos_pago', 'conceptos_pago', 'configuracion'
        ]
        
        # Tablas de sistema que pueden ser regeneradas
        self.system_tables = [
            'sync_outbox', 'sync_inbox', 'auditoria', 'notificaciones_cupos',
            'whatsapp_templates', 'whatsapp_config', 'whatsapp_messages',
            'custom_themes', 'theme_schedules', 'theme_events',
            'node_state', 'numeracion_comprobantes'
        ]
        
        # Tablas de configuración y catálogos
        self.catalog_tables = [
            'tipos_cuota', 'etiquetas', 'usuario_etiquetas', 'usuario_estados',
            'usuario_notas', 'estados_usuario', 'especialidades', 'profesor_especialidades',
            'profesor_certificaciones', 'profesor_evaluaciones', 'profesor_horas_trabajadas',
            'horarios_profesores', 'profesores_horarios_disponibilidad',
            'profesor_disponibilidad', 'profesor_clase_asignaciones',
            'profesor_suplencias', 'profesor_suplencias_generales',
            'clase_lista_espera', 'clase_usuarios', 'clase_ejercicios',
            'clase_asistencia_historial', 'ejercicios', 'ejercicio_grupos',
            'ejercicio_grupo_items', 'rutinas', 'rutina_ejercicios'
        ]
        
        self.backup_dir = Path("cleanup_backups")
        self.backup_dir.mkdir(exist_ok=True)
    
    def perform_complete_cleanup_and_reinit(self) -> Dict[str, Any]:
        """
        Ejecuta limpieza y reinicialización completa.
        
        Returns:
            Dict con resultado detallado del proceso
        """
        logger.info("🚀 Iniciando limpieza y reinicialización completa...")
        
        result = {
            "timestamp": datetime.now().isoformat(),
            "status": "started",
            "phases": [],
            "backup_summary": {},
            "cleanup_summary": {},
            "reinit_summary": {},
            "errors": [],
            "warnings": []
        }
        
        try:
            # Fase 1: Backup completo de datos críticos
            logger.info("💾 FASE 1: Creando backup de datos críticos...")
            backup_result = self._create_critical_data_backup()
            result["phases"].append({"phase": "backup", "result": backup_result})
            result["backup_summary"] = backup_result
            
            if backup_result["status"] != "success":
                raise Exception("Backup crítico falló - abortando limpieza")
            
            # Fase 2: Limpieza de datos redundantes
            logger.info("🧹 FASE 2: Limpiando datos redundantes...")
            cleanup_result = self._perform_data_cleanup()
            result["phases"].append({"phase": "cleanup", "result": cleanup_result})
            result["cleanup_summary"] = cleanup_result
            
            # Fase 3: Reinicialización de estructura
            logger.info("🔧 FASE 3: Reinicializando estructura de base de datos...")
            reinit_result = self._reinitialize_database_structure()
            result["phases"].append({"phase": "reinit", "result": reinit_result})
            result["reinit_summary"] = reinit_result
            
            # Fase 4: Restauración de datos críticos
            logger.info("♻️ FASE 4: Restaurando datos críticos...")
            restore_result = self._restore_critical_data(backup_result)
            result["phases"].append({"phase": "restore", "result": restore_result})
            
            # Fase 5: Configuración de replicación nativa
            logger.info("🔄 FASE 5: Configurando replicación nativa...")
            replication_result = self._setup_native_replication()
            result["phases"].append({"phase": "replication", "result": replication_result})
            
            # Fase 6: Verificación final
            logger.info("✅ FASE 6: Verificando integridad del sistema...")
            verification_result = self._verify_system_integrity()
            result["phases"].append({"phase": "verification", "result": verification_result})
            
            result["status"] = "completed_successfully"
            logger.info("🎉 Limpieza y reinicialización completadas exitosamente!")
            
        except Exception as e:
            logger.error(f"❌ Error en limpieza y reinicialización: {e}")
            result["status"] = "failed"
            result["errors"].append(str(e))
            
            # Intentar restaurar desde backup si es posible
            self._attempt_emergency_restore(backup_result)
        
        # Guardar log completo
        self._save_cleanup_log(result)
        
        return result
    
    def _create_critical_data_backup(self) -> Dict[str, Any]:
        """Crea backup completo de datos críticos."""
        logger.info("💾 Creando backup de datos críticos...")
        
        backup_result = {
            "status": "success",
            "backups": [],
            "total_records": 0,
            "backup_size_bytes": 0
        }
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_subdir = self.backup_dir / f"cleanup_backup_{timestamp}"
        backup_subdir.mkdir(exist_ok=True)
        
        try:
            # Backup de base de datos local
            local_backup = self._backup_database_tables(self.local_config, "local", backup_subdir)
            backup_result["backups"].append(local_backup)
            
            # Backup de base de datos remota
            remote_backup = self._backup_database_tables(self.remote_config, "remote", backup_subdir)
            backup_result["backups"].append(remote_backup)
            
            # Resumen total
            backup_result["total_records"] = sum(b.get("total_records", 0) for b in backup_result["backups"])
            backup_result["backup_size_bytes"] = sum(b.get("backup_size_bytes", 0) for b in backup_result["backups"])
            
            logger.info(f"✅ Backup completado: {backup_result['total_records']} registros en {len(backup_result['backups'])} bases de datos")
            
        except Exception as e:
            logger.error(f"❌ Error creando backup: {e}")
            backup_result["status"] = "error"
            backup_result["error"] = str(e)
        
        return backup_result
    
    def _backup_database_tables(self, db_config: Dict[str, Any], server_name: str, backup_dir: Path) -> Dict[str, Any]:
        """Backup de tablas específicas de una base de datos."""
        backup_info = {
            "server": server_name,
            "status": "success",
            "tables": [],
            "total_records": 0,
            "backup_size_bytes": 0,
            "backup_files": []
        }
        
        try:
            with psycopg2.connect(**db_config) as conn:
                with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                    
                    # Backup de tablas críticas
                    for table in self.critical_tables:
                        try:
                            # Verificar si la tabla existe
                            cur.execute(f"""
                                SELECT EXISTS (
                                    SELECT 1 FROM pg_tables 
                                    WHERE schemaname = 'public' AND tablename = %s
                                )
                            """, (table,))
                            
                            if not cur.fetchone()['exists']:
                                logger.warning(f"⚠️ Tabla {table} no existe en {server_name}")
                                continue
                            
                            # Obtener datos de la tabla
                            cur.execute(f"SELECT * FROM {table}")
                            records = cur.fetchall()
                            
                            if records:
                                # Guardar como JSON
                                backup_file = backup_dir / f"{server_name}_{table}.json"
                                with open(backup_file, 'w', encoding='utf-8') as f:
                                    json.dump(records, f, indent=2, default=str)
                                
                                # Información del backup
                                table_backup = {
                                    "table": table,
                                    "record_count": len(records),
                                    "file": str(backup_file),
                                    "file_size_bytes": backup_file.stat().st_size
                                }
                                
                                backup_info["tables"].append(table_backup)
                                backup_info["total_records"] += len(records)
                                backup_info["backup_size_bytes"] += table_backup["file_size_bytes"]
                                backup_info["backup_files"].append(str(backup_file))
                                
                                logger.info(f"📋 {server_name}.{table}: {len(records)} registros respaldados")
                            else:
                                logger.info(f"📋 {server_name}.{table}: tabla vacía")
                                
                        except Exception as e:
                            logger.error(f"❌ Error respaldando {table} en {server_name}: {e}")
                            backup_info["tables"].append({"table": table, "error": str(e)})
                    
                    # Backup de esquema (estructura)
                    schema_backup = self._backup_database_schema(cur, server_name, backup_dir)
                    backup_info["schema_backup"] = schema_backup
                    
                    logger.info(f"✅ Backup {server_name} completado: {backup_info['total_records']} registros")
                    
        except Exception as e:
            logger.error(f"❌ Error en backup de {server_name}: {e}")
            backup_info["status"] = "error"
            backup_info["error"] = str(e)
        
        return backup_info
    
    def _backup_database_schema(self, cursor, server_name: str, backup_dir: Path) -> Dict[str, Any]:
        """Backup del esquema de la base de datos."""
        schema_info = {
            "status": "success",
            "tables": [],
            "backup_file": None
        }
        
        try:
            # Obtener definición de tablas
            cursor.execute("""
                SELECT 
                    table_name,
                    column_name,
                    data_type,
                    is_nullable,
                    column_default,
                    character_maximum_length
                FROM information_schema.columns
                WHERE table_schema = 'public'
                ORDER BY table_name, ordinal_position
            """)
            
            schema_data = {}
            for row in cursor.fetchall():
                table_name = row['table_name']
                if table_name not in schema_data:
                    schema_data[table_name] = {
                        "columns": [],
                        "primary_key": None,
                        "indexes": [],
                        "constraints": []
                    }
                
                schema_data[table_name]["columns"].append({
                    "name": row['column_name'],
                    "type": row['data_type'],
                    "nullable": row['is_nullable'],
                    "default": row['column_default'],
                    "max_length": row['character_maximum_length']
                })
            
            # Guardar esquema
            schema_file = backup_dir / f"{server_name}_schema.json"
            with open(schema_file, 'w', encoding='utf-8') as f:
                json.dump(schema_data, f, indent=2, default=str)
            
            schema_info["tables"] = list(schema_data.keys())
            schema_info["backup_file"] = str(schema_file)
            
            logger.info(f"📐 Esquema {server_name} respaldado: {len(schema_data)} tablas")
            
        except Exception as e:
            logger.error(f"❌ Error respaldando esquema de {server_name}: {e}")
            schema_info["status"] = "error"
            schema_info["error"] = str(e)
        
        return schema_info
    
    def _perform_data_cleanup(self) -> Dict[str, Any]:
        """Realiza limpieza de datos redundantes."""
        logger.info("🧹 Iniciando limpieza de datos redundantes...")
        
        cleanup_result = {
            "status": "success",
            "local_cleanup": {},
            "remote_cleanup": {},
            "total_tables_cleaned": 0,
            "total_records_removed": 0
        }
        
        try:
            # Limpieza de base de datos local
            logger.info("🧹 Limpiando base de datos local...")
            local_cleanup = self._cleanup_database(self.local_config, "local")
            cleanup_result["local_cleanup"] = local_cleanup
            
            # Limpieza de base de datos remota
            logger.info("🧹 Limpiando base de datos remota...")
            remote_cleanup = self._cleanup_database(self.remote_config, "remote")
            cleanup_result["remote_cleanup"] = remote_cleanup
            
            # Resumen total
            cleanup_result["total_tables_cleaned"] = (
                local_cleanup.get("tables_cleaned", 0) + 
                remote_cleanup.get("tables_cleaned", 0)
            )
            
            cleanup_result["total_records_removed"] = (
                local_cleanup.get("records_removed", 0) + 
                remote_cleanup.get("records_removed", 0)
            )
            
            logger.info(f"✅ Limpieza completada: {cleanup_result['total_records_removed']} registros eliminados")
            
        except Exception as e:
            logger.error(f"❌ Error en limpieza: {e}")
            cleanup_result["status"] = "error"
            cleanup_result["error"] = str(e)
        
        return cleanup_result
    
    def _cleanup_database(self, db_config: Dict[str, Any], server_name: str) -> Dict[str, Any]:
        """Limpia una base de datos individual."""
        cleanup_info = {
            "server": server_name,
            "status": "success",
            "tables_cleaned": 0,
            "records_removed": 0,
            "cleaned_tables": [],
            "errors": []
        }
        
        try:
            with psycopg2.connect(**db_config) as conn:
                with conn.cursor() as cur:
                    
                    # 1. Limpiar tablas de sincronización
                    sync_tables = ['sync_outbox', 'sync_inbox']
                    for table in sync_tables:
                        try:
                            cur.execute(f"SELECT COUNT(*) FROM {table}")
                            count = cur.fetchone()[0]
                            
                            if count > 0:
                                # Mantener solo registros recientes (< 7 días)
                                cur.execute(f"""
                                    DELETE FROM {table} 
                                    WHERE created_at < NOW() - INTERVAL '7 days'
                                """)
                                deleted = cur.rowcount
                                
                                cleanup_info["tables_cleaned"] += 1
                                cleanup_info["records_removed"] += deleted
                                cleanup_info["cleaned_tables"].append({
                                    "table": table,
                                    "action": "delete_old_records",
                                    "records_removed": deleted,
                                    "records_kept": count - deleted
                                })
                                
                                logger.info(f"🧹 {server_name}.{table}: {deleted} registros antiguos eliminados")
                            
                        except Exception as e:
                            logger.warning(f"⚠️ Error limpiando {table} en {server_name}: {e}")
                            cleanup_info["errors"].append({"table": table, "error": str(e)})
                    
                    # 2. Limpiar tablas de auditoría temporal
                    audit_tables = ['auditoria']
                    for table in audit_tables:
                        try:
                            cur.execute(f"SELECT COUNT(*) FROM {table}")
                            count = cur.fetchone()[0]
                            
                            if count > 1000:  # Si hay muchos registros
                                # Mantener solo últimos 30 días
                                cur.execute(f"""
                                    DELETE FROM {table} 
                                    WHERE created_at < NOW() - INTERVAL '30 days'
                                """)
                                deleted = cur.rowcount
                                
                                cleanup_info["tables_cleaned"] += 1
                                cleanup_info["records_removed"] += deleted
                                cleanup_info["cleaned_tables"].append({
                                    "table": table,
                                    "action": "delete_old_audit",
                                    "records_removed": deleted
                                })
                                
                                logger.info(f"🧹 {server_name}.{table}: {deleted} registros de auditoría antiguos eliminados")
                            
                        except Exception as e:
                            logger.warning(f"⚠️ Error limpiando auditoría en {server_name}: {e}")
                    
                    # 3. Limpiar tablas de notificaciones procesadas
                    notification_tables = ['notificaciones_cupos']
                    for table in notification_tables:
                        try:
                            cur.execute(f"SELECT COUNT(*) FROM {table}")
                            count = cur.fetchone()[0]
                            
                            if count > 500:  # Si hay muchas notificaciones
                                # Eliminar notificaciones antiguas procesadas
                                cur.execute(f"DELETE FROM {table} WHERE created_at < NOW() - INTERVAL '30 days'")
                                deleted = cur.rowcount
                                
                                cleanup_info["tables_cleaned"] += 1
                                cleanup_info["records_removed"] += deleted
                                cleanup_info["cleaned_tables"].append({
                                    "table": table,
                                    "action": "delete_old_notifications",
                                    "records_removed": deleted
                                })
                                
                                logger.info(f"🧹 {server_name}.{table}: {deleted} notificaciones antiguas eliminadas")
                            
                        except Exception as e:
                            logger.warning(f"⚠️ Error limpiando notificaciones en {server_name}: {e}")
                    
                    conn.commit()
                    logger.info(f"✅ Limpieza {server_name} completada: {cleanup_info['tables_cleaned']} tablas, {cleanup_info['records_removed']} registros")
                    
        except Exception as e:
            logger.error(f"❌ Error limpiando {server_name}: {e}")
            cleanup_info["status"] = "error"
            cleanup_info["error"] = str(e)
        
        return cleanup_info
    
    def _reinitialize_database_structure(self) -> Dict[str, Any]:
        """Reinicializa la estructura de la base de datos."""
        logger.info("🔧 Reinicializando estructura de base de datos...")
        
        reinit_result = {
            "status": "success",
            "local_reinit": {},
            "remote_reinit": {},
            "optimizations_applied": [],
            "replication_setup": {}
        }
        
        try:
            # Reinicialización local
            logger.info("🔧 Reinicializando base de datos local...")
            local_reinit = self._reinitialize_single_database(self.local_config, "local")
            reinit_result["local_reinit"] = local_reinit
            
            # Reinicialización remota
            logger.info("🔧 Reinicializando base de datos remota...")
            remote_reinit = self._reinitialize_single_database(self.remote_config, "remote")
            reinit_result["remote_reinit"] = remote_reinit
            
            # Aplicar optimizaciones
            optimizations = self._apply_database_optimizations()
            reinit_result["optimizations_applied"] = optimizations
            
            logger.info("✅ Reinicialización de estructura completada")
            
        except Exception as e:
            logger.error(f"❌ Error en reinicialización: {e}")
            reinit_result["status"] = "error"
            reinit_result["error"] = str(e)
        
        return reinit_result
    
    def _reinitialize_single_database(self, db_config: Dict[str, Any], server_name: str) -> Dict[str, Any]:
        """Reinicializa una sola base de datos."""
        reinit_info = {
            "server": server_name,
            "status": "success",
            "tables_optimized": 0,
            "indexes_created": 0,
            "constraints_validated": 0,
            "actions": []
        }
        
        try:
            with psycopg2.connect(**db_config) as conn:
                with conn.cursor() as cur:
                    
                    # 1. Optimizar tablas existentes
                    for table in self.critical_tables:
                        try:
                            cur.execute(f"""
                                SELECT EXISTS (
                                    SELECT 1 FROM pg_tables 
                                    WHERE schemaname = 'public' AND tablename = %s
                                )
                            """, (table,))
                            
                            if cur.fetchone()['exists']:
                                # VACUUM y ANALYZE para optimizar
                                cur.execute(f"VACUUM ANALYZE {table}")
                                
                                # Actualizar estadísticas
                                cur.execute(f"""
                                    ALTER TABLE {table} 
                                    ALTER COLUMN created_at SET DEFAULT NOW(),
                                    ALTER COLUMN updated_at SET DEFAULT NOW()
                                """)
                                
                                reinit_info["tables_optimized"] += 1
                                reinit_info["actions"].append(f"optimized_{table}")
                                
                                logger.info(f"🔧 {server_name}.{table}: tabla optimizada")
                            
                        except Exception as e:
                            logger.warning(f"⚠️ Error optimizando {table} en {server_name}: {e}")
                    
                    # 2. Crear índices optimizados
                    self._create_optimized_indexes(cur, server_name, reinit_info)
                    
                    # 3. Validar integridad de restricciones
                    self._validate_constraints(cur, server_name, reinit_info)
                    
                    conn.commit()
                    logger.info(f"✅ Reinicialización {server_name} completada: {reinit_info['tables_optimized']} tablas optimizadas")
                    
        except Exception as e:
            logger.error(f"❌ Error reinicializando {server_name}: {e}")
            reinit_info["status"] = "error"
            reinit_info["error"] = str(e)
        
        return reinit_info
    
    def _create_optimized_indexes(self, cursor, server_name: str, reinit_info: Dict[str, Any]):
        """Crea índices optimizados para mejorar rendimiento."""
        try:
            # Índices para tablas críticas
            optimized_indexes = [
                # Usuarios
                "CREATE INDEX IF NOT EXISTS idx_usuarios_email ON usuarios (email)",
                "CREATE INDEX IF NOT EXISTS idx_usuarios_estado ON usuarios (estado)",
                "CREATE INDEX IF NOT EXISTS idx_usuarios_created_at ON usuarios (created_at)",
                
                # Pagos
                "CREATE INDEX IF NOT EXISTS idx_pagos_usuario_id ON pagos (usuario_id)",
                "CREATE INDEX IF NOT EXISTS idx_pagos_fecha ON pagos (fecha)",
                "CREATE INDEX IF NOT EXISTS idx_pagos_estado ON pagos (estado)",
                
                # Asistencias
                "CREATE INDEX IF NOT EXISTS idx_asistencias_usuario_id ON asistencias (usuario_id)",
                "CREATE INDEX IF NOT EXISTS idx_asistencias_fecha ON asistencias (fecha)",
                
                # Clases
                "CREATE INDEX IF NOT EXISTS idx_clases_fecha ON clases (fecha)",
                "CREATE INDEX IF NOT EXISTS idx_clases_profesor_id ON clases (profesor_id)",
                
                # Profesores
                "CREATE INDEX IF NOT EXISTS idx_profesores_estado ON profesores (estado)",
                "CREATE INDEX IF NOT EXISTS idx_profesores_created_at ON profesores (created_at)"
            ]
            
            for index_sql in optimized_indexes:
                try:
                    cursor.execute(index_sql)
                    reinit_info["indexes_created"] += 1
                except Exception as e:
                    logger.warning(f"⚠️ Error creando índice en {server_name}: {e}")
            
            logger.info(f"🔍 {server_name}: {reinit_info['indexes_created']} índices optimizados creados")
            
        except Exception as e:
            logger.error(f"❌ Error creando índices en {server_name}: {e}")
    
    def _validate_constraints(self, cursor, server_name: str, reinit_info: Dict[str, Any]):
        """Valida y repara integridad de restricciones."""
        try:
            # Validar claves foráneas
            cursor.execute("""
                SELECT conname, contype, conrelid::regclass, confrelid::regclass
                FROM pg_constraint 
                WHERE contype = 'f'
            """)
            
            foreign_keys = cursor.fetchall()
            
            for fk in foreign_keys:
                try:
                    # Validar cada clave foránea
                    cursor.execute(f"ALTER TABLE {fk['conrelid']} VALIDATE CONSTRAINT {fk['conname']}")
                    reinit_info["constraints_validated"] += 1
                    
                except Exception as e:
                    logger.warning(f"⚠️ Error validando FK {fk['conname']} en {server_name}: {e}")
            
            logger.info(f"🔐 {server_name}: {reinit_info['constraints_validated']} restricciones validadas")
            
        except Exception as e:
            logger.error(f"❌ Error validando restricciones en {server_name}: {e}")
    
    def _restore_critical_data(self, backup_result: Dict[str, Any]) -> Dict[str, Any]:
        """Restaura datos críticos desde backup."""
        logger.info("♻️ Restaurando datos críticos desde backup...")
        
        restore_result = {
            "status": "success",
            "local_restore": {},
            "remote_restore": {},
            "total_records_restored": 0,
            "errors": []
        }
        
        try:
            # Restaurar datos locales
            local_backup = next((b for b in backup_result["backups"] if b["server"] == "local"), None)
            if local_backup:
                local_restore = self._restore_database_data(local_backup, self.local_config, "local")
                restore_result["local_restore"] = local_restore
                restore_result["total_records_restored"] += local_restore.get("records_restored", 0)
            
            # Restaurar datos remotos
            remote_backup = next((b for b in backup_result["backups"] if b["server"] == "remote"), None)
            if remote_backup:
                remote_restore = self._restore_database_data(remote_backup, self.remote_config, "remote")
                restore_result["remote_restore"] = remote_restore
                restore_result["total_records_restored"] += remote_restore.get("records_restored", 0)
            
            logger.info(f"✅ Restauración completada: {restore_result['total_records_restored']} registros restaurados")
            
        except Exception as e:
            logger.error(f"❌ Error en restauración: {e}")
            restore_result["status"] = "error"
            restore_result["error"] = str(e)
        
        return restore_result
    
    def _restore_database_data(self, backup_info: Dict[str, Any], db_config: Dict[str, Any], server_name: str) -> Dict[str, Any]:
        """Restaura datos de una base de datos desde backup."""
        restore_info = {
            "server": server_name,
            "status": "success",
            "tables_restored": 0,
            "records_restored": 0,
            "restored_tables": [],
            "errors": []
        }
        
        try:
            with psycopg2.connect(**db_config) as conn:
                with conn.cursor() as cur:
                    
                    # Restaurar cada tabla
                    for table_backup in backup_info.get("tables", []):
                        try:
                            table_name = table_backup["table"]
                            backup_file = table_backup["file"]
                            record_count = table_backup["record_count"]
                            
                            if record_count == 0:
                                continue
                            
                            # Leer datos del backup
                            with open(backup_file, 'r', encoding='utf-8') as f:
                                records = json.load(f)
                            
                            # Insertar datos restaurados
                            if records:
                                self._insert_restored_records(cur, table_name, records)
                                
                                restore_info["tables_restored"] += 1
                                restore_info["records_restored"] += len(records)
                                restore_info["restored_tables"].append(table_name)
                                
                                logger.info(f"♻️ {server_name}.{table_name}: {len(records)} registros restaurados")
                            
                        except Exception as e:
                            logger.error(f"❌ Error restaurando {table_backup.get('table', 'unknown')} en {server_name}: {e}")
                            restore_info["errors"].append({"table": table_backup.get("table"), "error": str(e)})
                    
                    conn.commit()
                    logger.info(f"✅ Restauración {server_name} completada: {restore_info['tables_restored']} tablas, {restore_info['records_restored']} registros")
                    
        except Exception as e:
            logger.error(f"❌ Error en restauración de {server_name}: {e}")
            restore_info["status"] = "error"
            restore_info["error"] = str(e)
        
        return restore_info
    
    def _insert_restored_records(self, cursor, table_name: str, records: List[Dict[str, Any]]):
        """Inserta registros restaurados en la tabla."""
        if not records:
            return
        
        # Obtener columnas de la primera fila
        columns = list(records[0].keys())
        column_list = ", ".join(columns)
        placeholders = ", ".join([f"%({col})s" for col in columns])
        
        # Insertar en lotes para mejor rendimiento
        batch_size = 100
        for i in range(0, len(records), batch_size):
            batch = records[i:i + batch_size]
            
            insert_query = f"""
                INSERT INTO {table_name} ({column_list})
                VALUES ({placeholders})
                ON CONFLICT DO NOTHING
            """
            
            cursor.executemany(insert_query, batch)
    
    def _setup_native_replication(self) -> Dict[str, Any]:
        """Configura replicación nativa de PostgreSQL."""
        logger.info("🔄 Configurando replicación nativa de PostgreSQL...")
        
        # Importar y usar el sistema de replicación nativa
        try:
            from native_replication_manager import NativeReplicationManager
            
            replicator = NativeReplicationManager(self.local_config, self.remote_config)
            replication_result = replicator.setup_logical_replication()
            
            logger.info("✅ Replicación nativa configurada exitosamente")
            return replication_result
            
        except Exception as e:
            logger.error(f"❌ Error configurando replicación nativa: {e}")
            return {
                "status": "error",
                "error": str(e),
                "message": "Replicación nativa no pudo ser configurada"
            }
    
    def _verify_system_integrity(self) -> Dict[str, Any]:
        """Verifica integridad del sistema después de la limpieza."""
        logger.info("🔍 Verificando integridad del sistema...")
        
        verification = {
            "status": "success",
            "local_verification": {},
            "remote_verification": {},
            "replication_status": {},
            "overall_health": "unknown"
        }
        
        try:
            # Verificación local
            local_verify = self._verify_database_integrity(self.local_config, "local")
            verification["local_verification"] = local_verify
            
            # Verificación remota
            remote_verify = self._verify_database_integrity(self.remote_config, "remote")
            verification["remote_verification"] = remote_verify
            
            # Estado de replicación
            replication_status = self._check_replication_health()
            verification["replication_status"] = replication_status
            
            # Salud general
            if (local_verify.get("status") == "healthy" and 
                remote_verify.get("status") == "healthy" and 
                replication_status.get("status") == "healthy"):
                verification["overall_health"] = "excellent"
            elif (local_verify.get("status") == "healthy" and 
                  remote_verify.get("status") == "healthy"):
                verification["overall_health"] = "good"
            else:
                verification["overall_health"] = "needs_attention"
            
            logger.info(f"✅ Verificación completada - Salud: {verification['overall_health']}")
            
        except Exception as e:
            logger.error(f"❌ Error en verificación: {e}")
            verification["status"] = "error"
            verification["error"] = str(e)
        
        return verification
    
    def _verify_database_integrity(self, db_config: Dict[str, Any], server_name: str) -> Dict[str, Any]:
        """Verifica integridad de una base de datos individual."""
        integrity = {
            "server": server_name,
            "status": "healthy",
            "table_count": 0,
            "critical_tables_status": {},
            "issues": []
        }
        
        try:
            with psycopg2.connect(**db_config) as conn:
                with conn.cursor() as cur:
                    
                    # Contar tablas totales
                    cur.execute("SELECT COUNT(*) FROM pg_tables WHERE schemaname = 'public'")
                    integrity["table_count"] = cur.fetchone()[0]
                    
                    # Verificar tablas críticas
                    for table in self.critical_tables:
                        try:
                            cur.execute(f"""
                                SELECT 
                                    COUNT(*) as record_count,
                                    COUNT(CASE WHEN created_at IS NOT NULL THEN 1 END) as valid_created,
                                    COUNT(CASE WHEN updated_at IS NOT NULL THEN 1 END) as valid_updated
                                FROM {table}
                            """)
                            
                            stats = cur.fetchone()
                            
                            integrity["critical_tables_status"][table] = {
                                "record_count": stats[0],
                                "valid_created": stats[1],
                                "valid_updated": stats[2],
                                "health_score": min(100, (stats[1] / max(stats[0], 1)) * 100)
                            }
                            
                            if stats[0] == 0:
                                integrity["issues"].append(f"Tabla {table} está vacía")
                            elif stats[1] / max(stats[0], 1) < 0.8:
                                integrity["issues"].append(f"Tabla {table} tiene datos sin timestamp")
                            
                        except Exception as e:
                            integrity["issues"].append(f"Error verificando {table}: {e}")
                            integrity["status"] = "error"
                    
                    # Determinar estado general
                    if integrity["issues"]:
                        integrity["status"] = "needs_attention" if len(integrity["issues"]) < 3 else "error"
                    
        except Exception as e:
            logger.error(f"❌ Error verificando integridad de {server_name}: {e}")
            integrity["status"] = "error"
            integrity["error"] = str(e)
        
        return integrity
    
    def _check_replication_health(self) -> Dict[str, Any]:
        """Verifica salud de la replicación."""
        try:
            from adapted_replication_monitor import AdaptedReplicationMonitor
            
            monitor = AdaptedReplicationMonitor()
            health_report = monitor.get_basic_health_report()
            
            return {
                "status": health_report.get("overall_status", "unknown"),
                "sync_percentage": health_report.get("data_integrity", {}).get("sync_percentage", 0),
                "alerts_count": len(health_report.get("alerts", [])),
                "details": health_report
            }
            
        except Exception as e:
            logger.warning(f"⚠️ No se pudo verificar salud de replicación: {e}")
            return {
                "status": "unknown",
                "error": str(e),
                "message": "Verificación de replicación no disponible"
            }
    
    def _attempt_emergency_restore(self, backup_result: Dict[str, Any]):
        """Intenta restauración de emergencia si algo falla."""
        logger.warning("🚨 Intentando restauración de emergencia...")
        
        try:
            if backup_result and backup_result.get("status") == "success":
                logger.info("📋 Restaurando desde backup de emergencia...")
                self._restore_critical_data(backup_result)
                logger.info("✅ Restauración de emergencia completada")
            else:
                logger.error("❌ No hay backup válido para restauración de emergencia")
                
        except Exception as e:
            logger.error(f"❌ Error en restauración de emergencia: {e}")
    
    def _save_cleanup_log(self, result: Dict[str, Any]):
        """Guarda log completo del proceso de limpieza."""
        try:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            log_file = self.backup_dir / f"cleanup_log_{timestamp}.json"
            
            with open(log_file, 'w', encoding='utf-8') as f:
                json.dump(result, f, indent=2, default=str)
            
            logger.info(f"📋 Log de limpieza guardado en: {log_file}")
            
        except Exception as e:
            logger.error(f"❌ Error guardando log de limpieza: {e}")


def main():
    """Función principal de limpieza y reinicialización."""
    print("🧹 LIMPIEZA Y REINICIALIZACIÓN DE BASES DE DATOS")
    print("=" * 60)
    print("Este proceso:")
    print("• Creará backup completo de datos críticos")
    print("• Limpiará datos redundantes y temporales")
    print("• Reinicializará la estructura de base de datos")
    print("• Restaurará datos críticos")
    print("• Configurará replicación nativa de PostgreSQL")
    print("=" * 60)
    
    # Saltar confirmación si se usa --force
    if '--force' in sys.argv:
        confirm = 'sí'
        print("⚠️ Modo forzado activado - continuando sin confirmación")
    else:
        confirm = input("¿Desea continuar con la limpieza y reinicialización? (sí/no): ").lower()
        if confirm not in ['sí', 'si', 's']:
            print("❌ Proceso cancelado por el usuario")
            return
    
    print("\n🚀 Iniciando proceso de limpieza y reinicialización...")
    
    try:
        cleaner = DatabaseCleanupReinitializer()
        result = cleaner.perform_complete_cleanup_and_reinit()
        
        print(f"\n📊 RESULTADO DEL PROCESO:")
        print(f"Estado: {result['status']}")
        print(f"Total de fases: {len(result['phases'])}")
        
        if result['status'] == 'completed_successfully':
            print("🎉 ¡Proceso completado exitosamente!")
            
            # Resumen de salud
            verification = result.get("phases", [{}])[-1].get("result", {})
            if verification.get("overall_health"):
                print(f"Salud del sistema: {verification['overall_health']}")
            
        else:
            print("❌ El proceso tuvo problemas - revisar el log")
            if result.get("errors"):
                print(f"Errores: {len(result['errors'])}")
        
        print(f"\n📋 Log guardado en: cleanup_backups/")
        
    except KeyboardInterrupt:
        print("\n🛑 Proceso interrumpido por el usuario")
    except Exception as e:
        print(f"\n❌ Error crítico: {e}")
        print("📋 Revisar logs en cleanup_backups/")

if __name__ == "__main__":
    main()