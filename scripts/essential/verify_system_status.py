#!/usr/bin/env python3
"""
Verificación de Estado del Sistema

Este script verifica el estado completo del sistema tras la transición
a base de datos única Neon (sin replicación ni colas locales).
"""

import psycopg2
import logging
from datetime import datetime
from secure_config import SecureConfig

# Crear instancia de configuración segura
secure_config = SecureConfig()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def check_tables_status():
    """Verifica el estado de las tablas críticas."""
    logger.info("📊 Verificando estado de tablas...")
    
    local_config = secure_config.get_db_config('local')
    critical_tables = [
        'usuarios', 'pagos', 'asistencias', 'clases', 'profesores',
        'tipos_clases', 'metodos_pago', 'conceptos_pago', 'configuracion'
    ]
    
    results = {}
    
    try:
        with psycopg2.connect(**local_config) as conn:
            with conn.cursor() as cur:
                for table in critical_tables:
                    try:
                        cur.execute(f"""
                            SELECT COUNT(*) as total,
                                   MAX(created_at) as latest_record
                            FROM public.{table}
                        """)
                        result = cur.fetchone()
                        
                        cur.execute(f"""
                            SELECT column_name, data_type 
                            FROM information_schema.columns
                            WHERE table_name = %s AND table_schema = 'public'
                            ORDER BY ordinal_position
                        """, (table,))
                        columns = cur.fetchall()
                        
                        results[table] = {
                            'exists': True,
                            'row_count': result[0],
                            'latest_record': str(result[1]) if result[1] else None,
                            'columns': len(columns)
                        }
                            
                    except Exception as e:
                        results[table] = {
                            'exists': False,
                            'error': str(e)
                        }
                        
        logger.info("✅ Verificación de tablas completada")
        
    except Exception as e:
        logger.error(f"❌ Error verificando tablas: {e}")
        results['error'] = str(e)
    
    return results

def check_sync_artifacts_cleanup():
    """Verifica que los artefactos antiguos de sincronización hayan sido eliminados."""
    logger.info("🧹 Verificando limpieza de artefactos de sincronización antiguos...")
    
    local_config = secure_config.get_db_config('local')
    
    results = {}
    sync_artifacts = [
        'sync_outbox',
        'sync_inbox', 
        'sync_outbox_capture',
        'sync_outbox_ins',
        'sync_outbox_upd',
        'sync_outbox_del'
    ]
    
    try:
        with psycopg2.connect(**local_config) as conn:
            with conn.cursor() as cur:
                for obj in sync_artifacts:
                    try:
                        # Verificar si existe la tabla
                        cur.execute("""
                            SELECT EXISTS (
                                SELECT 1 FROM information_schema.tables 
                                WHERE table_name = %s AND table_schema = 'public'
                            )
                        """, (obj,))
                        table_exists = cur.fetchone()[0]
                        
                        # Verificar si existe la función
                        cur.execute("""
                            SELECT EXISTS (
                                SELECT 1 FROM pg_proc 
                                WHERE proname = %s
                            )
                        """, (obj,))
                        func_exists = cur.fetchone()[0]
                        
                        results[obj] = {
                            'table_exists': table_exists,
                            'function_exists': func_exists,
                            'status': 'REMOVED' if not table_exists and not func_exists else 'PRESENT'
                        }
                        
                    except Exception as e:
                        results[obj] = {
                            'error': str(e)
                        }
                        
        logger.info("✅ Verificación de limpieza de artefactos antiguos completada")
        
    except Exception as e:
        logger.error(f"❌ Error verificando limpieza de artefactos antiguos: {e}")
        results['error'] = str(e)
    
    return results

def main():
    """Función principal de verificación."""
    logger.info("🚀 Iniciando verificación completa del sistema...")
    logger.info(f"📅 Timestamp: {datetime.now().isoformat()}")
    
    report = {
        'timestamp': datetime.now().isoformat(),
        'replication_status': {'status': 'disabled', 'message': 'Replicación deshabilitada - se usa base de datos única Neon'},
        'tables_status': check_tables_status(),
        'sync_artifacts_cleanup': check_sync_artifacts_cleanup()
    }
    
    # Análisis y resumen
    logger.info("\n" + "="*60)
    logger.info("📋 RESUMEN DE VERIFICACIÓN")
    logger.info("="*60)
    
    # Replicación
    logger.info("📡 Replicación: ❌ Deshabilitada - usando base de datos única Neon")
    
    # Tablas
    tables = report['tables_status']
    if 'error' not in tables:
        total_rows = sum(t.get('row_count', 0) for t in tables.values() if t.get('exists'))
        existing_tables = [t for t, data in tables.items() if data.get('exists')]
        logger.info(f"📊 Tablas: {len(existing_tables)} existentes, {total_rows} filas totales")
    
    # Limpieza de artefactos antiguos
    cleanup = report['sync_artifacts_cleanup']
    if 'error' not in cleanup:
        removed = sum(1 for data in cleanup.values() if data.get('status') == 'REMOVED')
        total = len(cleanup)
        logger.info(f"🧹 Artefactos antiguos: {removed}/{total} objetos removidos")
    
    logger.info("\n✅ Verificación completada")
    
    # Guardar reporte
    import json
    report_file = f"system_verification_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    with open(report_file, 'w', encoding='utf-8') as f:
        json.dump(report, f, indent=2, ensure_ascii=False)
    
    logger.info(f"📄 Reporte guardado: {report_file}")
    
    return 0

if __name__ == "__main__":
    main()