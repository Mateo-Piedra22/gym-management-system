#!/usr/bin/env python3
"""
Verificación de Estado del Sistema - Post Limpieza Legacy

Este script verifica el estado completo del sistema después de la limpieza
total del sistema outbox legacy y la implementación de replicación nativa.
"""

import psycopg2
import logging
from datetime import datetime
from secure_config import config as secure_config

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def check_replication_status():
    """Verifica el estado de la replicación nativa PostgreSQL."""
    logger.info("🔍 Verificando estado de replicación nativa...")
    
    local_config = secure_config.get_db_config('local')
    remote_config = secure_config.get_db_config('remote')
    
    results = {}
    
    try:
        # Verificar publicación local
        with psycopg2.connect(**local_config) as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT pubname, puballtables 
                    FROM pg_publication 
                    WHERE pubname = 'gym_pub'
                """)
                pub_result = cur.fetchone()
                results['publication'] = {
                    'exists': bool(pub_result),
                    'all_tables': pub_result[1] if pub_result else False
                }
                
                # Verificar tablas en publicación
                cur.execute("""
                    SELECT schemaname, tablename
                    FROM pg_publication_tables
                    WHERE pubname = 'gym_pub'
                    ORDER BY tablename
                """)
                tables = cur.fetchall()
                results['publication']['tables'] = [f"{t[0]}.{t[1]}" for t in tables]
                
                # Verificar suscripción
                cur.execute("""
                    SELECT subname, subenabled, subconninfo
                    FROM pg_subscription
                    WHERE subname = 'gym_sub'
                """)
                sub_result = cur.fetchone()
                results['subscription'] = {
                    'exists': bool(sub_result),
                    'enabled': sub_result[1] if sub_result else False,
                    'connection': 'configured' if sub_result else 'none'
                }
                
                # Verificar workers de replicación
                cur.execute("""
                    SELECT pid, application_name, state, sync_state
                    FROM pg_stat_replication
                    ORDER BY pid
                """)
                workers = cur.fetchall()
                results['replication_workers'] = [
                    {
                        'pid': w[0],
                        'application': w[1],
                        'state': w[2],
                        'sync_state': w[3]
                    } for w in workers
                ]
                
                # Verificar estado de réplica
                cur.execute("SELECT pg_is_in_recovery()")
                results['is_replica'] = not cur.fetchone()[0]
                
        logger.info("✅ Verificación de replicación completada")
        
    except Exception as e:
        logger.error(f"❌ Error verificando replicación: {e}")
        results['error'] = str(e)
    
    return results

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
                            'columns': len(columns),
                            'replica_identity': 'unknown'
                        }
                        
                        # Verificar replica identity
                        try:
                            cur.execute(f"""
                                SELECT relreplident
                                FROM pg_class
                                WHERE relname = %s
                            """, (table,))
                            replica_id = cur.fetchone()
                            if replica_id:
                                results[table]['replica_identity'] = replica_id[0]
                        except Exception:
                            pass
                            
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

def check_legacy_cleanup():
    """Verifica que los artefactos legacy hayan sido eliminados."""
    logger.info("🧹 Verificando limpieza de artefactos legacy...")
    
    local_config = secure_config.get_db_config('local')
    
    results = {}
    legacy_objects = [
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
                for obj in legacy_objects:
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
                        
        logger.info("✅ Verificación de limpieza legacy completada")
        
    except Exception as e:
        logger.error(f"❌ Error verificando limpieza legacy: {e}")
        results['error'] = str(e)
    
    return results

def main():
    """Función principal de verificación."""
    logger.info("🚀 Iniciando verificación completa del sistema...")
    logger.info(f"📅 Timestamp: {datetime.now().isoformat()}")
    
    report = {
        'timestamp': datetime.now().isoformat(),
        'replication_status': check_replication_status(),
        'tables_status': check_tables_status(),
        'legacy_cleanup': check_legacy_cleanup()
    }
    
    # Análisis y resumen
    logger.info("\n" + "="*60)
    logger.info("📋 RESUMEN DE VERIFICACIÓN")
    logger.info("="*60)
    
    # Replicación
    repl = report['replication_status']
    if 'error' in repl:
        logger.error(f"❌ Replicación: ERROR - {repl['error']}")
    else:
        pub = repl.get('publication', {})
        sub = repl.get('subscription', {})
        workers = repl.get('replication_workers', [])
        
        logger.info(f"📢 Publicación: {'✅' if pub.get('exists') else '❌'} {pub.get('tables', [])}")
        logger.info(f"📥 Suscripción: {'✅' if sub.get('exists') and sub.get('enabled') else '❌'}")
        logger.info(f"👷 Workers: {len(workers)} activos")
    
    # Tablas
    tables = report['tables_status']
    if 'error' not in tables:
        total_rows = sum(t.get('row_count', 0) for t in tables.values() if t.get('exists'))
        existing_tables = [t for t, data in tables.items() if data.get('exists')]
        logger.info(f"📊 Tablas: {len(existing_tables)} existentes, {total_rows} filas totales")
    
    # Legacy cleanup
    cleanup = report['legacy_cleanup']
    if 'error' not in cleanup:
        removed = sum(1 for data in cleanup.values() if data.get('status') == 'REMOVED')
        total = len(cleanup)
        logger.info(f"🧹 Legacy: {removed}/{total} objetos removidos")
    
    logger.info("\n✅ Verificación completada")
    
    # Guardar reporte
    import json
    report_file = f"system_verification_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    with open(report_file, 'w', encoding='utf-8') as f:
        json.dump(report, f, indent=2, ensure_ascii=False)
    
    logger.info(f"📄 Reporte guardado: {report_file}")
    
    return report

if __name__ == "__main__":
    main()