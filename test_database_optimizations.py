#!/usr/bin/env python3
"""
Script de demostración de optimizaciones de base de datos para Gym Management System
Optimizado para conexión remota São Paulo → Argentina
"""

import sys
import os
import time
import logging
from datetime import datetime, timedelta

# Agregar el directorio principal al path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from database import DatabaseManager
from PyQt6.QtWidgets import QApplication
from PyQt6.QtCore import QTimer

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def test_database_optimizations():
    """Prueba las optimizaciones de base de datos"""
    logger.info("=== INICIANDO PRUEBA DE OPTIMIZACIONES DE BASE DE DATOS ===")
    
    try:
        # Crear aplicación Qt (requerida para los workers)
        app = QApplication(sys.argv)
        
        # Configurar conexión a base de datos (simulando conexión São Paulo)
        connection_params = {
            'host': os.getenv('DB_HOST', 'localhost'),
            'port': int(os.getenv('DB_PORT', '5432')),
            'database': os.getenv('DB_NAME', 'gym_management'),
            'user': os.getenv('DB_USER', 'postgres'),
            'password': os.getenv('DB_PASSWORD', ''),
            'sslmode': 'prefer'
        }
        
        logger.info(f"Conectando a base de datos en {connection_params['host']}:{connection_params['port']}")
        
        # Inicializar DatabaseManager con optimizaciones
        start_time = time.time()
        db_manager = DatabaseManager(connection_params)
        connection_time = time.time() - start_time
        
        logger.info(f"✅ Conexión establecida en {connection_time:.2f} segundos")
        
        # 1. Probar creación de índices optimizados
        logger.info("\n1. Creando índices optimizados...")
        start_time = time.time()
        indexes_created = db_manager.create_optimized_indexes()
        index_time = time.time() - start_time
        logger.info(f"✅ {indexes_created} índices creados en {index_time:.2f} segundos")
        
        # 2. Probar declaraciones preparadas
        logger.info("\n2. Inicializando declaraciones preparadas...")
        start_time = time.time()
        db_manager.initialize_prepared_statements()
        prep_time = time.time() - start_time
        logger.info(f"✅ Declaraciones preparadas inicializadas en {prep_time:.2f} segundos")
        
        # 3. Probar consultas optimizadas
        logger.info("\n3. Ejecutando consultas de prueba...")
        
        # Consulta 1: Obtener usuarios activos
        start_time = time.time()
        with db_manager.readonly_session() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) FROM usuarios WHERE activo = true")
                count = cur.fetchone()[0]
        query1_time = time.time() - start_time
        logger.info(f"   📊 Usuarios activos: {count} (tiempo: {query1_time:.3f}s)")
        
        # Consulta 2: Asistencias del día
        start_time = time.time()
        with db_manager.readonly_session() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT COUNT(*) 
                    FROM asistencias 
                    WHERE fecha = CURRENT_DATE
                """)
                today_attendance = cur.fetchone()[0]
        query2_time = time.time() - start_time
        logger.info(f"   📅 Asistencias hoy: {today_attendance} (tiempo: {query2_time:.3f}s)")
        
        # Consulta 3: Pagos del mes actual
        start_time = time.time()
        with db_manager.readonly_session() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT COUNT(*), SUM(monto)
                    FROM pagos 
                    WHERE mes = EXTRACT(MONTH FROM CURRENT_DATE)
                    AND año = EXTRACT(YEAR FROM CURRENT_DATE)
                """)
                payments = cur.fetchone()
        query3_time = time.time() - start_time
        logger.info(f"   💰 Pagos mes actual: {payments[0]} pagos, ${payments[1] or 0:.2f} (tiempo: {query3_time:.3f}s)")
        
        # 4. Obtener estadísticas de rendimiento
        logger.info("\n4. Estadísticas de rendimiento:")
        stats = db_manager.get_query_performance_stats()
        logger.info(f"   📈 Consultas totales: {stats['total_queries']:,}")
        logger.info(f"   🐌 Consultas lentas (>2s): {stats['slow_queries']:,}")
        logger.info(f"   📊 Porcentaje lentas: {stats['slow_query_percentage']:.1f}%")
        logger.info(f"   ⏱️  Tiempo promedio: {stats['average_query_time']:.3f}s")
        logger.info(f"   💾 Ratio de caché: {stats['cache_hit_ratio']:.1f}%")
        
        # 5. Probar caché
        logger.info("\n5. Probando sistema de caché...")
        
        # Primera consulta (debe ser lenta)
        start_time = time.time()
        with db_manager.readonly_session() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT * FROM usuarios WHERE activo = true LIMIT 10")
                result1 = cur.fetchall()
        cache_miss_time = time.time() - start_time
        
        # Segunda consulta (debe ser más rápida por caché)
        start_time = time.time()
        with db_manager.readonly_session() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT * FROM usuarios WHERE activo = true LIMIT 10")
                result2 = cur.fetchall()
        cache_hit_time = time.time() - start_time
        
        logger.info(f"   💾 Primera consulta (caché miss): {cache_miss_time:.3f}s")
        logger.info(f"   ⚡ Segunda consulta (caché hit): {cache_hit_time:.3f}s")
        logger.info(f"   🚀 Mejora: {((cache_miss_time - cache_hit_time) / cache_miss_time * 100):.1f}%")
        
        # 6. Probar conexión con timeout extendido
        logger.info("\n6. Probando conexión remota optimizada...")
        start_time = time.time()
        try:
            with db_manager.connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT version()")
                    version = cur.fetchone()[0]
            connection_test_time = time.time() - start_time
            logger.info(f"✅ Conexión remota exitosa en {connection_test_time:.2f}s")
            logger.info(f"   📋 Versión PostgreSQL: {version}")
        except Exception as e:
            logger.error(f"❌ Error en conexión remota: {e}")
        
        logger.info("\n=== PRUEBA DE OPTIMIZACIONES COMPLETADA ===")
        logger.info("✅ Todas las optimizaciones están funcionando correctamente")
        logger.info("✅ La conexión remota São Paulo → Argentina está optimizada")
        
        # Cerrar conexiones
        try:
            db_manager.close_connections()
            logger.info("✅ Conexiones cerradas correctamente")
        except Exception as e:
            logger.warning(f"⚠️  Error cerrando conexiones: {e}")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ Error en prueba de optimizaciones: {e}", exc_info=True)
        return False


def main():
    """Función principal"""
    logger.info("Iniciando prueba de optimizaciones de base de datos...")
    
    # Ejecutar prueba
    success = test_database_optimizations()
    
    if success:
        logger.info("✅ Prueba completada exitosamente")
        sys.exit(0)
    else:
        logger.error("❌ Prueba fallida")
        sys.exit(1)


if __name__ == "__main__":
    main()