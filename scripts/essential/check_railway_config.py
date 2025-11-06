#!/usr/bin/env python3
"""
Verificar configuración básica de PostgreSQL (Neon/base única, sin replicación)
"""

import os
import sys
import psycopg2
from pathlib import Path

# Agregar directorio raíz al path
sys.path.insert(0, str(Path(__file__).parent.parent))

try:
    from secure_config import SecureConfig
except ImportError:
    print("❌ Error: No se pudo importar secure_config")
    sys.exit(1)

def check_railway_basic_config():
    """Verificar configuración básica de PostgreSQL (Neon/base única, sin replicación)"""
    config = SecureConfig()
    
    print("🔍 Verificando configuración básica de PostgreSQL (Neon/base única)...")
    print("=" * 60)
    
    try:
        # Conectar a Neon/base única (perfil local)
        import psycopg2
        
        # Obtener configuración local
        local_config = config.get_db_config('local')
        
        # Construir DSN
        conn = psycopg2.connect(
            host=local_config['host'],
            port=local_config['port'],
            database=local_config['database'],
            user=local_config['user'],
            password=local_config['password'],
            sslmode=local_config.get('sslmode', 'require'),
            connect_timeout=local_config.get('connect_timeout', 10),
            application_name=local_config.get('application_name', 'gym_management_system')
        )
        
        with conn.cursor() as cursor:
            # Verificar versión de PostgreSQL
            cursor.execute("SELECT version();")
            version = cursor.fetchone()[0]
            print(f"✅ Versión PostgreSQL: {version}")
            
            # Verificar WAL level
            cursor.execute("SHOW wal_level;")
            wal_level = cursor.fetchone()[0]
            print(f"📊 WAL level actual: {wal_level}")
            
            # Verificar configuración básica (sin replicación)
            print("\n🔧 Configuración básica de Neon/PostgreSQL:")
            
            basic_configs = [
                'wal_level',
                'track_commit_timestamp',
                'max_worker_processes'
            ]
            
            for config_name in basic_configs:
                try:
                    cursor.execute(f"SHOW {config_name};")
                    value = cursor.fetchone()[0]
                    print(f"   {config_name}: {value}")
                except psycopg2.Error as e:
                    print(f"   {config_name}: No disponible - {e}")
            
            print("\nℹ️  Verificación de replicación omitida - se usa base de datos única Neon")
            
        conn.close()
        
        # Análisis de compatibilidad (sin replicación)
        print("\n" + "=" * 60)
        print("📋 ANÁLISIS DE COMPATIBILIDAD:")
        print("ℹ️  Usando base de datos única Neon - replicación deshabilitada")
        
        if wal_level.lower() == 'logical':
            print("✅ WAL level es 'logical' - configuración adecuada")
        else:
            print(f"ℹ️  WAL level es '{wal_level}' - adecuado para base de datos única")
        
        print("\n🎯 CONCLUSIÓN:")
        print("✅ PostgreSQL (Neon) está CONFIGURADO para uso con base de datos única")
        print("   No se requiere configuración de replicación")
        
        return True
        
    except psycopg2.Error as e:
        print(f"❌ Error de conexión: {e}")
        print("   Verifica las variables DB_LOCAL_* en tu .env")
        return False
    except Exception as e:
        print(f"❌ Error inesperado: {e}")
        return False

if __name__ == "__main__":
    success = check_railway_basic_config()
    sys.exit(0 if success else 1)