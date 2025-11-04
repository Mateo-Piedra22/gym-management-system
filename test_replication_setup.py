#!/usr/bin/env python3
"""
Script de prueba para el sistema de replicación nativa.
"""

from secure_config import config as secure_config
import psycopg2

def test_replication_setup():
    """Prueba la configuración de replicación."""
    print("🔧 Probando configuración de replicación nativa...")
    
    try:
        # Obtener configuraciones
        local_config = secure_config.get_db_config('local')
        remote_config = secure_config.get_db_config('remote')
        
        print("✅ Configuraciones de base de datos obtenidas")
        print(f"📋 Local: {local_config['host']}:{local_config['port']}")
        print(f"🌐 Remote: {remote_config['host']}:{remote_config['port']}")
        
        # Probar conexión local
        print("📋 Probando conexión local...")
        conn = psycopg2.connect(**local_config)
        with conn.cursor() as cur:
            cur.execute("SELECT version()")
            version = cur.fetchone()[0]
            print(f"✅ Conexión local exitosa: {version[:50]}...")
        conn.close()
        
        # Probar conexión remota
        print("🌐 Probando conexión remota...")
        conn = psycopg2.connect(**remote_config)
        with conn.cursor() as cur:
            cur.execute("SELECT current_database(), current_user")
            db_info = cur.fetchone()
            print(f"✅ Conexión remota exitosa: DB={db_info[0]}, User={db_info[1]}")
        conn.close()
        
        # Verificar si ya existe replicación
        print("🔍 Verificando estado de replicación actual...")
        
        # Verificar en local
        conn = psycopg2.connect(**local_config)
        with conn.cursor() as cur:
            cur.execute("""
                SELECT COUNT(*) 
                FROM pg_publication 
                WHERE pubname = 'gym_pub'
            """)
            local_pubs = cur.fetchone()[0]
            print(f"📊 Publicaciones locales: {local_pubs}")
        conn.close()
        
        # Verificar en remoto
        conn = psycopg2.connect(**remote_config)
        with conn.cursor() as cur:
            cur.execute("""
                SELECT COUNT(*) 
                FROM pg_subscription 
                WHERE subname = 'gym_sub'
            """)
            remote_subs = cur.fetchone()[0]
            print(f"📊 Suscripciones remotas: {remote_subs}")
        conn.close()
        
        print("🎯 Sistema listo para migración a replicación nativa")
        return True
        
    except Exception as e:
        print(f"❌ Error: {e}")
        return False

if __name__ == "__main__":
    success = test_replication_setup()
    if success:
        print("\n🎉 ¡Prueba exitosa! El sistema está listo para la modernización.")
    else:
        print("\n❌ Prueba fallida. Revisa los errores anteriores.")