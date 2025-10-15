#!/usr/bin/env python3
"""
Script para inicializar la base de datos y crear las tablas faltantes
"""

import os
import sys
from database import DatabaseManager

def main():
    """Inicializa la base de datos y crea las tablas faltantes"""
    try:
        print("Inicializando la base de datos...")
        
        # Crear instancia del DatabaseManager
        db_manager = DatabaseManager()
        
        # Inicializar la base de datos (esto creará todas las tablas)
        db_manager.inicializar_base_datos()
        
        print("✅ Base de datos inicializada correctamente")
        print("✅ Todas las tablas han sido creadas o verificadas")
        
        # Verificar que la tabla clase_asistencia_historial existe
        with db_manager.get_connection_context() as conn:
            with conn.cursor() as cursor:
                cursor.execute("""
                    SELECT table_name 
                    FROM information_schema.tables 
                    WHERE table_schema = 'public' 
                    AND table_name = 'clase_asistencia_historial'
                """)
                result = cursor.fetchone()
                
                if result:
                    print("✅ Tabla 'clase_asistencia_historial' creada exitosamente")
                    
                    # Mostrar estructura de la tabla
                    cursor.execute("""
                        SELECT column_name, data_type, is_nullable, column_default
                        FROM information_schema.columns
                        WHERE table_name = 'clase_asistencia_historial'
                        ORDER BY ordinal_position
                    """)
                    columns = cursor.fetchall()
                    
                    print("\n📋 Estructura de la tabla 'clase_asistencia_historial':")
                    for col in columns:
                        print(f"  - {col[0]}: {col[1]} ({'NULL' if col[2] == 'YES' else 'NOT NULL'})")
                else:
                    print("❌ Error: La tabla 'clase_asistencia_historial' no fue creada")
                    return 1
        
        return 0
        
    except Exception as e:
        print(f"❌ Error al inicializar la base de datos: {e}")
        return 1

if __name__ == "__main__":
    sys.exit(main())