#!/usr/bin/env python3
"""
Gym Management System - First Time Setup
Configuración automatizada para nueva instalación

Este script automatiza el proceso de instalación inicial incluyendo:
- Configuración de bases de datos local y remota
- Verificación de requisitos de replicación nativa
- Creación de esquemas y tablas
- Configuración de usuario administrador
- Activación de replicación bidireccional
"""

import os
import sys
import json
import psycopg2
import subprocess
from pathlib import Path
from datetime import datetime
from typing import Dict, Any, Optional

# Agregar directorio raíz al path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

try:
    from secure_config import SecureConfig
except ImportError as e:
    print(f"❌ Error de importación: {e}")
    print("Asegúrate de que todos los módulos estén en el directorio correcto")
    sys.exit(1)

class FirstTimeSetup:
    def __init__(self):
        self.config = SecureConfig()
        self.setup_log = []
        
    def log(self, message: str, level: str = "INFO"):
        """Registrar mensaje con timestamp"""
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        log_entry = f"[{timestamp}] [{level}] {message}"
        self.setup_log.append(log_entry)
        print(log_entry)
        
    def check_python_version(self) -> bool:
        """Verificar versión de Python"""
        self.log("Verificando versión de Python...")
        version = sys.version_info
        if version.major >= 3 and version.minor >= 8:
            self.log(f"✅ Python {version.major}.{version.minor}.{version.micro} compatible")
            return True
        else:
            self.log(f"❌ Python {version.major}.{version.minor} no es compatible. Requiere 3.8+", "ERROR")
            return False
            
    def check_postgresql_requirements(self) -> bool:
        """Verificar requisitos básicos de PostgreSQL"""
        self.log("Verificando requisitos de PostgreSQL...")
        
        try:
            # Conectar a base de datos usando configuración
            import psycopg2
            db_config = self.config.get_db_config('local')
            conn = psycopg2.connect(
                host=db_config['host'],
                port=db_config['port'],
                database=db_config['database'],
                user=db_config['user'],
                password=db_config['password'],
                sslmode=db_config['sslmode'],
                connect_timeout=db_config['connect_timeout'],
                application_name=db_config['application_name']
            )
            with conn.cursor() as cursor:
                # Verificar versión de PostgreSQL
                cursor.execute("SELECT version();")
                version = cursor.fetchone()[0]
                self.log(f"Versión PostgreSQL: {version}")
                
            conn.close()
            self.log("✅ Requisitos de PostgreSQL verificados")
            return True
            
        except Exception as e:
            self.log(f"❌ Error verificando PostgreSQL: {e}", "ERROR")
            return False
            
    def test_database_connections(self) -> Dict[str, bool]:
        """Probar conexión a base de datos"""
        self.log("Probando conexión a base de datos...")
        results = {"database": False}
        
        try:
            # Probar conexión a base de datos
            import psycopg2
            db_config = self.config.get_db_config('local')
            conn = psycopg2.connect(
                host=db_config['host'],
                port=db_config['port'],
                database=db_config['database'],
                user=db_config['user'],
                password=db_config['password'],
                sslmode=db_config['sslmode'],
                connect_timeout=db_config['connect_timeout'],
                application_name=db_config['application_name']
            )
            conn.close()
            results["database"] = True
            self.log("✅ Conexión a base de datos exitosa")
        except Exception as e:
            self.log(f"❌ Error conexión a base de datos: {e}", "ERROR")
            
        return results
        
    def create_database_schemas(self) -> bool:
        """Crear esquemas y tablas base"""
        self.log("Creando esquemas de base de datos...")
        
        try:
            import psycopg2
            local_config = self.config.get_db_config('local')
            
            # Primero, conectar a la base de datos postgres para crear la base de datos si no existe
            try:
                postgres_conn = psycopg2.connect(
                    host=local_config['host'],
                    port=local_config['port'],
                    database='postgres',  # Base de datos del sistema
                    user=local_config['user'],
                    password=local_config['password'],
                    sslmode=local_config['sslmode'],
                    connect_timeout=local_config['connect_timeout'],
                    application_name=local_config['application_name']
                )
                postgres_conn.autocommit = True
                
                with postgres_conn.cursor() as cursor:
                    # Verificar si la base de datos existe
                    cursor.execute("SELECT 1 FROM pg_database WHERE datname = %s", (local_config['database'],))
                    if not cursor.fetchone():
                        # Crear la base de datos
                        cursor.execute(f"CREATE DATABASE {local_config['database']}")
                        self.log(f"✅ Base de datos '{local_config['database']}' creada exitosamente")
                    else:
                        self.log(f"ℹ️  Base de datos '{local_config['database']}' ya existe")
                
                postgres_conn.close()
            except Exception as e:
                self.log(f"⚠️  No se pudo verificar/crear la base de datos: {e}")
            
            # Ahora conectar a la base de datos destino
            local_conn = psycopg2.connect(
                host=local_config['host'],
                port=local_config['port'],
                database=local_config['database'],
                user=local_config['user'],
                password=local_config['password'],
                sslmode=local_config['sslmode'],
                connect_timeout=local_config['connect_timeout'],
                application_name=local_config['application_name']
            )
            
            # Crear esquema básico manualmente
            with local_conn.cursor() as cursor:
                # Crear tabla de usuarios
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS usuarios (
                        id SERIAL PRIMARY KEY,
                        nombre VARCHAR(100) NOT NULL,
                        email VARCHAR(100) UNIQUE NOT NULL,
                        password VARCHAR(255) NOT NULL,
                        rol VARCHAR(20) DEFAULT 'user' CHECK (rol IN ('admin', 'user')),
                        activo BOOLEAN DEFAULT true,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    );
                """)
                
                # Crear tabla de configuración
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS configuracion (
                        id SERIAL PRIMARY KEY,
                        clave VARCHAR(100) UNIQUE NOT NULL,
                        valor TEXT,
                        descripcion TEXT,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    );
                """)
                
                # Insertar configuración por defecto
                cursor.execute("""
                    INSERT INTO configuracion (clave, valor, descripcion) VALUES
                    ('gym_nombre', 'Gym Management System', 'Nombre del gimnasio'),
                    ('gym_direccion', 'Dirección del gimnasio', 'Dirección del gimnasio'),
                    ('gym_telefono', '000-0000', 'Teléfono del gimnasio'),
                    ('gym_email', 'info@gym.com', 'Email del gimnasio')
                    ON CONFLICT (clave) DO NOTHING;
                """)
                
                local_conn.commit()
                self.log("✅ Esquemas básicos creados exitosamente")
                local_conn.close()
                return True
                
        except Exception as e:
            self.log(f"❌ Error creando esquemas: {e}", "ERROR")
            return False
            
    def setup_native_replication(self) -> bool:
        """Configurar replicación nativa bidireccional - DEPRECATED"""
        self.log("ℹ️  Replicación nativa deshabilitada - usando base de datos única Neon")
        return True
            
    def create_admin_user(self) -> bool:
        """Crear usuario administrador inicial"""
        self.log("Creando usuario administrador...")
        
        try:
            import psycopg2
            local_config = self.config.get_db_config('local')
            local_conn = psycopg2.connect(
                host=local_config['host'],
                port=local_config['port'],
                database=local_config['database'],
                user=local_config['user'],
                password=local_config['password'],
                sslmode=local_config['sslmode'],
                connect_timeout=local_config['connect_timeout'],
                application_name=local_config['application_name']
            )
            
            # Verificar si ya existe un administrador
            with local_conn.cursor() as cursor:
                cursor.execute("SELECT COUNT(*) FROM usuarios WHERE rol = 'admin';")
                admin_count = cursor.fetchone()[0]
                
                if admin_count > 0:
                    self.log("✅ Usuario administrador ya existe")
                    local_conn.close()
                    return True
                    
                # Crear usuario admin por defecto
                import bcrypt
                admin_password = bcrypt.hashpw("admin123".encode('utf-8'), bcrypt.gensalt()).decode('utf-8')
                cursor.execute("""
                    INSERT INTO usuarios (nombre, email, password, rol, activo)
                    VALUES (%s, %s, %s, %s, %s)
                """, ("Administrador", "admin@gym.com", admin_password, "admin", True))
                
                local_conn.commit()
                self.log("✅ Usuario administrador creado (admin@gym.com / admin123)")
                
            local_conn.close()
            return True
            
        except Exception as e:
            self.log(f"❌ Error creando administrador: {e}", "ERROR")
            return False
            
    def verify_system_status(self) -> bool:
        """Verificar estado final del sistema"""
        self.log("Verificando estado final del sistema...")
        
        try:
            # Ejecutar verificación del sistema
            result = subprocess.run([
                sys.executable, "scripts/verify_system_status.py"
            ], capture_output=True, text=True)
            
            if result.returncode == 0:
                self.log("✅ Verificación del sistema completada")
                self.log(f"Salida: {result.stdout}")
                return True
            else:
                self.log(f"❌ Error en verificación: {result.stderr}", "ERROR")
                return False
                
        except Exception as e:
            self.log(f"❌ Error ejecutando verificación: {e}", "ERROR")
            return False
            
    def save_setup_report(self):
        """Guardar reporte de instalación"""
        report = {
            "timestamp": datetime.now().isoformat(),
            "status": "completed" if self.success else "failed",
            "log": self.setup_log,
            "summary": self.summary
        }
        
        report_path = f"setup_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(report_path, 'w', encoding='utf-8') as f:
            json.dump(report, f, indent=2, ensure_ascii=False)
            
        self.log(f"Reporte guardado en: {report_path}")
        
    def run_setup(self) -> bool:
        """Ejecutar proceso completo de instalación optimizado"""
        self.log("🚀 Iniciando configuración inicial optimizada del Gym Management System")
        self.log("=" * 60)
        
        self.success = False
        self.summary = []
        setup_start_time = datetime.now()
        
        # Optimización: Ejecutar verificaciones en paralelo donde sea posible
        self.log("⚡ Ejecutando verificaciones iniciales optimizadas...")
        
        # Paso 1: Verificar Python (rápido y crítico)
        if not self.check_python_version():
            self.summary.append("❌ Falló verificación de Python")
            return False
        self.summary.append("✅ Python verificado")
        
        # Paso 2: Verificar requisitos PostgreSQL y conexiones en paralelo
        self.log("🔍 Verificando requisitos de PostgreSQL y conexiones simultáneamente...")
        
        import threading
        import queue
        
        results_queue = queue.Queue()
        
        def check_postgres_thread():
            result = self.check_postgresql_requirements()
            results_queue.put(('postgres', result))
        
        def test_connections_thread():
            result = self.test_database_connections()
            results_queue.put(('connections', result))
        
        # Iniciar threads
        postgres_thread = threading.Thread(target=check_postgres_thread)
        connections_thread = threading.Thread(target=test_connections_thread)
        
        postgres_thread.start()
        connections_thread.start()
        
        # Esperar resultados
        postgres_thread.join(timeout=30)
        connections_thread.join(timeout=30)
        
        # Procesar resultados
        postgres_ok = False
        connections_ok = False
        connections = {"local": False, "remote": False}
        
        while not results_queue.empty():
            check_type, result = results_queue.get()
            if check_type == 'postgres':
                postgres_ok = result
            elif check_type == 'connections':
                connections = result
                connections_ok = connections["local"] and connections["remote"]
        
        if not postgres_ok:
            self.summary.append("⚠️  PostgreSQL requiere configuración manual")
        
        if not connections_ok:
            self.summary.append("❌ Falló conexión a bases de datos")
            return False
        self.summary.append("✅ Conexiones a bases de datos verificadas")
        
        # Paso 3: Crear esquemas (sin replicación)
        self.log("🏗️  Configurando esquemas de base de datos...")
        
        schema_success = self.create_database_schemas()
        
        if not schema_success:
            self.summary.append("⚠️  Esquemas ya existen o requieren creación manual")
        else:
            self.summary.append("✅ Esquemas configurados exitosamente")
        
        # Paso 4: Crear usuario admin y verificar sistema (rápido)
        if not self.create_admin_user():
            self.summary.append("⚠️  Usuario admin ya existe o error en creación")
        
        if not self.verify_system_status():
            self.summary.append("⚠️  Verificación del sistema con advertencias")
        
        # Calcular tiempo total
        setup_end_time = datetime.now()
        setup_duration = (setup_end_time - setup_start_time).total_seconds()
        
        self.success = True
        self.log("=" * 60)
        self.log("✅ Configuración inicial completada exitosamente!")
        self.log(f"⏱️  Tiempo total de configuración: {setup_duration:.1f} segundos")
        
        # Validar contra objetivo de 2 minutos
        if setup_duration > 120:
            self.log(f"⚠️  Tiempo excede objetivo de 2 minutos ({setup_duration:.1f}s)")
        else:
            self.log(f"🎯 Tiempo dentro del objetivo de 2 minutos")
        
        self.log("📋 Resumen de instalación:")
        for item in self.summary:
            self.log(f"   {item}")
            
        self.log("\n🎯 Próximos pasos:")
        self.log("   1. Verificar que la aplicación se inicie correctamente")
        self.log("   2. Cambiar contraseña del administrador (admin@gym.com)")
        self.log("   3. Configurar datos de tu gimnasio")
        self.log("   4. Sistema configurado con base de datos única Neon")
        
        self.save_setup_report()
        return True
        
def main():
    """Función principal"""
    setup = FirstTimeSetup()
    
    try:
        success = setup.run_setup()
        return 0 if success else 1
    except KeyboardInterrupt:
        setup.log("\n❌ Instalación cancelada por el usuario", "ERROR")
        return 1
    except Exception as e:
        setup.log(f"❌ Error inesperado: {e}", "ERROR")
        return 1

if __name__ == "__main__":
    sys.exit(main())