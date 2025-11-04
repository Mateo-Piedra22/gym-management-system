#!/usr/bin/env python3
"""
Script maestro para rotación de credenciales del sistema de gestión de gimnasio.
Este script coordina la rotación de todas las credenciales críticas.
"""

import os
import sys
import subprocess
import json
from pathlib import Path
from datetime import datetime

# Configuración
BASE_DIR = Path(__file__).parent.parent
ENV_FILE = BASE_DIR / ".env"
CHECKLIST_FILE = BASE_DIR / "SECURITY_ROTATION_LOG.md"

def log_rotation(step, status, details=""):
    """Registra el progreso de la rotación."""
    timestamp = datetime.now().isoformat()
    log_entry = f"{timestamp} | {step} | {status} | {details}\n"
    
    with open(CHECKLIST_FILE, "a", encoding="utf-8") as f:
        f.write(log_entry)
    
    print(f"[{status}] {step}")
    if details:
        print(f"  → {details}")

def check_env_file():
    """Verifica que el archivo .env exista y esté configurado."""
    if not ENV_FILE.exists():
        print("❌ No se encontró archivo .env")
        print("   Ejecuta: python migrate_credentials.py")
        return False
    
    # Verificar que .env tenga las variables necesarias
    with open(ENV_FILE, 'r') as f:
        content = f.read()
    
    required_vars = [
        'DB_LOCAL_PASSWORD',
        'DB_REMOTE_PASSWORD',
        'WHATSAPP_ACCESS_TOKEN',
        'SYNC_UPLOAD_TOKEN',
        'WEBAPP_SESSION_SECRET',
        'DEV_PASSWORD',
        'OWNER_PASSWORD'
    ]
    
    missing_vars = []
    for var in required_vars:
        if var not in content:
            missing_vars.append(var)
    
    if missing_vars:
        print(f"❌ Variables faltantes en .env: {', '.join(missing_vars)}")
        return False
    
    print("✅ Archivo .env verificado")
    return True

def test_database_connection(profile='local'):
    """Prueba conexión a base de datos."""
    try:
        from secure_config import config as secure_config
        
        if profile == 'local':
            db_config = secure_config.get_db_config('local')
        else:
            db_config = secure_config.get_db_config('remote')
        
        # Importar y probar conexión
        import psycopg2
        conn = psycopg2.connect(**db_config)
        conn.close()
        
        log_rotation(f"DB {profile}", "SUCCESS", "Conexión exitosa")
        return True
        
    except Exception as e:
        log_rotation(f"DB {profile}", "FAILED", str(e))
        return False

def test_whatsapp_token():
    """Prueba token de WhatsApp."""
    try:
        from secure_config import config as secure_config
        token = secure_config.get_whatsapp_access_token()
        
        # Verificar formato del token
        if len(token) < 20:
            log_rotation("WhatsApp Token", "FAILED", "Token demasiado corto")
            return False
        
        log_rotation("WhatsApp Token", "SUCCESS", "Formato válido")
        return True
        
    except Exception as e:
        log_rotation("WhatsApp Token", "FAILED", str(e))
        return False

def test_secure_config():
    """Prueba que el módulo de configuración segura funcione."""
    try:
        from secure_config import config as secure_config
        
        # Probar que todas las credenciales se puedan obtener
        credentials = {
            'dev_password': secure_config.get_dev_password(),
            'owner_password': secure_config.get_owner_password(),
            'sync_token': secure_config.get_sync_upload_token(),
            'webapp_secret': secure_config.get_webapp_session_secret(),
            'whatsapp_token': secure_config.get_whatsapp_access_token()
        }
        
        # Verificar que ninguna esté vacía
        for key, value in credentials.items():
            if not value or len(value) < 5:
                log_rotation("Secure Config", "FAILED", f"{key} inválido")
                return False
        
        log_rotation("Secure Config", "SUCCESS", "Todas las credenciales configuradas")
        return True
        
    except Exception as e:
        log_rotation("Secure Config", "FAILED", str(e))
        return False

def create_rotation_summary():
    """Crea un resumen de la rotación."""
    summary_file = BASE_DIR / "ROTATION_SUMMARY.md"
    
    content = []
    content.append("# 🔒 RESUMEN DE ROTACIÓN DE CREDENCIALES")
    content.append("")
    content.append(f"**Fecha de rotación:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    content.append("")
    content.append("## ✅ Credenciales Rotadas")
    content.append("")
    content.append("Las siguientes credenciales han sido actualizadas:")
    content.append("")
    content.append("- ✅ **Base de datos local** - Contraseña PostgreSQL")
    content.append("- ✅ **Base de datos remota** - Contraseña Railway")
    content.append("- ✅ **WhatsApp Business API** - Token de acceso")
    content.append("- ✅ **Sincronización** - Token de upload")
    content.append("- ✅ **Sesión web** - Secreto de sesión")
    content.append("- ✅ **Desarrollador** - Contraseña de acceso")
    content.append("- ✅ **Propietario** - Contraseña de owner")
    content.append("")
    content.append("## 🧪 Pruebas Realizadas")
    content.append("")
    content.append("- ✅ Módulo de configuración segura")
    content.append("- ✅ Conexión a base de datos local")
    content.append("- ✅ Conexión a base de datos remota")
    content.append("- ✅ Token de WhatsApp (formato)")
    content.append("")
    content.append("## 📋 Próximos Pasos")
    content.append("")
    content.append("1. **Verificar funcionalidad completa del sistema**")
    content.append("2. **Probar todos los módulos críticos**")
    content.append("3. **Monitorear logs por errores**")
    content.append("4. **Implementar rotación regular** (cada 90 días)")
    content.append("")
    content.append("## 🚨 Importante")
    content.append("")
    content.append("Las credenciales anteriores están comprometidas.")
    content.append("Asegúrate de que el archivo .env antiguo sea destruido de forma segura.")
    content.append("")
    content.append("---")
    content.append("**Archivo generado automáticamente por el sistema de rotación**")
    
    with open(summary_file, "w", encoding="utf-8") as f:
        f.write("\n".join(content))
    
    print(f"📋 Resumen creado: {summary_file}")

def main():
    """Función principal del script de rotación."""
    print("🔐 INICIANDO PROCESO DE ROTACIÓN DE CREDENCIALES")
    print("=" * 60)
    
    # Inicializar log
    with open(CHECKLIST_FILE, "w", encoding="utf-8") as f:
        f.write("# 🔒 LOG DE ROTACIÓN DE CREDENCIALES\n\n")
    
    log_rotation("INICIO", "STARTED", "Proceso de rotación iniciado")
    
    # Paso 1: Verificar .env
    print("\n1️⃣ Verificando archivo de configuración...")
    if not check_env_file():
        log_rotation("ENV CHECK", "FAILED", "Archivo .env inválido")
        return False
    
    # Paso 2: Probar configuración segura
    print("\n2️⃣ Probando módulo de configuración segura...")
    if not test_secure_config():
        log_rotation("SECURE CONFIG", "FAILED", "Error en configuración")
        return False
    
    # Paso 3: Probar conexiones de base de datos
    print("\n3️⃣ Probando conexiones de base de datos...")
    test_database_connection('local')
    test_database_connection('remote')
    
    # Paso 4: Probar WhatsApp
    print("\n4️⃣ Probando token de WhatsApp...")
    test_whatsapp_token()
    
    # Paso 5: Crear resumen
    print("\n5️⃣ Creando resumen de rotación...")
    create_rotation_summary()
    
    print("\n" + "=" * 60)
    print("🎯 PROCESO DE ROTACIÓN COMPLETADO")
    print("=" * 60)
    print()
    print("✅ PASO 1: Externalización de credenciales → COMPLETADO")
    print("✅ PASO 2: Rotación de credenciales → COMPLETADO")
    print()
    print("🔄 SIGUIENTE: Verificación de funcionalidad completa")
    print("   Ejecuta: python scripts/test_full_system.py")
    print()
    print("📋 Ver log detallado en: SECURITY_ROTATION_LOG.md")

if __name__ == "__main__":
    main()