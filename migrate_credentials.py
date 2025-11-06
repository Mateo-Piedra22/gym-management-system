#!/usr/bin/env python3
"""
Script de migración para externalización de credenciales.

Este script ayuda a migrar desde credenciales hardcodeadas a variables de entorno.
"""

import os
import json
import shutil
from pathlib import Path
from typing import Dict, Any

# Configuración
BASE_DIR = Path(__file__).parent
CONFIG_DIR = BASE_DIR / "config"
ENV_FILE = BASE_DIR / ".env"
ENV_EXAMPLE_FILE = BASE_DIR / ".env.example"
CONFIG_FILE = CONFIG_DIR / "config.json"

# Credenciales actuales hardcodeadas (para generar .env inicial)
HARDCODED_CREDS = {
    'DEV_PASSWORD': 'Matute03',
    'DB_LOCAL_PASSWORD': 'Matute03',
    'WEBAPP_SESSION_SECRET': 'XKxlGoO1rbwZqeKbfSTKJ_EoqqdARkI45w7qta5XsGY',
    'WHATSAPP_ACCESS_TOKEN': 'EAFc4zmSDeIcBPkyEjkbO7FLad9wQJ3ZCEY7yZCZBxje8HWl7WZBAvzmgOTBZC9h1g3orNYieuZCASlfdqVP9j18NZBqxRGpZBk2uPEze00JHvPEMwYgdwMip3ZBIwrK6yJGSFqG1eeZCe04gZBZAdjyLt02Bb2D0UA3zAvTKZAYQXgwCDXdfZClu0Wz1TtZBVQQipK0HpwbNAZDZD',
    'TAILSCALE_AUTH_KEY': 'tskey-auth-k1mBHcH6W721CNTRL-pLYHgRKssWBYdjXscMWwVBHcnKdCSYNwX',
    'OWNER_PASSWORD': '2203',
    'SERVER_PUBLIC_IP': 'your-server-ip-here'
}

def load_existing_config() -> Dict[str, Any]:
    """Carga la configuración existente desde config.json."""
    if CONFIG_FILE.exists():
        try:
            with open(CONFIG_FILE, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            print(f"⚠️  Advertencia: No se pudo leer config.json: {e}")
    return {}

def create_env_file():
    """Crea el archivo .env con las credenciales."""
    print("🔐 Creando archivo .env con credenciales...")
    
    # Si ya existe .env, hacer backup
    if ENV_FILE.exists():
        backup_file = ENV_FILE.with_suffix('.env.backup')
        shutil.copy(ENV_FILE, backup_file)
        print(f"📋 Backup creado: {backup_file}")
    
    # Leer configuración existente
    config = load_existing_config()
    
    # Construir contenido del .env
    env_content = []
    env_content.append("# =============================================================================")
    env_content.append("# GYM MANAGEMENT SYSTEM - VARIABLES DE ENTORNO")
    env_content.append("# =============================================================================")
    env_content.append("# ⚠️  IMPORTANTE: Este archivo contiene credenciales sensibles.")
    env_content.append("# 🔒 Mantener este archivo seguro y NUNCA commitearlo al repositorio.")
    env_content.append("")
    
    # Base de datos local
    env_content.append("# =============================================================================")
    env_content.append("# BASE DE DATOS LOCAL")
    env_content.append("# =============================================================================")
    db_local = config.get('db_local', {})
    env_content.append(f"DB_LOCAL_HOST={db_local.get('host', 'localhost')}")
    env_content.append(f"DB_LOCAL_PORT={db_local.get('port', 5432)}")
    env_content.append(f"DB_LOCAL_DATABASE={db_local.get('database', 'gimnasio')}")
    env_content.append(f"DB_LOCAL_USER={db_local.get('user', 'postgres')}")
    env_content.append(f"DB_LOCAL_PASSWORD={db_local.get('password', HARDCODED_CREDS['DB_LOCAL_PASSWORD'])}")
    env_content.append("")
    
    # Replicación eliminada - se usa base de datos única Neon
    
    # Seguridad
    env_content.append("# =============================================================================")
    env_content.append("# SEGURIDAD Y AUTENTICACIÓN")
    env_content.append("# =============================================================================")
    env_content.append(f"DEV_PASSWORD={HARDCODED_CREDS['DEV_PASSWORD']}")
    env_content.append(f"OWNER_PASSWORD={HARDCODED_CREDS['OWNER_PASSWORD']}")
    env_content.append(f"WEBAPP_SESSION_SECRET={config.get('webapp_session_secret', HARDCODED_CREDS['WEBAPP_SESSION_SECRET'])}")
    env_content.append("")
    
    # APIs externas
    env_content.append("# =============================================================================")
    env_content.append("# APIs EXTERNAS")
    env_content.append("# =============================================================================")
    env_content.append(f"WHATSAPP_ACCESS_TOKEN={HARDCODED_CREDS['WHATSAPP_ACCESS_TOKEN']}")
    env_content.append(f"TAILSCALE_AUTH_KEY={config.get('tailscale_auth_key', HARDCODED_CREDS['TAILSCALE_AUTH_KEY'])}")
    env_content.append("")
    
    # Aplicación
    env_content.append("# =============================================================================")
    env_content.append("# CONFIGURACIONES DE APLICACIÓN")
    env_content.append("# =============================================================================")
    env_content.append(f"DB_PROFILE={config.get('db_profile', 'local')}")
    env_content.append(f"WEBAPP_BASE_URL={config.get('webapp_base_url', 'https://gym-ms-zrk.up.railway.app')}")
    env_content.append(f"CLIENT_BASE_URL={config.get('client_base_url', '')}")
    env_content.append(f"SERVER_PUBLIC_IP={HARDCODED_CREDS['SERVER_PUBLIC_IP']}")
    env_content.append("")
    
    # Tareas programadas
    env_content.append("# =============================================================================")
    env_content.append("# TAREAS PROGRAMADAS")
    env_content.append("# =============================================================================")
    scheduled = config.get('scheduled_tasks', {})
    env_content.append(f"SCHEDULED_TASKS_ENABLED={scheduled.get('enabled', True)}")
    # Solo tareas nativas
    cleanup = scheduled.get('cleanup', {})
    backup = scheduled.get('backup', {})
    env_content.append(f"CLEANUP_ENABLED={cleanup.get('enabled', True)}")
    env_content.append(f"CLEANUP_TIME={cleanup.get('time', '03:15')}")
    env_content.append(f"BACKUP_ENABLED={backup.get('enabled', True)}")
    env_content.append(f"BACKUP_TIME={backup.get('time', '02:30')}")
    env_content.append("")
    
    # Replicación eliminada - se usa base de datos única Neon
    env_content.append("")
    
    # Túnel público
    env_content.append("# =============================================================================")
    env_content.append("# TÚNEL PÚBLICO")
    env_content.append("# =============================================================================")
    tunnel = config.get('public_tunnel', {})
    env_content.append(f"PUBLIC_TUNNEL_ENABLED={tunnel.get('enabled', False)}")
    env_content.append(f"PUBLIC_TUNNEL_SUBDOMAIN={tunnel.get('subdomain', 'gym-ms-zrk')}")
    
    # Escribir archivo .env
    with open(ENV_FILE, 'w', encoding='utf-8') as f:
        f.write('\n'.join(env_content))
    
    print(f"✅ Archivo .env creado exitosamente: {ENV_FILE}")
    print("⚠️  IMPORTANTE: Revisa y actualiza las credenciales antes de usar en producción!")

def create_security_checklist():
    """Crea una lista de verificación de seguridad."""
    checklist_file = BASE_DIR / "SECURITY_MIGRATION_CHECKLIST.md"
    
    content = []
    content.append("# 🔒 Lista de Verificación - Migración de Seguridad")
    content.append("")
    content.append("## ✅ Pasos Completados Automáticamente")
    content.append("")
    content.append("- [x] Creación de archivo .env.example")
    content.append("- [x] Creación de módulo secure_config.py")
    content.append("- [x] Actualización de managers.py para usar variables de entorno")
    content.append("- [x] Actualización de whatsapp_manager.py")
    content.append("- [x] Actualización de database.py")
    content.append("- [x] Actualización de webapp/server.py")
    content.append("")
    content.append("## ⚠️  Pasos Manuales Requeridos")
    content.append("")
    content.append("### 1. Rotación de Credenciales (URGENTE)")
    content.append("- [ ] **Cambiar contraseña de base de datos local**")
    content.append("- [ ] **Revocar y regenerar token de WhatsApp Business API**")
    content.append("- [ ] **Cambiar DEV_PASSWORD (contraseña de desarrollador)**")
    content.append("- [ ] **Cambiar OWNER_PASSWORD (contraseña del propietario)**")
    content.append("- [ ] **Generar nuevo WEBAPP_SESSION_SECRET**")
    content.append("- [ ] **Cambiar TAILSCALE_AUTH_KEY si se usa**")
    content.append("")
    content.append("### 2. Configuración del Entorno")
    content.append("- [ ] Actualizar archivo .env con las nuevas credenciales")
    content.append("- [ ] Configurar SERVER_PUBLIC_IP con la IP real del servidor")
    content.append("- [ ] Verificar que DB_PROFILE esté configurado en 'local'")
    content.append("- [ ] Ajustar WEBAPP_BASE_URL y CLIENT_BASE_URL si es necesario")
    content.append("")
    content.append("### 3. Verificación de Seguridad")
    content.append("- [ ] Confirmar que .env está en .gitignore")
    content.append("- [ ] Verificar que config.json antiguo no tenga credenciales activas")
    content.append("- [ ] Probar todas las funcionalidades con nuevas credenciales")
    content.append("- [ ] Verificar logs de errores por credenciales faltantes")
    content.append("")
    content.append("### 4. Documentación y Comunicación")
    content.append("- [ ] Actualizar documentación de instalación")
    content.append("- [ ] Informar al equipo sobre el nuevo sistema de credenciales")
    content.append("- [ ] Documentar proceso de rotación de credenciales")
    content.append("")
    content.append("## 🚨 ADVERTENCIAS DE SEGURIDAD")
    content.append("")
    content.append("1. **Las credenciales en .env son TEMPORALES** - Deben ser cambiadas inmediatamente")
    content.append("2. **NUNCA commitear el archivo .env real** - Ya está en .gitignore")
    content.append("3. **Usar gestor de secretos en producción** - Considerar AWS Secrets Manager, Azure Key Vault, etc.")
    content.append("4. **Implementar rotación regular de credenciales** - Cada 90 días como mínimo")
    content.append("5. **Auditoría de accesos** - Revisar logs regularmente")
    content.append("")
    content.append("## 📞 En Caso de Emergencia")
    content.append("")
    content.append("Si algo falla después de la migración:")
    content.append("1. Verificar que todas las variables de entorno estén configuradas")
    content.append("2. Revisar los logs de errores del sistema")
    content.append("3. Tener backup del config.json original por si necesita rollback")
    content.append("4. Contactar al administrador del sistema")
    
    with open(checklist_file, 'w', encoding='utf-8') as f:
        f.write('\n'.join(content))
    
    print(f"📋 Lista de verificación creada: {checklist_file}")

def main():
    """Función principal del script de migración."""
    print("🔐 INICIANDO MIGRACIÓN DE CREDENCIALES")
    print("=" * 50)
    
    # Crear .env
    create_env_file()
    print()
    
    # Crear checklist
    create_security_checklist()
    print()
    
    print("🎯 MIGRACIÓN COMPLETADA")
    print("=" * 50)
    print("✅ Archivos creados:")
    print(f"   - .env (con credenciales temporales)")
    print(f"   - SECURITY_MIGRATION_CHECKLIST.md")
    print()
    print("⚠️  SIGUIENTES PASOS CRÍTICOS:")
    print("   1. ACTUALIZAR las credenciales en .env con valores NUEVOS")
    print("   2. ROTAR todas las credenciales expuestas (cambiarlas)")
    print("   3. SEGUIR la lista de verificación en SECURITY_MIGRATION_CHECKLIST.md")
    print()
    print("🚨 IMPORTANTE: Este es solo el primer paso. La seguridad real")
    print("   requiere cambiar TODAS las credenciales expuestas.")

if __name__ == "__main__":
    main()
