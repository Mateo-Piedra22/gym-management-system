#!/usr/bin/env python3
"""
Script de ayuda para rotación de contraseña de PostgreSQL local.
"""

import subprocess
import getpass
import secrets
import string

def generate_secure_password(length=16):
    """Genera una contraseña segura."""
    alphabet = string.ascii_letters + string.digits + "!@#$%^&*"
    password = ''.join(secrets.choice(alphabet) for _ in range(length))
    return password

def main():
    print("🔐 AYUDANTE PARA ROTACIÓN DE CONTRASEÑA POSTGRESQL LOCAL")
    print("=" * 60)
    
    # Generar contraseña segura
    new_password = generate_secure_password()
    print(f"📋 Nueva contraseña generada: {new_password}")
    print("💾 GUARDA ESTA CONTRASEÑA - No se mostrará de nuevo")
    print()
    
    print("📝 Comandos a ejecutar:")
    print("1. Conectar a PostgreSQL:")
    print("   sudo -u postgres psql")
    print()
    print("2. Cambiar contraseña:")
    print(f"   ALTER USER postgres PASSWORD '{new_password}';")
    print()
    print("3. Salir de psql:")
    print("   \\q")
    print()
    print("4. Actualizar archivo .env:")
    print(f"   DB_LOCAL_PASSWORD={new_password}")
    print()
    print("⚠️  IMPORTANTE: Después de cambiar la contraseña:")
    print("   - Reinicia el sistema de gestión de gimnasio")
    print("   - Verifica que las conexiones funcionen")
    print("   - Prueba las funcionalidades de base de datos")
    
    # Confirmar que el usuario guardó la contraseña
    input("\n📋 Presiona ENTER cuando hayas guardado la contraseña...")
    print("\n🎯 Proceso completado. Ahora actualiza tu archivo .env")

if __name__ == "__main__":
    main()