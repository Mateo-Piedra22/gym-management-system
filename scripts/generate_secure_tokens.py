#!/usr/bin/env python3
"""
Generador de tokens seguros para el sistema de gestión de gimnasio.
"""

import secrets
import string
import hashlib
import datetime

def generate_secure_token(prefix="gymms", length=32):
    """Genera un token seguro con prefijo opcional."""
    # Caracteres seguros para tokens
    alphabet = string.ascii_letters + string.digits
    random_part = ''.join(secrets.choice(alphabet) for _ in range(length))
    return f"{prefix}_{random_part}"

def generate_session_secret(length=64):
    """Genera un secreto de sesión criptográficamente seguro."""
    return secrets.token_urlsafe(length)

def generate_api_key(length=48):
    """Genera una API key segura."""
    return secrets.token_urlsafe(length)

def main():
    print("🔐 GENERADOR DE TOKENS SEGUROS")
    print("=" * 50)
    
    # Generar tokens para diferentes usos
    print("📋 Tokens generados (GUARDAR ESTOS VALORES):")
    print()
    
    # Token de sincronización
    sync_token = generate_secure_token("gymms_sync", 32)
    print(f"1️⃣ SYNC_UPLOAD_TOKEN:")
    print(f"   {sync_token}")
    print()
    
    # Secreto de sesión webapp
    session_secret = generate_session_secret(48)
    print(f"2️⃣ WEBAPP_SESSION_SECRET:")
    print(f"   {session_secret}")
    print()
    
    # Token de autenticación para Tailscale (si se usa)
    tailscale_key = generate_secure_token("tskey", 24)
    print(f"3️⃣ TAILSCALE_AUTH_KEY (opcional):")
    print(f"   {tailscale_key}")
    print()
    
    # DEV_PASSWORD (más corta para uso humano)
    dev_password = generate_secure_token("", 12)
    print(f"4️⃣ DEV_PASSWORD:")
    print(f"   {dev_password}")
    print()
    
    # OWNER_PASSWORD
    owner_password = generate_secure_token("", 12)
    print(f"5️⃣ OWNER_PASSWORD:")
    print(f"   {owner_password}")
    print()
    
    print("⚠️  IMPORTANTE:")
    print("   - GUARDA ESTOS VALORES DE FORMA SEGURA")
    print("   - ACTUALIZA TU ARCHIVO .env CON ESTOS NUEVOS VALORES")
    print("   - NO COMPARTAS ESTOS TOKENS")
    print("   - CONSIDERA USAR UN GESTOR DE CONTRASEÑAS")
    
    # Generar timestamp
    timestamp = datetime.datetime.now().isoformat()
    print(f"\n🕐 Generado el: {timestamp}")
    
    # Esperar confirmación
    input("\n📋 Presiona ENTER cuando hayas guardado los tokens...")
    print("\n✅ Proceso completado. Ahora actualiza tu archivo .env")

if __name__ == "__main__":
    main()