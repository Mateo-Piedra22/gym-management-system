#!/usr/bin/env python3
"""
Script de verificación y rotación de token de WhatsApp Business API.
"""

import os
import requests
import json
from pathlib import Path

def test_whatsapp_token(token):
    """Prueba si un token de WhatsApp es válido."""
    url = f"https://graph.facebook.com/v18.0/791155924083208/message_templates"
    headers = {
        'Authorization': f'Bearer {token}',
        'Content-Type': 'application/json'
    }
    
    try:
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            print("✅ Token VÁLIDO y funcional")
            return True
        else:
            print(f"❌ Token INVÁLIDO o expirado: {response.status_code}")
            print(f"Respuesta: {response.text}")
            return False
    except Exception as e:
        print(f"❌ Error al probar token: {e}")
        return False

def check_token_usage(token):
    """Verifica uso reciente del token (si hay actividad sospechosa)."""
    # Esta función requiere acceso a logs de Meta Business Suite
    print("⚠️  Para verificar uso del token:")
    print("   1. Accede a: https://business.facebook.com")
    print("   2. Ve a: Configuración → Registro de auditoría")
    print("   3. Revisa actividad reciente del token actual")
    print("   4. Busca envíos no autorizados o actividad inusual")

def main():
    print("🔐 VERIFICADOR DE TOKEN WHATSAPP BUSINESS API")
    print("=" * 60)
    
    # Cargar token actual desde .env
    from secure_config import config as secure_config
    try:
        current_token = secure_config.get_whatsapp_access_token()
        print(f"📋 Token actual: {current_token[:20]}...")
        print()
        
        # Verificar validez
        print("🔍 Verificando validez del token actual...")
        is_valid = test_whatsapp_token(current_token)
        
        if is_valid:
            print("\n⚠️  ATENCIÓN: El token actual sigue siendo válido")
            print("   Esto significa que podría estar siendo usado por atacantes")
            print("   Debes revocarlo INMEDIATAMENTE en Meta Business Suite")
        else:
            print("\n✅ El token actual ya no es válido (posiblemente ya revocado)")
        
        # Verificar uso
        print("\n🔍 Verificando uso del token...")
        check_token_usage(current_token)
        
        print("\n📝 Pasos para rotar el token:")
        print("1. Accede a: https://business.facebook.com")
        print("2. Ve a: WhatsApp → Configuración de API")
        print("3. Revoca el token actual")
        print("4. Genera un nuevo token")
        print("5. Actualiza WHATSAPP_ACCESS_TOKEN en tu archivo .env")
        print("6. Prueba el nuevo token con este script")
        
    except ValueError as e:
        print(f"❌ Error al obtener token: {e}")
        print("   Asegúrate de que WHATSAPP_ACCESS_TOKEN esté configurado en .env")

if __name__ == "__main__":
    main()