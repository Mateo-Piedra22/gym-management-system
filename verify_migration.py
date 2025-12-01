import sys
import os
import logging
from datetime import date, datetime
from dotenv import load_dotenv

# Add project root to path
sys.path.append(os.path.abspath(os.path.dirname(__file__)))

# Load env vars
load_dotenv()

from core.database.manager import DatabaseManager
from core.database.orm_models import Usuario

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def verify_system():
    print("="*50)
    print("INICIANDO VERIFICACIÓN DE MIGRACIÓN ORM")
    print("="*50)
    
    try:
        db = DatabaseManager()
        print("✅ DatabaseManager inicializado correctamente")
    except Exception as e:
        print(f"❌ Error inicializando DatabaseManager: {e}")
        return

    # 1. Test User Repository
    print("\n--- 1. Test User Repository ---")
    user_id = None
    try:
        # Crear usuario de prueba
        test_user = Usuario(
            nombre="Test Migration User",
            dni="99999999",
            telefono="55555555",
            rol="socio",
            activo=True
            # Removed email as it's not in the model
        )
        user_id = db.users.crear_usuario(test_user)
        print(f"✅ Usuario creado con ID: {user_id}")
        
        # Leer usuario
        fetched_user = db.users.obtener_usuario(user_id)
        if fetched_user and fetched_user.dni == "99999999":
            print(f"✅ Usuario recuperado correctamente: {fetched_user.nombre}")
        else:
            print("❌ Error recuperando usuario")

        # Actualizar usuario
        fetched_user.nombre = "Test Migration Updated"
        db.users.actualizar_usuario(fetched_user)
        
        # Re-fetch to confirm update
        updated_user = db.users.obtener_usuario(user_id)
        if updated_user.nombre == "Test Migration Updated":
             print(f"✅ Usuario actualizado correctamente")
        else:
             print("❌ Error actualizando usuario")
             
        # Test Business Logic: Check rol change side effects
        print("   Probando lógica de cambio de rol a 'profesor'...")
        updated_user.rol = "profesor"
        db.users.actualizar_usuario(updated_user)
        
        # Check if profesor profile was created
        profs = db.profesores.obtener_todos_profesores()
        prof_exists = any(p['usuario_id'] == user_id for p in profs)
        if prof_exists:
            print("✅ Lógica de negocio: Perfil de profesor creado automáticamente al cambiar rol")
        else:
            print("❌ Lógica de negocio: No se creó el perfil de profesor")

    except Exception as e:
        print(f"❌ Error en User Repository: {e}")
        import traceback
        traceback.print_exc()

    # 2. Test Gym Repository
    print("\n--- 2. Test Gym Repository ---")
    try:
        config = db.gym.obtener_configuracion_gimnasio()
        print(f"✅ Configuración leída: {config.get('gym_name', 'N/A')}")
        
        exercises = db.gym.obtener_todos_ejercicios()
        print(f"✅ Ejercicios cargados: {len(exercises)}")
        
        # ARPU Test
        arpu, morosos = db.gym.obtener_arpu_y_morosos_mes_actual()
        print(f"✅ Cálculo ARPU ejecutado: ARPU=${arpu:.2f}, Morosos={morosos}")

    except Exception as e:
        print(f"❌ Error en Gym Repository: {e}")

    # 3. Test Payment Repository
    print("\n--- 3. Test Payment Repository ---")
    try:
        stats = db.pagos.obtener_estadisticas_pagos(2024)
        print(f"✅ Estadísticas de pagos generadas para 2024: {stats['total_pagos']} pagos registrados")
    except Exception as e:
        print(f"❌ Error en Payment Repository: {e}")

    # Cleanup
    print("\n--- Limpieza ---")
    try:
        if user_id:
            db.users.eliminar_usuario(user_id)
            print(f"✅ Usuario de prueba eliminado (ID: {user_id})")
    except Exception as e:
        print(f"⚠️ Error limpiando usuario de prueba: {e}")

    db.close()
    print("\n" + "="*50)
    print("VERIFICACIÓN COMPLETADA")
    print("="*50)

if __name__ == "__main__":
    verify_system()
