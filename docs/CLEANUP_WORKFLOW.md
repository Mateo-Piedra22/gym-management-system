# Flujo de Limpieza Segura

El botón "Limpieza segura (backup + reinit)" en `cdbconfig` ejecuta:

1. Confirmación del usuario
2. Backup automático (`scripts/quick_backup_database.py`)
3. Limpieza y reinicialización (`cleanup_and_reinitialize.py --force --full-reset`)
4. Verificación de integridad (`verify_system_status.py`)
5. Reporte detallado en la UI

Este flujo garantiza la coherencia de datos y un estado consistente del sistema
conforme al modelo de base de datos única Neon.

