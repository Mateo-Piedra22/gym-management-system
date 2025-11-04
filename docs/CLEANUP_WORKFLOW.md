# Flujo de Limpieza Segura

El botón "Limpieza segura (backup + reinit)" en `cdbconfig` ejecuta:

1. Confirmación del usuario
2. Backup automático (`scripts/quick_backup_database.py`)
3. Limpieza y reinicialización (`cleanup_and_reinitialize.py --force --full-reset`)
4. Verificación de integridad (`verify_system_status.py`)
5. Reporte detallado en la UI

Este flujo garantiza que no haya pérdida de datos y que la replicación nativa
se mantenga saludable tras la limpieza.

