# Configuración de Entorno (.env)

Este sistema obtiene **toda** la configuración desde variables de entorno cargadas por `python-dotenv`.

Nota sobre centralización: `config/config.json` se mantiene sólo para preferencias no sensibles (anteriores). Para credenciales y endpoints, usa exclusivamente `.env` como fuente única.

## Base de datos única (Neon)

El sistema utiliza un único PostgreSQL (Neon). Tanto el Programa Desktop como la WebApp leen la conexión a la base desde variables genéricas `DB_*` y aceptan `DB_LOCAL_*` como fallback.

### Variables recomendadas (genéricas)
- `DB_HOST`
- `DB_PORT`
- `DB_NAME`
- `DB_USER`
- `DB_PASSWORD`
- `DB_SSLMODE` (recomendado: `require`)
- `DB_CONNECT_TIMEOUT` (ej: `10`)
- `DB_APPLICATION_NAME` (ej: `gym_management_system`)
- `DB_PROFILE` (usar `local`)

### Fallbacks (compatibilidad)
Si prefieres mantener nombres `DB_LOCAL_*`, el sistema los tomará como fallback:
- `DB_LOCAL_HOST`, `DB_LOCAL_PORT`, `DB_LOCAL_DATABASE`, `DB_LOCAL_USER`, `DB_LOCAL_PASSWORD`, `DB_LOCAL_SSLMODE`, `DB_LOCAL_CONNECT_TIMEOUT`, `DB_LOCAL_APPLICATION_NAME`.

### Ejemplo de configuración para Neon
```
DB_HOST=ep-tu-cluster.neon.tech
DB_PORT=5432
DB_NAME=gym_db
DB_USER=tu_usuario
DB_PASSWORD=tu_password_seguro
DB_SSLMODE=require
DB_CONNECT_TIMEOUT=10
DB_APPLICATION_NAME=gym_management_system
DB_PROFILE=local
```

### Manejo de secretos
- No se guardan contraseñas en `config.json`.
- Puedes usar `.env` (recomendado para despliegue) o el almacén seguro del sistema (keyring) en desktop.
- Formato de cuenta para keyring: `user@host:port` bajo el servicio `GymMS_DB`.

Ejemplo para guardar en keyring (opcional):
```
python -c "import keyring; svc='GymMS_DB'; acct='tu_usuario@ep-tu-cluster.neon.tech:5432'; keyring.set_password(svc, acct, 'tu_password_seguro'); print('OK')"
```

## Validación

Ejecuta:

```
python scripts/essential/verify_system_status.py
```

Verifica conectividad, tablas críticas y limpieza de artefactos antiguos ya no utilizados.

## Inicialización automática

```
python scripts/auto_initialize_system.py
```

– Verifica conexión a la base de datos única
– Ejecuta chequeo de salud

## Modelo de Datos

El sistema utiliza una única base de datos Neon y no requiere mecanismos locales de replicación ni reconciliación.
