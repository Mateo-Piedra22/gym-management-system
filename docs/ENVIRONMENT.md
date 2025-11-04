# Configuración de Entorno (.env)

Este sistema obtiene **toda** la configuración desde variables de entorno cargadas por `python-dotenv`.

## Variables requeridas

### Base de datos local
- `DB_LOCAL_HOST`
- `DB_LOCAL_PORT`
- `DB_LOCAL_DATABASE`
- `DB_LOCAL_USER`
- `DB_LOCAL_PASSWORD`

### Base de datos remota
- `DB_REMOTE_HOST`
- `DB_REMOTE_PORT`
- `DB_REMOTE_DATABASE`
- `DB_REMOTE_USER`
- `DB_REMOTE_PASSWORD`

### Replicación
- `REPLICATION_SUBSCRIPTION_NAME` (default: `gym_sub`)
- `REPLICATION_PUBLICATION_NAME` (default: `gym_pub`)
- `REMOTE_CAN_REACH_LOCAL` (`true` para bidireccional)

## Validación

Ejecuta:

```
python scripts/verify_environment_config.py
```

## Inicialización automática

```
python scripts/auto_initialize_system.py
```

– Configura VPN si corresponde
– Verifica conexiones y replicación
– Ejecuta chequeo de salud

