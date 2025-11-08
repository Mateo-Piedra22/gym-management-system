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

### Uso de `DATABASE_URL` (DSN)
Para despliegues en plataformas como Railway y proveedores como Neon, también puedes definir una única URL/DSN de conexión en `DATABASE_URL`.

- Formato: `postgresql://usuario:contraseña@host:puerto/base?sslmode=require&connect_timeout=12&application_name=app`
- Si `DATABASE_URL` está presente, se usa por encima de `DB_*` y `DB_LOCAL_*`.
- Detección de Neon: cuando el host contiene `neon.tech`, se deshabilitan automáticamente las opciones de sesión (`options`) no soportadas por el proveedor.
- Mantén `sslmode=require` para conexiones TLS seguras.

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

#### Ejemplo con `DATABASE_URL` (Railway/Neon)
```
DATABASE_URL=postgresql://neon_user:neon_pass@ep-tu-cluster.neon.tech:5432/neondb?sslmode=require&connect_timeout=12&application_name=railway
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

## WebApp y Hosts confiables (Vercel + Railway)

Para exponer la WebApp en producción:

- `WEBAPP_BASE_URL`: URL pública principal. En Vercel, usa tu dominio (`https://tu-dominio.tld`). Si no se define, el sistema detecta `VERCEL_URL`, `VERCEL_BRANCH_URL` o `VERCEL_PROJECT_PRODUCTION_URL` automáticamente.
- `TRUSTED_HOSTS`: lista separada por comas de dominios permitidos por el middleware de host confiable. Ejemplo recomendado:
  `TRUSTED_HOSTS=tu-dominio.tld,*.vercel.app,*.vercel.dev,localhost,127.0.0.1,*.loca.lt`
- Fallback Railway: si despliegas en Railway, `WEBAPP_BASE_URL` puede apuntar a tu dominio en Railway o al dominio generado por la plataforma.

### Variables de entorno Vercel (detectadas automáticamente)
- `VERCEL_URL`: host de preview/producción sin esquema.
- `VERCEL_BRANCH_URL`: host para branches (previews).
- `VERCEL_PROJECT_PRODUCTION_URL`: host del entorno de producción.

No es necesario definir estas variables manualmente; Vercel las inyecta. El sistema arma la URL pública con esquema `https://` y elimina sufijos `/dev`.
