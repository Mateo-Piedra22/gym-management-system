# Flush puntual de outbox (public.sync_outbox)

Este flujo permite enviar de forma controlada los cambios capturados por los triggers de `public.sync_outbox` hacia el servidor remoto, sin depender de la app en ejecución.

## Prerrequisitos
- Base de datos local accesible (según `config/config.json`).
- Triggers instalados en tablas configuradas en `config/sync_tables.json` bajo `uploads_local_to_remote`.
- Token de subida válido para el backend remoto.

## Instalar triggers del outbox

```bash
python scripts/install_outbox_triggers.py
```

Esto crea `public.sync_outbox`, la función `public.sync_outbox_capture()` y los triggers `AFTER INSERT/UPDATE/DELETE` en las tablas de `uploads_local_to_remote`.

## Configurar URL/token del backend

- Preferente: use el helper centralizado de `utils`.
  - Coloque `sync_upload_token` en `config/config.json` o exporte `SYNC_UPLOAD_TOKEN` antes de ejecutar.
  - La URL base se toma de `config/config.json` (`webapp_base_url`). Si falta, se usa `utils.get_webapp_base_url()`.
- Alternativa rápida (sólo si no usa `utils`):
  - Establezca `WEBAPP_BASE_URL` en el entorno.

El script persistirá el token desde ENV a `config/config.json` si detecta `utils.get_sync_upload_token(persist_from_env=True)`.

## Ejecutar un flush puntual

Opción Python directa:

```bash
python scripts/run_outbox_flush_once.py
```

Salida esperada (JSON):

```json
{"ok": true, "pending": 3, "sent": 3, "acked": 3}
```

- `pending`: cuántos cambios había al comenzar.
- `sent`: cuántos se intentaron enviar en este lote.
- `acked`: cuántos confirmó el servidor y fueron eliminados del outbox.
- Si falta/invalid token, verá `{"auth": "missing"}` o `{"auth": "invalid"}` respectivamente.

Opción PowerShell con logs (oculto):

```powershell
powershell -ExecutionPolicy Bypass -File scripts\run_outbox_flush_once.ps1
```

Registra en `backups/outbox_flush.log`.

## Sugerencias para prueba rápida

1. Asegure que una tabla en `uploads_local_to_remote` tenga un cambio (INSERT/UPDATE/DELETE).
2. Ejecute el flush puntual con el script anterior.
3. Verifique en el backend remoto o en los logs/DB que los cambios fueron procesados.