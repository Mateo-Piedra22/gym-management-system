Despliegue de SymmetricDS en Railway (servicio separado)

Objetivo
- Correr el servidor SymmetricDS en Railway sin afectar la webapp existente.
- Usar el mismo repositorio con un servicio adicional y configuración aislada.

Archivos relevantes
- `deploy/symmetricds/Dockerfile`: imagen con Python 3.11 y Java 17; arranca SymmetricDS.
- `deploy/symmetricds/Procfile`: comando de inicio (`python scripts/start_symmetric_server.py`).
- `scripts/start_symmetric_server.py`: genera `railway.properties`, usa `PORT`, conecta a Postgres con `DATABASE_URL`/`SYM_DB_*`.

Requisitos
- Base de datos Postgres accesible desde Railway.
- Variables de entorno por servicio (no compartidas entre servicios):
  - `DATABASE_URL` (recomendado): `postgres://usuario:password@host:puerto/nombre_db`.
  - Alternativa: `POSTGRES_HOST`, `POSTGRES_PORT`, `POSTGRES_DB`, `POSTGRES_USER`, `POSTGRES_PASSWORD`.
  - `SERVER_BASE_URL`: URL pública del servicio SymmetricDS (ej. `https://<tu-servicio>.up.railway.app`).

Pasos para desplegar como un nuevo servicio en Railway
1) Crear un nuevo servicio en el mismo proyecto: “Deploy from Repo”.
2) En configuración avanzada del servicio nuevo, establecer:
   - Dockerfile Path: `deploy/symmetricds/Dockerfile`.
3) En “Variables” del servicio nuevo, añadir:
   - `DATABASE_URL` o variables `POSTGRES_*`.
   - `SERVER_BASE_URL` con la URL pública que te asigna Railway al servicio nuevo.
4) Deploy: Railway construye la imagen y arranca el contenedor.

Qué sucede al arrancar
- Se genera `symmetricds/engines/railway.properties` con `engine.name=railway`, conexión a Postgres, `http.port=$PORT`, `sync.url` usando `SERVER_BASE_URL`.
- Se inicia SymmetricDS WebServer en primer plano (logs visibles en Railway).

Verificación y operación
- Ver logs del servicio en Railway para confirmar que SymmetricDS escucha en el puerto asignado.
- Opcional: ejecutar `symmetricds/scripts/verify_setup.py` (desde tu entorno) para validar tablas/config.

No interfiere con la webapp
- La webapp sigue usando su propia configuración, builder y start command.
- Este servicio usa su propio Dockerfile/variables en `deploy/symmetricds/`, aislado del raíz.

Alternativa: proyecto separado
- Si quieres aislamiento total, crea otro proyecto en Railway apuntando al mismo repo y usa `deploy/symmetricds/Dockerfile`.

Solución de problemas
- Postgres no conecta: revisa `DATABASE_URL`/`POSTGRES_*` y que la DB esté accesible.
- URL pública incorrecta: ajusta `SERVER_BASE_URL` y redeploy.
- Puerto: Railway define `PORT` automáticamente; el script lo usa.