# Imagen base con Python y Java 17
FROM python:3.11-slim

# Instalar Java 17 (headless) y utilidades básicas
RUN apt-get update && apt-get install -y --no-install-recommends \
    openjdk-17-jre-headless ca-certificates curl bash \
 && rm -rf /var/lib/apt/lists/*

# Definir directorio de trabajo
WORKDIR /app

# Copiar requirements si existen para cacheo de dependencias
COPY requirements.txt ./
RUN if [ -f requirements.txt ]; then pip install --no-cache-dir -r requirements.txt; fi

# Copiar el resto del proyecto
COPY . .

# Variables de entorno útiles en Railway
# Railway provee `PORT`; lo propagamos a la lógica de SymmetricDS
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    SYM_RAILWAY_HTTP_PORT=31415

# Puerto por defecto para desarrollo local; Railway sobrescribe con $PORT
EXPOSE 31415

# Comando de arranque: inicia el servidor SymmetricDS (engine 'railway')
CMD ["python", "scripts/start_symmetric_server.py"]