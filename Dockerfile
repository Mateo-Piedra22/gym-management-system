# syntax=docker/dockerfile:1
# Base Python image
FROM python:3.11-slim

# Prevents Python from writing .pyc files and ensures unbuffered output
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PORT=8000

# Install system dependencies and LibreOffice (headless capable)
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
      libreoffice \
      libreoffice-calc \
      libreoffice-writer \
      fonts-dejavu \
      fonts-liberation \
      locales \
      ca-certificates && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Set locale to avoid LO warnings
RUN sed -i 's/# en_US.UTF-8 UTF-8/en_US.UTF-8 UTF-8/' /etc/locale.gen && \
    locale-gen
ENV LANG=en_US.UTF-8 \
    LC_ALL=en_US.UTF-8

# Create working directory
WORKDIR /app

# Install Python dependencies (webapp minimal set)
COPY requirements-webapp.txt /app/requirements-webapp.txt
RUN pip install --no-cache-dir -r /app/requirements-webapp.txt

# Copy project source
COPY . /app

# Expose port (Railway will set PORT env var)
EXPOSE 8000

# Default command: run FastAPI webapp
CMD ["sh", "-c", "uvicorn apps.webapp.server:app --host 0.0.0.0 --port ${PORT}"]