# Base ligera; se fija bookworm para obtener Java 17 (compatible con Spark 3.5)
FROM python:3.11-slim-bookworm

# Java para Spark y procps (Spark usa `ps` en sus scripts)
RUN apt-get update && \
    apt-get install -y --no-install-recommends default-jre-headless procps && \
    rm -rf /var/lib/apt/lists/*

# Dependencias Python con versiones fijas
RUN pip install --no-cache-dir \
    kafka-python==2.0.2 \
    numpy==1.26.4 \
    scipy==1.13.1 \
    jsonschema==4.23.0 \
    pyspark==3.5.3 \
    pandas==2.2.3

# El código se monta como volumen en /app
WORKDIR /app

# Script principal del servicio
CMD ["python", "-u", "procesar_hrv.py"]
