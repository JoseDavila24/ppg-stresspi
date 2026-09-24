# Base ligera
FROM python:3.11-slim

# Compiladores por si alguna dependencia compila, y OpenMP para PyTorch
RUN apt-get update && \
    apt-get install -y --no-install-recommends build-essential libgomp1 && \
    rm -rf /var/lib/apt/lists/*

# PyTorch solo CPU desde el índice oficial (evita descargar CUDA, varios GB)
RUN pip install --no-cache-dir torch==2.5.1 --index-url https://download.pytorch.org/whl/cpu

# Resto de dependencias Python desde PyPI
RUN pip install --no-cache-dir \
    numpy==1.26.4 \
    scipy==1.13.1 \
    kafka-python==2.0.2 \
    jsonschema==4.23.0

# El código se monta en /app y los pesos en /app/weights (solo lectura)
WORKDIR /app

# Script principal del servicio
CMD ["python", "-u", "run_consumer.py"]
