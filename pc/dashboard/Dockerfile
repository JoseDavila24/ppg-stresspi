# Base ligera
FROM python:3.11-slim

# Dependencias Python con versiones fijas
RUN pip install --no-cache-dir \
    flask==3.0.3 \
    flask-socketio==5.4.1 \
    kafka-python==2.0.2 \
    eventlet==0.37.0

# El código (app.py, templates/, static/) se monta como volumen en /app
WORKDIR /app

# Puerto del servidor Flask
EXPOSE 5000

# Script principal del servicio
CMD ["python", "-u", "app.py"]
