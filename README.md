# PPG-StressPi

Sistema de monitoreo de estrés en tiempo real que integra IoT y BigData: adquisición de señal PPG con MAX30102 en Raspberry Pi 5, pipeline de eventos con Kafka (KRaft), análisis batch con Spark, y clasificación con el modelo fundacional Pulse-PPG.

## Arquitectura

- **Zona 1 — Raspberry Pi 5**: adquisición de señal PPG cruda (50 Hz) y publicación en Kafka.
- **Zona 2 — PC con Docker**: Kafka (KRaft), Spark (métricas HRV), Pulse-PPG (clasificación), Dashboard Flask.
- **Zona 3 — Usuario**: navegador accediendo al dashboard.

## Estructura
ppg-stresspi/
├── pi5/ # Código de la Raspberry Pi 5
├── pc/ # Backend con Docker
├── data/ # Volumen SQLite (no se sube a Git)
├── docs/ # Documentación y diagramas
└── README.md


## Estado

🚧 En desarrollo — versión inicial de la estructura.

## Documentación

- [Arquitectura](docs/arquitectura.md)
- [Guion de demo](docs/demo_guion.md)
- [Referencias](docs/referencias.md)
