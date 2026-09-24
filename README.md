
# PPG-StressPi

Sistema de monitoreo de estrés en tiempo real con MAX30102, Raspberry Pi 5 y pipeline de datos con Kafka (KRaft), Spark y Pulse-PPG.

## Arquitectura

- **Pi 5**: adquiere señal PPG cruda (50 Hz) y publica en Kafka.
- **PC con Docker**: Kafka, Spark (métricas HRV), Pulse-PPG (clasificación), Dashboard Flask.
- **Usuario**: dashboard web con señal, métricas y estado.

## Estructura

```
ppg-stresspi/
├── pi5/      # Código de la Raspberry Pi 5
├── pc/       # Backend con Docker
├── data/     # SQLite (no se sube a Git)
├── docs/     # Documentación
└── README.md
```

## Estado

🚧 En desarrollo — estructura inicial.

## Docs

- [PPG-StressPi_architecture_diagram](docs/PPG-StressPi_architecture_diagram.md)
- [Guion de demo](docs/demo_guion.md)
- [Referencias](docs/referencias.md)
- [Contratos](docs/contratos.md)
