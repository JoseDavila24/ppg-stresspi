``` mermaid
flowchart LR
    %% ========== ZONA 1: ADQUISICIÓN ==========
    subgraph Z1["ZONA 1 — Raspberry Pi 5 (adquisición)"]
        direction TB
        SENSOR["Sensor MAX30102<br/>I2C · 50 Hz"]
        BUFFER["Buffer circular<br/>100 muestras (2 s)"]
        PROD["Productor Kafka"]
        SENSOR -->|"muestras PPG crudas (IR/Red)"| BUFFER
        BUFFER -->|"lote de 100 muestras"| PROD
    end

    %% ========== ZONA 2: PROCESAMIENTO ==========
    subgraph Z2["ZONA 2 — PC con Docker (procesamiento)"]
        direction TB

        subgraph KAFKA["Contenedor Kafka · modo KRaft (sin Zookeeper)"]
            direction TB
            T1[["topic: ppg-crudo"]]
            T2[["topic: metricas-hrv"]]
            T3[["topic: estado-estres"]]
        end

        SPARK["Contenedor Spark<br/>cálculo SDNN · RMSSD · BPM"]
        PULSE["Contenedor Pulse-PPG<br/>ventana de 30 s<br/>→ embeddings 512-d<br/>→ clasificación de estrés"]
        DB[("SQLite · ./data/ppg.db<br/>volumen compartido<br/>tablas: metricas, estado")]
        DASH["Contenedor Dashboard<br/>Flask + Chart.js<br/>puerto 5000"]
    end

    %% ========== ZONA 3: USUARIO ==========
    subgraph Z3["ZONA 3 — Usuario"]
        direction TB
        NAV["Navegador web<br/>http://localhost:5000"]
        subgraph UI["Pantalla única del dashboard"]
            direction TB
            S1["Señal PPG en vivo"]
            S2["Métricas HRV"]
            S3["Estado actual"]
            S4["Resumen de sesión"]
        end
    end

    %% ========== FLUJOS ==========
    PROD -->|"JSON: muestras PPG + timestamp"| T1

    T1 -->|"señal PPG cruda"| SPARK
    T1 -->|"señal PPG cruda"| PULSE
    T1 -->|"señal PPG en vivo"| DASH

    SPARK -->|"INSERT tabla metricas"| DB
    SPARK -->|"JSON: SDNN, RMSSD, BPM"| T2

    PULSE -->|"INSERT tabla estado"| DB
    PULSE -->|"JSON: estado de estrés"| T3

    T2 -->|"métricas HRV"| DASH
    T3 -->|"estado de estrés"| DASH
    DB -.->|"SELECT histórico de sesión"| DASH

    DASH -->|"HTTP: HTML + datos JSON"| NAV
    NAV --> S1 & S2 & S3 & S4

    %% ========== ESTILOS ==========
    classDef adq fill:#dbeafe,stroke:#2563eb,stroke-width:2px,color:#1e3a8a
    classDef proc fill:#dcfce7,stroke:#16a34a,stroke-width:2px,color:#14532d
    classDef topic fill:#bbf7d0,stroke:#15803d,stroke-width:2px,color:#14532d
    classDef db fill:#d1fae5,stroke:#047857,stroke-width:2px,color:#064e3b
    classDef vis fill:#ffedd5,stroke:#ea580c,stroke-width:2px,color:#7c2d12

    class SENSOR,BUFFER,PROD adq
    class SPARK,PULSE proc
    class T1,T2,T3 topic
    class DB db
    class DASH,NAV,S1,S2,S3,S4 vis

    style Z1 fill:#eff6ff,stroke:#2563eb,stroke-width:2px
    style Z2 fill:#f0fdf4,stroke:#16a34a,stroke-width:2px
    style KAFKA fill:#ecfdf5,stroke:#15803d,stroke-dasharray:5 5
    style Z3 fill:#fff7ed,stroke:#ea580c,stroke-width:2px
    style UI fill:#fffbf5,stroke:#fb923c,stroke-dasharray:5 5
```
