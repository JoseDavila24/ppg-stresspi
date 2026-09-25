"""
test_producer.py — Simulador de la Raspberry Pi 5 para PPG-StressPi
====================================================================

Publica en el topic `ppg-crudo` mensajes que cumplen el contrato v1.0
(docs/contratos.md §3), pero con señal PPG SINTÉTICA en lugar del MAX30102.
Sirve para probar Kafka, Spark, Pulse-PPG y el Dashboard sin la Pi ni el sensor.

Configuración (en este orden de prioridad):
  1. Variables de entorno: HOST_IP, KAFKA_EXTERNAL_PORT
  2. Archivo ../pc/.env (relativo a ESTA carpeta pi5/, no al directorio actual)
  3. Valores por defecto: localhost y 9094

Uso:
  cd pi5
  pip install -r requirements.txt          # necesita kafka-python
  python test_producer.py                  # modo "neutro", sin fin (Ctrl+C para parar)
  python test_producer.py --modo estresado --duracion 120
  python test_producer.py --dry-run --lotes 1   # imprime el JSON sin conectarse a Kafka
"""

import argparse
import json
import math
import os
import random
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

# ----------------------------------------------------------------------------
# Constantes del contrato v1.0 (no cambiar sin actualizar docs/contratos.md)
# ----------------------------------------------------------------------------
VERSION_CONTRATO = "1.0"
TOPIC = "ppg-crudo"
FS_HZ = 50                 # muestras por segundo
N_MUESTRAS = 100           # muestras por lote -> 1 lote cada 2 s
DURACION_LOTE_S = N_MUESTRAS / FS_HZ
UMBRAL_DEDO = 50_000       # media IR por debajo de esto = sin contacto
ADC_MAX = 262_143          # ADC de 18 bits del MAX30102

# Perfiles de simulación: frecuencia cardiaca media y variabilidad.
# Estrés = pulso más alto y MENOS variabilidad (SDNN/RMSSD bajos).
PERFILES = {
    "relajado":  {"bpm": 64, "variabilidad_ms": 60},
    "neutro":    {"bpm": 74, "variabilidad_ms": 40},
    "estresado": {"bpm": 92, "variabilidad_ms": 15},
}


# ----------------------------------------------------------------------------
# Configuración: variables de entorno > ../pc/.env > valores por defecto
# ----------------------------------------------------------------------------
def leer_env(ruta: Path) -> dict:
    """Lee un archivo .env sencillo (CLAVE=valor). Ignora comentarios y líneas vacías."""
    valores = {}
    if not ruta.exists():
        return valores
    for linea in ruta.read_text(encoding="utf-8").splitlines():
        linea = linea.strip()
        if not linea or linea.startswith("#") or "=" not in linea:
            continue
        clave, valor = linea.split("=", 1)
        valores[clave.strip()] = valor.strip().strip('"').strip("'")
    return valores


def cargar_config() -> dict:
    # Ruta calculada desde la ubicación del script, así funciona
    # aunque lo ejecutes desde otra carpeta.
    ruta_env = Path(__file__).resolve().parent.parent / "pc" / ".env"
    archivo = leer_env(ruta_env)

    def obtener(clave, defecto):
        return os.environ.get(clave) or archivo.get(clave) or defecto

    return {
        "host_ip": obtener("HOST_IP", "localhost"),
        "puerto": obtener("KAFKA_EXTERNAL_PORT", "9094"),
        "ruta_env": ruta_env,
        "env_encontrado": ruta_env.exists(),
    }


# ----------------------------------------------------------------------------
# Generador de señal PPG sintética
# ----------------------------------------------------------------------------
class GeneradorPPG:
    """
    Genera la señal muestra a muestra, con continuidad entre lotes.
    Cada latido dura un intervalo RR aleatorio alrededor de la media del perfil,
    así Spark puede calcular SDNN/RMSSD con valores realistas.
    """

    def __init__(self, bpm: float, variabilidad_ms: float, semilla=None):
        self.rng = random.Random(semilla)
        self.rr_medio_s = 60.0 / bpm
        self.variabilidad_s = variabilidad_ms / 1000.0
        self.fase = 0.0                  # 0..1 dentro del latido actual
        self.rr_actual = self._nuevo_rr()
        self.t = 0.0                     # tiempo total, para la deriva respiratoria

    def _nuevo_rr(self) -> float:
        rr = self.rng.gauss(self.rr_medio_s, self.variabilidad_s)
        return min(max(rr, 0.3), 2.0)    # límites fisiológicos (200–30 bpm)

    @staticmethod
    def _forma_pulso(fase: float) -> float:
        """Forma típica de un pulso PPG: pico sistólico + muesca dicrótica."""
        sistolico = math.exp(-((fase - 0.20) ** 2) / (2 * 0.06 ** 2))
        dicrotico = 0.35 * math.exp(-((fase - 0.50) ** 2) / (2 * 0.08 ** 2))
        return sistolico + dicrotico

    def siguiente(self, base: int, amplitud: int, ruido: int) -> int:
        dt = 1.0 / FS_HZ
        self.fase += dt / self.rr_actual
        if self.fase >= 1.0:             # empieza un latido nuevo
            self.fase -= 1.0
            self.rr_actual = self._nuevo_rr()
        self.t += dt

        # En el MAX30102 la IR BAJA cuando llega el pulso (más sangre absorbe más luz)
        pulso = -amplitud * self._forma_pulso(self.fase)
        respiracion = 150 * math.sin(2 * math.pi * 0.25 * self.t)  # ~15 resp/min
        valor = base + pulso + respiracion + self.rng.gauss(0, ruido)
        return int(min(max(valor, 0), ADC_MAX))

    def lote(self, dedo: bool, con_rojo: bool):
        if dedo:
            ir = [self.siguiente(112_500, 900, 25) for _ in range(N_MUESTRAS)]
            # El rojo es más bajo y con menos amplitud; se deriva del mismo latido
            rojo = [int(v * 0.82) for v in ir] if con_rojo else None
        else:
            # Sin dedo: valores bajos y ruidosos, sin pulso
            ir = [int(abs(self.rng.gauss(1_500, 400))) for _ in range(N_MUESTRAS)]
            rojo = [int(abs(self.rng.gauss(1_200, 300))) for _ in range(N_MUESTRAS)] if con_rojo else None
        return ir, rojo


# ----------------------------------------------------------------------------
# Construcción del mensaje según el contrato
# ----------------------------------------------------------------------------
def iso_utc(dt: datetime) -> str:
    """ISO 8601 con milisegundos y sufijo Z: 2026-09-24T19:35:14.000Z"""
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.") + f"{dt.microsecond // 1000:03d}Z"


def construir_mensaje(dispositivo_id, sesion_id, seq, ts_inicio, ir, rojo) -> dict:
    msg = {
        "version_contrato": VERSION_CONTRATO,
        "dispositivo_id": dispositivo_id,
        "sesion_id": sesion_id,
        "seq": seq,
        "ts_inicio": iso_utc(ts_inicio),
        "fs_hz": FS_HZ,
        "n_muestras": N_MUESTRAS,
        "dedo_detectado": (sum(ir) / len(ir)) >= UMBRAL_DEDO,
        "ir_raw": ir,
    }
    if rojo is not None:
        msg["red_raw"] = rojo
    return msg


def verificar_mensaje(msg: dict) -> None:
    """Chequeo rápido del contrato antes de publicar (sin dependencias extra)."""
    assert len(msg["ir_raw"]) == msg["n_muestras"] == N_MUESTRAS, "ir_raw debe tener 100 muestras"
    assert all(0 <= v <= ADC_MAX for v in msg["ir_raw"]), "valor IR fuera de rango ADC"
    if "red_raw" in msg:
        assert len(msg["red_raw"]) == len(msg["ir_raw"]), "red_raw e ir_raw con longitudes distintas"
    tam = len(json.dumps(msg).encode("utf-8"))
    assert tam <= 4096, f"mensaje de {tam} bytes supera el máximo de 4 KB"


# ----------------------------------------------------------------------------
# Kafka
# ----------------------------------------------------------------------------
def crear_productor(servidor: str):
    try:
        from kafka import KafkaProducer
    except ImportError:
        sys.exit("Falta kafka-python. Instálalo con: pip install kafka-python")

    return KafkaProducer(
        bootstrap_servers=servidor,
        key_serializer=lambda k: k.encode("utf-8"),
        value_serializer=lambda v: json.dumps(v, separators=(",", ":")).encode("utf-8"),
        acks=1,
        linger_ms=0,
        request_timeout_ms=10_000,
    )


# ----------------------------------------------------------------------------
# Programa principal
# ----------------------------------------------------------------------------
def main():
    p = argparse.ArgumentParser(description="Simulador de la Pi 5: publica PPG sintético en ppg-crudo")
    p.add_argument("--modo", choices=PERFILES.keys(), default="neutro",
                   help="perfil fisiológico simulado (default: neutro)")
    p.add_argument("--duracion", type=float, default=None,
                   help="segundos a simular (default: sin fin)")
    p.add_argument("--lotes", type=int, default=None,
                   help="número de lotes a enviar (tiene prioridad sobre --duracion)")
    p.add_argument("--dispositivo", default="pi5-sim",
                   help="dispositivo_id (default: pi5-sim, para distinguirlo de la Pi real)")
    p.add_argument("--con-rojo", action="store_true", help="incluye el canal red_raw")
    p.add_argument("--sin-dedo-cada", type=int, default=0,
                   help="cada N lotes simula uno sin contacto (0 = nunca)")
    p.add_argument("--rapido", action="store_true",
                   help="no espera 2 s entre lotes (útil para pruebas; timestamps siguen siendo coherentes)")
    p.add_argument("--dry-run", action="store_true", help="imprime el JSON y no se conecta a Kafka")
    p.add_argument("--semilla", type=int, default=None, help="semilla aleatoria para resultados repetibles")
    args = p.parse_args()

    cfg = cargar_config()
    servidor = f"{cfg['host_ip']}:{cfg['puerto']}"

    # Sesión: S + fecha/hora UTC de inicio (contrato §2)
    inicio = datetime.now(timezone.utc).replace(microsecond=0)
    sesion_id = "S" + inicio.strftime("%Y%m%d-%H%M%S")

    total_lotes = args.lotes
    if total_lotes is None and args.duracion is not None:
        total_lotes = max(1, int(args.duracion / DURACION_LOTE_S))

    perfil = PERFILES[args.modo]
    gen = GeneradorPPG(perfil["bpm"], perfil["variabilidad_ms"], args.semilla)

    print(f".env: {cfg['ruta_env']} ({'encontrado' if cfg['env_encontrado'] else 'no encontrado, uso defaults/entorno'})")
    print(f"Kafka: {servidor} | topic: {TOPIC} | sesión: {sesion_id} | modo: {args.modo} "
          f"(~{perfil['bpm']} bpm) | lotes: {total_lotes or 'sin fin'}")

    productor = None if args.dry_run else crear_productor(servidor)

    seq = 0
    reloj_inicio = time.monotonic()
    try:
        while total_lotes is None or seq < total_lotes:
            sin_dedo = args.sin_dedo_cada > 0 and seq > 0 and seq % args.sin_dedo_cada == 0
            ir, rojo = gen.lote(dedo=not sin_dedo, con_rojo=args.con_rojo)

            # ts_inicio se calcula a partir de seq: sin deriva y alineado a la sesión
            ts_inicio = inicio + timedelta(seconds=seq * DURACION_LOTE_S)
            msg = construir_mensaje(args.dispositivo, sesion_id, seq, ts_inicio, ir, rojo)
            verificar_mensaje(msg)

            if args.dry_run:
                print(json.dumps(msg, ensure_ascii=False))
            else:
                productor.send(TOPIC, key=sesion_id, value=msg)
                media = sum(ir) / len(ir)
                print(f"seq={seq:4d}  ts={msg['ts_inicio']}  media_IR={media:9.0f}  "
                      f"dedo={'sí' if msg['dedo_detectado'] else 'NO'}")

            seq += 1

            # Espera hasta el momento exacto del siguiente lote (evita acumular retraso)
            if not args.rapido and (total_lotes is None or seq < total_lotes):
                objetivo = reloj_inicio + seq * DURACION_LOTE_S
                time.sleep(max(0.0, objetivo - time.monotonic()))
    except KeyboardInterrupt:
        print("\nDetenido por el usuario.")
    finally:
        if productor is not None:
            productor.flush(timeout=10)
            productor.close()
        print(f"Enviados {seq} lotes de la sesión {sesion_id}.")


if __name__ == "__main__":
    main()