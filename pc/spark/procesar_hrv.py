"""
PPG-StressPi — Consumidor HRV (contenedor Spark)

Flujo:  ppg-crudo (Kafka) -> buffer de 30 s (15 lotes) -> cálculo HRV -> SQLite (tabla metricas)
                                                                       -> Kafka (metricas-hrv)

Estado actual (esqueleto funcional):
  - Conecta a Kafka con reintentos (backoff exponencial, máx 30 s) y a SQLite en modo WAL.
  - Valida cada mensaje contra el contrato v1.0 (docs/contratos.md §3).
  - Agrupa los lotes en ventanas de 30 s alineadas a la sesión (seq // 15).
  - Registra "ventana lista con N muestras".
  - calcular_hrv() y PublicadorMetricas.publicar() son stubs con TODO.
"""

import json
import logging
import os
import signal
import sqlite3
import threading
from datetime import datetime, timedelta, timezone

import numpy as np
from jsonschema import ValidationError, validate
from kafka import KafkaConsumer
from kafka.errors import KafkaError  # en kafka-python 2.x NoBrokersAvailable hereda de KafkaError

# ------------------------------------------------------------------
# Configuración
# ------------------------------------------------------------------
KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092")
SQLITE_PATH = os.environ.get("SQLITE_PATH", "/data/ppg.db")

TOPIC_ENTRADA = "ppg-crudo"
TOPIC_SALIDA = "metricas-hrv"
GRUPO_CONSUMIDOR = "spark-hrv"  # grupo propio: recibe TODOS los mensajes de ppg-crudo

VERSION_CONTRATO = "1.0"
FS_HZ = 50
LOTES_POR_VENTANA = 15  # 15 lotes x 2 s = 30 s
VENTANA_S = 30
BACKOFF_MAX_S = 30
LATIDO_LOG_S = 60  # cada cuánto se escribe un "sigo vivo" en el log

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [SPARK] %(levelname)s: %(message)s",
)
log = logging.getLogger("spark")
logging.getLogger("kafka").setLevel(logging.CRITICAL)  # silencia kafka-python (ponlo en WARNING para depurar)

# Se activa al recibir SIGTERM/SIGINT; todos los bucles lo revisan.
parar = threading.Event()


def opciones_timeout_kafka(segundos: int = 5) -> dict:
    """Timeout corto al conectar, para que SIGTERM no espere 30 s. El nombre de la opción
    cambia entre kafka-python 2.x (api_version_auto_timeout_ms) y 3.x (bootstrap_timeout_ms)."""
    claves = ("api_version_auto_timeout_ms", "bootstrap_timeout_ms")
    return {k: segundos * 1000 for k in claves if k in KafkaConsumer.DEFAULT_CONFIG}

# ------------------------------------------------------------------
# Contrato ppg-crudo v1.0 (copiado de docs/contratos.md §9)
# ------------------------------------------------------------------
TS = {"type": "string", "pattern": r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$"}
MUESTRAS = {"type": "array", "minItems": 100, "maxItems": 100,
            "items": {"type": "integer", "minimum": 0, "maximum": 262143}}
ESQUEMA_PPG_CRUDO = {
    "type": "object",
    "required": ["version_contrato", "sesion_id", "dispositivo_id", "seq", "ts_inicio",
                 "fs_hz", "n_muestras", "ir_raw", "dedo_detectado"],
    "properties": {
        "version_contrato": {"const": VERSION_CONTRATO},
        "seq": {"type": "integer", "minimum": 0},
        "ts_inicio": TS,
        "fs_hz": {"const": FS_HZ},
        "n_muestras": {"const": 100},
        "ir_raw": MUESTRAS,
        "red_raw": {"anyOf": [MUESTRAS, {"type": "null"}]},
        "dedo_detectado": {"type": "boolean"},
    },
}


def validar_ppg_crudo(msg: dict) -> None:
    validate(msg, ESQUEMA_PPG_CRUDO)
    if msg.get("red_raw") and len(msg["red_raw"]) != len(msg["ir_raw"]):
        raise ValidationError("red_raw e ir_raw con longitudes distintas")


# ------------------------------------------------------------------
# Utilidades de tiempo (todo en UTC, formato del contrato)
# ------------------------------------------------------------------
def a_iso(dt: datetime) -> str:
    """datetime UTC -> '2026-09-24T19:35:00.000Z'."""
    return dt.strftime("%Y-%m-%dT%H:%M:%S.") + f"{dt.microsecond // 1000:03d}Z"


def desde_iso(texto: str) -> datetime:
    return datetime.strptime(texto, "%Y-%m-%dT%H:%M:%S.%fZ").replace(tzinfo=timezone.utc)


def inicio_de_sesion(sesion_id: str, lote: dict) -> datetime:
    """El sesion_id codifica el inicio: 'S20260924-193000'.
    Si no tiene ese formato, se estima con ts_inicio y seq del lote."""
    try:
        return datetime.strptime(sesion_id, "S%Y%m%d-%H%M%S").replace(tzinfo=timezone.utc)
    except ValueError:
        return desde_iso(lote["ts_inicio"]) - timedelta(seconds=2 * lote["seq"])


# ------------------------------------------------------------------
# Consumidor: conexión a Kafka, lectura, decodificación y validación
# ------------------------------------------------------------------
class ConsumidorPPG:
    def __init__(self, bootstrap: str, topic: str, grupo: str):
        self.bootstrap = bootstrap
        self.topic = topic
        self.grupo = grupo
        self.consumidor = None
        self.ultimo_seq = {}  # sesion_id -> último seq visto (para detectar lotes perdidos)

    def conectar(self) -> bool:
        """Reintenta con backoff exponencial (1, 2, 4, ... máx 30 s) hasta conectar o recibir SIGTERM."""
        intento = 0
        while not parar.is_set():
            try:
                self.consumidor = KafkaConsumer(
                    self.topic,
                    bootstrap_servers=self.bootstrap,
                    group_id=self.grupo,
                    auto_offset_reset="latest",  # en vivo: solo mensajes nuevos
                    enable_auto_commit=True,
                    **opciones_timeout_kafka(),
                )
                log.info("Conectado a Kafka %s · topic '%s' · grupo '%s'",
                         self.bootstrap, self.topic, self.grupo)
                return True
            except (KafkaError, ValueError) as e:
                espera = min(2 ** intento, BACKOFF_MAX_S)
                intento += 1
                log.warning("Kafka no disponible (%s). Reintento #%d en %d s", e, intento, espera)
                parar.wait(espera)
        return False

    def leer(self) -> list:
        """Devuelve los mensajes válidos recibidos en ~1 s. No lanza excepciones de Kafka."""
        if self.consumidor is None and not self.conectar():
            return []
        try:
            lotes = self.consumidor.poll(timeout_ms=1000)
        except KafkaError as e:
            log.error("Error leyendo de Kafka (%s). Se reconectará.", e)
            self.cerrar()
            return []

        mensajes = []
        for registros in lotes.values():
            for registro in registros:
                msg = self._decodificar(registro.value)
                if msg is not None:
                    mensajes.append(msg)
        return mensajes

    def _decodificar(self, crudo: bytes):
        try:
            msg = json.loads(crudo.decode("utf-8"))
            validar_ppg_crudo(msg)
        except (UnicodeDecodeError, json.JSONDecodeError) as e:
            log.warning("Mensaje que no es JSON válido, descartado: %s", e)
            return None
        except ValidationError as e:
            log.warning("Mensaje fuera de contrato, descartado: %s", e.message)
            return None

        # Contrato: un salto en seq = lotes perdidos -> se registra y se sigue
        anterior = self.ultimo_seq.get(msg["sesion_id"])
        if anterior is not None and msg["seq"] != anterior + 1:
            log.warning("Salto de seq en %s: %d -> %d", msg["sesion_id"], anterior, msg["seq"])
        self.ultimo_seq[msg["sesion_id"]] = msg["seq"]
        return msg

    def cerrar(self):
        if self.consumidor is not None:
            try:
                self.consumidor.close()
            except Exception:
                pass
            self.consumidor = None


# ------------------------------------------------------------------
# Acumulador: agrupa lotes en ventanas de 30 s alineadas a la sesión
# ------------------------------------------------------------------
class AcumuladorVentanas:
    """La ventana de un lote es seq // 15. Así Spark y Pulse-PPG producen exactamente
    las mismas ventanas (mismo ts_ventana_fin = clave de unión en el Dashboard)."""

    def __init__(self):
        self.sesion_id = None
        self.indice = None
        self.lotes = []

    def agregar(self, msg: dict) -> list:
        """Agrega un lote y devuelve las ventanas que se cerraron (0, 1 o 2)."""
        cerradas = []
        indice = msg["seq"] // LOTES_POR_VENTANA

        if self.lotes and msg["sesion_id"] == self.sesion_id and indice < self.indice:
            log.warning("Lote seq=%d llegó tarde (ventana ya cerrada), descartado", msg["seq"])
            return cerradas

        # Cambió la sesión o empezó otra ventana: se cierra la actual aunque esté incompleta
        if self.lotes and (msg["sesion_id"] != self.sesion_id or indice != self.indice):
            cerradas.append(self._cerrar())

        if not self.lotes:
            self.sesion_id, self.indice = msg["sesion_id"], indice
        self.lotes.append(msg)

        if len(self.lotes) == LOTES_POR_VENTANA:
            cerradas.append(self._cerrar())
        return cerradas

    def _cerrar(self) -> dict:
        lotes = sorted(self.lotes, key=lambda l: l["seq"])
        inicio = inicio_de_sesion(self.sesion_id, lotes[0]) + timedelta(seconds=VENTANA_S * self.indice)
        ventana = {
            "sesion_id": self.sesion_id,
            "indice": self.indice,
            "ts_ventana_inicio": a_iso(inicio),
            "ts_ventana_fin": a_iso(inicio + timedelta(seconds=VENTANA_S)),
            "lotes": lotes,
            "completa": len(lotes) == LOTES_POR_VENTANA,
        }
        self.sesion_id, self.indice, self.lotes = None, None, []
        return ventana


# ------------------------------------------------------------------
# Procesador: señal de la ventana -> métricas HRV
# ------------------------------------------------------------------
def calcular_hrv(senal_ir: np.ndarray, fs: int):
    """STUB. Debe devolver {"bpm", "sdnn_ms", "rmssd_ms", "n_latidos"} o None si la señal no sirve.

    TODO (implementación real, con numpy + scipy.signal):
      1. Filtro Butterworth pasa-banda 0.5–4 Hz (30–240 lpm), orden 2–4:
             b, a = butter(3, [0.5, 4.0], btype="bandpass", fs=fs)
             filtrada = filtfilt(b, a, senal_ir)      # filtfilt = sin desfase
      2. Invertir la señal: en el IR crudo del MAX30102 cada latido aparece como un VALLE
         (más sangre absorbe más luz), así que los picos se buscan en -filtrada.
      3. Detectar picos:
             picos, _ = find_peaks(-filtrada, distance=int(0.33 * fs), prominence=...)
         distance=0.33 s evita contar dos picos por latido (máx ~180 lpm).
         A 50 Hz cada muestra = 20 ms; refinar la posición del pico con interpolación
         parabólica sobre 3 puntos mejora bastante SDNN/RMSSD.
      4. Intervalos NN en ms: rr = np.diff(picos) / fs * 1000
         Descartar rr fuera de 300–2000 ms y saltos > 20 % respecto al anterior (artefactos).
      5. Métricas:
             bpm      = 60000 / rr.mean()
             sdnn_ms  = rr.std(ddof=1)
             rmssd_ms = np.sqrt(np.mean(np.diff(rr) ** 2))
             n_latidos = len(rr) + 1
      6. Si n_latidos < 10 -> return None (el contrato dice que no se publica).
    """
    return None


class ProcesadorHRV:
    def procesar(self, ventana: dict):
        """Devuelve el mensaje metricas-hrv listo para publicar, o None."""
        # Contrato: los lotes con dedo_detectado == false no entran al cálculo
        validos = [l for l in ventana["lotes"] if l["dedo_detectado"]]
        calidad = round(len(validos) / LOTES_POR_VENTANA, 2)
        # NOTA: al quitar lotes sin dedo quedan huecos; cuando calcular_hrv sea real,
        # conviene procesar solo tramos contiguos para no inventar un intervalo RR en el hueco.
        senal = (np.concatenate([np.asarray(l["ir_raw"], dtype=float) for l in validos])
                 if validos else np.array([]))

        log.info("Ventana lista con %d muestras · sesión %s · ventana #%d (%s → %s) · "
                 "%d/%d lotes con dedo · calidad %.2f",
                 senal.size, ventana["sesion_id"], ventana["indice"],
                 ventana["ts_ventana_inicio"], ventana["ts_ventana_fin"],
                 len(validos), LOTES_POR_VENTANA, calidad)
        if not ventana["completa"]:
            log.warning("Ventana #%d incompleta (%d lotes)", ventana["indice"], len(ventana["lotes"]))
        if senal.size == 0:
            return None

        resultado = calcular_hrv(senal, FS_HZ)
        if resultado is None:
            log.info("calcular_hrv() sin resultado (stub o señal insuficiente); no se guarda ni publica")
            return None

        return {
            "version_contrato": VERSION_CONTRATO,
            "sesion_id": ventana["sesion_id"],
            "ts_ventana_inicio": ventana["ts_ventana_inicio"],
            "ts_ventana_fin": ventana["ts_ventana_fin"],
            "ventana_s": VENTANA_S,
            "bpm": round(float(resultado["bpm"]), 2),
            "sdnn_ms": round(float(resultado["sdnn_ms"]), 2),
            "rmssd_ms": round(float(resultado["rmssd_ms"]), 2),
            "n_latidos": int(resultado["n_latidos"]),
            "calidad_senal": calidad,
        }


# ------------------------------------------------------------------
# Publicador: métricas -> topic metricas-hrv
# ------------------------------------------------------------------
class PublicadorMetricas:
    def __init__(self, bootstrap: str):
        self.bootstrap = bootstrap
        self.productor = None

    def publicar(self, metricas: dict) -> None:
        """STUB.

        TODO (siguiente paso):
          1. Crear el productor una sola vez (la primera vez que se llame):
                 from kafka import KafkaProducer
                 self.productor = KafkaProducer(
                     bootstrap_servers=self.bootstrap,
                     key_serializer=lambda k: k.encode("utf-8"),
                     value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                     acks=1)
             envolviéndolo en try/except KafkaError (si falla, loguear y seguir).
          2. Validar con ESQUEMA_METRICAS_HRV de docs/contratos.md §9.
          3. self.productor.send(TOPIC_SALIDA, key=metricas["sesion_id"], value=metricas)
             (la clave sesion_id conserva el orden, ver contrato §2).
        """
        log.info("[pendiente] publicar en '%s': %s", TOPIC_SALIDA, metricas)

    def cerrar(self):
        if self.productor is not None:
            try:
                self.productor.flush(timeout=5)
                self.productor.close()
            except Exception:
                pass


# ------------------------------------------------------------------
# Almacén SQLite: tabla metricas (Spark es su único escritor)
# ------------------------------------------------------------------
SQL_CREAR_METRICAS = """
CREATE TABLE IF NOT EXISTS metricas (
    id                INTEGER PRIMARY KEY AUTOINCREMENT,
    sesion_id         TEXT NOT NULL,
    ts_ventana_inicio TEXT NOT NULL,
    ts_ventana_fin    TEXT NOT NULL,
    bpm               REAL NOT NULL,
    sdnn_ms           REAL NOT NULL,
    rmssd_ms          REAL NOT NULL,
    n_latidos         INTEGER NOT NULL,
    calidad_senal     REAL,
    creado_en         TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
    UNIQUE (sesion_id, ts_ventana_fin)
);
"""


class AlmacenSQLite:
    def __init__(self, ruta: str):
        self.ruta = ruta
        self.con = None

    def conectar(self) -> bool:
        try:
            os.makedirs(os.path.dirname(self.ruta) or ".", exist_ok=True)
            self.con = sqlite3.connect(self.ruta, timeout=5)
            self.con.execute("PRAGMA busy_timeout=5000;")
            modo = self.con.execute("PRAGMA journal_mode=WAL;").fetchone()[0]
            self.con.executescript(SQL_CREAR_METRICAS)
            log.info("SQLite listo en %s (journal_mode=%s)", self.ruta, modo)
            return True
        except (sqlite3.Error, OSError) as e:
            log.error("No se pudo abrir SQLite en %s: %s (se reintentará)", self.ruta, e)
            self.cerrar()
            return False

    def guardar(self, m: dict) -> None:
        if self.con is None and not self.conectar():
            return
        try:
            with self.con:  # commit automático
                self.con.execute(
                    "INSERT OR IGNORE INTO metricas (sesion_id, ts_ventana_inicio, ts_ventana_fin, "
                    "bpm, sdnn_ms, rmssd_ms, n_latidos, calidad_senal) VALUES (?,?,?,?,?,?,?,?)",
                    (m["sesion_id"], m["ts_ventana_inicio"], m["ts_ventana_fin"], m["bpm"],
                     m["sdnn_ms"], m["rmssd_ms"], m["n_latidos"], m.get("calidad_senal")))
        except sqlite3.Error as e:
            log.error("Error guardando en SQLite: %s", e)
            self.cerrar()

    def cerrar(self):
        if self.con is not None:
            try:
                self.con.close()
            except Exception:
                pass
            self.con = None


# ------------------------------------------------------------------
# Programa principal
# ------------------------------------------------------------------
def instalar_senales():
    def manejar(signum, _frame):
        log.info("Señal %s recibida, cerrando…", signal.Signals(signum).name)
        parar.set()
    signal.signal(signal.SIGTERM, manejar)
    signal.signal(signal.SIGINT, manejar)


def main():
    instalar_senales()
    log.info("Iniciando · Kafka=%s · SQLite=%s", KAFKA_BOOTSTRAP, SQLITE_PATH)

    consumidor = ConsumidorPPG(KAFKA_BOOTSTRAP, TOPIC_ENTRADA, GRUPO_CONSUMIDOR)
    acumulador = AcumuladorVentanas()
    procesador = ProcesadorHRV()
    publicador = PublicadorMetricas(KAFKA_BOOTSTRAP)
    almacen = AlmacenSQLite(SQLITE_PATH)
    almacen.conectar()  # si falla, se reintenta al guardar

    recibidos = 0
    ultimo_latido = datetime.now(timezone.utc)
    try:
        while not parar.is_set():
            try:
                for msg in consumidor.leer():
                    recibidos += 1
                    for ventana in acumulador.agregar(msg):
                        metricas = procesador.procesar(ventana)
                        if metricas:
                            almacen.guardar(metricas)
                            publicador.publicar(metricas)
            except Exception:
                # Nada debe tumbar el servicio: se registra y se sigue
                log.exception("Error inesperado; el bucle continúa")
                parar.wait(1)

            ahora = datetime.now(timezone.utc)
            if (ahora - ultimo_latido).total_seconds() >= LATIDO_LOG_S:
                log.info("Activo · %d lotes recibidos · %d en buffer", recibidos, len(acumulador.lotes))
                ultimo_latido = ahora
    finally:
        consumidor.cerrar()
        publicador.cerrar()
        almacen.cerrar()
        log.info("Detenido limpiamente")


if __name__ == "__main__":
    main()