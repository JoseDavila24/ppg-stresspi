"""
PPG-StressPi — Consumidor Pulse-PPG (clasificación de estrés)

Flujo:  ppg-crudo (Kafka) -> buffer de 30 s (15 lotes) -> embedding 512-d (Pulse-PPG)
        -> clasificador (relajado / neutro / estresado) -> SQLite (tabla estado)
                                                        -> Kafka (estado-estres)

Estado actual (esqueleto funcional):
  - Verifica WEIGHTS_DIR y registra si falta o está vacío (el servicio sigue corriendo).
  - Conecta a Kafka con reintentos (backoff exponencial, máx 30 s) y a SQLite en modo WAL.
  - Valida cada mensaje contra el contrato v1.0 (docs/contratos.md §3).
  - Agrupa los lotes en ventanas de 30 s alineadas a la sesión (seq // 15), igual que Spark.
  - Registra "ventana lista".
  - extraer_embedding(), clasificar() y PublicadorEstado.publicar() son stubs con TODO.
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
WEIGHTS_DIR = os.environ.get("WEIGHTS_DIR", "/app/weights")

TOPIC_ENTRADA = "ppg-crudo"
TOPIC_SALIDA = "estado-estres"
GRUPO_CONSUMIDOR = "pulse-ppg"  # grupo propio: recibe TODOS los mensajes, independiente de Spark

VERSION_CONTRATO = "1.0"
FS_HZ = 50
LOTES_POR_VENTANA = 15  # 15 lotes x 2 s = 30 s
VENTANA_S = 30
MUESTRAS_VENTANA = FS_HZ * VENTANA_S  # 1500
CALIDAD_MINIMA = 0.8  # fracción mínima de lotes con dedo para clasificar la ventana
DIM_EMBEDDING = 512
ESTADOS = ["relajado", "neutro", "estresado"]
MODELO_VERSION = "pulse-ppg-1.0+clf-0.3"
BACKOFF_MAX_S = 30
LATIDO_LOG_S = 60

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [PULSE-PPG] %(levelname)s: %(message)s",
)
log = logging.getLogger("pulseppg")
logging.getLogger("kafka").setLevel(logging.CRITICAL)  # silencia kafka-python (ponlo en WARNING para depurar)

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
# Pesos del modelo
# ------------------------------------------------------------------
def verificar_pesos(directorio: str) -> list:
    """Revisa WEIGHTS_DIR. Nunca detiene el servicio: solo avisa en el log."""
    if not os.path.isdir(directorio):
        log.warning("WEIGHTS_DIR %s no existe. Se seguirá sin modelo (solo se registrarán ventanas).",
                    directorio)
        return []
    archivos = sorted(f for f in os.listdir(directorio) if not f.startswith("."))
    if not archivos:
        log.warning("WEIGHTS_DIR %s está vacío. Coloca ahí los pesos de Pulse-PPG y del clasificador.",
                    directorio)
    else:
        total_mb = sum(os.path.getsize(os.path.join(directorio, f)) for f in archivos) / 1e6
        log.info("WEIGHTS_DIR %s: %d archivo(s), %.1f MB → %s",
                 directorio, len(archivos), total_mb, ", ".join(archivos[:10]))
    return archivos


class ModeloPulsePPG:
    """Envoltorio del modelo fundacional + clasificador. Hoy solo guarda la lista de pesos."""

    def __init__(self, directorio: str):
        self.directorio = directorio
        self.archivos = verificar_pesos(directorio)
        self.cargado = False
        # TODO: cargar el modelo aquí, una sola vez al arrancar:
        #   import torch
        #   self.encoder = <clase del repo Pulse-PPG>(...)
        #   estado = torch.load(os.path.join(directorio, "pulse_ppg.pt"), map_location="cpu")
        #   self.encoder.load_state_dict(estado); self.encoder.eval()
        #   self.clf = cargar el clasificador (p. ej. capa lineal torch o sklearn vía joblib)
        #   self.cargado = True
        # Usar torch.set_num_threads(2–4) para no acaparar la CPU que usa Spark.

    def extraer_embedding(self, senal_ir: np.ndarray):
        """STUB. Debe devolver un np.ndarray de forma (512,) o None.

        TODO:
          1. Preprocesar igual que en el entrenamiento de Pulse-PPG:
               - pasa-banda (p. ej. 0.5–8 Hz con scipy.signal.butter + filtfilt),
               - remuestrear a la frecuencia que espera el modelo si no es 50 Hz
                 (scipy.signal.resample_poly),
               - normalizar (z-score por ventana).
          2. Tensor (1, 1, N): torch.from_numpy(x).float()[None, None, :]
          3. with torch.no_grad(): emb = self.encoder(x).squeeze().numpy()
          4. Verificar emb.shape == (512,) antes de devolver.
        """
        return None

    def clasificar(self, embedding: np.ndarray):
        """STUB. Debe devolver {"relajado": p, "neutro": p, "estresado": p} (suma 1.0) o None.

        TODO:
          1. logits = self.clf(embedding)            # 3 salidas, en el orden de ESTADOS
          2. probs = softmax(logits)
          3. return dict(zip(ESTADOS, probs.round(3)))
        El clasificador se entrena aparte con las ventanas de calibración (relajado vs estresado).
        """
        return None


# ------------------------------------------------------------------
# Procesador: ventana -> mensaje estado-estres
# ------------------------------------------------------------------
class ProcesadorEstado:
    def __init__(self, modelo: ModeloPulsePPG):
        self.modelo = modelo

    def procesar(self, ventana: dict):
        """Devuelve el mensaje estado-estres listo para publicar, o None."""
        validos = [l for l in ventana["lotes"] if l["dedo_detectado"]]
        calidad = len(validos) / LOTES_POR_VENTANA
        senal = (np.concatenate([np.asarray(l["ir_raw"], dtype=float) for l in validos])
                 if validos else np.array([]))

        log.info("Ventana lista · sesión %s · ventana #%d (%s → %s) · %d muestras · "
                 "%d/%d lotes con dedo",
                 ventana["sesion_id"], ventana["indice"], ventana["ts_ventana_inicio"],
                 ventana["ts_ventana_fin"], senal.size, len(validos), LOTES_POR_VENTANA)

        # El modelo espera una señal continua de 30 s: con huecos grandes no se clasifica
        if calidad < CALIDAD_MINIMA:
            log.info("Calidad %.2f < %.2f, ventana no clasificada", calidad, CALIDAD_MINIMA)
            return None

        embedding = self.modelo.extraer_embedding(senal)
        if embedding is None:
            log.info("extraer_embedding() sin resultado (stub); no se guarda ni publica")
            return None

        probs = self.modelo.clasificar(embedding)
        if probs is None:
            log.info("clasificar() sin resultado (stub); no se guarda ni publica")
            return None

        estado = max(probs, key=probs.get)
        return {
            "version_contrato": VERSION_CONTRATO,
            "sesion_id": ventana["sesion_id"],
            "ts_ventana_inicio": ventana["ts_ventana_inicio"],
            "ts_ventana_fin": ventana["ts_ventana_fin"],
            "estado": estado,
            "confianza": round(float(probs[estado]), 3),
            "probabilidades": {k: round(float(v), 3) for k, v in probs.items()},
            "modelo_version": MODELO_VERSION,
        }


# ------------------------------------------------------------------
# Publicador: estado -> topic estado-estres
# ------------------------------------------------------------------
class PublicadorEstado:
    def __init__(self, bootstrap: str):
        self.bootstrap = bootstrap
        self.productor = None

    def publicar(self, estado: dict) -> None:
        """STUB.

        TODO (siguiente paso):
          1. Crear el KafkaProducer una sola vez (igual que en Spark: key_serializer utf-8,
             value_serializer json, acks=1), con try/except KafkaError.
          2. Validar con validar_estado_estres() de docs/contratos.md §9
             (probabilidades suman 1.0 ± 0.01 y confianza == probabilidades[estado]).
          3. self.productor.send(TOPIC_SALIDA, key=estado["sesion_id"], value=estado)
        """
        log.info("[pendiente] publicar en '%s': %s", TOPIC_SALIDA, estado)

    def cerrar(self):
        if self.productor is not None:
            try:
                self.productor.flush(timeout=5)
                self.productor.close()
            except Exception:
                pass


# ------------------------------------------------------------------
# Almacén SQLite: tabla estado (Pulse-PPG es su único escritor)
# ------------------------------------------------------------------
SQL_CREAR_ESTADO = """
CREATE TABLE IF NOT EXISTS estado (
    id                INTEGER PRIMARY KEY AUTOINCREMENT,
    sesion_id         TEXT NOT NULL,
    ts_ventana_inicio TEXT NOT NULL,
    ts_ventana_fin    TEXT NOT NULL,
    estado            TEXT NOT NULL CHECK (estado IN ('relajado','neutro','estresado')),
    confianza         REAL NOT NULL CHECK (confianza BETWEEN 0 AND 1),
    prob_relajado     REAL,
    prob_neutro       REAL,
    prob_estresado    REAL,
    modelo_version    TEXT NOT NULL,
    creado_en         TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
    UNIQUE (sesion_id, ts_ventana_fin)
);
CREATE INDEX IF NOT EXISTS idx_estado_sesion ON estado (sesion_id, estado);
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
            self.con.executescript(SQL_CREAR_ESTADO)
            log.info("SQLite listo en %s (journal_mode=%s)", self.ruta, modo)
            return True
        except (sqlite3.Error, OSError) as e:
            log.error("No se pudo abrir SQLite en %s: %s (se reintentará)", self.ruta, e)
            self.cerrar()
            return False

    def guardar(self, e: dict) -> None:
        if self.con is None and not self.conectar():
            return
        p = e.get("probabilidades") or {}
        try:
            with self.con:
                self.con.execute(
                    "INSERT OR IGNORE INTO estado (sesion_id, ts_ventana_inicio, ts_ventana_fin, "
                    "estado, confianza, prob_relajado, prob_neutro, prob_estresado, modelo_version) "
                    "VALUES (?,?,?,?,?,?,?,?,?)",
                    (e["sesion_id"], e["ts_ventana_inicio"], e["ts_ventana_fin"], e["estado"],
                     e["confianza"], p.get("relajado"), p.get("neutro"), p.get("estresado"),
                     e["modelo_version"]))
        except sqlite3.Error as err:
            log.error("Error guardando en SQLite: %s", err)
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
    log.info("Iniciando · Kafka=%s · SQLite=%s · pesos=%s", KAFKA_BOOTSTRAP, SQLITE_PATH, WEIGHTS_DIR)

    modelo = ModeloPulsePPG(WEIGHTS_DIR)
    consumidor = ConsumidorPPG(KAFKA_BOOTSTRAP, TOPIC_ENTRADA, GRUPO_CONSUMIDOR)
    acumulador = AcumuladorVentanas()
    procesador = ProcesadorEstado(modelo)
    publicador = PublicadorEstado(KAFKA_BOOTSTRAP)
    almacen = AlmacenSQLite(SQLITE_PATH)
    almacen.conectar()

    recibidos = 0
    ultimo_latido = datetime.now(timezone.utc)
    try:
        while not parar.is_set():
            try:
                for msg in consumidor.leer():
                    recibidos += 1
                    for ventana in acumulador.agregar(msg):
                        estado = procesador.procesar(ventana)
                        if estado:
                            almacen.guardar(estado)
                            publicador.publicar(estado)
            except Exception:
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