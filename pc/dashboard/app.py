"""
PPG-StressPi — Dashboard Flask

Rutas:
  GET /             página inicial (templates/index.html)
  GET /health       {"status": "ok"}  (para el healthcheck de Docker)
  GET /api/status   estado de la conexión a Kafka y SQLite
  GET /api/metricas últimas 20 filas de la tabla metricas (orden cronológico)
  GET /api/estado   últimas 20 filas de la tabla estado (orden cronológico)

Estado actual (esqueleto funcional):
  - Un hilo en segundo plano verifica Kafka (backoff exponencial, máx 30 s) sin bloquear las peticiones.
  - SQLite se abre por petición, en modo WAL; si la base o las tablas no existen, devuelve listas vacías.
  - Consumo de los 3 topics y WebSocket (flask-socketio) quedan como TODO.
"""

import logging
import os
import signal
import sqlite3
import threading
from datetime import datetime, timezone

from flask import Flask, jsonify, render_template
from kafka import KafkaConsumer
from kafka.errors import KafkaError  # en kafka-python 2.x NoBrokersAvailable hereda de KafkaError
from werkzeug.serving import make_server

# ------------------------------------------------------------------
# Configuración
# ------------------------------------------------------------------
KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092")
SQLITE_PATH = os.environ.get("SQLITE_PATH", "/data/ppg.db")
HOST, PUERTO = "0.0.0.0", 5000

TOPICS = ["ppg-crudo", "metricas-hrv", "estado-estres"]
LIMITE_FILAS = 20
INTERVALO_CHEQUEO_S = 15  # con Kafka arriba, se revisa cada 15 s
BACKOFF_MAX_S = 30

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [DASHBOARD] %(levelname)s: %(message)s",
)
log = logging.getLogger("dashboard")
logging.getLogger("kafka").setLevel(logging.CRITICAL)  # silencia kafka-python (ponlo en WARNING para depurar)
logging.getLogger("werkzeug").setLevel(logging.WARNING)  # sin una línea por cada petición

parar = threading.Event()


def opciones_timeout_kafka(segundos: int = 5) -> dict:
    """Timeout corto al conectar, para que SIGTERM no espere 30 s. El nombre de la opción
    cambia entre kafka-python 2.x (api_version_auto_timeout_ms) y 3.x (bootstrap_timeout_ms)."""
    claves = ("api_version_auto_timeout_ms", "bootstrap_timeout_ms")
    return {k: segundos * 1000 for k in claves if k in KafkaConsumer.DEFAULT_CONFIG}


def ahora_iso() -> str:
    dt = datetime.now(timezone.utc)
    return dt.strftime("%Y-%m-%dT%H:%M:%S.") + f"{dt.microsecond // 1000:03d}Z"


# ------------------------------------------------------------------
# Kafka: solo verificación de conexión (hilo en segundo plano)
# ------------------------------------------------------------------
class MonitorKafka(threading.Thread):
    def __init__(self, bootstrap: str):
        super().__init__(daemon=True, name="monitor-kafka")
        self.bootstrap = bootstrap
        self.candado = threading.Lock()
        self.estado = {"conectado": False, "bootstrap": bootstrap, "topics": {},
                       "ultimo_chequeo": None, "error": "sin verificar todavía"}

    def run(self):
        intento = 0
        while not parar.is_set():
            ok = self._verificar()
            if ok:
                intento = 0
                espera = INTERVALO_CHEQUEO_S
            else:
                espera = min(2 ** intento, BACKOFF_MAX_S)
                intento += 1
                log.warning("Kafka no disponible. Reintento #%d en %d s", intento, espera)
            parar.wait(espera)

    def _verificar(self) -> bool:
        consumidor = None
        try:
            consumidor = KafkaConsumer(bootstrap_servers=self.bootstrap, **opciones_timeout_kafka())
            existentes = consumidor.topics()
            nuevo = {"conectado": True, "bootstrap": self.bootstrap,
                     "topics": {t: t in existentes for t in TOPICS},
                     "ultimo_chequeo": ahora_iso(), "error": None}
            if not self.estado["conectado"]:
                log.info("Conectado a Kafka %s · topics: %s", self.bootstrap, nuevo["topics"])
            ok = True
        except (KafkaError, ValueError, OSError) as e:
            nuevo = {"conectado": False, "bootstrap": self.bootstrap, "topics": {},
                     "ultimo_chequeo": ahora_iso(), "error": str(e) or e.__class__.__name__}
            ok = False
        finally:
            if consumidor is not None:
                try:
                    consumidor.close()
                except Exception:
                    pass
        with self.candado:
            self.estado = nuevo
        return ok

    def resumen(self) -> dict:
        with self.candado:
            return dict(self.estado)

    # TODO (consumo en vivo): en otro hilo (o tarea de eventlet), un KafkaConsumer suscrito a
    # TOPICS con group_id="dashboard" y auto_offset_reset="latest". Por cada mensaje:
    #   - ppg-crudo     -> reenviar ir_raw al navegador (gráfica de señal); si dedo_detectado
    #                      es false, mostrar "sin contacto".
    #   - metricas-hrv  -> actualizar tarjetas BPM / SDNN / RMSSD y la gráfica de tendencia.
    #   - estado-estres -> actualizar el indicador relajado / neutro / estresado.
    # Validar cada mensaje con los esquemas de docs/contratos.md §9 y descartar los inválidos.


# ------------------------------------------------------------------
# SQLite: lectura (el Dashboard nunca escribe)
# ------------------------------------------------------------------
def abrir_sqlite():
    """Devuelve una conexión o None si la base aún no existe. mode=rw evita crear un archivo vacío."""
    if not os.path.exists(SQLITE_PATH):
        return None
    con = sqlite3.connect(f"file:{SQLITE_PATH}?mode=rw", uri=True, timeout=5)
    con.row_factory = sqlite3.Row
    con.execute("PRAGMA busy_timeout=5000;")
    con.execute("PRAGMA journal_mode=WAL;")
    return con


def ultimas_filas(tabla: str) -> list:
    """Últimas LIMITE_FILAS filas en orden cronológico. [] si no hay base, tabla o hay error."""
    assert tabla in ("metricas", "estado")  # nombre fijo: no viene del usuario
    con = None
    try:
        con = abrir_sqlite()
        if con is None:
            return []
        filas = con.execute(f"SELECT * FROM {tabla} ORDER BY id DESC LIMIT ?", (LIMITE_FILAS,)).fetchall()
        return [dict(f) for f in reversed(filas)]
    except sqlite3.OperationalError as e:
        if "no such table" not in str(e):
            log.warning("SQLite (%s): %s", tabla, e)
        return []
    except sqlite3.Error as e:
        log.warning("SQLite (%s): %s", tabla, e)
        return []
    finally:
        if con is not None:
            con.close()


def estado_sqlite() -> dict:
    info = {"ruta": SQLITE_PATH, "existe": os.path.exists(SQLITE_PATH), "conectado": False,
            "journal_mode": None, "tablas": {}, "error": None}
    con = None
    try:
        con = abrir_sqlite()
        if con is None:
            info["error"] = "la base aún no existe (la crean Spark / Pulse-PPG al arrancar)"
            return info
        info["journal_mode"] = con.execute("PRAGMA journal_mode;").fetchone()[0]
        existentes = {r[0] for r in con.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        for tabla in ("metricas", "estado"):
            info["tablas"][tabla] = (con.execute(f"SELECT COUNT(*) FROM {tabla}").fetchone()[0]
                                     if tabla in existentes else None)  # None = tabla no creada
        info["conectado"] = True
    except sqlite3.Error as e:
        info["error"] = str(e)
    finally:
        if con is not None:
            con.close()
    return info


# ------------------------------------------------------------------
# Flask
# ------------------------------------------------------------------
app = Flask(__name__)
monitor = MonitorKafka(KAFKA_BOOTSTRAP)

# TODO (tiempo real): cambiar a flask-socketio con eventlet:
#   from flask_socketio import SocketIO
#   socketio = SocketIO(app, async_mode="eventlet", cors_allowed_origins="*")
#   ... y en el consumidor de Kafka: socketio.emit("metricas", msg) / ("estado", msg) / ("senal", msg)
#   Arrancar con socketio.run(app, host=HOST, port=PUERTO) en lugar de make_server.


@app.route("/")
def inicio():
    return render_template("index.html", iniciado=ahora_iso())


@app.route("/health")
def health():
    return jsonify({"status": "ok"})


@app.route("/api/status")
def api_status():
    kafka = monitor.resumen()
    sqlite_info = estado_sqlite()
    return jsonify({
        "status": "ok" if kafka["conectado"] and sqlite_info["conectado"] else "degradado",
        "ts": ahora_iso(),
        "kafka": kafka,
        "sqlite": sqlite_info,
    })


@app.route("/api/metricas")
def api_metricas():
    return jsonify(ultimas_filas("metricas"))


@app.route("/api/estado")
def api_estado():
    return jsonify(ultimas_filas("estado"))


# ------------------------------------------------------------------
# Programa principal
# ------------------------------------------------------------------
def main():
    log.info("Iniciando · Kafka=%s · SQLite=%s · http://%s:%d", KAFKA_BOOTSTRAP, SQLITE_PATH, HOST, PUERTO)
    if not os.path.exists(SQLITE_PATH):
        log.warning("SQLite %s aún no existe; las APIs devolverán listas vacías", SQLITE_PATH)

    monitor.start()
    servidor = make_server(HOST, PUERTO, app, threaded=True)
    hilo_http = threading.Thread(target=servidor.serve_forever, name="http", daemon=True)
    hilo_http.start()

    def manejar(signum, _frame):
        log.info("Señal %s recibida, cerrando…", signal.Signals(signum).name)
        parar.set()
    signal.signal(signal.SIGTERM, manejar)
    signal.signal(signal.SIGINT, manejar)

    parar.wait()
    servidor.shutdown()  # termina las peticiones en curso y libera el puerto
    log.info("Detenido limpiamente")


if __name__ == "__main__":
    main()