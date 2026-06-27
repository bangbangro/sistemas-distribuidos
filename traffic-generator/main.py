from kafka import KafkaProducer
import redis
import random
import time
import json
import socket
import uuid

# ─── Conexión Redis ───
r = redis.Redis(host='redis', port=6379, decode_responses=True)

# ─── Esperar Kafka ───
def wait_for_kafka(host='kafka', port=9092, timeout=120):
    print("Esperando Kafka...")
    start = time.time()
    while True:
        try:
            sock = socket.create_connection((host, port), timeout=3)
            sock.close()
            print("Puerto Kafka disponible, esperando inicialización...")
            time.sleep(5)
            return
        except (socket.error, ConnectionRefusedError):
            if time.time() - start > timeout:
                raise TimeoutError("Kafka no disponible tras 120s")
            print(f"   Puerto no disponible aún, reintentando en 3s...")
            time.sleep(3)

wait_for_kafka()

# ─── Kafka Producer ───
producer = None
while True:
    try:
        producer = KafkaProducer(
            bootstrap_servers='kafka:9092',
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        print("Kafka Producer conectado")
        break
    except Exception as e:
        print(f"   Producer falló: {e}, reintentando en 3s...")
        time.sleep(3)

# ─── Zonas y Queries ───
zonas = ["Z1", "Z2", "Z3", "Z4", "Z5"]

def generar_consulta(modo="zipf"):
    """Genera una consulta geoespacial con distribución Zipf o uniforme."""
    if modo == "uniforme":
        zona = random.choice(zonas)
    else:
        zona = random.choices(
            zonas,
            weights=[70, 15, 7, 5, 3]
        )[0]

    query = f"Q{random.randint(1, 5)}"
    conf = random.choice([0.6, 0.7, 0.8, 0.9])

    payload = {
        "query_id": str(uuid.uuid4()),
        "timestamp": time.time(),
        "zona": zona,
        "query": query,
        "confidence": conf,
        "reintentos": 0
    }

    if query == "Q4":
        zona_b = random.choice([z for z in zonas if z != zona])
        payload["zona_b"] = zona_b

    return payload

# ─── Control de tráfico (spike) ───
def get_delay():
    """
    Lee el modo actual desde Redis.
    normal: 1 consulta cada 100ms
    spike:  10 consultas por iteración sin delay (ráfaga)
    """
    modo = r.get("traffic_mode")
    if modo == "spike":
        return 0, 10
    return 0.1, 1

def enviar_consulta(modo_trafico):
    """Genera y publica una consulta en el tópico principal de Kafka."""
    request = generar_consulta(modo_trafico)
    q = request["query"]
    z = request["zona"]
    c = request["confidence"]

    if q == "Q4":
        key = f"compare:density:{z}:{request['zona_b']}:conf={c}"
    elif q == "Q5":
        key = f"confidence_dist:{z}:bins=5"
    else:
        prefijos = {"Q1": "count", "Q2": "area", "Q3": "density"}
        key = f"{prefijos[q]}:{z}:conf={c}"

    # TODAS las consultas van a Kafka (los consumers se encargan del cache)
    producer.send("consultas", request)
    print(f"TRAFFIC: {key} -> Kafka [id={request['query_id'][:8]}]")

# ─── Modo de tráfico configurable ───
MODO_TRAFICO = "uniforme"
print(f"Generando tráfico en modo: {MODO_TRAFICO}...")
print("Control: redis-cli set traffic_mode spike|normal")

r.set("traffic_mode", "normal")

consultas_enviadas = 0
start_time = time.time()

while True:
    delay, cantidad = get_delay()

    if cantidad > 1:
        print(f"⚡ SPIKE ACTIVO — enviando {cantidad} consultas de golpe")

    for _ in range(cantidad):
        enviar_consulta(MODO_TRAFICO)
        consultas_enviadas += 1

    # Log de progreso cada 100 consultas
    if consultas_enviadas % 100 == 0:
        elapsed = time.time() - start_time
        rate = consultas_enviadas / elapsed if elapsed > 0 else 0
        print(f"[Progreso] {consultas_enviadas} consultas enviadas | {rate:.1f} q/s")

    time.sleep(delay)
