from kafka import KafkaConsumer, KafkaProducer
import time
import json
import redis
import random

r = redis.Redis(host='redis', port=6379, decode_responses=True)

MAX_REINTENTOS = 3

# ======================
# KAFKA WAIT + CONNECT
# ======================
print("⏳ Esperando Kafka...")
while True:
    try:
        consumer = KafkaConsumer(
            'consultas',
            'consultas_retry',
            bootstrap_servers='kafka:9092',
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            group_id='consumidores',
            auto_offset_reset='earliest'
        )
        producer = KafkaProducer(
            bootstrap_servers='kafka:9092',
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        print("✅ Kafka conectado")
        break
    except Exception as e:
        print("Kafka no listo aún:", e)
        time.sleep(5)

# ======================
# HELPERS
# ======================
def build_key(request):
    q = request["query"]
    z = request["zona"]
    c = request["confidence"]
    if q == "Q4":
        return f"compare:density:{z}:{request['zona_b']}:conf={c}"
    elif q == "Q5":
        return f"confidence_dist:{z}:bins=5"
    else:
        prefijos = {"Q1": "count", "Q2": "area", "Q3": "density"}
        return f"{prefijos[q]}:{z}:conf={c}"

def procesar(request):
    # Verificar si el generador está "caído"
    if r.get("generador_activo") == "0":
        raise Exception("Generador de respuestas caído (falla simulada)")
    
    # Fallo aleatorio normal 20%
    if random.random() < 0.2:
        raise Exception("Fallo temporal simulado en el procesamiento")
    
    time.sleep(0.01)
    return f"resultado_{request['query']}_{request['zona']}"

def enviar_retry(request):
    request["reintentos"] = request.get("reintentos", 0) + 1
    producer.send('consultas_retry', request)
    print(f"🔁 Reintento {request['reintentos']}/{MAX_REINTENTOS} → {build_key(request)}")
    r.lpush("metricas_cola", f"RETRY,{request['reintentos']}")

def enviar_dlq(request):
    producer.send('consultas_dlq', request)
    print(f"💀 DLQ → {build_key(request)} (falló {request.get('reintentos', 0)} veces)")
    r.lpush("metricas_cola", f"DLQ,{request.get('reintentos', 0)}")

# ======================
# LOOP PRINCIPAL
# ======================
print("🔄 Escuchando mensajes...")

for msg in consumer:
    request = msg.value
    key = build_key(request)
    start = time.time()

    # Asegurar campos de reintento
    if "reintentos" not in request:
        request["reintentos"] = 0
    if "timestamp" not in request:
        request["timestamp"] = time.time()

    # Revisar caché primero
    cached = r.get(key)
    if cached:
        latencia = time.time() - start
        r.lpush("metricas_cola", f"HIT,{latencia}")
        print(f"✅ HIT CACHE → {key}")
        continue

    # Cache miss: intentar procesar
    try:
        resultado = procesar(request)
        r.set(key, resultado, ex=300)
        latencia = time.time() - start
        r.lpush("metricas_cola", f"MISS,{latencia}")
        print(f"🔄 MISS CACHE → {key} | Procesado en {latencia:.4f}s")

    except Exception as e:
        print(f"⚠️  Error procesando {key}: {e}")
        if request["reintentos"] < MAX_REINTENTOS-1:
            enviar_retry(request)
        else:
            enviar_dlq(request)
