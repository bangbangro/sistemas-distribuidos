from kafka import KafkaConsumer, KafkaProducer
import redis
import json
import uuid
import time

cache = redis.Redis(host="redis", port=6379, decode_responses=True)

consumer = KafkaConsumer(
    "topic_consultas_principales",
    "topic_reintentos",
    bootstrap_servers="kafka:9092",
    auto_offset_reset="earliest",
    group_id="grupo-consumidores",
    value_deserializer=lambda m: json.loads(m.decode("utf-8"))
)

producer = KafkaProducer(
    bootstrap_servers="kafka:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)
MAX_RETRIES = 3

def generar_cache_key(consulta):
    q, z, c = consulta["query"], consulta["zona"], consulta["confidence"]
    if q == "Q4":
        return f"compare:density:{z}:{consulta['zona_b']}:conf={c}"
    elif q == "Q5":
        return f"confidence_dist:{z}:bins=5"
    else:
        prefijos = {"Q1": "count", "Q2": "area", "Q3": "density"}
        return f"{prefijos[q]}:{z}:conf={c}"

print("Consumer iniciado...")

for msg in consumer:
    consulta = msg.value
    cache_key = generar_cache_key(consulta)
    start_time = time.time()

    try:
        if cache.get(cache_key):
            latencia = time.time() - start_time
            cache.lpush("metricas_cola", f"HIT,{latencia}")
            print(f"CACHE HIT -> {cache_key}")
            continue

        print(f"CACHE MISS -> {cache_key}. Delegando al Generador...")

        req_id = str(uuid.uuid4())
        tarea = {"id": req_id, "query": consulta}
        
        cache.lpush("cola_tareas", json.dumps(tarea))

        respuesta_redis = cache.brpop(f"respuesta_{req_id}", timeout=5)

        if respuesta_redis is None:
            raise Exception("Timeout: El Generador de Respuestas no respondió")

        resultado_final = json.loads(respuesta_redis[1])

        payload = {"result": resultado_final, "padding": "X" * 20000}
        cache.set(cache_key, json.dumps(payload), ex=300)
        
        latencia = time.time() - start_time
        cache.lpush("metricas_cola", f"MISS,{latencia}")

        if consulta.get("retry_count", 0) > 0:
            cache.lpush("metricas_cola", "RECOVERY,0")

        print("Procesada y guardada en caché correctamente")

    except Exception as e:
        print("Fallo detectado:", e)
        consulta["retry_count"] = consulta.get("retry_count", 0) + 1

        if consulta["retry_count"] <= MAX_RETRIES:
            producer.send("topic_reintentos", consulta)
            cache.lpush("metricas_cola", "RETRY,0")
            print(f"Enviada a topic_reintentos (intento {consulta['retry_count']})")
        else:
            producer.send("topic_dlq", consulta)
            cache.lpush("metricas_cola", "DLQ,0")
            print("Enviada a DLQ")
        producer.flush()
