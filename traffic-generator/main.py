import redis
import random
import time
import json

r = redis.Redis(
    host='redis',
    port=6379,
    decode_responses=True
)

zonas = ["Z1", "Z2", "Z3", "Z4", "Z5"]

def generar_consulta(modo="zipf"):
    if modo == "uniforme":
        zona = random.choice(zonas)
    else:
        zona = random.choices(
            zonas,
            weights=[70, 15, 7, 5, 3] # Sesgo hacia Z1
        )[0]

    query = f"Q{random.randint(1,5)}"
    conf = random.choice([0.6, 0.7, 0.8, 0.9])

    payload = {
        "zona": zona,
        "query": query,
        "confidence": conf
    }

    if query == "Q4":
        zona_b = random.choice([z for z in zonas if z != zona])
        payload["zona_b"] = zona_b

    return payload

MODO_TRAFICO = "uniforme"
print(f"Generando tráfico en modo: {MODO_TRAFICO}...")

while True:
    request = generar_consulta(MODO_TRAFICO)
    
    q, z, c = request["query"], request["zona"], request["confidence"]
    
    if q == "Q4":
        key = f"compare:density:{z}:{request['zona_b']}:conf={c}"
    elif q == "Q5":
        key = f"confidence_dist:{z}:bins=5"
    else:
        prefijos = {"Q1": "count", "Q2": "area", "Q3": "density"}
        key = f"{prefijos[q]}:{z}:conf={c}"

    start_time = time.time()
    res = r.get(key)

    if res:
        latencia = time.time() - start_time
        r.lpush("metricas_cola", f"HIT,{latencia}")
        print(f"TRAFFIC: {key} -> HIT")
    else:
        r.lpush("cola_consultas", json.dumps(request))
        print(f"TRAFFIC: {key} -> MISS (Enviado a cola)")

    time.sleep(0.1)
