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
            weights=[70, 10, 10, 5, 5]
                  )[0]

    query = f"Q{random.randint(1,5)}"

    conf = random.choice([0.6, 0.7, 0.8])

    return zona, query, conf


print("Generando tráfico...")


modo_trafico = "zipf"


while True:

    z, q, c = generar_consulta(modo_trafico)

    key = f"{q}:{z}:conf={c}"

    start = time.time()

    res = r.get(key)

    if res:
      
        tipo = "HIT"
        print(f"TRAFFIC: {key} -> HIT (Caché)")

    else:

        tipo = "MISS"
        print(f"TRAFFIC: {key} -> MISS (Enviando a cola)")

        request = {

            "zona": z,
            "query": q,
            "confidence": c
        }

        r.lpush(
            "cola_consultas",
            json.dumps(request)
        )

        while True:

            res = r.get(key)

            if res:
                break
 latencia = time.time() - start

    r.lpush(
        "metricas_cola",
        f"{tipo},{latencia}"
    )


    time.sleep(0.05)
