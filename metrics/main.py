import redis
import time
import numpy as np

r = redis.Redis(
    host='redis',
    port=6379,
    decode_responses=True
)

hits = 0
misses = 0

latencias = []

start_time = time.time()

print("Métricas esperando datos...")

while True:

    dato = r.rpop("metricas_cola")

    if dato:

        tipo, lat = dato.split(",")
      
        lat = float(lat)

        latencias.append(lat)

        if tipo == "HIT":
            hits += 1
        else:
            misses += 1

        total = hits + misses

        if total % 50 == 0:

            p50 = np.percentile(
                latencias,
                50
            )

            p95 = np.percentile(
                latencias,
                95
            )

            tiempo = time.time() - start_time

            throughput = total / tiempo

            hit_rate = hits / total

            print(
                f"Hits={hits} "
                f"Misses={misses} "
                f"HitRate={hit_rate:.2f} "
                f"p50={p50:.4f}s "
                f"p95={p95:.4f}s "
                f"Throughput={throughput:.2f}/s"
            )

    time.sleep(0.05)
