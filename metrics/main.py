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


latencias_generales = []
latencias_hits = []
latencias_misses = []

start_time = time.time()

print("Métricas esperando datos...")

while True:
    dato = r.rpop("metricas_cola")

    if dato:
        tipo, lat = dato.split(",")
        lat = float(lat)

        latencias_generales.append(lat)

        if tipo == "HIT":
            hits += 1
            latencias_hits.append(lat)
        else:
            misses += 1
            latencias_misses.append(lat)

        total = hits + misses


        if total % 10 == 0:
            # Métricas base
            p50 = np.percentile(latencias_generales, 50)
            p95 = np.percentile(latencias_generales, 95)

            tiempo_segundos = time.time() - start_time
            tiempo_minutos = tiempo_segundos / 60

            throughput = total / tiempo_segundos
            hit_rate = hits / total


            # Pedimos a Redis directamente cuántas llaves ha borrado
            info_redis = r.info('stats')
            evictions = info_redis.get('evicted_keys', 0)
            eviction_rate = evictions / tiempo_minutos if tiempo_minutos > 0 else 0


            t_cache = np.mean(latencias_hits) if latencias_hits else 0
            t_db = np.mean(latencias_misses) if latencias_misses else 0

            cache_efficiency = ((hits * t_cache) - (misses * t_db)) / total

            print(
                f"Hits={hits} "
                f"Misses={misses} "
                f"HitRate={hit_rate:.2f} "
                f"p50={p50:.4f}s "
                f"p95={p95:.4f}s "
                f"Throughput={throughput:.2f}/s | "
                f"Evictions={evictions} (Rate={eviction_rate:.2f}/min) "
                f"CacheEfficiency={cache_efficiency:.5f}s"
            )

    time.sleep(0.05)
