import redis
import time
import numpy as np

r = redis.Redis(host='redis', port=6379, decode_responses=True)

hits = 0
misses = 0
retries = 0
dlq_count = 0
recoveries = 0

latencias_generales = []
latencias_hits = []
latencias_misses = []

start_time = time.time()
print("Esperando metricas... ")

while True:
    dato = r.rpop("metricas_cola")

    if dato:
        tipo, lat = dato.split(",")
        lat = float(lat)

        if tipo == "HIT":
            hits += 1
            latencias_generales.append(lat)
            latencias_hits.append(lat)
        elif tipo == "MISS":
            misses += 1
            latencias_generales.append(lat)
            latencias_misses.append(lat)
        elif tipo == "RETRY":
            retries += 1
        elif tipo == "DLQ":
            dlq_count += 1
        elif tipo == "RECOVERY":
            recoveries += 1

        total_procesadas = hits + misses 

        
        if (total_procesadas + retries + dlq_count) % 10 == 0 and total_procesadas > 0:
            
            p50 = np.percentile(latencias_generales, 50)
            p95 = np.percentile(latencias_generales, 95)

            tiempo_segundos = time.time() - start_time
            
            throughput = total_procesadas / tiempo_segundos
            hit_rate = hits / total_procesadas
            
            
            retry_rate = retries / tiempo_segundos
            dlq_rate = dlq_count / tiempo_segundos
            recovery_rate = recoveries / tiempo_segundos

            print(
                f"\n--- Metricas --- "
                f"\nProcesadas (Exito)={total_procesadas} | Hits={hits} | Misses={misses} | HitRate={hit_rate:.2f}"
                f"\nLatencias: p50={p50:.4f}s | p95={p95:.4f}s | Throughput={throughput:.2f}/s"
                f"\nKAFKA -> Retries={retries} ({retry_rate:.2f}/s) | DLQ={dlq_count} ({dlq_rate:.2f}/s) | Recoveries={recoveries} ({recovery_rate:.2f}/s)"
            )

    time.sleep(0.05)