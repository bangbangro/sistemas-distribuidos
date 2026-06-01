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
            print(f"[HIT]  Latencia: {lat:.4f}s")
            hits += 1
            latencias_generales.append(lat)
            latencias_hits.append(lat)
        elif tipo == "MISS":
            print(f"[MISS] Latencia: {lat:.4f}s")
            misses += 1
            latencias_generales.append(lat)
            latencias_misses.append(lat)
        elif tipo == "RETRY":
            print(f"[RETRY] Timeout detectado - Enviando a reintentos")
            retries += 1
        elif tipo == "DLQ":
            print(f"[DLQ]  Consulta muerta - Enviada a Dead Letter Queue")
            dlq_count += 1
        elif tipo == "RECOVERY":
            print(f"[RECOVERY] Consulta rescatada exitosamente")
            recoveries += 1

        total_procesadas = hits + misses 
        total_eventos = total_procesadas + retries + dlq_count

        
        if total_eventos % 10 == 0 and total_eventos > 0:
            
            p50 = np.percentile(latencias_generales, 50) if latencias_generales else 0
            p95 = np.percentile(latencias_generales, 95) if latencias_generales else 0

            tiempo_segundos = time.time() - start_time
            
            throughput = total_procesadas / tiempo_segundos if tiempo_segundos > 0 else 0
            hit_rate = hits / total_procesadas if total_procesadas > 0 else 0
            
            retry_rate = retries / tiempo_segundos if tiempo_segundos > 0 else 0
            dlq_rate = dlq_count / tiempo_segundos if tiempo_segundos > 0 else 0
            recovery_rate = recoveries / tiempo_segundos if tiempo_segundos > 0 else 0

            
            info_redis = r.info('stats')
            evictions = info_redis.get('evicted_keys', 0)

            print(
                f"\n=================================================="
                f"\n Mericas: (Eventos: {total_eventos})"
                f"\nExitos={total_procesadas} | Hits={hits} | Misses={misses} | HitRate={hit_rate:.2f}"
                f"\nLatencias: p50={p50:.4f}s | p95={p95:.4f}s | Throughput={throughput:.2f}/s"
                f"\nKAFKA: Retries={retries} ({retry_rate:.2f}/s) | DLQ={dlq_count} | Recoveries={recoveries}"
                f"\nREDIS: Evictions={evictions} (Llaves borradas por caché llena)"
                f"\n==================================================\n"
            )

    time.sleep(0.05)