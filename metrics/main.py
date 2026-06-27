"""
Sistema de Métricas - Tarea 2 & 3
Consume eventos de métricas desde Kafka (metrics-topic) y Redis (metricas_cola).
Calcula: throughput, p50/p95, retry rate, recovery rate, DLQ rate, 
         backlog size, recovery time.
"""
from kafka import KafkaConsumer, KafkaAdminClient
from kafka.admin import NewTopic
from kafka.errors import TopicAlreadyExistsError
import redis
import time
import json
import numpy as np
import threading
import socket
from datetime import datetime

# ─── Conexión Redis ───
r = redis.Redis(host='redis', port=6379, decode_responses=True)

# ─── Esperar Kafka ───
def wait_for_kafka(host='kafka', port=9092, timeout=120):
    print("[Metrics] Esperando Kafka...")
    start = time.time()
    while True:
        try:
            sock = socket.create_connection((host, port), timeout=3)
            sock.close()
            print("[Metrics] Kafka disponible, esperando inicialización...")
            time.sleep(5)
            return
        except (socket.error, ConnectionRefusedError):
            if time.time() - start > timeout:
                raise TimeoutError("Kafka no disponible tras 120s")
            time.sleep(3)

wait_for_kafka()

# ─── Kafka Consumer para métricas ───
kafka_consumer = None
while True:
    try:
        kafka_consumer = KafkaConsumer(
            'metrics-topic',
            bootstrap_servers='kafka:9092',
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            group_id='metrics-monitor',
            auto_offset_reset='earliest',
            consumer_timeout_ms=500  # Non-blocking: timeout after 500ms
        )
        print("[Metrics] Kafka Consumer conectado a metrics-topic")
        break
    except Exception as e:
        print(f"[Metrics] Kafka no listo aún: {e}")
        time.sleep(5)

# ─── Estado de métricas ───
hits = 0
misses = 0
retries_total = 0
dlq_total = 0
recovered_total = 0  # Consultas recuperadas tras reintento exitoso
total_exitosas = 0

latencias_generales = []
latencias_hits = []
latencias_misses = []

# Recovery time tracking
falla_detectada_time = None  # Timestamp cuando se detecta primera falla
recovery_time_last = None     # Último recovery time calculado
en_falla = False              # Flag: estamos en período de fallas

# Consumer tracking
consumer_counts = {}  # consumer_id -> consultas procesadas

# Ventana de tiempo para throughput
ventana_inicio = time.time()
consultas_ventana = 0

start_time = time.time()
ultimo_reporte = 0

# ─── Hilo para leer métricas legacy de Redis ───
def leer_redis_metricas():
    """Lee métricas del formato antiguo en Redis (backward compatibility)."""
    global hits, misses, retries_total, dlq_total, latencias_generales
    global latencias_hits, latencias_misses

    while True:
        try:
            dato = r.rpop("metricas_cola")
            if dato:
                partes = dato.split(",")
                tipo = partes[0]
                if tipo == "HIT":
                    lat = float(partes[1])
                    # No duplicar si ya viene de Kafka
                elif tipo == "MISS":
                    lat = float(partes[1])
                elif tipo == "RETRY":
                    pass
                elif tipo == "DLQ":
                    pass
                elif tipo == "DROP":
                    pass
            else:
                time.sleep(0.1)
        except Exception as e:
            time.sleep(0.5)

# Iniciar hilo Redis (solo para drenar la cola legacy)
redis_thread = threading.Thread(target=leer_redis_metricas, daemon=True)
redis_thread.start()

# ─── Obtener backlog de Kafka ───
def get_kafka_backlog():
    """Calcula el backlog estimado del tópico principal de consultas."""
    try:
        backlog = r.llen("metricas_cola")
        return backlog
    except Exception:
        return -1

# ─── Loop principal: consumir desde Kafka metrics-topic ───
print("[Metrics] Esperando eventos de métricas...")
print("=" * 100)

INTERVALO_REPORTE = 5  # Reportar cada 5 segundos

while True:
    try:
        # Leer mensajes de Kafka (non-blocking con timeout)
        mensajes = kafka_consumer.poll(timeout_ms=1000)

        for tp, messages in mensajes.items():
            for msg in messages:
                event = msg.value

                query_type = event.get("query_type", "?")
                cache_hit = event.get("cache_hit", False)
                latency_ms = event.get("latency_ms", 0)
                status = event.get("status", "success")
                consumer_id = event.get("consumer_id", "unknown")
                retries_count = event.get("retries", 0)
                recovered = event.get("recovered", False)
                latency_s = latency_ms / 1000.0

                # Contadores
                if status == "success":
                    total_exitosas += 1
                    consultas_ventana += 1
                    latencias_generales.append(latency_s)

                    if cache_hit:
                        hits += 1
                        latencias_hits.append(latency_s)
                    else:
                        misses += 1
                        latencias_misses.append(latency_s)

                    if recovered:
                        recovered_total += 1

                    # Tracking de consumers
                    consumer_counts[consumer_id] = consumer_counts.get(consumer_id, 0) + 1

                    # Recovery time: si estábamos en falla y ahora hay éxito
                    if en_falla and falla_detectada_time is not None:
                        recovery_time_last = time.time() - falla_detectada_time
                        en_falla = False
                        print(f"\n  ✅ RECUPERACIÓN DETECTADA | Recovery Time: {recovery_time_last:.2f}s")

                elif status == "retry":
                    retries_total += 1
                    # Detectar inicio de falla
                    if not en_falla:
                        en_falla = True
                        falla_detectada_time = time.time()
                        print(f"\n  ⚠️ FALLA DETECTADA | Inicio: {datetime.now().strftime('%H:%M:%S')}")

                elif status == "dlq":
                    dlq_total += 1

        # ─── Reporte periódico ───
        ahora = time.time()
        if ahora - ultimo_reporte >= INTERVALO_REPORTE:
            ultimo_reporte = ahora
            elapsed = ahora - start_time
            total = hits + misses

            if total > 0:
                p50 = np.percentile(latencias_generales, 50) if latencias_generales else 0
                p95 = np.percentile(latencias_generales, 95) if latencias_generales else 0
                throughput = total_exitosas / elapsed if elapsed > 0 else 0
                hit_rate = hits / total if total > 0 else 0

                # Retry rate: proporción de consultas que necesitaron reintento
                total_procesadas = total + retries_total + dlq_total
                retry_rate = retries_total / total_procesadas if total_procesadas > 0 else 0

                # Recovery rate: consultas recuperadas / total retries
                recovery_rate = recovered_total / retries_total if retries_total > 0 else 0

                # DLQ rate
                dlq_rate = dlq_total / total_procesadas if total_procesadas > 0 else 0

                # Backlog Redis
                backlog = r.llen("metricas_cola")

                # Info Redis (evictions)
                info_redis = r.info('stats')
                evictions = info_redis.get('evicted_keys', 0)

                # Throughput ventana (últimos N segundos)
                throughput_ventana = consultas_ventana / INTERVALO_REPORTE

                print(
                    f"\n{'─' * 100}\n"
                    f"[{elapsed:.0f}s] MÉTRICAS DEL SISTEMA\n"
                    f"{'─' * 100}\n"
                    f"  Procesamiento:  Hits={hits}  Misses={misses}  Total={total}\n"
                    f"  Hit Rate:       {hit_rate:.2%}\n"
                    f"  Latencia:       p50={p50*1000:.2f}ms  p95={p95*1000:.2f}ms\n"
                    f"  Throughput:     {throughput:.2f} q/s (promedio) | {throughput_ventana:.2f} q/s (ventana)\n"
                    f"  Reintentos:     Total={retries_total}  Retry Rate={retry_rate:.2%}\n"
                    f"  Recuperación:   Recovered={recovered_total}  Recovery Rate={recovery_rate:.2%}"
                    + (f"  Recovery Time={recovery_time_last:.2f}s" if recovery_time_last else "") +
                    f"\n  DLQ:            Total={dlq_total}  DLQ Rate={dlq_rate:.2%}\n"
                    f"  Backlog Redis:  {backlog}\n"
                    f"  Evictions:      {evictions}\n"
                    f"  Consumers:      {dict(consumer_counts)}\n"
                    f"{'─' * 100}"
                )

                # Reset ventana
                consultas_ventana = 0
            else:
                print(f"[{elapsed:.0f}s] Esperando eventos... (backlog Redis: {r.llen('metricas_cola')})")

    except Exception as e:
        print(f"[Metrics] Error: {e}")
        time.sleep(1)
