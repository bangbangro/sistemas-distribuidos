"""
Sistema de Métricas – Tarea 3
Consume eventos de métricas desde Kafka (metrics‑topic) y publica
resúmenes consolidados en Elasticsearch para visualización en Kibana.
"""

import os, json, time, warnings, socket
import numpy as np
import redis
from datetime import datetime
from kafka import KafkaConsumer
import requests   # HTTP client for Elasticsearch

warnings.filterwarnings("ignore", category=DeprecationWarning)

# ----------------------------------------------------------------------
# Configuración (variables de entorno)
# ----------------------------------------------------------------------
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
METRICS_TOPIC = os.getenv("METRICS_TOPIC", "metrics-topic")
ES_HOST = os.getenv("ELASTICSEARCH_HOST", "elasticsearch")
ES_PORT = os.getenv("ELASTICSEARCH_PORT", "9200")
ES_INDEX = "system-metrics"

# ----------------------------------------------------------------------
# Conexión Redis (legacy backlog)
# ----------------------------------------------------------------------
r = redis.Redis(host="redis", port=6379, decode_responses=True)

# ----------------------------------------------------------------------
# Esperar Kafka (misma lógica que antes)
# ----------------------------------------------------------------------
def wait_for_kafka(host="kafka", port=9092, timeout=120):
    print("[Metrics] Esperando Kafka…")
    start = time.time()
    while True:
        try:
            sock = socket.create_connection((host, port), timeout=3)
            sock.close()
            print("[Metrics] Kafka disponible – esperando inicialización…")
            time.sleep(5)
            return
        except (socket.error, ConnectionRefusedError):
            if time.time() - start > timeout:
                raise TimeoutError("Kafka no disponible tras 120s")
            time.sleep(3)

wait_for_kafka()

# ----------------------------------------------------------------------
# Consumidor Kafka
# ----------------------------------------------------------------------
while True:
    try:
        consumer = KafkaConsumer(
            METRICS_TOPIC,
            bootstrap_servers=KAFKA_BOOTSTRAP,
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
            group_id="metrics-monitor",
            auto_offset_reset="earliest",
            consumer_timeout_ms=500,
        )
        print("[Metrics] Consumer conectado a", METRICS_TOPIC)
        break
    except Exception as e:
        print("[Metrics] Kafka no listo aún:", e)
        time.sleep(5)

# ----------------------------------------------------------------------
# Variables de métricas
# ----------------------------------------------------------------------
hits = misses = retries_total = dlq_total = recovered_total = 0
total_success = 0
latencias = []
lat_hits = []
lat_misses = []

# Recovery‑time tracking
en_falla = False
falla_ts = None
recovery_time = None

# Consumer tracking
consumer_counts = {}

# Throughput ventana (5 s)
INTERVAL = 5
ventana_start = time.time()
ventana_q = 0
start_time = time.time()
last_report = 0

# ----------------------------------------------------------------------
# Helper: publicar en Elasticsearch
# ----------------------------------------------------------------------
def publish_es(doc):
    url = f"http://{ES_HOST}:{ES_PORT}/{ES_INDEX}/_doc"
    try:
        resp = requests.post(url, json=doc, timeout=5)
        if resp.status_code not in (200, 201):
            print("[Metrics][ES] Error", resp.status_code, resp.text)
    except Exception as exc:
        print("[Metrics][ES] Exception", exc)

# ----------------------------------------------------------------------
# Loop principal
# ----------------------------------------------------------------------
print("[Metrics] Esperando eventos…")
while True:
    try:
        msgs = consumer.poll(timeout_ms=1000)
        for _, batch in msgs.items():
            for msg in batch:
                ev = msg.value
                qtype = ev.get("query_type")
                cache_hit = ev.get("cache_hit", False)
                latency = ev.get("latency_ms", 0) / 1000.0
                status = ev.get("status", "success")
                consumer_id = ev.get("consumer_id", "unknown")
                retries = ev.get("retries", 0)
                recovered = ev.get("recovered", False)

                if status == "success":
                    total_success += 1
                    ventana_q += 1
                    latencias.append(latency)
                    if cache_hit:
                        hits += 1
                        lat_hits.append(latency)
                    else:
                        misses += 1
                        lat_misses.append(latency)
                    if recovered:
                        recovered_total += 1
                    consumer_counts[consumer_id] = consumer_counts.get(consumer_id, 0) + 1
                    if en_falla and falla_ts is not None:
                        recovery_time = time.time() - falla_ts
                        en_falla = False
                        print(f"\n✅ RECUPERACIÓN DETECTADA – Recovery Time: {recovery_time:.2f}s")
                elif status == "retry":
                    retries_total += 1
                    if not en_falla:
                        en_falla = True
                        falla_ts = time.time()
                        print(f"\n⚠️ FALLA DETECTADA – {datetime.now().strftime('%H:%M:%S')}")
                elif status == "dlq":
                    dlq_total += 1

        # ------------------------------------------------------------------
        # Reporte periódico
        # ------------------------------------------------------------------
        now = time.time()
        if now - last_report >= INTERVAL:
            last_report = now
            elapsed = now - start_time
            total = hits + misses
            if total > 0:
                p50 = np.percentile(latencias, 50) * 1000 if latencias else 0
                p95 = np.percentile(latencias, 95) * 1000 if latencias else 0
                throughput = total_success / elapsed if elapsed > 0 else 0
                ventana_thr = ventana_q / INTERVAL
                hit_rate = hits / total
                total_processed = total + retries_total + dlq_total
                retry_rate = retries_total / total_processed if total_processed > 0 else 0
                recovery_rate = recovered_total / retries_total if retries_total > 0 else 0
                dlq_rate = dlq_total / total_processed if total_processed > 0 else 0
                backlog = r.llen("metricas_cola")
                evicts = r.info("stats").get("evicted_keys", 0)

                print(
                    f"\n{'─'*100}\n"
                    f"[{int(elapsed)}s] MÉTRICAS DEL SISTEMA\n"
                    f"{'─'*100}\n"
                    f"  Procesamiento: Hits={hits} Misses={misses} Total={total}\n"
                    f"  Hit Rate:       {hit_rate:.2%}\n"
                    f"  Latencia:       p50={p50:.2f}ms  p95={p95:.2f}ms\n"
                    f"  Throughput:     {throughput:.2f} q/s (prom) | {ventana_thr:.2f} q/s (ventana)\n"
                    f"  Reintentos:     Total={retries_total}  Retry Rate={retry_rate:.2%}\n"
                    f"  Recuperación:   Recovered={recovered_total}  Recovery Rate={recovery_rate:.2%}" 
                    + (f"  Recovery Time={recovery_time:.2f}s" if recovery_time else "") + f"\n"
                    f"  DLQ:            Total={dlq_total}  DLQ Rate={dlq_rate:.2%}\n"
                    f"  Backlog Redis:  {backlog}\n"
                    f"  Evictions:      {evicts}\n"
                    f"  Consumers:      {consumer_counts}\n"
                    f"{'─'*100}"
                )

                doc = {
                    "@timestamp": datetime.utcnow().isoformat() + "Z",
                    "elapsed_seconds": int(elapsed),
                    "hits": hits,
                    "misses": misses,
                    "total": total,
                    "hit_rate": hit_rate,
                    "latency_p50_ms": p50,
                    "latency_p95_ms": p95,
                    "throughput_overall_qps": throughput,
                    "throughput_window_qps": ventana_thr,
                    "retries_total": retries_total,
                    "retry_rate": retry_rate,
                    "recovered_total": recovered_total,
                    "recovery_rate": recovery_rate,
                    "recovery_time_sec": recovery_time if recovery_time else None,
                    "dlq_total": dlq_total,
                    "dlq_rate": dlq_rate,
                    "backlog_redis": backlog,
                    "evictions_redis": evicts,
                    "consumer_counts": consumer_counts,
                }
                publish_es(doc)

                # Reset ventana
                ventana_q = 0
                consumer_counts = {}
            else:
                print(f"[{int(elapsed)}s] Esperando eventos… (backlog Redis: {r.llen('metricas_cola')})")
    except Exception as err:
        print("[Metrics] Error:", err)
        time.sleep(1)

