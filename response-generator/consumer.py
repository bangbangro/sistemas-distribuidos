import socket
import uuid
import time
import json
import random

import redis
import numpy as np
import pandas as pd
from kafka import KafkaConsumer, KafkaProducer
from datetime import datetime

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
MAX_REINTENTOS = 3
CONSUMER_ID = socket.gethostname()

zonas_bbox = {
    "Z1": (-33.445, -33.420, -70.640, -70.600),
    "Z2": (-33.420, -33.390, -70.600, -70.550),
    "Z3": (-33.530, -33.490, -70.790, -70.740),
    "Z4": (-33.460, -33.430, -70.670, -70.630),
    "Z5": (-33.470, -33.430, -70.810, -70.760),
}

zonas_area_km2 = {"Z1": 1.5, "Z2": 2.0, "Z3": 3.2, "Z4": 1.2, "Z5": 2.8}

# ---------------------------------------------------------------------------
# Dataset loading
# ---------------------------------------------------------------------------
print(f"[{CONSUMER_ID}] Cargando dataset...")
df = pd.read_csv(
    "/data/967_buildings.csv.gz",
    usecols=["latitude", "longitude", "confidence", "area_in_meters"],
    dtype={
        "latitude": "float32",
        "longitude": "float32",
        "confidence": "float32",
        "area_in_meters": "float32",
    },
)
print(f"[{CONSUMER_ID}] Dataset cargado: {len(df)} registros")

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def filtrar_zona(zona_id, conf_min):
    """Filter the dataframe by zone bounding box and minimum confidence."""
    lat_min, lat_max, lon_min, lon_max = zonas_bbox[zona_id]
    mask = (
        (df["latitude"] >= lat_min)
        & (df["latitude"] <= lat_max)
        & (df["longitude"] >= lon_min)
        & (df["longitude"] <= lon_max)
        & (df["confidence"] >= conf_min)
    )
    return df[mask]


def build_key(request):
    """Build the Redis cache key for a given request."""
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


def procesar_consulta(request):
    """Execute the query described by *request* and return the result."""
    zona = request["zona"]
    conf_min = request.get("confidence", 0.0)
    query = request["query"]
    data = filtrar_zona(zona, conf_min)

    if query == "Q1":
        return len(data)

    elif query == "Q2":
        avg_area = float(data["area_in_meters"].mean()) if len(data) > 0 else 0.0
        total_area = float(data["area_in_meters"].sum())
        return {"avg_area": avg_area, "total_area": total_area, "n": len(data)}

    elif query == "Q3":
        return len(data) / zonas_area_km2[zona]

    elif query == "Q4":
        zona_b = request["zona_b"]
        data_b = filtrar_zona(zona_b, conf_min)
        density_a = len(data) / zonas_area_km2[zona]
        density_b = len(data_b) / zonas_area_km2[zona_b]
        return {
            "zona_a": zona,
            "density_a": density_a,
            "zona_b": zona_b,
            "density_b": density_b,
            "diff": density_a - density_b,
        }

    elif query == "Q5":
        counts, bin_edges = np.histogram(
            data["confidence"].dropna(), bins=5, range=(0, 1)
        )
        return {
            "bins": [f"{bin_edges[i]:.2f}-{bin_edges[i+1]:.2f}" for i in range(5)],
            "counts": counts.tolist(),
        }

    else:
        raise ValueError(f"Query desconocida: {query}")


# ---------------------------------------------------------------------------
# Kafka connection with retry loop
# ---------------------------------------------------------------------------

def wait_for_kafka(bootstrap_servers="kafka:9092", max_retries=30, wait_seconds=2):
    """Block until Kafka is reachable, then return (consumer, producer)."""
    for attempt in range(1, max_retries + 1):
        try:
            print(f"[{CONSUMER_ID}] Intentando conectar a Kafka ({attempt}/{max_retries})...")
            producer = KafkaProducer(
                bootstrap_servers=bootstrap_servers,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            )
            consumer = KafkaConsumer(
                "consultas",
                "consultas_retry",
                bootstrap_servers=bootstrap_servers,
                group_id="consumidores",
                value_deserializer=lambda m: json.loads(m.decode("utf-8")),
                auto_offset_reset="earliest",
                enable_auto_commit=True,
            )
            print(f"[{CONSUMER_ID}] Conectado a Kafka exitosamente.")
            return consumer, producer
        except Exception as e:
            print(f"[{CONSUMER_ID}] Kafka no disponible: {e}")
            if attempt < max_retries:
                time.sleep(wait_seconds)
            else:
                raise RuntimeError("No se pudo conectar a Kafka después de múltiples intentos")


# ---------------------------------------------------------------------------
# Redis connection
# ---------------------------------------------------------------------------
r = redis.Redis(host="redis", port=6379, decode_responses=True)
print(f"[{CONSUMER_ID}] Conectado a Redis")

# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------

def main():
    print(f"[{CONSUMER_ID}] Consumer ID: {CONSUMER_ID}")
    consumer, producer = wait_for_kafka()

    for message in consumer:
        start = time.time()
        request = message.value

        # -- Ensure default fields ------------------------------------------
        request.setdefault("reintentos", 0)
        request.setdefault("timestamp", time.time())
        request.setdefault("query_id", str(uuid.uuid4()))

        query_id = request["query_id"]
        query_type = request["query"]
        zona = request["zona"]
        reintentos = request["reintentos"]

        print(
            f"[{CONSUMER_ID}] Procesando query_id={query_id} "
            f"tipo={query_type} zona={zona} reintentos={reintentos}"
        )

        key = build_key(request)

        # -- 1. Check Redis cache -------------------------------------------
        cached = r.get(key)
        if cached is not None:
            latency = round((time.time() - start) * 1000, 2)
            print(f"[{CONSUMER_ID}] CACHE HIT para {key} ({latency} ms)")

            # Metric event → Kafka
            metric_event = {
                "timestamp": datetime.utcnow().isoformat() + "Z",
                "query_id": request.get("query_id", str(uuid.uuid4())),
                "query_type": query_type,
                "zone": zona,
                "cache_hit": True,
                "latency_ms": latency,
                "retries": reintentos,
                "status": "success",
                "consumer_id": CONSUMER_ID,
                "recovered": True if reintentos > 0 else False,
            }
            producer.send("metrics-topic", metric_event)

            # Backward-compatible metric → Redis
            r.lpush("metricas_cola", f"HIT,{latency}")
            continue

        # -- 2. Process query -----------------------------------------------
        try:
            # Simulated failure: generador_activo flag
            if r.get("generador_activo") == "0":
                raise Exception("Fallo simulado: generador_activo == 0")

            # 20 % random failure rate
            if random.random() < 0.2:
                raise Exception("Fallo aleatorio (20%)")

            result = procesar_consulta(request)

            # Store result in Redis with 20 KB padding
            payload = {"result": result, "padding": "X" * 20000}
            r.set(key, json.dumps(payload), ex=300)

            latency = round((time.time() - start) * 1000, 2)
            print(
                f"[{CONSUMER_ID}] SUCCESS query_id={query_id} "
                f"key={key} latency={latency} ms"
            )

            # Metric event → Kafka
            metric_event = {
                "timestamp": datetime.utcnow().isoformat() + "Z",
                "query_id": request.get("query_id", str(uuid.uuid4())),
                "query_type": query_type,
                "zone": zona,
                "cache_hit": False,
                "latency_ms": latency,
                "retries": reintentos,
                "status": "success",
                "consumer_id": CONSUMER_ID,
                "recovered": True if reintentos > 0 else False,
            }
            producer.send("metrics-topic", metric_event)

            # Backward-compatible metric → Redis
            r.lpush("metricas_cola", f"MISS,{latency}")

        except Exception as e:
            latency = round((time.time() - start) * 1000, 2)
            reintentos = request.get("reintentos", 0)

            if reintentos < MAX_REINTENTOS:
                # -- Retry --------------------------------------------------
                request["reintentos"] = reintentos + 1
                producer.send("consultas_retry", request)
                status = "retry"
                print(
                    f"[{CONSUMER_ID}] RETRY query_id={query_id} "
                    f"intento={request['reintentos']}/{MAX_REINTENTOS} error={e}"
                )

                # Backward-compatible metric → Redis
                r.lpush("metricas_cola", f"RETRY,{request['reintentos']}")
            else:
                # -- DLQ ----------------------------------------------------
                producer.send("consultas_dlq", request)
                status = "dlq"
                print(
                    f"[{CONSUMER_ID}] DLQ query_id={query_id} "
                    f"reintentos={reintentos} error={e}"
                )

                # Backward-compatible metric → Redis
                r.lpush("metricas_cola", f"DLQ,{reintentos}")

            # Metric event → Kafka
            metric_event = {
                "timestamp": datetime.utcnow().isoformat() + "Z",
                "query_id": request.get("query_id", str(uuid.uuid4())),
                "query_type": query_type,
                "zone": zona,
                "cache_hit": False,
                "latency_ms": latency,
                "retries": request.get("reintentos", reintentos),
                "status": status,
                "consumer_id": CONSUMER_ID,
                "recovered": False,
            }
            producer.send("metrics-topic", metric_event)


if __name__ == "__main__":
    main()
