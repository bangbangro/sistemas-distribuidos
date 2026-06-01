from kafka import KafkaProducer
import random
import time
import json

producer = KafkaProducer(
    bootstrap_servers="kafka:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

zonas = ["Z1", "Z2", "Z3", "Z4", "Z5"]

def generar_consulta(modo="zipf"):
    if modo == "uniforme":
        zona = random.choice(zonas)
    else:
        zona = random.choices(
            zonas,
            weights=[70, 15, 7, 5, 3]
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
    
    producer.send("topic_consultas_principales", request)
    producer.flush()
    
    print(f"TRAFFIC: Enviado a Kafka -> {request['query']} en {request['zona']}")
    time.sleep(0.1)
