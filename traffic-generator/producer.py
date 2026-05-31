from kafka import KafkaProducer
import json
import time

producer = KafkaProducer(
    bootstrap_servers="kafka:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

while True:
    mensaje = {
        "query": "Q1",
        "zona": "Z1"
    }

    producer.send("consultas", mensaje)
    producer.flush()

    print("Enviado:", mensaje)

    time.sleep(2)
