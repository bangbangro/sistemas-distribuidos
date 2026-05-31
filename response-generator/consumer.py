from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    "consultas",
    bootstrap_servers="kafka:9092",
    auto_offset_reset="earliest",
    group_id="grupo-prueba",
    value_deserializer=lambda m: json.loads(m.decode("utf-8"))
)

print("Esperando mensajes...")

for msg in consumer:
    print("Recibido:", msg.value)
