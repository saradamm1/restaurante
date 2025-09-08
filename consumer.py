from kafka import KafkaConsumer
import json

consumer = KafkaConsumer(
    'pedidos',
    bootstrap_servers='localhost:9092',
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)
print("esperando pedidos")
for msg in consumer:
    print("Novo pedido recebido:", msg.value)