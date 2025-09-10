from kafka import KafkaProducer
import json

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

pedido = {"pedido_id": 1, "cliente": {"id":"1","nome":"Bela"}, "valor": 59.9, "status": "criado"}

producer.send('pedidos', pedido)
producer.flush()
print("Pedido enviado!")