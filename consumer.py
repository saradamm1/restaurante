from kafka import KafkaConsumer
import json
import mysql.connector
from pymongo import MongoClient

# === Configurações MySQL ===
mysql_conn = mysql.connector.connect(
    host="localhost",
    user="root",
    password="190600sara",
    database="restaurante"
)
mysql_cursor = mysql_conn.cursor()

mongo_client = MongoClient("mongodb://localhost:27017/")
mongo_db = mongo_client["restaurante"]
mongo_collection = mongo_db["pedidos"]

consumer = KafkaConsumer(
    'pedidos',
    bootstrap_servers='localhost:9092',
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

print("Esperando pedidos...")
for msg in consumer:
    pedido = msg.value
    print("Novo pedido recebido:", pedido)

    # --- Inserir cliente se não existir (só id) ---
    cliente_id = pedido["cliente"]["id"]
    mysql_cursor.execute(
        "INSERT INTO pedido (id_cliente) VALUES (%s)",
        (cliente_id,)
    )
    mysql_conn.commit()

    # --- Salvar pedido completo no MongoDB ---
    mongo_collection.insert_one(pedido)

    print("Pedido salvo no MySQL e MongoDB!")