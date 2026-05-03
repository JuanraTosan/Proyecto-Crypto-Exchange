import websocket
import json
from kafka import KafkaProducer
import time

# Cambio para conectar desde Windows a Docker
PRODUCER = KafkaProducer(
    bootstrap_servers=['localhost:9094'], # Usamos el puerto EXTERNAL
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

STREAMS = "btcusdt@miniTicker/ethusdt@miniTicker/btcusdt@kline_1m/ethusdt@kline_1m"
SOCKET = f"wss://stream.binance.com:9443/ws/{STREAMS}"

def on_message(ws, message):
    data = json.loads(message)
    topic = "crypto_raw"
    PRODUCER.send(topic, data)
    print(f"✅ Enviado a Kafka: {data.get('s')} - {data.get('c')}")

def on_error(ws, error):
    print(f"❌ Error: {error}")

if __name__ == "__main__":
    print("🚀 Iniciando Productor en modo local...")
    ws = websocket.WebSocketApp(SOCKET, on_message=on_message, on_error=on_error)
    ws.run_forever()