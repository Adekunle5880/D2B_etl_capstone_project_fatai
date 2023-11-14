import websocket
import os
from confluent_kafka import Producer

producer_conf = {
    "bootstrap.servers":"localhost:9092"
}
producer = Producer(producer_conf)

# kafka-topics --bootstrap-server broker:29092 --create --topic HelloWorld --if-not-exists --replication-factor 1 --partition 1

#https://pypi.org/project/websocket_client/

def on_message(ws, message):
    print(message)

def on_error(ws, error):
    print(error)

def on_close(ws):
    print("### closed ###")

def on_open(ws):
    ws.send('{"type":"subscribe","symbol":"AAPL"}')
    ws.send('{"type":"subscribe","symbol":"AMZN"}')
    ws.send('{"type":"subscribe","symbol":"BINANCE:BTCUSDT"}')
    ws.send('{"type":"subscribe","symbol":"IC MARKETS:1"}')

if __name__ == "__main__":
    websocket.enableTrace(True)
    ws = websocket.WebSocketApp("wss://ws.finnhub.io?token=ckteanpr01qvc3s76rk0ckteanpr01qvc3s76rkg",
                              on_message = on_message,
                              on_error = on_error,
                              on_close = on_close)
    ws.on_open = on_open
    ws.run_forever()


producer.produce(topic='Delivery', key=msg['Type'], value=str(msg), callback=acked)
producer.poll(1)

