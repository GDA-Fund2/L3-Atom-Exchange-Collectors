import websockets
import asyncio
import time
import json

from normalise.bybit_normalisation import normalise
from helpers.read_config import get_symbols
from sink_connector.kafka_producer import KafkaProducer
from sink_connector.ws_to_kafka import produce_messages
from source_connector.websocket_connector import connect


url = 'wss://stream.bybit.com/realtime'

async def main():
    raw_producer = KafkaProducer("bybit-raw")
    normalised_producer = KafkaProducer("bybit-normalised")
    trades_producer = KafkaProducer("bybit-trades")
    symbols = get_symbols('bybit')
    await connect(url, handle_bybit, raw_producer, normalised_producer, trades_producer, symbols)

async def handle_bybit(ws, raw_producer, normalised_producer, trades_producer, symbols):
    for symbol in symbols:
        subscribe_message = {
                "op": "subscribe",
                "args": ["orderBook_200.100ms." + symbol, "trade." + symbol]
            }
        await ws.send(json.dumps(subscribe_message).encode('utf-8'))
    
    await produce_messages(ws, raw_producer, normalised_producer, trades_producer, normalise)

if __name__ == "__main__":
    asyncio.run(main())