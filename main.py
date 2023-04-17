import asyncio

import logging
import tracemalloc

from ws_apis.websocket import Websocket
from ws_apis.binance import BinanceWebsocket, BinanceWsAPI, StreamType
from ws_apis.events import OrderbookEvent


tracemalloc.start()
logging.basicConfig(level=logging.INFO)


async def main_context_manager():

    async with BinanceWebsocket(StreamType.BOOK, "ethusdt") as ws:
        while True:
            event = await ws.recv()

            from datetime import datetime

            ts_exch = datetime.fromtimestamp(event.ts_exchange / 1e9)
            ts_rec = datetime.fromtimestamp(event.ts_recorded / 1e9)
            msg = f"exch: {event.exch_name}, symbol: {event.symbol}"
            msg += f", ts_exch: {ts_exch}"
            msg += f", ts_rec: {ts_rec}"
            if isinstance(event, OrderbookEvent):
                msg += f", type: {event.type}"
            print(msg)


async def main():
    # subscription_msg = prepare_book_subscription_msg_binance(["etheur"], 0)
    streamType = StreamType.BOOK
    # streamType = StreamType.BOOK
    print("got streamType")
    binance = BinanceWebsocket(streamType, "ethusdt")
    await binance.connect()

    try:
        for _ in range(20):
            event = await binance.recv()
            print(event)
    finally:
        await binance.cleanup()


if __name__ == "__main__":
    try:
        # asyncio.run(main())
        asyncio.run(main_context_manager())
    except KeyboardInterrupt:
        print("KeyboardInterrupt")
