import asyncio

import logging
import tracemalloc

from datetime import datetime

from ws_apis.ws_api import WsApi
from ws_apis.binance import BinanceWebsocket
from ws_apis.kraken import KrakenWebsocket
from ws_apis.events import OrderbookEvent, StreamType


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


async def main_finally():
    # subscription_msg = prepare_book_subscription_msg_binance(["etheur"], 0)
    streamType = StreamType.TRADES
    # streamType = StreamType.BOOK
    print("got streamType")
    # binance = BinanceWebsocket(streamType, "ethusdt")
    binance = KrakenWebsocket(streamType, "etheur")
    await binance.connect()

    try:
        for _ in range(20):
            event = await binance.recv()
            print(event)
    finally:
        await binance.cleanup()


async def main():
    symbols = ["etheur", "ethusdt", "dogeusdt", "btcusdt", "btcusdc"]
    # wsApi = WsApi("book", "binance", symbols)
    wsApi = WsApi("book", "kraken", symbols)
    print(wsApi.streamType)

    await wsApi.start()
    # asyncio.create_task(wsApi.start())
    while True:
        # for _ in range(40):
        event = await wsApi.recv()
        ts_exch = datetime.fromtimestamp(event.ts_exchange / 1e9)
        ts_rec = datetime.fromtimestamp(event.ts_recorded / 1e9)
        msg = f"exch: {event.exch_name:<10s}, symbol: {event.symbol:<10s}"
        msg += f", ts_exch: {ts_exch}"
        msg += f", ts_rec: {ts_rec}"
        print(msg)


if __name__ == "__main__":
    try:
        asyncio.run(main())
        # asyncio.run(main_finally())
        # asyncio.run(main_context_manager())
    except KeyboardInterrupt:
        print("KeyboardInterrupt")
