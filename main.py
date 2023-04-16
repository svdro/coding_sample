import asyncio

import logging
import tracemalloc

from ws_apis.websocket import Websocket
from ws_apis.binance import BinanceWebsocket, BinanceAPI


tracemalloc.start()
logging.basicConfig(level=logging.INFO)


async def main():

    subscription_msg = BinanceAPI.prepare_book_subscription_msg(["etheur"], 0)

    async with BinanceWebsocket(BinanceAPI._ws_url, subscription_msg) as ws:
        # async with Websocket(BinanceWebsocket._ws_url, subscription_msg) as ws:
        for _ in range(10):
            msg = await ws.recv()
            print(msg)

    print("got it dude")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("KeyboardInterrupt")
