import asyncio
import logging

from typing import Union, NamedTuple
from .binance import BinanceWebsocket
from .kraken import KrakenWebsocket
from .events import StreamType, ExchangeType, OrderbookEvent, TradeEvent


class Subscription(NamedTuple):
    exch_name: str
    symbol: str
    stream_name: str


class WsManager:
    def __init__(self, subscriptions: list[Subscription]):
        self.subscriptions = subscriptions

        self._logger = logging.getLogger(__name__)
        self._loop = asyncio.get_event_loop()
        self._coros = []

        self._queue = asyncio.Queue()
        self._ws_connections = {}

        for sub in self.subscriptions:
            if sub not in self._ws_connections:
                self._ws_connections[sub] = self.__create_ws_connection(sub)

    async def __aenter__(self):
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.cleanup()

    def __create_ws_connection(self, subscription: Subscription):
        exchType = ExchangeType(subscription.exch_name)
        streamType = StreamType(subscription.stream_name)
        symbol = subscription.symbol

        if exchType == ExchangeType.BINANCE:
            ws = BinanceWebsocket(streamType, symbol)
        elif exchType == ExchangeType.KRAKEN:
            ws = KrakenWebsocket(streamType, symbol)
        else:
            raise ValueError(f"Exchange {exchType} not supported")

        return ws

    async def connect(self):
        for sub, ws in self._ws_connections.items():
            self._logger.info(f"Connecting to {sub}")
            self._coros.append(self._loop.create_task(ws._run(self._queue)))

    async def cleanup(self):
        for task in self._coros:
            task.cancel()
        await asyncio.wait(self._coros, timeout=10)

    async def recv(self) -> Union[OrderbookEvent, TradeEvent]:
        """receive an event from the websocket connections"""
        try:
            return await self._queue.get()
        except asyncio.CancelledError:
            self._logger.info("cancelled error")
            raise
        except BaseException as e:
            self._logger.exception("exception: ", e)
            raise
