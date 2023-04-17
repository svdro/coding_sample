import asyncio
import logging

from typing import Union
from .binance import BinanceWebsocket
from .events import StreamType, ExchangeType, OrderbookEvent, TradeEvent


# TODO: finish this class
class WsApi:
    """WsAPI can manage multiple ws connections"""

    def __init__(self, stream_name: str, exch_name: str, symbols: list[str]):
        self.exchType = ExchangeType(exch_name)
        self.streamType = StreamType(stream_name)
        self.symbols = symbols

        self._logger = logging.getLogger(__name__)

        self._loop = asyncio.get_event_loop()
        self._coros = []

        self._queue = asyncio.Queue()
        self._ws_connections = []

        if self.exchType == ExchangeType.BINANCE:
            self._ws_connections.extend(
                [BinanceWebsocket(self.streamType, symbol) for symbol in self.symbols]
            )
        elif self.exchType == ExchangeType.KRAKEN:
            self._ws_connections.extend(
                [BinanceWebsocket(self.streamType, symbol) for symbol in self.symbols]
            )

    async def __aenter__(self):
        await self.start()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.cleanup()

    async def cleanup(self):
        for task in self._coros:
            task.cancel()
        await asyncio.wait(self._coros, timeout=10)

    async def start(self):
        """run the websocket connections"""
        self._coros.extend(
            [self._loop.create_task(ws.run(self._queue)) for ws in self._ws_connections]
        )

    async def recv(self) -> Union[OrderbookEvent, TradeEvent]:
        """receive an event from the websocket connections"""
        try:
            return await self._queue.get()
        except asyncio.CancelledError:
            self._logger.info("cancelled error")
            raise
        except BaseException as e:
            self._logger.exception("exception")
            raise
