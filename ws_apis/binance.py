import asyncio
import json
from .utils import SymbolsMeta
from .websocket import Websocket
from .events import WsEventType
from typing import Any


class BinanceWebsocket(Websocket):
    """
    Binance Websocket returns WsEvents rather than raw messages
    It also buffers orderbook events
    """

    _symbolsMeta: SymbolsMeta = SymbolsMeta()

    def __init__(self, ws_url, subscription_msg: str):
        super().__init__(ws_url, subscription_msg)
        self._id = 0
        pass

    async def recv(self):
        # return await super().recv()
        msg = await asyncio.wait_for(self._queue.get(), timeout=self._timeout)
        data = json.loads(msg)
        if "e" in data:
            return data.get("e")

    @staticmethod
    def parse_event_type(data: dict[str, Any]) -> WsEventType:
        if data.get("e") == "depthUpdate":
            return WsEventType.BOOK
        if data.get("e") == "aggTrade":
            return WsEventType.TRADE
        return WsEventType.OTHER


class BinanceAPI:
    _ws_url = "wss://stream.binance.com:9443/ws"
    _rest_url = "https://api.binance.com/"

    def __init__(self):
        pass

    @staticmethod
    def prepare_trades_subscription_msg(symbols: list[str], r_id: int) -> str:
        msg = {
            "method": "SUBSCRIBE",
            "params": [f"{symbol}@aggTrade" for symbol in symbols],
            "id": r_id,
        }
        return json.dumps(msg)

    @staticmethod
    def prepare_book_subscription_msg(symbols: list[str], r_id: int) -> str:
        msg = {
            "method": "SUBSCRIBE",
            "params": [f"{symbol}@depth@100ms" for symbol in symbols],
            "id": r_id,
        }
        return json.dumps(msg)
