import asyncio
import json
import logging
import time

from typing import Any, Union

from .events import WsEventType, StreamType
from .events import OrderbookEvent, OBEventType, Level
from .events import TradeEvent, Trade, TradeSide
from .utils import SymbolsMeta
from .websocket import Websocket


def parse_event_type_kraken(data: Union[list, dict]) -> WsEventType:
    """parse the event type from the kraken websocket message"""
    if isinstance(data, dict) and data.get("event") == "heartbeat":
        return WsEventType.HEARTBEAT
    if isinstance(data, dict):
        return WsEventType.OTHER
    if "book" in data[-2]:
        return WsEventType.BOOK
    if "trade" in data[-2]:
        return WsEventType.TRADE
    return WsEventType.OTHER


def parse_level_kraken(levels: list[str]) -> Level:
    """parse a level from the kraken websocket message"""
    return Level(price=float(levels[0]), qty=float(levels[1]))


def parse_book_msg_kraken(exch_name: str, symbol: str, data: list) -> OrderbookEvent:
    """parse a book message from the kraken websocket message and return an OrderbookEvent"""
    akey, bkey, event_type = "a", "b", OBEventType.UPDATE
    if data[1].get("as") or data[1].get("bs"):
        akey, bkey, event_type = "as", "bs", OBEventType.SNAPSHOT

    asks = [parse_level_kraken(a) for a in data[1].get(akey, [])]
    bids = [parse_level_kraken(b) for b in data[1].get(bkey, [])]

    ask_timestamps = [float(x[2]) for x in data[1].get(akey, [])]
    bid_timestamps = [float(x[2]) for x in data[1].get(bkey, [])]
    if not ask_timestamps and not bid_timestamps:
        raise ValueError("no timestamps found in orderbook levels")
    ts_exchange = int(max(ask_timestamps + bid_timestamps) * 1e9)

    return OrderbookEvent(
        exch_name=exch_name,
        symbol=symbol,
        type=event_type,
        bids=bids,
        asks=asks,
        ts_exchange=ts_exchange,
        ts_recorded=int(time.time() * 1e9),
    )


def parse_trade_kraken(trade: list[Any]) -> Trade:
    """parses a trade from the kraken websocket message"""
    return Trade(
        price=float(trade[0]),
        qty=float(trade[1]),
        side=TradeSide.SELL if trade[3] == "s" else TradeSide.BUY,
    )


def parse_trade_msg_kraken(exch_name: str, symbol: str, data: list) -> TradeEvent:
    """parses a trade message from the kraken websocket message and returns a TradeEvent"""
    trades = [parse_trade_kraken(trade) for trade in data[1]]
    ts_exchange = max([int(float(trade[2]) * 1e9) for trade in data[1]])

    return TradeEvent(
        exch_name=exch_name,
        symbol=symbol,
        trades=trades,
        ts_exchange=ts_exchange,
        ts_recorded=int(time.time() * 1e9),
    )


def prepare_book_subscription_msg_kraken(symbols: list[str]) -> str:
    msg = {
        "event": "subscribe",
        "pair": [symbol for symbol in symbols],
        "subscription": {"name": "book"},
    }
    return json.dumps(msg)


def prepare_trades_subscription_msg_kraken(symbols: list[str]) -> str:
    msg = {
        "event": "subscribe",
        "pair": [symbol for symbol in symbols],
        "subscription": {"name": "trade"},
    }
    return json.dumps(msg)


class KrakenWebsocket(Websocket):
    _name = "kraken"
    _symbolsMeta: SymbolsMeta = SymbolsMeta()
    _ws_url: str = "wss://ws.kraken.com"

    def __init__(self, streamType: StreamType, symbol: str):
        subscription_msg = self._prepare_subscription_msg(streamType, symbol)
        print(subscription_msg)
        super().__init__(self._ws_url, subscription_msg)

        self.symbol = symbol
        self.streamType = streamType

        self._logger = logging.getLogger(__name__)

    async def connect(self):
        """connects to the websocket and subscribes to the symbols"""
        await super().connect()

    async def _listen(self):
        """listens for messages from the websocket, parses them and puts them in the queue"""
        assert self._conn is not None
        msg = await self._conn.recv()
        data = json.loads(msg)

        event_type = parse_event_type_kraken(data)
        if event_type == WsEventType.BOOK:
            await self._queue.put(self._parse_book_msg(data))
        elif event_type == WsEventType.TRADE:
            await self._queue.put(self._parse_trade_msg(data))
        elif event_type == WsEventType.HEARTBEAT:
            pass
        else:
            self._logger.info(f"received other event {event_type}")

    async def recv(self) -> Union[OrderbookEvent, TradeEvent]:
        while True:
            try:
                return await asyncio.wait_for(self._queue.get(), timeout=self._timeout)
            except asyncio.TimeoutError:
                self._logger.info("timeout in recv")

    def _parse_book_msg(self, data: list) -> OrderbookEvent:
        symbol = self._symbolsMeta.ws_sym2sym(self._name, data[-1])
        return parse_book_msg_kraken(self._name, symbol, data)

    def _parse_trade_msg(self, data: list) -> TradeEvent:
        symbol = self._symbolsMeta.ws_sym2sym(self._name, data[-1])
        return parse_trade_msg_kraken(self._name, symbol, data)

    def _prepare_subscription_msg(self, streamType: StreamType, symbol: str) -> str:
        """returns the subscription message for the given symbol and streamType"""
        ws_symbol = self._symbolsMeta.sym2ws_sym(self._name, symbol)

        if streamType == StreamType.BOOK:
            return prepare_book_subscription_msg_kraken([ws_symbol])
        if streamType == StreamType.TRADES:
            return prepare_trades_subscription_msg_kraken([ws_symbol])
        raise ValueError(f"invalid streamType: {streamType}")
