import asyncio
import json
import logging
import time

from typing import Any, Union
from urllib.parse import urljoin

from .events import WsEventType, StreamType
from .events import OrderbookEvent, OBEventType, Level
from .events import TradeEvent, Trade, TradeSide
from .utils import get_request, SymbolsMeta
from .websocket import Websocket


async def get_ob_snapshot_binance(symbol: str) -> dict[str, Any]:
    """gets the order book snapshot from the rest api"""
    rest_url = "https://api.binance.com/"
    url = urljoin(rest_url, "api/v3/depth")
    params = {"symbol": symbol, "limit": 10}
    return await get_request(url, params)


def parse_event_type_binance(data: dict[str, Any]) -> WsEventType:
    """parses the event type from the binance websocket message"""
    if data.get("e") == "depthUpdate":
        return WsEventType.BOOK
    if data.get("e") == "aggTrade":
        return WsEventType.TRADE
    return WsEventType.OTHER


def parse_level_binance(data: list[str]) -> Level:
    """parses a level from the binance websocket message"""
    price = float(data[0])
    qty = float(data[1])
    return Level(price, qty)


def parse_book_msg_binance(
    exch_name: str, symbol: str, data: dict[str, Any]
) -> OrderbookEvent:
    """parses a binance websocket message and returns an OrderbookEvent"""
    ts_exchange = int(data["E"] * 1e6)
    bids = [parse_level_binance(b) for b in data["b"]]
    asks = [parse_level_binance(a) for a in data["a"]]
    update_ids = {"first_update_id": data["U"], "last_update_id": data["u"]}
    return OrderbookEvent(
        exch_name=exch_name,
        symbol=symbol,
        type=OBEventType.UPDATE,
        bids=bids,
        asks=asks,
        ts_exchange=ts_exchange,
        ts_recorded=int(time.time() * 1e9),
        other=update_ids,
    )


def parse_trade_msg_binance(
    exch_name: str, symbol: str, data: dict[str, Any]
) -> TradeEvent:
    """parses a binance websocket message and returns a TradeEvent"""
    ts_exchange = int(data["T"] * 1e6)
    side = TradeSide.SELL if data["m"] else TradeSide.BUY
    trade = Trade(price=float(data["p"]), qty=float(data["q"]), side=side)
    return TradeEvent(
        exch_name=exch_name,
        symbol=symbol,
        ts_exchange=ts_exchange,
        ts_recorded=int(time.time() * 1e9),
        trades=[trade],
    )


def parse_snapshot_binance(
    exch_name: str, symbol: str, ts_exchange: int, data: dict[str, Any]
) -> OrderbookEvent:
    """parses a binance rest api response and returns an OrderbookEvent"""
    bids = [parse_level_binance(b) for b in data["bids"]]
    asks = [parse_level_binance(a) for a in data["asks"]]
    update_ids = {"last_update_id": data["lastUpdateId"]}
    return OrderbookEvent(
        exch_name=exch_name,
        symbol=symbol,
        type=OBEventType.SNAPSHOT,
        bids=bids,
        asks=asks,
        ts_exchange=ts_exchange,
        ts_recorded=int(time.time() * 1e9),
        other=update_ids,
    )


def _prepare_subscription_msg_binance(method: str, params: list[str], r_id: int) -> str:
    return json.dumps({"method": method, "params": params, "id": r_id})


def prepare_trades_subscription_msg_binance(symbols: list[str], r_id: int) -> str:
    params = [f"{symbol}@aggTrade" for symbol in symbols]
    return _prepare_subscription_msg_binance("SUBSCRIBE", params, r_id)


def prepare_book_subscription_msg_binance(symbols: list[str], r_id: int) -> str:
    params = [f"{symbol}@depth@100ms" for symbol in symbols]
    return _prepare_subscription_msg_binance("SUBSCRIBE", params, r_id)


class BinanceWebsocket(Websocket):
    """
    Binance Websocket returns WsEvents rather than raw messages
    It also buffers orderbook events and synchronizes them with snapshots
    """

    _name = "binance"
    _symbolsMeta: SymbolsMeta = SymbolsMeta()
    _rest_url = "https://api.binance.com/"
    _ws_url = "wss://stream.binance.com:9443/ws"

    def __init__(self, streamType: StreamType, symbol: str):
        subscription_msg = self._prepare_subscription_msg(streamType, symbol)
        super().__init__(self._ws_url, subscription_msg)

        self.symbol = symbol
        self.streamType = streamType

        # binance orderbook streams require special treatment.
        if self.streamType == StreamType.BOOK:
            self._buffer_queue = asyncio.Queue()
            self.eventsBuffer = BinanceEventsBuffer(self._buffer_queue)

        self._logger = logging.getLogger(__name__)

    async def connect(self):
        """connects to the websocket and subscribes to the symbols"""
        await super().connect()
        if self.streamType == StreamType.BOOK:
            task = self._loop.create_task(self._queue_snapshot_rest(self.symbol))
            self._coros.append(task)

    async def _listen(self):
        assert self._conn is not None
        msg = await self._conn.recv()

        data = json.loads(msg)
        event_type = parse_event_type_binance(data)
        if event_type == WsEventType.BOOK:
            await self._queue.put(self._parse_book_msg(data))
        elif event_type == WsEventType.TRADE:
            await self._queue.put(self._parse_trade_msg(data))
        else:
            self._logger.info(f"received other event {event_type}")

    async def recv(self) -> Union[OrderbookEvent, TradeEvent]:
        """returns an OrderbookEvent or a TradeEvent"""
        # always flush buffer queue when possible
        if self.streamType == StreamType.BOOK and not self._buffer_queue.empty():
            return await self._buffer_queue.get()

        # take events off the queue until one can get returned
        while True:
            try:
                event = await asyncio.wait_for(self._queue.get(), timeout=self._timeout)
            except asyncio.TimeoutError:
                self._logger.info("timeout in recv")
                continue

            if not isinstance(event, OrderbookEvent):
                return event

            if event.type == OBEventType.SNAPSHOT:
                await self.eventsBuffer.flush_to_buffer_queue(event)
                return event

            if self.eventsBuffer.is_event_valid(event):
                return event
            else:
                self.eventsBuffer.buffer_event(event)
                self._logger.info("buffered event")

    def _parse_book_msg(self, data: dict[str, Any]) -> OrderbookEvent:
        symbol = self._symbolsMeta.rest_sym2sym(self._name, data["s"])
        return parse_book_msg_binance(self._name, symbol, data)

    def _parse_trade_msg(self, data: dict[str, Any]) -> TradeEvent:
        symbol = self._symbolsMeta.rest_sym2sym(self._name, data["s"])
        return parse_trade_msg_binance(self._name, symbol, data)

    async def _get_snapshot_rest(self, symbol: str) -> OrderbookEvent:
        """gets the order book snapshot from the rest api and returns an OrderbookEvent"""
        t = time.time()
        rest_symbol = self._symbolsMeta.sym2rest_sym(self._name, symbol)
        data = await get_ob_snapshot_binance(rest_symbol)
        ts_exchange = int((time.time() + t) / 2 * 1e9)  # fake ts_exchange
        return parse_snapshot_binance(self._name, symbol, ts_exchange, data)

    async def _queue_snapshot_rest(self, symbol: str):
        """gets the order book snapshot from the rest api and queues it"""
        await asyncio.sleep(1)  # wait for the connection to be established
        snapshot = await self._get_snapshot_rest(symbol)
        await self._queue.put(snapshot)

    def _prepare_subscription_msg(self, streamType: StreamType, symbol: str) -> str:
        """returns the subscription message for the given symbol and streamType"""
        ws_symbol = self._symbolsMeta.sym2ws_sym(self._name, symbol)
        if streamType == StreamType.TRADES:
            return prepare_trades_subscription_msg_binance([ws_symbol], 0)
        if streamType == StreamType.BOOK:
            return prepare_book_subscription_msg_binance([ws_symbol], 0)
        raise ValueError(f"streamType {streamType} not supported")


class BinanceEventsBuffer:
    """for handling events order in Binance"""

    def __init__(self, buffer_queue: asyncio.Queue, max_buffer_size: int = 100):
        self.events_buffer: list[OrderbookEvent] = []
        self.max_buffer_size = max_buffer_size
        self.last_update_id = 0
        self.handled_first_event = False
        self.buffer_queue = buffer_queue

    def buffer_event(self, event: OrderbookEvent):
        if len(self.events_buffer) >= self.max_buffer_size:
            raise ValueError("buffer size exceeded")

        self.events_buffer.append(event)

    def _is_first_event_valid(self, fid: int, lid: int) -> bool:
        """the first event following a snapshot is a special case"""
        if fid <= self.last_update_id + 1 and lid >= self.last_update_id + 1:
            self.handled_first_event = True
            return True
        return False

    def _is_event_valid(self, fid: int, lid: int) -> bool:
        """returns true the first and last update ids are consistent with the last event"""
        if not self.handled_first_event:
            return self._is_first_event_valid(fid, lid)

        if fid == self.last_update_id + 1:
            return True

        return False

    def is_event_valid(self, event: OrderbookEvent) -> bool:
        """returns true if the event logically follows the last event"""
        fid, lid = self.parse_update_ids(event)
        if self._is_event_valid(fid, lid):
            self.last_update_id = lid
            return True
        return False

    async def flush_to_buffer_queue(self, snapEvent: OrderbookEvent):
        assert isinstance(snapEvent.other, dict)
        self.last_update_id = snapEvent.other["last_update_id"]

        while len(self.events_buffer) > 0:
            event = self.events_buffer.pop(0)
            fid, lid = self.parse_update_ids(event)

            # queue the next event if it is valid
            if self._is_event_valid(fid, lid):
                await self.buffer_queue.put(event)
                self.last_update_id = lid

    @staticmethod
    def parse_update_ids(event: OrderbookEvent) -> tuple[int, int]:
        assert isinstance(event.other, dict)
        lid = event.other["last_update_id"]
        fid = event.other["first_update_id"]
        return fid, lid
