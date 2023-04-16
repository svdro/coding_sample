import abc
import aiohttp
import json
import time

from enum import Enum
from urllib.parse import urljoin
from utils import SymbolsMeta
from events import OrderbookEvent, OBEventType, Level, WsEventType
from events import TradeEvent, Trade, TradeSide
from typing import Any, Union


async def get_request(url: str, params: dict[str, Any]) -> dict[str, Any]:
    """makes a get request to the given url and params. return JSON"""
    async with aiohttp.ClientSession() as session:
        async with session.get(url, params=params) as resp:
            resp.raise_for_status()
            return await resp.json()


class Exchange(abc.ABC):
    """
    Exchange is an abstract class that provides a collection of static
    methods for interacting with Crypto Exchange public APIs.
    """

    _name: str
    _ws_url: str
    _symbolsMeta: SymbolsMeta

    @staticmethod
    @abc.abstractmethod
    def prepare_book_subscription_msg(symbols: list[str]) -> str:
        """prepares an order book subscription message"""
        raise NotImplementedError

    @staticmethod
    @abc.abstractmethod
    def prepare_trades_subscription_msg(symbols: list[str]) -> str:
        """prepares an trades subscription message"""
        raise NotImplementedError

    @staticmethod
    @abc.abstractmethod
    def parse_event_type(data: Union[dict, list]) -> WsEventType:
        """parses the event type from the incoming ws message"""
        raise NotImplementedError

    @staticmethod
    @abc.abstractmethod
    def parse_book_msg(data: Union[dict, list]) -> OrderbookEvent:
        """parses the order book event from the incoming ws message"""
        raise NotImplementedError

    @staticmethod
    @abc.abstractmethod
    def parse_trade_msg(data: Union[dict, list]) -> TradeEvent:
        """parses the trade event from the incoming ws message"""
        raise NotImplementedError


class Binance(Exchange):
    _id = 0
    _name = "binance"
    _ws_url = "wss://stream.binance.com:9443/ws"
    _rest_url = "https://api.binance.com/"
    _symbolsMeta: SymbolsMeta = SymbolsMeta()

    @staticmethod
    def prepare_book_subscription_msg(symbols: list[str]) -> str:
        Binance._id += 1
        msg = {
            "method": "SUBSCRIBE",
            "params": [f"{symbol}@depth@100ms" for symbol in symbols],
            "id": Binance._id,
        }
        return json.dumps(msg)

    @staticmethod
    def prepare_trades_subscription_msg(symbols: list[str]) -> str:
        Binance._id += 1
        msg = {
            "method": "SUBSCRIBE",
            "params": [f"{symbol}@aggTrade" for symbol in symbols],
            "id": Binance._id,
        }
        return json.dumps(msg)

    @staticmethod
    def parse_event_type(data: dict[str, Any]) -> WsEventType:
        if data.get("e") == "depthUpdate":
            return WsEventType.BOOK
        if data.get("e") == "aggTrade":
            return WsEventType.TRADE
        return WsEventType.OTHER

    @staticmethod
    def parse_level(data: list[str]) -> Level:
        price = float(data[0])
        qty = float(data[1])
        return Level(price, qty)

    @staticmethod
    def parse_book_msg(data: dict[str, Any]) -> OrderbookEvent:
        symbol = Binance._symbolsMeta.rest_sym2sym(Binance._name, data["s"])
        ts_exchange = int(data["E"] * 1e6)
        bids = [Binance.parse_level(b) for b in data["b"]]
        asks = [Binance.parse_level(a) for a in data["a"]]
        update_ids = {"first_update_id": data["U"], "last_update_id": data["u"]}
        return OrderbookEvent(
            exch_name=Binance._name,
            symbol=symbol,
            type=OBEventType.UPDATE,
            bids=bids,
            asks=asks,
            ts_exchange=ts_exchange,
            ts_recorded=int(time.time() * 1e9),
            other=update_ids,
        )

    @staticmethod
    def parse_trade_msg(data: dict[str, Any]) -> TradeEvent:
        symbol = Binance._symbolsMeta.rest_sym2sym(Binance._name, data["s"])
        ts_exchange = int(data["T"] * 1e6)
        side = TradeSide.SELL if data["m"] else TradeSide.BUY
        trade = Trade(price=float(data["p"]), qty=float(data["q"]), side=side)
        return TradeEvent(
            exch_name=Binance._name,
            symbol=symbol,
            ts_exchange=ts_exchange,
            ts_recorded=int(time.time() * 1e9),
            trades=[trade],
        )

    @staticmethod
    async def get_snapshot(symbol: str) -> dict[str, Any]:
        """gets the order book snapshot from the rest api"""
        url = urljoin(Binance._rest_url, "api/v3/depth")
        params = {"symbol": symbol, "limit": 10}
        return await get_request(url, params)

    @staticmethod
    async def get_orderbook_rest(symbol: str) -> OrderbookEvent:
        """gets the order book snapshot from the rest api and returns an OrderbookEvent"""
        t = time.time()
        rest_symbol = Binance._symbolsMeta.sym2rest_sym(Binance._name, symbol)
        data = await Binance.get_snapshot(rest_symbol)
        bids = [Binance.parse_level(b) for b in data["bids"]]
        asks = [Binance.parse_level(a) for a in data["asks"]]
        update_ids = {"last_update_id": data["lastUpdateId"]}
        return OrderbookEvent(
            exch_name=Binance._name,
            symbol=symbol,
            type=OBEventType.SNAPSHOT,
            bids=bids,
            asks=asks,
            ts_exchange=int((time.time() + t) / 2 * 1e9),  # fake ts_exchange
            ts_recorded=int(time.time() * 1e9),
            other=update_ids,
        )


class Kraken(Exchange):
    _name: str = "kraken"
    _ws_url: str = "wss://ws.kraken.com"
    _symbolsMeta: SymbolsMeta = SymbolsMeta()

    @staticmethod
    def prepare_book_subscription_msg(symbols: list[str]) -> str:
        msg = {
            "event": "subscribe",
            "pair": [Kraken._symbolsMeta.sym2ws_sym(Kraken._name, s) for s in symbols],
            "subscription": {"name": "book"},
        }
        return json.dumps(msg)

    @staticmethod
    def prepare_trades_subscription_msg(symbols: list[str]) -> str:
        msg = {
            "event": "subscribe",
            "pair": [Kraken._symbolsMeta.sym2ws_sym(Kraken._name, s) for s in symbols],
            "subscription": {"name": "trade"},
        }
        return json.dumps(msg)

    @staticmethod
    def parse_event_type(data: Union[list, dict]) -> Enum:
        if isinstance(data, dict):
            return WsEventType.OTHER
        if "book" in data[-2]:
            return WsEventType.BOOK
        if "trade" in data[-2]:
            return WsEventType.TRADE
        return WsEventType.OTHER

    @staticmethod
    def parse_level(levels: list[str]) -> Level:
        return Level(price=float(levels[0]), qty=float(levels[1]))

    @staticmethod
    def parse_book_msg(data: list) -> OrderbookEvent:
        symbol = Kraken._symbolsMeta.ws_sym2sym(Kraken._name, data[-1])

        # figure out whether the event is a snapshot or an update
        akey, bkey, event_type = "a", "b", OBEventType.UPDATE
        if data[1].get("as") or data[1].get("bs"):
            akey, bkey, event_type = "as", "bs", OBEventType.SNAPSHOT

        # parse bids and asks levels
        asks = [Kraken.parse_level(a) for a in data[1].get(akey, [])]
        bids = [Kraken.parse_level(b) for b in data[1].get(bkey, [])]

        # find the most recent timestamp in orderbook levels
        ask_timestamps = [float(x[2]) for x in data[1].get(akey, [])]
        bid_timestamps = [float(x[2]) for x in data[1].get(bkey, [])]
        if not ask_timestamps and not bid_timestamps:
            raise ValueError("no timestamps found in orderbook levels")
        ts_exchange = int(max(ask_timestamps + bid_timestamps) * 1e9)

        return OrderbookEvent(
            exch_name=Kraken._name,
            symbol=symbol,
            type=event_type,
            bids=bids,
            asks=asks,
            ts_exchange=ts_exchange,
            ts_recorded=int(time.time() * 1e9),
        )

    @staticmethod
    def parse_trade(trade: list[Any]) -> Trade:
        return Trade(
            price=float(trade[0]),
            qty=float(trade[1]),
            side=TradeSide.SELL if trade[3] == "s" else TradeSide.BUY,
        )

    @staticmethod
    def parse_trade_msg(data: list) -> TradeEvent:
        symbol = Kraken._symbolsMeta.ws_sym2sym(Kraken._name, data[-1])
        trades = [Kraken.parse_trade(trade) for trade in data[1]]
        ts_exchange = max([int(float(trade[2]) * 1e9) for trade in data[1]])

        return TradeEvent(
            exch_name=Kraken._name,
            symbol=symbol,
            trades=trades,
            ts_exchange=ts_exchange,
            ts_recorded=int(time.time() * 1e9),
        )
