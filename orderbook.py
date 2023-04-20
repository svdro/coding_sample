import asyncio
import logging

from copy import deepcopy
from ws_apis.events import OrderbookEvent, OBEventType, Level
from typing import Any, Callable, Optional


def _update_side(side: dict[float, float], level: Level):
    if level.price in side and level.qty <= 0:
        del side[level.price]
    elif level.qty > 0:
        side[level.price] = level.qty


def _trim_side(side: dict[float, float], depth: int, reverse: bool = False):
    keys = sorted(side.keys(), reverse=reverse)[depth:]
    for k in keys:
        del side[k]


class Orderbook:
    """
    Orderbook maintains a local copy of the orderbook for a given symbol.
    It updates the orderbook state when it receives an update from the exchange.
    """

    def __init__(self, exch_name: str, symbol: str, depth: int):
        self.exch_name = exch_name
        self.symbol = symbol
        self.depth = depth

        self.asks: dict[float, float] = {}
        self.bids: dict[float, float] = {}
        self.ts_exchange: int = 0
        self.ts_recorded: int = 0

        self._lock = asyncio.Lock()
        self._logger = logging.getLogger(__name__)

    def _handle_snapshot(self, event: OrderbookEvent):
        self._logger.info(f"taking snapshot of {self.symbol}")

        self.asks = {l.price: l.qty for l in event.asks}
        self.bids = {l.price: l.qty for l in event.bids}

    def _handle_update(self, event: OrderbookEvent):
        for l in event.asks:
            _update_side(self.asks, l)

        for l in event.bids:
            _update_side(self.bids, l)

        if len(self.asks) > self.depth:
            _trim_side(self.asks, self.depth)

        if len(self.bids) > self.depth:
            _trim_side(self.bids, self.depth, reverse=True)

    def update(self, event: OrderbookEvent):
        self.ts_exchange = event.ts_exchange
        self.ts_recorded = event.ts_recorded

        if event.type == OBEventType.SNAPSHOT:
            self._handle_snapshot(event)
        else:
            self._handle_update(event)

    def take_snapshot(self, depth: Optional[int] = None) -> OrderbookEvent:
        depth = depth or self.depth
        return OrderbookEvent(
            self.exch_name,
            self.symbol,
            OBEventType.SNAPSHOT,
            [Level(p, q) for p, q in sorted(self.bids.items(), reverse=True)[:depth]],
            [Level(p, q) for p, q in sorted(self.asks.items())[:depth]],
            self.ts_exchange,
            self.ts_recorded,
        )

    async def async_update(self, event: OrderbookEvent):
        """thread-safe update of the orderbook"""
        async with self._lock:
            self.update(event)

    async def async_take_snapshot(self, depth: Optional[int] = None) -> OrderbookEvent:
        """thread-safe snapshot of the orderbook"""
        async with self._lock:
            return self.take_snapshot(depth)


""" ==================== Orderbook Alt ==================== """


def find_idx(x: list[Any], key: Callable):
    """
    find the idx of the first occurence of an element that matches the key.
    in practice this is faster than binary search when implemented in python
    because most of the time orderbook updates occur near the start of the list.
    """
    for i, b in enumerate(x):
        if key(b):
            return i
    return len(x)


def update_side(side: list[Level], l: Level, idx: int):
    """
    utility function to update a side of the orderbook
    does not need to return anything because it mutates the list
    """
    # skip
    if idx >= len(side) and l.qty <= 0:
        return

    # append
    elif idx >= len(side) and l.qty > 0:
        side.append(l)

    # replace
    elif side[idx].price == l.price and l.qty > 0:
        side[idx] = l

    # delete
    elif side[idx].price == l.price and l.qty <= 0:
        del side[idx]

    # insert
    elif l.qty > 0:
        side.insert(idx, l)


class OrderbookAlt:
    """
    List-based version of Orderbook (almost 100 x slower at depth = 5000)
    """

    def __init__(self, exch_name: str, symbol: str, depth: int = 10):
        self.exch_name = exch_name
        self.symbol = symbol
        self.depth = depth
        self._depth = depth

        self.bids: list[Level] = []
        self.asks: list[Level] = []
        self.ts_exchange = 0
        self.ts_recorded = 0

        self._lock = asyncio.Lock()
        self._logger = logging.getLogger(__name__)

    def _update_bids(self, l: Level):
        idx = find_idx(self.bids, lambda b: b.price <= l.price)
        update_side(self.bids, l, idx)

    def _update_asks(self, l: Level):
        idx = find_idx(self.asks, lambda a: a.price >= l.price)
        update_side(self.asks, l, idx)

    def _handle_snapshot(self, event: OrderbookEvent):
        self._logger.info(f"Received snapshot for {self.symbol}")
        self.bids = event.bids
        self.asks = event.asks

    def _handle_update(self, event: OrderbookEvent):
        for b in event.bids:
            self._update_bids(b)
        for a in event.asks:
            self._update_asks(a)
        self.bids = self.bids[: self.depth]
        self.asks = self.asks[: self.depth]

    def update(self, event: OrderbookEvent):
        """processes an update event and updates the orderbook state"""
        self.ts_exchange = event.ts_exchange
        self.ts_recorded = event.ts_recorded

        if event.type == OBEventType.SNAPSHOT:
            self._handle_snapshot(event)
        elif event.type == OBEventType.UPDATE:
            self._handle_update(event)

    def take_snapshot(self, depth: Optional[int] = None) -> OrderbookEvent:
        """takes a snapshot of the current state of the orderbook"""
        depth = depth or self.depth
        return OrderbookEvent(
            self.exch_name,
            self.symbol,
            OBEventType.SNAPSHOT,
            deepcopy(self.bids[:depth]),
            deepcopy(self.asks[:depth]),
            self.ts_exchange,
            self.ts_recorded,
        )

    async def async_update(self, event: OrderbookEvent):
        """thread-safe update of the orderbook"""
        async with self._lock:
            self.update(event)

    async def async_take_snapshot(self, depth: Optional[int] = None) -> OrderbookEvent:
        """thread-safe snapshot of the orderbook"""
        async with self._lock:
            return self.take_snapshot(depth)
