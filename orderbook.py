import asyncio
import logging

from copy import deepcopy
from datetime import datetime
from ws_apis.events import OrderbookEvent, OBEventType, Level
from typing import Any, Callable


def find_idx(x: list[Any], comparison: Callable):
    """utility function to find index of element in list"""
    for i, b in enumerate(x):
        if comparison(b):
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


class Orderbook:
    """
    Orderbook maintains a local copy of the orderbook for a given symbol.
    It updates the orderbook state when it receives an update from the exchange.
    """

    def __init__(self, exch_name: str, symbol: str, depth: int = 10):
        self.exch_name = exch_name
        self.symbol = symbol
        self.depth = depth
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

    def take_snapshot(self) -> OrderbookEvent:
        """taskes a snapshot of the current state of the orderbook"""
        return OrderbookEvent(
            self.exch_name,
            self.symbol,
            OBEventType.SNAPSHOT,
            deepcopy(self.bids),
            deepcopy(self.asks),
            self.ts_exchange,
            self.ts_recorded,
        )

    async def async_update(self, event: OrderbookEvent):
        """thread-safe update of the orderbook"""
        async with self._lock:
            self.update(event)

    async def async_take_snapshot(self) -> OrderbookEvent:
        """thread-safe snapshot of the orderbook"""
        async with self._lock:
            return self.take_snapshot()
