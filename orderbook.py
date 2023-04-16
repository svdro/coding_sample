from events import Level, OrderbookEvent, OBEventType
from typing import Any, Callable
from utils import raise_graceful_exit

# type OB struct {
# Depth                int
# mu                   sync.Mutex
# Time                 int64
# Symbol               string
# ExchangeName         string
# Bids                 []Level
# Asks                 []Level
# IsWaitingForSnapshot bool
# }


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
    An Orderbook is a data structure that does the following:
    1. It stores the current state of the orderbook for a given symbol.
    2. It updates the orderbook state when it receives an update from the exchange.
    3. It asserts that the orderbook state is insync.
       (If the orderbook state is not insync, it will raise an error.)
    4. It provides an API for the user to access the orderbook state.
    """

    def __init__(self, symbol, depth: int = 10):
        self.exch_name = "kraken"
        self.symbol = symbol
        self.depth = depth
        self.bids: list[Level] = []
        self.asks: list[Level] = []
        self.ts_exchange = 0
        self.ts_recorded = 0

    def _update_bids(self, l: Level):
        idx = find_idx(self.bids, lambda b: b.price <= l.price)
        update_side(self.bids, l, idx)

    def _update_asks(self, l: Level):
        idx = find_idx(self.asks, lambda a: a.price >= l.price)
        update_side(self.asks, l, idx)

    def _handle_snapshot(self, event: OrderbookEvent):
        print("snapshot")
        self.bids = event.bids
        self.asks = event.asks
        self.is_waiting_for_snapshot = False

    def _handle_update(self, event: OrderbookEvent):
        for b in event.bids:
            self._update_bids(b)
        for a in event.asks:
            self._update_asks(a)
        self.bids = self.bids[: self.depth]
        self.asks = self.asks[: self.depth]

    def update(self, event: OrderbookEvent):
        self.ts_exchange = event.ts_exchange
        self.ts_recorded = event.ts_recorded

        if event.type == OBEventType.SNAPSHOT:
            self._handle_snapshot(event)
        elif event.type == OBEventType.UPDATE:
            self._handle_update(event)

        from datetime import datetime

        exch_time = datetime.fromtimestamp(self.ts_exchange / 1e9)
        created_time = datetime.fromtimestamp(self.ts_recorded / 1e9)
        print(f"ORDERBOOK UPDATED ({self.symbol})")
        print(f"exchange time: {exch_time}")
        print(f"  created time: {created_time}")
        print(event)
        print(self)
        print()

    def __repr__(self):
        ob_str = ""
        for a in self.asks[::-1]:
            ob_str += f"{a.price} {a.qty}\n"
        ob_str += f"{(self.bids[0].price + self.asks[0].price) / 2}\n"
        for b in self.bids:
            ob_str += f"{b.price} {b.qty}\n"
        return ob_str


class BinanceEventsBuffer:
    """for handling events order in Binance"""

    def __init__(self):
        self.events_buffer: list[OrderbookEvent] = []
        self.last_update_id = 0
        self.handled_first_event = False

    def buffer_event(self, event: OrderbookEvent):
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

    def yield_events(self, snapEvent: OrderbookEvent):
        assert isinstance(snapEvent.other, dict)
        self.last_update_id = snapEvent.other["last_update_id"]

        for event in self.events_buffer:
            fid, lid = self.parse_update_ids(event)

            # yield the next event if it is valid
            if self._is_event_valid(fid, lid):
                print(f"yielded valid event ({fid}, {lid})")
                self.last_update_id = lid
                yield event

    @staticmethod
    def parse_update_ids(event: OrderbookEvent) -> tuple[int, int]:
        assert isinstance(event.other, dict)
        lid = event.other["last_update_id"]
        fid = event.other["first_update_id"]
        return fid, lid


class BinanceOrderbook:
    """
    An Orderbook is a data structure that does the following:
    1. It stores the current state of the orderbook for a given symbol.
    2. It updates the orderbook state when it receives an update from the exchange.
    3. It asserts that the orderbook state is insync.
       (If the orderbook state is not insync, it will raise an error.)
    4. It provides an API for the user to access the orderbook state.
    """

    def __init__(self, symbol, depth: int = 10):
        self.exch_name = "kraken"
        self.symbol = symbol
        self.depth = depth
        self.bids: list[Level] = []
        self.asks: list[Level] = []
        self.ts_exchange = 0
        self.ts_recorded = 0

        # for synchronizing Binance
        self._is_waiting_for_snapshot = True
        self.events_buffer = BinanceEventsBuffer()

    def _update_bids(self, l: Level):
        idx = find_idx(self.bids, lambda b: b.price <= l.price)
        update_side(self.bids, l, idx)

    def _update_asks(self, l: Level):
        idx = find_idx(self.asks, lambda a: a.price >= l.price)
        update_side(self.asks, l, idx)

    def _handle_snapshot(self, event: OrderbookEvent):
        print("snapshot")
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
        if event.type == OBEventType.SNAPSHOT:
            self._is_waiting_for_snapshot = False
            self._handle_snapshot(event)

            # _, lid = self.events_buffer.parse_update_ids(event)
            print("yielding buffered events")
            for event in self.events_buffer.yield_events(event):
                self._handle_update(event)
        elif event.type == OBEventType.UPDATE:

            if self._is_waiting_for_snapshot:
                self.events_buffer.buffer_event(event)
                print("buffered event")
                return

            if not self.events_buffer.is_event_valid(event):
                print("buffer reset")
                self.events_buffer = BinanceEventsBuffer()
                self._is_waiting_for_snapshot = True

                print("RAISING GRACEFUL EXIT")
                raise_graceful_exit()

            self._handle_update(event)

        self.ts_exchange = event.ts_exchange
        self.ts_recorded = event.ts_recorded
        print(f"ORDERBOOK UPDATED ({self.symbol}, event.type={event.type})")
