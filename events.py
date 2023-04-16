import asyncio
from dataclasses import dataclass
from enum import Enum
from typing import Optional, Any
from datetime import datetime


class WsEventType(Enum):
    OTHER = 1
    BOOK = 2
    TRADE = 3


class OBEventType(Enum):
    SNAPSHOT = 1
    UPDATE = 2


class TradeSide(Enum):
    BUY = 1
    SELL = 2


@dataclass
class Level:
    price: float
    qty: float


@dataclass
class OrderbookEvent:
    exch_name: str
    symbol: str
    type: OBEventType
    bids: list[Level]
    asks: list[Level]
    ts_exchange: int  # timestamp from exchange (in nanoseconds)
    ts_recorded: int  # timestamp when event was recorded (in nanoseconds)
    other: Optional[Any] = None


@dataclass
class Trade:
    price: float
    qty: float
    side: TradeSide


@dataclass
class TradeEvent:
    exch_name: str
    symbol: str
    ts_exchange: int
    ts_recorded: int
    trades: list[Trade]


async def handle_events(eventsQueue: asyncio.Queue, symbols: list[str]):
    from orderbook import BinanceOrderbook

    # raise_graceful_exit()

    orderbooks = {symbol: BinanceOrderbook(symbol) for symbol in symbols}

    while True:
        event = await eventsQueue.get()
        eventsQueue.task_done()

        if isinstance(event, OrderbookEvent):
            orderbooks[event.symbol].update(event)
            # ob.update(event)

        # exch_time = datetime.fromtimestamp(event.ts_exchange / 1e9)
        # created_time = datetime.fromtimestamp(event.ts_recorded / 1e9)
        # print(
        # f"\nreceived {event.__class__.__name__} ({event.type}) event",
        # event.symbol,
        # )
        # print(f"exchange time: {exch_time}")
        # print(f"  created time: {created_time}")
        # print(f"processed time: {datetime.now()}")

        # if isinstance(event, OrderbookEvent):
        # print("_OB_", end="")
        # elif isinstance(event, TradeEvent):
        # print("_TRADE_", end="")
        # sys.stdout.flush()

    # except GracefulExit:
    # print("GracefulExit")
    # return
