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
    SNAPSHOT = "snapshot"
    UPDATE = "update"


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
