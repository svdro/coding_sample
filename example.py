import asyncio
import sys

from datetime import datetime
from ws_apis import BinanceWebsocket, KrakenWebsocket, OrderbookEvent
from ws_apis import Level, StreamType
from orderbook import Orderbook

import logging

logging.basicConfig(level=logging.INFO, stream=sys.stdout)


def calculate_spot_price(b: list[Level], a: list[Level]) -> float:
    if not a or not b:
        return 0.0
    return (b[0].price * a[0].qty + a[0].price * b[0].qty) / (b[0].qty + a[0].qty)


def format_orderbook_snapshot(snap: OrderbookEvent, depth: int = 3) -> str:
    ts_exch = datetime.fromtimestamp(snap.ts_exchange / 1e9).strftime("%H:%M:%S.%f")

    info_str = f" - {ts_exch} - {snap.exch_name:<10s} ({snap.symbol:<8s}):"
    b_str = " ".join(f"({b.price:.4f} {b.qty:.3f})" for b in snap.bids[:depth][::-1])
    a_str = " ".join(f"({a.price:.4f} {a.qty:.3f})" for a in snap.asks[:3])
    s_str = f"{calculate_spot_price(snap.bids, snap.asks):.4f}"

    _g, _r, _dflt = "\033[92m", "\033[91m", "\033[0m"  # green, red, default
    return f"{info_str}\t{_g}{b_str}{_dflt} | {s_str} | {_r}{a_str}{_dflt}"


async def stream_orderbook(exch_name: str, symbol: str):
    orderbook = Orderbook(exch_name, symbol)
    Websocket = BinanceWebsocket if exch_name == "binance" else KrakenWebsocket

    async with Websocket(StreamType.BOOK, symbol) as ws:
        while True:
            event = await ws.recv()

            assert isinstance(event, OrderbookEvent)
            orderbook.update(event)

            snap = orderbook.take_snapshot()
            print(format_orderbook_snapshot(snap))


async def main(exch_name: str, symbol: str):
    await stream_orderbook(exch_name, symbol)


if __name__ == "__main__":
    exch_name = sys.argv[1] if len(sys.argv) > 1 else "binance"
    symbol = sys.argv[2] if len(sys.argv) > 2 else "btcusdt"
    try:
        asyncio.run(main(exch_name, symbol))
    except KeyboardInterrupt:
        pass
