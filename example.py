import asyncio
import sys
from datetime import datetime


from ws_apis import BinanceWebsocket, KrakenWebsocket, OrderbookEvent, StreamType
from orderbook import Orderbook


# logging.basicConfig(level=logging.INFO)
_g, _r, _dflt = "\033[92m", "\033[91m", "\033[0m"


def format_orderbook_snapshot(snap: OrderbookEvent) -> str:
    ts_exch = datetime.fromtimestamp(snap.ts_exchange / 1e9).strftime("%H:%M:%S.%f")

    info_str = f" - {ts_exch} - {snap.exch_name} ({snap.symbol}):"
    bids_str = ", ".join(f"({b.price:.4f} {b.qty:.3f})" for b in snap.bids[:3])
    asks_str = ", ".join(f"({a.price:.4f} {a.qty:.3f})" for a in snap.asks[:3])

    return f"{info_str}\tbids: {_g}{bids_str}{_dflt} | asks: {_r}{asks_str}{_dflt}"


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
    try:
        await stream_orderbook(exch_name, symbol)
    except KeyboardInterrupt:
        print("KeyboardInterrupt")
    finally:
        pass


if __name__ == "__main__":
    exch_name = sys.argv[1] if len(sys.argv) > 1 else "binance"
    symbol = sys.argv[2] if len(sys.argv) > 2 else "btcusdt"
    asyncio.run(main(exch_name, symbol))
