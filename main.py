import asyncio
import sys
from datetime import datetime


from ws_apis import BinanceWebsocket, KrakenWebsocket, OrderbookEvent, StreamType
from orderbook import Orderbook


# logging.basicConfig(level=logging.INFO)
_green, _red, _default = "\033[92m", "\033[91m", "\033[0m"


async def stream_orderbook(exch_name: str, symbol: str):
    orderbook = Orderbook(exch_name, symbol)
    Websocket = BinanceWebsocket if exch_name == "binance" else KrakenWebsocket

    async with Websocket(StreamType.BOOK, "ethusdt") as ws:
        while True:
            event = await ws.recv()
            assert isinstance(event, OrderbookEvent)
            orderbook.update(event)
            snap = orderbook.take_snapshot()

            ts_exch = datetime.fromtimestamp(snap.ts_exchange / 1e9)

            info_str = f"{ts_exch} - {snap.exch_name} ({snap.symbol}):"
            bids_str = ", ".join(f"({b.price:.4f} {b.qty:.3f})" for b in snap.bids[:3])
            asks_str = ", ".join(f"({a.price:.4f} {a.qty:.3f})" for a in snap.asks[:3])

            bids_str = f"{_green}{bids_str}{_default}"
            asks_str = f"{_red}{asks_str}{_default}"
            print(f"{info_str}\tbids: {bids_str} | asks: {asks_str}")


async def main(exch_name: str, symbol: str):
    try:
        await stream_orderbook(exch_name, symbol)
    except KeyboardInterrupt:
        print("KeyboardInterrupt")
    finally:
        pass


if __name__ == "__main__":
    # parse command line arguments
    exch_name = sys.argv[1] if len(sys.argv) > 1 else "binance"
    symbol = sys.argv[2] if len(sys.argv) > 2 else "ethusdt"
    asyncio.run(main(exch_name, symbol))
