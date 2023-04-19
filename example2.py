import asyncio
from collections import defaultdict
from datetime import datetime
from orderbook import Orderbook

from ws_apis import OrderbookEvent, Level, Subscription, WsManager


def calculate_spot_price(b: list[Level], a: list[Level]) -> float:
    if not a or not b:
        return 0.0
    return (b[0].price * a[0].qty + a[0].price * b[0].qty) / (b[0].qty + a[0].qty)


def format_orderbook_snapshot(snap: OrderbookEvent, depth: int = 3) -> str:
    ts_exch = datetime.fromtimestamp(snap.ts_exchange / 1e9).strftime("%H:%M:%S.%f")

    info_str = f" - {ts_exch} - {snap.exch_name:<10s} ({snap.symbol:<8s}):"
    b_str = " ".join(f"({b.price:.4f} {b.qty:.3f})" for b in snap.bids[:depth][::-1])
    a_str = " ".join(f"({a.price:.4f} {a.qty:.3f})" for a in snap.asks[:depth])
    s_str = f"{calculate_spot_price(snap.bids, snap.asks):.4f}"

    _g, _r, _dflt = "\033[92m", "\033[91m", "\033[0m"  # green, red, default
    return f"{info_str}\t{_g}{b_str}{_dflt} | {s_str} | {_r}{a_str}{_dflt}"


async def maintain_orderbooks(
    subs: list[Subscription], orderbooks: dict[str, dict[str, Orderbook]]
):
    async with WsManager(subs) as wsm:
        while True:
            event = await wsm.recv()
            exch_name, symbol = event.exch_name, event.symbol

            if isinstance(event, OrderbookEvent):
                await orderbooks[exch_name][symbol].async_update(event)


async def print_orderbooks(orderbooks: dict[str, dict[str, Orderbook]]) -> None:
    while True:
        t = datetime.now().strftime("%H:%M:%S.%f")
        print(f"\n{t} - Orderbook snapshots:")
        for exch_obs in orderbooks.values():
            for ob in exch_obs.values():
                snap = await ob.async_take_snapshot()
                print(format_orderbook_snapshot(snap))

        await asyncio.sleep(1)


async def main():
    subs = [
        Subscription("kraken", "btcusdt", "book"),
        Subscription("binance", "btcusdt", "book"),
    ]

    # create orderbooks
    orderbooks = defaultdict(dict)
    for sub in [s for s in subs if s.stream_name == "book"]:
        ob = Orderbook(sub.exch_name, sub.symbol)
        orderbooks[sub.exch_name][sub.symbol] = ob

    # start tasksj
    tasks = (
        asyncio.create_task(maintain_orderbooks(subs, orderbooks)),
        asyncio.create_task(print_orderbooks(orderbooks)),
    )

    await asyncio.wait(
        tasks,
        timeout=None,
        return_when=asyncio.FIRST_EXCEPTION,
    )


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
