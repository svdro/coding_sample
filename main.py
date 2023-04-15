import asyncio
import signal

from ws import run_ws

from utils import raise_graceful_exit

from exchanges import Binance, Kraken, WsEventType
from events import WsEventType, handle_events


async def main():
    # register signal handler
    loop = asyncio.get_event_loop()
    loop.add_signal_handler(signal.SIGINT, raise_graceful_exit)

    # create symbols list
    exch = Kraken()
    symbols = ["btceur"]
    # symbols = ["btceur", "etheur", "adaeur", "dogeeur"]

    # create eventsQueue and events_task
    eventsQueue = asyncio.Queue()
    events_task = asyncio.create_task(handle_events(eventsQueue))

    # create orderbook tasks for each symbol
    ob_tasks = [
        asyncio.create_task(run_ws(exch, [sym], WsEventType.BOOK, eventsQueue))
        for sym in symbols
    ]

    # # create trades tasks for each symbol
    # trades_tasks = [
    # asyncio.create_task(run_ws(exch, [sym], WsEventType.TRADE, eventsQueue))
    # for sym in symbols
    # ]
    trades_tasks = []

    await asyncio.gather(*ob_tasks, *trades_tasks)
    await eventsQueue.join()
    await events_task
    print("closed gracefully")


if __name__ == "__main__":
    asyncio.run(main())
    # asyncio.run(snapshot())
