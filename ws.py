import asyncio
import json
import websockets
from exchanges import Exchange, Binance
from events import WsEventType
from typing import Callable


async def queue_callback(
    eventsQueue: asyncio.Queue, callback: Callable, *args, **kwargs
):
    """executes a callback and puts the result in the eventsQueue"""
    result = await callback(*args, **kwargs)
    eventsQueue.put_nowait(result)


async def listen(
    exchange: Exchange,
    websocket: websockets.WebSocketClientProtocol,
    eventsQueue: asyncio.Queue,
):
    async for msg in websocket:
        data = json.loads(msg)
        msg_type = exchange.parse_event_type(data)

        if msg_type == WsEventType.OTHER:
            pass

        elif msg_type == WsEventType.BOOK:
            event = exchange.parse_book_msg(data)
            eventsQueue.put_nowait(event)

        elif msg_type == WsEventType.TRADE:
            event = exchange.parse_trade_msg(data)
            eventsQueue.put_nowait(event)


async def run_ws(
    exchange: Exchange,
    symbols: list[str],
    wsEventType: WsEventType,
    eventsQueue: asyncio.Queue,
):

    async with websockets.connect(exchange._ws_url) as websocket:
        if wsEventType == WsEventType.BOOK:
            subscription_msg = exchange.prepare_book_subscription_msg(symbols)
        elif wsEventType == WsEventType.TRADE:
            subscription_msg = exchange.prepare_trades_subscription_msg(symbols)
        else:
            raise ValueError("wsEventType must be either BOOK or TRADE")
        await websocket.send(subscription_msg)

        if isinstance(exchange, Binance):
            # query binance rest api to get the orderbook snapshot
            # and put it in the eventsQueue
            for s in symbols:
                asyncio.create_task(
                    queue_callback(eventsQueue, exchange.get_orderbook_rest, s)
                )

        # listen to incoming ws messages
        try:
            await listen(exchange, websocket, eventsQueue)

        except asyncio.exceptions.CancelledError:
            pass

        except Exception as e:
            print("Received other exception\n", e)

        finally:
            if not websocket.closed:
                await websocket.close()
