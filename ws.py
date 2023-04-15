import asyncio
import json
import websockets
from exchanges import Exchange
from events import WsEventType


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

    print("connecting to", exchange.get_ws_url())
    async with websockets.connect(exchange.get_ws_url()) as websocket:
        if wsEventType == WsEventType.BOOK:
            subscription_msg = exchange.prepare_book_subscription_msg(symbols)
        elif wsEventType == WsEventType.TRADE:
            subscription_msg = exchange.prepare_trades_subscription_msg(symbols)
        else:
            raise ValueError("wsEventType must be either BOOK or TRADE")
        await websocket.send(subscription_msg)

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
