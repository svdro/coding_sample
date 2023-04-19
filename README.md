## Coding Sample

This is a basic python wrapper that provides a uniform interface for interacting with public crypto exchange websocket streams using the asyncio framework.

## Features

 * Supports the [Kraken](https://docs.kraken.com/websockets) and [Binance](https://binance-docs.github.io/apidocs/) websocket apis
 * Implementation of Orderbook and Trades Streams
 * Common Interface for interacting with ws-streams 
   * maps symbol names (which differ between exchanges) to a standardized format (e.g: btcusdt)
   * abstracts subscribing/unsubscribing and handling ws-events
   * synchronizes binance orderbook (Diff. Depth) stream with rest api orderbook (depth) snapshots
 * Websocket messages are parsed and returned as standardized "python dataclasses" events
 * Handles maintaining and closing multiple ws-connections

## Example

```python
import asyncio
from ws_apis import BinanceWebsocket,  StreamType

async def main():
  ws = KrakenWebsocket(StreamType.TRADES, "ethusdt")
  await ws.connect()
  try:
    for _ in range(10):
      event = ws.recv()
      print(event)
  finally:
    ws.cleanup()

if __name__ == "__main__":
  asyncio.run(main())
```


## Example using async context managers

```python
import asyncio
from ws_apis import BinanceWebsocket, StreamType

async def main():
  async with BinanceWebsocket(StreamType.BOOK, "ethusdt") as ws:
    for _ in range(10):
      event = await ws.recv()
      print(event)
```

## Example subscribing to multiple websocket streams

```python
import asyncio
from ws_apis import WsManager, Subscription, OrderbookEvent

async def main():
  subs = [
    Subscription("kraken", "adaeur", "book"),
    Subscription("kraken", "adaeur", "trades"),
    Subscription("binance", "adausdt", "book"),
    Subscription("binance", "adausdt", "trades"),
    ]
  
  async with WsManager(subs) as wsm:
    for _ in range(50):
      event = await wsm.recv()
      stream_name = "book" if isinstance(event, OrderbookEvent) else "trades"
      print(f"{event.ts_exchange} {stream_name:5s} {event.exch_name:7s} {event.symbol:8s}")
```
