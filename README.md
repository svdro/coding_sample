## Coding Sample

This is a basic python wrapper that provides a uniform interface for interacting with public crypto exchange websocket streams using the asyncio framework.

## Features

 * Supports the [Kraken](https://docs.kraken.com/websockets) and [Binance](/https://binance-docs.github.io/apidocs/) websocket apis
 * Implementation of Orderbook and Trades Streams
 * Common Interface for interacting with ws-streams 
   * maps symbol names (which differ between exchanges) to a standardized format (e.g: btcusdt)
   * abstracts subscribing/unsubscribing and handling ws-events
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
  with BinanceWebsocket(StreamType.BOOK, "ethusdt") as ws:
    for _ in range(10):
      event = await ws.recv()
      print(event)

if __name__ == "__main__":
  asyncio.run(main())
```