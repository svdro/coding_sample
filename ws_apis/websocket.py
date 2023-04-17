import websockets
import asyncio
import logging

from typing import Optional


class Websocket:
    _timeout = 10

    def __init__(self, ws_url: str, subscription_msg: str):
        self._ws_url = ws_url
        self._subscription_msg = subscription_msg

        self._logger = logging.getLogger(__name__)

        self._loop = asyncio.get_event_loop()
        self._coros = []

        self._queue = asyncio.Queue()
        self._conn: Optional[websockets.WebSocketClientProtocol] = None

    async def __aenter__(self):
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.cleanup()

    async def connect(self):
        self._conn = await websockets.connect(self._ws_url)
        await self._conn.send(self._subscription_msg)

        task = self._loop.create_task(self._listen_loop())
        self._coros.append(task)

    async def cleanup(self):
        if self._conn is not None:
            await self._conn.close()

        for task in self._coros:
            task.cancel()

        await asyncio.wait(self._coros, timeout=10)

    async def _listen(self):
        assert self._conn is not None
        msg = await self._conn.recv()
        await self._queue.put(msg)

    async def _listen_loop(self):
        assert self._conn is not None

        while True:
            try:
                await self._listen()
            except asyncio.CancelledError as e:
                self._logger.info(f"cancelled error{e}")
                break
            except websockets.ConnectionClosedError as e:
                self._logger.info(f"connection closed{e}")
                break
            except websockets.ConnectionClosedOK as e:
                self._logger.info(f"connection closed ok{e}")
                break
            except Exception as e:
                self._logger.info(f"exception: {e}")
                return

    async def recv(self):
        msg = await asyncio.wait_for(self._queue.get(), timeout=self._timeout)
        return msg
