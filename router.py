#!/usr/bin/env python3
import asyncio
import json
import logging
import signal
import sys


class Router(object):

    def __init__(self):
        self._tg_reader = None
        self._tg_writer = None
        self._brain_reader = None
        self._brain_writer = None
        self._logger = logging.getLogger('router')

    async def connect_telegram(self):
        self._tg_reader, self._tg_writer = await asyncio.open_unix_connection("/tmp/pa_socket")

    async def connect_brain(self):
        self._brain_reader, self._brain_writer = await asyncio.open_unix_connection("/tmp/pa_brain")

    async def listen_tg(self):
        try:
            while True:
                sdata = await self._tg_reader.readline()
                if not sdata:
                    break
                data = sdata.decode().strip()
                if not data:
                    continue
                if data.startswith("message:"):
                    message = json.dumps({"text":data[8:]}, ensure_ascii=False)
                    self._logger.debug("TG -> Brain: [%s]", message)
                    self._brain_writer.write("{}\n".format(message).encode())
        except asyncio.CancelledError:
            pass

    async def listen_brain(self):
        try:
            while True:
                sdata = await self._brain_reader.readline()
                if not sdata:
                    break
                data = sdata.decode().strip()
                if not data:
                    continue
                event = json.loads(data)
                self._logger.debug("Brain -> TG: [%s]", event['text'])
                self._tg_writer.write("message: {}\n".format(event['text']).encode())
        except asyncio.CancelledError:
            pass

    def run(self):
        loop = asyncio.get_event_loop()
        for signame in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(signame, loop.stop)
        tg_task = loop.create_task(self.connect_telegram())
        brain_task = loop.create_task(self.connect_brain())
        loop.run_until_complete(asyncio.gather(tg_task, brain_task))
        self._tg_writer.write('register backend\n'.encode())
        loop.create_task(self.listen_tg())
        loop.create_task(self.listen_brain())
        loop.run_forever()


if __name__ == '__main__':
    logging.basicConfig(stream=sys.stderr, level=logging.DEBUG)
    logging.getLogger('router').setLevel(logging.DEBUG)
    logging.getLogger('asyncio').setLevel(logging.WARNING)
    Router().run()
