#!/usr/bin/env python3
import asyncio
import json
import logging
import signal
import sys


class Endpoint(object):

    def __init__(self, name, path):
        self._name = name
        self._path = path
        self._reader = self._writer = None

    @property
    def name(self):
        return self._name

    @property
    def writer(self):
        return self._writer

    async def connect(self):
        self._reader, self._writer = await asyncio.open_unix_connection(self._path)

    async def readline(self):
        return await self._reader.readline()

    def write(self, data):
        self._writer.write(data.encode())


class Router(object):

    def __init__(self):
        self._endpoints = {}
        self._logger = logging.getLogger('router')

    async def listen(self, endpoint_from, endpoint_to):
        e_from = self._endpoints[endpoint_from]
        e_to = self._endpoints[endpoint_to]
        try:
            while True:
                sdata = await e_from.readline()
                if not sdata:
                    break
                data = sdata.decode().strip()
                if not data:
                    continue
                if data.startswith("message:"):
                    message = json.dumps({"text":data[8:]}, ensure_ascii=False)
                    self._logger.debug("%s -> %s: [%s]", endpoint_from, endpoint_to, message)
                    e_to.write("{}\n".format(message))
        except asyncio.CancelledError:
            pass

    async def listen_brain(self):
        e_from = self._endpoints['brain']
        e_to = self._endpoints['tg']
        try:
            while True:
                sdata = await e_from.readline()
                if not sdata:
                    break
                data = sdata.decode().strip()
                if not data:
                    continue
                event = json.loads(data)
                self._logger.debug("Brain -> TG: [%s]", event['text'])
                e_to.write("message: {}\n".format(event['text']))
        except asyncio.CancelledError:
            pass

    def add_endpoint(self, name, path):
        self._endpoints[name] = Endpoint(name, path)

    def run(self):
        loop = asyncio.get_event_loop()
        for signame in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(signame, loop.stop)
        tasks = []
        for endpoint in self._endpoints.values():
            tasks.append(loop.create_task(endpoint.connect()))
        loop.run_until_complete(asyncio.gather(*tasks))
        self._endpoints['tg'].write('register backend\n')
        loop.create_task(self.listen('tg', 'brain'))
        loop.create_task(self.listen_brain())
        loop.run_forever()


if __name__ == '__main__':
    logging.basicConfig(stream=sys.stderr, level=logging.DEBUG)
    logging.getLogger('router').setLevel(logging.DEBUG)
    logging.getLogger('asyncio').setLevel(logging.WARNING)
    router = Router()
    router.add_endpoint('tg', "/tmp/pa_socket")
    router.add_endpoint('brain', "/tmp/pa_brain")
    router.run()
