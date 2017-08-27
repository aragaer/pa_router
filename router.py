#!/usr/bin/env python3
import asyncio
import json
import logging
import signal
import sys


class Endpoint(object):

    def __init__(self, name, path, pack_func, unpack_func):
        self._name = name
        self._path = path
        self._reader = self._writer = None
        self._unpack = unpack_func
        self._pack = pack_func

    @property
    def name(self):
        return self._name

    @property
    def writer(self):
        return self._writer

    async def connect(self):
        self._reader, self._writer = await asyncio.open_unix_connection(self._path)

    async def read(self):
        while True:
            sdata = await self._reader.readline()
            if not sdata:
                return None
            data = sdata.decode().strip()
            if not data:
                continue
            message = self._unpack(data)
            if message:
                return message

    def write(self, message):
        self.send(self._pack(message))

    def send(self, data):
        self._writer.write("{}\n".format(data).encode())


def unpack_tg_message(data):
    if data.startswith("message:"):
        return data[8:].strip()


def pack_brain_message(message):
    if message:
        return json.dumps({"text":message}, ensure_ascii=False)


def unpack_brain_message(data):
    return json.loads(data)['text']


def pack_tg_message(message):
    return "message: {}".format(message)


class Router(object):

    def __init__(self):
        self._endpoints = {}
        self._logger = logging.getLogger('router')

    async def listen(self, endpoint_from, endpoint_to):
        e_from = self._endpoints[endpoint_from]
        e_to = self._endpoints[endpoint_to]
        try:
            while True:
                message = await e_from.read()
                if not message:
                    break
                self._logger.debug("%s -> %s: [%s]", endpoint_from, endpoint_to, message)
                e_to.write(message)
        except asyncio.CancelledError:
            pass

    def add_endpoint(self, name, path, pack, unpack):
        self._endpoints[name] = Endpoint(name, path, pack, unpack)

    def run(self):
        loop = asyncio.get_event_loop()
        for signame in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(signame, loop.stop)
        tasks = []
        for endpoint in self._endpoints.values():
            tasks.append(loop.create_task(endpoint.connect()))
        loop.run_until_complete(asyncio.gather(*tasks))
        self._endpoints['tg'].send('register backend')
        loop.create_task(self.listen('tg', 'brain'))
        loop.create_task(self.listen('brain', 'tg'))
        loop.run_forever()


if __name__ == '__main__':
    logging.basicConfig(stream=sys.stderr, level=logging.DEBUG)
    logging.getLogger('router').setLevel(logging.DEBUG)
    logging.getLogger('asyncio').setLevel(logging.WARNING)
    router = Router()
    router.add_endpoint('tg', "/tmp/pa_socket", pack_tg_message, unpack_tg_message)
    router.add_endpoint('brain', "/tmp/pa_brain", pack_brain_message, unpack_brain_message)
    router.run()
