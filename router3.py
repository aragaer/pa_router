#!/usr/bin/env python3
import asyncio
import configparser
import json
import libtmux
import logging
import os
import signal
import sys


class Endpoint(object):

    def __init__(self, reader, writer, router, config):
        self._reader = reader
        self._writer = writer
        self._router = router
        self._config = config

    def start(self):
        self._writer.write("register backend\n".encode())


class Router(object):

    def __init__(self, args):
        self._args = args
        self._endpoints = {}
        self._logger = logging.getLogger('router')
        self._tmux_server = libtmux.Server()
        self._config = configparser.ConfigParser()
        self._config.read(self._args.config)

    async def connect(self, endpoint_name):
        socket_path = self._config[endpoint_name]['socket']
        for _ in range(10):
            if os.path.exists(socket_path):
                break
            await asyncio.sleep(0.1)
        reader, writer = await asyncio.open_unix_connection(socket_path)
        endpoint = Endpoint(reader, writer, self, self._config[endpoint_name])
        endpoint.start()
        self._endpoints[endpoint_name] = endpoint

    def start_submodule(self, module_name, **config):
        window = self._tmux_session.find_where({"window_name": module_name})
        if window is None:
            if os.path.exists(config['socket']):
                os.unlink(config['socket'])
            window = self._tmux_session.new_window(window_name=module_name,
                                                start_directory=config.get('workdir'))
            pane = window.attached_pane
            if 'virtualenv' in config:
                pane.send_keys('. {}/bin/activate'.format(config['virtualenv']))
            pane.send_keys(config['command'])

    def run(self):
        loop = asyncio.get_event_loop()
        #for signame in (signal.SIGINT, signal.SIGTERM):
        #    loop.add_signal_handler(signame, loop.stop)
        self._tmux_session = self._tmux_server.find_where({"session_name": self._args.session})
        if self._tmux_session is None:
            self._logger.info("creating session %s", self._args.session)
            self._tmux_session = self._tmux_server.new_session(session_name=self._args.session)
        for section in self._config.sections():
            config = self._config[section]
            self.start_submodule(section, **config)

        tasks = []
        for section in self._config.sections():
            tasks.append(loop.create_task(self.connect(section)))
        loop.run_until_complete(asyncio.gather(*tasks))
        loop.run_forever()


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description="Personal assistant message router")
    parser.add_argument("--session", default="pa", help="Tmux session name")
    parser.add_argument("--config", default="router.conf", help="Config file")
    logging.basicConfig(stream=sys.stderr, level=logging.DEBUG)
    logging.getLogger('router').setLevel(logging.DEBUG)
    logging.getLogger('asyncio').setLevel(logging.WARNING)
    router = Router(parser.parse_args())
    router.run()
