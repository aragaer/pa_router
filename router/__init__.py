import asyncio
import configparser
import json
import libtmux
import logging
import os
import signal


class Endpoint(object):

    def __init__(self, name, reader, writer, router, config):
        self._name = name
        self._logger = logging.getLogger(name)
        self._reader = reader
        self._writer = writer
        self._router = router
        self._config = config
        self._loop = asyncio.get_event_loop()

    @property
    def name(self):
        return self._name

    def start(self):
        self._writer.write("register backend\n".encode())
        self._loop.create_task(self.run())

    async def run(self):
        while True:
            data = await self._reader.readline()
            if not data:
                break
            sdata = data.decode().strip()
            if not sdata:
                continue
            tg_message = json.loads(sdata)['message']
            message = {'text': tg_message['text'],
                       'user': tg_message['from']['id']}
            self._logger.info("Message: %s", str(message))
            self._loop.create_task(self._router.handle(self, message))


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
        for _ in range(100):
            if os.path.exists(socket_path):
                self._logger.debug("Found socket %s", socket_path)
                break
            await asyncio.sleep(0.1)
        else:
            self._logger.error("Socket %s not found", socket_path)
        reader, writer = await asyncio.open_unix_connection(socket_path)
        endpoint = Endpoint(endpoint_name, reader, writer,
                            self, self._config[endpoint_name])
        endpoint.start()
        self._endpoints[endpoint_name] = endpoint

    def start_submodule(self, module_name, **config):
        window = self._tmux_session.find_where({"window_name": module_name})
        if window is None:
            if os.path.exists(config['socket']):
                os.unlink(config['socket'])
            # Explicitly set window_shell to sh
            # If window shell becomes bash it changes window_name
            window = self._tmux_session.new_window(window_name=module_name,
                                                   start_directory=config.get('workdir'),
                                                   window_shell="sh")
            pane = window.attached_pane
            if 'virtualenv' in config:
                pane.send_keys('. {}/bin/activate'.format(config['virtualenv']))
            pane.send_keys(config['command'])

    def run(self):
        loop = asyncio.get_event_loop()
        for signame in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(signame, loop.stop)
        try:
            self._tmux_session = self._tmux_server.find_where({"session_name": self._args.session})
        except libtmux.exc.LibTmuxException:
            self._tmux_session = None
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

    async def handle(self, endpoint, message):
        self._logger.info("Discarded message from %s: %s", endpoint.name, str(message))
