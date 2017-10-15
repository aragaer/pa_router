import logging
import os
import shlex
import socket
import subprocess
import time
import yaml

from routing import PipeFaucet, PipeSink, SocketFaucet, SocketSink


class App:

    def __init__(self, command, type="stdio", **kwargs):
        self._command = shlex.split(command)
        self._proc = None
        self._sink = None
        self._faucet = None
        self._type = type
        self._kwargs = kwargs

    def start(self):
        if self._type == 'stdio':
            self._proc = subprocess.Popen(self._command,
                                          stdin=subprocess.PIPE,
                                          stdout=subprocess.PIPE)
            self._sink = PipeSink(self._proc.stdin.fileno())
            self._faucet = PipeFaucet(self._proc.stdout.fileno())
        elif self._type == 'socket':
            sockname = self._kwargs['socket']
            self._proc = subprocess.Popen(self._command)
            sock = socket.socket(socket.AF_UNIX)
            while not os.path.exists(sockname):
                time.sleep(0.1)
            sock.connect(sockname)
            self._sink = SocketSink(sock)
            self._faucet = SocketFaucet(sock)

    @property
    def faucet(self):
        return self._faucet

    @property
    def sink(self):
        return self._sink


class Runner:

    def __init__(self):
        self._logger = logging.getLogger("runner")
        self._apps = {}

    def load(self, config_file_name):
        self._logger.info("Loading config %s", config_file_name)
        with open(config_file_name) as config_file:
            config = yaml.safe_load(config_file)
        for app, app_config in config.items():
            self._apps[app] = App(**app_config)

    def ensure_running(self, app_name):
        self._logger.info("Starting application %s", app_name)
        self._apps[app_name].start()

    def get_faucet(self, app_name):
        return self._apps[app_name].faucet

    def get_sink(self, app_name):
        return self._apps[app_name].sink
