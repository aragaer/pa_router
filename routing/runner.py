import logging
import os
import shlex
import socket
import subprocess
import time

import yaml

from . import PipeFaucet, PipeSink, SocketFaucet, SocketSink

_LOGGER = logging.getLogger(__name__)


class Proc:

    def __init__(self, proc, faucet, sink):
        self._proc = proc
        self._faucet = faucet
        self._sink = sink

    @property
    def faucet(self):
        return self._faucet

    @property
    def sink(self):
        return self._sink

    def terminate(self):
        self._faucet.close()
        self._sink.close()
        self._proc.terminate()
        self._proc.wait()


class App:

    def __init__(self, command, type="stdio", **kwargs):
        self._command = shlex.split(command)
        self._type = type
        self._kwargs = kwargs

    def start(self, extra_args, **extra_kwargs):
        if self._type == 'stdio':
            stdin = stdout = subprocess.PIPE
        elif self._type == 'socket':
            sockname = extra_kwargs.get('socket') or self._kwargs['socket']
            stdin = stdout = None
            if os.path.exists(sockname):
                os.unlink(sockname)
        command = self._command[:]
        if extra_args is not None:
            command += extra_args
        proc = subprocess.Popen(command,
                                stdin=stdin,
                                stdout=stdout,
                                cwd=self._kwargs.get('cwd'))
        if self._type == 'stdio':
            sink = PipeSink(proc.stdin.fileno())
            faucet = PipeFaucet(proc.stdout.fileno())
        elif self._type == 'socket':
            sock = socket.socket(socket.AF_UNIX)
            _LOGGER.debug("Waiting for socket %s", sockname)
            while not os.path.exists(sockname):
                time.sleep(0.1)
            sock.connect(sockname)
            sink = SocketSink(sock)
            faucet = SocketFaucet(sock)
        return Proc(proc, faucet, sink)


class Runner:

    def __init__(self):
        self._apps = {}
        self._procs = {}

    def load(self, config_file_name):
        _LOGGER.info("Loading config %s", config_file_name)
        with open(config_file_name) as config_file:
            config = yaml.safe_load(config_file)
        for app, app_config in config.items():
            self._apps[app] = App(**app_config)

    def ensure_running(self, app_name, alias=None, with_args=None, **kwargs):
        if alias is None:
            alias = app_name
        _LOGGER.info("Starting application %s as %s", app_name, alias)
        self._procs[alias] = self._apps[app_name].start(with_args, **kwargs)
        _LOGGER.debug("%s started", alias)

    def get_faucet(self, alias):
        return self._procs[alias].faucet

    def get_sink(self, alias):
        return self._procs[alias].sink

    def terminate(self, alias):
        proc = self._procs[alias]
        proc.terminate()
