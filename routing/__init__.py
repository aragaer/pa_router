import fcntl
import logging
import json
import os

from abc import ABCMeta, abstractmethod


class EndpointClosedException(Exception):
    pass


class Faucet(metaclass=ABCMeta):

    @abstractmethod
    def read(self): #pragma: no cover
        raise NotImplementedError


class PipeFaucet(Faucet):

    def __init__(self, pipe_fd):
        fl = fcntl.fcntl(pipe_fd, fcntl.F_GETFL)
        fcntl.fcntl(pipe_fd, fcntl.F_SETFL, fl | os.O_NONBLOCK)
        self._file = os.fdopen(pipe_fd, mode='rb')

    def read(self):
        try:
            line = self._file.readline()
        except OSError as ex:
            raise EndpointClosedException(ex)
        if line:
            return json.loads(line.decode())


class SocketFaucet(Faucet):

    def __init__(self, sock):
        self._sock = sock
        self._sock.setblocking(False)
        self._buf = ""

    def read(self):
        pos = self._buf.find("\n")
        if pos == -1:
            try:
                data = self._sock.recv(4096).decode()
                if not data:
                    raise EndpointClosedException()
                self._buf += data
            except BlockingIOError:
                pass
            pos = self._buf.find("\n")
        if pos == -1:
            return
        line, self._buf = self._buf[:pos], self._buf[pos+1:]
        return json.loads(line)


class Sink(metaclass=ABCMeta):

    @abstractmethod
    def write(self, message): #pragma: no cover
        raise NotImplementedError


class PipeSink(Sink):

    def __init__(self, pipe_fd):
        self._file = os.fdopen(pipe_fd, mode='wb')

    def write(self, message):
        try:
            self._file.write(json.dumps(message).encode())
            self._file.write(b'\n')
            self._file.flush()
        except OSError:
            raise EndpointClosedException()


class SocketSink(Sink):

    def __init__(self, sock):
        self._sock = sock

    def write(self, message):
        try:
            self._sock.send("{}\n".format(json.dumps(message)).encode())
        except BrokenPipeError:
            raise EndpointClosedException()


class Router(object):

    _sink_factory = None

    def __init__(self, default_sink):
        self._logger = logging.getLogger("router")
        self._faucets = {}
        self._rules = {}
        self._sinks = {}
        self._sinks[None] = default_sink

    def add_sink(self, sink, name):
        self._sinks[name] = sink

    def add_faucet(self, faucet, name):
        self._faucets[name] = faucet

    def add_rule(self, rule, faucet_name):
        if faucet_name not in self._rules:
            self._rules[faucet_name] = []
        self._rules[faucet_name].append(rule)
        self._rules[faucet_name] = sorted(self._rules[faucet_name],
                                          key=len,
                                          reverse=True)
        self._logger.debug("Added %s to faucet %s", rule, faucet_name)

    def add_sink_factory(self, factory):
        self._sink_factory = factory

    def tick(self):
        faucets = {**self._faucets}
        for faucet_name, faucet in faucets.items():
            while True:
                message = faucet.read()
                if message is None:
                    break
                self._logger.debug("message %s", message)
                dest = message.get("to", None)
                if dest is None and faucet_name in self._rules:
                    for rule in self._rules[faucet_name]:
                        dest = rule.target_for(message)
                        if dest is not None:
                            break
                if dest not in self._sinks and self._sink_factory is not None:
                    self._sink_factory(self, dest)
                if dest not in self._sinks:
                    dest = None
                self._sinks[dest].write(message)
                self._logger.debug("sent to %s", dest)

    def remove_faucet(self, faucet_or_name):
        if isinstance(faucet_or_name, str):
            name = faucet_or_name
        else:
            name = next(k for k, v in self._faucets.items() if v == faucet_or_name)
        del(self._faucets[name])

    def remove_sink(self, sink_or_name):
        if isinstance(sink_or_name, str):
            name = sink_or_name
        else:
            name = next(k for k, v in self._sinks.items() if v == sink_or_name)
        del(self._sinks[name])


class Rule(object):

    def __init__(self, target, **kwargs):
        self._target = target
        self._clauses = kwargs
        self._len = len(kwargs)

    def __len__(self):
        return self._len

    def target_for(self, message):
        source = message['from']
        if all(source.get(k) == v for k, v in self._clauses.items()):
            return self._target

    def __str__(self):
        return "Rule({}=>{})".format(str(self._clauses), self._target)
