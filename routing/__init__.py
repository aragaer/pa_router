import logging
import json

from abc import ABCMeta, abstractmethod


class AbstractFaucet(metaclass=ABCMeta):

    @abstractmethod
    def read(self): #pragma: no cover
        raise NotImplementedError

    @abstractmethod
    def close(self): #pragma: no cover
        raise NotImplementedError


class Faucet(AbstractFaucet):

    def __init__(self, channel):
        self._channel = channel
        self._buf = ""

    def read(self):
        pos = self._buf.find("\n")
        if pos == -1:
            data = self._channel.read()
            if not data:
                return
            self._buf += data.decode()
            pos = self._buf.find("\n")
        if pos == -1:
            return
        line, self._buf = self._buf[:pos], self._buf[pos+1:]
        return json.loads(line)

    def close(self):
        self._channel.close()


class AbstractSink(metaclass=ABCMeta):

    @abstractmethod
    def write(self, message): #pragma: no cover
        raise NotImplementedError

    @abstractmethod
    def close(self): #pragma: no cover
        raise NotImplementedError


class Sink(AbstractSink):

    def __init__(self, channel):
        self._channel = channel

    def write(self, message):
        self._channel.write(json.dumps(message).encode(), b'\n')

    def close(self):
        self._channel.close()


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
