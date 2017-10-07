import json
import os

from abc import abstractmethod


class Faucet(object):

    @abstractmethod
    def read(self):
        return None


class PipeFaucet(Faucet):

    def __init__(self, pipe_fd):
        self._file = os.fdopen(pipe_fd, mode='rb')

    def read(self):
        return json.loads(self._file.readline().decode())


class Sink(object):

    @abstractmethod
    def write(self, message):
        pass


class Router(object):

    def __init__(self, default_sink):
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

    def tick(self):
        for faucet_name, faucet in self._faucets.items():
            message = faucet.read()
            if message is None:
                continue
            dest = message.get("to", None)
            if dest is None and faucet_name in self._rules:
                for rule in self._rules[faucet_name]:
                    dest = rule.target_for(message)
                    if dest is not None:
                        break
            if dest not in self._sinks:
                dest = None
            self._sinks[dest].write(message)

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
