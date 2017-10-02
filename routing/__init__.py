import json
import logging


class Faucet(object):
    pass


class Sink(object):
    pass


class Router(object):

    def __init__(self, default_sink):
        self._logger = logging.getLogger("router")
        self._faucets = []
        self._sinks = []
        self._sinks.append((Rule(), default_sink))

    def add_sink(self, sink, rule):
        self._sinks.insert(0, (rule, sink))

    def add_faucet(self, faucet):
        self._faucets.append(faucet)

    def tick(self):
        for faucet in self._faucets:
            message = faucet.read()
            for rule, sink in self._sinks:
                if rule.matches(message):
                    sink.write(message)
                    break


class Rule(object):

    def __init__(self, **kwargs):
        self._clauses = kwargs

    def matches(self, message):
        source = message['from']
        return all(source.get(k) == v for k, v in self._clauses.items())
