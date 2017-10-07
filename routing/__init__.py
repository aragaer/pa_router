import json
import logging


class Faucet(object):
    pass


class Sink(object):
    pass


class Router(object):

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

    def tick(self):
        for faucet_name, faucet in self._faucets.items():
            message = faucet.read()
            if message is None:
                continue
            dest = message.get("to", None)
            if dest is None and faucet_name in self._rules:
                for rule in self._rules[faucet_name]:
                    if rule.matches(message):
                        dest = rule.target
                        break
            if dest not in self._sinks:
                dest = None
            self._sinks[dest].write(message)


class Rule(object):

    def __init__(self, target, **kwargs):
        self._target = target
        self._clauses = kwargs
        self._len = len(kwargs)

    def matches(self, message):
        source = message['from']
        return all(source.get(k) == v for k, v in self._clauses.items())

    @property
    def target(self):
        return self._target

    def __len__(self):
        return self._len
