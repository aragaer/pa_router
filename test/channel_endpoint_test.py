import json
import unittest

from routing import Faucet, Sink
from runner.channel import EndpointClosedException, Channel


class TestChannel(Channel):

    def __init__(self):
        self.ins = []
        self.outs = []
        self.closed = False

    def read(self):
        if self.ins:
            return self.ins.pop()
        else:
            return b''

    def write(self, *data):
        self.outs.append(''.join([d.decode() for d in data]))

    def close(self):
        self.closed = True


class FaucetTest(unittest.TestCase):

    def test_read(self):
        channel = TestChannel()
        faucet = Faucet(channel)

        channel.ins.append(b'{"message":"test"}\n{"message":"second"}\n"')
        self.assertEqual(faucet.read(), {"message": "test"})
        self.assertEqual(faucet.read(), {"message": "second"})
        self.assertEqual(faucet.read(), None)

    def test_close(self):
        channel = TestChannel()
        faucet = Faucet(channel)

        faucet.close()

        self.assertTrue(channel.closed)


class SinkTest(unittest.TestCase):

    def test_write(self):
        channel = TestChannel()
        sink = Sink(channel)
        msg = {"message": "test"}

        sink.write(msg)

        self.assertEqual(channel.outs, [json.dumps(msg)+'\n'])

    def test_close(self):
        channel = TestChannel()
        sink = Sink(channel)

        sink.close()

        self.assertTrue(channel.closed)
