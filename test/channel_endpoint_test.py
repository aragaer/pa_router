import json
import unittest

from routing import EndpointClosedException, ChannelFaucet, ChannelSink
from routing.channel import Channel


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

    def write(self, data):
        self.outs.append(data)

    def close(self):
        self.closed = True


class FaucetTest(unittest.TestCase):

    def test_read(self):
        channel = TestChannel()
        faucet = ChannelFaucet(channel)

        channel.ins.append(b'{"message":"test"}\n{"message":"second"}\n"')
        self.assertEqual(faucet.read(), {"message": "test"})
        self.assertEqual(faucet.read(), {"message": "second"})
        self.assertEqual(faucet.read(), None)

    def test_close(self):
        channel = TestChannel()
        faucet = ChannelFaucet(channel)

        faucet.close()

        self.assertTrue(channel.closed)


class SinkTest(unittest.TestCase):

    def test_write(self):
        channel = TestChannel()
        sink = ChannelSink(channel)
        msg = {"message": "test"}

        sink.write(msg)

        self.assertEqual(channel.outs, [json.dumps(msg).encode()])

    def test_close(self):
        channel = TestChannel()
        sink = ChannelSink(channel)

        sink.close()

        self.assertTrue(channel.closed)
