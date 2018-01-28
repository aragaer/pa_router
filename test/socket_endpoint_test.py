import json
import socket
import unittest

from routing import EndpointClosedException, ChannelSink, ChannelFaucet
from routing.channel import SocketChannel


class SocketFaucetTest(unittest.TestCase):

    def test_read(self):
        server, client = socket.socketpair()

        self.addCleanup(server.close)
        self.addCleanup(client.close)

        channel = SocketChannel(client)
        faucet = ChannelFaucet(channel)

        server.send(b'{"message": "test"}\n{"message": "post"}\n')

        self.assertEqual(faucet.read(), {"message": "test"})
        self.assertEqual(faucet.read(), {"message": "post"})
        self.assertEqual(faucet.read(), None)

    def test_closed(self):
        server, client = socket.socketpair()
        self.addCleanup(server.close)
        self.addCleanup(client.close)

        channel = SocketChannel(client)
        faucet = ChannelFaucet(channel)

        client.close()

        with self.assertRaises(EndpointClosedException):
            faucet.read()

    def test_close(self):
        server, client = socket.socketpair()
        self.addCleanup(server.close)
        self.addCleanup(client.close)

        channel = SocketChannel(client)
        faucet = ChannelFaucet(channel)

        faucet.close()

        with self.assertRaises(OSError) as ose:
            client.recv(1)
            self.assertEqual(ose.exception.error_code, 9)  # EBADF


class SocketSinkTest(unittest.TestCase):

    def test_write(self):
        server, client = socket.socketpair()
        server.setblocking(False)

        self.addCleanup(server.close)
        self.addCleanup(client.close)

        channel = SocketChannel(client)
        sink = ChannelSink(channel)

        sink.write({"message": "test"})

        line = server.recv(4096)
        self.assertEqual(line, "{}\n".format(json.dumps({"message": "test"})).encode())

    def test_closed(self):
        server, client = socket.socketpair()

        self.addCleanup(server.close)
        self.addCleanup(client.close)

        channel = SocketChannel(client)
        sink = ChannelSink(channel)

        server.close()

        with self.assertRaises(EndpointClosedException):
            sink.write({"message": "test"})

    def test_close(self):
        server, client = socket.socketpair()

        self.addCleanup(server.close)
        self.addCleanup(client.close)

        channel = SocketChannel(client)
        sink = ChannelSink(channel)

        sink.close()

        with self.assertRaises(OSError) as ose:
            client.recv(1)
            self.assertEqual(ose.exception.error_code, 9)  # EBADF


class SocketChannelTest(unittest.TestCase):

    _server = None
    _client = None
    _channel = None

    def setUp(self):
        self._server, self._client = socket.socketpair()
        self.addCleanup(self._server.close)
        self.addCleanup(self._client.close)
        self._channel = SocketChannel(self._client)

    def test_read(self):
        self._server.send(b'hello, world')

        self.assertEqual(self._channel.read(), b'hello, world')
        self.assertEqual(self._channel.read(), b'')
        self.assertEqual(self._channel.read(), b'')

    def test_write(self):
        self._channel.write(b'hello, world')

        self.assertEqual(self._server.recv(4096), b'hello, world')

    def test_write_list(self):
        self._channel.write(b'hello, ', b'world')

        self.assertEqual(self._server.recv(4096), b'hello, world')

    def test_closed_read(self):
        self._client.close()

        with self.assertRaises(EndpointClosedException):
            self._channel.read()

    def test_close_read(self):
        self._channel.close()

        with self.assertRaises(OSError) as ose:
            self._client.recv(1)
            self.assertEqual(ose.exception.error_code, 9)  # EBADF

    def test_closed_write(self):
        self._client.close()

        with self.assertRaises(EndpointClosedException):
            self._channel.write(b' ')

    def test_close_write(self):
        self._channel.close()

        with self.assertRaises(OSError) as ose:
            self._client.send(b' ')
            self.assertEqual(ose.exception.error_code, 9)  # EBADF
