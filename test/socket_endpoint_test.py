import json
import socket
import unittest

from routing import EndpointClosedException, SocketFaucet, SocketSink


class SocketFaucetTest(unittest.TestCase):

    def test_read(self):
        server, client = socket.socketpair()

        self.addCleanup(server.close)
        self.addCleanup(client.close)

        faucet = SocketFaucet(client)

        server.send(b'{"message": "test"}\n{"message": "post"}\n')

        self.assertEqual(faucet.read(), {"message": "test"})
        self.assertEqual(faucet.read(), {"message": "post"})
        self.assertEqual(faucet.read(), None)

    def test_closed(self):
        server, client = socket.socketpair()

        self.addCleanup(server.close)
        self.addCleanup(client.close)

        faucet = SocketFaucet(client)

        server.close()

        with self.assertRaises(EndpointClosedException):
            faucet.read()


class SocketSinkTest(unittest.TestCase):

    def test_create(self):
        server, client = socket.socketpair()
        server.setblocking(False)

        self.addCleanup(server.close)
        self.addCleanup(client.close)

        sink = SocketSink(client)

        sink.write({"message": "test"})

        line = server.recv(4096)
        self.assertEqual(line, "{}\n".format(json.dumps({"message": "test"})).encode())

    def test_closed(self):
        server, client = socket.socketpair()

        self.addCleanup(server.close)
        self.addCleanup(client.close)

        sink = SocketSink(client)

        server.close()

        with self.assertRaises(EndpointClosedException):
            sink.write({"message": "test"})
