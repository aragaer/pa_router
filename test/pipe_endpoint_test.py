import fcntl
import json
import os
import unittest

from routing import PipeFaucet, PipeSink


class PipeFaucetTest(unittest.TestCase):

    def test_read(self):
        faucet_fd, sink_fd = os.pipe()

        faucet = PipeFaucet(faucet_fd)

        sink_file = os.fdopen(sink_fd, mode='wb')
        sink_file.write(b'{"message":"test"}\n{"message":"second"}\n')
        sink_file.flush()

        self.assertEqual(faucet.read(), {"message": "test"})
        self.assertEqual(faucet.read(), {"message": "second"})
        self.assertEqual(faucet.read(), None)


class PipeSinkTest(unittest.TestCase):

    def test_create(self):
        faucet_fd, sink_fd = os.pipe()

        sink = PipeSink(sink_fd)

        sink.write({"message": "test"})

        faucet_file = os.fdopen(faucet_fd, mode='rb')
        fl = fcntl.fcntl(faucet_fd, fcntl.F_GETFL)
        fcntl.fcntl(faucet_fd, fcntl.F_SETFL, fl | os.O_NONBLOCK)
        line = faucet_file.readline()

        self.assertEqual(line, "{}\n".format(json.dumps({"message": "test"})).encode())
