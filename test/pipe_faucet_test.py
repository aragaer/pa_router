import os
import unittest

from routing import PipeFaucet


class PipeFaucetTest(unittest.TestCase):

    def test_create(self):
        faucet_fd, sink_fd = os.pipe2(os.O_NONBLOCK)

        faucet = PipeFaucet(faucet_fd)

        sink_file = os.fdopen(sink_fd, mode='wb')
        sink_file.write(b'{"message":"test"}\n')
        sink_file.flush()

        self.assertEquals(faucet.read(), {"message": "test"})
