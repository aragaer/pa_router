import os
import shutil
import time
import unittest

from tempfile import mkdtemp, mkstemp

from routing import Faucet, Sink
from runner import Runner


class RunnerTest(unittest.TestCase):

    _runner = None

    def setUp(self):
        self._runner = Runner()

    def _load_config(self, config_text):
        config_fd, config_name = mkstemp()
        config = os.fdopen(config_fd, "w")
        config.write(config_text)
        config.close()
        self._runner.load(config_name)
        os.unlink(config_name)

    def test_cat(self):
        self._load_config("cat:\n"
                          "  command: cat\n"
                          "  type: stdio\n")
        self._runner.ensure_running('cat')

        sink = self._runner.get_sink('cat')
        faucet = self._runner.get_faucet('cat')

        self.assertTrue(isinstance(sink, Sink))
        sink.write({"message": "test"})

        self.assertTrue(isinstance(faucet, Faucet))
        line = None
        while line is None:
            time.sleep(0.1)
            line = faucet.read()
        self.assertEquals(line, {"message": "test"})

    def test_cat_socat(self):
        dirname = mkdtemp()
        self.addCleanup(lambda: shutil.rmtree(dirname))
        sockname = os.path.join(dirname, "socket")

        self._load_config("socat:\n"
                          "  command: socat SYSTEM:cat UNIX-LISTEN:{socketname}\n"
                          "  type: socket\n"
                          "  socket: {socketname}\n".format(socketname=sockname))

        self._runner.ensure_running('socat')

        self._runner.get_sink('socat').write({"message": "test"})
        faucet = self._runner.get_faucet('socat')
        for _ in range(10):
            line = faucet.read()
            if line is None:
                time.sleep(0.1)
            else:
                break
        self.assertEquals(line, {"message": "test"})
