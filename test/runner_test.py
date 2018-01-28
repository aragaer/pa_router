import os
import shutil
import time
import unittest

from tempfile import mkdtemp, mkstemp

from routing import Faucet, Sink, EndpointClosedException
from routing.runner import Runner


class RunnerTest(unittest.TestCase):

    _runner = None

    def setUp(self):
        self._runner = Runner()

    @staticmethod
    def _readline(faucet):
        for _ in range(10):
            time.sleep(0.001)
            line = faucet.read()
            if line is not None:
                return line

    def test_cat(self):
        self._runner.update_config({"cat": {"command": "cat", "type": "stdio"}})
        self._runner.ensure_running('cat')

        sink = self._runner.get_sink('cat')
        faucet = self._runner.get_faucet('cat')

        self.assertTrue(isinstance(sink, Sink))
        sink.write({"message": "test"})

        self.assertTrue(isinstance(faucet, Faucet))
        self.assertEquals(self._readline(faucet), {"message": "test"})

    def test_cat_socat(self):
        dirname = mkdtemp()
        self.addCleanup(lambda: shutil.rmtree(dirname))
        sockname = os.path.join(dirname, "socket")

        self._runner.update_config({"socat":
                                    {"command": "socat SYSTEM:cat UNIX-LISTEN:"+sockname,
                                     "type": "socket",
                                     "socket": sockname}})

        self._runner.ensure_running('socat')

        self._runner.get_sink('socat').write({"message": "test"})
        faucet = self._runner.get_faucet('socat')
        self.assertEquals(self._readline(faucet), {"message": "test"})

    def test_cwd(self):
        dirname = mkdtemp()
        self.addCleanup(lambda: shutil.rmtree(dirname))
        with open(os.path.join(dirname, "file"), "w") as file:
            file.write('{"message": "test"}\n')

        self._runner.update_config({"cat":
                                    {"command": "cat file",
                                     "type": "stdio",
                                     "cwd": dirname}})

        self._runner.ensure_running('cat')

        faucet = self._runner.get_faucet('cat')
        self.assertTrue(isinstance(faucet, Faucet))
        self.assertEquals(self._readline(faucet), {"message": "test"})

    def test_alias(self):
        self._runner.update_config({"cat": {"command": "cat", "type": "stdio"}})
        self._runner.ensure_running('cat', alias='cat0')

        sink = self._runner.get_sink('cat0')
        faucet = self._runner.get_faucet('cat0')

        sink.write({"message": "test"})
        self.assertEquals(self._readline(faucet), {"message": "test"})

    def test_extra_args(self):
        dirname = mkdtemp()
        self.addCleanup(lambda: shutil.rmtree(dirname))
        with open(os.path.join(dirname, "file1"), "w") as file:
            file.write('{"message": "test1"}\n')
        with open(os.path.join(dirname, "file2"), "w") as file:
            file.write('{"message": "test2"}\n')

        self._runner.update_config({"cat":
                                    {"command": "cat",
                                     "type": "stdio",
                                     "cwd": dirname}})

        self._runner.ensure_running('cat', alias="cat1", with_args=['file1'])
        self._runner.ensure_running('cat', alias="cat2", with_args=['file2'])

        faucet1 = self._runner.get_faucet('cat1')
        self.assertEquals(self._readline(faucet1), {"message": "test1"})
        faucet2 = self._runner.get_faucet('cat2')
        self.assertEquals(self._readline(faucet2), {"message": "test2"})

    def test_terminate(self):
        self._runner.update_config({"cat": {"command": "cat", "type": "stdio"}})
        self._runner.ensure_running('cat')

        self._runner.terminate('cat')

        with self.assertRaises(EndpointClosedException):
            self._runner.get_faucet('cat').read()

        with self.assertRaises(EndpointClosedException):
            self._runner.get_sink('cat').write("")

    def test_terminate_socket(self):
        dirname = mkdtemp()
        self.addCleanup(lambda: shutil.rmtree(dirname))
        sockname = os.path.join(dirname, "socket")

        self._runner.update_config({"socat":
                                    {"command": "socat SYSTEM:cat UNIX-LISTEN:"+sockname,
                                     "type": "socket",
                                     "socket": sockname}})

        self._runner.ensure_running('socat')

        self._runner.terminate('socat')

        with self.assertRaises(EndpointClosedException):
            self._runner.get_faucet('socat').read()

        with self.assertRaises(EndpointClosedException):
            self._runner.get_sink('socat').write("")

    def test_socket_arg(self):
        dirname = mkdtemp()
        self.addCleanup(lambda: shutil.rmtree(dirname))
        sockname = os.path.join(dirname, "socket")
        with open(sockname, "w") as file:
            file.write("")

        self._runner.update_config({"socat":
                                    {"command": "socat SYSTEM:cat UNIX-LISTEN:"+sockname,
                                     "type": "socket",
                                     "cwd": dirname}})

        self._runner.ensure_running('socat', socket=sockname)

        self._runner.get_sink('socat').write({"message": "test"})
        faucet = self._runner.get_faucet('socat')
        self.assertEquals(self._readline(faucet), {"message": "test"})

    def test_config_from_file(self):
        config_fd, config_name = mkstemp()
        config = os.fdopen(config_fd, "w")
        config.write("cat:\n"
                     "  command: cat\n"
                     "  type: stdio\n")
        config.close()
        self._runner.load(config_name)
        os.unlink(config_name)

        self._runner.ensure_running('cat')

        sink = self._runner.get_sink('cat')
        faucet = self._runner.get_faucet('cat')

        self.assertTrue(isinstance(sink, Sink))
        sink.write({"message": "test"})

        self.assertTrue(isinstance(faucet, Faucet))
        self.assertEquals(self._readline(faucet), {"message": "test"})
