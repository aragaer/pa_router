import unittest

from routing import Faucet, Router, Rule, Sink


class TestSink(Sink):

    def __init__(self):
        self._messages = []

    def write(self, message):
        self._messages.append(message)

    @property
    def messages(self):
        return self._messages


class TestFaucet(Faucet):

    def __init__(self):
        self._messages = []

    def create_message(self, message):
        self._messages.append(message)

    def read(self):
        try:
            return self._messages.pop(0)
        except IndexError:
            return None


class RuleTest(unittest.TestCase):

    def test_create(self):
        rule = Rule(media="test")

        message1 = {"from": {"media": "test"}, "message": "test"}
        message2 = {"from": {"media": "telegram", "user": "123456"}, "message": "test"}

        self.assertTrue(rule.matches(message1))
        self.assertFalse(rule.matches(message2))


class RouterTest(unittest.TestCase):

    _router = None
    _sink = None

    def setUp(self):
        self._sink = TestSink()
        self._router = Router(default_sink=self._sink)

    def test_tick(self):
        self._router.tick()

    def test_add_faucet(self):
        faucet = TestFaucet()
        message = {"from": {"media": "test"}, "message": "test"}

        self._router.add_faucet(faucet)
        faucet.create_message(message)

        self._router.tick()

        self.assertEquals(self._sink.messages, [message])

    def test_add_sink(self):
        rule = Rule(media="telegram", user="123456")
        sink = TestSink()
        faucet = TestFaucet()
        message1 = {"from": {"media": "test"}, "message": "test"}
        message2 = {"from": {"media": "telegram", "user": "123456"}, "message": "test"}
        message3 = {"from": {"media": "telegram", "user": "1234567"}, "message": "test"}

        self._router.add_sink(sink, rule)

        self._router.add_faucet(faucet)
        for message in (message1, message2, message3):
            faucet.create_message(message)
            self._router.tick()

        self.assertEqual(self._sink.messages, [message1, message3])
        self.assertEqual(sink.messages, [message2])

    def test_no_message(self):
        self._router.add_faucet(TestFaucet())

        self._router.tick()

        self.assertEqual(self._sink.messages, [])
