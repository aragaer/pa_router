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


class TestChoiceRule(Rule):

    def __init__(self, target):
        base = super(TestChoiceRule, self)
        base.__init__(target=target, media="telegram")
        self._len += 1

    def target_for(self, message):
        source = message['from']
        if source.get('media') == "telegram" and "user" in source:
            return self._target + source['user']


class RuleTest(unittest.TestCase):

    def test_create(self):
        rule = Rule(target="brain", media="test")

        message1 = {"from": {"media": "test"}, "message": "test"}
        message2 = {"from": {"media": "telegram", "user": "123456"}, "message": "test"}

        self.assertEqual(rule.target_for(message1), "brain")
        self.assertEqual(rule.target_for(message2), None)

    def test_len(self):
        self.assertEqual(len(Rule(target="", media="test")), 1)


class RouterTest(unittest.TestCase):

    _router = None
    _sink = None
    _faucet = None

    def setUp(self):
        self._sink = TestSink()
        self._router = Router(default_sink=self._sink)
        self._faucet = TestFaucet()
        self._router.add_faucet(self._faucet, name="test")

    def test_tick(self):
        self._router.tick()

    def test_default_sink(self):
        message = {"from": {"media": "test"}, "message": "test"}

        self._faucet.create_message(message)

        self._router.tick()

        self.assertEquals(self._sink.messages, [message])

    def test_no_message(self):
        faucet = TestFaucet()

        self._router.add_faucet(faucet, name="empty")

        self._router.tick()
        # most important is that this doesn't raise any exceptions

        self.assertEquals(self._sink.messages, [])

    def test_add_sink(self):
        rule = Rule(target="sink", media="telegram", user="123456")
        sink = TestSink()
        message1 = {"from": {"media": "test"}, "message": "test"}
        message2 = {"from": {"media": "telegram", "user": "123456"}, "message": "test"}
        message3 = {"from": {"media": "telegram", "user": "1234567"}, "message": "test"}
        message4 = {"from": {"media": "test"}, "to": "sink", "message": "test"}

        self._router.add_sink(sink, name="sink")

        self._router.add_rule(rule, faucet_name="test")
        for message in (message1, message2, message3, message4):
            self._faucet.create_message(message)
            self._router.tick()

        self.assertEqual(self._sink.messages, [message1, message3])
        self.assertEqual(sink.messages, [message2, message4])

    def test_message_with_to_but_sink_not_present(self):
        message = {"from": {"media": "test"}, "to": "inexistent", "message": "test"}

        self._faucet.create_message(message)
        self._router.tick()

        self.assertEqual(self._sink.messages, [message])

    def test_more_specific_rule_has_priority(self):
        rule1 = Rule(target="sink1", media="telegram", user="123456")
        rule2 = Rule(target="sink2", media="telegram")
        rule3 = Rule(target="sink3", media="telegram", user="654321")

        for rule in (rule1, rule2, rule3):
            self._router.add_rule(rule, faucet_name="test")

        sinks = []
        for name in ("sink1", "sink2", "sink3"):
            sinks.append(TestSink())
            self._router.add_sink(sinks[-1], name=name)

        message1 = {"from": {"media": "telegram", "user": "123456"}, "message": "test"}
        message2 = {"from": {"media": "telegram", "user": "000000"}, "message": "test"}
        message3 = {"from": {"media": "telegram", "user": "654321"}, "message": "test"}

        for message in (message1, message2, message3):
            self._faucet.create_message(message)
            self._router.tick()

        self.assertEqual(sinks[0].messages, [message1])
        self.assertEqual(sinks[1].messages, [message2])
        self.assertEqual(sinks[2].messages, [message3])

    def test_remove_faucet(self):
        self._router.remove_faucet("test")

        self._faucet.create_message({"from": {"media": "test"}, "message": "test"})
        self._router.tick()

        self.assertEqual(self._sink.messages, [])

    def test_remove_faucet_object(self):
        self._router.remove_faucet(self._faucet)

        self._faucet.create_message({"from": {"media": "test"}, "message": "test"})
        self._router.tick()

        self.assertEqual(self._sink.messages, [])

    def test_remove_sink(self):
        self._router.add_sink(TestSink(), "sink")
        self._router.add_rule(Rule(target="sink", media="test"),
                              faucet_name="test")

        self._router.remove_sink("sink")

        message = {"from": {"media": "test"}, "message": "test"}
        self._faucet.create_message(message)
        self._router.tick()

        self.assertEqual(self._sink.messages, [message])

    def test_remove_sink_object(self):
        sink = TestSink()
        self._router.add_sink(sink, "sink")
        self._router.add_rule(Rule(target="sink", media="test"),
                              faucet_name="test")

        self._router.remove_sink(sink)

        message = {"from": {"media": "test"}, "message": "test"}
        self._faucet.create_message(message)
        self._router.tick()

        self.assertEqual(self._sink.messages, [message])

    def test_choice_rule(self):
        sink1 = TestSink()
        sink2 = TestSink()

        self._router.add_sink(sink1, "sink1")
        self._router.add_sink(sink2, "sink2")

        self._router.add_rule(TestChoiceRule("sink"), faucet_name="test")

        message1 = {"from": {"media": "telegram", "user": "1"}, "message": "test"}
        message2 = {"from": {"media": "telegram", "user": "2"}, "message": "test"}
        message3 = {"from": {"media": "telegram", "user": "3"}, "message": "test"}

        for message in (message1, message2, message3):
            self._faucet.create_message(message)
            self._router.tick()

        self.assertEqual(sink1.messages, [message1])
        self.assertEqual(sink2.messages, [message2])
        self.assertEqual(self._sink.messages, [message3])

    def test_sink_factory(self):
        from unittest.mock import MagicMock
        self._router.add_rule(Rule(target="sink"), faucet_name="test")

        sink = TestSink()
        def callback(router, name):
            router.add_sink(sink, name)
            router.add_faucet(TestFaucet(), name)
        factory = MagicMock(side_effect=callback)

        self._router.add_sink_factory(factory)
        message = {"from": {"media": "test"}, "message": "test"}

        self._faucet.create_message(message)
        self._router.tick()

        self.assertEqual(self._sink.messages, [])
        self.assertEqual(sink.messages, [message])
        factory.assert_called_once_with(self._router, "sink")
