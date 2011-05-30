
from twisted.internet import reactor
from twisted.internet.defer import Deferred
from twisted.internet.defer import DeferredList
from twisted.trial.unittest import TestCase

from txamqp_ext.factory import AmqpReconnectingFactory



class FabricA(TestCase):
    def setUp(self):
        self.f = AmqpReconnectingFactory()

    def test_001_basic_connect(self):
        return self.f.connected

    def _err(self, failure):
        print failure.getTraceback()

    def tearDown(self):
        return self.f.shutdown_factory()



