
from twisted.trial.unittest import TestCase
from twisted.internet import reactor
from twisted.internet import protocol
from twisted.internet.defer import Deferred
from twisted.internet.defer import DeferredList

from txamqp.client import TwistedDelegate
import txamqp.spec

from txamqp_ext.protocol import AmqpProtocol


class TestFactory(protocol.ClientFactory):
    protocol = AmqpProtocol
    def __init__(self):
        self.user = 'guest'
        self.password = 'guest'
        self.host = 'localhost'
        self.vhost = '/'
        self.port = 5672
        self.delegate = TwistedDelegate()
        self.spec = txamqp.spec.load('file:../txamqp_ext/spec/amqp0-8.xml')
        self.deferred = Deferred()
        self.deferred.addErrback(self._err)
        reactor.connectTCP(self.host, self.port, self)

    def startedConnecting(self, connector):
        print 'started connecting'
        
    def _err(self, failure):
        print failure
        raise failure
    
    def buildProtocol(self, addr):
        try:
            p = self.protocol(self.delegate, self.vhost, self.spec)
        except Exception, mess:
            print mess
        self.client = p
        self.client.factory = self
        return p

    def clientConnectionFailed(self, connector, reason):
        raise Exception, 'Connection failed due: %r'%reason

    def clientConnectionLost(self, connector, reason):
        pass

    def close_transport(self):
        return self.client.transport.loseConnection()


class ProtocolTest(TestCase):
    def setUp(self):
        self.f = TestFactory()

    def test_001_connect(self):
        return self.f.deferred

    def tearDown(self):
        return self.f.close_transport()
        
        
