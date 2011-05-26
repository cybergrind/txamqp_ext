
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
        self.connected = Deferred()
        self.connected.addErrback(self._err)
        self.err_fail = True
        reactor.connectTCP(self.host, self.port, self)

    def startedConnecting(self, connector):
        pass
        
    def _err(self, failure):
        if self.err_fail:
            print failure
            raise failure
        else:
            pass
    
    def buildProtocol(self, addr):
        try:
            p = self.protocol(self.delegate, self.vhost, self.spec)
            p._error = self._err
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
        self.failError = True

    def test_001_connect(self):
        def _conn(*args):
            d = self.f.client._auth_succ
            return d
        return self.f.connected.addCallback(_conn)

    def test_002_read_opened(self):
        def _on_connect(*args):
            d = self.f.client.on_read_channel_opened()
            return d
        ret = self.f.connected
        ret.addCallback(_on_connect)
        return ret

    def test_003_write_opened(self):
        def _on_connect(*args):
            d = self.f.client.on_write_channel_opened()
            return d
        ret = self.f.connected
        ret.addCallback(_on_connect)
        return ret

    def tearDown(self):
        self.f.err_fail = False
        return self.f.close_transport()
        
        
