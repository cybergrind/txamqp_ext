import time

from twisted.trial.unittest import TestCase
from twisted.internet import reactor
from twisted.internet import protocol
from twisted.internet.defer import Deferred
from twisted.internet.defer import DeferredList
from twisted.internet.defer import DeferredQueue

from txamqp.client import TwistedDelegate
import txamqp.spec

from txamqp_ext.protocol import AmqpProtocol


EXC = 'test_exchange'
QUE = 'test_queue'
RK = 'test_route'

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
        self.send_queue = DeferredQueue()
        self.processing_send = None
        self.parallel = True
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


class ProtocolA(TestCase):
    '''
    Tests for connection
    '''
    def setUp(self):
        self.f = TestFactory()
        self.failError = True

    def test_001_connect(self):
        def _authed(*args):
            pass
        def _conn(c):
            d = c._auth_succ
            d.addCallback(_authed).addErrback(self.f._err)
            return d
        d = self.f.connected.addCallback(_conn).addErrback(self.f._err)
        return d

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
        
class ProtocolB(TestCase):
    '''
    Connected tests
    '''
    def setUp(self):
        self.f = TestFactory()
        self.failError = True
        return self.f.connected

    def test_001_test_define(self):
        def _defined_resp(resp):
            assert resp.method.name=='declare-ok', resp
        def _on_write_open(w_chan):
            d1 = w_chan.exchange_declare(exchange=EXC,
                                        type='topic',
                                        durable=True,
                                        auto_delete=False)
            d1.addCallback(_defined_resp)

            d2 = w_chan.queue_declare(queue=QUE,
                                      durable=True,
                                      auto_delete=False)
            d2.addCallback(_defined_resp)
            return DeferredList([d1, d2])
        d = self.f.client.on_write_channel_opened()
        d.addCallback(_on_write_open)
        return d

    def test_002_test_par_send(self):
        self.f.parallel = True
        NUM = 1000
        self.s = 0
        t = time.time()
        def _sended(res):
            self.s += 1
        def _test_num_sended(res):
            assert self.s == NUM, 'sended: %r'%self.s
            print 'TIME: %.4f NUM: %r'%(time.time()-t, NUM)
        def _send_def():
            return Deferred().addCallback(_sended).addErrback(self.f._err)
        dl = []
        for i in xrange(NUM):
            d = _send_def()
            msg = {'exchange': EXC,
                   'rk': RK,
                   'content': 'cnt',
                   'callback': d}
            self.f.send_queue.put(msg)
            dl.append(d)
        return DeferredList(dl).addCallback(_test_num_sended)

    def test_003_test_not_par_send(self):
        self.f.parallel = False
        NUM = 1000
        self.s = 0
        t = time.time()
        def _sended(res):
            self.s += 1
        def _test_num_sended(res):
            assert self.s == NUM, 'sended: %r'%self.s
            print 'TIME: %.4f NUM: %r'%(time.time()-t, NUM)
        def _send_def():
            return Deferred().addCallback(_sended).addErrback(self.f._err)
        dl = []
        for i in xrange(NUM):
            d = _send_def()
            msg = {'exchange': EXC,
                   'rk': RK,
                   'content': 'cnt',
                   'callback': d}
            self.f.send_queue.put(msg)
            dl.append(d)
        return DeferredList(dl).addCallback(_test_num_sended)

    def tearDown(self):
        self.f.err_fail = False
        return self.f.close_transport()
