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
from txamqp_ext.test import EXC, QUE, RK


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
        self.read_queue = DeferredQueue()
        self.prefetch_count = 40
        self.processing_send = None
        self.parallel = True
        self.delivery_mode = 2
        self.tx_mode = False

        self.rq_enabled = True
        self.rq_exchange = EXC
        self.rq_name = QUE
        self.rq_rk = RK
        self.rq_exclusive = False
        self.rq_durable = True
        self.rq_auto_delete = False
        self.no_ack = True
        self.tid_name = 'tid'
        self.consumer_tag = 'test_tag'
        self.rb_name = 'route_back'
        self.rq_dynamic = False
        self.autodeclare = True
        self.autobind = True

        self.processing_send = None
        self.send_retries = 0

        self.serialization = 'cjson'
        self.default_content_type = 'application/json'
        self.skip_encoding = False
        self.skip_decoding = False
        self.content_type_name = 'content-type'
        self.delivery_mode_name = 'delivery-mode'

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
    timeout = 30
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
    Connected write tests
    '''
    timeout = 30
    def setUp(self):
        self.f = TestFactory()
        self.f.rq_enabled = False
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

    def test_003_test_tx_send(self):
        self.f.parallel = False
        NUM = 100
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
                   'callback': d,
                   'tx':True}
            self.f.send_queue.put(msg)
            dl.append(d)
        return DeferredList(dl).addCallback(_test_num_sended)

    def test_004_test_tx_parallel_send(self):
        self.f.parallel = True
        NUM = 100
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
                   'callback': d,
                   'tx':True}
            self.f.send_queue.put(msg)
            dl.append(d)
        return DeferredList(dl).addCallback(_test_num_sended)

    def tearDown(self):
        return self.f.client.shutdown_protocol()
        self.f.err_fail = False
        return self.f.close_transport()

class ProtocolC(TestCase):
    '''
    Connected read tests
    '''
    timeout = 30
    def setUp(self):
        self.f = TestFactory()
        self.f.rq_enabled = True
        self.f.delivery_mode = 2
        self.failError = True
        return self.f.connected

    def test_001_basic(self):
        '''
        test basic read loop started
        and clear messages from previous tests
        '''
        d2 = Deferred()
        def _read_started(_):
            d = self.f.client.on_read_loop_started()
            d.addErrback(self.f._err)
            return d
        d2.addCallback(_read_started)
        reactor.callLater(0.5, d2.callback, None)
        return d2

    def test_002_send_receive(self):
        NUM = 1000
        t = time.time()
        def _recv(msg, mess):
            print 'RECV: %r'%msg
            assert msg.body == mess, 'GOT: %r'%msg

        def _sended(_none):
            pass

        def _send(_none, send_s):
            d = Deferred()
            d.addCallback(_sended)
            d.addErrback(self.f._err)
            msg = {'exchange': EXC,
                   'rk': RK,
                   'content': send_s,
                   'callback': d}
            self.f.send_queue.put(msg)
        def _set_recv(_none, mess_s):
            d = self.f.read_queue.get()
            d.addErrback(_recv, mess_s)
            return d
        def _done(_none):
            print 'TIME: %.6f NUM: %r'%(time.time()-t, NUM)

        d = self.f.client.on_read_loop_started()
        for i in xrange(NUM):
            d.addCallback(_send, 'mess_%s'%i)
            d.addCallback(_set_recv, 'mess_%s'%i)
        d.addErrback(self.f._err)
        d.addCallback(_done)
        return d


    def tearDown(self):
        return self.f.client.shutdown_protocol()
