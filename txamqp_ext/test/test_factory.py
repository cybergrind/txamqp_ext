
import time

from twisted.internet import reactor
from twisted.internet.defer import Deferred
from twisted.internet.defer import DeferredList
from twisted.trial.unittest import TestCase

from txamqp.content import Content

from txamqp_ext.factory import AmqpReconnectingFactory
from txamqp_ext.factory import AmqpSynFactory
from txamqp_ext.test import EXC, QUE, RK, RK2, RK3


class FactoryA(TestCase):
    def setUp(self):
        kwargs = {'spec': 'file:../txamqp_ext/spec/amqp0-8.xml',
                  'parallel': False}
        self.f = AmqpReconnectingFactory(self, **kwargs)

    def test_001_basic_connect(self):
        return self.f.connected

    def test_002_reconnect_test(self):
        d = self.f.connected
        c = 'test_one'
        self.f.send_message(EXC, RK, c)

        def _analyze(_none):
            c = 'test_line'
            self.f.send_message(EXC, RK, c)
            print 'RETRIES: %r'%self.f.retries
            print 'CURR: %r'%self.f.processing_send
            assert len(self.f.dropped_send_messages.pending) == 1,\
                        'Dropped size: %r'%len(self.f.dropped_send_messages.pending)
        def p(_none):
            self.f.add_trap(self.f.client._trap_closed)
            c = Content({'body':'one'})
            self.f.send_message(EXC, RK, c, tx=True)
            dl = []
            for i in xrange(2):
                c = 'test_line'
                cb = Deferred()
                dl.append(cb)
                self.f.send_message(EXC, RK, c, callback=cb)
            return DeferredList(dl)
        return d.addCallback(p)

    def test_003_close_client(self):
        d = self.f.connected
        def _sd(_none):
            d2 = Deferred()
            reactor.callLater(5, d2.callback, None)
            d1 = self.f.client.shutdown_protocol()
            return DeferredList([d1, d2])
        d.addCallback(_sd)
        return d

    def _err(self, failure):
        print failure.getTraceback()

    def tearDown(self):
        return self.f.shutdown_factory()


class FactoryB(TestCase):
    def setUp(self):
        kwargs = {'spec': 'file:../txamqp_ext/spec/amqp0-8.xml',
                  'parallel': False}
        self.f = AmqpReconnectingFactory(self, **kwargs)

    def test_01_basic_setup_receive(self):
        def message_get(msg):
            print msg
        d = self.f.setup_read_queue(EXC, RK, message_get,
                                    queue_name=QUE,
                                    durable=True,
                                    auto_delete=False)
        return d

    def test_02_basic_send_and_receive(self):
        d1 = Deferred()
        txt = 'test_message'
        def message_get(msg):
            assert msg==txt, 'MSG: %r TXT: %r'%(msg, txt)
            d1.callback(msg)
        d = self.f.setup_read_queue(EXC, RK, message_get,
                                    no_ack=False,
                                    auto_delete=True,
                                    durable=False)
        def send_msg(_none):
            c = txt
            self.f.send_message(EXC, RK, c)
        def rloop_started(_none):
            d2 = self.f.client.on_read_loop_started()
            d2.addCallback(send_msg)
            return d2
        d.addCallback(rloop_started)
        return DeferredList([d, d1])

    def tearDown(self):
        return self.f.shutdown_factory()

class FactoryC(TestCase):
    def setUp(self):
        kwargs = {'spec': 'file:../txamqp_ext/spec/amqp0-8.xml',
                  'parallel': False,
                  'exchange': EXC,
                  'full_content': True,
                  'delivery_mode': 1,
                  'rk': RK2 }
        self.f = AmqpSynFactory(self, **kwargs)
        self.f.setup_read_queue(EXC, RK3,
                                durable=False,
                                auto_delete=True,
                                exclusive=True)
        self.f2 = AmqpReconnectingFactory(self, **kwargs)
        d = self.f2.setup_read_queue(EXC, RK2, self._test_echoer,
                                     durable=False,
                                     auto_delete=True,
                                     exclusive=True)
        return self.f.connected

    def _test_echoer(self, msg):
        c = Content(msg.body)
        c['headers'] = {'tid': msg['headers'].get('tid')}
        c['tid'] = msg['headers'].get('tid')
        #c['tid'] = msg['tid']
        self.f2.send_message(EXC, RK3, c, tid=msg['headers'].get('tid'))

    def test_01_basic_start(self):
        pass

    def test_02_send_rec(self):
        d1 = Deferred()
        def _get_result(result):
            print 'GOT RESULT: %r'%result
            d1.callback(True)
        d = self.f.push_message('test')
        d.addCallback(_get_result)
        return DeferredList([d1, d])

    def test_03_send_rec_many(self):
        d = {}
        def _get_result(result):
            tid = result['headers']['tid']
            assert result.body == tid
            d[tid].callback(True)
        for i in xrange(500):
            tid = str(int(time.time()*1e7))
            d[tid] = Deferred()
            d1 = self.f.push_message(tid, tid=tid)
            d1.addCallback(_get_result)
            d1.addErrback(self._err)
        return DeferredList(d.values())

    def _err(self, failure):
        raise failure

    def tearDown(self):
        dl = []
        dl.append(self.f.shutdown_factory())
        dl.append(self.f2.shutdown_factory())
        return DeferredList(dl)


