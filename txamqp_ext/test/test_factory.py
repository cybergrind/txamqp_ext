
from twisted.internet import reactor
from twisted.internet.defer import Deferred
from twisted.internet.defer import DeferredList
from twisted.trial.unittest import TestCase

from txamqp.content import Content

from txamqp_ext.factory import AmqpReconnectingFactory
from txamqp_ext.test import EXC, QUE, RK


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
            reactor.callLater(30, d2.callback, None)
            d1 = self.f.client.shutdown_protocol()
            return DeferredList([d1, d2])
        d.addCallback(_sd)
        return d

    def _err(self, failure):
        print failure.getTraceback()

    def tearDown(self):
        return self.f.shutdown_factory()



