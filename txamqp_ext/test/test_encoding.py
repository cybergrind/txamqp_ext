import time
from copy import copy

import cjson
from twisted.internet import reactor
from twisted.internet.defer import Deferred
from twisted.internet.defer import DeferredList
from twisted.trial.unittest import TestCase

from txamqp.content import Content

from txamqp_ext.factory import AmqpReconnectingFactory
from txamqp_ext.factory import AmqpSynFactory
from txamqp_ext.factory import SimpleListenFactory
from txamqp_ext.test import EXC, QUE, RK, RK2, RK3


class TestEncoding(TestCase):
    timeout = 3

    def setUp(self):
        kwargs = {'spec': 'file:../txamqp_ext/spec/amqp0-8.xml',
                  'parallel': False,
                  'serialization': 'cjson'}
        self.f = AmqpReconnectingFactory(self, **kwargs)
        return self.f.connected


    def test_001_fail_encoding(self):
        d = Deferred()
        d1 = Deferred()

        def _failed(failure):
            failure.trap(cjson.EncodeError)
            d.callback(True)

        self.f.send_message(EXC, RK, {1: self}, callback=d1).addErrback(_failed)
        return DeferredList([d])

    def test_002_skip_encoding(self):
        d = Deferred()
        d1 = Deferred()
        d2 = Deferred()

        def _ok(_any):
            d.callback(True)

        def _ok_msg(_any):
            pass
        def _err_msg(_any, msg):
            d2.callback(True)

        self.f.setup_read_queue(EXC, RK, _ok_msg,
                                queue_name=QUE, durable=True,
                                auto_delete=False,
                                requeue_on_error=False,
                                read_error_handler=_err_msg)
        self.f.send_message(EXC, RK, {1: self}, callback=d1,
                            skip_encoding=True).addCallback(_ok)

        return DeferredList([d, d2])

    def tearDown(self):
        return self.f.shutdown_factory()
                            
