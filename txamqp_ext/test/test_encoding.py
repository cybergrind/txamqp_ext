import cjson
import msgpack
from copy import copy
import cPickle

from twisted.internet.defer import Deferred, inlineCallbacks
from twisted.internet.defer import DeferredList
from twisted.trial.unittest import TestCase
from txamqp.content import Content

from txamqp_ext.factory import AmqpReconnectingFactory, AmqpSynFactory
from txamqp_ext.test import EXC, EQUE, RK, RK2, RK3



class TestEncoding(TestCase):
    timeout = 4

    def setUp(self):
        kwargs = {'spec': 'file:../txamqp_ext/spec/amqp0-8.xml',
                  'parallel': False,
                  'serialization': 'cPickle',
                  'full_content': True,
                  'skip_decoding': True}
        self.f = AmqpReconnectingFactory(self, **kwargs)

    def test_001_fail_encoding(self):
        d = Deferred()
        d1 = Deferred()

        def _failed(failure):
            failure.trap(TypeError)
            d.callback(True)

        self.f.send_message(EXC, RK, {1: self}, callback=d1).addErrback(_failed)
        return DeferredList([d, self.f.connected])

    @inlineCallbacks
    def test_002_skip_encoding(self):
        d = Deferred()
        d1 = Deferred()
        d2 = Deferred()

        encoded_body = cjson.encode({'test_message': 'asdf'})
        encoded = Content(encoded_body)
        encoded['content type'] = 'application/json'
        def _ok(_any):
            d.callback(True)

        def _ok_msg(_any):
            assert _any['content type'] == 'application/json', _any
            assert encoded_body == _any.body, 'Got %r'%_any.body
            d2.callback(True)

        def _err_msg(_any, msg):
            print 'Fail002: %r'%msg
            d2.errback(True)
            return {}

        yield self.f.setup_read_queue(EXC, RK, _ok_msg,
                                      queue_name=EQUE, durable=False,
                                      auto_delete=True,
                                      requeue_on_error=False,
                                      read_error_handler=_err_msg)
        yield self.f.client.on_read_loop_started()
        self.f.send_message(EXC, RK, encoded, callback=d1,
                            skip_encoding=True).addCallback(_ok)

        yield DeferredList([d, d2])

    @inlineCallbacks
    def test_003_set_content_type(self):
        d = Deferred()
        d1 = Deferred()
        d2 = Deferred()

        encoded = {'test_message': 'asdf'}
        def _ok(_any):
            d.callback(True)

        def _ok_msg(_any):
            assert _any['content type'] == 'application/x-pickle', _any
            assert encoded == cPickle.loads(_any.body), 'Got %r'%_any.body
            d2.callback(True)

        def _err_msg(_any, msg):
            print 'REJ: %r'%msg
            d2.errback(True)
            return {}

        yield self.f.setup_read_queue(EXC, RK, _ok_msg,
                                      queue_name=EQUE, durable=False,
                                      auto_delete=True,
                                      requeue_on_error=False,
                                      read_error_handler=_err_msg)
        yield self.f.client.on_read_loop_started()
        self.f.send_message(EXC, RK, encoded, callback=d1,
                            skip_encoding=False).addCallback(_ok)

        yield DeferredList([d, d2])

    @inlineCallbacks
    def test_004_msgpack(self):
        d = Deferred()
        d1 = Deferred()
        d2 = Deferred()

        msg_body = {'test_message': 'asdf'}
        encoded_body = msgpack.dumps(msg_body)
        encoded_msg = Content(encoded_body)
        encoded_msg['content type'] = 'application/x-msgpack'
        self.f.skip_decoding = False
        self.f.serialization = 'content_based'
        def _ok(_any):
            d.callback(True)

        def _ok_msg(_any):
            assert _any['content type'] == 'application/x-msgpack', _any
            assert msg_body == _any.body, 'Got %r'%_any.body
            d2.callback(True)

        def _err_msg(_any, msg):
            print 'Fail002: %r'%msg
            d2.errback(True)
            return {}

        yield self.f.setup_read_queue(EXC, RK, _ok_msg,
                                      queue_name=EQUE, durable=False,
                                      auto_delete=True,
                                      requeue_on_error=False,
                                      read_error_handler=_err_msg)
        yield self.f.client.on_read_loop_started()
        self.f.send_message(EXC, RK, encoded_msg, callback=d1,
                            skip_encoding=True).addCallback(_ok)

        yield DeferredList([d, d2])

    @inlineCallbacks
    def tearDown(self):
        if hasattr(self.f.protocol, 'read_chan'):
            yield self.f.protocol.read_chan.queue_delete(queue=EQUE)
        yield self.f.shutdown_factory()

class TestContentTypes(TestCase):
    timeout = 4

    def setUp(self):
        kwargs_fwdr = {'spec': 'file:../txamqp_ext/spec/amqp0-9-1.stripped.xml',
                   'parallel': True,
                   'full_content': True,
                   'skip_decoding': True,
                   'skip_encoding': True}
        self.forwarder = AmqpReconnectingFactory(self, **kwargs_fwdr)
        self.forwarder.setup_read_queue(EXC, RK2, self.fwdr_handle,
                                        queue_name='forwarder',  durable=False,
                                        auto_delete=True)

        self.backwarder = AmqpReconnectingFactory(self, **kwargs_fwdr)
        self.backwarder.setup_read_queue(EXC, RK3, self.fwdr_handle,
                                        queue_name='backwarder',  durable=False,
                                        auto_delete=True)

        kwargs_common = {'spec': 'file:../txamqp_ext/spec/amqp0-9-1.stripped.xml',
                         'parallel': True,
                         'exchange': EXC,
                         'serialization': 'content_based',
                         'rk': RK2,}
        kwargs_json = copy(kwargs_common)
        kwargs_json['default_content_type'] = 'application/json'
        self.push_json = AmqpSynFactory(self, **kwargs_json)
        self.push_json.setup_read_queue(EXC, 'route_json', durable=False, auto_delete=True)

        kwargs_pickle = copy(kwargs_common)
        kwargs_pickle['default_content_type'] = 'application/x-pickle'
        self.push_pickle = AmqpSynFactory(self, **kwargs_pickle)
        self.push_pickle.setup_read_queue(EXC, 'route_pickle', durable=False, auto_delete=True)

        kwargs_responder = {'spec': 'file:../txamqp_ext/spec/amqp0-9-1.stripped.xml',
                            'parallel': True,
                            'push_back': True,
                            'full_content': True,
                            'serialization': 'content_based',
                            'default_content_type': 'application/json'}
        self.responder = AmqpReconnectingFactory(self, **kwargs_responder)
        self.responder.setup_read_queue(EXC, RK, self.msg_handle,
                                        queue_name='responder', durable=False,
                                        auto_delete=True)

        return DeferredList([self.forwarder.connected, self.backwarder.connected,
                             self.push_json.connected, self.push_pickle.connected,
                             self.responder.connected])

    def tearDown(self):
        return DeferredList([self.forwarder.shutdown_factory(), self.backwarder.shutdown_factory(),
                             self.push_json.shutdown_factory(), self.push_pickle.shutdown_factory(),
                             self.responder.shutdown_factory()])

    @inlineCallbacks
    def msg_handle(self, full_msg, d):
        yield d.callback(full_msg)

    @inlineCallbacks
    def fwdr_handle(self, msg):
        real_back = msg['headers'].get('real_rb')
        if not real_back:
            msg['headers']['real_rb'] = msg['headers']['route_back']
            msg['headers']['route_back'] = RK3
            yield self.forwarder.send_message(EXC, RK, msg)
        else:
            yield self.backwarder.send_message(EXC, msg['headers']['real_rb'], msg)

    def test_0000(self):
        return

    def test_0001(self):
        d_json = Deferred()
        d_pickle = Deferred()

        @inlineCallbacks
        def run_json():
            yield self.push_json.push_message({'test_message': 1})
            d_json.callback(True)

        @inlineCallbacks
        def run_pickle():
            yield self.push_pickle.push_message({'test_message': 1})
            d_pickle.callback(True)

        run_json()
        run_pickle()
        return DeferredList([d_json, d_pickle])
