import os
import logging
import time
import cPickle

try:
    import cjson
    json_decode, json_encode = cjson.decode, cjson.encode
except ImportError:
    import json
    json_decode, json_encode = json.loads, json.dumps

import msgpack
msgpack_decode, msgpack_encode = msgpack.loads, msgpack.dumps

from twisted.internet import reactor
from twisted.internet import protocol
from twisted.internet.defer import Deferred
from twisted.internet.defer import DeferredList
from twisted.internet.defer import DeferredQueue
from twisted.internet.defer import maybeDeferred

import txamqp
import txamqp.spec
from txamqp.client import TwistedDelegate
from txamqp.content import Content

from txamqp_ext.protocol import AmqpProtocol


NO_REPLY = 'no_reply'
CONTENT_BASED = 'content_based'

class AmqpReconnectingFactory(protocol.ReconnectingClientFactory):
    protocol = AmqpProtocol
    log = logging.getLogger('AmqpReconnectingFactory')

    def __init__(self, parent, **kwargs):
        #self.initialDelay = 15
        #self.delay = 15
        #self.jitter = 0.8
        self.parent = parent
        self.host = kwargs.get('host', 'localhost')
        self.port = kwargs.get('port', 5672)
        self.user = kwargs.get('user', 'guest')
        self.password = kwargs.get('password', 'guest')
        spec_file = kwargs.get('spec', './txamqp_ext/spec/amqp0-8.xml')
        self.spec = txamqp.spec.load(spec_file)
        self.vhost = kwargs.get('vhost', '/')
        self.delegate = TwistedDelegate()
        # parallel sending and receiving messages
        self.parallel = kwargs.get('parallel', True)
        # default delivery_mode for messages
        self.delivery_mode = kwargs.get('delivery_mode', 1)
        # reconnect when lost connection
        self.continueTrying = kwargs.get('reconnect', True)
        # consumer tag for read queue will be queue_name if not defined
        self.consumer_tag = kwargs.get('tag', '')
        # transaction mode
        self.tx_mode = kwargs.get('transaction', False)
        # tid name. write this in header of message
        self.tid_name = kwargs.get('tid_name', 'tid')
        # return Content instead string or dict
        self.full_content = kwargs.get('full_content', False)
        # serialization
        self.serialization = kwargs.get('serialization', 'cjson')
        self.default_content_type = kwargs.get('default_content_type', 'application/json')
        # return deferred that will send reply
        self.push_back = kwargs.get('push_back', False)
        self.prefetch_count = kwargs.get('prefetch_count', 50)
        # route back name
        self.rb_name = kwargs.get('rb_name', 'route_back')
        # traps for catch errors from protocol
        self._traps = []

        # skip_encoding
        self.skip_encoding = kwargs.get('skip_encoding', False)
        # skip_decoding
        self.skip_decoding = kwargs.get('skip_decoding', False)

        self.rq_enabled = False
        self.rq_dynamic = False
        self.read_queue = DeferredQueue()
        self.send_queue = DeferredQueue()
        self.dropped_send_messages = DeferredQueue()
        self.processing_send = None
        self._read = set()
        self.send_retries = 0
        self.max_send_retries = 2
        # timeout for send message
        self.send_timeout = kwargs.get('send_timeout', 60)
        self.client = None

        # read delayed call. need for stop parallel read
        self._read_dc = None

        self._stopping = False
        self.init_deferreds()
        self.do_on_connect = []

        if self.spec.major == 8 and self.spec.minor == 0:
            self.content_type_name = 'content type'
            self.delivery_mode_name = 'delivery mode'
        else:
            self.content_type_name = 'content-type'
            self.delivery_mode_name = 'delivery-mode'
        reactor.connectTCP(self.host, self.port, self)

    def init_deferreds(self):
        self.connected = Deferred()
        def _run_on_connect(protocol):
            return DeferredList(map(lambda x: x(protocol), self.do_on_connect))
        self.connected.addCallback(_run_on_connect)
        self.connected.addErrback(self._error)

    def _error(self, failure):
        '''
        default error handler
        '''
        self.log.error(failure.getTraceback())
        raise failure

    def add_trap(self, trap):
        '''
        add traps for errors
        '''
        self._traps.append(trap)

    def declare(self, items):
        '''
        declare things, exchanges, queues, bindings
        example:
        {'type': 'queue', 'kwargs': {'exhchange': 'exchange_name',
                                     'durable': True,
                                     'auto_delete': False,
                                     'type': 'topic'}}
        '''
        def _declared(_none):
            self.log.debug('Declared')
        def _declare(d):
            if d['type'] in ('queue', 'exchange'):
                return getattr(self.client.write_chan, '%s_declare'%d['type'])(**d['kwargs'])
            elif d['type'] == 'binding':
                return getattr(self.client.write_chan, 'queue_bind')(**d['kwargs'])
        def _connected(_none, *args, **kwargs):
            if type(items) == list:
                d = map(_declare, items)
                return DeferredList(d).addCallbacks(_declared, self._error)
            else:
                return _declare(items).addCallbacks(_declared, self._error)
        self.do_on_connect.insert(0, _connected)
        if self.connected.called:
            self.connected.addCallback(_connected)
        return self.connected

    def change_rq_name(self):
        self.rq_name = '%s_%r_%s_%s_read_queue'%(
            self.parent.__class__.__name__,
            time.time(),
            os.getpid(),
            hex(hash(self.parent))[-4:])
        if self.rq_dynamic_route:
            self.rq_rk = 'route_back.%s.%r'%(self.parent.__class__.__name__,
                                             time.time())

    def setup_read_queue(self, exchange, routing_key=None, callback=None,
                         queue_name=None, exclusive=False,
                         durable=False, auto_delete=True,
                         no_ack=True, requeue_on_error=True,
                         requeue_timeout=120,
                         read_error_handler=None,
                         autodeclare=True, autobind=True):
        '''
        if you need read queue support, you should call this method
        '''
        self.autodeclare = autodeclare
        self.autobind = autobind
        self.rq_enabled = True
        self.rq_exchange = exchange
        if routing_key:
            self.rq_rk = routing_key
            self.rq_dynamic_route = False
        else:
            self.rq_dynamic_route = True
        if queue_name:
            self.rq_name = queue_name
        else:
            self.rq_dynamic = True
            self.change_rq_name()

        self.rq_exclusive = exclusive
        self.rq_durable = durable
        self.rq_auto_delete = auto_delete
        self.no_ack = no_ack
        self.rq_callback = callback
        self.requeue_on_error = requeue_on_error
        self.requeue_timeout = requeue_timeout
        self.read_error_handler = read_error_handler
        if not self.consumer_tag:
            self.consumer_tag = self.rq_name
        def _add_cb(_none):
            ret = self.client.on_read_loop_started()
            ret.addCallback(self.read_message_loop)
            ret.addErrback(self._error)
            return ret
        self.do_on_connect.append(lambda x: _add_cb(x).addErrback(self._error))
        if self.connected.called:
            self.connected.addCallback(lambda x: _add_cb(x).addErrback(self._error))
            self.client.start_read_loop()
        return self.connected

    def buildProtocol(self, addr):
        self.log.debug('BUILD PROTOCOL')
        p = self.protocol(self.delegate, self.vhost, self.spec)
        self.client = p
        self.client.factory = self
        self.resetDelay()
        return p

    def clientConnectionFailed(self, connector, reason):
        self.init_deferreds()
        self.log.error('Connection failed: %r [%s@%s:%s%s]'%(reason,
                                                             self.user,
                                                             self.host,
                                                             self.port,
                                                             self.vhost))
        if self.client:
            self.client.shutdown_protocol()
            self.client = None
            if hasattr(self, 'rq_dynamic') and self.rq_dynamic:
                self.change_rq_name()
        protocol.ReconnectingClientFactory\
                .clientConnectionFailed(self, connector, reason)

    def clientConnectionLost(self, connector, reason):
        '''
        amqp errors cause connectionLost
        '''
        self.init_deferreds()
        if not self._stopping:
            self.log.error('Connection lost: %r [%s@%s:%s%s]'%(reason,
                                                               self.user,
                                                               self.host,
                                                               self.port,
                                                               self.vhost))
        if self.client:
            self.client.shutdown_protocol()
            self.client = None
            if hasattr(self, 'rq_dynamic') and self.rq_dynamic:
                self.change_rq_name()
        protocol.ReconnectingClientFactory\
                .clientConnectionLost(self, connector, reason)

    encode_map = {'application/x-pickle': cPickle.dumps,
                  'application/json': json_encode,
                  'application/x-msgpack': msgpack_decode,
                  'text/plain': lambda x: x if isinstance(x, basestring) else str(x),
                  '': lambda x: x if isinstance(x, basestring) else str(x)}

    def encode_message(self, msg, skip_encoding=False):
        '''
        default method for encode amqp message body
        '''
        if type(msg) == Content:
            msg_body = msg.body
        else:
            msg_body = msg
        if self.skip_encoding or skip_encoding:
            encoded = msg_body
        elif self.serialization == 'cjson':
            encoded = json_encode(msg_body)
        elif self.serialization == 'cPickle':
            encoded = cPickle.dumps(msg_body)
        elif self.serialization == 'msgpack':
            encoded = msgpack_encode(msg_body)
        elif self.serialization == 'content_based':
            if isinstance(msg, Content):
                if msg.properties.get(self.content_type_name):
                    encoded = self.encode_map[msg[self.content_type_name]](msg.body)
                else:
                    encoded = self.encode_map[self.default_content_type](msg.body)
            else:
                content_type = self.default_content_type
                encoded = self.encode_map[content_type](msg)
        else:
            encoded = str(msg_body)
        if type(msg) == Content:
            msg.body = encoded
        else:
            msg = Content(encoded)
        return msg

    decode_map = {'application/x-pickle': cPickle.loads,
                  'application/json': json_decode,
                  'application/x-msgpack': msgpack_decode,
                  'text/plain': lambda x: x,
                  '': lambda x: x}
    def decode_message(self, msg):
        # msg really just msg.content.body
        if self.skip_decoding:
            return
        elif self.serialization == 'cjson':
            msg.content.body = json_decode(msg.content.body)
        elif self.serialization == 'cPickle':
            msg.content.body = cPickle.loads(msg.content.body)
        elif self.serialization == 'msgpack':
            msg.content.bogy = msgpack_encode(msg.content.body)
        elif self.serialization == 'content_based':
            dec_func = self.decode_map.get(msg.content.properties.get(self.content_type_name, ''))
            msg.content.body = dec_func(msg.content.body)

    def send_message(self, exchange, routing_key, msg, **kwargs):
        '''
        basic method for send message to amqp broker
        kwargs parameters is:
          @tx <bool> send message in transaction
          @callback <deferred> callback that will called after sending
        '''
        def _check_send_timeout(cb):
            # call if send was fail
            if not cb.called:
                self.log.info("Message sending timeout. Raise error %r"%cb)
                cb.errback(Exception('send_timeout'))
            else:
                self.log.debug("Message cb is called: %r -> %r"%(cb, cb.called))
        def _remove_timeout(res, d):
            # remove timeout task
            # self.log.debug("Call remove timeout from %r res %r"%(res, d))
            try:
                if d.active():
                    d.cancel()
            except Exception, mess:
                self.log.exception("Removing timeout cause error: %r"%mess)
            return res
        def _sending():
            if 'callback' in kwargs:
                callback = kwargs['callback']
            else:
                callback = Deferred()
            if 'skip_encoding' in kwargs:
                skip_encoding = kwargs['skip_encoding']
            else:
                skip_encoding = self.skip_encoding
            msg_send = self.encode_message(msg, skip_encoding)
            msg_dict = {'exchange': exchange,
                        'rk': routing_key,
                        'content': msg_send,
                        'callback': callback
                        }
            msg_dict.update(kwargs)
            # self.log.debug('Send msg: %r'%msg)
            to = reactor.callLater(self.send_timeout, _check_send_timeout, callback)
            callback.addCallback(_remove_timeout, to)
            self.send_queue.put(msg_dict)
            return callback
        ret = maybeDeferred(_sending)
        return ret

    def wrap_back(self, msg):
        '''
        wrap message, to support back messages
        we get some info from message headers (tid and route_back key)
        and send reply with this data
        '''
        d = Deferred()
        def _push_message(reply):
            if reply == 'no_reply':
                d1 = Deferred()
                d1.callback(True)
                if not self.no_ack:
                    self.client.basic_ack(msg)
                return d1
            route = msg.content['headers'].get(self.rb_name)
            tid = msg.content['headers'].get(self.tid_name)
            d1 = self.send_message(self.rq_exchange, route, reply,
                                   tid=tid)
            d1.addErrback(self._error)
            if not self.no_ack:
                self.client.basic_ack(msg)
            return d1
        def _read_new(_none):
            if not self.parallel and not self._stopping:
                reactor.callLater(0, self.read_message_loop)
        d.addCallback(_push_message)
        if not self.parallel:
            d.addCallback(_read_new)
        d.addErrback(self._error)
        return d

    def read_message_loop(self, *args):
        '''
        default message read method
        '''
        msg = self.read_queue.get()

        def _get_msg(msg):
            if self.parallel and not self._stopping:
                dc = reactor.callLater(0, self.read_message_loop)
                self._read_dc = dc

            # Will change body here!
            self.decode_message(msg)

            if not self.full_content:
                msg_out = msg.content.body
            else:
                msg_out = msg.content

            def _check_ack(*any):
                if not self.push_back and not self.no_ack:
                    self.log.debug('Ack message')
                    self.client.basic_ack(msg)
                if not self.parallel and not self._stopping:
                        reactor.callLater(0, self.read_message_loop)
            def _errr(failure):
                if not self.read_error_handler:
                    reactor.callLater(self.requeue_timeout,
                                  self.client.basic_reject,
                                  msg,
                                  requeue=self.requeue_on_error)
                    self.log.info('No ack message: %r'%failure.getTraceback())
                    read_new_message = True
                else:
                    try:
                        err_resp = self.read_error_handler(failure, msg)
                    except Exception, mess:
                        self.log.exception('During run read_error_handler: ')
                        raise Exception(mess)
                    if not isinstance(err_resp, dict):
                        err_resp = {}
                    requeue_timeout = err_resp.get('requeue_timeout',
                                                   self.requeue_timeout)
                    requeue_on_error = err_resp.get('requeue_on_error',
                                                    self.requeue_on_error)
                    read_new_message = err_resp.get('read_new_message',
                                                    True)
                    if not requeue_on_error:
                        self.log.debug('Drop message in %s seconds'%requeue_timeout)
                    else:
                        self.log.debug('Will requeue message in %s seconds'%requeue_timeout)
                    reactor.callLater(requeue_timeout,
                                      self.client.basic_reject,
                                      msg,
                                      requeue=requeue_on_error)
                if not self.parallel and not self._stopping:
                    if read_new_message:
                        reactor.callLater(0, self.read_message_loop)
                    else:
                        self.log.warning('Stop consuming')
            def __remove_rd(_any, _def):
                if _def in self._read:
                    self._read.remove(_def)
                return _any
            def __remove_rd_err(failure, _def):
                if _def in self._read:
                    self._read.remove(_def)
                raise failure
            if callable(self.rq_callback):
                if not self.push_back:
                    rd = maybeDeferred(self.rq_callback, msg_out)
                    self._read.add(rd)
                    rd.addCallback(__remove_rd, rd)
                    rd.addErrback(__remove_rd_err, rd)
                    rd.addCallbacks(_check_ack, _errr)
                else:
                    d = self.wrap_back(msg)
                    maybeDeferred(self.rq_callback, msg_out, d).addCallbacks(_check_ack, _errr)
        msg.addCallback(_get_msg)
        msg.addErrback(self._error)
        if self.read_error_handler:
            msg.addErrback(self.read_error_handler, msg)

    def shutdown_factory(self):
        '''
        shutdown factory in correct way
        '''
        self.log.debug('Shutdown factory')
        self._stopping = True
        if self._read_dc and not self._read_dc.called:
            self._read_dc.cancel()
        self.stopTrying()
        if self.client:
            self.log.debug('Going shutdown protocol')
            return self.client.shutdown_protocol()

class TimeoutException(Exception):
    pass

class AmqpSynFactory(AmqpReconnectingFactory):
    '''
    This factory implement non-blocking synchronous calls
    '''
    def __init__(self, parent, **kwargs):
        self.push_dict = {}
        self.push_exchange = kwargs['exchange']
        self.push_rk = kwargs['rk']
        self.default_timeout = kwargs.get('timeout', 5)
        self._timeout_calls = {}
        self.def_full_content = kwargs.get('full_content', False)
        AmqpReconnectingFactory.__init__(self, parent, **kwargs)
        self.full_content = True
        self.push_timeout_msg = None

    def setup_push(self, exchange, rk, timeout=None, timeout_msg=None):
        '''
        setup push exchange and routing key
        when we push messages, we will use this attributes
        '''
        self.push_exchange = exchange
        self.push_rk = rk
        if timeout:
            self.default_timeout = timeout
        self.push_timeout_msg = timeout_msg

    def push_message(self, msg, timeout_sec=None, **kwargs):
        '''
        this method is realization of syncronous calls over amqp
        '''
        d = Deferred()
        if kwargs.get('tid'):
            tid = kwargs['tid']
        elif type(msg) == dict and  msg.get(self.tid_name):
            tid = msg[self.tid_name]
        elif type(msg) == dict and msg.get('tid'):
            tid = msg['tid']
        elif type(msg) == Content and hasattr(msg, 'headers') and msg.headers.get(self.tid_name):
            tid = msg.headers[self.tid_name]
        else:
            tid = str(int(time.time()*1e7))
        if 'rk' in kwargs:
            rk = kwargs['rk']
        else:
            rk = self.push_rk
        if 'exchange' in kwargs:
            exchange = kwargs['exchange']
        else:
            exchange = self.push_exchange
        # user given timeout if have, else - default
        timeout = timeout_sec or self.default_timeout
        self.push_dict[tid] = d
        msg = self.encode_message(msg)

        msg_dict = {'exchange': exchange,
                    'rk': rk,
                    'content': msg,
                    'callback': Deferred(),
                    self.tid_name: tid,
                    self.rb_name: self.rq_rk
                   }
        msg_dict.update(kwargs)
        self.send_queue.put(msg_dict)
        if not 'default' in kwargs:
            _to = reactor.callLater(timeout, self.timeout, tid, d)
        else:
            _to = reactor.callLater(timeout, self.timeout,
                                    tid, d, kwargs['default'])
        self._timeout_calls[tid] = _to
        d.addErrback(self._error)
        return d

    def setup_read_queue(self, *args, **kwargs):
        AmqpReconnectingFactory.setup_read_queue(self, *args, **kwargs)
        self.rq_callback = self.push_read_process

    def push_read_process(self, msg):
        '''
        push message return
        '''
        tid = msg['headers'].get(self.tid_name)
        if tid in self.push_dict:
            # TODO: add decode message
            if self.def_full_content:
                self.push_dict[tid].callback(msg)
            else:
                self.push_dict[tid].callback(msg.body)
            del self.push_dict[tid]
        else:
            self.log.info('Got not our message in read queue. Check %r header'%self.tid_name)

    def push_read_loop(self):
        d = self.read_queue.get().addCallback(self.push_read_process)
        d.addErrback(self._error)

    def timeout(self, tid, callback, default=None):
        '''
        reactor call timeout
        '''
        del self._timeout_calls[tid]
        if not callback.called:
            del self.push_dict[tid]
            self.log.warning('Message syn timeout')
            if default:
                callback.callback(default)
            elif self.push_timeout_msg:
                callback.callback(self.push_timeout_msg)
            else:
                callback.errback(TimeoutException('syn_timeout'))

    def shutdown_factory(self):
        r = AmqpReconnectingFactory.shutdown_factory(self)
        for tid, to in self._timeout_calls.iteritems():
            to.cancel()
        return r


class SimpleListenFactory(AmqpReconnectingFactory):
    def __init__(self, parent, **kwargs):
        '''
        you need define only three params in kwargs
        rk, exchange and callback
        '''
        kwargs['push_back'] = True
        kwargs['no_ack'] = True
        kwargs['full_content'] = False
        AmqpReconnectingFactory.__init__(self, parent, **kwargs)
        self.push_back = True
        rq_rk = kwargs.get('rq_rk')
        exc = kwargs.get('exchange')
        cb = kwargs.get('callback')
        rq_name = kwargs.get('rq_name')
        durable = kwargs.get('rq_durable', True)
        auto_delete = kwargs.get('rq_autodelete', False)
        exclusive = kwargs.get('exclusive', False)
        no_ack = kwargs.get('no_ack', False)
        self.setup_read_queue(exc,
                              rq_rk,
                              queue_name=rq_name,
                              callback=cb,
                              durable=durable,
                              auto_delete=auto_delete,
                              exclusive=exclusive,
                              no_ack=no_ack)
