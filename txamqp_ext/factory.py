
import logging
import time
import cPickle

import cjson
from twisted.internet import reactor
from twisted.internet import protocol
from twisted.internet.defer import Deferred
from twisted.internet.defer import DeferredList
from twisted.internet.defer import DeferredQueue

import txamqp
from txamqp.client import TwistedDelegate
from txamqp.content import Content
from txamqp.queue import TimeoutDeferredQueue, Empty

from txamqp_ext.protocol import AmqpProtocol


class AmqpReconnectingFactory(protocol.ReconnectingClientFactory):
    protocol = AmqpProtocol
    log = logging.getLogger('AmqpReconnectingFactory')

    def __init__(self, parent, **kwargs):
        self.parent = parent
        self.host = kwargs.get('host', 'localhost')
        self.port = kwargs.get('port', 5672)
        self.user = kwargs.get('user', 'guest')
        self.password = kwargs.get('password', 'guest')
        spec_file = kwargs.get('spec', 'file:./txamqp_ext/spec/amqp0-8.xml')
        self.spec = txamqp.spec.load(spec_file)
        self.vhost = kwargs.get('vhost', '/')
        self.delegate = TwistedDelegate()
        # parallel sending and receiving messages
        self.parallel = kwargs.get('parallel', True)
        # default delivery_mode for messages
        self.delivery_mode = kwargs.get('delivery_mode', 2)
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
        # traps for catch errors from protocol
        self._traps = []

        self.rq_enabled = False
        self.read_queue = TimeoutDeferredQueue()
        self.send_queue = DeferredQueue()
        self.dropped_send_messages = DeferredQueue()
        self.processing_send = None
        self.send_retries = 0
        self.max_send_retries = 2
        self.client = None

        self._stopping = False
        self.init_deferreds()
        reactor.connectTCP(self.host, self.port, self)

    def init_deferreds(self):
        self.connected = Deferred()
        self.connected.addErrback(self._error)

    def _error(self, failure):
        print 'error: %r'%failure.getTraceback()
        self.log.error(failure.getTraceback())
        raise failure

    def add_trap(self, trap):
        self._traps.append(trap)

    def setup_read_queue(self, exchange, routing_key, callback=None,
                         queue_name=None, exclusive=False,
                         durable=False, auto_delete=True,
                         no_ack=True):
        self.rq_enabled = True
        self.rq_exchange = exchange
        if queue_name:
            self.rq_name = queue_name
        else:
            self.rq_name = '%s_%s_%s_read_queue'%(
                self.parent.__class__.__name__,
                time.time(),
                hex(hash(self.parent))[-4:])
        self.rq_rk = routing_key
        self.rq_exclusive = exclusive
        self.rq_durable = durable
        self.rq_auto_delete = auto_delete
        self.no_ack = no_ack
        self.rq_callback = callback
        if not self.consumer_tag:
            self.consumer_tag = self.rq_name
        def _add_cb(_none):
            ret = self.client.on_read_loop_started()
            ret.addCallback(self.read_message_loop)
            ret.addErrback(self._error)
            return ret
        c = self.connected
        c.addCallbacks(_add_cb, self._error)
        return c

    def buildProtocol(self, addr):
        p = self.protocol(self.delegate, self.vhost, self.spec)
        self.client = p
        self.client.factory = self
        self.resetDelay()
        return p

    def clientConnectionFailed(self, connector, reason):
        self.init_deferreds()

        print 'fail: %r'%reason
        self.log.error('Connection failed: %r'%reason)
        protocol.ReconnectingClientFactory\
                .clientConnectionFailed(self, connector, reason)

    def clientConnectionLost(self, connector, reason):
        '''
        amqp errors cause connectionLost
        '''
        self.init_deferreds()

        if not self._stopping:
            print 'lost: %r'%reason
            self.log.error('Connection lost: %r'%reason)
        protocol.ReconnectingClientFactory\
                .clientConnectionLost(self, connector, reason)

    def encode_message(self, msg):
        if type(msg) == Content:
            msg_body = msg.body
        else:
            msg_body = msg
        if self.serialization == 'cjson':
            encoded = cjson.encode(msg_body)
        elif self.serialization == 'cPickle':
            encoded = cPickle.dumps(msg_body)
        else:
            encoded = str(msg_body)
        if type(msg) == Content:
            msg.body = encoded
        else:
            msg = Content(encoded)
        return msg

    def send_message(self, exchange, routing_key, msg, **kwargs):
        '''
        basic method for send message to amqp broker
       kwargs parameters is:
          @tx <bool> send message in transaction
          @callback <deferred> callback that will called after sending
        '''

        if 'callback' in kwargs:
            callback = kwargs['callback']
        else:
            callback = Deferred()
        msg = self.encode_message(msg)
        msg_dict = {'exchange': exchange,
                    'rk': routing_key,
                    'content': msg,
                    'callback': callback
                   }
        msg_dict.update(kwargs)
        self.send_queue.put(msg_dict)
        return callback

    def read_message_loop(self, *args):
        msg = self.read_queue.get()
        def _get_msg(msg):
            # TODO add support for different types
            if self.serialization == 'cjson':
                msg.content.body = cjson.decode(msg.content.body)
            elif self.serialization == 'cPickle':
                msg.content.body = cPickle.loads(msg.content.body)

            if not self.full_content:
                msg_out = msg.content.body
            else:
                msg_out = msg.content
            if callable(self.rq_callback):
                self.rq_callback(msg_out)
            if not self.no_ack:
                self.client.read_chan.basic_ack(msg.delivery_tag,
                                                multiple=False)
            if not self.parallel:
                reactor.callLater(0, self.read_message_loop)
        if self.parallel:
           reactor.callLater(0, self.read_message_loop)
        msg.addCallback(_get_msg)

    def shutdown_factory(self):
        self._stopping = True
        self.stopTrying()
        return self.client.shutdown_protocol()

class TimeoutException(Exception):
    pass

class AmqpSynFactory(AmqpReconnectingFactory):
    '''
    This factory implement non-blocking synchronous calls
    '''
    def __init__(self, parent, **kwargs):
        self.route_back = kwargs.get('route_back', 'route_back')
        self.push_dict = {}
        self.push_exchange = kwargs['exchange']
        self.push_rk = kwargs['rk']
        self.default_timeout = kwargs.get('timeout', 5)
        self._timeout_calls = {}
        AmqpReconnectingFactory.__init__(self, parent, **kwargs)
        self.full_content = True
        self.connected.addCallback(self._setup_read)

    def _setup_read(self, _none):
        #reactor.callLater(0, self.push_read_loop)
        pass

    def setup_push(self, exchange, rk, timeout=None, timeout_msg=None):
        '''
        setup push exchange and routing key
        when we push messages, we will use this attributes
        '''
        self.push_exchange = exchange
        self.push_rk = rk
        self.push_timeout = timeout
        self.push_timeout_msg = timeout_msg

    def push_message(self, msg, timeout_sec=None, **kwargs):
        d = Deferred()
        if kwargs.get('tid'):
            tid = kwargs['tid']
        elif type(msg) == dict and msg.get('tid'):
            tid = content[self.factory.tid_name] = msg['tid']
        elif type(msg) == dict and  msg.get(self.factory.tid_name):
            tid = msg[self.factory.tid_name]
        else:
            tid = str(int(time.time()*1e7))
        # user given timeout if have, else - default
        timeout = timeout_sec or self.default_timeout
        self.push_dict[tid] = d
        msg = self.encode_message(msg)
        msg_dict = {'exchange': self.push_exchange,
                    'rk': self.push_rk,
                    'content': msg,
                    'callback': Deferred(),
                    'tid': tid
                   }
        msg_dict.update(kwargs)
        self.send_queue.put(msg_dict)
        _to = reactor.callLater(timeout, self.timeout, tid, d)
        self._timeout_calls[tid] = _to
        return d

    def setup_read_queue(self, *args, **kwargs):
        r = AmqpReconnectingFactory.setup_read_queue(self, *args, **kwargs)
        self.rq_callback = self.push_read_process

    def push_read_process(self, msg):
        tid = msg['headers'].get('tid')
        if tid in self.push_dict:
            # TODO: add decode message
            self.push_dict[tid].callback(msg)
            del self.push_dict[tid]
        else:
            print 'RRRR'
        #reactor.callLater(0, self.push_read_loop)

    def push_read_loop(self):
        d = self.read_queue.get().addCallback(self.push_read_process)
        d.addErrback(self._error)

    def timeout(self, tid, callback):
        del self._timeout_calls[tid]
        if not callback.called:
            del self.push_dict[tid]
            print 'errback'
            callback.errback(TimeoutException('syn_timeout'))

    def shutdown_factory(self):
        r = AmqpReconnectingFactory.shutdown_factory(self)
        dl = []
        for tid, to in self._timeout_calls.iteritems():
            to.cancel()
        return r
