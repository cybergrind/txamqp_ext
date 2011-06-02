
import logging

from twisted.internet import reactor
from twisted.internet import protocol
from twisted.internet.defer import Deferred
from twisted.internet.defer import DeferredList
from twisted.internet.defer import DeferredQueue

import txamqp
from txamqp.client import TwistedDelegate

from txamqp_ext.protocol import AmqpProtocol


class AmqpReconnectingFactory(protocol.ReconnectingClientFactory):
    protocol = AmqpProtocol
    log = logging.getLogger('AmqpReconnectingFactory')

    def __init__(self, parent, **kwargs):
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
        # traps for catch errors from protocol
        self._traps = []

        self.rq_enabled = False
        self.read_queue = DeferredQueue()
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

    def setup_read_queue(self, exchange, routing_key, callback,
                         queue_name=None, exclusive=False,
                         durable=False, auto_delete=True,
                         no_ack=True):
        self.rq_enabled = True
        self.rq_exchange = exchange

        self.rq_name = queue_name
        self.consumer_tag = queue_name
        self.rq_rk = routing_key
        self.rq_exclusive = exclusive
        self.rq_durable = durable
        self.rq_auto_delete = auto_delete
        self.no_ack = no_ack
        self.rq_callback = callback
        if not self.consumer_tag:
            self.consumer_tag = queue_name
        ret = self.on_read_loop_started()
        ret.addCallback(self.read_message_loop)
        ret.addErrback(self._error)
        return ret

    def buildProtocol(self, addr):
        p = self.protocol(self.delegate, self.vhost, self.spec)
        self.client = p
        self.client.factory = self
        self.resetDelay()
        #if not self._reconnect_enabled:
        #    print 'Not enabled'
        #    self.stopTrying()
        return p

    def clientConnectionFailed(self, connector, reason):
        self.init_deferreds()

        print 'fail: %r'%reason
        self.log.error('Connection failed: %r'%reason)
        protocol.ReconnectingClientFactory\
                .clientConnectionFailed(self, connector, reason)

    def clientConnectionLost(self, connector, reason):
        self.init_deferreds()

        if not self._stopping:
            print 'lost: %r'%reason
            self.log.error('Connection lost: %r'%reason)
        protocol.ReconnectingClientFactory\
                .clientConnectionLost(self, connector, reason)

    def send_message(self, exchange, routing_key, msg, **kwargs):
        '''
        basic method for send message to amqp broker
        kwargs parameters is:
          @tx <bool> send message in transaction
          @callback <deferred> callback that will called after sending
        '''
        if type(msg) in (dict, list):
            msg = cjson.encode(msg)

        if 'callback' in kwargs:
            callback = kwargs['callback']
        else:
            callback = Deferred()

        msg_dict = {'exchange': exchange,
                    'rk': routing_key,
                    'content': msg,
                    'callback': callback
                   }
        msg_dict.update(kwargs)
        self.send_queue.put(msg_dict)

    def read_message_loop(self, *args):
        msg = self.read_queue.get()
        def _get_msg(msg):
            if msg.startswith('{'):
                msg_out = cjson.decode(msg.content.body)
            else:
                msg_out = msg.content.body
            self.callback(msg_out)
            if not self.parallel:
                reactor.callLater(0, self.read_message_loop)
        if self.parallel:
           reactor.callLater(0, self.read_message_loop)
        msg.addCallback(_get_msg)

    def shutdown_factory(self):
        self._stopping = True
        self.stopTrying()
        return self.client.shutdown_protocol()

