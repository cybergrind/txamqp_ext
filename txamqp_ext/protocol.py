
import logging
import time

from zope.interface import implements
from twisted.internet import reactor
from twisted.internet.defer import Deferred
from twisted.internet.defer import DeferredList

import txamqp
from txamqp.client import TwistedDelegate
from txamqp.protocol import AMQClient
from txamqp.content import Content
import txamqp.spec

from txamqp_ext.interfaces import IAmqpProtocol


class AmqpProtocol(AMQClient):
    implements(IAmqpProtocol)
    log = logging.getLogger('AmqpProtocol')

    def __init__(self, *args, **kwargs):

        # callback on authenticated
        self._auth_succ = Deferred()
        # callback on read_channel opened
        self._read_opened = Deferred()
        # callback on write_channel opened
        self._write_opened = Deferred()
        # callback on read_loop started
        self._read_loop_started = Deferred()
        # callback on shutdown read loop
        self._read_loop_down = Deferred()
        # read queue timeout
        self.q_timeout = 1
        # read loop call
        self._rloop_call = None
        self.read_queue = None
        AMQClient.__init__(self, *args, **kwargs)

    def makeConnection(self, transport):
        '''
        This only for debug errors
        '''
        try:
            AMQClient.makeConnection(self, transport)
        except Exception, mess:
            self.log.error('During makeConnection: %r'%mess)
            print 'Error on make connection: %r'%mess

    def connectionMade(self):
        AMQClient.connectionMade(self)

        # set that we are not connected
        # since we should authenticate and open channels
        self.connected = False
        d = self.authenticate(self.factory.user, self.factory.password)
        d.addCallback(self._authenticated)
        d.addErrback(self._error)
        #d.addCallback(self._auth_succ.callback)
        return d

    def _authenticated(self, _none):
        '''
        this function will be called after authentification
        add call _auth_succ deferred
        we open two separate channels for read and write
        for use AMQP ability for multiplexing messaging
        that will give ability to utilize all bandwidth
        '''
        rd = self.channel(1)
        rd.addCallback(self._open_read_channel)
        rd.addErrback(self._error)

        wd = self.channel(2)
        wd.addCallback(self._open_write_channel)
        wd.addErrback(self._error)
        self._auth_succ.callback(True)
        dl = DeferredList([rd, wd])
        return dl

    def _open_read_channel(self, chan):
        '''
        called when have read channel
        return deffered, called when channel will
        be opened
        '''
        self.read_chan = chan
        def _opened(*args):
            self._read_opened.callback(self.read_chan)
            if self.factory.rq_enabled:
                self.start_read_loop()
        d = self.read_chan.channel_open()
        d.addCallback(_opened)
        d.addErrback(self._error)
        return d

    def _open_write_channel(self, chan):
        '''
        called when have write channel
        return deffered, called when channel will
        be opened
        '''
        self.write_chan = chan
        def _opened(*args):
            self.factory.connected.callback(self)
            self.connected = True
            self._write_opened.callback(self.write_chan)
            self.send_loop()
        d = self.write_chan.channel_open()
        d.addCallback(_opened)
        d.addErrback(self._error)
        return d

    def _error(self, failure):
        '''
        all deferred should addErrback(self._error)
        so, all errors will fail here
        '''
        print failure.getTraceback()
        raise failure

    def send_loop(self, *args):
        '''
        this function initiate send_message process
        we should store current message in fabric
        since we can fail
        '''
        # check for messages waiting sending
        if self.factory.processing_send:
            msg = self.factory.processing_send
            self.process_message(msg)
        else:
            msg = self.factory.send_queue.get()
            msg.addCallback(self.process_message)
            msg.addErrback(self._error)

    def process_message(self, msg):
        '''
        all messages should be dict with same fields
        {
         @exchange - exchange name,
         @rk - routing key,
         @content - text or Content
         @callback - deffered, that will be called after
         message was sended
         @tid - optional param, that should be unique
         needed for syncronous messaging
        }
        '''
        # run one more task, if we have parallel factory
        if self.factory.parallel:
            reactor.callLater(0, self.send_loop)

        self.factory.processing_send = msg
        exc = msg.get('exchange')
        rk = msg.get('rk')
        content = msg.get('content')
        cb = msg.get('callback')
        # convert to content type if not Content
        if type(content) != Content:
            content = Content(content)
        # setup tid in Content.properties
        # tid will be unique id for back message
        if msg.get('tid'):
            content['tid'] = msg['tid']
        else:
            content['tid'] = int(time.time()*1e7)
        # set delivery mode if not provided
        if not content.properties.get('delivery mode'):
            content['delivery mode'] = getattr(self.factory, 'delivery_mode', 2)
        def _after_send(res):
            if not cb.called:
                cb.callback(res)
            self.factory.processing_send = None
            # if we have non-parallel factory
            # we should run next message only after
            # previous has been processed
            if not self.factory.parallel:
                reactor.callLater(0, self.send_loop)
        w = self.write_chan.basic_publish(exchange=exc,
                                          routing_key=rk,
                                          content=content)
        w.addCallback(_after_send)
        return w

    def on_read_channel_opened(self):
        return self._read_opened

    def on_write_channel_opened(self):
        return self._write_opened

    def start_read_loop(self):
        exc = self.factory.rq_exchange
        q_name = self.factory.rq_name
        q_rk = self.factory.rq_rk
        q_dur = self.factory.rq_durable
        q_excl = self.factory.rq_exclusive
        q_auto_delete = self.factory.rq_auto_delete
        no_ack = self.factory.no_ack
        tag = self.factory.consumer_tag

        def _set_queue(queue):
            self.read_queue = queue
            self._read_loop_started.callback(True)
            reactor.callLater(0, self.read_loop)

        def _consume_started(res):
            return self.queue(tag).addCallback(_set_queue)

        def _queue_binded(res):
            d = self.read_chan.basic_consume(queue=q_name,
                                             no_ack=no_ack,
                                             consumer_tag=tag)
            d.addCallback(_consume_started)
            return d

        def _queue_declared(ok):
            d = self.read_chan.queue_bind(exchange=exc,
                                          queue=q_name,
                                          routing_key=q_rk)
            d.addCallback(_queue_binded)
            return d

        d = self.read_chan.queue_declare(queue=q_name,
                                         durable=True,
                                         exclusive=q_excl,
                                         auto_delete=q_auto_delete)
        d.addCallback(_queue_declared)
        d.addErrback(self._error)
        return d

    def read_loop(self, *args):
        def _get_message(msg):
            self.factory.read_queue.put(msg)
            self._rloop_call = reactor.callLater(0, self.read_loop)
        def _get_empty(failure):
            failure.trap(txamqp.queue.Empty)
            self._rloop_call =  reactor.callLater(0, self.read_loop)
        def _get_closed(failure):
            failure.trap(txamqp.queue.Closed)
            self._read_loop_down.callback(True)
        d = self.read_queue.get(self.q_timeout)
        d.addCallback(_get_message)
        d.addErrback(_get_empty)
        d.addErrback(_get_closed)
        d.addErrback(self._error)
        return d


    def on_read_loop_started(self):
        return self._read_loop_started

    def shutdown_protocol(self):
        def _lose_connection(_none):
            self.transport.loseConnection()
        def _close_connection(_none):
            d = self.channels[0].connection_close()
            if self._rloop_call and not self._rloop_call.called:
                self._rloop_call.cancel()
            return DeferredList([d]).addCallback(_lose_connection)
        def _close_channels(_none):
            if self.read_queue:
                self.read_queue.close()
            dl = []
            if not self.write_chan.closed:
                dl.append(self.write_chan.channel_close())
            if not self.read_chan.closed:
                dl.append(self.read_chan.channel_close())
            return DeferredList(dl)
        def _unsubscribe_read_queue(_none):
            d = self.read_chan.basic_cancel(self.factory.consumer_tag)
            return d
        d = _unsubscribe_read_queue(None)
        d.addCallback(_close_channels)
        d.addCallback(_close_connection)
        d.addErrback(self._error)
        return d


if __name__ == '__main__':
    am = AmqpProtocol(TwistedDelegate(), '/',
                      txamqp.spec.load('file:txamqp_ext/spec/amqp0-8.xml'))


