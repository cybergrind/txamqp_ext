import logging
import time

from zope.interface import implements
from twisted.internet import reactor
from twisted.internet.defer import Deferred
from twisted.internet.defer import DeferredList
from twisted.internet.defer import succeed

import txamqp
import txamqp.spec
from txamqp.client import TwistedDelegate
from txamqp.protocol import AMQClient
from txamqp.content import Content

from txamqp_ext.interfaces import IAmqpProtocol


class AmqpProtocol(AMQClient):
    implements(IAmqpProtocol)
    log = logging.getLogger('AmqpProtocol')
    content_mapping = {'cPickle': 'application/x-pickle',
                       'cjson': 'application/json',
                       'json': 'application/json',
                       'msgpack': 'application/x-msgpack'}

    def __init__(self, *args, **kwargs):
        self.log.debug('init protocol')
        self._stop = False
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
        self._sloop_call = None
        # ensure that we start read loop only once
        self._read_loop_enabled = False
        self.read_queue = None
        self.read_chan = None
        kwargs['heartbeat'] = kwargs.get('heartbeat', 10)
        self.__messages = set()
        AMQClient.__init__(self, *args, **kwargs)


    def makeConnection(self, transport):
        '''
        This only for debug errors
        '''
        try:
            AMQClient.makeConnection(self, transport)
        except Exception, mess:
            self.log.error('During makeConnection: %r'%mess)

    def connectionMade(self):
        AMQClient.connectionMade(self)
        # set that we are not connected
        # since we should authenticate and open channels
        self.connected = False
        self.log.debug('go authentication %r'%self.factory.user)
        d = self.authenticate(self.factory.user, self.factory.password)
        d.addCallback(self._authenticated)
        d.addErrback(self._error)
        return d

    def _authenticated(self, _none):
        '''
        this function will be called after authentification
        add call _auth_succ deferred
        we open two separate channels for read and write
        for use AMQP ability for multiplexing messaging
        that will give ability to utilize all bandwidth
        '''
        self.log.debug('authenticated')
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
            if not self.factory.connected.called:
                self.factory.connected.callback(self)
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
            if not self.factory.connected.called:
                self.factory.connected.callback(self)
            self.connected = True
            self._write_opened.callback(self.write_chan)
            self._sloop_call = reactor.callLater(0, self.send_loop)
        d = self.write_chan.channel_open()
        d.addCallback(_opened)
        d.addErrback(self._error)
        return d

    def _trap_closed(self, failure):
        failure.trap(txamqp.client.Closed)
        self.log.debug('trap closed')
        return True

    def _error(self, failure, send_requeue=None):
        '''
        all deferred should addErrback(self._error)
        so, all errors will fail here
        you could define you traps, and add it to _traps
        '''
        for trap in self.factory._traps:
            if trap(failure):
                return
        self.log.error('Protocol _error: %s'%failure.getTraceback())
        if send_requeue and not send_requeue['callback'].called:
            self.log.debug('requeue failed send message')
            self.factory.send_queue.put(send_requeue)
        self.shutdown_protocol()
        raise failure

    def _skip_error(self, failure):
        self.log.error('skip_error: %s'%failure.getTraceback())

    def doClose(self, reason):
        '''
        override standart method
        '''
        self.log.warning('Close due %r'%reason)
        AMQClient.doClose(self, reason)

    def send_loop(self, *args):
        '''
        this function initiate send_message process
        we should store current message in fabric
        since we can fail
        '''
        # check for messages waiting sending
        if self.factory.processing_send:
            msg = self.factory.processing_send
            self.factory.send_retries += 1
            if self.factory.send_retries > self.factory.max_send_retries:
                self.log.error('Drop message: %r'%msg)
                self.factory.dropped_send_messages.put(msg)
                self.factory.processing_send = None
                self.factory.send_retries = 0
                msg = self.factory.send_queue.get()
                msg.addCallback(self.process_message)
                msg.addErrback(self._error, send_requeue=msg)
            else:
                p = self.process_message(msg)
                p.addErrback(self._error, send_requeue=msg)
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
         @tx - boolean that define transaction mode
        }
        '''
        # run one more task, if we have parallel factory
        if self._stop:
            self.log.debug('Get message for inactive connection. requeue')
            self.factory.send_queue.put(msg)
        if self.factory.parallel and not self._stop:
            self._sloop_call = reactor.callLater(0, self.send_loop)

        self.factory.processing_send = msg
        exc = msg.get('exchange')
        rk = msg.get('rk')
        content = msg.get('content')
        cb = msg.get('callback')
        # get transaction flag of message
        # you should set it to True if want
        # send message in transaction
        msg_tx = msg.get('tx', False)
        # convert to content type if not Content
        if type(content) != Content:
            content = Content(content)
        # setup tid in Content.properties
        # tid will be unique id for back message
        if not 'headers' in content.properties:
            content['headers'] = {}

        if self.factory.content_type_name in content.properties:
            pass
        elif self.factory.serialization == 'content_based' and self.factory.default_content_type:
            content[self.factory.content_type_name] = self.factory.default_content_type
        elif not content.properties.get(self.factory.content_type_name):
            content[self.factory.content_type_name] = \
                self.content_mapping.get(self.factory.serialization, 'text/plain')


        #TODO forwarding reimplement. ensure all tid_name is ok
        if msg.get('tid'):
            content['headers'][self.factory.tid_name] = str(msg['tid'])
        elif msg.get(self.factory.tid_name):
            content['headers'][self.factory.tid_name] = str(msg[self.factory.tid_name])
        elif content['headers'].get(self.factory.tid_name):
            pass
        else:
            content['headers'][self.factory.tid_name] = str(int(time.time()*1e7))

        if msg.get(self.factory.rb_name):
            content['headers'][self.factory.rb_name] = msg[self.factory.rb_name]
        # set delivery mode if not provided
        if not content.properties.get(self.factory.delivery_mode_name):
            content[self.factory.delivery_mode_name] = getattr(self.factory, 'delivery-mode', 1)
        def _after_send(res):
            if not cb.called:
                cb.callback(res)
            self.factory.processing_send = None
            self.factory.send_retries = 0
            # if we have non-parallel factory
            # we should run next message only after
            # previous has been processed
            if not self.factory.parallel and not self._stop:
                self._sloop_call = reactor.callLater(0, self.send_loop)

        def _commit_tx(res):
            return self.write_chan.tx_commit()

        def _send_message(res):
            w = self.write_chan.basic_publish(exchange=exc,
                                              routing_key=rk,
                                              content=content)
            return w
        if (not self.factory.parallel) and (self.factory.tx_mode or msg_tx):
            d = self.write_chan.tx_select()
            d.addCallback(_send_message)
            d.addCallback(_commit_tx)
            d.addCallback(_after_send)
        else:
            d = _send_message(None)
            d.addCallback(_after_send)
        return d

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
        self.log.debug('Start read loop %r'%[exc, q_name, q_rk])
        if self._read_loop_enabled:
            # do not start second read loop
            return
        self._read_loop_enabled = True
        def _set_queue(queue):
            self.read_queue = queue
            self._read_loop_started.callback(True)
            self.log.debug('Start read loop')
            reactor.callLater(0, self.read_loop)

        def _consume_started(res):
            return self.queue(tag).addCallback(_set_queue)

        def _queue_binded(res):
            self.log.debug('Queue binded start consume %s => %s'%(q_name, q_rk))
            self.read_chan.basic_qos(prefetch_count=self.factory.prefetch_count)
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
        if self.factory.autodeclare:
            d = self.read_chan.queue_declare(queue=q_name,
                                             durable=q_dur,
                                             exclusive=q_excl,
                                             auto_delete=q_auto_delete)
            self.log.debug('Autodeclare.')
            if self.factory.autobind:
                self.log.debug('And autobind.')
                d.addCallback(_queue_declared)
        elif self.factory.autobind:
            self.log.debug('Only autobind.')
            d = _queue_declared(True)
        else:
            self.log.debug('No autodeclare and no autobind.')
            d = _queue_binded(True)
        d.addErrback(self._error)
        return d

    def read_loop(self, *args):
        '''
        run read loop
        we try to get messages from queue with timeout
        if timeout fired - we got empty message, so we need
        call read_loop again.
        '''
        def _get_message(msg):
            if not self.factory.no_ack:
                self.__messages.add(msg)
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

    def basic_ack(self, msg):
        if msg in self.__messages and not self._stop:
            self.__messages.remove(msg)
            return self.read_chan.basic_ack(msg.delivery_tag, multiple=False)


    def basic_reject(self, msg, requeue):
        if msg in self.__messages and not self._stop:
            self.__messages.remove(msg)
            return self.read_chan.basic_reject(msg.delivery_tag, requeue)



    def on_read_loop_started(self):
        return self._read_loop_started

    def shutdown_protocol(self):
        '''
        During shutdown we should:
        * unsubscribe read queue
        * close channels
        * close connection on channel0
        * lose transport connection
        '''
        self.log.debug('Shutdown protocol %r'%hash(self))
        self._stop = True
        if self.factory.read_queue and self.factory.read_queue.pending:
            self.factory.read_queue.pending = []
        if self.heartbeatInterval:
            if self.sendHB.running:
                self.sendHB.stop()
            if self.checkHB.active():
                self.checkHB.cancel()
        def _lose_connection(_none):
            self.transport.unregisterProducer()
            self.transport.loseConnection()
        def _close_connection(_none):
            if 0 in self.channels:
                d = self.channels[0].connection_close()
                d.addErrback(self._skip_error)
            else:
                d = succeed(True)
            if self._rloop_call and not self._rloop_call.called:
                self._rloop_call.cancel()
            return DeferredList([d]).addCallback(_lose_connection)
        def _close_channels(_none):
            dl = []
            if self.read_queue:
                self.read_queue.close()
            if hasattr(self, 'write_chan') and not self.write_chan.closed:
                dl.append(self.write_chan.channel_close()\
                          .addErrback(self._skip_error))
            if self.read_chan and not self.read_chan.closed:
                dl.append(self.read_chan.channel_close()\
                          .addErrback(self._skip_error))
            return DeferredList(dl)
        def _unsubscribe_read_queue(_none):
            if self.read_chan:
                d = self.read_chan.basic_cancel(self.factory.consumer_tag)
                d.addErrback(self._skip_error)
                return d
            else:
                return succeed(lambda x: x)
        def _ok_fail(failure):
            if self.transport.connected:
                self.transport.loseConnection()
            return

        if self._sloop_call and self._sloop_call.active():
            self.log.debug('Stop send loop for protocol')
            self._sloop_call.cancel()

        if self.transport.connected:
            d = _unsubscribe_read_queue(None)
            d.addCallback(_close_channels)
            d.addCallback(_close_connection)
            d.addErrback(self._skip_error)
            d.addErrback(_close_connection)
            d.addErrback(_ok_fail)
            return d
        else:
            self.log.warning('LOSE CONNECTION FAIL')
            try:
                # we should unregister producer or connection will
                # never be closed
                self.transport.unregisterProducer()
                self.transport.loseConnection()
            except:
                pass


if __name__ == '__main__':
    am = AmqpProtocol(TwistedDelegate(), '/',
                      txamqp.spec.load('file:txamqp_ext/spec/amqp0-8.xml'))
