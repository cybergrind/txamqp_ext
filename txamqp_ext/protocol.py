
import logging

from zope.interface import implements
from twisted.internet.defer import Deferred
from twisted.internet.defer import DeferredList

from txamqp.client import TwistedDelegate
from txamqp.protocol import AMQClient
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
        d.addCallback(self._auth_succ.callback)
        return d

    def _authenticated(self, _none):
        self.factory.connected.callback(self)
        rd = self.channel(1)
        rd.addCallback(self._open_read_channel)
        rd.addErrback(self._error)

        wd = self.channel(2)
        wd.addCallback(self._open_write_channel)
        wd.addErrback(self._error)
        
        dl = DeferredList([rd, wd])
        return dl

    def _open_read_channel(self, chan):
        self.read_chan = chan
        def _opened(*args):
            self._read_opened.callback(self.read_chan)
        d = self.read_chan.channel_open()
        d.addCallback(_opened)
        d.addErrback(self._error)
        return d

    def _open_write_channel(self, chan):
        self.write_chan = chan
        def _opened(*args):
            self._write_opened.callback(self.write_chan)
        d = self.write_chan.channel_open()
        d.addCallback(_opened)
        d.addErrback(self._error)
        return d

    def _error(self, failure):
        print dir(failure)
        print failure.getTraceback()
        raise failure

    def on_read_channel_opened(self):
        return self._read_opened

    def on_write_channel_opened(self):
        return self._write_opened

    def start_read_loop(self, bindings):
        pass

    def on_read_loop_started(self):
        pass

    def shutdown_protocol(self):
        pass


if __name__ == '__main__':
    am = AmqpProtocol(TwistedDelegate(), '/', txamqp.spec.load('file:txamqp_ext/spec/amqp0-8.xml'))


