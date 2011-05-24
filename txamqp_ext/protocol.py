
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
        self._read_opened = Deferred()
        self._write_opened = Deferred()
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

        self.connected = False
        d = self.authenticate(self.factory.user, self.factory.password)
        d.addCallback(self._authenticated)
        d.addErrback(self._error)
        return d

    def _authenticated(self, _none):
        self.factory.deferred.callback(self)

    def _error(self, failure):
        print failure
        raise failure

    def on_read_channel_opened():
        pass

    def on_write_channel_opened():
        pass

    def start_read_loop(bindings):
        pass

    def on_read_loop_started():
        pass

    def shutdown_protocol():
        pass


if __name__ == '__main__':
    am = AmqpProtocol(TwistedDelegate(), '/', txamqp.spec.load('file:txamqp_ext/spec/amqp0-8.xml'))


