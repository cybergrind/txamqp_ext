
from zope.interface import Interface
from twisted.internet.interfaces import IProtocol
from twisted.internet.interfaces import IProtocolFactory

class IAmqpProtocol(IProtocol):
    '''
    Interface for basic AmqpProtocol
    '''

    def on_read_channel_opened():
        '''
        @return: defered with channel that will be fired
        when read channel will be opened
        '''

    def on_write_channel_opened():
        '''
        @return: deferred with channel that will be fired
        when write channel will be opened
        '''

    def start_read_loop(bindings):
        '''
        start read loop

        @bingings: list of dicts
        [{exchange:routing_key}]
        '''

    def on_read_loop_started():
        '''
        @return: deferred runned when read loop was
        started
        '''

    def shutdown_protocol():
        '''
        unsubscribe read channel and close it
        close write channel
        close connection
        
        @return: deferred runned after protocol was
        stopped and disconnected
        '''

class IAmqpProtocolMonitoring(Interface):
    '''
    methods for monitoring
    '''
    def add_error_receiver(callback):
        '''
        register error receiver.
        callback will called on every error was occured

        @callback function for call on every error
        '''

    def on_message_send(callback):
        '''
        
        '''


class IAmqpFactory(IProtocolFactory):
    '''
    support 
    '''


class IAmqpSynFactory(IAmqpFactory):
    '''
    Implements synchronous message sending
    '''

    

    
