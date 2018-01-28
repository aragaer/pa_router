import fcntl
import os

from abc import ABCMeta, abstractmethod

from . import EndpointClosedException


class Channel(metaclass=ABCMeta):

    @abstractmethod
    def read(self): #pragma: no cover
        raise NotImplementedError

    @abstractmethod
    def write(self): #pragma: no cover
        raise NotImplementedError

    @abstractmethod
    def close(self): #pragma: no cover
        raise NotImplementedError


class PipeChannel(Channel):

    _in = None
    _out = None

    def __init__(self, faucet=None, sink=None):
        if faucet is not None:
            fl = fcntl.fcntl(faucet, fcntl.F_GETFL)
            fcntl.fcntl(faucet, fcntl.F_SETFL, fl | os.O_NONBLOCK)
            self._in = os.fdopen(faucet, mode='rb')
        if sink is not None:
            self._out = os.fdopen(sink, mode='wb')

    def read(self):
        try:
            return self._in.read() or b''
        except OSError as ex:
            raise EndpointClosedException(ex)

    def write(self, data):
        try:
            self._out.write(data)
            self._out.flush()
        except OSError as ex:
            raise EndpointClosedException(ex)

    def close(self):
        if self._in is not None:
            self._in.close()
        if self._out is not None:
            self._out.close()


