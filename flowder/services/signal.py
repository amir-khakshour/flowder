from zope.interface.declarations import implementer

from twisted.application import service
from pydispatch import dispatcher
from pygear.logging import log

from flowder.interfaces import ISignalManager
from flowder.signal import send_catch_log, send_catch_log_deferred, disconnect_all

@implementer(ISignalManager)
class SignalManager(service.Service):

    def __init__(self, sender=dispatcher.Anonymous):
        self.sender = sender

    def startService(self):
        log.msg("Starting signal manager...")

    def connect(self, *a, **kw):
        kw.setdefault('sender', self.sender)
        return dispatcher.connect(*a, **kw)

    def disconnect(self, *a, **kw):
        kw.setdefault('sender', self.sender)
        return dispatcher.disconnect(*a, **kw)

    def send_catch_log(self, *a, **kw):
        kw.setdefault('sender', self.sender)
        return send_catch_log(*a, **kw)

    def send_catch_log_deferred(self, *a, **kw):
        kw.setdefault('sender', self.sender)
        return send_catch_log_deferred(*a, **kw)

    def disconnect_all(self, *a, **kw):
        kw.setdefault('sender', self.sender)
        return disconnect_all(*a, **kw)