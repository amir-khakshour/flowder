from zope.interface.declarations import implementer

from twisted.application import service
from twisted.python.failure import Failure
from twisted.internet.defer import maybeDeferred, DeferredList, Deferred

from pygear.logging import log
from pydispatch import dispatcher
from pydispatch.robustapply import robustApply
from pydispatch.dispatcher import (
    Any, Anonymous, liveReceivers, getAllReceivers, disconnect
)

from flowder.interfaces import ISignalManager


def send_catch_log(signal=Any, sender=Anonymous, *arguments, **named):
    """Like pydispatcher.robust.sendRobust but it also logs errors and returns
    Failures instead of exceptions.
    """
    dont_log = named.pop('dont_log', None)
    responses = []
    for receiver in liveReceivers(getAllReceivers(sender, signal)):
        try:
            response = robustApply(receiver, signal=signal, sender=sender,
                                   *arguments, **named)
            if isinstance(response, Deferred):
                log.msg(format="Cannot return deferreds from signal handler: %(receiver)s",
                        level=log.ERROR, receiver=receiver)
        except dont_log:
            result = Failure()
        except Exception:
            result = Failure()
            log.err(result, "Error caught on signal handler: %s" % receiver)
        else:
            result = response
        responses.append((receiver, result))
    return responses


def send_catch_log_deferred(signal=Any, sender=Anonymous, *arguments, **named):
    """Like send_catch_log but supports returning deferreds on signal handlers.
    Returns a deferred that gets fired once all signal handlers deferreds were
    fired.
    """

    def logerror(failure, recv):
        if dont_log is None or not isinstance(failure.value, dont_log):
            log.err(failure, "Error caught on signal handler: %s" % recv)
        return failure

    dont_log = named.pop('dont_log', None)
    dfds = []
    for receiver in liveReceivers(getAllReceivers(sender, signal)):
        d = maybeDeferred(robustApply, receiver, signal=signal, sender=sender,
                          *arguments, **named)
        d.addErrback(logerror, receiver)
        d.addBoth(lambda result: (receiver, result))
        dfds.append(d)
    d = DeferredList(dfds)
    d.addCallback(lambda out: [x[1] for x in out])
    return d


def disconnect_all(signal=Any, sender=Any):
    """Disconnect all signal handlers. Useful for cleaning up after running
    tests
    """
    for receiver in liveReceivers(getAllReceivers(sender, signal)):
        disconnect(receiver, signal=signal, sender=sender)


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


def get_signal_manager(app):
    return app.getComponent(ISignalManager)