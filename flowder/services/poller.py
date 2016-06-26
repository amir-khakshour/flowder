from zope.interface import implementer

from twisted.application import service
from twisted.internet.defer import DeferredQueue, inlineCallbacks, maybeDeferred, returnValue

from pygear.logging import log
from pygear.twisted.signal import get_signal_manager

from flowder import signals
from flowder.interfaces import IPoller


@implementer(IPoller)
class QueuePoller(service.Service):
    name = 'poller'

    def __init__(self, app, poll_size=5):
        self.app = app
        self.dq = DeferredQueue(size=poll_size)
        self.queue = None

    def startService(self):
        log.msg("Start pooler ...")
        IApp = service.IServiceCollection(self.app, self.app)
        self.task_storage = IApp.getServiceNamed('task_storage')
        self.signal_manager = get_signal_manager(self.app)
        self.signal_manager.connect(self.update_tasks, signal=signals.tasks_updated)

        self.update_tasks()

    @inlineCallbacks
    def poll(self):
        if self.dq.pending or \
                not self.task_storage or \
                not self.task_storage.ready:
            return
        c = yield maybeDeferred(self.task_storage.count)
        if c:
            for task in self.tasks_list:
                if task['status'] != self.task_storage.TASK_STANDBY:
                    continue
                task['status'] = self.task_storage.TASK_HOLD
                log.msg("add task to queue %s." % task['job_id'])
                self.put(task)
                break

    @inlineCallbacks
    def put(self, task):
        yield self.set_task_hold(task['job_id'])
        self.dq.put(task)

    def next(self):
        return self.dq.get()

    def update_tasks(self):
        log.debug("Poller > Updating tasks")
        self.tasks_list = self.task_storage.tasks

    def failed(self, why):
        ex = why.value
        log.err(ex.message)

    def set_task_hold(self, job_id):
        dfd = maybeDeferred(self.task_storage.set_task_hold, job_id)
        dfd.addErrback(self.failed)
        return dfd

    def set_task_running(self, job_id):
        dfd = maybeDeferred(self.task_storage.set_task_running, job_id)
        dfd.addErrback(self.failed)
        return dfd

    def set_task_finished(self, job_id, result_type, result_message):
        dfd = maybeDeferred(self.task_storage.set_task_finished, job_id, result_type, result_message)
        dfd.addErrback(self.failed)
        return dfd

    def set_task_succesfull(self, job_id, result_message):
        result_type = self.task_storage.RESULT_SUCCESS
        return self.set_task_finished(job_id, result_type, result_message)

    def set_task_failed(self, job_id, result_message):
        result_type = self.task_storage.RESULT_FAILED
        return self.set_task_finished(job_id, result_type, result_message)

    def set_task_retry(self, job_id, result_message):
        result_type = self.task_storage.RESULT_RETRY
        dfd = maybeDeferred(self.task_storage.set_task_standby, job_id, result_type, result_message)
        dfd.addErrback(self.failed)
        return dfd

    def check_url_already_fetched(self, url):
        dfd = maybeDeferred(self.task_storage.check_url_already_fetched, url)
        dfd.addErrback(self.failed)
        return dfd

    def reset_all_tasks(self):
        dfd = maybeDeferred(self.task_storage.reset_all_tasks)
        dfd.addErrback(self.failed)
        return dfd


