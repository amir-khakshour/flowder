from zope.interface import implementer
from twisted.application import service

from flowder.signal import get_signal_manager
from pygear.logging import log

from flowder import signals
from flowder.interfaces import IScheduler

@implementer(IScheduler)
class TaskScheduler(service.Service):

    name = 'scheduler'

    def __init__(self, config, app):
        self.app = app
        self.config = config
        self.tasks_list = {}

    def startService(self):
        IApp = service.IServiceCollection(self.app, self.app)
        self.task_storage = IApp.getServiceNamed('task_storage')

        self.signal_manager = get_signal_manager(self.app)
        self.signal_manager.connect(self.update_tasks, signal=signals.tasks_updated)

        self.update_tasks()  # same as pooler > pools list of projects

    def schedule(self, task_info):
        self.task_storage.add(task_info)

    def cancel(self, task_id):
        return self.tasks_list.remove(task_id)

    def list_tasks(self):
        pass

    def update_tasks(self):
        log.debug("Scheduler > Updating tasks")
        self.tasks_list = self.task_storage.tasks
        log.debug("Current tasks count: %s" % len(self.task_storage))
