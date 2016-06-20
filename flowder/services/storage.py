from zope.interface import implementer
from twisted.application import service

from flowder.interfaces import ITaskStorage


@implementer(ITaskStorage)
class FileDownloaderTaskStorage(service.Service):
    name = 'task_storage'

    TASK_DONE = 'D'
    TASK_STANDBY = 'S'
    TASK_HOLD = 'H'
    TASK_RUNNING = 'R'

    RESULT_FAILED = 'F'
    RESULT_RETRY = 'R'
    RESULT_SUCCESS = 'S'

    tasks = list()  # tasks list - get from DB

    def __init__(self, app, database=None, table="task_list"):
        """
        if the project is ae and dbspath from settings is dbs:
        dbpath = os.path.join(dbsdir, '%s.db' % project)
        ==
        dbs/ae.db
        """

        self.app = app
        self.database = database or ':memory:'
        self.table = table

        self.conn = None
        self.ready = False
