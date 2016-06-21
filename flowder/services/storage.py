import json
import time
import sqlite3

from zope.interface import implementer
from twisted.application import service

from pygear.logging import log
from flowder import signals
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

    def startService(self):
        log.msg("Start connecting to Database ...")
        self.start()

    def start(self):
        self.create_connection()
        self.create_or_update_table()

    def create_connection(self):
        self.conn = sqlite3.connect(self.database, check_same_thread=False)
        self.ready = True

    def create_or_update_table(self):
        # about check_same_thread: http://twistedmatrix.com/trac/ticket/4040
        q = "create table if not exists %s (id integer primary key, job_id text, status text, " \
            "fetch_uri text, result_url text, settings text, " \
            "created text, updated text, " \
            "result_type text, result_message text)" % self.table

        self.conn.execute(q)
        self._update_tasks()

    def add(self, task_info):
        task = task_info.copy()
        status = self.TASK_STANDBY
        job_id = task.pop('job_id')
        fetch_uri = task.pop('fetch_uri')
        settings = task.pop('settings')
        _time = int(time.time())

        args = (
            job_id, status,
            fetch_uri, settings,
            _time, _time
        )
        q = "insert into %s (job_id, status, " \
            "fetch_uri, settings, " \
            "created, updated) values (?,?,?,?,?,?)" % self.table
        c = self.conn.execute(q, args)
        self.commit()

        if c.rowcount:
            self.commit()
            self._update_tasks()
            self.signal_manager.send_catch_log(signal=signals.tasks_updated, job_id=job_id, task_info=task_info)

    def commit(self):
        try:
            self.conn.commit()
        except Exception as e:
            log.err(e.message)

    def remove(self, job_id):
        q = "select id from %s where job_id=?" % self.table
        c = self.conn.execute(q, (str(job_id),))
        val = c.fetchone()
        if not val:
            raise IndexError("Given job id is not valid or job doesn't exits!")
        id = val[0]
        q = "delete from %s where id=?" % self.table
        c = self.conn.execute(q, (id,))
        if not c.rowcount:  # record vanished, so let's try again
            self.conn.rollback()
            return self.remove(job_id)
        self.commit()
        self.signal_manager.send_catch_log(signal=signals.tasks_updated, job_id=job_id)

    def count(self):
        return len(self)

    def _update_tasks(self):
        self.tasks = [i for i in self._tasks()]

    def __len__(self):
        q = "select count(*) from %s" % self.table
        return self.conn.execute(q).fetchone()[0]

    def _tasks(self):
        q = "select id, job_id, status,  " \
            "fetch_uri, result_url, settings from %s WHERE status!= ? order by created asc, id asc limit 50" % \
            self.table
        return ({"id": id, "job_id": job_id, 'status': status,
                 "fetch_uri": fetch_uri, "result_url": result_url, "settings": settings}
                for id, job_id, status, fetch_uri, result_url, settings in self.conn.execute(q, (self.TASK_DONE,)))

    @staticmethod
    def encode(obj):
        return json.dumps(obj)

    @staticmethod
    def decode(text):
        return json.loads(text)

    def set_task_status(self, job_id, status, **kwargs):
        q = "UPDATE %s SET status=?, updated=?"
        sql_args = (status, int(time.time()))

        if status in (self.TASK_STANDBY, self.TASK_STANDBY):
            q += " , result_type=?, result_message=?"
            sql_args += (kwargs.get('message', ''), kwargs.get('result_type', ''))

        q += " WHERE job_id=?;"

    def set_task_running(self, job_id):
        q = "UPDATE %s SET status=?, updated=? WHERE job_id=?;" % self.table
        c = self.conn.execute(q, (self.TASK_RUNNING, int(time.time()), str(job_id),))
        self.commit()
        self.signal_manager.send_catch_log(signal=signals.tasks_updated, job_id=job_id)

    def set_task_finished(self, job_id, result_type='', result_message=''):
        q = "UPDATE %s SET status=?, updated=?, result_type=?, result_message=? WHERE job_id=?;" % self.table
        c = self.conn.execute(q, (
            self.TASK_DONE, int(time.time()), str(result_type), str(result_message), str(job_id)))
        self.commit()
        self.signal_manager.send_catch_log(signal=signals.tasks_updated, job_id=job_id)

    def set_task_standby(self, job_id, result_type='', result_message=''):
        q = "UPDATE %s SET status=?, updated=?, result_type=?, result_message=? WHERE job_id=?;" % self.table
        c = self.conn.execute(q, (
            self.TASK_STANDBY, int(time.time()), str(result_type), str(result_message), str(job_id),))
        self.commit()
        self.signal_manager.send_catch_log(signal=signals.tasks_updated, job_id=job_id)

    def set_task_hold(self, job_id):
        q = "UPDATE %s SET status=?, updated=? WHERE job_id=?;" % self.table
        c = self.conn.execute(q, (self.TASK_HOLD, int(time.time()), str(job_id),))
        self.commit()
        self.signal_manager.send_catch_log(signal=signals.tasks_updated, job_id=job_id)

    def check_url_already_fetched(self, url):
        output = None
        q = "SELECT * from %s where fetch_uri=? and status=? and result_type=? and result_url IS NOT NULL AND result_url != '' LIMIT 1" \
            % self.table
        result = self.conn.execute(q, (url, self.TASK_DONE, self.RESULT_SUCCESS)).fetchone()
        if result:
            keys = [
                'id', 'job_id', 'status', 'fetch_uri',
                'result_url', 'settings', 'created',
                'updated', 'result_type', 'result_message'
            ]
            output = dict(zip(keys, result))

        return output

    def set_jobid_result_url(self, job_id, url):
        q = "UPDATE %s SET result_url=?  WHERE job_id=?;" % self.table
        self.conn.execute(q, (str(url), str(job_id),))
        self.commit()
        self.signal_manager.send_catch_log(signal=signals.tasks_updated, job_id=job_id)

    def reset_all_tasks(self):
        """
        Update status
        """
        q = "UPDATE %s SET status=?, updated=? WHERE status!=?" % self.table
        args = (self.TASK_STANDBY, int(time.time()), self.TASK_DONE)
        c = self.conn.execute(q, args, )
        self.commit()
        return c.rowcount
