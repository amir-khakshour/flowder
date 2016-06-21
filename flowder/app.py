# -*- coding: utf-8 -*-
from twisted.application.service import Application
from twisted.python import log
from twisted.web import server
from twisted.python.log import ILogObserver
from twisted.application.internet import TimerService, TCPServer, UDPServer


from .interfaces import ISignalManager
from .services.signal import SignalManager
from .services.poller import QueuePoller
from .services.fetcher import FetcherService
from .services.storage import FileDownloaderTaskStorage
from .services.scheduler import TaskScheduler


def application(config):
    app = Application("Flowder")
    app_id = config.get('app_id', 'fw0')
    logfile = config.get('logfile', '/var/log/flowder.log')
    loglevel = config.get('loglevel', 'info')

    db_file = config.get('db_file', 'flowder')
    rest_port = config.getint('rest_port', 4000)
    rest_bind = config.get('rest_bind', '0.0.0.0')
    poll_interval = config.getfloat('poll_interval', 1)
    poll_size = config.getint("poll_size", 5)

    signalmanager = SignalManager()
    app.setComponent(ISignalManager, signalmanager)

    # Fetcher service
    fetcher = FetcherService(config)
    fetcher.setServiceParent(app)

    # Queue Poller Service
    poller = QueuePoller(app, poll_size)
    poller.setServiceParent(app)

    # Task Storage
    db_file = '%s.db' % db_file
    task_storage = FileDownloaderTaskStorage(app, db_file)
    task_storage.setServiceParent(app)

    timer = TimerService(poll_interval, poller.poll)
    timer.setServiceParent(app)

    # Scheduler
    scheduler = TaskScheduler(config, app)
    scheduler.setServiceParent(app)

    log.msg("Starting Flowder services (;-)")

    return app
