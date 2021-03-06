# -*- coding: utf-8 -*-
from twisted.application.service import Application
from twisted.python import log
from twisted.web import server
from twisted.application.internet import TimerService, TCPServer, UDPServer

from pygear.system.loading import load_object
from pygear.twisted.interfaces import ISignalManager
from pygear.twisted.signal import SignalManager

from .services.website import Root
from .services.poller import QueuePoller
from .services.fetcher import FetcherService
from .services.storage import FileDownloaderTaskStorage
from .services.scheduler import TaskScheduler
from .services.amqp import AmqpService


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

    fetcher = FetcherService(config)
    fetcher.setServiceParent(app)

    poller = QueuePoller(app, poll_size)
    poller.setServiceParent(app)

    db_file = '%s.db' % db_file
    task_storage = FileDownloaderTaskStorage(app, db_file)
    task_storage.setServiceParent(app)

    timer = TimerService(poll_interval, poller.poll)
    timer.setServiceParent(app)

    scheduler = TaskScheduler(config, app)
    scheduler.setServiceParent(app)

    laupath = config.get('launcher', 'flowder.launcher.Launcher')
    laucls = load_object(laupath)
    launcher = laucls(app, config)
    launcher.setServiceParent(app)

    restService = TCPServer(rest_port, server.Site(Root(app, config)), interface=rest_bind)
    restService.setServiceParent(app)

    amqp_publisher = AmqpService(app, config)
    amqp_publisher.setServiceParent(app)

    log.msg("Starting Flowder services (;-)")

    return app
