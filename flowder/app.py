# -*- coding: utf-8 -*-
from twisted.application.service import Application
from twisted.python import log
from twisted.web import server
from twisted.python.log import ILogObserver
from twisted.application.internet import TimerService, TCPServer, UDPServer


from flowder.signal import SignalManager
from flowder.interfaces import ISignalManager
from flowder.services.fetcher import FetcherService

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

    # add Fetcher service
    fetcher = FetcherService(config)
    fetcher.setServiceParent(app)

    log.msg("Starting Flowder services (;-)")

    return app
