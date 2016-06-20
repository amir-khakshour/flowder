# -*- coding: utf-8 -*-
from twisted.application.service import Application

def application(config):
    app = Application("Flowder")
    return app
