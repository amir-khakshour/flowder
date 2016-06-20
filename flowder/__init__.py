# -*- coding: utf-8 -*-
from pkg_resources import resource_string
from pygear.system.loading import load_object
from .config import FlowderConfig


__name__ = 'flowder'
__shortName__ = 'Flowder'
__author__ = 'Amir Khakshour'
__email__ = 'khakshour.amir@gmail.com'
__version__ = resource_string(__name__, "VERSION").strip()
__summary__ = 'Flowder is a service daemon based on twisted to download files asynchronously.'
__description__ = __summary__


def get_application(config=None):
    if config is None:
        config = FlowderConfig()
    app_path = config.get('application', 'flowder.app.application')
    app_func = load_object(app_path)
    return app_func(config)

application = get_application()
