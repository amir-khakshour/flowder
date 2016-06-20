# -*- coding: utf-8 -*-
from pkg_resources import resource_string
from miutils.modules import load_object
from flowder.config import Config

__name__ = 'flowder'
__shortName__ = 'Flowder'
__author__ = 'Amir Khakshour'
__email__ = 'khakshour.amir@gmail.com'
__version__ = resource_string(__name__, "VERSION").strip()
__summary__ = 'Flowder is a service daemon based on twisted to download files asynchronously.'
__description__ = __summary__


def get_application(config=None):
    if config is None:
        config = Config()
    app_path = config.get('application', 'flowder.app.application')
    app_func = load_object(app_path)
    return app_func(config)

application = get_application()
