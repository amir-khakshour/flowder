import glob
from os.path import expanduser
from pkgutil import get_data

from pygear.core.config import Config


class FlowderConfig(Config):
    SECTION = 'flowder'
