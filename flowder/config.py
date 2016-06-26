import glob
from os.path import expanduser
from pkgutil import get_data

from pygear.core.config import Config


class FlowderConfig(Config):
    SECTION = 'flowder'

    def get_default_config(self):
        try:
            return get_data(__package__, 'default_{section}.conf'.format(section=self.SECTION))
        except IOError:
            pass
