"""
Settings for DataJoint.
"""
from . import DataJointError
import json
import pprint
from collections import OrderedDict

import logging
import collections
from enum import Enum

LOCALCONFIG = 'dj_local_conf.json'
CONFIGVAR = 'DJ_LOCAL_CONF'

validators = collections.defaultdict(lambda: lambda value: True)
validators['database.port'] = lambda a: isinstance(a, int)

Role = Enum('Role', 'manual lookup imported computed job')
role_to_prefix = {
    Role.manual: '',
    Role.lookup: '#',
    Role.imported: '_',
    Role.computed: '__',
    Role.job: '~'
}
prefix_to_role = dict(zip(role_to_prefix.values(), role_to_prefix.keys()))

default = OrderedDict({
    'database.host': 'localhost',
    'database.password': 'datajoint',
    'database.user': 'datajoint',
    'database.port': 3306,
    #
    'connection.init_function': None,
    #
    'loglevel': 'DEBUG',
    #
    'safemode': True,
    #
    'display.limit': 7,
    'display.width': 14
})

logger = logging.getLogger()
log_levels = {
    'INFO': logging.INFO,
    'WARNING': logging.WARNING,
    'CRITICAL': logging.CRITICAL,
    'DEBUG': logging.DEBUG,
    'ERROR': logging.ERROR,
    None:  logging.NOTSET
}


class Borg:
    _shared_state = {}

    def __init__(self):
        self.__dict__ = self._shared_state


class Config(Borg, collections.MutableMapping):
    """
    Stores datajoint settings. Behaves like a dictionary, but applies validator functions
    when certain keys are set.

    The default parameters are stored in datajoint.settings.default . If a local config file
    exists, the settings specified in this file override the default settings.
    """

    def __init__(self, *args, **kwargs):
        Borg.__init__(self)
        self._conf = dict(default)
        self.update(dict(*args, **kwargs))  # use the free update to set keys

    def __getitem__(self, key):
        return self._conf[key]

    def __setitem__(self, key, value):
        logger.log(logging.INFO, u"Setting {0:s} to {1:s}".format(str(key), str(value)))
        if isinstance(value, collections.Mapping):
            raise ValueError("Nested settings are not supported!")
        if validators[key](value):
            self._conf[key] = value
        else:
            raise DataJointError(u'Validator for {0:s} did not pass'.format(key, ))

    def __delitem__(self, key):
        del self._conf[key]

    def __iter__(self):
        return iter(self._conf)

    def __len__(self):
        return len(self._conf)

    def __str__(self):
        return pprint.pformat(self._conf, indent=4)

    def __repr__(self):
        return self.__str__()

    def save(self, filename=None):
        """
        Saves the settings in JSON format to the given file path.
        :param filename: filename of the local JSON settings file. If None, the local config file is used.
        """
        if filename is None:
            filename = LOCALCONFIG
        with open(filename, 'w') as fid:
            json.dump(self._conf, fid, indent=4)

    def load(self, filename):
        """
        Updates the setting from config file in JSON format.

        :param filename=None: filename of the local JSON settings file. If None, the local config file is used.
        """
        if filename is None:
            filename = LOCALCONFIG
        with open(filename, 'r') as fid:
            self.update(json.load(fid))


