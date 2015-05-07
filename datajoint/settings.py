"""
Settings for DataJoint.
"""
from . import DataJointError
import json
import pprint
from collections import OrderedDict

__author__ = 'eywalker'
import logging
import collections
from enum import Enum

LOCALCONFIG = 'dj_local_conf.json'
CONFIGVAR = 'DJ_LOCAL_CONF'

validators = collections.defaultdict(lambda: lambda value: True)

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
})


class Config(collections.MutableMapping):
    """
    Stores datajoint settings. Behaves like a dictionary, but applies validator functions
    when certain keys are set.

    The default parameters are stored in datajoint.settings.default . If a local config file
    exists, the settings specified in this file override the default settings.
    """

    def __init__(self, *args, **kwargs):
        self._conf = dict(default)
        self.update(dict(*args, **kwargs))  # use the free update to set keys

    def __getitem__(self, key):
        return self._conf[key]

    def __setitem__(self, key, value):
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
        return  pprint.pformat(self._conf, indent=4)

    def __repr__(self):
        return self.__str__()

    def save(self, filename=None):
        """
        Saves the settings in JSON format to the given file path.
        :param filename: filename of the local JSON settings file. If None, the local config file is used.
        """
        if filename is None:
            import datajoint as dj
            filename = dj.config['config.file']
        with open(filename, 'w') as fid:
            json.dump(self._conf, fid)

    def load(self, filename):
        """
        Updates the setting from config file in JSON format.

        :param filename=None: filename of the local JSON settings file. If None, the local config file is used.
        """
        if filename is None:
            import datajoint as dj
            filename = dj.config['config.file']
        with open(filename, 'r') as fid:
            self.update(json.load(fid))


#############################################################################
logger = logging.getLogger()
logger.setLevel(logging.DEBUG) #set package wide logger level TODO:make this respond to environmental variable
