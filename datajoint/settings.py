"""
Settings for DataJoint.
"""
from . import DataJointError
import json
import pprint

__author__ = 'eywalker'
import logging
import collections


validators = collections.defaultdict(lambda: lambda value: True)

default = {
    'database.host': 'localhost',
    'database.password': 'datajoint',
    'database.user': 'datajoint',
    'database.port': 3306,
    #
    'connection.init_function': None,
    #
    'config.file': 'dj_local_conf.json',
    'config.varname': 'DJ_LOCAL_CONF'
}

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
