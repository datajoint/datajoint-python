"""
Settings for DataJoint.
"""
from contextlib import contextmanager
import json
import os
import pprint
from collections import OrderedDict
import logging
import collections
from enum import Enum
from . import DataJointError

LOCALCONFIG = 'dj_local_conf.json'
GLOBALCONFIG = '.datajoint_config.json'

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


server_error_codes = {
    'command denied': 1142,
    'tables does not exist': 1146,
    'syntax error': 1149
}


default = OrderedDict({
    'database.host': 'localhost',
    'database.password': None,
    'database.user': None,
    'database.port': 3306,
    'connection.init_function': None,
    'database.reconnect': False,
    'loglevel': 'INFO',
    'safemode': True,
    'display.limit': 7,
    'display.width': 14
})

logger = logging.getLogger(__name__)
log_levels = {
    'INFO': logging.INFO,
    'WARNING': logging.WARNING,
    'CRITICAL': logging.CRITICAL,
    'DEBUG': logging.DEBUG,
    'ERROR': logging.ERROR,
    None:  logging.NOTSET
}


class Config(collections.MutableMapping):

    instance = None

    def __init__(self, *args, **kwargs):
            if not Config.instance:
                Config.instance = Config.__Config(*args, **kwargs)
            else:
                Config.instance._conf.update(dict(*args, **kwargs))

    def __getattr__(self, name):
        return getattr(self.instance, name)

    def __getitem__(self, item):
        return self.instance.__getitem__(item)

    def __setitem__(self, item, value):
        self.instance.__setitem__(item, value)

    def __str__(self):
        return pprint.pformat(self.instance._conf, indent=4)

    def __repr__(self):
        return self.__str__()

    def __delitem__(self, key):
        del self.instance._conf[key]

    def __iter__(self):
        return iter(self.instance._conf)

    def __len__(self):
        return len(self.instance._conf)

    def save_local(self):
        """
        saves the settings in the local config file
        """
        self.save(LOCALCONFIG)

    def save_global(self):
        """
        saves the settings in the global config file
        """
        self.save(os.path.expanduser(os.path.join('~', GLOBALCONFIG)))

    @contextmanager
    def __call__(self, **kwargs):
        """
        The config object can also be used in a with statement to change the state of the configuration
        temporarily. kwargs to the context manager are the keys into config, where '.' is replaced by a
        double underscore '__'. The context manager yields the changed config object.

        Example:
        >>> import datajoint as dj
        >>> with dj.config(safemode=False, database__host="localhost") as cfg:
        >>>     # do dangerous stuff here
        """

        try:
            backup = self.instance
            self.instance = Config.__Config(self.instance._conf)
            new = {k.replace('__', '.'): v for k, v in kwargs.items()}
            self.instance._conf.update(new)
            yield self
        except:
            self.instance = backup
            raise
        else:
            self.instance = backup

    class __Config:
        """
        Stores datajoint settings. Behaves like a dictionary, but applies validator functions
        when certain keys are set.

        The default parameters are stored in datajoint.settings.default . If a local config file
        exists, the settings specified in this file override the default settings.
        """

        def __init__(self, *args, **kwargs):
            self._conf = dict(default)
            self._conf.update(dict(*args, **kwargs))  # use the free update to set keys

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

        def save(self, filename):
            """
            Saves the settings in JSON format to the given file path.
            :param filename: filename of the local JSON settings file.
            """
            with open(filename, 'w') as fid:
                json.dump(self._conf, fid, indent=4)

        def load(self, filename):
            """
            Updates the setting from config file in JSON format.
            :param filename: filename of the local JSON settings file. If None, the local config file is used.
            """
            if filename is None:
                filename = LOCALCONFIG
            with open(filename, 'r') as fid:
                self._conf.update(json.load(fid))




