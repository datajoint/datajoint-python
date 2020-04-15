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
from .errors import DataJointError

LOCALCONFIG = 'dj_local_conf.json'
GLOBALCONFIG = '.datajoint_config.json'
# subfolding for external storage in filesystem.
# 2, 2 means that file abcdef is stored as /ab/cd/abcdef
DEFAULT_SUBFOLDING = (2, 2)

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
prefix_to_role = dict(zip(role_to_prefix.values(), role_to_prefix))

default = OrderedDict({
    'database.host': 'localhost',
    'database.password': None,
    'database.user': None,
    'database.port': 3306,
    'database.reconnect': True,
    'connection.init_function': None,
    'connection.charset': '',   # pymysql uses '' as default
    'loglevel': 'INFO',
    'safemode': True,
    'fetch_format': 'array',
    'display.limit': 12,
    'display.width': 14,
    'display.show_tuple_count': True,
    'database.use_tls': None,
    'enable_python_native_blobs': False,  # python-native/dj0 encoding support
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

    def save(self, filename, verbose=False):
        """
        Saves the settings in JSON format to the given file path.
        :param filename: filename of the local JSON settings file.
        :param verbose: report having saved the settings file
        """
        with open(filename, 'w') as fid:
            json.dump(self._conf, fid, indent=4)
        if verbose:
            print('Saved settings in ' + filename)

    def load(self, filename):
        """
        Updates the setting from config file in JSON format.
        :param filename: filename of the local JSON settings file. If None, the local config file is used.
        """
        if filename is None:
            filename = LOCALCONFIG
        with open(filename, 'r') as fid:
            self._conf.update(json.load(fid))

    def save_local(self, verbose=False):
        """
        saves the settings in the local config file
        """
        self.save(LOCALCONFIG, verbose)

    def save_global(self, verbose=False):
        """
        saves the settings in the global config file
        """
        self.save(os.path.expanduser(os.path.join('~', GLOBALCONFIG)), verbose)

    def get_store_spec(self, store):
        """
        find configuration of external stores for blobs and attachments
        """
        try:
            spec = self['stores'][store]
        except KeyError:
            raise DataJointError('Storage {store} is requested but not configured'.format(store=store)) from None

        spec['subfolding'] = spec.get('subfolding', DEFAULT_SUBFOLDING)
        spec_keys = {  # REQUIRED in uppercase and allowed in lowercase
            'file': ('PROTOCOL', 'LOCATION', 'subfolding', 'stage'),
            's3': ('PROTOCOL', 'ENDPOINT', 'BUCKET', 'ACCESS_KEY', 'SECRET_KEY', 'LOCATION', 'secure', 'subfolding', 'stage')}

        try:
            spec_keys = spec_keys[spec.get('protocol', '').lower()]
        except KeyError:
            raise DataJointError(
                'Missing or invalid protocol in dj.config["stores"]["{store}"]'.format(store=store)) from None

        # check that all required keys are present in spec
        try:
            raise DataJointError('dj.config["stores"]["{store}"] is missing "{k}"'.format(
                store=store, k=next(k.lower() for k in spec_keys if k.isupper() and k.lower() not in spec)))
        except StopIteration:
            pass

        # check that only allowed keys are present in spec
        try:
            raise DataJointError('Invalid key "{k}" in dj.config["stores"]["{store}"]'.format(
                store=store, k=next(k for k in spec if k.upper() not in spec_keys and k.lower() not in spec_keys)))
        except StopIteration:
            pass  # no invalid keys

        return spec

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
            if validators[key](value):
                self._conf[key] = value
            else:
                raise DataJointError(u'Validator for {0:s} did not pass'.format(key))


# Load configuration from file
config = Config()
config_files = (os.path.expanduser(n) for n in (LOCALCONFIG, os.path.join('~', GLOBALCONFIG)))
try:
    config_file = next(n for n in config_files if os.path.exists(n))
except StopIteration:
    pass
else:
    config.load(config_file)

# override login credentials with environment variables
mapping = {k: v for k, v in zip(
    ('database.host', 'database.user', 'database.password',
     'external.aws_access_key_id', 'external.aws_secret_access_key',),
    map(os.getenv, ('DJ_HOST', 'DJ_USER', 'DJ_PASS',
                    'DJ_AWS_ACCESS_KEY_ID', 'DJ_AWS_SECRET_ACCESS_KEY',)))
           if v is not None}
config.update(mapping)

logger.setLevel(log_levels[config['loglevel']])
