import os
from . import config, DataJointError
from .hash import long_hash
from .blob import pack, unpack
from .base_relation import BaseRelation
from .declare import STORE_HASH_LENGTH, HASH_DATA_TYPE


class ExternalFileHandler:

    _handlers = {}  # handler mapping - statically initialized below.

    def __init__(self, store, database):
        self._store = store
        self._database = database
        self._spec = config[store]

    @classmethod
    def get_handler(cls, store, database):

        if store not in config:
            raise DataJointError(
                'Store "{store}" is not configured'.format(store=store))

        spec = config[store]

        if 'protocol' not in spec:
            raise DataJointError(
                'Store "{store}" config is missing the protocol field'
                .format(store=store))

        protocol = spec['protocol']

        if protocol not in cls._handlers:
            raise DataJointError(
                'Unknown external storage protocol "{protocol}" for "{store}"'
                .format(store=store, protocol=protocol))

        return cls._handlers[protocol](store, database)

    def check_required(self, store, storetype, required):

        missing = list(i for i in required if i not in self._spec)

        if len(missing):
            raise DataJointError(
                'Store "{s}" incorrectly configured for "{t}", missing: {m}'
                .format(s=store, t=storetype, m=missing))

    def hash_obj(self, obj):
        blob = pack(obj)
        hash = long_hash(blob) + self._store[len('external-'):]
        return (blob, hash)

    @staticmethod
    def hash_to_store(hash):
        store = hash[STORE_HASH_LENGTH:]
        return 'external' + ('-' if store else '') + store

    def put(self, obj):
        ''' returns (blob, hash) '''
        raise NotImplementedError(
            'put method not implemented for', type(self))

    def get_blob(self, hash):
        ''' returns undecoded 'blob' '''
        raise NotImplementedError(
            'get_blob method not implemented for', type(self))

    def get_obj(self, hash):
        ''' returns decoded 'obj' '''
        raise NotImplementedError(
            'get_obj method not implemented for', type(self))

    def get(self, hash):
        ''' returns decoded 'obj' '''
        return self.get_obj(hash)


class RawFileHandler(ExternalFileHandler):

    required = ('location',)

    def __init__(self, store, database):
        super().__init__(store, database)
        self.check_required(store, 's3', RawFileHandler.required)
        self._location = self._spec['location']

    def get_folder(self):
        return os.path.join(self._location, self._database)

    def put(self, obj):
        (blob, hash) = self.hash_obj(obj)

        folder = self.get_folder()
        full_path = os.path.join(folder, hash)
        if not os.path.isfile(full_path):
            try:
                with open(full_path, 'wb') as f:
                    f.write(blob)
            except FileNotFoundError:
                os.makedirs(folder)
                with open(full_path, 'wb') as f:
                    f.write(blob)

        return (blob, hash)

    def get_blob(self, hash):
        full_path = os.path.join(self.get_folder(), hash)
        try:
            with open(full_path, 'rb') as f:
                return f.read()
        except FileNotFoundError:
                raise DataJointError('Lost external blob')

    def get_obj(self, hash):
        return unpack(self.get_blob(hash))

    def get(self, hash):
        return self.get_obj(hash)


class CacheFileHandler(ExternalFileHandler):
    '''
    A CacheFileHandler mixin implementation.

    Requires a concrete 'upstream' backend file handling implementation.
    Should be 1st in inheritance list w/r/t backend implementation - e.g.:

      CachedFoo(CacheFileHandler, OtherFileHandler)

    Will cache objects in config['external-cache']['location'], relying on
    superclass for coherent get/put operations on the 'reference' blobs.

    Cleanup currently not implemented.
    '''

    def __init__(self, store, database):
        super().__init__(store, database)  # initialize non-mixin parameters

        # validate mixin cache parameters
        if 'external-cache' not in config:
            raise DataJointError('External Cache is not configured')

        cache_spec = config['external-cache']

        if 'location' not in cache_spec:
            raise DataJointError(
                'External Cache configuration missing "location"')

        self._cache_spec = cache_spec
        self._cache_location = cache_spec['location']

    def get_cache_folder(self):
        return os.path.join(self._cache_location, self._database)

    def _clean_cache(self):
        pass  # TODO: implement _clean_cache

    def _put_cache(self, blob, hash):

        self._clean_cache()

        folder = self.get_cache_folder()
        full_path = os.path.join(folder, hash)

        if not os.path.isfile(full_path):
            try:
                with open(full_path, 'wb') as f:
                    f.write(blob)
            except FileNotFoundError:
                os.makedirs(folder)
                with open(full_path, 'wb') as f:
                    f.write(blob)
        else:
            pass  # TODO: track recent file usage to assist _clean_cache

        return (blob, hash)

    def put(self, obj):
        return self._put_cache(*super().put(obj))

    def get_blob(self, hash):
        blob = super().get_blob(hash)
        self._put_cache(blob, hash)
        return blob

    def get_obj(self, hash):
        return unpack(self.get_blob(hash))

    def get(self, hash):
        return self.get_obj(hash)


class CachedRawFileHandler(CacheFileHandler, RawFileHandler):
    pass


ExternalFileHandler._handlers = {
    'file': RawFileHandler,
    'cache': CacheFileHandler,
    'cache-file': CachedRawFileHandler,
}


class ExternalTable(BaseRelation):
    """
    The table tracking externally stored objects.
    Declare as ExternalTable(connection, database)
    """
    def __init__(self, arg, database=None):
        if isinstance(arg, ExternalTable):
            super().__init__(arg)
            # copy constructor
            self.database = arg.database
            self._connection = arg._connection
            return
        super().__init__()
        self.database = database
        self._connection = arg
        if not self.is_declared:
            self.declare()

    @property
    def definition(self):
        return """
        # external storage tracking
        hash  : {hash_data_type}  # the hash of stored object + store name
        ---
        size      :bigint unsigned   # size of object in bytes
        timestamp=CURRENT_TIMESTAMP  :timestamp   # automatic timestamp
        """.format(hash_data_type=HASH_DATA_TYPE)

    @property
    def table_name(self):
        return '~external'

    def put(self, store, obj):
        """
        put an object in external store
        """
        (blob, hash) = ExternalFileHandler.get_handler(
            store, self.database).put(obj)

        # insert tracking info
        self.connection.query(
            "INSERT INTO {tab} (hash, size) VALUES ('{hash}', {size}) "
            "ON DUPLICATE KEY UPDATE timestamp=CURRENT_TIMESTAMP".format(
                tab=self.full_table_name, hash=hash, size=len(blob)))

        return hash

    def get(self, hash):
        """
        get an object from external store
        """
        return ExternalFileHandler.get_handler(
            ExternalFileHandler.hash_to_store(hash), self.database).get(hash)
