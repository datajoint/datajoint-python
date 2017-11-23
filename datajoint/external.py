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
                'Store "{s}" incorrectly configured for "{n}"'.format(
                    store=store, storetype=storetype), 'missing', *missing)

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
        raise NotImplementedError('put method not implemented for', type(self))

    def get(self, hash):
        ''' returns 'obj' '''
        raise NotImplementedError('get method not implemented for', type(self))


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

    def get(self, hash):
        full_path = os.path.join(self.get_folder(), hash)
        try:
            with open(full_path, 'rb') as f:
                return unpack(f.read())
        except FileNotFoundError:
                raise DataJointError('Lost external blob')


class CacheFileHandler(ExternalFileHandler):
    '''
    A CacheFileHandler mixin implementation.
    Requires a concrete file-handling implementation to properly function.

    Should be 1st in inheritance list e.g.:

      CachedFoo(CacheFileHandler, OtherFileHandler)

    Will cache objects in 'cache_location', relies on superclass for
    coherent get/put operations on the 'reference' blobs.

    Cleanup currently not implemented.
    '''
    required = ('cache_location',)

    def __init__(self, store, database):
        super().__init__(store, database)
        self.check_required(store, 'cache', CacheFileHandler.required)
        # XXX: move ._location logic to base? currently duplicated
        self._location = self._spec['cache_location']

    def get_folder(self):
        return os.path.join(self._location, self._database)

    def _clean_cache(self):
        # TODO: implement _clean_cache
        pass

    def _put_cache(self, blob, hash):

        self._clean_cache()

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

    def put(self, obj):
        return self._put_cache(*super().put(obj))

    def get(self, hash):
        full_path = os.path.join(self.get_folder(), hash)

        try:
            with open(full_path, 'rb') as f:
                # TODO: indicate usage in cross-platform manner
                return unpack(f.read())
        except FileNotFoundError:
                pass

        # TODO/FIXME: inefficient - decodes->reencodes
        # to fix need to refactor other classes to allow blob access shortcut
        # return self._put_cache_obj(super().get(hash), hash)[1]
        obj = super().get(hash)

        (blob, nhash) = self._put_cache(self.hash_obj(obj))
        assert(hash == nhash)

        return obj


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
