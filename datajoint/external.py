import os
from . import config, DataJointError
from .hash import long_hash
from .blob import pack, unpack
from .base_relation import BaseRelation
from .declare import STORE_HASH_LENGTH, HASH_DATA_TYPE


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
        try:
            spec = config[store]
        except KeyError:
            raise DataJointError('Storage {store} is not configured'.format(store=store))

        # serialize object
        blob = pack(obj)
        hash = long_hash(blob) + store[len('external-'):]

        try:
            protocol = spec['protocol']
        except KeyError:
            raise DataJointError('Storage {store} config is missing the protocol field'.format(store=store))

        if protocol == 'file':
            folder = os.path.join(spec['location'], self.database)
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
            raise DataJointError('Unknown external storage protocol {protocol} for {store}'.format(
                store=store, protocol=protocol))

        # insert tracking info
        self.connection.query(
            "INSERT INTO {tab} (hash, size) VALUES ('{hash}', {size}) "
            "ON DUPLICATE KEY UPDATE timestamp=CURRENT_TIMESTAMP".format(
                tab=self.full_table_name,
                hash=hash,
                size=len(blob)))
        return hash

    def get(self, hash):
        """
        get an object from external store
        """
        store = hash[STORE_HASH_LENGTH:]
        store = 'external' + ('-' if store else '') + store
        try:
            spec = config[store]
        except KeyError:
            raise DataJointError('Store `%s` is not configured' % store)

        try:
            protocol = spec['protocol']
        except KeyError:
            raise DataJointError('Storage {store} config is missing the protocol field'.format(store=store))

        if protocol == 'file':
            full_path = os.path.join(spec['location'], self.database, hash)
            try:
                with open(full_path, 'rb') as f:
                    blob = f.read()
            except FileNotFoundError:
                    raise DataJointError('Lost external blob')
        else:
            raise DataJointError('Unknown external storage %s' % store)

        return unpack(blob)