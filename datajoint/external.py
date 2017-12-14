import os
from tqdm import tqdm
from . import config, DataJointError
from .hash import long_hash
from .blob import pack, unpack
from .base_relation import BaseRelation
from .declare import STORE_HASH_LENGTH, HASH_DATA_TYPE


def safe_write(filename, blob):
    """
    A two-step write.
    :param filename: full path
    :param blob: binary data
    :return: None
    """
    temp_file = filename + '.saving'
    with open(temp_file, 'bw') as f:
        f.write(blob)
    os.rename(temp_file, filename)


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
        spec = self._get_store_spec(store)
        blob = pack(obj)
        blob_hash = long_hash(blob) + store[len('external-'):]
        if spec['protocol'] == 'file':
            folder = os.path.join(spec['location'], self.database)
            full_path = os.path.join(folder, blob_hash)
            if not os.path.isfile(full_path):
                try:
                    safe_write(full_path, blob)
                except FileNotFoundError:
                    os.makedirs(folder)
                    safe_write(full_path, blob)
        else:
            raise DataJointError('Unknown external storage protocol {protocol} for {store}'.format(
                store=store, protocol=protocol))

        # insert tracking info
        self.connection.query(
            "INSERT INTO {tab} (hash, size) VALUES ('{hash}', {size}) "
            "ON DUPLICATE KEY UPDATE timestamp=CURRENT_TIMESTAMP".format(
                tab=self.full_table_name,
                hash=blob_hash,
                size=len(blob)))
        return blob_hash

    def get(self, blob_hash):
        """
        get an object from external store.
        Does not need to check whether it's in the table.
        """
        store = blob_hash[STORE_HASH_LENGTH:]
        store = 'external' + ('-' if store else '') + store
        cache_file = os.path.join(config['cache'], blob_hash) if 'cache' in config and config['cache'] else None

        blob = None
        if cache_file:
            try:
                with open(cache_file, 'rb') as f:
                    blob = f.read()
            except FileNotFoundError:
                pass

        if blob is None:
            spec = self._get_store_spec(store)
            if spec['protocol'] == 'file':
                full_path = os.path.join(spec['location'], self.database, blob_hash)
                try:
                    with open(full_path, 'rb') as f:
                        blob = f.read()
                except FileNotFoundError:
                    raise DataJointError('Lost external blob %s.' % full_path) from None
            else:
                raise DataJointError('Unknown external storage protocol "%s"' % self['protocol'])

            if cache_file:
                safe_write(cache_file, blob)

        return unpack(blob)

    @property
    def references(self):
        """
        return the list of referencing tables and their referencing columns
        :return:
        """
        return self.connection.query("""
        SELECT concat('`', table_schema, '`.`', table_name, '`') as referencing_table, column_name
        FROM information_schema.key_column_usage
        WHERE referenced_table_name="{tab}" and referenced_table_schema="{db}"
        """.format(tab=self.table_name, db=self.database), as_dict=True)

    @property
    def garbage_count(self):
        """
        :return: number of items that are no longer referenced
        """
        return self.connection.query(
            "SELECT COUNT(*) FROM `{db}`.`{tab}` WHERE ".format(tab=self.table_name, db=self.database) +
            (" AND ".join('hash NOT IN (SELECT {column_name} FROM {referencing_table})'.format(**ref)
                          for ref in self.references) or "TRUE")).fetchone()[0]

    def delete(self):
        return self.delete_quick()

    def delete_quick(self):
        raise DataJointError('The external table does not support delete. Please use delete_garbage instead.')

    def drop(self):
        """drop the table"""
        self.drop_quick()

    def drop_quick(self):
        """drop the external table -- works only when it's empty"""
        if self:
            raise DataJointError('Cannot non-empty external table. Please use delete_garabge to clear it.')
        self.drop_quick()

    def delete_garbage(self):
        """
        Delete items that are no longer referenced.
        This operation is safe to perform at any time.
        """
        self.connection.query(
            "DELETE FROM `{db}`.`{tab}` WHERE ".format(tab=self.table_name, db=self.database) +
            " AND ".join(
                'hash NOT IN (SELECT {column_name} FROM {referencing_table})'.format(**ref)
                for ref in self.references) or "TRUE")
        print('Deleted %d items' % self.connection.query("SELECT ROW_COUNT()").fetchone()[0])

    def clean_store(self, store, display_progress=True):
        """
        Clean unused data in an external storage repository from unused blobs.
        This must be performed after delete_garbage during low-usage periods to reduce risks of data loss.
        """
        spec = self._get_store_spec(store)
        progress = tqdm if display_progress else lambda x: x
        if spec['protocol'] == 'file':
            folder = os.path.join(spec['location'], self.database)
            delete_list = set(os.listdir(folder)).difference(self.fetch('hash'))
            print('Deleting %d unused items from %s' % (len(delete_list), folder), flush=True)
            for f in progress(delete_list):
                os.remove(os.path.join(folder, f))
        else:
            raise DataJointError('Unknown external storage protocol {protocol} for {store}'.format(
                store=store, protocol=self['protocol']))

    @staticmethod
    def _get_store_spec(store):
        try:
            spec = config[store]
        except KeyError:
            raise DataJointError('Storage {store} is not configured'.format(store=store)) from None
        if 'protocol' not in spec:
            raise DataJointError('Storage {store} config is missing the protocol field'.format(store=store))
        return spec

