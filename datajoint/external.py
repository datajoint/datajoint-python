import os
import itertools
from .settings import config
from .errors import DataJointError
from .hash import long_hash
from .table import Table
from .declare import STORE_HASH_LENGTH, HASH_DATA_TYPE
from . import s3 
from .utils import safe_write


def subfold(name, folds):
    """
    subfolding for external storage:   e.g.  subfold('abcdefg', (2, 3))  -->  ['ab','cde']
    """
    return (name[:folds[0]].lower(),) + subfold(name[folds[0]:], folds[1:]) if folds else ()


class ExternalTable(Table):
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

    def put(self, store, blob):
        """
        put an object in external store
        """
        store = ''.join(store.split('-')[1:])
        spec = config.get_store_spec(store)
        blob_hash = long_hash(blob) + store
        if spec['protocol'] == 'file':
            folder = os.path.join(spec['location'], self.database, *subfold(blob_hash, spec['subfolding']))
            full_path = os.path.join(folder, blob_hash)
            if not os.path.isfile(full_path):
                try:
                    safe_write(full_path, blob)
                except FileNotFoundError:
                    os.makedirs(folder)
                    safe_write(full_path, blob)
        elif spec['protocol'] == 's3':
            folder = '/'.join(subfold(blob_hash, spec['subfolding']))
            s3.Folder(database=self.database, **spec).put('/'.join((folder, blob_hash)), blob)
        else:
            raise DataJointError('Unknown external storage protocol {protocol} in store "-{store}"'.format(
                store=store, protocol=spec['protocol']))

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
        if blob_hash is None:
            return None

        # attempt to get object from cache
        blob = None
        cache_folder = config.get('cache', None)
        if cache_folder:
            try:
                with open(os.path.join(cache_folder, blob_hash), 'rb') as f:
                    blob = f.read()
            except FileNotFoundError:
                pass

        # attempt to get object from store
        if blob is None:
            store = blob_hash[STORE_HASH_LENGTH:]
            spec = config.get_store_spec(store)
            if spec['protocol'] == 'file':
                subfolders = os.path.join(*subfold(blob_hash, spec['subfolding']))
                full_path = os.path.join(spec['location'], self.database, subfolders, blob_hash)
                try:
                    with open(full_path, 'rb') as f:
                        blob = f.read()
                except FileNotFoundError:
                    raise DataJointError('Lost access to external blob %s.' % full_path) from None
            elif spec['protocol'] == 's3':
                try:
                    subfolder = '/'.join(subfold(blob_hash, spec['subfolding']))
                    blob = s3.Folder(database=self.database, **spec).get('/'.join((subfolder, blob_hash)))
                except TypeError:
                    raise DataJointError('External store {store} configuration is incomplete.'.format(store=store))
            else:
                raise DataJointError('Unknown external storage protocol "%s"' % spec['protocol'])

            if cache_folder:
                if not os.path.exists(cache_folder):
                    os.makedirs(cache_folder)
                safe_write(os.path.join(cache_folder, blob_hash), blob)

        return blob

    @property
    def references(self):
        """
        :return: generator of referencing table names and their referencing columns
        """
        return self.connection.query("""
        SELECT concat('`', table_schema, '`.`', table_name, '`') as referencing_table, column_name
        FROM information_schema.key_column_usage
        WHERE referenced_table_name="{tab}" and referenced_table_schema="{db}"
        """.format(tab=self.table_name, db=self.database), as_dict=True)

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
            raise DataJointError('Cannot drop a non-empty external table. Please use delete_garabge to clear it.')
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

    def clean_store(self, store, verbose=True):
        """
        Clean unused data in an external storage repository from unused blobs.
        This must be performed after delete_garbage during low-usage periods to reduce risks of data loss.
        """
        spec = config.get_store_spec(store)
        in_use = set(x for x in (self & '`hash` LIKE "%%{store}"'.format(store=store)).fetch('hash'))
        if spec['protocol'] == 'file':
            count = itertools.count()
            print('Deleting...')
            deleted_folders = set()
            for folder, dirs, files in os.walk(os.path.join(spec['location'], self.database), topdown=False):
                if dirs and files:
                    raise DataJointError('Invalid repository with files in non-terminal folder %s' % folder)
                dirs = set(d for d in dirs if os.path.join(folder, d) not in deleted_folders)
                if not dirs:
                    files_not_in_use = [f for f in files if f not in in_use]
                    for f in files_not_in_use:
                        filename = os.path.join(folder, f)
                        next(count)
                        if verbose:
                            print(filename)
                        os.remove(filename)
                    if len(files_not_in_use) == len(files):
                        os.rmdir(folder)
                        deleted_folders.add(folder)
            print('Deleted %d objects' % next(count))
        elif spec['protocol'] == 's3':
            try:
                failed_deletes = s3.Folder(database=self.database, **spec).clean(in_use, verbose=verbose)
            except TypeError:
                raise DataJointError('External store {store} configuration is incomplete.'.format(store=store))
