import os
import itertools
from collections import Mapping
from .settings import config
from .errors import DataJointError
from .hash import uuid_from_buffer
from .table import Table
from .declare import EXTERNAL_TABLE_ROOT
from . import s3 
from .utils import safe_write

CACHE_SUBFOLDING = (2, 2)   # (2, 2) means  "0123456789abcd" will be saved as "01/23/0123456789abcd"


def subfold(name, folds):
    """
    subfolding for external storage:   e.g.  subfold('aBCdefg', (2, 3))  -->  ['ab','cde']
    """
    return (name[:folds[0]].lower(),) + subfold(name[folds[0]:], folds[1:]) if folds else ()


class ExternalTable(Table):
    """
    The table tracking externally stored objects.
    Declare as ExternalTable(connection, database)
    """
    def __init__(self, connection, store=None, database=None):

        # copy constructor -- all QueryExpressions must provide
        if isinstance(connection, ExternalTable):
            other = connection   # the first argument is interpreted as the other object
            super().__init__(other)
            self.store = other.store
            self.spec = other.spec
            self.database = other.database
            self._connection = other._connection
            return

        # nominal constructor
        super().__init__()
        self.store = store
        self.spec = config.get_store_spec(store)
        self.database = database
        self._connection = connection
        if not self.is_declared:
            self.declare()

    @property
    def definition(self):
        return """
        # external storage tracking
        hash  : uuid
        ---
        size      :bigint unsigned   # size of object in bytes
        timestamp=CURRENT_TIMESTAMP  :timestamp   # automatic timestamp
        """

    @property
    def table_name(self):
        return '{external_table_root}_{store}'.format(external_table_root=EXTERNAL_TABLE_ROOT, store=self.store)

    def put(self, blob):
        """
        put an object in external store
        """
        blob_hash = uuid_from_buffer(blob)
        if self.spec['protocol'] == 'file':
            folder = os.path.join(self.spec['location'], self.database, *subfold(blob_hash.hex, self.spec['subfolding']))
            full_path = os.path.join(folder, blob_hash.hex)
            if not os.path.isfile(full_path):
                try:
                    safe_write(full_path, blob)
                except FileNotFoundError:
                    os.makedirs(folder)
                    safe_write(full_path, blob)
        elif self.spec['protocol'] == 's3':
            folder = '/'.join(subfold(blob_hash.hex, self.spec['subfolding']))
            s3.Folder(database=self.database, **self.spec).put('/'.join((folder, blob_hash.hex)), blob)

        # insert tracking info
        self.connection.query(
            "INSERT INTO {tab} (hash, size) VALUES (%s, {size}) "
            "ON DUPLICATE KEY UPDATE timestamp=CURRENT_TIMESTAMP".format(
                tab=self.full_table_name, size=len(blob)), args=(blob_hash.bytes,))
        return blob_hash

    def peek(self, blob_hash, bytes_to_peek=120):
        return self.get(blob_hash, size=bytes_to_peek)

    def get(self, blob_hash, size=-1):
        """
        get an object from external store.
        :param size: max number of bytes to retrieve. If size<0, retrieve entire blob
        """
        if blob_hash is None:
            return None

        # attempt to get object from cache
        blob = None
        cache_folder = config.get('cache', None)
        blob_size = None
        if cache_folder:
            try:
                cache_path = os.path.join(cache_folder, *subfold(blob_hash.hex, CACHE_SUBFOLDING))
                cache_file = os.path.join(cache_path, blob_hash.hex)
                with open(cache_file, 'rb') as f:
                    blob = f.read(size)
            except FileNotFoundError:
                pass
            else:
                if size > 0:
                    blob_size = os.path.getsize(cache_file)

        # attempt to get object from store
        if blob is None:
            if self.spec['protocol'] == 'file':
                subfolders = os.path.join(*subfold(blob_hash.hex, self.spec['subfolding']))
                full_path = os.path.join(self.spec['location'], self.database, subfolders, blob_hash.hex)
                try:
                    with open(full_path, 'rb') as f:
                        blob = f.read(size)
                except FileNotFoundError:
                    raise DataJointError('Lost access to external blob %s.' % full_path) from None
                else:
                    if size > 0:
                        blob_size = os.path.getsize(full_path)
            elif self.spec['protocol'] == 's3':
                try:
                    full_path = '/'.join(('/'.join(subfold(blob_hash.hex, self.spec['subfolding'])), blob_hash.hex))
                    s3_folder = s3.Folder(database=self.database, **self.spec)
                except TypeError:
                    raise DataJointError('External store {store} configuration is incomplete.'.format(store=self.store))
                else:
                    if size < 0:
                        blob = s3_folder.get(full_path) 
                    else:
                        blob = s3_folder.partial_get(full_path, 0, size)
                        blob_size = s3_folder.get_size(full_path)
            else:
                raise DataJointError('Unknown external storage protocol "%s"' % self.spec['protocol'])

            if cache_folder and size < 0:
                if not os.path.exists(cache_path):
                    os.makedirs(cache_path)
                safe_write(os.path.join(cache_path, blob_hash.hex), blob)

        return blob if size < 0 else (blob, blob_size)

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

    def delete_quick(self):
        raise DataJointError('The external table does not support delete_quick. Please use delete instead.')

    def delete(self):
        """
        Delete items that are no longer referenced.
        This operation is safe to perform at any time but may reduce performance of queries while in progress.
        """
        self.connection.query(
            "DELETE FROM `{db}`.`{tab}` WHERE ".format(tab=self.table_name, db=self.database) + (
                    " AND ".join(
                        'hash NOT IN (SELECT {column_name} FROM {referencing_table})'.format(**ref)
                        for ref in self.references) or "TRUE"))
        print('Deleted %d items' % self.connection.query("SELECT ROW_COUNT()").fetchone()[0])

    def clean(self, verbose=True):
        """
        Clean unused data in an external storage repository from unused blobs.
        This must be performed after external_table.delete() during low-usage periods to 
        reduce risks of data loss.
        """
        in_use = set(x.hex for x in self.fetch('hash'))
        if self.spec['protocol'] == 'file':
            count = itertools.count()
            print('Deleting...')
            deleted_folders = set()
            for folder, dirs, files in os.walk(
                    os.path.join(self.spec['location'], self.database), topdown=False):
                if dirs and files:
                    raise DataJointError(
                            'Invalid repository with files in non-terminal folder %s' % folder)
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
        elif self.spec['protocol'] == 's3':
            try:
                failed_deletes = s3.Folder(database=self.database, **self.spec).clean(in_use, verbose=verbose)
            except TypeError:
                raise DataJointError('External store {store} configuration is incomplete.'.format(store=self.store))


class ExternalMapping(Mapping):
    """
    The external manager contains all the tables for all external stores for a given schema
    :Example:
        e = ExternalMapping(schema)
        external_table = e[store]
    """
    def __init__(self, schema):
        self.schema = schema
        self._tables = {}

    def __getitem__(self, store):
        """
        Triggers the creation of an external table.
        Should only be used when ready to save or read from external storage.
        :param store: the name of the store
        :return: the ExternalTable object for the store
        """
        if store not in self._tables:
            self._tables[store] = ExternalTable(
                connection=self.schema.connection, store=store, database=self.schema.database)
        return self._tables[store]

    def __len__(self):
        return len(self._tables)
    
    def __iter__(self):
        return iter(self._tables)
