import os
import itertools
from .settings import config
from .errors import DataJointError
from .hash import long_bin_hash, to_ascii
from .table import Table
from .declare import EXTERNAL_TABLE_ROOT
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
        hash  : external_hash
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
        blob_hash = long_bin_hash(blob)
        ascii_hash = to_ascii(blob_hash)
        if self.spec['protocol'] == 'file':
            folder = os.path.join(self.spec['location'], self.database, *subfold(ascii_hash, self.spec['subfolding']))
            full_path = os.path.join(folder, ascii_hash)
            if not os.path.isfile(full_path):
                try:
                    safe_write(full_path, blob)
                except FileNotFoundError:
                    os.makedirs(folder)
                    safe_write(full_path, blob)
        elif self.spec['protocol'] == 's3':
            folder = '/'.join(subfold(ascii_hash, self.spec['subfolding']))
            s3.Folder(database=self.database, **self.spec).put('/'.join((folder, ascii_hash)), blob)
        else:
            assert False  # This won't happen
        # insert tracking info
        self.connection.query(
            "INSERT INTO {tab} (hash, size) VALUES (%s, {size}) "
            "ON DUPLICATE KEY UPDATE timestamp=CURRENT_TIMESTAMP".format(
                tab=self.full_table_name, size=len(blob)), args=(blob_hash,))
        return blob_hash

    def get(self, blob_hash):
        """
        get an object from external store.
        Does not need to check whether it's in the table.
        """
        if blob_hash is None:
            return None
        ascii_hash = to_ascii(blob_hash)

        # attempt to get object from cache
        blob = None
        cache_folder = config.get('cache', None)
        if cache_folder:
            try:
                with open(os.path.join(cache_folder, ascii_hash), 'rb') as f:
                    blob = f.read()
            except FileNotFoundError:
                pass

        # attempt to get object from store
        if blob is None:
            if self.spec['protocol'] == 'file':
                subfolders = os.path.join(*subfold(ascii_hash, self.spec['subfolding']))
                full_path = os.path.join(self.spec['location'], self.database, subfolders, ascii_hash)
                try:
                    with open(full_path, 'rb') as f:
                        blob = f.read()
                except FileNotFoundError:
                    raise DataJointError('Lost access to external blob %s.' % full_path) from None
            elif self.spec['protocol'] == 's3':
                try:
                    subfolder = '/'.join(subfold(ascii_hash, self.spec['subfolding']))
                    blob = s3.Folder(database=self.database, **self.spec).get('/'.join((subfolder, ascii_hash)))
                except TypeError:
                    raise DataJointError('External store {store} configuration is incomplete.'.format(store=self.store))
            else:
                raise DataJointError('Unknown external storage protocol "%s"' % self.spec['protocol'])

            if cache_folder:
                if not os.path.exists(cache_folder):
                    os.makedirs(cache_folder)
                safe_write(os.path.join(cache_folder, ascii_hash), blob)

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
        in_use = set(x for x in (self & '`hash` LIKE "%%{store}"'.format(store=store)).fetch('hash'))
        if self.spec['protocol'] == 'file':
            count = itertools.count()
            print('Deleting...')
            deleted_folders = set()
            for folder, dirs, files in os.walk(os.path.join(self.spec['location'], self.database), topdown=False):
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
        elif self.spec['protocol'] == 's3':
            try:
                failed_deletes = s3.Folder(database=self.database, **self.spec).clean(in_use, verbose=verbose)
            except TypeError:
                raise DataJointError('External store {store} configuration is incomplete.'.format(store=store))


class ExternalMapping:
    """
    The external manager contains all the tables for all external stores for a given schema
    :Example:
        e = ExternalMapping(schema)
        external_table = e[store]
    """
    def __init__(self, schema):
        self.schema = schema
        self.external_tables = {}

    def __getitem__(self, store):
        if store not in self.external_tables:
            self.external_tables[store] = ExternalTable(
                connection=self.schema.connection, store=store, database=self.schema.database)
        return self.external_tables[store]
