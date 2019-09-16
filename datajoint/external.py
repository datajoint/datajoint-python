import itertools
from pathlib import Path
from collections import Mapping
from .settings import config
from .errors import DataJointError, MissingExternalFile
from .hash import uuid_from_buffer, uuid_from_file
from .table import Table
from .declare import EXTERNAL_TABLE_ROOT
from . import s3 
from .utils import safe_write, safe_copy

CACHE_SUBFOLDING = (2, 2)   # (2, 2) means  "0123456789abcd" will be saved as "01/23/0123456789abcd"
SUPPORT_MIGRATED_BLOBS = True   # support blobs migrated from datajoint 0.11.*


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

        self._s3 = None

    @property
    def definition(self):
        return """
        # external storage tracking
        hash  : uuid    #  hash of contents (blob), of filename + contents (attach), or relative filepath (filepath)
        ---
        size      :bigint unsigned     # size of object in bytes
        filepath=null : varchar(1000)  # relative filepath used in the filepath datatype
        contents_hash=null : uuid      # used for the filepath datatype 
        timestamp=CURRENT_TIMESTAMP  :timestamp   # automatic timestamp
        """

    @property
    def table_name(self):
        return '{external_table_root}_{store}'.format(external_table_root=EXTERNAL_TABLE_ROOT, store=self.store)

    @property
    def s3(self):
        if self._s3 is None:
            self._s3 = s3.Folder(**self.spec)
        return self._s3

    def upload_file(self, local_path, remote_path, metadata=None):
        if self.spec['protocol'] == 's3':
            self.s3.fput(remote_path, local_path, metadata)
        else:
            safe_copy(local_path, Path(self.spec['location']) / remote_path, overwrite=True)

    # --- BLOBS ----

    def put(self, blob):
        """
        put a binary string in external store
        """
        uuid = uuid_from_buffer(blob)
        if self.spec['protocol'] == 's3':
            self.s3.put('/'.join((self.database, '/'.join(subfold(uuid.hex, self.spec['subfolding'])), uuid.hex)), blob)
        else:
            remote_file = Path(Path(
                self.spec['location'], self.database, *subfold(uuid.hex, self.spec['subfolding'])), uuid.hex)
            safe_write(remote_file, blob)
        # insert tracking info
        self.connection.query(
            "INSERT INTO {tab} (hash, size) VALUES (%s, {size}) ON DUPLICATE KEY "
            "UPDATE timestamp=CURRENT_TIMESTAMP".format(
                tab=self.full_table_name, size=len(blob)), args=(uuid.bytes,))
        return uuid

    def get(self, blob_hash):
        """
        get an object from external store.
        """
        if blob_hash is None:
            return None
        # attempt to get object from cache
        blob = None
        cache_folder = config.get('cache', None)
        if cache_folder:
            try:
                cache_path = Path(cache_folder, *subfold(blob_hash.hex, CACHE_SUBFOLDING))
                cache_file = Path(cache_path, blob_hash.hex)
                blob = cache_file.read_bytes()
            except FileNotFoundError:
                pass  # not cached
        # attempt to get object from store
        if blob is None:
            if self.spec['protocol'] == 'file':
                subfolders = Path(*subfold(blob_hash.hex, self.spec['subfolding']))
                full_path = Path(self.spec['location'], self.database, subfolders, blob_hash.hex)
                try:
                    blob = full_path.read_bytes()
                except MissingExternalFile:
                    if not SUPPORT_MIGRATED_BLOBS:
                        raise MissingExternalFile("Missing blob file " + full_path) from None
                    # migrated blobs from 0.11
                    relative_filepath, contents_hash = (self & {'hash': blob_hash}).fetch1(
                        'filepath', 'contents_hash')
                    if relative_filepath is None:
                        raise MissingExternalFile("Missing blob file " + full_path) from None
                    stored_path = Path(self.spec['location'], relative_filepath)
                    try:
                        blob = stored_path.read_bytes()
                    except FileNotFoundError:
                        raise MissingExternalFile("Missing blob file " + stored_path)
            elif self.spec['protocol'] == 's3':
                full_path = '/'.join(
                    (self.database,) + subfold(blob_hash.hex, self.spec['subfolding']) + (blob_hash.hex,))
                try:
                    blob = self.s3.get(full_path)
                except MissingExternalFile:
                    if not SUPPORT_MIGRATED_BLOBS:
                        raise
                    relative_filepath, contents_hash = (self & {'hash': blob_hash}).fetch1(
                        'filepath', 'contents_hash')
                    if relative_filepath is None:
                        raise
                    blob = self.s3.get(relative_filepath)
            if cache_folder:
                cache_path.mkdir(parents=True, exist_ok=True)
                safe_write(cache_path / blob_hash.hex, blob)
        return blob

    # --- ATTACHMENTS ---

    def upload_attachment(self, local_path):
        basename = Path(local_path).name
        uuid = uuid_from_file(local_path, init_string=basename + '\0')
        remote_path = '/'.join((
            self.database, '/'.join(subfold(uuid.hex, self.spec['subfolding'])),  uuid.hex + '-' + basename))
        self.upload_file(local_path, remote_path)
        # insert tracking info
        self.connection.query(
            "INSERT INTO {tab} (hash, size) VALUES (%s, {size}) ON DUPLICATE KEY "
            "UPDATE timestamp=CURRENT_TIMESTAMP".format(
                tab=self.full_table_name, size=Path(local_path).stat().st_size), args=[uuid.bytes])
        return uuid

    def get_attachment_basename(self, uuid):
        """
        get the original filename, stripping the checksum
        """
        remote_path = '/'.join((self.database, '/'.join(subfold(uuid.hex, self.spec['subfolding']))))
        name_generator = (
            Path(self.spec['location'], remote_path).glob(uuid.hex + '-*') if self.spec['protocol'] == 'file'
            else (obj.object_name for obj in self.s3.list_objects(remote_path) if uuid.hex in obj.object_name))
        try:
            attachment_filename = next(name_generator)
        except StopIteration:
            raise MissingExternalFile('Missing attachment {protocol}://{path}'.format(
                path=remote_path + '/' + uuid.hex + '-*', **self.spec))
        return attachment_filename.split(uuid.hex + '-')[-1]

    def download_attachment(self, uuid, basename, download_path):
        """ save attachment from memory buffer into the save_path """
        remote_path = '/'.join([
            self.database, '/'.join(subfold(uuid.hex, self.spec['subfolding'])), uuid.hex + '-' + basename])
        if self.spec['protocol'] == 's3':
            self.s3.fget(remote_path, download_path)
        else:
            safe_copy(Path(self.spec['location']) / remote_path, download_path)

    # --- FILEPATH ---

    def upload_filepath(self, local_filepath):
        """
        Raise exception if an external entry already exists with a different contents checksum.
        Otherwise, copy (with overwrite) file to remote and
        If an external entry exists with the same checksum, then no copying should occur
        """
        local_filepath = Path(local_filepath)
        local_folder = local_filepath.parent
        try:
            relative_filepath = str(local_filepath.relative_to(self.spec['stage']))
        except:
            raise DataJointError('The path {path} is not in stage {stage}'.format(path=local_folder, **self.spec))
        uuid = uuid_from_buffer(init_string=relative_filepath)
        contents_hash = uuid_from_file(local_filepath)

        # check if the remote file already exists and verify that it matches
        check_hash = (self & {'hash': uuid}).fetch('contents_hash')
        if check_hash:
            # the tracking entry exists, check that it's the same file as before
            if contents_hash != check_hash[0]:
                raise DataJointError(
                    "A different version of '{file}' has already been placed.".format(file=relative_filepath))
        else:
            # upload the file and create its tracking entry
            self.upload_file(local_filepath, relative_filepath, metadata={'contents_hash': str(contents_hash)})
            self.connection.query(
                "INSERT INTO {tab} (hash, size, filepath, contents_hash) VALUES (%s, {size}, '{filepath}', %s)".format(
                    tab=self.full_table_name, size=Path(local_filepath).stat().st_size,
                    filepath=relative_filepath), args=(uuid.bytes, contents_hash.bytes))
        return uuid

    def download_filepath(self, filepath_hash):
        """
        sync a file from external store to the local stage
        :param filepath_hash: The hash (UUID) of the relative_path
        :return: hash (UUID) of the contents of the downloaded file or Nones
        """
        if filepath_hash is not None:
            relative_filepath, contents_hash = (self & {'hash': filepath_hash}).fetch1('filepath', 'contents_hash')
            local_filepath = Path(self.spec['stage']).absolute() / Path(relative_filepath)
            file_exists = Path(local_filepath).is_file() and uuid_from_file(local_filepath) == contents_hash
            if not file_exists:
                if self.spec['protocol'] == 's3':
                    checksum = s3.Folder(**self.spec).fget(relative_filepath, local_filepath)
                else:
                    remote_file = Path(self.spec['location'], relative_filepath)
                    safe_copy(remote_file, local_filepath)
                    checksum = uuid_from_file(local_filepath)
                if checksum != contents_hash:  # this should never happen without outside interference
                    raise DataJointError("'{file}' downloaded but did not pass checksum'".format(file=local_filepath))
            return local_filepath, contents_hash

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
                        'hash NOT IN (SELECT `{column_name}` FROM {referencing_table})'.format(**ref)
                        for ref in self.references) or "TRUE"))
        print('Deleted %d items' % self.connection.query("SELECT ROW_COUNT()").fetchone()[0])

    def get_untracked_external_files(self, *, limit=None,
                                     include_blobs=True, include_attachments=True, include_filepaths=True):
        """
        :return: generate the absolute paths to external blobs, attachments, and filepaths that are no longer
        tracked by this external table.
        Caution: when multiple schemas manage the same filepath location or if multiple servers use the same
        external location for blobs and attachments, then it is not safe to assume that untracked external files
        are no longer needed by other schemas. Delete with caution. The safest approach is to ensure that each external
        store is tracked by one database server only and that filepath locations are tracked by only one schema.
        """
        raise NotImplementedError

    def clean(self, *, limit=None, verbose=True,
              delete_blobs=True, delete_attachments=True, delete_filepaths=True):
        """
        remove blobs, attachments, and
        :param verbose: if True, print information about deleted files
        :param: limit: max number of items to delete. None=delete all
        :param include_{blobs, attachments, filepaths}: if True, delete blobs, attachments, filepaths
        """
        delete_list = self.get_untracked_external_files(
            limit=limit,
            include_blobs=delete_blobs, include_attachments=delete_attachments, include_filepaths=delete_filepaths)
        if verbose:
            print('Deleting %i items:' % len(delete_list))
            for item in delete_list:
                print(item)


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
