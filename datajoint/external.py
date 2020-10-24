from pathlib import Path, PurePosixPath, PureWindowsPath
from collections import Mapping
from tqdm import tqdm
from .settings import config
from .errors import DataJointError, MissingExternalFile
from .hash import uuid_from_buffer, uuid_from_file
from .table import Table, FreeTable
from .heading import Heading
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
    def __init__(self, connection, store, database):
        self.store = store
        self.spec = config.get_store_spec(store)
        self._s3 = None
        self.database = database
        self._connection = connection
        self._heading = Heading(table_info=dict(
            conn=connection,
            database=database,
            table_name=self.table_name,
            context=None))
        self._support = [self.full_table_name]
        if not self.is_declared:
            self.declare()
        self._s3 = None
        if self.spec['protocol'] == 'file' and not Path(self.spec['location']).is_dir():
            raise FileNotFoundError('Inaccessible local directory %s' %
                                    self.spec['location']) from None

    @property
    def definition(self):
        return """
        # external storage tracking
        hash  : uuid    #  hash of contents (blob), of filename + contents (attach), or relative filepath (filepath)
        ---
        size      :bigint unsigned     # size of object in bytes
        attachment_name=null : varchar(255)  # the filename of an attachment
        filepath=null : varchar(1000)  # relative filepath or attachment filename
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

    # - low-level operations - private

    def _make_external_filepath(self, relative_filepath):
        """resolve the complete external path based on the relative path"""
        # Strip root
        if self.spec['protocol'] == 's3':
            posix_path = PurePosixPath(PureWindowsPath(self.spec['location']))
            location_path = Path(
                    *posix_path.parts[1:]) if len(
                    self.spec['location']) > 0 and any(
                    case in posix_path.parts[0] for case in (
                        '\\', ':')) else Path(posix_path)
            return PurePosixPath(location_path, relative_filepath)
        # Preserve root
        elif self.spec['protocol'] == 'file':
            return PurePosixPath(Path(self.spec['location']), relative_filepath)
        else:
            assert False

    def _make_uuid_path(self, uuid, suffix=''):
        """create external path based on the uuid hash"""
        return self._make_external_filepath(PurePosixPath(
            self.database, '/'.join(subfold(uuid.hex, self.spec['subfolding'])), uuid.hex).with_suffix(suffix))

    def _upload_file(self, local_path, external_path, metadata=None):
        if self.spec['protocol'] == 's3':
            self.s3.fput(local_path, external_path, metadata)
        elif self.spec['protocol'] == 'file':
            safe_copy(local_path, external_path, overwrite=True)
        else:
            assert False

    def _download_file(self, external_path, download_path):
        if self.spec['protocol'] == 's3':
            self.s3.fget(external_path, download_path)
        elif self.spec['protocol'] == 'file':
            safe_copy(external_path, download_path)
        else:
            assert False

    def _upload_buffer(self, buffer, external_path):
        if self.spec['protocol'] == 's3':
            self.s3.put(external_path, buffer)
        elif self.spec['protocol'] == 'file':
            safe_write(external_path, buffer)
        else:
            assert False

    def _download_buffer(self, external_path):
        if self.spec['protocol'] == 's3':
            return self.s3.get(external_path)
        if self.spec['protocol'] == 'file':
            return Path(external_path).read_bytes()
        assert False

    def _remove_external_file(self, external_path):
        if self.spec['protocol'] == 's3':
            self.s3.remove_object(external_path)
        elif self.spec['protocol'] == 'file':
            Path(external_path).unlink()

    def exists(self, external_filepath):
        """
        :return: True if the external file is accessible
        """
        if self.spec['protocol'] == 's3':
            return self.s3.exists(external_filepath)
        if self.spec['protocol'] == 'file':
            return Path(external_filepath).is_file()
        assert False

    # --- BLOBS ----

    def put(self, blob):
        """
        put a binary string (blob) in external store
        """
        uuid = uuid_from_buffer(blob)
        self._upload_buffer(blob, self._make_uuid_path(uuid))
        # insert tracking info
        self.connection.query(
            "INSERT INTO {tab} (hash, size) VALUES (%s, {size}) ON DUPLICATE KEY "
            "UPDATE timestamp=CURRENT_TIMESTAMP".format(
                tab=self.full_table_name, size=len(blob)), args=(uuid.bytes,))
        return uuid

    def get(self, uuid):
        """
        get an object from external store.
        """
        if uuid is None:
            return None
        # attempt to get object from cache
        blob = None
        cache_folder = config.get('cache', None)
        if cache_folder:
            try:
                cache_path = Path(cache_folder, *subfold(uuid.hex, CACHE_SUBFOLDING))
                cache_file = Path(cache_path, uuid.hex)
                blob = cache_file.read_bytes()
            except FileNotFoundError:
                pass  # not cached
        # download blob from external store
        if blob is None:
            try:
                blob = self._download_buffer(self._make_uuid_path(uuid))
            except MissingExternalFile:
                if not SUPPORT_MIGRATED_BLOBS:
                    raise
                # blobs migrated from datajoint 0.11 are stored at explicitly defined filepaths
                relative_filepath, contents_hash = (self & {'hash': uuid}).fetch1('filepath', 'contents_hash')
                if relative_filepath is None:
                    raise
                blob = self._download_buffer(self._make_external_filepath(relative_filepath))
            if cache_folder:
                cache_path.mkdir(parents=True, exist_ok=True)
                safe_write(cache_path / uuid.hex, blob)
        return blob

    # --- ATTACHMENTS ---

    def upload_attachment(self, local_path):
        attachment_name = Path(local_path).name
        uuid = uuid_from_file(local_path, init_string=attachment_name + '\0')
        external_path = self._make_uuid_path(uuid, '.' + attachment_name)
        self._upload_file(local_path, external_path)
        # insert tracking info
        self.connection.query("""
        INSERT INTO {tab} (hash, size, attachment_name)
        VALUES (%s, {size}, "{attachment_name}")
        ON DUPLICATE KEY UPDATE timestamp=CURRENT_TIMESTAMP""".format(
                tab=self.full_table_name,
                size=Path(local_path).stat().st_size,
                attachment_name=attachment_name), args=[uuid.bytes])
        return uuid

    def get_attachment_name(self, uuid):
        return (self & {'hash': uuid}).fetch1('attachment_name')

    def download_attachment(self, uuid, attachment_name, download_path):
        """ save attachment from memory buffer into the save_path """
        external_path = self._make_uuid_path(uuid, '.' + attachment_name)
        self._download_file(external_path, download_path)

    # --- FILEPATH ---

    def upload_filepath(self, local_filepath):
        """
        Raise exception if an external entry already exists with a different contents checksum.
        Otherwise, copy (with overwrite) file to remote and
        If an external entry exists with the same checksum, then no copying should occur
        """
        local_filepath = Path(local_filepath)
        try:
            relative_filepath = str(local_filepath.relative_to(self.spec['stage']).as_posix())
        except ValueError:
            raise DataJointError('The path {path} is not in stage {stage}'.format(
                path=local_filepath.parent, **self.spec)) from None
        uuid = uuid_from_buffer(init_string=relative_filepath)  # hash relative path, not contents
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
            self._upload_file(local_filepath, self._make_external_filepath(relative_filepath),
                              metadata={'contents_hash': str(contents_hash)})
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
            external_path = self._make_external_filepath(relative_filepath)
            local_filepath = Path(self.spec['stage']).absolute() / relative_filepath
            file_exists = Path(local_filepath).is_file() and uuid_from_file(local_filepath) == contents_hash
            if not file_exists:
                self._download_file(external_path, local_filepath)
                checksum = uuid_from_file(local_filepath)
                if checksum != contents_hash:  # this should never happen without outside interference
                    raise DataJointError("'{file}' downloaded but did not pass checksum'".format(file=local_filepath))
            return str(local_filepath), contents_hash

    # --- UTILITIES ---

    @property
    def references(self):
        """
        :return: generator of referencing table names and their referencing columns
        """
        return ({k.lower(): v for k, v in elem.items()} for elem in self.connection.query("""
        SELECT concat('`', table_schema, '`.`', table_name, '`') as referencing_table, column_name
        FROM information_schema.key_column_usage
        WHERE referenced_table_name="{tab}" and referenced_table_schema="{db}"
        """.format(tab=self.table_name, db=self.database), as_dict=True))

    def fetch_external_paths(self, **fetch_kwargs):
        """
        generate complete external filepaths from the query.
        Each element is a tuple: (uuid, path)
        :param fetch_kwargs: keyword arguments to pass to fetch
        """
        fetch_kwargs.update(as_dict=True)
        paths = []
        for item in self.fetch('hash', 'attachment_name', 'filepath', **fetch_kwargs):
            if item['attachment_name']:
                # attachments
                path = self._make_uuid_path(item['hash'], '.' + item['attachment_name'])
            elif item['filepath']:
                # external filepaths
                path = self._make_external_filepath(item['filepath'])
            else:
                # blobs
                path = self._make_uuid_path(item['hash'])
            paths.append((item['hash'], path))
        return paths

    def unused(self):
        """
        query expression for unused hashes
        :return: self restricted to elements that are not in use by any tables in the schema
        """
        return self - [FreeTable(self.connection, ref['referencing_table']).proj(hash=ref['column_name'])
                       for ref in self.references]

    def used(self):
        """
        query expression for used hashes
        :return: self restricted to elements that in use by tables in the schema
        """
        return self & [FreeTable(self.connection, ref['referencing_table']).proj(hash=ref['column_name'])
                       for ref in self.references]

    def delete(self, *, delete_external_files=None, limit=None, display_progress=True):
        """
        :param delete_external_files: True or False. If False, only the tracking info is removed from the
        external store table but the external files remain intact. If True, then the external files
        themselves are deleted too.
        :param limit: (integer) limit the number of items to delete
        :param display_progress: if True, display progress as files are cleaned up
        :return: yields
        """
        if delete_external_files not in (True, False):
            raise DataJointError("The delete_external_files argument must be set to either True or False in delete()")

        if not delete_external_files:
            self.unused().delete_quick()
        else:
            items = self.unused().fetch_external_paths(limit=limit)
            if display_progress:
                items = tqdm(items)
            # delete items one by one, close to transaction-safe
            error_list = []
            for uuid, external_path in items:
                try:
                    count = (self & {'hash': uuid}).delete_quick(get_count=True)  # optimize
                except Exception:
                    pass   # if delete failed, do not remove the external file
                else:
                    assert count in (0, 1)
                    try:
                        self._remove_external_file(external_path)
                    except Exception as error:
                        error_list.append((uuid, external_path, str(error)))
            return error_list


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

    def __repr__(self):
        return ("External file tables for schema `{schema}`:\n    ".format(schema=self.schema.database)
                + "\n    ".join('"{store}" {protocol}:{location}'.format(
                    store=k, **v.spec) for k, v in self.items()))

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
