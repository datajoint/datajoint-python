import os
from .hash import key_hash
import platform
from .table import Table
from .settings import config
from .errors import DuplicateError
from .heading import Heading

ERROR_MESSAGE_LENGTH = 2047
TRUNCATION_APPENDIX = '...truncated'


class JobTable(Table):
    """
    A base relation with no definition. Allows reserving jobs
    """
    def __init__(self, conn, database):
        self.database = database
        self._connection = conn
        self._heading = Heading(table_info=dict(
            conn=conn,
            database=database,
            table_name=self.table_name,
            context=None
        ))
        self._support = [self.full_table_name]

        self._definition = """    # job reservation table for `{database}`
        table_name  :varchar(255)  # className of the table
        key_hash  :char(32)  # key hash
        ---
        status  :enum('reserved','error','ignore')  # if tuple is missing, the job is available
        key=null  :blob  # structure containing the key
        error_message=""  :varchar({error_message_length})  # error message returned if failed
        error_stack=null  :blob  # error stack if failed
        user="" :varchar(255) # database user
        host=""  :varchar(255)  # system hostname
        pid=0  :int unsigned  # system process id
        connection_id = 0  : bigint unsigned          # connection_id()
        timestamp=CURRENT_TIMESTAMP  :timestamp   # automatic timestamp
        """.format(database=database, error_message_length=ERROR_MESSAGE_LENGTH)
        if not self.is_declared:
            self.declare()
        self._user = self.connection.get_user()

    @property
    def definition(self):
        return self._definition

    @property
    def table_name(self):
        return '~jobs'

    def delete(self):
        """bypass interactive prompts and dependencies"""
        self.delete_quick()

    def drop(self):
        """bypass interactive prompts and dependencies"""
        self.drop_quick()

    def reserve(self, table_name, key):
        """
        Reserve a job for computation.  When a job is reserved, the job table contains an entry for the
        job key, identified by its hash. When jobs are completed, the entry is removed.
        :param table_name: `database`.`table_name`
        :param key: the dict of the job's primary key
        :return: True if reserved job successfully. False = the jobs is already taken
        """
        job = dict(
            table_name=table_name,
            key_hash=key_hash(key),
            status='reserved',
            host=platform.node(),
            pid=os.getpid(),
            connection_id=self.connection.connection_id,
            key=key,
            user=self._user)
        try:
            with config(enable_python_native_blobs=True):
                self.insert1(job, ignore_extra_fields=True)
        except DuplicateError:
            return False
        return True

    def complete(self, table_name, key):
        """
        Log a completed job.  When a job is completed, its reservation entry is deleted.
        :param table_name: `database`.`table_name`
        :param key: the dict of the job's primary key
        """
        job_key = dict(table_name=table_name, key_hash=key_hash(key))
        (self & job_key).delete_quick()

    def error(self, table_name, key, error_message, error_stack=None):
        """
        Log an error message.  The job reservation is replaced with an error entry.
        if an error occurs, leave an entry describing the problem
        :param table_name: `database`.`table_name`
        :param key: the dict of the job's primary key
        :param error_message: string error message
        :param error_stack: stack trace
        """
        if len(error_message) > ERROR_MESSAGE_LENGTH:
            error_message = error_message[:ERROR_MESSAGE_LENGTH-len(TRUNCATION_APPENDIX)] + TRUNCATION_APPENDIX
        with config(enable_python_native_blobs=True):
            self.insert1(
                dict(
                    table_name=table_name,
                    key_hash=key_hash(key),
                    status="error",
                    host=platform.node(),
                    pid=os.getpid(),
                    connection_id=self.connection.connection_id,
                    user=self._user,
                    key=key,
                    error_message=error_message,
                    error_stack=error_stack),
                replace=True, ignore_extra_fields=True)
