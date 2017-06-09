import hashlib
import os
import pymysql
from .base_relation import BaseRelation

ERROR_MESSAGE_LENGTH = 2047
TRUNCATION_APPENDIX = '...truncated'


def key_hash(key):
    """
    32-byte hash used for lookup of primary keys of jobs
    """
    hashed = hashlib.md5()
    for k, v in sorted(key.items()):
        hashed.update(str(v).encode())
    return hashed.hexdigest()


class JobTable(BaseRelation):
    """
    A base relation with no definition. Allows reserving jobs
    """
    def __init__(self, arg, database=None):
        if isinstance(arg, JobTable):
            super().__init__(arg)
            # copy constructor
            self.database = arg.database
            self._connection = arg._connection
            self._definition = arg._definition
            self._user = arg._user
            return
        super().__init__()
        self.database = database
        self._connection = arg
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
            host=os.uname().nodename,
            pid=os.getpid(),
            connection_id=self.connection.connection_id,
            key=key,
            user=self._user)
        try:
            self.insert1(job, ignore_extra_fields=True)
        except pymysql.err.IntegrityError:
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

    def error(self, table_name, key, error_message):
        """
        Log an error message.  The job reservation is replaced with an error entry.
        if an error occurs, leave an entry describing the problem
        :param table_name: `database`.`table_name`
        :param key: the dict of the job's primary key
        :param error_message: string error message
        """
        if len(error_message) > ERROR_MESSAGE_LENGTH:
            error_message = error_message[:ERROR_MESSAGE_LENGTH-len(TRUNCATION_APPENDIX)] + TRUNCATION_APPENDIX
        job_key = dict(table_name=table_name, key_hash=key_hash(key))
        self.insert1(
            dict(job_key,
                 status="error",
                 host=os.uname().nodename,
                 pid=os.getpid(),
                 connection_id=self.connection.connection_id,
                 user=self._user,
                 key=key,
                 error_message=error_message), replace=True, ignore_extra_fields=True)


class JobManager:
    """
    A container for all job tables (one job table per schema).
    """
    def __init__(self, connection):
        self.connection = connection
        self._jobs = {}

    def __getitem__(self, database):
        if database not in self._jobs:
            self._jobs[database] = JobTable(self.connection, database)
        return self._jobs[database]
