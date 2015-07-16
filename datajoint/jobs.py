import hashlib
import os
import pymysql
from .relation import Relation


def key_hash(key):
    """
    32-byte hash used for lookup of primary keys of jobs
    """
    hashed = hashlib.md5()
    for k, v in sorted(key.items()):
        hashed.update(str(v).encode())
    return hashed.hexdigest()


class JobRelation(Relation):
    """
    A base relation with no definition. Allows reserving jobs
    """

    def __init__(self, connection, database):
        self.database = database
        self._table_name = '~jobs'
        self._connection = connection
        self._definition = """    # job reservation table
        table_name  :varchar(255)  # className of the table
        key_hash  :char(32)  # key hash
        ---
        status  :enum('reserved','error','ignore')  # if tuple is missing, the job is available
        key=null  :blob  # structure containing the key
        error_message=""  :varchar(1023)  # error message returned if failed
        error_stack=null  :blob  # error stack if failed
        host=""  :varchar(255)  # system hostname
        pid=0  :int unsigned  # system process id
        timestamp=CURRENT_TIMESTAMP  :timestamp   # automatic timestamp
        """

    @property
    def definition(self):
        return self._definition

    @property
    def connection(self):
        return self._connection

    @property
    def table_name(self):
        return self._table_name

    def reserve(self, table_name, key):
        """
        Reserve a job for computation.  When a job is reserved, the job table contains an entry for the
        job key, identified by its hash. When jobs are completed, the entry is removed.
        :param full_table_name: `database`.`table_name`
        :param key: the dict of the job's primary key
        :return: True if reserved job successfully. False = the jobs is already taken
        """
        job_key = dict(table_name=table_name, key_hash=key_hash(key))
        try:
            self.insert1(
                dict(job_key,
                     status="reserved",
                     host=os.uname().nodename,
                     pid=os.getpid()))
        except pymysql.err.IntegrityError:
            return False
        else:
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
        job_key = dict(table_name=table_name, key_hash=key_hash(key))
        self.insert1(
            dict(job_key,
                 status="error",
                 host=os.uname().nodename,
                 pid=os.getpid(),
                 error_message=error_message), replace=True)


class JobManager:
    def __init__(self, connection):
        self.connection = connection
        self._jobs = {}

    def __getitem__(self, database):
        if database not in self._jobs:
            self._jobs[database] = JobRelation(self.connection, database)
        return self._jobs[database]
