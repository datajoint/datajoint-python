import hashlib
import os
import pymysql

from .relation import Relation, schema


def get_jobs(database):
    """
    :return: the base relation of the job reservation table for database
    """

    # cache for containing an instance
    self = get_jobs
    if not hasattr(self, 'lookup'):
        self.lookup = {}

    if database not in self.lookup:
        @schema(database, context={})
        class JobsRelation(Relation):
            definition = """
            # the job reservation table
            table_name:  varchar(255)   # className of the table
            key_hash:    char(32)       # key hash
            ---
            status: enum('reserved','error','ignore') # if tuple is missing, the job is available
            key=null:          blob # structure containing the key
            error_message="":  varchar(1023)         # error message returned if failed
            error_stack=null:  blob                  # error stack if failed
            host="":           varchar(255)          # system hostname
            pid=0:             int unsigned          # system process id
            timestamp=CURRENT_TIMESTAMP: timestamp   # automatic timestamp
            """

            @property
            def table_name(self):
                return '~jobs'

        self.lookup[database] = JobsRelation()

    return self.lookup[database]


def split_name(full_table_name):
    [database, table_name] = full_table_name.split('.')
    return database.strip('` '), table_name.strip('` ')


def key_hash(key):
    hashed = hashlib.md5()
    for k, v in sorted(key.items()):
        hashed.update(str(v).encode())
    return hashed.hexdigest()


def reserve(reserve_jobs, full_table_name, key):
    """
    Reserve a job for computation.  When a job is reserved, the job table contains an entry for the
    job key, identified by its hash. When jobs are completed, the entry is removed.
    :param reserve_jobs: if True, use job reservation
    :param full_table_name: `database`.`table_name`
    :param key: the dict of the job's primary key
    :return: True if reserved job successfully
    """
    if not reserve_jobs:
        return True
    database, table_name = split_name(full_table_name)
    jobs = get_jobs(database)
    job_key = dict(table_name=table_name, key_hash=key_hash(key))
    if jobs & job_key:
        return False
    try:
        jobs.insert1(dict(job_key, status="reserved", host=os.uname().nodename, pid=os.getpid()))
    except pymysql.err.IntegrityError:
        success = False
    else:
        success = True
    return success


def complete(reserve_jobs, full_table_name, key):
    """
    Log a completed job.  When a job is completed, its reservation entry is deleted.
    :param reserve_jobs: if True, use job reservation
    :param full_table_name: `database`.`table_name`
    :param key: the dict of the job's primary key
    """
    if reserve_jobs:
        database, table_name = split_name(full_table_name)
        job_key = dict(table_name=table_name, key_hash=key_hash(key))
        entry = get_jobs(database) & job_key
        entry.delete_quick()


def error(reserve_jobs, full_table_name, key, error_message):
    """
    Log an error message.  The job reservation is replaced with an error entry.
    if an error occurs, leave an entry describing the problem
    :param reserve_jobs: if True, use job reservation
    :param full_table_name: `database`.`table_name`
    :param key: the dict of the job's primary key
    :param error_message: string error message
    """
    if reserve_jobs:
        database, table_name = split_name(full_table_name)
        job_key = dict(table_name=table_name, key_hash=key_hash(key))
        jobs = get_jobs(database)
        jobs.insert(dict(job_key,
                         status="error",
                         host=os.uname(),
                         pid=os.getpid(),
                         error_message=error_message), replace=True)
