import hashlib
import os
import pymysql

from .relation import Relation, schema


def get_jobs_table(database):
    """
    :return: the base relation of the job reservation table for database
    """
    self = get_jobs_table
    if not hasattr(self, 'lookup'):
        self.lookup = {}

    if database not in self.lookup:
        @schema(database, context={})
        class JobsRelation(Relation):
            definition = """
            # the job reservation table
            table_name:  varchar(255)          # className of the table
            key_hash:    char(32)              # key hash
            ---
            status:            enum('reserved','error','ignore')# if tuple is missing, the job is available
            key=null:          blob                  # structure containing the key
            error_message="":  varchar(1023)         # error message returned if failed
            error_stack=null:  blob                  # error stack if failed
            host="":           varchar(255)          # system hostname
            pid=0:             int unsigned          # system process id
            timestamp=CURRENT_TIMESTAMP: timestamp    # automatic timestamp
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


def reserve(full_table_name, key):
    """
    Insert a reservation record in the jobs table
    :return: True if reserved job successfully
    """
    database, table_name = split_name(full_table_name)
    jobs = get_jobs_table(database)
    job_key = dict(table_name=table_name, key_hash=key_hash(key))
    if jobs & job_key:
        return False
    try:
        jobs.insert(dict(job_key, status="reserved", host=os.uname().nodename, pid=os.getpid()))
    except pymysql.err.IntegrityError:
        success = False
    else:
        success = True
    return success


def complete(full_table_name, key):
    """
    upon job completion the job entry is removed
    """
    database, table_name = split_name(full_table_name)
    job_key = dict(table_name=table_name, key_hash=key_hash(key))
    entry = get_jobs_table(full_table_name) & job_key
    entry.delete_quick()


def error(full_table_name, key, error_message):
    """
    if an error occurs, leave an entry describing the problem
    """
    database, table_name = split_name(full_table_name)
    job_key = dict(table_name=table_name, key_hash=key_hash(key))
    jobs = get_jobs_table(database)
    jobs.insert(dict(job_key,
                     status="error",
                     host=os.uname(),
                     pid=os.getpid(),
                     error_message=error_message), replace=True)
