import os
import datetime
import platform

from .errors import DuplicateError
from .hash import key_hash
from .heading import Heading
from .settings import config
from .table import Table

ERROR_MESSAGE_LENGTH = 2047
TRUNCATION_APPENDIX = "...truncated"


class JobTable(Table):
    """
    A base table with no definition. Allows reserving jobs
    """

    def __init__(self, conn, database):
        self.database = database
        self._connection = conn
        self._heading = Heading(
            table_info=dict(
                conn=conn, database=database, table_name=self.table_name, context=None
            )
        )
        self._support = [self.full_table_name]

        self._definition = """    # job reservation table for `{database}`
        table_name  :varchar(255)  # className of the table
        key_hash  :char(32)  # key hash
        ---
        status  :enum('reserved','error','ignore','scheduled','success') 
        key=null  :blob  # structure containing the key
        error_message=""  :varchar({error_message_length})  # error message returned if failed
        error_stack=null  :mediumblob  # error stack if failed
        user="" :varchar(255) # database user
        host=""  :varchar(255)  # system hostname
        pid=0  :int unsigned  # system process id
        connection_id = 0  : bigint unsigned      # connection_id()
        timestamp  :timestamp   # the scheduled time (UTC) for the job to run at or after
        run_duration=null  : float  # run duration in seconds
        run_version=""  : varchar(255) # some string representation of the code/env version of a run (e.g. git commit hash)
        index(table_name, status)
        index(status)
        """.format(
            database=database, error_message_length=ERROR_MESSAGE_LENGTH
        )
        if not self.is_declared:
            self.declare()
        self._user = self.connection.get_user()

    @property
    def definition(self):
        return self._definition

    @property
    def table_name(self):
        return "~jobs"

    def delete(self):
        """bypass interactive prompts and dependencies"""
        self.delete_quick()

    def drop(self):
        """bypass interactive prompts and dependencies"""
        self.drop_quick()

    def schedule(self, table_name, key, seconds_delay=0, force=False):
        """
        Schedule a job for computation.

        :param table_name: `database`.`table_name`
        :param key: the dict of the job's primary key
        :param seconds_delay: add time delay (in second) in scheduling this job
        :param force: force scheduling this job (even if it is in error/ignore status)
        :return: True if schedule job successfully. False = the jobs already exists with a different status
        """
        job_key = dict(table_name=table_name, key_hash=key_hash(key))
        if self & job_key:
            current_status = (self & job_key).fetch1("status")
            if current_status in ("scheduled", "reserved", "success") or (
                current_status in ("error", "ignore") and not force
            ):
                return False

        job = dict(
            job_key,
            status="scheduled",
            host=platform.node(),
            pid=os.getpid(),
            connection_id=self.connection.connection_id,
            key=key,
            user=self._user,
            timestamp=datetime.datetime.utcnow()
            + datetime.timedelta(seconds=seconds_delay),
        )

        with config(enable_python_native_blobs=True):
            self.insert1(job, replace=True, ignore_extra_fields=True)

        return True

    def reserve(self, table_name, key):
        """
        Reserve a job for computation.

        :param table_name: `database`.`table_name`
        :param key: the dict of the job's primary key
        :return: True if reserved job successfully. False = the jobs is already taken
        """
        job_key = dict(table_name=table_name, key_hash=key_hash(key))
        if self & job_key:
            current_status = (self & job_key).fetch1("status")
            if current_status != "scheduled":
                return False

        job = dict(
            job_key,
            status="reserved",
            host=platform.node(),
            pid=os.getpid(),
            connection_id=self.connection.connection_id,
            key=key,
            user=self._user,
            timestamp=datetime.datetime.utcnow(),
        )

        with config(enable_python_native_blobs=True):
            self.insert1(job, replace=True, ignore_extra_fields=True)

        return True

    def ignore(self, table_name, key, message=""):
        """
        Set a job to be ignored for computation.  When a job is ignored, the job table contains an entry for the
        job key, identified by its hash, with status "ignore".

        Args:
        table_name:
            Table name (str) - `database`.`table_name`
        key:
            The dict of the job's primary key
        message:
            The optional message for why the key is to be ignored

        Returns:
            True if ignore job successfully. False = the jobs is already taken
        """
        job_key = dict(table_name=table_name, key_hash=key_hash(key))
        if self & job_key:
            current_status = (self & job_key).fetch1("status")
            if current_status not in ("scheduled", "ignore"):
                return False

        job = dict(
            job_key,
            status="ignore",
            host=platform.node(),
            pid=os.getpid(),
            connection_id=self.connection.connection_id,
            key=key,
            error_message=message,
            user=self._user,
            timestamp=datetime.datetime.utcnow(),
        )

        with config(enable_python_native_blobs=True):
            self.insert1(job, replace=True, ignore_extra_fields=True)

        return True

    def complete(self, table_name, key, run_duration=None, run_version=""):
        """
        Log a completed job.  When a job is completed, its reservation entry is deleted.

        :param table_name: `database`.`table_name`
        :param key: the dict of the job's primary key
        :param run_duration: duration in second of the job run
        :param run_version: some string representation of the code/env version of a run (e.g. git commit hash)
        """
        job_key = dict(table_name=table_name, key_hash=key_hash(key))
        if self & job_key:
            current_status = (self & job_key).fetch1("status")
            if current_status == "success":
                return

        with config(enable_python_native_blobs=True):
            self.insert1(
                dict(
                    table_name=table_name,
                    key_hash=key_hash(key),
                    status="success",
                    host=platform.node(),
                    pid=os.getpid(),
                    connection_id=self.connection.connection_id,
                    user=self._user,
                    key=key,
                    run_duration=run_duration,
                    run_version=run_version,
                    timestamp=datetime.datetime.utcnow(),
                ),
                replace=True,
                ignore_extra_fields=True,
            )

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
            error_message = (
                error_message[: ERROR_MESSAGE_LENGTH - len(TRUNCATION_APPENDIX)]
                + TRUNCATION_APPENDIX
            )
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
                    error_stack=error_stack,
                    timestamp=datetime.datetime.utcnow(),
                ),
                replace=True,
                ignore_extra_fields=True,
            )
