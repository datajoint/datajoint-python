import os
import datetime
import platform
import json
from typing import Dict, Any, Union

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
        key=null  :json  # structure containing the key for querying
        error_message=""  :varchar({error_message_length})  # error message returned if failed
        error_stack=null  :mediumblob  # error stack if failed
        user="" :varchar(255) # database user
        host=""  :varchar(255)  # system hostname
        pid=0  :int unsigned  # system process id
        connection_id = 0  : bigint unsigned      # connection_id()
        timestamp  :timestamp   # timestamp of the job status change or scheduled time
        run_duration=null  : float  # run duration in seconds
        run_metadata=null  :json  # metadata about the run (e.g. code version, environment info)
        index(table_name, status)
        index(status)
        index(timestamp)  # for ordering jobs
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
        Schedule a job for computation in the DataJoint pipeline.

        This method manages job scheduling with the following key behaviors:
        1. Creates a new job entry if one doesn't exist
        2. Updates existing jobs based on their current status:
           - Allows rescheduling if job is in error/ignore status and force=True
           - Prevents rescheduling if job is already scheduled/reserved/success
        3. Records job metadata including host, process ID, and user info
        4. Supports delayed execution through seconds_delay parameter

        Args:
            table_name: Full table name in format `database`.`table_name`
            key: Dictionary containing the job's primary key
            seconds_delay: Optional delay in seconds before job execution (default: 0)
            force: If True, allows rescheduling jobs in error/ignore status (default: False)

        Returns:
            bool: True if job was successfully scheduled, False if job already exists with incompatible status
        """
        job_key = dict(table_name=table_name, key_hash=key_hash(key))
        if self & job_key:
            current_status = (self & job_key).fetch1("status")
            if current_status in ("scheduled", "reserved") or (
                current_status in ("error", "ignore") and not force
            ):
                return False

        job = dict(
            job_key,
            status="scheduled",
            host=platform.node(),
            pid=os.getpid(),
            connection_id=self.connection.connection_id,
            key=_jsonify(key),
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
            key=_jsonify(key),
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
            key=_jsonify(key),
            error_message=message,
            user=self._user,
            timestamp=datetime.datetime.utcnow(),
        )

        with config(enable_python_native_blobs=True):
            self.insert1(job, replace=True, ignore_extra_fields=True)

        return True

    def complete(self, table_name, key):
        """
        Log a completed job.  When a job is completed, its reservation entry is deleted.

        Args:
            table_name: `database`.`table_name`
            key: the dict of the job's primary key
        """
        job_key = dict(table_name=table_name, key_hash=key_hash(key))
        (self & job_key).delete()

    def error(
        self,
        table_name,
        key,
        error_message,
        error_stack=None,
        run_duration=None,
        run_metadata=None,
    ):
        """
        Log an error message.  The job reservation is replaced with an error entry.
        if an error occurs, leave an entry describing the problem

        Args:
            table_name: `database`.`table_name`
            key: the dict of the job's primary key
            error_message: string error message
            error_stack: stack trace
            run_duration: duration in second of the job run
            run_metadata: dict containing metadata about the run (e.g. code version, environment info)
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
                    key=_jsonify(key),
                    error_message=error_message,
                    error_stack=error_stack,
                    run_duration=run_duration,
                    run_metadata=_jsonify(run_metadata) if run_metadata else None,
                    timestamp=datetime.datetime.utcnow(),
                ),
                replace=True,
                ignore_extra_fields=True,
            )


def _jsonify(key: Dict[str, Any]) -> Dict[str, Any]:
    """
    Ensure the key is JSON serializable by converting to JSON and back.
    Uses str() as fallback for any non-serializable objects.
    """
    return json.loads(json.dumps(key, default=str))
