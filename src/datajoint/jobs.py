"""
Job queue management for AutoPopulate 2.0.

Each auto-populated table (Computed/Imported) has an associated jobs table
with the naming pattern ``~~table_name``. The jobs table tracks job status,
priority, scheduling, and error information.
"""

from __future__ import annotations

import logging
import os
import platform
import subprocess

from .condition import AndList
from .errors import DataJointError, DuplicateError
from .heading import Heading
from .table import Table

ERROR_MESSAGE_LENGTH = 2047
TRUNCATION_APPENDIX = "...truncated"

logger = logging.getLogger(__name__.split(".")[0])


def _get_job_version() -> str:
    """
    Get version string based on config settings.

    Returns
    -------
    str
        Version string, or empty string if version tracking disabled.
    """
    from .settings import config

    method = config.jobs.version_method
    if method is None or method == "none":
        return ""
    elif method == "git":
        try:
            result = subprocess.run(
                ["git", "rev-parse", "--short", "HEAD"],
                capture_output=True,
                text=True,
                timeout=5,
            )
            return result.stdout.strip() if result.returncode == 0 else ""
        except Exception:
            return ""
    return ""


class Job(Table):
    """
    Per-table job queue for AutoPopulate 2.0.

    Each auto-populated table (Computed/Imported) has an associated job table
    with the naming pattern ``~~table_name``. The job table tracks job status,
    priority, scheduling, and error information.

    Parameters
    ----------
    target_table : Table
        The Computed/Imported table instance this jobs table manages.

    Attributes
    ----------
    target : Table
        The auto-populated table this jobs table manages.
    pending : QueryExpression
        Query for jobs with ``status='pending'``.
    reserved : QueryExpression
        Query for jobs with ``status='reserved'``.
    errors : QueryExpression
        Query for jobs with ``status='error'``.
    completed : QueryExpression
        Query for jobs with ``status='success'``.
    ignored : QueryExpression
        Query for jobs with ``status='ignore'``.

    Examples
    --------
    >>> MyTable.jobs.refresh()      # Add new jobs, clean up stale ones
    >>> MyTable.jobs.pending        # Query pending jobs
    >>> MyTable.jobs.errors         # Query failed jobs
    """

    def __init__(self, target_table: Table) -> None:
        """
        Initialize jobs table for an auto-populated table.

        Parameters
        ----------
        target_table : Table
            The Computed/Imported table instance this jobs table manages.
        """
        self._target = target_table
        self._connection = target_table.connection
        self.database = target_table.database

        # Compute table name: ~~base_name
        target_name = target_table.table_name
        base_name = target_name.lstrip("_")
        self._table_name = f"~~{base_name}"

        # Generate definition from target's FK-derived primary key
        self._definition = self._generate_definition()

        # Initialize heading and support
        self._heading = Heading(
            table_info=dict(
                conn=self._connection,
                database=self.database,
                table_name=self._table_name,
                context=None,
            )
        )
        self._support = [self.full_table_name]

    @property
    def table_name(self):
        return self._table_name

    @property
    def definition(self):
        return self._definition

    @property
    def target(self):
        """The auto-populated table this jobs table manages."""
        return self._target

    def _generate_definition(self) -> str:
        """
        Generate jobs table definition from target's FK-derived primary key.

        Returns
        -------
        str
            DataJoint table definition string.
        """
        pk_attrs = self._get_fk_derived_pk_attrs()

        if not pk_attrs:
            raise DataJointError(
                f"Cannot create jobs table for {self._target.full_table_name}: no FK-derived primary key attributes found."
            )

        pk_lines = "\n    ".join(f"{name} : {dtype}" for name, dtype in pk_attrs)

        return f"""
    # Job queue for {self._target.full_table_name}
    {pk_lines}
    ---
    status          : enum('pending', 'reserved', 'success', 'error', 'ignore')
    priority        : int8
    created_time=CURRENT_TIMESTAMP(3) : datetime(3)
    scheduled_time=CURRENT_TIMESTAMP(3) : datetime(3)
    reserved_time=null  : datetime(3)
    completed_time=null : datetime(3)
    duration=null   : float64
    error_message="" : varchar({ERROR_MESSAGE_LENGTH})
    error_stack=null : <blob>
    user=""         : varchar(255)
    host=""         : varchar(255)
    pid=0           : int32
    connection_id=0 : int64
    version=""      : varchar(64)
    INDEX (status, priority, scheduled_time)
    """

    def _get_fk_derived_pk_attrs(self) -> list[tuple[str, str]]:
        """
        Extract FK-derived primary key attributes using the dependency graph.

        FK-derived attributes are those that come from primary FK references.
        Uses connection.dependencies to identify FK relationships.

        Returns
        -------
        list[tuple[str, str]]
            List of (attribute_name, datatype) tuples in target PK order.
        """
        heading = self._target.heading
        target_pk = heading.primary_key

        # Load dependency graph if not already loaded
        self._connection.dependencies.load()

        # Get primary FK parents and collect their attribute mappings
        # parents(primary=True) returns FKs that contribute to primary key
        parents = self._target.parents(primary=True, foreign_key_info=True)
        fk_derived_attrs = set()
        for _parent_name, props in parents:
            # attr_map: child_attr -> parent_attr
            fk_derived_attrs.update(props.get("attr_map", {}).keys())

        fk_attrs = []
        for name in target_pk:
            if name in fk_derived_attrs:
                # FK-derived: comes from a primary FK parent
                attr = heading[name]
                fk_attrs.append((name, attr.type))
            else:
                # Native PK attribute - not from FK
                logger.warning(
                    f"Ignoring non-FK primary key attribute '{name}' in jobs table "
                    f"for {self._target.full_table_name}. Job granularity will be degraded."
                )

        return fk_attrs

    def _get_pk(self, key: dict) -> dict:
        """
        Extract primary key values from a key dict.

        Parameters
        ----------
        key : dict
            Dictionary containing at least the primary key attributes.

        Returns
        -------
        dict
            Dictionary with only the primary key attributes.
        """
        return {k: key[k] for k in self.primary_key if k in key}

    def delete(self) -> None:
        """Delete all entries, bypassing interactive prompts and dependencies."""
        self.delete_quick()

    def drop(self) -> None:
        """Drop the table, bypassing interactive prompts and dependencies."""
        self.drop_quick()

    # -------------------------------------------------------------------------
    # Status filter properties
    # -------------------------------------------------------------------------

    @property
    def pending(self) -> "Job":
        """
        Query for pending jobs awaiting processing.

        Returns
        -------
        Job
            Restricted query with ``status='pending'``.
        """
        return self & 'status="pending"'

    @property
    def reserved(self) -> "Job":
        """
        Query for jobs currently being processed.

        Returns
        -------
        Job
            Restricted query with ``status='reserved'``.
        """
        return self & 'status="reserved"'

    @property
    def errors(self) -> "Job":
        """
        Query for jobs that failed with errors.

        Returns
        -------
        Job
            Restricted query with ``status='error'``.
        """
        return self & 'status="error"'

    @property
    def ignored(self) -> "Job":
        """
        Query for jobs marked to be skipped.

        Returns
        -------
        Job
            Restricted query with ``status='ignore'``.
        """
        return self & 'status="ignore"'

    @property
    def completed(self) -> "Job":
        """
        Query for successfully completed jobs.

        Returns
        -------
        Job
            Restricted query with ``status='success'``.
        """
        return self & 'status="success"'

    # -------------------------------------------------------------------------
    # Core job management methods
    # -------------------------------------------------------------------------

    def refresh(
        self,
        *restrictions,
        delay: float = 0,
        priority: int | None = None,
        stale_timeout: float | None = None,
        orphan_timeout: float | None = None,
    ) -> dict:
        """
        Refresh the jobs queue: add new jobs and clean up stale/orphaned jobs.

        Parameters
        ----------
        *restrictions : any
            Conditions to filter key_source (for adding new jobs).
        delay : float, optional
            Seconds from now until new jobs become available for processing.
            Default 0 (immediately available). Uses database server time.
        priority : int, optional
            Priority for new jobs (lower = more urgent).
            Default from ``config.jobs.default_priority``.
        stale_timeout : float, optional
            Seconds after which jobs are checked for staleness.
            Jobs older than this are removed if key not in key_source.
            Default from ``config.jobs.stale_timeout``. Set to 0 to skip.
        orphan_timeout : float, optional
            Seconds after which reserved jobs are considered orphaned.
            Reserved jobs older than this are deleted and re-added as pending.
            Default None (no orphan cleanup).

        Returns
        -------
        dict
            Status counts with keys: ``'added'``, ``'removed'``,
            ``'orphaned'``, ``'re_pended'``.

        Notes
        -----
        Operations performed:

        1. Add new jobs: ``(key_source & restrictions) - target - jobs`` → insert as pending
        2. Re-pend success jobs: if ``keep_completed=True`` and key in key_source but not in target
        3. Remove stale jobs: jobs older than stale_timeout whose keys not in key_source
        4. Remove orphaned jobs: reserved jobs older than orphan_timeout (if specified)
        """
        from .settings import config

        # Ensure jobs table exists
        if not self.is_declared:
            self.declare()

        # Get defaults from config
        if priority is None:
            priority = config.jobs.default_priority
        if stale_timeout is None:
            stale_timeout = config.jobs.stale_timeout

        result = {"added": 0, "removed": 0, "orphaned": 0, "re_pended": 0}

        # 1. Add new jobs
        key_source = self._target.key_source
        if restrictions:
            key_source = key_source & AndList(restrictions)

        # Keys that need jobs: in key_source, not in target, not in jobs
        # Disable semantic_check for Job table (self) because its attributes may not have matching lineage
        from .condition import Not

        new_keys = (key_source - self._target).restrict(Not(self), semantic_check=False).proj()
        new_key_list = new_keys.keys()

        if new_key_list:
            # Use server time for scheduling (CURRENT_TIMESTAMP(3) matches datetime(3) precision)
            scheduled_time = self.connection.query(f"SELECT CURRENT_TIMESTAMP(3) + INTERVAL {delay} SECOND").fetchone()[0]

            for key in new_key_list:
                job_entry = {
                    **key,
                    "status": "pending",
                    "priority": priority,
                    "scheduled_time": scheduled_time,
                }
                try:
                    self.insert1(job_entry, ignore_extra_fields=True)
                    result["added"] += 1
                except DuplicateError:
                    pass  # Job already exists

        # 2. Re-pend success jobs if keep_completed=True
        if config.jobs.keep_completed:
            # Success jobs whose keys are in key_source but not in target
            # Disable semantic_check for Job table operations
            success_to_repend = self.completed.restrict(key_source, semantic_check=False) - self._target
            repend_keys = success_to_repend.keys()
            for key in repend_keys:
                (self & key).delete_quick()
                self.insert1({**key, "status": "pending", "priority": priority})
                result["re_pended"] += 1

        # 3. Remove stale jobs (not ignore status) - use server CURRENT_TIMESTAMP for consistent timing
        if stale_timeout > 0:
            old_jobs = self & f"created_time < CURRENT_TIMESTAMP - INTERVAL {stale_timeout} SECOND" & 'status != "ignore"'

            for key in old_jobs.keys():
                # Check if key still in key_source
                if not (key_source & key):
                    (self & key).delete_quick()
                    result["removed"] += 1

        # 4. Handle orphaned reserved jobs - use server CURRENT_TIMESTAMP for consistent timing
        if orphan_timeout is not None and orphan_timeout > 0:
            orphaned_jobs = self.reserved & f"reserved_time < CURRENT_TIMESTAMP - INTERVAL {orphan_timeout} SECOND"

            for key in orphaned_jobs.keys():
                (self & key).delete_quick()
                self.insert1({**key, "status": "pending", "priority": priority})
                result["orphaned"] += 1

        return result

    def reserve(self, key: dict) -> bool:
        """
        Attempt to reserve a pending job for processing.

        Updates status to ``'reserved'`` if currently ``'pending'`` and
        ``scheduled_time <= now``.

        Parameters
        ----------
        key : dict
            Primary key dict of the job to reserve.

        Returns
        -------
        bool
            True if reservation successful, False if job not available.
        """
        # Check if job is pending and scheduled (use CURRENT_TIMESTAMP(3) for datetime(3) precision)
        job = (self & key & 'status="pending"' & "scheduled_time <= CURRENT_TIMESTAMP(3)").to_dicts()

        if not job:
            return False

        # Get server time for reserved_time
        server_now = self.connection.query("SELECT CURRENT_TIMESTAMP").fetchone()[0]

        # Build update row with primary key and new values
        pk = self._get_pk(key)
        update_row = {
            **pk,
            "status": "reserved",
            "reserved_time": server_now,
            "host": platform.node(),
            "pid": os.getpid(),
            "connection_id": self.connection.connection_id,
            "user": self.connection.get_user(),
            "version": _get_job_version(),
        }

        try:
            self.update1(update_row)
            return True
        except Exception:
            return False

    def complete(self, key: dict, duration: float | None = None) -> None:
        """
        Mark a job as successfully completed.

        Parameters
        ----------
        key : dict
            Primary key dict of the job.
        duration : float, optional
            Execution duration in seconds.

        Notes
        -----
        Based on ``config.jobs.keep_completed``:

        - If True: updates status to ``'success'`` with completion time and duration
        - If False: deletes the job entry
        """
        from .settings import config

        if config.jobs.keep_completed:
            # Use server time for completed_time
            server_now = self.connection.query("SELECT CURRENT_TIMESTAMP").fetchone()[0]
            pk = self._get_pk(key)
            update_row = {
                **pk,
                "status": "success",
                "completed_time": server_now,
            }
            if duration is not None:
                update_row["duration"] = duration
            self.update1(update_row)
        else:
            (self & key).delete_quick()

    def error(self, key: dict, error_message: str, error_stack: str | None = None) -> None:
        """
        Mark a job as failed with error details.

        Parameters
        ----------
        key : dict
            Primary key dict of the job.
        error_message : str
            Error message (truncated to 2047 chars if longer).
        error_stack : str, optional
            Full stack trace.
        """
        if len(error_message) > ERROR_MESSAGE_LENGTH:
            error_message = error_message[: ERROR_MESSAGE_LENGTH - len(TRUNCATION_APPENDIX)] + TRUNCATION_APPENDIX

        # Use server time for completed_time
        server_now = self.connection.query("SELECT CURRENT_TIMESTAMP").fetchone()[0]

        pk = self._get_pk(key)
        update_row = {
            **pk,
            "status": "error",
            "completed_time": server_now,
            "error_message": error_message,
        }
        if error_stack is not None:
            update_row["error_stack"] = error_stack

        self.update1(update_row)

    def ignore(self, key: dict) -> None:
        """
        Mark a job to be ignored (skipped during populate).

        If the key doesn't exist in the jobs table, inserts it with
        ``status='ignore'``. If it exists, updates the status to ``'ignore'``.

        Parameters
        ----------
        key : dict
            Primary key dict of the job.
        """
        from .settings import config

        pk = self._get_pk(key)
        if pk in self:
            self.update1({**pk, "status": "ignore"})
        else:
            priority = config.jobs.default_priority
            self.insert1({**pk, "status": "ignore", "priority": priority})

    def progress(self) -> dict:
        """
        Return job status breakdown.

        Returns
        -------
        dict
            Counts by status with keys: ``'pending'``, ``'reserved'``,
            ``'success'``, ``'error'``, ``'ignore'``, ``'total'``.
        """
        if not self.is_declared:
            return {
                "pending": 0,
                "reserved": 0,
                "success": 0,
                "error": 0,
                "ignore": 0,
                "total": 0,
            }

        # Query status counts
        result = self.connection.query(f"SELECT status, COUNT(*) as n FROM {self.full_table_name} GROUP BY status").fetchall()

        counts = {
            "pending": 0,
            "reserved": 0,
            "success": 0,
            "error": 0,
            "ignore": 0,
        }

        for row in result:
            status, n = row
            counts[status] = n

        counts["total"] = sum(counts.values())
        return counts
