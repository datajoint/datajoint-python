"""
Autopopulate 2.0 Jobs System

This module implements per-table job tables for auto-populated tables.
Each dj.Imported or dj.Computed table gets its own hidden jobs table
with FK-derived primary keys and rich status tracking.
"""

import logging
import os
import platform
from typing import TYPE_CHECKING

from .errors import DataJointError, DuplicateError
from .expression import QueryExpression
from .heading import Heading
from .settings import config
from .table import Table

if TYPE_CHECKING:
    from .autopopulate import AutoPopulate

logger = logging.getLogger(__name__.split(".")[0])

ERROR_MESSAGE_LENGTH = 2047
TRUNCATION_APPENDIX = "...truncated"

# Default configuration values
DEFAULT_STALE_TIMEOUT = 3600  # 1 hour
DEFAULT_PRIORITY = 5
DEFAULT_KEEP_COMPLETED = False


class JobsTable(Table):
    """
    Per-table job queue for auto-populated tables.

    Each dj.Imported or dj.Computed table has an associated hidden jobs table
    with the naming convention ~<table_name>__jobs.

    The jobs table primary key includes only those attributes derived from
    foreign keys in the target table's primary key. Additional primary key
    attributes (if any) are excluded.

    Status values:
        - pending: Job is queued and ready to be processed
        - reserved: Job is currently being processed by a worker
        - success: Job completed successfully
        - error: Job failed with an error
        - ignore: Job should be skipped (manually set)
    """

    def __init__(self, target: "AutoPopulate"):
        """
        Initialize a JobsTable for the given auto-populated table.

        Args:
            target: The auto-populated table (dj.Imported or dj.Computed)
        """
        self._target = target
        self._connection = target.connection
        self.database = target.database
        self._user = self.connection.get_user()

        # Derive the jobs table name from the target table
        # e.g., __filtered_image -> _filtered_image__jobs
        target_table_name = target.table_name
        if target_table_name.startswith("__"):
            # Computed table: __foo -> _foo__jobs
            self._table_name = f"~{target_table_name[2:]}__jobs"
        elif target_table_name.startswith("_"):
            # Imported table: _foo -> _foo__jobs
            self._table_name = f"~{target_table_name[1:]}__jobs"
        else:
            # Manual/Lookup (shouldn't happen for auto-populated)
            self._table_name = f"~{target_table_name}__jobs"

        # Build the definition dynamically based on target's FK-derived primary key
        self._definition = self._build_definition()

        # Initialize heading
        self._heading = Heading(
            table_info=dict(
                conn=self._connection,
                database=self.database,
                table_name=self.table_name,
                context=None,
            )
        )
        self._support = [self.full_table_name]

    def _get_fk_derived_primary_key(self) -> list[tuple[str, str]]:
        """
        Get the FK-derived primary key attributes from the target table.

        Returns:
            List of (attribute_name, attribute_type) tuples for FK-derived PK attributes.
        """
        # Get parent tables that contribute to the primary key
        parents = self._target.parents(primary=True, as_objects=True, foreign_key_info=True)

        # Collect all FK-derived primary key attributes
        fk_pk_attrs = set()
        for parent_table, props in parents:
            # attr_map maps child attr -> parent attr
            for child_attr in props["attr_map"].keys():
                fk_pk_attrs.add(child_attr)

        # Get attribute definitions from target table's heading
        pk_definitions = []
        for attr_name in self._target.primary_key:
            if attr_name in fk_pk_attrs:
                attr = self._target.heading.attributes[attr_name]
                # Build attribute definition string
                attr_def = f"{attr_name} : {attr.type}"
                pk_definitions.append((attr_name, attr_def))

        return pk_definitions

    def _build_definition(self) -> str:
        """
        Build the table definition for the jobs table.

        Returns:
            DataJoint table definition string.
        """
        # Get FK-derived primary key attributes
        pk_attrs = self._get_fk_derived_primary_key()

        if not pk_attrs:
            raise DataJointError(
                f"Cannot create jobs table for {self._target.full_table_name}: "
                "no foreign-key-derived primary key attributes found."
            )

        # Build primary key section
        pk_lines = [attr_def for _, attr_def in pk_attrs]

        definition = f"""# Job queue for {self._target.class_name}
{chr(10).join(pk_lines)}
---
status          : enum('pending', 'reserved', 'success', 'error', 'ignore')
priority        : int             # Lower = more urgent (0 = highest priority)
created_time    : datetime(6)     # When job was added to queue
scheduled_time  : datetime(6)     # Process on or after this time
reserved_time=null : datetime(6)  # When job was reserved
completed_time=null : datetime(6) # When job completed
duration=null   : float           # Execution duration in seconds
error_message="" : varchar({ERROR_MESSAGE_LENGTH})  # Error message if failed
error_stack=null : mediumblob     # Full error traceback
user=""         : varchar(255)    # Database user who reserved/completed job
host=""         : varchar(255)    # Hostname of worker
pid=0           : int unsigned    # Process ID of worker
connection_id=0 : bigint unsigned # MySQL connection ID
version=""      : varchar(255)    # Code version
"""
        return definition

    @property
    def definition(self) -> str:
        return self._definition

    @property
    def table_name(self) -> str:
        return self._table_name

    @property
    def target(self) -> "AutoPopulate":
        """The auto-populated table this jobs table is associated with."""
        return self._target

    def _ensure_declared(self) -> None:
        """Ensure the jobs table is declared in the database."""
        if not self.is_declared:
            self.declare()

    # --- Status filter properties ---

    @property
    def pending(self) -> QueryExpression:
        """Return query for pending jobs."""
        self._ensure_declared()
        return self & 'status="pending"'

    @property
    def reserved(self) -> QueryExpression:
        """Return query for reserved jobs."""
        self._ensure_declared()
        return self & 'status="reserved"'

    @property
    def errors(self) -> QueryExpression:
        """Return query for error jobs."""
        self._ensure_declared()
        return self & 'status="error"'

    @property
    def ignored(self) -> QueryExpression:
        """Return query for ignored jobs."""
        self._ensure_declared()
        return self & 'status="ignore"'

    @property
    def completed(self) -> QueryExpression:
        """Return query for completed (success) jobs."""
        self._ensure_declared()
        return self & 'status="success"'

    # --- Core methods ---

    def delete(self) -> None:
        """Delete jobs without confirmation (inherits from delete_quick)."""
        self.delete_quick()

    def drop(self) -> None:
        """Drop the jobs table without confirmation."""
        self.drop_quick()

    def refresh(
        self,
        *restrictions,
        delay: float = 0,
        priority: int = None,
        stale_timeout: float = None,
    ) -> dict:
        """
        Refresh the jobs queue: add new jobs and remove stale ones.

        Operations performed:
        1. Add new jobs: (key_source & restrictions) - target - jobs → insert as 'pending'
        2. Remove stale jobs: pending jobs older than stale_timeout whose keys
           are no longer in key_source

        Args:
            restrictions: Conditions to filter key_source
            delay: Seconds from now until jobs become available for processing.
                   Default: 0 (jobs are immediately available).
                   Uses database server time to avoid clock sync issues.
            priority: Priority for new jobs (lower = more urgent). Default from config.
            stale_timeout: Seconds after which pending jobs are checked for staleness.
                          Default from config.

        Returns:
            {'added': int, 'removed': int} - counts of jobs added and stale jobs removed
        """
        self._ensure_declared()

        if priority is None:
            priority = config.jobs.default_priority
        if stale_timeout is None:
            stale_timeout = config.jobs.stale_timeout

        # Get FK-derived primary key attribute names
        pk_attrs = [name for name, _ in self._get_fk_derived_primary_key()]

        # Step 1: Find new keys to add
        # (key_source & restrictions) - target - jobs
        key_source = self._target.key_source
        if restrictions:
            from .expression import AndList

            key_source = key_source & AndList(restrictions)

        # Project to FK-derived attributes only
        key_source_proj = key_source.proj(*pk_attrs)
        target_proj = self._target.proj(*pk_attrs)
        existing_jobs = self.proj(*pk_attrs)

        # Keys that need jobs: in key_source, not in target, not already in jobs
        new_keys = (key_source_proj - target_proj - existing_jobs).fetch("KEY")

        # Insert new jobs
        added = 0
        for key in new_keys:
            try:
                self._insert_job_with_delay(key, priority, delay)
                added += 1
            except DuplicateError:
                # Job was added by another process
                pass

        # Step 2: Remove stale pending jobs
        # Find pending jobs older than stale_timeout whose keys are not in key_source
        removed = 0
        if stale_timeout > 0:
            stale_condition = f'status="pending" AND ' f"created_time < NOW() - INTERVAL {stale_timeout} SECOND"
            stale_jobs = (self & stale_condition).proj(*pk_attrs)

            # Check which stale jobs are no longer in key_source
            orphaned_keys = (stale_jobs - key_source_proj).fetch("KEY")
            for key in orphaned_keys:
                (self & key).delete_quick()
                removed += 1

        return {"added": added, "removed": removed}

    def _insert_job_with_delay(self, key: dict, priority: int, delay: float) -> None:
        """
        Insert a new job with scheduled_time set using database server time.

        Args:
            key: Primary key dict for the job
            priority: Job priority (lower = more urgent)
            delay: Seconds from now until job becomes available
        """
        # Build column names and values
        pk_attrs = [name for name, _ in self._get_fk_derived_primary_key()]
        columns = pk_attrs + ["status", "priority", "created_time", "scheduled_time", "user", "host", "pid", "connection_id"]

        # Build values
        pk_values = [f"'{key[attr]}'" if isinstance(key[attr], str) else str(key[attr]) for attr in pk_attrs]
        other_values = [
            "'pending'",
            str(priority),
            "NOW(6)",  # created_time
            f"NOW(6) + INTERVAL {delay} SECOND" if delay > 0 else "NOW(6)",  # scheduled_time
            f"'{self._user}'",
            f"'{platform.node()}'",
            str(os.getpid()),
            str(self.connection.connection_id),
        ]

        sql = f"""
            INSERT INTO {self.full_table_name}
            ({', '.join(f'`{c}`' for c in columns)})
            VALUES ({', '.join(pk_values + other_values)})
        """
        self.connection.query(sql)

    def reserve(self, key: dict) -> bool:
        """
        Attempt to reserve a job for processing.

        Updates status to 'reserved' if currently 'pending' and scheduled_time <= now.

        Args:
            key: Primary key dict for the job

        Returns:
            True if reservation successful, False if job not found or not pending.
        """
        self._ensure_declared()

        # Build WHERE clause for the key
        pk_attrs = [name for name, _ in self._get_fk_derived_primary_key()]
        key_conditions = " AND ".join(
            f"`{attr}`='{key[attr]}'" if isinstance(key[attr], str) else f"`{attr}`={key[attr]}" for attr in pk_attrs
        )

        # Attempt atomic update: pending -> reserved
        sql = f"""
            UPDATE {self.full_table_name}
            SET status='reserved',
                reserved_time=NOW(6),
                user='{self._user}',
                host='{platform.node()}',
                pid={os.getpid()},
                connection_id={self.connection.connection_id}
            WHERE {key_conditions}
              AND status='pending'
              AND scheduled_time <= NOW(6)
        """
        result = self.connection.query(sql)
        return result.rowcount > 0

    def complete(self, key: dict, duration: float = None, keep: bool = None) -> None:
        """
        Mark a job as successfully completed.

        Args:
            key: Primary key dict for the job
            duration: Execution duration in seconds
            keep: If True, mark as 'success'. If False, delete the job entry.
                  Default from config (jobs.keep_completed).
        """
        self._ensure_declared()

        if keep is None:
            keep = config.jobs.keep_completed

        pk_attrs = [name for name, _ in self._get_fk_derived_primary_key()]
        job_key = {attr: key[attr] for attr in pk_attrs if attr in key}

        if keep:
            # Update to success status
            duration_sql = f", duration={duration}" if duration is not None else ""
            key_conditions = " AND ".join(
                f"`{attr}`='{job_key[attr]}'" if isinstance(job_key[attr], str) else f"`{attr}`={job_key[attr]}"
                for attr in pk_attrs
            )
            sql = f"""
                UPDATE {self.full_table_name}
                SET status='success',
                    completed_time=NOW(6){duration_sql}
                WHERE {key_conditions}
            """
            self.connection.query(sql)
        else:
            # Delete the job entry
            (self & job_key).delete_quick()

    def error(self, key: dict, error_message: str, error_stack: str = None) -> None:
        """
        Mark a job as failed with error details.

        Args:
            key: Primary key dict for the job
            error_message: Error message string
            error_stack: Full stack trace
        """
        self._ensure_declared()

        # Truncate error message if necessary
        if len(error_message) > ERROR_MESSAGE_LENGTH:
            error_message = error_message[: ERROR_MESSAGE_LENGTH - len(TRUNCATION_APPENDIX)] + TRUNCATION_APPENDIX

        pk_attrs = [name for name, _ in self._get_fk_derived_primary_key()]
        job_key = {attr: key[attr] for attr in pk_attrs if attr in key}

        key_conditions = " AND ".join(
            f"`{attr}`='{job_key[attr]}'" if isinstance(job_key[attr], str) else f"`{attr}`={job_key[attr]}"
            for attr in pk_attrs
        )

        # Escape error message for SQL
        error_message_escaped = error_message.replace("'", "''").replace("\\", "\\\\")

        sql = f"""
            UPDATE {self.full_table_name}
            SET status='error',
                completed_time=NOW(6),
                error_message='{error_message_escaped}'
            WHERE {key_conditions}
        """
        self.connection.query(sql)

        # Update error_stack separately using parameterized query if provided
        if error_stack is not None:
            with config.override(enable_python_native_blobs=True):
                (self & job_key)._update("error_stack", error_stack)

    def ignore(self, key: dict) -> None:
        """
        Mark a key to be ignored (skipped during populate).

        Can be called on keys not yet in the jobs table.

        Args:
            key: Primary key dict for the job
        """
        self._ensure_declared()

        pk_attrs = [name for name, _ in self._get_fk_derived_primary_key()]
        job_key = {attr: key[attr] for attr in pk_attrs if attr in key}

        # Check if job already exists
        if job_key in self:
            # Update existing job to ignore
            key_conditions = " AND ".join(
                f"`{attr}`='{job_key[attr]}'" if isinstance(job_key[attr], str) else f"`{attr}`={job_key[attr]}"
                for attr in pk_attrs
            )
            sql = f"""
                UPDATE {self.full_table_name}
                SET status='ignore'
                WHERE {key_conditions}
            """
            self.connection.query(sql)
        else:
            # Insert new job with ignore status
            self._insert_job_with_status(job_key, "ignore")

    def _insert_job_with_status(self, key: dict, status: str) -> None:
        """Insert a new job with the given status."""
        pk_attrs = [name for name, _ in self._get_fk_derived_primary_key()]
        columns = pk_attrs + ["status", "priority", "created_time", "scheduled_time", "user", "host", "pid", "connection_id"]

        pk_values = [f"'{key[attr]}'" if isinstance(key[attr], str) else str(key[attr]) for attr in pk_attrs]
        other_values = [
            f"'{status}'",
            str(DEFAULT_PRIORITY),
            "NOW(6)",
            "NOW(6)",
            f"'{self._user}'",
            f"'{platform.node()}'",
            str(os.getpid()),
            str(self.connection.connection_id),
        ]

        sql = f"""
            INSERT INTO {self.full_table_name}
            ({', '.join(f'`{c}`' for c in columns)})
            VALUES ({', '.join(pk_values + other_values)})
        """
        self.connection.query(sql)

    def progress(self) -> dict:
        """
        Report detailed progress of job processing.

        Returns:
            Dict with counts for each status and total.
        """
        self._ensure_declared()

        result = {
            "pending": len(self.pending),
            "reserved": len(self.reserved),
            "success": len(self.completed),
            "error": len(self.errors),
            "ignore": len(self.ignored),
        }
        result["total"] = sum(result.values())
        return result

    def fetch_pending(
        self,
        limit: int = None,
        priority: int = None,
    ) -> list[dict]:
        """
        Fetch pending jobs ordered by priority and scheduled time.

        Args:
            limit: Maximum number of jobs to fetch
            priority: Only fetch jobs at this priority or more urgent (lower values)

        Returns:
            List of job key dicts
        """
        self._ensure_declared()

        # Build query for non-stale pending jobs
        query = self & 'status="pending" AND scheduled_time <= NOW(6)'

        if priority is not None:
            query = query & f"priority <= {priority}"

        # Fetch with ordering
        return query.fetch(
            "KEY",
            order_by=["priority ASC", "scheduled_time ASC"],
            limit=limit,
        )
