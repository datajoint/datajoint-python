"""This module defines class dj.AutoPopulate"""

from __future__ import annotations

import contextlib
import datetime
import inspect
import logging
import multiprocessing as mp
import signal
import traceback
from typing import TYPE_CHECKING, Any, Generator

import deepdiff
from tqdm import tqdm

from .errors import DataJointError, LostConnectionError
from .expression import AndList, QueryExpression

if TYPE_CHECKING:
    from .jobs import Job
    from .table import Table

# noinspection PyExceptionInherit,PyCallingNonCallable

logger = logging.getLogger(__name__.split(".")[0])


# --- helper functions for multiprocessing --


def _initialize_populate(table: Table, jobs: Job | None, populate_kwargs: dict[str, Any]) -> None:
    """
    Initialize a worker process for multiprocessing.

    Saves the unpickled table to the current process and reconnects to database.

    Parameters
    ----------
    table : Table
        Table instance to populate.
    jobs : Job or None
        Job management object or None for direct mode.
    populate_kwargs : dict
        Arguments for _populate1().
    """
    process = mp.current_process()
    process.table = table
    process.jobs = jobs
    process.populate_kwargs = populate_kwargs
    table.connection.connect()  # reconnect


def _call_populate1(key: dict[str, Any]) -> bool | tuple[dict[str, Any], Any]:
    """
    Call _populate1() for a single key in the worker process.

    Parameters
    ----------
    key : dict
        Primary key specifying job to compute.

    Returns
    -------
    bool or tuple
        Result from _populate1().
    """
    process = mp.current_process()
    return process.table._populate1(key, process.jobs, **process.populate_kwargs)


class AutoPopulate:
    """
    Mixin class that adds automated population to Table classes.

    Auto-populated tables (Computed, Imported) inherit from both Table and
    AutoPopulate. They must implement the ``make()`` method that computes
    and inserts data for one primary key.

    Attributes
    ----------
    key_source : QueryExpression
        Query yielding keys to be populated. Default is join of FK parents.
    jobs : Job
        Job table (``~~table_name``) for distributed processing.

    Notes
    -----
    Subclasses may override ``key_source`` to customize population scope.
    """

    _key_source = None
    _allow_insert = False
    _jobs = None

    class _JobsDescriptor:
        """Descriptor allowing jobs access on both class and instance."""

        def __get__(self, obj, objtype=None):
            """
            Access the job table for this auto-populated table.

            The job table (``~~table_name``) is created lazily on first access.
            It tracks job status, priority, scheduling, and error information
            for distributed populate operations.

            Can be accessed on either the class or an instance::

                # Both work equivalently
                Analysis.jobs.refresh()
                Analysis().jobs.refresh()

            Returns
            -------
            Job
                Job management object for this table.
            """
            if obj is None:
                # Accessed on class - instantiate first
                obj = objtype()
            if obj._jobs is None:
                from .jobs import Job

                obj._jobs = Job(obj)
                if not obj._jobs.is_declared:
                    obj._jobs.declare()
            return obj._jobs

    jobs: Job = _JobsDescriptor()

    def _declare_check(self, primary_key: list[str], fk_attribute_map: dict[str, tuple[str, str]]) -> None:
        """
        Validate FK-only primary key constraint for auto-populated tables.

        Auto-populated tables (Computed/Imported) must derive all primary key
        attributes from foreign key references. This ensures proper job granularity
        for distributed populate operations.

        Parameters
        ----------
        primary_key : list
            List of primary key attribute names.
        fk_attribute_map : dict
            Mapping of child_attr -> (parent_table, parent_attr).

        Raises
        ------
        DataJointError
            If native (non-FK) PK attributes are found, unless bypassed via
            ``dj.config.jobs.allow_new_pk_fields_in_computed_tables = True``.
        """
        from .settings import config

        # Check if validation is bypassed
        if config.jobs.allow_new_pk_fields_in_computed_tables:
            return

        # Check for native (non-FK) primary key attributes
        native_pk_attrs = [attr for attr in primary_key if attr not in fk_attribute_map]

        if native_pk_attrs:
            raise DataJointError(
                f"Auto-populated table `{self.full_table_name}` has non-FK primary key "
                f"attribute(s): {', '.join(native_pk_attrs)}. "
                f"Computed and Imported tables must derive all primary key attributes "
                f"from foreign key references. The make() method is called once per entity "
                f"(row) in the table. If you need to compute multiple entities per job, "
                f"define a Part table to store them. "
                f"To bypass this restriction, set: dj.config.jobs.allow_new_pk_fields_in_computed_tables = True"
            )

    @property
    def key_source(self) -> QueryExpression:
        """
        Query expression yielding keys to be populated.

        Returns the primary key values to be passed sequentially to ``make()``
        when ``populate()`` is called. The default is the join of parent tables
        referenced from the primary key.

        Returns
        -------
        QueryExpression
            Expression yielding keys for population.

        Notes
        -----
        Subclasses may override to change the scope or granularity of make calls.
        """

        def _rename_attributes(table, props):
            return (
                table.proj(**{attr: ref for attr, ref in props["attr_map"].items() if attr != ref})
                if props["aliased"]
                else table.proj()
            )

        if self._key_source is None:
            parents = self.parents(primary=True, as_objects=True, foreign_key_info=True)
            if not parents:
                raise DataJointError("A table must have dependencies from its primary key for auto-populate to work")
            self._key_source = _rename_attributes(*parents[0])
            for q in parents[1:]:
                self._key_source *= _rename_attributes(*q)
        return self._key_source

    def make(self, key: dict[str, Any]) -> None | Generator[Any, Any, None]:
        """
        Compute and insert data for one key.

        Must be implemented by subclasses to perform automated computation.
        The method implements three steps:

        1. Fetch data from parent tables, restricted by the given key
        2. Compute secondary attributes based on the fetched data
        3. Insert the new row(s) into the current table

        Parameters
        ----------
        key : dict
            Primary key value identifying the entity to compute.

        Raises
        ------
        NotImplementedError
            If neither ``make()`` nor the tripartite methods are implemented.

        Notes
        -----
        **Simple make**: Implement as a regular method that performs all three
        steps in a single database transaction. Must return None.

        **Tripartite make**: For long-running computations, implement:

        - ``make_fetch(key)``: Fetch data from parent tables
        - ``make_compute(key, *fetched_data)``: Compute results
        - ``make_insert(key, *computed_result)``: Insert results

        The tripartite pattern allows computation outside the transaction,
        with referential integrity checking before commit.
        """

        if not (hasattr(self, "make_fetch") and hasattr(self, "make_insert") and hasattr(self, "make_compute")):
            # user must implement `make`
            raise NotImplementedError(
                "Subclasses of AutoPopulate must implement the method `make` "
                "or (`make_fetch` + `make_compute` + `make_insert`)"
            )

        # User has implemented `_fetch`, `_compute`, and `_insert` methods instead

        # Step 1: Fetch data from parent tables
        fetched_data = self.make_fetch(key)  # fetched_data is a tuple
        computed_result = yield fetched_data  # passed as input into make_compute

        # Step 2: If computed result is not passed in, compute the result
        if computed_result is None:
            # this is only executed in the first invocation
            computed_result = self.make_compute(key, *fetched_data)
            yield computed_result  # this is passed to the second invocation of make

        # Step 3: Insert the computed result into the current table.
        self.make_insert(key, *computed_result)
        yield

    def _jobs_to_do(self, restrictions: tuple) -> QueryExpression:
        """
        Return the query yielding keys to be computed.

        Parameters
        ----------
        restrictions : tuple
            Conditions to filter key_source.

        Returns
        -------
        QueryExpression
            Keys derived from key_source that need computation.
        """
        if self.restriction:
            raise DataJointError(
                "Cannot call populate on a restricted table. Instead, pass conditions to populate() as arguments."
            )
        todo = self.key_source

        # key_source is a QueryExpression subclass -- trigger instantiation
        if inspect.isclass(todo) and issubclass(todo, QueryExpression):
            todo = todo()

        if not isinstance(todo, QueryExpression):
            raise DataJointError("Invalid key_source value")

        try:
            # check if target lacks any attributes from the primary key of key_source
            raise DataJointError(
                "The populate target lacks attribute %s "
                "from the primary key of key_source"
                % next(name for name in todo.heading.primary_key if name not in self.heading)
            )
        except StopIteration:
            pass
        return (todo & AndList(restrictions)).proj()

    def populate(
        self,
        *restrictions: Any,
        suppress_errors: bool = False,
        return_exception_objects: bool = False,
        reserve_jobs: bool = False,
        max_calls: int | None = None,
        display_progress: bool = False,
        processes: int = 1,
        make_kwargs: dict[str, Any] | None = None,
        priority: int | None = None,
        refresh: bool | None = None,
    ) -> dict[str, Any]:
        """
        Populate the table by calling ``make()`` for unpopulated keys.

        Calls ``make(key)`` for every primary key in ``key_source`` for which
        there is not already a row in this table.

        Parameters
        ----------
        *restrictions
            Conditions to filter key_source.
        suppress_errors : bool, optional
            If True, collect errors instead of raising. Default False.
        return_exception_objects : bool, optional
            If True, return exception objects instead of messages. Default False.
        reserve_jobs : bool, optional
            If True, use job table for distributed processing. Default False.
        max_calls : int, optional
            Maximum number of ``make()`` calls.
        display_progress : bool, optional
            If True, show progress bar. Default False.
        processes : int, optional
            Number of worker processes. Default 1.
        make_kwargs : dict, optional
            Keyword arguments passed to each ``make()`` call.
        priority : int, optional
            (Distributed mode) Only process jobs at this priority or higher.
        refresh : bool, optional
            (Distributed mode) Refresh job queue before processing.
            Default from ``config.jobs.auto_refresh``.

        Returns
        -------
        dict
            ``{"success_count": int, "error_list": list}``.

        Notes
        -----
        **Direct mode** (``reserve_jobs=False``): Keys computed from
        ``(key_source & restrictions) - target``. No job table. Suitable for
        single-worker, development, and debugging.

        **Distributed mode** (``reserve_jobs=True``): Uses job table
        (``~~table_name``) for multi-worker coordination with priority and
        status tracking.
        """
        if self.connection.in_transaction:
            raise DataJointError("Populate cannot be called during a transaction.")

        if reserve_jobs:
            return self._populate_distributed(
                *restrictions,
                suppress_errors=suppress_errors,
                return_exception_objects=return_exception_objects,
                max_calls=max_calls,
                display_progress=display_progress,
                processes=processes,
                make_kwargs=make_kwargs,
                priority=priority,
                refresh=refresh,
            )
        else:
            return self._populate_direct(
                *restrictions,
                suppress_errors=suppress_errors,
                return_exception_objects=return_exception_objects,
                max_calls=max_calls,
                display_progress=display_progress,
                processes=processes,
                make_kwargs=make_kwargs,
            )

    def _populate_direct(
        self,
        *restrictions,
        suppress_errors,
        return_exception_objects,
        max_calls,
        display_progress,
        processes,
        make_kwargs,
    ):
        """
        Populate without job table coordination.

        Computes keys directly from key_source, suitable for single-worker
        execution, development, and debugging.
        """
        keys = (self._jobs_to_do(restrictions) - self).keys()

        logger.debug("Found %d keys to populate" % len(keys))

        keys = keys[:max_calls]
        nkeys = len(keys)

        error_list = []
        success_list = []

        if nkeys:
            processes = min(_ for _ in (processes, nkeys, mp.cpu_count()) if _)

            populate_kwargs = dict(
                suppress_errors=suppress_errors,
                return_exception_objects=return_exception_objects,
                make_kwargs=make_kwargs,
            )

            if processes == 1:
                for key in tqdm(keys, desc=self.__class__.__name__) if display_progress else keys:
                    status = self._populate1(key, jobs=None, **populate_kwargs)
                    if status is True:
                        success_list.append(1)
                    elif isinstance(status, tuple):
                        error_list.append(status)
                    else:
                        assert status is False
            else:
                # spawn multiple processes
                self.connection.close()
                del self.connection._conn.ctx  # SSLContext is not pickleable
                with (
                    mp.Pool(processes, _initialize_populate, (self, None, populate_kwargs)) as pool,
                    tqdm(desc="Processes: ", total=nkeys) if display_progress else contextlib.nullcontext() as progress_bar,
                ):
                    for status in pool.imap(_call_populate1, keys, chunksize=1):
                        if status is True:
                            success_list.append(1)
                        elif isinstance(status, tuple):
                            error_list.append(status)
                        else:
                            assert status is False
                        if display_progress:
                            progress_bar.update()
                self.connection.connect()

        return {
            "success_count": sum(success_list),
            "error_list": error_list,
        }

    def _populate_distributed(
        self,
        *restrictions,
        suppress_errors,
        return_exception_objects,
        max_calls,
        display_progress,
        processes,
        make_kwargs,
        priority,
        refresh,
    ):
        """
        Populate with job table coordination.

        Uses job table for multi-worker coordination, priority scheduling,
        and status tracking.
        """
        from .settings import config

        # Define a signal handler for SIGTERM
        def handler(signum, frame):
            logger.info("Populate terminated by SIGTERM")
            raise SystemExit("SIGTERM received")

        old_handler = signal.signal(signal.SIGTERM, handler)

        try:
            # Refresh job queue if configured
            if refresh is None:
                refresh = config.jobs.auto_refresh
            if refresh:
                # Use delay=-1 to ensure jobs are immediately schedulable
                # (avoids race condition with scheduled_time <= NOW(3) check)
                self.jobs.refresh(*restrictions, priority=priority, delay=-1)

            # Fetch pending jobs ordered by priority (use NOW(3) to match CURRENT_TIMESTAMP(3) precision)
            pending_query = self.jobs.pending & "scheduled_time <= NOW(3)"
            if priority is not None:
                pending_query = pending_query & f"priority <= {priority}"

            keys = pending_query.keys(order_by="priority ASC, scheduled_time ASC", limit=max_calls)

            logger.debug("Found %d pending jobs to populate" % len(keys))

            nkeys = len(keys)
            error_list = []
            success_list = []

            if nkeys:
                processes = min(_ for _ in (processes, nkeys, mp.cpu_count()) if _)

                populate_kwargs = dict(
                    suppress_errors=suppress_errors,
                    return_exception_objects=return_exception_objects,
                    make_kwargs=make_kwargs,
                )

                if processes == 1:
                    for key in tqdm(keys, desc=self.__class__.__name__) if display_progress else keys:
                        status = self._populate1(key, jobs=self.jobs, **populate_kwargs)
                        if status is True:
                            success_list.append(1)
                        elif isinstance(status, tuple):
                            error_list.append(status)
                        # status is False means job was already reserved
                else:
                    # spawn multiple processes
                    self.connection.close()
                    del self.connection._conn.ctx  # SSLContext is not pickleable
                    with (
                        mp.Pool(processes, _initialize_populate, (self, self.jobs, populate_kwargs)) as pool,
                        tqdm(desc="Processes: ", total=nkeys)
                        if display_progress
                        else contextlib.nullcontext() as progress_bar,
                    ):
                        for status in pool.imap(_call_populate1, keys, chunksize=1):
                            if status is True:
                                success_list.append(1)
                            elif isinstance(status, tuple):
                                error_list.append(status)
                            if display_progress:
                                progress_bar.update()
                    self.connection.connect()

            return {
                "success_count": sum(success_list),
                "error_list": error_list,
            }
        finally:
            signal.signal(signal.SIGTERM, old_handler)

    def _populate1(
        self,
        key: dict[str, Any],
        jobs: Job | None,
        suppress_errors: bool,
        return_exception_objects: bool,
        make_kwargs: dict[str, Any] | None = None,
    ) -> bool | tuple[dict[str, Any], Any]:
        """
        Populate table for one key, calling make() inside a transaction.

        Parameters
        ----------
        key : dict
            Primary key specifying the job to populate.
        jobs : Job or None
            Job object for distributed mode, None for direct mode.
        suppress_errors : bool
            If True, errors are suppressed and returned.
        return_exception_objects : bool
            If True, return exception objects instead of messages.
        make_kwargs : dict, optional
            Keyword arguments passed to ``make()``.

        Returns
        -------
        bool or tuple
            True if make() succeeded, False if skipped (already done or reserved),
            (key, error) tuple if suppress_errors=True and error occurred.
        """
        import time

        # use the legacy `_make_tuples` callback.
        make = self._make_tuples if hasattr(self, "_make_tuples") else self.make

        # Try to reserve the job (distributed mode only)
        if jobs is not None and not jobs.reserve(key):
            return False

        start_time = time.time()

        # if make is a generator, transaction can be delayed until the final stage
        is_generator = inspect.isgeneratorfunction(make)
        if not is_generator:
            self.connection.start_transaction()

        if key in self:  # already populated
            if not is_generator:
                self.connection.cancel_transaction()
            if jobs is not None:
                jobs.complete(key)
            return False

        logger.debug(f"Making {key} -> {self.full_table_name}")
        self.__class__._allow_insert = True

        try:
            if not is_generator:
                make(dict(key), **(make_kwargs or {}))
            else:
                # tripartite make - transaction is delayed until the final stage
                gen = make(dict(key), **(make_kwargs or {}))
                fetched_data = next(gen)
                fetch_hash = deepdiff.DeepHash(fetched_data, ignore_iterable_order=False)[fetched_data]
                computed_result = next(gen)  # perform the computation
                # fetch and insert inside a transaction
                self.connection.start_transaction()
                gen = make(dict(key), **(make_kwargs or {}))  # restart make
                fetched_data = next(gen)
                if (
                    fetch_hash != deepdiff.DeepHash(fetched_data, ignore_iterable_order=False)[fetched_data]
                ):  # raise error if fetched data has changed
                    raise DataJointError("Referential integrity failed! The `make_fetch` data has changed")
                gen.send(computed_result)  # insert

        except (KeyboardInterrupt, SystemExit, Exception) as error:
            try:
                self.connection.cancel_transaction()
            except LostConnectionError:
                pass
            error_message = "{exception}{msg}".format(
                exception=error.__class__.__name__,
                msg=": " + str(error) if str(error) else "",
            )
            logger.debug(f"Error making {key} -> {self.full_table_name} - {error_message}")
            if jobs is not None:
                jobs.error(key, error_message=error_message, error_stack=traceback.format_exc())
            if not suppress_errors or isinstance(error, SystemExit):
                raise
            else:
                logger.error(error)
                return key, error if return_exception_objects else error_message
        else:
            self.connection.commit_transaction()
            duration = time.time() - start_time
            logger.debug(f"Success making {key} -> {self.full_table_name}")

            # Update hidden job metadata if table has the columns
            if self._has_job_metadata_attrs():
                from .jobs import _get_job_version

                self._update_job_metadata(
                    key,
                    start_time=datetime.datetime.fromtimestamp(start_time),
                    duration=duration,
                    version=_get_job_version(),
                )

            if jobs is not None:
                jobs.complete(key, duration=duration)
            return True
        finally:
            self.__class__._allow_insert = False

    def progress(self, *restrictions: Any, display: bool = False) -> tuple[int, int]:
        """
        Report the progress of populating the table.

        Uses a single aggregation query to efficiently compute both total and
        remaining counts.

        Parameters
        ----------
        *restrictions
            Conditions to restrict key_source.
        display : bool, optional
            If True, log the progress. Default False.

        Returns
        -------
        tuple
            (remaining, total) - number of keys yet to populate and total keys.
        """
        todo = self._jobs_to_do(restrictions)

        # Get primary key attributes from key_source for join condition
        # These are the "job keys" - the granularity at which populate() works
        pk_attrs = todo.primary_key
        assert pk_attrs, "key_source must have a primary key"

        # Find common attributes between key_source and self for the join
        # This handles cases where self has additional PK attributes
        common_attrs = [attr for attr in pk_attrs if attr in self.heading.names]

        if not common_attrs:
            # No common attributes - fall back to two-query method
            total = len(todo)
            remaining = len(todo - self)
        else:
            # Build a single query that computes both total and remaining
            # Using LEFT JOIN with COUNT(DISTINCT) to handle 1:many relationships
            todo_sql = todo.make_sql()
            target_sql = self.make_sql()

            # Build join condition on common attributes
            join_cond = " AND ".join(f"`$ks`.`{attr}` = `$tgt`.`{attr}`" for attr in common_attrs)

            # Build DISTINCT key expression for counting unique jobs
            # Use CONCAT for composite keys to create a single distinct value
            if len(pk_attrs) == 1:
                distinct_key = f"`$ks`.`{pk_attrs[0]}`"
                null_check = f"`$tgt`.`{common_attrs[0]}`"
            else:
                distinct_key = "CONCAT_WS('|', {})".format(", ".join(f"`$ks`.`{attr}`" for attr in pk_attrs))
                null_check = f"`$tgt`.`{common_attrs[0]}`"

            # Single aggregation query:
            # - COUNT(DISTINCT key) gives total unique jobs in key_source
            # - Remaining = jobs where no matching target row exists
            sql = f"""
                SELECT
                    COUNT(DISTINCT {distinct_key}) AS total,
                    COUNT(DISTINCT CASE WHEN {null_check} IS NULL THEN {distinct_key} END) AS remaining
                FROM ({todo_sql}) AS `$ks`
                LEFT JOIN ({target_sql}) AS `$tgt` ON {join_cond}
            """

            result = self.connection.query(sql).fetchone()
            total, remaining = result

        if display:
            logger.info(
                "%-20s" % self.__class__.__name__
                + " Completed %d of %d (%2.1f%%)   %s"
                % (
                    total - remaining,
                    total,
                    100 - 100 * remaining / (total + 1e-12),
                    datetime.datetime.strftime(datetime.datetime.now(), "%Y-%m-%d %H:%M:%S"),
                ),
            )
        return remaining, total

    def _has_job_metadata_attrs(self):
        """Check if table has hidden job metadata columns."""
        # Access _attributes directly to include hidden attributes
        all_attrs = self.heading._attributes
        return all_attrs is not None and "_job_start_time" in all_attrs

    def _update_job_metadata(self, key, start_time, duration, version):
        """
        Update hidden job metadata for the given key.

        Parameters
        ----------
        key : dict
            Primary key identifying the row(s) to update.
        start_time : datetime
            When computation started.
        duration : float
            Computation duration in seconds.
        version : str
            Code version (truncated to 64 chars).
        """
        from .condition import make_condition

        pk_condition = make_condition(self, key, set())
        self.connection.query(
            f"UPDATE {self.full_table_name} SET "
            "`_job_start_time`=%s, `_job_duration`=%s, `_job_version`=%s "
            f"WHERE {pk_condition}",
            args=(start_time, duration, version[:64] if version else ""),
        )
