"""This module defines class dj.AutoPopulate"""

import contextlib
import datetime
import inspect
import logging
import multiprocessing as mp
import signal
import traceback

import deepdiff
from tqdm import tqdm

from .errors import DataJointError, LostConnectionError
from .expression import AndList, QueryExpression

# noinspection PyExceptionInherit,PyCallingNonCallable

logger = logging.getLogger(__name__.split(".")[0])


# --- helper functions for multiprocessing --


def _initialize_populate(table, jobs, populate_kwargs):
    """
    Initialize the process for multiprocessing.
    Saves the unpickled copy of the table to the current process and reconnects.
    """
    process = mp.current_process()
    process.table = table
    process.jobs = jobs
    process.populate_kwargs = populate_kwargs
    table.connection.connect()  # reconnect


def _call_populate1(key):
    """
    Call current process' table._populate1()
    :key - a dict specifying job to compute
    :return: key, error if error, otherwise None
    """
    process = mp.current_process()
    return process.table._populate1(key, process.jobs, **process.populate_kwargs)


class AutoPopulate:
    """
    AutoPopulate is a mixin class that adds the method populate() to a Table class.
    Auto-populated tables must inherit from both Table and AutoPopulate,
    must define the property `key_source`, and must define the callback method `make`.
    """

    _key_source = None
    _allow_insert = False
    _jobs = None

    @property
    def jobs(self):
        """
        Access the job table for this auto-populated table.

        The job table (~~table_name) is created lazily on first access.
        It tracks job status, priority, scheduling, and error information
        for distributed populate operations.

        :return: Job object for this table
        """
        if self._jobs is None:
            from .jobs import Job

            self._jobs = Job(self)
            if not self._jobs.is_declared:
                self._jobs.declare()
        return self._jobs

    def _declare_check(self, primary_key, fk_attribute_map):
        """
        Validate FK-only primary key constraint for auto-populated tables.

        Auto-populated tables (Computed/Imported) must derive all primary key
        attributes from foreign key references. This ensures proper job granularity
        for distributed populate operations.

        This validation can be bypassed by setting:
            dj.config.jobs.allow_new_pk_fields_in_computed_tables = True

        :param primary_key: list of primary key attribute names
        :param fk_attribute_map: dict mapping child_attr -> (parent_table, parent_attr)
        :raises DataJointError: if native PK attributes are found (unless bypassed)
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
    def key_source(self):
        """
        :return: the query expression that yields primary key values to be passed,
        sequentially, to the ``make`` method when populate() is called.
        The default value is the join of the parent tables references from the primary key.
        Subclasses may override they key_source to change the scope or the granularity
        of the make calls.
        """

        def _rename_attributes(table, props):
            return (
                table.proj(**{attr: ref for attr, ref in props["attr_map"].items() if attr != ref})
                if props["aliased"]
                else table.proj()
            )

        if self._key_source is None:
            parents = self.target.parents(primary=True, as_objects=True, foreign_key_info=True)
            if not parents:
                raise DataJointError("A table must have dependencies from its primary key for auto-populate to work")
            self._key_source = _rename_attributes(*parents[0])
            for q in parents[1:]:
                self._key_source *= _rename_attributes(*q)
        return self._key_source

    def make(self, key):
        """
        This method must be implemented by derived classes to perform automated computation.
        The method must implement the following three steps:

        1. Fetch data from tables above in the dependency hierarchy, restricted by the given key.
        2. Compute secondary attributes based on the fetched data.
        3. Insert the new tuple(s) into the current table.

        The method can be implemented either as:
        (a) Regular method: All three steps are performed in a single database transaction.
            The method must return None.
        (b) Generator method:
            The make method is split into three functions:
            - `make_fetch`: Fetches data from the parent tables.
            - `make_compute`: Computes secondary attributes based on the fetched data.
            - `make_insert`: Inserts the computed data into the current table.

            Then populate logic is executes as follows:

            <pseudocode>
            fetched_data1 = self.make_fetch(key)
            computed_result = self.make_compute(key, *fetched_data1)
            begin transaction:
                fetched_data2 = self.make_fetch(key)
                if fetched_data1 != fetched_data2:
                    cancel transaction
                else:
                    self.make_insert(key, *computed_result)
                    commit_transaction
            <pseudocode>

        Importantly, the output of make_fetch is a tuple that serves as the input into `make_compute`.
        The output of `make_compute` is a tuple that serves as the input into `make_insert`.

        The functionality must be strictly divided between these three methods:
        - All database queries must be completed in `make_fetch`.
        - All computation must be completed in `make_compute`.
        - All database inserts must be completed in `make_insert`.

        DataJoint may programmatically enforce this separation in the future.

        :param key: The primary key value used to restrict the data fetching.
        :raises NotImplementedError: If the derived class does not implement the required methods.
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

    @property
    def target(self):
        """
        :return: table to be populated.
        In the typical case, dj.AutoPopulate is mixed into a dj.Table class by
        inheritance and the target is self.
        """
        return self

    def _jobs_to_do(self, restrictions):
        """
        :return: the query yielding the keys to be computed (derived from self.key_source)
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
                % next(name for name in todo.heading.primary_key if name not in self.target.heading)
            )
        except StopIteration:
            pass
        return (todo & AndList(restrictions)).proj()

    def populate(
        self,
        *restrictions,
        suppress_errors=False,
        return_exception_objects=False,
        reserve_jobs=False,
        max_calls=None,
        display_progress=False,
        processes=1,
        make_kwargs=None,
        priority=None,
        refresh=None,
    ):
        """
        ``table.populate()`` calls ``table.make(key)`` for every primary key in
        ``self.key_source`` for which there is not already a tuple in table.

        Two execution modes:

        **Direct mode** (reserve_jobs=False, default):
            Keys computed directly from: (key_source & restrictions) - target
            No job table involvement. Suitable for single-worker scenarios,
            development, and debugging.

        **Distributed mode** (reserve_jobs=True):
            Uses the job table (~~table_name) for multi-worker coordination.
            Supports priority, scheduling, and status tracking.

        :param restrictions: conditions to filter key_source
        :param suppress_errors: if True, collect errors instead of raising
        :param return_exception_objects: return error objects instead of just error messages
        :param reserve_jobs: if True, use job table for distributed processing
        :param max_calls: maximum number of make() calls (total across all processes)
        :param display_progress: if True, show progress bar
        :param processes: number of worker processes
        :param make_kwargs: keyword arguments passed to each make() call
        :param priority: (reserve_jobs only) only process jobs at this priority or more urgent
        :param refresh: (reserve_jobs only) refresh job queue before processing.
            Default from config.jobs.auto_refresh
        :return: dict with "success_count" and "error_list"
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
        keys = (self._jobs_to_do(restrictions) - self.target).fetch("KEY")

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
                self.jobs.refresh(*restrictions, priority=priority)

            # Fetch pending jobs ordered by priority
            pending_query = self.jobs.pending & "scheduled_time <= NOW()"
            if priority is not None:
                pending_query = pending_query & f"priority <= {priority}"

            keys = pending_query.fetch("KEY", order_by="priority ASC, scheduled_time ASC", limit=max_calls)

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

    def _populate1(self, key, jobs, suppress_errors, return_exception_objects, make_kwargs=None):
        """
        Populate table for one source key, calling self.make inside a transaction.

        :param key: dict specifying job to populate
        :param jobs: the Job object or None if not reserve_jobs
        :param suppress_errors: if True, errors are suppressed and returned
        :param return_exception_objects: if True, errors returned as objects
        :return: (key, error) when suppress_errors=True,
            True if successfully invoke one make() call, otherwise False
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

        if key in self.target:  # already populated
            if not is_generator:
                self.connection.cancel_transaction()
            if jobs is not None:
                jobs.complete(key)
            return False

        logger.debug(f"Making {key} -> {self.target.full_table_name}")
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
            logger.debug(f"Error making {key} -> {self.target.full_table_name} - {error_message}")
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
            logger.debug(f"Success making {key} -> {self.target.full_table_name}")

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

    def progress(self, *restrictions, display=False):
        """
        Report the progress of populating the table.
        :return: (remaining, total) -- numbers of tuples to be populated
        """
        todo = self._jobs_to_do(restrictions)
        total = len(todo)
        remaining = len(todo - self.target)
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
        all_attrs = self.target.heading._attributes
        return all_attrs is not None and "_job_start_time" in all_attrs

    def _update_job_metadata(self, key, start_time, duration, version):
        """
        Update hidden job metadata for the given key.

        Args:
            key: Primary key dict identifying the row(s) to update
            start_time: datetime when computation started
            duration: float seconds elapsed
            version: str code version (truncated to 64 chars)
        """
        from .condition import make_condition

        pk_condition = make_condition(self.target, key, set())
        self.connection.query(
            f"UPDATE {self.target.full_table_name} SET "
            "`_job_start_time`=%s, `_job_duration`=%s, `_job_version`=%s "
            f"WHERE {pk_condition}",
            args=(start_time, duration, version[:64] if version else ""),
        )
