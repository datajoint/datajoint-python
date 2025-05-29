"""This module defines class dj.AutoPopulate"""
import contextlib
import datetime
import inspect
import logging
import multiprocessing as mp
import random
import signal
import traceback

import deepdiff
from tqdm import tqdm

from .errors import DataJointError, LostConnectionError
from .expression import AndList, QueryExpression
from .hash import key_hash
from .settings import config
from .utils import user_choice, to_camel_case

# noinspection PyExceptionInherit,PyCallingNonCallable

logger = logging.getLogger(__name__.split(".")[0])


# --- helper functions for multiprocessing --


def _initialize_populate(table, reserve_jobs, populate_kwargs):
    """
    Initialize the process for multiprocessing.
    Saves the unpickled copy of the table to the current process and reconnects.
    """
    process = mp.current_process()
    process.table = table
    process.reserve_jobs = reserve_jobs
    process.populate_kwargs = populate_kwargs
    table.connection.connect()  # reconnect


def _call_populate1(key):
    """
    Call current process' table._populate1()
    :key - a dict specifying job to compute
    :return: key, error if error, otherwise None
    """
    process = mp.current_process()
    return process.table._populate1(
        key, process.reserve_jobs, **process.populate_kwargs
    )


class AutoPopulate:
    """
    AutoPopulate is a mixin class that adds the method populate() to a Table class.
    Auto-populated tables must inherit from both Table and AutoPopulate,
    must define the property `key_source`, and must define the callback method `make`.
    """

    _key_source = None
    _allow_insert = False

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
                table.proj(
                    **{
                        attr: ref
                        for attr, ref in props["attr_map"].items()
                        if attr != ref
                    }
                )
                if props["aliased"]
                else table.proj()
            )

        if self._key_source is None:
            parents = self.target.parents(
                primary=True, as_objects=True, foreign_key_info=True
            )
            if not parents:
                raise DataJointError(
                    "A table must have dependencies "
                    "from its primary key for auto-populate to work"
                )
            self._key_source = _rename_attributes(*parents[0])
            for q in parents[1:]:
                self._key_source *= _rename_attributes(*q)

        return self._key_source

    def make(self, key):
        """
        Derived classes must implement method `make` that fetches data from tables
        above them in the dependency hierarchy, restricting by the given key,
        computes secondary attributes, and inserts the new tuples into self.
        """
        raise NotImplementedError(
            "Subclasses of AutoPopulate must implement the method `make`"
        )

    @property
    def target(self):
        """
        :return: table to be populated.
        In the typical case, dj.AutoPopulate is mixed into a dj.Table class by
        inheritance and the target is self.
        """
        return self

    def _job_key(self, key):
        """
        :param key:  they key returned for the job from the key source
        :return: the dict to use to generate the job reservation hash
        This method allows subclasses to control the job reservation granularity.
        """
        return key

    def _jobs_to_do(self, restrictions):
        """
        :return: the query yielding the keys to be computed (derived from self.key_source)
        """
        if self.restriction:
            raise DataJointError(
                "Cannot call populate on a restricted table. "
                "Instead, pass conditions to populate() as arguments."
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
                % next(
                    name
                    for name in todo.heading.primary_key
                    if name not in self.target.heading
                )
            )
        except StopIteration:
            pass
        return (todo & AndList(restrictions)).proj()

    def populate(
        self,
        *restrictions,
        keys=None,
        suppress_errors=False,
        return_exception_objects=False,
        reserve_jobs=False,
        order="original",
        limit=None,
        max_calls=None,
        display_progress=False,
        processes=1,
        make_kwargs=None,
        schedule_jobs=True,
    ):
        """
        ``table.populate()`` calls ``table.make(key)`` for every primary key in
        ``self.key_source`` for which there is not already a tuple in table.

        :param restrictions: a list of restrictions each restrict
            (table.key_source - target.proj())
        :param keys: The list of keys (dicts) to send to self.make().
            If None (default), then use self.key_source to query they keys.
        :param suppress_errors: if True, do not terminate execution.
        :param return_exception_objects: return error objects instead of just error messages
        :param reserve_jobs: if True, reserve jobs to populate in asynchronous fashion
        :param order: "original"|"reverse"|"random"  - the order of execution
        :param limit: if not None, check at most this many keys
        :param max_calls: if not None, populate at most this many keys
        :param display_progress: if True, report progress_bar
        :param processes: number of processes to use. Set to None to use all cores
        :param make_kwargs: Keyword arguments which do not affect the result of computation
            to be passed down to each ``make()`` call. Computation arguments should be
            specified within the pipeline e.g. using a `dj.Lookup` table.
        :type make_kwargs: dict, optional
        :param schedule_jobs: if True, run schedule_jobs before doing populate (default: True),
            only applicable if reserved_jobs is True
        :return: a dict with two keys
            "success_count": the count of successful ``make()`` calls in this ``populate()`` call
            "error_list": the error list that is filled if `suppress_errors` is True
        """
        if self.connection.in_transaction:
            raise DataJointError("Populate cannot be called during a transaction.")

        valid_order = ["original", "reverse", "random"]
        if order not in valid_order:
            raise DataJointError(
                "The order argument must be one of %s" % str(valid_order)
            )
        
        if schedule_jobs:
            self.schedule_jobs(*restrictions, purge_invalid_jobs=False)

        # define and set up signal handler for SIGTERM:
        if reserve_jobs:
            def handler(signum, frame):
                logger.info("Populate terminated by SIGTERM")
                raise SystemExit("SIGTERM received")
            old_handler = signal.signal(signal.SIGTERM, handler)

        # retrieve `keys` if not provided
        if keys is None:
            if reserve_jobs:
                keys = (
                    self.jobs
                    & {'status': 'scheduled'}
                ).fetch("key", order_by="timestamp", limit=limit)
                if restrictions:
                    # hitting the `key_source` again to apply the restrictions
                    # this is expensive/suboptimal
                    keys = (self._jobs_to_do(restrictions) & keys).fetch("KEY")
            else:
                keys = (self._jobs_to_do(restrictions) - self.target).fetch(
                    "KEY", limit=limit
                )
        else:
            # exclude "error", "ignore" or "reserved" jobs
            if reserve_jobs:
                exclude_key_hashes = (
                    self.jobs
                    & 'status in ("error", "ignore", "reserved")'
                ).fetch("key_hash")
                keys = [key for key in keys if key_hash(key) not in exclude_key_hashes]

        if order == "reverse":
            keys.reverse()
        elif order == "random":
            random.shuffle(keys)

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
                for key in (
                    tqdm(keys, desc=self.__class__.__name__)
                    if display_progress
                    else keys
                ):
                    status = self._populate1(key, reserve_jobs, **populate_kwargs)
                    if status is True:
                        success_list.append(1)
                    elif isinstance(status, tuple):
                        error_list.append(status)
                    else:
                        assert status is False
            else:
                # spawn multiple processes
                self.connection.close()  # disconnect parent process from MySQL server
                del self.connection._conn.ctx  # SSLContext is not pickleable
                with (
                    mp.Pool(
                        processes, _initialize_populate, (self, jobs, populate_kwargs)
                    ) as pool,
                    (
                        tqdm(desc="Processes: ", total=nkeys)
                        if display_progress
                        else contextlib.nullcontext()
                    ) as progress_bar,
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
                self.connection.connect()  # reconnect parent process to MySQL server

        # restore original signal handler:
        if reserve_jobs:
            signal.signal(signal.SIGTERM, old_handler)

        return {
            "success_count": sum(success_list),
            "error_list": error_list,
        }

    def _populate1(
        self,
        key,
        reserve_jobs,
        suppress_errors,
        return_exception_objects,
        make_kwargs=None,
    ):
        """
        populates table for one source key, calling self.make inside a transaction.
        :param reserve_jobs: if True, reserve jobs to populate in asynchronous fashion
        :param key: dict specifying job to populate
        :param suppress_errors: bool if errors should be suppressed and returned
        :param return_exception_objects: if True, errors must be returned as objects
        :return: (key, error) when suppress_errors=True,
            True if successfully invoke one `make()` call, otherwise False
        """
        # use the legacy `_make_tuples` callback.
        make = self._make_tuples if hasattr(self, "_make_tuples") else self.make

        if reserve_jobs and not self._Jobs.reserve(
            self.target.table_name, self._job_key(key)
        ):
            return False

        # if make is a generator, it transaction can be delayed until the final stage
        is_generator = inspect.isgeneratorfunction(make)
        if not is_generator:
            self.connection.start_transaction()

        if key in self.target:  # already populated
            if not is_generator:
                self.connection.cancel_transaction()
            self._Jobs.complete(self.target.table_name, self._job_key(key))
            return False

        logger.debug(f"Making {key} -> {self.target.full_table_name}")
        self.__class__._allow_insert = True
        make_start = datetime.datetime.utcnow()

        try:
            if not is_generator:
                make(dict(key), **(make_kwargs or {}))
            else:
                # tripartite make - transaction is delayed until the final stage
                gen = make(dict(key), **(make_kwargs or {}))
                fetched_data = next(gen)
                fetch_hash = deepdiff.DeepHash(
                    fetched_data, ignore_iterable_order=False
                )[fetched_data]
                computed_result = next(gen)  # perform the computation
                # fetch and insert inside a transaction
                self.connection.start_transaction()
                gen = make(dict(key), **(make_kwargs or {}))  # restart make
                fetched_data = next(gen)
                if (
                    fetch_hash
                    != deepdiff.DeepHash(fetched_data, ignore_iterable_order=False)[
                        fetched_data
                    ]
                ):  # rollback due to referential integrity fail
                    self.connection.cancel_transaction()
                    return False
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
            logger.debug(
                f"Error making {key} -> {self.target.full_table_name} - {error_message}"
            )
            if reserve_jobs:
                # show error name and error message (if any)
                self._Jobs.error(
                    self.target.table_name,
                    self._job_key(key),
                    error_message=error_message,
                    error_stack=traceback.format_exc(),
                    run_duration=(
                        datetime.datetime.utcnow() - make_start
                    ).total_seconds(),
                )
            if not suppress_errors or isinstance(error, SystemExit):
                raise
            else:
                logger.error(error)
                return key, error if return_exception_objects else error_message
        else:
            self.connection.commit_transaction()
            self._Jobs.complete(
                self.target.table_name,
                self._job_key(key),
                run_duration=(
                    datetime.datetime.utcnow() - make_start
                ).total_seconds(),
            )
            logger.debug(f"Success making {key} -> {self.target.full_table_name}")
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
                    datetime.datetime.strftime(
                        datetime.datetime.now(), "%Y-%m-%d %H:%M:%S"
                    ),
                ),
            )
        return remaining, total

    @property
    def _Jobs(self):
        return self.connection.schemas[self.target.database].jobs

    @property
    def jobs(self):
        return self._Jobs & {"table_name": self.target.table_name}

    def schedule_jobs(self, *restrictions, purge_invalid_jobs=True, min_scheduling_interval=None):
        """
        Schedule new jobs for this autopopulate table by finding keys that need computation.
        
        This method implements an optimization strategy to avoid excessive scheduling:
        1. First checks if any jobs were scheduled recently (within min_scheduling_interval)
        2. If recent jobs exist, skips scheduling to prevent database load
        3. Otherwise, finds keys that need computation and schedules them
        
        The method also optionally purges invalid jobs (jobs that no longer exist in key_source)
        to maintain database cleanliness.
        
        Args:
            restrictions: a list of restrictions each restrict (table.key_source - target.proj())
            purge_invalid_jobs: if True, remove invalid entry from the jobs table (potentially expensive operation)
            min_scheduling_interval: minimum time in seconds that must have passed since last job scheduling.
                If None, uses the value from dj.config["min_scheduling_interval"] (default: None)
            
        Returns:
            None
        """
        if min_scheduling_interval is None:
            min_scheduling_interval = config["min_scheduling_interval"]

        # First check if we have any recent jobs
        if min_scheduling_interval > 0:
            recent_jobs = len(
                self.jobs
                & {"status": "scheduled"}
                & f"timestamp <= UTC_TIMESTAMP()"  # Only consider jobs up to current UTC time
                & f"timestamp >= DATE_SUB(UTC_TIMESTAMP(), INTERVAL {min_scheduling_interval} SECOND)"
            )
            if recent_jobs > 0:
                logger.debug(
                    f"Skipping job scheduling for `{to_camel_case(self.target.table_name)}` - "
                    f"found {recent_jobs} jobs created within last {min_scheduling_interval} seconds"
                )
                return

        try:
            with self.connection.transaction:
                schedule_count = 0
                for key in (self._jobs_to_do(restrictions) - self.target).fetch("KEY"):
                    schedule_count += self._Jobs.schedule(self.target.table_name, key)
        except Exception as e:
            logger.exception(str(e))
        else:
            logger.info(
                f"{schedule_count} new jobs scheduled for `{to_camel_case(self.target.table_name)}`"
            )
        finally:
            if purge_invalid_jobs:
                self.purge_invalid_jobs()

    def purge_invalid_jobs(self):
        """
        Check and remove any invalid/outdated jobs in the JobTable for this autopopulate table
        Job keys that are in the JobTable (regardless of status) but are no longer in the `key_source`
        (e.g. jobs added but entries in upstream table(s) got deleted)
        This is potentially a time-consuming process - but should not expect to have to run very often
        """

        jobs_query = self._Jobs & {"table_name": self.target.table_name}

        invalid_count = len(jobs_query) - len(self._jobs_to_do({}))
        invalid_removed = 0
        if invalid_count > 0:
            for key, job_key in zip(*jobs_query.fetch("KEY", "key")):
                if not (self._jobs_to_do({}) & job_key):
                    (jobs_query & key).delete()
                    invalid_removed += 1

            logger.info(
                f"{invalid_removed}/{invalid_count} invalid jobs removed for `{to_camel_case(self.target.table_name)}`"
            )
