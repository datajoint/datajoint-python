"""This module defines class dj.AutoPopulate"""
import logging
import datetime
import traceback
import random
import inspect
from tqdm import tqdm
from .hash import key_hash
from .expression import QueryExpression, AndList
from .errors import DataJointError, LostConnectionError
from .settings import config
from .utils import user_choice, to_camel_case
import signal
import multiprocessing as mp
import contextlib

# noinspection PyExceptionInherit,PyCallingNonCallable

logger = logging.getLogger(__name__.split(".")[0])


# --- helper functions for multiprocessing --


def _initialize_populate(table, reserve_jobs, populate_kwargs):
    """
    Initialize the process for mulitprocessing.
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
        :return: the query yeilding the keys to be computed (derived from self.key_source)
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
        suppress_errors=False,
        return_exception_objects=False,
        reserve_jobs=False,
        order="original",
        limit=None,
        max_calls=None,
        display_progress=False,
        processes=1,
        return_success_count=False,
        make_kwargs=None,
        schedule_jobs=True,
    ):
        """
        ``table.populate()`` calls ``table.make(key)`` for every primary key in
        ``self.key_source`` for which there is not already a tuple in table.

        :param restrictions: a list of restrictions each restrict
            (table.key_source - target.proj())
        :param suppress_errors: if True, do not terminate execution.
        :param return_exception_objects: return error objects instead of just error messages
        :param reserve_jobs: if True, reserve jobs to populate in asynchronous fashion
        :param order: "original"|"reverse"|"random"  - the order of execution
        :param limit: if not None, check at most this many keys
        :param max_calls: if not None, populate at most this many keys
        :param display_progress: if True, report progress_bar
        :param processes: number of processes to use. Set to None to use all cores
        :param return_success_count: if True, return the count of successful `make()` calls.
            If suppress_errors is also True, returns a tuple: (success_count, errors)
        :param make_kwargs: Keyword arguments which do not affect the result of computation
            to be passed down to each ``make()`` call. Computation arguments should be
            specified within the pipeline e.g. using a `dj.Lookup` table.
        :type make_kwargs: dict, optional
        :param schedule_jobs: if True, run schedule_jobs before doing populate (default: True),
            only applicable if reserved_jobs is True
        """
        if self.connection.in_transaction:
            raise DataJointError("Populate cannot be called during a transaction.")

        valid_order = ["original", "reverse", "random"]
        if order not in valid_order:
            raise DataJointError(
                "The order argument must be one of %s" % str(valid_order)
            )
        # define and set up signal handler for SIGTERM:
        if reserve_jobs:

            def handler(signum, frame):
                logger.info("Populate terminated by SIGTERM")
                raise SystemExit("SIGTERM received")

            old_handler = signal.signal(signal.SIGTERM, handler)

            if schedule_jobs:
                self.schedule_jobs(*restrictions)

            keys = (
                self._Jobs
                & {"table_name": self.target.table_name}
                & 'status = "scheduled"'
            ).fetch("key", limit=limit)

            if restrictions:
                # hitting the `key_source` again to apply the restrictions
                # this is expensive/suboptimal
                keys = (self._jobs_to_do(restrictions) & keys).fetch("KEY", limit=limit)
        else:
            keys = (self._jobs_to_do(restrictions) - self.target).fetch(
                "KEY", limit=limit
            )

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
                    if status is not None:
                        if isinstance(status, tuple):
                            error_list.append(status)
                        elif status:
                            success_list.append(1)
            else:
                # spawn multiple processes
                self.connection.close()  # disconnect parent process from MySQL server
                del self.connection._conn.ctx  # SSLContext is not pickleable
                with mp.Pool(
                    processes,
                    _initialize_populate,
                    (self, reserve_jobs, populate_kwargs),
                ) as pool, (
                    tqdm(desc="Processes: ", total=nkeys)
                    if display_progress
                    else contextlib.nullcontext()
                ) as progress_bar:
                    for status in pool.imap(_call_populate1, keys, chunksize=1):
                        if status is not None:
                            if isinstance(status, tuple):
                                error_list.append(status)
                            elif status:
                                success_list.append(1)
                        if display_progress:
                            progress_bar.update()
                self.connection.connect()  # reconnect parent process to MySQL server

        # restore original signal handler:
        if reserve_jobs:
            signal.signal(signal.SIGTERM, old_handler)

        if suppress_errors and return_success_count:
            return sum(success_list), error_list
        if suppress_errors:
            return error_list
        if return_success_count:
            return sum(success_list)

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
        :return: (key, error) when suppress_errors=True, otherwise None
        """
        make = self._make_tuples if hasattr(self, "_make_tuples") else self.make

        if not reserve_jobs or self._Jobs.reserve(
            self.target.table_name, self._job_key(key)
        ):
            self.connection.start_transaction()
            if key in self.target:  # already populated
                self.connection.cancel_transaction()
                self._Jobs.complete(self.target.table_name, self._job_key(key))
            else:
                logger.debug(f"Making {key} -> {self.target.full_table_name}")
                self.__class__._allow_insert = True
                make_start = datetime.datetime.utcnow()
                try:
                    make(dict(key), **(make_kwargs or {}))
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
                    logger.debug(
                        f"Success making {key} -> {self.target.full_table_name}"
                    )
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

    def schedule_jobs(self, *restrictions, purge_invalid_jobs=True):
        """
        Schedule new jobs for this autopopulate table
        :param restrictions: a list of restrictions each restrict
            (table.key_source - target.proj())
        :param purge_invalid_jobs: if True, remove invalid entry from the jobs table (potentially expensive operation)
        :return:
        """
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
