"""This module defines class dj.AutoPopulate"""
import logging
import datetime
import traceback
import random
import inspect
from tqdm import tqdm
from .expression import QueryExpression, AndList
from .errors import DataJointError, LostConnectionError
from .table import FreeTable
import signal

# noinspection PyExceptionInherit,PyCallingNonCallable

logger = logging.getLogger(__name__)


class AutoPopulate:
    """
    AutoPopulate is a mixin class that adds the method populate() to a Relation class.
    Auto-populated relations must inherit from both Relation and AutoPopulate,
    must define the property `key_source`, and must define the callback method `make`.
    """
    _key_source = None
    _allow_insert = False

    @property
    def key_source(self):
        """
        :return: the relation whose primary key values are passed, sequentially, to the
                ``make`` method when populate() is called.
                The default value is the join of the parent relations.
                Users may override to change the granularity or the scope of populate() calls.
        """
        def parent_gen(self):
            if self.target.full_table_name not in self.connection.dependencies:
                self.connection.dependencies.load()
            for parent_name, fk_props in self.target.parents(primary=True).items():
                if not parent_name.isdigit():  # simple foreign key
                    yield FreeTable(self.connection, parent_name).proj()
                else:
                    grandparent = list(self.connection.dependencies.in_edges(parent_name))[0][0]
                    yield FreeTable(self.connection, grandparent).proj(**{
                        attr: ref for attr, ref in fk_props['attr_map'].items() if ref != attr})

        if self._key_source is None:
            parents = parent_gen(self)
            try:
                self._key_source = next(parents)
            except StopIteration:
                raise DataJointError('A relation must have primary dependencies for auto-populate to work') from None
            for q in parents:
                self._key_source *= q
        return self._key_source

    def make(self, key):
        """
        Derived classes must implement method `make` that fetches data from tables that are
        above them in the dependency hierarchy, restricting by the given key, computes dependent
        attributes, and inserts the new tuples into self.
        """
        raise NotImplementedError('Subclasses of AutoPopulate must implement the method `make`')

    @property
    def target(self):
        """
        relation to be populated.
        Typically, AutoPopulate are mixed into a Relation object and the target is self.
        """
        return self

    def _job_key(self, key):
        """
        :param key:  they key returned for the job from the key source
        :return: the dict to use to generate the job reservation hash
        """
        return key

    def _jobs_to_do(self, restrictions):
        """
        :return: the relation containing the keys to be computed (derived from self.key_source)
        """
        if self.restriction:
            raise DataJointError('Cannot call populate on a restricted table. '
                                 'Instead, pass conditions to populate() as arguments.')
        todo = self.key_source

        # key_source is a QueryExpression subclass -- trigger instantiation
        if inspect.isclass(todo) and issubclass(todo, QueryExpression):
            todo = todo()

        if not isinstance(todo, QueryExpression):
            raise DataJointError('Invalid key_source value')
        # check if target lacks any attributes from the primary key of key_source
        try:
            raise DataJointError(
                'The populate target lacks attribute %s from the primary key of key_source' % next(
                    name for name in todo.heading.primary_key if name not in self.target.heading))
        except StopIteration:
            pass
        return (todo & AndList(restrictions)).proj()

    def populate(self, *restrictions, suppress_errors=False, return_exception_objects=False,
                 reserve_jobs=False, order="original", limit=None, max_calls=None,
                 display_progress=False):
        """
        rel.populate() calls rel.make(key) for every primary key in self.key_source
        for which there is not already a tuple in rel.
        :param restrictions: a list of restrictions each restrict (rel.key_source - target.proj())
        :param suppress_errors: if True, do not terminate execution.
        :param return_exception_objects: return error objects instead of just error messages
        :param reserve_jobs: if true, reserves job to populate in asynchronous fashion
        :param order: "original"|"reverse"|"random"  - the order of execution
        :param display_progress: if True, report progress_bar
        :param limit: if not None, checks at most that many keys
        :param max_calls: if not None, populates at max that many keys
        """
        if self.connection.in_transaction:
            raise DataJointError('Populate cannot be called during a transaction.')

        valid_order = ['original', 'reverse', 'random']
        if order not in valid_order:
            raise DataJointError('The order argument must be one of %s' % str(valid_order))
        error_list = [] if suppress_errors else None
        jobs = self.connection.schemas[self.target.database].jobs if reserve_jobs else None

        # define and setup signal handler for SIGTERM
        if reserve_jobs:
            def handler(signum, frame):
                logger.info('Populate terminated by SIGTERM')
                raise SystemExit('SIGTERM received')
            old_handler = signal.signal(signal.SIGTERM, handler)

        keys = (self._jobs_to_do(restrictions) - self.target).fetch("KEY", limit=limit)
        if order == "reverse":
            keys.reverse()
        elif order == "random":
            random.shuffle(keys)

        call_count = 0
        logger.info('Found %d keys to populate' % len(keys))

        make = self._make_tuples if hasattr(self, '_make_tuples') else self.make

        for key in (tqdm(keys) if display_progress else keys):
            if max_calls is not None and call_count >= max_calls:
                break
            if not reserve_jobs or jobs.reserve(self.target.table_name, self._job_key(key)):
                self.connection.start_transaction()
                if key in self.target:  # already populated
                    self.connection.cancel_transaction()
                    if reserve_jobs:
                        jobs.complete(self.target.table_name, self._job_key(key))
                else:
                    logger.info('Populating: ' + str(key))
                    call_count += 1
                    self.__class__._allow_insert = True
                    try:
                        make(dict(key))
                    except (KeyboardInterrupt, SystemExit, Exception) as error:
                        try:
                            self.connection.cancel_transaction()
                        except LostConnectionError:
                            pass
                        error_message = '{exception}{msg}'.format(
                            exception=error.__class__.__name__,
                            msg=': ' + str(error) if str(error) else '')
                        if reserve_jobs:
                            # show error name and error message (if any)
                            jobs.error(
                                self.target.table_name, self._job_key(key),
                                error_message=error_message, error_stack=traceback.format_exc())
                        if not suppress_errors or isinstance(error, SystemExit):
                            raise
                        else:
                            logger.error(error)
                            error_list.append((key, error if return_exception_objects else error_message))
                    else:
                        self.connection.commit_transaction()
                        if reserve_jobs:
                            jobs.complete(self.target.table_name, self._job_key(key))
                    finally:
                        self.__class__._allow_insert = False

        # place back the original signal handler
        if reserve_jobs:
            signal.signal(signal.SIGTERM, old_handler)
        return error_list

    def progress(self, *restrictions, display=True):
        """
        report progress of populating the table
        :return: remaining, total -- tuples to be populated
        """
        todo = self._jobs_to_do(restrictions)
        total = len(todo)
        remaining = len(todo - self.target)
        if display:
            print('%-20s' % self.__class__.__name__,
                  'Completed %d of %d (%2.1f%%)   %s' % (
                      total - remaining, total, 100 - 100 * remaining / (total+1e-12),
                      datetime.datetime.strftime(datetime.datetime.now(), '%Y-%m-%d %H:%M:%S')), flush=True)
        return remaining, total
