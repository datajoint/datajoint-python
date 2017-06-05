"""autopopulate containing the dj.AutoPopulate class. See `dj.AutoPopulate` for more info."""
import logging
import datetime
import random
from pymysql import OperationalError
from .relational_operand import RelationalOperand, AndList
from . import DataJointError
from .base_relation import FreeRelation
import signal

# noinspection PyExceptionInherit,PyCallingNonCallable

logger = logging.getLogger(__name__)


class AutoPopulate:
    """
    AutoPopulate is a mixin class that adds the method populate() to a Relation class.
    Auto-populated relations must inherit from both Relation and AutoPopulate,
    must define the property `key_source`, and must define the callback method _make_tuples.
    """
    _jobs = None
    _key_source = None

    @property
    def key_source(self):
        """
        :return: the relation whose primary key values are passed, sequentially, to the
                `_make_tuples` method when populate() is called.The default value is the
                join of the parent relations. Users may override to change the granularity
                or the scope of populate() calls.
        """
        if self._key_source is None:
            self.connection.dependencies.load(self.full_table_name)
            parents = self.target.parents(primary=True)
            if not parents:
                raise DataJointError('A relation must have parent relations to be able to be populated')
            self._key_source = FreeRelation(self.connection, parents.pop(0)).proj()
            while parents:
                self._key_source *= FreeRelation(self.connection, parents.pop(0)).proj()
        return self._key_source

    def _make_tuples(self, key):
        """
        Derived classes must implement method _make_tuples that fetches data from tables that are
        above them in the dependency hierarchy, restricting by the given key, computes dependent
        attributes, and inserts the new tuples into self.
        """
        raise NotImplementedError('Subclasses of AutoPopulate must implement the method "_make_tuples"')

    @property
    def target(self):
        """
        relation to be populated.
        Typically, AutoPopulate are mixed into a Relation object and the target is self.
        """
        return self

    def populate(self, *restrictions, suppress_errors=False, reserve_jobs=False, order="original"):
        """
        rel.populate() calls rel._make_tuples(key) for every primary key in self.key_source
        for which there is not already a tuple in rel.

        :param restrictions: a list of restrictions each restrict (rel.key_source - target.proj())
        :param suppress_errors: suppresses error if true
        :param reserve_jobs: if true, reserves job to populate in asynchronous fashion
        :param order: "original"|"reverse"|"random"  - the order of execution
        """
        if self.connection.in_transaction:
            raise DataJointError('Populate cannot be called during a transaction.')

        valid_order = ['original', 'reverse', 'random']
        if order not in valid_order:
            raise DataJointError('The order argument must be one of %s' % str(valid_order))

        todo = self.key_source
        if not isinstance(todo, RelationalOperand):
            raise DataJointError('Invalid key_source value')
        todo = todo.proj() & AndList(restrictions)

        error_list = [] if suppress_errors else None

        jobs = self.connection.jobs[self.target.database] if reserve_jobs else None

        # define and setup signal handler for SIGTERM
        if reserve_jobs:
            def handler(signum, frame):
                logger.info('Populate terminated by SIGTERM')
                raise SystemExit('SIGTERM received')
            old_handler = signal.signal(signal.SIGTERM, handler)

        todo -= self.target
        keys = list(todo.fetch.keys())
        if order == "reverse":
            keys.reverse()
        elif order == "random":
            random.shuffle(keys)

        logger.info('Found %d keys to populate' % len(keys))
        for key in keys:
            if not reserve_jobs or jobs.reserve(self.target.table_name, key):
                self.connection.start_transaction()
                if key in self.target:  # already populated
                    self.connection.cancel_transaction()
                    if reserve_jobs:
                        jobs.complete(self.target.table_name, key)
                else:
                    logger.info('Populating: ' + str(key))
                    try:
                        self._make_tuples(dict(key))
                    except (KeyboardInterrupt, SystemExit, Exception) as error:
                        try:
                            self.connection.cancel_transaction()
                        except OperationalError:
                            pass
                        if reserve_jobs:
                            # show error name and error message (if any)
                            error_message = ': '.join([error.__class__.__name__, str(error)]).strip(': ')
                            jobs.error(self.target.table_name, key, error_message=error_message)

                        if not suppress_errors or isinstance(error, SystemExit):
                            raise
                        else:
                            logger.error(error)
                            error_list.append((key, error))
                    else:
                        self.connection.commit_transaction()
                        if reserve_jobs:
                            jobs.complete(self.target.table_name, key)

        # place back the original signal handler
        if reserve_jobs:
            signal.signal(signal.SIGTERM, old_handler)

        return error_list

    def progress(self, *restrictions, display=True):
        """
        report progress of populating this table
        :return: remaining, total -- tuples to be populated
        """
        todo = self.key_source & AndList(restrictions)
        total = len(todo)
        remaining = len(todo.proj() - self.target)
        if display:
            print('%-20s' % self.__class__.__name__,
                  'Completed %d of %d (%2.1f%%)   %s' % (
                      total - remaining, total, 100 - 100 * remaining / (total+1e-12),
                      datetime.datetime.strftime(datetime.datetime.now(), '%Y-%m-%d %H:%M:%S')), flush=True)
        return remaining, total
