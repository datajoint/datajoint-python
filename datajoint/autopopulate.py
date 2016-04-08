"""autopopulate containing the dj.AutoPopulate class. See `dj.AutoPopulate` for more info."""
import abc
import logging
import datetime
import random
from .relational_operand import RelationalOperand, AndList
from . import DataJointError
from .base_relation import FreeRelation

# noinspection PyExceptionInherit,PyCallingNonCallable

logger = logging.getLogger(__name__)


class AutoPopulate(metaclass=abc.ABCMeta):
    """
    AutoPopulate is a mixin class that adds the method populate() to a Relation class.
    Auto-populated relations must inherit from both Relation and AutoPopulate,
    must define the property populated_from, and must define the callback method _make_tuples.
    """
    _jobs = None
    _populated_from = None

    @property
    def populated_from(self):
        """
        :return: the relation whose primary key values are passed, sequentially, to the
                `_make_tuples` method when populate() is called.The default value is the
                join of the parent relations. Users may override to change the granularity
                or the scope of populate() calls.
        """
        if self._populated_from is None:
            self.connection.dependencies.load()
            parents = [FreeRelation(self.target.connection, rel) for rel in self.target.parents]
            if not parents:
                raise DataJointError('A relation must have parent relations to be able to be populated')
            ret = parents.pop(0)
            while parents:
                ret *= parents.pop(0)
            self._populated_from = ret
        return self._populated_from

    @abc.abstractmethod
    def _make_tuples(self, key):
        """
        Derived classes must implement method _make_tuples that fetches data from tables that are
        above them in the dependency hierarchy, restricting by the given key, computes dependent
        attributes, and inserts the new tuples into self.
        """

    @property
    def target(self):
        """
        relation to be populated.
        Typically, AutoPopulate are mixed into a Relation object and the target is self.
        """
        return self

    def populate(self, *restrictions, suppress_errors=False, reserve_jobs=False, order="original"):
        """
        rel.populate() calls rel._make_tuples(key) for every primary key in self.populated_from
        for which there is not already a tuple in rel.

        :param restrictions: a list of restrictions each restrict (rel.populated_from - target.proj())
        :param suppress_errors: suppresses error if true
        :param reserve_jobs: if true, reserves job to populate in asynchronous fashion
        :param order: "original"|"reverse"|"random"  - the order of execution
        """
        if self.connection.in_transaction:
            raise DataJointError('Populate cannot be called during a transaction.')

        valid_order = ['original', 'reverse', 'random']
        if order not in valid_order:
            raise DataJointError('The order argument must be one of %s' % str(valid_order))

        todo = self.populated_from
        if not isinstance(todo, RelationalOperand):
            raise DataJointError('Invalid populated_from value')
        todo.restrict(AndList(restrictions))

        error_list = [] if suppress_errors else None

        jobs = self.connection.jobs[self.target.database] if reserve_jobs else None
        todo -= self.target.proj()
        keys = todo.fetch.keys()
        if order == "reverse":
            keys = list(keys)
            keys.reverse()
        elif order == "random":
            keys = list(keys)
            random.shuffle(keys)
        elif order != "original":
            raise DataJointError('Invalid order specification')

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
                    except Exception as error:
                        self.connection.cancel_transaction()
                        if reserve_jobs:
                            jobs.error(self.target.table_name, key, error_message=str(error))
                        if not suppress_errors:
                            raise
                        else:
                            logger.error(error)
                            error_list.append((key, error))
                    else:
                        self.connection.commit_transaction()
                        if reserve_jobs:
                            jobs.complete(self.target.table_name, key)
        return error_list

    def progress(self, *restrictions, display=True):
        """
        report progress of populating this table
        :return: remaining, total -- tuples to be populated
        """
        todo = self.populated_from & AndList(restrictions)
        total = len(todo)
        remaining = len(todo - self.target.project())
        if display:
            print('%-20s' % self.__class__.__name__, flush=True, end=': ')
            print('Completed %d of %d (%2.1f%%)   %s' %
                  (total - remaining, total, 100 - 100 * remaining / (total+1e-12),
                   datetime.datetime.strftime(datetime.datetime.now(), '%Y-%m-%d %H:%M:%S')
                   ), flush=True)
        return remaining, total
