"""autopopulate containing the dj.AutoPopulate class. See `dj.AutoPopulate` for more info."""
import abc
import logging
import datetime
import random
from .relational_operand import RelationalOperand
from . import DataJointError
from .relation import FreeRelation

# noinspection PyExceptionInherit,PyCallingNonCallable

logger = logging.getLogger(__name__)


class AutoPopulate(metaclass=abc.ABCMeta):
    """
    AutoPopulate is a mixin class that adds the method populate() to a Relation class.
    Auto-populated relations must inherit from both Relation and AutoPopulate,
    must define the property populated_from, and must define the callback method _make_tuples.
    """
    _jobs = None

    @property
    def populated_from(self):
        """
        :return: the relation whose primary key values are passed, sequentially, to the
                `_make_tuples` method when populate() is called.The default value is the
                join of the parent relations. Users may override to change the granularity
                or the scope of populate() calls.
        """
        parents = [FreeRelation(self.target.connection, rel) for rel in self.target.parents]
        if not parents:
            raise DataJointError('A relation must have parent relations to be able to be populated')
        ret = parents.pop(0)
        while parents:
            ret *= parents.pop(0)
        return ret

    @abc.abstractmethod
    def _make_tuples(self, key):
        """
        Derived classes must implement method _make_tuples that fetches data from tables that are
        above them in the dependency hierarchy, restricting by the given key, computes dependent
        attributes, and inserts the new tuples into self.
        """
        pass

    @property
    def target(self):
        """
        relation to be populated.
        Typically, AutoPopulate are mixed into a Relation object and the target is self.
        """
        return self

    def populate(self, restriction=None, suppress_errors=False,
                 reserve_jobs=False, order="original"):
        """
        rel.populate() calls rel._make_tuples(key) for every primary key in self.populated_from
        for which there is not already a tuple in rel.

        :param restriction: restriction on rel.populated_from - target
        :param suppress_errors: suppresses error if true
        :param reserve_jobs: currently not implemented
        :param batch: batch size of a single job
        :param order: "original"|"reverse"|"random"  - the order of execution
        """
        if not isinstance(self.populated_from, RelationalOperand):
            raise DataJointError('Invalid populated_from value')

        if self.connection.in_transaction:
            raise DataJointError('Populate cannot be called during a transaction.')

        valid_order = ['original', 'reverse', 'random']
        if order not in valid_order:
            raise DataJointError('The order argument must be one of %s' % str(valid_order))

        error_list = [] if suppress_errors else None

        jobs = self.connection.jobs[self.target.database]
        table_name = self.target.table_name
        unpopulated = (self.populated_from & restriction) - self.target.project()
        keys = unpopulated.fetch.keys()
        if order == "reverse":
            keys = list(keys).reverse()
        elif order == "random":
            keys = list(keys)
            random.shuffle(keys)

        for key in keys:
            if not reserve_jobs or jobs.reserve(table_name, key):
                self.connection.start_transaction()
                if key in self.target:  # already populated
                    self.connection.cancel_transaction()
                    if reserve_jobs:
                        jobs.complete(table_name, key)
                else:
                    logger.info('Populating: ' + str(key))
                    try:
                        self._make_tuples(dict(key))
                    except Exception as error:
                        self.connection.cancel_transaction()
                        if reserve_jobs:
                            jobs.error(table_name, key, error_message=str(error))
                        if not suppress_errors:
                            raise
                        else:
                            logger.error(error)
                            error_list.append((key, error))
                    else:
                        self.connection.commit_transaction()
                        if reserve_jobs:
                            jobs.complete(table_name, key)
        return error_list

    def progress(self, restriction=None, display=True):
        """
        report progress of populating this table
        :return: remaining, total -- tuples to be populated
        """
        total = len(self.populated_from & restriction)
        remaining = len((self.populated_from & restriction) - self.target.project())
        if display:
            print('%-20s' % self.__class__.__name__, flush=True, end=': ')
            print('Completed %d of %d (%2.1f%%)   %s' %
                  (total - remaining, total, 100 - 100 * remaining / (total+1e-12),
                   datetime.datetime.strftime(datetime.datetime.now(), '%Y-%m-%d %H:%M:%S')
                   ), flush=True)
        return remaining, total
