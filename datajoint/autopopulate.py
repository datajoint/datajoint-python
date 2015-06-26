import abc
import logging
from .relational_operand import RelationalOperand
from . import DataJointError
from .relation import Relation, FreeRelation, schema
from . import jobs

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
        _make_tuples method when populate() is called.
        The default value is the join of the parent relations. Users may override to change
        the granularity or the scope of populate() calls.
        """
        parents = [FreeRelation(self.target.connection, rel) for rel in self.target.parents]
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
        return self

    def populate(self, restriction=None, suppress_errors=False, reserve_jobs=False):
        """
        rel.populate() calls rel._make_tuples(key) for every primary key in self.populated_from
        for which there is not already a tuple in rel.

        :param restriction: restriction on rel.populated_from - target
        :param suppress_errors: suppresses error if true
        :param reserve_jobs: currently not implemented
        """
        error_list = [] if suppress_errors else None
        if not isinstance(self.populated_from, RelationalOperand):
            raise DataJointError('Invalid populated_from value')

        if self.connection.in_transaction:
            raise DataJointError('Populate cannot be called during a transaction.')

        unpopulated = (self.populated_from - self.target) & restriction
        for key in unpopulated.project():
            if not reserve_jobs or jobs.reserve(self.target.full_table_name, key):
                self.connection.start_transaction()
                if key in self.target:  # already populated
                    self.connection.cancel_transaction()
                    if reserve_jobs:
                        jobs.complete(self.target.full_table_name, key)
                else:
                    logger.info('Populating: ' + str(key))
                    try:
                        self._make_tuples(dict(key))
                    except Exception as error:
                        self.connection.cancel_transaction()
                        if reserve_jobs:
                            jobs.log_error(self.target.full_table_name, key,
                                           error_message=str(error))
                        if not suppress_errors:
                            raise
                        else:
                            logger.error(error)
                            error_list.append((key, error))
                    else:
                        self.connection.commit_transaction()
                        if reserve_jobs:
                            jobs.complete(self.target.full_table_name, key)
        return error_list

    def progress(self):
        """
        report progress of populating this table
        """
        total = len(self.populated_from)
        remaining = len(self.populated_from - self.target)
        print('Remaining %d of %d (%2.1f%%)' % (remaining, total, 100*remaining/total)
              if remaining else 'Complete', flush=True)
