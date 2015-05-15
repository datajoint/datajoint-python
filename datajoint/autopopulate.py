from .relational_operand import RelationalOperand
from . import DataJointError
import abc
import logging

#noinspection PyExceptionInherit,PyCallingNonCallable

logger = logging.getLogger(__name__)


class AutoPopulate(metaclass=abc.ABCMeta):
    """
    AutoPopulate is a mixin class that adds the method populate() to a Relation class.
    Auto-populated relations must inherit from both Relation and AutoPopulate,
    must define the property pop_rel, and must define the callback method make_tuples.
    """

    @abc.abstractproperty
    def pop_rel(self):
        """
        Derived classes must implement the read-only property pop_rel (populate relation) which is the relational
        expression (a Relation object) that defines how keys are generated for the populate call.
        """
        pass

    @abc.abstractmethod
    def _make_tuples(self, key):
        """
        Derived classes must implement method make_tuples that fetches data from parent tables, restricting by
        the given key, computes dependent attributes, and inserts the new tuples into self.
        """
        pass

    @property
    def target(self):
        return self

    def populate(self, restriction=None, suppress_errors=False, reserve_jobs=False):
        """
        rel.populate() calls rel._make_tuples(key) for every primary key in self.pop_rel
        for which there is not already a tuple in rel.
        """
        assert not reserve_jobs, NotImplemented   # issue #5
        error_list = [] if suppress_errors else None
        if not isinstance(self.pop_rel, RelationalOperand):
            raise DataJointError('Invalid pop_rel value')
        self.conn._cancel_transaction()  # rollback previous transaction, if any
        unpopulated = (self.pop_rel - self.target) & restriction
        for key in unpopulated.project():
            self.conn._start_transaction()
            if key in self.target:  # already populated
                self.conn._cancel_transaction()
            else:
                logger.info('Populating: ' + str(key))
                try:
                    self._make_tuples(key)
                except Exception as error:
                    self.conn._cancel_transaction()
                    if not suppress_errors:
                        raise
                    else:
                        print(error)
                        error_list.append((key, error))
                else:
                    self.conn._commit_transaction()
        logger.info('Done populating.', flush=True)
        return error_list