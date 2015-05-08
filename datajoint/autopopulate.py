from .relational import Relation
from . import DataJointError
import pprint
import abc
import logging

#noinspection PyExceptionInherit,PyCallingNonCallable

logger = logging.getLogger(__name__)

class AutoPopulate(metaclass=abc.ABCMeta):
    """
    AutoPopulate is a mixin class that adds the method populate() to a Base class.
    Auto-populated relations must inherit from both Base and AutoPopulate,
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
    def make_tuples(self, key):
        """
        Derived classes must implement method make_tuples that fetches data from parent tables, restricting by
        the given key, computes dependent attributes, and inserts the new tuples into self.
        """
        pass

    @property
    def target(self):
        return self

    def populate(self, catch_errors=False, reserve_jobs=False, restrict=None):
        """
        rel.populate() will call rel.make_tuples(key) for every primary key in self.pop_rel
        for which there is not already a tuple in rel.
        """
        if not isinstance(self.pop_rel, Relation):
            raise DataJointError('')
        self.conn.cancel_transaction()

        unpopulated = self.pop_rel - self.target
        if not unpopulated.count:
            logger.info('Nothing to populate', flush=True)
            if catch_errors:
                error_keys, errors = [], []
            for key in unpopulated.fetch():
                self.conn.start_transaction()
                n = self(key).count
                if n:  # already populated
                    self.conn.cancel_transaction()
                else:
                    print('Populating:')
                    pprint.pprint(key)
                    try:
                        self.make_tuples(key)
                    except Exception as e:
                        self.conn.cancel_transaction()
                        if not catch_errors:
                            raise
                        print(e)
                        errors += [e]
                        error_keys += [key]
                    else:
                        self.conn.commit_transaction()
        if catch_errors:
            return errors, error_keys