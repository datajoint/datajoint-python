from .relational import _Relational
import pprint
import abc

#noinspection PyExceptionInherit,PyCallingNonCallable


class AutoPopulate(metaclass=abc.ABCMeta):
    """
    Class datajoint.AutoPopulate is a mixin that adds the method populate() to a dj.Relvar class.
    Auto-populated relvars must inherit from both datajoint.Relvar and datajoint.AutoPopulate,
    must define the property pop_rel, and must define the callback method make_tuples.
    """

    @abc.abstractproperty
    def pop_rel(self):
        """
        Derived classes must implement the read-only property pop_rel (populate relation) which is the relational
        expression (a dj.Relvar object) that defines how keys are generated for the populate call.
        """
        pass

    @abc.abstractmethod
    def make_tuples(self, key):
        """
        Derived classes must implement methods make_tuples that fetches data from parent tables, restricting by
        the given key, computes dependent attributes, and inserts the new tuples into self.
        """
        pass

    def populate(self, catch_errors=False, reserve_jobs=False, restrict=None):
        """
        rel.populate() will call rel.make_tuples(key) for every primary key in self.pop_rel
        for which there is not already a tuple in rel.
        """
        self.conn.cancel_transaction()

        # enumerate unpopulated keys
        unpopulated = self.pop_rel
        if ~isinstance(unpopulated, _Relational):
            unpopulated = unpopulated()   # instantiate
            
        if not unpopulated.count:
            print('Nothing to populate')
        else:
            unpopulated = unpopulated(*args, **kwargs) # - self   # TODO: implement antijoin

            # execute
            if catch_errors:
                errKeys, errors = [], []
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
                        errKeys+= [key]
                    else:
                        self.conn.commit_transaction()
        if catch_errors:
            return errors, errKeys
