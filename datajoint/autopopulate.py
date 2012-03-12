from core import DataJointError
import pprint

#noinspection PyExceptionInherit,PyCallingNonCallable
class AutoPopulate:
    """
    Class datajoint.AutoPopulate adds the method populate() to a relvar.
    Auto-populated relvars must inherit from datajoint.Relvar and datajoint.AutoPopulate,
    must define the member popRel, and must define the callback method makeTuples.
    """

    def populate(self, catchErrors=False, *args, **kwargs):
        """
        rel.populate() will call rel.makeTuples(key) for every primary key in self.popRel
        for which there is not already a tuple in rel.
        """

        if self.isDerived or len(self._sql.pro):
            raise DataJointError('Cannot populate a derived relation')

        try:
            callback = self.makeTuples
        except AttributeError:
            raise DataJointError('An auto-populated relation must define method makeTuples()')

        # rollback previous transaction
        try:
            self._conn.cancelTransaction()
        except AttributeError:
            raise DataJointError('An auto-populated relation is not a valid relvar')

        # enumerate unpopulated keys
        try:
            unpopulated = self.popRel
        except AttributeError:
            raise DataJointError('An auto-populated relation must define property popRel')

        if not unpopulated.count:
            print 'Nothing to populate'
        else:
            unpopulated = unpopulated(*args, **kwargs) - self

            # execute 
            errKeys, errors = [], []
            for key in unpopulated.fetch():
                self._conn.startTransaction()
                if self(key):
                    self._conn.cancelTransaction()
                else:
                    pprint.pprint('Populating', key)

                    try:
                        callback(key)
                    except Exception as e:
                        self._conn.cancelTransaction()
                        if not catchErrors:
                            raise
                        errors += [e]
                        errKeys+= [key]
                    else:
                        self._conn.commitTransaction()
