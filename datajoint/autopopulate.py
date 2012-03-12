from core import DataJointError
import pprint

class AutoPopulate:
    """
    Class datajoint.AutoPopulate adds the method populate() to a relvar.
    Auto-populated relvars must inherit from datajoint.Relvar and datajoint.AutoPopulate,
    must define the member popRel, and must define the callback method makeTuples.
    """

    def populate(self, catchErrors=False, *restrictions):

        if self.isDerived or len(self._sql.pro):
            raise DataJointError('Cannot populate a derived relation')

        try:
            callback = self.makeTuples
        except AttributeError:
            raise DataJointError('An auto-populated relation must define makeTuples()')

        try:
            self._conn.cancelTransaction()
        except AttributeError:
            raise DataJointError('An auto-populated relation is not a valid datajoint relvar')

        try:
            unpopulated = self.popRel
        except AttributeError:
            raise DataJointError('An auto-populated relation must define property popRel')

        if not unpopulated.count:
            print 'Nothing to populate'
        else:
            unpopulated = unpopulated(*restrictions) - self
            errKeys, errors = [], []
            for key in unpopulated.fetch():
                self._conn.startTransaction()
                if self(key):
                    self._conn.cancelTransaction()
                else:
                    pprint.pprint("Populating", key)

                    try:
                        callback(key)
                    except Exception as e:
                        self._conn.cancelTransaction()
                        if not catchErrors:
                            raise e
                        errors += [e]
                        errKeys+= [key]
                    else:
                        self._conn.commitTransaction()
