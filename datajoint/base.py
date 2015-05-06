import importlib
import abc
from types import ModuleType
from . import DataJointError
from .table import Table
import logging
from .declare import declare

logger = logging.getLogger(__name__)


class Base(Table, metaclass=abc.ABCMeta):
    """
    Base is a Table that implements data definition functions.
    It is an abstract class with the abstract property 'definition'.

    Example for a usage of Base::

        import datajoint as dj


        class Subjects(dj.Base):
            definition = '''
            test1.Subjects (manual)                                    # Basic subject info
            subject_id            : int                                # unique subject id
            ---
            real_id               : varchar(40)                        #  real-world name
            species = "mouse"     : enum('mouse', 'monkey', 'human')   # species
            '''
    """

    @abc.abstractproperty
    def definition(self):
        """
        :return: string containing the table declaration using the DataJoint Data Definition Language.
        The DataJoint DDL is described at:  TODO
        """
        pass

    def __init__(self):
        self.class_name = self.__class__.__name__
        module = self.__module__
        mod_obj = importlib.import_module(module)
        use_package = False
        try:
            conn = mod_obj.conn
        except AttributeError:
            try:
                # check if database bound at the package level instead
                pkg_obj = importlib.import_module(mod_obj.__package__)
                conn = pkg_obj.conn
                use_package = True
            except AttributeError:
                raise DataJointError(
                    "Please define object 'conn' in '{}' or in its containing package.".format(self.__module__))
        self.conn = conn
        try:
            if use_package:
                pkg_name = '.'.join(module.split('.')[:-1])
                dbname = self.conn.mod_to_db[pkg_name]
            else:
                dbname = self.conn.mod_to_db[module]
        except KeyError:
            raise DataJointError(
                'Module {} is not bound to a database. See datajoint.connection.bind'.format(self.__module__))
        declare(self.conn, self.definition, self.full_class_name)
        super().__init__(conn=conn, dbname=dbname, class_name=self.__class__.__name__)

    @classmethod
    def get_module(cls, module_name):
        """
        Resolve short name reference to a module and return the corresponding module object

        :param module_name: short name for the module, whose reference is to be resolved
        :return: resolved module object. If no module matches the short name, `None` will be returned

        The module_name resolution steps in the following order:

        1. Global reference to a module of the same name defined in the module that contains this Base derivative.
           This is the recommended use case.
        2. Module of the same name defined in the package containing this Base derivative. This will only look for the
           most immediate containing package (e.g. if this class is contained in package.subpackage.module, it will
           check within `package.subpackage` but not inside `package`).
        3. Globally accessible module with the same name.
        """
        mod_obj = importlib.import_module(cls.__module__)
        attr = getattr(mod_obj, module_name, None)
        if isinstance(attr, ModuleType):
            return attr
        if mod_obj.__package__:
            try:
                return importlib.import_module('.' + module_name, mod_obj.__package__)
            except ImportError:
                try:
                    return importlib.import_module(module_name)
                except ImportError:
                    return None