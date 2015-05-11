import importlib
import abc
from types import ModuleType
from . import DataJointError
from .free_relation import FreeRelation
import logging


logger = logging.getLogger(__name__)


class Relation(FreeRelation, metaclass=abc.ABCMeta):
    """
    Relation is a Table that implements data definition functions.
    It is an abstract class with the abstract property 'definition'.

    Example for a usage of Relation::

        import datajoint as dj


        class Subjects(dj.Relation):
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

    @property
    def full_class_name(self):
        """
        :return: full class name including the entire package hierarchy
        """
        return '{}.{}'.format(self.__module__, self.class_name)

    @property
    def ref_name(self):
        """
        :return: name by which this class should be accessible as
        """
        parent = self.__module__.split('.')[-2 if self._use_package else -1]
        return parent + '.' + self.class_name


    def __init__(self): #TODO: support taking in conn obj
        class_name = self.__class__.__name__
        module_name = self.__module__
        mod_obj = importlib.import_module(module_name)
        self._use_package = False
        # first, find the conn object
        try:
            conn = mod_obj.conn
        except AttributeError:
            try:
                # check if database bound at the package level instead
                pkg_obj = importlib.import_module(mod_obj.__package__)
                conn = pkg_obj.conn
                self._use_package = True
            except AttributeError:
                raise DataJointError(
                    "Please define object 'conn' in '{}' or in its containing package.".format(module_name))
        # now use the conn object to determine the dbname this belongs to
        try:
            if self._use_package:
                # the database is bound to the package
                pkg_name = '.'.join(module_name.split('.')[:-1])
                dbname = conn.mod_to_db[pkg_name]
            else:
                dbname = conn.mod_to_db[module_name]
        except KeyError:
            raise DataJointError(
                'Module {} is not bound to a database. See datajoint.connection.bind'.format(module_name))
        # initialize using super class's constructor
        super().__init__(conn, dbname, class_name)

    def get_base(self, module_name, class_name):
        """
        Loads the base relation from the module.  If the base relation is not defined in
        the module, then construct it using Relation constructor.

        :param module_name: module name
        :param class_name: class name
        :returns: the base relation
        """
        mod_obj = self.get_module(module_name)
        if not mod_obj:
            raise DataJointError('Module named {mod_name} was not found. Please make'
                                 ' sure that it is in the path or you import the module.'.format(mod_name=module_name))
        try:
            ret = getattr(mod_obj, class_name)()
        except AttributeError:
            ret = FreeRelation(conn=self.conn,
                        dbname=self.conn.mod_to_db[mod_obj.__name__],
                        class_name=class_name)
        return ret

    @classmethod
    def get_module(cls, module_name):
        """
        Resolve short name reference to a module and return the corresponding module object

        :param module_name: short name for the module, whose reference is to be resolved
        :return: resolved module object. If no module matches the short name, `None` will be returned

        The module_name resolution steps in the following order:

        1. Global reference to a module of the same name defined in the module that contains this Relation derivative.
           This is the recommended use case.
        2. Module of the same name defined in the package containing this Relation derivative. This will only look for the
           most immediate containing package (e.g. if this class is contained in package.subpackage.module, it will
           check within `package.subpackage` but not inside `package`).
        3. Globally accessible module with the same name.
        """
        # from IPython import embed
        # embed()
        mod_obj = importlib.import_module(cls.__module__)
        if cls.__module__.split('.')[-1] == module_name:
            return mod_obj
        attr = getattr(mod_obj, module_name, None)
        if isinstance(attr, ModuleType):
            return attr
        if mod_obj.__package__:
            try:
                return importlib.import_module('.' + module_name, mod_obj.__package__)
            except ImportError:
                pass
        try:
            return importlib.import_module(module_name)
        except ImportError:
            return None
