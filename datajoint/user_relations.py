"""
Hosts the table tiers, user relations should be derived from.
"""

from datajoint.relation import Relation
from .autopopulate import AutoPopulate
from .utils import from_camel_case
from . import DataJointError


class Sub(Relation):

    @property
    def master(self):
        if not hasattr(self, '_master'):
            raise DataJointError(
                'subordinate relations must be declared inside a base relation class')
        return self._master

    @property
    def table_name(self):
        return self.master.table_name + '__' + from_camel_case(self.__class__.__name__)


class MasterMeta(type):
    """
    The metaclass for master relations.  Assigns the class into the _master property of all
    properties of type Sub.
    """
    def __new__(cls, name, parents, dct):
        for value in dct.values():
            if issubclass(value, Sub):
                value._master = cls
        return super().__new__(cls, name, parents, dct)



class Manual(Relation, metaclass=MasterMeta):
    """
    Inherit from this class if the table's values are entered manually.
    """

    @property
    def table_name(self):
        """
        :returns: the table name of the table formatted for mysql.
        """
        return from_camel_case(self.__class__.__name__)


class Lookup(ClassedRelation):
    """
    Inherit from this class if the table's values are for lookup. This is
    currently equivalent to defining the table as Manual and serves semantic
    purposes only.
    """

    @property
    def table_name(self):
        """
        :returns: the table name of the table formatted for mysql.
        """
        return '#' + from_camel_case(self.__class__.__name__)

    def _prepare(self):
        """
        Checks whether the instance has a property called `contents` and inserts its elements.
        """
        if hasattr(self, 'contents'):
            self.insert(self.contents, ignore_errors=True)


class Imported(ClassedRelation, AutoPopulate):
    """
    Inherit from this class if the table's values are imported from external data sources.
    The inherited class must at least provide the function `_make_tuples`.
    """

    @property
    def table_name(self):
        """
        :returns: the table name of the table formatted for mysql.
        """
        return "_" + from_camel_case(self.__class__.__name__)


class Computed(ClassedRelation, AutoPopulate):
    """
    Inherit from this class if the table's values are computed from other relations in the schema.
    The inherited class must at least provide the function `_make_tuples`.
    """

    @property
    def table_name(self):
        """
        :returns: the table name of the table formatted for mysql.
        """
        return "__" + from_camel_case(self.__class__.__name__)


class Subordinate(dj.Relation):
    """
    Subordinate relation is declared within another relation class

    :input master: instance of the master relation containing the suborinate relation
    """

    def __init__(self, master):
        self._master = master

    def table_name(self):
        return self._master.table_name + '__' + from_camel_case(self.__class__.__name__)






