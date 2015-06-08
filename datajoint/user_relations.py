from datajoint.relation import ClassBoundRelation
from .autopopulate import AutoPopulate
from .utils import from_camel_case


class Manual(ClassBoundRelation):
    @property
    @classmethod
    def table_name(cls):
        return from_camel_case(cls.__name__)


class Lookup(ClassBoundRelation):
    @property
    @classmethod
    def table_name(cls):
        return '#' + from_camel_case(cls.__name__)


class Imported(ClassBoundRelation, AutoPopulate):
    @property
    @classmethod
    def table_name(cls):
        return "_" + from_camel_case(cls.__name__)


class Computed(ClassBoundRelation, AutoPopulate):
    @property
    @classmethod
    def table_name(cls):
        return "__" + from_camel_case(cls.__name__)