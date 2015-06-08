from datajoint.relation import Relation
from .autopopulate import AutoPopulate
from .utils import from_camel_case


class Manual(Relation):
    @property
    @classmethod
    def table_name(cls):
        return from_camel_case(cls.__name__)


class Lookup(Relation):
    @property
    @classmethod
    def table_name(cls):
        return '#' + from_camel_case(cls.__name__)


class Imported(Relation, AutoPopulate):
    @property
    @classmethod
    def table_name(cls):
        return "_" + from_camel_case(cls.__name__)


class Computed(Relation, AutoPopulate):
    @property
    @classmethod
    def table_name(cls):
        return "__" + from_camel_case(cls.__name__)