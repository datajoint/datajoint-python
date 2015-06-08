from datajoint.relation import Relation, classproperty
from .autopopulate import AutoPopulate
from .utils import from_camel_case


class Manual(Relation):
    @classproperty
    def table_name(cls):
        return from_camel_case(cls.__name__)


class Lookup(Relation):
    @classproperty
    def table_name(cls):
        return '#' + from_camel_case(cls.__name__)


class Imported(Relation, AutoPopulate):
    @classproperty
    def table_name(cls):
        return "_" + from_camel_case(cls.__name__)


class Computed(Relation, AutoPopulate):
    @classproperty
    def table_name(cls):
        return "__" + from_camel_case(cls.__name__)