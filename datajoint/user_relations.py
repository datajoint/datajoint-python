from .relation_class import RelationClass
from .autopopulate import AutoPopulate
from .utils import from_camel_case


class ManualRelation(RelationClass):
    @property
    @classmethod
    def table_name(cls):
        return from_camel_case(cls.__name__)


class LookupRelation(RelationClass):
    @property
    @classmethod
    def table_name(cls):
        return '#' + from_camel_case(cls.__name__)


class ImportedRelation(RelationClass, AutoPopulate):
    @property
    @classmethod
    def table_name(cls):
        return "_" + from_camel_case(cls.__name__)


class ComputedRelation(RelationClass, AutoPopulate):
    @property
    @classmethod
    def table_name(cls):
        return "__" + from_camel_case(cls.__name__)