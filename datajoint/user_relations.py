from .relation_class import RelationClass
from .autopopulate import AutoPopulate
from .utils import from_camel_case


class ManualRelation(RelationClass):
    @property
    def table_name(self):
        return from_camel_case(self.__class__.__name__)


class LookupRelation(RelationClass):
    @property
    def table_name(self):
        return '#' + from_camel_case(self.__class__.__name__)


class ImportedRelation(RelationClass, AutoPopulate):
    @property
    def table_name(self):
        return "_" + from_camel_case(self.__class__.__name__)


class ComputedRelation(RelationClass, AutoPopulate):
    @property
    def table_name(self):
        return "__" + from_camel_case(self.__class__.__name__)