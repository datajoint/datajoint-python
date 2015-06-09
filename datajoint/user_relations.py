import re
from datajoint.relation import Relation
from .autopopulate import AutoPopulate
from . import DataJointError

class Manual(Relation):
    def table_name(cls):
        return from_camel_case(cls.__name__)


class Lookup(Relation):
    def table_name(cls):
        return '#' + from_camel_case(cls.__name__)


class Imported(Relation, AutoPopulate):
    def table_name(cls):
        return "_" + from_camel_case(cls.__name__)


class Computed(Relation, AutoPopulate):
    def table_name(cls):
        return "__" + from_camel_case(cls.__name__)


def from_camel_case(s):
    """
    Convert names in camel case into underscore (_) separated names

    Example:
    >>>from_camel_case("TableName")
        "table_name"
    """
    def convert(match):
        return ('_' if match.groups()[0] else '') + match.group(0).lower()

    if not re.match(r'[A-Z][a-zA-Z0-9]*', s):
        raise DataJointError(
            'ClassName must be alphanumeric in CamelCase, begin with a capital letter')
    return re.sub(r'(\B[A-Z])|(\b[A-Z])', convert, s)

