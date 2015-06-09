import re
import abc
from datajoint.relation import Relation
from .autopopulate import AutoPopulate
from . import DataJointError


class Manual(Relation):
    @property
    def table_name(self):
        return from_camel_case(self.__class__.__name__)


class Lookup(Relation):
    @property
    def table_name(self):
        return '#' + from_camel_case(self.__class__.__name__)


class Imported(Relation, AutoPopulate):
    @property
    def table_name(self):
        return "_" + from_camel_case(self.__class__.__name__)


class Computed(Relation, AutoPopulate):
    @property
    def table_name(self):
        return "__" + from_camel_case(self.__class__.__name__)


class Subordinate:
    """
    Mix-in to make computed tables subordinate
    """
    @property
    def populate_relation(self):
        return None

    def _make_tuples(self, key):
        raise NotImplementedError('Subtables should not be populated directly.')


# ---------------- utilities --------------------
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

