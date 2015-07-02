"""
Hosts the table tiers, user relations should be derived from.
"""

import re
import abc
from datajoint.relation import Relation
from .autopopulate import AutoPopulate
from datajoint.utils import from_camel_case


class Manual(Relation):
    """
    Inherit from this class if the table's values are entered manually.
    """

    @property
    def table_name(self):
        """
        :returns: the table name of the table formatted for mysql.
        """
        return from_camel_case(self.__class__.__name__)


class Lookup(Relation):
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

    def prepare(self):
        """
        Checks whether the instance has a property called `contents` and inserts its elements.
        """
        if hasattr(self, 'contents'):
            self.insert(self.contents, ignore_errors=True)


class Imported(Relation, AutoPopulate):
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


class Computed(Relation, AutoPopulate):
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


class Subordinate:
    """
    Mix-in to make computed tables subordinate
    """

    @property
    def populated_from(self):
        """
        Overrides the `populate_from` property because subtables should not be populated
        directly.

        :return: None
        """
        return None

    def _make_tuples(self, key):
        """
        Overrides the `_make_tuples` property because subtables should not be populated
        directly. Raises an error if this method is called (usually from populate of the
        inheriting object).

        :raises: NotImplementedError
        """
        raise NotImplementedError('Subtables should not be populated directly.')


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
