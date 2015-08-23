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
        return self.master().table_name + '__' + from_camel_case(self.__class__.__name__)


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

    def _prepare(self):
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
    Mix-in to make computed tables subordinate.
    This class is DEPRECATED and will be removed in a future version. Use dj.Sub instead.
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
        raise NotImplementedError(
            'This table is subordinate: it cannot be populated directly. Refer to its parent table.')

    def progress(self):
        """
        Overrides the `progress` method because subtables should not be populated directly.
        """
        raise NotImplementedError(
            'This table is subordinate: it cannot be populated directly. Refer to its parent table.')

    def populate(self, *args, **kwargs):
        raise NotImplementedError(
            'This table is subordinate: it cannot be populated directly. Refer to its parent table.')