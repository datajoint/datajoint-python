"""
Hosts the table tiers, user relations should be derived from.
"""

import abc
from .base_relation import BaseRelation
from .autopopulate import AutoPopulate
from .utils import from_camel_case
from . import DataJointError
import re

_base_regexp = r'(?P<TIER>[a-z]+[a-z0-9]*(_[a-z]+[a-z0-9]*)*)'


class Manual(BaseRelation, metaclass=abc.ABCMeta):
    """
    Inherit from this class if the table's values are entered manually.
    """

    _prefix = r''
    _regexp = _prefix + _base_regexp.replace('TIER', 'manual')

    @property
    def table_name(self):
        """
        :returns: the table name of the table formatted for mysql.
        """
        return self._prefix + from_camel_case(self.__class__.__name__)


class Lookup(BaseRelation, metaclass=abc.ABCMeta):
    """
    Inherit from this class if the table's values are for lookup. This is
    currently equivalent to defining the table as Manual and serves semantic
    purposes only.
    """

    _prefix = '#'
    _regexp = _prefix + _base_regexp.replace('TIER', 'lookup')

    @property
    def table_name(self):
        """
        :returns: the table name of the table formatted for mysql.
        """
        return self._prefix + from_camel_case(self.__class__.__name__)

    def _prepare(self):
        """
        Checks whether the instance has a property called `contents` and inserts its elements.
        """
        if hasattr(self, 'contents'):
            self.insert(self.contents, skip_duplicates=True)


class Imported(BaseRelation, AutoPopulate, metaclass=abc.ABCMeta):
    """
    Inherit from this class if the table's values are imported from external data sources.
    The inherited class must at least provide the function `_make_tuples`.
    """

    _prefix = '_'
    _regexp = _prefix + _base_regexp.replace('TIER', 'imported')

    @property
    def table_name(self):
        """
        :returns: the table name of the table formatted for mysql.
        """
        return self._prefix + from_camel_case(self.__class__.__name__)


class Computed(BaseRelation, AutoPopulate, metaclass=abc.ABCMeta):
    """
    Inherit from this class if the table's values are computed from other relations in the schema.
    The inherited class must at least provide the function `_make_tuples`.
    """

    _prefix = '__'
    _regexp = _prefix + _base_regexp.replace('TIER', 'computed')

    @property
    def table_name(self):
        """
        :returns: the table name of the table formatted for mysql.
        """
        return self._prefix + from_camel_case(self.__class__.__name__)


class Part(BaseRelation, metaclass=abc.ABCMeta):
    """
    Inherit from this class if the table's values are details of an entry in another relation
    and if this table is populated by this relation. For example, the entries inheriting from
    dj.Part could be single entries of a matrix, while the parent table refers to the entire matrix.
    Part relations are implemented as classes inside classes.
    """

    _regexp = r'(' + '|'.join([c._regexp for c in [Manual, Imported, Computed, Lookup]]) + r'){1,1}' \
              + '__' + _base_regexp.replace('TIER', 'part')

    @property
    def master(self):
        if not hasattr(self, '_master'):
            raise DataJointError(
                'Part relations must be declared inside a base relation class')
        return self._master

    @property
    def table_name(self):
        return self.master().table_name + '__' + from_camel_case(self.__class__.__name__)
