import numpy as np
from collections import namedtuple, OrderedDict
import re
import logging
from . import DataJointError

logger = logging.getLogger(__name__)

default_attribute_properties = dict(    # these default values are set in computed attributes
    name=None, type='expression', in_key=False, nullable=False, default=None, comment='calculated attribute',
    autoincrement=False, numeric=None, string=None, is_blob=False, sql_expression=None, dtype=object)


class Attribute(namedtuple('_Attribute', default_attribute_properties.keys())):
    """
    Properties of a table column (attribute)
    """
    def todict(self):
        """Convert namedtuple to dict."""
        return OrderedDict((name, self[i]) for i, name in enumerate(self._fields))

    @property
    def sql(self):
        """
        Convert primary key attribute tuple into its SQL CREATE TABLE clause.
        Default values are not reflected.
        :return: SQL code
        """
        assert self.in_key and not self.nullable   # primary key attributes are never nullable
        return '`{name}` {type} NOT NULL COMMENT "{comment}"'.format(
            name=self.name, type=self.type, comment=self.comment)


class Heading:
    """
    Local class for relations' headings.
    Heading contains the property attributes, which is an OrderedDict in which the keys are
    the attribute names and the values are Attributes.
    """

    def __init__(self, arg=None):
        """
        :param arg: a list of dicts with the same keys as Attribute
        """
        assert not isinstance(arg, Heading), 'Headings cannot be copied'
        self.table_info = None
        self.attributes = None if arg is None else OrderedDict(
            (q['name'], Attribute(**q)) for q in arg)

    def __len__(self):
        return 0 if self.attributes is None else len(self.attributes)

    def __bool__(self):
        return self.attributes is not None

    @property
    def names(self):
        return [k for k in self.attributes]

    @property
    def primary_key(self):
        return [k for k, v in self.attributes.items() if v.in_key]

    @property
    def dependent_attributes(self):
        return [k for k, v in self.attributes.items() if not v.in_key]

    @property
    def blobs(self):
        return [k for k, v in self.attributes.items() if v.is_blob]

    @property
    def non_blobs(self):
        return [k for k, v in self.attributes.items() if not v.is_blob]

    @property
    def expressions(self):
        return [k for k, v in self.attributes.items() if v.sql_expression is not None]

    def __getitem__(self, name):
        """shortcut to the attribute"""
        return self.attributes[name]

    def __repr__(self):
        if self.attributes is None:
            return 'heading not loaded'
        in_key = True
        ret = ''
        if self.table_info:
            ret += '# ' + self.table_info['comment'] + '\n'
        for v in self.attributes.values():
            if in_key and not v.in_key:
                ret += '---\n'
                in_key = False
            ret += '%-20s : %-28s # %s\n' % (
                v.name if v.default is None else '%s=%s' % (v.name, v.default),
                '%s%s' % (v.type, 'auto_increment' if v.autoincrement else ''), v.comment)
        return ret

    @property
    def has_autoincrement(self):
        return any(e.autoincrement for e in self.attributes.values())

    @property
    def as_dtype(self):
        """
        represent the heading as a numpy dtype
        """
        return np.dtype(dict(
            names=self.names,
            formats=[v.dtype for v in self.attributes.values()]))

    @property
    def as_sql(self):
        """
        represent heading as SQL field list
        """
        return ','.join(['`%s`' % name
                         if self.attributes[name].sql_expression is None
                         else '%s as `%s`' % (self.attributes[name].sql_expression, name)
                         for name in self.names])

    def __iter__(self):
        return iter(self.attributes)

    def init_from_database(self, conn, database, table_name):
        """
        initialize heading from a database table.  The table must exist already.
        """
        info = conn.query('SHOW TABLE STATUS FROM `{database}` WHERE name="{table_name}"'.format(
            table_name=table_name, database=database), as_dict=True).fetchone()
        if info is None:
            if table_name == '~log':
                logger.warning('Could not create the ~log table')
                return
            else:
                raise DataJointError('The table `{database}`.`{table_name}` is not defined.'.format(
                    table_name=table_name, database=database))
        self.table_info = {k.lower(): v for k, v in info.items()}

        cur = conn.query(
            'SHOW FULL COLUMNS FROM `{table_name}` IN `{database}`'.format(
                table_name=table_name, database=database), as_dict=True)

        attributes = cur.fetchall()

        rename_map = {
            'Field': 'name',
            'Type': 'type',
            'Null': 'nullable',
            'Default': 'default',
            'Key': 'in_key',
            'Comment': 'comment'}

        fields_to_drop = ('Privileges', 'Collation')

        # rename and drop attributes
        attributes = [{rename_map[k] if k in rename_map else k: v
                       for k, v in x.items() if k not in fields_to_drop}
                      for x in attributes]

        numeric_types = {
            ('float', False): np.float64,
            ('float', True): np.float64,
            ('double', False): np.float64,
            ('double', True): np.float64,
            ('tinyint', False): np.int64,
            ('tinyint', True): np.int64,
            ('smallint', False): np.int64,
            ('smallint', True): np.int64,
            ('mediumint', False): np.int64,
            ('mediumint', True): np.int64,
            ('int', False): np.int64,
            ('int', True): np.int64,
            ('bigint', False): np.int64,
            ('bigint', True): np.uint64
            }

        sql_literals = ['CURRENT_TIMESTAMP']

        # additional attribute properties
        for attr in attributes:
            attr['nullable'] = (attr['nullable'] == 'YES')
            attr['in_key'] = (attr['in_key'] == 'PRI')
            attr['autoincrement'] = bool(re.search(r'auto_increment', attr['Extra'], flags=re.IGNORECASE))
            attr['type'] = re.sub(r'int\(\d+\)', 'int', attr['type'], count=1)   # strip size off integers
            attr['numeric'] = bool(re.match(r'(tiny|small|medium|big)?int|decimal|double|float', attr['type']))
            attr['string'] = bool(re.match(r'(var)?char|enum|date|year|time|timestamp', attr['type']))
            attr['is_blob'] = bool(re.match(r'(tiny|medium|long)?blob', attr['type']))

            if attr['string'] and attr['default'] is not None and attr['default'] not in sql_literals:
                attr['default'] = '"%s"' % attr['default']

            if attr['nullable']:   # nullable fields always default to null
                attr['default'] = 'null'

            attr['sql_expression'] = None
            if not (attr['numeric'] or attr['string'] or attr['is_blob']):
                raise DataJointError('Unsupported field type {field} in `{database}`.`{table_name}`'.format(
                    field=attr['type'], database=database, table_name=table_name))
            attr.pop('Extra')

            # fill out dtype. All floats and non-nullable integers are turned into specific dtypes
            attr['dtype'] = object
            if attr['numeric']:
                is_integer = bool(re.match(r'(tiny|small|medium|big)?int', attr['type']))
                is_float = bool(re.match(r'(double|float)', attr['type']))
                if is_integer and not attr['nullable'] or is_float:
                    is_unsigned = bool(re.match('\sunsigned', attr['type'], flags=re.IGNORECASE))
                    t = attr['type']
                    t = re.sub(r'\(.*\)', '', t)    # remove parentheses
                    t = re.sub(r' unsigned$', '', t)   # remove unsigned
                    assert (t, is_unsigned) in numeric_types, 'dtype not found for type %s' % t
                    attr['dtype'] = numeric_types[(t, is_unsigned)]
        self.attributes = OrderedDict([(q['name'], Attribute(**q)) for q in attributes])

    def project(self, attribute_list, named_attributes=None, force_primary_key=None):
        """
        derive a new heading by selecting, renaming, or computing attributes.
        In relational algebra these operators are known as project, rename, and extend.
        :param attribute_list:  the full list of existing attributes to include
        :param force_primary_key:  attributes to force to be converted to primary
        :param named_attributes:  dictionary of renamed attributes
        """
        try:  # check for missing attributes
            raise DataJointError('Attribute `%s` is not found' % next(a for a in attribute_list if a not in self.names))
        except StopIteration:
            if named_attributes is None:
                named_attributes = {}
            if force_primary_key is None:
                force_primary_key = set()
            return Heading(
                [dict(   # copied attributes
                    self.attributes[k].todict(),
                    in_key=self.attributes[k].in_key or k in force_primary_key)
                 for k in attribute_list] +
                [dict(  # renamed attributes
                    self.attributes[sql_expression].todict(),
                    name=new_name,
                    sql_expression='`%s`' % sql_expression,
                    in_key=self.attributes[sql_expression].in_key or sql_expression in force_primary_key)
                 if sql_expression in self.names else
                 dict(  # computed attributes
                     default_attribute_properties,
                     name=new_name,
                     sql_expression=sql_expression)
                 for new_name, sql_expression in named_attributes.items()])

    def join(self, other):
        """
        Join two headings into a new one.
        It assumes that self and other are headings that share no common dependent attributes.
        """
        return Heading(
            [self.attributes[name].todict() for name in self.primary_key] +
            [other.attributes[name].todict() for name in other.primary_key if name not in self.primary_key] +
            [self.attributes[name].todict() for name in self.dependent_attributes if name not in other.primary_key] +
            [other.attributes[name].todict() for name in other.dependent_attributes if name not in self.primary_key])

    def make_subquery_heading(self):
        """
        Create a new heading with removed attribute sql_expressions.
        Used by subqueries, which resolve the sql_expressions.
        """
        return Heading(dict(v.todict(), sql_expression=None) for v in self.attributes.values())

    def extend_primary_key(self, new_attributes):
        """
        Create a new heading in which the primary key also includes new_attributes.
        :param new_attributes: new attributes to be added to the primary key.
        """
        try:  # check for missing attributes
            raise DataJointError('Attribute `%s` is not found' % next(a for a in new_attributes if a not in self.names))
        except StopIteration:
            return Heading(dict(v.todict(), in_key=v.in_key or v.name in new_attributes)
                           for v in self.attributes.values())
