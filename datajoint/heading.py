import numpy as np
from . import DataJointError
from collections import namedtuple, OrderedDict
import re


class Attribute(namedtuple('Attribute',
                           ('name', 'type', 'in_key', 'nullable', 'default',
                            'comment', 'autoincrement', 'numeric', 'string', 'is_blob',
                            'computation', 'dtype'))):
    def _asdict(self):
        """
        for some reason the inherted _asdict does not work after subclassing from namedtuple
        """
        return OrderedDict((name, self[i]) for i, name in enumerate(self._fields))

    @property
    def sql(self):
        """
        Convert attribute tuple into its SQL CREATE TABLE clause.
        :rtype : SQL code
        """
        literals = ['CURRENT_TIMESTAMP']
        if self.nullable:
            default = 'DEFAULT NULL'
        else:
            default = 'NOT NULL'
            if self.default:
                # enclose value in quotes except special SQL values or already enclosed
                quote = self.default.upper() not in literals and self.default[0] not in '"\''
                default += ' DEFAULT ' + ('"%s"' if quote else "%s") % self.default
        if any((c in r'\"' for c in self.comment)):
            raise DataJointError('Illegal characters in attribute comment "%s"' % self.comment)
        return '`{name}` {type} {default} COMMENT "{comment}"'.format(
            name=self.name, type=self.type, default=default, comment=self.comment)


class Heading:
    """
    Local class for relations' headings.
    Heading contains the property attributes, which is an OrderedDict in which the keys are
    the attribute names and the values are Attributes.
    """

    def __init__(self, attributes=None):
        """
        :param attributes: a list of dicts with the same keys as Attribute
        """
        if attributes:
            attributes = OrderedDict([(q['name'], Attribute(**q)) for q in attributes])
        self.attributes = attributes

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
    def blobs(self):
        return [k for k, v in self.attributes.items() if v.is_blob]

    @property
    def non_blobs(self):
        return [k for k, v in self.attributes.items() if not v.is_blob]

    @property
    def computed(self):
        return [k for k, v in self.attributes.items() if v.computation]

    def __getitem__(self, name):
        """shortcut to the attribute"""
        return self.attributes[name]

    def __repr__(self):
        if self.attributes is None:
            return 'Empty heading'
        else:
            return '\n'.join(['%-20s : %-28s # %s' % (
                k if v.default is None else '%s="%s"' % (k, v.default),
                '%s%s' % (v.type, 'auto_increment' if v.autoincrement else ''),
                v.comment)
                for k, v in self.attributes.items()])

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
                         if self.attributes[name].computation is None
                         else '%s as `%s`' % (self.attributes[name].computation, name)
                         for name in self.names])

    def __iter__(self):
        return iter(self.attributes)

    def init_from_database(self, conn, database, table_name):
        """
        initialize heading from a database table.  The table must exist already.
        """
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
            ('float', False): np.float32,
            ('float', True): np.float32,
            ('double', False): np.float32,
            ('double', True): np.float64,
            ('tinyint', False): np.int8,
            ('tinyint', True): np.uint8,
            ('smallint', False): np.int16,
            ('smallint', True): np.uint16,
            ('mediumint', False): np.int32,
            ('mediumint', True): np.uint32,
            ('int', False): np.int32,
            ('int', True): np.uint32,
            ('bigint', False): np.int64,
            ('bigint', True): np.uint64
            # TODO: include types DECIMAL and NUMERIC
            }

        # additional attribute properties
        for attr in attributes:
            attr['nullable'] = (attr['nullable'] == 'YES')
            attr['in_key'] = (attr['in_key'] == 'PRI')
            attr['autoincrement'] = bool(re.search(r'auto_increment', attr['Extra'], flags=re.IGNORECASE))
            attr['numeric'] = bool(re.match(r'(tiny|small|medium|big)?int|decimal|double|float', attr['type']))
            attr['string'] = bool(re.match(r'(var)?char|enum|date|time|timestamp', attr['type']))
            attr['is_blob'] = bool(re.match(r'(tiny|medium|long)?blob', attr['type']))

            attr['computation'] = None
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

    def proj(self, *attribute_list, **renamed_attributes):
        """
        derive a new heading by selecting, renaming, or computing attributes.
        In relational algebra these operators are known as project, rename, and expand.
        The primary key is always included.
        """
        # check missing attributes
        missing = [a for a in attribute_list if a not in self.names]
        if missing:
            raise DataJointError('Attributes `%s` are not found' % '`, `'.join(missing))

        # always add primary key attributes
        attribute_list = self.primary_key + [a for a in attribute_list if a not in self.primary_key]

        # convert attribute_list into a list of dicts but exclude renamed attributes
        attribute_list = [v._asdict() for k, v in self.attributes.items()
                          if k in attribute_list and k not in renamed_attributes.values()]

        # add renamed and computed attributes
        for new_name, computation in renamed_attributes.items():
            if computation in self.names:
                # renamed attribute
                new_attr = self.attributes[computation]._asdict()
                new_attr['name'] = new_name
                new_attr['computation'] = '`' + computation + '`'
            else:
                # computed attribute
                new_attr = dict(
                    name=new_name,
                    type='computed',
                    in_key=False,
                    nullable=False,
                    default=None,
                    comment='computed attribute',
                    autoincrement=False,
                    numeric=None,
                    string=None,
                    is_blob=False,
                    computation=computation,
                    dtype=object)
            attribute_list.append(new_attr)

        return Heading(attribute_list)

    def join(self, other, left):
        """
        Joins two headings.
        """
        assert isinstance(other, Heading)
        attribute_list = [v._asdict() for v in self.attributes.values()]
        for name in other.names:
            if name not in self.names:
                attribute = other.attributes[name]._asdict();
                if left:
                    attribute['in_key'] = False
                attribute_list.append(attribute)
        return Heading(attribute_list)

    def resolve(self):
        """
        Remove attribute computations after they have been resolved in a subquery
        """
        return Heading([dict(v._asdict(), computation=None) for v in self.attributes.values()])
