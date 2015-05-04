
import re
from collections import OrderedDict, namedtuple
import numpy as np
from datajoint import DataJointError


class Heading:
    """
    local class for relations' headings.
    """
    AttrTuple = namedtuple('AttrTuple',
                           ('name', 'type', 'in_key', 'nullable', 'default', 'comment', 'autoincrement', 
                            'numeric', 'string', 'is_blob', 'computation', 'dtype'))

    def __init__(self, attributes):
        # Input: attributes -list of dicts with attribute descriptions
        self.attributes = OrderedDict([(q['name'], Heading.AttrTuple(**q)) for q in attributes])

    @property
    def names(self):
        return [k for k in self.attributes]

    @property
    def primary_key(self):
        return [k for k, v in self.attributes.items() if v.in_key]

    @property
    def dependent_fields(self):
        return [k for k, v in self.attributes.items() if not v.in_key]

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
        autoincrement_string = {False: '', True: ' auto_increment'}
        return '\n'.join(['%-20s : %-28s # %s' % (
            k if v.default is None else '%s="%s"' % (k, v.default),
            '%s%s' % (v.type, autoincrement_string[v.autoincrement]),
            v.comment)
            for k, v in self.attributes.items()])

    @property
    def as_dtype(self):
        """
        represent the heading as a numpy dtype
        """
        return np.dtype(dict(
            names=self.names,
            formats=[v.dtype for k, v in self.attributes.items()]))

    @property
    def as_sql(self):
        """
        represent heading as SQL field list
        """
        return ','.join(['`%s`' % name
                         if self.attributes[name].computation is None
                         else '%s as `%s`' % (self.attributes[name].computation, name)
                         for name in self.names])

    def keys(self):
        return self.attributes.keys()

    def values(self):
        return self.attributes.values()

    def items(self):
        return self.attributes.items()

    @classmethod
    def init_from_database(cls, conn, dbname, table_name):
        """
        initialize heading from a database table
        """
        cur = conn.query(
            'SHOW FULL COLUMNS FROM `{table_name}` IN `{dbname}`'.format(
            table_name=table_name, dbname=dbname), asDict=True)
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

            # strip field lengths off integer types
            attr['type'] = re.sub(r'((tiny|small|medium|big)?int)\(\d+\)', r'\1', attr['type'])

            attr['computation'] = None
            if not (attr['numeric'] or attr['string'] or attr['is_blob']):
                raise DataJointError('Unsupported field type {field} in `{dbname}`.`{table_name}`'.format(
                    field=attr['type'], dbname=dbname, table_name=table_name))
            attr.pop('Extra')

            # fill out the dtype. All floats and non-nullable integers are turned into specific dtypes
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

        return cls(attributes)

    def pro(self, *attribute_list, **rename_dict):
        """
        derive a new heading by selecting, renaming, or computing attributes.
        In relational algebra these operators are known as project, rename, and expand.
        The primary key is always included.
        """
        # include all if '*' is in attribute_set, always include primary key
        attribute_set = set(self.names) if '*' in attribute_list \
            else set(attribute_list).union(self.primary_key)

        # report missing attributes
        missing = attribute_set.difference(self.names)
        if missing:
            raise DataJointError('Attributes %s are not found' % str(missing))

        # make attribute_list a list of dicts for initializing a Heading
        attribute_list = [v._asdict() for k, v in self.attributes.items()
                          if k in attribute_set and k not in rename_dict.values()]

        # add renamed and computed attributes
        for new_name, computation in rename_dict.items():
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

    def join(self, other):
        """
        join two headings
        """
        assert isinstance(other, Heading)
        attribute_list = [v._asdict() for v in self.attributes.values()]
        for name in other.names:
            if name not in self.names:
                attribute_list.append(other.attributes[name]._asdict())
        return Heading(attribute_list)

    def resolve_computations(self):
        """
        Remove computations.  To be done after computations have been resolved in a subquery
        """
        return Heading([dict(v._asdict(), computation=None) for v in self.attributes.values()])