# -*- coding: utf-8 -*-
"""
Created on Mon Aug  4 01:29:51 2014

@author: dimitri, eywalker
"""

import re
from collections import OrderedDict, namedtuple
import numpy as np
from .core import DataJointError


class Heading:
    """
    local class for relationals' headings.
    """

    AttrTuple = namedtuple('AttrTuple',('name','type','isKey','isNullable',
    'default','comment','isAutoincrement','isNumeric','isString','isBlob',
    'computation','dtype'))

    def __init__(self, attrs):
        # Input: attrs -list of dicts with attribute descriptions
        self.attrs = OrderedDict([(q['name'], Heading.AttrTuple(**q)) for q in attrs])

    @property
    def names(self):
        return [k for k in self.attrs]

    @property
    def primary_key(self):
        return [k for k,v in self.attrs.items() if v.isKey]

    @property
    def dependent_fields(self):
        return [k for k,v in self.attrs.items() if not v.isKey]

    @property
    def blobs(self):
        return [k for k,v in self.attrs.items() if v.isBlob]

    @property
    def non_blobs(self):
        return [k for k,v in self.attrs.items() if not v.isBlob]

    @property
    def computed(self):
        return [k for k,v in self.attrs.items() if v.computation]

    def __getitem__(self,name):
        """shortcut to the attribute"""
        return self.attrs[name]

    def __repr__(self):
        autoIncrementString = {False:'', True:' auto_increment'}
        return '\n'.join(['%-20s : %-28s # %s' % (
            k if v.default is None else '%s="%s"'%(k,v.default),
            '%s%s' % (v.type, autoIncrementString[v.isAutoincrement]),
            v.comment)
            for k,v in self.attrs.items()])

    @property
    def asdtype(self):
        """
        represent the heading as a numpy dtype
        """
        return np.dtype(dict(
            names=self.names,
            formats=[v.dtype for k,v in self.attrs.items()]))

    @property
    def asSQL(self):
        """represent heading as SQL field list"""
        attrNames = ['`%s`' % name if self.attrs[name].computation is None else '%s as `%s`' % (self.attrs[name].computation, name)
                 for name in self.names]
        return ','.join(attrNames)

    # Use heading as a dictionary like object

    def keys(self):
        return self.attrs.keys()

    def values(self):
        return self.attrs.values()

    def items(self):
        return self.attrs.items()


    @classmethod
    def init_from_database(cls, conn, dbname, tabname):
        """
        initialize heading from a database table
        """
        cur = conn.query(
            'SHOW FULL COLUMNS FROM `{tabname}` IN `{dbname}`'.format(
            tabname=tabname, dbname=dbname),asDict=True)
        attrs = cur.fetchall()

        rename_map = {
            'Field'  : 'name',
            'Type'   : 'type',
            'Null'   : 'isNullable',
            'Default': 'default',
            'Key'    : 'isKey',
            'Comment': 'comment'}

        dropFields = ('Privileges', 'Collation')  # unncessary

        # rename and drop attributes
        attrs = [{rename_map[k] if k in rename_map else k: v
                    for k, v in x.items() if k not in dropFields}
                        for x in attrs]
        numTypes ={
            ('float',False):np.float32,
            ('float',True):np.float32,
            ('double',False):np.float32,
            ('double',True):np.float64,
            ('tinyint',False):np.int8,
            ('tinyint',True):np.uint8,
            ('smallint',False):np.int16,
            ('smallint',True):np.uint16,
            ('mediumint',False):np.int32,
            ('mediumint',True):np.uint32,
            ('int',False):np.int32,
            ('int',True):np.uint32,
            ('bigint',False):np.int64,
            ('bigint',True):np.uint64
            # TODO: include decimal and numeric datatypes
            }


        # additional attribute properties
        for attr in attrs:
            attr['isNullable'] = (attr['isNullable'] == 'YES')
            attr['isKey'] = (attr['isKey'] == 'PRI')
            attr['isAutoincrement'] = bool(re.search(r'auto_increment', attr['Extra'], flags=re.IGNORECASE))
            attr['isNumeric'] = bool(re.match(r'(tiny|small|medium|big)?int|decimal|double|float', attr['type']))
            attr['isString'] = bool(re.match(r'(var)?char|enum|date|time|timestamp', attr['type']))
            attr['isBlob'] = bool(re.match(r'(tiny|medium|long)?blob', attr['type']))

            # strip field lengths off integer types
            attr['type'] = re.sub(r'((tiny|small|medium|big)?int)\(\d+\)', r'\1', attr['type'])

            attr['computation'] = None
            if not (attr['isNumeric'] or attr['isString'] or attr['isBlob']):
                raise DataJointError('Unsupported field type {field} in `{dbname}`.`{tabname}`'.format(
                    field=attr['type'], dbname=dbname, tabname=tabname))
            attr.pop('Extra')

            # fill out the dtype. All floats and non-nullable integers are turned into specific dtypes
            attr['dtype'] = object
            if attr['isNumeric'] :
                isInteger = bool(re.match(r'(tiny|small|medium|big)?int',attr['type']))
                isFloat    = bool(re.match(r'(double|float)',attr['type']))
                if isInteger and not attr['isNullable']  or isFloat:
                    isUnsigned = bool(re.match('\sunsigned', attr['type'], flags=re.IGNORECASE))
                    t = attr['type']
                    t = re.sub(r'\(.*\)','',t)    # remove parentheses
                    t = re.sub(r' unsigned$','',t)   # remove unsigned
                    assert (t,isUnsigned) in numTypes, 'dtype not found for type %s' % t
                    attr['dtype'] = numTypes[(t,isUnsigned)]

        return cls(attrs)


    def pro(self, *attrList, **renameDict):
        """
        derive a new heading by selecting, renaming, or computing attributes.
        In relational algebra these operators are known as project, rename, and expand.
        The primary key is always included.
        """
        # include all if '*' is in attrSet, always include primary key
        attrSet = set(self.names) if '*' in attrList \
            else set(attrList).union(self.primary_key)

        # report missing attributes
        missing = attrSet.difference(self.names)
        if missing:
            raise DataJointError('Attributes %s are not found' % str(missing))

        # make attrList a list of dicts for initializing a Heading
        attrList = [v._asdict() for k,v in self.attrs.items() 
            if k in attrSet and k not in renameDict.values()]

        # add renamed and computed attributes
        for newName, computation in renameDict.items():
            if computation in self.names:
                # renamed attribute
                newAttr = self.attrs[computation]._asdict()
                newAttr['name'] = newName
                newAttr['computation'] = '`' + computation + '`'
            else:
                # computed attribute
                newAttr = dict(
                    name = newName,
                    type = 'computed',
                    isKey = False,
                    isNullable = False,
                    default = None,
                    comment = 'computed attribute',
                    isAutoincrement = False,
                    isNumeric = None,
                    isString = None,
                    isBlob = False,
                    computation = computation,
                    dtype = object)
            attrList.append(newAttr)

        return Heading(attrList)


    def join(self, other):
        """
        join two headings
        """
        assert isinstance(other,Heading)
        attrList = [v._asdict() for v in self.attrs.values()]
        for name in other.names:
            if name not in self.names:
                attrList.append(other.attrs[name]._asdict())
        return Heading(attrList)


    def resolveComputations(self):
        """
        Remove computations.  To be done after computations have been resolved in a subquery
        """
        return Heading( [dict(v._asdict(),computation=None) for v in self.attrs.values()] )

