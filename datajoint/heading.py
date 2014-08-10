# -*- coding: utf-8 -*-
"""
Created on Mon Aug  4 01:29:51 2014

@author: dimitri
"""

import re
from collections import OrderedDict, namedtuple
import numpy as np
from .core import DataJointError


class Heading:
    """
    local class for handling table headings
    """
    
    AttrTuple = namedtuple('AttrTuple',('name','type','isKey','isNullable',
    'default','comment','isAutoincrement','isNumeric','isString','isBlob',
    'alias','dtype'))

    def __init__(self, attrs):
        # Input: attrs - array of dicts with attribute descriptions
        self.attrs = OrderedDict([(q['name'], Heading.AttrTuple(**q)) for q in attrs])
    

    @property
    def names(self):
        return [k for k in self.attrs]

    @property
    def primaryKey(self):
        return [k for k,v in self.attrs.items() if v.isKey]

    @property
    def dependentFields(self):
        return [k for k,v in self.attrs.items() if not v.isKey]

    @property
    def blobs(self):
        return [k for k,v in self.attrs.items() if v.isBlob]

    @property
    def notBlobs(self):
        return [k for k,v in self.attrs.items() if not v.isBlob]

    @property
    def hasAliases(self):
        return any((bool(v.alias) for v in self.attrs.values()))
        
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

    def pro(self, *attrs, **renames):
        """
        derive a new heading by selecting, renaming, or computing new attributes.
        In relational algebra these operators are known as project, rename, and expand.
        The primary key is always included.
        """
        # TODO: parse computed and renamed attributes

    @property        
    def asdtype(self):
        """
        represent the heading as a numpy dtype
        """        
        return np.dtype(dict(
            names=self.names, 
            formats=[v.dtype for k,v in self.attrs.items()]))        
                        
                        
    @classmethod
    def initFromDatabase(cls, conn, dbname, tabname):
        """
        initialize heading from a database table
        """
        cur = conn.query(
            'SHOW FULL COLUMNS FROM `{tabname}` IN `{dbname}`'.format(
            tabname=tabname, dbname=dbname),asDict=True)
        attrs = cur.fetchall()

        renameMap = {
            'Field'  : 'name',
            'Type'   : 'type',
            'Null'   : 'isNullable',
            'Default': 'default',
            'Key'    : 'isKey',
            'Comment': 'comment'}
            
        dropFields = ('Privileges', 'Collation')

        # rename and drop attributes
        attrs = [{renameMap[k] if k in renameMap else k: v
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
           
            attr['alias'] = None
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
    