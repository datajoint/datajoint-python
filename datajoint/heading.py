# -*- coding: utf-8 -*-
"""
Created on Mon Aug  4 01:29:51 2014

@author: dimitri
"""

import re
from core import DataJointError


class Heading:
    """
    local class for handling table headings
    """
    @property
    def names(self):
        return [atr['name'] for atr in self.attrs]

    @property
    def primaryKey(self):
        return [atr['name'] for atr in self.attrs if atr['isKey']]

    @property
    def dependentFields(self):
        return [atr['name'] for atr in self.attrs if not atr['isKey']]

    @property
    def blobNames(self):
        return [atr['name'] for atr in self.attrs if atr['isBlob']]

    @property
    def notBlobs(self):
        return [atr['name'] for atr in self.attrs if not atr['isBlob']]

    @property
    def hasAliases(self):
        return any((bool(atr['alias']) for atr in self.attrs))

    @property
    def byName(self, name):
        for attr in self.attrs:
            if attr['name'] == name:
                return attr
        raise KeyError("Field with name '%s' not found" % name)

    def __init__(self, dbName, tableName, attrs):
        self.dbName = dbName
        self.tableName = tableName
        self.attrs = attrs

    # Class methods
    @classmethod
    def initFromDatabase(cls, conn, dbName, tableName):
        """
        initialize heading from database
        """
        
        cur = conn.query(
            """
            SHOW FULL COLUMNS FROM `{tableName}` IN `{dbName}`
            """.format(tableName=tableName, dbName=dbName), 
                       asDict=True)
        attrs = cur.fetchall()

        renameMap = {
            'Field': 'name',
            'Type': 'type',
            'Null': 'isNullable',
            'Default': 'default',
            'Key': 'isKey',
            'Comment': 'comment'}

        dropFields = ['Privileges', 'Collation']

        # rename fields using renameMap and drop unwanted fields
        attrs = [{renameMap[k] if k in renameMap else k: v
                  for k, v in x.items() if k not in dropFields}
                 for x in attrs]

        for attr in attrs:
            attr['isNullable'] = (attr['isNullable'] == 'YES')
            attr['isKey'] = (attr['isKey'] == 'PRI')
            attr['isAutoincrement'] = bool(re.search(r'auto_increment', attr['Extra'], flags=re.IGNORECASE))
            attr['isNumeric'] = bool(re.match(r'(tiny|small|medium|big)?int|decimal|double|float', attr['type']))
            attr['isString'] = bool(re.match(r'(var)?char|enum|date|time|timestamp', attr['type']))
            attr['isBlob'] = bool(re.match(r'(tiny|medium|long)?blob', attr['type']))

            # strip field lengths off integer types
            attr['type'] = re.sub(r'((tiny|small|medium|big)?int)\(\d+\)', r'\1', attr['type'])
            attr['alias'] = ''
            if not (attr['isNumeric'] or attr['isString'] or attr['isBlob']):
                raise DataJointError('Unsupported field type {field} in `{dbName}`.`{tableName}`'.format(
                    field=attr['type'], dbName=dbName, tableName=tableName))
            attr.pop('Extra')

        return cls(dbName, tableName, attrs)


    def pro(self, attrs):
        """
        project heading onto a list of attributes.
        Always include primary key.
        """
        # TODO: parse computed and renamed attributes
        return Heading(self.dbName, self.tableName,
                      [{k:v for k,v in x.items()} 
                      for x in self.attrs 
                      if '*' in attrs or x['name'] in attrs or x['isKey']])
        
        
if __name__ == "__main__":
    from conn import conn
    h = Heading.initFromDatabase(conn(),'common','animal')
    print('Attributes: ', h.names)
    print('Primary key: ', h.primaryKey)
        