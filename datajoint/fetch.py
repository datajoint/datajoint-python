# -*- coding: utf-8 -*-
"""
Created on Wed Aug 20 22:05:29 2014

@author: dimitri
"""

from .blob import unpack
import numpy as np
from .core import log


class Fetch:
    """
    Fetch defines callable objects that fetch data from a relation
    """
    
    def __init__(self, relational):
        self.rel = relational
        self._orderBy = None        
        self._offset = 0
        self._limit = None
    
    def limit(self, n, offset=0):
        self._limit = n
        self._offset = offset
        return self

    def orderBy(self, *attrs):
        self._orderBy = attrs
        return self
    
    def __call__(self, *attrs, **renames):
        """
        fetch relation from database into an np.array
        """
        cur, heading = self._cursor(*attrs, **renames)
        ret = np.array(list(cur), dtype=heading.asdtype)
        # unpack blobs
        for i in range(len(ret)):
            for f in heading.blobs:
                ret[i][f] = unpack(ret[i][f])
        return ret
    
    def _cursor(self, *attrs, **renames):
        sql, heading = self.rel.pro(*attrs, **renames)._compile()
        sql = 'SELECT ' + heading.asSQL + ' FROM ' + sql 
        # add ORDER BY clause
        if self._orderBy:
            sql += ' ORDER BY ' + ', '.join(self._orderBy)

        # add LIMIT clause
        if self._limit:
            sql += ' LIMIT %d' %  self._limit
            if self._offset:
                sql += ' OFFSET %d ' %  self._offset

        log(sql)
        return self.rel.conn.query(sql), heading