# -*- coding: utf-8 -*-
"""
Created on Thu Aug  7 17:00:02 2014

@author: dimitri
"""
import numpy as np
from copy import copy
from .core import DataJointError
from .blob import unpack

class Relational:    
    """
    Relational implements relational algebra and fetching data.
    Relvar objects sharing the same connection object can be combined into 
    queries using relational operators: restrict, project, and join.
    """    
    def __init__(self, conn):
        self._restrictions = [];
        self.conn = conn
      
    def __mul__(self, other):
        "relational join"
        return Join(self,other)
        
    def pro(self, *arg, _sub=None, **kwarg):
        "relational projection abd aggregation"
        print('**SELECT**', arg)
        return Projection(self, _sub=_sub, *arg, **kwarg)
        
    def fetch(self, *arg, _limit=None, _offset=0, **kwarg):
        """
        fetch relation from database into a recarray
        """
        sql, heading = self.pro(*arg,**kwarg)._compile()
        #TODO: implement offset and limt
        print('SELECT '+heading.asSQL+' FROM ' + sql)
        cur = self.conn.query('SELECT '+heading.asSQL+' FROM ' + sql)
        ret = np.array(list(cur), dtype=heading.asdtype)
        # unpack blobs
        for i in range(len(ret)):
            for f in heading.blobs:
                ret[i][f] = unpack(ret[i][f])                 
        return ret

    def __and__(self, restriction):
        "relational restriction or semijoin"
        ret = copy(self)
        ret &= restriction
        return ret
        
    def __iand__(self, restriction):
        "in-place relational restriction or semijoin"
        self._restrictions.append(restriction)
        return self    

    def __isub__(self, restriction):
        "in-place relational restriction or antijoin"
        self &= Not(restriction)
        return self

    def __sub__(self, restriction):
        "inverted restriction or antijoin"
        return self & Not(restriction)


class Not:
    "inverse of a restriction" 
    def __init__(self,restriction):
        self.restriction = restriction
  
   
class Join(Relational):

    aliasCounter = 0
    
    def __init__(self,rel1,rel2):
        super().__init__(rel1.conn)
        if not isinstance(rel2,Relational):
            raise DataJointError('relvars can only be joined with other relvars')
        if not rel1.conn is rel2.conn:
            raise DataJointError('Cannot join relvars from different connections')
        self._rel1 = rel1;
        self._rel2 = rel2;
    
    def _compile(self):
        sql1, heading1 = self._rel1._compile()
        sql2, heading2 = self._rel2._compile()
        #TODO: incomplete
        heading = heading1.join(heading2)
        sql = '%s NATURAL JOIN %s as `$t%x`' % (sql1, sql2) 
        return sql, heading


        
class Projection(Relational):

    aliasCounter = 0

    def __init__(self, rel, *arg, _sub, **kwarg):
        if _sub and isinstance(_sub, Relational):
            raise DataJointError('A relationl is required for ')
        if _sub and not kwarg:
            raise DataJointError('No aggregation attributes requested')
        super().__init__(rel.conn)
        self._rel = rel        
        self._sub = _sub        
        self._selection = arg
        self._renames = kwarg
        print('Selection: ', self._selection)
        
    def _compile(self):
        sql, heading = self._rel._compile()
        heading = heading._pro(*self._selection, **self._renames)
        # TODO: enclose in subquery if necessary
        return sql, heading