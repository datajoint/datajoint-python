# -*- coding: utf-8 -*-
"""
Created on Thu Aug  7 17:00:02 2014

@author: dimitri
"""
import numpy as np
from copy import copy
from .core import DataJointError

class Relational:    
    """
    Relational implements relational algebra and fetching data.
    Relvar objects sharing the same connection object can be combined into 
    queries using relational operators: restrict, project, and join.
    """    
    def __init__(self):
        self._restrictions = []; 
      
    def __mul__(self, other):
        " relational join "
        return Join(self,other)
        
    def pro(self, _sub=None, *arg, **kwarg):
        " relational projection "
        if not _sub is None and not isinstance(_sub, Relational):
            raise DataJointError('Aggregation can only be performed on relvars')
        return Projection(self, *arg, **kwarg)
        
    def aggr(self, rel, *arg, **kwarg):
        " relational aggregation "
        return self.pro(_sub=rel, *arg, **kwarg)
               
    def fetch(self, _limit=None, _offset=0, *arg, **kwarg):
        """
        fetch relation from database into a recarray
        """
        conn, sql, heading = Projection(self,*arg,**kwarg)._compile()
        cur = conn.query('SELECT `'+'`,`'.join(heading.names)+'` FROM ' + sql)
        ret = np.array(list(cur), dtype=heading.asdtype)
        # unpack blobs
                
        
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
        if not isinstance(rel2,Relational):
            raise DataJointError('relvars can only be joined with other relvars')
        self.rel1 = rel1;
        self.rel2 = rel2;
    
    def _compile(self):
        conn1, sql1, heading1 = self.rel1._compile()
        conn2, sql2, heading2 = self.rel2._compile()
        if not conn1 is conn2:
            raise DataJointError('Cannot join relvars from different connections')
        #TODO: complete
        return conn1, sql, joinHeading


        
class Projection(Relational):

    aliasCounter = 0

    def __init__(self, rel, _sub=None, *arg, **kwarg):
        self._rel = rel        
        self._sub = _sub        
        self._renames = kwarg;
        self._attributes = arg
        
    def _compile(self):
        conn, sql, heading = self._rel._compile()   
        # TODO: implement projection
        return conn, sql, heading