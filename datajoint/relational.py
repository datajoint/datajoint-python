# -*- coding: utf-8 -*-
"""
Created on Thu Aug  7 17:00:02 2014

@author: dimitri
"""
import numpy as np
import abc
from copy import copy
from .core import DataJointError, log
from .blob import unpack

class _Relational(metaclass=abc.ABCMeta):   
    """
    Relational implements relational algebra and fetching data.
    It is a mixin class that provides relational operators, iteration, and 
    fetch capability.
    Relational operators are: restrict, pro, aggr, and join. 
    """    
    _restrictions = None
      
    @abc.abstractmethod 
    def _compile():
        """
        all deriving classes must define _compile(self) to return the sql string
        and the heading        
        """
        return NotImplemented  # must override
        

    ######    Relational algebra   ##############

    def __mul__(self, other):
        "relational join"
        return Join(self,other)
        
    def pro(self, *arg, _sub=None, **kwarg):
        "relational projection abd aggregation"
        return Projection(self, _sub=_sub, *arg, **kwarg)
            
    def __iand__(self, restriction):
        "in-place relational restriction or semijoin"
        if self._restrictions is None:
            self._restrictions = []
        self._restrictions.append(restriction)
        return self        
    
    def __and__(self, restriction):
        "relational restriction or semijoin"
        ret = copy(self)
        ret._restrictions = list(ret._restrictions)  # copy restiction
        ret &= restriction
        return ret

    def __isub__(self, restriction):
        "in-place relational restriction or antijoin"
        self &= Not(restriction)
        return self

    def __sub__(self, restriction):
        "inverted restriction or antijoin"
        return self & Not(restriction)
        

    ######    Fetching the data   ##############

    @property 
    def count(self):
        [sql,heading] = self._compile()
        sql = 'SELECT count(*) FROM ' + sql + self._whereClause
        cur = self.conn.query(sql)
        return cur.fetchone()[0]
        
    def fetch(self, *attrs, _limit=None, _offset=0, _orderBy=None, **renames):
        """
        fetch relation from database into a recarray
        """
        cur, heading = self._fetchCursor(*attrs, _limit=_limit, _offset=_offset, _orderBy=_orderBy, **renames)
        ret = np.array(list(cur), dtype=heading.asdtype)
        # unpack blobs
        for i in range(len(ret)):
            for f in heading.blobs:
                ret[i][f] = unpack(ret[i][f])                 
        return ret
    
    def _fetchCursor(self, *attrs, _limit, _offset, _orderBy, **renames):
        sql, heading = self.pro(*attrs, **renames)._compile()
        #TODO: implement offset, limit, and order by
        sql = 'SELECT '+heading.asSQL+' FROM ' + sql + self._whereClause
        log(sql)
        return self.conn.query(sql), heading
        
        
    ########  iterator  ###############
    def __iter__(self):
        cur, h = self._fetchCursor(_limit=None, _offset=0, _orderBy=None)
        dtype = h.asdtype        
        q = cur.fetchone()       
        while q:
            yield np.array([q,],dtype=dtype)
            q = cur.fetchone()       
            


    @property
    def _whereClause(self):
        "make there WHERE clause based on the current restriction"
        def makeCondition(arg):
            if isinstance(arg,dict):
                conds = ['`%s`=%s'%(k,repr(v)) for k,v in arg.items()]
            elif isinstance(arg,np.void):
                conds = ['`%s`=%s'%(k, arg[k]) for k in arg.dtype.fields]
            else:
                raise DataJointError('invalid restriction type')
            
            return ' AND '.join(conds)
                
        
        if not self._restrictions:
            sql = ''
        else:
            condStr = []
            for r in self._restrictions:
                negate = isinstance(r,Not)
                if negate:
                    r = r._restriction
                if isinstance(r,dict) or isinstance(r,np.void):
                    r = makeCondition(r)
                elif isinstance(r,np.ndarray) or isinstance(r,list):
                    r = '('+') OR ('.join([makeCondition(q) for q in r])+')'
                        
                #TODO: imlement restriction by dict and np.array
                assert isinstance(r,str), 'condition must be converted into a string'
                r = '('+r+')'
                if negate:
                    r = 'NOT '+r;
                condStr.append(r)
            sql = ' WHERE ' + ' AND '.join(condStr)
        return sql


class Not:
    "inverse of a restriction" 
    def __init__(self,restriction):
        self._restriction = restriction
  
   
class Join(_Relational):

    aliasCounter = 0
    
    def __init__(self,rel1,rel2):
        if not isinstance(rel2,_Relational):
            raise DataJointError('relvars can only be joined with other relvars')
        if not rel1.conn is rel2.conn:
            raise DataJointError('Cannot join relvars from different connections')
        self.conn = rel1.conn
        self._rel1 = rel1;
        self._rel2 = rel2;
    
    def _compile(self):
        sql1, heading1 = self._rel1._compile()
        sql2, heading2 = self._rel2._compile()
        #TODO: incomplete
        heading = heading1.join(heading2)
        sql = '%s NATURAL JOIN %s as `$t%x`' % (sql1, sql2, Join.aliasCounter)
        Join.aliasCounter += 1
        return sql+self._whereClause, heading


        
class Projection(_Relational):

    aliasCounter = 0

    def __init__(self, rel, *arg, _sub, **kwarg):
        if _sub and isinstance(_sub, _Relational):
            raise DataJointError('A relationl is required for ')
        if _sub and not kwarg:
            raise DataJointError('No aggregation attributes requested')
        self.conn = rel.conn
        self._rel = rel        
        self._sub = _sub        
        self._selection = arg
        self._renames = kwarg
        
    def _compile(self):
        sql, heading = self._rel._compile()
        heading = heading._pro(*self._selection, **self._renames)
        # TODO: enclose subqueries
        return sql + self._whereClause, heading