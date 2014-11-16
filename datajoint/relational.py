# -*- coding: utf-8 -*-
"""
Created on Thu Aug  7 17:00:02 2014

@author: dimitri, eywalker
"""
import numpy as np
import abc
from copy import copy
from .core import DataJointError
from .fetch import Fetch

class _Relational(metaclass=abc.ABCMeta):   
    """
    Relational implements relational operators.
    Relational objects reference other relational objects linked by operators.
    The leaves of this tree of objects are base relvars.    
    When fetching data from the database, this tree of objects is compiled into
    and SQL expression. 
    It is a mixin class that provides relational operators, iteration, and 
    fetch capability.
    Relational operators are: restrict, pro, aggr, and join. 
    """    
    _restrictions = []
    _limit = None
    _offset = 0
    _order_by = []
    
    #### abstract properties that subclasses must define  #####
    @abc.abstractproperty
    def sql(self):
        return NotImplemented
    
    @abc.abstractproperty
    def heading(self):
        return NotImplemented
            

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
        ret._restrictions = list(ret._restrictions)  # copy restriction
        ret &= restriction
        return ret

    def __isub__(self, restriction):
        "in-place inverted restriction aka antijoin"
        self &= Not(restriction)
        return self

    def __sub__(self, restriction):
        "inverted restriction aka antijoin"
        return self & Not(restriction)
        

    ######    Fetching the data   ##############
    @property
    def count(self):
        sql = 'SELECT count(*) FROM ' + self.sql + self._whereClause
        cur = self.conn.query(sql)
        return cur.fetchone()[0]

    @property    
    def fetch(self):
        return Fetch(self)
        
    def __repr__(self):
        header = self.heading.notBlobs;        
        limit = 13;
        width = 12;
        template = '%%-%d.%ds' % (width,width);
        str = ' '.join([template % column for column in header])+'\n';
        str+= ' '.join(['+'+'-'*(width-2)+'+' for column in header])+'\n';
        
        tuples = self.fetch.limit(limit)(*header);
        for tup in tuples:
            str += ' '.join([template % column for column in tup])+'\n';
        cnt = self.count
        if cnt > limit:
            str += '...\n'
        str += '%d tuples\n' % self.count
        return str
        
    ########  iterator  ###############
    def __iter__(self):
        cur, h = self.fetch._cursor()
        dtype = h.asdtype        
        q = cur.fetchone()       
        while q:
            yield np.array([q,],dtype=dtype)
            q = cur.fetchone()       
            


    @property
    def _whereClause(self):
        "make there WHERE clause based on the current restriction"

        if not self._restrictions:
            return ''
        
        def makeCondition(arg):
            if isinstance(arg,dict):
                conds = ['`%s`=%s'%(k,repr(v)) for k,v in arg.items()]
            elif isinstance(arg,np.void):
                conds = ['`%s`=%s'%(k, arg[k]) for k in arg.dtype.fields]
            else:
                raise DataJointError('invalid restriction type')            
            return ' AND '.join(conds)
             
        condStr = []
        for r in self._restrictions:
            negate = isinstance(r,Not)
            if negate:
                r = r._restriction
            if isinstance(r,dict) or isinstance(r,np.void):
                r = makeCondition(r)
            elif isinstance(r,np.ndarray) or isinstance(r,list):
                r = '('+') OR ('.join([makeCondition(q) for q in r])+')'
            elif isinstance(r,_Relational):
                commonAttrs = ','.join([q for q in self.heading.names if r.heading.names])  
                r = '(%s) in (SELECT %s FROM %s)' % (commonAttrs, commonAttrs, r.sql)
                
            assert isinstance(r,str), 'condition must be converted into a string'
            r = '('+r+')'
            if negate:
                r = 'NOT '+r;
            condStr.append(r)
            
        return ' WHERE ' + ' AND '.join(condStr)


class Not:
    "inverse of restriction: turns semi" 
    def __init__(self,restriction):
        self._restriction = restriction
  
   
class Join(_Relational):

    aliasCounter = 0
    
    def __init__(self,rel1,rel2):
        if not isinstance(rel2,_Relational):
            raise DataJointError('relvars can only be joined with other relvars')
        if rel1.conn is not rel2.conn:
            raise DataJointError('Cannot join relations with different database connections')
        self.conn = rel1.conn
        self._rel1 = rel1;
        self._rel2 = rel2;
    
    @property
    def heading(self):
        return self._rel1.heading.join(self._rel2.heading)
        
    @property
    def sql(self):
        Join.aliasCounter += 1
        return '%s NATURAL JOIN %s as `j%x`' % (self._rel1.sql, self._rel2.sql, Join.aliasCounter)

        
class Projection(_Relational):

    aliasCounter = 0

    def __init__(self, rel, *arg, _sub, **kwarg):
        if _sub and isinstance(_sub, _Relational):
            raise DataJointError('Relational join must receive two relations')
        self.conn = rel.conn
        self._rel = rel        
        self._sub = _sub        
        self._selection = arg
        self._renames = kwarg
        
    @property 
    def sql(self):
        return self._rel.sql
        
    @property
    def heading(self):
        return self._rel.heading.pro(*self._selection, **self._renames)


class Subquery(_Relational):
    
    aliasCounter = 0;    
    
    def __init__(self, rel):
        self.conn = rel.conn;
        self._rel = rel;
        
    @property
    def sql(self):
        self.aliasCounter = self.aliasCounter + 1;
        return '(SELECT ' + self._rel.heading.asSQL + ' FROM ' + self._rel.sql + ') as `s%x`' % self.aliasCounter
        
    @property
    def heading(self):
        return self._rel.heading.resolveComputations()