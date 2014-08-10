# -*- coding: utf-8 -*-
"""
Created on Tue Aug  5 22:35:55 2014

@author: dimitri
"""

import pymysql
from collections import namedtuple

class TupleCursorMixin:
    """
    cursor that fethes data in named tuples
    """    
    def _do_get_result(self):
        super()._do_get_result()
        fields = []
        if self.description:
            for f in self._result.fields:
                name = f.name
                if name in fields:
                    name = f.table_name + '.' + name
                fields.append(name)
            self._fields = fields
        
        self.TupleType = namedtuple('tup', self._fields)
        if fields and self._rows:
            self._rows = [self._conv_row(r) for r in self._rows]

    def _conv_row(self, row):
        if row is None:
            return None
        return self.TupleType(*row)
        

class TupleCursor(TupleCursorMixin, pymysql.cursors.Cursor):
    """A cursor that returns results as namedtuples"""
    
class SSTupleCursor(TupleCursorMixin, pymysql.cursors.SSCursor):
    """An unbuffered cursor that returns results as namedtuples"""