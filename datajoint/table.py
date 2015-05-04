import numpy as np
from . import DataJointError
from .relational import Relation
import logging

logger = logging.getLogger(__name__)


class Table(Relation):
    """
    A Table object is a relation associated with a table.
    A Table object provides insert and delete methods.
    Table objects are only used internally and for debugging.
    The table must already exist in the schema for the table object to work.
    The table is identified by its "class name", or its CamelCase version.
    """

    def __init__(self, conn=None, dbname=None, class_name=None):
        self._use_package = False
        self.class_name = class_name
        self.conn = conn
        self.dbname = dbname
        if dbname not in self.conn.db_to_mod:
            # register with a fake module, enclosed in back quotes
            self.conn.bind('`{0}`'.format(dbname), dbname)

    @property
    def sql(self):
        return self.full_table_name + self._whereClause

    @property
    def heading(self):
        self.declare()
        return self.conn.headings[self.dbname][self.table_name]

    @property
    def full_table_name(self):
        """
        :return: full name of the associated table
        """
        return '`%s`.`%s`' % (self.dbname, self.table_name)

    @property
    def table_name(self):
        """
        :return: name of the associated table
        """
        return self.conn.table_names[self.dbname][self.class_name]

    def insert(self, tup, ignore_errors=False, replace=False):
        """
        Insert one data tuple.

        :param tup: Data tuple. Can be an iterable in matching order, a dict with named fields, or an np.void.
        :param ignore_errors=False: Ignores errors if True.
        :param replace=False: Replaces data tuple if True.

        Example::

            b = djtest.Subject()
            b.insert(dict(subject_id = 7, species="mouse",\\
                           real_id = 1007, date_of_birth = "2014-09-01"))
        """
        # todo: do we support records and named tuples for tup?

        if issubclass(type(tup), tuple) or issubclass(type(tup), list):
            value_list = ','.join([repr(q) for q in tup])
            attribute_list = '`'+'`,`'.join(self.heading.names[0:len(tup)]) + '`'
        elif issubclass(type(tup), dict):
            value_list = ','.join([repr(tup[q])
                                  for q in self.heading.names if q in tup])
            attribute_list = '`' + '`,`'.join([q for q in self.heading.names if q in tup]) + '`'
        elif issubclass(type(tup), np.void):
            value_list = ','.join([repr(tup[q])
                                  for q in self.heading.names if q in tup])
            attribute_list = '`' + '`,`'.join(tup.dtype.fields) + '`'
        else:
            raise DataJointError('Datatype %s cannot be inserted' % type(tup))
        if replace:
            sql = 'REPLACE'
        elif ignore_errors:
            sql = 'INSERT IGNORE'
        else:
            sql = 'INSERT'
        sql += " INTO %s (%s) VALUES (%s)" % (self.full_table_name,
                                              attribute_list, value_list)
        logger.info(sql)
        self.conn.query(sql)

    def delete(self):   # TODO
        pass
