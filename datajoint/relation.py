from collections.abc import Mapping
import numpy as np
import logging
import abc
import pymysql

from . import DataJointError, config, conn
from .declare import declare
from .relational_operand import RelationalOperand
from .blob import pack
from .utils import user_choice
from .heading import Heading

logger = logging.getLogger(__name__)


def schema(database, context, connection=None):
    """
    Returns a decorator that can be used to associate a Relation class to a database.

    :param database: name of the database to associate the decorated class with
    :param context: dictionary for looking up foreign keys references, usually set to locals()
    :param connection: Connection object. Defaults to datajoint.conn()
    :return: a decorator for Relation subclasses
    """
    if connection is None:
        connection = conn()

    # if the database does not exist, create it
    cur = connection.query("SHOW DATABASES LIKE '{database}'".format(database=database))
    if cur.rowcount == 0:
        logger.info("Database `{database}` could not be found. "
                    "Attempting to create the database.".format(database=database))
        try:
            connection.query("CREATE DATABASE `{database}`".format(database=database))
            logger.info('Created database `{database}`.'.format(database=database))
        except pymysql.OperationalError:
            raise DataJointError("Database named `{database}` was not defined, and"
                                 "an attempt to create has failed. Check"
                                 " permissions.".format(database=database))

    def decorator(cls):
        """
        The decorator binds its argument class object to a database
        :param cls: class to be decorated
        """
        # class-level attributes
        cls.database = database
        cls._connection = connection
        cls._heading = Heading()
        cls._context = context
        # trigger table declaration by requesting the heading from an instance
        cls().heading
        return cls

    return decorator


class Relation(RelationalOperand, metaclass=abc.ABCMeta):
    """
    Relation is an abstract class that represents a base relation, i.e. a table in the database.
    To make it a concrete class, override the abstract properties specifying the connection,
    table name, database, context, and definition.
    A Relation implements insert and delete methods in addition to inherited relational operators.
    """
    _heading = None
    _context = None

    # ---------- abstract properties ------------ #
    @property
    @abc.abstractmethod
    def table_name(self):
        """
        :return: the name of the table in the database
        """
        raise NotImplementedError('Relation subclasses must define property table_name')

    @property
    @abc.abstractmethod
    def definition(self):
        """
        :return: a string containing the table definition using the DataJoint DDL
        """
        pass

    # -------------- required by RelationalOperand ----------------- #
    @property
    def connection(self):
        return self._connection

    @property
    def heading(self):
        """
        Get the table headng.
        If the table is not declared, attempts to declare it and return heading.
        :return:
        """
        if self._heading is None:
            self._heading = Heading()  # instance-level heading
        if not self._heading:
            if not self.is_declared:
                self.connection.query(
                    declare(self.full_table_name, self.definition, self._context))
            if self.is_declared:
                self.connection.erd.load_dependencies(self.connection, self.full_table_name)
                self._heading.init_from_database(self.connection, self.database, self.table_name)
        return self._heading

    @property
    def from_clause(self):
        """
        :return: the FROM clause of SQL SELECT statements.
        """
        return self.full_table_name


    # ------------- dependencies ---------- #
    @property
    def parents(self):
        return self.connection.erd.parents[self.full_table_name]

    @property
    def children(self):
        return self.connection.erd.children[self.full_table_name]

    @property
    def references(self):
        return self.connection.erd.references[self.full_table_name]

    @property
    def referenced(self):
        return self.connection.erd.referenced[self.full_table_name]

    @property
    def descendants(self):
        """
        :return: list of relation objects for all children and references, recursively,
        in order of dependence.
        This is helpful for cascading delete or drop operations.
        """
        relations = (FreeRelation(self.connection, table)
                     for table in self.connection.erd.get_descendants(self.full_table_name))
        return [relation for relation in relations if relation.is_declared]

    # --------- SQL functionality --------- #
    @property
    def is_declared(self):
        cur = self.connection.query(
            'SHOW TABLES in `{database}`LIKE "{table_name}"'.format(
                database=self.database, table_name=self.table_name))
        return cur.rowcount > 0

    def insert(self, rows, **kwargs):
        """
        Inserts a collection of tuples. Additional keyword arguments are passed to insert.

        :param iter: Must be an iterator that generates a sequence of valid arguments for insert.
        """
        for row in rows:
            self.insert1(row, **kwargs)


    @property
    def full_table_name(self):
        return r"`{0:s}`.`{1:s}`".format(self.database, self.table_name)

    def insert1(self, tup, ignore_errors=False, replace=False):
        """
        Insert one data record or one Mapping (like a dict).

        :param tup: Data record, or a Mapping (like a dict).
        :param ignore_errors=False: Ignores errors if True.
        :param replace=False: Replaces data tuple if True.

        Example::
            rel.insert(dict(subject_id = 7, species="mouse", date_of_birth = "2014-09-01"))
        """
        heading = self.heading
        if isinstance(tup, np.void):
            for fieldname in tup.dtype.fields:
                if fieldname not in heading:
                    raise KeyError(u'{0:s} is not in the attribute list'.format(fieldname))
            value_list = ','.join([repr(tup[name]) if not heading[name].is_blob else '%s'
                                   for name in heading if name in tup.dtype.fields])
            args = tuple(pack(tup[name]) for name in heading
                         if name in tup.dtype.fields and heading[name].is_blob)
            attribute_list = '`' + '`,`'.join(q for q in heading if q in tup.dtype.fields) + '`'
        elif isinstance(tup, Mapping):
            for fieldname in tup.keys():
                if fieldname not in heading:
                    raise KeyError(u'{0:s} is not in the attribute list'.format(fieldname))
            value_list = ','.join(repr(tup[name]) if not heading[name].is_blob else '%s'
                                  for name in heading if name in tup)
            args = tuple(pack(tup[name]) for name in heading
                         if name in tup and heading[name].is_blob)
            attribute_list = '`' + '`,`'.join(name for name in heading if name in tup) + '`'
        else:
            raise DataJointError('Datatype %s cannot be inserted' % type(tup))
        if replace:
            sql = 'REPLACE'
        elif ignore_errors:
            sql = 'INSERT IGNORE'
        else:
            sql = 'INSERT'
        sql += " INTO %s (%s) VALUES (%s)" % (self.from_clause, attribute_list, value_list)
        logger.info(sql)
        self.connection.query(sql, args=args)

    def delete_quick(self):
        """
        delete without cascading and without user prompt
        """
        self.connection.query('DELETE FROM ' + self.from_clause + self.where_clause)

    def delete(self):
        """
        Delete the contents of the table and its dependent tables, recursively.
        User is prompted for confirmation if config['safemode']
        """
        relations = self.descendants
        if self.restrictions and len(relations)>1:
            raise NotImplementedError('Restricted cascading deletes are not yet implemented')
        do_delete = True
        if config['safemode']:
            print('The contents of the following tables are about to be deleted:')
            for relation in relations:
                print(relation.full_table_name, '(%d tuples)'' % len(relation)')
            do_delete = user_choice("Proceed?", default='no') == 'yes'
        if do_delete:
            with self.connection.transaction:
                while relations:
                    relations.pop().delete_quick()

    def drop_quick(self):
        """
        Drops the table associated with this relation without cascading and without user prompt.
        """
        if self.is_declared:
            self.connection.query('DROP TABLE %s' % self.full_table_name)
            self.connection.erd.clear_dependencies(self.full_table_name)
            if self._heading:
                self._heading.reset()
            logger.info("Dropped table %s" % self.full_table_name)

    def drop(self):
        """
        Drop the table and all tables that reference it, recursively.
        User is prompted for confirmation if config['safemode']
        """
        do_drop = True
        relations = self.descendants
        if config['safemode']:
            print('The following tables are about to be dropped:')
            for relation in relations:
                print(relation.full_table_name, '(%d tuples)' % len(relation))
            do_drop = user_choice("Proceed?", default='no') == 'yes'
        if do_drop:
            while relations:
                relations.pop().drop_quick()
            print('Dropped tables..')

    def size_on_disk(self):
        """
        :return: size of data and indices in GiB taken by the table on the storage device
        """
        ret = self.connection.query(
            'SHOW TABLE STATUS FROM `{database}` WHERE NAME="{table}"'.format(
                database=self.database, table=self.table_name), as_dict=True
        ).fetchone()
        return (ret['Data_length'] + ret['Index_length'])/1024**2

class FreeRelation(Relation):
    """
    A relation with no definition. Its table must already exist in the database.
    """
    def __init__(self, connection, full_table_name, definition=None, context=None):
        [database, table_name] = full_table_name.split('.')
        self.database = database.strip('`')
        self._table_name = table_name.strip('`')
        self._connection = connection
        self._definition = definition
        self._context = context

    @property
    def definition(self):
        return self._definition

    @property
    def connection(self):
        return self._connection

    @property
    def table_name(self):
        return self._table_name
