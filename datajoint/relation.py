from collections import namedtuple
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
from .declare import compile_attribute

logger = logging.getLogger(__name__)

TableLink = namedtuple(
    'TableLink',
    ('database', 'context', 'connection', 'heading'))


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
        The decorator declares the table and binds the class to the database table
        """
        cls._table_info = TableLink(
            database=database,
            context=context,
            connection=connection,
            heading=None
        )
        declare(cls())
        return cls

    return decorator



class Relation(RelationalOperand, metaclass=abc.ABCMeta):
    """
    Relation is an abstract class that represents a base relation, i.e. a table in the database.
    To make it a concrete class, override the abstract properties specifying the connection,
    table name, database, context, and definition.
    A Relation implements insert and delete methods in addition to inherited relational operators.
    It also loads table heading and dependencies from the database.
    It also handles the table declaration based on its definition property
    """

    _table_info = None

    def __init__(self):
        if self._table_info is None:
            raise DataJointError('The class must define _table_info')

    # ---------- abstract properties ------------ #
    @property
    @abc.abstractmethod
    def table_name(self):
        """
        :return: the name of the table in the database
        """
        pass

    @property
    @abc.abstractmethod
    def definition(self):
        """
        :return: a string containing the table definition using the DataJoint DDL
        """
        pass

    # -------------- table info ----------------- #
    @property
    def connection(self):
        return self._table_info.connection

    @property
    def database(self):
        return self._table_info.database

    @property
    def context(self):
        return self._table_info.context

    @property
    def heading(self):
        if self._table_info.heading is None:
            self._table_info.heading = Heading.init_from_database(
                self.connection, self.database, self.table_name)
        return self._table_info.heading


    # --------- SQL functionality --------- #
    @property
    def from_clause(self):
        """
        Required by the Relational class, this property specifies the contents of the FROM clause 
        for the SQL SELECT statements.
        :return:
        """
        return self.full_table_name

    def iter_insert(self, rows, **kwargs):
        """
        Inserts an entire batch of entries. Additional keyword arguments are passed to insert.

        :param iter: Must be an iterator that generates a sequence of valid arguments for insert.
        """
        for row in rows:
            self.insert(row, **kwargs)

    def batch_insert(self, data, **kwargs):
        """
        Inserts an entire batch of entries. Additional keyword arguments are passed to insert.

        :param data: must be iterable, each row must be a valid argument for insert
        """
        self.iter_insert(data.__iter__(), **kwargs)

    def full_table_name(self):
        return r"`{0:s}`.`{1:s}`".format(self.database, self.table_name)

    @classmethod
    def insert(self, tup, ignore_errors=False, replace=False):
        """
        Insert one data record or one Mapping (like a dictionary).

        :param tup: Data record, or a Mapping (like a dictionary).
        :param ignore_errors=False: Ignores errors if True.
        :param replace=False: Replaces data tuple if True.

        Example::

            b = djtest.Subject()
            b.insert(dict(subject_id = 7, species="mouse",\\
                           real_id = 1007, date_of_birth = "2014-09-01"))
        """

        heading = self.heading
        if isinstance(tup, np.void):
            for fieldname in tup.dtype.fields:
                if fieldname not in heading:
                    raise KeyError(u'{0:s} is not in the attribute list'.format(fieldname, ))
            value_list = ','.join([repr(tup[name]) if not heading[name].is_blob else '%s'
                                   for name in heading if name in tup.dtype.fields])

            args = tuple(pack(tup[name]) for name in heading
                         if name in tup.dtype.fields and heading[name].is_blob)
            attribute_list = '`' + '`,`'.join(
                [q for q in heading if q in tup.dtype.fields]) + '`'
        elif isinstance(tup, Mapping):
            for fieldname in tup.keys():
                if fieldname not in heading:
                    raise KeyError(u'{0:s} is not in the attribute list'.format(fieldname, ))
            value_list = ','.join([repr(tup[name]) if not heading[name].is_blob else '%s'
                                   for name in heading if name in tup])
            args = tuple(pack(tup[name]) for name in heading
                         if name in tup and heading[name].is_blob)
            attribute_list = '`' + '`,`'.join(
                [name for name in heading if name in tup]) + '`'
        else:
            raise DataJointError('Datatype %s cannot be inserted' % type(tup))
        if replace:
            sql = 'REPLACE'
        elif ignore_errors:
            sql = 'INSERT IGNORE'
        else:
            sql = 'INSERT'
        sql += " INTO %s (%s) VALUES (%s)" % (self.from_caluse, attribute_list, value_list)
        logger.info(sql)
        self.connection.query(sql, args=args)

    def delete(self):
        if not config['safemode'] or user_choice(
                "You are about to delete data from a table. This operation cannot be undone.\n"
                "Proceed?", default='no') == 'yes':
            self.connection.query('DELETE FROM ' + self.from_clause + self.where_clause)  # TODO: make cascading (issue #15)

    def drop(self):
        """
        Drops the table associated to this class.
        """
        if self.is_declared:
            if not config['safemode'] or user_choice(
                    "You are about to drop an entire table. This operation cannot be undone.\n"
                    "Proceed?", default='no') == 'yes':
                self.connection.query('DROP TABLE %s' % self.full_table_name)  # TODO: make cascading (issue #16)
                # cls.connection.clear_dependencies(dbname=cls.dbname) #TODO: reimplement because clear_dependencies will be gone
                # cls.connection.load_headings(dbname=cls.dbname, force=True) #TODO: reimplement because load_headings is gone
                logger.info("Dropped table %s" % cls.full_table_name)

    def size_on_disk(self):
        """
        :return: size of data and indices in MiB taken by the table on the storage device
        """
        ret = self.connection.query(
            'SHOW TABLE STATUS FROM `(database}` WHERE NAME="{table}"'.format(
                database=self.database, table=self.table_name), as_dict=True
        ).fetchone()
        return (ret['Data_length'] + ret['Index_length'])/1024**2

    def set_table_comment(self, comment):
        """
        Update the table comment in the table definition.
        :param comment: new comment as string
        """
        self._alter('COMMENT="%s"' % comment)

    def add_attribute(self, definition, after=None):
        """
        Add a new attribute to the table. A full line from the table definition
        is passed in as definition.

        The definition can specify where to place the new attribute. Use after=None
        to add the attribute as the first attribute or after='attribute' to place it
        after an existing attribute.

        :param definition: table definition
        :param after=None: After which attribute of the table the new attribute is inserted.
                           If None, the attribute is inserted in front.
        """
        position = ' FIRST' if after is None else (
            ' AFTER %s' % after if after else '')
        sql = compile_attribute(definition)[1]
        self._alter('ADD COLUMN %s%s' % (sql, position))

    def drop_attribute(self, attribute_name):
        """
        Drops the attribute attrName from this table.
        :param attribute_name: Name of the attribute that is dropped.
        """
        if not config['safemode'] or user_choice(
                "You are about to drop an attribute from a table."
                "This operation cannot be undone.\n"
                "Proceed?", default='no') == 'yes':
            self._alter('DROP COLUMN `%s`' % attribute_name)

    def alter_attribute(self, attribute_name, definition):
        """
        Alter the definition of the field attr_name in this table using the new definition.

        :param attribute_name: field that is redefined
        :param definition: new definition of the field
        """
        sql = compile_attribute(definition)[1]
        self._alter('CHANGE COLUMN `%s` %s' % (attribute_name, sql))

    def erd(self, subset=None):
        """
        Plot the schema's entity relationship diagram (ERD).
        """
        NotImplemented

    def _alter(self, alter_statement):
        """
        Execute ALTER TABLE statement for this table. The schema
        will be reloaded within the connection object.

        :param alter_statement: alter statement
        """
        if self.connection.in_transaction:
            raise DataJointError("Table definition cannot be altered during a transaction.")
        sql = 'ALTER TABLE %s %s' % (self.full_table_name, alter_statement)
        self.connection.query(sql)
        self._table_info.heading = None
