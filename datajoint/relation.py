from collections import namedtuple
from collections.abc import MutableMapping, Mapping
import numpy as np
import logging
import re
import abc

from . import DataJointError, config, TransactionError
import pymysql
from datajoint import DataJointError, conn
from .relational_operand import RelationalOperand
from .blob import pack
from .utils import user_choice
from .parsing import parse_attribute_definition, field_to_sql, parse_index_definition
from .heading import Heading

logger = logging.getLogger(__name__)

SharedInfo = namedtuple(
    'SharedInfo',
    ('database', 'context', 'connection', 'heading', 'parents', 'children', 'references', 'referenced'))


class classproperty:
    def __init__(self, getf):
        self._getf = getf

    def __get__(self, instance, owner):
        return self._getf(owner)


def schema(database, context, connection=None):
    """
    Returns a schema decorator that can be used to associate a Relation class to a
    specific database with :param name:. Name reference to other tables in the table definition
    will be resolved by looking up the corresponding key entry in the passed in context dictionary.
    It is most common to set context equal to the return value of call to locals() in the module.
    For more details, please refer to the tutorial online.

    :param database: name of the database to associate the decorated class with
    :param context: dictionary used to resolve (any) name references within the table definition string
    :param connection: connection object to the database server. If ommited, will try to establish connection according to
    config values
    :return: a decorator function to be used on Relation derivative classes
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
        cls._shared_info = SharedInfo(
            database=database,
            context=context,
            connection=connection,
            heading=None,
        )
        cls.declare()
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

    __heading = None

    _shared_info = None

    def __init__(self):  # TODO: Think about it
        if self._shared_info is None:
            raise DataJointError('The class must define _shared_info')

    # ---------- abstract properties ------------ #
    @classproperty
    @abc.abstractmethod
    def table_name(cls):
        """
        :return: the name of the table in the database
        """
        pass

    @classproperty
    @abc.abstractmethod
    def database(cls):
        """
        :return: string containing the database name on the server
        """
        pass

    @classproperty
    @abc.abstractmethod
    def definition(cls):
        """
        :return: a string containing the table definition using the DataJoint DDL
        """
        pass

    @classproperty
    @abc.abstractmethod
    def context(cls):
        """
        :return: a dict with other relations that can be referenced by foreign keys
        """
        pass

    # --------- base relation functionality --------- #
    @classproperty
    def is_declared(cls):
        if cls.__heading is not None:
            return True
        cur = cls._shared_info.connection.query(
            'SHOW TABLE STATUS FROM `{database}` WHERE name="{table_name}"'.format(
                table_name=cls.table_name))
        return cur.rowcount == 1

    @classproperty
    def heading(cls):
        """
        Required by relational operand
        :return: a datajoint.Heading object
        """
        if cls.__heading is None:
            cls.__heading = Heading.init_from_database(cls.connection, cls.database, cls.table_name)
        return cls.__heading

    @classproperty
    def from_clause(cls):
        """
        Required by the Relational class, this property specifies the contents of the FROM clause 
        for the SQL SELECT statements.
        :return:
        """
        return '`%s`.`%s`' % (cls.database, cls.table_name)

    @classmethod
    def declare(cls):
        """
        Declare the table in database if it doesn't already exist.

        :raises: DataJointError if the table cannot be declared.
        """
        if not cls.is_declared:
            cls._declare()

    @classmethod
    def iter_insert(cls, rows, **kwargs):
        """
        Inserts an entire batch of entries. Additional keyword arguments are passed to insert.

        :param iter: Must be an iterator that generates a sequence of valid arguments for insert.
        """
        for row in rows:
            cls.insert(row, **kwargs)

    @classmethod
    def batch_insert(cls, data, **kwargs):
        """
        Inserts an entire batch of entries. Additional keyword arguments are passed to insert.

        :param data: must be iterable, each row must be a valid argument for insert
        """
        cls.iter_insert(data.__iter__(), **kwargs)

    @classproperty
    def full_table_name(cls):
        return r"`{0:s}`.`{1:s}`".format(cls.database, cls.table_name)

    @classmethod
    def insert(cls, tup, ignore_errors=False, replace=False):
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

        heading = cls.heading
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
        sql += " INTO %s (%s) VALUES (%s)" % (cls.full_table_name,
                                              attribute_list, value_list)
        logger.info(sql)
        cls.connection.query(sql, args=args)

    def delete(self):
        if not config['safemode'] or user_choice(
                "You are about to delete data from a table. This operation cannot be undone.\n"
                "Proceed?", default='no') == 'yes':
            self.connection.query('DELETE FROM ' + self.from_clause + self.where_clause)  # TODO: make cascading (issue #15)

    @classmethod
    def drop(cls):
        """
        Drops the table associated to this class.
        """
        if cls.is_declared:
            if not config['safemode'] or user_choice(
                    "You are about to drop an entire table. This operation cannot be undone.\n"
                    "Proceed?", default='no') == 'yes':
                cls.connection.query('DROP TABLE %s' % cls.full_table_name)  # TODO: make cascading (issue #16)
                # cls.connection.clear_dependencies(dbname=cls.dbname) #TODO: reimplement because clear_dependencies will be gone
                # cls.connection.load_headings(dbname=cls.dbname, force=True) #TODO: reimplement because load_headings is gone
                logger.info("Dropped table %s" % cls.full_table_name)

    @classproperty
    def size_on_disk(cls):
        """
        :return: size of data and indices in MiB taken by the table on the storage device
        """
        cur = cls.connection.query(
            'SHOW TABLE STATUS FROM `{dbname}` WHERE NAME="{table}"'.format(
                dbname=cls.dbname, table=cls.table_name), as_dict=True)
        ret = cur.fetchone()
        return (ret['Data_length'] + ret['Index_length'])/1024**2

    @classmethod
    def set_table_comment(cls, comment):
        """
        Update the table comment in the table definition.
        :param comment: new comment as string
        """
        cls.alter('COMMENT="%s"' % comment)

    @classmethod
    def add_attribute(cls, definition, after=None):
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
        sql = field_to_sql(parse_attribute_definition(definition))
        cls._alter('ADD COLUMN %s%s' % (sql[:-2], position))

    @classmethod
    def drop_attribute(cls, attr_name):
        """
        Drops the attribute attrName from this table.

        :param attr_name: Name of the attribute that is dropped.
        """
        if not config['safemode'] or user_choice(
                "You are about to drop an attribute from a table."
                "This operation cannot be undone.\n"
                "Proceed?", default='no') == 'yes':
            cls._alter('DROP COLUMN `%s`' % attr_name)

    @classmethod
    def alter_attribute(cls, attr_name, new_definition):
        """
        Alter the definition of the field attr_name in this table using the new definition.

        :param attr_name: field that is redefined
        :param new_definition: new definition of the field
        """
        sql = field_to_sql(parse_attribute_definition(new_definition))
        cls._alter('CHANGE COLUMN `%s` %s' % (attr_name, sql[:-2]))

    @classmethod
    def erd(cls, subset=None):
        """
        Plot the schema's entity relationship diagram (ERD).
        """

    @classmethod
    def _alter(cls, alter_statement):
        """
        Execute ALTER TABLE statement for this table. The schema
        will be reloaded within the connection object.

        :param alter_statement: alter statement
        """
        if cls.connection.in_transaction:
            raise DataJointError(
                u"_alter is currently in transaction. Operation not allowed to avoid implicit commits.")

        sql = 'ALTER TABLE %s %s' % (cls.full_table_name, alter_statement)
        cls.connection.query(sql)
        cls.connection.load_headings(cls.dbname, force=True)
        # TODO: place table definition sync mechanism

    @classmethod
    def _declare(cls):
        """
        Declares the table in the database if no table in the database matches this object.
        """
        if cls.connection.in_transaction:
            raise DataJointError(
                u"_declare is currently in transaction. Operation not allowed to avoid implicit commits.")

        if not cls.definition:  # if empty definition was supplied
            raise DataJointError('Table definition is missing!')
        table_info, parents, referenced, field_defs, index_defs = cls._parse_declaration()

        sql = 'CREATE TABLE %s (\n' % cls.full_table_name

        # add inherited primary key fields
        primary_key_fields = set()
        non_key_fields = set()
        for p in parents:
            for key in p.primary_key:
                field = p.heading[key]
                if field.name not in primary_key_fields:
                    primary_key_fields.add(field.name)
                    sql += field_to_sql(field)
                else:
                    logger.debug('Field definition of {} in {} ignored'.format(
                        field.name, p.full_class_name))

        # add newly defined primary key fields
        for field in (f for f in field_defs if f.in_key):
            if field.nullable:
                raise DataJointError('Primary key attribute {} cannot be nullable'.format(
                    field.name))
            if field.name in primary_key_fields:
                raise DataJointError('Duplicate declaration of the primary attribute {key}. '
                                     'Ensure that the attribute is not already declared '
                                     'in referenced tables'.format(key=field.name))
            primary_key_fields.add(field.name)
            sql += field_to_sql(field)

        # add secondary foreign key attributes
        for r in referenced:
            for key in r.primary_key:
                field = r.heading[key]
                if field.name not in primary_key_fields | non_key_fields:
                    non_key_fields.add(field.name)
                    sql += field_to_sql(field)

        # add dependent attributes
        for field in (f for f in field_defs if not f.in_key):
            non_key_fields.add(field.name)
            sql += field_to_sql(field)

        # add primary key declaration
        assert len(primary_key_fields) > 0, 'table must have a primary key'
        keys = ', '.join(primary_key_fields)
        sql += 'PRIMARY KEY (%s),\n' % keys

        # add foreign key declarations
        for ref in parents + referenced:
            keys = ', '.join(ref.primary_key)
            sql += 'FOREIGN KEY (%s) REFERENCES %s (%s) ON UPDATE CASCADE ON DELETE RESTRICT,\n' % \
                   (keys, ref.full_table_name, keys)

        # add secondary index declarations
        # gather implicit indexes due to foreign keys first
        implicit_indices = []
        for fk_source in parents + referenced:
            implicit_indices.append(fk_source.primary_key)

        # for index in indexDefs:
        # TODO: finish this up...

        # close the declaration
        sql = '%s\n) ENGINE = InnoDB, COMMENT "%s"' % (
            sql[:-2], table_info['comment'])

        # # make sure that the table does not alredy exist
        # cls.load_heading()
        # if not cls.is_declared:
        #     # execute declaration
        #     logger.debug('\n<SQL>\n' + sql + '</SQL>\n\n')
        #     cls.connection.query(sql)
        #     cls.load_heading()

    @classmethod
    def _parse_declaration(cls):
        """
        Parse declaration and create new SQL table accordingly.
        """
        parents = []
        referenced = []
        index_defs = []
        field_defs = []
        declaration = re.split(r'\s*\n\s*', cls.definition.strip())

        # remove comment lines
        declaration = [x for x in declaration if not x.startswith('#')]
        ptrn = """
        \#\s*(?P<comment>.*)$                       #  comment
        """
        p = re.compile(ptrn, re.X)
        table_info = p.search(declaration[0]).groupdict()

        #table_info['tier'] = Role[table_info['tier']]  # convert into enum

        in_key = True  # parse primary keys
        attribute_regexp = re.compile("""
        ^[a-z][a-z\d_]*\s*          # name
        (=\s*\S+(\s+\S+)*\s*)?      # optional defaults
        :\s*\w.*$                   # type, comment
        """, re.I + re.X)  # ignore case and verbose

        for line in declaration[1:]:
            if line.startswith('---'):
                in_key = False  # start parsing non-PK fields
            elif line.startswith('->'):
                # foreign key
                ref_name = line[2:].strip()
                ref_list = parents if in_key else referenced
                ref_list.append(cls.lookup_name(ref_name))
            elif re.match(r'^(unique\s+)?index[^:]*$', line, re.I):
                index_defs.append(parse_index_definition(line))
            elif attribute_regexp.match(line):
                field_defs.append(parse_attribute_definition(line, in_key))
            else:
                raise DataJointError(
                    'Invalid table declaration line "%s"' % line)

        return table_info, parents, referenced, field_defs, index_defs

    @classmethod
    def lookup_name(cls, name):
        """
        Lookup the referenced name in the context dictionary

        e.g. for reference `common.Animals`, it will first check if `context` dictionary contains key
        `common`. If found, it then checks for attribute `Animals` in `common`, and returns the result.
        """
        parts = name.strip().split('.')
        try:
            ref = cls.context.get(parts[0])
            for attr in parts[1:]:
                ref = getattr(ref, attr)
        except (KeyError, AttributeError):
            raise DataJointError(
                'Foreign key reference to %s could not be resolved.'
                'Please make sure the name exists'
                'in the context of the class' % name)
        return ref

    @classproperty
    def connection(cls):
        """
        Returns the connection object of the class

        :return: the connection object
        """
        return cls.connection

    @classproperty
    def database(cls):
        return cls._shared_info.database

    @classproperty
    def context(cls):
        return cls._shared_info.context
