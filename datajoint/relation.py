from _collections_abc import MutableMapping, Mapping
import numpy as np
import logging
from . import DataJointError, config, TransactionError
from .relational_operand import RelationalOperand
from .blob import pack
from .heading import Heading
import re
from .settings import Role, role_to_prefix
from .utils import from_camel_case, user_choice
from .connection import  conn
import abc

logger = logging.getLogger(__name__)


class Relation(RelationalOperand):
    """
    A FreeRelation object is a relation associated with a table.
    A FreeRelation object provides insert and delete methods.
    FreeRelation objects are only used internally and for debugging.
    The table must already exist in the schema for its FreeRelation object to work.

    The table associated with an instance of Relation is identified by its 'class name'.
    property, which is a string in CamelCase. The actual table name is obtained
    by converting className from CamelCase to underscore_separated_words and
    prefixing according to the table's role.

    Relation instances obtain their table's heading by looking it up in the connection
    object. This ensures that Relation instances contain the current table definition
    even after tables are modified after the instance is created.
    """

    # defines class properties


    def __init__(self, table_name, schema_name=None, connection=None, definition=None, context=None):
        self._table_name = table_name
        self._schema_name = schema_name
        if connection is None:
            connection = conn()
        self._connection = connection
        self._definition = definition
        if context is None:
            context = {}
        self._context = context
        self._heading = None

    @property
    def schema_name(self):
        return self._schema_name

    @property
    def connection(self):
        return self._connection

    @property
    def definition(self):
        return self._definition

    @property
    def context(self):
        return self._context

    @property
    def heading(self):
        return self._heading

    @heading.setter
    def heading(self, new_heading):
        self._heading = new_heading

    @property
    def table_prefix(self):
        return ''

    @property
    def table_name(self):
        """
        TODO: allow table kind to be specified
        :return: name of the table. This is equal to table_prefix + class name with underscores
        """
        return self._table_name

    @property
    def definition(self):
        return self._definition


    # ============================== Shared implementations ==============================

    @property
    def full_table_name(self):
        """
        :return: full name of the associated table
        """
        return '`%s`.`%s`' % (self.schema_name, self.table_name)

    @property
    def from_clause(self):
        return self.full_table_name

    # TODO: consider if this should be a class method for derived classes
    def load_heading(self, forced=False):
        """
        Load the heading information for this table. If the table does not exist in the database server, Heading will be
        set to None if the table is not yet defined in the database.
        """
        pass
        # TODO: I want to be able to tell whether load_heading has already been attempted in the past... `self.heading is None` is not informative
        # TODO: make sure to assign new heading to self.heading, not to self._heading or any other direct variables

    @property
    def is_declared(self):
        #TODO: this implementation is rather expensive and stupid
        # - if table is not declared yet, repeated call to this method causes loading attempt each time

        if self.heading is None:
            self.load_heading()
        return self.heading is not None


    def declare(self):
        """
        Declare the table in database if it doesn't already exist.

        :raises: DataJointError if the table cannot be declared.
        """
        if not self.is_declared:
            self._declare()
            # verify that declaration completed successfully
            if not self.is_declared:
                raise DataJointError(
                    'FreeRelation could not be declared for %s' % self.class_name)

    @staticmethod
    def _field_to_sql(field):  # TODO move this into Attribute Tuple
        """
        Converts an attribute definition tuple into SQL code.
        :param field: attribute definition
        :rtype : SQL code
        """
        mysql_constants = ['CURRENT_TIMESTAMP']
        if field.nullable:
            default = 'DEFAULT NULL'
        else:
            default = 'NOT NULL'
            # if some default specified
            if field.default:
                # enclose value in quotes except special SQL values or already enclosed
                quote = field.default.upper() not in mysql_constants and field.default[0] not in '"\''
                default += ' DEFAULT ' + ('"%s"' if quote else "%s") % field.default
        if any((c in r'\"' for c in field.comment)):
            raise DataJointError('Illegal characters in attribute comment "%s"' % field.comment)

        return '`{name}` {type} {default} COMMENT "{comment}",\n'.format(
            name=field.name, type=field.type, default=default, comment=field.comment)



    @property
    def primary_key(self):
        """
        :return: primary key of the table
        """
        return self.heading.primary_key

    def iter_insert(self, iter, **kwargs):
        """
        Inserts an entire batch of entries. Additional keyword arguments are passed to insert.

        :param iter: Must be an iterator that generates a sequence of valid arguments for insert.
        """
        for row in iter:
            self.insert(row, **kwargs)

    def batch_insert(self, data, **kwargs):
        """
        Inserts an entire batch of entries. Additional keyword arguments are passed to insert.

        :param data: must be iterable, each row must be a valid argument for insert
        """
        self.iter_insert(data.__iter__(), **kwargs)

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
        sql += " INTO %s (%s) VALUES (%s)" % (self.full_table_name,
                                              attribute_list, value_list)
        logger.info(sql)
        self.connection.query(sql, args=args)

    def delete(self):
        if not config['safemode'] or user_choice(
                "You are about to delete data from a table. This operation cannot be undone.\n"
                "Proceed?", default='no') == 'yes':
            self.connection.query('DELETE FROM ' + self.from_clause + self.where_clause)  # TODO: make cascading (issue #15)

    def drop(self):
        """
        Drops the table associated to this object.
        """
        if self.is_declared:
            if not config['safemode'] or user_choice(
                    "You are about to drop an entire table. This operation cannot be undone.\n"
                    "Proceed?", default='no') == 'yes':
                self.connection.query('DROP TABLE %s' % self.full_table_name)  # TODO: make cascading (issue #16)
                self.connection.clear_dependencies(dbname=self.dbname)
                self.connection.load_headings(dbname=self.dbname, force=True)
                logger.info("Dropped table %s" % self.full_table_name)

    @property
    def size_on_disk(self):
        """
        :return: size of data and indices in MiB taken by the table on the storage device
        """
        cur = self.connection.query(
            'SHOW TABLE STATUS FROM `{dbname}` WHERE NAME="{table}"'.format(
                dbname=self.dbname, table=self.table_name), as_dict=True)
        ret = cur.fetchone()
        return (ret['Data_length'] + ret['Index_length'])/1024**2

    def set_table_comment(self, comment):
        """
        Update the table comment in the table definition.
        :param comment: new comment as string
        """
        # TODO: add verification procedure (github issue #24)
        self.alter('COMMENT="%s"' % comment)

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
        sql = self.field_to_sql(parse_attribute_definition(definition))
        self._alter('ADD COLUMN %s%s' % (sql[:-2], position))

    def drop_attribute(self, attr_name):
        """
        Drops the attribute attrName from this table.

        :param attr_name: Name of the attribute that is dropped.
        """
        if not config['safemode'] or user_choice(
                "You are about to drop an attribute from a table."
                "This operation cannot be undone.\n"
                "Proceed?", default='no') == 'yes':
            self._alter('DROP COLUMN `%s`' % attr_name)

    def alter_attribute(self, attr_name, new_definition):
        """
        Alter the definition of the field attr_name in this table using the new definition.

        :param attr_name: field that is redefined
        :param new_definition: new definition of the field
        """
        sql = self.field_to_sql(parse_attribute_definition(new_definition))
        self._alter('CHANGE COLUMN `%s` %s' % (attr_name, sql[:-2]))

    def erd(self, subset=None):
        """
        Plot the schema's entity relationship diagram (ERD).
        """

    def _alter(self, alter_statement):
        """
        Execute ALTER TABLE statement for this table. The schema
        will be reloaded within the connection object.

        :param alter_statement: alter statement
        """
        if self._conn.in_transaction:
            raise TransactionError(
                u"_alter is currently in transaction. Operation not allowed to avoid implicit commits.",
                        self._alter, args=(alter_statement,))

        sql = 'ALTER TABLE %s %s' % (self.full_table_name, alter_statement)
        self.connection.query(sql)
        self.connection.load_headings(self.dbname, force=True)
        # TODO: place table definition sync mechanism

    @staticmethod
    def _parse_index_def(line):
        """
        Parses index definition.

        :param line: definition line
        :return: groupdict with index info
        """
        line = line.strip()
        index_regexp = re.compile("""
        ^(?P<unique>UNIQUE)?\s*INDEX\s*      # [UNIQUE] INDEX
        \((?P<attributes>[^\)]+)\)$          # (attr1, attr2)
        """, re.I + re.X)
        m = index_regexp.match(line)
        assert m, 'Invalid index declaration "%s"' % line
        index_info = m.groupdict()
        attributes = re.split(r'\s*,\s*', index_info['attributes'].strip())
        index_info['attributes'] = attributes
        assert len(attributes) == len(set(attributes)), \
            'Duplicate attributes in index declaration "%s"' % line
        return index_info


    def _declare(self):
        """
        Declares the table in the database if no table in the database matches this object.
        """
        if self.connection.in_transaction:
            raise TransactionError(
                u"_declare is currently in transaction. Operation not allowed to avoid implicit commits.", self._declare)

        if not self.definition: # if empty definition was supplied
            raise DataJointError('Table definition is missing!')
        table_info, parents, referenced, field_defs, index_defs = self._parse_declaration()

        sql = 'CREATE TABLE %s (\n' % self.full_table_name

        # add inherited primary key fields
        primary_key_fields = set()
        non_key_fields = set()
        for p in parents:
            for key in p.primary_key:
                field = p.heading[key]
                if field.name not in primary_key_fields:
                    primary_key_fields.add(field.name)
                    sql += self._field_to_sql(field)
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
            sql += self._field_to_sql(field)

        # add secondary foreign key attributes
        for r in referenced:
            for key in r.primary_key:
                field = r.heading[key]
                if field.name not in primary_key_fields | non_key_fields:
                    non_key_fields.add(field.name)
                    sql += self._field_to_sql(field)

        # add dependent attributes
        for field in (f for f in field_defs if not f.in_key):
            non_key_fields.add(field.name)
            sql += self._field_to_sql(field)

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

        # make sure that the table does not alredy exist
        self.load_heading()
        if not self.is_declared:
            # execute declaration
            logger.debug('\n<SQL>\n' + sql + '</SQL>\n\n')
            self.connection.query(sql)
            self.load_heading()

    def _parse_declaration(self):
        """
        Parse declaration and create new SQL table accordingly.
        """
        parents = []
        referenced = []
        index_defs = []
        field_defs = []
        declaration = re.split(r'\s*\n\s*', self.definition.strip())

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
                ref_list.append(self.lookup_name(ref_name))
            elif re.match(r'^(unique\s+)?index[^:]*$', line, re.I):
                index_defs.append(self._parse_index_def(line))
            elif attribute_regexp.match(line):
                field_defs.append(parse_attribute_definition(line, in_key))
            else:
                raise DataJointError(
                    'Invalid table declaration line "%s"' % line)

        return table_info, parents, referenced, field_defs, index_defs

    def lookup_name(self, name):
        """
        Lookup the referenced name in the context dictionary

        e.g. for reference `common.Animals`, it will first check if `context` dictionary contains key
        `common`. If found, it then checks for attribute `Animals` in `common`, and returns the result.
        """
        parts = name.strip().split('.')
        try:
            ref = self.context.get(parts[0])
            for attr in parts[1:]:
                ref = getattr(ref, attr)
        except (KeyError, AttributeError):
            raise DataJointError('Foreign reference %s could not be resolved. Please make sure the name exists'
                                 'in the context of the class' % name)
        return ref

class ClassRelation(Relation, metaclass=abc.ABCMeta):
    """
    A relation object that is handled at class level. All instances of the derived classes
    share common connection and schema binding
    """

    _connection = None  # connection information
    _schema_name = None  # name of schema this relation belongs to
    _heading = None  # heading information for this relation
    _context = None    # name reference lookup context

    def __init__(self, schema_name=None, connection=None, context=None):
        """
        Use this constructor to specify class level
        """
        if schema_name is not None:
            self.schema_name = schema_name

        # TODO: Think about this implementation carefully
        if connection is not None:
            self.connection = connection
        elif self.connection is None:
            self.connection = conn()

        if context is not None:
            self.context = context
        elif self.context is None:
            self.context = {} # initialize with an empty dictionary

    @property
    def schema_name(self):
        return self.__class__._schema_name

    @schema_name.setter
    def schema_name(self, new_schema_name):
        if self.schema_name is not None:
            logger.warn('Overriding associated schema for class %s'
                        '- this will affect all existing instances!' % self.__class__.__name__)
        self.__class__._schema_name = new_schema_name

    @property
    def connection(self):
        return self.__class__._connection

    @connection.setter
    def connection(self, new_connection):
        if self.connection is not None:
            logger.warn('Overriding associated connection for class %s'
                        '- this will affect all existing instances!' % self.__class__.__name__)
        self.__class__._connection = new_connection

    @property
    def context(self):
        # TODO: should this be a copy or the original?
        return self.__class__._context.copy()

    @context.setter
    def context(self, new_context):
        if self.context is not None:
            logger.warn('Overriding associated reference context for class %s'
                        '- this will affect all existing instances!' % self.__class__.__name__)
        self.__class__._context = new_context

    @property
    def heading(self):
        return self.__class__._heading

    @heading.setter
    def heading(self, new_heading):
        self.__class__._heading = new_heading

    @abc.abstractproperty
    def definition(self):
        """
        Inheriting class must override this property with a valid table definition string
        """
        pass

    @abc.abstractproperty
    def table_prefix(self):
        pass


class ManualRelation(ClassRelation):
    @property
    def table_prefix(self):
        return ""


class AutoRelation(ClassRelation):
    pass


class ComputedRelation(AutoRelation):
    @property
    def table_prefix(self):
        return "_"





def parse_attribute_definition(line, in_key=False):
    """
    Parse attribute definition line in the declaration and returns
    an attribute tuple.

    :param line: attribution line
    :param in_key: set to True if attribute is in primary key set
    :returns: attribute tuple
    """
    line = line.strip()
    attribute_regexp = re.compile("""
    ^(?P<name>[a-z][a-z\d_]*)\s*             # field name
    (=\s*(?P<default>\S+(\s+\S+)*?)\s*)?     # default value
    :\s*(?P<type>\w[^\#]*[^\#\s])\s*         # datatype
    (\#\s*(?P<comment>\S*(\s+\S+)*)\s*)?$    # comment
    """, re.X)
    m = attribute_regexp.match(line)
    if not m:
        raise DataJointError('Invalid field declaration "%s"' % line)
    attr_info = m.groupdict()
    if not attr_info['comment']:
        attr_info['comment'] = ''
    if not attr_info['default']:
        attr_info['default'] = ''
    attr_info['nullable'] = attr_info['default'].lower() == 'null'
    assert (not re.match(r'^bigint', attr_info['type'], re.I) or not attr_info['nullable']), \
        'BIGINT attributes cannot be nullable in "%s"' % line

    return Heading.AttrTuple(
        in_key=in_key,
        autoincrement=None,
        numeric=None,
        string=None,
        is_blob=None,
        computation=None,
        dtype=None,
        **attr_info
    )
