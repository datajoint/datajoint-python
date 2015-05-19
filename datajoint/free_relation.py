from _collections_abc import MutableMapping, Mapping
import numpy as np
import logging
from . import DataJointError, config
from .relational_operand import RelationalOperand
from .blob import pack
from .heading import Heading
import re
from .settings import Role, role_to_prefix
from .utils import from_camel_case, user_choice

logger = logging.getLogger(__name__)


class FreeRelation(RelationalOperand):
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

    def __init__(self, conn, dbname, class_name=None, definition=None):
        self.class_name = class_name
        self._conn = conn
        self.dbname = dbname
        self._definition = definition

        if dbname not in self.conn.db_to_mod:
            # register with a fake module, enclosed in back quotes
            # necessary for loading mechanism
            self.conn.bind('`{0}`'.format(dbname), dbname)
        super().__init__(conn)

    @property
    def from_clause(self):
        return self.full_table_name

    @property
    def heading(self):
        self.declare()
        return self.conn.headings[self.dbname][self.table_name]

    @property
    def definition(self):
        return self._definition

    @property
    def is_declared(self):
        self.conn.load_headings(self.dbname)
        return self.class_name in self.conn.table_names[self.dbname]

    def declare(self):
        """
        Declare the table in database if it doesn't already exist.

        :raises: DataJointError if the table cannot be declared.
        """
        if not self.is_declared:
            self._declare()
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
        return self.conn.table_names[self.dbname][self.class_name] if self.is_declared else None

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
        self.conn.query(sql, args=args)

    def delete(self):
        if not config['safemode'] or user_choice(
                "You are about to delete data from a table. This operation cannot be undone.\n"
                "Proceed?", 'no') == 'yes':
            self.conn.query('DELETE FROM ' + self.from_clause + self.where_clause)  # TODO: make cascading (issue #15)

    def drop(self):
        """
        Drops the table associated to this object.
        """
        if self.is_declared:
            if not config['safemode'] or user_choice(
                    "You are about to drop an entire table. This operation cannot be undone.\n"
                    "Proceed?", 'no') == 'yes':
                self.conn.query('DROP TABLE %s' % self.full_table_name)  # TODO: make cascading (issue #16)
                self.conn.clear_dependencies(dbname=self.dbname)
                self.conn.load_headings(dbname=self.dbname, force=True)
                logger.info("Dropped table %s" % self.full_table_name)

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
                "Proceed?", 'no') == 'yes':
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
        sql = 'ALTER TABLE %s %s' % (self.full_table_name, alter_statement)
        self.conn.query(sql)
        self.conn.load_headings(self.dbname, force=True)
        # TODO: place table definition sync mechanism

    def _parse_index_def(self, line):
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

    def get_base(self, module_name, class_name):
        if not module_name:
            module_name = r'`{dbname}`'.format(self.dbname)
        m = re.match(r'`(\w+)`', module_name)
        return FreeRelation(self.conn, m.group(1), class_name) if m else None

    @property
    def ref_name(self):
        """
        :return: the name to refer to this class, taking form module.class or `database`.class
        """
        return '`{0}`'.format(self.dbname) + '.' + self.class_name

    def _declare(self):
        """
        Declares the table in the database if no table in the database matches this object.
        """
        if not self.definition:
            raise DataJointError('Table definition is missing!')
        table_info, parents, referenced, field_defs, index_defs = self._parse_declaration()
        defined_name = table_info['module'] + '.' + table_info['className']

        if not defined_name == self.ref_name:
            raise DataJointError('Table name {} does not match the declared'
                                 'name {}'.format(self.ref_name, defined_name))

        # compile the CREATE TABLE statement
        # TODO: support prefix
        table_name = role_to_prefix[table_info['tier']] + from_camel_case(self.class_name)
        sql = 'CREATE TABLE `%s`.`%s` (\n' % (self.dbname, table_name)

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
                raise DataJointError('Primary key {} cannot be nullable'.format(
                    field.name))
            if field.name in primary_key_fields:
                raise DataJointError('Duplicate declaration of the primary key '
                                     '{key}. Check to make sure that the key '
                                     'is not declared already in referenced '
                                     'tables'.format(key=field.name))
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
        self.conn.load_headings(self.dbname, force=True)
        if not self.is_declared:
            # execute declaration
            logger.debug('\n<SQL>\n' + sql + '</SQL>\n\n')
            self.conn.query(sql)
            self.conn.load_headings(self.dbname, force=True)

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
        ^(?P<module>[\w\`]+)\.(?P<className>\w+)\s*     #  module.className
        \(\s*(?P<tier>\w+)\s*\)\s*                  #  (tier)
        \#\s*(?P<comment>.*)$                       #  comment
        """
        p = re.compile(ptrn, re.X)
        table_info = p.match(declaration[0]).groupdict()
        if table_info['tier'] not in Role.__members__:
            raise DataJointError('InvalidTableTier: Invalid tier {tier} for table\
                                 {module}.{cls}'.format(tier=table_info['tier'],
                                                        module=table_info[
                                                            'module'],
                                                        cls=table_info['className']))
        table_info['tier'] = Role[table_info['tier']]  # convert into enum

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
                if '.' in line[2:]:
                    module_name, class_name = line[2:].strip().split('.')
                else:
                    # assume it's a shorthand
                    module_name = ''
                    class_name = line[2:].strip()
                ref = parents if in_key else referenced
                ref.append(self.get_base(module_name, class_name))
            elif re.match(r'^(unique\s+)?index[^:]*$', line, re.I):
                index_defs.append(self._parse_index_def(line))
            elif attribute_regexp.match(line):
                field_defs.append(parse_attribute_definition(line, in_key))
            else:
                raise DataJointError(
                    'Invalid table declaration line "%s"' % line)

        return table_info, parents, referenced, field_defs, index_defs


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
