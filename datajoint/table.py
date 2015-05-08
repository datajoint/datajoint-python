import numpy as np
import logging
from . import DataJointError
from .relational import Relation
from .blob import pack
from .heading import Heading
import re
from .settings import Role, role_to_prefix
from .utils import from_camel_case

logger = logging.getLogger(__name__)


class Table(Relation):
    """
    A Table object is a relation associated with a table.
    A Table object provides insert and delete methods.
    Table objects are only used internally and for debugging.
    The table must already exist in the schema for its Table object to work.

    The table associated with an instance of Base is identified by its 'class name'.
    property, which is a string in CamelCase. The actual table name is obtained
    by converting className from CamelCase to underscore_separated_words and
    prefixing according to the table's role.

    Base instances obtain their table's heading by looking it up in the connection
    object. This ensures that Base instances contain the current table definition
    even after tables are modified after the instance is created.
    """

    def __init__(self, conn=None, dbname=None, class_name=None, definition=None):
        self.class_name = class_name
        self.conn = conn
        self.dbname = dbname
        self.definition = definition

        if dbname not in self.conn.db_to_mod:
            # register with a fake module, enclosed in back quotes
            # necessary for loading mechanism
            self.conn.bind('`{0}`'.format(dbname), dbname)


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
                    'Table could not be declared for %s' % self.class_name)

    @staticmethod
    def _field_to_sql(field): #TODO move this into Attribute Tuple
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
                # enclose value in quotes (even numeric), except special SQL values
                # or values already enclosed by the user
                if field.default.upper() in mysql_constants or field.default[:1] in ["'", '"']:
                    default = '%s DEFAULT %s' % (default, field.default)
                else:
                    default = '%s DEFAULT "%s"' % (default, field.default)

        if any((c in r'\"' for c in field.comment)):
            raise DataJointError('Illegal characters in attribute comment "%s"' % field.comment)

        return '`{name}` {type} {default} COMMENT "{comment}",\n'.format(
            name=field.name, type=field.type, default=default, comment=field.comment)

    @property
    def sql(self):
        return self.full_table_name

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
        self.iter_insert(data.__iter__())

    def insert(self, tup, ignore_errors=False, replace=False):  # TODO: in progress (issue #8)
        """
        Insert one data tuple, one data record, or one dictionary.

        :param tup: Data tuple, record, or dictionary.
        :param ignore_errors=False: Ignores errors if True.
        :param replace=False: Replaces data tuple if True.

        Example::

            b = djtest.Subject()
            b.insert(dict(subject_id = 7, species="mouse",\\
                           real_id = 1007, date_of_birth = "2014-09-01"))
        """

        if isinstance(tup, tuple) or isinstance(tup, list) or isinstance(tup, np.ndarray):
            value_list = ','.join([repr(val) if not name in self.heading.blobs else '%s'
                                    for name, val in zip(self.heading.names, tup)])
            args = tuple(pack(val) for name, val in zip(self.heading.names, tup) if name in self.heading.blobs)
            attribute_list = '`' + '`,`'.join(self.heading.names[0:len(tup)]) + '`'

        elif isinstance(tup, dict):
            value_list = ','.join([repr(tup[name]) if not name in self.heading.blobs else '%s'
                                    for name in self.heading.names if name in tup])
            args = tuple(pack(tup[name]) for name in self.heading.names
                                if (name in tup and name in self.heading.blobs) )
            attribute_list = '`' + '`,`'.join([name for name in self.heading.names if name in tup]) + '`'
        elif isinstance(tup, np.void):
            value_list = ','.join([repr(tup[name]) if not name in self.heading.blobs else '%s'
                                    for name in self.heading.names if name in tup.dtype.fields])

            args = tuple(pack(tup[name]) for name in self.heading.names
                                if (name in tup.dtype.fields and name in self.heading.blobs) )
            attribute_list = '`' + '`,`'.join([q for q in self.heading.names if q in tup.dtype.fields]) + '`'
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

    def delete(self):  # TODO: (issues #14 and #15)
        pass

    def drop(self):
        """
        Drops the table associated to this object.
        """
        # TODO: make cascading (issue #16)
        self.conn.query('DROP TABLE %s' % self.full_table_name)
        self.conn.clear_dependencies(dbname=self.dbname)
        self.conn.load_headings(dbname=self.dbname, force=True)
        logger.debug("Dropped table %s" % self.full_table_name)

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
        self._alter('DROP COLUMN `%s`' % attr_name)

    def alter_attribute(self, attr_name, new_definition):
        """
        Alter the definition of the field attr_name in this table using the new definition.

        :param attr_name: field that is redefined
        :param new_definition: new definition of the field
        """
        sql = self.field_to_sql(parse_attribute_definition(new_definition))
        self._alter('CHANGE COLUMN `%s` %s' % (attr_name, sql[:-2]))

    def erd(self, subset=None, prog='dot'):
        """
        Plot the schema's entity relationship diagram (ERD).
        The layout programs can be 'dot' (default), 'neato', 'fdp', 'sfdp', 'circo', 'twopi'
        """
        if not subset:
            g = self.graph
        else:
            g = self.graph.copy()
        # todo: make erd work (github issue #7)
        """
         g = self.graph
         else:
         g = self.graph.copy()
         for i in g.nodes():
         if i not in subset:
         g.remove_node(i)
        def tablelist(tier):
        return [i for i in g if self.tables[i].tier==tier]

        pos=nx.graphviz_layout(g,prog=prog,args='')
        plt.figure(figsize=(8,8))
        nx.draw_networkx_edges(g, pos, alpha=0.3)
        nx.draw_networkx_nodes(g, pos, nodelist=tablelist('manual'),
        node_color='g', node_size=200, alpha=0.3)
        nx.draw_networkx_nodes(g, pos, nodelist=tablelist('computed'),
        node_color='r', node_size=200, alpha=0.3)
        nx.draw_networkx_nodes(g, pos, nodelist=tablelist('imported'),
        node_color='b', node_size=200, alpha=0.3)
        nx.draw_networkx_nodes(g, pos, nodelist=tablelist('lookup'),
        node_color='gray', node_size=120, alpha=0.3)
        nx.draw_networkx_labels(g, pos, nodelist = subset, font_weight='bold', font_size=9)
        nx.draw(g,pos,alpha=0,with_labels=false)
        plt.show()
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
        index_ptrn = """
        ^(?P<unique>UNIQUE)?\s*INDEX\s*      # [UNIQUE] INDEX
        \((?P<attributes>[^\)]+)\)$          # (attr1, attr2)
        """
        indexP = re.compile(index_ptrn, re.I + re.X)
        m = indexP.match(line)
        assert m, 'Invalid index declaration "%s"' % line
        index_info = m.groupdict()
        attributes = re.split(r'\s*,\s*', index_info['attributes'].strip())
        index_info['attributes'] = attributes
        assert len(attributes) == len(set(attributes)), \
            'Duplicate attributes in index declaration "%s"' % line
        return index_info

    def _parse_attr_def(self, line, in_key=False):
        """
        Parse attribute definition line in the declaration and returns
        an attribute tuple.

        :param line: attribution line
        :param in_key: set to True if attribute is in primary key set
        :returns: attribute tuple
        """
        line = line.strip()
        attr_ptrn = """
        ^(?P<name>[a-z][a-z\d_]*)\s*             # field name
        (=\s*(?P<default>\S+(\s+\S+)*?)\s*)?     # default value
        :\s*(?P<type>\w[^\#]*[^\#\s])\s*         # datatype
        (\#\s*(?P<comment>\S*(\s+\S+)*)\s*)?$    # comment
        """

        attrP = re.compile(attr_ptrn, re.I + re.X)
        m = attrP.match(line)
        assert m, 'Invalid field declaration "%s"' % line
        attr_info = m.groupdict()
        if not attr_info['comment']:
            attr_info['comment'] = ''
        if not attr_info['default']:
            attr_info['default'] = ''
        attr_info['nullable'] = attr_info['default'].lower() == 'null'
        assert (not re.match(r'^bigint', attr_info['type'], re.I) or not attr_info['nullable']), \
            'BIGINT attributes cannot be nullable in "%s"' % line

        attr_info['in_key'] = in_key
        attr_info['autoincrement'] = None
        attr_info['numeric'] = None
        attr_info['string'] = None
        attr_info['is_blob'] = None
        attr_info['computation'] = None
        attr_info['dtype'] = None

        return Heading.AttrTuple(**attr_info)

    def get_base(self, module_name, class_name):
        return None

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
        if self._use_package:
            parent = self.__module__.split('.')[-2]
        else:
            parent = self.__module__.split('.')[-1]
        expected_name = parent + '.' + self.class_name
        if not defined_name == expected_name:
            raise DataJointError('Table name {} does not match the declared'
                                 'name {}'.format(expected_name, defined_name))

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
            keys = (x for x in r.heading.attrs.values() if x.in_key)
            for field in keys:
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
        ^(?P<module>\w+)\.(?P<className>\w+)\s*     #  module.className
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
        field_ptrn = """
        ^[a-z][a-z\d_]*\s*          # name
        (=\s*\S+(\s+\S+)*\s*)?      # optional defaults
        :\s*\w.*$                   # type, comment
        """
        fieldP = re.compile(field_ptrn, re.I + re.X)  # ignore case and verbose

        for line in declaration[1:]:
            if line.startswith('---'):
                in_key = False  # start parsing non-PK fields
            elif line.startswith('->'):
                # foreign key
                module_name, class_name = line[2:].strip().split('.')
                rel = self.get_base(module_name, class_name)
                (parents if in_key else referenced).append(rel)
            elif re.match(r'^(unique\s+)?index[^:]*$', line):
                index_defs.append(self._parse_index_def(line))
            elif fieldP.match(line):
                field_defs.append(self._parse_attr_def(line, in_key))
            else:
                raise DataJointError(
                    'Invalid table declaration line "%s"' % line)

        return table_info, parents, referenced, field_defs, index_defs
