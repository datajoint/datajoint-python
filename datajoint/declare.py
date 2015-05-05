import re
import logging
from .heading import Heading
from . import DataJointError
from .utils import from_camel_case
from .settings import Role, role_to_prefix

mysql_constants = ['CURRENT_TIMESTAMP']

logger = logging.getLogger(__name__)


def declare(conn, definition, class_name):
    """
    Declares the table in the data base if no table in the database matches this object.
    """
    table_info, parents, referenced, field_definitions, index_definitions = _parse_declaration(conn, definition)
    defined_name = table_info['module'] + '.' + table_info['className']
    # TODO: clean up this mess... currently just ignoring the name used to define the table
    #if not defined_name == class_name:
    #    raise DataJointError('Table name {} does not match the declared'
    #                         'name {}'.format(class_name, defined_name))

    # compile the CREATE TABLE statement
    table_name = role_to_prefix[table_info['tier']] + from_camel_case(defined_name)
    sql = 'CREATE TABLE `%s`.`%s` (\n' % (self.dbname, table_name)

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
    for field in (f for f in field_definitions if f.isKey):
        if field.nullable:
            raise DataJointError('Primary key {} cannot be nullable'.format(
                field.name))
        if field.name in primary_key_fields:
            raise DataJointError('Duplicate declaration of the primary key '
                                 '{key}. Check to make sure that the key '
                                 'is not declared already in referenced '
                                 'tables'.format(key=field.name))
        primary_key_fields.add(field.name)
        sql += field_to_sql(field)

    # add secondary foreign key attributes
    for r in referenced:
        keys = (x for x in r.heading.attrs.values() if x.isKey)
        for field in keys:
            if field.name not in primary_key_fields | non_key_fields:
                non_key_fields.add(field.name)
                sql += field_to_sql(field)

    # add dependent attributes
    for field in (f for f in field_definitions if not f.isKey):
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

    # for index in index_definitions:
    # TODO: finish this up...

    # close the declaration
    sql = '%s\n) ENGINE = InnoDB, COMMENT "%s"' % (
        sql[:-2], table_info['comment'])

    # make sure that the table does not alredy exist
    # TODO: there will be a problem with resolving the module here...
    conn.load_headings(self.dbname, force=True)
    if not self.is_declared:
        # execute declaration
        logger.debug('\n<SQL>\n' + sql + '</SQL>\n\n')
        self.conn.query(sql)
        self.conn.load_headings(self.dbname, force=True)


def _parse_declaration(conn, definition):
    """
    Parse declaration and create new SQL table accordingly.
    """
    parents = []
    referenced = []
    index_defs = []
    field_defs = []
    declaration = re.split(r'\s*\n\s*', definition.strip())

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
            index_defs.append(parse_index_defnition(line))
        elif fieldP.match(line):
            field_defs.append(parse_attribute_definition(line, in_key))
        else:
            raise DataJointError(
                'Invalid table declaration line "%s"' % line)

    return table_info, parents, referenced, field_defs, index_defs


def field_to_sql(field):
    """
    Converts an attribute definition tuple into SQL code.
    :param field: attribute definition
    :rtype : SQL code
    """
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

    # TODO: escape instead! - same goes for Matlab side implementation
    assert not any((c in r'\"' for c in field.comment)), \
        'Illegal characters in attribute comment "%s"' % field.comment

    return '`{name}` {type} {default} COMMENT "{comment}",\n'.format(
        name=field.name, type=field.type, default=default, comment=field.comment)


def parse_attribute_definition(line, in_key=False):  # todo add docu for in_key
    """
    Parse attribute definition line in the declaration and returns
    an attribute tuple.
    :param line: attribution line
    :param in_key:
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


def parse_index_definition(line):    # why is this a method of Base instead of a local function?
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
