import re
import pyparsing as pp
import logging


logger = logging.getLogger(__name__)


def compile_attribute(line, in_key=False):
    """
    Convert attribute definition from DataJoint format to SQL
    :param line: attribution line
    :param in_key: set to True if attribute is in primary key set
    :returns: (name, sql) -- attribute name and sql code for its declaration
    """
    quoted = pp.Or(pp.QuotedString('"'), pp.QuotedString("'"))
    colon = pp.Literal(':').suppress()
    attribute_name = pp.Word(pp.srange('[a-z]'), pp.srange('[a-z0-9_]')).setResultsName('name')

    data_type = pp.Combine(pp.Word(pp.alphas)+pp.SkipTo("#", ignore=quoted)).setResultsName('type')
    default = pp.Literal('=').suppress() + pp.SkipTo(colon, ignore=quoted).setResultsName('default')
    comment = pp.Literal('#').suppress() + pp.restOfLine.setResultsName('comment')

    attribute_parser = attribute_name + pp.Optional(default) + colon + data_type + comment

    match = attribute_parser.parseString(line+'#', parseAll=True)
    match['comment'] = match['comment'].rstrip('#')
    if 'default' not in match:
        match['default'] = ''
    match = {k: v.strip() for k, v in match.items()}
    match['nullable'] = match['default'].lower() == 'null'

    literals = ['CURRENT_TIMESTAMP']   # not to be enclosed in quotes
    assert not re.match(r'^bigint', match['type'], re.I) or not match['nullable'], \
        'BIGINT attributes cannot be nullable in "%s"' % line    # TODO: This was a MATLAB limitation. Handle this correctly.
    if match['nullable']:
        if in_key:
            raise DataJointError('Primary key attributes cannot be nullable in line %s' % line)
        match['default'] = 'DEFAULT NULL'  # nullable attributes default to null
    else:
        if match['default']:
            quote = match['default'].upper() not in literals and match['default'][0] not in '"\''
            match['default'] = ('NOT NULL DEFAULT ' +
                                ('"%s"' if quote else "%s") % match['default'])
        else:
            match['default'] = 'NOT NULL'
    match['comment'] = match['comment'].replace('"', '\\"')   # escape double quotes in comment
    sql = ('`{name}` {type} {default}' + (' COMMENT "{comment}"' if match['comment'] else '')
           ).format(**match)
    return match['name'], sql


def parse_index(line):
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


def parse_declaration(cls):
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
            ref_list.append(eval(ref_name, locals=cls.context))
        elif re.match(r'^(unique\s+)?index[^:]*$', line, re.I):
            index_defs.append(parse_index(line))
        elif attribute_regexp.match(line):
            field_defs.append(parse_attribute_definition(line, in_key))
        else:
            raise DataJointError('Invalid table declaration line "%s"' % line)

    return table_info, parents, referenced, field_defs, index_defs


def declare(full_table_name,  definition, context):
    """
    Declares the table in the database if it does not exist already
    """
    cur = relation.connection.query(
        'SHOW TABLE STATUS FROM `{database}` WHERE name="{table_name}"'.format(
            database=relation.database, table_name=relation.table_name))
    if cur.rowcount:
        return

    if relation.connection.in_transaction:
        raise DataJointError("Tables cannot be declared during a transaction.")

    if not relation.definition:
        raise DataJointError('Missing table definition.')

    table_info, parents, referenced, field_defs, index_defs = parse_declaration()

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
                logger.debug(
                    'Field definition of {} in {} ignored'.format(field.name, p.full_class_name))

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
    # TODO: add index declaration

    # close the declaration
    sql = '%s\n) ENGINE = InnoDB, COMMENT "%s"' % (
        sql[:-2], table_info['comment'])

    # # make sure that the table does not already exist
    # cls.load_heading()
    # if not cls.is_declared:
    #     # execute declaration
    #     logger.debug('\n<SQL>\n' + sql + '</SQL>\n\n')
    #     cls.connection.query(sql)
    #     cls.load_heading()

