"""
This module hosts functions to convert DataJoint table definitions into mysql table definitions, and to
declare the corresponding mysql tables.
"""
import re
import pyparsing as pp
import logging

from . import config
from .errors import DataJointError

STORE_NAME_LENGTH = 8
STORE_HASH_LENGTH = 43
HASH_DATA_TYPE = 'char(51)'

logger = logging.getLogger(__name__)


def build_foreign_key_parser():
    left = pp.Literal('(').suppress()
    right = pp.Literal(')').suppress()
    attribute_name = pp.Word(pp.srange('[a-z]'), pp.srange('[a-z0-9_]'))
    new_attrs = pp.Optional(left + pp.delimitedList(attribute_name) + right).setResultsName('new_attrs')
    arrow = pp.Literal('->').suppress()
    lbracket = pp.Literal('[').suppress()
    rbracket = pp.Literal(']').suppress()
    option = pp.Word(pp.srange('[a-zA-Z]'))
    options = pp.Optional(lbracket + pp.delimitedList(option) + rbracket)
    ref_table = pp.Word(pp.alphas, pp.alphanums + '._').setResultsName('ref_table')
    ref_attrs = pp.Optional(left + pp.delimitedList(attribute_name) + right).setResultsName('ref_attrs')
    return new_attrs + arrow + options + ref_table + ref_attrs


def build_attribute_parser():
    quoted = pp.Or(pp.QuotedString('"'), pp.QuotedString("'"))
    colon = pp.Literal(':').suppress()
    attribute_name = pp.Word(pp.srange('[a-z]'), pp.srange('[a-z0-9_]')).setResultsName('name')
    data_type = pp.Combine(pp.Word(pp.alphas) + pp.SkipTo("#", ignore=quoted)).setResultsName('type')
    default = pp.Literal('=').suppress() + pp.SkipTo(colon, ignore=quoted).setResultsName('default')
    comment = pp.Literal('#').suppress() + pp.restOfLine.setResultsName('comment')
    return attribute_name + pp.Optional(default) + colon + data_type + comment


foreign_key_parser = build_foreign_key_parser()
attribute_parser = build_attribute_parser()


def is_foreign_key(line):
    """
    :param line: a line from the table definition
    :return: true if the line appears to be a foreign key definition
    """
    arrow_position = line.find('->')
    return arrow_position >= 0 and not any(c in line[0:arrow_position] for c in '"#\'')


def compile_foreign_key(line, context, attributes, primary_key, attr_sql, foreign_key_sql):
    """
    :param line: a line from a table definition
    :param context: namespace containing referenced objects
    :param attributes: list of attribute names already in the declaration -- to be updated by this function
    :param primary_key: None if the current foreign key is made from the dependent section. Otherwise it is the list
        of primary key attributes thus far -- to be updated by the function
    :param attr_sql: a list of sql statements defining attributes -- to be updated by this function.
    :param foreign_key_sql: a list of sql statements specifying foreign key constraints -- to be updated by this function.
    """
    # Parse and validate 
    from .base_relation import BaseRelation
    try:
        result = foreign_key_parser.parseString(line)
    except pp.ParseException as err:
        raise DataJointError('Parsing error in line "%s". %s.' % (line, err))
    try:
        referenced_class = eval(result.ref_table, context)
    except NameError:
        raise DataJointError('Foreign key reference %s could not be resolved' % result.ref_table)
    if not issubclass(referenced_class, BaseRelation):
        raise DataJointError('Foreign key reference %s must be a subclass of UserRelation' % result.ref_table)
    ref = referenced_class()
    if not all(r in ref.primary_key for r in result.ref_attrs):
        raise DataJointError('Invalid foreign key attributes in "%s"' % line)
    try:
        raise DataJointError('Duplicate attributes "{attr}" in "{line}"'.format(
            attr=next(attr for attr in result.new_attrs if attr in attributes),
            line=line))
    except StopIteration:
        pass   # the normal outcome

    # Match the primary attributes of the referenced table to local attributes
    new_attrs = list(result.new_attrs)
    ref_attrs = list(result.ref_attrs)

    # special case, the renamed attribute is implicit
    if new_attrs and not ref_attrs:
        if len(new_attrs) != 1:
            raise DataJointError('Renamed foreign key must be mapped to the primary key in "%s"' % line)
        if len(ref.primary_key) == 1:
            # if the primary key has one attribute, allow implicit renaming
            ref_attrs = ref.primary_key
        else:
            # if only one primary key attribute remains, then allow implicit renaming
            ref_attrs = [attr for attr in ref.primary_key if attr not in attributes]
            if len(ref_attrs) != 1:
                raise DataJointError('Could not resovle which primary key attribute should be referenced in "%s"' % line)

    if len(new_attrs) != len(ref_attrs):
        raise DataJointError('Mismatched attributes in foreign key "%s"' % line)

    # expand the primary key of the referenced table
    lookup = dict(zip(ref_attrs, new_attrs)).get  # from foreign to local
    ref_attrs = [attr for attr in ref.primary_key if lookup(attr, attr) not in attributes]
    new_attrs = [lookup(attr, attr) for attr in ref_attrs]

    # sanity check
    assert len(new_attrs) == len(ref_attrs) and not any(attr in attributes for attr in new_attrs)

    # declare new foreign key attributes
    for ref_attr in ref_attrs:
        new_attr = lookup(ref_attr, ref_attr)
        attributes.append(new_attr)
        if primary_key is not None:
            primary_key.append(new_attr)
        attr_sql.append(ref.heading[ref_attr].sql.replace(ref_attr, new_attr, 1))

    # declare the foreign key
    foreign_key_sql.append(
        'FOREIGN KEY (`{fk}`) REFERENCES {ref} (`{pk}`) ON UPDATE CASCADE ON DELETE RESTRICT'.format(
            fk='`,`'.join(lookup(attr, attr) for attr in ref.primary_key),
            pk='`,`'.join(ref.primary_key),
            ref=ref.full_table_name))


def declare(full_table_name, definition, context):
    """
    Parse declaration and create new SQL table accordingly.

    :param full_table_name: full name of the table
    :param definition: DataJoint table definition
    :param context: dictionary of objects that might be referred to in the table. Usually this will be locals()
    """
    # split definition into lines
    definition = re.split(r'\s*\n\s*', definition.strip())
    # check for optional table comment
    table_comment = definition.pop(0)[1:].strip() if definition[0].startswith('#') else ''
    in_key = True  # parse primary keys
    primary_key = []
    attributes = []
    attribute_sql = []
    foreign_key_sql = []
    index_sql = []
    uses_external = False

    for line in definition:
        if line.startswith('#'):  # additional comments are ignored
            pass
        elif line.startswith('---') or line.startswith('___'):
            in_key = False  # start parsing dependent attributes
        elif is_foreign_key(line):
            compile_foreign_key(line, context, attributes,
                                primary_key if in_key else None,
                                attribute_sql, foreign_key_sql)
        elif re.match(r'^(unique\s+)?index[^:]*$', line, re.I):   # index
            index_sql.append(line)  # the SQL syntax is identical to DataJoint's
        else:
            name, sql, is_external = compile_attribute(line, in_key, foreign_key_sql)
            uses_external = uses_external or is_external
            if in_key and name not in primary_key:
                primary_key.append(name)
            if name not in attributes:
                attributes.append(name)
                attribute_sql.append(sql)
    # compile SQL
    if not primary_key:
        raise DataJointError('Table must have a primary key')
    return ('CREATE TABLE IF NOT EXISTS %s (\n' % full_table_name +
            ',\n'.join(attribute_sql +
                       ['PRIMARY KEY (`' + '`,`'.join(primary_key) + '`)'] +
                       foreign_key_sql +
                       index_sql) +
            '\n) ENGINE=InnoDB, COMMENT "%s"' % table_comment), uses_external


def compile_attribute(line, in_key, foreign_key_sql):
    """
    Convert attribute definition from DataJoint format to SQL

    :param line: attribution line
    :param in_key: set to True if attribute is in primary key set
    :param foreign_key_sql:
    :returns: (name, sql, is_external) -- attribute name and sql code for its declaration
    """

    try:
        match = attribute_parser.parseString(line+'#', parseAll=True)
    except pp.ParseException as err:
        raise DataJointError('Declaration error in position {pos} in line:\n  {line}\n{msg}'.format(
            line=err.args[0], pos=err.args[1], msg=err.args[2]))
    match['comment'] = match['comment'].rstrip('#')
    if 'default' not in match:
        match['default'] = ''
    match = {k: v.strip() for k, v in match.items()}
    match['nullable'] = match['default'].lower() == 'null'

    literals = ['CURRENT_TIMESTAMP']   # not to be enclosed in quotes
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

    is_external = match['type'].startswith('external')
    if not is_external:
        sql = ('`{name}` {type} {default}' + (' COMMENT "{comment}"' if match['comment'] else '')).format(**match)
    else:
        # process externally stored attribute
        if in_key:
            raise DataJointError('External attributes cannot be primary in:\n%s' % line)
        store_name = match['type'].split('-')
        if store_name[0] != 'external':
            raise DataJointError('External store types must be specified as "external" or "external-<name>"')
        store_name = '-'.join(store_name[1:])
        if store_name != '' and not store_name.isidentifier():
            raise DataJointError(
                'The external store name `{type}` is invalid. Make like a python identifier.'.format(**match))
        if len(store_name)>STORE_NAME_LENGTH:
            raise DataJointError(
                'The external store name `{type}` is too long. Must be <={max_len} characters.'.format(
                    max_len=STORE_NAME_LENGTH, **match))
        if not match['default'] in ('DEFAULT NULL', 'NOT NULL'):
            raise DataJointError('The only acceptable default value for an external field is null in:\n%s' % line)
        if match['type'] not in config:
            raise DataJointError('The external store `{type}` is not configured.'.format(**match))

        # append external configuration name to the end of the comment
        sql = '`{name}` {hash_type} {default} COMMENT ":{type}:{comment}"'.format(
            hash_type=HASH_DATA_TYPE, **match)
        foreign_key_sql.append(
            "FOREIGN KEY (`{name}`) REFERENCES {{external_table}} (`hash`) "
            "ON UPDATE RESTRICT ON DELETE RESTRICT".format(**match))

    return match['name'], sql, is_external
