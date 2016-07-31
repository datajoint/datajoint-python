"""
This module hosts functions to convert DataJoint table definitions into mysql table definitions, and to
declare the corresponding mysql tables.
"""
import re
import pyparsing as pp
import logging

from . import DataJointError

logger = logging.getLogger(__name__)


def build_foreign_key_parser():
    attribute_name = pp.Word(pp.srange('[a-z]'), pp.srange('[a-z0-9_]'))
    new_attributes = pp.Optional(pp.delimitedList(attribute_name)).setResultsName('new_attributes')
    arrow = pp.Literal('->').suppress()
    ref_table = pp.Word(pp.alphas, pp.alphanums + '._').setResultsName('ref_table')
    left = pp.Literal('(').suppress()
    right = pp.Literal(')').suppress()
    ref_attrs = pp.Optional(left + pp.delimitedList(attribute_name) + right).setResultsName('ref_attrs')
    return new_attributes + arrow + ref_table + ref_attrs


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
    from .base_relation import BaseRelation
    try:
        result = foreign_key_parser.parseString(line)
    except pp.ParseException as err:
        raise DataJointError('Parsing error in line "%s". %s.' % line, err)
    try:
        referenced_class = eval(result.ref_table, context)
    except NameError:
        raise DataJointError('Foreign key reference %s could not be resolved' % result.ref_table)
    if not issubclass(referenced_class, BaseRelation):
        raise DataJointError('Foreign key reference %s must be a subclass of UserRelation' % result.ref_table)
    if result.ref_attrs and len(result.new_attributes) != len(result.ref_attrs):
        raise DataJointError('The number of new attributes and referenced attributes does not match in "%s"' % line)
    ref = referenced_class()
    if not result.new_attributes:
        #  a simple foreign key
        for attr in ref.primary_key:
            if attr not in attributes:
                attributes.append(attr)
                attr_sql.append(ref.heading[attr].sql)
                if primary_key is not None:
                    primary_key.append(attr)
        fk = ref.primary_key
    elif len(result.new_attributes) == 1 and not result.ref_attrs:
        #  a one-alias foreign key
        ref_attr = (ref.primary_key if len(ref.primary_key) == 1 else
                    [attr for attr in ref.primary_key if attr not in attributes])
        if len(ref_attr) != 1:
            raise DataJointError('Mismatched attributes in foreign key "%s"' % line)
        ref_attr = ref_attr[0]
        attr = result.new_attributes[0]
        attributes.append(attr)
        assert ref.heading[ref_attr].sql.startswith('`%s`' % ref_attr)
        attr_sql.append(ref.heading[ref_attr].sql.replace(ref_attr, attr, 1))
        if primary_key is not None:
            primary_key.append(attr)
        fk = [attr if k == ref_attr else k for k in ref.primary_key]
    else:
        #  a mapped foreign key
        raise NotImplementedError('TBD mapped foreign keys ')

    foreign_key_sql.append(
        'FOREIGN KEY (`{fk}`) REFERENCES {ref} (`{pk}`) ON UPDATE CASCADE ON DELETE RESTRICT'.format(
            fk='`,`'.join(fk), pk='`,`'.join(ref.primary_key), ref=ref.full_table_name))


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
            name, sql = compile_attribute(line, in_key)
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
            '\n) ENGINE=InnoDB, COMMENT "%s"' % table_comment)


def compile_attribute(line, in_key=False):
    """
    Convert attribute definition from DataJoint format to SQL

    :param line: attribution line
    :param in_key: set to True if attribute is in primary key set
    :returns: (name, sql) -- attribute name and sql code for its declaration
    """

    try:
        match = attribute_parser.parseString(line+'#', parseAll=True)
    except pp.ParseException:
        logger.error('Declaration error in line: ', line)
        raise
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
    sql = ('`{name}` {type} {default}' + (' COMMENT "{comment}"' if match['comment'] else '')).format(**match)
    return match['name'], sql
