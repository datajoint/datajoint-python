import re
import pyparsing as pp
import logging


logger = logging.getLogger(__name__)



def declare(full_table_name,  definition, context):
    """
    Parse declaration and create new SQL table accordingly.
    """
    # split definition into lines
    definition = re.split(r'\s*\n\s*', definition.strip())

    table_comment = definition.pop(0)[1:] if definition[0].startswith('#') else ''

    in_key = True  # parse primary keys
    primary_key = []
    attributes = []
    attribute_sql = []
    foreign_key_sql = []
    index_sql = []

    for line in definition:
        if line.startswith('#'):  # additional comments are ignored
            pass
        elif line.startswith('---'):
            in_key = False  # start parsing dependent attributes
        elif line.startswith('->'):
            # foreign key
            ref = eval(line[2:], context)()
            foreign_key_sql.append(
                'FOREIGN KEY ({primary_key})'
                ' REFERENCES {ref} ({primary_key})'
                ' ON UPDATE CASCADE ON DELETE RESTRICT'.format(
                    primary_key='`' + '`,`'.join(primary_key) + '`', ref=ref.full_table_name)
            )
            for name in ref.primary_key:
                if in_key and name not in primary_key:
                    primary_key.append(name)
                if name not in attributes:
                    attributes.append(name)
                    attribute_sql.append(ref.heading[name].sql())
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
    sql = 'CREATE TABLE %s (\n  ' % full_table_name
    sql += ',  \n'.join(attribute_sql)
    if foreign_key_sql:
        sql += ',  \n' + ',  \n'.join(foreign_key_sql)
    if index_sql:
        sql += ',  \n' + ',  \n'.join(index_sql)
    sql += '\n) ENGINE = InnoDB, COMMENT "%s"' % table_comment
    return sql




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
