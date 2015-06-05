import re
from . import DataJointError
from .heading import Heading


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


def field_to_sql(field):  # TODO move this into Attribute Tuple
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


def parse_index_definition(line):
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
