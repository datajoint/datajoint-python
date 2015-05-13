import re
from . import DataJointError
import collections


def to_camel_case(s):
    """
    Convert names with under score (_) separation
    into camel case names.

    Example:
    >>>to_camel_case("table_name")
        "TableName"
    """
    def to_upper(match):
        return match.group(0)[-1].upper()
    return re.sub('(^|[_\W])+[a-zA-Z]', to_upper, s)


def from_camel_case(s):
    """
    Convert names in camel case into underscore
    (_) separated names

    Example:
    >>>from_camel_case("TableName")
        "table_name"
    """
    if re.search(r'\s', s):
        raise DataJointError('Input cannot contain white space')
    if re.match(r'\d.*', s):
        raise DataJointError('Input cannot begin with a digit')
    if not re.match(r'^[a-zA-Z0-9]*$', s):
        raise DataJointError('String can only contain alphanumeric characters')

    def convert(match):
        return ('_' if match.groups()[0] else '') + match.group(0).lower()

    return re.sub(r'(\B[A-Z])|(\b[A-Z])', convert, s)
