import re
# package-wide settings that control execution

# setup root logger
from . import DataJointError





def to_camel_case(s):
    """
    Convert names with under score (_) separation
    into camel case names.

    Example:
    >>>to_camel_case("table_name")
        "TableName"
    """
    def to_upper(matchobj):
        return matchobj.group(0)[-1].upper()
    return re.sub('(^|[_\W])+[a-zA-Z]', to_upper, s)


def from_camel_case(s):
    """
    Conver names in camel case into underscore
    (_) separated names

    Example:
    >>>from_camel_case("TableName")
        "table_name"
    """
    if re.search(r'\s', s):
        raise DataJointError('White space is not allowed')
    if re.match(r'\d.*', s):
        raise DataJointError('String cannot begin with a digit')
    if not re.match(r'^[a-zA-Z0-9]*$', s):
        raise DataJointError('String can only contain alphanumeric characters')
    def conv(matchobj):
        return ('_' if matchobj.groups()[0] else '') + matchobj.group(0).lower()

    return re.sub(r'(\B[A-Z])|(\b[A-Z])', conv, s)
