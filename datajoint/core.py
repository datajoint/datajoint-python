import re
import logging
# package-wide settings that control execution

# setup root logger
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)


class Settings:
    pass
    # verbose = True

class DataJointError(Exception):
    """
    Base class for errors specific to DataJoint internal
    operation.
    """
    pass


def to_camel_case(s):
    """
    Convert names with under score (_) separation
    into camel case names.

    Example:
    >>>to_camel_case("table_name")
        "TableName"
    """
    def toUpper(matchobj):
        return matchobj.group(0)[-1].upper()
    return re.sub('(^|[_\W])+[a-zA-Z]', toUpper, s)


def from_camel_case(s):
    """
    Conver names in camel case into underscore
    (_) separated names

    Example:
    >>>from_camel_case("TableName")
        "table_name"
    """
    assert not re.search(r'\s', s), 'white space is not allowed'
    assert not re.match(r'\d.*', s), 'string cannot begin with a digit'
    assert re.match(r'^[a-zA-Z0-9]*$', s), 'fromCameCase string can only contain alphanumerica characters'
    def conv(matchobj):
        return ('_' if matchobj.groups()[0] else '') + matchobj.group(0).lower()

    return re.sub(r'(\B[A-Z])|(\b[A-Z])', conv, s)
