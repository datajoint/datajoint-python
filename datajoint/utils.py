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
    Convert names in camel case into underscore (_) separated names

    Example:
    >>>from_camel_case("TableName")
        "table_name"
    """
    def convert(match):
        return ('_' if match.groups()[0] else '') + match.group(0).lower()

    if not re.match(r'[A-Z][a-zA-Z0-9]*', s):
        raise DataJointError(
            'ClassName must be alphanumeric in CamelCase, begin with a capital letter')
    return re.sub(r'(\B[A-Z])|(\b[A-Z])', convert, s)

def user_confirmation(infostring, choices, default=None):
    """
    Prompts the user for confirmation.

    :param infostring: Information to display to the user.
    :param choices: an iterable of possible choices.
    :param default=None: default choice
    :return: the user's choice
    """
    print(infostring)
    cho = list(choices)
    if default is not None:
        cho[cho.index(default)] += ' (default)'
    cho = ', '.join(cho)

    response = input('Please answer ' + cho)
    while not ((response in choices) or (default is not None and len(response.strip())==0)):
        response = input('Please answer (' + cho + '):')
    if default is not None and len(response.strip())==0:
        response = choices[choices.index(default)]
    return response
