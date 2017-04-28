import re
from datajoint import DataJointError

class ClassProperty:
    def __init__(self, f):
        self.f = f

    def __get__(self, obj, owner):
        return self.f(owner)


def user_choice(prompt, choices=("yes", "no"), default=None):
    """
    Prompts the user for confirmation.  The default value, if any, is capitalized.

    :param prompt: Information to display to the user.
    :param choices: an iterable of possible choices.
    :param default: default choice
    :return: the user's choice
    """
    choice_list = ', '.join((choice.title() if choice == default else choice for choice in choices))
    valid = False
    while not valid:
        response = input(prompt + ' [' + choice_list + ']: ')
        response = response.lower() if response else default
        valid = response in choices
    return response


def to_camel_case(s):
    """
    Convert names with under score (_) separation into camel case names.

    :param s: string in under_score notation
    :returns: string in CamelCase notation

    Example:

    >>> to_camel_case("table_name") # yields "TableName"

    """

    def to_upper(match):
        return match.group(0)[-1].upper()

    return re.sub('(^|[_\W])+[a-zA-Z]', to_upper, s)


def from_camel_case(s):
    """
    Convert names in camel case into underscore (_) separated names

    :param s: string in CamelCase notation
    :returns: string in under_score notation

    Example:

    >>> from_camel_case("TableName") # yields "table_name"

    """

    def convert(match):
        return ('_' if match.groups()[0] else '') + match.group(0).lower()

    if not re.match(r'[A-Z][a-zA-Z0-9]*', s):
        raise DataJointError(
            'ClassName must be alphanumeric in CamelCase, begin with a capital letter')
    return re.sub(r'(\B[A-Z])|(\b[A-Z])', convert, s)
