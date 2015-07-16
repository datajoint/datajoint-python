import numpy as np
import re
from datajoint import DataJointError



def user_choice(prompt, choices=("yes", "no"), default=None):
    """
    Prompts the user for confirmation.  The default value, if any, is capitalized.
    :parsam prompt: Information to display to the user.
    :param choices: an iterable of possible choices.
    :param default: default choice
    :return: the user's choice
    """
    choice_list = ', '.join((choice.title() if choice == default else choice for choice in choices))
    valid = False
    while not valid:
        response = input(prompt + ' [' + choice_list + ']: ')
        response = response if response else default
        valid = response in choices
    return response


def group_by(rel, *attributes, sortby=None):
    r = rel.project(*attributes).fetch()
    dtype2 = np.dtype({name:r.dtype.fields[name] for name in attributes})
    r2 = np.unique(np.ndarray(r.shape, dtype2, r, 0, r.strides))
    r2.sort(order=sortby if sortby is not None else attributes)
    for nk in r2:
        restr = ' and '.join(["%s='%s'" % (fn, str(v)) for fn, v in zip(r2.dtype.names, nk)])
        if len(nk) == 1:
            yield nk[0], rel & restr
        else:
            yield nk, rel & restr


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
