"""General-purpose utilities"""

import re
from pathlib import Path
import shutil
import sys
from .errors import DataJointError


if sys.version_info[1] < 6:
    from collections import OrderedDict
else:
    # use dict in Python 3.6+ -- They are already ordered and look nicer
    OrderedDict = dict


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
    assert default is None or default in choices
    choice_list = ', '.join((choice.title() if choice == default else choice for choice in choices))
    response = None
    while response not in choices:
        response = input(prompt + ' [' + choice_list + ']: ')
        response = response.lower() if response else default
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


def safe_write(filepath, blob):
    """
    A two-step write.
    :param filename: full path
    :param blob: binary data
    """
    filepath = Path(filepath)
    if not filepath.is_file():
        filepath.parent.mkdir(parents=True, exist_ok=True)
        temp_file = filepath.with_suffix(filepath.suffix + '.saving')
        temp_file.write_bytes(blob)
        temp_file.rename(filepath)


def safe_copy(src, dest, overwrite=False):
    """
    Copy the contents of src file into dest file as a two-step process. Skip if dest exists already
    """
    src, dest = Path(src), Path(dest)
    if not (dest.exists() and src.samefile(dest)) and (overwrite or not dest.is_file()):
        dest.parent.mkdir(parents=True, exist_ok=True)
        temp_file = dest.with_suffix(dest.suffix + '.copying')
        shutil.copyfile(str(src), str(temp_file))
        temp_file.rename(dest)


def parse_sql(filepath):
    """
    yield SQL statements from an SQL file
    """
    delimiter = ';'
    statement = []
    with Path(filepath).open('rt') as f:
        for line in f:
            line = line.strip()
            if not line.startswith('--') and len(line) > 1:
                if line.startswith('delimiter'):
                    delimiter = line.split()[1]
                else:
                    statement.append(line)
                    if line.endswith(delimiter):
                        yield ' '.join(statement)
                        statement = []
