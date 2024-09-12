"""General-purpose utilities"""

import re
from pathlib import Path
import shutil
from .errors import DataJointError


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
    choice_list = ", ".join(
        (choice.title() if choice == default else choice for choice in choices)
    )
    response = None
    while response not in choices:
        response = input(prompt + " [" + choice_list + "]: ")
        response = response.lower() if response else default
    return response


def get_master(full_table_name: str) -> str:
    """
    If the table name is that of a part table, then return what the master table name would be.
    This follows DataJoint's table naming convention where a master and a part must be in the
    same schema and the part table is prefixed with the master table name + ``__``.

    Example:
       `ephys`.`session`    -- master
       `ephys`.`session__recording`  -- part

    :param full_table_name: Full table name including part.
    :type full_table_name: str
    :return: Supposed master full table name or empty string if not a part table name.
    :rtype: str
    """
    match = re.match(r"(?P<master>`\w+`.`\w+)__(?P<part>\w+)`", full_table_name)
    return match["master"] + "`" if match else ""


def is_camel_case(s):
    """
    Check if a string is in CamelCase notation.

    :param s: string to check
    :returns: True if the string is in CamelCase notation, False otherwise
    Example:
    >>> is_camel_case("TableName")  # returns True
    >>> is_camel_case("table_name")  # returns False
    """
    return bool(re.match(r"^[A-Z][A-Za-z0-9]*$", s))


def to_camel_case(s):
    """
    Convert names with under score (_) separation into camel case names.

    :param s: string in under_score notation
    :returns: string in CamelCase notation
    Example:
    >>> to_camel_case("table_name")  # returns "TableName"
    """

    def to_upper(match):
        return match.group(0)[-1].upper()

    return re.sub(r"(^|[_\W])+[a-zA-Z]", to_upper, s)


def from_camel_case(s):
    """
    Convert names in camel case into underscore (_) separated names

    :param s: string in CamelCase notation
    :returns: string in under_score notation
    Example:
    >>> from_camel_case("TableName") # yields "table_name"
    """

    def convert(match):
        return ("_" if match.groups()[0] else "") + match.group(0).lower()

    if not is_camel_case(s):
        raise DataJointError(
            "ClassName must be alphanumeric in CamelCase, begin with a capital letter"
        )
    return re.sub(r"(\B[A-Z])|(\b[A-Z])", convert, s)


def safe_write(filepath, blob):
    """
    A two-step write.

    :param filename: full path
    :param blob: binary data
    """
    filepath = Path(filepath)
    if not filepath.is_file():
        filepath.parent.mkdir(parents=True, exist_ok=True)
        temp_file = filepath.with_suffix(filepath.suffix + ".saving")
        temp_file.write_bytes(blob)
        temp_file.rename(filepath)


def safe_copy(src, dest, overwrite=False):
    """
    Copy the contents of src file into dest file as a two-step process. Skip if dest exists already
    """
    src, dest = Path(src), Path(dest)
    if not (dest.exists() and src.samefile(dest)) and (overwrite or not dest.is_file()):
        dest.parent.mkdir(parents=True, exist_ok=True)
        temp_file = dest.with_suffix(dest.suffix + ".copying")
        shutil.copyfile(str(src), str(temp_file))
        temp_file.rename(dest)


def parse_sql(filepath):
    """
    yield SQL statements from an SQL file
    """
    delimiter = ";"
    statement = []
    with Path(filepath).open("rt") as f:
        for line in f:
            line = line.strip()
            if not line.startswith("--") and len(line) > 1:
                if line.startswith("delimiter"):
                    delimiter = line.split()[1]
                else:
                    statement.append(line)
                    if line.endswith(delimiter):
                        yield " ".join(statement)
                        statement = []
