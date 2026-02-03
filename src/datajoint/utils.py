"""General-purpose utilities"""

import re
import shutil
from pathlib import Path

from .errors import DataJointError


def user_choice(prompt, choices=("yes", "no"), default=None):
    """
    Prompt the user for confirmation.

    The default value, if any, is capitalized.

    Parameters
    ----------
    prompt : str
        Information to display to the user.
    choices : tuple, optional
        An iterable of possible choices. Default ("yes", "no").
    default : str, optional
        Default choice. Default None.

    Returns
    -------
    str
        The user's choice.
    """
    assert default is None or default in choices
    choice_list = ", ".join((choice.title() if choice == default else choice for choice in choices))
    response = None
    while response not in choices:
        response = input(prompt + " [" + choice_list + "]: ")
        response = response.lower() if response else default
    return response


def get_master(full_table_name: str, adapter=None) -> str:
    """
    Get the master table name from a part table name.

    If the table name is that of a part table, then return what the master table name would be.
    This follows DataJoint's table naming convention where a master and a part must be in the
    same schema and the part table is prefixed with the master table name + ``__``.

    Parameters
    ----------
    full_table_name : str
        Full table name including part.
    adapter : DatabaseAdapter, optional
        Database adapter for backend-specific parsing. Default None.

    Returns
    -------
    str
        Supposed master full table name or empty string if not a part table name.

    Examples
    --------
    >>> get_master('`ephys`.`session__recording`')  # MySQL part table
    '`ephys`.`session`'
    >>> get_master('"ephys"."session__recording"')  # PostgreSQL part table
    '"ephys"."session"'
    >>> get_master('`ephys`.`session`')  # Not a part table
    ''
    """
    if adapter is not None:
        result = adapter.get_master_table_name(full_table_name)
        return result if result else ""

    # Fallback: handle both MySQL backticks and PostgreSQL double quotes
    match = re.match(r'(?P<master>(?P<q>[`"])[\w]+(?P=q)\.(?P=q)[\w]+)__[\w]+(?P=q)', full_table_name)
    if match:
        return match["master"] + match["q"]
    return ""


def is_camel_case(s):
    """
    Check if a string is in CamelCase notation.

    Parameters
    ----------
    s : str
        String to check.

    Returns
    -------
    bool
        True if the string is in CamelCase notation, False otherwise.

    Examples
    --------
    >>> is_camel_case("TableName")
    True
    >>> is_camel_case("table_name")
    False
    """
    return bool(re.match(r"^[A-Z][A-Za-z0-9]*$", s))


def to_camel_case(s):
    """
    Convert names with underscore (_) separation into camel case names.

    Parameters
    ----------
    s : str
        String in under_score notation.

    Returns
    -------
    str
        String in CamelCase notation.

    Examples
    --------
    >>> to_camel_case("table_name")
    'TableName'
    """

    def to_upper(match):
        return match.group(0)[-1].upper()

    return re.sub(r"(^|[_\W])+[a-zA-Z]", to_upper, s)


def from_camel_case(s):
    """
    Convert names in camel case into underscore (_) separated names.

    Parameters
    ----------
    s : str
        String in CamelCase notation.

    Returns
    -------
    str
        String in under_score notation.

    Raises
    ------
    DataJointError
        If the string is not in valid CamelCase notation.

    Examples
    --------
    >>> from_camel_case("TableName")
    'table_name'
    """

    def convert(match):
        return ("_" if match.groups()[0] else "") + match.group(0).lower()

    if not is_camel_case(s):
        raise DataJointError("ClassName must be alphanumeric in CamelCase, begin with a capital letter")
    return re.sub(r"(\B[A-Z])|(\b[A-Z])", convert, s)


def safe_write(filepath, blob):
    """
    Write data to a file using a two-step process.

    Writes to a temporary file first, then renames to the final path.
    This ensures atomic writes and prevents partial file corruption.

    Parameters
    ----------
    filepath : str or Path
        Full path to the destination file.
    blob : bytes
        Binary data to write.
    """
    filepath = Path(filepath)
    if not filepath.is_file():
        filepath.parent.mkdir(parents=True, exist_ok=True)
        temp_file = filepath.with_suffix(filepath.suffix + ".saving")
        temp_file.write_bytes(blob)
        temp_file.rename(filepath)


def safe_copy(src, dest, overwrite=False):
    """
    Copy the contents of src file into dest file as a two-step process.

    Copies to a temporary file first, then renames to the final path.
    Skips if dest exists already (unless overwrite is True).

    Parameters
    ----------
    src : str or Path
        Source file path.
    dest : str or Path
        Destination file path.
    overwrite : bool, optional
        If True, overwrite existing destination file. Default False.
    """
    src, dest = Path(src), Path(dest)
    if not (dest.exists() and src.samefile(dest)) and (overwrite or not dest.is_file()):
        dest.parent.mkdir(parents=True, exist_ok=True)
        temp_file = dest.with_suffix(dest.suffix + ".copying")
        shutil.copyfile(str(src), str(temp_file))
        temp_file.rename(dest)
