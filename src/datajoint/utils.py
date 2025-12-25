"""
General-purpose utilities for DataJoint.

This module provides helper functions for common operations including
naming conventions, file operations, and SQL parsing.
"""

from __future__ import annotations

import re
import shutil
from collections.abc import Callable, Generator
from pathlib import Path
from typing import Any

from .errors import DataJointError


class ClassProperty:
    """
    Descriptor for defining class-level properties.

    Similar to @property but works on the class itself rather than instances.
    """

    def __init__(self, f: Callable) -> None:
        self.f = f

    def __get__(self, obj: Any, owner: type) -> Any:
        return self.f(owner)


def user_choice(
    prompt: str,
    choices: tuple[str, ...] = ("yes", "no"),
    default: str | None = None,
) -> str:
    """
    Prompt the user to select from a list of choices.

    The default value, if any, is displayed capitalized.

    Args:
        prompt: Message to display to the user.
        choices: Tuple of valid response options.
        default: Default choice if user presses Enter without input.

    Returns:
        The user's selected choice (lowercase).

    Raises:
        AssertionError: If default is not None and not in choices.
    """
    assert default is None or default in choices
    choice_list = ", ".join((choice.title() if choice == default else choice for choice in choices))
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


def is_camel_case(s: str) -> bool:
    """
    Check if a string is in CamelCase notation.

    Args:
        s: The string to check.

    Returns:
        True if the string matches CamelCase pattern (starts with uppercase,
        contains only alphanumeric characters).

    Examples:
        >>> is_camel_case("TableName")  # True
        >>> is_camel_case("table_name")  # False
    """
    return bool(re.match(r"^[A-Z][A-Za-z0-9]*$", s))


def to_camel_case(s: str) -> str:
    """
    Convert underscore-separated names to CamelCase.

    Args:
        s: String in underscore_notation.

    Returns:
        String in CamelCase notation.

    Example:
        >>> to_camel_case("table_name")  # "TableName"
    """

    def to_upper(match: re.Match) -> str:
        return match.group(0)[-1].upper()

    return re.sub(r"(^|[_\W])+[a-zA-Z]", to_upper, s)


def from_camel_case(s: str) -> str:
    """
    Convert CamelCase names to underscore-separated lowercase.

    Args:
        s: String in CamelCase notation.

    Returns:
        String in underscore_notation.

    Raises:
        DataJointError: If the input is not valid CamelCase.

    Example:
        >>> from_camel_case("TableName")  # "table_name"
    """

    def convert(match: re.Match) -> str:
        return ("_" if match.groups()[0] else "") + match.group(0).lower()

    if not is_camel_case(s):
        raise DataJointError("ClassName must be alphanumeric in CamelCase, begin with a capital letter")
    return re.sub(r"(\B[A-Z])|(\b[A-Z])", convert, s)


def safe_write(filepath: str | Path, blob: bytes) -> None:
    """
    Write binary data to a file atomically using a two-step process.

    Creates a temporary file first, then renames it to the target path.
    This prevents partial writes from corrupting the file.

    Args:
        filepath: Destination file path.
        blob: Binary data to write.
    """
    filepath = Path(filepath)
    if not filepath.is_file():
        filepath.parent.mkdir(parents=True, exist_ok=True)
        temp_file = filepath.with_suffix(filepath.suffix + ".saving")
        temp_file.write_bytes(blob)
        temp_file.rename(filepath)


def safe_copy(
    src: str | Path,
    dest: str | Path,
    overwrite: bool = False,
) -> None:
    """
    Copy a file atomically using a two-step process.

    Creates a temporary file first, then renames it. Skips if destination
    exists (unless overwrite=True) or if src and dest are the same file.

    Args:
        src: Source file path.
        dest: Destination file path.
        overwrite: If True, overwrite existing destination file.
    """
    src, dest = Path(src), Path(dest)
    if not (dest.exists() and src.samefile(dest)) and (overwrite or not dest.is_file()):
        dest.parent.mkdir(parents=True, exist_ok=True)
        temp_file = dest.with_suffix(dest.suffix + ".copying")
        shutil.copyfile(str(src), str(temp_file))
        temp_file.rename(dest)


def parse_sql(filepath: str | Path) -> Generator[str, None, None]:
    """
    Parse SQL statements from a file.

    Handles custom delimiters and skips SQL comments.

    Args:
        filepath: Path to the SQL file.

    Yields:
        Individual SQL statements as strings.
    """
    delimiter = ";"
    statement: list[str] = []
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
        if statement:
            yield " ".join(statement)
