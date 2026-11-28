"""General-purpose utilities"""

import contextlib
import os
import random
import re
import shutil
import time
import uuid
import warnings
from pathlib import Path

from .errors import DataJointError

_REPLACE_DEADLINE = 2.0  # seconds; the retry runs on Windows only
_IS_WINDOWS = os.name == "nt"


def is_plain_file_name(name: str) -> bool:
    """
    Check that ``name`` is a plain file name with no directory part.

    Rejects empty names, ``.`` and ``..``, the separators ``/`` and ``\\`` (on
    every platform), NUL bytes, and on Windows any ``:`` (drives and alternate
    data streams). A name that passes always names an entry directly inside
    the directory it is joined to.
    """
    return not (name in ("", ".", "..") or "/" in name or "\\" in name or "\x00" in name or (os.name == "nt" and ":" in name))


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

    # Handle underscores: warn and remove them
    if "_" in s:
        warnings.warn(
            f"Table class name `{s}` contains underscores. " "CamelCase names without underscores are recommended.",
            UserWarning,
            stacklevel=3,
        )
        s = s.replace("_", "")
    if not is_camel_case(s):
        raise DataJointError("ClassName must be alphanumeric in CamelCase, begin with a capital letter")
    return re.sub(r"(\B[A-Z])|(\b[A-Z])", convert, s)


def _replace(temp_file, dest, stop_if_taken=False):
    """
    Move ``temp_file`` over ``dest``, retrying on Windows while another program holds ``dest``.

    On Windows ``os.replace`` raises ``PermissionError`` while another process or
    thread is replacing or reading ``dest``, or while antivirus or search indexing
    has it open. It is retried until ``_REPLACE_DEADLINE`` seconds have passed, with
    exponential backoff and a random factor so that colliding writers drift apart.
    Elsewhere, and once the deadline passes, the ``PermissionError`` is raised.

    With ``stop_if_taken`` the error is also raised at once when ``dest`` exists, so
    that a caller that does not overwrite does not wait for a file that is already there.
    """
    deadline = time.monotonic() + _REPLACE_DEADLINE
    pause = 0.01
    while True:
        try:
            os.replace(temp_file, dest)
            return
        except PermissionError:
            if not _IS_WINDOWS or time.monotonic() + pause > deadline or (stop_if_taken and dest.is_file()):
                raise
        time.sleep(pause * random.uniform(0.5, 1.5))
        pause = min(pause * 2, 0.25)


def _replace_through_temp_file(dest, write, skip_if_taken=False):
    """
    Create or replace ``dest`` by filling a temporary file beside it first.

    ``write(temp_file)`` fills the temporary file, which is then moved over
    ``dest`` with ``os.replace``. Unlike ``Path.rename``, that replaces an existing
    file on Windows too. The temporary name is unique and does not depend on the
    length of ``dest``'s name, so writers of the same destination do not share a
    file and a long destination name still fits. The temporary file is removed
    whether the write succeeds or fails; after a hard crash (a killed process or a
    power loss) it stays behind, as a hidden ``.<16 hex digits>.tmp`` file in the
    destination folder.

    With ``skip_if_taken`` a replace that fails because ``dest`` now exists is not an
    error: another writer stored the file first, and the caller does not overwrite.
    """
    temp_file = dest.with_name(f".{uuid.uuid4().hex[:16]}.tmp")
    try:
        write(temp_file)
        try:
            _replace(temp_file, dest, stop_if_taken=skip_if_taken)
        except PermissionError:
            if not (skip_if_taken and dest.is_file()):
                raise
    finally:
        # Gone after a successful replace. After a failure, a failed cleanup must
        # not hide the error that caused it.
        with contextlib.suppress(OSError):
            temp_file.unlink(missing_ok=True)


def safe_write(filepath, blob):
    """
    Write data to a file using a two-step process.

    Writes to a temporary file first, then moves it to the final path.
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
        _replace_through_temp_file(filepath, lambda temp_file: temp_file.write_bytes(blob), skip_if_taken=True)


def safe_copy(src, dest, overwrite=False):
    """
    Copy the contents of src file into dest file as a two-step process.

    Copies to a temporary file first, then moves it to the final path.
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
        _replace_through_temp_file(dest, lambda temp_file: shutil.copyfile(str(src), str(temp_file)))
