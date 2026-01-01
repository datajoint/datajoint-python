"""
Schema definitions for object type tests.
"""

import datajoint as dj

LOCALS_OBJECT = locals()


class ObjectFile(dj.Manual):
    """Table for testing object type with files."""

    definition = """
    file_id : int
    ---
    data_file : <object>  # stored file
    """


class ObjectFolder(dj.Manual):
    """Table for testing object type with folders."""

    definition = """
    folder_id : int
    ---
    data_folder : <object>  # stored folder
    """


class ObjectMultiple(dj.Manual):
    """Table for testing multiple object attributes."""

    definition = """
    record_id : int
    ---
    raw_data : <object>    # raw data file
    processed : <object>   # processed data file
    """


class ObjectWithOther(dj.Manual):
    """Table for testing object type with other attributes."""

    definition = """
    subject_id : int
    session_id : int
    ---
    name : varchar(100)
    data_file : <object>
    notes : varchar(255)
    """
