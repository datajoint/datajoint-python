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
    data_file : <object@local>  # stored file
    """


class ObjectFolder(dj.Manual):
    """Table for testing object type with folders."""

    definition = """
    folder_id : int
    ---
    data_folder : <object@local>  # stored folder
    """


class ObjectMultiple(dj.Manual):
    """Table for testing multiple object attributes."""

    definition = """
    record_id : int
    ---
    raw_data : <object@local>    # raw data file
    processed : <object@local>   # processed data file
    """


class ObjectWithOther(dj.Manual):
    """Table for testing object type with other attributes."""

    definition = """
    subject_id : int
    session_id : int
    ---
    name : varchar(100)
    data_file : <object@local>
    notes : varchar(255)
    """
