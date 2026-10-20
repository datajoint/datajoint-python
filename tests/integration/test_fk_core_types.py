"""
A column copied by a foreign key keeps the parent's core type.

Core types (``int32``, ``int16``, ``uuid``, ...) are recorded in the column comment as
``:type:``. A foreign key copies the parent's primary-key columns into the child; the copy
must carry the same marker, or the child's heading reports the native SQL type and every
table derived from it (a jobs table, a further child) inherits the loss.
"""

import logging

import pytest

import datajoint as dj


class _Records(logging.Handler):
    def __init__(self):
        super().__init__(level=logging.WARNING)
        self.messages = []

    def emit(self, record):
        self.messages.append(record.getMessage())


@pytest.mark.backend_agnostic
def test_fk_columns_keep_core_types(connection_by_backend, backend, prefix):
    schema = dj.Schema(f"{prefix}_fk_core_types_{backend}", connection=connection_by_backend)
    try:

        @schema
        class Subject(dj.Manual):
            definition = """
            subject_id : int32
            """

        @schema
        class Session(dj.Manual):
            definition = """
            -> Subject
            session_idx : int16
            """

        @schema
        class Summary(dj.Manual):
            definition = """
            -> Session
            ---
            n : int32
            """

        assert Subject.heading["subject_id"].original_type == "int32"
        # inherited one level down
        assert Session.heading["subject_id"].original_type == "int32"
        assert Session.heading["session_idx"].original_type == "int16"
        # inherited two levels down
        assert Summary.heading["subject_id"].original_type == "int32"
        assert Summary.heading["session_idx"].original_type == "int16"
        # the comment text itself is unchanged
        assert Summary.heading["subject_id"].comment == Subject.heading["subject_id"].comment
    finally:
        schema.drop(prompt=False)


@pytest.mark.backend_agnostic
def test_fk_columns_keep_uuid(connection_by_backend, backend, prefix):
    schema = dj.Schema(f"{prefix}_fk_core_types_uuid_{backend}", connection=connection_by_backend)
    try:

        @schema
        class Item(dj.Manual):
            definition = """
            item_id : uuid       # identifier of the item
            """

        @schema
        class Tag(dj.Manual):
            definition = """
            -> Item
            tag : varchar(16)
            """

        assert Tag.heading["item_id"].uuid
        assert Tag.heading["item_id"].original_type == "uuid"
        assert Tag.heading["item_id"].comment == "identifier of the item"
    finally:
        schema.drop(prompt=False)


@pytest.mark.backend_agnostic
def test_jobs_table_keeps_inherited_core_types(connection_by_backend, backend, prefix):
    """A jobs table built from inherited keys declares core types and warns about nothing."""
    schema = dj.Schema(f"{prefix}_fk_core_types_jobs_{backend}", connection=connection_by_backend)
    records = _Records()
    logger = logging.getLogger("datajoint")
    try:

        @schema
        class Subject(dj.Manual):
            definition = """
            subject_id : int32
            """

        @schema
        class Session(dj.Manual):
            definition = """
            -> Subject
            session_idx : int16
            """

        @schema
        class Summary(dj.Computed):
            definition = """
            -> Session
            ---
            n : int32
            """

            def make(self, key):
                self.insert1(dict(key, n=1))

        logger.addHandler(records)
        jobs = Summary.jobs
        assert jobs.heading["subject_id"].original_type == "int32"
        assert jobs.heading["session_idx"].original_type == "int16"
        native = [m for m in records.messages if m.startswith("Native type")]
        assert native == [], native
    finally:
        logger.removeHandler(records)
        schema.drop(prompt=False)
