"""
Integration tests for changing attributes with ``Table.alter()`` on both backends.

Each test alters a table that holds data and checks that the table now describes
itself exactly as a table declared directly from the new definition does, that
``alter()`` then finds nothing left to change, and that the data survived.
MySQL makes every change with one ALTER TABLE statement; PostgreSQL uses
``ALTER COLUMN``, ``RENAME COLUMN`` and ``COMMENT ON`` statements inside one
transaction (#1625).
"""

import itertools
import time

import pytest

import datajoint as dj
from datajoint.declare import alter
from datajoint.errors import DataJointError


REFERENCE_NUMBERS = itertools.count()


@pytest.fixture
def schema_by_backend(connection_by_backend, backend, prefix):
    """A fresh schema on each backend, dropped afterward."""
    test_id = str(int(time.time() * 1000))[-8:]
    schema = dj.Schema(f"{prefix}_alter_attr_{backend}_{test_id}", connection=connection_by_backend)
    yield schema
    schema.drop(prompt=False)


def declare_measurement(schema, definition):
    class Measurement(dj.Manual):
        pass

    Measurement.definition = definition
    return schema(Measurement)


def alter_to(schema, table, definition):
    """Alter ``table`` to ``definition`` and check that the result matches a direct declaration."""
    table.definition = definition
    table.alter(prompt=False)
    altered = table.describe(context={})

    # The reference tables stay until the schema is dropped: on PostgreSQL,
    # dropping one would also drop the enum types it shares with the table.
    reference = type(f"Reference{next(REFERENCE_NUMBERS)}", (dj.Manual,), dict(definition=definition))
    assert altered == schema(reference)().describe(context={})

    # describe() reads back what the definition declares, so there is nothing left to alter
    statements, _, _ = alter(definition, altered, {}, schema.connection.adapter, full_table_name=table.full_table_name)
    assert statements == []
    return altered


@pytest.mark.backend_agnostic
def test_change_type(schema_by_backend):
    Measurement = declare_measurement(
        schema_by_backend,
        """
        measurement_id : int32
        ---
        value : float32   # the value
        count : int16   # a count
        code : varchar(8)   # a numeric code
        """,
    )
    Measurement.insert1(dict(measurement_id=1, value=2.5, count=3, code="12"))

    alter_to(
        schema_by_backend,
        Measurement,
        """
        measurement_id : int32
        ---
        value : float64   # the value
        count : varchar(8)   # a count
        code : int32   # a numeric code
        """,
    )
    assert Measurement.heading["value"].original_type == "float64"
    assert Measurement.fetch1() == dict(measurement_id=1, value=2.5, count="3", code=12)


@pytest.mark.backend_agnostic
def test_change_between_number_and_bool(schema_by_backend):
    Measurement = declare_measurement(
        schema_by_backend,
        """
        measurement_id : int32
        ---
        flag : int8   # a flag
        """,
    )
    Measurement.insert([dict(measurement_id=1, flag=1), dict(measurement_id=2, flag=0)])

    alter_to(
        schema_by_backend,
        Measurement,
        """
        measurement_id : int32
        ---
        flag : bool   # a flag
        """,
    )
    assert [bool(f) for f in Measurement.to_arrays("flag", order_by="measurement_id")] == [True, False]

    alter_to(
        schema_by_backend,
        Measurement,
        """
        measurement_id : int32
        ---
        flag : int32   # a flag
        """,
    )
    assert list(Measurement.to_arrays("flag", order_by="measurement_id")) == [1, 0]


@pytest.mark.backend_agnostic
def test_change_nullability(schema_by_backend):
    Measurement = declare_measurement(
        schema_by_backend,
        """
        measurement_id : int32
        ---
        value : float32   # the value
        """,
    )
    Measurement.insert1(dict(measurement_id=1, value=2.5))

    alter_to(
        schema_by_backend,
        Measurement,
        """
        measurement_id : int32
        ---
        value = null : float32   # the value
        """,
    )
    Measurement.insert1(dict(measurement_id=2))
    assert (Measurement & dict(measurement_id=2)).fetch1("value") is None

    (Measurement & dict(measurement_id=2)).delete_quick()
    alter_to(
        schema_by_backend,
        Measurement,
        """
        measurement_id : int32
        ---
        value : float32   # the value
        """,
    )
    assert Measurement.fetch1("value") == 2.5


@pytest.mark.backend_agnostic
def test_change_default(schema_by_backend):
    Measurement = declare_measurement(
        schema_by_backend,
        """
        measurement_id : int32
        ---
        value : float32   # the value
        label : varchar(16)   # a label
        """,
    )
    Measurement.insert1(dict(measurement_id=1, value=2.5, label="first"))

    alter_to(
        schema_by_backend,
        Measurement,
        """
        measurement_id : int32
        ---
        value = 1.5 : float32   # the value
        label = "it's unset" : varchar(16)   # a label
        """,
    )
    Measurement.insert1(dict(measurement_id=2))
    assert (Measurement & dict(measurement_id=2)).fetch1("value", "label") == (1.5, "it's unset")

    alter_to(
        schema_by_backend,
        Measurement,
        """
        measurement_id : int32
        ---
        value : float32   # the value
        label : varchar(16)   # a label
        """,
    )
    assert Measurement.heading["value"].default is None
    assert len(Measurement()) == 2


@pytest.mark.backend_agnostic
def test_change_comment(schema_by_backend):
    Measurement = declare_measurement(
        schema_by_backend,
        """
        measurement_id : int32
        ---
        value : float32   # the value
        """,
    )
    Measurement.insert1(dict(measurement_id=1, value=2.5))

    altered = alter_to(
        schema_by_backend,
        Measurement,
        """
        measurement_id : int32
        ---
        value : float32   # another comment
        """,
    )
    assert "# another comment" in altered
    # the core-type marker is kept with the new comment
    assert Measurement.heading["value"].original_type == "float32"
    assert Measurement.fetch1("value") == 2.5


@pytest.mark.backend_agnostic
def test_change_table_comment(schema_by_backend):
    Measurement = declare_measurement(
        schema_by_backend,
        """
        measurement_id : int32
        ---
        value : float32   # the value
        """,
    )
    altered = alter_to(
        schema_by_backend,
        Measurement,
        """
        # measurements of the value
        measurement_id : int32
        ---
        value : float32   # the value
        """,
    )
    assert altered.startswith("# measurements of the value\n")


@pytest.mark.backend_agnostic
def test_combined_changes(schema_by_backend):
    Measurement = declare_measurement(
        schema_by_backend,
        """
        measurement_id : int32
        ---
        value : float32   # the value
        label : varchar(16)   # a label
        obsolete = null : int32   # to be dropped
        """,
    )
    Measurement.insert1(dict(measurement_id=1, value=2.5, label="first"))

    alter_to(
        schema_by_backend,
        Measurement,
        """
        measurement_id : int32
        ---
        value = 0.5 : float64   # a wider value
        label = null : varchar(32)   # a longer label
        added = null : int16   # an added attribute
        """,
    )
    assert Measurement.fetch1() == dict(measurement_id=1, value=2.5, label="first", added=None)


@pytest.mark.backend_agnostic
def test_rename(schema_by_backend):
    Measurement = declare_measurement(
        schema_by_backend,
        """
        measurement_id : int32
        ---
        value : float32   # the value
        """,
    )
    Measurement.insert1(dict(measurement_id=1, value=2.5))

    alter_to(
        schema_by_backend,
        Measurement,
        """
        measurement_id : int32
        ---
        reading : float32   # {value} the value
        """,
    )
    assert Measurement.fetch1() == dict(measurement_id=1, reading=2.5)


@pytest.mark.backend_agnostic
def test_rename_and_change(schema_by_backend):
    Measurement = declare_measurement(
        schema_by_backend,
        """
        measurement_id : int32
        ---
        value = 1.5 : float32   # the value
        """,
    )
    Measurement.insert1(dict(measurement_id=1, value=2.5))

    alter_to(
        schema_by_backend,
        Measurement,
        """
        measurement_id : int32
        ---
        reading = null : float64   # {value} the value, now optional
        """,
    )
    assert Measurement.fetch1() == dict(measurement_id=1, reading=2.5)
    assert Measurement.heading["reading"].original_type == "float64"


@pytest.mark.backend_agnostic
def test_change_to_enum(schema_by_backend):
    Measurement = declare_measurement(
        schema_by_backend,
        """
        measurement_id : int32
        ---
        species = "mouse" : varchar(16)   # the species
        """,
    )
    Measurement.insert([dict(measurement_id=1, species="mouse"), dict(measurement_id=2, species="rat")])

    alter_to(
        schema_by_backend,
        Measurement,
        """
        measurement_id : int32
        ---
        species = "mouse" : enum('mouse', 'rat')   # the species
        """,
    )
    alter_to(
        schema_by_backend,
        Measurement,
        """
        measurement_id : int32
        ---
        species = "mouse" : enum('mouse', 'rat', 'vole')   # the species
        """,
    )
    Measurement.insert1(dict(measurement_id=3, species="vole"))
    assert list(Measurement.to_arrays("species", order_by="measurement_id")) == ["mouse", "rat", "vole"]


@pytest.mark.backend_agnostic
def test_failed_change_leaves_table_unchanged(schema_by_backend):
    definition = """
    measurement_id : int32
    ---
    code : varchar(8)   # a code
    """
    Measurement = declare_measurement(schema_by_backend, definition)
    Measurement.insert1(dict(measurement_id=1, code="abc"))
    before = Measurement.describe(context={})

    # "abc" is not a number; on PostgreSQL the rename runs before the type
    # change, and the transaction undoes it.
    Measurement.definition = """
    measurement_id : int32
    ---
    number : int32   # {code} a code
    """
    with pytest.raises(Exception):
        Measurement.alter(prompt=False)
    Measurement.definition = definition
    assert Measurement.describe(context={}) == before
    assert Measurement.fetch1() == dict(measurement_id=1, code="abc")


@pytest.mark.backend_agnostic
def test_unconvertible_type_is_refused(schema_by_backend, backend):
    if backend != "postgresql":
        pytest.skip("MySQL converts a date to a number")
    definition = """
    measurement_id : int32
    ---
    taken : date   # when it was taken
    """
    Measurement = declare_measurement(schema_by_backend, definition)
    before = Measurement.describe(context={})
    Measurement.definition = """
    measurement_id : int32
    ---
    taken : int32   # when it was taken
    """
    with pytest.raises(DataJointError, match="cannot convert attribute `taken` from date to integer"):
        Measurement.alter(prompt=False)
    assert Measurement.describe(context={}) == before
