"""
Integration tests for the master-part group cycle rule.

A part may reference its master, another part of the same master, or a table
that does not depend on its master. A reference to a table that depends on the
master would make the master-part group depend on itself, and DataJoint could
no longer order the tables for delete, drop or trace, so the declaration is
refused before any DDL runs.

A cycle declared before the rule existed (here, with the check switched off)
makes the operations that must order it raise an error that names the cycle,
while operations on the rest of the schema keep working.
"""

import itertools
import time
import warnings

import pytest

import datajoint as dj
from datajoint.errors import DataJointError

_counter = itertools.count()


@pytest.fixture(scope="function")
def make_schema(connection_by_backend, db_creds_by_backend):
    """Create fresh schemas for a test and drop them afterwards, last created first."""
    backend = db_creds_by_backend["backend"]
    schemas = []

    def make():
        name = f"djtest_groupcycle_{backend}_{str(int(time.time() * 1000))[-8:]}_{next(_counter)}"[:64]
        schemas.append(dj.Schema(name, connection=connection_by_backend))
        return schemas[-1]

    yield make

    for schema in reversed(schemas):
        try:
            schema.drop(prompt=False)
        except Exception:
            pass


@pytest.fixture(scope="function")
def schema_by_backend(make_schema):
    return make_schema()


def regrow(master, **parts):
    """
    Return a new class for ``master``'s table that adds part tables to it, as
    when parts are added to a pipeline that is already declared.

    Each keyword names a part and gives its definition.
    """
    namespace = {"definition": master.definition}
    for name, definition in parts.items():
        namespace[name] = type(name, (dj.Part,), {"definition": definition})
    return type(master.__name__, master.__bases__, namespace)


def master_key_warnings(record):
    return [w for w in record if "does not inherit the primary key of its master" in str(w.message)]


# ---------------------------------------------------------------------------
# Refused declarations
# ---------------------------------------------------------------------------


def test_part_referencing_a_descendant_of_its_master_is_refused(schema_by_backend):
    """Master -> X -> Master.Part: X references the master, the part references X."""
    schema = schema_by_backend

    @schema
    class Master(dj.Manual):
        definition = """
        master_id : int32
        """

    @schema
    class X(dj.Manual):
        definition = """
        -> Master
        x_id : int32
        """

    Grown = regrow(
        Master,
        Part="""
        -> master
        -> X
        """,
    )
    with pytest.raises(DataJointError) as error:
        schema(Grown)

    part, master, x = Grown.Part.full_table_name, Master.full_table_name, X.full_table_name
    message = str(error.value)
    assert f"Part table {part} cannot reference {x}, which depends on the part's master {master}" in message
    assert f"{master} -> {x} -> {part} -> {master}" in message

    # Refused before any DDL ran: the part does not exist, and the schema still sorts.
    assert not Grown.Part().is_declared
    assert set(schema.list_tables()) == {Master.table_name, X.table_name}
    Master.insert1({"master_id": 1})
    X.insert1({"master_id": 1, "x_id": 1})
    Master.delete()
    assert len(X()) == 0


@pytest.mark.parametrize("reference", ["A", "A.Part"])
def test_parts_referencing_each_others_masters_are_refused(schema_by_backend, reference):
    """A.Part references B; a part of B that references A (or A.Part) closes the cycle."""
    schema = schema_by_backend

    @schema
    class B(dj.Manual):
        definition = """
        b_id : int32
        """

    @schema
    class A(dj.Manual):
        definition = """
        a_id : int32
        """

        class Part(dj.Part):
            definition = """
            -> master
            -> B
            """

    Grown = regrow(
        B,
        Part=f"""
        -> master
        -> {reference}
        """,
    )
    with pytest.raises(DataJointError) as error:
        schema(Grown)

    part, b, a, a_part = Grown.Part.full_table_name, B.full_table_name, A.full_table_name, A.Part.full_table_name
    message = str(error.value)
    if reference == "A":
        assert f"Part table {part} cannot reference {a}" in message
        assert f"{b} -> {a_part} -> {a} -> {part} -> {b}" in message
    else:
        assert f"Part table {part} cannot reference {a_part}" in message
        assert f"{b} -> {a_part} -> {part} -> {b}" in message
    assert not Grown.Part().is_declared


def test_cycle_across_schemas_is_refused(make_schema):
    """The table that depends on the master lives in another schema."""
    upstream, downstream = make_schema(), make_schema()

    @upstream
    class Master(dj.Manual):
        definition = """
        master_id : int32
        """

    @downstream
    class Analysis(dj.Manual):
        definition = """
        -> Master
        analysis_id : int32
        """

    @downstream
    class Report(dj.Manual):
        definition = """
        -> Analysis
        """

    Grown = regrow(
        Master,
        Summary="""
        -> master
        -> Report
        """,
    )
    with pytest.raises(DataJointError) as error:
        upstream(Grown)
    master, analysis, report = Master.full_table_name, Analysis.full_table_name, Report.full_table_name
    assert f"{master} -> {analysis} -> {report} -> {Grown.Summary.full_table_name} -> {master}" in str(error.value)
    assert not Grown.Summary().is_declared


# ---------------------------------------------------------------------------
# Allowed declarations
# ---------------------------------------------------------------------------


def test_allowed_part_references(schema_by_backend):
    """A part may reference its master, a sibling, an upstream table, or a table downstream of another group."""
    schema = schema_by_backend

    @schema
    class Probe(dj.Lookup):
        definition = """
        probe_id : int32
        """
        contents = [(1,), (2,)]

    @schema
    class Other(dj.Manual):
        definition = """
        other_id : int32
        """

        class Item(dj.Part):
            definition = """
            -> master
            item_id : int32
            """

    @schema
    class OtherSummary(dj.Manual):
        definition = """
        -> Other
        """

    with warnings.catch_warnings(record=True) as record:
        warnings.simplefilter("always")

        @schema
        class Session(dj.Manual):
            definition = """
            session_id : int32
            """

            class Channel(dj.Part):
                definition = """
                -> master
                channel_id : int32
                ---
                -> Probe
                """

            class Spike(dj.Part):
                definition = """
                -> master.Channel
                spike_id : int32
                """

            class Link(dj.Part):
                definition = """
                -> master
                -> OtherSummary
                -> Other.Item
                """

    assert not master_key_warnings(record)
    assert {Session.Channel.table_name, Session.Spike.table_name, Session.Link.table_name} <= set(schema.list_tables())

    Other.insert1({"other_id": 1})
    Other.Item.insert1({"other_id": 1, "item_id": 1})
    OtherSummary.insert1({"other_id": 1})
    Session.insert1({"session_id": 1})
    Session.Channel.insert1({"session_id": 1, "channel_id": 1, "probe_id": 1})
    Session.Spike.insert1({"session_id": 1, "channel_id": 1, "spike_id": 1})
    Session.Link.insert1({"session_id": 1, "other_id": 1, "item_id": 1})

    trace = dj.Diagram.trace(Session.Link & {"session_id": 1})
    assert len(trace[Other]) == 1
    assert dj.Diagram.cascade(Other & {"other_id": 1}).counts()[Session.Link.full_table_name] == 1
    (Session & {"session_id": 1}).delete()
    assert len(Session.Spike()) == len(Session.Link()) == 0
    (Other & {"other_id": 1}).delete()
    assert len(OtherSummary()) == 0


# ---------------------------------------------------------------------------
# Cycles declared before the rule
# ---------------------------------------------------------------------------


def test_legacy_cycle_is_named_and_contained(schema_by_backend, monkeypatch):
    schema = schema_by_backend

    @schema
    class Master(dj.Manual):
        definition = """
        master_id : int32
        """

    @schema
    class X(dj.Manual):
        definition = """
        -> Master
        x_id : int32
        """

    @schema
    class Unrelated(dj.Manual):
        definition = """
        unrelated_id : int32
        """

    @schema
    class UnrelatedChild(dj.Manual):
        definition = """
        -> Unrelated
        child_id : int32
        """

    # Declare the cycle the way a schema declared before the rule would hold it.
    monkeypatch.setattr(dj.user_tables, "group_cycle", lambda graph, part, parents: None)
    Cyclic = regrow(
        Master,
        Part="""
        -> master
        -> X
        """,
    )
    schema(Cyclic)
    monkeypatch.undo()
    assert Cyclic.Part().is_declared

    Master.insert1({"master_id": 1})
    X.insert1({"master_id": 1, "x_id": 1})
    Cyclic.Part.insert1({"master_id": 1, "x_id": 1})
    Unrelated.insert1({"unrelated_id": 1})
    UnrelatedChild.insert1({"unrelated_id": 1, "child_id": 1})

    master, x, part = Master.full_table_name, X.full_table_name, Cyclic.Part.full_table_name
    cycle = f"{master} -> {x} -> {part} -> {master}"

    # Operations that must order the cycle name it, instead of a networkx error.
    for operation in (
        lambda: Master.delete(),
        lambda: dj.Diagram.cascade(Master & {"master_id": 1}),
        lambda: dj.Diagram.trace(Cyclic.Part & {"master_id": 1}),
        lambda: schema.list_tables(),
    ):
        with pytest.raises(DataJointError) as error:
            operation()
        assert cycle in str(error.value)
        assert f"part table {part} references a table that depends on its own master" in str(error.value)
    assert len(Master()) == len(X()) == len(Cyclic.Part()) == 1

    # Operations elsewhere do not need to order the cycle and keep working.
    assert len(dj.Diagram.trace(UnrelatedChild & {"child_id": 1})[Unrelated]) == 1
    (Unrelated & {"unrelated_id": 1}).delete()
    assert len(Unrelated()) == len(UnrelatedChild()) == 0
    UnrelatedChild.drop()

    # Recovery, as the error suggests: drop the part and declare it without the reference.
    Cyclic.Part.drop(part_integrity="ignore")
    schema(
        regrow(
            Master,
            Part="""
            -> master
            x_id : int32
            """,
        )
    )
    assert set(schema.list_tables()) >= {Master.table_name, X.table_name, Cyclic.Part.table_name}
    Master.delete()
    assert len(Master()) == len(X()) == 0


# ---------------------------------------------------------------------------
# A part that does not inherit its master's primary key
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "definition",
    [
        """
        part_id : int32
        """,
        """
        -> master.proj(other_id="master_id")
        part_id : int32
        """,
        """
        part_id : int32
        ---
        -> [nullable] master
        """,
    ],
    ids=["no-reference", "renamed", "nullable"],
)
def test_part_without_master_key_warns(schema_by_backend, definition):
    schema = schema_by_backend

    @schema
    class Master(dj.Manual):
        definition = """
        master_id : int32
        """

    with pytest.warns(UserWarning, match="does not inherit the primary key of its master") as record:
        schema(regrow(Master, Part=definition))
    assert Master.full_table_name in str(master_key_warnings(record)[0].message)


def test_part_with_master_key_through_secondary_reference_does_not_warn(schema_by_backend):
    schema = schema_by_backend

    with warnings.catch_warnings(record=True) as record:
        warnings.simplefilter("always")

        @schema
        class Master(dj.Manual):
            definition = """
            master_id : int32
            """

            class Part(dj.Part):
                definition = """
                part_id : int32
                ---
                -> master
                """

    assert not master_key_warnings(record)
