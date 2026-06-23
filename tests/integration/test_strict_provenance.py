"""
Integration tests for ``dj.config["strict_provenance"]`` (#1425).

Strict mode gates reads (``QueryExpression.cursor``) and writes
(``Table.insert``) inside ``make()`` to the declared upstream graph
and the target table + its Parts. Off by default; opt-in.
"""

import pytest

import datajoint as dj
from datajoint import DataJointError


@pytest.fixture
def strict_mode(connection_test):
    """Enable strict_provenance for the duration of one test."""
    config = connection_test._config
    previous = config.get("strict_provenance", False)
    config["strict_provenance"] = True
    try:
        yield
    finally:
        config["strict_provenance"] = previous


def test_strict_compliant_make_passes(prefix, connection_test, strict_mode):
    """A make() that reads via self.upstream and writes to self with key consistency runs cleanly."""
    schema = dj.Schema(f"{prefix}_strict_compliant", connection=connection_test)

    @schema
    class Subject(dj.Lookup):
        definition = """
        subject_id : int32
        ---
        name : varchar(64)
        """
        contents = [(1, "alice"), (2, "bob")]

    @schema
    class Greeting(dj.Computed):
        definition = """
        -> Subject
        ---
        greeting : varchar(128)
        """

        def make(self, key):
            name = self.upstream[Subject].fetch1("name")
            self.insert1({**key, "greeting": f"Hello, {name}!"})

    Greeting.populate()
    assert (Greeting & {"subject_id": 1}).fetch1("greeting") == "Hello, alice!"
    assert (Greeting & {"subject_id": 2}).fetch1("greeting") == "Hello, bob!"


def test_strict_blocks_read_from_undeclared_table(prefix, connection_test, strict_mode):
    """Reading from a table NOT in the trace's ancestor set raises."""
    schema = dj.Schema(f"{prefix}_strict_undeclared", connection=connection_test)

    @schema
    class Subject(dj.Lookup):
        definition = """
        subject_id : int32
        """
        contents = [(1,)]

    @schema
    class Unrelated(dj.Lookup):
        definition = """
        u_id : int32
        ---
        secret : varchar(64)
        """
        contents = [(42, "should-not-read")]

    captured: list[Exception] = []

    @schema
    class Bad(dj.Computed):
        definition = """
        -> Subject
        ---
        val : int32
        """

        def make(self, key):
            try:
                Unrelated.fetch()  # not in declared upstream of Bad
            except DataJointError as e:
                captured.append(e)
            # Insert anyway so populate doesn't fail
            self.insert1({**key, "val": 0})

    Bad.populate()
    assert len(captured) == 1
    assert "strict_provenance" in str(captured[0]).lower()
    assert "undeclared" in str(captured[0]).lower()


def test_strict_blocks_write_to_other_table(prefix, connection_test, strict_mode):
    """Writing into a table other than self / self.Parts raises."""
    schema = dj.Schema(f"{prefix}_strict_other_target", connection=connection_test)

    @schema
    class Subject(dj.Lookup):
        definition = """
        subject_id : int32
        """
        contents = [(1,)]

    @schema
    class AuditLog(dj.Manual):
        definition = """
        log_id : int32
        ---
        event : varchar(64)
        """

    captured: list[Exception] = []

    @schema
    class Derived(dj.Computed):
        definition = """
        -> Subject
        ---
        val : int32
        """

        def make(self, key):
            try:
                AuditLog.insert1({"log_id": 1, "event": "side-effect"}, allow_direct_insert=True)
            except DataJointError as e:
                captured.append(e)
            self.insert1({**key, "val": 1})

    Derived.populate()
    assert len(captured) == 1
    assert "strict_provenance" in str(captured[0]).lower()
    assert "not permitted" in str(captured[0]).lower()


def test_strict_blocks_write_with_mismatched_key(prefix, connection_test, strict_mode):
    """Writing a row whose PK columns disagree with the current key raises."""
    schema = dj.Schema(f"{prefix}_strict_key_mismatch", connection=connection_test)

    @schema
    class Subject(dj.Lookup):
        definition = """
        subject_id : int32
        """
        contents = [(1,), (2,)]

    captured: list[Exception] = []

    @schema
    class Wrong(dj.Computed):
        definition = """
        -> Subject
        ---
        val : int32
        """

        def make(self, key):
            try:
                # Try to insert a row for a DIFFERENT subject than the current key
                bogus_key = {"subject_id": 99}
                self.insert1({**bogus_key, "val": 0})
            except DataJointError as e:
                captured.append(e)
            # Insert correctly to let populate complete
            self.insert1({**key, "val": 1})

    Wrong.populate()
    assert len(captured) == 2  # fires for both subjects
    assert all("does not match the current make() key" in str(e) for e in captured)


def test_strict_writes_to_part_table_pass(prefix, connection_test, strict_mode):
    """Writing into self.Parts (with key consistency) is allowed."""
    schema = dj.Schema(f"{prefix}_strict_parts", connection=connection_test)

    @schema
    class Subject(dj.Lookup):
        definition = """
        subject_id : int32
        """
        contents = [(1,)]

    @schema
    class Master(dj.Computed):
        definition = """
        -> Subject
        ---
        summary : varchar(32)
        """

        class Bin(dj.Part):
            definition = """
            -> master
            bin_id : int32
            ---
            energy : float64
            """

        def make(self, key):
            self.insert1({**key, "summary": "ok"})
            self.Bin.insert([{**key, "bin_id": i, "energy": float(i)} for i in range(3)])

    Master.populate()
    assert (Master & {"subject_id": 1}).fetch1("summary") == "ok"
    assert len(Master.Bin & {"subject_id": 1}) == 3


def test_strict_off_by_default_no_change(prefix, connection_test):
    """With strict_provenance unset (default False), existing patterns work unchanged."""
    schema = dj.Schema(f"{prefix}_strict_default_off", connection=connection_test)

    @schema
    class Subject(dj.Lookup):
        definition = """
        subject_id : int32
        """
        contents = [(1,)]

    @schema
    class DerivedLegacy(dj.Computed):
        definition = """
        -> Subject
        ---
        val : int32
        """

        def make(self, key):
            # Direct ancestor fetch — would be flagged in strict mode (read from
            # undeclared, but Subject IS an ancestor — actually allowed under
            # the current "table in allowed set" rule even in strict mode).
            # In default-off mode, this must work either way.
            (Subject & key).fetch1("subject_id")
            self.insert1({**key, "val": 0})

    # No strict_mode fixture — default-off
    DerivedLegacy.populate()
    assert (DerivedLegacy & {"subject_id": 1}).fetch1("val") == 0
