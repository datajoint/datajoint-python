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


def test_strict_generator_insert_not_dropped(prefix, connection_test, strict_mode):
    """Regression (#1474 bug 1): a one-shot generator of compliant rows must not
    be consumed by the write gate. Before the fix, assert_write_allowed iterated
    `rows` for its key check, exhausting the generator so insert saw zero rows and
    silently wrote nothing."""
    schema = dj.Schema(f"{prefix}_strict_generator", connection=connection_test)

    @schema
    class Subject(dj.Lookup):
        definition = """
        subject_id : int32
        """
        contents = [(1,), (2,)]

    @schema
    class Spectrum(dj.Computed):
        definition = """
        -> Subject
        ---
        n : int32
        """

        class Bin(dj.Part):
            definition = """
            -> master
            bin_id : int32
            ---
            energy : float64
            """

        def make(self, key):
            n = 5
            self.insert1({**key, "n": n})
            # one-shot generator (not a list) — must survive the write gate
            self.Bin.insert({**key, "bin_id": i, "energy": float(i)} for i in range(n))

    Spectrum.populate()
    for sid in (1, 2):
        assert (Spectrum & {"subject_id": sid}).fetch1("n") == 5
        # The core assertion: all 5 generated rows landed, none silently dropped.
        assert len(Spectrum.Bin & {"subject_id": sid}) == 5


def test_strict_generator_insert_mismatched_key_still_caught(prefix, connection_test, strict_mode):
    """The per-row key check still fires when rows come from a generator — a row
    whose key disagrees with the current make() key raises, not silently passes."""
    schema = dj.Schema(f"{prefix}_strict_gen_mismatch", connection=connection_test)

    @schema
    class Subject(dj.Lookup):
        definition = """
        subject_id : int32
        """
        contents = [(1,)]

    @schema
    class Derived(dj.Computed):
        definition = """
        -> Subject
        ---
        val : int32
        """

        class Bin(dj.Part):
            definition = """
            -> master
            bin_id : int32
            """

        def make(self, key):
            self.insert1({**key, "val": 0})
            # generator whose 3rd row carries a bogus subject_id
            self.Bin.insert({**({**key, "subject_id": 999} if i == 2 else key), "bin_id": i} for i in range(4))

    with pytest.raises(DataJointError, match="does not match the current make"):
        Derived.populate()


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


def test_strict_blocks_update1_on_other_table(prefix, connection_test, strict_mode):
    """update1 is a write: under strict mode, updating a table that is not
    self or one of its Parts raises, and the target row is left unmodified.
    Regression for the ungated-update1 hole found in the 2.3 post-release audit."""
    schema = dj.Schema(f"{prefix}_strict_update1_other", connection=connection_test)

    @schema
    class Subject(dj.Lookup):
        definition = """
        subject_id : int32
        """
        contents = [(1,)]

    @schema
    class SideResult(dj.Manual):
        definition = """
        side_id : int32
        ---
        val : int32
        """

    SideResult.insert1({"side_id": 1, "val": 100})

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
                SideResult.update1({"side_id": 1, "val": 999})
            except DataJointError as e:
                captured.append(e)
            self.insert1({**key, "val": 1})

    Derived.populate()
    assert len(captured) == 1
    assert "update1 on" in str(captured[0])
    assert "not permitted" in str(captured[0])
    # The side table must be untouched.
    assert (SideResult & {"side_id": 1}).fetch1("val") == 100


def test_strict_blocks_update1_with_mismatched_key(prefix, connection_test, strict_mode):
    """update1 on self with key columns that disagree with the current make()
    key raises the key-consistency error (before any existence check)."""
    schema = dj.Schema(f"{prefix}_strict_update1_key", connection=connection_test)

    @schema
    class Subject(dj.Lookup):
        definition = """
        subject_id : int32
        """
        contents = [(1,)]

    captured: list[Exception] = []

    @schema
    class Derived(dj.Computed):
        definition = """
        -> Subject
        ---
        val : int32
        """

        def make(self, key):
            self.insert1({**key, "val": 1})
            try:
                # bogus key — must be rejected by the provenance key check,
                # not by the "one existing entry" check
                self.update1({"subject_id": 999, "val": 2})
            except DataJointError as e:
                captured.append(e)

    Derived.populate()
    assert len(captured) == 1
    assert "updated row's" in str(captured[0])
    assert "does not match the current make() key" in str(captured[0])


def test_strict_update1_on_self_with_matching_key_allowed(prefix, connection_test, strict_mode):
    """update1 on self with a key-consistent row is permitted under strict mode
    (corrective update within the provenance boundary)."""
    schema = dj.Schema(f"{prefix}_strict_update1_self", connection=connection_test)

    @schema
    class Subject(dj.Lookup):
        definition = """
        subject_id : int32
        """
        contents = [(1,)]

    @schema
    class Derived(dj.Computed):
        definition = """
        -> Subject
        ---
        val : int32
        """

        def make(self, key):
            self.insert1({**key, "val": 1})
            self.update1({**key, "val": 2})

    Derived.populate()
    assert (Derived & {"subject_id": 1}).fetch1("val") == 2


def test_update1_unchanged_without_strict(prefix, connection_test):
    """With strict_provenance off (default), update1 from inside make() behaves
    as before — no gate fires."""
    schema = dj.Schema(f"{prefix}_update1_default_off", connection=connection_test)

    @schema
    class Subject(dj.Lookup):
        definition = """
        subject_id : int32
        """
        contents = [(1,)]

    @schema
    class SideResult(dj.Manual):
        definition = """
        side_id : int32
        ---
        val : int32
        """

    SideResult.insert1({"side_id": 1, "val": 100})

    @schema
    class DerivedLegacy(dj.Computed):
        definition = """
        -> Subject
        ---
        val : int32
        """

        def make(self, key):
            SideResult.update1({"side_id": 1, "val": 200})
            self.insert1({**key, "val": 0})

    DerivedLegacy.populate()
    assert (SideResult & {"side_id": 1}).fetch1("val") == 200
