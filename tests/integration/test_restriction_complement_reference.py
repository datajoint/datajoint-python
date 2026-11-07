"""
Restriction by a query expression and its complement, checked against a Python reference.

The reference matches a row of A to a row of the operand on every shared attribute name,
and ``None`` never matches. The keys of ``A & B`` and ``A - B`` must be disjoint and
together make up the keys of A, and each must agree with the reference. Since the
reference does not depend on the backend, both backends agree with each other wherever
they agree with it.

The writes at the end (``populate`` with reserved jobs, cascading deletes and
``insert``) take their rows from complements of restrictions.
"""

import time
from collections import namedtuple

import pytest

import datajoint as dj


@pytest.fixture(scope="function")
def schema_by_backend(connection_by_backend, db_creds_by_backend):
    backend = db_creds_by_backend["backend"]
    schema_name = f"djtest_complref_{backend}_{str(int(time.time() * 1000))[-8:]}"
    schema = dj.Schema(schema_name, connection=connection_by_backend)
    yield schema
    schema.drop(prompt=False)


def key_set(query, primary_key):
    """The primary keys of ``query`` as a set of tuples ordered by ``primary_key``."""
    return {tuple(key[k] for k in primary_key) for key in query.keys()}


def assert_partition(A, B):
    """
    The keys of ``A & B`` and ``A - B`` are disjoint and together are the keys of A.

    Returns the two key sets.
    """
    primary_key = A.primary_key
    inside, outside = key_set(A & B, primary_key), key_set(A - B, primary_key)
    assert not inside & outside, f"keys in both A & B and A - B: {sorted(inside & outside, key=repr)}"
    assert inside | outside == key_set(A, primary_key)
    return inside, outside


# ---------- Python reference ----------

Ref = namedtuple("Ref", "names rows")  # attribute names and rows (dicts) of a query expression


def ref(query, rows=None):
    """Reference operand for ``query``, with its rows from the database unless given."""
    return Ref(frozenset(query.heading.names), query.to_dicts() if rows is None else rows)


def ref_matches(row, operand):
    """
    Whether ``row`` (a dict) satisfies the reference ``operand``.

    An operand is a ``Ref``, or ``("any", items)``, ``("all", items)`` or ``("not", item)``.
    """
    if isinstance(operand, Ref):
        shared = [name for name in operand.names if name in row]
        if not shared:
            return bool(operand.rows)
        return any(all(row[a] is not None and row[a] == other[a] for a in shared) for other in operand.rows)
    kind, items = operand
    if kind == "any":
        return any(ref_matches(row, item) for item in items)
    if kind == "all":
        return all(ref_matches(row, item) for item in items)
    assert kind == "not"
    return not ref_matches(row, items)


def ref_has_null(operand, names):
    """Whether any row of the reference ``operand`` holds NULL in one of ``names``."""
    if isinstance(operand, Ref):
        return any(row[a] is None for row in operand.rows for a in operand.names & names)
    kind, items = operand
    return any(ref_has_null(item, names) for item in (items if kind != "not" else [items]))


def ref_restrict(rows, operand):
    return [row for row in rows if ref_matches(row, operand)]


def ref_complement(rows, operand):
    return [row for row in rows if not ref_matches(row, operand)]


# ---------- Operand matrix ----------

TAGS = [None, "a", "b", "c"]
COLORS = [None, "r", "g"]
RIGHT = [
    (0, "a", "r"),
    (1, None, "r"),
    (2, "b", None),
    (3, None, None),
    (4, "c", "g"),
    (5, "a", None),
    (6, "b", "g"),
    (7, None, "g"),
    (8, "a", "g"),
    (9, "b", "r"),
]
MARKS = [(0, 0), (1, None), (2, 3), (3, 4), (4, None), (5, 7), (6, 9), (7, 2)]


@pytest.fixture(params=["left", "right", "both"])
def nulls(request):
    """Where the shared nullable attributes ``tag`` and ``color`` hold NULLs."""
    return request.param


@pytest.fixture
def matrix(schema_by_backend, nulls):
    schema = schema_by_backend

    @schema
    class Tag(dj.Lookup):
        definition = """
        tag : varchar(8)
        """
        contents = [("a",), ("b",), ("c",)]

    @schema
    class Color(dj.Lookup):
        definition = """
        color : varchar(8)
        """
        contents = [("r",), ("g",)]

    @schema
    class Left(dj.Manual):
        definition = """
        left_id : int32
        ---
        -> [nullable] Tag
        -> [nullable] Color
        """

    @schema
    class Right(dj.Manual):
        definition = """
        right_id : int32
        ---
        -> [nullable] Tag
        -> [nullable] Color
        """

    @schema
    class Note(dj.Manual):
        definition = """
        -> Right
        note : int32
        """

    @schema
    class Mark(dj.Manual):
        definition = """
        mark_id : int32
        ---
        -> [nullable] Right
        """

    left = [(t, c) for t in TAGS for c in COLORS]
    if nulls == "right":
        left = [(t, c) for t, c in left if t is not None and c is not None]
    Left.insert([(i, t, c) for i, (t, c) in enumerate(left)])
    right = RIGHT
    if nulls == "left":
        right = [r for r in right if None not in r]
    Right.insert(right)
    right_ids = {r[0] for r in right}
    Note.insert([(i, n) for i in right_ids for n in range(i % 3)])
    # a mark without a right row is NULL-filled by extend(), except when only the left has NULLs
    Mark.insert([(m, r) for m, r in MARKS if r in right_ids or (r is None and nulls != "left")])
    return dict(Left=Left, Right=Right, Note=Note, Mark=Mark)


def build_operand(name, Right, Note, Mark):
    """The operand ``name`` as a query and as its reference."""
    if name == "base":
        return Right, ref(Right)
    if name == "join":
        q = Right * Note
        return q, ref(q)
    if name == "aggregation":
        q = Right.aggr(Note, "tag", "color", n="count(*)") & "n > 0"
        return q, ref(q)
    if name == "u":
        q = dj.U("tag") & (Right & "right_id > 4")
        return q, ref(q)
    if name == "union":
        q = (dj.U("tag", "color") & (Right & "right_id < 4")) + (dj.U("tag", "color") & (Right & "right_id > 6"))
        return q, ref(q)
    if name == "top":
        q = Right & dj.Top(4, order_by="right_id")
        return q, ref(q)
    if name == "nested":
        # A - (B - C), the inner complement matched on the nullable color
        C = dj.U("color") & (Right & "right_id in (0, 3)")
        inner = Right - C
        return inner, ref(inner, rows=ref_complement(Right.to_dicts(), ref(C)))
    B, C = Right & "right_id < 5", dj.U("tag") & (Right & "right_id > 5")
    if name == "or_list":
        return [B, C], ("any", [ref(B), ref(C)])
    if name == "and_list":
        return dj.AndList([B, C]), ("all", [ref(B), ref(C)])
    if name == "not_list":
        return dj.Not([B, C]), ("not", ("any", [ref(B), ref(C)]))
    if name == "extend":
        q = Mark.extend(Right)
        return q, ref(q)
    raise ValueError(name)


@pytest.mark.parametrize(
    "operand",
    ["base", "join", "aggregation", "u", "union", "top", "nested", "or_list", "and_list", "not_list", "extend"],
)
def test_matches_reference(matrix, nulls, operand):
    """``A & B`` and ``A - B`` partition A and agree with the Python reference."""
    Left = matrix["Left"]
    B, reference = build_operand(operand, matrix["Right"], matrix["Note"], matrix["Mark"])
    rows = Left.to_dicts()
    pk = Left.primary_key
    # the NULL layout under test is really present on each side
    shared = {"tag", "color"}
    assert any(r[a] is None for r in rows for a in shared) == (nulls != "right")
    assert ref_has_null(reference, shared) == (nulls != "left")
    expected_inside = {tuple(r[k] for k in pk) for r in ref_restrict(rows, reference)}
    expected_outside = {tuple(r[k] for k in pk) for r in ref_complement(rows, reference)}
    assert expected_inside and expected_outside, "the data must exercise both sides"

    assert key_set(Left - B, pk) == expected_outside
    assert key_set(Left & dj.Not(B), pk) == expected_outside
    if operand == "top" and Left.connection.adapter.backend == "mysql":
        pytest.skip("positive restriction by a LIMIT query fails on MySQL (error 1235), see #1657")
    inside, outside = assert_partition(Left, B)
    assert inside == expected_inside
    assert outside == expected_outside
    assert key_set(Left - dj.Not(B), pk) == expected_inside


# ---------- populate with reserved jobs ----------


def _statuses(jobs, attr):
    return {row[attr]: row["status"] for row in jobs.proj("status").to_dicts()}


def test_populate_reserved_aggregation_key_source(schema_by_backend):
    """Jobs refresh removes jobs whose key left an aggregated key_source."""
    schema = schema_by_backend

    @schema
    class Raw(dj.Manual):
        definition = """
        raw_id : int32
        """

    @schema
    class Sample(dj.Manual):
        definition = """
        -> Raw
        sample : int32
        """

    @schema
    class Summary(dj.Computed):
        definition = """
        -> Raw
        ---
        n : int32
        """

        @property
        def key_source(self):
            return Raw.aggr(Sample, n="count(*)", exclude_nonmatching=True) & "n > 1"

        def make(self, key):
            self.insert1(dict(key, n=len(Sample & key)))

    n_samples = {0: 2, 1: 3, 2: 2, 3: 1, 4: 0, 5: 2}
    Raw.insert([(i,) for i in n_samples])
    Sample.insert([(i, s) for i, n in n_samples.items() for s in range(n)])
    assert set(Summary.key_source.fetch("raw_id")) == {0, 1, 2, 5}

    with dj.config.override(jobs={"keep_completed": True, "stale_timeout": 0}):
        Summary.populate("raw_id < 2", reserve_jobs=True)
        Summary.jobs.refresh()
        assert Summary.jobs.reserve({"raw_id": 5})
        Summary.jobs.error({"raw_id": 5}, "failed")
        assert _statuses(Summary.jobs, "raw_id") == {0: "success", 1: "success", 2: "pending", 5: "error"}

        (Raw & {"raw_id": 1}).delete(prompt=False)  # gone with its Summary row
        (Sample & {"raw_id": 2, "sample": 1}).delete(prompt=False)  # pending job, key left key_source
        (Sample & {"raw_id": 5}).delete(prompt=False)  # error record, key left key_source
        Sample.insert1((3, 1))  # enters key_source
        assert set(Summary.key_source.fetch("raw_id")) == {0, 3}

        result = Summary.populate(reserve_jobs=True)

    assert result["success_count"] == 1
    assert _statuses(Summary.jobs, "raw_id") == {0: "success", 3: "success"}
    assert dict(zip(*Summary.fetch("raw_id", "n"))) == {0: 2, 3: 2}


def test_populate_reserved_nullable_key_source(schema_by_backend):
    """A key_source restricted on a nullable attribute; NULL never matches the operand."""
    schema = schema_by_backend

    @schema
    class Tag(dj.Lookup):
        definition = """
        tag : varchar(8)
        ---
        color = null : varchar(8)
        """
        contents = [("a", "red"), ("b", None), ("c", "red")]

    @schema
    class Raw(dj.Manual):
        definition = """
        raw_id : int32
        ---
        -> [nullable] Tag
        """

    @schema
    class Proc(dj.Computed):
        definition = """
        -> Raw
        """

        @property
        def key_source(self):
            # raws whose tag has no color, by a complement matched on the nullable tag
            return Raw - (Tag & "color IS NULL")

        def make(self, key):
            self.insert1(key)

    Raw.insert([(0, "a"), (1, "b"), (2, "c"), (3, None), (4, "a"), (5, None)])
    assert set(Proc.key_source.fetch("raw_id")) == {0, 2, 3, 4, 5}

    with dj.config.override(jobs={"stale_timeout": 0}):
        Proc.jobs.refresh()
        assert Proc.jobs.reserve({"raw_id": 4})  # held by another worker
        assert Proc.jobs.reserve({"raw_id": 5})
        Proc.jobs.error({"raw_id": 5}, "failed")
        assert _statuses(Proc.jobs, "raw_id") == {0: "pending", 2: "pending", 3: "pending", 4: "reserved", 5: "error"}

        Raw.update1(dict(raw_id=3, tag="b"))  # pending job, key left key_source
        Raw.update1(dict(raw_id=5, tag="b"))  # error record, key left key_source
        Raw.update1(dict(raw_id=1, tag=None))  # enters key_source
        assert set(Proc.key_source.fetch("raw_id")) == {0, 1, 2, 4}

        result = Proc.populate(reserve_jobs=True)

    assert result["success_count"] == 3
    assert _statuses(Proc.jobs, "raw_id") == {4: "reserved"}
    assert set(Proc.fetch("raw_id")) == {0, 1, 2}


# ---------- cascading delete and insert ----------


@pytest.fixture
def lab(schema_by_backend):
    schema = schema_by_backend

    @schema
    class Tag(dj.Lookup):
        definition = """
        tag : varchar(8)
        ---
        color = null : varchar(8)
        """
        contents = [("a", "red"), ("b", None)]

    @schema
    class Animal(dj.Manual):
        definition = """
        animal_id : int32
        ---
        -> [nullable] Tag
        """

    @schema
    class Site(dj.Manual):
        definition = """
        site_id : int32
        ---
        -> [nullable] Animal
        """

    @schema
    class Recording(dj.Manual):
        definition = """
        -> Animal
        rec : int32
        """

    @schema
    class Unit(dj.Manual):
        definition = """
        -> Recording
        -> Site
        unit : int32
        """

    @schema
    class Batch(dj.Manual):
        definition = """
        batch_id : int32
        ---
        -> [nullable] Tag
        """

        class Item(dj.Part):
            definition = """
            -> master
            item : int32
            """

    @schema
    class Label(dj.Manual):
        definition = """
        -> Batch.Item
        label : int32
        """

    Animal.insert(enumerate(["a", "b", None, "a", None, "b"]))
    Site.insert(enumerate([0, 1, None, 3, 4, None]))
    Recording.insert([(i, r) for i in range(6) for r in range(1 + i % 2)])
    Unit.insert([(i, r, s, 0) for i, r in zip(*Recording.fetch("animal_id", "rec")) for s in {(i + r) % 6, (i + 2) % 6}])
    Batch.insert(enumerate(["a", None, "b", "a", None, "b", "a"]))
    Batch.Item.insert([(b, k) for b in range(7) for k in range(b % 3 + 1)])
    Label.insert([(b, k, 0) for b, k in zip(*Batch.Item.fetch("batch_id", "item")) if k != 1])
    return dict(Tag=Tag, Animal=Animal, Site=Site, Recording=Recording, Unit=Unit, Batch=Batch, Label=Label)


def test_cascade_to_child_of_two_restricted_parents(lab):
    """The cascade restricts Unit by an OR of its two restricted parents, Recording and Site."""
    Tag, Animal, Site, Recording, Unit = (lab[k] for k in ("Tag", "Animal", "Site", "Recording", "Unit"))
    red = Tag & "color = 'red'"
    keep = {a for (a,) in key_set(Animal & red, ["animal_id"])}
    doomed = {r["animal_id"] for r in Animal.to_dicts()} - keep
    # NULL tags never match, so animals 2 and 4 are in the complement
    assert doomed == {1, 2, 4, 5}
    doomed_sites = {r["site_id"] for r in Site.to_dicts() if r["animal_id"] in doomed}
    units = Unit.to_dicts()
    # each parent alone removes units that the other parent keeps
    assert any(u["animal_id"] in keep and u["site_id"] in doomed_sites for u in units)
    assert any(u["animal_id"] in doomed and u["site_id"] not in doomed_sites for u in units)
    expected_units = {
        (u["animal_id"], u["rec"], u["site_id"], u["unit"])
        for u in units
        if u["animal_id"] not in doomed and u["site_id"] not in doomed_sites
    }

    (Animal - red).delete(prompt=False)

    assert {a for (a,) in key_set(Animal, ["animal_id"])} == keep
    assert {r["animal_id"] for r in Recording.to_dicts()} == keep
    assert {r["site_id"] for r in Site.to_dicts()} == set(range(6)) - doomed_sites
    assert key_set(Unit, ["animal_id", "rec", "site_id", "unit"]) == expected_units


def test_delete_complement_with_parts(lab):
    """``(A - B).delete()`` deletes exactly the complement and its part and child rows."""
    Animal, Batch, Label = lab["Animal"], lab["Batch"], lab["Label"]
    B = dj.U("tag") & (Animal & "animal_id in (0, 2, 3)")  # tags a and NULL
    assert None in B.fetch("tag")
    inside, outside = assert_partition(Batch, B)
    # batches with a NULL tag never match, also when the operand holds NULL
    assert {b for (b,) in outside} == {1, 2, 4, 5} and {b for (b,) in inside} == {0, 3, 6}
    kept = (Batch & B).proj()
    items_kept = key_set(Batch.Item & kept, ["batch_id", "item"])
    labels_kept = key_set(Label & kept, ["batch_id", "item", "label"])
    assert len(Batch.Item()) > len(items_kept) and len(Label()) > len(labels_kept) > 0

    (Batch - B).delete(prompt=False)

    assert key_set(Batch, ["batch_id"]) == inside
    assert key_set(Batch.Item, ["batch_id", "item"]) == items_kept
    assert key_set(Label, ["batch_id", "item", "label"]) == labels_kept


def test_delete_complement_by_same_table(lab):
    """The operand may read the table being deleted from (MySQL error 1093 otherwise)."""
    Batch = lab["Batch"]
    B = dj.U("tag") & (Batch & "batch_id < 2")  # tags a and NULL
    inside, outside = assert_partition(Batch, B)
    assert {b for (b,) in outside} == {1, 2, 4, 5}
    (Batch - B).delete(prompt=False)
    assert key_set(Batch, ["batch_id"]) == inside


def test_insert_missing_rows(schema_by_backend):
    """``T.insert(Src - T)`` inserts exactly the rows of Src that T lacks."""
    schema = schema_by_backend

    @schema
    class Tag(dj.Lookup):
        definition = """
        tag : varchar(8)
        """
        contents = [("a",), ("b",)]

    @schema
    class Src(dj.Manual):
        definition = """
        src_id : int32
        ---
        -> [nullable] Tag
        """

    @schema
    class Copy(dj.Manual):
        definition = """
        -> Src
        ---
        -> [nullable] Tag
        """

    def contents(table):
        return {(r["src_id"], r["tag"]) for r in table.to_dicts()}

    Src.insert(enumerate(["a", None, "b", None, "a", "b"]))
    Copy.insert((Src & "src_id in (0, 2)").to_dicts())
    assert len(Src - Copy) == 4
    Copy.insert(Src - Copy)
    assert contents(Copy) == contents(Src)

    # Matching on the key alone: the NULL tag of a copied row would never match
    (Copy & "src_id > 1").delete(prompt=False)
    assert contents(Copy) == {(0, "a"), (1, None)}
    assert 1 in (Src - Copy).fetch("src_id")
    Copy.insert(Src - Copy.proj())
    assert contents(Copy) == contents(Src)
