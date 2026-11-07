"""
The complement of a restriction by a query expression (``A - B``, ``A & dj.Not(B)``)
compiles to a correlated ``NOT EXISTS``.

Matching uses ``=`` on every shared attribute, so NULL matches nothing and ``A - B``
is the exact complement of ``A & B``, also when a shared attribute is nullable or the
operand contains NULLs (``NOT IN`` dropped such rows from both sides).
"""

import time

import pytest

import datajoint as dj


@pytest.fixture(scope="function")
def schema_by_backend(connection_by_backend, db_creds_by_backend):
    backend = db_creds_by_backend["backend"]
    schema_name = f"djtest_complement_{backend}_{str(int(time.time() * 1000))[-8:]}"
    schema = dj.Schema(schema_name, connection=connection_by_backend)
    yield schema
    schema.drop(prompt=False)


@pytest.fixture
def tables(schema_by_backend):
    schema = schema_by_backend

    @schema
    class Tag(dj.Lookup):
        definition = """
        tag : varchar(8)
        ---
        color = null : varchar(8)
        """
        contents = [("xa", "red"), ("xb", None), ("y%", "blue")]

    @schema
    class Subject(dj.Manual):
        definition = """
        subject_id : int32
        ---
        -> [nullable] Tag
        weight : int32
        """

    @schema
    class Session(dj.Manual):
        definition = """
        -> Subject
        session : int32
        ---
        -> [nullable] Tag
        """

    @schema
    class Scan(dj.Manual):
        definition = """
        -> Session
        scan : int32
        """

    @schema
    class SessionCount(dj.Computed):
        definition = """
        -> Subject
        ---
        n_sessions : int32
        """

        @property
        def key_source(self):
            return Subject - (Session & "session = 0 AND tag LIKE 'y%'")

        def make(self, key):
            self.insert1(dict(key, n_sessions=len(Session & key)))

    tag = [None, "xa", "xb", "y%"]
    Subject.insert([(i, tag[i % 4], i % 5) for i in range(24)])
    # subjects 20..23 have no sessions
    Session.insert([(i, s, tag[(i + s) % 4]) for i in range(20) for s in range(i % 3 + 1)])
    Scan.insert([(i, s, k) for i, s in zip(*Session.fetch("subject_id", "session")) for k in range(2) if (i + s) % 2])
    return Tag(), Subject(), Session(), Scan(), SessionCount()


def rows(query, attrs):
    """Distinct tuples of ``attrs`` (in this order) from ``query``."""
    return {tuple(row[a] for a in attrs) for row in query.proj(*attrs).to_dicts()}


def expected_anti(a_rows, b_rows):
    """Rows of A that match no row of B with SQL equality (NULL matches nothing)."""
    return {r for r in a_rows if not any(all(x is not None and x == y for x, y in zip(r, s)) for s in b_rows)}


def check_complement(A, B, attrs):
    """A & B and A - B partition A, and A - B agrees with SQL equality semantics."""
    keys = A.primary_key
    pos, neg = rows(A & B, keys), rows(A - B, keys)
    assert pos and neg, "the operand must split A"
    assert len(A & B) + len(A - B) == len(A)
    assert pos | neg == rows(A, keys) and not pos & neg
    assert rows(A & dj.Not(B), keys) == neg
    # A - B agrees with a direct evaluation on the matched attributes
    b_rows = rows(B, attrs)
    expected = {tuple(row[k] for k in keys) for row in A.to_dicts() if expected_anti({tuple(row[a] for a in attrs)}, b_rows)}
    assert neg == expected


def test_sql_form(tables):
    _, Subject, Session, _, _ = tables
    sql = (Subject - Session).make_sql()
    assert "NOT EXISTS" in sql
    assert " not in " not in sql.lower()
    # positive restriction still uses IN
    sql = (Subject & Session).make_sql()
    assert "EXISTS" not in sql and " in (" in sql


def test_complement_nullable_namesake(tables):
    """Subject and Session share subject_id and the nullable tag."""
    _, Subject, Session, _, _ = tables
    common = ["subject_id", "tag"]
    assert None in (Session & "tag IS NULL").fetch("tag")
    check_complement(Subject, Session, common)
    check_complement(Subject, Session.proj("tag"), common)
    check_complement(Session, Subject, common)


def test_null_in_operand(tables):
    """The operand's NULL tag must not empty the anti-restriction (NOT IN returned no rows)."""
    _, Subject, Session, _, _ = tables
    B = dj.U("tag") & (Session & "subject_id < 4")  # every tag, and NULL
    assert None in B.fetch("tag")
    check_complement(Subject, B, ["tag"])
    assert len(Subject - B) > 0
    # the operand holds every tag, so only subjects with a NULL tag remain
    assert rows(Subject & "tag IS NULL", ["subject_id"]) == rows(Subject - B, ["subject_id"])


@pytest.mark.parametrize(
    "operand",
    ["join", "restricted", "aggregation", "u", "union", "top", "complemented"],
)
def test_operand_shapes(tables, operand):
    _, Subject, Session, Scan, _ = tables
    B = {
        "join": lambda: Session.proj() * Scan,
        "restricted": lambda: Session.proj() & "session > 0",
        "aggregation": lambda: Subject.aggr(Session.proj(), n="count(*)") & "n > 1",
        "u": lambda: dj.U("subject_id") & (Scan & "scan = 1"),
        "union": lambda: (Session.proj() & "session = 1") + (Session.proj() & "session = 2"),
        "top": lambda: Session.proj() & dj.Top(7, order_by="subject_id DESC"),
        "complemented": lambda: Session.proj() - Scan,
    }[operand]()
    expected = expected_anti(rows(Subject, ["subject_id"]), rows(B, ["subject_id"]))
    assert expected and expected != rows(Subject, ["subject_id"]), "the operand must split Subject"
    assert rows(Subject - B, ["subject_id"]) == expected
    assert len(Subject - B) == len(expected)
    if operand == "top" and Subject.connection.adapter.backend == "mysql":
        pytest.skip("positive restriction by a LIMIT query fails on MySQL (error 1235), see #1657")
    assert len(Subject & B) + len(Subject - B) == len(Subject)


def test_union_operand(tables):
    """Restriction by a union compiles for both & and - (#1658)."""
    _, Subject, Session, _, _ = tables
    U = (Session.proj() & "session = 1") + (Session.proj() & "session = 2")
    with_sessions = set((Session & "session > 0").fetch("subject_id"))
    assert set((Subject & U).fetch("subject_id")) == with_sessions
    assert set((Subject - U).fetch("subject_id")) == set(Subject.fetch("subject_id")) - with_sessions
    # a union with secondary attributes compiles to a left join UNION an anti-restriction
    V = (Subject & "weight < 2").proj(w="weight") + (Subject & "weight > 3").proj(t="tag")
    picked = set((Subject & "weight < 2 OR weight > 3").fetch("subject_id"))
    assert set((Session & V).fetch("subject_id")) == picked & set(Session.fetch("subject_id"))
    assert set((Session - V).fetch("subject_id")) == set(Session.fetch("subject_id")) - picked
    assert len(Session & V) + len(Session - V) == len(Session)


def test_or_list_and_not(tables):
    _, Subject, Session, Scan, _ = tables
    # both match on subject_id alone, and the subjects fall into all four regions
    B1 = (Session & "session = 1").proj()
    B2 = (Scan & "session = 0").proj()
    keys = ["subject_id"]
    s1, s2 = rows(Subject & B1, keys), rows(Subject & B2, keys)
    every = rows(Subject, keys)
    assert s1 - s2 and s2 - s1 and s1 & s2 and every - s1 - s2
    assert rows(Subject - [B1, B2], keys) == every - s1 - s2
    assert rows(Subject - [B1, B2], keys) == rows(Subject - B1 - B2, keys)
    assert rows(Subject & [B1, B2], keys) == s1 | s2
    assert rows(Subject - dj.AndList([B1, B2]), keys) == every - (s1 & s2)
    assert rows(Subject & dj.Not(dj.Not(B1)), keys) == rows(Subject & B1, keys)
    assert rows(Subject & [dj.Not(B1), "weight = 0"], keys) == (every - s1) | rows(Subject & "weight = 0", keys)
    assert rows(Subject - [B1, "weight = 0"], keys) == (every - s1) - rows(Subject & "weight = 0", keys)
    # edge cases of the negated lists
    assert len(Subject - []) == len(Subject)
    assert len(Subject - dj.AndList([])) == 0
    assert len(Subject - [B1, True]) == 0
    assert rows(Subject - [B1, False], keys) == every - s1
    # complement with nullable namesakes in an OR-list
    pos, neg = rows(Subject & [Session, B2], keys), rows(Subject - [Session, B2], keys)
    assert pos and neg and not pos & neg and pos | neg == every


def test_complement_aggregation(tables):
    """A restricted aggregation compiles its restriction to HAVING on MySQL, a wrapper on PostgreSQL."""
    _, Subject, Session, Scan, _ = tables
    counts = Subject.aggr(Session.proj(), n="count(*)", exclude_nonmatching=True)
    keys = ["subject_id"]
    exclude = Scan & "session = 0"
    for aggr in (counts, counts & "n > 1"):
        expected = rows(aggr, keys) - rows(exclude, keys)
        assert expected and expected != rows(aggr, keys), "the operand must split the aggregation"
        assert rows(aggr - exclude, keys) == expected
        assert len(aggr - exclude) == len(expected)
        assert len(aggr & exclude) + len(aggr - exclude) == len(aggr)
    # anti-restriction followed by a further restriction of the aggregation
    q = (counts - exclude) & "n > 1"
    assert rows(q, keys) == rows(counts & "n > 1", keys) - rows(exclude, keys)


def test_complement_left_join(tables):
    """extend() is a left join; its NULL-filled columns never match."""
    Tag, _, Session, _, _ = tables
    E = Session.extend(Tag)
    assert len(E) == len(Session)
    keys = ["subject_id", "session"]
    red = (Tag & "color = 'red'").proj()
    expected = rows(E, keys) - rows(E & "tag = 'xa'", keys)
    assert rows(E - red, keys) == expected
    assert len(E & red) + len(E - red) == len(E)
    # match on the left-joined, nullable `color` (no lineage, so without semantic check)
    colors = dj.U("color") & Tag
    assert None in colors.fetch("color")
    q = E.restrict(dj.Not(colors), semantic_check=False)
    assert rows(q, keys) == rows(E & "color IS NULL", keys)
    q = E.restrict(colors, semantic_check=False)
    assert len(q) + len(E.restrict(dj.Not(colors), semantic_check=False)) == len(E)


def test_populate_key_source_minus_self(tables):
    _, Subject, Session, _, SessionCount = tables
    source = SessionCount.key_source
    assert "NOT EXISTS" in source.make_sql()
    assert SessionCount.progress() == (len(source), len(source))
    SessionCount.populate("subject_id < 10")
    remaining, total = SessionCount.progress()
    assert total == len(source)
    assert remaining == len(source - "subject_id < 10")
    assert set(SessionCount.fetch("subject_id")) == set((source & "subject_id < 10").fetch("subject_id"))
    SessionCount.populate()
    assert SessionCount.progress() == (0, len(source))
    assert len(SessionCount) == len(source)


def test_cascade_delete_from_complement(tables):
    _, Subject, Session, Scan, _ = tables
    keys = ["subject_id"]
    session_keys, scan_keys = ["subject_id", "session"], ["subject_id", "session", "scan"]
    with_scans = rows(Subject & Scan, keys)
    doomed = rows(Subject - Scan, keys)
    assert doomed and with_scans
    sessions_kept = {s for s in rows(Session, session_keys) if s[:1] in with_scans}
    assert sessions_kept != rows(Session, session_keys)
    (Subject - Scan).delete()
    assert rows(Subject, keys) == with_scans
    assert rows(Session, session_keys) == sessions_kept
    # cascade through Session to Scan, seeded by an anti-restriction by a restricted operand
    second = (Session & "session = 1").proj()
    keep = rows(Subject & second, keys)
    assert keep and keep != rows(Subject, keys) and len(Scan - (Subject & second).proj())
    scans_kept = {s for s in rows(Scan, scan_keys) if s[:1] in keep}
    assert scans_kept
    (Subject - second).delete()
    assert rows(Subject, keys) == keep
    assert rows(Session, keys) == keep
    assert rows(Scan, scan_keys) == scans_kept


def test_negated_composite_condition(tables):
    """NOT applies to the whole conjunction of a multi-attribute dict, record or AndList."""
    _, _, Session, _, _ = tables
    key = {"subject_id": 1, "session": 0}
    keys = ["subject_id", "session"]
    assert len(Session & key) == 1
    # subject 1 has two sessions, so a NOT applied to subject_id = 1 alone would differ
    assert len(Session & "subject_id = 1") == 2
    expected = rows(Session, keys) - {(1, 0)}
    assert rows(Session - key, keys) == expected
    assert rows(Session - [key], keys) == expected
    assert rows(Session & dj.Not(key), keys) == expected
    assert rows(Session - dj.AndList(["subject_id = 1", "session = 0"]), keys) == expected
    record = (Session & key).proj().to_arrays()
    assert rows(Session - record[0], keys) == expected
    assert rows(Session - record, keys) == expected


def test_percent_in_restriction_strings(tables):
    """`%` in string restrictions inside and outside the NOT EXISTS operand."""
    _, Subject, Session, _, _ = tables
    keys = ["subject_id"]
    like_y = Session & "tag LIKE 'y%'"
    assert len(like_y)
    expected = rows(Subject, keys) - rows(like_y, keys)
    assert rows(Subject - like_y.proj(), keys) == expected
    outer = Subject & "tag LIKE 'x%'"
    assert rows(outer - like_y.proj(), keys) == rows(outer, keys) - rows(like_y, keys)
    assert rows(Subject - (Session & {"tag": "y%"}).proj(), keys) == expected
    check_complement(Subject, like_y, ["subject_id", "tag"])
