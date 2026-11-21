"""
DISTINCT only where a query can return duplicate rows.

A DataJoint query's primary key identifies its rows, so the SQL needs
``DISTINCT`` (or ``UNION`` deduplication) only where an operator removes part
of that key: ``dj.U(...) & expr`` projecting onto attributes that do not
include ``expr``'s primary key, and the union of two operands whose keys may
overlap. A table without a primary key (created outside DataJoint) can hold
repeated rows, and so can any query built from it, even one with a primary key
such as ``Parent * keyless``. So ``dj.U(...) & expr`` and the union deduplicate
whenever any table the operand is built from has no primary key. Everywhere
else ``DISTINCT`` is redundant work for the database.

Where ``DISTINCT`` is needed it must survive composition: an operand
``dj.U(...) & expr`` used in a join, ``extend``, ``aggr`` or ``dj.U(...).aggr``
must still contribute one row per key (#1660). ``len()`` must equal the number
of rows the query returns. All tests run on MySQL and PostgreSQL.
"""

import re
import time

import pytest

import datajoint as dj


@pytest.fixture(scope="function")
def schema_by_backend(connection_by_backend, db_creds_by_backend):
    """Create a fresh schema per test, parameterized across backends."""
    backend = db_creds_by_backend["backend"]
    test_id = str(int(time.time() * 1000))[-8:]
    schema_name = f"djtest_distinct_{backend}_{test_id}"[:64]
    drop = f"DROP DATABASE IF EXISTS {connection_by_backend.adapter.quote_identifier(schema_name)}"

    if connection_by_backend.is_connected:
        try:
            connection_by_backend.query(drop)
        except Exception:
            pass

    schema = dj.Schema(schema_name, connection=connection_by_backend)
    yield schema

    if connection_by_backend.is_connected:
        try:
            connection_by_backend.query(drop)
        except Exception:
            pass


@pytest.fixture
def tables(schema_by_backend):
    @schema_by_backend
    class Parent(dj.Manual):
        definition = """
        parent_id : int32
        ---
        label : varchar(16)
        """

    @schema_by_backend
    class Child(dj.Manual):
        definition = """
        -> Parent
        child_id : int32
        ---
        kind : varchar(16)
        """

    @schema_by_backend
    class Note(dj.Manual):
        definition = """
        -> Parent
        ---
        note : varchar(16)
        """

    Parent.insert(dict(parent_id=i, label=label) for i, label in enumerate(["one", "two", "three", "four", "five"], start=1))
    # Children per parent: 1 -> a, a, b; 2 -> a, c; 3 -> b; 4 -> c; 5 -> none
    children = [(1, 1, "a"), (1, 2, "a"), (1, 3, "b"), (2, 1, "a"), (2, 2, "c"), (3, 1, "b"), (4, 1, "c")]
    Child.insert(dict(parent_id=p, child_id=c, kind=k) for p, c, k in children)
    Note.insert([dict(parent_id=2, note="x"), dict(parent_id=3, note="y")])
    return Parent, Child, Note


def create_keyless_table(schema, name, columns, values):
    """Create a table with no primary key by plain SQL and return it as a FreeTable."""
    conn = schema.connection
    quote = conn.adapter.quote_identifier
    full_table_name = f"{quote(schema.database)}.{quote(name)}"
    conn.query(f"CREATE TABLE {full_table_name} ({columns})")
    conn.query(f"INSERT INTO {full_table_name} VALUES {values}")
    return dj.FreeTable(conn, full_table_name)


@pytest.fixture
def keyless(schema_by_backend):
    """A table with no primary key, created by plain SQL, with a repeated row."""
    return create_keyless_table(
        schema_by_backend,
        "message",
        "msg varchar(16) NOT NULL, n int NOT NULL",
        "('hello', 1), ('hello', 1), ('hello', 2), ('bye', 3)",
    )


def has_distinct(sql):
    """Whether the SQL deduplicates (the schema names in these tests contain 'distinct')."""
    return re.search(r"\bDISTINCT\b", sql, flags=re.IGNORECASE) is not None


def assert_rows(query, expected):
    """``len()``, the fetched rows and the expected row count all agree."""
    rows = query.to_dicts()
    assert len(rows) == expected
    assert len(query) == expected


# --- DISTINCT is emitted only where rows can repeat ---------------------------------------------


def test_universal_restriction_needs_distinct_only_without_the_key(tables):
    """``dj.U(...) & expr`` deduplicates unless its attributes include ``expr``'s primary key."""
    Parent, Child, Note = tables

    kinds = dj.U("kind") & Child
    assert has_distinct(kinds.make_sql())
    assert_rows(kinds, 3)

    parents = dj.U("parent_id") & Child
    assert has_distinct(parents.make_sql())
    assert_rows(parents, 4)

    for query in (dj.U("parent_id", "child_id") & Child, dj.U("parent_id", "child_id", "kind") & Child):
        assert not has_distinct(query.make_sql())
        assert_rows(query, 7)


def test_aggregation_does_not_emit_distinct(tables):
    """GROUP BY on the primary key already yields one row per key."""
    Parent, Child, Note = tables
    for query in (Parent.aggr(Child), Parent.aggr(Child, n="count(*)"), dj.U("kind").aggr(Child)):
        assert not has_distinct(query.make_sql())
    assert_rows(Parent.aggr(Child), 5)
    assert_rows(Parent.aggr(Child, exclude_nonmatching=True), 4)
    assert_rows(dj.U("kind").aggr(Child), 3)


def test_universal_restriction_of_aggregation(tables):
    """``dj.U(...) & aggregation`` deduplicates the aggregated values."""
    Parent, Child, Note = tables
    counts = Parent.aggr(Child, n="count(child_id)")  # n = 3, 2, 1, 1, 0

    distinct_counts = dj.U("n") & counts
    assert sorted(distinct_counts.to_arrays("n")) == [0, 1, 2, 3]
    assert len(distinct_counts) == 4

    matched_counts = dj.U("n") & Parent.aggr(Child, n="count(child_id)", exclude_nonmatching=True)
    assert sorted(matched_counts.to_arrays("n")) == [1, 2, 3]
    assert len(matched_counts) == 3

    # Restricted on the computed attribute: proj() puts the aggregation in a subquery,
    # and the DISTINCT must apply outside it, to the projected counts.
    positive_counts = dj.U("n") & (counts & "n > 0")
    assert sorted(positive_counts.to_arrays("n")) == [1, 2, 3]
    assert_rows(positive_counts, 3)

    keyed = dj.U("parent_id") & counts
    assert not has_distinct(keyed.make_sql())
    assert_rows(keyed, 5)


def test_union_with_secondary_attributes_uses_union_all(tables):
    """The two branches of an outer union are disjoint, so no deduplication is needed."""
    Parent, Child, Note = tables
    union = (Parent & "parent_id <= 2") + Note
    assert "UNION ALL" in union.make_sql()
    assert_rows(union, 3)
    rows = {row["parent_id"]: (row["label"], row["note"]) for row in union.to_dicts()}
    assert rows == {1: ("one", None), 2: ("two", "x"), 3: (None, "y")}


def test_union_of_keys_deduplicates(tables):
    """The union of overlapping key sets still removes the overlap."""
    Parent, Child, Note = tables
    union = (Parent.proj() & "parent_id <= 3") + (Parent.proj() & "parent_id >= 3")
    assert "UNION ALL" not in union.make_sql()
    assert_rows(union, 5)


# --- len() ----------------------------------------------------------------------------------------


def test_len_does_not_deduplicate(tables, monkeypatch):
    """``len()`` counts rows directly unless the query itself deduplicates."""
    Parent, Child, Note = tables
    conn = Child.connection
    issued = []
    query = conn.query

    def spy(sql, *args, **kwargs):
        issued.append(sql)
        return query(sql, *args, **kwargs)

    monkeypatch.setattr(conn, "query", spy)
    for expr in (Child(), Child & "kind = 'a'", Parent * Child, Parent.extend(Note), Child.proj()):
        issued.clear()
        len(expr)
        assert not any(has_distinct(sql) for sql in issued), issued

    issued.clear()
    len(dj.U("kind") & Child)
    assert any(has_distinct(sql) for sql in issued)


def test_len_matches_fetch(tables):
    """``len()`` equals the number of fetched rows across operators."""
    Parent, Child, Note = tables
    cases = [
        (Child(), 7),
        (Child & "kind = 'a'", 3),
        (Child - Note, 4),
        (Parent * Child, 7),
        (Parent.extend(Note), 5),
        (Child.extend(Note), 7),
        (Child.proj(), 7),
        (Child.proj(k="kind"), 7),
        (Child & dj.Top(2), 2),
        ((Parent & "parent_id <= 2") + Note, 3),
        (dj.U("kind") & Child, 3),
        ((dj.U("kind") & Child) & "kind != 'c'", 2),
        (dj.U("parent_id", "kind") & Child, 6),
    ]
    for query, expected in cases:
        assert_rows(query, expected)


# --- DISTINCT survives composition (#1660) ------------------------------------------------------


def test_join_keeps_distinct_operand(tables):
    """A ``dj.U(...) & expr`` operand contributes one row per key to a join."""
    Parent, Child, Note = tables
    parents_with_children = dj.U("parent_id") & Child
    assert_rows(Parent * parents_with_children, 4)
    assert_rows(parents_with_children * Parent, 4)
    assert_rows(Parent.extend(parents_with_children), 5)
    assert_rows(Note * (dj.U("parent_id", "kind") & Child), 3)


def test_aggr_over_distinct_group(tables):
    """Aggregating a ``dj.U(...) & expr`` group counts each key once."""
    Parent, Child, Note = tables
    parent_kinds = dj.U("parent_id", "kind") & Child  # (1,a) (1,b) (2,a) (2,c) (3,b) (4,c)

    kinds_per_parent = Parent.aggr(parent_kinds, n="count(kind)")
    assert dict(zip(*kinds_per_parent.to_arrays("parent_id", "n"))) == {1: 2, 2: 2, 3: 1, 4: 1, 5: 0}

    parents_per_kind = dj.U("kind").aggr(parent_kinds, n="count(*)")
    assert dict(zip(*parents_per_kind.to_arrays("kind", "n"))) == {"a": 2, "b": 2, "c": 2}

    distinct_parents = dj.U("parent_id") & Child
    by_parent = Parent.aggr(Child * distinct_parents, n="count(*)", exclude_nonmatching=True)
    assert dict(zip(*by_parent.to_arrays("parent_id", "n"))) == {1: 3, 2: 2, 3: 1, 4: 1}


def test_universal_restriction_of_union(tables):
    """``dj.U(...) & union`` deduplicates outside the union."""
    Parent, Child, Note = tables
    union = Parent.proj(parity="mod(parent_id, 2)") + Note  # parity = 1, 0, 1, 0, 1
    parities = dj.U("parity") & union
    assert sorted(parities.to_arrays("parity")) == [0, 1]
    assert len(parities) == 2


def test_distinct_operand_cases_from_issue(tables):
    """Each query of the reproduction in #1660, on this fixture's data."""
    Parent, Child, Note = tables
    has_children = dj.U("parent_id") & Child  # parents 1, 2, 3, 4

    assert_rows((has_children & "parent_id < 3") * Parent, 2)
    assert_rows(has_children.extend(Parent), 4)
    assert_rows(Parent * (dj.U("parent_id", "kind") & Child), 6)
    assert_rows(Parent & has_children, 4)
    assert_rows(Parent - has_children, 1)

    per_parent = Parent.aggr(has_children, n="count(*)", exclude_nonmatching=True)
    assert dict(zip(*per_parent.to_arrays("parent_id", "n"))) == {1: 1, 2: 1, 3: 1, 4: 1}
    labels = has_children.aggr(Parent, n="count(label)")
    assert dict(zip(*labels.to_arrays("parent_id", "n"))) == {1: 1, 2: 1, 3: 1, 4: 1}
    assert dj.U().aggr(dj.U("kind") & Child, n="count(*)").fetch1("n") == 3


def test_join_with_union_on_the_right_without_common_attributes(tables):
    """A union on the right of a join with no common attributes is a subquery."""
    Parent, Child, Note = tables
    kinds = dj.U("kind") & Child
    two_kinds = (kinds & "kind = 'a'") + (kinds & "kind = 'b'")
    assert_rows(Parent * two_kinds, 10)


def test_len_of_keyless_table(keyless):
    """``len()`` counts the rows of a table without a primary key, repeated rows included."""
    assert not keyless.primary_key
    assert_rows(keyless, 4)


def test_universal_restriction_of_keyless_table(tables, keyless):
    """A table without a primary key can repeat rows: ``dj.U(...) & keyless`` deduplicates."""
    Parent, Child, Note = tables
    assert not keyless.primary_key

    messages = dj.U("msg") & keyless
    assert has_distinct(messages.make_sql())
    assert sorted(messages.to_arrays("msg")) == ["bye", "hello"]
    assert_rows(messages, 2)

    # U's attributes include every attribute, yet ('hello', 1) appears twice in the table
    pairs = dj.U("msg", "n") & keyless
    assert has_distinct(pairs.make_sql())
    assert sorted(zip(*pairs.to_arrays("msg", "n"))) == [("bye", 3), ("hello", 1), ("hello", 2)]
    assert_rows(pairs, 3)

    assert_rows(Parent * messages, 10)
    per_message = dj.U("msg").aggr(pairs, k="count(*)")
    assert dict(zip(*per_message.to_arrays("msg", "k"))) == {"bye": 1, "hello": 2}


def test_union_of_keyless_tables_deduplicates(schema_by_backend, keyless):
    """Rows of a keyless operand can repeat, so its outer union keeps UNION deduplication."""
    tags = create_keyless_table(schema_by_backend, "tag", "tag varchar(16) NOT NULL", "('t')")
    union = keyless + tags
    assert "UNION ALL" not in union.make_sql()
    rows = sorted((row["msg"], row["n"], row["tag"]) for row in union.to_dicts())
    assert rows == [("bye", 3, "t"), ("hello", 1, "t"), ("hello", 2, "t")]
    assert_rows(union, 3)


# --- Queries built from a keyless table -----------------------------------------------------------
# Heading.join treats an empty primary key as determined by any heading, so a query built from a
# table without a primary key can have a primary key and still repeat rows. The expected rows are
# those master returns: dj.U(...) & expr and the outer union deduplicate whenever any table the
# operand is built from has no primary key.


@pytest.fixture
def log(schema_by_backend):
    """A table with no primary key, created by plain SQL, that refers to parents: (1, 'start') repeats."""
    return create_keyless_table(
        schema_by_backend,
        "log",
        "parent_id int NOT NULL, event varchar(16) NOT NULL",
        "(1, 'start'), (1, 'start'), (1, 'stop'), (2, 'start'), (9, 'start')",
    )


def fetch_sorted(query, *attributes):
    """The rows of query as sorted tuples of the attributes; ``len()`` agrees with the row count."""
    rows = sorted(tuple(row[a] for a in attributes) for row in query.to_dicts())
    assert len(query) == len(rows)
    return rows


MESSAGES = [("bye", 3), ("hello", 1), ("hello", 1), ("hello", 2)]
DISTINCT_MESSAGES = sorted(set(MESSAGES))
PARENTS = [1, 2, 3, 4, 5]


def test_universal_restriction_of_keyless_table_with_parent_key(log):
    """Direct operand: U's attributes cover every attribute of the keyless table, yet rows repeat."""
    parents = dj.U("parent_id") & log
    assert has_distinct(parents.make_sql())
    assert fetch_sorted(parents, "parent_id") == [(1,), (2,), (9,)]

    events = dj.U("parent_id", "event") & log
    assert has_distinct(events.make_sql())
    assert fetch_sorted(events, "parent_id", "event") == [(1, "start"), (1, "stop"), (2, "start"), (9, "start")]


def test_universal_restriction_of_join_with_keyless_table(tables, keyless, log):
    """``Parent * keyless`` has Parent's primary key, but repeats Parent's rows: U deduplicates."""
    Parent, Child, Note = tables
    for joined in (Parent * keyless, keyless * Parent):
        assert joined.primary_key == ["parent_id"]
        assert len(fetch_sorted(joined, "parent_id", "msg", "n")) == 20

        parents = dj.U("parent_id") & joined
        assert has_distinct(parents.make_sql())
        assert fetch_sorted(parents, "parent_id") == [(p,) for p in PARENTS]

        rows = dj.U("parent_id", "msg", "n") & joined
        assert has_distinct(rows.make_sql())
        assert fetch_sorted(rows, "parent_id", "msg", "n") == [(p, *m) for p in PARENTS for m in DISTINCT_MESSAGES]

    joined = Parent.join(log, semantic_check=False)
    assert joined.primary_key == ["parent_id"]
    assert fetch_sorted(joined, "parent_id", "event") == [(1, "start"), (1, "start"), (1, "stop"), (2, "start")]
    assert fetch_sorted(dj.U("parent_id") & joined, "parent_id") == [(1,), (2,)]
    events = dj.U("parent_id", "event") & joined
    assert fetch_sorted(events, "parent_id", "event") == [(1, "start"), (1, "stop"), (2, "start")]


def test_universal_restriction_of_extension_by_keyless_table(tables, keyless, log):
    """``Parent.extend(keyless)`` keeps Parent's primary key and repeats its rows: U deduplicates."""
    Parent, Child, Note = tables
    extended = Parent.extend(keyless)
    assert extended.primary_key == ["parent_id"]
    assert len(fetch_sorted(extended, "parent_id", "msg", "n")) == 20
    rows = dj.U("parent_id", "msg", "n") & extended
    assert has_distinct(rows.make_sql())
    assert fetch_sorted(rows, "parent_id", "msg", "n") == [(p, *m) for p in PARENTS for m in DISTINCT_MESSAGES]

    extended = Parent.extend(log, semantic_check=False)
    assert extended.primary_key == ["parent_id"]
    assert len(fetch_sorted(extended, "parent_id")) == 7  # 3 + 1 events, and parents 3, 4, 5 without
    assert fetch_sorted(dj.U("parent_id") & extended, "parent_id") == [(p,) for p in PARENTS]
    events = dj.U("parent_id", "event") & extended  # event is NULL for parents 3, 4 and 5
    rows = sorted((row["parent_id"], row["event"] or "") for row in events.to_dicts())
    assert rows == [(1, "start"), (1, "stop"), (2, "start"), (3, ""), (4, ""), (5, "")]
    assert len(events) == len(rows)


def test_universal_restriction_with_keyless_table_in_subquery(tables, keyless):
    """A keyless table inside a subquery of the operand still makes its rows repeat."""
    Parent, Child, Note = tables
    doubled = keyless.proj(..., k="n * 2") & "k > 1"  # restricted on a computed attribute: a subquery
    rows = dj.U("parent_id", "msg", "n", "k") & (Parent * doubled)
    assert has_distinct(rows.make_sql())
    assert fetch_sorted(rows, "parent_id", "msg", "n", "k") == [
        (p, msg, n, 2 * n) for p in PARENTS for msg, n in DISTINCT_MESSAGES
    ]

    first_three = keyless & dj.Top(3, order_by=["msg", "n"])  # ('bye', 3) and ('hello', 1) twice
    joined = Parent * first_three
    assert len(fetch_sorted(joined, "parent_id", "msg", "n")) == 15
    rows = dj.U("parent_id", "msg", "n") & joined
    assert has_distinct(rows.make_sql())
    assert fetch_sorted(rows, "parent_id", "msg", "n") == [(p, *m) for p in PARENTS for m in [("bye", 3), ("hello", 1)]]


def test_union_with_operand_built_from_keyless_table(tables, keyless):
    """The outer union keeps UNION when an operand is built from a keyless table."""
    Parent, Child, Note = tables
    union = (Parent * keyless) + Note
    assert union.primary_key == ["parent_id"]
    assert "UNION ALL" not in union.make_sql()
    rows = fetch_sorted(union, "parent_id", "msg", "n", "note")
    notes = {2: "x", 3: "y"}
    assert rows == [(p, *m, notes.get(p)) for p in PARENTS for m in DISTINCT_MESSAGES]

    parents = dj.U("parent_id") & union
    assert fetch_sorted(parents, "parent_id") == [(p,) for p in PARENTS]
