"""
Union and Aggregation select only the attributes their callers need.

Covers Union.make_sql(fields), restriction by a Union, insert from a Union,
and the narrowed len()/bool() of Union and Aggregation, on both backends.
"""

import pytest

import datajoint as dj


@pytest.fixture
def schema_narrow(connection_by_backend, backend, prefix):
    schema = dj.Schema(f"{prefix}_narrow_{backend}", connection=connection_by_backend)

    @schema
    class Subject(dj.Manual):
        definition = """
        subject_id : int32
        ---
        subject_name : varchar(16)
        """

    @schema
    class Session(dj.Manual):
        definition = """
        -> Subject
        session_id : int32
        ---
        note : varchar(16)
        """

    @schema
    class NoteA(dj.Manual):
        definition = """
        -> Subject
        ---
        a_val : int32
        a_blob : bytes
        """

    @schema
    class NoteB(dj.Manual):
        definition = """
        -> Subject
        ---
        b_val : int32
        """

    @schema
    class Target(dj.Manual):
        definition = """
        -> Subject
        ---
        a_val = null : int32
        b_val = null : int32
        """

    Subject.insert([(i, f"s{i}") for i in range(1, 6)])
    Session.insert([(1, 1, "x"), (1, 2, "x"), (1, 3, "x"), (2, 1, "x"), (3, 1, "x"), (3, 2, "x"), (5, 1, "x")])
    NoteA.insert([(1, 10, b"\x00" * 100), (2, 20, b"\x01" * 100), (3, 30, b"\x02" * 100)])
    NoteB.insert([(3, 300), (4, 400)])
    yield dict(Subject=Subject, Session=Session, NoteA=NoteA, NoteB=NoteB, Target=Target)
    schema.drop(prompt=False)


@pytest.mark.backend_agnostic
def test_union_with_secondary_attributes(schema_narrow):
    s = schema_narrow
    u = s["NoteA"].proj("a_val") + s["NoteB"]
    rows = {r["subject_id"]: (r["a_val"], r["b_val"]) for r in u.to_dicts()}
    assert rows == {1: (10, None), 2: (20, None), 3: (30, 300), 4: (None, 400)}
    assert len(u) == 4
    assert bool(u)
    assert bool(u & "subject_id > 3")
    assert not bool(u & "subject_id > 4")


@pytest.mark.backend_agnostic
def test_union_selected_fields(schema_narrow):
    s = schema_narrow
    u = s["NoteA"] + s["NoteB"]
    assert sorted(u.to_arrays("b_val").tolist(), key=lambda v: (v is None, v)) == [300, 400, None, None]
    assert sorted(u.keys(), key=lambda k: k["subject_id"]) == [{"subject_id": i} for i in (1, 2, 3, 4)]
    sql = u.make_sql(["subject_id", "b_val"])
    assert "a_blob" not in sql and "UNION ALL" in sql


@pytest.mark.backend_agnostic
def test_restrict_by_union(schema_narrow):
    s = schema_narrow
    u = s["NoteA"].proj("a_val") + s["NoteB"]
    assert set((s["Subject"] & u).to_arrays("subject_id")) == {1, 2, 3, 4}
    assert set((s["Subject"] - u).to_arrays("subject_id")) == {5}
    assert len(s["Session"] & (u & "b_val > 350")) == 0
    assert len(s["Session"] & (u & "a_val >= 20")) == 3


@pytest.mark.backend_agnostic
def test_insert_from_union(schema_narrow):
    s = schema_narrow
    s["Target"].insert(s["NoteA"].proj("a_val") + s["NoteB"])
    rows = {r["subject_id"]: (r["a_val"], r["b_val"]) for r in s["Target"].to_dicts()}
    assert rows == {1: (10, None), 2: (20, None), 3: (30, 300), 4: (None, 400)}


@pytest.mark.backend_agnostic
def test_union_with_having_operand(schema_narrow):
    """A key-only union whose operand is an Aggregation restricted by HAVING."""
    s = schema_narrow
    busy = s["Subject"].aggr(s["Session"], n="count(*)") & "n > 1"  # subjects 1, 3
    u = busy + s["NoteB"]  # subjects 1, 3, 4
    assert len(u) == 3
    assert set(u.to_arrays("subject_id")) == {1, 3, 4}
    assert bool(u)


@pytest.mark.backend_agnostic
def test_aggregation_len_and_bool(schema_narrow):
    s = schema_narrow
    agg = s["Subject"].aggr(s["Session"], ..., n="count(*)")
    assert len(agg) == 5
    assert len(agg & "n > 1") == 2
    assert len(agg & "n > 5") == 0
    assert bool(agg & "n > 2")
    assert not bool(agg & "n > 3")
    top = agg & dj.Top(limit=2, order_by="n DESC")
    assert len(top) == 2  # ORDER BY n keeps n in the counted SELECT
    assert bool(top)
    # no GROUP BY: a single row
    assert len(dj.U().aggr(s["Session"], n="count(*)")) == 1


# --- config["prune_subqueries"]: nested subqueries select only what the enclosing query uses ---

PRUNING_CASES = {
    "top restricted": lambda s: (s["NoteA"] & dj.Top(2, "a_val DESC")) & "a_val > 15",
    "top reordered": lambda s: (s["NoteA"] & dj.Top(3, "a_val DESC")) & dj.Top(2, "subject_id"),
    "join of three": lambda s: (s["Subject"] * s["Session"] * s["NoteA"]).proj(),
    "aggr joined": lambda s: (s["NoteA"].aggr(s["Session"], ..., n="count(*)") * s["NoteB"]).proj("n", "b_val"),
    "aggr having": lambda s: s["Subject"] & (s["NoteA"].aggr(s["Session"], ..., n="count(*)", m="max(session_id)") & "n > 1"),
    "aggr having proj": lambda s: (s["NoteA"].aggr(s["Session"], ..., n="count(*)") & "n > 1").proj(),
    "aggr top": lambda s: (s["NoteA"].aggr(s["Session"], ..., n="count(*)") & dj.Top(2, "n DESC")) * s["Session"],
    "aggr no group by": lambda s: dj.U().aggr(s["Session"], n="count(*)", m="max(session_id)"),
    "union joined": lambda s: ((s["NoteA"] + s["NoteB"]) * s["Session"]).proj(),
    "union restricted": lambda s: ((s["NoteA"] + s["NoteB"]) & "b_val > 100").proj("b_val"),
    "union of aggr": lambda s: (s["Subject"].aggr(s["Session"], n="count(*)") & "n > 1") + s["NoteB"],
    "restrict by union": lambda s: s["Session"] & ((s["NoteA"] + s["NoteB"]) & "a_val > 15"),
    "renamed": lambda s: (s["NoteA"].proj(v="a_val") & "v > 15").proj(),
    "computed": lambda s: (s["NoteA"].proj(..., w="a_val * 2") & "w > 30").proj("w"),
    "renamed key": lambda s: (s["NoteA"].proj("a_val", sid="subject_id") & "sid > 1") * s["Subject"].proj(sid="subject_id"),
    "left join": lambda s: (s["Session"] & dj.Top(5, "session_id")).extend(s["NoteA"]).proj("a_val"),
}


def _evaluate(expr):
    def plain(v):
        return bytes(v) if isinstance(v, memoryview) else v

    rows = sorted(repr(sorted((k, plain(v)) for k, v in r.items())) for r in expr.to_dicts())
    return rows, len(expr), bool(expr)


@pytest.mark.backend_agnostic
@pytest.mark.parametrize("case", PRUNING_CASES)
def test_pruning_preserves_results(schema_narrow, case):
    make = PRUNING_CASES[case]
    with dj.config.override(prune_subqueries=False):
        expected = _evaluate(make(schema_narrow))
    with dj.config.override(prune_subqueries=True):
        assert _evaluate(make(schema_narrow)) == expected


@pytest.mark.backend_agnostic
@pytest.mark.parametrize("case", ["top restricted", "aggr joined", "aggr having proj", "union joined", "union restricted"])
def test_pruning_drops_unused_blob(schema_narrow, case):
    """The outer query selects only keys, so no subquery needs the blob."""
    with dj.config.override(prune_subqueries=False):
        assert "a_blob" in PRUNING_CASES[case](schema_narrow).proj().make_sql()
    with dj.config.override(prune_subqueries=True):
        assert "a_blob" not in PRUNING_CASES[case](schema_narrow).proj().make_sql()


@pytest.mark.backend_agnostic
def test_pruning_keeps_having_alias(schema_narrow):
    s = schema_narrow
    # the restriction's subquery is rendered when it is applied, so build it with pruning on
    with dj.config.override(prune_subqueries=True):
        expr = s["Subject"] & (s["NoteA"].aggr(s["Session"], ..., n="count(*)", m="max(session_id)") & "n > 1")
        sql = expr.make_sql()
    assert " as " in sql.lower() and "count(*)" in sql.lower()  # n stays for HAVING
    assert "max(" not in sql.lower()  # m is not used
