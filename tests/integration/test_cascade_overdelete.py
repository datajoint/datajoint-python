"""
Regression tests for cascade restrictions that reached rows they should not.

A restriction may be copied verbatim across a foreign key only when it names
attributes the foreign key carries and the foreign key lies in the child's
primary key. The part-to-master lift never copies: a master joins the cascade
only if one of its part rows did. A ``Top`` seed restricts the seed to the keys
it selects.
"""

import pytest

import datajoint as dj


@pytest.fixture(scope="function")
def schema_by_backend(connection_by_backend, db_creds_by_backend, request):
    """Create a fresh schema per test (mirrors test_cascade_integrity.py)."""
    backend = db_creds_by_backend["backend"]
    import time

    test_id = str(int(time.time() * 1000))[-8:]
    schema_name = f"djtest_overdelete_{backend}_{test_id}"[:64]

    if connection_by_backend.is_connected:
        try:
            connection_by_backend.query(
                f"DROP DATABASE IF EXISTS {connection_by_backend.adapter.quote_identifier(schema_name)}"
            )
        except Exception:
            pass

    schema = dj.Schema(schema_name, connection=connection_by_backend)
    yield schema

    if connection_by_backend.is_connected:
        try:
            connection_by_backend.query(
                f"DROP DATABASE IF EXISTS {connection_by_backend.adapter.quote_identifier(schema_name)}"
            )
        except Exception:
            pass


def test_top_seed_deletes_only_the_top_rows(schema_by_backend):
    """``(T & dj.Top(n)).delete()`` and ``delete_quick()`` delete n rows, not
    every row of T."""

    @schema_by_backend
    class Item(dj.Manual):
        definition = """
        item_id : int32
        """

    @schema_by_backend
    class Child(dj.Manual):
        definition = """
        -> Item
        child_id : int32
        """

    Item.insert([(i,) for i in range(5)])
    Child.insert([(i, 0) for i in range(5)])

    seed = Item & dj.Top(2, order_by="item_id")
    counts = dj.Diagram.cascade(seed).counts()
    assert counts[Item.full_table_name] == 2
    assert counts[Child.full_table_name] == 2

    seed.delete(prompt=False)
    assert sorted(Item.to_arrays("item_id")) == [2, 3, 4]
    assert sorted(Child.to_arrays("item_id")) == [2, 3, 4]

    (Child & dj.Top(1, order_by="item_id")).delete_quick()
    assert sorted(Child.to_arrays("item_id")) == [3, 4]


def test_top_seed_that_selects_no_rows_deletes_nothing(schema_by_backend):
    """A ``Top`` over a restriction that matches nothing seeds an empty cascade:
    the preview counts nothing and the delete removes nothing."""

    @schema_by_backend
    class Item(dj.Manual):
        definition = """
        item_id : int32
        """

    @schema_by_backend
    class Child(dj.Manual):
        definition = """
        -> Item
        child_id : int32
        """

    Item.insert([(i,) for i in range(3)])
    Child.insert([(i, 0) for i in range(3)])

    seed = Item & "item_id > 100" & dj.Top(2, order_by="item_id")
    counts = dj.Diagram.cascade(seed).counts()
    assert counts[Item.full_table_name] == 0
    assert counts[Child.full_table_name] == 0

    seed.delete(prompt=False)
    assert len(Item()) == 3
    assert len(Child()) == 3


def test_top_seed_in_trace(schema_by_backend):
    """``trace`` also starts from the rows a ``Top`` selects."""

    @schema_by_backend
    class Item(dj.Manual):
        definition = """
        item_id : int32
        """

    @schema_by_backend
    class Child(dj.Manual):
        definition = """
        -> Item
        child_id : int32
        """

    Item.insert([(i,) for i in range(5)])
    Child.insert([(i, 0) for i in range(5)])

    traced = dj.Diagram.trace(Child & dj.Top(2, order_by="item_id"))
    assert traced.counts()[Child.full_table_name] == 2
    assert traced.counts()[Item.full_table_name] == 2


def test_nullable_fk_sharing_a_primary_key_column(schema_by_backend):
    """A child whose nullable foreign key shares a primary-key column with the
    parent keeps the rows whose foreign key is NULL: they reference nothing."""

    @schema_by_backend
    class Subject(dj.Manual):
        definition = """
        subject_id : int32
        """

    @schema_by_backend
    class Surgery(dj.Manual):
        definition = """
        -> Subject
        surgery_id : int32
        """

    @schema_by_backend
    class Session(dj.Manual):
        definition = """
        -> Subject
        session_id : int32
        ---
        -> [nullable] Surgery
        """

    Subject.insert([(1,)])
    Surgery.insert([(1, 1)])
    Session.insert(
        [
            {"subject_id": 1, "session_id": 1, "surgery_id": 1},
            {"subject_id": 1, "session_id": 2, "surgery_id": None},
        ]
    )

    seed = Surgery & {"subject_id": 1}
    assert dj.Diagram.cascade(seed).counts()[Session.full_table_name] == 1

    seed.delete(prompt=False)
    assert len(Surgery()) == 0
    assert Session.to_dicts() == [{"subject_id": 1, "session_id": 2, "surgery_id": None}]


def test_diamond_child_is_reached_through_its_secondary_fk(schema_by_backend):
    """D references B directly (primary) and C (secondary). Deleting B=1 must
    delete the D rows that reference the deleted C rows, whatever D's own b_id."""

    @schema_by_backend
    class B(dj.Manual):
        definition = """
        b_id : int32
        """

    @schema_by_backend
    class C(dj.Manual):
        definition = """
        c_id : int32
        ---
        -> B
        """

    @schema_by_backend
    class D(dj.Manual):
        definition = """
        -> B
        d_id : int32
        ---
        -> C
        """

    B.insert([(1,), (2,)])
    C.insert([{"c_id": 10, "b_id": 1}])
    D.insert([{"b_id": 2, "d_id": 1, "c_id": 10}])

    seed = B & {"b_id": 1}
    assert dj.Diagram.cascade(seed).counts()[D.full_table_name] == 1

    seed.delete(prompt=False)
    assert B.to_arrays("b_id").tolist() == [2]
    assert len(C()) == 0
    assert len(D()) == 0


def test_part_cascade_lifts_only_masters_that_have_a_matching_part(schema_by_backend):
    """With part_integrity="cascade", a part seed lifts to the masters of the
    matching part rows. A master that has no matching part row is untouched."""

    @schema_by_backend
    class Master(dj.Manual):
        definition = """
        master_id : int32
        """

        class P(dj.Part):
            definition = """
            -> master
            p_id : int32
            """

    @schema_by_backend
    class Downstream(dj.Manual):
        definition = """
        -> Master
        d_id : int32
        """

    Master.insert([(1,), (2,)])
    Master.P.insert([(1, 1)])  # master 2 has no parts
    Downstream.insert([(1, 1), (2, 1)])

    seed = Master.P & [{"master_id": 1}, {"master_id": 2}]
    assert dj.Diagram.cascade(seed, part_integrity="cascade").counts()[Master.full_table_name] == 1

    seed.delete(prompt=False, part_integrity="cascade")
    assert Master.to_arrays("master_id").tolist() == [2]
    assert len(Master.P()) == 0
    assert Downstream.to_arrays("master_id").tolist() == [2]


SUBQUERY_SEEDS = {
    # seed -> (subjects deleted, sessions deleted)
    "restricted by a query": (lambda S, T: S & (T & "quality = 0"), {1, 3}, {(1, 1), (1, 2), (3, 1)}),
    "anti-restricted by a query": (lambda S, T: S - (T & "quality = 0"), {2, 4}, {(2, 1)}),
    "restricted by a table": (lambda S, T: S & T, {1, 2, 3}, {(1, 1), (1, 2), (2, 1), (3, 1)}),
}


@pytest.mark.parametrize("seed_kind", list(SUBQUERY_SEEDS))
def test_seed_restricted_by_a_downstream_query(schema_by_backend, seed_kind):
    """A seed restricted by a query on a downstream table deletes the rows that
    query selected when the delete started, with all their descendants."""

    @schema_by_backend
    class Subject(dj.Manual):
        definition = """
        subject_id : int32
        """

    @schema_by_backend
    class Session(dj.Manual):
        definition = """
        -> Subject
        session_id : int32
        ---
        quality : int32
        """

    Subject.insert([(1,), (2,), (3,), (4,)])
    Session.insert([(1, 1, 1), (1, 2, 0), (2, 1, 1), (3, 1, 0)])
    all_sessions = {(1, 1), (1, 2), (2, 1), (3, 1)}

    make_seed, subjects, sessions = SUBQUERY_SEEDS[seed_kind]
    seed = make_seed(Subject, Session)

    counts = dj.Diagram.cascade(seed).counts()
    assert counts[Subject.full_table_name] == len(subjects)
    assert counts[Session.full_table_name] == len(sessions)

    seed.delete(prompt=False)
    assert set(Subject.to_arrays("subject_id").tolist()) == {1, 2, 3, 4} - subjects
    assert set(zip(*Session.to_arrays("subject_id", "session_id"))) == all_sessions - sessions


def test_seed_condition_with_a_percent_sign(schema_by_backend):
    """A seed condition containing % selects the same rows in the cascade as
    on its own."""

    @schema_by_backend
    class Label(dj.Manual):
        definition = """
        label_id : int32
        ---
        name : varchar(16)
        """

    @schema_by_backend
    class Use(dj.Manual):
        definition = """
        -> Label
        use_id : int32
        """

    Label.insert([(1, "50%"), (2, "other"), (3, "5%%")])
    Use.insert([(1, 1), (2, 1), (3, 1)])

    for seed, label in [
        (Label & "name = '50%'", 1),
        (Label & {"name": "50%"}, 1),
        (Label & "name = '5%%'", 3),
    ]:
        assert seed.to_arrays("label_id").tolist() == [label]
        assert dj.Diagram.cascade(seed).counts()[Use.full_table_name] == 1

    (Label & "name LIKE '50%'").delete(prompt=False)
    assert sorted(Label.to_arrays("label_id").tolist()) == [2, 3]
    assert sorted(Use.to_arrays("label_id").tolist()) == [2, 3]


def _string_seed_schema(schema):
    """Parent and Child both have a secondary attribute ``notes``. Child's
    ``notes`` is unrelated to its parent's."""

    @schema
    class Parent(dj.Manual):
        definition = """
        subject : varchar(16)
        ---
        notes : varchar(8)
        """

    @schema
    class Child(dj.Manual):
        definition = """
        -> Parent
        trial : int32
        ---
        notes : varchar(8)
        """

    return Parent, Child


# A string condition's attribute names are read from its SQL text. Each form
# names `notes` in a way that reading leaves out; the value is the subject the
# condition selects by name.
STRING_FORMS = {"uppercase name": "a", "literal ending in backslash": "a\\"}


def _string_condition(table, form):
    """Select subject ``STRING_FORMS[form]``, and subject ``w`` where ``notes = '1'``."""
    adapter = table.connection.adapter
    literal = adapter.quote_string(STRING_FORMS[form])
    notes = "NOTES" if form == "uppercase name" else adapter.quote_identifier("notes")
    return f"subject = {literal} OR ({notes} = '1' AND subject = 'w')"


def _insert_string_seed_rows(Parent, Child, a):
    Parent.insert([(a, "1"), ("w", "1"), ("z", "2")])
    Child.insert([(a, 1, "2"), (a, 2, "1"), ("w", 1, "2"), ("z", 1, "1")])


@pytest.mark.parametrize("form", list(STRING_FORMS))
def test_cascade_string_restriction_projects_through_parent(schema_by_backend, form):
    """A cascade from a string-restricted seed selects every child of the
    selected parents, whatever the children's own attributes of the same name."""
    Parent, Child = _string_seed_schema(schema_by_backend)
    a = STRING_FORMS[form]
    _insert_string_seed_rows(Parent, Child, a)

    seed = Parent & _string_condition(Parent, form)
    assert sorted(seed.to_arrays("subject").tolist()) == sorted([a, "w"])
    assert not seed._restriction_attributes_exact

    counts = dj.Diagram.cascade(seed).counts()
    assert counts[Parent.full_table_name] == 2
    assert counts[Child.full_table_name] == 3

    seed.delete(prompt=False)
    assert Parent.to_arrays("subject").tolist() == ["z"]
    assert Child.to_arrays("subject").tolist() == ["z"]


def test_cascade_dict_restriction_is_copied_to_child(schema_by_backend):
    """A dict restriction names its attributes exactly, so the child receives
    the parent's condition itself rather than a projection of the parent."""
    Parent, Child = _string_seed_schema(schema_by_backend)
    _insert_string_seed_rows(Parent, Child, "a")

    seed = Parent & {"subject": "a"}
    assert seed._restriction_attributes_exact

    diagram = dj.Diagram.cascade(seed)
    parent_restriction = diagram._cascade_restrictions[Parent.full_table_name]
    assert diagram._cascade_restrictions[Child.full_table_name] == parent_restriction
    assert diagram.counts()[Child.full_table_name] == 2

    seed.delete(prompt=False)
    assert sorted(Parent.to_arrays("subject").tolist()) == ["w", "z"]
    assert sorted(Child.to_arrays("subject").tolist()) == ["w", "z"]


@pytest.mark.parametrize("form", list(STRING_FORMS))
def test_restrict_string_restriction_projects_through_parent(schema_by_backend, form):
    """``Diagram.restrict`` selects every child of the selected parents."""
    Parent, Child = _string_seed_schema(schema_by_backend)
    _insert_string_seed_rows(Parent, Child, STRING_FORMS[form])

    restricted = dj.Diagram(schema_by_backend).restrict(Parent & _string_condition(Parent, form))
    counts = restricted.counts()
    assert counts[Parent.full_table_name] == 2
    assert counts[Child.full_table_name] == 3


@pytest.mark.parametrize("form", list(STRING_FORMS))
def test_trace_string_restriction_projects_through_child(schema_by_backend, form):
    """``trace`` from a string-restricted child selects the parents those child
    rows reference, whatever the parents' own attributes of the same name."""
    Parent, Child = _string_seed_schema(schema_by_backend)
    a = STRING_FORMS[form]
    Parent.insert([(a, "2"), ("w", "1"), ("z", "1")])
    Child.insert([(a, 1, "1"), ("w", 1, "2"), ("z", 1, "2")])

    seed = Child & _string_condition(Child, form)
    assert seed.to_arrays("subject").tolist() == [a]

    traced = dj.Diagram.trace(seed)
    assert traced[Parent.full_table_name].to_arrays("subject").tolist() == [a]


def test_part_cascade_string_restriction_lifts_to_master(schema_by_backend):
    """With part_integrity="cascade", a string-restricted part seed lifts to the
    masters of the selected part rows and cascades to all of their parts."""

    @schema_by_backend
    class Master(dj.Manual):
        definition = """
        subject : varchar(16)
        ---
        notes : varchar(8)
        """

        class P(dj.Part):
            definition = """
            -> master
            p_id : int32
            ---
            notes : varchar(8)
            """

    form = "uppercase name"
    a = STRING_FORMS[form]
    Master.insert([(a, "2"), ("w", "1"), ("z", "1")])
    Master.P.insert([(a, 1, "1"), (a, 2, "2"), ("w", 1, "2"), ("z", 1, "1")])

    seed = Master.P & _string_condition(Master.P, form)
    assert seed.to_arrays("subject").tolist() == [a, a]

    counts = dj.Diagram.cascade(seed, part_integrity="cascade").counts()
    assert counts[Master.full_table_name] == 1
    assert counts[Master.P.full_table_name] == 2

    seed.delete(prompt=False, part_integrity="cascade")
    assert sorted(Master.to_arrays("subject").tolist()) == ["w", "z"]
    assert sorted(Master.P.to_arrays("subject").tolist()) == ["w", "z"]


def test_trace_string_seed_selects_referenced_parents(schema_by_backend):
    """``trace`` from a string-restricted child selects only the parents that
    the selected child rows reference. A dict seed is copied to the parent
    as a condition, so it selects every parent row that satisfies it."""
    Parent, Child = _string_seed_schema(schema_by_backend)
    Parent.insert([("a", "1"), ("b", "1"), ("c", "1")])
    Child.insert([("a", 1, "1")])

    string_seed = Child & "subject < 'c'"
    assert len(dj.Diagram.trace(string_seed)[Parent.full_table_name]) == 1

    dict_seed = Child & [{"subject": "a"}, {"subject": "b"}]
    assert len(dj.Diagram.trace(dict_seed)[Parent.full_table_name]) == 2
