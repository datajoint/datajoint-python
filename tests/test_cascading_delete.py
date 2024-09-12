import pytest
import datajoint as dj
from .schema_simple import A, B, D, E, G, L, Website, Profile
from .schema import ComplexChild, ComplexParent


@pytest.fixture
def schema_simp_pop(schema_simp):
    A().insert(A.contents, skip_duplicates=True)
    L().insert(L.contents, skip_duplicates=True)
    B().populate()
    D().populate()
    E().populate()
    G().populate()
    yield schema_simp


def test_delete_tree(schema_simp_pop):
    assert not dj.config["safemode"], "safemode must be off for testing"
    assert (
        L() and A() and B() and B.C() and D() and E() and E.F()
    ), "schema is not populated"
    A().delete()
    assert not A() or B() or B.C() or D() or E() or E.F(), "incomplete delete"


def test_stepwise_delete(schema_simp_pop):
    assert not dj.config["safemode"], "safemode must be off for testing"
    assert L() and A() and B() and B.C(), "schema population failed"
    B.C().delete(force=True)
    assert not B.C(), "failed to delete child tables"
    B().delete()
    assert (
        not B()
    ), "failed to delete from the parent table following child table deletion"


def test_delete_tree_restricted(schema_simp_pop):
    assert not dj.config["safemode"], "safemode must be off for testing"
    assert (
        L() and A() and B() and B.C() and D() and E() and E.F()
    ), "schema is not populated"
    cond = "cond_in_a"
    rel = A() & cond
    rest = dict(
        A=len(A()) - len(rel),
        B=len(B() - rel),
        C=len(B.C() - rel),
        D=len(D() - rel),
        E=len(E() - rel),
        F=len(E.F() - rel),
    )
    rel.delete()
    assert not (
        rel or B() & rel or B.C() & rel or D() & rel or E() & rel or (E.F() & rel)
    ), "incomplete delete"
    assert len(A()) == rest["A"], "invalid delete restriction"
    assert len(B()) == rest["B"], "invalid delete restriction"
    assert len(B.C()) == rest["C"], "invalid delete restriction"
    assert len(D()) == rest["D"], "invalid delete restriction"
    assert len(E()) == rest["E"], "invalid delete restriction"
    assert len(E.F()) == rest["F"], "invalid delete restriction"


def test_delete_lookup(schema_simp_pop):
    assert not dj.config["safemode"], "safemode must be off for testing"
    assert bool(
        L() and A() and B() and B.C() and D() and E() and E.F()
    ), "schema is not populated"
    L().delete()
    assert not bool(L() or D() or E() or E.F()), "incomplete delete"
    A().delete()  # delete all is necessary because delete L deletes from subtables.


def test_delete_lookup_restricted(schema_simp_pop):
    assert not dj.config["safemode"], "safemode must be off for testing"
    assert (
        L() and A() and B() and B.C() and D() and E() and E.F()
    ), "schema is not populated"
    rel = L() & "cond_in_l"
    original_count = len(L())
    deleted_count = len(rel)
    rel.delete()
    assert len(L()) == original_count - deleted_count


def test_delete_complex_keys(schema_any):
    """
    https://github.com/datajoint/datajoint-python/issues/883
    https://github.com/datajoint/datajoint-python/issues/886
    """
    assert not dj.config["safemode"], "safemode must be off for testing"
    parent_key_count = 8
    child_key_count = 1
    restriction = dict(
        {"parent_id_{}".format(i + 1): i for i in range(parent_key_count)},
        **{
            "child_id_{}".format(i + 1): (i + parent_key_count)
            for i in range(child_key_count)
        },
    )
    assert len(ComplexParent & restriction) == 1, "Parent record missing"
    assert len(ComplexChild & restriction) == 1, "Child record missing"
    (ComplexParent & restriction).delete()
    assert len(ComplexParent & restriction) == 0, "Parent record was not deleted"
    assert len(ComplexChild & restriction) == 0, "Child record was not deleted"


def test_delete_master(schema_simp_pop):
    Profile().populate_random()
    Profile().delete()


def test_delete_parts_error(schema_simp_pop):
    """test issue #151"""
    with pytest.raises(dj.DataJointError):
        Profile().populate_random()
        Website().delete(force_masters=False)


def test_delete_parts(schema_simp_pop):
    """test issue #151"""
    Profile().populate_random()
    Website().delete(force_masters=True)


def test_delete_parts_complex(schema_simp_pop):
    """test issue #151 with complex master/part. PR #1158."""
    prev_len = len(G())
    (A() & "id_a=1").delete(force_masters=True)
    assert prev_len - len(G()) == 16, "Failed to delete parts"


def test_drop_part(schema_simp_pop):
    """test issue #374"""
    with pytest.raises(dj.DataJointError):
        Website().drop()


def test_delete_1159(thing_tables):
    tbl_a, tbl_c, tbl_c, tbl_d, tbl_e = thing_tables

    tbl_c.insert([dict(a=i) for i in range(6)])
    tbl_d.insert([dict(a=i, d=i) for i in range(5)])
    tbl_e.insert([dict(d=i) for i in range(4)])

    (tbl_a & "a=3").delete()

    assert len(tbl_a) == 6, "Failed to cascade restriction attributes"
    assert len(tbl_e) == 3, "Failed to cascade restriction attributes"
