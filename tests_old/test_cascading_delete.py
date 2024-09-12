from nose.tools import assert_false, assert_true, assert_equal, raises
import datajoint as dj
from .schema_simple import A, B, D, E, L, Website, Profile
from .schema import ComplexChild, ComplexParent


class TestDelete:
    @staticmethod
    def setup():
        """
        class-level test setup. Executes before each test method.
        """
        A().insert(A.contents, skip_duplicates=True)
        L().insert(L.contents, skip_duplicates=True)
        B().populate()
        D().populate()
        E().populate()

    @staticmethod
    def test_delete_tree():
        assert_false(dj.config["safemode"], "safemode must be off for testing")
        assert_true(
            L() and A() and B() and B.C() and D() and E() and E.F(),
            "schema is not populated",
        )
        A().delete()
        assert_false(A() or B() or B.C() or D() or E() or E.F(), "incomplete delete")

    @staticmethod
    def test_stepwise_delete():
        assert not dj.config["safemode"], "safemode must be off for testing"
        assert L() and A() and B() and B.C(), "schema population failed"
        B.C().delete(force=True)
        assert not B.C(), "failed to delete child tables"
        B().delete()
        assert (
            not B()
        ), "failed to delete from the parent table following child table deletion"

    @staticmethod
    def test_delete_tree_restricted():
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

    @staticmethod
    def test_delete_lookup():
        assert_false(dj.config["safemode"], "safemode must be off for testing")
        assert_true(
            bool(L() and A() and B() and B.C() and D() and E() and E.F()),
            "schema is not populated",
        )
        L().delete()
        assert_false(bool(L() or D() or E() or E.F()), "incomplete delete")
        A().delete()  # delete all is necessary because delete L deletes from subtables.

    @staticmethod
    def test_delete_lookup_restricted():
        assert_false(dj.config["safemode"], "safemode must be off for testing")
        assert_true(
            L() and A() and B() and B.C() and D() and E() and E.F(),
            "schema is not populated",
        )
        rel = L() & "cond_in_l"
        original_count = len(L())
        deleted_count = len(rel)
        rel.delete()
        assert_true(len(L()) == original_count - deleted_count)

    @staticmethod
    def test_delete_complex_keys():
        # https://github.com/datajoint/datajoint-python/issues/883
        # https://github.com/datajoint/datajoint-python/issues/886
        assert_false(dj.config["safemode"], "safemode must be off for testing")
        parent_key_count = 8
        child_key_count = 1
        restriction = dict(
            {"parent_id_{}".format(i + 1): i for i in range(parent_key_count)},
            **{
                "child_id_{}".format(i + 1): (i + parent_key_count)
                for i in range(child_key_count)
            }
        )
        assert len(ComplexParent & restriction) == 1, "Parent record missing"
        assert len(ComplexChild & restriction) == 1, "Child record missing"
        (ComplexParent & restriction).delete()
        assert len(ComplexParent & restriction) == 0, "Parent record was not deleted"
        assert len(ComplexChild & restriction) == 0, "Child record was not deleted"

    def test_delete_master(self):
        Profile().populate_random()
        Profile().delete()

    @raises(dj.DataJointError)
    def test_delete_parts(self):
        """test issue #151"""
        Profile().populate_random()
        Website().delete()

    @raises(dj.DataJointError)
    def test_drop_part(self):
        """test issue #374"""
        Website().drop()
