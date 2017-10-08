from nose.tools import assert_false, assert_true
import datajoint as dj
from .schema_simple import A, B, D, E, L


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
        assert_false(dj.config['safemode'], 'safemode must be off for testing')
        assert_true(L() and A() and B() and B.C() and D() and E() and E.F(),
                    'schema is not populated')
        A().delete()
        assert_false(A() or B() or B.C() or D() or E() or E.F(), 'incomplete delete')

    @staticmethod
    def test_stepwise_delete():
        assert_false(dj.config['safemode'], 'safemode must be off for testing') #TODO: just turn it off instead of warning
        assert_true(L() and A() and B() and B.C(), 'schema population failed as a precondition to test')
        B.C().delete(force=True)
        assert_false(B.C(), 'failed to delete child tables')
        B().delete()
        assert_false(B(), 'failed to delete the parent table following child table deletion')

    @staticmethod
    def test_delete_tree_restricted():
        assert_false(dj.config['safemode'], 'safemode must be off for testing')
        assert_true(L() and A() and B() and B.C() and D() and E() and E.F(), 'schema is not populated')
        cond = 'cond_in_a'
        rel = A() & cond
        rest = dict(
            A=len(A())-len(rel),
            B=len(B()-rel),
            C=len(B.C()-rel),
            D=len(D()-rel),
            E=len(E()-rel),
            F=len(E.F()-rel))
        rel.delete()
        assert_false(rel or
                     (B() & rel) or
                     (B.C() & rel) or
                     (D() & rel) or
                     (E() & rel) or
                     (E.F() & rel),
                     'incomplete delete')
        assert_true(
            len(A()) == rest['A'] and
            len(B()) == rest['B'] and
            len(B.C()) == rest['C'] and
            len(D()) == rest['D'] and
            len(E()) == rest['E'] and
            len(E.F()) == rest['F'],
            'incorrect restricted delete')

    @staticmethod
    def test_delete_lookup():
        assert_false(dj.config['safemode'], 'safemode must be off for testing')
        assert_true(bool(L() and A() and B() and B.C() and D() and E() and E.F()), 'schema is not populated')
        L().delete()
        assert_false(bool(L() or D() or E() or E.F()), 'incomplete delete')
        A().delete()  # delete all is necessary because delete L deletes from subtables.

    @staticmethod
    def test_delete_lookup_restricted():
        assert_false(dj.config['safemode'], 'safemode must be off for testing')
        assert_true(L() and A() and B() and B.C() and D() and E() and E.F(), 'schema is not populated')
        rel = L() & 'cond_in_l'
        original_count = len(L())
        deleted_count = len(rel)
        rel.delete()
        assert_true(len(L()) == original_count - deleted_count)
