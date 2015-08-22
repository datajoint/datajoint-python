from nose.tools import assert_false, assert_true
import datajoint as dj
from .schema_simple import A, B, C, D, E, F, L


class TestDelete:

    @staticmethod
    def setup():
        """
        class-level test setup. Executes before each test method.
        """
        A()._prepare()
        L()._prepare()
        B().populate()
        D().populate()
        E().populate()

    @staticmethod
    def test_delete_tree():
        assert_false(dj.config['safemode'], 'safemode must be off for testing')
        assert_true(L() and A() and B() and C() and D() and E() and F(),
                    'schema is not populated')
        A().delete()
        assert_false(A() or B() or C() or D() or E() or F(), 'incomplete delete')

    @staticmethod
    def test_delete_tree_restricted():
        assert_false(dj.config['safemode'], 'safemode must be off for testing')
        assert_true(L() and A() and B() and C() and D() and E() and F(),
                    'schema is not populated')
        cond = 'cond_in_a'
        rel = A() & cond
        rest = dict(
            A=len(A())-len(cond),
            B=len(B()-rel),
            C=len(C()-rel),
            D=len(D()-rel),
            E=len(E()-rel),
            F=len(F()-rel)
        )
        rel.delete()
        assert_false(rel or
                     (B() & rel) or
                     (C() & rel) or
                     (D() & rel) or
                     (E() & rel) or
                     (F() & rel),
                     'incomplete delete')
        assert_true(
            len(A()) == rest['A'] and
            len(B()) == rest['B'] and
            len(C()) == rest['C'] and
            len(D()) == rest['D'] and
            len(E()) == rest['E'] and
            len(F()) == rest['F'],
            'incorrect restricted delete')

    @staticmethod
    def test_delete_lookup():
        assert_false(dj.config['safemode'], 'safemode must be off for testing')
        assert_true(L() and A() and B() and C() and D() and E() and F(),
                    'schema is not populated')
        L().delete()
        assert_false(bool(L() or D() or E() or F()), 'incomplete delete')
        A().delete()  # delete all is necessary because delete L deletes from subtables. TODO: submit this as an issue

    # @staticmethod
    # def test_delete_lookup_restricted():
    #     assert_false(dj.config['safemode'], 'safemode must be off for testing')
    #     assert_true(L() and A() and B() and C() and D() and E() and F(),
    #                 'schema is not populated')
    #     rel = L() & 'cond_in_l'
    #     L().delete()
