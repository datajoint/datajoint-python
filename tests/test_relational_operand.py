import numpy as np
from nose.tools import assert_raises, assert_equal, \
    assert_false, assert_true, assert_list_equal, \
    assert_tuple_equal, assert_dict_equal, raises
import datajoint as dj
from .schema_simple import A, B, C, D, E, F, L


def setup():
    """
    module-level test setup
    """
    B().populate()
    D().populate()
    E().populate()


class TestRelational:

    @staticmethod
    def test_populate():
        assert_false(B().progress(display=False)[0], 'B incompletely populated')
        assert_false(D().progress(display=False)[0], 'D incompletely populated')
        assert_false(E().progress(display=False)[0], 'E incompletely populated')

        assert_true(len(B()) == 40, 'B populated incorrectly')
        assert_true(len(C()) > 0, 'C populated incorrectly')
        assert_true(len(D()) == 40, 'D populated incorrectly')
        assert_true(len(E()) == len(B())*len(D())/len(A()), 'E populated incorrectly')
        assert_true(len(F()) > 0, 'F populated incorrectly')

    @staticmethod
    def test_join():
        # Test cartesian product
        x = A()
        y = L()
        rel = x*y
        assert_equal(len(rel), len(x)*len(y),
                     'incorrect join')
        assert_equal(set(x.heading.names).union(y.heading.names), set(rel.heading.names),
                     'incorrect join heading')
        assert_equal(set(x.primary_key).union(y.primary_key), set(rel.primary_key),
                     'incorrect join primary_key')

        # Test cartesian product of restricted relations
        x = A() & 'cond_in_a=1'
        y = L() & 'cond_in_l=1'
        rel = x*y
        assert_equal(len(rel), len(x)*len(y),
                     'incorrect join')
        assert_equal(set(x.heading.names).union(y.heading.names), set(rel.heading.names),
                     'incorrect join heading')
        assert_equal(set(x.primary_key).union(y.primary_key), set(rel.primary_key),
                     'incorrect join primary_key')

        # Test join with common attributes
        cond = A() & 'cond_in_a=1'
        x = B() & cond
        y = D()
        rel = x*y
        assert_true(len(rel) >= len(x) and len(rel) >= len(y), 'incorrect join')
        assert_false(rel - cond, 'incorrect join, restriction, or antijoin')
        assert_equal(set(x.heading.names).union(y.heading.names), set(rel.heading.names),
                     'incorrect join heading')
        assert_equal(set(x.primary_key).union(y.primary_key), set(rel.primary_key),
                     'incorrect join primary_key')

        # test renamed join
        x = B().project(i='id_a')   # rename the common attribute to achieve full cartesian product
        y = D()
        rel = x*y
        assert_equal(len(rel), len(x)*len(y),
                     'incorrect join')
        assert_equal(set(x.heading.names).union(y.heading.names), set(rel.heading.names),
                     'incorrect join heading')
        assert_equal(set(x.primary_key).union(y.primary_key), set(rel.primary_key),
                     'incorrect join primary_key')

        # test the % notation
        x = B() % ['id_a->a']
        y = D()
        rel = x*y
        assert_equal(len(rel), len(x)*len(y),
                     'incorrect join')
        assert_equal(set(x.heading.names).union(y.heading.names), set(rel.heading.names),
                     'incorrect join heading')
        assert_equal(set(x.primary_key).union(y.primary_key), set(rel.primary_key),
                     'incorrect join primary_key')

        # test pairing
        # Approach 1: join then restrict
        x = A().project(a1='id_a', c1='cond_in_a')
        y = A().project(a2='id_a', c2='cond_in_a')
        rel = x*y & 'c1=0' & 'c2=1'
        assert_equal(len(x & 'c1=0')+len(y & 'c2=1'), len(A()),
                     'incorrect restriction')
        assert_equal(len(rel), len(x & 'c1=0')*len(y & 'c2=1'),
                     'incorrect pairing')
        # Approach 2: restrict then join
        x = (A() & 'cond_in_a=0').project(a1='id_a')
        y = (A() & 'cond_in_a=1').project(a2='id_a')
        assert_equal(len(rel), len(x*y))

    @staticmethod
    def test_project():
        x = A().project(a='id_a')  # rename
        assert_equal(x.heading.names, ['a'],
                     'renaming does not work')
        x = A().project(a='(id_a)')  # extend
        assert_equal(set(x.heading.names), set(('id_a', 'a')),
                     'extend does not work')

        # projection after restriction
        cond = L() & 'cond_in_l'
        assert_equal(len(D() & cond) + len(D() - cond), len(D()),
                     'failed semijoin or antijoin')
        assert_equal(len((D() & cond).project()), len((D() & cond)),
                     'projection failed: altered its argument''s cardinality')

    @staticmethod
    def test_aggregate():
        x = B().aggregate(C(), 'n', count='count(id_c)', mean='avg(value)', max='max(value)')
        assert_equal(len(x), len(B()))
        for n, count, mean, max_, key in zip(*x.fetch['n', 'count', 'mean', 'max', dj.key]):
            assert_equal(n, count, 'aggregation failed (count)')
            values = (C() & key).fetch['value']
            assert_true(bool(len(values)) == bool(n),
                        'aggregation failed (restriction)')
            if n:
                assert_true(np.isclose(mean, values.mean(), rtol=1e-4, atol=1e-5),
                            "aggregation failed (mean)")
                assert_true(np.isclose(max_, values.max(), rtol=1e-4, atol=1e-5),
                            "aggregation failed (max)")
