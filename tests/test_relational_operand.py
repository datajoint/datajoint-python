import numpy as np
from nose.tools import assert_raises, assert_equal, \
    assert_false, assert_true, assert_list_equal, \
    assert_tuple_equal, assert_dict_equal, raises
import datajoint as dj
from .schema_simple import A, B, D, E, L, DataA, DataB
from .schema import Experiment


def setup():
    """
    module-level test setup
    """
    A().insert(A.contents, skip_duplicates=True)
    L().insert(L.contents, skip_duplicates=True)
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
        assert_true(len(B.C()) > 0, 'C populated incorrectly')
        assert_true(len(D()) == 40, 'D populated incorrectly')
        assert_true(len(E()) == len(B())*len(D())/len(A()), 'E populated incorrectly')
        assert_true(len(E.F()) > 0, 'F populated incorrectly')

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
        x = B().proj(i='id_a')   # rename the common attribute to achieve full cartesian product
        y = D()
        rel = x*y
        assert_equal(len(rel), len(x)*len(y),
                     'incorrect join')
        assert_equal(set(x.heading.names).union(y.heading.names), set(rel.heading.names),
                     'incorrect join heading')
        assert_equal(set(x.primary_key).union(y.primary_key), set(rel.primary_key),
                     'incorrect join primary_key')
        x = B().proj(a='id_a')
        y = D()
        rel = x*y
        assert_equal(len(rel), len(x)*len(y), 'incorrect join')
        assert_equal(set(x.heading.names).union(y.heading.names), set(rel.heading.names),
                     'incorrect join heading')
        assert_equal(set(x.primary_key).union(y.primary_key), set(rel.primary_key),
                     'incorrect join primary_key')

        # test pairing
        # Approach 1: join then restrict
        x = A().proj(a1='id_a', c1='cond_in_a')
        y = A().proj(a2='id_a', c2='cond_in_a')
        rel = x*y & 'c1=0' & 'c2=1'
        lenx = len(x & 'c1=0')
        leny = len(y & 'c2=1')
        assert_equal(lenx + leny, len(A()),
                     'incorrect restriction')
        assert_equal(len(rel), len(x & 'c1=0')*len(y & 'c2=1'),
                     'incorrect pairing')
        # Approach 2: restrict then join
        x = (A() & 'cond_in_a=0').proj(a1='id_a')
        y = (A() & 'cond_in_a=1').proj(a2='id_a')
        assert_equal(len(rel), len(x*y))

    @staticmethod
    def test_project():
        x = A().proj(a='id_a')  # rename
        assert_equal(x.heading.names, ['a'],
                     'renaming does not work')
        x = A().proj(a='(id_a)')  # extend
        assert_equal(set(x.heading.names), set(('id_a', 'a')),
                     'extend does not work')

        # projection after restriction
        cond = L() & 'cond_in_l'
        assert_equal(len(D() & cond) + len(D() - cond), len(D()),
                     'failed semijoin or antijoin')
        assert_equal(len((D() & cond).proj()), len((D() & cond)),
                     'projection failed: altered its argument''s cardinality')

    @staticmethod
    def test_preview():
        x = A().proj(a='id_a')
        s = x.preview()
        assert_equal(len(s.split('\n')), len(x)+2)

    @staticmethod
    def test_heading_repr():
        x = A()*D()
        s = repr(x.heading)
        assert_equal(len(s.split('\n')), len(x.heading.attributes))

    @staticmethod
    def test_aggregate():
        x = B().aggregate(B.C())
        assert_equal(len(x), len(B() & B.C()))

        x = B().aggregate(B.C(), keep_all_rows=True)
        assert_equal(len(x), len(B()))    # test LEFT join

        assert_equal(len((x & 'id_b=0').fetch()), len(B() & 'id_b=0'))   # test restricted aggregation

        x = B().aggregate(B.C(), 'n', count='count(id_c)', mean='avg(value)', max='max(value)', keep_all_rows=True)
        assert_equal(len(x), len(B()))
        y = x & 'mean>0'   # restricted aggregation
        assert_true(len(y) > 0)
        assert_true(all(y.fetch['mean'] > 0))
        for n, count, mean, max_, key in zip(*x.fetch['n', 'count', 'mean', 'max', dj.key]):
            assert_equal(n, count, 'aggregation failed (count)')
            values = (B.C() & key).fetch['value']
            assert_true(bool(len(values)) == bool(n),
                        'aggregation failed (restriction)')
            if n:
                assert_true(np.isclose(mean, values.mean(), rtol=1e-4, atol=1e-5),
                            "aggregation failed (mean)")
                assert_true(np.isclose(max_, values.max(), rtol=1e-4, atol=1e-5),
                            "aggregation failed (max)")

    @staticmethod
    def test_restrictions_by_lists():
        x = D()
        y = L() & 'cond_in_l'
        lenx = len(x)
        assert_true(lenx > 0 and len(y) > 0 and len(x & y) < len(x), 'incorrect test setup')
        assert_equal(len(x & y), len(D()*L() & 'cond_in_l'),
                     'incorrect restriction of restriction')
        assert_true(len(x & []) == 0,
                    'incorrect restriction by an empty list')
        assert_true(len(x & ()) == 0,
                    'incorrect restriction by an empty tuple')
        assert_true(len(x & set()) == 0,
                    'incorrect restriction by an empty set')
        assert_equal(len(x - []), lenx,
                     'incorrect restriction by an empty list')
        assert_equal(len(x - ()), lenx,
                     'incorrect restriction by an empty tuple')
        assert_equal(len(x - set()), lenx,
                     'incorrect restriction by an empty set')
        assert_equal(len(x & {}), lenx,
                     'incorrect restriction by a tuple with no attributes')
        assert_true(len(x - {}) == 0,
                    'incorrect restriction by a tuple with no attributes')
        assert_equal(len(x & {'foo': 0}), lenx,
                     'incorrect restriction by a tuple with no matching attributes')
        assert_true(len(x - {'foo': 0}) == 0,
                    'incorrect restriction by a tuple with no matching attributes')
        assert_equal(len(x & y), len(x & y.fetch()),
                     'incorrect restriction by a list')
        assert_equal(len(x - y), len(x - y.fetch()),
                     'incorrect restriction by a list')
        w = A()
        assert_true(len(w) > 0, 'incorrect test setup: w is empty')
        assert_false(bool(set(w.heading.names) & set(y.heading.names)),
                     'incorrect test setup: w and y should have no common attributes')
        assert_equal(len(w), len(w & y),
                     'incorrect restriction without common attributes')
        assert_true(len(w - y) == 0,
                    'incorrect restriction without common attributes')

    @staticmethod
    def test_datetime():
        """Test date retrieval"""
        date = Experiment().fetch['experiment_date'][0]
        e1 = Experiment() & dict(experiment_date=str(date))
        e2 = Experiment() & dict(experiment_date=date)
        assert_true(len(e1) == len(e2) > 0, 'Two date restriction do not yield the same result')

    @staticmethod
    def test_join_project_optimization():
        """Test optimization for join of projected relations with matching non-primary key"""
        assert_true(len(DataA().proj() * DataB().proj()) == len(DataA()) == len(DataB()),
                    "Join of projected relations does not work")
