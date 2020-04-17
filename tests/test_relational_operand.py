import random
import string
import pandas
import datetime

import numpy as np
from nose.tools import assert_equal, assert_false, assert_true, raises, assert_set_equal, assert_list_equal

import datajoint as dj
from .schema_simple import A, B, D, E, F, L, DataA, DataB, TTestUpdate, IJ, JI, ReservedWord
from .schema import Experiment, TTest3


def setup():
    """
    module-level test setup
    """
    A.insert(A.contents, skip_duplicates=True)
    L.insert(L.contents, skip_duplicates=True)
    B.populate()
    D.populate()
    E.populate()
    Experiment.populate()


class TestRelational:
    @staticmethod
    def test_populate():
        assert_false(B().progress(display=False)[0], 'B incompletely populated')
        assert_false(D().progress(display=False)[0], 'D incompletely populated')
        assert_false(E().progress(display=False)[0], 'E incompletely populated')

        assert_true(len(B()) == 40, 'B populated incorrectly')
        assert_true(len(B.C()) > 0, 'C populated incorrectly')
        assert_true(len(D()) == 40, 'D populated incorrectly')
        assert_true(len(E()) == len(B()) * len(D()) / len(A()), 'E populated incorrectly')
        assert_true(len(E.F()) > 0, 'F populated incorrectly')

    @staticmethod
    def test_free_relation():
        b = B()
        free = dj.FreeTable(b.connection, b.full_table_name)
        assert_true(repr(free).startswith('FreeTable') and b.full_table_name in repr(free))
        r = 'n>5'
        assert_equal((B() & r).make_sql(), (free & r).make_sql())

    @staticmethod
    def test_rename():
        # test renaming
        x = B().proj(i='id_a') & 'i in (1,2,3,4)'
        lenx = len(x)
        assert_equal(len(x), len(B() & 'id_a in (1,2,3,4)'),
                     'incorrect restriction of renamed attributes')
        assert_equal(len(x & 'id_b in (1,2)'), len(B() & 'id_b in (1,2) and id_a in (1,2,3,4)'),
                     'incorrect restriction of renamed restriction')
        assert_equal(len(x), lenx, 'restriction modified original')
        y = x.proj(j='i')
        assert_equal(len(y), len(B() & 'id_a in (1,2,3,4)'),
                     'incorrect projection of restriction')
        assert_equal(len(y & 'j in (3,4,5,6)'), len(B() & 'id_a in (3,4)'),
                     'incorrect nested subqueries')

    @staticmethod
    def test_rename_order():
        """
        Renaming projection should not change the order of the primary key attributes.
        See issues #483 and #516.
        """
        pk1 = D.primary_key
        pk2 = D.proj(a='id_a').primary_key
        assert_list_equal(['a' if i == 'id_a' else i for i in pk1], pk2)

    @staticmethod
    def test_join():
        # Test cartesian product
        x = A()
        y = L()
        rel = x * y
        assert_equal(len(rel), len(x) * len(y),
                     'incorrect join')
        assert_equal(set(x.heading.names).union(y.heading.names), set(rel.heading.names),
                     'incorrect join heading')
        assert_equal(set(x.primary_key).union(y.primary_key), set(rel.primary_key),
                     'incorrect join primary_key')

        # Test cartesian product of restricted relations
        x = A() & 'cond_in_a=1'
        y = L() & 'cond_in_l=1'
        rel = x * y
        assert_equal(len(rel), len(x) * len(y),
                     'incorrect join')
        assert_equal(set(x.heading.names).union(y.heading.names), set(rel.heading.names),
                     'incorrect join heading')
        assert_equal(set(x.primary_key).union(y.primary_key), set(rel.primary_key),
                     'incorrect join primary_key')

        # Test join with common attributes
        cond = A() & 'cond_in_a=1'
        x = B() & cond
        y = D()
        rel = x * y
        assert_true(len(rel) >= len(x) and len(rel) >= len(y), 'incorrect join')
        assert_false(rel - cond, 'incorrect join, restriction, or antijoin')
        assert_equal(set(x.heading.names).union(y.heading.names), set(rel.heading.names),
                     'incorrect join heading')
        assert_equal(set(x.primary_key).union(y.primary_key), set(rel.primary_key),
                     'incorrect join primary_key')

        # test renamed join
        x = B().proj(i='id_a')  # rename the common attribute to achieve full cartesian product
        y = D()
        rel = x * y
        assert_equal(len(rel), len(x) * len(y),
                     'incorrect join')
        assert_equal(set(x.heading.names).union(y.heading.names), set(rel.heading.names),
                     'incorrect join heading')
        assert_equal(set(x.primary_key).union(y.primary_key), set(rel.primary_key),
                     'incorrect join primary_key')
        x = B().proj(a='id_a')
        y = D()
        rel = x * y
        assert_equal(len(rel), len(x) * len(y), 'incorrect join')
        assert_equal(set(x.heading.names).union(y.heading.names), set(rel.heading.names),
                     'incorrect join heading')
        assert_equal(set(x.primary_key).union(y.primary_key), set(rel.primary_key),
                     'incorrect join primary_key')

        # test pairing
        # Approach 1: join then restrict
        x = A.proj(a1='id_a', c1='cond_in_a')
        y = A.proj(a2='id_a', c2='cond_in_a')
        rel = x * y & 'c1=0' & 'c2=1'
        lenx = len(x & 'c1=0')
        leny = len(y & 'c2=1')
        assert_equal(lenx + leny, len(A()),
                     'incorrect restriction')
        assert_equal(len(rel), len(x & 'c1=0') * len(y & 'c2=1'),
                     'incorrect pairing')
        # Approach 2: restrict then join
        x = (A & 'cond_in_a=0').proj(a1='id_a')
        y = (A & 'cond_in_a=1').proj(a2='id_a')
        assert_equal(len(rel), len(x * y))

    @staticmethod
    def test_issue_376():
        tab = TTest3()
        tab.delete_quick()
        tab.insert((
            (1, '%%%'),
            (2, 'one%'),
            (3, 'one')))
        assert_equal(len(tab & 'value="%%%"'), 1)
        assert_equal(len(tab & {'value': "%%%"}), 1)
        assert_equal(len(tab & 'value like "o%"'), 2)
        assert_equal(len(tab & 'value like "o%%"'), 2)

    @staticmethod
    def test_issue_463():
        assert_equal(((A & B) * B).fetch().size, len(A * B))

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
    def test_union():
        x = set(zip(*IJ.fetch('i','j')))
        y = set(zip(*JI.fetch('i','j')))
        assert_true(len(x) > 0 and len(y) > 0 and len(IJ() * JI()) < len(x))  # ensure the IJ and JI are non-trivial
        z = set(zip(*(IJ + JI).fetch('i','j')))   # union
        assert_set_equal(x.union(y), z)

    @staticmethod
    def test_preview():
        with dj.config(display__limit=7):
            x = A().proj(a='id_a')
            s = x.preview()
            assert_equal(len(s.split('\n')), len(x) + 2)

    @staticmethod
    def test_heading_repr():
        x = A * D
        s = repr(x.heading)
        assert_equal(len(list(1 for g in s.split('\n') if g.strip() and not g.strip().startswith(('-', '#')))),
                     len(x.heading.attributes))

    @staticmethod
    def test_aggregate():
        x = B().aggregate(B.C())
        assert_equal(len(x), len(B() & B.C()))

        x = B().aggregate(B.C(), keep_all_rows=True)
        assert_equal(len(x), len(B()))  # test LEFT join

        assert_equal(len((x & 'id_b=0').fetch()), len(B() & 'id_b=0'))  # test restricted aggregation

        x = B().aggregate(B.C(), 'n', count='count(id_c)', mean='avg(value)', max='max(value)', keep_all_rows=True)
        assert_equal(len(x), len(B()))
        y = x & 'mean>0'  # restricted aggregation
        assert_true(len(y) > 0)
        assert_true(all(y.fetch('mean') > 0))
        for n, count, mean, max_, key in zip(*x.fetch('n', 'count', 'mean', 'max', dj.key)):
            assert_equal(n, count, 'aggregation failed (count)')
            values = (B.C() & key).fetch('value')
            assert_true(bool(len(values)) == bool(n),
                        'aggregation failed (restriction)')
            if n:
                assert_true(np.isclose(mean, values.mean(), rtol=1e-4, atol=1e-5),
                            "aggregation failed (mean)")
                assert_true(np.isclose(max_, values.max(), rtol=1e-4, atol=1e-5),
                            "aggregation failed (max)")

    @staticmethod
    def test_aggr():
        x = B.aggr(B.C)
        assert_equal(len(x), len(B() & B.C()))

        x = B().aggr(B.C(), keep_all_rows=True)
        assert_equal(len(x), len(B()))  # test LEFT join

        assert_equal(len((x & 'id_b=0').fetch()), len(B() & 'id_b=0'))  # test restricted aggregation

        x = B().aggr(B.C(), 'n', count='count(id_c)', mean='avg(value)', max='max(value)', keep_all_rows=True)
        assert_equal(len(x), len(B()))
        y = x & 'mean>0'  # restricted aggregation
        assert_true(len(y) > 0)
        assert_true(all(y.fetch('mean') > 0))
        for n, count, mean, max_, key in zip(*x.fetch('n', 'count', 'mean', 'max', dj.key)):
            assert_equal(n, count, 'aggregation failed (count)')
            values = (B.C() & key).fetch('value')
            assert_true(bool(len(values)) == bool(n),
                        'aggregation failed (restriction)')
            if n:
                assert_true(np.isclose(mean, values.mean(), rtol=1e-4, atol=1e-5),
                            "aggregation failed (mean)")
                assert_true(np.isclose(max_, values.max(), rtol=1e-4, atol=1e-5),
                            "aggregation failed (max)")

    @staticmethod
    def test_semijoin():
        """
        test that semijoins and antijoins are formed correctly
        """
        x = IJ()
        y = JI()
        n = len(x & y.fetch(as_dict=True))
        m = len(x - y.fetch(as_dict=True))
        assert_true(n > 0 and m > 0)
        assert_true(len(x) == m + n)
        assert_true(len(x & y.fetch()) == n)
        assert_true(len(x - y.fetch()) == m)
        semi = x & y
        anti = x - y
        assert_true(len(semi) == n)
        assert_true(len(anti) == m)

    @staticmethod
    def test_pandas_fetch_and_restriction():
        q = (L & 'cond_in_l = 0')
        df = q.fetch(format='frame')   # pandas dataframe
        assert_true(isinstance(df, pandas.DataFrame))
        assert_equal(len(E & q), len(E & df))

    @staticmethod
    def test_restrictions_by_lists():
        x = D()
        y = L() & 'cond_in_l'

        lenx = len(x)
        assert_true(lenx > 0 and len(y) > 0 and len(x & y) < len(x), 'incorrect test setup')

        assert_equal(len(D()), len(D & dj.AndList([])))
        assert_true(len(D & []) == 0)
        assert_true(len(D & [[]]) == 0)  # an OR-list of OR-list

        lenx = len(x)
        assert_true(lenx > 0 and len(y) > 0 and len(x & y) < len(x), 'incorrect test setup')
        assert_equal(len(x & y), len(D * L & 'cond_in_l'),
                     'incorrect semijoin')
        assert_equal(len(x - y), len(x) - len(x & y),
                     'incorrect antijoin')
        assert_equal(len(y - x), len(y) - len(y & x),
                     'incorrect antijoin')
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
        date = Experiment().fetch('experiment_date')[0]
        e1 = Experiment() & dict(experiment_date=str(date))
        e2 = Experiment() & dict(experiment_date=date)
        assert_true(len(e1) == len(e2) > 0, 'Two date restriction do not yield the same result')

    @staticmethod
    def test_date():
        """Test date update"""
        # https://github.com/datajoint/datajoint-python/issues/664
        F.insert1((2, '2019-09-25'))

        new_value = None
        (F & 'id=2')._update('date', new_value)
        assert_equal((F & 'id=2').fetch1('date'), new_value)

        new_value = datetime.date(2019, 10, 25)
        (F & 'id=2')._update('date', new_value)
        assert_equal((F & 'id=2').fetch1('date'), new_value)

        (F & 'id=2')._update('date')
        assert_equal((F & 'id=2').fetch1('date'), None)

    @staticmethod
    def test_join_project():
        """Test join of projected relations with matching non-primary key"""
        assert_true(len(DataA.proj() * DataB.proj()) == len(DataA()) == len(DataB()),
                    "Join of projected relations does not work")

    @staticmethod
    def test_ellipsis():
        r = Experiment.proj(..., '- data_path').head(1, as_dict=True)
        assert_set_equal(set(Experiment.heading).difference(r[0]), {'data_path'})

    @staticmethod
    @raises(dj.DataJointError)
    def test_update_single_key():
        """Test that only one row can be updated"""
        TTestUpdate()._update('string_attr', 'my new string')

    @staticmethod
    @raises(dj.DataJointError)
    def test_update_no_primary():
        """Test that no primary key can be updated"""
        TTestUpdate()._update('primary_key', 2)

    @staticmethod
    @raises(dj.DataJointError)
    def test_update_missing_attribute():
        """Test that attribute is in table"""
        TTestUpdate()._update('not_existing', 2)

    @staticmethod
    def test_update_string_attribute():
        """Test replacing a string value"""
        rel = (TTestUpdate() & dict(primary_key=0))
        s = ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(10))
        rel._update('string_attr', s)
        assert_equal(s, rel.fetch1('string_attr'), "Updated string does not match")

    @staticmethod
    def test_update_numeric_attribute():
        """Test replacing a string value"""
        rel = (TTestUpdate() & dict(primary_key=0))
        s = random.randint(0, 10)
        rel._update('num_attr', s)
        assert_equal(s, rel.fetch1('num_attr'), "Updated integer does not match")
        rel._update('num_attr', None)
        assert_true(np.isnan(rel.fetch1('num_attr')), "Numeric value is not NaN")

    @staticmethod
    def test_update_blob_attribute():
        """Test replacing a string value"""
        rel = (TTestUpdate() & dict(primary_key=0))
        s = rel.fetch1('blob_attr')
        rel._update('blob_attr', s.T)
        assert_equal(s.T.shape, rel.fetch1('blob_attr').shape, "Array dimensions do not match")

    @staticmethod
    def test_reserved_words():
        """Test the user of SQL reserved words as attributes"""
        rel = ReservedWord()
        rel.insert1({'key': 1, 'in': 'ouch', 'from': 'bummer', 'int': 3, 'select': 'major pain'})
        assert_true((rel & {'key': 1, 'in': 'ouch', 'from': 'bummer'}).fetch1('int') == 3)
        assert_true((rel.proj('int', double='from') & {'double': 'bummer'}).fetch1('int') == 3)
        (rel & {'key': 1}).delete()

    @staticmethod
    @raises(dj.DataJointError)
    def test_reserved_words2():
        """Test the user of SQL reserved words as attributes"""
        rel = ReservedWord()
        rel.insert1({'key': 1, 'in': 'ouch', 'from': 'bummer', 'int': 3, 'select': 'major pain'})
        (rel & 'key=1').fetch('in')  # error because reserved word `key` is not in backquotes. See issue #249
