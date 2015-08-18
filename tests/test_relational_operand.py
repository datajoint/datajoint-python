import datajoint as dj
from . import PREFIX, CONN_INFO
import random
import numpy as np
from nose.tools import assert_raises, assert_equal, \
    assert_false, assert_true, assert_list_equal, \
    assert_tuple_equal, assert_dict_equal, raises


schema = dj.schema(PREFIX + '_relational', locals(), connection=dj.conn(**CONN_INFO))


@schema
class A(dj.Lookup):
    definition = """
    id_a :int
    ---
    cond_in_a :tinyint
    """
    contents = [(i, i % 4 > i % 3) for i in range(10)]


@schema
class B(dj.Computed):
    definition = """
    -> A
    id_b :int
    ---
    mu :float  # mean value
    sigma :float  # standard deviation
    n :smallint # number samples
    """

    def _make_tuples(self, key):
        random.seed(str(key))
        sub = C()
        for i in range(4):
            key['id_b'] = i
            mu = random.normalvariate(0, 10)
            sigma = random.lognormvariate(0, 4)
            n = random.randint(0, 10)
            self.insert1(dict(key, mu=mu, sigma=sigma, n=n))
            for j in range(n):
                sub.insert1(dict(key, id_c=j, value=random.normalvariate(mu, sigma)))


@schema
class C(dj.Subordinate, dj.Computed):
    definition = """
    -> B
    id_c :int
    ---
    value :float  # normally distributed variables according to parameters in B
    """


@schema
class L(dj.Lookup):
    definition = """
    id_l: int
    ---
    cond_in_l :tinyint
    """
    contents = ((i, i % 3 >= i % 5) for i in range(30))


@schema
class D(dj.Computed):
    definition = """
    -> A
    id_d :int
    ---
    -> L
    """

    def _make_tuples(self, key):
        # connect to random L
        random.seed(str(key))
        lookup = list(L().fetch.keys())
        for i in range(4):
            self.insert1(dict(key, id_d=i, **random.choice(lookup)))


@schema
class E(dj.Computed):
    definition = """
    -> B
    -> D
    ---
    -> L
    """

    def _make_tuples(self, key):
        random.seed(str(key))
        self.insert1(dict(key, **random.choice(list(L().fetch.keys()))))
        sub = F()
        references = list((C() & key).fetch.keys())
        random.shuffle(references)
        for i, ref in enumerate(references):
            if random.getrandbits(1):
                sub.insert1(dict(key, id_f=i, **ref))


@schema
class F(dj.Subordinate, dj.Computed):
    definition = """
    -> E
    id_f :int
    ---
    -> C
    """


def setup():
    """
    module-level test setup
    """
    B().populate()
    D().populate()
    E().populate()
    pass


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

    @staticmethod
    def test_aggregate():
        x = B().aggregate(C(), 'n', count='count(id_c)', mean='avg(value)', max='max(value)')
        assert_equal(len(x), len(B()))
        for n, count, mean, max, key in zip(*x.fetch['n', 'count', 'mean', 'max', dj.key]):
            assert_equal(n, count, 'aggregation failed (count)')
            values = (C() & key).fetch['value']
            assert_true(bool(len(values)) == bool(n),
                        'aggregation failed (restriction)')
            if n:
                assert_true(np.isclose(mean, values.mean(), rtol=1e-4, atol=1e-5),
                            "aggregation failed (mean)")
                assert_true(np.isclose(max, values.max(), rtol=1e-4, atol=1e-5),
                            "aggregation failed (max)")
