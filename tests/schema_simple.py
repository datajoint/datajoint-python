"""
A simple, abstract schema to test relational algebra
"""
import random
import datajoint as dj
from . import PREFIX, CONN_INFO


schema = dj.schema(PREFIX + '_relational', locals(), connection=dj.conn(**CONN_INFO))


class A(dj.Lookup):
    definition = """
    id_a :int
    ---
    cond_in_a :tinyint
    """
    contents = [(i, i % 4 > i % 3) for i in range(10)]

assert issubclass(A, dj.BaseRelation)
assert not issubclass(A, dj.Part)
A = schema(A)
assert issubclass(A, dj.BaseRelation)
assert not issubclass(A, dj.Part)

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

    class C(dj.Part):
        definition = """
        -> B
        id_c :int
        ---
        value :float  # normally distributed variables according to parameters in B
        """

    def _make_tuples(self, key):
        random.seed(str(key))
        sub = B.C()
        for i in range(4):
            key['id_b'] = i
            mu = random.normalvariate(0, 10)
            sigma = random.lognormvariate(0, 4)
            n = random.randint(0, 10)
            self.insert1(dict(key, mu=mu, sigma=sigma, n=n))
            sub.insert((dict(key, id_c=j, value=random.normalvariate(mu, sigma)) for j in range(n)))


@schema
class L(dj.Lookup):
    definition = """
    id_l: int
    ---
    cond_in_l :tinyint
    """
    contents = [(i, i % 3 >= i % 5) for i in range(30)]


@schema
class D(dj.Computed):
    definition = """
    -> A
    id_d :int
    ---
    -> L
    """

    def _make_tuples(self, key):
        # make reference to a random tuple from L
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

    class F(dj.Part):
        definition = """
        -> E
        id_f :int
        ---
        -> B.C
        """

    def _make_tuples(self, key):
        random.seed(str(key))
        self.insert1(dict(key, **random.choice(list(L().fetch.keys()))))
        sub = E.F()
        references = list((B.C() & key).fetch.keys())
        random.shuffle(references)
        for i, ref in enumerate(references):
            if random.getrandbits(1):
                sub.insert1(dict(key, id_f=i, **ref))


@schema
class DataA(dj.Lookup):
    definition = """
    idx     : int
    ---
    a       : int
    """

    @property
    def contents(self):
        yield from zip(range(5), range(5))


@schema
class DataB(dj.Lookup):
    definition = """
    idx     : int
    ---
    a       : int
    """

    @property
    def contents(self):
        yield from zip(range(5), range(5, 10))
