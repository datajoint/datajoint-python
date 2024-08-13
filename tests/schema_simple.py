"""
A simple, abstract schema to test relational algebra
"""

import random
import datajoint as dj
import itertools
import hashlib
import uuid
import faker
import numpy as np
from datetime import date, timedelta
import inspect


class IJ(dj.Lookup):
    definition = """  # tests restrictions
    i  : int
    j  : int
    """
    contents = list(dict(i=i, j=j + 2) for i in range(3) for j in range(3))


class JI(dj.Lookup):
    definition = """  # tests restrictions by relations when attributes are reordered
    j  : int
    i  : int
    """
    contents = list(dict(i=i + 1, j=j) for i in range(3) for j in range(3))


class A(dj.Lookup):
    definition = """
    id_a :int
    ---
    cond_in_a :tinyint
    """
    contents = [(i, i % 4 > i % 3) for i in range(10)]


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

    def make(self, key):
        random.seed(str(key))
        sub = B.C()
        for i in range(4):
            key["id_b"] = i
            mu = random.normalvariate(0, 10)
            sigma = random.lognormvariate(0, 4)
            n = random.randint(0, 10)
            self.insert1(dict(key, mu=mu, sigma=sigma, n=n))
            sub.insert(
                dict(key, id_c=j, value=random.normalvariate(mu, sigma))
                for j in range(n)
            )


class L(dj.Lookup):
    definition = """
    id_l: int
    ---
    cond_in_l :tinyint
    """
    contents = [(i, i % 3 >= i % 5) for i in range(30)]


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
        lookup = list(L().fetch("KEY"))
        self.insert(dict(key, id_d=i, **random.choice(lookup)) for i in range(4))


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

    class G(dj.Part):
        definition = """ # test secondary fk reference
        -> E
        id_g :int
        ---
        -> L
        """

    class H(dj.Part):
        definition = """ # test no additional fk reference
        -> E
        id_h :int
        """

    class M(dj.Part):
        definition = """ # test force_masters revisit
        -> E
        id_m :int
        ---
        -> E.H
        """

    def make(self, key):
        random.seed(str(key))
        l_contents = list(L().fetch("KEY"))
        part_f, part_g, part_h, part_m = E.F(), E.G(), E.H(), E.M()
        bc_references = list((B.C() & key).fetch("KEY"))
        random.shuffle(bc_references)

        self.insert1(dict(key, **random.choice(l_contents)))
        part_f.insert(
            dict(key, id_f=i, **ref)
            for i, ref in enumerate(bc_references)
            if random.getrandbits(1)
        )
        g_inserts = [dict(key, id_g=i, **ref) for i, ref in enumerate(l_contents)]
        part_g.insert(g_inserts)
        h_inserts = [dict(key, id_h=i) for i in range(4)]
        part_h.insert(h_inserts)
        part_m.insert(dict(key, id_m=m, **random.choice(h_inserts)) for m in range(4))


class F(dj.Manual):
    definition = """
    id: int
    ----
    date=null: date
    """


class G(dj.Computed):
    definition = """ # test downstream of complex master/parts
    -> E
    """

    def make(self, key):
        self.insert1(key)


class DataA(dj.Lookup):
    definition = """
    idx     : int
    ---
    a       : int
    """
    contents = list(zip(range(5), range(5)))


class DataB(dj.Lookup):
    definition = """
    idx     : int
    ---
    a       : int
    """
    contents = list(zip(range(5), range(5, 10)))


class Website(dj.Lookup):
    definition = """
    url_hash : uuid
    ---
    url : varchar(1000)
    """

    def insert1_url(self, url):
        hashed = hashlib.sha1()
        hashed.update(url.encode())
        url_hash = uuid.UUID(bytes=hashed.digest()[:16])
        self.insert1(dict(url=url, url_hash=url_hash), skip_duplicates=True)
        return url_hash


class Profile(dj.Manual):
    definition = """
    ssn : char(11)
    ---
    name : varchar(70)
    residence : varchar(255)
    blood_group  : enum('A+', 'A-', 'AB+', 'AB-', 'B+', 'B-', 'O+', 'O-')
    username : varchar(120)
    birthdate : date
    job : varchar(120)
    sex : enum('M', 'F')
    """

    class Website(dj.Part):
        definition = """
        -> master
        -> Website
        """

    def populate_random(self, n=10):
        fake = faker.Faker()
        faker.Faker.seed(0)  # make test deterministic
        for _ in range(n):
            profile = fake.profile()
            with self.connection.transaction:
                self.insert1(profile, ignore_extra_fields=True)
                for url in profile["website"]:
                    self.Website().insert1(
                        dict(ssn=profile["ssn"], url_hash=Website().insert1_url(url))
                    )


class TTestUpdate(dj.Lookup):
    definition = """
    primary_key     : int
    ---
    string_attr     : varchar(255)
    num_attr=null   : float
    blob_attr       : longblob
    """

    contents = [
        (0, "my_string", 0.0, np.random.randn(10, 2)),
        (1, "my_other_string", 1.0, np.random.randn(20, 1)),
    ]


class ArgmaxTest(dj.Lookup):
    definition = """
    primary_key     : int
    ---
    secondary_key   : char(2)
    val             : float
    """

    n = 10

    @property
    def contents(self):
        n = self.n
        yield from zip(
            range(n**2),
            itertools.chain(*itertools.repeat(tuple(map(chr, range(100, 100 + n))), n)),
            np.random.rand(n**2),
        )


class ReservedWord(dj.Manual):
    definition = """
    # Test of SQL reserved words
    key : int
    ---
    in    :  varchar(25)
    from  :  varchar(25)
    int   :  int
    select : varchar(25)
    """


class OutfitLaunch(dj.Lookup):
    definition = """
    # Monthly released designer outfits
    release_id: int
    ---
    day: date
    """
    contents = [(0, date.today() - timedelta(days=15))]

    class OutfitPiece(dj.Part, dj.Lookup):
        definition = """
        # Outfit piece associated with outfit
        -> OutfitLaunch
        piece: varchar(20)
        """
        contents = [(0, "jeans"), (0, "sneakers"), (0, "polo")]


LOCALS_SIMPLE = {k: v for k, v in locals().items() if inspect.isclass(v)}
__all__ = list(LOCALS_SIMPLE)
