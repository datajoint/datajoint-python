"""
A simple, abstract schema to test relational algebra
"""

import random
import datajoint as dj
import itertools
import hashlib
import uuid
import faker
from . import PREFIX, CONN_INFO
import numpy as np
from datetime import date, timedelta

schema = dj.Schema(PREFIX + "_relational", locals(), connection=dj.conn(**CONN_INFO))


@schema
class IJ(dj.Lookup):
    definition = """  # tests restrictions
    i  : int
    j  : int
    """
    contents = list(dict(i=i, j=j + 2) for i in range(3) for j in range(3))


@schema
class JI(dj.Lookup):
    definition = """  # tests restrictions by relations when attributes are reordered
    j  : int
    i  : int
    """
    contents = list(dict(i=i + 1, j=j) for i in range(3) for j in range(3))


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
        lookup = list(L().fetch("KEY"))
        self.insert(dict(key, id_d=i, **random.choice(lookup)) for i in range(4))


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

    def make(self, key):
        random.seed(str(key))
        self.insert1(dict(key, **random.choice(list(L().fetch("KEY")))))
        sub = E.F()
        references = list((B.C() & key).fetch("KEY"))
        random.shuffle(references)
        sub.insert(
            dict(key, id_f=i, **ref)
            for i, ref in enumerate(references)
            if random.getrandbits(1)
        )


@schema
class F(dj.Manual):
    definition = """
    id: int
    ----
    date=null: date
    """


@schema
class DataA(dj.Lookup):
    definition = """
    idx     : int
    ---
    a       : int
    """
    contents = list(zip(range(5), range(5)))


@schema
class DataB(dj.Lookup):
    definition = """
    idx     : int
    ---
    a       : int
    """
    contents = list(zip(range(5), range(5, 10)))


@schema
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


@schema
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


@schema
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


@schema
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


@schema
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


@schema
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
