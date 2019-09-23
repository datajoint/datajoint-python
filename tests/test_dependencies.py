from nose.tools import assert_true, assert_false, assert_equal, assert_list_equal, raises
from .schema import *


def test_nullable_dependency():
    """test nullable unique foreign key"""

    # Thing C has a nullable dependency on B whose primary key is composite
    a = ThingA()
    b = ThingB()
    c = ThingC()

    # clear previous contents if any.
    c.delete_quick()
    b.delete_quick()
    a.delete_quick()

    a.insert(dict(a=a) for a in range(7))

    b.insert1(dict(b1=1, b2=1, b3=100))
    b.insert1(dict(b1=1, b2=2, b3=100))

    # missing foreign key attributes = ok
    c.insert1(dict(a=0))
    c.insert1(dict(a=1, b1=33))
    c.insert1(dict(a=2, b2=77))

    # unique foreign key attributes = ok
    c.insert1(dict(a=3, b1=1, b2=1))
    c.insert1(dict(a=4, b1=1, b2=2))

    assert_true(len(c) == len(c.fetch()) == 5)


@raises(dj.errors.DuplicateError)
def test_unique_dependency():
    """test nullable unique foreign key"""

    # Thing C has a nullable dependency on B whose primary key is composite
    a = ThingA()
    b = ThingB()
    c = ThingC()

    # clear previous contents if any.
    c.delete_quick()
    b.delete_quick()
    a.delete_quick()

    a.insert(dict(a=a) for a in range(7))

    b.insert1(dict(b1=1, b2=1, b3=100))
    b.insert1(dict(b1=1, b2=2, b3=100))

    c.insert1(dict(a=0, b1=1, b2=1))
    # duplicate foreign key attributes = not ok
    c.insert1(dict(a=1, b1=1, b2=1))
