from nose.tools import assert_equal
import uuid
from .schema import Item
from itertools import count


def test_uuid():
    u, n = uuid.uuid4(), -1
    Item().insert1(dict(item=u, number=n))
    Item().insert(zip(map(uuid.uuid1, range(20)), count()))
    keys = Item().fetch('KEY')
    keys_array = Item().fetch('KEY_ARRAY')
    number = (Item() & {'item': u}).fetch1('number')
    assert_equal(number, n)
