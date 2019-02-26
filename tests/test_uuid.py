from nose.tools import assert_equal
import uuid
from .schema import Item
from itertools import count


def test_uuid():
    u, n = uuid.uuid4(), -1
    Item().insert1(dict(item=u, number=n))
    Item().insert(zip(map(uuid.uuid1, range(20)), count()))
    keys, key_array = Item().fetch('KEY', 'KEY_ARRAY')
    assert_equal(keys[0]['item'], key_array['item'][0])
    number = (Item() & {'item': u}).fetch1('number')
    assert_equal(number, n)
    item = (Item & {'number': n}).fetch1('item')
    assert_equal(u, item)
