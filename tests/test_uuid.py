from nose.tools import assert_true, assert_equal, raises
import uuid
from .schema_uuid import Basic, Item, Topic
from datajoint import DataJointError
from itertools import count


def test_uuid():
    """test inserting and fetching of UUID attributes and restricting by UUID attributes"""
    u, n = uuid.uuid4(), -1
    Basic().insert1(dict(item=u, number=n))
    Basic().insert(zip(map(uuid.uuid1, range(20)), count()))
    number = (Basic() & {'item': u}).fetch1('number')
    assert_equal(number, n)
    item = (Basic & {'number': n}).fetch1('item')
    assert_equal(u, item)


@raises(DataJointError)
def test_invalid_uuid_type():
    """test that only UUID objects are accepted when inserting UUID fields"""
    Basic().insert(dict(item='00000000-0000-0000-0000-000000000000', number=0))


def test_uuid_dependencies():
    """
    :return:
    """
    for word in ('Neuroscience', 'Knowledge', 'Curiosity', 'Inspiration', 'Science', 'Philosophy', 'Conscience'):
        Topic().add(word)
    Item.populate()
    assert_equal(Item().progress(), (0, len(Topic())))
