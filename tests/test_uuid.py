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


def test_string_uuid():
    """test that only UUID objects are accepted when inserting UUID fields"""
    u, n = '00000000-0000-0000-0000-000000000000', 24601
    Basic().insert1(dict(item=u, number=n))
    k, m = (Basic & {'item': u}).fetch1('KEY', 'number')
    assert_equal(m, n)
    assert_true(isinstance(k['item'], uuid.UUID))


@raises(DataJointError)
def test_invalid_uuid_insert1():
    """test that only UUID objects are accepted when inserting UUID fields"""
    u, n = 0, 24601
    Basic().insert1(dict(item=u, number=n))


@raises(DataJointError)
def test_invalid_uuid_insert2():
    """test that only UUID objects are accepted when inserting UUID fields"""
    u, n = 'abc', 24601
    Basic().insert1(dict(item=u, number=n))


@raises(DataJointError)
def test_invalid_uuid_restrict1():
    """test that only UUID objects are accepted when inserting UUID fields"""
    u = 0
    k, m = (Basic & {'item': u}).fetch1('KEY', 'number')


@raises(DataJointError)
def test_invalid_uuid_restrict1():
    """test that only UUID objects are accepted when inserting UUID fields"""
    u = 'abc'
    k, m = (Basic & {'item': u}).fetch1('KEY', 'number')


def test_uuid_dependencies():
    """ test the use of UUID in foreign keys """
    for word in ('Neuroscience', 'Knowledge', 'Curiosity', 'Inspiration', 'Science', 'Philosophy', 'Conscience'):
        Topic().add(word)
    Item.populate()
    assert_equal(Item().progress(), (0, len(Topic())))
