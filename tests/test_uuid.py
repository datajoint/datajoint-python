import pytest
import uuid
from .schema_uuid import Basic, Item, Topic
from datajoint import DataJointError
from itertools import count


def test_uuid(schema_uuid):
    """test inserting and fetching of UUID attributes and restricting by UUID attributes"""
    u, n = uuid.uuid4(), -1
    Basic().insert1(dict(item=u, number=n))
    Basic().insert(zip(map(uuid.uuid1, range(20)), count()))
    number = (Basic() & {"item": u}).fetch1("number")
    assert number == n
    item = (Basic & {"number": n}).fetch1("item")
    assert u == item


def test_string_uuid(schema_uuid):
    """test that only UUID objects are accepted when inserting UUID fields"""
    u, n = "00000000-0000-0000-0000-000000000000", 24601
    Basic().insert1(dict(item=u, number=n))
    k, m = (Basic & {"item": u}).fetch1("KEY", "number")
    assert m == n
    assert isinstance(k["item"], uuid.UUID)


def test_invalid_uuid_insert1(schema_uuid):
    """test that only UUID objects are accepted when inserting UUID fields"""
    u, n = 0, 24601
    with pytest.raises(DataJointError):
        Basic().insert1(dict(item=u, number=n))


def test_invalid_uuid_insert2(schema_uuid):
    """test that only UUID objects are accepted when inserting UUID fields"""
    u, n = "abc", 24601
    with pytest.raises(DataJointError):
        Basic().insert1(dict(item=u, number=n))


def test_invalid_uuid_restrict1(schema_uuid):
    """test that only UUID objects are accepted when inserting UUID fields"""
    u = 0
    with pytest.raises(DataJointError):
        k, m = (Basic & {"item": u}).fetch1("KEY", "number")


def test_invalid_uuid_restrict1(schema_uuid):
    """test that only UUID objects are accepted when inserting UUID fields"""
    u = "abc"
    with pytest.raises(DataJointError):
        k, m = (Basic & {"item": u}).fetch1("KEY", "number")


def test_uuid_dependencies(schema_uuid):
    """test the use of UUID in foreign keys"""
    for word in (
        "Neuroscience",
        "Knowledge",
        "Curiosity",
        "Inspiration",
        "Science",
        "Philosophy",
        "Conscience",
    ):
        Topic().add(word)
    Item.populate()
    assert Item().progress() == (0, len(Topic()))
