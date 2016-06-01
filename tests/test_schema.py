from nose.tools import assert_raises, assert_equal, assert_not_equal, \
    assert_false, assert_true, assert_list_equal, \
    assert_tuple_equal, assert_dict_equal, raises
import datajoint as dj
from inspect import getmembers
from . import schema
from . import schema_empty
from . import PREFIX, CONN_INFO


def relation_selector(attr):
    try:
        return issubclass(attr, dj.BaseRelation)
    except TypeError:
        return False


def part_selector(attr):
    try:
        return issubclass(attr, dj.Part)
    except TypeError:
        return False


def test_namespace_population():
    for name, rel in getmembers(schema, relation_selector):
        assert_true(hasattr(schema_empty, name), '{name} not found in schema_empty'.format(name=name))
        assert_true(rel.__base__ is getattr(schema_empty, name).__base__, 'Wrong tier for {name}'.format(name=name))

        for name_part, part in getmembers(rel, part_selector):
            assert_true(hasattr(rel, name_part),
                        '{name_part} not found in {name}'.format(name_part=name_part, name=name))
            assert_true(getattr(rel, name_part).__base__ is dj.Part, 'Wrong tier for {name}'.format(name=name_part))


@raises(dj.DataJointError)
def test_undecorated_table():
    """
    Undecorated user relation classes should raise an informative exception upon first use
    """

    class UndecoratedClass(dj.Manual):
        definition = ""

    a = UndecoratedClass()
    a.full_table_name


@raises(dj.DataJointError)
def test_reject_decorated_part():
    """
    Decorating a dj.Part table should raise an informative exception.
    """

    @schema.schema
    class A(dj.Manual):
        definition = ...

        @schema.schema
        class B(dj.Part):
            definition = ...


@raises(dj.DataJointError)
def test_unauthorized_database():
    """
    an attempt to create a database to which user has no privileges should raise an informative exception.
    """
    dj.schema('unauthorized_schema', locals(), connection=dj.conn(**CONN_INFO))


def test_drop_database():
    schema = dj.schema(PREFIX + '_drop_test', locals(), connection=dj.conn(**CONN_INFO))
    assert_true(schema.exists)
    schema.drop()
    assert_false(schema.exists)
    schema.drop()   # should do nothing