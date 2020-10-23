from nose.tools import assert_false, assert_true, raises
import datajoint as dj
from inspect import getmembers
from . import schema
from . import schema_empty
from . import PREFIX, CONN_INFO


def relation_selector(attr):
    try:
        return issubclass(attr, dj.Table)
    except TypeError:
        return False


def part_selector(attr):
    try:
        return issubclass(attr, dj.Part)
    except TypeError:
        return False


def test_schema_size_on_disk():
    number_of_bytes = schema.schema.size_on_disk
    assert_true(isinstance(number_of_bytes, int))


def test_namespace_population():
    for name, rel in getmembers(schema, relation_selector):
        assert_true(hasattr(schema_empty, name), '{name} not found in schema_empty'.format(name=name))
        assert_true(rel.__base__ is getattr(schema_empty, name).__base__, 'Wrong tier for {name}'.format(name=name))

        for name_part in dir(rel):
            if name_part[0].isupper() and part_selector(getattr(rel, name_part)):
                assert_true(getattr(rel, name_part).__base__ is dj.Part, 'Wrong tier for {name}'.format(name=name_part))


@raises(dj.DataJointError)
def test_undecorated_table():
    """
    Undecorated user relation classes should raise an informative exception upon first use
    """

    class UndecoratedClass(dj.Manual):
        definition = ""

    a = UndecoratedClass()
    print(a.full_table_name)


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
    dj.Schema('unauthorized_schema', connection=dj.conn(reset=True, **CONN_INFO))


def test_drop_database():
    schema = dj.Schema(PREFIX + '_drop_test', connection=dj.conn(reset=True, **CONN_INFO))
    assert_true(schema.exists)
    schema.drop()
    assert_false(schema.exists)
    schema.drop()  # should do nothing


def test_overlapping_name():
    test_schema = dj.Schema(PREFIX + '_overlapping_schema', connection=dj.conn(**CONN_INFO))

    @test_schema
    class Unit(dj.Manual):
        definition = """
        id:  int     # simple id
        """

    # hack to update the locals dictionary
    locals()

    @test_schema
    class Cell(dj.Manual):
        definition = """
        type:  varchar(32)    # type of cell
        """

        class Unit(dj.Part):
            definition = """
            -> master
            -> Unit
            """

    test_schema.drop()


def test_schema_save():
    assert_true("class Experiment(dj.Imported)" in schema.schema.code)
    assert_true("class Experiment(dj.Imported)" in schema_empty.schema.code)
