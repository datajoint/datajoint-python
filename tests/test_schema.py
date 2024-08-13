import types
import pytest
import inspect
import datajoint as dj
from inspect import getmembers
from . import schema


class Ephys(dj.Imported):
    definition = """  # This is already declare in ./schema.py
    """


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


@pytest.fixture
def schema_empty_module(schema_any, schema_empty):
    """
    Mock the module tests_old.schema_empty.
    The test `test_namespace_population` will check that the module contains all the
    classes in schema_any, after running `spawn_missing_classes`.
    """
    namespace_dict = {
        "_": schema_any,
        "schema": schema_empty,
        "Ephys": Ephys,
    }
    module = types.ModuleType("schema_empty")

    # Add classes to the module's namespace
    for k, v in namespace_dict.items():
        setattr(module, k, v)

    return module


@pytest.fixture
def schema_empty(connection_test, schema_any, prefix):
    context = {**schema.LOCALS_ANY, "Ephys": Ephys}
    schema_empty = dj.Schema(
        prefix + "_test1", context=context, connection=connection_test
    )
    schema_empty(Ephys)
    # load the rest of the classes
    schema_empty.spawn_missing_classes(context=context)
    yield schema_empty
    schema_empty.drop()


def test_schema_size_on_disk(schema_any):
    number_of_bytes = schema_any.size_on_disk
    assert isinstance(number_of_bytes, int)


def test_schema_list(schema_any):
    schemas = dj.list_schemas()
    assert schema_any.database in schemas


def test_drop_unauthorized():
    info_schema = dj.schema("information_schema")
    with pytest.raises(dj.errors.AccessError):
        info_schema.drop()


def test_namespace_population(schema_empty_module):
    """
    With the schema_empty_module fixture, this test
    mimics the behavior of `spawn_missing_classes`, as if the schema
    was declared in a separate module and `spawn_missing_classes` was called in that namespace.
    """
    # Spawn missing classes in the caller's (self) namespace.
    schema_empty_module.schema.context = None
    schema_empty_module.schema.spawn_missing_classes(context=None)
    # Then add them to the mock module's namespace.
    for k, v in locals().items():
        if inspect.isclass(v):
            setattr(schema_empty_module, k, v)

    for name, rel in getmembers(schema, relation_selector):
        assert hasattr(
            schema_empty_module, name
        ), "{name} not found in schema_empty".format(name=name)
        assert (
            rel.__base__ is getattr(schema_empty_module, name).__base__
        ), "Wrong tier for {name}".format(name=name)

        for name_part in dir(rel):
            if name_part[0].isupper() and part_selector(getattr(rel, name_part)):
                assert (
                    getattr(rel, name_part).__base__ is dj.Part
                ), "Wrong tier for {name}".format(name=name_part)


def test_undecorated_table():
    """
    Undecorated user table classes should raise an informative exception upon first use
    """

    class UndecoratedClass(dj.Manual):
        definition = ""

    a = UndecoratedClass()
    with pytest.raises(dj.DataJointError):
        print(a.full_table_name)


def test_reject_decorated_part(schema_any):
    """
    Decorating a dj.Part table should raise an informative exception.
    """

    class A(dj.Manual):
        definition = ...

        class B(dj.Part):
            definition = ...

    with pytest.raises(dj.DataJointError):
        schema_any(A.B)
        schema_any(A)


def test_unauthorized_database(db_creds_test):
    """
    an attempt to create a database to which user has no privileges should raise an informative exception.
    """
    with pytest.raises(dj.DataJointError):
        dj.Schema(
            "unauthorized_schema", connection=dj.conn(reset=True, **db_creds_test)
        )


def test_drop_database(db_creds_test, prefix):
    schema = dj.Schema(
        prefix + "_drop_test", connection=dj.conn(reset=True, **db_creds_test)
    )
    assert schema.exists
    schema.drop()
    assert not schema.exists
    schema.drop()  # should do nothing


def test_overlapping_name(connection_test, prefix):
    test_schema = dj.Schema(prefix + "_overlapping_schema", connection=connection_test)

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


def test_list_tables(schema_simp):
    """
    https://github.com/datajoint/datajoint-python/issues/838
    """
    expected = set(
        [
            "reserved_word",
            "#l",
            "#a",
            "__d",
            "__b",
            "__b__c",
            "__e",
            "__e__f",
            "__e__g",
            "__e__h",
            "__e__m",
            "__g",
            "#outfit_launch",
            "#outfit_launch__outfit_piece",
            "#i_j",
            "#j_i",
            "#t_test_update",
            "#data_a",
            "#data_b",
            "f",
            "#argmax_test",
            "#website",
            "profile",
            "profile__website",
        ]
    )
    actual = set(schema_simp.list_tables())
    assert actual == expected, f"Missing from list_tables(): {expected - actual}"


def test_schema_save_any(schema_any):
    assert "class Experiment(dj.Imported)" in schema_any.code


def test_schema_save_empty(schema_empty):
    assert "class Experiment(dj.Imported)" in schema_empty.code


def test_uppercase_schema(db_creds_root):
    """
    https://github.com/datajoint/datajoint-python/issues/564
    """
    dj.conn(**db_creds_root, reset=True)
    schema1 = dj.Schema("Schema_A")

    @schema1
    class Subject(dj.Manual):
        definition = """
        name: varchar(32)
        """

    Schema_A = dj.VirtualModule("Schema_A", "Schema_A")

    schema2 = dj.Schema("schema_b")

    @schema2
    class Recording(dj.Manual):
        definition = """
        -> Schema_A.Subject
        id: smallint
        """

    schema2.drop()
    schema1.drop()
