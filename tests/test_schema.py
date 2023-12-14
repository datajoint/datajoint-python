import pytest
import datajoint as dj
from inspect import getmembers
from . import schema
from . import PREFIX


class Ephys(dj.Imported):
    definition = """  # This is already declared in ./schema.py
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
def schema_empty(connection_test, schema_any):
    context = {
        **schema.LOCALS_ANY,
        "Ephys": Ephys
    }
    schema_emp = dj.Schema(PREFIX + "_test1", context=context, connection=connection_test)
    schema_emp(Ephys)
    # load the rest of the classes
    schema_emp.spawn_missing_classes()
    breakpoint()
    yield schema_emp
    schema_emp.drop()


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


def test_namespace_population(schema_empty, schema_any):
    for name, rel in getmembers(schema, relation_selector):
        assert hasattr(schema_empty, name), "{name} not found in schema_empty".format(name=name)
        assert rel.__base__ is getattr(schema_empty, name).__base__, "Wrong tier for {name}".format(name=name)

        for name_part in dir(rel):
            if name_part[0].isupper() and part_selector(getattr(rel, name_part)):
                assert getattr(rel, name_part).__base__ is dj.Part, "Wrong tier for {name}".format(name=name_part)


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
        dj.Schema("unauthorized_schema", connection=dj.conn(reset=True, **db_creds_test))


def test_drop_database(db_creds_test):
    schema = dj.Schema(
        PREFIX + "_drop_test", connection=dj.conn(reset=True, **db_creds_test)
    )
    assert schema.exists
    schema.drop()
    assert not schema.exists
    schema.drop()  # should do nothing


def test_overlapping_name(connection_test):
    test_schema = dj.Schema(
        PREFIX + "_overlapping_schema", connection=connection_test
    )

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
    assert set(
        [
            "reserved_word",
            "#l",
            "#a",
            "__d",
            "__b",
            "__b__c",
            "__e",
            "__e__f",
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
    ) == set(schema_simp.list_tables())


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
