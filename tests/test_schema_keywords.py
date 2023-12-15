import pytest
import datajoint as dj
from . import PREFIX


class A(dj.Manual):
    definition = """
    a_id: int   # a id
    """


class B(dj.Manual):
    source = None
    definition = """
    -> self.source
    b_id: int   # b id
    """

    class H(dj.Part):
        definition = """
        -> master
        name: varchar(128)  # name
        """

    class C(dj.Part):
        definition = """
        -> master
        -> master.H
        """


class D(B):
    source = A


@pytest.fixture
def schema_kwd(connection_test):
    schema = dj.Schema(PREFIX + "_keywords", connection=connection_test)
    schema(A)
    schema(D)
    yield schema
    schema.drop()


def test_inherited_part_table(schema_kwd):
    assert "a_id" in D().heading.attributes
    assert "b_id" in D().heading.attributes
    assert "a_id" in D.C().heading.attributes
    assert "b_id" in D.C().heading.attributes
    assert "name" in D.C().heading.attributes
