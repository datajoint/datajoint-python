import datajoint as dj
import pytest


@pytest.fixture
def schema():
    schema = dj.Schema()
    yield schema
    schema.drop()


@pytest.fixture
def Parent(schema):
    @schema
    class Parent(dj.Lookup):
        definition = """
        id: int
        """
        contents = [(1,)]

    yield Parent
    Parent.drop()


@pytest.fixture
def Child(schema, Parent):
    @schema
    class Child(dj.Computed):
        definition = """
        -> Parent
        """

        def make(self, key):
            self.insert1(key)

    yield Child
    Child.drop()


@pytest.fixture
def NoAccess(schema):
    @schema
    class NoAccess(dj.Lookup):
        definition = """
        string: varchar(10)
        """

    yield NoAccess
    NoAccess.drop()


@pytest.fixture
def NoAccessAgain(schema, NoAccess):
    @schema
    class NoAccessAgain(dj.Manual):
        definition = """
        -> NoAccess
        """

    yield NoAccessAgain
    NoAccessAgain.drop()
