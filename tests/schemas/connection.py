import datajoint as dj
from .. import PREFIX, connection_test
import pytest


@pytest.fixture
def schema(connection_test):
    schema = dj.Schema(PREFIX + "_transactions", locals(), connection=connection_test)
    yield schema
    schema.drop()


@pytest.fixture
def Subjects(schema):
    @schema
    class Subjects(dj.Manual):
        definition = """
        #Basic subject
        subject_id                  : int      # unique subject id
        ---
        real_id                     :  varchar(40)    #  real-world name
        species = "mouse"           : enum('mouse', 'monkey', 'human')   # species
        """

    yield Subjects
    Subjects.drop()
