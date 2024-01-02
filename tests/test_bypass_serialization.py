import pytest
import datajoint as dj
import numpy as np
from numpy.testing import assert_array_equal

test_blob = np.array([1, 2, 3])


class Input(dj.Lookup):
    definition = """
    id:                 int
    ---
    data:               blob
    """
    contents = [(0, test_blob)]


class Output(dj.Manual):
    definition = """
    id:                 int
    ---
    data:               blob
    """


@pytest.fixture
def schema_in(connection_test, prefix):
    schema = dj.Schema(
        prefix + "_test_bypass_serialization_in",
        context=dict(Input=Input),
        connection=connection_test,
    )
    schema(Input)
    yield schema
    schema.drop()


@pytest.fixture
def schema_out(connection_test, prefix):
    schema = dj.Schema(
        prefix + "_test_blob_bypass_serialization_out",
        context=dict(Output=Output),
        connection=connection_test,
    )
    schema(Output)
    yield schema
    schema.drop()


def test_bypass_serialization(schema_in, schema_out):
    dj.blob.bypass_serialization = True
    contents = Input.fetch(as_dict=True)
    assert isinstance(contents[0]["data"], bytes)
    Output.insert(contents)
    dj.blob.bypass_serialization = False
    assert_array_equal(Input.fetch1("data"), Output.fetch1("data"))
