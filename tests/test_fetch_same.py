import pytest
import numpy as np
import datajoint as dj


class ProjData(dj.Manual):
    definition = """
    id : int
    ---
    resp : float
    sim  : float
    big : longblob
    blah : varchar(10)
    """


@pytest.fixture
def schema_fetch_same(connection_test, prefix):
    schema = dj.Schema(
        prefix + "_fetch_same",
        context=dict(ProjData=ProjData),
        connection=connection_test,
    )
    schema(ProjData)
    ProjData().insert(
        [
            {"id": 0, "resp": 20.33, "sim": 45.324, "big": 3, "blah": "yes"},
            {
                "id": 1,
                "resp": 94.3,
                "sim": 34.23,
                "big": {"key1": np.random.randn(20, 10)},
                "blah": "si",
            },
            {
                "id": 2,
                "resp": 1.90,
                "sim": 10.23,
                "big": np.random.randn(4, 2),
                "blah": "sim",
            },
        ]
    )
    yield schema
    schema.drop()


def test_object_conversion_one(schema_fetch_same):
    new = ProjData().proj(sub="resp").fetch("sub")
    assert new.dtype == np.float64


def test_object_conversion_two(schema_fetch_same):
    [sub, add] = ProjData().proj(sub="resp", add="sim").fetch("sub", "add")
    assert sub.dtype == np.float64
    assert add.dtype == np.float64


def test_object_conversion_all(schema_fetch_same):
    new = ProjData().proj(sub="resp", add="sim").fetch()
    assert new["sub"].dtype == np.float64
    assert new["add"].dtype == np.float64


def test_object_no_convert(schema_fetch_same):
    new = ProjData().fetch()
    assert new["big"].dtype == "object"
    assert new["blah"].dtype == "object"
