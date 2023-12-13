import pytest
from . import PREFIX, CONN_INFO
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
def schema_fetch_same(connection_root):
    schema = dj.Schema(
        PREFIX + "_fetch_same",
        context=dict(ProjData=ProjData),
        connection=connection_root,
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


@pytest.fixture
def projdata():
    yield ProjData()


class TestFetchSame:
    def test_object_conversion_one(self, schema_fetch_same, projdata):
        new = projdata.proj(sub="resp").fetch("sub")
        assert new.dtype == np.float64

    def test_object_conversion_two(self, schema_fetch_same, projdata):
        [sub, add] = projdata.proj(sub="resp", add="sim").fetch("sub", "add")
        assert sub.dtype == np.float64
        assert add.dtype == np.float64

    def test_object_conversion_all(self, schema_fetch_same, projdata):
        new = projdata.proj(sub="resp", add="sim").fetch()
        assert new["sub"].dtype == np.float64
        assert new["add"].dtype == np.float64

    def test_object_no_convert(self, schema_fetch_same, projdata):
        new = projdata.fetch()
        assert new["big"].dtype == "object"
        assert new["blah"].dtype == "object"
