import numpy as np
import datajoint as dj
from . import PREFIX
import pytest


class NanTest(dj.Manual):
    definition = """
    id :int
    ---
    value=null :double
    """


@pytest.fixture(scope="module")
def schema(connection_test):
    schema = dj.Schema(PREFIX + "_nantest", connection=connection_test)
    schema(NanTest)
    yield schema
    schema.drop()


@pytest.fixture(scope="class")
def setup_class(request, schema):
    rel = NanTest()
    with dj.config(safemode=False):
        rel.delete()
    a = np.array([0, 1 / 3, np.nan, np.pi, np.nan])
    rel.insert(((i, value) for i, value in enumerate(a)))
    request.cls.rel = rel
    request.cls.a = a


class TestNaNInsert:
    def test_insert_nan(self, setup_class):
        """Test fetching of null values"""
        b = self.rel.fetch("value", order_by="id")
        assert (np.isnan(self.a) == np.isnan(b)).all(), "incorrect handling of Nans"
        assert np.allclose(
            self.a[np.logical_not(np.isnan(self.a))], b[np.logical_not(np.isnan(b))]
        ), "incorrect storage of floats"

    def test_nulls_do_not_affect_primary_keys(self, setup_class):
        """Test against a case that previously caused a bug when skipping existing entries."""
        self.rel.insert(
            ((i, value) for i, value in enumerate(self.a)), skip_duplicates=True
        )
