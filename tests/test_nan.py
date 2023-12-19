import numpy as np
import datajoint as dj
import pytest


class NanTest(dj.Manual):
    definition = """
    id :int
    ---
    value=null :double
    """


@pytest.fixture
def schema_nan(connection_test, prefix):
    schema = dj.Schema(
        prefix + "_nantest", context=dict(NanTest=NanTest), connection=connection_test
    )
    schema(NanTest)
    yield schema
    schema.drop()


@pytest.fixture
def arr_a():
    return np.array([0, 1 / 3, np.nan, np.pi, np.nan])


@pytest.fixture
def schema_nan_pop(schema_nan, arr_a):
    rel = NanTest()
    with dj.config(safemode=False):
        rel.delete()
    rel.insert(((i, value) for i, value in enumerate(arr_a)))
    return schema_nan


def test_insert_nan(schema_nan_pop, arr_a):
    """Test fetching of null values"""
    b = NanTest().fetch("value", order_by="id")
    assert (np.isnan(arr_a) == np.isnan(b)).all(), "incorrect handling of Nans"
    assert np.allclose(
        arr_a[np.logical_not(np.isnan(arr_a))], b[np.logical_not(np.isnan(b))]
    ), "incorrect storage of floats"


def test_nulls_do_not_affect_primary_keys(schema_nan_pop, arr_a):
    """Test against a case that previously caused a bug when skipping existing entries."""
    NanTest().insert(
        ((i, value) for i, value in enumerate(arr_a)), skip_duplicates=True
    )
