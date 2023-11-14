# decompyle3 version 3.9.0
# Python bytecode version base 3.7.0 (3394)
# Decompiled from: Python 3.7.17 (default, Jun 13 2023, 16:22:33) 
# [GCC 10.2.1 20210110]
# Embedded file name: /workspaces/datajoint-python/tests/test_nan.py
# Compiled at: 2023-02-20 16:58:48
# Size of source mod 2**32: 1293 bytes
import builtins as @py_builtins
import _pytest.assertion.rewrite as @pytest_ar
import datajoint as dj, numpy as np
from numpy.testing import assert_array_equal
import pytest
from . import PREFIX, connection_root, connection_test

@pytest.fixture
def schema(connection_test):
    schema = dj.Schema((PREFIX + '_nantest'), connection=connection_test)
    yield schema
    schema.drop()


expected = np.array([0, 0.3333333333333333, np.nan, np.pi, np.nan])

@pytest.fixture
def NanTest(schema):

    @schema
    class NanTest(dj.Manual):
        definition = '\n        id :int\n        ---\n        value=null :double\n        '

    NanTest.insert(((i, value) for i, value in enumerate(expected)))
    yield NanTest
    NanTest.drop()


def test_insert_nan(NanTest):
    """Test fetching of null values"""
    fetched = NanTest.fetch('value', order_by='id')
    assert_array_equal(np.isnan(fetched), np.isnan(expected), 'incorrect handling of Nans')
    @py_assert1 = np.allclose
    @py_assert3 = expected[np.logical_not(np.isnan(expected))]
    @py_assert5 = fetched[np.logical_not(np.isnan(fetched))]
    @py_assert7 = @py_assert1(@py_assert3, @py_assert5)
    if not @py_assert7:
        @py_format9 = (@pytest_ar._format_assertmsg('incorrect storage of floats') + '\n>assert %(py8)s\n{%(py8)s = %(py2)s\n{%(py2)s = %(py0)s.allclose\n}(%(py4)s, %(py6)s)\n}') % {'py0':@pytest_ar._saferepr(np) if 'np' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(np) else 'np',  'py2':@pytest_ar._saferepr(@py_assert1),  'py4':@pytest_ar._saferepr(@py_assert3),  'py6':@pytest_ar._saferepr(@py_assert5),  'py8':@pytest_ar._saferepr(@py_assert7)}
        raise AssertionError(@pytest_ar._format_explanation(@py_format9))
    @py_assert1 = @py_assert3 = @py_assert5 = @py_assert7 = None


def test_nulls_do_not_affect_primary_keys(NanTest):
    """Test against a case that previously caused a bug when skipping existing entries."""
    NanTest.insert(((i, value) for i, value in enumerate(expected)), skip_duplicates=True)