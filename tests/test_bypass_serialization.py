# decompyle3 version 3.9.0
# Python bytecode version base 3.7.0 (3394)
# Decompiled from: Python 3.7.17 (default, Jun 13 2023, 16:22:33) 
# [GCC 10.2.1 20210110]
# Embedded file name: /workspaces/datajoint-python/tests/test_bypass_serialization.py
# Compiled at: 2023-02-17 22:49:23
# Size of source mod 2**32: 980 bytes
import builtins as @py_builtins
import _pytest.assertion.rewrite as @pytest_ar
import datajoint as dj, numpy as np
from numpy.testing import assert_array_equal
from . import PREFIX, connection_root, connection_test
import pytest

@pytest.fixture
def schema(connection_test):
    schema = dj.Schema((PREFIX + '_test_bypass_serialization'),
      connection=connection_test)
    yield schema
    schema.drop()


@pytest.fixture
def Input(schema):

    @schema
    class Input(dj.Lookup):
        definition = '\n        id:                 int\n        ---\n        data:               blob\n        '
        contents = [(0, np.array([1, 2, 3]))]

    dj.blob.bypass_serialization = True
    yield Input
    dj.blob.bypass_serialization = False
    Input.drop()


def test_bypass_serialization(Input):
    contents = Input.fetch(as_dict=True)
    @py_assert1 = contents[0]['data']
    @py_assert4 = isinstance(@py_assert1, bytes)
    if not @py_assert4:
        @py_format6 = 'assert %(py5)s\n{%(py5)s = %(py0)s(%(py2)s, %(py3)s)\n}' % {'py0':@pytest_ar._saferepr(isinstance) if 'isinstance' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(isinstance) else 'isinstance',  'py2':@pytest_ar._saferepr(@py_assert1),  'py3':@pytest_ar._saferepr(bytes) if 'bytes' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(bytes) else 'bytes',  'py5':@pytest_ar._saferepr(@py_assert4)}
        raise AssertionError(@pytest_ar._format_explanation(@py_format6))
    @py_assert1 = @py_assert4 = None
    Input.insert([dict((contents[0]), id=1)])
    assert_array_equal((Input & dict(id=0)).fetch1('data'), (Input & dict(id=1)).fetch1('data'))