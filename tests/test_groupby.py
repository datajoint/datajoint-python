# decompyle3 version 3.9.0
# Python bytecode version base 3.7.0 (3394)
# Decompiled from: Python 3.7.17 (default, Jun 13 2023, 16:22:33) 
# [GCC 10.2.1 20210110]
# Embedded file name: /workspaces/datajoint-python/tests/test_groupby.py
# Compiled at: 2023-02-20 15:56:32
# Size of source mod 2**32: 656 bytes
import builtins as @py_builtins
import _pytest.assertion.rewrite as @pytest_ar
from . import connection_root, connection_test
from schemas.simple import schema, A, D, L

def test_aggr_with_proj(A, D):
    A.aggr(D.proj(m='id_l'), ..., n='max(m) - min(m)')