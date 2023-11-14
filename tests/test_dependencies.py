# decompyle3 version 3.9.0
# Python bytecode version base 3.7.0 (3394)
# Decompiled from: Python 3.7.17 (default, Jun 13 2023, 16:22:33) 
# [GCC 10.2.1 20210110]
# Embedded file name: /workspaces/datajoint-python/tests/test_dependencies.py
# Compiled at: 2023-02-18 20:13:54
# Size of source mod 2**32: 2633 bytes
import builtins as @py_builtins
import _pytest.assertion.rewrite as @pytest_ar
import datajoint as dj
from datajoint.dependencies import unite_master_parts
import pytest
from . import connection_root, connection_test
from schemas.default import schema, ThingA, ThingB, ThingC

def test_unite_master_parts():
    @py_assert1 = [
     '`s`.`a`','`s`.`a__q`','`s`.`b`','`s`.`c`','`s`.`c__q`','`s`.`b__q`','`s`.`d`','`s`.`a__r`']
    @py_assert3 = unite_master_parts(@py_assert1)
    @py_assert6 = [
     '`s`.`a`','`s`.`a__q`','`s`.`a__r`','`s`.`b`','`s`.`b__q`','`s`.`c`','`s`.`c__q`','`s`.`d`']
    @py_assert5 = @py_assert3 == @py_assert6
    if not @py_assert5:
        @py_format8 = @pytest_ar._call_reprcompare(('==', ), (@py_assert5,), ('%(py4)s\n{%(py4)s = %(py0)s(%(py2)s)\n} == %(py7)s', ), (@py_assert3, @py_assert6)) % {'py0':@pytest_ar._saferepr(unite_master_parts) if 'unite_master_parts' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(unite_master_parts) else 'unite_master_parts',  'py2':@pytest_ar._saferepr(@py_assert1),  'py4':@pytest_ar._saferepr(@py_assert3),  'py7':@pytest_ar._saferepr(@py_assert6)}
        @py_format10 = 'assert %(py9)s' % {'py9': @py_format8}
        raise AssertionError(@pytest_ar._format_explanation(@py_format10))
    @py_assert1 = @py_assert3 = @py_assert5 = @py_assert6 = None
    @py_assert1 = [
     '`lab`.`#equipment`','`cells`.`cell_analysis_method`','`cells`.`cell_analysis_method_task_type`','`cells`.`cell_analysis_method_users`','`cells`.`favorite_selection`','`cells`.`cell_analysis_method__cell_selection_params`','`lab`.`#equipment__config`','`cells`.`cell_analysis_method__field_detect_params`']
    @py_assert3 = unite_master_parts(@py_assert1)
    @py_assert6 = [
     '`lab`.`#equipment`','`lab`.`#equipment__config`','`cells`.`cell_analysis_method`','`cells`.`cell_analysis_method__cell_selection_params`','`cells`.`cell_analysis_method__field_detect_params`','`cells`.`cell_analysis_method_task_type`','`cells`.`cell_analysis_method_users`','`cells`.`favorite_selection`']
    @py_assert5 = @py_assert3 == @py_assert6
    if not @py_assert5:
        @py_format8 = @pytest_ar._call_reprcompare(('==', ), (@py_assert5,), ('%(py4)s\n{%(py4)s = %(py0)s(%(py2)s)\n} == %(py7)s', ), (@py_assert3, @py_assert6)) % {'py0':@pytest_ar._saferepr(unite_master_parts) if 'unite_master_parts' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(unite_master_parts) else 'unite_master_parts',  'py2':@pytest_ar._saferepr(@py_assert1),  'py4':@pytest_ar._saferepr(@py_assert3),  'py7':@pytest_ar._saferepr(@py_assert6)}
        @py_format10 = 'assert %(py9)s' % {'py9': @py_format8}
        raise AssertionError(@pytest_ar._format_explanation(@py_format10))
    @py_assert1 = @py_assert3 = @py_assert5 = @py_assert6 = None


def test_nullable_dependency(ThingA, ThingB, ThingC):
    """test nullable unique foreign key"""
    a = ThingA()
    b = ThingB()
    c = ThingC()
    a.insert((dict(a=a) for a in range(7)))
    b.insert1(dict(b1=1, b2=1, b3=100))
    b.insert1(dict(b1=1, b2=2, b3=100))
    c.insert1(dict(a=0))
    c.insert1(dict(a=1, b1=33))
    c.insert1(dict(a=2, b2=77))
    c.insert1(dict(a=3, b1=1, b2=1))
    c.insert1(dict(a=4, b1=1, b2=2))
    @py_assert2 = len(c)
    @py_assert8 = c.fetch
    @py_assert10 = @py_assert8()
    @py_assert12 = len(@py_assert10)
    @py_assert4 = @py_assert2 == @py_assert12
    @py_assert14 = 5
    @py_assert5 = @py_assert12 == @py_assert14
    if not (@py_assert4 and @py_assert5):
        @py_format16 = @pytest_ar._call_reprcompare(('==', '=='), (@py_assert4, @py_assert5), ('%(py3)s\n{%(py3)s = %(py0)s(%(py1)s)\n} == %(py13)s\n{%(py13)s = %(py6)s(%(py11)s\n{%(py11)s = %(py9)s\n{%(py9)s = %(py7)s.fetch\n}()\n})\n}',
                                                                                               '%(py13)s\n{%(py13)s = %(py6)s(%(py11)s\n{%(py11)s = %(py9)s\n{%(py9)s = %(py7)s.fetch\n}()\n})\n} == %(py15)s'), (@py_assert2, @py_assert12, @py_assert14)) % {'py0':@pytest_ar._saferepr(len) if 'len' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(len) else 'len',  'py1':@pytest_ar._saferepr(c) if 'c' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(c) else 'c',  'py3':@pytest_ar._saferepr(@py_assert2),  'py6':@pytest_ar._saferepr(len) if 'len' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(len) else 'len',  'py7':@pytest_ar._saferepr(c) if 'c' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(c) else 'c',  'py9':@pytest_ar._saferepr(@py_assert8),  'py11':@pytest_ar._saferepr(@py_assert10),  'py13':@pytest_ar._saferepr(@py_assert12),  'py15':@pytest_ar._saferepr(@py_assert14)}
        @py_format18 = 'assert %(py17)s' % {'py17': @py_format16}
        raise AssertionError(@pytest_ar._format_explanation(@py_format18))
    @py_assert2 = @py_assert4 = @py_assert5 = @py_assert8 = @py_assert10 = @py_assert12 = @py_assert14 = None


def test_unique_dependency(ThingA, ThingB, ThingC):
    """test nullable unique foreign key"""
    a = ThingA()
    b = ThingB()
    c = ThingC()
    a.insert((dict(a=a) for a in range(7)))
    b.insert1(dict(b1=1, b2=1, b3=100))
    b.insert1(dict(b1=1, b2=2, b3=100))
    c.insert1(dict(a=0, b1=1, b2=1))
    with pytest.raises(dj.errors.DuplicateError):
        c.insert1(dict(a=1, b1=1, b2=1))