# decompyle3 version 3.9.0
# Python bytecode version base 3.7.0 (3394)
# Decompiled from: Python 3.7.17 (default, Jun 13 2023, 16:22:33) 
# [GCC 10.2.1 20210110]
# Embedded file name: /workspaces/datajoint-python/tests/test_plugin.py
# Compiled at: 2023-02-20 17:00:15
# Size of source mod 2**32: 1691 bytes
import builtins as @py_builtins
import _pytest.assertion.rewrite as @pytest_ar
import datajoint.errors as djerr
import datajoint.plugin as p
import pkg_resources
from os import path

def test_check_pubkey():
    base_name = 'datajoint'
    base_meta = pkg_resources.get_distribution(base_name)
    pubkey_meta = base_meta.get_metadata('{}.pub'.format(base_name))
    with open(path.join(path.abspath(path.dirname(__file__)), '..', 'datajoint.pub'), 'r') as f:
        @py_assert1 = f.read
        @py_assert3 = @py_assert1()
        @py_assert5 = @py_assert3 == pubkey_meta
        if not @py_assert5:
            @py_format7 = @pytest_ar._call_reprcompare(('==', ), (@py_assert5,), ('%(py4)s\n{%(py4)s = %(py2)s\n{%(py2)s = %(py0)s.read\n}()\n} == %(py6)s', ), (@py_assert3, pubkey_meta)) % {'py0':@pytest_ar._saferepr(f) if 'f' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(f) else 'f',  'py2':@pytest_ar._saferepr(@py_assert1),  'py4':@pytest_ar._saferepr(@py_assert3),  'py6':@pytest_ar._saferepr(pubkey_meta) if 'pubkey_meta' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(pubkey_meta) else 'pubkey_meta'}
            @py_format9 = 'assert %(py8)s' % {'py8': @py_format7}
            raise AssertionError(@pytest_ar._format_explanation(@py_format9))
        @py_assert1 = @py_assert3 = @py_assert5 = None


def test_normal_djerror():
    try:
        raise djerr.DataJointError
    except djerr.DataJointError as e:
        try:
            @py_assert1 = e.__cause__
            @py_assert4 = None
            @py_assert3 = @py_assert1 is @py_assert4
            if not @py_assert3:
                @py_format6 = @pytest_ar._call_reprcompare(('is', ), (@py_assert3,), ('%(py2)s\n{%(py2)s = %(py0)s.__cause__\n} is %(py5)s', ), (@py_assert1, @py_assert4)) % {'py0':@pytest_ar._saferepr(e) if 'e' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(e) else 'e',  'py2':@pytest_ar._saferepr(@py_assert1),  'py5':@pytest_ar._saferepr(@py_assert4)}
                @py_format8 = 'assert %(py7)s' % {'py7': @py_format6}
                raise AssertionError(@pytest_ar._format_explanation(@py_format8))
            @py_assert1 = @py_assert3 = @py_assert4 = None
        finally:
            e = None
            del e


def test_verified_djerror(category='connection'):
    try:
        curr_plugins = getattr(p, '{}_plugins'.format(category))
        setattr(p, '{}_plugins'.format(category), dict(test_plugin_id=dict(verified=True, object='example')))
        raise djerr.DataJointError
    except djerr.DataJointError as e:
        try:
            setattr(p, '{}_plugins'.format(category), curr_plugins)
            @py_assert1 = e.__cause__
            @py_assert4 = None
            @py_assert3 = @py_assert1 is @py_assert4
            if not @py_assert3:
                @py_format6 = @pytest_ar._call_reprcompare(('is', ), (@py_assert3,), ('%(py2)s\n{%(py2)s = %(py0)s.__cause__\n} is %(py5)s', ), (@py_assert1, @py_assert4)) % {'py0':@pytest_ar._saferepr(e) if 'e' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(e) else 'e',  'py2':@pytest_ar._saferepr(@py_assert1),  'py5':@pytest_ar._saferepr(@py_assert4)}
                @py_format8 = 'assert %(py7)s' % {'py7': @py_format6}
                raise AssertionError(@pytest_ar._format_explanation(@py_format8))
            @py_assert1 = @py_assert3 = @py_assert4 = None
        finally:
            e = None
            del e


def test_verified_djerror_type():
    test_verified_djerror(category='type')


def test_unverified_djerror(category='connection'):
    try:
        curr_plugins = getattr(p, '{}_plugins'.format(category))
        setattr(p, '{}_plugins'.format(category), dict(test_plugin_id=dict(verified=False, object='example')))
        raise djerr.DataJointError('hello')
    except djerr.DataJointError as e:
        try:
            setattr(p, '{}_plugins'.format(category), curr_plugins)
            @py_assert2 = e.__cause__
            @py_assert5 = djerr.PluginWarning
            @py_assert7 = isinstance(@py_assert2, @py_assert5)
            if not @py_assert7:
                @py_format9 = 'assert %(py8)s\n{%(py8)s = %(py0)s(%(py3)s\n{%(py3)s = %(py1)s.__cause__\n}, %(py6)s\n{%(py6)s = %(py4)s.PluginWarning\n})\n}' % {'py0':@pytest_ar._saferepr(isinstance) if 'isinstance' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(isinstance) else 'isinstance',  'py1':@pytest_ar._saferepr(e) if 'e' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(e) else 'e',  'py3':@pytest_ar._saferepr(@py_assert2),  'py4':@pytest_ar._saferepr(djerr) if 'djerr' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(djerr) else 'djerr',  'py6':@pytest_ar._saferepr(@py_assert5),  'py8':@pytest_ar._saferepr(@py_assert7)}
                raise AssertionError(@pytest_ar._format_explanation(@py_format9))
            @py_assert2 = @py_assert5 = @py_assert7 = None
        finally:
            e = None
            del e


def test_unverified_djerror_type():
    test_unverified_djerror(category='type')