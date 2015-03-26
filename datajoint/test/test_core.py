"""
Collection of test cases to test core module.
"""
from test.schemata.schema1 import test1

__author__ = 'eywalker'
from . import (CONN_INFO, PREFIX, BASE_CONN, cleanup)
from nose.tools import assert_true, assert_raises, assert_equal
from datajoint.core import to_camel_case, from_camel_case, DataJointError


def setup():
    pass


def teardown():
    pass


def test_to_camel_case():
    assert_equal(to_camel_case('basic_sessions'), 'BasicSessions')
    assert_equal(to_camel_case('_another_table'), 'AnotherTable')


def test_from_camel_case():
    assert_equal(from_camel_case('AllGroups'), 'all_groups')
    assert_equal(from_camel_case('repNames'), 'rep_names')
    with assert_raises(DataJointError):
        from_camel_case('10_all')
    with assert_raises(DataJointError):
        from_camel_case('hello world')
    with assert_raises(DataJointError):
        from_camel_case('#baisc_names')
