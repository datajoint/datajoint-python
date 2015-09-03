"""
Collection of test cases to test core module.
"""
from nose.tools import assert_true, assert_raises, assert_equal
from datajoint import DataJointError
from datajoint.utils import from_camel_case, to_camel_case


def setup():
    pass


def teardown():
    pass


def test_from_camel_case():
    assert_equal(from_camel_case('AllGroups'), 'all_groups')
    with assert_raises(DataJointError):
        from_camel_case('repNames')
    with assert_raises(DataJointError):
        from_camel_case('10_all')
    with assert_raises(DataJointError):
        from_camel_case('hello world')
    with assert_raises(DataJointError):
        from_camel_case('#baisc_names')


def test_to_camel_case():
    assert_equal(to_camel_case('all_groups'), 'AllGroups')
    assert_equal(to_camel_case('hello'), 'Hello')
    assert_equal(to_camel_case('this_is_a_sample_case'), 'ThisIsASampleCase')
    assert_equal(to_camel_case('This_is_Mixed'), 'ThisIsMixed')


