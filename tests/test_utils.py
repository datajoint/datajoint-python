# """
# Collection of test cases to test core module.
# """
#
# __author__ = 'eywalker'
# from nose.tools import assert_true, assert_raises, assert_equal
# from datajoint.utils import to_camel_case, from_camel_case
# from datajoint import DataJointError
#
#
# def setup():
#     pass
#
#
# def teardown():
#     pass
#
#
# def test_to_camel_case():
#     assert_equal(to_camel_case('basic_sessions'), 'BasicSessions')
#     assert_equal(to_camel_case('_another_table'), 'AnotherTable')
#
#
# def test_from_camel_case():
#     assert_equal(from_camel_case('AllGroups'), 'all_groups')
#     with assert_raises(DataJointError):
#         from_camel_case('repNames')
#     with assert_raises(DataJointError):
#         from_camel_case('10_all')
#     with assert_raises(DataJointError):
#         from_camel_case('hello world')
#     with assert_raises(DataJointError):
#         from_camel_case('#baisc_names')
