from operator import itemgetter
from numpy.testing import assert_array_equal
import numpy as np

from . import schema
import datajoint as dj

# """
# Collection of test cases to test relational methods
# """
#
# __author__ = 'eywalker'
#
#
# def setup():
#     """
#     Setup
#     :return:
#     """
#
# class TestRelationalAlgebra(object):
#
#     def setup(self):
#         pass
#
#     def test_mul(self):
#         pass
#
#     def test_project(self):
#         pass
#
#     def test_iand(self):
#         pass
#
#     def test_isub(self):
#         pass
#
#     def test_sub(self):
#         pass
#
#     def test_len(self):
#         pass
#
#     def test_fetch(self):
#         pass
#
#     def test_repr(self):
#         pass
#
#     def test_iter(self):
#         pass
#
#     def test_not(self):
#         pass

class TestRelationalOperand:
    def __init__(self):
        self.subject = schema.Subject()

    def test_getitem(self):
        """Testing RelationalOperand.__getitem__"""

        np.testing.assert_array_equal(sorted(self.subject.project().fetch(), key=itemgetter(0)),
                                      sorted(self.subject.fetch[dj.key], key=itemgetter(0)),
                                      'Primary key is not returned correctly')

        tmp = self.subject.fetch(order_by=['subject_id'])

        for column, field in zip(self.subject.fetch[:], [e[0] for e in tmp.dtype.descr]):
            np.testing.assert_array_equal(sorted(tmp[field]), sorted(column), 'slice : does not work correctly')

        subject_notes, key, real_id = self.subject.fetch['subject_notes', dj.key, 'real_id']
        #
        np.testing.assert_array_equal(sorted(subject_notes), sorted(tmp['subject_notes']))
        np.testing.assert_array_equal(sorted(real_id), sorted(tmp['real_id']))
        np.testing.assert_array_equal(sorted(key, key=itemgetter(0)),
                                      sorted(self.subject.project().fetch(), key=itemgetter(0)))

        for column, field in zip(self.subject.fetch['subject_id'::2], [e[0] for e in tmp.dtype.descr][::2]):
            np.testing.assert_array_equal(sorted(tmp[field]), sorted(column), 'slice : does not work correctly')
