import random
import string
from numpy.testing import assert_array_equal
import numpy as np
from nose.tools import assert_raises, assert_equal, \
    assert_false, assert_true, assert_list_equal, \
    assert_tuple_equal, assert_dict_equal, raises

from . import schema


class TestRelation:
    """
    Test base relations: insert, delete
    """

    def __init__(self):
        self.user = schema.User()
        self.subject = schema.Subject()
        self.experiment = schema.Experiment()
        self.trial = schema.Trial()
        self.ephys = schema.Ephys()
        self.channel = schema.EphysChannel()

    def test_contents(self):
        """
        test the ability of tables to self-populate using the contents property
        """

        # test contents
        assert_true(self.user)
        assert_true(len(self.user) == len(self.user.contents))
        u = self.user.fetch(order_by=['username'])
        assert_list_equal(list(u['username']), sorted([s[0] for s in self.user.contents]))

        # test prepare
        assert_true(self.subject)
        assert_true(len(self.subject) == len(self.subject.contents))
        u = self.subject.fetch(order_by=['subject_id'])
        assert_list_equal(list(u['subject_id']), sorted([s[0] for s in self.subject.contents]))

    def test_delete_quick(self):
        tmp = np.array([
            (2, 'Klara', 'monkey', '2010-01-01', ''),
            (1, 'Peter', 'mouse', '2015-01-01', '')],
            dtype=self.subject.heading.as_dtype)
        self.subject.insert(tmp)
        s = self.subject & ('subject_id in (%s)' % ','.join(str(r) for r in tmp['subject_id']))
        assert_true(len(s) == 2, 'insert did not work.')
        s.delete_quick()
        assert_true(len(s) == 0, 'delete did not work.')
