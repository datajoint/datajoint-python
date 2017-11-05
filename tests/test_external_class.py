
from nose.tools import assert_true, raises, assert_equal, assert_dict_equal, assert_list_equal

from . import schema_external as modu


def test_insert_and_fetch():
    assert_list_equal(modu.Simple().heading.externals, ['item'])


def test_populate():
    print('here')
#    modu.Image().populate()