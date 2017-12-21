from nose.tools import assert_true, assert_list_equal, raises
from numpy.testing import assert_almost_equal
import datajoint as dj
from . import schema_external as modu


def test_heading():
    heading = modu.Simple().heading
    assert_true('item' in heading)
    assert_true(heading['item'].is_external)


def test_insert_and_fetch():
    original_list = [1, 3, 8]
    modu.Simple().insert1(dict(simple=1, item=original_list))
    # test fetch
    q = (modu.Simple() & {'simple': 1}).fetch('item')[0]
    assert_list_equal(list(q), original_list)
    # test fetch1 as a tuple
    q = (modu.Simple() & {'simple': 1}).fetch1('item')
    assert_list_equal(list(q), original_list)
    # test fetch1 as a dict
    q = (modu.Simple() & {'simple': 1}).fetch1()
    assert_list_equal(list(q['item']), original_list)
    # test without cache
    previous_cache = dj.config['cache']
    dj.config['cache'] = None
    q = (modu.Simple() & {'simple': 1}).fetch1()
    assert_list_equal(list(q['item']), original_list)
    # test with cache
    dj.config['cache'] = previous_cache
    q = (modu.Simple() & {'simple': 1}).fetch1()
    assert_list_equal(list(q['item']), original_list)


def test_populate():
    image = modu.Image()
    image.populate()
    remaining, total = image.progress()
    image.external_table.clean_store('external-raw')
    assert_true(total == len(modu.Dimension() * modu.Seed()) and remaining == 0)
    for img, neg, dimensions in zip(*(image * modu.Dimension()).fetch('img', 'neg', 'dimensions')):
        assert_list_equal(list(img.shape), list(dimensions))
        assert_almost_equal(img, -neg)
    image.delete()
    image.external_table.delete_garbage()
    image.external_table.clean_store('external-raw')


@raises(dj.DataJointError)
def test_drop():
    image = modu.Image()
    image.populate()
    image.external_table.drop()


@raises(dj.DataJointError)
def test_delete():
    image = modu.Image()
    image.external_table.delete()



