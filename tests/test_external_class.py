from nose.tools import assert_true, assert_list_equal

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
    # test fetch1
    q = (modu.Simple() & {'simple': 1}).fetch1('item')
    assert_list_equal(list(q), original_list)


def test_populate():
    image = modu.Image()
    image.populate()
    remaining, total = image.progress()
    assert_true(total == len(modu.Dimension() * modu.Seed()) and remaining == 0)
    for img, dimensions in zip(*(image * modu.Dimension()).fetch('img', 'dimensions')):
        assert_list_equal(list(img.shape), list(dimensions))