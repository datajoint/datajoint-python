from nose.tools import assert_true, assert_list_equal
from numpy.testing import assert_almost_equal

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


def test_populate():
    image = modu.Image()
    image.populate()
    remaining, total = image.progress()
    assert_true(total == len(modu.Dimension() * modu.Seed()) and remaining == 0)
    for img, neg, dimensions in zip(*(image * modu.Dimension()).fetch('img', 'neg', 'dimensions')):
        assert_list_equal(list(img.shape), list(dimensions))
        assert_almost_equal(img, -neg)


def test_clean():
    image = modu.Image()
    ext = modu.schema.external_table
    assert_true(image.external_table is ext)

    image.populate()

    set1 = (image & 'seed MOD 2 = 0')   # to keep
    set2 = image - set1.proj()          # to delete

    keys1 = list(set1.fetch.keys())
    keys2 = list(set1.fetch.keys())

    (image & keys2).delete()
    hashes_before = ext.fetch('hash')
    ext.clean()
    hashes_after = ext.fetch('hash')


