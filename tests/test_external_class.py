from numpy.testing import assert_almost_equal
import datajoint as dj
from . import schema_external


def test_heading(schema_ext, mock_stores):
    heading = schema_external.Simple().heading
    assert "item" in heading
    assert heading["item"].is_external


def test_insert_and_fetch(schema_ext, mock_stores, mock_cache):
    original_list = [1, 3, 8]
    schema_external.Simple().insert1(dict(simple=1, item=original_list))
    # test fetch
    q = (schema_external.Simple() & {"simple": 1}).fetch("item")[0]
    assert list(q) == original_list
    # test fetch1 as a tuple
    q = (schema_external.Simple() & {"simple": 1}).fetch1("item")
    assert list(q) == original_list
    # test fetch1 as a dict
    q = (schema_external.Simple() & {"simple": 1}).fetch1()
    assert list(q["item"]) == original_list
    # test without cache
    previous_cache = dj.config["cache"]
    dj.config["cache"] = None
    q = (schema_external.Simple() & {"simple": 1}).fetch1()
    assert list(q["item"]) == original_list
    # test with cache
    dj.config["cache"] = previous_cache
    q = (schema_external.Simple() & {"simple": 1}).fetch1()
    assert list(q["item"]) == original_list


def test_populate(schema_ext, mock_stores):
    image = schema_external.Image()
    image.populate()
    remaining, total = image.progress()
    assert (
        total == len(schema_external.Dimension() * schema_external.Seed())
        and remaining == 0
    )
    for img, neg, dimensions in zip(
        *(image * schema_external.Dimension()).fetch("img", "neg", "dimensions")
    ):
        assert list(img.shape) == list(dimensions)
        assert_almost_equal(img, -neg)
    image.delete()
    for external_table in image.external.values():
        external_table.delete(display_progress=False, delete_external_files=True)
