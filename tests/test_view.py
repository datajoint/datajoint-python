from nose.tools import assert_dict_equal
from . import schema


def test_view():
    view = schema.Glot()
    assert_dict_equal(dict(view.fetch()), dict(view.definition.fetch()))
    