from nose.tools import assert_dict_equal, raises
import datajoint as dj
from . import schema


def test_view():
    view = schema.Glot()
    assert_dict_equal(dict(view.fetch()), dict(view.definition.fetch()))


@raises(dj.DataJointError)
def test_missing_definition():
    @schema.schema
    class BadView(dj.View):
        misspelled = schema.Glot()


@raises(dj.DataJointError)
def test_invalid_definition():

    @schema.schema
    class BadView(dj.View):
        definition = """
        id : int
        ---
        value : double
        """
