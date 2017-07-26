from nose.tools import assert_false, assert_true, raises
import datajoint as dj


@raises(dj.DataJointError)
def test_no_create_schema():
    schema = dj.schema('forbidden_schema', locals())

