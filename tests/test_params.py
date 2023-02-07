from nose.tools import (
    assert_true,
    assert_false,
    assert_equal,
    assert_list_equal,
    raises,
    assert_set_equal,
)
from .schema_simple import P, schema
import datajoint as dj
import inspect
from datajoint.declare import declare
from datajoint.hash import uuid_from_buffer

params = P()
params.fill()
entry = params.fetch(as_dict=True, limit=1)[0]


class TestDeclare:
    @staticmethod
    def test_exact_entry():
        """Test ignoring insertion of the same entry"""
        previous_length = len(params)
        params.insert1(entry)
        assert_equal(len(params), previous_length)

    @staticmethod
    def test_new_pk():
        """Test ignoring insertion of the same params with a new primary key"""
        previous_length = len(params)
        new_pk = dict(id=99, **{k: entry[k] for k in entry.keys() if k != "id"})
        params.insert1(new_pk)
        assert_equal(len(params), previous_length)

    @staticmethod
    def test_uuid_generation():
        """Test UUID entered matches hash generated here"""
        paramset_dict = {k: v for k, v in entry.items() if "param" in k}
        paramset_hash = uuid_from_buffer(f"{paramset_dict}".encode())
        assert_equal(len(params & {"__paramset_hash": paramset_hash}), 1)

    @staticmethod
    @raises(dj.DataJointError)
    def test_no_params_field():
        """Test unable to declare a Params table without params field"""

        @schema
        class NoParams(dj.Params):
            definition = """
            id : int
            """

    @staticmethod
    @raises(dj.DataJointError)
    def test_hidden_primary_key():
        """Test unable to declare table with hidden attribute in primary key"""

        @schema
        class HiddenPK(dj.Manual):
            definition = """
            __hidden : int
            """
