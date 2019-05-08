from nose.tools import assert_true, assert_false, assert_equal, \
                        assert_list_equal, raises
from . import PREFIX, CONN_INFO
import numpy as np
import importlib
try:
    dj = importlib.import_module('datajoint-python.datajoint', None)
except:
    import datajoint as dj

schema = dj.schema(PREFIX + '_fetch_same', connection=dj.conn(**CONN_INFO))


@schema
class ProjData(dj.Manual):
    definition = """
    id : int
    ---
    resp : float
    sim  : float
    big : longblob
    blah : varchar(10)
    """

ProjData().insert([
    {'id': 0, 'resp': 20.33, 'sim': 45.324, 'big': 3, 'blah': 'yes'},
    {'id': 1, 'resp': 94.3, 'sim': 34.23,
        'big': {'key1': np.random.randn(20, 10)}, 'blah': 'si'},
    {'id': 2, 'resp': 1.90, 'sim': 10.23,
        'big': np.random.randn(4, 2), 'blah': 'sim'}
])


class TestFetchSame:

    @staticmethod
    def test_object_conversion_one():

        new = ProjData.proj(sub='resp-sim').fetch('sub')

        assert_equal(new.dtype, np.float64)

    @staticmethod
    def test_object_conversion_two():

        [sub, add] = ProjData.proj(sub='resp-sim', add='resp+sim').fetch(
                'sub', 'add')

        assert_equal(sub.dtype, np.float64)
        assert_equal(add.dtype, np.float64)

    @staticmethod
    def test_object_conversion_all():

        new = ProjData.proj(sub='resp-sim', add='resp+sim').fetch()

        assert_equal(new['sub'].dtype, np.float64)
        assert_equal(new['add'].dtype, np.float64)

    @staticmethod
    def test_object_no_convert():

        new = ProjData.fetch()
        assert_equal(new['big'].dtype, 'object')
        assert_equal(new['blah'].dtype, 'object')
