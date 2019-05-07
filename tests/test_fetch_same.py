from nose.tools import assert_true, assert_false, assert_equal, \
                        assert_list_equal, raises
from . import PREFIX, CONN_INFO
import datajoint as dj
# import importlib
# dj = importlib.import_module('datajoint-python.datajoint', None)
import numpy as np

Schema = dj.schema(PREFIX + '_fetch_same', connection=dj.conn(**CONN_INFO))


@Schema
class ProjData(dj.Manual):
    definition = """
    id : int
    ---
    resp : float
    sim  : float
    big : longblob
    blah : varchar(10)
    """

data_ins = [
    {'id': 0, 'resp': 20.33, 'sim': 45.324, 'big': np.random.randn(10, 5), 'blah': 'yes'},
    {'id': 1, 'resp': 94.3, 'sim': 34.23, 'big': np.random.randn(20, 10), 'blah': 'si'},
    {'id': 2, 'resp': 1.90, 'sim': 10.23, 'big': np.random.randn(4, 2), 'blah': 'sim'}
]

ProjData().insert(data_ins)


class TestFetchSame:

    @staticmethod
    def test_object_conversion_one():

        trials_new = ProjData.proj(sub='resp-sim')
        new = trials_new.fetch('sub')

        assert_equal(new.dtype, np.float64)

    @staticmethod
    def test_object_conversion_two():

        trials_new = ProjData.proj(sub='resp-sim', add='resp+sim')
        new = trials_new.fetch('sub', 'add')

        assert_equal(new[0].dtype, np.float64)
        assert_equal(new[1].dtype, np.float64)

    @staticmethod
    def test_object_conversion_all():

        trials_new = ProjData.proj(sub='resp-sim', add='resp+sim')
        new = trials_new.fetch()

        assert_equal(new['sub'].dtype, np.float64)
        assert_equal(new['add'].dtype, np.float64)

    @staticmethod
    def test_object_no_convert():

        new = ProjData.fetch()
        assert_equal(new['big'].dtype, 'object')
        assert_equal(new['blah'].dtype, 'object')
