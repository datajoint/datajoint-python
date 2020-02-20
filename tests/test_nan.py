import numpy as np
from nose.tools import assert_true
import datajoint as dj
from . import PREFIX, CONN_INFO

schema = dj.Schema(PREFIX + '_nantest', locals(), connection=dj.conn(**CONN_INFO))


@schema
class NanTest(dj.Manual):
    definition = """
    id :int
    ---
    value=null :double
    """


class TestNaNInsert:
    @classmethod
    def setup_class(cls):
        cls.rel = NanTest()
        with dj.config(safemode=False):
            cls.rel.delete()
        a = np.array([0, 1/3, np.nan, np.pi, np.nan])
        cls.rel.insert(((i, value) for i, value in enumerate(a)))
        cls.a = a

    def test_insert_nan(self):
        """Test fetching of null values"""
        b = self.rel.fetch('value', order_by='id')
        assert_true((np.isnan(self.a) == np.isnan(b)).all(),
                    'incorrect handling of Nans')
        assert_true(np.allclose(self.a[np.logical_not(np.isnan(self.a))], b[np.logical_not(np.isnan(b))]),
                    'incorrect storage of floats')

    def test_nulls_do_not_affect_primary_keys(self):
        """Test against a case that previously caused a bug when skipping existing entries."""
        self.rel.insert(((i, value) for i, value in enumerate(self.a)), skip_duplicates=True)
