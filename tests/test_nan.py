import numpy as np
from nose.tools import assert_true
import datajoint as dj
from . import PREFIX, CONN_INFO

schema = dj.schema(PREFIX + '_nantest', locals(), connection=dj.conn(**CONN_INFO))


@schema
class NanTest(dj.Manual):
    definition = """
    id :int
    ---
    value=null :double
    """


def test_insert_nan():
    rel = NanTest()
    a = np.array([0, 1/3, np.nan, np.pi, np.nan])
    rel.insert(((i, value) for i, value in enumerate(a)))
    b = rel.fetch.order_by('id')['value']
    assert_true((np.isnan(a) == np.isnan(b)).all(),
                'incorrect handling of Nans')
    assert_true(np.allclose(a[np.logical_not(np.isnan(a))], b[np.logical_not(np.isnan(b))]),
                'incorrect storage of floats')
