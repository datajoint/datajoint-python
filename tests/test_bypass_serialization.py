import datajoint as dj
import numpy as np

from . import PREFIX, CONN_INFO
from numpy.testing import assert_array_equal
from nose.tools import assert_true



schema_in = dj.Schema(PREFIX + '_test_bypass_serialization_in',
                      connection=dj.conn(**CONN_INFO))

schema_out = dj.Schema(PREFIX + '_test_blob_bypass_serialization_out',
                       connection=dj.conn(**CONN_INFO))


test_blob = np.array([1, 2, 3])


@schema_in
class Input(dj.Lookup):
    definition = """
    id:                 int
    ---
    data:               blob
    """
    contents = [(0, test_blob)]


@schema_out
class Output(dj.Manual):
    definition = """
    id:                 int
    ---
    data:               blob
    """


def test_bypass_serialization():
    dj.blob.bypass_serialization = True
    contents = Input.fetch(as_dict=True)
    assert_true(isinstance(contents[0]['data'], bytes))
    Output.insert(contents)
    dj.blob.bypass_serialization = False
    assert_array_equal(Input.fetch1('data'), Output.fetch1('data'))

