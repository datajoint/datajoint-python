
from . import PREFIX, CONN_INFO
from numpy.testing import assert_array_equal

import datajoint as dj
import numpy as np

schema_in = dj.schema(PREFIX + '_test_bypass_serialization_in',
                      connection=dj.conn(**CONN_INFO))

schema_out = dj.schema(PREFIX + '_test_blob_bypass_serialization_out',
                       connection=dj.conn(**CONN_INFO))


test_blob = np.array([1, 2, 3])

@schema_in
class InputTable(dj.Lookup):
    definition = """
    id:                 int
    ---
    data:               blob
    """
    contents = [(0, test_blob)]


@schema_out
class OutputTable(dj.Manual):
    definition = """
    id:                 int
    ---
    data:               blob
    """


def test_bypass_serialization():
    dj.blob.bypass_serialization = True
    OutputTable.insert(InputTable.fetch(as_dict=True))
    dj.blob.bypass_serialization = False

    i = InputTable.fetch1()
    o = OutputTable.fetch1()
    assert_array_equal(i['data'], o['data'])

