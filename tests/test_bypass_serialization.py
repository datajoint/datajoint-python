
from . import PREFIX, CONN_INFO
from nose.tools import assert_true

import datajoint as dj
import numpy as np

schema_in = dj.schema(PREFIX + '_test_bypass_serialization_in',
                      connection=dj.conn(**CONN_INFO))

schema_out = dj.schema(PREFIX + '_test_blob_bypass_serialization_out',
                       connection=dj.conn(**CONN_INFO))


tst_blob = np.array([1, 2, 3])  # test blob;

@schema_in
class InputTable(dj.Lookup):
    definition = """
    id:                 int
    ---
    data:               blob
    """
    contents = [(0, tst_blob)]


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
    dj.blob.bypass_serialization = True

    ins = InputTable.fetch(as_dict=True)
    outs = OutputTable.fetch(as_dict=True)

    assert_true(all(np.array_equals(i[0]['data'], tst_blob),
                np.array_equals(i[0]['data'], i[1]['data']))
                for i in zip(ins, outs))

