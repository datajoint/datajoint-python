#! /usr/bin/env python

import os

import datajoint as dj
import numpy as np


schema_in = dj.schema('test_blob_bypass_in')
schema_out = dj.schema('test_blob_bypass_out')

schema_in.drop(force=True)
schema_out.drop(force=True)

schema_in = dj.schema('test_blob_bypass_in')
schema_out = dj.schema('test_blob_bypass_out')

tst_dat = np.array([1, 2, 3])  # test blob; TODO: more complex example


@schema_in
class InputTable(dj.Lookup):
    definition = """
    id:                 int
    ---
    data:               blob
    """
    contents = [(0, tst_dat)]


@schema_out
class OutputTable(dj.Manual):
    definition = """
    id:                 int
    ---
    data:               blob
    """



def test_blob_bypass():
    dj.config['blob.encode_bypass'] = True
    OutputTable.insert(InputTable.fetch(as_dict=True))
    dj.config['blob.encode_bypass'] = False

    ins = InputTable.fetch(as_dict=True)
    outs = OutputTable.fetch(as_dict=True)

    assert((i[0]['data'] == tst_dat and i[0]['data'] == i[1]['data'])
           for i in zip(ins, outs))

    schema_in.drop(force=True)
    schema_out.drop(force=True)
