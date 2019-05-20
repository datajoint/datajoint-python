from nose.tools import assert_true, assert_false, \
        assert_equal, assert_list_equal, raises
from . import PREFIX, CONN_INFO
import importlib
try:
    dj = importlib.import_module('datajoint-python.datajoint', None)
    blob = importlib.import_module('datajoint-python.datajoint.blob', None)
    djerrors = importlib.import_module('datajoint-python.datajoint.errors',
            None)
except:
    import datajoint as dj
    from datajoint import blob
    from datajoint import errors as djerrors
import datetime as dt
import numpy as np
from collections import OrderedDict

schema = dj.schema(PREFIX + '_blob_migrate', connection=dj.conn(**CONN_INFO))


@schema
class BlobData(dj.Manual):
    definition = """
    # Testing blobs
    id : int                 # unique id for blob data
    ---
    lb_pl : longblob       # long blob data
    b_pl  : blob           # blob data
    """

timenow = dt.datetime.now()
tests = [
    {
        'insert_value': 3,
        'forward_stored_value': np.int64(3),
        'backward_stored_value': np.int64(3)
    },
    {
        'insert_value': 2.9,
        'forward_stored_value': np.float64(2.9),
        'backward_stored_value': np.float64(2.9)
    },
    {
        'insert_value': True,
        'forward_stored_value': np.bool_(True),
        'backward_stored_value': np.bool_(True)
    },
    {
        'insert_value': 'yes',
        'forward_stored_value': 'yes',
        'backward_stored_value': np.array(['yes'])
    },
    {
        'insert_value': timenow,
        'forward_stored_value': timenow,
        'backward_stored_value': np.array([str(timenow)])
    },
    {
        'insert_value': [3]*5,
        'forward_stored_value': list(np.array([3]*5)),
        'backward_stored_value': np.array([3]*5)
    },
    {
        'insert_value': [2.9]*5,
        'forward_stored_value': list(np.array([2.9]*5)),
        'backward_stored_value': np.array([2.9]*5)
    },
    {
        'insert_value': [True]*5,
        'forward_stored_value': list(np.array([True]*5)),
        'backward_stored_value': np.array([True]*5)
    },
    {
        'insert_value': [[3]*5, 3],
        'forward_stored_value': [list(np.array([3]*5)), 3],
        'backward_stored_value': np.array([np.array([3]*5), 3])
    },
    {
        'insert_value': (3,)*5,
        'forward_stored_value': tuple(np.array([3]*5)),
        'backward_stored_value': np.array([3]*5)
    },
    {
        'insert_value': (2.9,)*5,
        'forward_stored_value': tuple(np.array([2.9]*5)),
        'backward_stored_value': np.array([2.9]*5)
    },
    {
        'insert_value': (True,)*5,
        'forward_stored_value': tuple(np.array([True]*5)),
        'backward_stored_value': np.array([True]*5)
    },
    {
        'insert_value': (tuple((3,)*5), tuple((3,)*5)),
        'forward_stored_value': (tuple((3,)*5), tuple((3,)*5)),
        'backward_stored_value': np.array([np.array([3]*5), np.array([3]*5)])
    },
    {
        'insert_value': set([3]*5),
        'forward_stored_value': set(np.array([3]*5)),
        'backward_stored_value': np.array([3])
    },
    {
        'insert_value': set([2.9]*5),
        'forward_stored_value': set(np.array([2.9]*5)),
        'backward_stored_value': np.array([2.9])
    },
    {
        'insert_value': set([True]*5),
        'forward_stored_value': set(np.array([True]*5)),
        'backward_stored_value': np.array([True])
    },
    {
        'insert_value': np.array([3]*5),
        'forward_stored_value': np.array([3]*5),
        'backward_stored_value': np.array([3]*5)
    },
    {
        'insert_value': np.array([2.9]*5),
        'forward_stored_value': np.array([2.9]*5),
        'backward_stored_value': np.array([2.9]*5)
    },
    {
        'insert_value': np.array([True]*5),
        'forward_stored_value': np.array([True]*5),
        'backward_stored_value': np.array([True]*5)
    },
    {
        'insert_value': np.array([np.array([3]*5), 3]),
        'forward_stored_value': np.array([np.array([3]*5), 3]),
        'backward_stored_value': np.array([np.array([3]*5), 3])
    },
    {
        'insert_value': OrderedDict({'tmp0': 3, 'tmp1': 4, 'tmp2': 3, 'tmp3': 5, 'tmp4': 3}),
        'forward_stored_value': OrderedDict({'tmp0': 3, 'tmp1': 4, 'tmp2': 3, 'tmp3': 5, 'tmp4': 3}),
        'backward_stored_value': np.array([(3, 4, 3, 5, 3)], dtype=[('tmp0', 'O'), ('tmp1', 'O'), ('tmp2', 'O'), ('tmp3', 'O'), ('tmp4', 'O')]).view(np.recarray)
    },
    {
        'insert_value': OrderedDict({'tmp0': 2.9, 'tmp1': 2.9, 'tmp2': 2.9, 'tmp3': 2.9, 'tmp4': 2.9}),
        'forward_stored_value': OrderedDict({'tmp0': 2.9, 'tmp1': 2.9, 'tmp2': 2.9, 'tmp3': 2.9, 'tmp4': 2.9}),
        'backward_stored_value': np.array([(2.9, 2.9, 2.9, 2.9, 2.9)], dtype=[('tmp0', 'O'), ('tmp1', 'O'), ('tmp2', 'O'), ('tmp3', 'O'), ('tmp4', 'O')]).view(np.recarray)
    },
    {
        'insert_value': OrderedDict({'tmp0': True, 'tmp1': True, 'tmp2': True, 'tmp3': True, 'tmp4': True}),
        'forward_stored_value': OrderedDict({'tmp0': True, 'tmp1': True, 'tmp2': True, 'tmp3': True, 'tmp4': True}),
        'backward_stored_value': np.array([(True, True, True, True, True)], dtype=[('tmp0', 'O'), ('tmp1', 'O'), ('tmp2', 'O'), ('tmp3', 'O'), ('tmp4', 'O')]).view(np.recarray)
    },
    {
        'insert_value': OrderedDict({'tmp0': 'yes', 'tmp1': 'yes', 'tmp2': 'yes', 'tmp3': 'yes', 'tmp4': 'yes'}),
        'forward_stored_value': OrderedDict({'tmp0': 'yes', 'tmp1': 'yes', 'tmp2': 'yes', 'tmp3': 'yes', 'tmp4': 'yes'}),
        'backward_stored_value': np.array([(np.array(['yes']), np.array(['yes']), np.array(['yes']), np.array(['yes']), np.array(['yes']))], dtype=[('tmp0', 'O'), ('tmp1', 'O'), ('tmp2', 'O'), ('tmp3', 'O'), ('tmp4', 'O')]).view(np.recarray)
    },
    {
        'insert_value': OrderedDict({'tmp0': timenow, 'tmp1': timenow, 'tmp2': timenow, 'tmp3': timenow, 'tmp4': timenow}),
        'forward_stored_value': OrderedDict({'tmp0': timenow, 'tmp1': timenow, 'tmp2': timenow, 'tmp3': timenow, 'tmp4': timenow}),
        'backward_stored_value': np.array([(np.array([str(timenow)]), np.array([str(timenow)]), np.array([str(timenow)]), np.array([str(timenow)]), np.array([str(timenow)]))], dtype=[('tmp0', 'O'), ('tmp1', 'O'), ('tmp2', 'O'), ('tmp3', 'O'), ('tmp4', 'O')]).view(np.recarray)
    }
]

data = []
for i, n in enumerate(tests):
    data.append({
        'id': i,
        'lb_pl': n['insert_value'],
        'b_pl': n['insert_value']
    })

BlobData().insert(data)


class TestMigrate:

    @staticmethod
    def test_convert_forward():

        [id, lb_pl, b_pl] = BlobData.fetch('id', 'lb_pl', 'b_pl')

        [lb_pl_ford, b_pl_ford] = dj.convert_blob(lb_pl, b_pl)

        for idx in range(id.size):
            assert_equal(blob.pack(lb_pl_ford[id == idx][0]), blob.pack(
                    tests[idx]['forward_stored_value']))
            assert_true(isinstance(tests[idx]['forward_stored_value'], type(lb_pl_ford[id == idx][0])))

            if type(lb_pl_ford[id == idx][0]) == np.ndarray:
                assert_equal(lb_pl_ford[id == idx][0].dtype,
                        tests[idx]['forward_stored_value'].dtype)

            assert_equal(blob.pack(b_pl_ford[id == idx][0]), blob.pack(
                    tests[idx]['forward_stored_value']))
            assert_true(isinstance(tests[idx]['forward_stored_value'], type(b_pl_ford[id == idx][0])))

            if type(b_pl_ford[id == idx][0]) == np.ndarray:
                assert_equal(b_pl_ford[id == idx][0].dtype,
                        tests[idx]['forward_stored_value'].dtype)

    @staticmethod
    def test_convert_backward():

        [id, lb_pl, b_pl] = BlobData.fetch('id', 'lb_pl', 'b_pl')

        [lb_pl_back, b_pl_back] = dj.convert_blob(lb_pl, b_pl, mode='backward')

        for idx in range(id.size):
            assert_equal(blob.pack(lb_pl_back[id == idx][0]), blob.pack(
                    tests[idx]['backward_stored_value']))
            assert_equal(type(lb_pl_back[id == idx][0]), type(
                    tests[idx]['backward_stored_value']))

            if type(lb_pl_back[id == idx][0]) == np.ndarray:
                assert_equal(lb_pl_back[id == idx][0].dtype,
                        tests[idx]['backward_stored_value'].dtype)

            assert_equal(blob.pack(b_pl_back[id == idx][0]), blob.pack(
                    tests[idx]['backward_stored_value']))
            assert_equal(type(b_pl_back[id == idx][0]), type(
                    tests[idx]['backward_stored_value']))

            if type(b_pl_back[id == idx][0]) == np.ndarray:
                assert_equal(b_pl_back[id == idx][0].dtype,
                        tests[idx]['backward_stored_value'].dtype)
