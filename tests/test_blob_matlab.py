import numpy as np
import datajoint as dj
from datajoint.blob import pack, unpack

from nose.tools import assert_equal, assert_true, assert_tuple_equal, assert_false
from numpy.testing import assert_array_equal

from . import PREFIX, CONN_INFO

schema = dj.Schema(PREFIX + '_test1', locals(), connection=dj.conn(**CONN_INFO))


@schema
class Blob(dj.Manual):
    definition = """  # diverse types of blobs
    id : int
    -----
    comment  :  varchar(255)
    blob  : longblob
    """


def insert_blobs():
    """
    This function inserts blobs resulting from the following datajoint-matlab code:

        self.insert({
             1  'simple string'    'character string'
             2  '1D vector'        1:15:180
             3  'string array'     {'string1'  'string2'}
             4  'struct array'     struct('a', {1,2},  'b', {struct('c', magic(3)), struct('C', magic(5))})
             5  '3D double array'  reshape(1:24, [2,3,4])
             6  '3D uint8 array'   reshape(uint8(1:24), [2,3,4])
             7  '3D complex array' fftn(reshape(1:24, [2,3,4]))
            })

            and then dumped using the command
            mysqldump -u username -p --hex-blob test_schema blob_table > blob.sql
    """

    schema.connection.query("""
    INSERT INTO {table_name} VALUES
    (1,'simple string',0x6D596D00410200000000000000010000000000000010000000000000000400000000000000630068006100720061006300740065007200200073007400720069006E006700),
    (2,'1D vector',0x6D596D0041020000000000000001000000000000000C000000000000000600000000000000000000000000F03F00000000000030400000000000003F4000000000000047400000000000804E4000000000000053400000000000C056400000000000805A400000000000405E4000000000000061400000000000E062400000000000C06440),
    (3,'string array',0x6D596D00430200000000000000010000000000000002000000000000002F0000000000000041020000000000000001000000000000000700000000000000040000000000000073007400720069006E00670031002F0000000000000041020000000000000001000000000000000700000000000000040000000000000073007400720069006E0067003200),
    (4,'struct array',0x6D596D005302000000000000000100000000000000020000000000000002000000610062002900000000000000410200000000000000010000000000000001000000000000000600000000000000000000000000F03F9000000000000000530200000000000000010000000000000001000000000000000100000063006900000000000000410200000000000000030000000000000003000000000000000600000000000000000000000000204000000000000008400000000000001040000000000000F03F0000000000001440000000000000224000000000000018400000000000001C40000000000000004029000000000000004102000000000000000100000000000000010000000000000006000000000000000000000000000040100100000000000053020000000000000001000000000000000100000000000000010000004300E9000000000000004102000000000000000500000000000000050000000000000006000000000000000000000000003140000000000000374000000000000010400000000000002440000000000000264000000000000038400000000000001440000000000000184000000000000028400000000000003240000000000000F03F0000000000001C400000000000002A400000000000003340000000000000394000000000000020400000000000002C400000000000003440000000000000354000000000000000400000000000002E400000000000003040000000000000364000000000000008400000000000002240),
    (5,'3D double array',0x6D596D004103000000000000000200000000000000030000000000000004000000000000000600000000000000000000000000F03F000000000000004000000000000008400000000000001040000000000000144000000000000018400000000000001C40000000000000204000000000000022400000000000002440000000000000264000000000000028400000000000002A400000000000002C400000000000002E40000000000000304000000000000031400000000000003240000000000000334000000000000034400000000000003540000000000000364000000000000037400000000000003840),
    (6,'3D uint8 array',0x6D596D0041030000000000000002000000000000000300000000000000040000000000000009000000000000000102030405060708090A0B0C0D0E0F101112131415161718),
    (7,'3D complex array',0x6D596D0041030000000000000002000000000000000300000000000000040000000000000006000000010000000000000000C0724000000000000028C000000000000038C0000000000000000000000000000038C0000000000000000000000000000052C00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000052C00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000052C00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000AA4C58E87AB62B400000000000000000AA4C58E87AB62BC0000000000000008000000000000052400000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000080000000000000008000000000000052C000000000000000800000000000000080000000000000008000000000000000800000000000000080
    );
    """.format(table_name=Blob.full_table_name))


class TestFetch:

    @classmethod
    def setup_class(cls):
        assert_false(dj.config['safemode'], 'safemode must be disabled')
        Blob().delete()
        insert_blobs()

    @staticmethod
    def test_complex_matlab_blobs():
        """
        test correct de-serialization of various blob types
        """
        blobs = Blob().fetch('blob', order_by='KEY')

        blob = blobs[0]  # 'simple string'    'character string'
        assert_equal(blob[0], 'character string')

        blob = blobs[1]  # '1D vector'        1:15:180
        assert_array_equal(blob, np.r_[1:180:15][None, :])
        assert_array_equal(blob, unpack(pack(blob)))

        blob = blobs[2]  # 'string array'     {'string1'  'string2'}
        assert_true(isinstance(blob, dj.MatCell))
        assert_array_equal(blob, np.array([['string1', 'string2']]))
        assert_array_equal(blob, unpack(pack(blob)))

        blob = blobs[3]  # 'struct array'     struct('a', {1,2},  'b', {struct('c', magic(3)), struct('C', magic(5))})
        assert_true(isinstance(blob, dj.MatStruct))
        assert_tuple_equal(blob.dtype.names, ('a', 'b'))
        assert_array_equal(blob.a[0, 0], np.array([[1.]]))
        assert_array_equal(blob.a[0, 1], np.array([[2.]]))
        assert_true(isinstance(blob.b[0, 1], dj.MatStruct))
        assert_tuple_equal(blob.b[0, 1].C[0, 0].shape, (5, 5))
        b = unpack(pack(blob))
        assert_array_equal(b[0, 0].b[0, 0].c, blob[0, 0].b[0, 0].c)
        assert_array_equal(b[0, 1].b[0, 0].C, blob[0, 1].b[0, 0].C)

        blob = blobs[4]  # '3D double array'  reshape(1:24, [2,3,4])
        assert_array_equal(blob, np.r_[1:25].reshape((2, 3, 4), order='F'))
        assert_true(blob.dtype == 'float64')
        assert_array_equal(blob, unpack(pack(blob)))

        blob = blobs[5]  # reshape(uint8(1:24), [2,3,4])
        assert_true(np.array_equal(blob, np.r_[1:25].reshape((2, 3, 4), order='F')))
        assert_true(blob.dtype == 'uint8')
        assert_array_equal(blob, unpack(pack(blob)))

        blob = blobs[6]  # fftn(reshape(1:24, [2,3,4]))
        assert_tuple_equal(blob.shape, (2, 3, 4))
        assert_true(blob.dtype == 'complex128')
        assert_array_equal(blob, unpack(pack(blob)))

    @staticmethod
    def test_complex_matlab_squeeze():
        """
        test correct de-serialization of various blob types
        """
        blob = (Blob & 'id=1').fetch1('blob', squeeze=True)  # 'simple string'    'character string'
        assert_equal(blob, 'character string')

        blob = (Blob & 'id=2').fetch1('blob', squeeze=True)  # '1D vector'        1:15:180
        assert_array_equal(blob, np.r_[1:180:15])

        blob = (Blob & 'id=3').fetch1('blob', squeeze=True)  # 'string array'     {'string1'  'string2'}
        assert_true(isinstance(blob, dj.MatCell))
        assert_array_equal(blob, np.array(['string1', 'string2']))

        blob = (Blob & 'id=4').fetch1('blob', squeeze=True)  # 'struct array' struct('a', {1,2},  'b', {struct('c', magic(3)), struct('C', magic(5))})
        assert_true(isinstance(blob, dj.MatStruct))
        assert_tuple_equal(blob.dtype.names, ('a', 'b'))
        assert_array_equal(blob.a, np.array([1., 2,]))
        assert_true(isinstance(blob[1].b, dj.MatStruct))
        assert_tuple_equal(blob[1].b.C.item().shape, (5, 5))

        blob = (Blob & 'id=5').fetch1('blob', squeeze=True)  # '3D double array'  reshape(1:24, [2,3,4])
        assert_true(np.array_equal(blob, np.r_[1:25].reshape((2, 3, 4), order='F')))
        assert_true(blob.dtype == 'float64')

        blob = (Blob & 'id=6').fetch1('blob', squeeze=True)  # reshape(uint8(1:24), [2,3,4])
        assert_true(np.array_equal(blob, np.r_[1:25].reshape((2, 3, 4), order='F')))
        assert_true(blob.dtype == 'uint8')

        blob = (Blob & 'id=7').fetch1('blob', squeeze=True)  # fftn(reshape(1:24, [2,3,4]))
        assert_tuple_equal(blob.shape, (2, 3, 4))
        assert_true(blob.dtype == 'complex128')

    @staticmethod
    def test_iter():
        """
        test iterator over the entity set
        """
        from_iter = {d['id']: d for d in Blob()}
        assert_equal(len(from_iter), len(Blob()))
        assert_equal(from_iter[1]['blob'], 'character string')
