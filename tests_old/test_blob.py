import datajoint as dj
import timeit
import numpy as np
import uuid
from . import schema
from decimal import Decimal
from datetime import datetime
from datajoint.blob import pack, unpack
from numpy.testing import assert_array_equal
from nose.tools import (
    assert_equal,
    assert_true,
    assert_false,
    assert_list_equal,
    assert_set_equal,
    assert_tuple_equal,
    assert_dict_equal,
)


def test_pack():
    for x in (
        32,
        -3.7e-2,
        np.float64(3e31),
        -np.inf,
        np.int8(-3),
        np.uint8(-1),
        np.int16(-33),
        np.uint16(-33),
        np.int32(-3),
        np.uint32(-1),
        np.int64(373),
        np.uint64(-3),
    ):
        assert_equal(x, unpack(pack(x)), "Scalars don't match!")

    x = np.nan
    assert_true(np.isnan(unpack(pack(x))), "nan scalar did not match!")

    x = np.random.randn(8, 10)
    assert_array_equal(x, unpack(pack(x)), "Arrays do not match!")

    x = np.random.randn(10)
    assert_array_equal(x, unpack(pack(x)), "Arrays do not match!")

    x = 7j
    assert_equal(x, unpack(pack(x)), "Complex scalar does not match")

    x = np.float32(np.random.randn(3, 4, 5))
    assert_array_equal(x, unpack(pack(x)), "Arrays do not match!")

    x = np.int16(np.random.randn(1, 2, 3))
    assert_array_equal(x, unpack(pack(x)), "Arrays do not match!")

    x = None
    assert_true(unpack(pack(x)) is None, "None did not match")

    x = -255
    y = unpack(pack(x))
    assert_true(
        x == y and isinstance(y, int) and not isinstance(y, np.ndarray),
        "Scalar int did not match",
    )

    x = -25523987234234287910987234987098245697129798713407812347
    y = unpack(pack(x))
    assert_true(
        x == y and isinstance(y, int) and not isinstance(y, np.ndarray),
        "Unbounded int did not match",
    )

    x = 7.0
    y = unpack(pack(x))
    assert_true(
        x == y and isinstance(y, float) and not isinstance(y, np.ndarray),
        "Scalar float did not match",
    )

    x = 7j
    y = unpack(pack(x))
    assert_true(
        x == y and isinstance(y, complex) and not isinstance(y, np.ndarray),
        "Complex scalar did not match",
    )

    x = True
    assert_true(unpack(pack(x)) is True, "Scalar bool did not match")

    x = [None]
    assert_list_equal(x, unpack(pack(x)))

    x = {
        "name": "Anonymous",
        "age": 15,
        99: datetime.now(),
        "range": [110, 190],
        (11, 12): None,
    }
    y = unpack(pack(x))
    assert_dict_equal(x, y, "Dict do not match!")
    assert_false(
        isinstance(["range"][0], np.ndarray), "Scalar int was coerced into array."
    )

    x = uuid.uuid4()
    assert_equal(x, unpack(pack(x)), "UUID did not match")

    x = Decimal("-112122121.000003000")
    assert_equal(x, unpack(pack(x)), "Decimal did not pack/unpack correctly")

    x = [1, datetime.now(), {1: "one", "two": 2}, (1, 2)]
    assert_list_equal(x, unpack(pack(x)), "List did not pack/unpack correctly")

    x = (1, datetime.now(), {1: "one", "two": 2}, (uuid.uuid4(), 2))
    assert_tuple_equal(x, unpack(pack(x)), "Tuple did not pack/unpack correctly")

    x = (
        1,
        {datetime.now().date(): "today", "now": datetime.now().date()},
        {"yes!": [1, 2, np.array((3, 4))]},
    )
    y = unpack(pack(x))
    assert_dict_equal(x[1], y[1])
    assert_array_equal(x[2]["yes!"][2], y[2]["yes!"][2])

    x = {"elephant"}
    assert_set_equal(x, unpack(pack(x)), "Set did not pack/unpack correctly")

    x = tuple(range(10))
    assert_tuple_equal(
        x, unpack(pack(range(10))), "Iterator did not pack/unpack correctly"
    )

    x = Decimal("1.24")
    assert_true(x == unpack(pack(x)), "Decimal object did not pack/unpack correctly")

    x = datetime.now()
    assert_true(x == unpack(pack(x)), "Datetime object did not pack/unpack correctly")

    x = np.bool_(True)
    assert_true(x == unpack(pack(x)), "Numpy bool object did not pack/unpack correctly")

    x = "test"
    assert_true(x == unpack(pack(x)), "String object did not pack/unpack correctly")

    x = np.array(["yes"])
    assert_true(
        x == unpack(pack(x)), "Numpy string array object did not pack/unpack correctly"
    )

    x = np.datetime64("1998").astype("datetime64[us]")
    assert_true(x == unpack(pack(x)))


def test_recarrays():
    x = np.array([(1.0, 2), (3.0, 4)], dtype=[("x", float), ("y", int)])
    assert_array_equal(x, unpack(pack(x)))

    x = x.view(np.recarray)
    assert_array_equal(x, unpack(pack(x)))

    x = np.array([(3, 4)], dtype=[("tmp0", float), ("tmp1", "O")]).view(np.recarray)
    assert_array_equal(x, unpack(pack(x)))


def test_object_arrays():
    x = np.array(((1, 2, 3), True), dtype="object")
    assert_array_equal(x, unpack(pack(x)), "Object array did not serialize correctly")


def test_complex():
    z = np.random.randn(8, 10) + 1j * np.random.randn(8, 10)
    assert_array_equal(z, unpack(pack(z)), "Arrays do not match!")

    z = np.random.randn(10) + 1j * np.random.randn(10)
    assert_array_equal(z, unpack(pack(z)), "Arrays do not match!")

    x = np.float32(np.random.randn(3, 4, 5)) + 1j * np.float32(np.random.randn(3, 4, 5))
    assert_array_equal(x, unpack(pack(x)), "Arrays do not match!")

    x = np.int16(np.random.randn(1, 2, 3)) + 1j * np.int16(np.random.randn(1, 2, 3))
    assert_array_equal(x, unpack(pack(x)), "Arrays do not match!")


def test_insert_longblob():
    insert_dj_blob = {"id": 1, "data": [1, 2, 3]}
    schema.Longblob.insert1(insert_dj_blob)
    assert (schema.Longblob & "id=1").fetch1() == insert_dj_blob
    (schema.Longblob & "id=1").delete()

    query_mym_blob = {"id": 1, "data": np.array([1, 2, 3])}
    schema.Longblob.insert1(query_mym_blob)
    assert (schema.Longblob & "id=1").fetch1()["data"].all() == query_mym_blob[
        "data"
    ].all()
    (schema.Longblob & "id=1").delete()

    query_32_blob = (
        "INSERT INTO djtest_test1.longblob (id, data) VALUES (1, "
        "X'6D596D00530200000001000000010000000400000068697473007369646573007461736B73007374"
        "616765004D000000410200000001000000070000000600000000000000000000000000F8FF00000000"
        "0000F03F000000000000F03F0000000000000000000000000000F03F00000000000000000000000000"
        "00F8FF230000004102000000010000000700000004000000000000006C006C006C006C00720072006C"
        "0023000000410200000001000000070000000400000000000000640064006400640064006400640025"
        "00000041020000000100000008000000040000000000000053007400610067006500200031003000')"
    )
    dj.conn().query(query_32_blob).fetchall()
    dj.blob.use_32bit_dims = True
    assert (schema.Longblob & "id=1").fetch1() == {
        "id": 1,
        "data": np.rec.array(
            [
                [
                    (
                        np.array([[np.nan, 1.0, 1.0, 0.0, 1.0, 0.0, np.nan]]),
                        np.array(["llllrrl"], dtype="<U7"),
                        np.array(["ddddddd"], dtype="<U7"),
                        np.array(["Stage 10"], dtype="<U8"),
                    )
                ]
            ],
            dtype=[("hits", "O"), ("sides", "O"), ("tasks", "O"), ("stage", "O")],
        ),
    }
    (schema.Longblob & "id=1").delete()
    dj.blob.use_32bit_dims = False


def test_datetime_serialization_speed():
    # If this fails that means for some reason deserializing/serializing
    # np arrays of np.datetime64 types is now slower than regular arrays of datetime

    optimized_exe_time = timeit.timeit(
        setup="myarr=pack(np.array([np.datetime64('2022-10-13 03:03:13') for _ in range(0, 10000)]))",
        stmt="unpack(myarr)",
        number=10,
        globals=globals(),
    )
    print(f"np time {optimized_exe_time}")
    baseline_exe_time = timeit.timeit(
        setup="myarr2=pack(np.array([datetime(2022,10,13,3,3,13) for _ in range (0, 10000)]))",
        stmt="unpack(myarr2)",
        number=10,
        globals=globals(),
    )
    print(f"python time {baseline_exe_time}")

    assert optimized_exe_time * 900 < baseline_exe_time
