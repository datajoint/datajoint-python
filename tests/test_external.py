import numpy as np
from numpy.testing import assert_array_equal
from datajoint.external import ExternalTable
from datajoint.blob import pack, unpack
import datajoint as dj
from .schema_external import SimpleRemote, Simple
import os


def test_external_put(schema_ext, mock_stores, mock_cache):
    """
    external storage put and get and remove
    """
    ext = ExternalTable(
        schema_ext.connection, store="raw", database=schema_ext.database
    )
    initial_length = len(ext)
    input_ = np.random.randn(3, 7, 8)
    count = 7
    extra = 3
    for i in range(count):
        hash1 = ext.put(pack(input_))
    for i in range(extra):
        hash2 = ext.put(pack(np.random.randn(4, 3, 2)))

    fetched_hashes = ext.fetch("hash")
    assert all(hash in fetched_hashes for hash in (hash1, hash2))
    assert len(ext) == initial_length + 1 + extra

    output_ = unpack(ext.get(hash1))
    assert_array_equal(input_, output_)


class TestLeadingSlash:
    def test_s3_leading_slash(self, schema_ext, mock_stores, mock_cache, minio_client):
        """
        s3 external storage configured with leading slash
        """
        self._leading_slash(schema_ext, index=100, store="share")

    def test_file_leading_slash(
        self, schema_ext, mock_stores, mock_cache, minio_client
    ):
        """
        File external storage configured with leading slash
        """
        self._leading_slash(schema_ext, index=200, store="local")

    def _leading_slash(self, schema_ext, index, store):
        oldConfig = dj.config["stores"][store]["location"]
        value = np.array([1, 2, 3])

        id = index
        dj.config["stores"][store]["location"] = "leading/slash/test"
        SimpleRemote.insert([{"simple": id, "item": value}])
        assert np.array_equal(
            value, (SimpleRemote & "simple={}".format(id)).fetch1("item")
        )

        id = index + 1
        dj.config["stores"][store]["location"] = "/leading/slash/test"
        SimpleRemote.insert([{"simple": id, "item": value}])
        assert np.array_equal(
            value, (SimpleRemote & "simple={}".format(id)).fetch1("item")
        )

        id = index + 2
        dj.config["stores"][store]["location"] = "leading\\slash\\test"
        SimpleRemote.insert([{"simple": id, "item": value}])
        assert np.array_equal(
            value, (SimpleRemote & "simple={}".format(id)).fetch1("item")
        )

        id = index + 3
        dj.config["stores"][store]["location"] = "f:\\leading\\slash\\test"
        SimpleRemote.insert([{"simple": id, "item": value}])
        assert np.array_equal(
            value, (SimpleRemote & "simple={}".format(id)).fetch1("item")
        )

        id = index + 4
        dj.config["stores"][store]["location"] = "f:\\leading/slash\\test"
        SimpleRemote.insert([{"simple": id, "item": value}])
        assert np.array_equal(
            value, (SimpleRemote & "simple={}".format(id)).fetch1("item")
        )

        id = index + 5
        dj.config["stores"][store]["location"] = "/"
        SimpleRemote.insert([{"simple": id, "item": value}])
        assert np.array_equal(
            value, (SimpleRemote & "simple={}".format(id)).fetch1("item")
        )

        id = index + 6
        dj.config["stores"][store]["location"] = "C:\\"
        SimpleRemote.insert([{"simple": id, "item": value}])
        assert np.array_equal(
            value, (SimpleRemote & "simple={}".format(id)).fetch1("item")
        )

        id = index + 7
        dj.config["stores"][store]["location"] = ""
        SimpleRemote.insert([{"simple": id, "item": value}])
        assert np.array_equal(
            value, (SimpleRemote & "simple={}".format(id)).fetch1("item")
        )

        dj.config["stores"][store]["location"] = oldConfig


def test_remove_fail(schema_ext, mock_stores, mock_cache, minio_client):
    """
    https://github.com/datajoint/datajoint-python/issues/953
    """
    assert dj.config["stores"]["local"]["location"]

    data = dict(simple=2, item=[1, 2, 3])
    Simple.insert1(data)
    path1 = dj.config["stores"]["local"]["location"] + "/djtest_extern/4/c/"
    currentMode = int(oct(os.stat(path1).st_mode), 8)
    os.chmod(path1, 0o40555)
    (Simple & "simple=2").delete()
    listOfErrors = schema_ext.external["local"].delete(delete_external_files=True)

    assert (
        len(schema_ext.external["local"] & dict(hash=listOfErrors[0][0])) == 1
    ), "unexpected number of rows in external table"
    # ---------------------CLEAN UP--------------------
    os.chmod(path1, currentMode)
    listOfErrors = schema_ext.external["local"].delete(delete_external_files=True)
