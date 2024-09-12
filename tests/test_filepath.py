import pytest
import datajoint as dj
import os
from pathlib import Path
import random
from .schema_external import Filepath, FilepathS3
import logging
import io


def test_path_match(schema_ext, enable_filepath_feature, minio_client, store="repo"):
    """test file path matches and empty file"""
    ext = schema_ext.external[store]
    stage_path = dj.config["stores"][store]["stage"]

    # create a mock file
    relpath = "path/to/films"
    managed_file = Path(stage_path, relpath, "vid.mov")
    managed_file.parent.mkdir(parents=True, exist_ok=True)
    open(str(managed_file), "a").close()

    # put the file
    uuid = ext.upload_filepath(str(managed_file))

    # remove
    managed_file.unlink()
    assert not managed_file.exists()

    # check filepath
    assert (ext & {"hash": uuid}).fetch1("filepath") == str(
        managed_file.relative_to(stage_path).as_posix()
    )

    # # Download the file and check its contents.
    restored_path, checksum = ext.download_filepath(uuid)
    assert restored_path == str(managed_file)
    assert checksum == dj.hash.uuid_from_file(str(managed_file))

    # cleanup
    ext.delete(delete_external_files=True)


@pytest.mark.parametrize("store", ("repo", "repo-s3"))
def test_filepath(enable_filepath_feature, schema_ext, store):
    """test file management"""
    ext = schema_ext.external[store]
    stage_path = dj.config["stores"][store]["stage"]
    filename = "picture.dat"

    # create a mock file
    relpath = "one/two/three"
    managed_file = Path(stage_path, relpath, filename)
    managed_file.parent.mkdir(parents=True, exist_ok=True)
    data = os.urandom(3000)
    with managed_file.open("wb") as f:
        f.write(data)

    # put the same file twice to ensure storing once
    uuid1 = ext.upload_filepath(str(managed_file))
    # no duplication should arise if file is the same
    uuid2 = ext.upload_filepath(str(managed_file))
    assert uuid1 == uuid2

    # remove to ensure downloading
    managed_file.unlink()
    assert not managed_file.exists()

    # Download the file and check its contents. Repeat causes no download from remote
    for _ in 1, 2:
        restored_path, checksum = ext.download_filepath(uuid1)
        assert restored_path == str(managed_file)
        assert checksum == dj.hash.uuid_from_file(str(managed_file))

    # verify same data
    with managed_file.open("rb") as f:
        synced_data = f.read()
    assert data == synced_data

    # cleanup
    ext.delete(delete_external_files=True)
    assert not ext.exists(ext._make_external_filepath(str(Path(relpath, filename))))


@pytest.mark.parametrize("store", ("repo", "repo-s3"))
def test_duplicate_upload(schema_ext, store):
    ext = schema_ext.external[store]
    stage_path = dj.config["stores"][store]["stage"]
    relpath = "one/two/three"
    managed_file = Path(stage_path, relpath, "plot.dat")
    managed_file.parent.mkdir(parents=True, exist_ok=True)
    with managed_file.open("wb") as f:
        f.write(os.urandom(300))
    ext.upload_filepath(str(managed_file))
    ext.upload_filepath(str(managed_file))  # this is fine because the file is the same


@pytest.mark.parametrize("store", ("repo", "repo-s3"))
def test_duplicate_error(schema_ext, store):
    """syncing duplicate non-matching file should fail"""
    ext = schema_ext.external[store]
    stage_path = dj.config["stores"][store]["stage"]
    relpath = "one/two/three"
    managed_file = Path(stage_path, relpath, "thesis.dat")
    managed_file.parent.mkdir(parents=True, exist_ok=True)
    with managed_file.open("wb") as f:
        f.write(os.urandom(300))
    ext.upload_filepath(str(managed_file))
    with managed_file.open("wb") as f:
        f.write(os.urandom(300))
    # this should raise exception because the file has changed
    with pytest.raises(dj.DataJointError):
        ext.upload_filepath(str(managed_file))


class TestFilepath:
    def _test_filepath_class(
        self, table=Filepath(), store="repo", verify_checksum=True
    ):
        if not verify_checksum:
            dj.config["filepath_checksum_size_limit"] = 0
        stage_path = dj.config["stores"][store]["stage"]
        # create a mock file
        relative_path = "one/two/three"
        managed_file = Path(stage_path, relative_path, "attachment.dat")
        managed_file.parent.mkdir(parents=True, exist_ok=True)
        data = os.urandom(3000)
        with managed_file.open("wb") as f:
            f.write(data)
        with managed_file.open("rb") as f:
            contents = f.read()
        assert data == contents

        # upload file into shared repo
        table.insert1((1, str(managed_file)))

        # remove file locally
        managed_file.unlink()
        assert not managed_file.is_file()

        # fetch file from remote
        filepath = (table & {"fnum": 1}).fetch1("img")
        assert filepath == str(managed_file)

        # verify original contents
        with managed_file.open("rb") as f:
            contents = f.read()
        assert data == contents

        # delete from table
        table.delete()
        assert table.external[store]

        # delete from external table
        table.external[store].delete(delete_external_files=True)
        dj.config["filepath_checksum_size_limit"] = None

    @pytest.mark.parametrize(
        "table, store, n_repeats",
        (
            (Filepath(), "repo", 2),
            (FilepathS3(), "repo-s3", 2),
        ),
    )
    def test_filepath_class(
        self,
        schema_ext,
        table,
        store,
        n_repeats,
        minio_client,
        enable_filepath_feature,
        verify_checksum=True,
    ):
        for _ in range(n_repeats):
            self._test_filepath_class(table, store, verify_checksum)

    def test_filepath_class_no_checksum(self, schema_ext, enable_filepath_feature):
        logger = logging.getLogger("datajoint")
        log_capture = io.StringIO()
        stream_handler = logging.StreamHandler(log_capture)
        log_format = logging.Formatter(
            "[%(asctime)s][%(funcName)s][%(levelname)s]: %(message)s"
        )
        stream_handler.setFormatter(log_format)
        stream_handler.set_name("test_limit_warning")
        logger.addHandler(stream_handler)
        self._test_filepath_class(table=Filepath(), store="repo", verify_checksum=False)
        log_contents = log_capture.getvalue()
        log_capture.close()
        for handler in logger.handlers:  # Clean up handler
            if handler.name == "test_limit_warning":
                logger.removeHandler(handler)
        assert "Skipped checksum for file with hash:" in log_contents


@pytest.mark.parametrize(
    "table, store",
    (
        (Filepath(), "repo"),
        (FilepathS3(), "repo-s3"),
    ),
)
def test_filepath_cleanup(table, store, schema_ext, enable_filepath_feature):
    """test deletion of filepath entries from external table"""
    stage_path = dj.config["stores"][store]["stage"]
    n = 20
    contents = os.urandom(345)
    for i in range(n):
        relative_path = Path(*random.sample(("one", "two", "three", "four"), k=3))
        managed_file = Path(stage_path, relative_path, "file.dat")
        managed_file.parent.mkdir(parents=True, exist_ok=True)
        with managed_file.open("wb") as f:
            f.write(contents)  # same in all files
        table.insert1((i, str(managed_file)))
    assert len(table) == n

    ext = schema_ext.external[store]

    assert len(table) == n
    assert 0 < len(ext) < n

    (table & "fnum in (1, 2, 3, 4, 5, 6)").delete()
    m = n - len(table)  # number deleted
    assert m == 6

    ext.delete(delete_external_files=True)  # delete unused entries
    assert 0 < len(ext) <= n - m


def test_delete_without_files(
    schema_ext,
    enable_filepath_feature,
    store="repo",
):
    """test deletion of filepath entries from external table without removing files"""
    # do not delete unused entries
    schema_ext.external[store].delete(delete_external_files=False)


def test_return_string(
    schema_ext, enable_filepath_feature, table=Filepath(), store="repo"
):
    """test returning string on fetch"""
    stage_path = dj.config["stores"][store]["stage"]
    # create a mock file
    relative_path = "this/is/a/test"
    managed_file = Path(stage_path, relative_path, "string.dat")
    managed_file.parent.mkdir(parents=True, exist_ok=True)
    data = os.urandom(3000)
    with managed_file.open("wb") as f:
        f.write(data)
    with managed_file.open("rb") as f:
        contents = f.read()
    assert data == contents

    # upload file into shared repo
    table.insert1((138, str(managed_file)))

    # remove file locally
    managed_file.unlink()
    assert not managed_file.is_file()

    # fetch file from remote
    filepath = (table & {"fnum": 138}).fetch1("img")
    assert isinstance(filepath, str)
