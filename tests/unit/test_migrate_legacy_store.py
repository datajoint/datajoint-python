"""Reading DataJoint 0.14 in-store objects after migration.

DataJoint 0.14 kept blobs and attachments at paths derived from a 16-byte MD5
digest. Its layout, taken from ``datajoint/external.py`` and ``datajoint/hash.py``
at the 0.14.6 tag and unchanged through 0.14.10:

- a blob at ``{database}/{subfolders}/{hex digest}``, the digest of its bytes;
- an attachment at ``{database}/{subfolders}/{hex digest}.{name}``, the digest of
  its name, a NUL byte, and its contents, with the contents alone in the file;
- subfolders from the store's ``subfolding`` setting, (2, 2) unless set.

These tests pin the paths and hashes the migration writes, and the read of a
migrated attachment, without a database.
"""

import hashlib
from pathlib import Path

import pytest

from datajoint.errors import DataJointError
from datajoint.hash_registry import compute_hash, get_hash
from datajoint.adapters.mysql import MySQLAdapter
from datajoint.migrate import (
    _comment_literal,
    _legacy_hash,
    _legacy_kind,
    _legacy_path,
    _legacy_subfolding,
    _new_comment,
    verify_schema_v20,
)
from datajoint.settings import Config

# The digest from the issue report: `{location}/{database}/ec/88/<hash hex>`.
DIGEST = bytes.fromhex("ec88a4468ab852d319309178a2e34955")


class TestLegacyLayout:
    def test_blob_path_is_subfolded_hex_digest(self):
        hex_digest = DIGEST.hex()
        assert _legacy_path("lab_ephys", DIGEST, (2, 2)) == f"lab_ephys/ec/88/{hex_digest}"

    def test_attachment_path_appends_the_name(self):
        hex_digest = DIGEST.hex()
        assert _legacy_path("lab_ephys", DIGEST, (2, 2), "report.txt") == f"lab_ephys/ec/88/{hex_digest}.report.txt"

    def test_custom_and_empty_subfolding(self):
        hex_digest = DIGEST.hex()
        assert _legacy_path("db", DIGEST, (3,)) == f"db/ec8/{hex_digest}"
        assert _legacy_path("db", DIGEST, ()) == f"db/{hex_digest}"

    def test_hash_is_the_same_digest_in_base32(self):
        data = b"any serialized blob"
        assert _legacy_hash(hashlib.md5(data).digest()) == compute_hash(data)
        # The issue's digest gives the hash 2.x computed from that file.
        assert _legacy_hash(DIGEST).startswith("5sekiruk")

    def test_attachment_hash_matches_what_attach_stores(self):
        # 0.14: uuid_from_file(path, init_string=name + "\0"); 2.x <attach> stores name\0contents.
        name, contents = "report.txt", b"spike count: 12\n"
        digest = hashlib.md5((name + "\0").encode() + contents).digest()
        assert _legacy_hash(digest) == compute_hash(name.encode() + b"\x00" + contents)


class TestLegacySubfolding:
    def test_default_is_014_default(self):
        config = Config()
        config.stores["legacy"] = {"protocol": "file", "location": "/data"}
        assert _legacy_subfolding("legacy", config) == (2, 2)

    def test_store_setting_wins(self):
        config = Config()
        config.stores["legacy"] = {"protocol": "file", "location": "/data", "subfolding": [3]}
        assert _legacy_subfolding("legacy", config) == (3,)
        config.stores["legacy"] = {"protocol": "file", "location": "/data", "subfolding": None}
        assert _legacy_subfolding("legacy", config) == ()


class TestMigratedAttachmentRead:
    """`get_hash` puts a migrated attachment's name back before checking the hash."""

    NAME = "report.txt"
    CONTENTS = b"spike count: 12\n"

    @pytest.fixture
    def cfg(self, tmp_path):
        config = Config()
        config.stores["legacy"] = {"protocol": "file", "location": str(tmp_path)}
        return config

    @pytest.fixture
    def metadata(self, tmp_path):
        digest = hashlib.md5((self.NAME + "\0").encode() + self.CONTENTS).digest()
        path = _legacy_path("db", digest, (2, 2), self.NAME)
        file = Path(tmp_path, path)
        file.parent.mkdir(parents=True)
        file.write_bytes(self.CONTENTS)
        return {
            "hash": _legacy_hash(digest),
            "path": path,
            "store": "legacy",
            "size": len(self.CONTENTS),
            "attachment_name": self.NAME,
        }

    def test_returns_what_attach_decodes(self, cfg, metadata):
        assert get_hash(metadata, config=cfg) == self.NAME.encode() + b"\x00" + self.CONTENTS

    def test_changed_contents_fail_the_hash_check(self, cfg, metadata, tmp_path):
        Path(tmp_path, metadata["path"]).write_bytes(b"spike count: 13\n")
        with pytest.raises(DataJointError, match="Hash mismatch"):
            get_hash(metadata, config=cfg)

    def test_changed_name_fails_the_hash_check(self, cfg, metadata):
        with pytest.raises(DataJointError, match="Hash mismatch"):
            get_hash({**metadata, "attachment_name": "other.txt"}, config=cfg)

    def test_without_the_name_the_hash_check_fails(self, cfg, metadata):
        del metadata["attachment_name"]
        with pytest.raises(DataJointError, match="Hash mismatch"):
            get_hash(metadata, config=cfg)


class _PyMySQLFormatting:
    """Formats each query with its arguments as PyMySQL does, and returns no rows."""

    def __init__(self):
        self.queries = []

    def query(self, sql, args=()):
        self.queries.append(sql % tuple(repr(a) for a in args))
        return self

    def fetchone(self):
        return (0,)

    def fetchall(self):
        return []


def test_verify_schema_v20_query_formats_with_arguments():
    """The LIKE pattern's % is escaped, so formatting with the schema name succeeds."""
    connection = _PyMySQLFormatting()
    result = verify_schema_v20("lab_ephys", connection=connection)
    assert result["compatible"]
    assert any("LIKE '%blob'" in q for q in connection.queries)


class _MySQLConnection:
    adapter = MySQLAdapter()


@pytest.mark.parametrize(
    "comment",
    ["subject's report", "100% done", "path C:\\data\\raw", "it's 50% \\ 'quoted'"],
)
def test_comment_literal_survives_formatting(comment):
    """A comment literal formats with arguments and reads back as the same text."""
    literal = _comment_literal(_MySQLConnection(), comment)
    formatted = literal % ()  # what Connection.query does with no arguments
    assert formatted.startswith("'") and formatted.endswith("'")
    body = formatted[1:-1]
    assert body.replace("''", "'").replace("\\\\", "\\") == comment


@pytest.mark.parametrize(
    "comment, kind, store, new",
    [
        (":blob@legacy:raw signal", "blob", "legacy", ":<blob@legacy>:raw signal"),
        (":attach@ext-store:subject's report", "attach", "ext-store", ":<attach@ext-store>:subject's report"),
        (":filepath@raw:", "filepath", "raw", ":<filepath@raw>:"),
    ],
)
def test_legacy_kind_and_new_comment(comment, kind, store, new):
    assert _legacy_kind(comment) == (kind, store)
    assert _new_comment(kind, store, comment) == new


def test_2x_comment_is_not_a_legacy_kind():
    assert _legacy_kind(":<blob@legacy>:raw signal") == (None, None)
