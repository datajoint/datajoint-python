"""
Joins on namesake attributes that no foreign key relates (#1640).

Lineage comes from foreign keys. An attribute that no foreign key brings in, such
as a native secondary attribute, a computed attribute or a key promoted by
``dj.U``, has no lineage, and a join on two such namesakes is refused as a join
on unrelated attributes, not as missing lineage.
"""

import time

import pytest

import datajoint as dj
from datajoint.errors import DataJointError


@pytest.fixture(scope="function")
def schema_by_backend(connection_by_backend, db_creds_by_backend):
    backend = db_creds_by_backend["backend"]
    schema_name = f"djtest_namesakes_{backend}_{str(int(time.time() * 1000))[-8:]}"
    schema = dj.Schema(schema_name, connection=connection_by_backend)
    yield schema
    schema.drop(prompt=False)


@pytest.fixture
def tables(schema_by_backend):
    @schema_by_backend
    class Image(dj.Manual):
        definition = """
        image_id : int32
        """

    @schema_by_backend
    class Segmentation(dj.Manual):
        definition = """
        -> Image
        ---
        input_hash : varchar(64)
        cell_count : int32
        """

    @schema_by_backend
    class Profile(dj.Manual):
        definition = """
        -> Image
        ---
        input_hash : varchar(64)
        mean_area : float32
        """

    @schema_by_backend
    class Label(dj.Manual):
        definition = """
        label_id : int32
        ---
        image_id : int32   # a plain number, not a reference to Image
        """

    Image.insert([(1,), (2,)])
    Segmentation.insert([(1, "a", 10), (2, "b", 20)])
    Profile.insert([(1, "a", 1.5), (2, "c", 2.5)])
    Label.insert([(1, 1)])
    return Image, Segmentation, Profile, Label


UNRELATED = "neither side inherits it through a foreign key"


def test_secondary_namesakes(schema_by_backend, db_creds_by_backend, tables):
    """Namesake secondary attributes of two tables: refused as unrelated, with no
    suggestion to rebuild lineage, and the same after rebuilding it."""
    _, Segmentation, Profile, _ = tables
    with pytest.raises(DataJointError, match=UNRELATED) as exc_info:
        Segmentation * Profile
    assert "`input_hash`" in str(exc_info.value)
    assert "rebuild_lineage" not in str(exc_info.value)

    if db_creds_by_backend["backend"] == "mysql":  # rebuild_lineage() is MySQL-only for now
        schema_by_backend.rebuild_lineage()
        with pytest.raises(DataJointError, match=UNRELATED):
            Segmentation * Profile


def test_computed_namesakes(tables):
    """Computed attributes of two projections are unrelated namesakes."""
    _, Segmentation, Profile, _ = tables
    with pytest.raises(DataJointError, match=UNRELATED):
        Segmentation.proj(n="cell_count + 1") * Profile.proj(n="mean_area * 2")


def test_promoted_key_namesakes(tables):
    """Primary-key attributes can lack lineage too: keys promoted by dj.U."""
    _, Segmentation, Profile, _ = tables
    left = dj.U("input_hash") & Segmentation
    right = dj.U("input_hash") & Profile
    assert "input_hash" in left.primary_key
    with pytest.raises(DataJointError, match=UNRELATED):
        left * right


def test_one_sided_namesake(tables):
    """A native key against a secondary namesake that no foreign key brings in."""
    Image, _, _, Label = tables
    with pytest.raises(DataJointError, match="but the other does not inherit it through a foreign key"):
        Image * Label


def test_renamed_or_excluded_namesake_joins(tables):
    """The remedy the error names: rename or exclude the attribute on one side."""
    _, Segmentation, Profile, _ = tables
    assert len(Segmentation * Profile.proj("mean_area")) == 2
    assert len(Segmentation * Profile.proj("mean_area", profile_hash="input_hash")) == 2
