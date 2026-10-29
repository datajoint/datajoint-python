"""Unit tests for ObjectRef."""

from datetime import datetime, timezone

import pytest

from datajoint.objectref import ObjectRef
from datajoint.storage import StorageBackend


def _object_ref(spec):
    return ObjectRef(
        path="_schema/scratch/result/data_abc.txt",
        size=5,
        hash=None,
        ext=".txt",
        is_dir=False,
        timestamp=datetime.now(timezone.utc),
        _backend=StorageBackend(spec),
    )


@pytest.mark.parametrize(
    ("spec", "prefix"),
    [
        (
            {"protocol": "s3", "endpoint": "localhost:9000", "bucket": "bkt", "location": "proj/outbox"},
            "s3://bkt/proj/outbox/",
        ),
        ({"protocol": "s3", "endpoint": "localhost:9000", "bucket": "bkt"}, "s3://bkt/"),
        ({"protocol": "gcs", "bucket": "bkt", "location": "proj"}, "gs://bkt/proj/"),
        ({"protocol": "azure", "container": "ctr", "location": "proj"}, "az://ctr/proj/"),
    ],
)
def test_full_path_includes_store_location(spec, prefix):
    """full_path points where the backend stores the object, location included."""
    ref = _object_ref(spec)
    assert ref.full_path == prefix + ref.path
