"""
Cleanup in ``Job.refresh()`` (#1577).

``refresh(*restrictions)`` narrows the keys it adds, but the cleanup must judge
which keys have vanished against the full ``key_source``: jobs outside the
restriction -- reserved by other workers, or recording errors -- are not stale.
``success`` and ``error`` records whose key has left both ``key_source`` and the
target are removed on refresh, in one statement rather than one per key.
``stale_timeout=0`` means no grace period for other jobs whose key has left
``key_source``: they are removed on that refresh, not kept indefinitely.
"""

import time
from datetime import datetime

import pytest

import datajoint as dj


@pytest.fixture(scope="function")
def schema_by_backend(connection_by_backend, db_creds_by_backend):
    """Create a fresh schema per test, parameterized across backends."""
    backend = db_creds_by_backend["backend"]
    test_id = str(int(time.time() * 1000))[-8:]
    schema_name = f"djtest_jobsrefresh_{backend}_{test_id}"[:64]

    if connection_by_backend.is_connected:
        try:
            connection_by_backend.query(
                f"DROP DATABASE IF EXISTS {connection_by_backend.adapter.quote_identifier(schema_name)}"
            )
        except Exception:
            pass

    schema = dj.Schema(schema_name, connection=connection_by_backend)
    yield schema

    if connection_by_backend.is_connected:
        try:
            connection_by_backend.query(
                f"DROP DATABASE IF EXISTS {connection_by_backend.adapter.quote_identifier(schema_name)}"
            )
        except Exception:
            pass


@pytest.fixture
def pipeline(schema_by_backend):
    @schema_by_backend
    class Raw(dj.Manual):
        definition = """
        raw_id : int32
        ---
        v : float32
        """

    @schema_by_backend
    class Proc(dj.Computed):
        definition = """
        -> Raw
        ---
        w : float32
        """

        def make(self, key):
            self.insert1(dict(key, w=2 * (Raw & key).fetch1("v")))

    return Raw, Proc


def _statuses(jobs):
    """Map raw_id to job status."""
    return {row["raw_id"]: row["status"] for row in jobs.proj("status").to_dicts()}


def _backdate(jobs):
    """Make every job older than the default stale_timeout."""
    for key in jobs.keys():
        jobs.update1({**key, "created_time": datetime(2000, 1, 1)})


def test_restricted_refresh_keeps_jobs_outside_restriction(pipeline):
    """A restricted refresh leaves other workers' reservations and errors alone."""
    Raw, Proc = pipeline
    Raw.insert([dict(raw_id=i, v=float(i)) for i in range(4)])
    Proc.jobs.refresh()
    assert Proc.jobs.reserve({"raw_id": 1})  # held by another worker
    assert Proc.jobs.reserve({"raw_id": 2})
    Proc.jobs.error({"raw_id": 2}, "failed")
    (Raw & {"raw_id": 3}).delete(prompt=False)  # pending job for a vanished key
    _backdate(Proc.jobs)

    result = Proc.jobs.refresh({"raw_id": 0})

    assert _statuses(Proc.jobs) == {0: "pending", 1: "reserved", 2: "error"}
    assert result["removed"] == 1


def test_restricted_populate_keeps_jobs_outside_restriction(pipeline):
    """``populate(restriction, reserve_jobs=True)`` refreshes with the restriction."""
    Raw, Proc = pipeline
    Raw.insert([dict(raw_id=i, v=float(i)) for i in range(3)])
    Proc.jobs.refresh()
    assert Proc.jobs.reserve({"raw_id": 1})
    assert Proc.jobs.reserve({"raw_id": 2})
    Proc.jobs.error({"raw_id": 2}, "failed")
    _backdate(Proc.jobs)

    Proc.populate({"raw_id": 0}, reserve_jobs=True)

    assert len(Proc()) == 1
    assert _statuses(Proc.jobs) == {1: "reserved", 2: "error"}


def test_refresh_removes_records_of_vanished_keys(pipeline):
    """Success and error records whose upstream row was deleted go on the next refresh."""
    Raw, Proc = pipeline
    with dj.config.override(jobs={"keep_completed": True}):
        Raw.insert([dict(raw_id=i, v=float(i)) for i in range(3)])
        Proc.populate(reserve_jobs=True)
        Raw.insert1(dict(raw_id=3, v=3.0))
        Proc.jobs.refresh()
        assert Proc.jobs.reserve({"raw_id": 3})
        Proc.jobs.error({"raw_id": 3}, "failed")
        assert _statuses(Proc.jobs) == {0: "success", 1: "success", 2: "success", 3: "error"}

        (Raw & "raw_id in (0, 3)").delete(prompt=False)
        result = Proc.jobs.refresh()  # records are fresh: no stale_timeout involved

        assert _statuses(Proc.jobs) == {1: "success", 2: "success"}
        assert result["removed"] == 2


def test_refresh_keeps_success_records_of_existing_rows(schema_by_backend):
    """A fresh success record stays while its row exists, even if its key leaves key_source."""

    @schema_by_backend
    class Raw(dj.Manual):
        definition = """
        raw_id : int32
        ---
        v : float32
        """

    @schema_by_backend
    class Gated(dj.Computed):
        definition = """
        -> Raw
        ---
        w : float32
        """

        @property
        def key_source(self):
            return Raw & "v >= 0"

        def make(self, key):
            self.insert1(dict(key, w=2 * (Raw & key).fetch1("v")))

    with dj.config.override(jobs={"keep_completed": True}):
        Raw.insert([dict(raw_id=i, v=float(i)) for i in range(2)])
        Gated.populate(reserve_jobs=True)
        Raw.update1(dict(raw_id=0, v=-1.0))  # leaves key_source; Gated row remains

        Gated.jobs.refresh()

        assert _statuses(Gated.jobs) == {0: "success", 1: "success"}


def test_stale_timeout_zero_means_no_grace_period(pipeline):
    """``stale_timeout=0`` removes vanished pending jobs at once; the default keeps them for its grace period."""
    Raw, Proc = pipeline
    Raw.insert([dict(raw_id=i, v=float(i)) for i in range(3)])
    Proc.jobs.refresh()
    (Raw & "raw_id in (1, 2)").delete(prompt=False)

    result = Proc.jobs.refresh()  # stale_timeout=None: the configured grace period applies
    assert _statuses(Proc.jobs) == {0: "pending", 1: "pending", 2: "pending"}
    assert result["removed"] == 0

    result = Proc.jobs.refresh(stale_timeout=0)
    assert _statuses(Proc.jobs) == {0: "pending"}
    assert result["removed"] == 2


def test_configured_stale_timeout_zero_means_no_grace_period(pipeline):
    """``config.jobs.stale_timeout = 0`` makes the refresh in ``populate()`` remove vanished jobs at once."""
    Raw, Proc = pipeline
    Raw.insert([dict(raw_id=i, v=float(i)) for i in range(2)])
    Proc.jobs.refresh()
    assert Proc.jobs.reserve({"raw_id": 0})  # keeps populate() from computing raw_id=0
    (Raw & {"raw_id": 1}).delete(prompt=False)

    with dj.config.override(jobs={"stale_timeout": 0}):
        Proc.populate(reserve_jobs=True, suppress_errors=True)

    assert _statuses(Proc.jobs) == {0: "reserved"}


def test_refresh_cleanup_is_set_based(pipeline, monkeypatch):
    """Cleanup issues a fixed number of statements, not one per vanished key."""
    Raw, Proc = pipeline
    n = 20
    with dj.config.override(jobs={"keep_completed": True}):
        Raw.insert([dict(raw_id=i, v=float(i)) for i in range(n)])
        Proc.populate(reserve_jobs=True)
        Raw.delete(prompt=False)
        _backdate(Proc.jobs)  # also past stale_timeout: both cleanup steps apply

        jobs = Proc.jobs
        queries = []
        query = jobs.connection.query

        def counting_query(sql, *args, **kwargs):
            queries.append(sql)
            return query(sql, *args, **kwargs)

        monkeypatch.setattr(jobs.connection, "query", counting_query)
        result = jobs.refresh()
        monkeypatch.undo()

        assert result["removed"] == n
        assert len(Proc.jobs) == 0
        deletes = [sql for sql in queries if sql.lstrip().upper().startswith("DELETE")]
        assert 1 <= len(deletes) <= 2
        assert len(queries) < n
