"""
Cleanup in ``Job.refresh()`` for a ``key_source`` restricted by part tables.

A ``key_source`` such as ``A * B & A.Item & B.Item`` keeps the keys whose masters
carry parts. Checking a job against it with ``NOT IN (key_source)`` lets MySQL
materialize the subquery as a join of both part tables, whose size is the product
of their rows: with tens of thousands of parts per master, every ``refresh()``
took minutes, and a loop that refreshes before every ``populate()`` paid that on
every pass. The cleanup checks the jobs against ``key_source`` without nesting it
in ``NOT IN``.
"""

import time
from datetime import datetime

import pytest

import datajoint as dj

N_A_ITEMS = 3000
N_B_ITEMS = 2000


@pytest.fixture(scope="function")
def schema_by_backend(connection_by_backend, db_creds_by_backend):
    backend = db_creds_by_backend["backend"]
    test_id = str(int(time.time() * 1000))[-8:]
    schema_name = f"djtest_partks_{backend}_{test_id}"[:64]
    quote = connection_by_backend.adapter.quote_identifier
    connection_by_backend.query(f"DROP DATABASE IF EXISTS {quote(schema_name)}")
    schema = dj.Schema(schema_name, connection=connection_by_backend)
    yield schema
    if connection_by_backend.is_connected:
        connection_by_backend.query(f"DROP DATABASE IF EXISTS {quote(schema_name)}")


@pytest.fixture
def pipeline(schema_by_backend):
    @schema_by_backend
    class Session(dj.Manual):
        definition = """
        session_id : int32
        """

    @schema_by_backend
    class Profile(dj.Manual):
        definition = """
        -> Session
        profile_id : int16
        """

        class Item(dj.Part):
            definition = """
            -> master
            item : int32
            """

    @schema_by_backend
    class Grouping(dj.Manual):
        definition = """
        -> Session
        grouping_id : int16
        """

        class Item(dj.Part):
            definition = """
            -> master
            item : int32
            """

    @schema_by_backend
    class Figure(dj.Computed):
        definition = """
        -> Profile
        -> Grouping
        ---
        n : int32
        """

        @property
        def key_source(self):
            # both masters must carry parts, not just keys
            return Profile * Grouping & Profile.Item & Grouping.Item

        def make(self, key):
            self.insert1(dict(key, n=len(Profile.Item & key)))

    return Session, Profile, Grouping, Figure


def _add(master, key, n_items):
    master.insert1(key)
    master.Item.insert([dict(key, item=i) for i in range(n_items)])


def _backdate(jobs):
    """Make every job older than the default stale_timeout."""
    for key in jobs.keys():
        jobs.update1({**key, "created_time": datetime(2000, 1, 1)})


def _statuses(jobs):
    return {row["profile_id"]: row["status"] for row in jobs.proj("status").to_dicts()}


def test_refresh_removes_jobs_of_vanished_keys(pipeline):
    """Records of a deleted key go at once; a key whose parts went goes after stale_timeout."""
    Session, Profile, Grouping, Figure = pipeline
    with dj.config.override(jobs={"keep_completed": True}):
        Session.insert1(dict(session_id=1))
        _add(Grouping, dict(session_id=1, grouping_id=0), 3)
        for p in range(3):
            _add(Profile, dict(session_id=1, profile_id=p), 3)
        Figure.populate(reserve_jobs=True)
        assert _statuses(Figure.jobs) == {0: "success", 1: "success", 2: "success"}

        (Profile & {"profile_id": 0}).delete(prompt=False)  # the row and its key go
        (Profile.Item & {"profile_id": 1}).delete_quick()  # the key leaves key_source; the row stays

        result = Figure.jobs.refresh()
        assert _statuses(Figure.jobs) == {1: "success", 2: "success"}
        assert result["removed"] == 1

        _backdate(Figure.jobs)
        result = Figure.jobs.refresh()
        assert _statuses(Figure.jobs) == {2: "success"}
        assert result["removed"] == 1


def test_refresh_does_not_join_the_parts(pipeline, connection_by_backend, db_creds_by_backend):
    """refresh() stays fast when key_source is restricted by two large part tables."""
    if db_creds_by_backend["backend"] != "mysql":
        pytest.skip("MySQL's semijoin materialization")
    Session, Profile, Grouping, Figure = pipeline
    conn = connection_by_backend
    with dj.config.override(jobs={"keep_completed": True}):
        Session.insert1(dict(session_id=1))
        _add(Grouping, dict(session_id=1, grouping_id=0), N_B_ITEMS)
        for p in range(2):
            _add(Profile, dict(session_id=1, profile_id=p), N_A_ITEMS)
        Figure.populate(reserve_jobs=True)
        _backdate(Figure.jobs)

        # At these sizes MySQL may still pick FirstMatch; with tens of thousands of
        # parts it picks materialization, which is forced here to keep the test small.
        switch = conn.query("SELECT @@SESSION.optimizer_switch").fetchone()[0]
        conn.query("SET SESSION optimizer_switch='firstmatch=off,loosescan=off,duplicateweedout=off'")
        try:
            start = time.monotonic()
            result = Figure.jobs.refresh()
            elapsed = time.monotonic() - start
        finally:
            conn.query(f"SET SESSION optimizer_switch='{switch}'")

    assert result["removed"] == 0
    assert len(Figure.jobs.completed) == 2
    assert elapsed < 2, f"refresh() took {elapsed:.1f} s"
