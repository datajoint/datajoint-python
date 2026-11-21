"""
Unit tests for Job._delete_vanished(), which refresh() uses to remove jobs whose
keys have left key_source (#1673). Query expressions are replaced by an
in-memory stand-in, so the paging and the count shortcut are checked without a
database.
"""

from types import SimpleNamespace

import pytest

import datajoint.jobs
from datajoint.jobs import Job


class Rows:
    """Rows of one table, filtered by a list of predicates; reads see deletions."""

    def __init__(self, store, primary_key, predicates=(), unmatchable=()):
        self.store = store  # shared list of row dicts
        self.primary_key = list(primary_key)
        self.predicates = list(predicates)
        self.unmatchable = set(unmatchable)  # key values a restriction never matches, like an inexact float
        self.deletes = 0

    def _rows(self):
        return [row for row in self.store if all(p(row) for p in self.predicates)]

    def __len__(self):
        return len(self._rows())

    def proj(self):
        return self

    def restrict(self, other, semantic_check=True):
        keys = other._rows() if isinstance(other, Rows) else list(other)
        shared = [name for name in self.primary_key if all(name in key for key in keys)]
        wanted = {tuple(key[name] for name in shared) for key in keys}
        unmatchable = self.unmatchable

        def matches(row):
            value = tuple(row[name] for name in shared)
            return value in wanted and not (isinstance(other, list) and row["k"] in unmatchable)

        return Rows(self.store, self.primary_key, self.predicates + [matches], self.unmatchable)

    def __and__(self, other):
        return self.restrict(other)

    def keys(self, order_by=None, limit=None, offset=0):
        rows = sorted(self._rows(), key=lambda row: tuple(row[name] for name in self.primary_key))
        return [{name: row[name] for name in self.primary_key} for row in rows[offset:][:limit]]

    def delete_quick(self, get_count=False):
        doomed = self._rows()
        self.store[:] = [row for row in self.store if row not in doomed]
        return len(doomed)


def _job(job_keys, source_keys, unmatchable=()):
    jobs = Rows([{"k": k} for k in job_keys], ["k"], unmatchable=unmatchable)
    key_source = Rows([{"k": k} for k in source_keys], ["k"], unmatchable=unmatchable)
    job = SimpleNamespace(primary_key=["k"], _target=SimpleNamespace(key_source=key_source))
    return job, jobs


def _remaining(jobs):
    return sorted(row["k"] for row in jobs.store)


@pytest.fixture
def page_size(monkeypatch):
    monkeypatch.setattr(datajoint.jobs, "VANISHED_DELETE_BATCH", 2)
    return 2


def test_nothing_vanished_returns_without_deleting(page_size):
    job, jobs = _job(range(5), range(5))
    assert Job._delete_vanished(job, jobs) == 0
    assert _remaining(jobs) == [0, 1, 2, 3, 4]


def test_no_candidates(page_size):
    job, jobs = _job([], range(5))
    assert Job._delete_vanished(job, jobs) == 0


def test_pages_through_all_candidates(page_size):
    """Vanished keys on page boundaries and inside pages are all removed; the rest are kept."""
    job, jobs = _job(range(9), [2, 3, 5, 7])
    assert Job._delete_vanished(job, jobs) == 5
    assert _remaining(jobs) == [2, 3, 5, 7]


def test_candidates_limit_the_deletion(page_size):
    """Only candidate jobs are removed, even when other jobs' keys have vanished too."""
    job, jobs = _job(range(6), [0])
    candidates = Rows(jobs.store, ["k"], [lambda row: row["k"] % 2 == 1])
    assert Job._delete_vanished(job, candidates) == 3
    assert _remaining(jobs) == [0, 2, 4]


def test_moves_past_keys_that_do_not_match_themselves(page_size):
    """A page of keys that cannot be matched by value does not stop the cleanup."""
    job, jobs = _job(range(6), [2, 3], unmatchable={0, 1})
    assert Job._delete_vanished(job, jobs) == 2
    assert _remaining(jobs) == [0, 1, 2, 3]
