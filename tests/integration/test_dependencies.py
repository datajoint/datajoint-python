from unittest.mock import patch

from pytest import raises

from datajoint import errors


def test_nullable_dependency(thing_tables):
    """test nullable unique foreign key"""
    # Thing C has a nullable dependency on B whose primary key is composite
    _, _, c, _, _ = thing_tables

    # missing foreign key attributes = ok
    c.insert1(dict(a=0))
    c.insert1(dict(a=1, b1=33))
    c.insert1(dict(a=2, b2=77))

    # unique foreign key attributes = ok
    c.insert1(dict(a=3, b1=1, b2=1))
    c.insert1(dict(a=4, b1=1, b2=2))

    assert len(c) == len(c.to_arrays()) == 5


def test_topo_sort():
    import networkx as nx

    import datajoint as dj

    graph = nx.DiGraph(
        [
            ("`a`.`a`", "`a`.`m`"),
            ("`a`.`a`", "`a`.`z`"),
            ("`a`.`m`", "`a`.`m__part`"),
            ("`a`.`z`", "`a`.`m__part`"),
        ]
    )
    assert dj.dependencies.topo_sort(graph) == [
        "`a`.`a`",
        "`a`.`z`",
        "`a`.`m`",
        "`a`.`m__part`",
    ]


def test_unique_dependency(thing_tables):
    """test nullable unique foreign key"""
    # Thing C has a nullable dependency on B whose primary key is composite
    _, _, c, _, _ = thing_tables

    c.insert1(dict(a=0, b1=1, b2=1))
    # duplicate foreign key attributes = not ok
    with raises(errors.DuplicateError):
        c.insert1(dict(a=1, b1=1, b2=1))


class TestLoadAllShortCircuit:
    """Finding #7 (audit deferred): the ``load_all_upstream`` /
    ``load_all_downstream`` helpers must short-circuit when the graph already
    covers every schema they would discover. The guard at
    ``dependencies.py:266-267`` and ``:300-301`` was introduced by PR #1499 as
    the ~53 ms/key populate speedup; a regression would silently pass CI
    because the correctness of the graph is unaffected — only its cost.

    These tests count calls to ``Dependencies.load`` on the second invocation.
    The first call must load once (to populate ``_loaded_schemas``); the
    second must NOT call ``load`` at all — the ``if self._loaded and
    known_schemas <= self._loaded_schemas: return`` early-return owns the
    entire path.
    """

    def test_load_all_downstream_short_circuits_on_repeat(self, thing_tables):
        # thing_tables ensures at least one schema is activated on the
        # connection, so the ``if not known_schemas: self.load(); return``
        # bail-out branch does not fire.
        a, _, _, _, _ = thing_tables
        conn = a.connection

        # Warm up the graph once — this call is allowed to invoke load().
        conn.dependencies.load_all_downstream()
        assert conn.dependencies._loaded is True
        assert conn.dependencies._loaded_schemas, "warm-up must populate _loaded_schemas"

        # Second identical call must hit the early-return: no load(), no rebuild.
        with patch.object(type(conn.dependencies), "load", wraps=conn.dependencies.load) as spy_load:
            conn.dependencies.load_all_downstream()
        assert spy_load.call_count == 0, (
            "load_all_downstream must short-circuit when _loaded_schemas already "
            "covers the discovered set (#1493, PR #1499). Deleting the "
            "`if self._loaded and known_schemas <= self._loaded_schemas: return` "
            "at dependencies.py:266-267 would make this test fail."
        )

    def test_load_all_upstream_short_circuits_on_repeat(self, thing_tables):
        a, _, _, _, _ = thing_tables
        conn = a.connection

        conn.dependencies.load_all_upstream()
        assert conn.dependencies._loaded is True
        assert conn.dependencies._loaded_schemas

        with patch.object(type(conn.dependencies), "load", wraps=conn.dependencies.load) as spy_load:
            conn.dependencies.load_all_upstream()
        assert spy_load.call_count == 0, (
            "load_all_upstream must short-circuit when _loaded_schemas already "
            "covers the discovered set (#1493, PR #1499). Deleting the "
            "`if self._loaded and known_schemas <= self._loaded_schemas: return` "
            "at dependencies.py:300-301 would make this test fail."
        )
