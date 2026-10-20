"""
Guards the diagram edge-weight (cardinality) rule (#1532).

Line weight encodes cardinality only, and it is binary:
- **thick** (penwidth 2): the foreign key constitutes the child's *entire*
  primary key -> a 1:1 dependency.
- **thin** (penwidth 0.75): the child has primary-key attributes beyond those
  the foreign key contributes (newly declared, or inherited from another foreign
  key) -> a one-to-many dependency.

Master-part is NOT a weight: a part almost always adds a key attribute, so its
edge is thin under this same rule. This test pins that, since the historical
documentation inverted it ("thick = master-part").
"""

import time

import pytest

import datajoint as dj

THICK = 2.0
THIN = 0.75


@pytest.fixture(scope="function")
def schema_by_backend(connection_by_backend, db_creds_by_backend):
    backend = db_creds_by_backend["backend"]
    test_id = str(int(time.time() * 1000))[-8:]
    schema_name = f"djtest_edgewt_{backend}_{test_id}"[:64]
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


def _penwidth_by_dest(dot):
    """Map each edge's destination-node tail -> penwidth (float)."""
    out = {}
    for edge in dot.get_edges():
        dest = edge.get_destination().strip('"').lower()
        try:
            pw = float(edge.get_penwidth())
        except (TypeError, ValueError):
            pw = None
        out.setdefault(dest, []).append((edge.get_source().strip('"').lower(), pw))
    return out


def _penwidth_for(edges_by_dest, dest_name):
    matches = edges_by_dest.get(dest_name, [])
    assert matches, f"no edge found into node {dest_name!r}; nodes: {list(edges_by_dest)}"
    return matches


def test_edge_weight_encodes_cardinality(schema_by_backend):
    if not dj.diagram.diagram_active:
        pytest.skip("networkx/pydot not available")

    @schema_by_backend
    class Parent(dj.Manual):
        definition = """
        parent_id : int32
        """

        class Part(dj.Part):
            definition = """
            -> master
            part_id : int32
            """

    @schema_by_backend
    class OneToOne(dj.Manual):
        definition = """
        -> Parent
        """

    @schema_by_backend
    class OneToMany(dj.Manual):
        definition = """
        -> Parent
        sub_id : int32
        """

    @schema_by_backend
    class RenamedOneToOne(dj.Manual):
        # A renamed foreign key can still be 1:1: the renamed column is
        # RenamedOneToOne's entire primary key, so the dependency is 1:1 -> thick.
        # The rule must compare child columns to the child PK, not parent-PK
        # names to child-PK names (which renaming would break).
        definition = """
        -> Parent.proj(alt_parent_id='parent_id')
        """

    dot = dj.Diagram(schema_by_backend).make_dot()
    edges = _penwidth_by_dest(dot)

    # 1:1 — the FK is OneToOne's entire primary key -> thick.
    assert all(
        pw == THICK for _, pw in _penwidth_for(edges, "onetoone")
    ), f"1:1 dependency must be thick ({THICK}); edges={edges}"
    # multi-valued — OneToMany adds `sub_id` -> thin.
    assert all(
        pw == THIN for _, pw in _penwidth_for(edges, "onetomany")
    ), f"multi-valued dependency must be thin ({THIN}); edges={edges}"
    # master -> part — the part adds `part_id` -> thin (NOT thick).
    assert all(
        pw == THIN for _, pw in _penwidth_for(edges, "parent.part")
    ), f"master-part edge must be thin ({THIN}); it is not a 1:1 dependency; edges={edges}"
    # renamed FK that is the child's whole primary key — still 1:1 -> thick.
    assert all(
        pw == THICK for _, pw in _penwidth_for(edges, "renamedonetoone")
    ), f"a renamed 1:1 foreign key must be thick ({THICK}); the rule must be rename-safe; edges={edges}"
