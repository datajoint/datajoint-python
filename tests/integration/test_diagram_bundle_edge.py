"""
Guards the collapsed-edge (bundle) rule.

A collapsed node stands for a set of tables -- a whole schema, or any subset of
one, since expanding a table back out of a collapsed diagram leaves the rest
collapsed. When *both* ends of an edge are collapsed nodes, the edge is a
*bundle*: it stands for every foreign key between the two sets of tables, so the
per-foreign-key properties do not apply to it:

- cardinality (thick = 1:1, thin = one-to-many) is defined for one foreign key;
- solid-vs-dashed distinguishes a primary from a secondary foreign key;
- the renamed-foreign-key hue marks one renamed reference.

A bundle may mix all of these, so it claims none of them: every bundle edge is
drawn identically, whether its endpoints stand for whole schemas or parts of
them. Before this rule the collapsed edge inherited whichever member foreign key
``_apply_collapse`` happened to visit first, which made the drawn style depend on
graph traversal order -- the same schemas could render a bundle solid in one
process and dashed in another, and a two-foreign-key bundle could come out either
thick or thin.

An edge with only *one* collapsed end is not a bundle: it still names a single
table, so its style remains meaningful and is preserved.
"""

import time

import pytest

import datajoint as dj

BUNDLE_PENWIDTH = 2.0
THIN = 0.75


@pytest.fixture(scope="function")
def two_schemas(connection_by_backend, db_creds_by_backend):
    backend = db_creds_by_backend["backend"]
    test_id = str(int(time.time() * 1000))[-8:]
    names = [f"djtest_bundle_{backend}_{test_id}_{i}"[:64] for i in (0, 1)]

    def drop():
        if not connection_by_backend.is_connected:
            return
        for name in reversed(names):
            try:
                connection_by_backend.query(f"DROP DATABASE IF EXISTS {connection_by_backend.adapter.quote_identifier(name)}")
            except Exception:
                pass

    drop()
    schemas = [dj.Schema(name, connection=connection_by_backend) for name in names]
    yield schemas
    drop()


def _bundle_edges(dot):
    """Every edge in a collapsed diagram, as (source, dest, penwidth, style)."""
    out = []
    for edge in dot.get_edges():
        try:
            pw = float(edge.get_penwidth())
        except (TypeError, ValueError):
            pw = None
        out.append(
            (
                edge.get_source().strip('"').lower(),
                edge.get_destination().strip('"').lower(),
                pw,
                (edge.get_style() or "solid").strip('"'),
            )
        )
    return out


def test_bundle_edges_are_uniform(two_schemas):
    """A bundle mixing every per-FK property still renders as one uniform edge."""
    if not dj.diagram.diagram_active:
        pytest.skip("networkx/pydot not available")

    upstream, downstream = two_schemas

    @upstream
    class Root(dj.Manual):
        definition = """
        root_id : int32
        """

    @upstream
    class Tag(dj.Manual):
        definition = """
        tag_id : int32
        """

    @downstream
    class Mixed(dj.Manual):
        # The bundle upstream -> downstream deliberately contains one primary,
        # 1:1-eligible foreign key and one secondary (nullable) one, so an
        # order-sensitive implementation could pick either style.
        definition = """
        -> Root
        ---
        -> [nullable] Tag
        """

    @downstream
    class Leaf(dj.Manual):
        definition = """
        -> Mixed
        leaf_id : int32
        """

    collapsed = (dj.Diagram(upstream) + dj.Diagram(downstream)).collapse()
    edges = _bundle_edges(collapsed.make_dot())
    assert edges, "collapsed diagram produced no edges"

    # Every edge here crosses between two collapsed schema nodes, so all are bundles.
    for source, dest, penwidth, style in edges:
        assert penwidth == BUNDLE_PENWIDTH, (
            f"bundle edge {source} -> {dest} has penwidth {penwidth}; every bundle "
            f"edge must be {BUNDLE_PENWIDTH} regardless of its members' cardinality"
        )
        assert "dashed" not in style, (
            f"bundle edge {source} -> {dest} is {style!r}; a bundle mixing primary "
            "and secondary foreign keys must not claim either"
        )


def test_bundle_style_is_order_independent(two_schemas):
    """Declaring the bundle's members in the opposite order changes nothing."""
    if not dj.diagram.diagram_active:
        pytest.skip("networkx/pydot not available")

    upstream, downstream = two_schemas

    @upstream
    class Root(dj.Manual):
        definition = """
        root_id : int32
        """

    @upstream
    class Tag(dj.Manual):
        definition = """
        tag_id : int32
        """

    # Secondary reference declared on the table created *first* this time, so the
    # two foreign keys in the bundle are reached in the opposite order.
    @downstream
    class SecondaryFirst(dj.Manual):
        definition = """
        -> Tag
        ---
        -> [nullable] Root
        """

    @downstream
    class PrimaryLater(dj.Manual):
        definition = """
        -> Root
        """

    collapsed = (dj.Diagram(upstream) + dj.Diagram(downstream)).collapse()
    widths = {pw for _, _, pw, _ in _bundle_edges(collapsed.make_dot())}
    styles = {"dashed" in style for _, _, _, style in _bundle_edges(collapsed.make_dot())}

    assert widths == {BUNDLE_PENWIDTH}, (
        f"bundle penwidths {widths} depend on declaration order; expected only " f"{BUNDLE_PENWIDTH}"
    )
    assert styles == {False}, "bundle solid/dashed depends on declaration order"


def test_one_collapsed_end_preserves_edge_style(two_schemas):
    """An edge with a single expanded endpoint keeps its own style.

    Only edges between two collapsed nodes are bundles. When one end is a real
    table the edge still describes a specific foreign key, so its cardinality and
    primary-vs-secondary styling stay.
    """
    if not dj.diagram.diagram_active:
        pytest.skip("networkx/pydot not available")

    upstream, downstream = two_schemas

    @upstream
    class Root(dj.Manual):
        definition = """
        root_id : int32
        """

    @downstream
    class Multi(dj.Manual):
        # Adds its own key attribute -> multi-valued -> thin, which differs from the
        # uniform bundle weight, so a bundle rule leaking in here would show up.
        definition = """
        -> Root
        sub_id : int32
        """

    diagram = (dj.Diagram(upstream) + dj.Diagram(downstream)).collapse() + dj.Diagram(Multi)
    edges = {(src, dest): (pw, style) for src, dest, pw, style in _bundle_edges(diagram.make_dot())}
    assert edges, "partially collapsed diagram produced no edges"

    into_multi = [(pw, style) for (src, dest), (pw, style) in edges.items() if dest.endswith("multi")]
    assert into_multi, f"expected an edge into the expanded Multi table; got {list(edges)}"
    for penwidth, style in into_multi:
        assert penwidth == THIN, (
            "an edge with one expanded end is not a bundle: it must keep its own "
            f"cardinality weight ({THIN} for multi-valued), got {penwidth}"
        )
