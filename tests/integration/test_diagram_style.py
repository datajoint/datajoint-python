"""
Style-contract (visual-regression) guard for the modernized dj.Diagram (#1532).

Rather than diff exact SVG geometry against a checked-in reference — which drifts
with the Graphviz version — this renders a fixed schema per theme and asserts the
style invariants the restyle controls: each tier's fill/stroke palette, the
thick/thin edge weights, rounded boxes, entity clusters, the dark background, and
the adaptive `prefers-color-scheme` block. A palette, weight, or theme regression
fails here; a layout tweak does not.
"""

import time

import pytest

import datajoint as dj


@pytest.fixture(scope="function")
def schema_by_backend(connection_by_backend, db_creds_by_backend):
    backend = db_creds_by_backend["backend"]
    test_id = str(int(time.time() * 1000))[-8:]
    schema_name = f"djtest_style_{backend}_{test_id}"[:64]
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


def _build(schema):
    @schema
    class Subject(dj.Manual):
        definition = "subject_id : int32"

    @schema
    class Params(dj.Lookup):
        definition = "param_id : int32"

    @schema
    class Session(dj.Manual):
        definition = "-> Subject\nsession_id : int32"

        class Note(dj.Part):
            definition = "-> master\nnote_id : int32"

    @schema
    class Scan(dj.Imported):
        definition = "-> Session"  # 1:1 -> thick edge

    @schema
    class Analysis(dj.Computed):
        definition = "-> Scan\n-> Params"  # composite -> thin edge

    # Return a context so the diagram resolves nodes to class names (the normal
    # rendering path — users have their classes in scope).
    return dict(Subject=Subject, Params=Params, Session=Session, Scan=Scan, Analysis=Analysis)


def _svg(schema, context, theme):
    with dj.config.override(display__diagram_theme=theme):
        return dj.Diagram(schema, context=context).svg_string().lower()


def test_light_theme_style(schema_by_backend):
    if not dj.diagram.diagram_active:
        pytest.skip("networkx/pydot not available")
    ctx = _build(schema_by_backend)
    svg = _svg(schema_by_backend, ctx, "light")
    # tier fills
    for fill in ("#e7f3ec", "#f2f4f7", "#e2ecfa", "#fbeaec", "#ffffff"):
        assert fill in svg, f"light tier fill {fill} missing"
    # a couple tier strokes
    for stroke in ("#2f7d5b", "#b23a48"):
        assert stroke in svg, f"light tier stroke {stroke} missing"
    # thick (1:1) and thin (multi) edge weights both present
    assert 'stroke-width="2"' in svg, "thick (1:1) edge missing"
    assert 'stroke-width="0.75"' in svg, "thin (multi-valued) edge missing"
    assert "cluster_entity_" in svg, "entity cluster missing"
    assert "161a21" not in svg, "light theme must not use the dark background"


def test_dark_theme_style(schema_by_backend):
    if not dj.diagram.diagram_active:
        pytest.skip("networkx/pydot not available")
    ctx = _build(schema_by_backend)
    svg = _svg(schema_by_backend, ctx, "dark")
    assert "#161a21" in svg, "dark background missing"
    for fill in ("#16281f", "#152538", "#331a1f"):
        assert fill in svg, f"dark tier fill {fill} missing"


def test_auto_theme_is_adaptive(schema_by_backend):
    if not dj.diagram.diagram_active:
        pytest.skip("networkx/pydot not available")
    ctx = _build(schema_by_backend)
    svg = _svg(schema_by_backend, ctx, "auto")
    assert "@media (prefers-color-scheme: dark)" in svg, "auto theme must inject the adaptive media block"
    # base render is light; the media block maps a light color to its dark counterpart
    assert "#e7f3ec" in svg and "#16281f" in svg, "auto theme must carry both light base and dark override colors"


def test_mermaid_matches_modernized_notation(schema_by_backend):
    """`make_mermaid` speaks the same notation as the Graphviz renderer:
    shared palette, cardinality-weighted edges, amber renamed FKs, and
    master-part entity nesting."""
    if not dj.diagram.diagram_active:
        pytest.skip("networkx/pydot not available")
    schema = schema_by_backend

    @schema
    class Subject(dj.Manual):
        definition = "subject_id : int32"

    @schema
    class Scan(dj.Imported):
        definition = "-> Subject\nscan_id : int32"  # composite PK -> thin edges

        class Field(dj.Part):
            definition = "-> master\nfield_id : int32"

    @schema
    class Analysis(dj.Computed):
        definition = "-> Scan.proj(src_scan='scan_id')"  # renamed FK, whole PK -> thick amber

    ctx = dict(Subject=Subject, Scan=Scan, Analysis=Analysis)
    mmd = dj.Diagram(schema, context=ctx).make_mermaid()

    # shared light-theme palette (classDefs), not the old bright colors
    assert "classDef manual fill:#E7F3EC,stroke:#2F7D5B" in mmd
    assert "classDef computed fill:#FBEAEC,stroke:#B23A48" in mmd
    assert "#90EE90" not in mmd, "old bright palette must be gone"

    # cardinality edge weights via linkStyle: thick 1:1 and thin one-to-many both present
    assert "stroke-width:2px" in mmd, "thick (1:1) edge missing"
    assert "stroke-width:1px" in mmd, "thin (one-to-many) edge missing"
    # cardinality replaces the old primary/secondary dotted-edge encoding
    assert "-.->" not in mmd, "edges must not encode primary/secondary as dotted"

    # renamed FK takes the theme's amber; ordinary edges the slate
    assert "stroke:#C77D3A" in mmd, "renamed-FK amber missing"
    assert "stroke:#3A424F" in mmd, "ordinary edge slate missing"

    # master-part group nests in an entity subgraph
    assert "subgraph entity_" in mmd, "master-part entity nesting missing"
