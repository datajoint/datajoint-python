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
    for fill in ("#e8f0e9", "#f0f0f1", "#e0f4fc", "#ffede5", "#ffffff"):
        assert fill in svg, f"light tier fill {fill} missing"
    # a couple tier strokes
    for stroke in ("#3e7a52", "#ff5113"):
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
    for fill in ("#16281f", "#0f2433", "#331b12"):
        assert fill in svg, f"dark tier fill {fill} missing"


def test_auto_theme_is_adaptive(schema_by_backend):
    if not dj.diagram.diagram_active:
        pytest.skip("networkx/pydot not available")
    ctx = _build(schema_by_backend)
    svg = _svg(schema_by_backend, ctx, "auto")
    assert "@media (prefers-color-scheme: dark)" in svg, "auto theme must inject the adaptive media block"
    # base render is light; the media block maps a light color to its dark counterpart
    assert "#e8f0e9" in svg and "#16281f" in svg, "auto theme must carry both light base and dark override colors"


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
    assert "classDef manual fill:#E8F0E9,stroke:#3E7A52" in mmd
    assert "classDef computed fill:#FFEDE5,stroke:#FF5113" in mmd
    assert "#90EE90" not in mmd, "old bright palette must be gone"

    # cardinality edge weights via linkStyle: thick 1:1 and thin one-to-many both present
    assert "stroke-width:2px" in mmd, "thick (1:1) edge missing"
    assert "stroke-width:1px" in mmd, "thin (one-to-many) edge missing"
    # cardinality replaces the old primary/secondary dotted-edge encoding
    assert "-.->" not in mmd, "edges must not encode primary/secondary as dotted"

    # renamed FK takes the theme's amber; ordinary edges the slate
    assert "stroke:#C77D3A" in mmd, "renamed-FK amber missing"
    assert "stroke:#171C39" in mmd, "ordinary edge navy missing"

    # master-part group nests in an entity subgraph
    assert "subgraph entity_" in mmd, "master-part entity nesting missing"


def _contrast(fg, bg):
    def lum(h):
        c = [int(h.lstrip("#")[i : i + 2], 16) / 255 for i in (0, 2, 4)]
        c = [x / 12.92 if x <= 0.03928 else ((x + 0.055) / 1.055) ** 2.4 for x in c]
        return 0.2126 * c[0] + 0.7152 * c[1] + 0.0722 * c[2]

    hi, lo = sorted([lum(fg), lum(bg)], reverse=True)
    return (hi + 0.05) / (lo + 0.05)


def test_theme_text_contrast_meets_aa():
    """Every tier's text-on-fill pair holds WCAG AA (>= 4.5:1) in both themes (#1543)."""
    for name, theme in dj.diagram._DIAGRAM_THEMES.items():
        for tier, (fill, _stroke, text) in theme["palette"].items():
            ratio = _contrast(text, fill)
            assert ratio >= 4.5, f"{name}/{tier}: {text} on {fill} = {ratio:.2f} < 4.5"


def test_adaptive_mapping_is_collision_free():
    """Every light color maps to exactly one dark counterpart per attribute (#1532 invariant)."""
    light, dark = dj.diagram._DIAGRAM_THEMES["light"], dj.diagram._DIAGRAM_THEMES["dark"]
    for idx, kind in ((0, "fill"), (1, "stroke"), (2, "fill")):
        mapping = {}
        for tier in light["palette"]:
            lv, dv = light["palette"][tier][idx].lower(), dark["palette"][tier][idx]
            assert mapping.setdefault((kind, lv), dv) == dv, f"collision on {kind} {lv}"
    strokes = {}
    for lv, dv in [
        (light["edge"].lower(), dark["edge"]),
        (light["edge_renamed"].lower(), dark["edge_renamed"]),
        (light["schema_cluster"][0].lower(), dark["schema_cluster"][0]),
    ]:
        assert strokes.setdefault(lv, dv) == dv, f"stroke collision on {lv}"
