import datajoint as dj
from .schema_simple import LOCALS_SIMPLE, A, B, D, E, G, L, OutfitLaunch
from .schema_advanced import *


def test_decorator(schema_simp):
    assert issubclass(A, dj.Lookup)
    assert not issubclass(A, dj.Part)
    assert B.database == schema_simp.database
    assert issubclass(B.C, dj.Part)
    assert B.C.database == schema_simp.database
    assert B.C.master is B and E.F.master is E


def test_dependencies(schema_simp):
    deps = schema_simp.connection.dependencies
    deps.load()
    assert all(cls.full_table_name in deps for cls in (A, B, B.C, D, E, E.F, L))
    assert set(A().children()) == set([B.full_table_name, D.full_table_name])
    assert set(D().parents(primary=True)) == set([A.full_table_name])
    assert set(D().parents(primary=False)) == set([L.full_table_name])
    assert set(deps.descendants(L.full_table_name)).issubset(
        cls.full_table_name for cls in (L, D, E, E.F, E.G, E.H, E.M, G)
    )


def test_erd(schema_simp):
    assert dj.diagram.diagram_active, "Failed to import networkx and pydot"
    erd = dj.ERD(schema_simp, context=LOCALS_SIMPLE)
    graph = erd._make_graph()
    assert set(cls.__name__ for cls in (A, B, D, E, L)).issubset(graph.nodes())


def test_erd_algebra(schema_simp):
    erd0 = dj.ERD(B)
    erd1 = erd0 + 3
    erd2 = dj.Di(E) - 3
    erd3 = erd1 * erd2
    erd4 = (erd0 + E).add_parts() - B - E
    assert erd0.nodes_to_show == set(cls.full_table_name for cls in [B])
    assert erd1.nodes_to_show == set(
        cls.full_table_name for cls in (B, B.C, E, E.F, E.G, E.H, E.M, G)
    )
    assert erd2.nodes_to_show == set(cls.full_table_name for cls in (A, B, D, E, L))
    assert erd3.nodes_to_show == set(cls.full_table_name for cls in (B, E))
    assert erd4.nodes_to_show == set(
        cls.full_table_name for cls in (B.C, E.F, E.G, E.H, E.M)
    )


def test_repr_svg(schema_adv):
    erd = dj.ERD(schema_adv, context=dict())
    svg = erd._repr_svg_()
    assert svg.startswith("<svg") and svg.endswith("svg>")


def test_make_image(schema_simp):
    erd = dj.ERD(schema_simp, context=dict())
    img = erd.make_image()
    assert img.ndim == 3 and img.shape[2] in (3, 4)


def test_part_table_parsing(schema_simp):
    # https://github.com/datajoint/datajoint-python/issues/882
    erd = dj.Di(schema_simp, context=LOCALS_SIMPLE)
    graph = erd._make_graph()
    assert "OutfitLaunch" in graph.nodes()
    assert "OutfitLaunch.OutfitPiece" in graph.nodes()
