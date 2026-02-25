import pytest as _pytest

import datajoint as dj

from tests.schema_simple import LOCALS_SIMPLE, A, B, D, E, G, L, Profile, Website


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
    assert set(deps.descendants(L.full_table_name)).issubset(cls.full_table_name for cls in (L, D, E, E.F, E.G, E.H, E.M, G))


def test_erd(schema_simp):
    assert dj.diagram.diagram_active, "Failed to import networkx and pydot"
    erd = dj.Diagram(schema_simp, context=LOCALS_SIMPLE)
    graph = erd._make_graph()
    assert set(cls.__name__ for cls in (A, B, D, E, L)).issubset(graph.nodes())


def test_diagram_algebra(schema_simp):
    """Test Diagram algebra operations (+, -, *)."""
    diag0 = dj.Diagram(B)
    diag1 = diag0 + 3
    diag2 = dj.Diagram(E) - 3
    diag3 = diag1 * diag2
    diag4 = (diag0 + E).add_parts() - B - E
    assert diag0.nodes_to_show == set(cls.full_table_name for cls in [B])
    assert diag1.nodes_to_show == set(cls.full_table_name for cls in (B, B.C, E, E.F, E.G, E.H, E.M, G))
    assert diag2.nodes_to_show == set(cls.full_table_name for cls in (A, B, D, E, L))
    assert diag3.nodes_to_show == set(cls.full_table_name for cls in (B, E))
    assert diag4.nodes_to_show == set(cls.full_table_name for cls in (B.C, E.F, E.G, E.H, E.M))


def test_repr_svg(schema_adv):
    erd = dj.Diagram(schema_adv, context=dict())
    svg = erd._repr_svg_()
    assert svg.startswith("<svg") and svg.endswith("svg>")


def test_make_image(schema_simp):
    erd = dj.Diagram(schema_simp, context=dict())
    img = erd.make_image()
    assert img.ndim == 3 and img.shape[2] in (3, 4)


def test_part_table_parsing(schema_simp):
    # https://github.com/datajoint/datajoint-python/issues/882
    erd = dj.Diagram(schema_simp, context=LOCALS_SIMPLE)
    graph = erd._make_graph()
    assert "OutfitLaunch" in graph.nodes()
    assert "OutfitLaunch.OutfitPiece" in graph.nodes()


# --- prune() tests ---


@_pytest.fixture
def schema_simp_pop(schema_simp):
    """Populate the simple schema for prune tests."""
    Profile().delete()
    Website().delete()
    G().delete()
    E().delete()
    D().delete()
    B().delete()
    L().delete()
    A().delete()

    A().insert(A.contents, skip_duplicates=True)
    L().insert(L.contents, skip_duplicates=True)
    B().populate()
    D().populate()
    E().populate()
    G().populate()
    yield schema_simp


def test_prune_unrestricted(schema_simp_pop):
    """Prune on unrestricted diagram removes physically empty tables."""
    diag = dj.Diagram(schema_simp_pop, context=LOCALS_SIMPLE)
    original_count = len(diag.nodes_to_show)
    pruned = diag.prune()

    # Populated tables (A, L, B, B.C, D, E, E.F, G, etc.) should survive
    for cls in (A, B, D, E, L):
        assert cls.full_table_name in pruned.nodes_to_show, f"{cls.__name__} should not be pruned"

    # Empty tables like Profile should be removed
    assert Profile.full_table_name not in pruned.nodes_to_show, "empty Profile should be pruned"

    # Pruned diagram should have fewer nodes
    assert len(pruned.nodes_to_show) < original_count


def test_prune_after_restrict(schema_simp_pop):
    """Prune after restrict removes tables with zero matching rows."""
    diag = dj.Diagram(schema_simp_pop, context=LOCALS_SIMPLE)
    restricted = diag.restrict(A & "id_a=0")
    counts = restricted.preview()

    pruned = restricted.prune()
    pruned_counts = pruned.preview()

    # Every table in pruned preview should have > 0 rows
    assert all(c > 0 for c in pruned_counts.values()), "pruned diagram should have no zero-count tables"

    # Tables with zero rows in the original preview should be gone
    for table, count in counts.items():
        if count == 0:
            assert table not in pruned._restrict_conditions, f"{table} had 0 rows but was not pruned"


def test_prune_after_cascade(schema_simp_pop):
    """Prune after cascade removes tables with zero matching rows."""
    diag = dj.Diagram(schema_simp_pop, context=LOCALS_SIMPLE)
    cascaded = diag.cascade(A & "id_a=0")
    counts = cascaded.preview()

    pruned = cascaded.prune()
    pruned_counts = pruned.preview()

    assert all(c > 0 for c in pruned_counts.values())

    for table, count in counts.items():
        if count == 0:
            assert table not in pruned._cascade_restrictions, f"{table} had 0 rows but was not pruned"


def test_prune_idempotent(schema_simp_pop):
    """Pruning twice gives the same result."""
    diag = dj.Diagram(schema_simp_pop, context=LOCALS_SIMPLE)
    restricted = diag.restrict(A & "id_a=0")
    pruned_once = restricted.prune()
    pruned_twice = pruned_once.prune()

    assert pruned_once.nodes_to_show == pruned_twice.nodes_to_show
    assert set(pruned_once._restrict_conditions) == set(pruned_twice._restrict_conditions)


def test_prune_then_restrict(schema_simp_pop):
    """Restrict can be called after prune."""
    diag = dj.Diagram(schema_simp_pop, context=LOCALS_SIMPLE)
    pruned = diag.restrict(A & "id_a < 5").prune()
    # Restrict again on the same seed table with a tighter condition
    further = pruned.restrict(A & "id_a=0")

    # Should not raise; further restriction should narrow results
    counts = further.preview()
    assert all(c >= 0 for c in counts.values())
    # Tighter restriction should produce fewer or equal rows
    pruned_counts = pruned.preview()
    for table in counts:
        assert counts[table] <= pruned_counts.get(table, 0)
