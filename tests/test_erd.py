from nose.tools import assert_false, assert_true
import datajoint as dj
from .schema_simple import A, B, D, E, L, schema
from . import schema_advanced

namespace = locals()


class TestERD:

    @staticmethod
    def setup():
        """
        class-level test setup. Executes before each test method.
        """

    @staticmethod
    def test_decorator():
        assert_true(issubclass(A, dj.Lookup))
        assert_false(issubclass(A, dj.Part))
        assert_true(B.database == schema.database)
        assert_true(issubclass(B.C, dj.Part))
        assert_true(B.C.database == schema.database)
        assert_true(B.C.master is B and E.F.master is E)

    @staticmethod
    def test_dependencies():
        deps = schema.connection.dependencies
        assert_true(all(cls.full_table_name in deps for cls in (A, B, B.C, D, E, E.F, L)))
        assert_true(set(A().children()) == set([B.full_table_name, D.full_table_name]))
        assert_true(set(D().parents(primary=True)) == set([A.full_table_name]))
        assert_true(set(D().parents(primary=False)) == set([L.full_table_name]))
        assert_true(set(deps.descendants(L.full_table_name)).issubset(cls.full_table_name for cls in (L, D, E, E.F)))

    @staticmethod
    def test_erd():
        assert_true(dj.diagram.diagram_active, 'Failed to import networkx and pydot')
        erd = dj.ERD(schema, context=namespace)
        graph = erd._make_graph()
        assert_true(set(cls.__name__ for cls in (A, B, D, E, L)).issubset(graph.nodes()))

    @staticmethod
    def test_erd_algebra():
        erd0 = dj.ERD(B)
        erd1 = erd0 + 3
        erd2 = dj.Di(E) - 3
        erd3 = erd1 * erd2
        erd4 = (erd0 + E).add_parts() - B - E
        assert_true(erd0.nodes_to_show == set(cls.full_table_name for cls in [B]))
        assert_true(erd1.nodes_to_show == set(cls.full_table_name for cls in (B, B.C, E, E.F)))
        assert_true(erd2.nodes_to_show == set(cls.full_table_name for cls in (A, B, D, E, L)))
        assert_true(erd3.nodes_to_show == set(cls.full_table_name for cls in (B, E)))
        assert_true(erd4.nodes_to_show == set(cls.full_table_name for cls in (B.C, E.F)))

    @staticmethod
    def test_repr_svg():
        erd = dj.ERD(schema_advanced, context=namespace)
        svg = erd._repr_svg_()
        assert_true(svg.startswith('<svg') and svg.endswith('svg>'))

    @staticmethod
    def test_make_image():
        erd = dj.ERD(schema, context=namespace)
        img = erd.make_image()
        assert_true(img.ndim == 3 and img.shape[2] in (3, 4))



