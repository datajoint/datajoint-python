from nose.tools import assert_false, assert_true
import datajoint as dj
from .schema_simple import A, B, D, E, L, schema


class TestERD:

    @staticmethod
    def setup():
        """
        class-level test setup. Executes before each test method.
        """

    @staticmethod
    def test_decorator():
        assert_true(issubclass(A, dj.BaseRelation))
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
        erd = dj.ERD(schema)

