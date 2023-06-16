# decompyle3 version 3.9.0
# Python bytecode version base 3.7.0 (3394)
# Decompiled from: Python 3.7.17 (default, Jun 13 2023, 16:22:33) 
# [GCC 10.2.1 20210110]
# Embedded file name: /workspaces/datajoint-python/tests/test_erd.py
# Compiled at: 2023-02-18 21:06:56
# Size of source mod 2**32: 2358 bytes
import builtins as @py_builtins
import _pytest.assertion.rewrite as @pytest_ar
import datajoint as dj
from . import connection_root, connection_test
from schemas.simple import schema, A, B, E, D, L, OutfitLaunch

def test_decorator(schema, A, B, E):
    @py_assert3 = dj.Lookup
    @py_assert5 = issubclass(A, @py_assert3)
    if not @py_assert5:
        @py_format7 = 'assert %(py6)s\n{%(py6)s = %(py0)s(%(py1)s, %(py4)s\n{%(py4)s = %(py2)s.Lookup\n})\n}' % {'py0':@pytest_ar._saferepr(issubclass) if 'issubclass' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(issubclass) else 'issubclass',  'py1':@pytest_ar._saferepr(A) if 'A' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(A) else 'A',  'py2':@pytest_ar._saferepr(dj) if 'dj' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(dj) else 'dj',  'py4':@pytest_ar._saferepr(@py_assert3),  'py6':@pytest_ar._saferepr(@py_assert5)}
        raise AssertionError(@pytest_ar._format_explanation(@py_format7))
    @py_assert3 = @py_assert5 = None
    @py_assert3 = dj.Part
    @py_assert5 = issubclass(A, @py_assert3)
    @py_assert7 = not @py_assert5
    if not @py_assert7:
        @py_format8 = 'assert not %(py6)s\n{%(py6)s = %(py0)s(%(py1)s, %(py4)s\n{%(py4)s = %(py2)s.Part\n})\n}' % {'py0':@pytest_ar._saferepr(issubclass) if 'issubclass' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(issubclass) else 'issubclass',  'py1':@pytest_ar._saferepr(A) if 'A' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(A) else 'A',  'py2':@pytest_ar._saferepr(dj) if 'dj' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(dj) else 'dj',  'py4':@pytest_ar._saferepr(@py_assert3),  'py6':@pytest_ar._saferepr(@py_assert5)}
        raise AssertionError(@pytest_ar._format_explanation(@py_format8))
    @py_assert3 = @py_assert5 = @py_assert7 = None
    @py_assert1 = B.database
    @py_assert5 = schema.database
    @py_assert3 = @py_assert1 == @py_assert5
    if not @py_assert3:
        @py_format7 = @pytest_ar._call_reprcompare(('==', ), (@py_assert3,), ('%(py2)s\n{%(py2)s = %(py0)s.database\n} == %(py6)s\n{%(py6)s = %(py4)s.database\n}', ), (@py_assert1, @py_assert5)) % {'py0':@pytest_ar._saferepr(B) if 'B' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(B) else 'B',  'py2':@pytest_ar._saferepr(@py_assert1),  'py4':@pytest_ar._saferepr(schema) if 'schema' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(schema) else 'schema',  'py6':@pytest_ar._saferepr(@py_assert5)}
        @py_format9 = 'assert %(py8)s' % {'py8': @py_format7}
        raise AssertionError(@pytest_ar._format_explanation(@py_format9))
    @py_assert1 = @py_assert3 = @py_assert5 = None
    @py_assert2 = B.C
    @py_assert5 = dj.Part
    @py_assert7 = issubclass(@py_assert2, @py_assert5)
    if not @py_assert7:
        @py_format9 = 'assert %(py8)s\n{%(py8)s = %(py0)s(%(py3)s\n{%(py3)s = %(py1)s.C\n}, %(py6)s\n{%(py6)s = %(py4)s.Part\n})\n}' % {'py0':@pytest_ar._saferepr(issubclass) if 'issubclass' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(issubclass) else 'issubclass',  'py1':@pytest_ar._saferepr(B) if 'B' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(B) else 'B',  'py3':@pytest_ar._saferepr(@py_assert2),  'py4':@pytest_ar._saferepr(dj) if 'dj' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(dj) else 'dj',  'py6':@pytest_ar._saferepr(@py_assert5),  'py8':@pytest_ar._saferepr(@py_assert7)}
        raise AssertionError(@pytest_ar._format_explanation(@py_format9))
    @py_assert2 = @py_assert5 = @py_assert7 = None
    @py_assert1 = B.C
    @py_assert3 = @py_assert1.database
    @py_assert7 = schema.database
    @py_assert5 = @py_assert3 == @py_assert7
    if not @py_assert5:
        @py_format9 = @pytest_ar._call_reprcompare(('==', ), (@py_assert5,), ('%(py4)s\n{%(py4)s = %(py2)s\n{%(py2)s = %(py0)s.C\n}.database\n} == %(py8)s\n{%(py8)s = %(py6)s.database\n}', ), (@py_assert3, @py_assert7)) % {'py0':@pytest_ar._saferepr(B) if 'B' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(B) else 'B',  'py2':@pytest_ar._saferepr(@py_assert1),  'py4':@pytest_ar._saferepr(@py_assert3),  'py6':@pytest_ar._saferepr(schema) if 'schema' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(schema) else 'schema',  'py8':@pytest_ar._saferepr(@py_assert7)}
        @py_format11 = 'assert %(py10)s' % {'py10': @py_format9}
        raise AssertionError(@pytest_ar._format_explanation(@py_format11))
    @py_assert1 = @py_assert3 = @py_assert5 = @py_assert7 = None
    @py_assert1 = []
    @py_assert3 = B.C
    @py_assert5 = @py_assert3.master
    @py_assert7 = @py_assert5 is B
    @py_assert0 = @py_assert7
    if @py_assert7:
        @py_assert13 = E.F
        @py_assert15 = @py_assert13.master
        @py_assert17 = @py_assert15 is E
        @py_assert0 = @py_assert17
    if not @py_assert0:
        @py_format9 = @pytest_ar._call_reprcompare(('is', ), (@py_assert7,), ('%(py6)s\n{%(py6)s = %(py4)s\n{%(py4)s = %(py2)s.C\n}.master\n} is %(py8)s', ), (@py_assert5, B)) % {'py2':@pytest_ar._saferepr(B) if 'B' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(B) else 'B',  'py4':@pytest_ar._saferepr(@py_assert3),  'py6':@pytest_ar._saferepr(@py_assert5),  'py8':@pytest_ar._saferepr(B) if 'B' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(B) else 'B'}
        @py_format11 = '%(py10)s' % {'py10': @py_format9}
        @py_assert1.append(@py_format11)
        if @py_assert7:
            @py_format19 = @pytest_ar._call_reprcompare(('is', ), (@py_assert17,), ('%(py16)s\n{%(py16)s = %(py14)s\n{%(py14)s = %(py12)s.F\n}.master\n} is %(py18)s', ), (@py_assert15, E)) % {'py12':@pytest_ar._saferepr(E) if 'E' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(E) else 'E',  'py14':@pytest_ar._saferepr(@py_assert13),  'py16':@pytest_ar._saferepr(@py_assert15),  'py18':@pytest_ar._saferepr(E) if 'E' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(E) else 'E'}
            @py_format21 = '%(py20)s' % {'py20': @py_format19}
            @py_assert1.append(@py_format21)
        @py_format22 = @pytest_ar._format_boolop(@py_assert1, 0) % {}
        @py_format24 = 'assert %(py23)s' % {'py23': @py_format22}
        raise AssertionError(@pytest_ar._format_explanation(@py_format24))
    @py_assert0 = @py_assert1 = @py_assert3 = @py_assert5 = @py_assert7 = @py_assert13 = @py_assert15 = @py_assert17 = None


def test_dependencies(schema, A, B, D, E, L):
    deps = schema.connection.dependencies
    deps.load()
    @py_assert1 = (cls.full_table_name in deps for cls in (A, B, B.C, D, E, E.F, L))
    @py_assert3 = all(@py_assert1)
    if not @py_assert3:
        @py_format5 = 'assert %(py4)s\n{%(py4)s = %(py0)s(%(py2)s)\n}' % {'py0':@pytest_ar._saferepr(all) if 'all' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(all) else 'all',  'py2':@pytest_ar._saferepr(@py_assert1),  'py4':@pytest_ar._saferepr(@py_assert3)}
        raise AssertionError(@pytest_ar._format_explanation(@py_format5))
    @py_assert1 = @py_assert3 = None
    @py_assert2 = A()
    @py_assert4 = @py_assert2.children
    @py_assert6 = @py_assert4()
    @py_assert8 = set(@py_assert6)
    @py_assert12 = [
     B.full_table_name, D.full_table_name]
    @py_assert14 = set(@py_assert12)
    @py_assert10 = @py_assert8 == @py_assert14
    if not @py_assert10:
        @py_format16 = @pytest_ar._call_reprcompare(('==', ), (@py_assert10,), ('%(py9)s\n{%(py9)s = %(py0)s(%(py7)s\n{%(py7)s = %(py5)s\n{%(py5)s = %(py3)s\n{%(py3)s = %(py1)s()\n}.children\n}()\n})\n} == %(py15)s\n{%(py15)s = %(py11)s(%(py13)s)\n}', ), (@py_assert8, @py_assert14)) % {'py0':@pytest_ar._saferepr(set) if 'set' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(set) else 'set',  'py1':@pytest_ar._saferepr(A) if 'A' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(A) else 'A',  'py3':@pytest_ar._saferepr(@py_assert2),  'py5':@pytest_ar._saferepr(@py_assert4),  'py7':@pytest_ar._saferepr(@py_assert6),  'py9':@pytest_ar._saferepr(@py_assert8),  'py11':@pytest_ar._saferepr(set) if 'set' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(set) else 'set',  'py13':@pytest_ar._saferepr(@py_assert12),  'py15':@pytest_ar._saferepr(@py_assert14)}
        @py_format18 = 'assert %(py17)s' % {'py17': @py_format16}
        raise AssertionError(@pytest_ar._format_explanation(@py_format18))
    @py_assert2 = @py_assert4 = @py_assert6 = @py_assert8 = @py_assert10 = @py_assert12 = @py_assert14 = None
    @py_assert2 = D()
    @py_assert4 = @py_assert2.parents
    @py_assert6 = True
    @py_assert8 = @py_assert4(primary=@py_assert6)
    @py_assert10 = set(@py_assert8)
    @py_assert14 = [
     A.full_table_name]
    @py_assert16 = set(@py_assert14)
    @py_assert12 = @py_assert10 == @py_assert16
    if not @py_assert12:
        @py_format18 = @pytest_ar._call_reprcompare(('==', ), (@py_assert12,), ('%(py11)s\n{%(py11)s = %(py0)s(%(py9)s\n{%(py9)s = %(py5)s\n{%(py5)s = %(py3)s\n{%(py3)s = %(py1)s()\n}.parents\n}(primary=%(py7)s)\n})\n} == %(py17)s\n{%(py17)s = %(py13)s(%(py15)s)\n}', ), (@py_assert10, @py_assert16)) % {'py0':@pytest_ar._saferepr(set) if 'set' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(set) else 'set',  'py1':@pytest_ar._saferepr(D) if 'D' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(D) else 'D',  'py3':@pytest_ar._saferepr(@py_assert2),  'py5':@pytest_ar._saferepr(@py_assert4),  'py7':@pytest_ar._saferepr(@py_assert6),  'py9':@pytest_ar._saferepr(@py_assert8),  'py11':@pytest_ar._saferepr(@py_assert10),  'py13':@pytest_ar._saferepr(set) if 'set' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(set) else 'set',  'py15':@pytest_ar._saferepr(@py_assert14),  'py17':@pytest_ar._saferepr(@py_assert16)}
        @py_format20 = 'assert %(py19)s' % {'py19': @py_format18}
        raise AssertionError(@pytest_ar._format_explanation(@py_format20))
    @py_assert2 = @py_assert4 = @py_assert6 = @py_assert8 = @py_assert10 = @py_assert12 = @py_assert14 = @py_assert16 = None
    @py_assert2 = D()
    @py_assert4 = @py_assert2.parents
    @py_assert6 = False
    @py_assert8 = @py_assert4(primary=@py_assert6)
    @py_assert10 = set(@py_assert8)
    @py_assert14 = [
     L.full_table_name]
    @py_assert16 = set(@py_assert14)
    @py_assert12 = @py_assert10 == @py_assert16
    if not @py_assert12:
        @py_format18 = @pytest_ar._call_reprcompare(('==', ), (@py_assert12,), ('%(py11)s\n{%(py11)s = %(py0)s(%(py9)s\n{%(py9)s = %(py5)s\n{%(py5)s = %(py3)s\n{%(py3)s = %(py1)s()\n}.parents\n}(primary=%(py7)s)\n})\n} == %(py17)s\n{%(py17)s = %(py13)s(%(py15)s)\n}', ), (@py_assert10, @py_assert16)) % {'py0':@pytest_ar._saferepr(set) if 'set' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(set) else 'set',  'py1':@pytest_ar._saferepr(D) if 'D' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(D) else 'D',  'py3':@pytest_ar._saferepr(@py_assert2),  'py5':@pytest_ar._saferepr(@py_assert4),  'py7':@pytest_ar._saferepr(@py_assert6),  'py9':@pytest_ar._saferepr(@py_assert8),  'py11':@pytest_ar._saferepr(@py_assert10),  'py13':@pytest_ar._saferepr(set) if 'set' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(set) else 'set',  'py15':@pytest_ar._saferepr(@py_assert14),  'py17':@pytest_ar._saferepr(@py_assert16)}
        @py_format20 = 'assert %(py19)s' % {'py19': @py_format18}
        raise AssertionError(@pytest_ar._format_explanation(@py_format20))
    @py_assert2 = @py_assert4 = @py_assert6 = @py_assert8 = @py_assert10 = @py_assert12 = @py_assert14 = @py_assert16 = None
    @py_assert2 = deps.descendants
    @py_assert5 = L.full_table_name
    @py_assert7 = @py_assert2(@py_assert5)
    @py_assert9 = set(@py_assert7)
    @py_assert11 = @py_assert9.issubset
    @py_assert13 = (cls.full_table_name for cls in (L, D, E, E.F))
    @py_assert15 = @py_assert11(@py_assert13)
    if not @py_assert15:
        @py_format17 = 'assert %(py16)s\n{%(py16)s = %(py12)s\n{%(py12)s = %(py10)s\n{%(py10)s = %(py0)s(%(py8)s\n{%(py8)s = %(py3)s\n{%(py3)s = %(py1)s.descendants\n}(%(py6)s\n{%(py6)s = %(py4)s.full_table_name\n})\n})\n}.issubset\n}(%(py14)s)\n}' % {'py0':@pytest_ar._saferepr(set) if 'set' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(set) else 'set',  'py1':@pytest_ar._saferepr(deps) if 'deps' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(deps) else 'deps',  'py3':@pytest_ar._saferepr(@py_assert2),  'py4':@pytest_ar._saferepr(L) if 'L' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(L) else 'L',  'py6':@pytest_ar._saferepr(@py_assert5),  'py8':@pytest_ar._saferepr(@py_assert7),  'py10':@pytest_ar._saferepr(@py_assert9),  'py12':@pytest_ar._saferepr(@py_assert11),  'py14':@pytest_ar._saferepr(@py_assert13),  'py16':@pytest_ar._saferepr(@py_assert15)}
        raise AssertionError(@pytest_ar._format_explanation(@py_format17))
    @py_assert2 = @py_assert5 = @py_assert7 = @py_assert9 = @py_assert11 = @py_assert13 = @py_assert15 = None


def test_erd(schema, A, B, D, E, L):
    @py_assert1 = dj.diagram
    @py_assert3 = @py_assert1.diagram_active
    if not @py_assert3:
        @py_format5 = (@pytest_ar._format_assertmsg('Failed to import networkx and pydot') + '\n>assert %(py4)s\n{%(py4)s = %(py2)s\n{%(py2)s = %(py0)s.diagram\n}.diagram_active\n}') % {'py0':@pytest_ar._saferepr(dj) if 'dj' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(dj) else 'dj',  'py2':@pytest_ar._saferepr(@py_assert1),  'py4':@pytest_ar._saferepr(@py_assert3)}
        raise AssertionError(@pytest_ar._format_explanation(@py_format5))
    @py_assert1 = @py_assert3 = None
    erd = dj.ERD(schema, context=(locals()))
    graph = erd._make_graph()
    @py_assert1 = (cls.__name__ for cls in (A, B, D, E, L))
    @py_assert3 = set(@py_assert1)
    @py_assert5 = @py_assert3.issubset
    @py_assert8 = graph.nodes
    @py_assert10 = @py_assert8()
    @py_assert12 = @py_assert5(@py_assert10)
    if not @py_assert12:
        @py_format14 = 'assert %(py13)s\n{%(py13)s = %(py6)s\n{%(py6)s = %(py4)s\n{%(py4)s = %(py0)s(%(py2)s)\n}.issubset\n}(%(py11)s\n{%(py11)s = %(py9)s\n{%(py9)s = %(py7)s.nodes\n}()\n})\n}' % {'py0':@pytest_ar._saferepr(set) if 'set' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(set) else 'set',  'py2':@pytest_ar._saferepr(@py_assert1),  'py4':@pytest_ar._saferepr(@py_assert3),  'py6':@pytest_ar._saferepr(@py_assert5),  'py7':@pytest_ar._saferepr(graph) if 'graph' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(graph) else 'graph',  'py9':@pytest_ar._saferepr(@py_assert8),  'py11':@pytest_ar._saferepr(@py_assert10),  'py13':@pytest_ar._saferepr(@py_assert12)}
        raise AssertionError(@pytest_ar._format_explanation(@py_format14))
    @py_assert1 = @py_assert3 = @py_assert5 = @py_assert8 = @py_assert10 = @py_assert12 = None


def test_erd_algebra(A, B, D, E, L):
    erd0 = dj.ERD(B)
    erd1 = erd0 + 3
    erd2 = dj.Di(E) - 3
    erd3 = erd1 * erd2
    erd4 = (erd0 + E).add_parts() - B - E
    @py_assert1 = erd0.nodes_to_show
    @py_assert5 = (cls.full_table_name for cls in [B])
    @py_assert7 = set(@py_assert5)
    @py_assert3 = @py_assert1 == @py_assert7
    if not @py_assert3:
        @py_format9 = @pytest_ar._call_reprcompare(('==', ), (@py_assert3,), ('%(py2)s\n{%(py2)s = %(py0)s.nodes_to_show\n} == %(py8)s\n{%(py8)s = %(py4)s(%(py6)s)\n}', ), (@py_assert1, @py_assert7)) % {'py0':@pytest_ar._saferepr(erd0) if 'erd0' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(erd0) else 'erd0',  'py2':@pytest_ar._saferepr(@py_assert1),  'py4':@pytest_ar._saferepr(set) if 'set' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(set) else 'set',  'py6':@pytest_ar._saferepr(@py_assert5),  'py8':@pytest_ar._saferepr(@py_assert7)}
        @py_format11 = 'assert %(py10)s' % {'py10': @py_format9}
        raise AssertionError(@pytest_ar._format_explanation(@py_format11))
    @py_assert1 = @py_assert3 = @py_assert5 = @py_assert7 = None
    @py_assert1 = erd1.nodes_to_show
    @py_assert5 = (cls.full_table_name for cls in (B, B.C, E, E.F))
    @py_assert7 = set(@py_assert5)
    @py_assert3 = @py_assert1 == @py_assert7
    if not @py_assert3:
        @py_format9 = @pytest_ar._call_reprcompare(('==', ), (@py_assert3,), ('%(py2)s\n{%(py2)s = %(py0)s.nodes_to_show\n} == %(py8)s\n{%(py8)s = %(py4)s(%(py6)s)\n}', ), (@py_assert1, @py_assert7)) % {'py0':@pytest_ar._saferepr(erd1) if 'erd1' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(erd1) else 'erd1',  'py2':@pytest_ar._saferepr(@py_assert1),  'py4':@pytest_ar._saferepr(set) if 'set' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(set) else 'set',  'py6':@pytest_ar._saferepr(@py_assert5),  'py8':@pytest_ar._saferepr(@py_assert7)}
        @py_format11 = 'assert %(py10)s' % {'py10': @py_format9}
        raise AssertionError(@pytest_ar._format_explanation(@py_format11))
    @py_assert1 = @py_assert3 = @py_assert5 = @py_assert7 = None
    @py_assert1 = erd2.nodes_to_show
    @py_assert5 = (cls.full_table_name for cls in (A, B, D, E, L))
    @py_assert7 = set(@py_assert5)
    @py_assert3 = @py_assert1 == @py_assert7
    if not @py_assert3:
        @py_format9 = @pytest_ar._call_reprcompare(('==', ), (@py_assert3,), ('%(py2)s\n{%(py2)s = %(py0)s.nodes_to_show\n} == %(py8)s\n{%(py8)s = %(py4)s(%(py6)s)\n}', ), (@py_assert1, @py_assert7)) % {'py0':@pytest_ar._saferepr(erd2) if 'erd2' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(erd2) else 'erd2',  'py2':@pytest_ar._saferepr(@py_assert1),  'py4':@pytest_ar._saferepr(set) if 'set' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(set) else 'set',  'py6':@pytest_ar._saferepr(@py_assert5),  'py8':@pytest_ar._saferepr(@py_assert7)}
        @py_format11 = 'assert %(py10)s' % {'py10': @py_format9}
        raise AssertionError(@pytest_ar._format_explanation(@py_format11))
    @py_assert1 = @py_assert3 = @py_assert5 = @py_assert7 = None
    @py_assert1 = erd3.nodes_to_show
    @py_assert5 = (cls.full_table_name for cls in (B, E))
    @py_assert7 = set(@py_assert5)
    @py_assert3 = @py_assert1 == @py_assert7
    if not @py_assert3:
        @py_format9 = @pytest_ar._call_reprcompare(('==', ), (@py_assert3,), ('%(py2)s\n{%(py2)s = %(py0)s.nodes_to_show\n} == %(py8)s\n{%(py8)s = %(py4)s(%(py6)s)\n}', ), (@py_assert1, @py_assert7)) % {'py0':@pytest_ar._saferepr(erd3) if 'erd3' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(erd3) else 'erd3',  'py2':@pytest_ar._saferepr(@py_assert1),  'py4':@pytest_ar._saferepr(set) if 'set' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(set) else 'set',  'py6':@pytest_ar._saferepr(@py_assert5),  'py8':@pytest_ar._saferepr(@py_assert7)}
        @py_format11 = 'assert %(py10)s' % {'py10': @py_format9}
        raise AssertionError(@pytest_ar._format_explanation(@py_format11))
    @py_assert1 = @py_assert3 = @py_assert5 = @py_assert7 = None
    @py_assert1 = erd4.nodes_to_show
    @py_assert5 = (cls.full_table_name for cls in (B.C, E.F))
    @py_assert7 = set(@py_assert5)
    @py_assert3 = @py_assert1 == @py_assert7
    if not @py_assert3:
        @py_format9 = @pytest_ar._call_reprcompare(('==', ), (@py_assert3,), ('%(py2)s\n{%(py2)s = %(py0)s.nodes_to_show\n} == %(py8)s\n{%(py8)s = %(py4)s(%(py6)s)\n}', ), (@py_assert1, @py_assert7)) % {'py0':@pytest_ar._saferepr(erd4) if 'erd4' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(erd4) else 'erd4',  'py2':@pytest_ar._saferepr(@py_assert1),  'py4':@pytest_ar._saferepr(set) if 'set' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(set) else 'set',  'py6':@pytest_ar._saferepr(@py_assert5),  'py8':@pytest_ar._saferepr(@py_assert7)}
        @py_format11 = 'assert %(py10)s' % {'py10': @py_format9}
        raise AssertionError(@pytest_ar._format_explanation(@py_format11))
    @py_assert1 = @py_assert3 = @py_assert5 = @py_assert7 = None


def test_repr_svg(schema):
    erd = dj.ERD(schema, context=(locals()))
    svg = erd._repr_svg_()
    @py_assert1 = []
    @py_assert3 = svg.startswith
    @py_assert5 = '<svg'
    @py_assert7 = @py_assert3(@py_assert5)
    @py_assert0 = @py_assert7
    if @py_assert7:
        @py_assert11 = svg.endswith
        @py_assert13 = 'svg>'
        @py_assert15 = @py_assert11(@py_assert13)
        @py_assert0 = @py_assert15
    if not @py_assert0:
        @py_format9 = '%(py8)s\n{%(py8)s = %(py4)s\n{%(py4)s = %(py2)s.startswith\n}(%(py6)s)\n}' % {'py2':@pytest_ar._saferepr(svg) if 'svg' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(svg) else 'svg',  'py4':@pytest_ar._saferepr(@py_assert3),  'py6':@pytest_ar._saferepr(@py_assert5),  'py8':@pytest_ar._saferepr(@py_assert7)}
        @py_assert1.append(@py_format9)
        if @py_assert7:
            @py_format17 = '%(py16)s\n{%(py16)s = %(py12)s\n{%(py12)s = %(py10)s.endswith\n}(%(py14)s)\n}' % {'py10':@pytest_ar._saferepr(svg) if 'svg' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(svg) else 'svg',  'py12':@pytest_ar._saferepr(@py_assert11),  'py14':@pytest_ar._saferepr(@py_assert13),  'py16':@pytest_ar._saferepr(@py_assert15)}
            @py_assert1.append(@py_format17)
        @py_format18 = @pytest_ar._format_boolop(@py_assert1, 0) % {}
        @py_format20 = 'assert %(py19)s' % {'py19': @py_format18}
        raise AssertionError(@pytest_ar._format_explanation(@py_format20))
    @py_assert0 = @py_assert1 = @py_assert3 = @py_assert5 = @py_assert7 = @py_assert11 = @py_assert13 = @py_assert15 = None


def test_make_image(schema):
    erd = dj.ERD(schema, context=(locals()))
    img = erd.make_image()
    @py_assert1 = []
    @py_assert3 = img.ndim
    @py_assert6 = 3
    @py_assert5 = @py_assert3 == @py_assert6
    @py_assert0 = @py_assert5
    if @py_assert5:
        @py_assert11 = img.shape[2]
        @py_assert14 = (3, 4)
        @py_assert13 = @py_assert11 in @py_assert14
        @py_assert0 = @py_assert13
    if not @py_assert0:
        @py_format8 = @pytest_ar._call_reprcompare(('==', ), (@py_assert5,), ('%(py4)s\n{%(py4)s = %(py2)s.ndim\n} == %(py7)s', ), (@py_assert3, @py_assert6)) % {'py2':@pytest_ar._saferepr(img) if 'img' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(img) else 'img',  'py4':@pytest_ar._saferepr(@py_assert3),  'py7':@pytest_ar._saferepr(@py_assert6)}
        @py_format10 = '%(py9)s' % {'py9': @py_format8}
        @py_assert1.append(@py_format10)
        if @py_assert5:
            @py_format16 = @pytest_ar._call_reprcompare(('in', ), (@py_assert13,), ('%(py12)s in %(py15)s', ), (@py_assert11, @py_assert14)) % {'py12':@pytest_ar._saferepr(@py_assert11),  'py15':@pytest_ar._saferepr(@py_assert14)}
            @py_format18 = '%(py17)s' % {'py17': @py_format16}
            @py_assert1.append(@py_format18)
        @py_format19 = @pytest_ar._format_boolop(@py_assert1, 0) % {}
        @py_format21 = 'assert %(py20)s' % {'py20': @py_format19}
        raise AssertionError(@pytest_ar._format_explanation(@py_format21))
    @py_assert0 = @py_assert1 = @py_assert3 = @py_assert5 = @py_assert6 = @py_assert11 = @py_assert13 = @py_assert14 = None


def test_part_table_parsing(schema, OutfitLaunch):
    erd = dj.Di(schema)
    graph = erd._make_graph()
    @py_assert0 = 'OutfitLaunch'
    @py_assert4 = graph.nodes
    @py_assert6 = @py_assert4()
    @py_assert2 = @py_assert0 in @py_assert6
    if not @py_assert2:
        @py_format8 = @pytest_ar._call_reprcompare(('in', ), (@py_assert2,), ('%(py1)s in %(py7)s\n{%(py7)s = %(py5)s\n{%(py5)s = %(py3)s.nodes\n}()\n}', ), (@py_assert0, @py_assert6)) % {'py1':@pytest_ar._saferepr(@py_assert0),  'py3':@pytest_ar._saferepr(graph) if 'graph' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(graph) else 'graph',  'py5':@pytest_ar._saferepr(@py_assert4),  'py7':@pytest_ar._saferepr(@py_assert6)}
        @py_format10 = 'assert %(py9)s' % {'py9': @py_format8}
        raise AssertionError(@pytest_ar._format_explanation(@py_format10))
    @py_assert0 = @py_assert2 = @py_assert4 = @py_assert6 = None
    @py_assert0 = 'OutfitLaunch.OutfitPiece'
    @py_assert4 = graph.nodes
    @py_assert6 = @py_assert4()
    @py_assert2 = @py_assert0 in @py_assert6
    if not @py_assert2:
        @py_format8 = @pytest_ar._call_reprcompare(('in', ), (@py_assert2,), ('%(py1)s in %(py7)s\n{%(py7)s = %(py5)s\n{%(py5)s = %(py3)s.nodes\n}()\n}', ), (@py_assert0, @py_assert6)) % {'py1':@pytest_ar._saferepr(@py_assert0),  'py3':@pytest_ar._saferepr(graph) if 'graph' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(graph) else 'graph',  'py5':@pytest_ar._saferepr(@py_assert4),  'py7':@pytest_ar._saferepr(@py_assert6)}
        @py_format10 = 'assert %(py9)s' % {'py9': @py_format8}
        raise AssertionError(@pytest_ar._format_explanation(@py_format10))
    @py_assert0 = @py_assert2 = @py_assert4 = @py_assert6 = None