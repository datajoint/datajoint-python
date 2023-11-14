# decompyle3 version 3.9.0
# Python bytecode version base 3.7.0 (3394)
# Decompiled from: Python 3.7.17 (default, Jun 13 2023, 16:22:33) 
# [GCC 10.2.1 20210110]
# Embedded file name: /workspaces/datajoint-python/tests/test_cascading_delete.py
# Compiled at: 2023-02-18 00:22:32
# Size of source mod 2**32: 4417 bytes
import builtins as @py_builtins
import _pytest.assertion.rewrite as @pytest_ar
import datajoint as dj
from datajoint import DataJointError
import random, pytest
from . import connection_root, connection_test
from schemas.simple import schema, A, B, D, E, L, Profile, Website
from schemas.default import ComplexChild, ComplexParent

def test_delete_tree(A, B, D, E, L):
    random.seed('cascade')
    B().populate()
    D().populate()
    E().populate()
    @py_assert0 = dj.config['safemode']
    @py_assert2 = not @py_assert0
    if not @py_assert2:
        @py_format3 = (@pytest_ar._format_assertmsg('safemode must be off for testing') + '\n>assert not %(py1)s') % {'py1': @pytest_ar._saferepr(@py_assert0)}
        raise AssertionError(@pytest_ar._format_explanation(@py_format3))
    @py_assert0 = @py_assert2 = None
    @py_assert1 = []
    @py_assert3 = L()
    @py_assert0 = @py_assert3
    if @py_assert3:
        @py_assert7 = A()
        @py_assert0 = @py_assert7
        if @py_assert7:
            @py_assert11 = B()
            @py_assert0 = @py_assert11
            if @py_assert11:
                @py_assert15 = B.C
                @py_assert17 = @py_assert15()
                @py_assert0 = @py_assert17
                if @py_assert17:
                    @py_assert21 = D()
                    @py_assert0 = @py_assert21
                    if @py_assert21:
                        @py_assert25 = E()
                        @py_assert0 = @py_assert25
                        if @py_assert25:
                            @py_assert29 = E.F
                            @py_assert31 = @py_assert29()
                            @py_assert0 = @py_assert31
    if not @py_assert0:
        @py_format5 = '%(py4)s\n{%(py4)s = %(py2)s()\n}' % {'py2':@pytest_ar._saferepr(L) if 'L' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(L) else 'L',  'py4':@pytest_ar._saferepr(@py_assert3)}
        @py_assert1.append(@py_format5)
        if @py_assert3:
            @py_format9 = '%(py8)s\n{%(py8)s = %(py6)s()\n}' % {'py6':@pytest_ar._saferepr(A) if 'A' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(A) else 'A',  'py8':@pytest_ar._saferepr(@py_assert7)}
            @py_assert1.append(@py_format9)
            if @py_assert7:
                @py_format13 = '%(py12)s\n{%(py12)s = %(py10)s()\n}' % {'py10':@pytest_ar._saferepr(B) if 'B' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(B) else 'B',  'py12':@pytest_ar._saferepr(@py_assert11)}
                @py_assert1.append(@py_format13)
                if @py_assert11:
                    @py_format19 = '%(py18)s\n{%(py18)s = %(py16)s\n{%(py16)s = %(py14)s.C\n}()\n}' % {'py14':@pytest_ar._saferepr(B) if 'B' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(B) else 'B',  'py16':@pytest_ar._saferepr(@py_assert15),  'py18':@pytest_ar._saferepr(@py_assert17)}
                    @py_assert1.append(@py_format19)
                    if @py_assert17:
                        @py_format23 = '%(py22)s\n{%(py22)s = %(py20)s()\n}' % {'py20':@pytest_ar._saferepr(D) if 'D' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(D) else 'D',  'py22':@pytest_ar._saferepr(@py_assert21)}
                        @py_assert1.append(@py_format23)
                        if @py_assert21:
                            @py_format27 = '%(py26)s\n{%(py26)s = %(py24)s()\n}' % {'py24':@pytest_ar._saferepr(E) if 'E' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(E) else 'E',  'py26':@pytest_ar._saferepr(@py_assert25)}
                            @py_assert1.append(@py_format27)
                            if @py_assert25:
                                @py_format33 = '%(py32)s\n{%(py32)s = %(py30)s\n{%(py30)s = %(py28)s.F\n}()\n}' % {'py28':@pytest_ar._saferepr(E) if 'E' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(E) else 'E',  'py30':@pytest_ar._saferepr(@py_assert29),  'py32':@pytest_ar._saferepr(@py_assert31)}
                                @py_assert1.append(@py_format33)
        @py_format34 = @pytest_ar._format_boolop(@py_assert1, 0) % {}
        @py_format36 = (@pytest_ar._format_assertmsg('schema is not populated') + '\n>assert %(py35)s') % {'py35': @py_format34}
        raise AssertionError(@pytest_ar._format_explanation(@py_format36))
    @py_assert0 = @py_assert1 = @py_assert3 = @py_assert7 = @py_assert11 = @py_assert15 = @py_assert17 = @py_assert21 = @py_assert25 = @py_assert29 = @py_assert31 = None
    A().delete()
    @py_assert1 = []
    @py_assert3 = A()
    @py_assert0 = @py_assert3
    if not @py_assert3:
        @py_assert7 = B()
        @py_assert0 = @py_assert7
        if not @py_assert7:
            @py_assert11 = B.C
            @py_assert13 = @py_assert11()
            @py_assert0 = @py_assert13
            if not @py_assert13:
                @py_assert17 = D()
                @py_assert0 = @py_assert17
                if not @py_assert17:
                    @py_assert21 = E()
                    @py_assert0 = @py_assert21
                    if not @py_assert21:
                        @py_assert25 = E.F
                        @py_assert27 = @py_assert25()
                        @py_assert0 = @py_assert27
    @py_assert32 = not @py_assert0
    if not @py_assert32:
        @py_format5 = '%(py4)s\n{%(py4)s = %(py2)s()\n}' % {'py2':@pytest_ar._saferepr(A) if 'A' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(A) else 'A',  'py4':@pytest_ar._saferepr(@py_assert3)}
        @py_assert1.append(@py_format5)
        if not @py_assert3:
            @py_format9 = '%(py8)s\n{%(py8)s = %(py6)s()\n}' % {'py6':@pytest_ar._saferepr(B) if 'B' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(B) else 'B',  'py8':@pytest_ar._saferepr(@py_assert7)}
            @py_assert1.append(@py_format9)
            if not @py_assert7:
                @py_format15 = '%(py14)s\n{%(py14)s = %(py12)s\n{%(py12)s = %(py10)s.C\n}()\n}' % {'py10':@pytest_ar._saferepr(B) if 'B' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(B) else 'B',  'py12':@pytest_ar._saferepr(@py_assert11),  'py14':@pytest_ar._saferepr(@py_assert13)}
                @py_assert1.append(@py_format15)
                if not @py_assert13:
                    @py_format19 = '%(py18)s\n{%(py18)s = %(py16)s()\n}' % {'py16':@pytest_ar._saferepr(D) if 'D' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(D) else 'D',  'py18':@pytest_ar._saferepr(@py_assert17)}
                    @py_assert1.append(@py_format19)
                    if not @py_assert17:
                        @py_format23 = '%(py22)s\n{%(py22)s = %(py20)s()\n}' % {'py20':@pytest_ar._saferepr(E) if 'E' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(E) else 'E',  'py22':@pytest_ar._saferepr(@py_assert21)}
                        @py_assert1.append(@py_format23)
                        if not @py_assert21:
                            @py_format29 = '%(py28)s\n{%(py28)s = %(py26)s\n{%(py26)s = %(py24)s.F\n}()\n}' % {'py24':@pytest_ar._saferepr(E) if 'E' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(E) else 'E',  'py26':@pytest_ar._saferepr(@py_assert25),  'py28':@pytest_ar._saferepr(@py_assert27)}
                            @py_assert1.append(@py_format29)
        @py_format30 = @pytest_ar._format_boolop(@py_assert1, 1) % {}
        @py_format33 = (@pytest_ar._format_assertmsg('incomplete delete') + '\n>assert not %(py31)s') % {'py31': @py_format30}
        raise AssertionError(@pytest_ar._format_explanation(@py_format33))
    @py_assert0 = @py_assert1 = @py_assert3 = @py_assert7 = @py_assert11 = @py_assert13 = @py_assert17 = @py_assert21 = @py_assert25 = @py_assert27 = @py_assert32 = None


def test_stepwise_delete(L, A, B):
    random.seed('cascade')
    B().populate()
    @py_assert0 = dj.config['safemode']
    @py_assert2 = not @py_assert0
    if not @py_assert2:
        @py_format3 = (@pytest_ar._format_assertmsg('safemode must be off for testing') + '\n>assert not %(py1)s') % {'py1': @pytest_ar._saferepr(@py_assert0)}
        raise AssertionError(@pytest_ar._format_explanation(@py_format3))
    @py_assert0 = @py_assert2 = None
    @py_assert1 = []
    @py_assert3 = L()
    @py_assert0 = @py_assert3
    if @py_assert3:
        @py_assert7 = A()
        @py_assert0 = @py_assert7
        if @py_assert7:
            @py_assert11 = B()
            @py_assert0 = @py_assert11
            if @py_assert11:
                @py_assert15 = B.C
                @py_assert17 = @py_assert15()
                @py_assert0 = @py_assert17
    if not @py_assert0:
        @py_format5 = '%(py4)s\n{%(py4)s = %(py2)s()\n}' % {'py2':@pytest_ar._saferepr(L) if 'L' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(L) else 'L',  'py4':@pytest_ar._saferepr(@py_assert3)}
        @py_assert1.append(@py_format5)
        if @py_assert3:
            @py_format9 = '%(py8)s\n{%(py8)s = %(py6)s()\n}' % {'py6':@pytest_ar._saferepr(A) if 'A' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(A) else 'A',  'py8':@pytest_ar._saferepr(@py_assert7)}
            @py_assert1.append(@py_format9)
            if @py_assert7:
                @py_format13 = '%(py12)s\n{%(py12)s = %(py10)s()\n}' % {'py10':@pytest_ar._saferepr(B) if 'B' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(B) else 'B',  'py12':@pytest_ar._saferepr(@py_assert11)}
                @py_assert1.append(@py_format13)
                if @py_assert11:
                    @py_format19 = '%(py18)s\n{%(py18)s = %(py16)s\n{%(py16)s = %(py14)s.C\n}()\n}' % {'py14':@pytest_ar._saferepr(B) if 'B' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(B) else 'B',  'py16':@pytest_ar._saferepr(@py_assert15),  'py18':@pytest_ar._saferepr(@py_assert17)}
                    @py_assert1.append(@py_format19)
        @py_format20 = @pytest_ar._format_boolop(@py_assert1, 0) % {}
        @py_format22 = (@pytest_ar._format_assertmsg('schema population failed') + '\n>assert %(py21)s') % {'py21': @py_format20}
        raise AssertionError(@pytest_ar._format_explanation(@py_format22))
    @py_assert0 = @py_assert1 = @py_assert3 = @py_assert7 = @py_assert11 = @py_assert15 = @py_assert17 = None
    B.C().delete(force=True)
    @py_assert1 = B.C
    @py_assert3 = @py_assert1()
    @py_assert5 = not @py_assert3
    if not @py_assert5:
        @py_format6 = (@pytest_ar._format_assertmsg('failed to delete child tables') + '\n>assert not %(py4)s\n{%(py4)s = %(py2)s\n{%(py2)s = %(py0)s.C\n}()\n}') % {'py0':@pytest_ar._saferepr(B) if 'B' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(B) else 'B',  'py2':@pytest_ar._saferepr(@py_assert1),  'py4':@pytest_ar._saferepr(@py_assert3)}
        raise AssertionError(@pytest_ar._format_explanation(@py_format6))
    @py_assert1 = @py_assert3 = @py_assert5 = None
    B().delete()
    @py_assert1 = B()
    @py_assert3 = not @py_assert1
    if not @py_assert3:
        @py_format4 = (@pytest_ar._format_assertmsg('failed to delete from the parent table following child table deletion') + '\n>assert not %(py2)s\n{%(py2)s = %(py0)s()\n}') % {'py0':@pytest_ar._saferepr(B) if 'B' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(B) else 'B',  'py2':@pytest_ar._saferepr(@py_assert1)}
        raise AssertionError(@pytest_ar._format_explanation(@py_format4))
    @py_assert1 = @py_assert3 = None


def test_delete_tree_restricted(L, A, B, D, E):
    random.seed('cascade')
    B().populate()
    D().populate()
    E().populate()
    @py_assert0 = dj.config['safemode']
    @py_assert2 = not @py_assert0
    if not @py_assert2:
        @py_format3 = (@pytest_ar._format_assertmsg('safemode must be off for testing') + '\n>assert not %(py1)s') % {'py1': @pytest_ar._saferepr(@py_assert0)}
        raise AssertionError(@pytest_ar._format_explanation(@py_format3))
    @py_assert0 = @py_assert2 = None
    @py_assert1 = []
    @py_assert3 = L()
    @py_assert0 = @py_assert3
    if @py_assert3:
        @py_assert7 = A()
        @py_assert0 = @py_assert7
        if @py_assert7:
            @py_assert11 = B()
            @py_assert0 = @py_assert11
            if @py_assert11:
                @py_assert15 = B.C
                @py_assert17 = @py_assert15()
                @py_assert0 = @py_assert17
                if @py_assert17:
                    @py_assert21 = D()
                    @py_assert0 = @py_assert21
                    if @py_assert21:
                        @py_assert25 = E()
                        @py_assert0 = @py_assert25
                        if @py_assert25:
                            @py_assert29 = E.F
                            @py_assert31 = @py_assert29()
                            @py_assert0 = @py_assert31
    if not @py_assert0:
        @py_format5 = '%(py4)s\n{%(py4)s = %(py2)s()\n}' % {'py2':@pytest_ar._saferepr(L) if 'L' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(L) else 'L',  'py4':@pytest_ar._saferepr(@py_assert3)}
        @py_assert1.append(@py_format5)
        if @py_assert3:
            @py_format9 = '%(py8)s\n{%(py8)s = %(py6)s()\n}' % {'py6':@pytest_ar._saferepr(A) if 'A' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(A) else 'A',  'py8':@pytest_ar._saferepr(@py_assert7)}
            @py_assert1.append(@py_format9)
            if @py_assert7:
                @py_format13 = '%(py12)s\n{%(py12)s = %(py10)s()\n}' % {'py10':@pytest_ar._saferepr(B) if 'B' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(B) else 'B',  'py12':@pytest_ar._saferepr(@py_assert11)}
                @py_assert1.append(@py_format13)
                if @py_assert11:
                    @py_format19 = '%(py18)s\n{%(py18)s = %(py16)s\n{%(py16)s = %(py14)s.C\n}()\n}' % {'py14':@pytest_ar._saferepr(B) if 'B' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(B) else 'B',  'py16':@pytest_ar._saferepr(@py_assert15),  'py18':@pytest_ar._saferepr(@py_assert17)}
                    @py_assert1.append(@py_format19)
                    if @py_assert17:
                        @py_format23 = '%(py22)s\n{%(py22)s = %(py20)s()\n}' % {'py20':@pytest_ar._saferepr(D) if 'D' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(D) else 'D',  'py22':@pytest_ar._saferepr(@py_assert21)}
                        @py_assert1.append(@py_format23)
                        if @py_assert21:
                            @py_format27 = '%(py26)s\n{%(py26)s = %(py24)s()\n}' % {'py24':@pytest_ar._saferepr(E) if 'E' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(E) else 'E',  'py26':@pytest_ar._saferepr(@py_assert25)}
                            @py_assert1.append(@py_format27)
                            if @py_assert25:
                                @py_format33 = '%(py32)s\n{%(py32)s = %(py30)s\n{%(py30)s = %(py28)s.F\n}()\n}' % {'py28':@pytest_ar._saferepr(E) if 'E' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(E) else 'E',  'py30':@pytest_ar._saferepr(@py_assert29),  'py32':@pytest_ar._saferepr(@py_assert31)}
                                @py_assert1.append(@py_format33)
        @py_format34 = @pytest_ar._format_boolop(@py_assert1, 0) % {}
        @py_format36 = (@pytest_ar._format_assertmsg('schema is not populated') + '\n>assert %(py35)s') % {'py35': @py_format34}
        raise AssertionError(@pytest_ar._format_explanation(@py_format36))
    @py_assert0 = @py_assert1 = @py_assert3 = @py_assert7 = @py_assert11 = @py_assert15 = @py_assert17 = @py_assert21 = @py_assert25 = @py_assert29 = @py_assert31 = None
    cond = 'cond_in_a'
    rel = A() & cond
    rest = dict(A=(len(A()) - len(rel)),
      B=(len(B() - rel)),
      C=(len(B.C() - rel)),
      D=(len(D() - rel)),
      E=(len(E() - rel)),
      F=(len(E.F() - rel)))
    rel.delete()
    @py_assert1 = []
    @py_assert0 = rel
    if not rel:
        @py_assert5 = B()
        @py_assert8 = @py_assert5 & rel
        @py_assert0 = @py_assert8
        if not @py_assert8:
            @py_assert11 = B.C
            @py_assert13 = @py_assert11()
            @py_assert16 = @py_assert13 & rel
            @py_assert0 = @py_assert16
            if not @py_assert16:
                @py_assert19 = D()
                @py_assert22 = @py_assert19 & rel
                @py_assert0 = @py_assert22
                if not @py_assert22:
                    @py_assert25 = E()
                    @py_assert28 = @py_assert25 & rel
                    @py_assert0 = @py_assert28
                    if not @py_assert28:
                        @py_assert31 = E.F
                        @py_assert33 = @py_assert31()
                        @py_assert36 = @py_assert33 & rel
                        @py_assert0 = @py_assert36
    @py_assert40 = not @py_assert0
    if not @py_assert40:
        @py_format3 = '%(py2)s' % {'py2': @pytest_ar._saferepr(rel) if ('rel' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(rel)) else 'rel'}
        @py_assert1.append(@py_format3)
        if not rel:
            @py_format9 = '(%(py6)s\n{%(py6)s = %(py4)s()\n} & %(py7)s)' % {'py4':@pytest_ar._saferepr(B) if 'B' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(B) else 'B',  'py6':@pytest_ar._saferepr(@py_assert5),  'py7':@pytest_ar._saferepr(rel) if 'rel' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(rel) else 'rel'}
            @py_assert1.append(@py_format9)
            if not @py_assert8:
                @py_format17 = '(%(py14)s\n{%(py14)s = %(py12)s\n{%(py12)s = %(py10)s.C\n}()\n} & %(py15)s)' % {'py10':@pytest_ar._saferepr(B) if 'B' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(B) else 'B',  'py12':@pytest_ar._saferepr(@py_assert11),  'py14':@pytest_ar._saferepr(@py_assert13),  'py15':@pytest_ar._saferepr(rel) if 'rel' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(rel) else 'rel'}
                @py_assert1.append(@py_format17)
                if not @py_assert16:
                    @py_format23 = '(%(py20)s\n{%(py20)s = %(py18)s()\n} & %(py21)s)' % {'py18':@pytest_ar._saferepr(D) if 'D' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(D) else 'D',  'py20':@pytest_ar._saferepr(@py_assert19),  'py21':@pytest_ar._saferepr(rel) if 'rel' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(rel) else 'rel'}
                    @py_assert1.append(@py_format23)
                    if not @py_assert22:
                        @py_format29 = '(%(py26)s\n{%(py26)s = %(py24)s()\n} & %(py27)s)' % {'py24':@pytest_ar._saferepr(E) if 'E' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(E) else 'E',  'py26':@pytest_ar._saferepr(@py_assert25),  'py27':@pytest_ar._saferepr(rel) if 'rel' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(rel) else 'rel'}
                        @py_assert1.append(@py_format29)
                        if not @py_assert28:
                            @py_format37 = '(%(py34)s\n{%(py34)s = %(py32)s\n{%(py32)s = %(py30)s.F\n}()\n} & %(py35)s)' % {'py30':@pytest_ar._saferepr(E) if 'E' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(E) else 'E',  'py32':@pytest_ar._saferepr(@py_assert31),  'py34':@pytest_ar._saferepr(@py_assert33),  'py35':@pytest_ar._saferepr(rel) if 'rel' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(rel) else 'rel'}
                            @py_assert1.append(@py_format37)
        @py_format38 = @pytest_ar._format_boolop(@py_assert1, 1) % {}
        @py_format41 = (@pytest_ar._format_assertmsg('incomplete delete') + '\n>assert not %(py39)s') % {'py39': @py_format38}
        raise AssertionError(@pytest_ar._format_explanation(@py_format41))
    @py_assert0 = @py_assert1 = @py_assert5 = @py_assert8 = @py_assert11 = @py_assert13 = @py_assert16 = @py_assert19 = @py_assert22 = @py_assert25 = @py_assert28 = @py_assert31 = @py_assert33 = @py_assert36 = @py_assert40 = None
    @py_assert2 = A()
    @py_assert4 = len(@py_assert2)
    @py_assert7 = rest['A']
    @py_assert6 = @py_assert4 == @py_assert7
    if not @py_assert6:
        @py_format9 = @pytest_ar._call_reprcompare(('==', ), (@py_assert6,), ('%(py5)s\n{%(py5)s = %(py0)s(%(py3)s\n{%(py3)s = %(py1)s()\n})\n} == %(py8)s', ), (@py_assert4, @py_assert7)) % {'py0':@pytest_ar._saferepr(len) if 'len' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(len) else 'len',  'py1':@pytest_ar._saferepr(A) if 'A' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(A) else 'A',  'py3':@pytest_ar._saferepr(@py_assert2),  'py5':@pytest_ar._saferepr(@py_assert4),  'py8':@pytest_ar._saferepr(@py_assert7)}
        @py_format11 = (@pytest_ar._format_assertmsg('invalid delete restriction') + '\n>assert %(py10)s') % {'py10': @py_format9}
        raise AssertionError(@pytest_ar._format_explanation(@py_format11))
    @py_assert2 = @py_assert4 = @py_assert6 = @py_assert7 = None
    @py_assert2 = B()
    @py_assert4 = len(@py_assert2)
    @py_assert7 = rest['B']
    @py_assert6 = @py_assert4 == @py_assert7
    if not @py_assert6:
        @py_format9 = @pytest_ar._call_reprcompare(('==', ), (@py_assert6,), ('%(py5)s\n{%(py5)s = %(py0)s(%(py3)s\n{%(py3)s = %(py1)s()\n})\n} == %(py8)s', ), (@py_assert4, @py_assert7)) % {'py0':@pytest_ar._saferepr(len) if 'len' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(len) else 'len',  'py1':@pytest_ar._saferepr(B) if 'B' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(B) else 'B',  'py3':@pytest_ar._saferepr(@py_assert2),  'py5':@pytest_ar._saferepr(@py_assert4),  'py8':@pytest_ar._saferepr(@py_assert7)}
        @py_format11 = (@pytest_ar._format_assertmsg('invalid delete restriction') + '\n>assert %(py10)s') % {'py10': @py_format9}
        raise AssertionError(@pytest_ar._format_explanation(@py_format11))
    @py_assert2 = @py_assert4 = @py_assert6 = @py_assert7 = None
    @py_assert2 = B.C
    @py_assert4 = @py_assert2()
    @py_assert6 = len(@py_assert4)
    @py_assert9 = rest['C']
    @py_assert8 = @py_assert6 == @py_assert9
    if not @py_assert8:
        @py_format11 = @pytest_ar._call_reprcompare(('==', ), (@py_assert8,), ('%(py7)s\n{%(py7)s = %(py0)s(%(py5)s\n{%(py5)s = %(py3)s\n{%(py3)s = %(py1)s.C\n}()\n})\n} == %(py10)s', ), (@py_assert6, @py_assert9)) % {'py0':@pytest_ar._saferepr(len) if 'len' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(len) else 'len',  'py1':@pytest_ar._saferepr(B) if 'B' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(B) else 'B',  'py3':@pytest_ar._saferepr(@py_assert2),  'py5':@pytest_ar._saferepr(@py_assert4),  'py7':@pytest_ar._saferepr(@py_assert6),  'py10':@pytest_ar._saferepr(@py_assert9)}
        @py_format13 = (@pytest_ar._format_assertmsg('invalid delete restriction') + '\n>assert %(py12)s') % {'py12': @py_format11}
        raise AssertionError(@pytest_ar._format_explanation(@py_format13))
    @py_assert2 = @py_assert4 = @py_assert6 = @py_assert8 = @py_assert9 = None
    @py_assert2 = D()
    @py_assert4 = len(@py_assert2)
    @py_assert7 = rest['D']
    @py_assert6 = @py_assert4 == @py_assert7
    if not @py_assert6:
        @py_format9 = @pytest_ar._call_reprcompare(('==', ), (@py_assert6,), ('%(py5)s\n{%(py5)s = %(py0)s(%(py3)s\n{%(py3)s = %(py1)s()\n})\n} == %(py8)s', ), (@py_assert4, @py_assert7)) % {'py0':@pytest_ar._saferepr(len) if 'len' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(len) else 'len',  'py1':@pytest_ar._saferepr(D) if 'D' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(D) else 'D',  'py3':@pytest_ar._saferepr(@py_assert2),  'py5':@pytest_ar._saferepr(@py_assert4),  'py8':@pytest_ar._saferepr(@py_assert7)}
        @py_format11 = (@pytest_ar._format_assertmsg('invalid delete restriction') + '\n>assert %(py10)s') % {'py10': @py_format9}
        raise AssertionError(@pytest_ar._format_explanation(@py_format11))
    @py_assert2 = @py_assert4 = @py_assert6 = @py_assert7 = None
    @py_assert2 = E()
    @py_assert4 = len(@py_assert2)
    @py_assert7 = rest['E']
    @py_assert6 = @py_assert4 == @py_assert7
    if not @py_assert6:
        @py_format9 = @pytest_ar._call_reprcompare(('==', ), (@py_assert6,), ('%(py5)s\n{%(py5)s = %(py0)s(%(py3)s\n{%(py3)s = %(py1)s()\n})\n} == %(py8)s', ), (@py_assert4, @py_assert7)) % {'py0':@pytest_ar._saferepr(len) if 'len' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(len) else 'len',  'py1':@pytest_ar._saferepr(E) if 'E' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(E) else 'E',  'py3':@pytest_ar._saferepr(@py_assert2),  'py5':@pytest_ar._saferepr(@py_assert4),  'py8':@pytest_ar._saferepr(@py_assert7)}
        @py_format11 = (@pytest_ar._format_assertmsg('invalid delete restriction') + '\n>assert %(py10)s') % {'py10': @py_format9}
        raise AssertionError(@pytest_ar._format_explanation(@py_format11))
    @py_assert2 = @py_assert4 = @py_assert6 = @py_assert7 = None
    @py_assert2 = E.F
    @py_assert4 = @py_assert2()
    @py_assert6 = len(@py_assert4)
    @py_assert9 = rest['F']
    @py_assert8 = @py_assert6 == @py_assert9
    if not @py_assert8:
        @py_format11 = @pytest_ar._call_reprcompare(('==', ), (@py_assert8,), ('%(py7)s\n{%(py7)s = %(py0)s(%(py5)s\n{%(py5)s = %(py3)s\n{%(py3)s = %(py1)s.F\n}()\n})\n} == %(py10)s', ), (@py_assert6, @py_assert9)) % {'py0':@pytest_ar._saferepr(len) if 'len' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(len) else 'len',  'py1':@pytest_ar._saferepr(E) if 'E' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(E) else 'E',  'py3':@pytest_ar._saferepr(@py_assert2),  'py5':@pytest_ar._saferepr(@py_assert4),  'py7':@pytest_ar._saferepr(@py_assert6),  'py10':@pytest_ar._saferepr(@py_assert9)}
        @py_format13 = (@pytest_ar._format_assertmsg('invalid delete restriction') + '\n>assert %(py12)s') % {'py12': @py_format11}
        raise AssertionError(@pytest_ar._format_explanation(@py_format13))
    @py_assert2 = @py_assert4 = @py_assert6 = @py_assert8 = @py_assert9 = None


def test_delete_lookup(L, A, B, D, E):
    random.seed('cascade')
    B().populate()
    D().populate()
    E().populate()
    @py_assert0 = dj.config['safemode']
    @py_assert2 = not @py_assert0
    if not @py_assert2:
        @py_format3 = (@pytest_ar._format_assertmsg('safemode must be off for testing') + '\n>assert not %(py1)s') % {'py1': @pytest_ar._saferepr(@py_assert0)}
        raise AssertionError(@pytest_ar._format_explanation(@py_format3))
    @py_assert0 = @py_assert2 = None
    @py_assert2 = []
    @py_assert4 = L()
    @py_assert1 = @py_assert4
    if @py_assert4:
        @py_assert8 = A()
        @py_assert1 = @py_assert8
        if @py_assert8:
            @py_assert12 = B()
            @py_assert1 = @py_assert12
            if @py_assert12:
                @py_assert16 = B.C
                @py_assert18 = @py_assert16()
                @py_assert1 = @py_assert18
                if @py_assert18:
                    @py_assert22 = D()
                    @py_assert1 = @py_assert22
                    if @py_assert22:
                        @py_assert26 = E()
                        @py_assert1 = @py_assert26
                        if @py_assert26:
                            @py_assert30 = E.F
                            @py_assert32 = @py_assert30()
                            @py_assert1 = @py_assert32
    @py_assert37 = bool(@py_assert1)
    if not @py_assert37:
        @py_format6 = '%(py5)s\n{%(py5)s = %(py3)s()\n}' % {'py3':@pytest_ar._saferepr(L) if 'L' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(L) else 'L',  'py5':@pytest_ar._saferepr(@py_assert4)}
        @py_assert2.append(@py_format6)
        if @py_assert4:
            @py_format10 = '%(py9)s\n{%(py9)s = %(py7)s()\n}' % {'py7':@pytest_ar._saferepr(A) if 'A' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(A) else 'A',  'py9':@pytest_ar._saferepr(@py_assert8)}
            @py_assert2.append(@py_format10)
            if @py_assert8:
                @py_format14 = '%(py13)s\n{%(py13)s = %(py11)s()\n}' % {'py11':@pytest_ar._saferepr(B) if 'B' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(B) else 'B',  'py13':@pytest_ar._saferepr(@py_assert12)}
                @py_assert2.append(@py_format14)
                if @py_assert12:
                    @py_format20 = '%(py19)s\n{%(py19)s = %(py17)s\n{%(py17)s = %(py15)s.C\n}()\n}' % {'py15':@pytest_ar._saferepr(B) if 'B' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(B) else 'B',  'py17':@pytest_ar._saferepr(@py_assert16),  'py19':@pytest_ar._saferepr(@py_assert18)}
                    @py_assert2.append(@py_format20)
                    if @py_assert18:
                        @py_format24 = '%(py23)s\n{%(py23)s = %(py21)s()\n}' % {'py21':@pytest_ar._saferepr(D) if 'D' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(D) else 'D',  'py23':@pytest_ar._saferepr(@py_assert22)}
                        @py_assert2.append(@py_format24)
                        if @py_assert22:
                            @py_format28 = '%(py27)s\n{%(py27)s = %(py25)s()\n}' % {'py25':@pytest_ar._saferepr(E) if 'E' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(E) else 'E',  'py27':@pytest_ar._saferepr(@py_assert26)}
                            @py_assert2.append(@py_format28)
                            if @py_assert26:
                                @py_format34 = '%(py33)s\n{%(py33)s = %(py31)s\n{%(py31)s = %(py29)s.F\n}()\n}' % {'py29':@pytest_ar._saferepr(E) if 'E' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(E) else 'E',  'py31':@pytest_ar._saferepr(@py_assert30),  'py33':@pytest_ar._saferepr(@py_assert32)}
                                @py_assert2.append(@py_format34)
        @py_format35 = @pytest_ar._format_boolop(@py_assert2, 0) % {}
        @py_format39 = (@pytest_ar._format_assertmsg('schema is not populated') + '\n>assert %(py38)s\n{%(py38)s = %(py0)s(%(py36)s)\n}') % {'py0':@pytest_ar._saferepr(bool) if 'bool' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(bool) else 'bool',  'py36':@py_format35,  'py38':@pytest_ar._saferepr(@py_assert37)}
        raise AssertionError(@pytest_ar._format_explanation(@py_format39))
    @py_assert1 = @py_assert2 = @py_assert4 = @py_assert8 = @py_assert12 = @py_assert16 = @py_assert18 = @py_assert22 = @py_assert26 = @py_assert30 = @py_assert32 = @py_assert37 = None
    L().delete()
    @py_assert2 = []
    @py_assert4 = L()
    @py_assert1 = @py_assert4
    if not @py_assert4:
        @py_assert8 = D()
        @py_assert1 = @py_assert8
        if not @py_assert8:
            @py_assert12 = E()
            @py_assert1 = @py_assert12
            if not @py_assert12:
                @py_assert16 = E.F
                @py_assert18 = @py_assert16()
                @py_assert1 = @py_assert18
    @py_assert23 = bool(@py_assert1)
    @py_assert25 = not @py_assert23
    if not @py_assert25:
        @py_format6 = '%(py5)s\n{%(py5)s = %(py3)s()\n}' % {'py3':@pytest_ar._saferepr(L) if 'L' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(L) else 'L',  'py5':@pytest_ar._saferepr(@py_assert4)}
        @py_assert2.append(@py_format6)
        if not @py_assert4:
            @py_format10 = '%(py9)s\n{%(py9)s = %(py7)s()\n}' % {'py7':@pytest_ar._saferepr(D) if 'D' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(D) else 'D',  'py9':@pytest_ar._saferepr(@py_assert8)}
            @py_assert2.append(@py_format10)
            if not @py_assert8:
                @py_format14 = '%(py13)s\n{%(py13)s = %(py11)s()\n}' % {'py11':@pytest_ar._saferepr(E) if 'E' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(E) else 'E',  'py13':@pytest_ar._saferepr(@py_assert12)}
                @py_assert2.append(@py_format14)
                if not @py_assert12:
                    @py_format20 = '%(py19)s\n{%(py19)s = %(py17)s\n{%(py17)s = %(py15)s.F\n}()\n}' % {'py15':@pytest_ar._saferepr(E) if 'E' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(E) else 'E',  'py17':@pytest_ar._saferepr(@py_assert16),  'py19':@pytest_ar._saferepr(@py_assert18)}
                    @py_assert2.append(@py_format20)
        @py_format21 = @pytest_ar._format_boolop(@py_assert2, 1) % {}
        @py_format26 = (@pytest_ar._format_assertmsg('incomplete delete') + '\n>assert not %(py24)s\n{%(py24)s = %(py0)s(%(py22)s)\n}') % {'py0':@pytest_ar._saferepr(bool) if 'bool' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(bool) else 'bool',  'py22':@py_format21,  'py24':@pytest_ar._saferepr(@py_assert23)}
        raise AssertionError(@pytest_ar._format_explanation(@py_format26))
    @py_assert1 = @py_assert2 = @py_assert4 = @py_assert8 = @py_assert12 = @py_assert16 = @py_assert18 = @py_assert23 = @py_assert25 = None
    A().delete()


def test_delete_lookup_restricted(L, A, B, D, E):
    random.seed('cascade')
    B().populate()
    D().populate()
    E().populate()
    @py_assert0 = dj.config['safemode']
    @py_assert2 = not @py_assert0
    if not @py_assert2:
        @py_format3 = (@pytest_ar._format_assertmsg('safemode must be off for testing') + '\n>assert not %(py1)s') % {'py1': @pytest_ar._saferepr(@py_assert0)}
        raise AssertionError(@pytest_ar._format_explanation(@py_format3))
    @py_assert0 = @py_assert2 = None
    @py_assert1 = []
    @py_assert3 = L()
    @py_assert0 = @py_assert3
    if @py_assert3:
        @py_assert7 = A()
        @py_assert0 = @py_assert7
        if @py_assert7:
            @py_assert11 = B()
            @py_assert0 = @py_assert11
            if @py_assert11:
                @py_assert15 = B.C
                @py_assert17 = @py_assert15()
                @py_assert0 = @py_assert17
                if @py_assert17:
                    @py_assert21 = D()
                    @py_assert0 = @py_assert21
                    if @py_assert21:
                        @py_assert25 = E()
                        @py_assert0 = @py_assert25
                        if @py_assert25:
                            @py_assert29 = E.F
                            @py_assert31 = @py_assert29()
                            @py_assert0 = @py_assert31
    if not @py_assert0:
        @py_format5 = '%(py4)s\n{%(py4)s = %(py2)s()\n}' % {'py2':@pytest_ar._saferepr(L) if 'L' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(L) else 'L',  'py4':@pytest_ar._saferepr(@py_assert3)}
        @py_assert1.append(@py_format5)
        if @py_assert3:
            @py_format9 = '%(py8)s\n{%(py8)s = %(py6)s()\n}' % {'py6':@pytest_ar._saferepr(A) if 'A' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(A) else 'A',  'py8':@pytest_ar._saferepr(@py_assert7)}
            @py_assert1.append(@py_format9)
            if @py_assert7:
                @py_format13 = '%(py12)s\n{%(py12)s = %(py10)s()\n}' % {'py10':@pytest_ar._saferepr(B) if 'B' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(B) else 'B',  'py12':@pytest_ar._saferepr(@py_assert11)}
                @py_assert1.append(@py_format13)
                if @py_assert11:
                    @py_format19 = '%(py18)s\n{%(py18)s = %(py16)s\n{%(py16)s = %(py14)s.C\n}()\n}' % {'py14':@pytest_ar._saferepr(B) if 'B' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(B) else 'B',  'py16':@pytest_ar._saferepr(@py_assert15),  'py18':@pytest_ar._saferepr(@py_assert17)}
                    @py_assert1.append(@py_format19)
                    if @py_assert17:
                        @py_format23 = '%(py22)s\n{%(py22)s = %(py20)s()\n}' % {'py20':@pytest_ar._saferepr(D) if 'D' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(D) else 'D',  'py22':@pytest_ar._saferepr(@py_assert21)}
                        @py_assert1.append(@py_format23)
                        if @py_assert21:
                            @py_format27 = '%(py26)s\n{%(py26)s = %(py24)s()\n}' % {'py24':@pytest_ar._saferepr(E) if 'E' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(E) else 'E',  'py26':@pytest_ar._saferepr(@py_assert25)}
                            @py_assert1.append(@py_format27)
                            if @py_assert25:
                                @py_format33 = '%(py32)s\n{%(py32)s = %(py30)s\n{%(py30)s = %(py28)s.F\n}()\n}' % {'py28':@pytest_ar._saferepr(E) if 'E' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(E) else 'E',  'py30':@pytest_ar._saferepr(@py_assert29),  'py32':@pytest_ar._saferepr(@py_assert31)}
                                @py_assert1.append(@py_format33)
        @py_format34 = @pytest_ar._format_boolop(@py_assert1, 0) % {}
        @py_format36 = (@pytest_ar._format_assertmsg('schema is not populated') + '\n>assert %(py35)s') % {'py35': @py_format34}
        raise AssertionError(@pytest_ar._format_explanation(@py_format36))
    @py_assert0 = @py_assert1 = @py_assert3 = @py_assert7 = @py_assert11 = @py_assert15 = @py_assert17 = @py_assert21 = @py_assert25 = @py_assert29 = @py_assert31 = None
    rel = L() & 'cond_in_l'
    original_count = len(L())
    deleted_count = len(rel)
    rel.delete()
    @py_assert2 = L()
    @py_assert4 = len(@py_assert2)
    @py_assert9 = original_count - deleted_count
    @py_assert6 = @py_assert4 == @py_assert9
    if not @py_assert6:
        @py_format10 = @pytest_ar._call_reprcompare(('==', ), (@py_assert6,), ('%(py5)s\n{%(py5)s = %(py0)s(%(py3)s\n{%(py3)s = %(py1)s()\n})\n} == (%(py7)s - %(py8)s)', ), (@py_assert4, @py_assert9)) % {'py0':@pytest_ar._saferepr(len) if 'len' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(len) else 'len',  'py1':@pytest_ar._saferepr(L) if 'L' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(L) else 'L',  'py3':@pytest_ar._saferepr(@py_assert2),  'py5':@pytest_ar._saferepr(@py_assert4),  'py7':@pytest_ar._saferepr(original_count) if 'original_count' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(original_count) else 'original_count',  'py8':@pytest_ar._saferepr(deleted_count) if 'deleted_count' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(deleted_count) else 'deleted_count'}
        @py_format12 = 'assert %(py11)s' % {'py11': @py_format10}
        raise AssertionError(@pytest_ar._format_explanation(@py_format12))
    @py_assert2 = @py_assert4 = @py_assert6 = @py_assert9 = None


def test_delete_complex_keys(ComplexParent, ComplexChild):
    @py_assert0 = dj.config['safemode']
    @py_assert2 = not @py_assert0
    if not @py_assert2:
        @py_format3 = (@pytest_ar._format_assertmsg('safemode must be off for testing') + '\n>assert not %(py1)s') % {'py1': @pytest_ar._saferepr(@py_assert0)}
        raise AssertionError(@pytest_ar._format_explanation(@py_format3))
    @py_assert0 = @py_assert2 = None
    parent_key_count = 8
    child_key_count = 1
    restriction = dict(
     {'parent_id_{}'.format(i + 1): i for i in range(parent_key_count)}, **)
    @py_assert3 = ComplexParent & restriction
    @py_assert4 = len(@py_assert3)
    @py_assert7 = 1
    @py_assert6 = @py_assert4 == @py_assert7
    if not @py_assert6:
        @py_format9 = @pytest_ar._call_reprcompare(('==', ), (@py_assert6,), ('%(py5)s\n{%(py5)s = %(py0)s((%(py1)s & %(py2)s))\n} == %(py8)s', ), (@py_assert4, @py_assert7)) % {'py0':@pytest_ar._saferepr(len) if 'len' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(len) else 'len',  'py1':@pytest_ar._saferepr(ComplexParent) if 'ComplexParent' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(ComplexParent) else 'ComplexParent',  'py2':@pytest_ar._saferepr(restriction) if 'restriction' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(restriction) else 'restriction',  'py5':@pytest_ar._saferepr(@py_assert4),  'py8':@pytest_ar._saferepr(@py_assert7)}
        @py_format11 = (@pytest_ar._format_assertmsg('Parent record missing') + '\n>assert %(py10)s') % {'py10': @py_format9}
        raise AssertionError(@pytest_ar._format_explanation(@py_format11))
    @py_assert3 = @py_assert4 = @py_assert6 = @py_assert7 = None
    @py_assert3 = ComplexChild & restriction
    @py_assert4 = len(@py_assert3)
    @py_assert7 = 1
    @py_assert6 = @py_assert4 == @py_assert7
    if not @py_assert6:
        @py_format9 = @pytest_ar._call_reprcompare(('==', ), (@py_assert6,), ('%(py5)s\n{%(py5)s = %(py0)s((%(py1)s & %(py2)s))\n} == %(py8)s', ), (@py_assert4, @py_assert7)) % {'py0':@pytest_ar._saferepr(len) if 'len' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(len) else 'len',  'py1':@pytest_ar._saferepr(ComplexChild) if 'ComplexChild' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(ComplexChild) else 'ComplexChild',  'py2':@pytest_ar._saferepr(restriction) if 'restriction' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(restriction) else 'restriction',  'py5':@pytest_ar._saferepr(@py_assert4),  'py8':@pytest_ar._saferepr(@py_assert7)}
        @py_format11 = (@pytest_ar._format_assertmsg('Child record missing') + '\n>assert %(py10)s') % {'py10': @py_format9}
        raise AssertionError(@pytest_ar._format_explanation(@py_format11))
    @py_assert3 = @py_assert4 = @py_assert6 = @py_assert7 = None
    (ComplexParent & restriction).delete()
    @py_assert3 = ComplexParent & restriction
    @py_assert4 = len(@py_assert3)
    @py_assert7 = 0
    @py_assert6 = @py_assert4 == @py_assert7
    if not @py_assert6:
        @py_format9 = @pytest_ar._call_reprcompare(('==', ), (@py_assert6,), ('%(py5)s\n{%(py5)s = %(py0)s((%(py1)s & %(py2)s))\n} == %(py8)s', ), (@py_assert4, @py_assert7)) % {'py0':@pytest_ar._saferepr(len) if 'len' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(len) else 'len',  'py1':@pytest_ar._saferepr(ComplexParent) if 'ComplexParent' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(ComplexParent) else 'ComplexParent',  'py2':@pytest_ar._saferepr(restriction) if 'restriction' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(restriction) else 'restriction',  'py5':@pytest_ar._saferepr(@py_assert4),  'py8':@pytest_ar._saferepr(@py_assert7)}
        @py_format11 = (@pytest_ar._format_assertmsg('Parent record was not deleted') + '\n>assert %(py10)s') % {'py10': @py_format9}
        raise AssertionError(@pytest_ar._format_explanation(@py_format11))
    @py_assert3 = @py_assert4 = @py_assert6 = @py_assert7 = None
    @py_assert3 = ComplexChild & restriction
    @py_assert4 = len(@py_assert3)
    @py_assert7 = 0
    @py_assert6 = @py_assert4 == @py_assert7
    if not @py_assert6:
        @py_format9 = @pytest_ar._call_reprcompare(('==', ), (@py_assert6,), ('%(py5)s\n{%(py5)s = %(py0)s((%(py1)s & %(py2)s))\n} == %(py8)s', ), (@py_assert4, @py_assert7)) % {'py0':@pytest_ar._saferepr(len) if 'len' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(len) else 'len',  'py1':@pytest_ar._saferepr(ComplexChild) if 'ComplexChild' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(ComplexChild) else 'ComplexChild',  'py2':@pytest_ar._saferepr(restriction) if 'restriction' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(restriction) else 'restriction',  'py5':@pytest_ar._saferepr(@py_assert4),  'py8':@pytest_ar._saferepr(@py_assert7)}
        @py_format11 = (@pytest_ar._format_assertmsg('Child record was not deleted') + '\n>assert %(py10)s') % {'py10': @py_format9}
        raise AssertionError(@pytest_ar._format_explanation(@py_format11))
    @py_assert3 = @py_assert4 = @py_assert6 = @py_assert7 = None


def test_delete_master(Profile):
    Profile().populate_random()
    Profile().delete()


def test_delete_parts(Profile, Website):
    """test issue #151"""
    Profile().populate_random()
    with pytest.raises(DataJointError):
        Website().delete()


def test_drop_part(Profile, Website):
    """test issue #374"""
    with pytest.raises(DataJointError):
        Website().drop()