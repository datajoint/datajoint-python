# decompyle3 version 3.9.0
# Python bytecode version base 3.7.0 (3394)
# Decompiled from: Python 3.7.17 (default, Jun 13 2023, 16:22:33) 
# [GCC 10.2.1 20210110]
# Embedded file name: /workspaces/datajoint-python/tests/test_aggr_regressions.py
# Compiled at: 2023-02-17 12:16:09
# Size of source mod 2**32: 3916 bytes
__doc__ = '\nRegression tests for issues 386, 449, 484, and 558 — all related to processing complex aggregations and projections.\n'
import builtins as @py_builtins
import _pytest.assertion.rewrite as @pytest_ar
import datajoint as dj, itertools, uuid, pytest
from . import PREFIX, connection_root, connection_test
from schemas.uuid import top_level_namespace_id, Topic, Item

@pytest.fixture
def schema(connection_test):
    schema = dj.Schema((PREFIX + '_aggr_regress'), connection=connection_test)
    yield schema
    schema.drop()


@pytest.fixture
def R(schema):

    @schema
    class R(dj.Lookup):
        definition = '\n        r : char(1)\n        '
        contents = zip('ABCDFGHIJKLMNOPQRST')

    yield R
    R.drop()


@pytest.fixture
def Q(schema, R):

    @schema
    class Q(dj.Lookup):
        definition = '\n        -> R\n        '
        contents = zip('ABCDFGH')

    yield Q
    Q.drop()


@pytest.fixture
def S(schema, R):

    @schema
    class S(dj.Lookup):
        definition = '\n        -> R\n        s : int\n        '
        contents = itertools.product('ABCDF', range(10))

    yield S
    S.drop()


def test_issue386(R, S, Q):
    result = R.aggr(S, n='count(*)') & 'n=10'
    result = Q & result
    result.fetch()


def test_issue449(R, S):
    result = dj.U('n') * R.aggr(S, n='max(s)')
    result.fetch()


def test_issue484(S):
    q = dj.U().aggr(S, n='max(s)')
    n = q.fetch('n')
    n = q.fetch1('n')
    q = dj.U().aggr(S, n='avg(s)')
    result = dj.U().aggr(q, m='max(n)')
    result.fetch()


@pytest.fixture
def A(schema):

    @schema
    class A(dj.Lookup):
        definition = '\n        id: int\n        '
        contents = zip(range(10))

    yield A
    A.drop()


@pytest.fixture
def B(schema, A):

    @schema
    class B(dj.Lookup):
        definition = '\n        -> A\n        id2: int\n        '
        contents = zip(range(5), range(5, 10))

    yield B
    B.drop()


@pytest.fixture
def X(schema):

    @schema
    class X(dj.Lookup):
        definition = '\n        id: int\n        '
        contents = zip(range(10))

    yield X
    X.drop()


def test_issue558_part1(A, B):
    q = (A - B).proj(id2='3')
    @py_assert3 = A - B
    @py_assert4 = len(@py_assert3)
    @py_assert9 = len(q)
    @py_assert6 = @py_assert4 == @py_assert9
    if not @py_assert6:
        @py_format11 = @pytest_ar._call_reprcompare(('==', ), (@py_assert6,), ('%(py5)s\n{%(py5)s = %(py0)s((%(py1)s - %(py2)s))\n} == %(py10)s\n{%(py10)s = %(py7)s(%(py8)s)\n}', ), (@py_assert4, @py_assert9)) % {'py0':@pytest_ar._saferepr(len) if 'len' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(len) else 'len',  'py1':@pytest_ar._saferepr(A) if 'A' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(A) else 'A',  'py2':@pytest_ar._saferepr(B) if 'B' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(B) else 'B',  'py5':@pytest_ar._saferepr(@py_assert4),  'py7':@pytest_ar._saferepr(len) if 'len' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(len) else 'len',  'py8':@pytest_ar._saferepr(q) if 'q' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(q) else 'q',  'py10':@pytest_ar._saferepr(@py_assert9)}
        @py_format13 = 'assert %(py12)s' % {'py12': @py_format11}
        raise AssertionError(@pytest_ar._format_explanation(@py_format13))
    @py_assert3 = @py_assert4 = @py_assert6 = @py_assert9 = None


def test_issue558_part2(X):
    d = dict(id=3, id2=5)
    @py_assert3 = X & d
    @py_assert4 = len(@py_assert3)
    @py_assert10 = X & d
    @py_assert11 = @py_assert10.proj
    @py_assert13 = '3'
    @py_assert15 = @py_assert11(id2=@py_assert13)
    @py_assert17 = len(@py_assert15)
    @py_assert6 = @py_assert4 == @py_assert17
    if not @py_assert6:
        @py_format19 = @pytest_ar._call_reprcompare(('==', ), (@py_assert6,), ('%(py5)s\n{%(py5)s = %(py0)s((%(py1)s & %(py2)s))\n} == %(py18)s\n{%(py18)s = %(py7)s(%(py16)s\n{%(py16)s = %(py12)s\n{%(py12)s = (%(py8)s & %(py9)s).proj\n}(id2=%(py14)s)\n})\n}', ), (@py_assert4, @py_assert17)) % {'py0':@pytest_ar._saferepr(len) if 'len' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(len) else 'len',  'py1':@pytest_ar._saferepr(X) if 'X' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(X) else 'X',  'py2':@pytest_ar._saferepr(d) if 'd' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(d) else 'd',  'py5':@pytest_ar._saferepr(@py_assert4),  'py7':@pytest_ar._saferepr(len) if 'len' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(len) else 'len',  'py8':@pytest_ar._saferepr(X) if 'X' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(X) else 'X',  'py9':@pytest_ar._saferepr(d) if 'd' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(d) else 'd',  'py12':@pytest_ar._saferepr(@py_assert11),  'py14':@pytest_ar._saferepr(@py_assert13),  'py16':@pytest_ar._saferepr(@py_assert15),  'py18':@pytest_ar._saferepr(@py_assert17)}
        @py_format21 = 'assert %(py20)s' % {'py20': @py_format19}
        raise AssertionError(@pytest_ar._format_explanation(@py_format21))
    @py_assert3 = @py_assert4 = @py_assert6 = @py_assert10 = @py_assert11 = @py_assert13 = @py_assert15 = @py_assert17 = None


def test_left_join_len(Topic, Item):
    Topic().add('jeff')
    Item.populate()
    Topic().add('jeff2')
    Topic().add('jeff3')
    q = Topic.join((Item - dict(topic_id=(uuid.uuid5(top_level_namespace_id, 'jeff')))),
      left=True)
    qf = q.fetch()
    @py_assert2 = len(q)
    @py_assert7 = len(qf)
    @py_assert4 = @py_assert2 == @py_assert7
    if not @py_assert4:
        @py_format9 = @pytest_ar._call_reprcompare(('==', ), (@py_assert4,), ('%(py3)s\n{%(py3)s = %(py0)s(%(py1)s)\n} == %(py8)s\n{%(py8)s = %(py5)s(%(py6)s)\n}', ), (@py_assert2, @py_assert7)) % {'py0':@pytest_ar._saferepr(len) if 'len' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(len) else 'len',  'py1':@pytest_ar._saferepr(q) if 'q' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(q) else 'q',  'py3':@pytest_ar._saferepr(@py_assert2),  'py5':@pytest_ar._saferepr(len) if 'len' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(len) else 'len',  'py6':@pytest_ar._saferepr(qf) if 'qf' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(qf) else 'qf',  'py8':@pytest_ar._saferepr(@py_assert7)}
        @py_format11 = 'assert %(py10)s' % {'py10': @py_format9}
        raise AssertionError(@pytest_ar._format_explanation(@py_format11))
    @py_assert2 = @py_assert4 = @py_assert7 = None


def test_union_join(A, B):
    A.insert(zip([100,200,300,400,500,600]))
    B.insert([(100, 11), (200, 22), (300, 33), (400, 44)])
    q1 = B & 'id < 300'
    q2 = B & 'id > 300'
    expected_data = [
     {'id':0, 
      'id2':5},
     {'id':1, 
      'id2':6},
     {'id':2, 
      'id2':7},
     {'id':3, 
      'id2':8},
     {'id':4, 
      'id2':9},
     {'id':100, 
      'id2':11},
     {'id':200, 
      'id2':22},
     {'id':400, 
      'id2':44}]
    @py_assert2 = q1 + q2
    @py_assert4 = @py_assert2 * A
    @py_assert5 = @py_assert4.fetch
    @py_assert7 = True
    @py_assert9 = @py_assert5(as_dict=@py_assert7)
    @py_assert11 = @py_assert9 == expected_data
    if not @py_assert11:
        @py_format13 = @pytest_ar._call_reprcompare(('==', ), (@py_assert11,), ('%(py10)s\n{%(py10)s = %(py6)s\n{%(py6)s = ((%(py0)s + %(py1)s) * %(py3)s).fetch\n}(as_dict=%(py8)s)\n} == %(py12)s', ), (@py_assert9, expected_data)) % {'py0':@pytest_ar._saferepr(q1) if 'q1' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(q1) else 'q1',  'py1':@pytest_ar._saferepr(q2) if 'q2' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(q2) else 'q2',  'py3':@pytest_ar._saferepr(A) if 'A' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(A) else 'A',  'py6':@pytest_ar._saferepr(@py_assert5),  'py8':@pytest_ar._saferepr(@py_assert7),  'py10':@pytest_ar._saferepr(@py_assert9),  'py12':@pytest_ar._saferepr(expected_data) if 'expected_data' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(expected_data) else 'expected_data'}
        @py_format15 = 'assert %(py14)s' % {'py14': @py_format13}
        raise AssertionError(@pytest_ar._format_explanation(@py_format15))
    @py_assert2 = @py_assert4 = @py_assert5 = @py_assert7 = @py_assert9 = @py_assert11 = None