# decompyle3 version 3.9.0
# Python bytecode version base 3.7.0 (3394)
# Decompiled from: Python 3.7.17 (default, Jun 13 2023, 16:22:33) 
# [GCC 10.2.1 20210110]
# Embedded file name: /workspaces/datajoint-python/tests/test_fetch.py
# Compiled at: 2023-02-19 07:56:50
# Size of source mod 2**32: 13190 bytes
import builtins as @py_builtins
import _pytest.assertion.rewrite as @pytest_ar
import datajoint as dj, numpy as np
from operator import itemgetter
import pandas, itertools, decimal, logging, io, os, pytest
from . import connection_root, connection_test
from schemas.default import schema, Subject, Language, User, DecimalPrimaryKey, NullableNumbers, Child, Parent, TTest3, Stimulus
logger = logging.getLogger('datajoint')

def test_getattribute(Subject):
    """Testing Fetch.__call__ with attributes"""
    list1 = sorted(Subject.proj().fetch(as_dict=True), key=(itemgetter('subject_id')))
    list2 = sorted((Subject.fetch(dj.key)), key=(itemgetter('subject_id')))
    for l1, l2 in zip(list1, list2):
        @py_assert1 = l1 == l2
        if not @py_assert1:
            @py_format3 = @pytest_ar._call_reprcompare(('==', ), (@py_assert1,), ('%(py0)s == %(py2)s', ), (l1, l2)) % {'py0':@pytest_ar._saferepr(l1) if 'l1' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(l1) else 'l1',  'py2':@pytest_ar._saferepr(l2) if 'l2' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(l2) else 'l2'}
            @py_format5 = (@pytest_ar._format_assertmsg('Primary key is not returned correctly') + '\n>assert %(py4)s') % {'py4': @py_format3}
            raise AssertionError(@pytest_ar._format_explanation(@py_format5))
        else:
            @py_assert1 = None

    tmp = Subject.fetch(order_by='subject_id')
    subject_notes, key, real_id = Subject.fetch('subject_notes', dj.key, 'real_id')
    np.testing.assert_array_equal(sorted(subject_notes), sorted(tmp['subject_notes']))
    np.testing.assert_array_equal(sorted(real_id), sorted(tmp['real_id']))
    list1 = sorted(key, key=(itemgetter('subject_id')))
    for l1, l2 in zip(list1, list2):
        @py_assert1 = l1 == l2
        if not @py_assert1:
            @py_format3 = @pytest_ar._call_reprcompare(('==', ), (@py_assert1,), ('%(py0)s == %(py2)s', ), (l1, l2)) % {'py0':@pytest_ar._saferepr(l1) if 'l1' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(l1) else 'l1',  'py2':@pytest_ar._saferepr(l2) if 'l2' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(l2) else 'l2'}
            @py_format5 = (@pytest_ar._format_assertmsg('Primary key is not returned correctly') + '\n>assert %(py4)s') % {'py4': @py_format3}
            raise AssertionError(@pytest_ar._format_explanation(@py_format5))
        else:
            @py_assert1 = None


def test_getattribute_for_fetch1(Subject):
    """Testing Fetch1.__call__ with attributes"""
    @py_assert1 = 'subject_id=10'
    @py_assert3 = Subject & @py_assert1
    @py_assert4 = @py_assert3.fetch1
    @py_assert6 = 'subject_id'
    @py_assert8 = @py_assert4(@py_assert6)
    @py_assert11 = 10
    @py_assert10 = @py_assert8 == @py_assert11
    if not @py_assert10:
        @py_format13 = @pytest_ar._call_reprcompare(('==', ), (@py_assert10,), ('%(py9)s\n{%(py9)s = %(py5)s\n{%(py5)s = (%(py0)s & %(py2)s).fetch1\n}(%(py7)s)\n} == %(py12)s', ), (@py_assert8, @py_assert11)) % {'py0':@pytest_ar._saferepr(Subject) if 'Subject' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(Subject) else 'Subject',  'py2':@pytest_ar._saferepr(@py_assert1),  'py5':@pytest_ar._saferepr(@py_assert4),  'py7':@pytest_ar._saferepr(@py_assert6),  'py9':@pytest_ar._saferepr(@py_assert8),  'py12':@pytest_ar._saferepr(@py_assert11)}
        @py_format15 = 'assert %(py14)s' % {'py14': @py_format13}
        raise AssertionError(@pytest_ar._format_explanation(@py_format15))
    @py_assert1 = @py_assert3 = @py_assert4 = @py_assert6 = @py_assert8 = @py_assert10 = @py_assert11 = None
    @py_assert1 = 'subject_id=10'
    @py_assert3 = Subject & @py_assert1
    @py_assert4 = @py_assert3.fetch1
    @py_assert6 = 'subject_id'
    @py_assert8 = 'species'
    @py_assert10 = @py_assert4(@py_assert6, @py_assert8)
    @py_assert13 = (10, 'monkey')
    @py_assert12 = @py_assert10 == @py_assert13
    if not @py_assert12:
        @py_format15 = @pytest_ar._call_reprcompare(('==', ), (@py_assert12,), ('%(py11)s\n{%(py11)s = %(py5)s\n{%(py5)s = (%(py0)s & %(py2)s).fetch1\n}(%(py7)s, %(py9)s)\n} == %(py14)s', ), (@py_assert10, @py_assert13)) % {'py0':@pytest_ar._saferepr(Subject) if 'Subject' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(Subject) else 'Subject',  'py2':@pytest_ar._saferepr(@py_assert1),  'py5':@pytest_ar._saferepr(@py_assert4),  'py7':@pytest_ar._saferepr(@py_assert6),  'py9':@pytest_ar._saferepr(@py_assert8),  'py11':@pytest_ar._saferepr(@py_assert10),  'py14':@pytest_ar._saferepr(@py_assert13)}
        @py_format17 = 'assert %(py16)s' % {'py16': @py_format15}
        raise AssertionError(@pytest_ar._format_explanation(@py_format17))
    @py_assert1 = @py_assert3 = @py_assert4 = @py_assert6 = @py_assert8 = @py_assert10 = @py_assert12 = @py_assert13 = None


def test_order_by(Language):
    """Tests order_by sorting order"""
    languages = Language.contents
    for ord_name, ord_lang in (itertools.product)(*2 * [['ASC', 'DESC']]):
        cur = Language.fetch(order_by=('name ' + ord_name, 'language ' + ord_lang))
        languages.sort(key=(itemgetter(1)), reverse=(ord_lang == 'DESC'))
        languages.sort(key=(itemgetter(0)), reverse=(ord_name == 'DESC'))
        for c, l in zip(cur, languages):
            @py_assert1 = np.all
            @py_assert3 = (cc == ll for cc, ll in zip(c, l))
            @py_assert5 = @py_assert1(@py_assert3)
            if not @py_assert5:
                @py_format7 = (@pytest_ar._format_assertmsg('Sorting order is different') + '\n>assert %(py6)s\n{%(py6)s = %(py2)s\n{%(py2)s = %(py0)s.all\n}(%(py4)s)\n}') % {'py0':@pytest_ar._saferepr(np) if 'np' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(np) else 'np',  'py2':@pytest_ar._saferepr(@py_assert1),  'py4':@pytest_ar._saferepr(@py_assert3),  'py6':@pytest_ar._saferepr(@py_assert5)}
                raise AssertionError(@pytest_ar._format_explanation(@py_format7))
            else:
                @py_assert1 = @py_assert3 = @py_assert5 = None


def test_order_by_default(Language):
    """Tests order_by sorting order with defaults"""
    languages = Language.contents
    cur = Language.fetch(order_by=('language', 'name DESC'))
    languages.sort(key=(itemgetter(0)), reverse=True)
    languages.sort(key=(itemgetter(1)), reverse=False)
    for c, l in zip(cur, languages):
        @py_assert1 = np.all
        @py_assert3 = [cc == ll for cc, ll in zip(c, l)]
        @py_assert5 = @py_assert1(@py_assert3)
        if not @py_assert5:
            @py_format7 = (@pytest_ar._format_assertmsg('Sorting order is different') + '\n>assert %(py6)s\n{%(py6)s = %(py2)s\n{%(py2)s = %(py0)s.all\n}(%(py4)s)\n}') % {'py0':@pytest_ar._saferepr(np) if 'np' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(np) else 'np',  'py2':@pytest_ar._saferepr(@py_assert1),  'py4':@pytest_ar._saferepr(@py_assert3),  'py6':@pytest_ar._saferepr(@py_assert5)}
            raise AssertionError(@pytest_ar._format_explanation(@py_format7))
        else:
            @py_assert1 = @py_assert3 = @py_assert5 = None


def test_limit(Language):
    """Test the limit kwarg"""
    limit = 4
    cur = Language.fetch(limit=limit)
    @py_assert2 = len(cur)
    @py_assert4 = @py_assert2 == limit
    if not @py_assert4:
        @py_format6 = @pytest_ar._call_reprcompare(('==', ), (@py_assert4,), ('%(py3)s\n{%(py3)s = %(py0)s(%(py1)s)\n} == %(py5)s', ), (@py_assert2, limit)) % {'py0':@pytest_ar._saferepr(len) if 'len' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(len) else 'len',  'py1':@pytest_ar._saferepr(cur) if 'cur' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(cur) else 'cur',  'py3':@pytest_ar._saferepr(@py_assert2),  'py5':@pytest_ar._saferepr(limit) if 'limit' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(limit) else 'limit'}
        @py_format8 = (@pytest_ar._format_assertmsg('Length is not correct') + '\n>assert %(py7)s') % {'py7': @py_format6}
        raise AssertionError(@pytest_ar._format_explanation(@py_format8))
    @py_assert2 = @py_assert4 = None


def test_order_by_limit(Language):
    """Test the combination of order by and limit kwargs"""
    languages = Language.contents
    cur = Language.fetch(limit=4, order_by=['language', 'name DESC'])
    languages.sort(key=(itemgetter(0)), reverse=True)
    languages.sort(key=(itemgetter(1)), reverse=False)
    @py_assert2 = len(cur)
    @py_assert5 = 4
    @py_assert4 = @py_assert2 == @py_assert5
    if not @py_assert4:
        @py_format7 = @pytest_ar._call_reprcompare(('==', ), (@py_assert4,), ('%(py3)s\n{%(py3)s = %(py0)s(%(py1)s)\n} == %(py6)s', ), (@py_assert2, @py_assert5)) % {'py0':@pytest_ar._saferepr(len) if 'len' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(len) else 'len',  'py1':@pytest_ar._saferepr(cur) if 'cur' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(cur) else 'cur',  'py3':@pytest_ar._saferepr(@py_assert2),  'py6':@pytest_ar._saferepr(@py_assert5)}
        @py_format9 = (@pytest_ar._format_assertmsg('Length is not correct') + '\n>assert %(py8)s') % {'py8': @py_format7}
        raise AssertionError(@pytest_ar._format_explanation(@py_format9))
    @py_assert2 = @py_assert4 = @py_assert5 = None
    for c, l in list(zip(cur, languages))[:4]:
        @py_assert1 = np.all
        @py_assert3 = [cc == ll for cc, ll in zip(c, l)]
        @py_assert5 = @py_assert1(@py_assert3)
        if not @py_assert5:
            @py_format7 = (@pytest_ar._format_assertmsg('Sorting order is different') + '\n>assert %(py6)s\n{%(py6)s = %(py2)s\n{%(py2)s = %(py0)s.all\n}(%(py4)s)\n}') % {'py0':@pytest_ar._saferepr(np) if 'np' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(np) else 'np',  'py2':@pytest_ar._saferepr(@py_assert1),  'py4':@pytest_ar._saferepr(@py_assert3),  'py6':@pytest_ar._saferepr(@py_assert5)}
            raise AssertionError(@pytest_ar._format_explanation(@py_format7))
        else:
            @py_assert1 = @py_assert3 = @py_assert5 = None


def test_head_tail(Language, User):
    query = User * Language
    n = 5
    frame = query.head(n, format='frame')
    @py_assert3 = pandas.DataFrame
    @py_assert5 = isinstance(frame, @py_assert3)
    if not @py_assert5:
        @py_format7 = 'assert %(py6)s\n{%(py6)s = %(py0)s(%(py1)s, %(py4)s\n{%(py4)s = %(py2)s.DataFrame\n})\n}' % {'py0':@pytest_ar._saferepr(isinstance) if 'isinstance' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(isinstance) else 'isinstance',  'py1':@pytest_ar._saferepr(frame) if 'frame' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(frame) else 'frame',  'py2':@pytest_ar._saferepr(pandas) if 'pandas' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(pandas) else 'pandas',  'py4':@pytest_ar._saferepr(@py_assert3),  'py6':@pytest_ar._saferepr(@py_assert5)}
        raise AssertionError(@pytest_ar._format_explanation(@py_format7))
    @py_assert3 = @py_assert5 = None
    array = query.head(n, format='array')
    @py_assert1 = array.size
    @py_assert3 = @py_assert1 == n
    if not @py_assert3:
        @py_format5 = @pytest_ar._call_reprcompare(('==', ), (@py_assert3,), ('%(py2)s\n{%(py2)s = %(py0)s.size\n} == %(py4)s', ), (@py_assert1, n)) % {'py0':@pytest_ar._saferepr(array) if 'array' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(array) else 'array',  'py2':@pytest_ar._saferepr(@py_assert1),  'py4':@pytest_ar._saferepr(n) if 'n' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(n) else 'n'}
        @py_format7 = 'assert %(py6)s' % {'py6': @py_format5}
        raise AssertionError(@pytest_ar._format_explanation(@py_format7))
    @py_assert1 = @py_assert3 = None
    @py_assert2 = len(frame)
    @py_assert4 = @py_assert2 == n
    if not @py_assert4:
        @py_format6 = @pytest_ar._call_reprcompare(('==', ), (@py_assert4,), ('%(py3)s\n{%(py3)s = %(py0)s(%(py1)s)\n} == %(py5)s', ), (@py_assert2, n)) % {'py0':@pytest_ar._saferepr(len) if 'len' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(len) else 'len',  'py1':@pytest_ar._saferepr(frame) if 'frame' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(frame) else 'frame',  'py3':@pytest_ar._saferepr(@py_assert2),  'py5':@pytest_ar._saferepr(n) if 'n' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(n) else 'n'}
        @py_format8 = 'assert %(py7)s' % {'py7': @py_format6}
        raise AssertionError(@pytest_ar._format_explanation(@py_format8))
    @py_assert2 = @py_assert4 = None
    @py_assert1 = query.primary_key
    @py_assert5 = frame.index
    @py_assert7 = @py_assert5.names
    @py_assert3 = @py_assert1 == @py_assert7
    if not @py_assert3:
        @py_format9 = @pytest_ar._call_reprcompare(('==', ), (@py_assert3,), ('%(py2)s\n{%(py2)s = %(py0)s.primary_key\n} == %(py8)s\n{%(py8)s = %(py6)s\n{%(py6)s = %(py4)s.index\n}.names\n}', ), (@py_assert1, @py_assert7)) % {'py0':@pytest_ar._saferepr(query) if 'query' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(query) else 'query',  'py2':@pytest_ar._saferepr(@py_assert1),  'py4':@pytest_ar._saferepr(frame) if 'frame' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(frame) else 'frame',  'py6':@pytest_ar._saferepr(@py_assert5),  'py8':@pytest_ar._saferepr(@py_assert7)}
        @py_format11 = 'assert %(py10)s' % {'py10': @py_format9}
        raise AssertionError(@pytest_ar._format_explanation(@py_format11))
    @py_assert1 = @py_assert3 = @py_assert5 = @py_assert7 = None
    n = 4
    frame = query.tail(n, format='frame')
    array = query.tail(n, format='array')
    @py_assert1 = array.size
    @py_assert3 = @py_assert1 == n
    if not @py_assert3:
        @py_format5 = @pytest_ar._call_reprcompare(('==', ), (@py_assert3,), ('%(py2)s\n{%(py2)s = %(py0)s.size\n} == %(py4)s', ), (@py_assert1, n)) % {'py0':@pytest_ar._saferepr(array) if 'array' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(array) else 'array',  'py2':@pytest_ar._saferepr(@py_assert1),  'py4':@pytest_ar._saferepr(n) if 'n' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(n) else 'n'}
        @py_format7 = 'assert %(py6)s' % {'py6': @py_format5}
        raise AssertionError(@pytest_ar._format_explanation(@py_format7))
    @py_assert1 = @py_assert3 = None
    @py_assert2 = len(frame)
    @py_assert4 = @py_assert2 == n
    if not @py_assert4:
        @py_format6 = @pytest_ar._call_reprcompare(('==', ), (@py_assert4,), ('%(py3)s\n{%(py3)s = %(py0)s(%(py1)s)\n} == %(py5)s', ), (@py_assert2, n)) % {'py0':@pytest_ar._saferepr(len) if 'len' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(len) else 'len',  'py1':@pytest_ar._saferepr(frame) if 'frame' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(frame) else 'frame',  'py3':@pytest_ar._saferepr(@py_assert2),  'py5':@pytest_ar._saferepr(n) if 'n' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(n) else 'n'}
        @py_format8 = 'assert %(py7)s' % {'py7': @py_format6}
        raise AssertionError(@pytest_ar._format_explanation(@py_format8))
    @py_assert2 = @py_assert4 = None
    @py_assert1 = query.primary_key
    @py_assert5 = frame.index
    @py_assert7 = @py_assert5.names
    @py_assert3 = @py_assert1 == @py_assert7
    if not @py_assert3:
        @py_format9 = @pytest_ar._call_reprcompare(('==', ), (@py_assert3,), ('%(py2)s\n{%(py2)s = %(py0)s.primary_key\n} == %(py8)s\n{%(py8)s = %(py6)s\n{%(py6)s = %(py4)s.index\n}.names\n}', ), (@py_assert1, @py_assert7)) % {'py0':@pytest_ar._saferepr(query) if 'query' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(query) else 'query',  'py2':@pytest_ar._saferepr(@py_assert1),  'py4':@pytest_ar._saferepr(frame) if 'frame' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(frame) else 'frame',  'py6':@pytest_ar._saferepr(@py_assert5),  'py8':@pytest_ar._saferepr(@py_assert7)}
        @py_format11 = 'assert %(py10)s' % {'py10': @py_format9}
        raise AssertionError(@pytest_ar._format_explanation(@py_format11))
    @py_assert1 = @py_assert3 = @py_assert5 = @py_assert7 = None


def test_limit_offset(Language):
    """Test the limit and offset kwargs together"""
    languages = Language.contents
    cur = Language.fetch(offset=2, limit=4, order_by=['language', 'name DESC'])
    languages.sort(key=(itemgetter(0)), reverse=True)
    languages.sort(key=(itemgetter(1)), reverse=False)
    @py_assert2 = len(cur)
    @py_assert5 = 4
    @py_assert4 = @py_assert2 == @py_assert5
    if not @py_assert4:
        @py_format7 = @pytest_ar._call_reprcompare(('==', ), (@py_assert4,), ('%(py3)s\n{%(py3)s = %(py0)s(%(py1)s)\n} == %(py6)s', ), (@py_assert2, @py_assert5)) % {'py0':@pytest_ar._saferepr(len) if 'len' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(len) else 'len',  'py1':@pytest_ar._saferepr(cur) if 'cur' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(cur) else 'cur',  'py3':@pytest_ar._saferepr(@py_assert2),  'py6':@pytest_ar._saferepr(@py_assert5)}
        @py_format9 = (@pytest_ar._format_assertmsg('Length is not correct') + '\n>assert %(py8)s') % {'py8': @py_format7}
        raise AssertionError(@pytest_ar._format_explanation(@py_format9))
    @py_assert2 = @py_assert4 = @py_assert5 = None
    for c, l in list(zip(cur, languages[2:6])):
        @py_assert1 = np.all
        @py_assert3 = [cc == ll for cc, ll in zip(c, l)]
        @py_assert5 = @py_assert1(@py_assert3)
        if not @py_assert5:
            @py_format7 = (@pytest_ar._format_assertmsg('Sorting order is different') + '\n>assert %(py6)s\n{%(py6)s = %(py2)s\n{%(py2)s = %(py0)s.all\n}(%(py4)s)\n}') % {'py0':@pytest_ar._saferepr(np) if 'np' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(np) else 'np',  'py2':@pytest_ar._saferepr(@py_assert1),  'py4':@pytest_ar._saferepr(@py_assert3),  'py6':@pytest_ar._saferepr(@py_assert5)}
            raise AssertionError(@pytest_ar._format_explanation(@py_format7))
        else:
            @py_assert1 = @py_assert3 = @py_assert5 = None


def test_iter(Language):
    """Test iterator"""
    languages = Language.contents
    cur = Language.fetch(order_by=['language', 'name DESC'])
    languages.sort(key=(itemgetter(0)), reverse=True)
    languages.sort(key=(itemgetter(1)), reverse=False)
    for (name, lang), (tname, tlang) in list(zip(cur, languages)):
        @py_assert1 = []
        @py_assert3 = name == tname
        @py_assert0 = @py_assert3
        if @py_assert3:
            @py_assert9 = lang == tlang
            @py_assert0 = @py_assert9
        else:
            if not @py_assert0:
                @py_format5 = @pytest_ar._call_reprcompare(('==', ), (@py_assert3,), ('%(py2)s == %(py4)s', ), (name, tname)) % {'py2':@pytest_ar._saferepr(name) if 'name' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(name) else 'name',  'py4':@pytest_ar._saferepr(tname) if 'tname' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(tname) else 'tname'}
                @py_format7 = '%(py6)s' % {'py6': @py_format5}
                @py_assert1.append(@py_format7)
                if @py_assert3:
                    @py_format11 = @pytest_ar._call_reprcompare(('==', ), (@py_assert9,), ('%(py8)s == %(py10)s', ), (lang, tlang)) % {'py8':@pytest_ar._saferepr(lang) if 'lang' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(lang) else 'lang',  'py10':@pytest_ar._saferepr(tlang) if 'tlang' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(tlang) else 'tlang'}
                    @py_format13 = '%(py12)s' % {'py12': @py_format11}
                    @py_assert1.append(@py_format13)
                @py_format14 = @pytest_ar._format_boolop(@py_assert1, 0) % {}
                @py_format16 = (@pytest_ar._format_assertmsg('Values are not the same') + '\n>assert %(py15)s') % {'py15': @py_format14}
                raise AssertionError(@pytest_ar._format_explanation(@py_format16))
            @py_assert0 = @py_assert1 = @py_assert3 = @py_assert9 = None

    cur = Language.fetch(as_dict=True, order_by=('language', 'name DESC'))
    for row, (tname, tlang) in list(zip(cur, languages)):
        @py_assert1 = []
        @py_assert2 = row['name']
        @py_assert4 = @py_assert2 == tname
        @py_assert0 = @py_assert4
        if @py_assert4:
            @py_assert9 = row['language']
            @py_assert11 = @py_assert9 == tlang
            @py_assert0 = @py_assert11
        else:
            if not @py_assert0:
                @py_format6 = @pytest_ar._call_reprcompare(('==', ), (@py_assert4,), ('%(py3)s == %(py5)s', ), (@py_assert2, tname)) % {'py3':@pytest_ar._saferepr(@py_assert2),  'py5':@pytest_ar._saferepr(tname) if 'tname' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(tname) else 'tname'}
                @py_format8 = '%(py7)s' % {'py7': @py_format6}
                @py_assert1.append(@py_format8)
                if @py_assert4:
                    @py_format13 = @pytest_ar._call_reprcompare(('==', ), (@py_assert11,), ('%(py10)s == %(py12)s', ), (@py_assert9, tlang)) % {'py10':@pytest_ar._saferepr(@py_assert9),  'py12':@pytest_ar._saferepr(tlang) if 'tlang' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(tlang) else 'tlang'}
                    @py_format15 = '%(py14)s' % {'py14': @py_format13}
                    @py_assert1.append(@py_format15)
                @py_format16 = @pytest_ar._format_boolop(@py_assert1, 0) % {}
                @py_format18 = (@pytest_ar._format_assertmsg('Values are not the same') + '\n>assert %(py17)s') % {'py17': @py_format16}
                raise AssertionError(@pytest_ar._format_explanation(@py_format18))
            @py_assert0 = @py_assert1 = @py_assert2 = @py_assert4 = @py_assert9 = @py_assert11 = None


def test_keys(Language):
    """test key fetch"""
    languages = Language.contents
    languages.sort(key=(itemgetter(0)), reverse=True)
    languages.sort(key=(itemgetter(1)), reverse=False)
    cur = Language.fetch('name', 'language', order_by=('language', 'name DESC'))
    cur2 = list(Language.fetch('KEY', order_by=['language', 'name DESC']))
    for c, c2 in zip(zip(*cur), cur2):
        @py_assert4 = c2.values
        @py_assert6 = @py_assert4()
        @py_assert8 = tuple(@py_assert6)
        @py_assert1 = c == @py_assert8
        if not @py_assert1:
            @py_format10 = @pytest_ar._call_reprcompare(('==', ), (@py_assert1,), ('%(py0)s == %(py9)s\n{%(py9)s = %(py2)s(%(py7)s\n{%(py7)s = %(py5)s\n{%(py5)s = %(py3)s.values\n}()\n})\n}', ), (c, @py_assert8)) % {'py0':@pytest_ar._saferepr(c) if 'c' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(c) else 'c',  'py2':@pytest_ar._saferepr(tuple) if 'tuple' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(tuple) else 'tuple',  'py3':@pytest_ar._saferepr(c2) if 'c2' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(c2) else 'c2',  'py5':@pytest_ar._saferepr(@py_assert4),  'py7':@pytest_ar._saferepr(@py_assert6),  'py9':@pytest_ar._saferepr(@py_assert8)}
            @py_format12 = (@pytest_ar._format_assertmsg('Values are not the same') + '\n>assert %(py11)s') % {'py11': @py_format10}
            raise AssertionError(@pytest_ar._format_explanation(@py_format12))
        else:
            @py_assert1 = @py_assert4 = @py_assert6 = @py_assert8 = None


def test_attributes_as_dict(Subject):
    attrs = ('species', 'date_of_birth')
    result = (Subject.fetch)(*attrs, **{'as_dict': True})
    @py_assert1 = []
    @py_assert4 = bool(result)
    @py_assert0 = @py_assert4
    if @py_assert4:
        @py_assert9 = len(result)
        @py_assert14 = Subject()
        @py_assert16 = len(@py_assert14)
        @py_assert11 = @py_assert9 == @py_assert16
        @py_assert0 = @py_assert11
    if not @py_assert0:
        @py_format6 = '%(py5)s\n{%(py5)s = %(py2)s(%(py3)s)\n}' % {'py2':@pytest_ar._saferepr(bool) if 'bool' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(bool) else 'bool',  'py3':@pytest_ar._saferepr(result) if 'result' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(result) else 'result',  'py5':@pytest_ar._saferepr(@py_assert4)}
        @py_assert1.append(@py_format6)
        if @py_assert4:
            @py_format18 = @pytest_ar._call_reprcompare(('==', ), (@py_assert11,), ('%(py10)s\n{%(py10)s = %(py7)s(%(py8)s)\n} == %(py17)s\n{%(py17)s = %(py12)s(%(py15)s\n{%(py15)s = %(py13)s()\n})\n}', ), (@py_assert9, @py_assert16)) % {'py7':@pytest_ar._saferepr(len) if 'len' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(len) else 'len',  'py8':@pytest_ar._saferepr(result) if 'result' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(result) else 'result',  'py10':@pytest_ar._saferepr(@py_assert9),  'py12':@pytest_ar._saferepr(len) if 'len' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(len) else 'len',  'py13':@pytest_ar._saferepr(Subject) if 'Subject' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(Subject) else 'Subject',  'py15':@pytest_ar._saferepr(@py_assert14),  'py17':@pytest_ar._saferepr(@py_assert16)}
            @py_format20 = '%(py19)s' % {'py19': @py_format18}
            @py_assert1.append(@py_format20)
        @py_format21 = @pytest_ar._format_boolop(@py_assert1, 0) % {}
        @py_format23 = 'assert %(py22)s' % {'py22': @py_format21}
        raise AssertionError(@pytest_ar._format_explanation(@py_format23))
    @py_assert0 = @py_assert1 = @py_assert4 = @py_assert9 = @py_assert11 = @py_assert14 = @py_assert16 = None
    @py_assert1 = result[0]
    @py_assert3 = set(@py_assert1)
    @py_assert8 = set(attrs)
    @py_assert5 = @py_assert3 == @py_assert8
    if not @py_assert5:
        @py_format10 = @pytest_ar._call_reprcompare(('==', ), (@py_assert5,), ('%(py4)s\n{%(py4)s = %(py0)s(%(py2)s)\n} == %(py9)s\n{%(py9)s = %(py6)s(%(py7)s)\n}', ), (@py_assert3, @py_assert8)) % {'py0':@pytest_ar._saferepr(set) if 'set' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(set) else 'set',  'py2':@pytest_ar._saferepr(@py_assert1),  'py4':@pytest_ar._saferepr(@py_assert3),  'py6':@pytest_ar._saferepr(set) if 'set' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(set) else 'set',  'py7':@pytest_ar._saferepr(attrs) if 'attrs' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(attrs) else 'attrs',  'py9':@pytest_ar._saferepr(@py_assert8)}
        @py_format12 = 'assert %(py11)s' % {'py11': @py_format10}
        raise AssertionError(@pytest_ar._format_explanation(@py_format12))
    @py_assert1 = @py_assert3 = @py_assert5 = @py_assert8 = None


def test_fetch1_step1(Language):
    key = {'name':'Edgar', 
     'language':'Japanese'}
    true = Language.contents[-1]
    dat = (Language & key).fetch1()
    for k, (ke, c) in zip(true, dat.items()):
        @py_assert1 = k == c
        @py_assert6 = Language & key
        @py_assert7 = @py_assert6.fetch1
        @py_assert10 = @py_assert7(ke)
        @py_assert2 = c == @py_assert10
        if not (@py_assert1 and @py_assert2):
            @py_format12 = @pytest_ar._call_reprcompare(('==', '=='), (@py_assert1, @py_assert2), ('%(py0)s == %(py3)s',
                                                                                                   '%(py3)s == %(py11)s\n{%(py11)s = %(py8)s\n{%(py8)s = (%(py4)s & %(py5)s).fetch1\n}(%(py9)s)\n}'), (k, c, @py_assert10)) % {'py0':@pytest_ar._saferepr(k) if 'k' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(k) else 'k',  'py3':@pytest_ar._saferepr(c) if 'c' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(c) else 'c',  'py4':@pytest_ar._saferepr(Language) if 'Language' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(Language) else 'Language',  'py5':@pytest_ar._saferepr(key) if 'key' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(key) else 'key',  'py8':@pytest_ar._saferepr(@py_assert7),  'py9':@pytest_ar._saferepr(ke) if 'ke' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(ke) else 'ke',  'py11':@pytest_ar._saferepr(@py_assert10)}
            @py_format14 = (@pytest_ar._format_assertmsg('Values are not the same') + '\n>assert %(py13)s') % {'py13': @py_format12}
            raise AssertionError(@pytest_ar._format_explanation(@py_format14))
        else:
            @py_assert1 = @py_assert2 = @py_assert6 = @py_assert7 = @py_assert10 = None


def test_misspelled_attribute(Language):
    with pytest.raises(dj.DataJointError):
        f = (Language & 'lang = "ENGLISH"').fetch()


def test_repr(Subject):
    """Test string representation of fetch, returning table preview"""
    repr = Subject.fetch.__repr__()
    n = len(repr.strip().split('\n'))
    limit = dj.config['display.limit']
    @py_assert1 = 3
    @py_assert3 = n - @py_assert1
    @py_assert4 = @py_assert3 <= limit
    if not @py_assert4:
        @py_format6 = @pytest_ar._call_reprcompare(('<=', ), (@py_assert4,), ('(%(py0)s - %(py2)s) <= %(py5)s', ), (@py_assert3, limit)) % {'py0':@pytest_ar._saferepr(n) if 'n' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(n) else 'n',  'py2':@pytest_ar._saferepr(@py_assert1),  'py5':@pytest_ar._saferepr(limit) if 'limit' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(limit) else 'limit'}
        @py_format8 = 'assert %(py7)s' % {'py7': @py_format6}
        raise AssertionError(@pytest_ar._format_explanation(@py_format8))
    @py_assert1 = @py_assert3 = @py_assert4 = None


def test_fetch_none(Language):
    """Test preparing attributes for getitem"""
    with pytest.raises(dj.DataJointError):
        Language.fetch(None)


def test_asdict(Language):
    """Test returns as dictionaries"""
    d = Language.fetch(as_dict=True)
    for dd in d:
        @py_assert3 = isinstance(dd, dict)
        if not @py_assert3:
            @py_format5 = 'assert %(py4)s\n{%(py4)s = %(py0)s(%(py1)s, %(py2)s)\n}' % {'py0':@pytest_ar._saferepr(isinstance) if 'isinstance' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(isinstance) else 'isinstance',  'py1':@pytest_ar._saferepr(dd) if 'dd' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(dd) else 'dd',  'py2':@pytest_ar._saferepr(dict) if 'dict' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(dict) else 'dict',  'py4':@pytest_ar._saferepr(@py_assert3)}
            raise AssertionError(@pytest_ar._format_explanation(@py_format5))
        else:
            @py_assert3 = None


def test_offset(Language):
    """Tests offset"""
    cur = Language.fetch(limit=4, offset=1, order_by=['language', 'name DESC'])
    languages = Language.contents
    languages.sort(key=(itemgetter(0)), reverse=True)
    languages.sort(key=(itemgetter(1)), reverse=False)
    @py_assert2 = len(cur)
    @py_assert5 = 4
    @py_assert4 = @py_assert2 == @py_assert5
    if not @py_assert4:
        @py_format7 = @pytest_ar._call_reprcompare(('==', ), (@py_assert4,), ('%(py3)s\n{%(py3)s = %(py0)s(%(py1)s)\n} == %(py6)s', ), (@py_assert2, @py_assert5)) % {'py0':@pytest_ar._saferepr(len) if 'len' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(len) else 'len',  'py1':@pytest_ar._saferepr(cur) if 'cur' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(cur) else 'cur',  'py3':@pytest_ar._saferepr(@py_assert2),  'py6':@pytest_ar._saferepr(@py_assert5)}
        @py_format9 = (@pytest_ar._format_assertmsg('Length is not correct') + '\n>assert %(py8)s') % {'py8': @py_format7}
        raise AssertionError(@pytest_ar._format_explanation(@py_format9))
    @py_assert2 = @py_assert4 = @py_assert5 = None
    for c, l in list(zip(cur, languages[1:]))[:4]:
        @py_assert1 = np.all
        @py_assert3 = [cc == ll for cc, ll in zip(c, l)]
        @py_assert5 = @py_assert1(@py_assert3)
        if not @py_assert5:
            @py_format7 = (@pytest_ar._format_assertmsg('Sorting order is different') + '\n>assert %(py6)s\n{%(py6)s = %(py2)s\n{%(py2)s = %(py0)s.all\n}(%(py4)s)\n}') % {'py0':@pytest_ar._saferepr(np) if 'np' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(np) else 'np',  'py2':@pytest_ar._saferepr(@py_assert1),  'py4':@pytest_ar._saferepr(@py_assert3),  'py6':@pytest_ar._saferepr(@py_assert5)}
            raise AssertionError(@pytest_ar._format_explanation(@py_format7))
        else:
            @py_assert1 = @py_assert3 = @py_assert5 = None


def test_limit_warning(Language):
    """Tests whether warning is raised if offset is used without limit."""
    log_capture = io.StringIO()
    stream_handler = logging.StreamHandler(log_capture)
    log_format = logging.Formatter('[%(asctime)s][%(funcName)s][%(levelname)s]: %(message)s')
    stream_handler.setFormatter(log_format)
    stream_handler.set_name('test_limit_warning')
    logger.addHandler(stream_handler)
    Language.fetch(offset=1)
    log_contents = log_capture.getvalue()
    log_capture.close()
    for handler in logger.handlers:
        if handler.name == 'test_limit_warning':
            logger.removeHandler(handler)

    @py_assert0 = '[WARNING]: Offset set, but no limit.'
    @py_assert2 = @py_assert0 in log_contents
    if not @py_assert2:
        @py_format4 = @pytest_ar._call_reprcompare(('in', ), (@py_assert2,), ('%(py1)s in %(py3)s', ), (@py_assert0, log_contents)) % {'py1':@pytest_ar._saferepr(@py_assert0),  'py3':@pytest_ar._saferepr(log_contents) if 'log_contents' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(log_contents) else 'log_contents'}
        @py_format6 = 'assert %(py5)s' % {'py5': @py_format4}
        raise AssertionError(@pytest_ar._format_explanation(@py_format6))
    @py_assert0 = @py_assert2 = None


def test_len(Language):
    """Tests __len__"""
    @py_assert2 = Language.fetch
    @py_assert4 = @py_assert2()
    @py_assert6 = len(@py_assert4)
    @py_assert11 = Language()
    @py_assert13 = len(@py_assert11)
    @py_assert8 = @py_assert6 == @py_assert13
    if not @py_assert8:
        @py_format15 = @pytest_ar._call_reprcompare(('==', ), (@py_assert8,), ('%(py7)s\n{%(py7)s = %(py0)s(%(py5)s\n{%(py5)s = %(py3)s\n{%(py3)s = %(py1)s.fetch\n}()\n})\n} == %(py14)s\n{%(py14)s = %(py9)s(%(py12)s\n{%(py12)s = %(py10)s()\n})\n}', ), (@py_assert6, @py_assert13)) % {'py0':@pytest_ar._saferepr(len) if 'len' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(len) else 'len',  'py1':@pytest_ar._saferepr(Language) if 'Language' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(Language) else 'Language',  'py3':@pytest_ar._saferepr(@py_assert2),  'py5':@pytest_ar._saferepr(@py_assert4),  'py7':@pytest_ar._saferepr(@py_assert6),  'py9':@pytest_ar._saferepr(len) if 'len' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(len) else 'len',  'py10':@pytest_ar._saferepr(Language) if 'Language' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(Language) else 'Language',  'py12':@pytest_ar._saferepr(@py_assert11),  'py14':@pytest_ar._saferepr(@py_assert13)}
        @py_format17 = (@pytest_ar._format_assertmsg('__len__ is not behaving properly') + '\n>assert %(py16)s') % {'py16': @py_format15}
        raise AssertionError(@pytest_ar._format_explanation(@py_format17))
    @py_assert2 = @py_assert4 = @py_assert6 = @py_assert8 = @py_assert11 = @py_assert13 = None


def test_fetch1_step2(Language):
    """Tests whether fetch1 raises error"""
    with pytest.raises(dj.DataJointError):
        Language.fetch1()


def test_fetch1_step3(Language):
    """Tests whether fetch1 raises error"""
    with pytest.raises(dj.DataJointError):
        Language.fetch1('name')


def test_decimal(DecimalPrimaryKey):
    """Tests that decimal fields are correctly fetched and used in restrictions, see issue #334"""
    rel = DecimalPrimaryKey()
    rel.insert1([decimal.Decimal('3.1415926')])
    keys = rel.fetch()
    @py_assert2 = keys[0]
    @py_assert4 = rel & @py_assert2
    @py_assert5 = len(@py_assert4)
    @py_assert8 = 1
    @py_assert7 = @py_assert5 == @py_assert8
    if not @py_assert7:
        @py_format10 = @pytest_ar._call_reprcompare(('==', ), (@py_assert7,), ('%(py6)s\n{%(py6)s = %(py0)s((%(py1)s & %(py3)s))\n} == %(py9)s', ), (@py_assert5, @py_assert8)) % {'py0':@pytest_ar._saferepr(len) if 'len' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(len) else 'len',  'py1':@pytest_ar._saferepr(rel) if 'rel' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(rel) else 'rel',  'py3':@pytest_ar._saferepr(@py_assert2),  'py6':@pytest_ar._saferepr(@py_assert5),  'py9':@pytest_ar._saferepr(@py_assert8)}
        @py_format12 = 'assert %(py11)s' % {'py11': @py_format10}
        raise AssertionError(@pytest_ar._format_explanation(@py_format12))
    @py_assert2 = @py_assert4 = @py_assert5 = @py_assert7 = @py_assert8 = None
    keys = rel.fetch(dj.key)
    @py_assert2 = keys[1]
    @py_assert4 = rel & @py_assert2
    @py_assert5 = len(@py_assert4)
    @py_assert8 = 1
    @py_assert7 = @py_assert5 == @py_assert8
    if not @py_assert7:
        @py_format10 = @pytest_ar._call_reprcompare(('==', ), (@py_assert7,), ('%(py6)s\n{%(py6)s = %(py0)s((%(py1)s & %(py3)s))\n} == %(py9)s', ), (@py_assert5, @py_assert8)) % {'py0':@pytest_ar._saferepr(len) if 'len' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(len) else 'len',  'py1':@pytest_ar._saferepr(rel) if 'rel' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(rel) else 'rel',  'py3':@pytest_ar._saferepr(@py_assert2),  'py6':@pytest_ar._saferepr(@py_assert5),  'py9':@pytest_ar._saferepr(@py_assert8)}
        @py_format12 = 'assert %(py11)s' % {'py11': @py_format10}
        raise AssertionError(@pytest_ar._format_explanation(@py_format12))
    @py_assert2 = @py_assert4 = @py_assert5 = @py_assert7 = @py_assert8 = None


def test_nullable_numbers(NullableNumbers):
    """test mixture of values and nulls in numeric attributes"""
    np.random.seed(800)
    table = NullableNumbers()
    table.insert(((k, np.random.randn(), np.random.randint(-1000, 1000), np.random.randn()) for k in range(10)))
    table.insert1((100, None, None, None))
    f, d, i = table.fetch('fvalue', 'dvalue', 'ivalue')
    @py_assert0 = None
    @py_assert2 = @py_assert0 in i
    if not @py_assert2:
        @py_format4 = @pytest_ar._call_reprcompare(('in', ), (@py_assert2,), ('%(py1)s in %(py3)s', ), (@py_assert0, i)) % {'py1':@pytest_ar._saferepr(@py_assert0),  'py3':@pytest_ar._saferepr(i) if 'i' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(i) else 'i'}
        @py_format6 = 'assert %(py5)s' % {'py5': @py_format4}
        raise AssertionError(@pytest_ar._format_explanation(@py_format6))
    @py_assert0 = @py_assert2 = None
    @py_assert2 = np.isnan
    @py_assert5 = @py_assert2(d)
    @py_assert7 = any(@py_assert5)
    if not @py_assert7:
        @py_format9 = 'assert %(py8)s\n{%(py8)s = %(py0)s(%(py6)s\n{%(py6)s = %(py3)s\n{%(py3)s = %(py1)s.isnan\n}(%(py4)s)\n})\n}' % {'py0':@pytest_ar._saferepr(any) if 'any' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(any) else 'any',  'py1':@pytest_ar._saferepr(np) if 'np' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(np) else 'np',  'py3':@pytest_ar._saferepr(@py_assert2),  'py4':@pytest_ar._saferepr(d) if 'd' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(d) else 'd',  'py6':@pytest_ar._saferepr(@py_assert5),  'py8':@pytest_ar._saferepr(@py_assert7)}
        raise AssertionError(@pytest_ar._format_explanation(@py_format9))
    @py_assert2 = @py_assert5 = @py_assert7 = None
    @py_assert2 = np.isnan
    @py_assert5 = @py_assert2(f)
    @py_assert7 = any(@py_assert5)
    if not @py_assert7:
        @py_format9 = 'assert %(py8)s\n{%(py8)s = %(py0)s(%(py6)s\n{%(py6)s = %(py3)s\n{%(py3)s = %(py1)s.isnan\n}(%(py4)s)\n})\n}' % {'py0':@pytest_ar._saferepr(any) if 'any' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(any) else 'any',  'py1':@pytest_ar._saferepr(np) if 'np' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(np) else 'np',  'py3':@pytest_ar._saferepr(@py_assert2),  'py4':@pytest_ar._saferepr(f) if 'f' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(f) else 'f',  'py6':@pytest_ar._saferepr(@py_assert5),  'py8':@pytest_ar._saferepr(@py_assert7)}
        raise AssertionError(@pytest_ar._format_explanation(@py_format9))
    @py_assert2 = @py_assert5 = @py_assert7 = None


def test_fetch_format(Subject):
    """test fetch_format='frame'"""
    with dj.config(fetch_format='frame'):
        list1 = sorted(Subject.proj().fetch(as_dict=True), key=(itemgetter('subject_id')))
        list2 = sorted((Subject.fetch(dj.key)), key=(itemgetter('subject_id')))
        for l1, l2 in zip(list1, list2):
            @py_assert1 = l1 == l2
            if not @py_assert1:
                @py_format3 = @pytest_ar._call_reprcompare(('==', ), (@py_assert1,), ('%(py0)s == %(py2)s', ), (l1, l2)) % {'py0':@pytest_ar._saferepr(l1) if 'l1' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(l1) else 'l1',  'py2':@pytest_ar._saferepr(l2) if 'l2' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(l2) else 'l2'}
                @py_format5 = (@pytest_ar._format_assertmsg('Primary key is not returned correctly') + '\n>assert %(py4)s') % {'py4': @py_format3}
                raise AssertionError(@pytest_ar._format_explanation(@py_format5))
            else:
                @py_assert1 = None

        tmp = Subject.fetch(order_by='subject_id')
        @py_assert3 = pandas.DataFrame
        @py_assert5 = isinstance(tmp, @py_assert3)
        if not @py_assert5:
            @py_format7 = 'assert %(py6)s\n{%(py6)s = %(py0)s(%(py1)s, %(py4)s\n{%(py4)s = %(py2)s.DataFrame\n})\n}' % {'py0':@pytest_ar._saferepr(isinstance) if 'isinstance' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(isinstance) else 'isinstance',  'py1':@pytest_ar._saferepr(tmp) if 'tmp' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(tmp) else 'tmp',  'py2':@pytest_ar._saferepr(pandas) if 'pandas' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(pandas) else 'pandas',  'py4':@pytest_ar._saferepr(@py_assert3),  'py6':@pytest_ar._saferepr(@py_assert5)}
            raise AssertionError(@pytest_ar._format_explanation(@py_format7))
        @py_assert3 = @py_assert5 = None
        tmp = tmp.to_records()
        subject_notes, key, real_id = Subject.fetch('subject_notes', dj.key, 'real_id')
        np.testing.assert_array_equal(sorted(subject_notes), sorted(tmp['subject_notes']))
        np.testing.assert_array_equal(sorted(real_id), sorted(tmp['real_id']))
        list1 = sorted(key, key=(itemgetter('subject_id')))
        for l1, l2 in zip(list1, list2):
            @py_assert1 = l1 == l2
            if not @py_assert1:
                @py_format3 = @pytest_ar._call_reprcompare(('==', ), (@py_assert1,), ('%(py0)s == %(py2)s', ), (l1, l2)) % {'py0':@pytest_ar._saferepr(l1) if 'l1' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(l1) else 'l1',  'py2':@pytest_ar._saferepr(l2) if 'l2' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(l2) else 'l2'}
                @py_format5 = (@pytest_ar._format_assertmsg('Primary key is not returned correctly') + '\n>assert %(py4)s') % {'py4': @py_format3}
                raise AssertionError(@pytest_ar._format_explanation(@py_format5))
            else:
                @py_assert1 = None


def test_key_fetch1(Subject):
    """test KEY fetch1 - issue #976"""
    with dj.config(fetch_format='array'):
        k1 = (Subject & 'subject_id=10').fetch1('KEY')
    with dj.config(fetch_format='frame'):
        k2 = (Subject & 'subject_id=10').fetch1('KEY')
    @py_assert1 = k1 == k2
    if not @py_assert1:
        @py_format3 = @pytest_ar._call_reprcompare(('==', ), (@py_assert1,), ('%(py0)s == %(py2)s', ), (k1, k2)) % {'py0':@pytest_ar._saferepr(k1) if 'k1' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(k1) else 'k1',  'py2':@pytest_ar._saferepr(k2) if 'k2' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(k2) else 'k2'}
        @py_format5 = 'assert %(py4)s' % {'py4': @py_format3}
        raise AssertionError(@pytest_ar._format_explanation(@py_format5))
    @py_assert1 = None


def test_same_secondary_attribute(Child, Parent):
    children = (Child * Parent().proj()).fetch()['name']
    @py_assert2 = len(children)
    @py_assert5 = 1
    @py_assert4 = @py_assert2 == @py_assert5
    if not @py_assert4:
        @py_format7 = @pytest_ar._call_reprcompare(('==', ), (@py_assert4,), ('%(py3)s\n{%(py3)s = %(py0)s(%(py1)s)\n} == %(py6)s', ), (@py_assert2, @py_assert5)) % {'py0':@pytest_ar._saferepr(len) if 'len' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(len) else 'len',  'py1':@pytest_ar._saferepr(children) if 'children' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(children) else 'children',  'py3':@pytest_ar._saferepr(@py_assert2),  'py6':@pytest_ar._saferepr(@py_assert5)}
        @py_format9 = 'assert %(py8)s' % {'py8': @py_format7}
        raise AssertionError(@pytest_ar._format_explanation(@py_format9))
    @py_assert2 = @py_assert4 = @py_assert5 = None
    @py_assert0 = children[0]
    @py_assert3 = 'Dan'
    @py_assert2 = @py_assert0 == @py_assert3
    if not @py_assert2:
        @py_format5 = @pytest_ar._call_reprcompare(('==', ), (@py_assert2,), ('%(py1)s == %(py4)s', ), (@py_assert0, @py_assert3)) % {'py1':@pytest_ar._saferepr(@py_assert0),  'py4':@pytest_ar._saferepr(@py_assert3)}
        @py_format7 = 'assert %(py6)s' % {'py6': @py_format5}
        raise AssertionError(@pytest_ar._format_explanation(@py_format7))
    @py_assert0 = @py_assert2 = @py_assert3 = None


def test_query_caching(TTest3):
    os.mkdir(os.path.expanduser('~/dj_query_cache'))
    with dj.config(query_cache=(os.path.expanduser('~/dj_query_cache'))):
        conn = TTest3.connection
        TTest3.insert([dict(key=(100 + i), value=(200 + i)) for i in range(2)])
        conn.set_query_cache(query_cache='main')
        cached_res = TTest3().fetch()
        try:
            TTest3.insert([dict(key=(200 + i), value=(400 + i)) for i in range(2)])
            @py_assert0 = False
            if not @py_assert0:
                @py_format2 = (@pytest_ar._format_assertmsg('Insert allowed while query caching enabled') + '\n>assert %(py1)s') % {'py1': @pytest_ar._saferepr(@py_assert0)}
                raise AssertionError(@pytest_ar._format_explanation(@py_format2))
            @py_assert0 = None
        except dj.DataJointError:
            conn.set_query_cache()

        TTest3.insert([dict(key=(600 + i), value=(800 + i)) for i in range(2)])
        conn.set_query_cache(query_cache='main')
        previous_cache = TTest3().fetch()
        @py_assert1 = [c == p for c, p in zip(cached_res, previous_cache)]
        @py_assert3 = all(@py_assert1)
        if not @py_assert3:
            @py_format5 = 'assert %(py4)s\n{%(py4)s = %(py0)s(%(py2)s)\n}' % {'py0':@pytest_ar._saferepr(all) if 'all' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(all) else 'all',  'py2':@pytest_ar._saferepr(@py_assert1),  'py4':@pytest_ar._saferepr(@py_assert3)}
            raise AssertionError(@pytest_ar._format_explanation(@py_format5))
        @py_assert1 = @py_assert3 = None
        conn.set_query_cache()
        uncached_res = TTest3().fetch()
        @py_assert2 = len(uncached_res)
        @py_assert7 = len(cached_res)
        @py_assert4 = @py_assert2 > @py_assert7
        if not @py_assert4:
            @py_format9 = @pytest_ar._call_reprcompare(('>', ), (@py_assert4,), ('%(py3)s\n{%(py3)s = %(py0)s(%(py1)s)\n} > %(py8)s\n{%(py8)s = %(py5)s(%(py6)s)\n}', ), (@py_assert2, @py_assert7)) % {'py0':@pytest_ar._saferepr(len) if 'len' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(len) else 'len',  'py1':@pytest_ar._saferepr(uncached_res) if 'uncached_res' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(uncached_res) else 'uncached_res',  'py3':@pytest_ar._saferepr(@py_assert2),  'py5':@pytest_ar._saferepr(len) if 'len' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(len) else 'len',  'py6':@pytest_ar._saferepr(cached_res) if 'cached_res' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(cached_res) else 'cached_res',  'py8':@pytest_ar._saferepr(@py_assert7)}
            @py_format11 = 'assert %(py10)s' % {'py10': @py_format9}
            raise AssertionError(@pytest_ar._format_explanation(@py_format11))
        @py_assert2 = @py_assert4 = @py_assert7 = None
        conn.purge_query_cache()
    os.rmdir(os.path.expanduser('~/dj_query_cache'))


def test_fetch_group_by(Parent):
    @py_assert1 = Parent()
    @py_assert3 = @py_assert1.fetch
    @py_assert5 = 'KEY'
    @py_assert7 = 'name'
    @py_assert9 = @py_assert3(@py_assert5, order_by=@py_assert7)
    @py_assert12 = [
     {'parent_id': 1}]
    @py_assert11 = @py_assert9 == @py_assert12
    if not @py_assert11:
        @py_format14 = @pytest_ar._call_reprcompare(('==', ), (@py_assert11,), ('%(py10)s\n{%(py10)s = %(py4)s\n{%(py4)s = %(py2)s\n{%(py2)s = %(py0)s()\n}.fetch\n}(%(py6)s, order_by=%(py8)s)\n} == %(py13)s', ), (@py_assert9, @py_assert12)) % {'py0':@pytest_ar._saferepr(Parent) if 'Parent' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(Parent) else 'Parent',  'py2':@pytest_ar._saferepr(@py_assert1),  'py4':@pytest_ar._saferepr(@py_assert3),  'py6':@pytest_ar._saferepr(@py_assert5),  'py8':@pytest_ar._saferepr(@py_assert7),  'py10':@pytest_ar._saferepr(@py_assert9),  'py13':@pytest_ar._saferepr(@py_assert12)}
        @py_format16 = 'assert %(py15)s' % {'py15': @py_format14}
        raise AssertionError(@pytest_ar._format_explanation(@py_format16))
    @py_assert1 = @py_assert3 = @py_assert5 = @py_assert7 = @py_assert9 = @py_assert11 = @py_assert12 = None


def test_dj_u_distinct(Stimulus):
    contents = [
     (1, 2, 3), (2, 2, 3), (3, 3, 2), (4, 5, 5)]
    Stimulus.insert(contents)
    test_query = Stimulus()
    result = dj.U('contrast', 'brightness') & test_query
    expected_result = [
     {'contrast':2, 
      'brightness':3},
     {'contrast':3, 
      'brightness':2},
     {'contrast':5, 
      'brightness':5}]
    fetched_result = result.fetch(as_dict=True, order_by=('contrast', 'brightness'))
    Stimulus.delete_quick()
    @py_assert1 = fetched_result == expected_result
    if not @py_assert1:
        @py_format3 = @pytest_ar._call_reprcompare(('==', ), (@py_assert1,), ('%(py0)s == %(py2)s', ), (fetched_result, expected_result)) % {'py0':@pytest_ar._saferepr(fetched_result) if 'fetched_result' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(fetched_result) else 'fetched_result',  'py2':@pytest_ar._saferepr(expected_result) if 'expected_result' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(expected_result) else 'expected_result'}
        @py_format5 = 'assert %(py4)s' % {'py4': @py_format3}
        raise AssertionError(@pytest_ar._format_explanation(@py_format5))
    @py_assert1 = None


def test_backslash(Parent):
    expected = 'She\\Hulk'
    Parent.insert([(2, expected)])
    q = Parent & dict(name=expected)
    @py_assert1 = q.fetch1
    @py_assert3 = 'name'
    @py_assert5 = @py_assert1(@py_assert3)
    @py_assert7 = @py_assert5 == expected
    if not @py_assert7:
        @py_format9 = @pytest_ar._call_reprcompare(('==', ), (@py_assert7,), ('%(py6)s\n{%(py6)s = %(py2)s\n{%(py2)s = %(py0)s.fetch1\n}(%(py4)s)\n} == %(py8)s', ), (@py_assert5, expected)) % {'py0':@pytest_ar._saferepr(q) if 'q' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(q) else 'q',  'py2':@pytest_ar._saferepr(@py_assert1),  'py4':@pytest_ar._saferepr(@py_assert3),  'py6':@pytest_ar._saferepr(@py_assert5),  'py8':@pytest_ar._saferepr(expected) if 'expected' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(expected) else 'expected'}
        @py_format11 = 'assert %(py10)s' % {'py10': @py_format9}
        raise AssertionError(@pytest_ar._format_explanation(@py_format11))
    @py_assert1 = @py_assert3 = @py_assert5 = @py_assert7 = None
    q.delete()