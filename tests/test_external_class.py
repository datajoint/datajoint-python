# decompyle3 version 3.9.0
# Python bytecode version base 3.7.0 (3394)
# Decompiled from: Python 3.7.17 (default, Jun 13 2023, 16:22:33) 
# [GCC 10.2.1 20210110]
# Embedded file name: /workspaces/datajoint-python/tests/test_external_class.py
# Compiled at: 2023-02-18 21:36:07
# Size of source mod 2**32: 1617 bytes
import builtins as @py_builtins
import _pytest.assertion.rewrite as @pytest_ar
import datajoint as dj, numpy as np
from numpy.testing import assert_almost_equal
from . import connection_root, connection_test, bucket
from schemas.external import schema, stores, store_local, store_share, Simple, Image, Dimension, Seed

def test_heading(Simple):
    heading = Simple().heading
    @py_assert0 = 'item'
    @py_assert2 = @py_assert0 in heading
    if not @py_assert2:
        @py_format4 = @pytest_ar._call_reprcompare(('in', ), (@py_assert2,), ('%(py1)s in %(py3)s', ), (@py_assert0, heading)) % {'py1':@pytest_ar._saferepr(@py_assert0),  'py3':@pytest_ar._saferepr(heading) if 'heading' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(heading) else 'heading'}
        @py_format6 = 'assert %(py5)s' % {'py5': @py_format4}
        raise AssertionError(@pytest_ar._format_explanation(@py_format6))
    @py_assert0 = @py_assert2 = None
    @py_assert0 = heading['item']
    @py_assert2 = @py_assert0.is_external
    if not @py_assert2:
        @py_format4 = 'assert %(py3)s\n{%(py3)s = %(py1)s.is_external\n}' % {'py1':@pytest_ar._saferepr(@py_assert0),  'py3':@pytest_ar._saferepr(@py_assert2)}
        raise AssertionError(@pytest_ar._format_explanation(@py_format4))
    @py_assert0 = @py_assert2 = None


def test_insert_and_fetch(Simple):
    original_list = [
     1, 3, 8]
    Simple().insert1(dict(simple=1, item=original_list))
    q = (Simple() & {'simple': 1}).fetch('item')[0]
    @py_assert2 = list(q)
    @py_assert4 = @py_assert2 == original_list
    if not @py_assert4:
        @py_format6 = @pytest_ar._call_reprcompare(('==', ), (@py_assert4,), ('%(py3)s\n{%(py3)s = %(py0)s(%(py1)s)\n} == %(py5)s', ), (@py_assert2, original_list)) % {'py0':@pytest_ar._saferepr(list) if 'list' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(list) else 'list',  'py1':@pytest_ar._saferepr(q) if 'q' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(q) else 'q',  'py3':@pytest_ar._saferepr(@py_assert2),  'py5':@pytest_ar._saferepr(original_list) if 'original_list' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(original_list) else 'original_list'}
        @py_format8 = 'assert %(py7)s' % {'py7': @py_format6}
        raise AssertionError(@pytest_ar._format_explanation(@py_format8))
    @py_assert2 = @py_assert4 = None
    q = (Simple() & {'simple': 1}).fetch1('item')
    @py_assert2 = list(q)
    @py_assert4 = @py_assert2 == original_list
    if not @py_assert4:
        @py_format6 = @pytest_ar._call_reprcompare(('==', ), (@py_assert4,), ('%(py3)s\n{%(py3)s = %(py0)s(%(py1)s)\n} == %(py5)s', ), (@py_assert2, original_list)) % {'py0':@pytest_ar._saferepr(list) if 'list' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(list) else 'list',  'py1':@pytest_ar._saferepr(q) if 'q' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(q) else 'q',  'py3':@pytest_ar._saferepr(@py_assert2),  'py5':@pytest_ar._saferepr(original_list) if 'original_list' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(original_list) else 'original_list'}
        @py_format8 = 'assert %(py7)s' % {'py7': @py_format6}
        raise AssertionError(@pytest_ar._format_explanation(@py_format8))
    @py_assert2 = @py_assert4 = None
    q = (Simple() & {'simple': 1}).fetch1()
    @py_assert1 = q['item']
    @py_assert3 = list(@py_assert1)
    @py_assert5 = @py_assert3 == original_list
    if not @py_assert5:
        @py_format7 = @pytest_ar._call_reprcompare(('==', ), (@py_assert5,), ('%(py4)s\n{%(py4)s = %(py0)s(%(py2)s)\n} == %(py6)s', ), (@py_assert3, original_list)) % {'py0':@pytest_ar._saferepr(list) if 'list' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(list) else 'list',  'py2':@pytest_ar._saferepr(@py_assert1),  'py4':@pytest_ar._saferepr(@py_assert3),  'py6':@pytest_ar._saferepr(original_list) if 'original_list' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(original_list) else 'original_list'}
        @py_format9 = 'assert %(py8)s' % {'py8': @py_format7}
        raise AssertionError(@pytest_ar._format_explanation(@py_format9))
    @py_assert1 = @py_assert3 = @py_assert5 = None
    previous_cache = dj.config['cache']
    dj.config['cache'] = None
    q = (Simple() & {'simple': 1}).fetch1()
    @py_assert1 = q['item']
    @py_assert3 = list(@py_assert1)
    @py_assert5 = @py_assert3 == original_list
    if not @py_assert5:
        @py_format7 = @pytest_ar._call_reprcompare(('==', ), (@py_assert5,), ('%(py4)s\n{%(py4)s = %(py0)s(%(py2)s)\n} == %(py6)s', ), (@py_assert3, original_list)) % {'py0':@pytest_ar._saferepr(list) if 'list' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(list) else 'list',  'py2':@pytest_ar._saferepr(@py_assert1),  'py4':@pytest_ar._saferepr(@py_assert3),  'py6':@pytest_ar._saferepr(original_list) if 'original_list' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(original_list) else 'original_list'}
        @py_format9 = 'assert %(py8)s' % {'py8': @py_format7}
        raise AssertionError(@pytest_ar._format_explanation(@py_format9))
    @py_assert1 = @py_assert3 = @py_assert5 = None
    dj.config['cache'] = previous_cache
    q = (Simple() & {'simple': 1}).fetch1()
    @py_assert1 = q['item']
    @py_assert3 = list(@py_assert1)
    @py_assert5 = @py_assert3 == original_list
    if not @py_assert5:
        @py_format7 = @pytest_ar._call_reprcompare(('==', ), (@py_assert5,), ('%(py4)s\n{%(py4)s = %(py0)s(%(py2)s)\n} == %(py6)s', ), (@py_assert3, original_list)) % {'py0':@pytest_ar._saferepr(list) if 'list' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(list) else 'list',  'py2':@pytest_ar._saferepr(@py_assert1),  'py4':@pytest_ar._saferepr(@py_assert3),  'py6':@pytest_ar._saferepr(original_list) if 'original_list' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(original_list) else 'original_list'}
        @py_format9 = 'assert %(py8)s' % {'py8': @py_format7}
        raise AssertionError(@pytest_ar._format_explanation(@py_format9))
    @py_assert1 = @py_assert3 = @py_assert5 = None


def test_populate(Image, Dimension, Seed):
    np.random.seed(500)
    image = Image()
    image.populate()
    remaining, total = image.progress()
    @py_assert1 = []
    @py_assert6 = Dimension()
    @py_assert9 = Seed()
    @py_assert11 = @py_assert6 * @py_assert9
    @py_assert12 = len(@py_assert11)
    @py_assert3 = total == @py_assert12
    @py_assert0 = @py_assert3
    if @py_assert3:
        @py_assert19 = 0
        @py_assert18 = remaining == @py_assert19
        @py_assert0 = @py_assert18
    if not @py_assert0:
        @py_format14 = @pytest_ar._call_reprcompare(('==', ), (@py_assert3,), ('%(py2)s == %(py13)s\n{%(py13)s = %(py4)s((%(py7)s\n{%(py7)s = %(py5)s()\n} * %(py10)s\n{%(py10)s = %(py8)s()\n}))\n}', ), (total, @py_assert12)) % {'py2':@pytest_ar._saferepr(total) if 'total' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(total) else 'total',  'py4':@pytest_ar._saferepr(len) if 'len' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(len) else 'len',  'py5':@pytest_ar._saferepr(Dimension) if 'Dimension' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(Dimension) else 'Dimension',  'py7':@pytest_ar._saferepr(@py_assert6),  'py8':@pytest_ar._saferepr(Seed) if 'Seed' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(Seed) else 'Seed',  'py10':@pytest_ar._saferepr(@py_assert9),  'py13':@pytest_ar._saferepr(@py_assert12)}
        @py_format16 = '%(py15)s' % {'py15': @py_format14}
        @py_assert1.append(@py_format16)
        if @py_assert3:
            @py_format21 = @pytest_ar._call_reprcompare(('==', ), (@py_assert18,), ('%(py17)s == %(py20)s', ), (remaining, @py_assert19)) % {'py17':@pytest_ar._saferepr(remaining) if 'remaining' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(remaining) else 'remaining',  'py20':@pytest_ar._saferepr(@py_assert19)}
            @py_format23 = '%(py22)s' % {'py22': @py_format21}
            @py_assert1.append(@py_format23)
        @py_format24 = @pytest_ar._format_boolop(@py_assert1, 0) % {}
        @py_format26 = 'assert %(py25)s' % {'py25': @py_format24}
        raise AssertionError(@pytest_ar._format_explanation(@py_format26))
    @py_assert0 = @py_assert1 = @py_assert3 = @py_assert6 = @py_assert9 = @py_assert11 = @py_assert12 = @py_assert18 = @py_assert19 = None
    for img, neg, dimensions in zip(*(image * Dimension()).fetch('img', 'neg', 'dimensions')):
        @py_assert2 = img.shape
        @py_assert4 = list(@py_assert2)
        @py_assert9 = list(dimensions)
        @py_assert6 = @py_assert4 == @py_assert9
        if not @py_assert6:
            @py_format11 = @pytest_ar._call_reprcompare(('==', ), (@py_assert6,), ('%(py5)s\n{%(py5)s = %(py0)s(%(py3)s\n{%(py3)s = %(py1)s.shape\n})\n} == %(py10)s\n{%(py10)s = %(py7)s(%(py8)s)\n}', ), (@py_assert4, @py_assert9)) % {'py0':@pytest_ar._saferepr(list) if 'list' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(list) else 'list',  'py1':@pytest_ar._saferepr(img) if 'img' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(img) else 'img',  'py3':@pytest_ar._saferepr(@py_assert2),  'py5':@pytest_ar._saferepr(@py_assert4),  'py7':@pytest_ar._saferepr(list) if 'list' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(list) else 'list',  'py8':@pytest_ar._saferepr(dimensions) if 'dimensions' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(dimensions) else 'dimensions',  'py10':@pytest_ar._saferepr(@py_assert9)}
            @py_format13 = 'assert %(py12)s' % {'py12': @py_format11}
            raise AssertionError(@pytest_ar._format_explanation(@py_format13))
        else:
            @py_assert2 = @py_assert4 = @py_assert6 = @py_assert9 = None
            assert_almost_equal(img, -neg)