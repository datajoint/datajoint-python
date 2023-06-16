# decompyle3 version 3.9.0
# Python bytecode version base 3.7.0 (3394)
# Decompiled from: Python 3.7.17 (default, Jun 13 2023, 16:22:33) 
# [GCC 10.2.1 20210110]
# Embedded file name: /workspaces/datajoint-python/tests/test_foreign_keys.py
# Compiled at: 2023-02-20 15:52:02
# Size of source mod 2**32: 1561 bytes
import builtins as @py_builtins
import _pytest.assertion.rewrite as @pytest_ar
import datajoint.declare as declare
from . import connection_root, connection_test
from schemas.advanced import schema, Person, Parent, LocalSynapse, GlobalSynapse, Cell, Slice, Prep

def test_aliased_fk(Person, Parent):
    person = Person()
    parent = Parent()
    person.delete()
    @py_assert1 = not person
    if not @py_assert1:
        @py_format2 = 'assert not %(py0)s' % {'py0': @pytest_ar._saferepr(person) if ('person' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(person)) else 'person'}
        raise AssertionError(@pytest_ar._format_explanation(@py_format2))
    @py_assert1 = None
    @py_assert1 = not parent
    if not @py_assert1:
        @py_format2 = 'assert not %(py0)s' % {'py0': @pytest_ar._saferepr(parent) if ('parent' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(parent)) else 'parent'}
        raise AssertionError(@pytest_ar._format_explanation(@py_format2))
    @py_assert1 = None
    person.fill()
    parent.fill()
    if not person:
        @py_format1 = 'assert %(py0)s' % {'py0': @pytest_ar._saferepr(person) if ('person' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(person)) else 'person'}
        raise AssertionError(@pytest_ar._format_explanation(@py_format1))
    if not parent:
        @py_format1 = 'assert %(py0)s' % {'py0': @pytest_ar._saferepr(parent) if ('parent' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(parent)) else 'parent'}
        raise AssertionError(@pytest_ar._format_explanation(@py_format1))
    link = person.proj(parent_name='full_name', parent='person_id')
    parents = person * parent * link
    parents &= dict(full_name='May K. Hall')
    @py_assert2 = parents.fetch
    @py_assert4 = 'parent_name'
    @py_assert6 = @py_assert2(@py_assert4)
    @py_assert8 = set(@py_assert6)
    @py_assert11 = {
     'Hanna R. Walters', 'Russel S. James'}
    @py_assert10 = @py_assert8 == @py_assert11
    if not @py_assert10:
        @py_format13 = @pytest_ar._call_reprcompare(('==', ), (@py_assert10,), ('%(py9)s\n{%(py9)s = %(py0)s(%(py7)s\n{%(py7)s = %(py3)s\n{%(py3)s = %(py1)s.fetch\n}(%(py5)s)\n})\n} == %(py12)s', ), (@py_assert8, @py_assert11)) % {'py0':@pytest_ar._saferepr(set) if 'set' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(set) else 'set',  'py1':@pytest_ar._saferepr(parents) if 'parents' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(parents) else 'parents',  'py3':@pytest_ar._saferepr(@py_assert2),  'py5':@pytest_ar._saferepr(@py_assert4),  'py7':@pytest_ar._saferepr(@py_assert6),  'py9':@pytest_ar._saferepr(@py_assert8),  'py12':@pytest_ar._saferepr(@py_assert11)}
        @py_format15 = 'assert %(py14)s' % {'py14': @py_format13}
        raise AssertionError(@pytest_ar._format_explanation(@py_format15))
    @py_assert2 = @py_assert4 = @py_assert6 = @py_assert8 = @py_assert10 = @py_assert11 = None
    delete_count = person.delete()
    @py_assert2 = 16
    @py_assert1 = delete_count == @py_assert2
    if not @py_assert1:
        @py_format4 = @pytest_ar._call_reprcompare(('==', ), (@py_assert1,), ('%(py0)s == %(py3)s', ), (delete_count, @py_assert2)) % {'py0':@pytest_ar._saferepr(delete_count) if 'delete_count' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(delete_count) else 'delete_count',  'py3':@pytest_ar._saferepr(@py_assert2)}
        @py_format6 = 'assert %(py5)s' % {'py5': @py_format4}
        raise AssertionError(@pytest_ar._format_explanation(@py_format6))
    @py_assert1 = @py_assert2 = None


def test_describe(schema, LocalSynapse, GlobalSynapse, Cell):
    """real_definition should match original definition"""
    for rel in (LocalSynapse, GlobalSynapse):
        describe = rel.describe()
        s1 = declare(rel.full_table_name, rel.definition, locals())[0].split('\n')
        s2 = declare(rel.full_table_name, describe, locals())[0].split('\n')
        for c1, c2 in zip(s1, s2):
            @py_assert1 = c1 == c2
            if not @py_assert1:
                @py_format3 = @pytest_ar._call_reprcompare(('==', ), (@py_assert1,), ('%(py0)s == %(py2)s', ), (c1, c2)) % {'py0':@pytest_ar._saferepr(c1) if 'c1' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(c1) else 'c1',  'py2':@pytest_ar._saferepr(c2) if 'c2' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(c2) else 'c2'}
                @py_format5 = 'assert %(py4)s' % {'py4': @py_format3}
                raise AssertionError(@pytest_ar._format_explanation(@py_format5))
            else:
                @py_assert1 = None


def test_delete(Person, Parent):
    person = Person()
    parent = Parent()
    person.delete()
    @py_assert1 = not person
    if not @py_assert1:
        @py_format2 = 'assert not %(py0)s' % {'py0': @pytest_ar._saferepr(person) if ('person' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(person)) else 'person'}
        raise AssertionError(@pytest_ar._format_explanation(@py_format2))
    @py_assert1 = None
    @py_assert1 = not parent
    if not @py_assert1:
        @py_format2 = 'assert not %(py0)s' % {'py0': @pytest_ar._saferepr(parent) if ('parent' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(parent)) else 'parent'}
        raise AssertionError(@pytest_ar._format_explanation(@py_format2))
    @py_assert1 = None
    person.fill()
    parent.fill()
    if not parent:
        @py_format1 = 'assert %(py0)s' % {'py0': @pytest_ar._saferepr(parent) if ('parent' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(parent)) else 'parent'}
        raise AssertionError(@pytest_ar._format_explanation(@py_format1))
    original_len = len(parent)
    to_delete = len(parent & '11 in (person_id, parent)')
    (person & 'person_id=11').delete()
    @py_assert1 = []
    @py_assert0 = to_delete
    if to_delete:
        @py_assert6 = len(parent)
        @py_assert11 = original_len - to_delete
        @py_assert8 = @py_assert6 == @py_assert11
        @py_assert0 = @py_assert8
    if not @py_assert0:
        @py_format3 = '%(py2)s' % {'py2': @pytest_ar._saferepr(to_delete) if ('to_delete' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(to_delete)) else 'to_delete'}
        @py_assert1.append(@py_format3)
        if to_delete:
            @py_format12 = @pytest_ar._call_reprcompare(('==', ), (@py_assert8,), ('%(py7)s\n{%(py7)s = %(py4)s(%(py5)s)\n} == (%(py9)s - %(py10)s)', ), (@py_assert6, @py_assert11)) % {'py4':@pytest_ar._saferepr(len) if 'len' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(len) else 'len',  'py5':@pytest_ar._saferepr(parent) if 'parent' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(parent) else 'parent',  'py7':@pytest_ar._saferepr(@py_assert6),  'py9':@pytest_ar._saferepr(original_len) if 'original_len' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(original_len) else 'original_len',  'py10':@pytest_ar._saferepr(to_delete) if 'to_delete' in @py_builtins.locals() or @pytest_ar._should_repr_global_name(to_delete) else 'to_delete'}
            @py_format14 = '%(py13)s' % {'py13': @py_format12}
            @py_assert1.append(@py_format14)
        @py_format15 = @pytest_ar._format_boolop(@py_assert1, 0) % {}
        @py_format17 = 'assert %(py16)s' % {'py16': @py_format15}
        raise AssertionError(@pytest_ar._format_explanation(@py_format17))
    @py_assert0 = @py_assert1 = @py_assert6 = @py_assert8 = @py_assert11 = None