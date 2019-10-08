from nose.tools import assert_equal, assert_false, assert_true, raises
from datajoint.declare import declare

from . import schema_advanced


def test_aliased_fk():
    person = schema_advanced.Person()
    parent = schema_advanced.Parent()
    person.delete()
    assert_false(person)
    assert_false(parent)
    person.fill()
    parent.fill()
    assert_true(person)
    assert_true(parent)
    link = person.proj(parent_name='full_name', parent='person_id')
    parents = person * parent * link
    parents &= dict(full_name="May K. Hall")
    assert_equal(set(parents.fetch('parent_name')), {'Hanna R. Walters', 'Russel S. James'})
    person.delete()


def test_describe():
    """real_definition should match original definition"""
    for rel in (schema_advanced.LocalSynapse, schema_advanced.GlobalSynapse):
        describe = rel.describe()
        s1 = declare(rel.full_table_name, rel.definition, schema_advanced.schema.context)[0].split('\n')
        s2 = declare(rel.full_table_name, describe, globals())[0].split('\n')
        for c1, c2 in zip(s1, s2):
            assert_equal(c1, c2)


def test_delete():
    person = schema_advanced.Person()
    parent = schema_advanced.Parent()
    person.delete()
    assert_false(person)
    assert_false(parent)
    person.fill()
    parent.fill()
    assert_true(parent)
    original_len = len(parent)
    to_delete = len(parent & '11 in (person_id, parent)')
    (person & 'person_id=11').delete()
    assert_true(to_delete and len(parent) == original_len - to_delete)
