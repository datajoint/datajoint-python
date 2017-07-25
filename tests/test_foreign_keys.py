from nose.tools import assert_equal
from datajoint.declare import declare

from . import schema_advanced

context = schema_advanced.schema.context


def test_aliased_fk():
    person = schema_advanced.Person()
    parent = schema_advanced.Parent()
    person.fill()
    parent.fill()
    link = person.proj(parent_name='full_name', parent='person_id')
    parents = person*parent*link
    parents &= dict(full_name="May K. Hall")
    assert_equal(set(parents.fetch('parent_name')), {'Hanna R. Walters', 'Russel S. James'})


def test_describe():
    """real_definition should match original definition"""
    for rel in (schema_advanced.LocalSynapse(), schema_advanced.GlobalSynapse()):
        describe = rel.describe()
        s1 = declare(rel.full_table_name, rel.definition, context)
        s2 = declare(rel.full_table_name, describe, context)
        assert_equal(s1, s2)
