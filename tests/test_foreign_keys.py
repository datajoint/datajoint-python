from nose.tools import assert_equal

from . import schema_advanced


def test_aliased_fk():
    person = schema_advanced.Person()
    parent = schema_advanced.Parent()
    person.fill()
    parent.fill()
    parents = person*parent*person.proj(parent_name='full_name', parent='person_id')
    parents &= dict(full_name="May K. Hall")
    assert_equal(set(parents.fetch['parent_name']), {'Hanna R. Walters', 'Russel S. James'})
