from datajoint.declare import declare
from .schema_advanced import *


def test_aliased_fk(schema_adv):
    person = Person()
    parent = Parent()
    person.delete()
    assert not person
    assert not parent
    person.fill()
    parent.fill()
    assert person
    assert parent
    link = person.proj(parent_name="full_name", parent="person_id")
    parents = person * parent * link
    parents &= dict(full_name="May K. Hall")
    assert set(parents.fetch("parent_name")) == {"Hanna R. Walters", "Russel S. James"}
    delete_count = person.delete()
    assert delete_count == 16


def test_describe(schema_adv):
    """real_definition should match original definition"""
    for rel in (LocalSynapse, GlobalSynapse):
        describe = rel.describe()
        s1 = declare(rel.full_table_name, rel.definition, schema_adv.context)[0].split(
            "\n"
        )
        s2 = declare(rel.full_table_name, describe, globals())[0].split("\n")
        for c1, c2 in zip(s1, s2):
            assert c1 == c2


def test_delete(schema_adv):
    person = Person()
    parent = Parent()
    person.delete()
    assert not person
    assert not parent
    person.fill()
    parent.fill()
    assert parent
    original_len = len(parent)
    to_delete = len(parent & "11 in (person_id, parent)")
    (person & "person_id=11").delete()
    assert to_delete and len(parent) == original_len - to_delete
