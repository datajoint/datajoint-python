import numpy as np
from nose.tools import assert_true
import datajoint as dj
from . import PREFIX, CONN_INFO

from . import schema_advanced


def test_aliased_fk():
    person = schema_advanced.Person()
    parent = schema_advanced.Parent()
    person.fill()
    parent.fill()
    name = dict(full_name="May K. Hall")
    parents = person*parent*person.proj(parent_name='full_name', parent='person_id')
    assert_true(set((parents & name).fetch['parent_name']) == {'Hanna R. Walters', 'Russel S. James'})
