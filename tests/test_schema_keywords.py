from . import PREFIX, CONN_INFO
import datajoint as dj
from nose.tools import assert_true


schema = dj.schema(PREFIX + '_keywords', locals(), connection=dj.conn(**CONN_INFO))


class A(dj.Manual):
    definition = """
    a_id: int   # a id
    """


class B(dj.Manual):
    source = None
    definition = """
    -> self.source
    b_id: int   # b id
    """

    class H(dj.Part):
        definition = """
        -> master
        name: varchar(128)  # name
        """

    class C(dj.Part):
        definition = """
        -> master
        -> master.H
        """


class D(B):
    source = A


def setup():
    global A
    global D
    A = schema(A)
    D = schema(D)


def teardown():
    schema.drop(force=True)


def test_inherited_part_table():
    assert_true('a_id' in D().heading.attributes)
    assert_true('b_id' in D().heading.attributes)
    assert_true('a_id' in D.C().heading.attributes)
    assert_true('b_id' in D.C().heading.attributes)
    assert_true('name' in D.C().heading.attributes)



