from pytest import raises

from datajoint import errors


def test_nullable_dependency(thing_tables):
    """test nullable unique foreign key"""
    # Thing C has a nullable dependency on B whose primary key is composite
    _, _, c, _, _ = thing_tables

    # missing foreign key attributes = ok
    c.insert1(dict(a=0))
    c.insert1(dict(a=1, b1=33))
    c.insert1(dict(a=2, b2=77))

    # unique foreign key attributes = ok
    c.insert1(dict(a=3, b1=1, b2=1))
    c.insert1(dict(a=4, b1=1, b2=2))

    assert len(c) == len(c.fetch()) == 5


def test_topo_sort():
    import networkx as nx

    import datajoint as dj

    graph = nx.DiGraph(
        [
            ("`a`.`a`", "`a`.`m`"),
            ("`a`.`a`", "`a`.`z`"),
            ("`a`.`m`", "`a`.`m__part`"),
            ("`a`.`z`", "`a`.`m__part`"),
        ]
    )
    assert dj.dependencies.topo_sort(graph) == [
        "`a`.`a`",
        "`a`.`z`",
        "`a`.`m`",
        "`a`.`m__part`",
    ]


def test_unique_dependency(thing_tables):
    """test nullable unique foreign key"""
    # Thing C has a nullable dependency on B whose primary key is composite
    _, _, c, _, _ = thing_tables

    c.insert1(dict(a=0, b1=1, b2=1))
    # duplicate foreign key attributes = not ok
    with raises(errors.DuplicateError):
        c.insert1(dict(a=1, b1=1, b2=1))
