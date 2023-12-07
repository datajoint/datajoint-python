import datajoint as dj
import itertools
import inspect


class R(dj.Lookup):
    definition = """
    r : char(1)
    """
    contents = zip("ABCDFGHIJKLMNOPQRST")


class Q(dj.Lookup):
    definition = """
    -> R
    """
    contents = zip("ABCDFGH")


class S(dj.Lookup):
    definition = """
    -> R
    s : int
    """
    contents = itertools.product("ABCDF", range(10))


class A(dj.Lookup):
    definition = """
    id: int
    """
    contents = zip(range(10))


class B(dj.Lookup):
    definition = """
    -> A
    id2: int
    """
    contents = zip(range(5), range(5, 10))


class X(dj.Lookup):
    definition = """
    id: int
    """
    contents = zip(range(10))


LOCALS_AGGR_REGRESS = {k: v for k, v in locals().items() if inspect.isclass(v)}
__all__ = list(LOCALS_AGGR_REGRESS)
