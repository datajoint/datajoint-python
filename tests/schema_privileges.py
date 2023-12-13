import datajoint as dj
import inspect


class Parent(dj.Lookup):
    definition = """
    id: int
    """
    contents = [(1,)]


class Child(dj.Computed):
    definition = """
    -> Parent
    """

    def make(self, key):
        self.insert1(key)


class NoAccess(dj.Lookup):
    definition = """
    string: varchar(10)
    """


class NoAccessAgain(dj.Manual):
    definition = """
    -> NoAccess
    """


LOCALS_PRIV = {k: v for k, v in locals().items() if inspect.isclass(v)}
__all__ = list(LOCALS_PRIV)
