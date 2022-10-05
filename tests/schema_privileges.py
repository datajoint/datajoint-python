import datajoint as dj

schema = dj.Schema()


@schema
class Parent(dj.Lookup):
    definition = """
    id: int
    """
    contents = [(1,)]


@schema
class Child(dj.Computed):
    definition = """
    -> Parent
    """

    def make(self, key):
        self.insert1(key)


@schema
class NoAccess(dj.Lookup):
    definition = """
    string: varchar(10)
    """


@schema
class NoAccessAgain(dj.Manual):
    definition = """
    -> NoAccess
    """
