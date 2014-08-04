import imp
class GeneralRelvar: pass


class Relvar(GeneralRelvar):
    def __init__(self):
        self.__module = imp.importlib.__import__(self.__class__.__module__)
        self.declaration = self.__doc__
        
    @property
    def dbname(self):
        return self.__module.dbname
        
    @property
    def conn(self):
        return self.__module.connection