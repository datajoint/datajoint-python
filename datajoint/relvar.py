import imp


class GeneralRelvar: pass


class Relvar(GeneralRelvar):
    """
    dj.Relvar is the base class for all user base relvar classes. It provides
    all the basic functionality for data definition and data manipulation.
    """

    def __init__(self, moduleName=None, className=None, declaration=None):
        # unless otherwise specified, take the name and module of the derived class
        # The doc string of the derived class contains the table declaration
        self.className = className if className else self.__class__.__name__;
        self.moduleName = moduleName if moduleName else self.__class__.__module__;
        self.module = imp.importlib.__import__(self.moduleName)
        self.declaration = declaration if declaration else self.__doc__
        
    @property
    def dbname(self):
        return self.module.conn.schemas[self.moduleName]