import imp
from enum import Enum
from core import DataJointError

# table names have prefixes that designate their roles in the processing chain
Role = Enum('Role','manual lookup imported computed')
rolePrefix = {
    Role.manual   : '',
    Role.lookup   : '#',
    Role.imported : '_',
    Role.computed : '__'
    }   
prefixRole = dict(zip(rolePrefix.values(),rolePrefix.keys()))


class GeneralRelvar:    
    """
    datajoint.GeneralRelvar implements relational algebra and fetching data.
    Relvar objects sharing the same connection object can be combined into 
    queries using relational operators: restrict, project, and join.
    """    
    def __init__(self, conn, operator, restrictions=None, *operands):
        self.conn = conn
        self.operator = operator
        self.restrictions = restrictions
        self.operands = operands


class Table:
    """
    datajoint.Table implement data declaration functions.
    """
    def __init__(self, conn, dbname, tabName, declaration=None):
        self.conn = conn                # a dj.Connection object    
        self.dbname = dbname            # database schema name
        self.name = tabName             # table name
        self.declaration = declaration  # table declaration in datajoint syntax
        
    @property
    def fullname(self):
        return "`%s`.`%s`" % (self.dbname,self.name) 



class Relvar(GeneralRelvar, Table):
    """
    datajoint.Relvar integrates all data manipulation and data declaration functions.
    An instance of the class provides an interface to a single table in the database.
    
    An instance of the the class can be produce in two ways: 
        1. direct instantiation  (used mostly for debugging and odd jobs)
        2. instantiation from a derived class (regular recommended use)
        
    With direct instantiation, instance parameters must be explicitly specified.
    With a derived class, all the instance parameters are taken from the module
    of the deriving class. The module must declare the connection object conn.
    The name of the deriving class is used as the table's prettyName.
    
    Tables are identified by their "pretty names", which are CamelCase. The actual 
    table names are converted from CamelCase to underscore_separated_words and 
    prefixed according to the table's role.
    """

    def __init__(self, conn=None, dbname=None, prettyName=None, declaration=None):
        """
        INPUTS:
        conn - a datajoint.Connection object
        dbname - database schema name
        prettyName - module.PrettyTableName (the CamelCase version of the table name)
        declaration - table declaration string in DataJoint syntax 
        """
        # unless otherwise specified, take the name and module of the derived class
        # The doc string of the derived class contains the table declaration
        if conn is None or dbname is None or prettyName is None:
            # must be a derived class
            if not issubclass(type(self), Relvar):
                raise DataJointError('Relvar is missing connection information')
            module = imp.importlib.__import__(self.__class__.__module__)
            prettyName = self.__class__.__name__
            try:            
                conn = module.conn   
            except AttributeError:
                raise DataJointError('DataJoint module %s must declare a connection object conn.' % module.__name__)
            dbname = conn.schemas[module.__name__]
            declaration = self.__class__.__doc__   # table declaration is in the doc string
  
        self.conn = conn;
        self.prettyName = prettyName
        self.dbname = dbname
        self.declaration = declaration
        self.conn.loadHeadings(dbname)
    
    @property
    def isDeclared(self):
        # True if found in the database
        return self.prettyName in self.conn.tableNames[self.dbname]
        
    @property
    def tableName(self):
        if self.isDeclared:
            return self.conn.tableNames[self.dbname][self.prettyName]            
    
    @property
    def heading(self):
        if self.isDeclared:
            return self.conn.headings[self.dbname][self.tableName]
        
    