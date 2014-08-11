import imp
from enum import Enum
from .core import DataJointError, camelCase
from .relational import Relational

# table names have prefixes that designate their roles in the processing chain
Role = Enum('Role','manual lookup imported computed job')
rolePrefix = {
    Role.manual   : '',
    Role.lookup   : '#',
    Role.imported : '_',
    Role.computed : '__',
    Role.job      : '~'
    }   
prefixRole = dict(zip(rolePrefix.values(),rolePrefix.keys()))



class Relvar(Relational):
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
        # TODO: change prettyName by something more sensible for the user 
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
            assert prettyName == camelCase(prettyName), 'Class %s should be renamed %s' % (prettyName, camelCase(prettyName))
            try:            
                conn = module.conn   
            except AttributeError:
                raise DataJointError('DataJoint module %s must declare a connection object conn.' % module.__name__)
            dbname = conn.schemas[module.__name__]
            declaration = self.__class__.__doc__   # table declaration is in the doc string
  
        super().__init__(conn)
        self.prettyName = prettyName
        self.dbname = dbname
        self.declaration = declaration
        self.conn.loadHeadings(dbname)
    

    def _compile(self):
        sql = '`%s`.`%s`' % (self.dbname, self.tableName)
        return self.conn, sql, self.heading
        
    
    @property
    def isDeclared(self):
        # True if found in the database
        return self.prettyName in self.conn.tableNames[self.dbname]
        
    @property
    def tableName(self):
        self.declare
        return self.conn.tableNames[self.dbname][self.prettyName]            
    
    @property
    def heading(self):
        self.declare
        return self.conn.headings[self.dbname][self.tableName]
        
    def declare(self):
        if not self.isDeclared:
            self._declare()  
            if not self.isDeclared:
                raise DataJointError('Table could not be declared for %s' % self.prettyName)
    
    def _declare(self):
        """
        _declare is called when no table in the database matches this object
        """
        # TODO: declare the table based on self.declaration
