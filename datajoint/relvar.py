import imp
import re
from enum import Enum
from .core import DataJointError, fromCamelCase, log
from .relational import _Relational
from .heading import Heading


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

mysql_constants = ['CURRENT_TIMESTAMP']


class Relvar(_Relational):
    """
    datajoint.Relvar integrates all data manipulation and data declaration functions.
    An instance of the class provides an interface to a single table in the database.

    An instance of the the class can be produce in two ways:
        1. direct instantiation  (used mostly for debugging and odd jobs)
        2. instantiation from a derived class (regular recommended use)

    With direct instantiation, instance parameters must be explicitly specified.
    With a derived class, all the instance parameters are taken from the module
    of the deriving class. The module must declare the connection object conn.
    The name of the deriving class is used as the table's displayName.

    Tables are identified by their "pretty names", which are CamelCase. The actual
    table names are converted from CamelCase to underscore_separated_words and
    prefixed according to the table's role.
    """
 
    def __init__(self, conn=None, dbname=None, className=None, declaration=None):
        
        if self.__class__ is Relvar:
            # instantiate without subclassing 
            if not(conn and dbname and className):
                raise DataJointError('Missing argument: please specify conn, dbanem, and className.')
            self.className = className
            self.conn = conn
            self.dbname = dbname
            self.declaration = declaration
            if dbname not in self.conn.modules:    # register with a fake module, enclosed in backquotes
                self.conn.bind('`%s`'%dbname, dbname)
        else:
            # instantiate a derived class 
            if conn or dbname or className or declaration:
                raise DataJointError('With derived classes, constructor arguments are ignored')
            self.className = self.__class__.__name__
            module = imp.importlib.import_module(self.__module__)                
            try:
                self.conn = module.conn
            except AttributeError:
                raise DataJointError("Please define object 'conn' in '%s'." % module.__name__)
            try:
                self.dbname = self.conn.dbnames[self.__module__]
            except KeyError:
                raise DataJointError('Module %s is not bound to a database. See datajoint.connection.bind' % self.__module__)
            self.declaration = self.__doc__
               


    def _compile(self):
        sql = '`%s`.`%s`' % (self.dbname, self.table)
        return sql, self.heading



    @property
    def isDeclared(self):
        "True if table is found in the database"
        self.conn.loadHeadings(self.dbname)
        return self.className in self.conn.tableNames[self.dbname]


    @property
    def table(self):
        self.declare()
        return self.conn.tableNames[self.dbname][self.className]

    @property
    def heading(self):
        self.declare()
        return self.conn.headings[self.dbname][self.table]

    @property
    def fullTableName(self):
        return '`%s`.`%s`' % (self.dbname, self.table)

    @property
    def primaryKey(self):
        return self.heading.primaryKey

    def declare(self):
        if not self.isDeclared:
            self._declare()
            if not self.isDeclared:
                raise DataJointError('Table could not be declared for %s' % self.className)
    
    
    @classmethod
    def getRelvar(cls, conn, module, className):
        """load relvar from module if available"""
        modObj = imp.importlib.__import__(module)
        try:
            ret = modObj.__dict__[className]()
        except KeyError:
            ret = cls(conn=conn, dbname=conn.schemas[module], className=className)
        return ret



    def _fieldToSQL(self, field):
        """
        Converts an attribute definition tuple into SQL code 
        """
        if field.isNullable:
            default = 'DEFAULT NULL'
        else:
            default = 'NOT NULL'
            # if some default specified
            if field.default:
                # enclose value in quotes (even numeric), except special SQL values
                # or values already enclosed by the user
                if field.default.upper() in mysql_constants or field.default[:1] in ["'", '"']:
                    default = '%s DEFAULT %s' % (default, field.default)
                else:
                    default = '%s DEFAULT "%s"' % (default, field.default)

        # TODO: escase instead! - same goes for Matlab side implementation
        assert not any((c in r'\"' for c in field.comment)), \
            'Illegal characters in attribute comment "%s"' % field.comment

        return '`{name}` {type} {default} COMMENT "{comment}", \n'.format(\
            name=field.name, type=field.type, default=default, comment=field.comment)



    def _declare(self):
        """
        _declare is called when no table in the database matches this object
        """
        tableInfo, parents, referenced, fieldDefs, indexDefs = self._parseDeclaration()
        fullName = tableInfo['module'] + '.' + tableInfo['className']
        clsName = self.__module__ + '.' + self.displayName
        assert fullName == clsName, 'Table name %s does not match the declared name %s' % (clsName, fullName)

        # compile the CREATE TABLE statement
        # TODO: support prefix
        tableName = rolePrefix[tableInfo['tier']] + fromCamelCase(self.className)
        sql = 'CREATE TABLE `%s`.`%s` (\n' % (self.dbname, tableName)

        # add inherited primary key fields
        primaryKeyFields = set()
        nonKeyFields = set()
        for p in parents:
            keys = (x for x in p.heading.attrs.values() if x.isKey)
            for field in keys:
                if field.name not in primaryKeyFields:
                    primaryKeyFields.add(field.name)
                    sql += self._fieldToSQL(field)

        # add newly defined primary key fields
        for field in (f for f in fieldDefs if f.isKey):
            primaryKeyFields.add(field.name)
            assert not field.isNullable, 'primary key header cannot be nullable'
            sql += self._fieldToSQL(field)

        # add secondary foreign key attributes
        for r in referenced:
            keys = (x for x in r.heading.attrs.values() if x.isKey)
            for field in keys:
                if field.name not in primaryKeyFields | nonKeyFields:
                    nonKeyFields.add(field.name)
                    sql += self._fieldToSQL(field)

        # add dependent attributes
        for field in (f for f in fieldDefs if not f.isKey):
            nonKeyFields.add(field.name)
            sql += self._fieldToSQL(field)

        # add primary key declaration
        assert len(primaryKeyFields)>0, 'table must have a primary key'
        keys = ', '.join(primaryKeyFields)
        sql += 'PRIMARY KEY (%s),\n' % keys

        # add foreign key declarations
        for ref in parents+referenced:
            keys = ', '.join(ref.primaryKey)
            sql += 'FOREIGN KEY (%s) REFERENCES %s (%s) ON UPDATE CASCADE ON DELETE RESTRICT,\n' % \
                    (keys, ref.fullTableName, keys)


        # add secondary index declarations
        # gather implicit indexes due to foreign keys first
        implicitIndexes = []
        for fkSource in parents+referenced:
            implicitIndexes.append(fkSource.primaryKey)

        #for index in indexDefs:
        #TODO: finish this up...

        # close the declaration
        sql = '%s\n) ENGINE = InnoDB, COMMENT "%s"' % (sql[:-2], tableInfo['comment'])

        # make sure that the table does not alredy exist
        self.conn.loadHeadings(self.dbname, force=True)
        if not self.isDeclared:
            # execute declaration
            log('\n<SQL>\n' + sql + '</SQL>\n\n')
            self.conn.query(sql)
            self.conn.loadHeadings(self.dbname, force=True)


    def _parseDeclaration(self):
        parents = []
        referenced = []
        indexDefs = []
        fieldDefs = []
        declaration = re.split(r'\s*\n\s*', self.declaration.strip())

        # remove comment lines
        declaration = [x for x in declaration if not x.startswith('#')]
        ptrn = """
        ^(?P<module>\w+)\.(?P<className>\w+)\s*     #  module.className
        \(\s*(?P<tier>\w+)\s*\)\s*                  #  (tier)
        \#\s*(?P<comment>.*)$                       #  comment
        """
        p = re.compile(ptrn, re.X)
        tableInfo = p.match(declaration[0]).groupdict()
        assert tableInfo['tier'] in Role.__members__, 'invalidTableTier:Invalid tier for table %s.%s' % (tableInfo['module'], tableInfo['className'])
        tableInfo['tier'] = Role[tableInfo['tier']] # convert into enum
        inKey = True

        fieldPtrn = """
        ^[a-z][a-z\d_]*\s*          # name
        (=\s*\S+(\s+\S+)*\s*)?      # optional defaults
        :\s*\w.*$                   # type, comment
        """
        fieldP = re.compile(fieldPtrn, re.I + re.X) # ignore case and verbose

        for line in declaration[1:]:
            if line.startswith('---'):
                inKey = False
            elif line.startswith('->'):
                # foreign key
                module, className = line[2:].strip().split('.')
                rel = self.getRelvar(self.conn, module, className)
                (parents if inKey else referenced).append(rel)
            elif re.match(r'^(unique\s+)?index[^:]*$', line):
                indexDefs.append(self._parseIndexDef(line))
            elif fieldP.match(line):
                fieldDefs.append(self._parseAttrDef(line, inKey))
            else:
                raise DataJointError('Invalid table declaration line "%s"' % line)

        return tableInfo, parents, referenced, fieldDefs, indexDefs

    def _parseAttrDef(self, line, inKey):
        line = line.strip()
        attrPtrn = """
        ^(?P<name>[a-z][a-z\d_]*)\s*             # field name
        (=\s*(?P<default>\S+(\s+\S+)*)\s*)?      # default value
        :\s*(?P<type>\w[^\#]*[^\#\s])\s*         # datatype
        (\#\s*(?P<comment>\S.*\S)\s*)?$          # comment
        """
        attrP = re.compile(attrPtrn, re.I + re.X)
        m = attrP.match(line)
        assert m, 'Invalid field declaration "%s"' % line
        attrInfo = m.groupdict()
        if not attrInfo['comment']:
            attrInfo['comment'] = ''
        if not attrInfo['default']:
            attrInfo['default'] = ''
        attrInfo['isNullable'] = attrInfo['default'].lower() == 'null'
        assert (not re.match(r'^bigint', attrInfo['type'], re.I) or not attrInfo['isNullable']), \
            'BIGINT attributes cannot be nullable in "%s"' % line

        attrInfo['isKey'] = inKey;
        attrInfo['isAutoincrement'] = None
        attrInfo['isNumeric'] = None
        attrInfo['isString'] = None
        attrInfo['isBlob'] = None
        attrInfo['alias'] = None
        attrInfo['dtype'] = None

        return Heading.AttrTuple(**attrInfo)

    def _parseIndexDef(self, line):
        line = line.strip()
        indexPtrn = """
        ^(?P<unique>UNIQUE)?\s*INDEX\s*      # [UNIQUE] INDEX
        \((?P<attributes>[^\)]+)\)$          # (attr1, attr2)
        """
        indexP = re.compile(indexPtrn, re.I + re.X)
        m = indexP.match(line)
        assert m, 'Invalid index declaration "%s"' % line
        indexInfo = m.groupdict()
        attributes = re.split(r'\s*,\s*', indexInfo['attributes'].strip())
        indexInfo['attributes'] = attributes
        assert len(attributes) == len(set(attributes)), \
        'Duplicate attributes in index declaration "%s"' % line
        return indexInfo








