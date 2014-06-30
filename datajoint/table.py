from core import DataJointError
import importlib

class Table(object):
    """
    datajoint.Table implements data definition functions
    """

    constantNames = ("CURRENT_TIME", "CURRENT_DATE", "CURRENT_TIMESTAMP")

    def __init__(self, className):
        self.className = className
        self._plainTableName = None
        self._fullTableName = None
        self._header = None
        self._info = None
        self._module = None

    @property
    def package(self):
        return self.className.split('.')[0]

    @property
    def schema(self):
        """schema object of the base relvar"""
        return self.module.schema

    @property
    def module(self):
        """python module containing the base relvar"""
        if not self._module:
            self._module = importlib.import_module(self.package)
        return self._module

    @property
    def tableHeader(self):
        """
        Return header obj with table info and attribute info
        """
        if not self._header:
            self._header = self.schema.headers[self.plainTableName]
        return self._header

    @property
    def plainTableName(self):
        if not self._plainTableName:
            self._plainTableName = self.schema.tableNames[self.className]
        return self._plainTableName

    @property
    def fullTableName(self):
        if not self._fullTableName:
            self._fullTableName = '`%s`.`%s`' % (self.schema.dbName, self.plainTableName)
        return self._fullTableName

    @property
    def info(self):
        """
        table name, tier, comment retrieved from the database
        """
        if not self._info:
            self._info = self.tableHeader.info
        return self._info

    @property
    def parents(self):
        self.schema.reload()
        return self.schema.conn.parents[self.fullTableName]

    @property
    def referenced(self):
        self.schema.reload()
        return self.schema.conn.referenced[self.fullTableName]

    @property
    def referencing(self):
        self.schema.reload()
        return self.schema.conn.referencing(self.fullTableName)

    @property
    def children(self):
        self.schema.reload()
        return self.schema.conn.children(self.fullTableName)

    #def isStrictParent(self, parent):
    #    """
    #    True if all the primary key fields in parent are also primary key fields in self
    #    This indicates a purely hierarchical relationship.
    #    Parent here is a class name.
    #    """
    #    return reduce(lambda x, y: x and (y in self.primaryKey), Table(parent).primaryKey)

    @property
    def children(self):
        self.schema.reload()
        return self.schema.conn.gT

    @property
    def ancestors(self):
        levelMap = {}
        def recurse(table, level):
            if (table.className not in levelMap) or (level > levelMap[table.className]):
                refs = table.parents + table.referenced
                for tbl in refs:
                    recurse(Table(self.schema.conn.tableToClass(tbl)), level+1)
                levelMap[table.className] = level

        recurse(self, 0)
        return sorted(levelMap, key=levelMap.get, reverse=True)

    @property
    def declaration(self):
        """
        table declaration from the base relvar class' doc string
        """
        try:
            cl = self.module.__dict__[self.className.split('.')[1]]
        except KeyError:
            raise DataJointError('Table %s has no class' % self.className)
        return cl.__doc__

    def __repr__(self):
        return '\n' + self.re()

    def re(self):
        """
        reverse-engineer the declaration of an existing table
        """

        def makeField(field):
            default = self.header[field].default
            if default is None:
                default = ''
            else:
                if default in Table.constantNames:
                    default = '='+default
                else:
                    default = "='%s'" % default
            ret = "{name:<20}: {type:<16} # {comment} \n".format(
                name = "{name}{default}".format(name=field,default=default),
                default = default,
                type = self.header[field].type,
                comment = self.header[field].comment
            )
            return ret

        ret =  "{name}({tier})   # {comment}\n".format(
            name=self.className,
            tier=self.info.tier,
            comment=self.info.comment)

        # add strict parent references
        usedFields = []
        for parent in self.parents:
            if self.isStrictParent(parent):
                ret += '-> %s\n' % parent
                usedFields += Table(parent).primaryKey

        # add primary key fields that are not inherited from parents
        for field in self.primaryKey:
            if field not in usedFields:
                ret += makeField(field)
                usedFields += field

        # dividing line
        ret+= "-----\n"

        # add non-strict parent references
        for parent in self.parents:
            if not self.isStrictParent(parent):
                ret += '-> %s\n' % parent
                usedFields += Table(parent).primaryKey

        # add dependent fields
        for field in self.header.keys():
            if field not in usedFields:
                ret += makeField(field)
                usedFields += field

        return ret

    def create(self):
        """creates the table in the database based on self.declaration"""
        raise DataJointError("Not implemented yet")

    def __call__(*args, **kwargs):
        print "Not yet implemented"
