from core import DataJointError

class Table(object):
    """
    datajoint.Table implements data definition functions
    """

    constantNames = ("CURRENT_TIME", "CURRENT_DATE", "CURRENT_TIMESTAMP")

    def __init__(self, className):
        self.className = className
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
            exec('import '+self.package+' as module')
            self._module = module
        return self._module

    @property
    def parents(self):
        return self.info.parents

    def isStrictParent(self, parent):
        """
        True if all the primary key fields in parent are also primary key fields in self 
        This indicates a purely hierarchical relationship.
        Parent here is a class name.
        """
        return reduce(lambda x, y: x and (y in self.primaryKey), Table(parent).primaryKey)

    @property
    def children(self):
        return self.info.children

    @property
    def header(self):
        """
        attribute information
        """
        return self.info.header


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


    @property
    def primaryKey(self):
        return [k for (k,v) in self.header.iteritems() if v.isKey]

    @property 
    def info(self):
        """
        table name, tier, comment retrieved from the database
        """
        return self.schema.tables[self.className]


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