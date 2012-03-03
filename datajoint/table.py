import re

class Table(object):
    """
    datajoint.Table implements data definition functions
    """

    def __init__(self, className):
        self.className = className
        self._module = None


    @property
    def package(self):
        return self.className.split('.')[0]

    @property 
    def schema(self):
        "schema object of the base relvar"
        return self.module.schema

    @property 
    def module(self):
        "python module containing the base relvar"
        if not self._module:
            exec('import '+self.package+' as module')
            self._module = module
        return self._module

    @property 
    def relvarClass(self):
        "the class object of the base relvar"
        return self.module.__dict__[self.className.split('.')[1]]
        
    @property 
    def declaration(self):
        "table declaration from the base relvar class' doc string"
        return self.relvarClass.__doc__

    @property 
    def info(self):
        return self.schema.tables[self.className]


    def __repr__(self):
        ret =  "{name}({tier})   # {comment}\n".format(
            name=self.className,tier=self.info.tier,comment=self.info.comment) 
        inKey = True
        for k, attr in self.info.attrs.items():
            if inKey and not attr.isKey:
                inKey = False
                ret+= '-----\n'
            ret+= "%-16s: %-20s # %s\n" % (k, attr.type, attr.comment)
        return ret
