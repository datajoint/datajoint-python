import collections, copy
import numpy as np
from core import DataJointError


SQLClause = collections.namedtuple('SQLClause', ('pro','src','res'))


class Relvar(object):
    """
    datajoint.Relvar provides data manipulation functions
    """

    def __init__(self, *args, **kwargs):
        # self.table must be defined by derived class
        try:
            self.schema = self.table.schema
        except AttributeError:
            # no table --> must be a copy constructor
            if not args: 
                raise DataJointError(
                    'Relvar classes must define a property named table')
            else:
                if len(args)<>1 or not isinstance(args[0], Relvar):
                    raise DataJointError('Relvar has no default constructor')
                # copy constructor
                arg = args[0]
                self._conn = arg._conn
                self.schema = arg.schema
                self._sql = arg._sql
                self.header = copy.copy(arg.header)
        else:
            # load data from table
            self._conn = self.table.schema.conn
            self.header = copy.copy(self.table.info.header)
            self._sql = SQLClause(
                pro = self.header.keys(),
                src = "`%s`.`%s`" % (self.schema.dbname, self.table.info.name),
                res = [] 
                )
            # in-constructor restriction of base relation
            self(*args, **kwargs)


    @property 
    def primaryKey(self):
        """ tuple of primary key attribute names
        """
        return [k for (k,v) in self.header.items() if v.isKey]


    @property
    def isDerived(self):
        """ derived relvars cannot be inserted into
        """
        try:
            ret = not self.table or len(self._sql.pro)<len(self.header)
        except AttributeError:
            ret = True
        return ret


    @property
    def count(self):
        """return the number of tuples in the relation
        """
        query = "SELECT count(*) FROM {src} {res}".format(
            pro = ','.join(self._sql.pro), 
            src = self._sql.src,
            res = whereList(self._sql.res))
        return self._conn.query(query).fetchall()[0][0]



    def __repr__(self):
        ret = ("Relvar (derived)" if self.isDerived 
            else "Base relvar "+self.table.className)+"\n"
        inKey = True
        for k, attr in self.header.items():
            if k in self._sql.pro: 
                if inKey and not attr.isKey:
                    inKey = False
                    ret+= '-----\n'
                ret+= "%-16s: %-20s # %s\n" % (k, attr.type, attr.comment)
        ret+= "  {count} tuples".format(count=self.count)
        return ret



    def __call__(self, *args, **kwargs):
        """
        In-place relational restriction by conditions.

        Conditions can be one of the following:
            - a string containing an SQL condition applied to the relation
            - a set of named attributes with values to match
            - another relvar containing tuples to match (semijoin)
        """
        for arg in args:
            if isinstance(arg, Relvar):
                self._semijoin(arg)
            else: 
                self._sql = self._sql._replace(
                    res = self._sql.res + ["(" + arg + ")"])

        if kwargs:
            cond = ''
            word = ''
            for k, v in kwargs.iteritems():
                #TODO: improve datatype handling and character escaping
                cond += word+'`%s`="%s"' % (k, str(v))  
                word = " AND "
            self._sql = self._sql._replace(
                res = self._sql.res + ["(" + cond + ")"])

        return self



    def _semijoin(self, rel):
        raise DataJointError("Seminjoin is not yet implemented")



    def pro(self, *args):
        """
        relational projection: selects a subset of attributes
        from the original relation. 
        The primary key is always included by default.
        """
        if args<>('*'):  
            args = union(self.primaryKey, args)   
            self._sql = self._sql._replace(pro=intersect(self._sql.pro, args))
        return self



    def fetch(self, *args):
        R = self.pro(*args)
        query = "SELECT {pro} FROM {src} {res}".format(
            pro = ','.join(R._sql.pro),
            src = R._sql.src, 
            res = whereList(R._sql.res))
        result = R._conn.query(query).fetchall()
        return result         



    def insert(self, row, command="INSERT"):
        """
        insert row into the table
        row must be a dict
        """

        if self.isDerived:
            raise DataJointError("Cannot insert into a derived relation")
        if command.upper() not in ("INSERT", "INSERT IGNORE", "REPLACE"):
            raise DataJointError("Invalid insert command %s" % command)

        query = ','.join(['{attr}=%s'.format(attr=k)  for k in row.keys()])
        if query:
            query = "{command} {src} SET {sets}".format(
                command=command, src=self._sql.src, sets=query)
            values = [v for v in row.values()]
            self._conn.query(query, values)





################## MISCELLANEOUS HELPER FUNCTIONS ####################

def whereList(lst):
    "convert list of strings into WHERE clause"
    ret = " AND ".join(lst)
    ret = "WHERE "+ret if ret else ""
    return ret


def setdiff(a,b):
    return [v for v in a if v not in b]


def union(a,b):
    return a + setdiff(b,a)


def intersect(a,b):
    return [v for v in a if v in b]

