import collections, copy
from core import DataJointError


SQLClause = collections.namedtuple('SQLClause', ('pro','src','res'))


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
    returen [v for v in a if v in b]



class Relvar(object):
    """
    dj.Relvar provides data manipulation functions
    """

    def __init__(self, otherRel=None, **kwargs):
        # self.table must be defined by derived class
        if otherRel:
            # copy constructor
            self._conn = otherRel._conn
            self.schema = otherRel.schema
            self._sql = otherRel._sql
            self.attrs = copy.copy(otherRel.attrs)
        else:
            # constuct from the table object
            try:
                self.schema = self.table.schema
            except AttributeError:
                raise DataJointError('Relvar classes must define a property named table')
            self._conn = self.table.schema.conn
            self.attrs = copy.copy(self.table.info.attrs)
            self._sql = SQLClause(
                pro = self.attrs.keys(),
                src = "`%s`.`%s`" % (self.schema.dbname, self.table.info.name),
                res = [] 
                )

            # in-constructor restriction for base relations
            self(otherRel, **kwargs)


    @property 
    def primaryKey(self):
        "tuple of primary key attribute names"
        return [k for (k,v) in self.attrs.items() if v.isKey]


    @property
    def isDerived(self):
        "derived relvars cannot be inserted into"
        try:
            ret = not self.table or len(self._sql.res)>0
        except AttributeError:
            ret = True
        return ret


    def __call__(self, sqlCond=None, **kwargs):
        """
        In-place relational restriction by a condition.

        Condition can be one of the following:
            - a string containing an SQL condition applied to the relation
            - a dict or a namedtuple specifying fields to match
            - another relvar containing tuples to match (semijoin)
        """
        if sqlCond is not None:
            if isinstance(sqlCond, Relvar):
                self._semijoin(sqlCond)
            else: 
                self._sql = self._sql._replace(res = self._sql.res + ["(" + sqlCond + ")"])

        if kwargs:
            cond = ''
            word = ''
            for k, v in kwargs.iteritems():
                #TODO: improve datatype handling and character escaping
                cond += word+'`%s`="%s"' % (k, str(v))  
                word = " AND "
            self._sql = self._sql._replace(res = self._sql.res + ["(" + cond + ")"])

        return self



    def _semijoin(self, rel):
        raise DataJointError("Seminjoin is not yet implemented")



    def pro(self, *args):
        if args<>('*'):  
            args = union(self.primaryKey, args)   
            self._sql = self._sql._replace(pro=intersect(self._sql.pro, args))
        return self



    def fetch(self, *args):
        self.pro(*args)
        query = "SELECT {pro} FROM {src} {res}".format(
            pro = ','.join(self._sql.pro),
            src = self._sql.src, 
            res = whereList(self._sql.res))
        print "<QUERY>"
        print query
        print "</QUERY>"
        return self._conn.query(query).fetchall()



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
            query = "{command} {src} SET {sets}".format(command=command, src=self._sql.src, sets=query)
            values = [v for v in row.values()]
            self._conn.query(query, values)
