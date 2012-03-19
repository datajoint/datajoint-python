import collections, copy, pprint
from core import DataJointError
from schema import HeaderEntry
import blob

SQLClause = collections.namedtuple('SQLClause', ('pro','src','res','groupBy'))


class NamedTupleList(list):
    """
    NamedTupleList is a list of named tuples. It provides dotted attribute lookup for the list as a whole.
    """
    def __getattribute__(self, attr):
        return [v.__getattribute__(attr) for v in self]



class Relvar(object):
    """
    relvar implements data manipulation functions:
    retrieval, insertion, and relational algebra
    """

    def __init__(self, *args, **kwargs):
        # self.table must be defined by derived class
        try:
            self.schema = self.table.schema
        except AttributeError:
            # no table --> must be a copy constructor
            if not args: 
                raise DataJointError('Relvar classes must define a property named table')
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
                res = [],
                groupBy = []
                )
            # in-constructor restriction of base relation
            self(*args, **kwargs)

    @property 
    def primaryKey(self):
        """
        tuple of primary key attribute names
        """
        return [k for (k,v) in self.header.iteritems() if v.isKey]

    @property
    def isDerived(self):
        """
        derived relvars cannot be inserted into
        """
        try:
            ret = self._sql.groupBy or not self.table or len(self._sql.pro)<len(self.header)
        except AttributeError:
            ret = True
        return ret

    
    def autoincrement(self, key={}, attr=None):
        """
        Complete the provided key with the missing field's new value that is not found in the table.
        The missing attribute name can be supplied as argument attr or (by default) taken from the primary key.
        """
        if self.isDerived or len(self._sql.res)>0:
            raise DataJointError('Cannot generate a new key for a derived relation')
        if attr is None:
            # take the first missing attribute from the primary key
            attr = setdiff(self.table.primaryKey, key)
            if not attr:
                raise DataJointError('The supplied key has no missing attributes')
            attr=attr[0]
        # retrieve the next value. TODO: replace with server-side computation
        v = self(key).fetchAttr(attr)
        try:
            v = max(v)+1   # autoincrement
        except ValueError:
            v = 0    # start with 0
        key = key.copy()
        key[attr] = v
        return key
    

    @property
    def sql(self):
        """
        the full SQL fetch statement
        """
        ret = "SELECT {pro}\n   FROM {src}\n   {res}".format(
            pro = ",".join([self.header[attr].expression+'`'+attr+'`' for attr in self._sql.pro]),
            src = self._sql.src,
            res = whereList(self._sql.res))
        if self._sql.groupBy:
            ret += ' GROUP BY `' + '`,`'.join(self._sql.groupBy)
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
        ret = ("\nRelvar (derived)" if self.isDerived
            else "\nBase relvar "+self.table.className)+"\n"
        n = self.count
        limit = max(16,min(n,20))
        ret+= '\n'+ pprint.pformat(self.fetch(limit=limit))
        if limit < n:
            ret += '\n  ...'
        ret+= "\n {count} tuples".format(count=self.count)
        return ret


    def __call__(self, *args, **kwargs):
        """  Relational restriction by conditions.

        Conditions can be one of the following:
            - a string containing an SQL condition applied to the relation
            - a set of named attributes with values to match
            - another relvar containing tuples to match (semijoin)
        """
        self = copy.copy(self)
        if kwargs:
            # collapse named arguments into a dict
            unknownAttrs = setdiff(kwargs.items(), self.header.iterms())
            if unknownAttrs:
                raise DataJointError("unknown attributes " + str(unknownAttrs))
            self(kwargs)

        for arg in args:
            if not isinstance(arg, Relvar):
                try:
                    # non-dict restrictors must provide method _asdict()
                    arg = arg._asdict()
                except AttributeError:
                    pass
                
                try:
                    cond = ''
                    word = '' 
                    for k, v in arg.iteritems():
                        if self.header.has_key(k):
                            cond += word+'`%s`="%s"' % (k, str(v))
                            word = " AND "
                    self._sql = self._sql._replace(res = self._sql.res + [cond])
                except AttributeError:
                    # if the argument does not quack like a dict, assume it's an SQL string
                    self._sql = self._sql._replace(res = self._sql.res + [arg])
            else:
                # relational semijoin
                commonAttrs = intersect(self.header.keys(), arg.header.keys())
                if commonAttrs:
                    commonAttrs = "`"+"`,`".join(commonAttrs)+"`"
                    self._sql = self._sql._replace(res = self._sql.res + [
                        '({commonAttrs}) IN (SELECT {pro} FROM {src} {res})'.format(
                            commonAttrs = commonAttrs,
                            pro = commonAttrs,
                            src = arg._sql.src,
                            res = whereList(arg._sql.res))])
        return self

    

    def __sub__(self, arg):
        """
        relational antijoin
        """
        rel = Relvar(self)   # make a copy
        commonAttrs = intersect(rel.header.keys(), arg.header.keys())
        if commonAttrs:
            commonAttrs = "`"+"`,`".join(commonAttrs)+"`"
            rel._sql = rel._sql._replace(res = rel._sql.res + [
                '({commonAttrs}) NOT IN (SELECT {pro} FROM {src} {res})'.format(
                    commonAttrs = commonAttrs,
                    pro = commonAttrs,
                    src = arg._sql.src,
                    res = whereList(arg._sql.res))])
        return rel

    
            
    def pro(self, *args):
        """
        relational projection: selects a subset of attributes
        from the original relation. 
        The primary key is always included by default.
        """
        if args<>('*',):
            args = union(self.primaryKey, args)   
            self._sql = self._sql._replace(pro=intersect(self._sql.pro, args))
        return self



    def __mul__(self, R2):
        """
        relational join
        """
        if self.isDerived or self._sql.res:
            src = '(' + self.sql + ') as ' + newAlias()
        else:
            src = self._sql.src
        src += " NATURAL JOIN "
        if R2.isDerived or R2._sql.res:
            src += '(' + R2.sql + ') as ' + newAlias()
        else:
            src += R2._sql.src
        R1 = Relvar(self)
        for k in setdiff(R2.header.keys(), R1.header.keys()):
            R1.header[k] = R2.header[k]
        R1._sql = SQLClause(
            pro = R1.header.keys(),
            res = [],
            src = src
        )
        return R1




    def extend(self, **kwargs):
        ret = Relvar(self)
        for k,v in kwargs.items():
            ret.header[k] = HeaderEntry(
                isKey = False,
                type = '',
                isNullable = True,
                comment = '',
                default = '',
                isNumeric = False,
                isString = False,
                isBlob = False,
                expression = '(' + v + ') as '
            )
            ret._sql = ret._sql._replace(pro=ret._sql.pro + [k])
        return ret



    def aggregate(self, other, **kwargs):
        """
        relational aggregation operator
        """
        ret = extend(self*other, **kwargs)
        ret._sql = ret._sql._replace(groupBy = setdiff(ret.primaryKey, other.primaryKey))





    def fetchAttr(self, attr):
        """
        fetch values from a single column
        """
        return eval('self.fetch(attr).%s' % attr)

    
    def fetch(self, *args, **kwargs):
        """
        fetches data from the database as a list of named tuples. Blobs are unpacked
        """
        rel = Relvar(self)
        R = rel._fetch(*args, **kwargs)
        Tuple = collections.namedtuple('Tuple', rel._sql.pro)
        ret = [Tuple(*rel._unpackTuple(tup)) for tup in R]
        return NamedTupleList(ret)

    

    def _fetch(self, *args, **kwargs):
        """
        Project in place and fetch data from the database
        """
        self.pro(*args)
        query = self.sql
        try:
            query += ' LIMIT %d, %d' % kwargs['limit']
        except KeyError:
            #no limit specified - do nothing
            pass
        except TypeError:
            # only the number of fields is specified
            query += ' LIMIT %d' % kwargs['limit']
            
        result = self._conn.query(query).fetchall()
        return result



    def _unpackTuple(self, tup):
        """
        unpacks blobs in a single tuple.
        The tuple must correspond to the current projection in self._sql.pro
        """
        return [blob.unpack(tup[i]) if self.header[attr].isBlob and tup[i] else tup[i]
                for i,attr in enumerate(self._sql.pro)]
    

    def insert(self, row, command="INSERT"):
        """
        insert row into the table
        row must be a dict or must define method _asdict
        If row has extra fields, they are ignored.
        """
        def makeNice(row,k):
            """
            prepare vallue from row to be inserted
            """
            v = row[k]
            if self.header[k].isBlob:
                v = blob.pack(v)
            else:
                try:
                    v = v.tolist()
                except AttributeError:
                    pass
            return v
            
        if self.isDerived:
            raise DataJointError('Cannot insert into a derived relation')
        if command.upper() not in ('INSERT', 'INSERT IGNORE', 'REPLACE'):
            raise DataJointError('Invalid insert command %s' % command)

        try:
            row = row._asdict()
        except AttributeError:
            pass

        try:
            attrs = row.keys()
        except AttributeError:
            raise DataJointError('insert tuple must be a dict or must have method _asdict()')

        attrs = [k for k in attrs if k in self.header.keys()]
        query = ','.join(['{attr}=%s'.format(attr=k) for k in attrs])
        if query:
            query = '{command} {src} SET {sets}'.format(
                command=command, src=self._sql.src, sets=query)
            values = [makeNice(row,k) for k in attrs]
            self._conn.query(query, values)




################## MISCELLANEOUS HELPER FUNCTIONS ####################
def whereList(lst):
    """convert list of conditions into a WHERE clause."""
    ret = ') AND ('.join(lst)
    if ret:
        ret = 'WHERE (%s)' % ret
    return ret

def setdiff(a,b):
    return [v for v in a if v not in b]

def union(a,b):
    return a + setdiff(b,a)

def intersect(a,b):
    return [v for v in a if v in b]

aliasNum = 1
def newAlias():
    """make an alias table name"""
    global aliasNum
    aliasNum += 1
    return "`$dj%x`" % aliasNum