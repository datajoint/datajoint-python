import collections, copy, pprint
import abc
from core import DataJointError
from schema import Header, HeaderEntry
import blob


class NamedTupleList(list):
    """
    NamedTupleList is a list of named tuples. It provides dotted attribute lookup for the list as a whole.
    """
    def __getattribute__(self, attr):
        return [v.__getattribute__(attr) for v in self]


class GeneralRelvar(object):
    """
    GeneralRelvar implements relational algebra and data query functions
    on either base or derived relations.
    """
    __aliasCount = 0

    def __init__(self, operator, operands, restrictions=None):
        self._operator = operator
        self._operands = operands

        self._restrictions = restrictions or []
        self.schema = self._operands[0].schema

    @property
    def sql(self):
        return self.__compile()[1]

    @property 
    def header(self):
        return self.__compile()[0]

    @property 
    def primaryKey(self):
        return [k for (k,v) in self.header.iteritems() if v.isKey]

    @property
    def conn(self):
        return self.schema.conn

    @property
    def count(self):
        query = "SELECT count(*) FROM %s" % self.sql
        return self.conn.query(query).fetchall()[0][0]

    def __repr__(self):
        ret = ""
        n = self.count
        limit = max(16,min(n,20))
        ret+= '\n'+ pprint.pformat(self.fetch(limit=limit))
        if limit < n:
            ret += '\n  ...'
        ret+= "\n {count} tuples".format(count=n)
        return ret

    # RELATIONAL ALGEBRA ######
    def restrict(self, *args, **kwargs):
        self._restrictions += args
        if kwargs:
            self._restrictions.append(kwargs)

    def __call__(self, *args, **kwargs):
        ret = copy.copy(self)
        ret.restrict(*args, **kwargs)
        return ret

    def __sub__(self, other):
        return self('not', other)

    def pro(self, *args, **kwargs):
        return GeneralRelvar('pro', [self]+list(args)+kwargs.items())

    def __mul__(self, other):
        return GeneralRelvar('join', [self, other])


    def __compile(self):
        """
        return (header, sql)
        """

        def recurse(rel):
            self.__aliasCount += 1
            header, sql = rel.__compile()
            #isolate previous projection (if not already)
            if rel._operator == 'pro' and not rel.restrictions:
                sql = '(SELECT %s FROM %s AS `$a%x' % (header.attrList, sql, self.__aliasCount)
            return header, sql

        def makeWhereClause(header, args):
            """
            make an SQL where clause from a list of restrictions in args
            """
            if not args:
                return ""
            else:
                s = []
                for arg in args:
                    if isinstance(arg, GeneralRelvar):
                        assert False   # not implemented yet
                    else:
                        # apply a dict-like condition
                        try:
                            arg = arg._asdict()
                        except AttributeError: pass
                        try:
                            for k, v in arg.iteritems():
                                if k in header:
                                    s.append('`%s`="%s"' % (k, str(v)))
                        except AttributeError:
                            # if the argument does not quack like a dict, assume it's an SQL string
                            s.append('(%s)'%arg)
                s = ' WHERE ' + ' AND '.join(s)
                return s

        if self._operator=='table':
            table = self._operands[0]
            sql = '`{dbname}`.`{tablename}`'.format(dbname=table.schema.dbname, tablename=table.info.name)
            header = table.header
        else:
            if self._operator == 'pro':
                header, sql = recurse(self._operands.pop(0))
                if self._operands and isinstance(self._operands[0], GeneralRelvar):
                    header2, sql2 = recurse(self._operands.pop(0))
                    commonAttrs = [s for h in header.keys() if h in header2.keys()]
                    sql = '%s as `$r%x` NATURAL JOIN %s as `$q%x` GROUP BY (`%s`)' % (
                        sql, aliasCount, sql2, aliasCount, '`.`'.join(commonAttrs)
                    )
                header = header.pro(self._operands)

            elif self._operator == 'join':
                header1, sql1 = recurse(self._operands[0])
                header2, sql2 = recurse(self._operands[1])
                header2.update(header1)
                header = header2
                sql = '%s NATURAL JOIN %s' % (sql1, sql2)
            else:
                raise RuntimeError('Unimplemented relational operator: ' + self._operator)

        # apply restrictions
        if self._restrictions:
            # clear aliases
            if [True for s in header.itervalues() if s.alias]:
                self.__aliasCount+=1
                sql = '(SELECT %s FROM %s) as `$s%x`' % (header.attrList, sql, self.__aliasCount)
                header.clearAliases()
            sql += makeWhereClause(header, self._restrictions)

        return header, sql

    
            

    def fetchAttr(self, attr):
        """
        fetch values from a single column
        """
        return eval('self.fetch(attr).%s' % attr)

    
    def fetch(self, *args, **kwargs):
        """
        fetches data from the database as a list of named tuples. Blobs are unpacked
        """
        def unpackTuple(tup, header):
            return [blob.unpack(tup[i]) if v.isBlob and tup[i] else tup[i]
                for i,v in enumerate(header.values())]
        try:
            limit = kwargs.pop('LIMIT')
        except KeyError:
            limit = ''
        else:
            try:
                limit = ' LIMIT %d, %d' % limit
            except TypeError:
                limit = ' LIMIT %d' % limit

        rel  = self.pro(*args, **kwargs)
        [header, sql] = rel.__compile()
        sql = 'SELECT {attrList} FROM {sql}{limit}'.format(attrList=header.attrList, sql=sql, limit=limit)
        R = self.conn.query(sql)
        Tuple = collections.namedtuple('Tuple', header)
        ret = [Tuple(*unpackTuple(tup,header)) for tup in R]
        return NamedTupleList(ret)

    


class BaseRelvar(GeneralRelvar):
    """
    BaseRelvar is a relvar linked to a specific table in the database with no additional operators applied
    other than restriction.
    """
    
    def __init__(self, table, restrictions=None):
        GeneralRelvar.__init__(self, 'table', [table], restrictions)

    @property
    def _table(self):
        return self._operands[0]
    

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
            if self._table.header[k].isBlob:
                v = blob.pack(v)
            return v

        command = command.upper()
        if command not in ('INSERT', 'INSERT IGNORE', 'REPLACE'):
            raise DataJointError('Invalid insert command %s' % command)

        try:
            row = row._asdict()
        except AttributeError:
            pass

        try:
            attrs = row.keys()
        except AttributeError:
            raise DataJointError('insert tuple must be a dict or must have method _asdict()')


        attrList = ','.join(['{attr}=%s'.format(attr=k) for k in attrs if k in self._table.header])
        if attrList:
            query = '{command} `{dbname}`.`{table}` SET {sets}'.format(
                command=command, dbname=self.schema.dbname, table=self._table.info.name, sets=attrList)
            # pack blobs
            values = [makeNice(row,k) for k in attrs]
            self.conn.query(query, values)


    def autoincrement(self, key={}, attr=None):
        """
        autoincrement completes the provided key with the missing field's smallest value that is not found in the table.
        The missing attribute name can be supplied as argument attr or (by default) taken from the primary key.
        """
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




class Relvar(BaseRelvar):
    """
    Relvar is an abstract class which represents a table in the database. Deriving classes must define the property
    table of type dj.Table.
    """
    __metaclass__ = abc.ABCMeta

    @abc.abstractproperty
    def table(self):
        """
        Relvars must define the property table
        """
        pass

    def __init__(self, *args, **kwargs):
        BaseRelvar.__init__(self, self.table)
        self.restrict(*args, **kwargs)




    
def setdiff(a,b):
    return [v for v in a if v not in b]

def union(a,b):
    return a + setdiff(b,a)

def intersect(a,b):
    return [v for v in a if v in b]