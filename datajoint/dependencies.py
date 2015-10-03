from collections import defaultdict
import pyparsing as pp
from . import DataJointError


class Dependencies:
    """
    Lookup for dependencies between tables
    """
    __primary_key_parser = (pp.CaselessLiteral('PRIMARY KEY') +
                            pp.QuotedString('(', endQuoteChar=')').setResultsName('primary_key'))

    def __init__(self, conn):
        self._conn = conn
        self._parents = dict()
        self._referenced = dict()
        self._children = defaultdict(list)
        self._references = defaultdict(list)

    @property
    def parents(self):
        return self._parents

    @property
    def children(self):
        return self._children

    @property
    def references(self):
        return self._references

    @property
    def referenced(self):
        return self._referenced

    def get_descendants(self, full_table_name):
        """
        :param full_table_name: a table name in the format `database`.`table_name`
        :return: list of all children and references, in order of dependence.
        This is helpful for cascading delete or drop operations.
        """
        ret = defaultdict(lambda: 0)

        def recurse(name, level):
            ret[name] = max(ret[name], level)
            for child in self.children[name] + self.references[name]:
                recurse(child, level+1)

        recurse(full_table_name, 0)
        return sorted(ret.keys(), key=ret.__getitem__)

    @staticmethod
    def __foreign_key_parser(database):
        def add_database(string, loc, toc):
            return ['`{database}`.`{table}`'.format(database=database, table=toc[0])]

        return (pp.CaselessLiteral('CONSTRAINT').suppress() +
                pp.QuotedString('`').suppress() +
                pp.CaselessLiteral('FOREIGN KEY').suppress() +
                pp.QuotedString('(', endQuoteChar=')').setResultsName('attributes') +
                pp.CaselessLiteral('REFERENCES') +
                pp.Or([
                    pp.QuotedString('`').setParseAction(add_database),
                    pp.Combine(pp.QuotedString('`', unquoteResults=False) + '.' +
                               pp.QuotedString('`', unquoteResults=False))]).setResultsName('referenced_table') +
                pp.QuotedString('(', endQuoteChar=')').setResultsName('referenced_attributes'))

    def load(self):
        """
        Load dependencies for all tables that have not yet been loaded in all registered schemas
        """
        for database in self._conn.schemas:
            cur = self._conn.query('SHOW TABLES FROM `{database}`'.format(database=database))
            fk_parser = self.__foreign_key_parser(database)
            # list tables that have not yet been loaded, exclude service tables that starts with ~
            tables = ('`{database}`.`{table_name}`'.format(database=database, table_name=info[0])
                      for info in cur if not info[0].startswith('~'))
            tables = (name for name in tables if name not in self._parents)
            for name in tables:
                self._parents[name] = list()
                self._referenced[name] = list()
                # fetch the CREATE TABLE statement
                create_statement = self._conn.query('SHOW CREATE TABLE %s' % name).fetchone()
                create_statement = create_statement[1].split('\n')
                primary_key = None
                for line in create_statement:
                    if primary_key is None:
                        try:
                            result = self.__primary_key_parser.parseString(line)
                        except pp.ParseException:
                            pass
                        else:
                            primary_key = [s.strip(' `') for s in result.primary_key.split(',')]
                    try:
                        result = fk_parser.parseString(line)
                    except pp.ParseException:
                        pass
                    else:
                        if not primary_key:
                            raise DataJointError('No primary key found %s' % name)
                        if result.referenced_attributes != result.attributes:
                            raise DataJointError(
                                "%s's foreign key refers to differently named attributes in %s"
                                % (self.__class__.__name__, result.referenced_table))
                        if all(q in primary_key for q in [s.strip('` ') for s in result.attributes.split(',')]):
                            self._parents[name].append(result.referenced_table)
                            self._children[result.referenced_table].append(name)
                        else:
                            self._referenced[name].append(result.referenced_table)
                            self._references[result.referenced_table].append(name)

    def get_descendants(self, full_table_name):
        """
        :param full_table_name: a table name in the format `database`.`table_name`
        :return: list of all children and references, in order of dependence.
        This is helpful for cascading delete or drop operations.
        """
        ret = defaultdict(lambda: 0)

        def recurse(name, level):
            ret[name] = max(ret[name], level)
            for child in self.children[name] + self.references[name]:
                recurse(child, level+1)

        recurse(full_table_name, 0)
        return sorted(ret.keys(), key=ret.__getitem__)
