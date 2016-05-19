import pyparsing as pp
import networkx as nx


class Dependencies(nx.DiGraph):
    """
    Lookup for dependencies between tables
    """
    __primary_key_parser = (pp.CaselessLiteral('PRIMARY KEY') +
                            pp.QuotedString('(', endQuoteChar=')').setResultsName('primary_key'))

    def __init__(self, conn):
        self._conn = conn
        self.loaded_tables = set()
        super().__init__(self)

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
        Load dependencies for all loaded schemas.
        This method gets called before any operation that requires dependencies: delete, drop, populate, progress.
        """
        for database in self._conn.schemas:
            cur = self._conn.query('SHOW TABLES FROM `{database}`'.format(database=database))
            fk_parser = self.__foreign_key_parser(database)
            tables = ('`{database}`.`{table_name}`'.format(database=database, table_name=info[0])
                      for info in cur if not info[0].startswith('~'))  # exclude service tables
            for name in tables:
                if name in self.loaded_tables:
                    continue
                self.loaded_tables.add(name)
                self.add_node(name)
                create_statement = self._conn.query('SHOW CREATE TABLE %s' % name).fetchone()[1].split('\n')
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
                        self.add_edge(result.referenced_table, name,
                                      primary=all(r.strip('` ') in primary_key for r in result.attributes.split(',')))

    def descendants(self, full_table_name):
        """
        :param full_table_name:  In form `schema`.`table_name`
        :return: all dependent tables sorted by path length, including self.
        """
        path = nx.shortest_path_length(self, full_table_name)
        return sorted(path.keys(), key=path.__getitem__)
