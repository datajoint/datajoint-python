import collections
import itertools
import numpy as np
import logging
from . import config, DataJointError
from .declare import declare
from .relational_operand import RelationalOperand
from .blob import pack
from .utils import user_choice
from .heading import Heading

logger = logging.getLogger(__name__)


class BaseRelation(RelationalOperand):
    """
    BaseRelation is an abstract class that represents a base relation, i.e. a table in the database.
    To make it a concrete class, override the abstract properties specifying the connection,
    table name, database, context, and definition.
    A Relation implements insert and delete methods in addition to inherited relational operators.
    """
    _heading = None
    _context = None
    database = None

    # -------------- required by RelationalOperand ----------------- #
    @property
    def heading(self):
        """
        Returns the table heading. If the table is not declared, attempts to declare it and return heading.
        :return: table heading
        """
        if self._heading is None:
            self._heading = Heading()  # instance-level heading
        if not self._heading:  # lazy loading of heading
            self._heading.init_from_database(self.connection, self.database, self.table_name)
        return self._heading

    def declare(self):
        """
        Loads the table heading. If the table is not declared, use self.definition to declare
        """
        self.connection.query(
            declare(self.full_table_name, self.definition, self._context))

    @property
    def from_clause(self):
        """
        :return: the FROM clause of SQL SELECT statements.
        """
        return self.full_table_name

    @property
    def select_fields(self):
        """
        :return: the selected attributes from the SQL SELECT statement.
        """
        return '*'

    def parents(self, primary=None):
        """
        :param primary: if None, then all parents are returned. If True, then only foreign keys composed of
        primary key attributes are considered.  If False, the only foreign keys including at least one non-primary
        attribute are considered.
        :return: list of tables referenced with self's foreign keys
        """
        return [p[0] for p in self.connection.dependencies.in_edges(self.full_table_name, data=True)
                if primary is None or p[2]['primary'] == primary]

    def children(self, primary=None):
        """
        :param primary: if None, then all parents are returned. If True, then only foreign keys composed of
        primary key attributes are considered.  If False, the only foreign keys including at least one non-primary
        attribute are considered.
        :return: list of tables with foreign keys referencing self
        """
        return [p[1] for p in self.connection.dependencies.out_edges(self.full_table_name, data=True)
                if primary is None or p[2]['primary'] == primary]

    @property
    def is_declared(self):
        """
        :return: True is the table is declared in the database
        """
        cur = self.connection.query(
            'SHOW TABLES in `{database}`LIKE "{table_name}"'.format(
                database=self.database, table_name=self.table_name))
        return cur.rowcount > 0

    @property
    def full_table_name(self):
        """
        :return: full table name in the database
        """
        return r"`{0:s}`.`{1:s}`".format(self.database, self.table_name)

    def insert1(self, row, **kwargs):
        """
        Insert one data record or one Mapping (like a dict).
        :param row: Data record, a Mapping (like a dict), or a list or tuple with ordered values.
        """
        self.insert((row,), **kwargs)

    def insert(self, rows, replace=False, ignore_errors=False, skip_duplicates=False):
        """
        Insert a collection of rows. Additional keyword arguments are passed to insert1.

        :param rows: An iterable where an element is a valid arguments for insert1.
        :param replace: If True, replaces the matching data tuple in the table if it exists.
        :param ignore_errors: If True, ignore errors: e.g. constraint violations.
        :param skip_duplicates: If True, silently skip duplicate inserts.

        Example::
        >>> relation.insert([
        >>>     dict(subject_id=7, species="mouse", date_of_birth="2014-09-01"),
        >>>     dict(subject_id=8, species="mouse", date_of_birth="2014-09-02")])
        """
        heading = self.heading
        field_list = None  # ensures that all rows have the same attributes in the same order as the first row.

        def make_row_to_insert(row):
            """
            :param row:  A tuple to insert
            :return: a dict with fields 'names', 'placeholders', 'values'
            """

            def make_placeholder(name, value):
                """
                For a given attribute `name` with `value`, return its processed value or value placeholder
                as a string to be included in the query and the value, if any, to be submitted for
                processing by mysql API.
                :param name:
                :param value:
                """
                if heading[name].is_blob:
                    value = pack(value)
                    placeholder = '%s'
                elif heading[name].numeric:
                    if value is None or np.isnan(value):  # nans are turned into NULLs
                        placeholder = 'NULL'
                        value = None
                    else:
                        placeholder = '%s'
                        value = repr(int(value) if isinstance(value, bool) else value)
                else:
                    placeholder = '%s'
                return name, placeholder, value

            def check_fields(fields):
                """
                Validates that all items in `fields` are valid attributes in the heading
                :param fields: field names of a tuple
                """
                if field_list is None:
                    for field in fields:
                        if field not in heading:
                            raise KeyError(u'{0:s} is not in the attribute list'.format(field))
                elif set(field_list) != set(fields):
                    raise DataJointError('Attempt to insert rows with different fields')

            if isinstance(row, np.void):  # np.array
                check_fields(row.dtype.fields)
                attributes = [make_placeholder(name, row[name])
                              for name in heading if name in row.dtype.fields]
            elif isinstance(row, collections.abc.Mapping):  # dict-based
                check_fields(row.keys())
                attributes = [make_placeholder(name, row[name]) for name in heading if name in row]
            else:  # positional
                try:
                    if len(row) != len(heading):
                        raise DataJointError(
                            'Invalid insert argument. Incorrect number of attributes: '
                            '{given} given; {expected} expected'.format(
                                given=len(row), expected=len(heading)))
                except TypeError:
                    raise DataJointError('Datatype %s cannot be inserted' % type(row))
                else:
                    attributes = [make_placeholder(name, value) for name, value in zip(heading, row)]

            if not attributes:
                raise DataJointError('Empty tuple')
            row_to_insert = dict(zip(('names', 'placeholders', 'values'), zip(*attributes)))
            nonlocal field_list
            if field_list is None:
                # first row sets the composition of the field list
                field_list = row_to_insert['names']
            else:
                #  reorder attributes in row_to_insert to match field_list
                order = list(row_to_insert['names'].index(field) for field in field_list)
                row_to_insert['names'] = list(row_to_insert['names'][i] for i in order)
                row_to_insert['placeholders'] = list(row_to_insert['placeholders'][i] for i in order)
                row_to_insert['values'] = list(row_to_insert['values'][i] for i in order)

            return row_to_insert

        def row_exists(row):
            """
            :param row: a dict with keys 'row' and 'values'.
            :return: True if row is already in table.
            """
            primary_key_value = dict((name, value)
                                     for (name, value) in zip(row['names'], row['values']) if heading[name].in_key)
            return primary_key_value in self

        rows = list(make_row_to_insert(row) for row in rows)
        if rows:
            # skip duplicates only if the entire primary key is specified.
            skip_duplicates = skip_duplicates and set(heading.primary_key).issubset(set(field_list))
            if skip_duplicates:
                rows = list(row for row in rows if not row_exists(row))
            if rows:
                self.connection.query(
                    "{command} INTO {destination}(`{fields}`) VALUES {placeholders}".format(
                        command='REPLACE' if replace else 'INSERT IGNORE' if ignore_errors else 'INSERT',
                        destination=self.from_clause,
                        fields='`,`'.join(field_list),
                        placeholders=','.join('(' + ','.join(row['placeholders']) + ')' for row in rows)),
                    args=list(itertools.chain.from_iterable((v for v in r['values'] if v is not None) for r in rows)))

    def delete_quick(self):
        """
        Deletes the table without cascading and without user prompt. If this table has any dependent
        table(s), this will fail.
        """
        self.connection.query('DELETE FROM ' + self.from_clause + self.where_clause)

    def delete(self):
        """
        Deletes the contents of the table and its dependent tables, recursively.
        User is prompted for confirmation if config['safemode'] is set to True.
        """
        self.connection.dependencies.load()

        relations_to_delete = collections.OrderedDict(
            (r, FreeRelation(self.connection, r))
            for r in self.connection.dependencies.descendants(self.full_table_name))

        # construct restrictions for each relation
        restrict_by_me = set()
        restrictions = collections.defaultdict(list)
        if self.restrictions:
            restrict_by_me.add(self.full_table_name)
            restrictions[self.full_table_name].append(self.restrictions)  # copy own restrictions
        for r in relations_to_delete.values():
            restrict_by_me.update(r.children(primary=False))
        for name, r in relations_to_delete.items():
            for dep in r.children():
                if name in restrict_by_me:
                    restrictions[dep].append(r)
                else:
                    restrictions[dep].extend(restrictions[name])

        # apply restrictions
        for name, r in relations_to_delete.items():
            if restrictions[name]:  # do not restrict by an empty list
                r.restrict([r.proj() if isinstance(r, RelationalOperand) else r
                            for r in restrictions[name]])  # project
        # execute
        do_delete = False  # indicate if there is anything to delete
        if config['safemode']:
            print('The contents of the following tables are about to be deleted:')
        for relation in list(relations_to_delete.values()):
            count = len(relation)
            if count:
                do_delete = True
                if config['safemode']:
                    print(relation.full_table_name, '(%d tuples)' % count)
            else:
                relations_to_delete.pop(relation.full_table_name)
        if not do_delete:
            if config['safemode']:
                print('Nothing to delete')
        else:
            if not config['safemode'] or user_choice("Proceed?", default='no') == 'yes':
                with self.connection.transaction:
                    for r in reversed(list(relations_to_delete.values())):
                        r.delete_quick()
                print('Done')

    def drop_quick(self):
        """
        Drops the table associated with this relation without cascading and without user prompt.
        If the table has any dependent table(s), this call will fail with an error.
        """
        if self.is_declared:
            self.connection.query('DROP TABLE %s' % self.full_table_name)
            logger.info("Dropped table %s" % self.full_table_name)
        else:
            logger.info("Nothing to drop: table %s is not declared" % self.full_table_name)

    def drop(self):
        """
        Drop the table and all tables that reference it, recursively.
        User is prompted for confirmation if config['safemode'] is set to True.
        """
        self.connection.dependencies.load()
        do_drop = True
        tables = self.connection.dependencies.descendants(self.full_table_name)
        if config['safemode']:
            for table in tables:
                print(table, '(%d tuples)' % len(FreeRelation(self.connection, table)))
            do_drop = user_choice("Proceed?", default='no') == 'yes'
        if do_drop:
            for table in reversed(tables):
                FreeRelation(self.connection, table).drop_quick()
            print('Tables dropped.  Restart kernel.')

    @property
    def size_on_disk(self):
        """
        :return: size of data and indices in bytes on the storage device
        """
        ret = self.connection.query(
            'SHOW TABLE STATUS FROM `{database}` WHERE NAME="{table}"'.format(
                database=self.database, table=self.table_name), as_dict=True).fetchone()
        return ret['Data_length'] + ret['Index_length']


class FreeRelation(BaseRelation):
    """
    A base relation without a dedicated class. Each instance is associated with a table
    specified by full_table_name.
    :param arg:  a dj.Connection or a dj.FreeRelation
    """

    def __init__(self, arg, full_table_name=None):
        super().__init__()
        if isinstance(arg, FreeRelation):
            # copy constructor
            self.database = arg.database
            self._table_name = arg._table_name
            self._connection = arg._connection
        else:
            self.database, self._table_name = (s.strip('`') for s in full_table_name.split('.'))
            self._connection = arg

    def __repr__(self):
        return "FreeRelation(`%s`.`%s`)" % (self.database, self._table_name)

    @property
    def table_name(self):
        """
        :return: the table name in the database
        """
        return self._table_name
