from collections.abc import Mapping
from collections import OrderedDict, defaultdict
import numpy as np
import logging
import abc
import binascii
from . import config
from . import DataJointError
from .declare import declare
from .relational_operand import RelationalOperand
from .blob import pack
from .utils import user_choice
from .heading import Heading

logger = logging.getLogger(__name__)


class BaseRelation(RelationalOperand, metaclass=abc.ABCMeta):
    """
    BaseRelation is an abstract class that represents a base relation, i.e. a table in the database.
    To make it a concrete class, override the abstract properties specifying the connection,
    table name, database, context, and definition.
    A Relation implements insert and delete methods in addition to inherited relational operators.
    """
    _heading = None
    _context = None
    database = None

    # ---------- abstract properties ------------ #
    @property
    @abc.abstractmethod
    def table_name(self):
        """
        :return: the name of the table in the database
        """

    @property
    @abc.abstractmethod
    def definition(self):
        """
        :return: a string containing the table definition using the DataJoint DDL
        """

    # -------------- required by RelationalOperand ----------------- #
    @property
    def connection(self):
        """
        :return: the connection object of the relation
        """
        return self._connection

    @property
    def heading(self):
        """
        Returns the table heading. If the table is not declared, attempts to declare it and return heading.

        :return: table heading
        """
        if self._heading is None:
            self._heading = Heading()  # instance-level heading
        if not self._heading:  # heading is not initialized
            self.declare()
            self._heading.init_from_database(self.connection, self.database, self.table_name)

        return self._heading

    def declare(self):
        """
        Loads the table heading. If the table is not declared, use self.definition to declare
        """
        if not self.is_declared:
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

    def erd(self, *args, fill=True, mode='updown', **kwargs):
        """
        :param mode: diffent methods of creating a graph pertaining to this relation.
        Currently options includes the following:
        * 'updown': Contains this relation and all other nodes that can be reached within specific
        number of ups and downs in the graph. ups(=2) and downs(=2) are optional keyword arguments
        * 'ancestors': Returs
        :return: the entity relationship diagram object of this relation
        """
        erd = self.connection.erd()
        if mode == 'updown':
            nodes = erd.up_down_neighbors(self.full_table_name, *args, **kwargs)
        elif mode == 'ancestors':
            nodes = erd.ancestors(self.full_table_name, *args, **kwargs)
        elif mode == 'descendants':
            nodes = erd.descendants(self.full_table_name, *args, **kwargs)
        else:
            raise DataJointError('Unsupported erd mode', 'Mode "%s" is currently not supported' % mode)
        return erd.restrict_by_tables(nodes, fill=fill)

    # ------------- dependencies ---------- #
    @property
    def parents(self):
        """
        :return: the parent relation of this relation
        """
        return self.connection.dependencies.parents[self.full_table_name]

    @property
    def children(self):
        """
        :return: the child relations of this relation
        """
        return self.connection.dependencies.children[self.full_table_name]

    @property
    def references(self):
        """
        :return: list of tables that this tables refers to
        """
        return self.connection.dependencies.references[self.full_table_name]

    @property
    def referenced(self):
        """
        :return: list of tables for which this table is referenced by
        """
        return self.connection.dependencies.referenced[self.full_table_name]

    @property
    def descendants(self):
        """
        Returns a list of relation objects for all children and references, recursively,
        in order of dependence. The returned values do not include self.
        This is helpful for cascading delete or drop operations.

        :return: list of descendants
        """
        relations = (FreeRelation(self.connection, table)
                     for table in self.connection.dependencies.get_descendants(self.full_table_name))
        return [relation for relation in relations if relation.is_declared]

    def _repr_helper(self):
        """
        :return: String representation of this object
        """
        return "%s.%s()" % (self.__module__, self.__class__.__name__)

    @property
    def is_declared(self):
        """
        :return: True is the table is declared
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
        self.insert([row], **kwargs)

    def insert(self, rows, replace=False, ignore_errors=False, skip_duplicates=False):
        """
        Insert a collection of rows. Additional keyword arguments are passed to insert1.

        :param rows: An iterable where an element is a valid arguments for insert1.
        :param replace=False: If True, replaces the matching data tuple in the table if it exists.
        :param ignore_errors=False: If True, ignore errors: e.g. constraint violations.
        :param skip_dublicates=False: If True, silently skip duplicate inserts.

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
                    # This is a temporary hack to address issue #131 (slow blob inserts).
                    # When this problem is fixed by pymysql or python, then pass blob as query argument.
                    placeholder = '0x' + binascii.b2a_hex(value).decode('ascii')
                    value = None
                elif heading[name].numeric:
                    if value is None or np.isnan(value):   # nans are turned into NULLs
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

            if isinstance(row, np.void):    # np.array
                check_fields(row.dtype.fields)
                attributes = [make_placeholder(name, row[name])
                              for name in heading if name in row.dtype.fields]
            elif isinstance(row, Mapping):   # dict-based
                check_fields(row.keys())
                attributes = [make_placeholder(name, row[name]) for name in heading if name in row]
            else:    # positional
                try:
                    if len(row) != len(heading):
                        raise DataJointError(
                            'Incorrect number of attributes: '
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
            return self & primary_key_value

        rows = list(make_row_to_insert(row) for row in rows)

        if not rows:
            return

        # skip duplicates only if the entire primary key is specified.
        skip_duplicates = skip_duplicates and set(heading.primary_key).issubset(set(field_list))

        if skip_duplicates:
            rows = list(row for row in rows if not row_exists(row))

        if not rows:
            return

        if replace:
            sql = 'REPLACE'
        elif ignore_errors:
            sql = 'INSERT IGNORE'
        else:
            sql = 'INSERT'

        sql += " INTO %s (`%s`) VALUES " % (self.from_clause, '`,`'.join(field_list))

        # add placeholders to sql
        sql += ','.join('(' + ','.join(row['placeholders']) + ')' for row in rows)
        # compile all values into one list
        args = []
        for r in rows:
            args.extend(v for v in r['values'] if v is not None)
        self.connection.query(sql, args=args)

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

        # construct a list (OrderedDict) of relations to delete
        relations = OrderedDict((r.full_table_name, r) for r in self.descendants)

        # construct restrictions for each relation
        restrict_by_me = set()
        restrictions = defaultdict(list)
        if self.restrictions:
            restrict_by_me.add(self.full_table_name)
            restrictions[self.full_table_name].append(self.restrictions)  # copy own restrictions
        for r in relations.values():
            restrict_by_me.update(r.references)
        for name, r in relations.items():
            for dep in (r.children + r.references):
                if name in restrict_by_me:
                    restrictions[dep].append(r)
                else:
                    restrictions[dep].extend(restrictions[name])

        # apply restrictions
        for name, r in relations.items():
            if restrictions[name]:  # do not restrict by an empty list
                r.restrict([r.project() if isinstance(r, RelationalOperand) else r
                            for r in restrictions[name]])  # project

        # execute
        do_delete = False  # indicate if there is anything to delete
        if config['safemode']:
            print('The contents of the following tables are about to be deleted:')
        for relation in relations.values():
            count = len(relation)
            if count:
                do_delete = True
                if config['safemode']:
                    print(relation.full_table_name, '(%d tuples)' % count)
            else:
                relations.pop(relation.full_table_name)
        if not do_delete:
            if config['safemode']:
                print('Nothing to delete')
        else:
            if not config['safemode'] or user_choice("Proceed?", default='no') == 'yes':
                with self.connection.transaction:
                    for r in reversed(list(relations.values())):
                        r.delete_quick()
                print('Done')

    def drop_quick(self):
        """
        Drops the table associated with this relation without cascading and without user prompt.
        If the table has any dependent table(s), this call will fail with an error.
        """
        # TODO: give a better exception message
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
        relations = self.descendants
        if config['safemode']:
            for relation in relations:
                print(relation.full_table_name, '(%d tuples)' % len(relation))
            do_drop = user_choice("Proceed?", default='no') == 'yes'
        if do_drop:
            while relations:
                relations.pop().drop_quick()
            print('Tables dropped.')

    @property
    def size_on_disk(self):
        """
        :return: size of data and indices in bytes on the storage device
        """
        ret = self.connection.query(
            'SHOW TABLE STATUS FROM `{database}` WHERE NAME="{table}"'.format(
                database=self.database, table=self.table_name), as_dict=True).fetchone()
        return ret['Data_length'] + ret['Index_length']

    # --------- functionality used by the decorator ---------
    def _prepare(self):
        """
        This method is overridden by the user_relations subclasses. It is called on an instance
        once when the class is declared.
        """
        pass


class FreeRelation(BaseRelation):
    """
    A base relation without a dedicated class. Each instance is associated with a table
    specified by full_table_name.
    """

    def __init__(self, connection, full_table_name, definition=None, context=None):
        self.database, self._table_name = (s.strip('`') for s in full_table_name.split('.'))
        self._connection = connection
        self._definition = definition
        self._context = context

    def __repr__(self):
        return "FreeRelation(`%s`.`%s`)" % (self.database, self._table_name)

    @property
    def definition(self):
        """
        Definition of the table.

        :return: the definition
        """
        return self._definition

    @property
    def connection(self):
        """
        :return: the connection object of the relation.
        """
        return self._connection

    @property
    def table_name(self):
        """
        :return: the table name in the database
        """
        return self._table_name
