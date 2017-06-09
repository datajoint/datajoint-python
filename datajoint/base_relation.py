import collections
import itertools
import inspect
import platform
import numpy as np
import pymysql
import logging
from . import config, DataJointError
from .declare import declare
from .relational_operand import RelationalOperand
from .blob import pack
from .utils import user_choice
from .heading import Heading
from .settings import server_error_codes
from . import __version__ as version

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
    _log_ = None

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

    @property
    def context(self):
        return self._context

    def declare(self):
        """
        Loads the table heading. If the table is not declared, use self.definition to declare
        """
        try:
            self.connection.query(
                declare(self.full_table_name, self.definition, self._context))
        except pymysql.OperationalError as error:
            if error.args[0] == server_error_codes['command denied']:
                logger.warning(error.args[1])
        else:
            self._log('Declared ' + self.full_table_name)

    @property
    def from_clause(self):
        """
        :return: the FROM clause of SQL SELECT statements.
        """
        return self.full_table_name

    def get_select_fields(self, select_fields=None):
        """
        :return: the selected attributes from the SQL SELECT statement.
        """
        return '*' if select_fields is None else self.heading.project(select_fields).as_sql

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

    @property
    def _log(self):
        if self._log_ is None:
            self._log_ = Log(self.connection, database=self.database)
        return self._log_

    def insert1(self, row, **kwargs):
        """
        Insert one data record or one Mapping (like a dict).
        :param row: a numpy record, a dict-like object, or an ordered sequence to be inserted as one row.
        For kwargs, see insert()
        """
        self.insert((row,), **kwargs)

    def insert(self, rows, replace=False, ignore_errors=False, skip_duplicates=False, ignore_extra_fields=False):
        """
        Insert a collection of rows.

        :param rows: An iterable where an element is a numpy record, a dict-like object, or an ordered sequence.
            rows may also be another relation with the same heading.
        :param replace: If True, replaces the existing tuple.
        :param ignore_errors: If True, ignore errors: e.g. constraint violations.
        :param skip_duplicates: If True, silently skip duplicate inserts.
        :param ignore_extra_fields: If False, fields that are not in the heading raise error.

        Example::
        >>> relation.insert([
        >>>     dict(subject_id=7, species="mouse", date_of_birth="2014-09-01"),
        >>>     dict(subject_id=8, species="mouse", date_of_birth="2014-09-02")])
        """

        if isinstance(rows, RelationalOperand):
            # INSERT FROM SELECT
            query = 'INSERT{ignore} INTO {table} ({fields}) {select}'.format(
                ignore=" IGNORE" if ignore_errors or skip_duplicates else "",
                table=self.full_table_name,
                fields='`'+'`,`'.join(rows.heading.names)+'`',
                select=rows.make_sql())
            self.connection.query(query)
            return

        heading = self.heading
        if heading.attributes is None:
            logger.warning('Could not access table {table}'.format(table=self.full_table_name))
            return

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
                if ignore_extra_fields and name not in heading:
                    return None
                if heading[name].is_blob:
                    value = pack(value)
                    placeholder = '%s'
                elif heading[name].numeric:
                    if value is None or value == '' or np.isnan(np.float(value)): # nans are turned into NULLs
                        placeholder = 'NULL'
                        value = None
                    else:
                        placeholder = '%s'
                        value = str(int(value) if isinstance(value, bool) else value)
                else:
                    placeholder = '%s'
                return name, placeholder, value

            def check_fields(fields):
                """
                Validates that all items in `fields` are valid attributes in the heading
                :param fields: field names of a tuple
                """
                if field_list is None:
                    if not ignore_extra_fields:
                        for field in fields:
                            if field not in heading:
                                raise KeyError(u'`{0:s}` is not in the table heading'.format(field))
                elif set(field_list) != set(fields).intersection(heading.names):
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
            if ignore_extra_fields:
                attributes = [a for a in attributes if a is not None]

            assert len(attributes), 'Empty tuple'
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

        rows = list(make_row_to_insert(row) for row in rows)
        if rows:
            self.connection.query(
                "{command} INTO {destination}(`{fields}`) VALUES {placeholders}".format(
                    command='REPLACE' if replace else 'INSERT IGNORE' if ignore_errors or skip_duplicates else 'INSERT',
                    destination=self.from_clause,
                    fields='`,`'.join(field_list),
                    placeholders=','.join('(' + ','.join(row['placeholders']) + ')' for row in rows)),
                args=list(itertools.chain.from_iterable((v for v in r['values'] if v is not None) for r in rows)))

    def delete_quick(self):
        """
        Deletes the table without cascading and without user prompt. If this table has any dependent
        table(s), this will fail.
        """
        query = 'DELETE FROM ' + self.full_table_name + self.where_clause
        self.connection.query(query)
        self._log(query[0:255])

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
        if config['safemode']:  # pragma: no cover
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
            query = 'DROP TABLE %s' % self.full_table_name
            self.connection.query(query)
            logger.info("Dropped table %s" % self.full_table_name)
            self._log(query[0:255])
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

    def describe(self):
        return self.show_definition()

    def show_definition(self):
        """
        :return:  the definition string for the relation using DataJoint DDL.
            This does not yet work for aliased foreign keys.
        """
        self.connection.dependencies.load()
        parents = {r: FreeRelation(self.connection, r).primary_key for r in self.parents()}
        in_key = True
        definition = '# ' + self.heading.table_info['comment'] + '\n'
        attributes_thus_far = set()
        for attr in self.heading.attributes.values():
            if in_key and not attr.in_key:
                definition += '---\n'
                in_key = False
            attributes_thus_far.add(attr.name)
            do_include = True
            for parent, primary_key in list(parents.items()):
                if attr.name in primary_key:
                    do_include = False
                if attributes_thus_far.issuperset(primary_key):
                    parents.pop(parent)
                    definition += '-> ' + (lookup_class_name(parent, self.context) or parent) + '\n'
            if do_include:
                definition += '%-20s : %-28s # %s\n' % (
                    attr.name if attr.default is None else '%s=%s' % (attr.name, attr.default),
                    '%s%s' % (attr.type, 'auto_increment' if attr.autoincrement else ''), attr.comment)
        print(definition)
        return definition

    def _update(self, attrname, value=None):
        """
            Updates a field in an existing tuple. This is not a datajoyous operation and should not be used
            routinely. Relational database maintain referential integrity on the level of a tuple. Therefore,
            the UPDATE operator can violate referential integrity. The datajoyous way to update information is
            to delete the entire tuple and insert the entire update tuple.

            Safety constraints:
               1. self must be restricted to exactly one tuple
               2. the update attribute must not be in primary key

            Example

            >>> (v2p.Mice() & key).update('mouse_dob',   '2011-01-01')
            >>> (v2p.Mice() & key).update( 'lens')   # set the value to NULL

        """
        if len(self) != 1:
            raise DataJointError('Update is only allowed on one tuple at a time')
        if attrname not in self.heading:
            raise DataJointError('Invalid attribute name')
        if attrname in self.heading.primary_key:
            raise DataJointError('Cannot update a key value.')

        attr = self.heading[attrname]

        if attr.is_blob:
            value = pack(value)
            placeholder = '%s'
        elif attr.numeric:
            if value is None or np.isnan(np.float(value)):  # nans are turned into NULLs
                placeholder = 'NULL'
                value = None
            else:
                placeholder = '%s'
                value = str(int(value) if isinstance(value, bool) else value)
        else:
            placeholder = '%s'
        command = "UPDATE {full_table_name} SET `{attrname}`={placeholder} {where_clause}".format(
            full_table_name=self.from_clause,
            attrname=attrname,
            placeholder=placeholder,
            where_clause=self.where_clause
        )
        self.connection.query(command, args=(value, ) if value is not None else ())


def lookup_class_name(name, context, depth=3):
    """
    given a table name in the form `database`.`table_name`, find its class in the context.
    :param name: `database`.`table_name`
    :param context: dictionary representing the namespace
    :param depth: search depth into imported modules, helps avoid infinite recursion.
    :return: class name found in the context or None if not found
    """
    # breadth-first search
    nodes = [dict(context=context, context_name='', depth=depth)]
    while nodes:
        node = nodes.pop(0)
        for member_name, member in node['context'].items():
            if inspect.isclass(member) and issubclass(member, BaseRelation):
                if member.full_table_name == name:   # found it!
                    return '.'.join([node['context_name'],  member_name]).lstrip('.')
                try:  # look for part tables
                    parts = member._ordered_class_members
                except AttributeError:
                    pass  # not a UserRelation -- cannot have part tables.
                else:
                    for part in (getattr(member, p) for p in parts):
                        if inspect.isclass(part) and issubclass(part, BaseRelation) and part.full_table_name == name:
                            return '.'.join([node['context_name'], member_name, part.__name__]).lstrip('.')
            elif node['depth'] > 0 and inspect.ismodule(member) and member.__name__ != 'datajoint':
                try:
                    nodes.append(
                        dict(context=dict(inspect.getmembers(member)),
                             context_name=node['context_name'] + '.' + member_name,
                             depth=node['depth']-1))
                except ImportError:
                    pass  # could not import, so do not attempt
    return None


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


class Log(BaseRelation):
    """
    The log table for each database.
    Instances are callable.  Calls log the time and identifying information along with the event.
    """

    def __init__(self, arg, database=None):
        super().__init__()

        if isinstance(arg, Log):
            # copy constructor
            self.database = arg.database
            self._connection = arg._connection
            self._definition = arg._definition
            self._user = arg._user
            return

        self.database = database
        self._connection = arg
        self._definition = """    # event logging table for `{database}`
        timestamp = CURRENT_TIMESTAMP : timestamp
        ---
        version  :varchar(12)   # datajoint version
        user     :varchar(255)  # user@host
        host=""  :varchar(255)  # system hostname
        event="" :varchar(255)  # custom message
        """.format(database=database)

        if not self.is_declared:
            self.declare()
        self._user = self.connection.get_user()

    @property
    def definition(self):
        return self._definition

    @property
    def table_name(self):
        return '~log'

    def __call__(self, event):
        try:
            self.insert1(dict(
                user=self._user,
                version=version + 'py',
                host=platform.uname().node,
                event=event), ignore_errors=True, ignore_extra_fields=True)
        except pymysql.err.OperationalError:
            logger.info('could not log event in table ~log')

    def delete(self):
        """bypass interactive prompts and cascading dependencies"""
        self.delete_quick()

    def drop(self):
        """bypass interactive prompts and cascading dependencies"""
        self.drop_quick()
