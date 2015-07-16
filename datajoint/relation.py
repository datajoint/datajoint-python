from collections.abc import Mapping
import numpy as np
import logging
import abc

from . import config
from . import DataJointError
from .declare import declare
from .relational_operand import RelationalOperand
from .blob import pack
from .utils import user_choice
from .heading import Heading

logger = logging.getLogger(__name__)


class Relation(RelationalOperand, metaclass=abc.ABCMeta):
    """
    Relation is an abstract class that represents a base relation, i.e. a table in the database.
    To make it a concrete class, override the abstract properties specifying the connection,
    table name, database, context, and definition.
    A Relation implements insert and delete methods in addition to inherited relational operators.
    """
    _heading = None
    _context = None

    # ---------- abstract properties ------------ #
    @property
    @abc.abstractmethod
    def table_name(self):
        """
        :return: the name of the table in the database
        """
        raise NotImplementedError('Relation subclasses must define property table_name')

    @property
    @abc.abstractmethod
    def definition(self):
        """
        :return: a string containing the table definition using the DataJoint DDL
        """
        pass

    # -------------- required by RelationalOperand ----------------- #
    @property
    def connection(self):
        return self._connection

    @property
    def heading(self):
        """
        Get the table heading.
        If the table is not declared, attempts to declare it and return heading.
        :return:
        """
        if self._heading is None:
            self._heading = Heading()  # instance-level heading
        if not self._heading:
            if not self.is_declared:
                self.connection.query(
                    declare(self.full_table_name, self.definition, self._context))
            if self.is_declared:
                self.connection.erm.load_dependencies(self.full_table_name)
                self._heading.init_from_database(self.connection, self.database, self.table_name)
        return self._heading

    @property
    def from_clause(self):
        """
        :return: the FROM clause of SQL SELECT statements.
        """
        return self.full_table_name

    # ------------- dependencies ---------- #
    @property
    def parents(self):
        return self.connection.erm.parents[self.full_table_name]

    @property
    def children(self):
        return self.connection.erm.children[self.full_table_name]

    @property
    def references(self):
        return self.connection.erm.references[self.full_table_name]

    @property
    def referenced(self):
        return self.connection.erm.referenced[self.full_table_name]

    @property
    def descendants(self):
        """
        :return: list of relation objects for all children and references, recursively,
        in order of dependence.
        This is helpful for cascading delete or drop operations.
        """
        relations = (FreeRelation(self.connection, table)
                     for table in self.connection.erm.get_descendants(self.full_table_name))
        return [relation for relation in relations if relation.is_declared]

    # --------- SQL functionality --------- #
    @property
    def is_declared(self):
        cur = self.connection.query(
            'SHOW TABLES in `{database}`LIKE "{table_name}"'.format(
                database=self.database, table_name=self.table_name))
        return cur.rowcount > 0

    @property
    def full_table_name(self):
        return r"`{0:s}`.`{1:s}`".format(self.database, self.table_name)

    def insert(self, rows, **kwargs):
        """
        Inserts a collection of tuples. Additional keyword arguments are passed to insert1.

        :param iter: Must be an iterator that generates a sequence of valid arguments for insert.
        """
        for row in rows:
            self.insert1(row, **kwargs)

    def insert1(self, tup, replace=False, ignore_errors=False):
        """
        Insert one data record or one Mapping (like a dict).

        :param tup: Data record, a Mapping (like a dict), or a list or tuple with ordered values.
        :param replace=False: Replaces data tuple if True.
        :param ignore_errors=False: If True, ignore errors: e.g. constraint violations or duplicates

        Example::
            relation.insert1(dict(subject_id=7, species="mouse", date_of_birth="2014-09-01"))
        """
        heading = self.heading

        if isinstance(tup, np.void):    # np.array insert
            for fieldname in tup.dtype.fields:
                if fieldname not in heading:
                    raise KeyError(u'{0:s} is not in the attribute list'.format(fieldname))
            value_list = ','.join([repr(tup[name]) if not heading[name].is_blob else '%s'
                                   for name in heading if name in tup.dtype.fields])
            args = tuple(pack(tup[name]) for name in heading
                         if name in tup.dtype.fields and heading[name].is_blob)
            attribute_list = '`' + '`,`'.join(q for q in heading if q in tup.dtype.fields) + '`'

        elif isinstance(tup, Mapping):   #  dict-based insert
            for fieldname in tup.keys():
                if fieldname not in heading:
                    raise KeyError(u'{0:s} is not in the attribute list'.format(fieldname))
            value_list = ','.join(repr(tup[name]) if not heading[name].is_blob else '%s'
                                  for name in heading if name in tup)
            args = tuple(pack(tup[name]) for name in heading
                         if name in tup and heading[name].is_blob)
            attribute_list = '`' + '`,`'.join(name for name in heading if name in tup) + '`'

        else:    # positional insert
            try:
                if len(tup) != len(self.heading):
                    raise DataJointError(
                        'Tuple size does not match the number of relation attributes')
            except TypeError:
                raise DataJointError('Datatype %s cannot be inserted' % type(tup))
            else:
                pairs = zip(heading, tup)
                value_list = ','.join('%s' if heading[name].is_blob else repr(value) for name, value in pairs)
                attribute_list = '`' + '`,`'.join(heading.names) + '`'
                args = tuple(pack(value) for name, value in pairs if heading[name].is_blob)
        if replace:
            sql = 'REPLACE'
        elif ignore_errors:
            sql = 'INSERT IGNORE'
        else:
            sql = 'INSERT'
        sql += " INTO %s (%s) VALUES (%s)" % (self.from_clause, attribute_list, value_list)
        logger.info(sql)
        self.connection.query(sql, args=args)

    def delete_quick(self):
        """
        delete without cascading and without user prompt
        """
        self.connection.query('DELETE FROM ' + self.from_clause + self.where_clause)

    def delete(self):
        """
        Delete the contents of the table and its dependent tables, recursively.
        User is prompted for confirmation if config['safemode']
        """
        relations = self.descendants
        if self.restrictions and len(relations)>1:
            raise NotImplementedError('Restricted cascading deletes are not yet implemented')
        do_delete = True
        if config['safemode']:
            do_delete = False
            print('The contents of the following tables are about to be deleted:')
            for relation in relations:
                count = len(relation)
                if count:
                    do_delete = True
                    print(relation.full_table_name, '(%d tuples)' % count)
            do_delete = do_delete and user_choice("Proceed?", default='no') == 'yes'
        if do_delete:
            with self.connection.transaction:
                while relations:
                    relations.pop().delete_quick()

    def drop_quick(self):
        """
        Drops the table associated with this relation without cascading and without user prompt.
        """
        if self.is_declared:
            self.connection.query('DROP TABLE %s' % self.full_table_name)
            self.connection.erm.clear_dependencies(self.full_table_name)
            if self._heading:
                self._heading.reset()
            logger.info("Dropped table %s" % self.full_table_name)

    def drop(self):
        """
        Drop the table and all tables that reference it, recursively.
        User is prompted for confirmation if config['safemode']
        """
        do_drop = True
        relations = self.descendants
        if config['safemode']:
            print('The following tables are about to be dropped:')
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
                database=self.database, table=self.table_name), as_dict=True
        ).fetchone()
        return ret['Data_length'] + ret['Index_length']

    # --------- functionality used by the decorator ---------
    def prepare(self):
        """
        This method is overridden by the user_relations subclasses. It is called on an instance
        once when the class is declared.
        """
        pass


class FreeRelation(Relation):
    """
    A base relation without a dedicated class.  The table name is explicitly set.
    """
    def __init__(self, connection, full_table_name, definition=None, context=None):
        self.database, self._table_name = (s.strip('`') for s in full_table_name.split('.'))
        self._connection = connection
        self._definition = definition
        self._context = context

    @property
    def definition(self):
        return self._definition

    @property
    def connection(self):
        return self._connection

    @property
    def table_name(self):
        return self._table_name
