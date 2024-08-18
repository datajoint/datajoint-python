import collections
import itertools
import inspect
import platform
import numpy as np
import pandas
import logging
import uuid
import csv
import re
import json
from pathlib import Path
from .settings import config
from .declare import declare, alter
from .condition import make_condition
from .expression import QueryExpression
from . import blob
from .utils import user_choice, get_master, is_camel_case
from .heading import Heading
from .errors import (
    DuplicateError,
    AccessError,
    DataJointError,
    UnknownAttributeError,
    IntegrityError,
)
from typing import Union
from .version import __version__ as version

logger = logging.getLogger(__name__.split(".")[0])

foreign_key_error_regexp = re.compile(
    r"[\w\s:]*\((?P<child>`[^`]+`.`[^`]+`), "
    r"CONSTRAINT (?P<name>`[^`]+`) "
    r"(FOREIGN KEY \((?P<fk_attrs>[^)]+)\) "
    r"REFERENCES (?P<parent>`[^`]+`(\.`[^`]+`)?) \((?P<pk_attrs>[^)]+)\)[\s\w]+\))?"
)

constraint_info_query = " ".join(
    """
    SELECT
        COLUMN_NAME as fk_attrs,
        CONCAT('`', REFERENCED_TABLE_SCHEMA, '`.`', REFERENCED_TABLE_NAME, '`') as parent,
        REFERENCED_COLUMN_NAME as pk_attrs
    FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
    WHERE
        CONSTRAINT_NAME = %s AND TABLE_SCHEMA = %s AND TABLE_NAME = %s;
    """.split()
)


class _RenameMap(tuple):
    """for internal use"""

    pass


class Table(QueryExpression):
    """
    Table is an abstract class that represents a table in the schema.
    It implements insert and delete methods and inherits query functionality.
    To make it a concrete class, override the abstract properties specifying the connection,
    table name, database, and definition.
    """

    _table_name = None  # must be defined in subclass
    _log_ = None  # placeholder for the Log table object

    # These properties must be set by the schema decorator (schemas.py) at class level
    # or by FreeTable at instance level
    database = None
    declaration_context = None

    @property
    def table_name(self):
        return self._table_name

    @property
    def class_name(self):
        return self.__class__.__name__

    @property
    def definition(self):
        raise NotImplementedError(
            "Subclasses of Table must implement the `definition` property"
        )

    def declare(self, context=None):
        """
        Declare the table in the schema based on self.definition.

        :param context: the context for foreign key resolution. If None, foreign keys are
            not allowed.
        """
        if self.connection.in_transaction:
            raise DataJointError(
                "Cannot declare new tables inside a transaction, "
                "e.g. from inside a populate/make call"
            )
        # Enforce strict CamelCase #1150
        if not is_camel_case(self.class_name):
            raise DataJointError(
                "Table class name `{name}` is invalid. Please use CamelCase. ".format(
                    name=self.class_name
                )
                + "Classes defining tables should be formatted in strict CamelCase."
            )
        sql, external_stores = declare(self.full_table_name, self.definition, context)
        sql = sql.format(database=self.database)
        try:
            # declare all external tables before declaring main table
            for store in external_stores:
                self.connection.schemas[self.database].external[store]
            self.connection.query(sql)
        except AccessError:
            # skip if no create privilege
            pass
        else:
            self._log("Declared " + self.full_table_name)

    def alter(self, prompt=True, context=None):
        """
        Alter the table definition from self.definition
        """
        if self.connection.in_transaction:
            raise DataJointError(
                "Cannot update table declaration inside a transaction, "
                "e.g. from inside a populate/make call"
            )
        if context is None:
            frame = inspect.currentframe().f_back
            context = dict(frame.f_globals, **frame.f_locals)
            del frame
        old_definition = self.describe(context=context)
        sql, external_stores = alter(self.definition, old_definition, context)
        if not sql:
            if prompt:
                logger.warn("Nothing to alter.")
        else:
            sql = "ALTER TABLE {tab}\n\t".format(
                tab=self.full_table_name
            ) + ",\n\t".join(sql)
            if not prompt or user_choice(sql + "\n\nExecute?") == "yes":
                try:
                    # declare all external tables before declaring main table
                    for store in external_stores:
                        self.connection.schemas[self.database].external[store]
                    self.connection.query(sql)
                except AccessError:
                    # skip if no create privilege
                    pass
                else:
                    # reset heading
                    self.__class__._heading = Heading(
                        table_info=self.heading.table_info
                    )
                    if prompt:
                        logger.info("Table altered")
                    self._log("Altered " + self.full_table_name)

    def from_clause(self):
        """
        :return: the FROM clause of SQL SELECT statements.
        """
        return self.full_table_name

    def get_select_fields(self, select_fields=None):
        """
        :return: the selected attributes from the SQL SELECT statement.
        """
        return (
            "*" if select_fields is None else self.heading.project(select_fields).as_sql
        )

    def parents(self, primary=None, as_objects=False, foreign_key_info=False):
        """

        :param primary: if None, then all parents are returned. If True, then only foreign keys composed of
            primary key attributes are considered.  If False, return foreign keys including at least one
            secondary attribute.
        :param as_objects: if False, return table names. If True, return table objects.
        :param foreign_key_info: if True, each element in result also includes foreign key info.
        :return: list of parents as table names or table objects
            with (optional) foreign key information.
        """
        get_edge = self.connection.dependencies.parents
        nodes = [
            next(iter(get_edge(name).items())) if name.isdigit() else (name, props)
            for name, props in get_edge(self.full_table_name, primary).items()
        ]
        if as_objects:
            nodes = [(FreeTable(self.connection, name), props) for name, props in nodes]
        if not foreign_key_info:
            nodes = [name for name, props in nodes]
        return nodes

    def children(self, primary=None, as_objects=False, foreign_key_info=False):
        """

        :param primary: if None, then all children are returned. If True, then only foreign keys composed of
            primary key attributes are considered.  If False, return foreign keys including at least one
            secondary attribute.
        :param as_objects: if False, return table names. If True, return table objects.
        :param foreign_key_info: if True, each element in result also includes foreign key info.
        :return: list of children as table names or table objects
            with (optional) foreign key information.
        """
        get_edge = self.connection.dependencies.children
        nodes = [
            next(iter(get_edge(name).items())) if name.isdigit() else (name, props)
            for name, props in get_edge(self.full_table_name, primary).items()
        ]
        if as_objects:
            nodes = [(FreeTable(self.connection, name), props) for name, props in nodes]
        if not foreign_key_info:
            nodes = [name for name, props in nodes]
        return nodes

    def descendants(self, as_objects=False):
        """

        :param as_objects: False - a list of table names; True - a list of table objects.
        :return: list of tables descendants in topological order.
        """
        return [
            FreeTable(self.connection, node) if as_objects else node
            for node in self.connection.dependencies.descendants(self.full_table_name)
            if not node.isdigit()
        ]

    def ancestors(self, as_objects=False):
        """

        :param as_objects: False - a list of table names; True - a list of table objects.
        :return: list of tables ancestors in topological order.
        """
        return [
            FreeTable(self.connection, node) if as_objects else node
            for node in self.connection.dependencies.ancestors(self.full_table_name)
            if not node.isdigit()
        ]

    def parts(self, as_objects=False):
        """
        return part tables either as entries in a dict with foreign key information or a list of objects

        :param as_objects: if False (default), the output is a dict describing the foreign keys. If True, return table objects.
        """
        nodes = [
            node
            for node in self.connection.dependencies.nodes
            if not node.isdigit() and node.startswith(self.full_table_name[:-1] + "__")
        ]
        return [FreeTable(self.connection, c) for c in nodes] if as_objects else nodes

    @property
    def is_declared(self):
        """
        :return: True is the table is declared in the schema.
        """
        return (
            self.connection.query(
                'SHOW TABLES in `{database}` LIKE "{table_name}"'.format(
                    database=self.database, table_name=self.table_name
                )
            ).rowcount
            > 0
        )

    @property
    def full_table_name(self):
        """
        :return: full table name in the schema
        """
        return r"`{0:s}`.`{1:s}`".format(self.database, self.table_name)

    @property
    def _log(self):
        if self._log_ is None:
            self._log_ = Log(
                self.connection,
                database=self.database,
                skip_logging=self.table_name.startswith("~"),
            )
        return self._log_

    @property
    def external(self):
        return self.connection.schemas[self.database].external

    def update1(self, row):
        """
        ``update1`` updates one existing entry in the table.
        Caution: In DataJoint the primary modes for data manipulation is to ``insert`` and
        ``delete`` entire records since referential integrity works on the level of records,
        not fields. Therefore, updates are reserved for corrective operations outside of main
        workflow. Use UPDATE methods sparingly with full awareness of potential violations of
        assumptions.

        :param row: a ``dict`` containing the primary key values and the attributes to update.
            Setting an attribute value to None will reset it to the default value (if any).

        The primary key attributes must always be provided.

        Examples:

        >>> table.update1({'id': 1, 'value': 3})  # update value in record with id=1
        >>> table.update1({'id': 1, 'value': None})  # reset value to default
        """
        # argument validations
        if not isinstance(row, collections.abc.Mapping):
            raise DataJointError("The argument of update1 must be dict-like.")
        if not set(row).issuperset(self.primary_key):
            raise DataJointError(
                "The argument of update1 must supply all primary key values."
            )
        try:
            raise DataJointError(
                "Attribute `%s` not found."
                % next(k for k in row if k not in self.heading.names)
            )
        except StopIteration:
            pass  # ok
        if len(self.restriction):
            raise DataJointError("Update cannot be applied to a restricted table.")
        key = {k: row[k] for k in self.primary_key}
        if len(self & key) != 1:
            raise DataJointError("Update can only be applied to one existing entry.")
        # UPDATE query
        row = [
            self.__make_placeholder(k, v)
            for k, v in row.items()
            if k not in self.primary_key
        ]
        query = "UPDATE {table} SET {assignments} WHERE {where}".format(
            table=self.full_table_name,
            assignments=",".join("`%s`=%s" % r[:2] for r in row),
            where=make_condition(self, key, set()),
        )
        self.connection.query(query, args=list(r[2] for r in row if r[2] is not None))

    def insert1(self, row, **kwargs):
        """
        Insert one data record into the table. For ``kwargs``, see ``insert()``.

        :param row: a numpy record, a dict-like object, or an ordered sequence to be inserted
            as one row.
        """
        self.insert((row,), **kwargs)

    def insert(
        self,
        rows,
        replace=False,
        skip_duplicates=False,
        ignore_extra_fields=False,
        allow_direct_insert=None,
    ):
        """
        Insert a collection of rows.

        :param rows: Either (a) an iterable where an element is a numpy record, a
            dict-like object, a pandas.DataFrame, a sequence, or a query expression with
            the same heading as self, or (b) a pathlib.Path object specifying a path
            relative to the current directory with a CSV file, the contents of which
            will be inserted.
        :param replace: If True, replaces the existing tuple.
        :param skip_duplicates: If True, silently skip duplicate inserts.
        :param ignore_extra_fields: If False, fields that are not in the heading raise error.
        :param allow_direct_insert: Only applies in auto-populated tables. If False (default),
            insert may only be called from inside the make callback.

        Example:

            >>> Table.insert([
            >>>     dict(subject_id=7, species="mouse", date_of_birth="2014-09-01"),
            >>>     dict(subject_id=8, species="mouse", date_of_birth="2014-09-02")])
        """
        if isinstance(rows, pandas.DataFrame):
            # drop 'extra' synthetic index for 1-field index case -
            # frames with more advanced indices should be prepared by user.
            rows = rows.reset_index(
                drop=len(rows.index.names) == 1 and not rows.index.names[0]
            ).to_records(index=False)

        if isinstance(rows, Path):
            with open(rows, newline="") as data_file:
                rows = list(csv.DictReader(data_file, delimiter=","))

        # prohibit direct inserts into auto-populated tables
        if not allow_direct_insert and not getattr(self, "_allow_insert", True):
            raise DataJointError(
                "Inserts into an auto-populated table can only be done inside "
                "its make method during a populate call."
                " To override, set keyword argument allow_direct_insert=True."
            )

        if inspect.isclass(rows) and issubclass(rows, QueryExpression):
            rows = rows()  # instantiate if a class
        if isinstance(rows, QueryExpression):
            # insert from select
            if not ignore_extra_fields:
                try:
                    raise DataJointError(
                        "Attribute %s not found. To ignore extra attributes in insert, "
                        "set ignore_extra_fields=True."
                        % next(
                            name for name in rows.heading if name not in self.heading
                        )
                    )
                except StopIteration:
                    pass
            fields = list(name for name in rows.heading if name in self.heading)
            query = "{command} INTO {table} ({fields}) {select}{duplicate}".format(
                command="REPLACE" if replace else "INSERT",
                fields="`" + "`,`".join(fields) + "`",
                table=self.full_table_name,
                select=rows.make_sql(fields),
                duplicate=(
                    " ON DUPLICATE KEY UPDATE `{pk}`={table}.`{pk}`".format(
                        table=self.full_table_name, pk=self.primary_key[0]
                    )
                    if skip_duplicates
                    else ""
                ),
            )
            self.connection.query(query)
            return

        field_list = []  # collects the field list from first row (passed by reference)
        rows = list(
            self.__make_row_to_insert(row, field_list, ignore_extra_fields)
            for row in rows
        )
        if rows:
            try:
                query = "{command} INTO {destination}(`{fields}`) VALUES {placeholders}{duplicate}".format(
                    command="REPLACE" if replace else "INSERT",
                    destination=self.from_clause(),
                    fields="`,`".join(field_list),
                    placeholders=",".join(
                        "(" + ",".join(row["placeholders"]) + ")" for row in rows
                    ),
                    duplicate=(
                        " ON DUPLICATE KEY UPDATE `{pk}`=`{pk}`".format(
                            pk=self.primary_key[0]
                        )
                        if skip_duplicates
                        else ""
                    ),
                )
                self.connection.query(
                    query,
                    args=list(
                        itertools.chain.from_iterable(
                            (v for v in r["values"] if v is not None) for r in rows
                        )
                    ),
                )
            except UnknownAttributeError as err:
                raise err.suggest(
                    "To ignore extra fields in insert, set ignore_extra_fields=True"
                )
            except DuplicateError as err:
                raise err.suggest(
                    "To ignore duplicate entries in insert, set skip_duplicates=True"
                )

    def delete_quick(self, get_count=False):
        """
        Deletes the table without cascading and without user prompt.
        If this table has populated dependent tables, this will fail.
        """
        query = "DELETE FROM " + self.full_table_name + self.where_clause()
        self.connection.query(query)
        count = (
            self.connection.query("SELECT ROW_COUNT()").fetchone()[0]
            if get_count
            else None
        )
        self._log(query[:255])
        return count

    def delete(
        self,
        transaction: bool = True,
        safemode: Union[bool, None] = None,
        force_parts: bool = False,
        force_masters: bool = False,
    ) -> int:
        """
        Deletes the contents of the table and its dependent tables, recursively.

        Args:
            transaction: If `True`, use of the entire delete becomes an atomic transaction.
                This is the default and recommended behavior. Set to `False` if this delete is
                nested within another transaction.
            safemode: If `True`, prohibit nested transactions and prompt to confirm. Default
                is `dj.config['safemode']`.
            force_parts: Delete from parts even when not deleting from their masters.
            force_masters: If `True`, include part/master pairs in the cascade.
                Default is `False`.

        Returns:
            Number of deleted rows (excluding those from dependent tables).

        Raises:
            DataJointError: Delete exceeds maximum number of delete attempts.
            DataJointError: When deleting within an existing transaction.
            DataJointError: Deleting a part table before its master.
        """
        deleted = set()
        visited_masters = set()

        def cascade(table):
            """service function to perform cascading deletes recursively."""
            max_attempts = 50
            for _ in range(max_attempts):
                try:
                    delete_count = table.delete_quick(get_count=True)
                except IntegrityError as error:
                    match = foreign_key_error_regexp.match(error.args[0]).groupdict()
                    if "`.`" not in match["child"]:  # if schema name missing, use table
                        match["child"] = "{}.{}".format(
                            table.full_table_name.split(".")[0], match["child"]
                        )
                    if (
                        match["pk_attrs"] is not None
                    ):  # fully matched, adjusting the keys
                        match["fk_attrs"] = [
                            k.strip("`") for k in match["fk_attrs"].split(",")
                        ]
                        match["pk_attrs"] = [
                            k.strip("`") for k in match["pk_attrs"].split(",")
                        ]
                    else:  # only partially matched, querying with constraint to determine keys
                        match["fk_attrs"], match["parent"], match["pk_attrs"] = list(
                            map(
                                list,
                                zip(
                                    *table.connection.query(
                                        constraint_info_query,
                                        args=(
                                            match["name"].strip("`"),
                                            *[
                                                _.strip("`")
                                                for _ in match["child"].split("`.`")
                                            ],
                                        ),
                                    ).fetchall()
                                ),
                            )
                        )
                        match["parent"] = match["parent"][0]

                    # Restrict child by table if
                    #   1. if table's restriction attributes are not in child's primary key
                    #   2. if child renames any attributes
                    # Otherwise restrict child by table's restriction.
                    child = FreeTable(table.connection, match["child"])
                    if (
                        set(table.restriction_attributes) <= set(child.primary_key)
                        and match["fk_attrs"] == match["pk_attrs"]
                    ):
                        child._restriction = table._restriction
                        child._restriction_attributes = table.restriction_attributes
                    elif match["fk_attrs"] != match["pk_attrs"]:
                        child &= table.proj(
                            **dict(zip(match["fk_attrs"], match["pk_attrs"]))
                        )
                    else:
                        child &= table.proj()

                    master_name = get_master(child.full_table_name)
                    if (
                        force_masters
                        and master_name
                        and master_name != table.full_table_name
                        and master_name not in visited_masters
                    ):
                        master = FreeTable(table.connection, master_name)
                        master._restriction_attributes = set()
                        master._restriction = [
                            make_condition(  # &= may cause in target tables in subquery
                                master,
                                (master.proj() & child.proj()).fetch(),
                                master._restriction_attributes,
                            )
                        ]
                        visited_masters.add(master_name)
                        cascade(master)
                    else:
                        cascade(child)
                else:
                    deleted.add(table.full_table_name)
                    logger.info(
                        "Deleting {count} rows from {table}".format(
                            count=delete_count, table=table.full_table_name
                        )
                    )
                    break
            else:
                raise DataJointError("Exceeded maximum number of delete attempts.")
            return delete_count

        safemode = config["safemode"] if safemode is None else safemode

        # Start transaction
        if transaction:
            if not self.connection.in_transaction:
                self.connection.start_transaction()
            else:
                if not safemode:
                    transaction = False
                else:
                    raise DataJointError(
                        "Delete cannot use a transaction within an ongoing transaction. "
                        "Set transaction=False or safemode=False)."
                    )

        # Cascading delete
        try:
            delete_count = cascade(self)
        except:
            if transaction:
                self.connection.cancel_transaction()
            raise

        if not force_parts:
            # Avoid deleting from child before master (See issue #151)
            for part in deleted:
                master = get_master(part)
                if master and master not in deleted:
                    if transaction:
                        self.connection.cancel_transaction()
                    raise DataJointError(
                        "Attempt to delete part table {part} before deleting from "
                        "its master {master} first.".format(part=part, master=master)
                    )

        # Confirm and commit
        if delete_count == 0:
            if safemode:
                logger.warn("Nothing to delete.")
            if transaction:
                self.connection.cancel_transaction()
        else:
            if not safemode or user_choice("Commit deletes?", default="no") == "yes":
                if transaction:
                    self.connection.commit_transaction()
                if safemode:
                    logger.info("Deletes committed.")
            else:
                if transaction:
                    self.connection.cancel_transaction()
                if safemode:
                    logger.warn("Deletes cancelled")
        return delete_count

    def drop_quick(self):
        """
        Drops the table without cascading to dependent tables and without user prompt.
        """
        if self.is_declared:
            query = "DROP TABLE %s" % self.full_table_name
            self.connection.query(query)
            logger.info("Dropped table %s" % self.full_table_name)
            self._log(query[:255])
        else:
            logger.info(
                "Nothing to drop: table %s is not declared" % self.full_table_name
            )

    def drop(self):
        """
        Drop the table and all tables that reference it, recursively.
        User is prompted for confirmation if config['safemode'] is set to True.
        """
        if self.restriction:
            raise DataJointError(
                "A table with an applied restriction cannot be dropped."
                " Call drop() on the unrestricted Table."
            )
        self.connection.dependencies.load()
        do_drop = True
        tables = [
            table
            for table in self.connection.dependencies.descendants(self.full_table_name)
            if not table.isdigit()
        ]

        # avoid dropping part tables without their masters: See issue #374
        for part in tables:
            master = get_master(part)
            if master and master not in tables:
                raise DataJointError(
                    "Attempt to drop part table {part} before dropping "
                    "its master. Drop {master} first.".format(part=part, master=master)
                )

        if config["safemode"]:
            for table in tables:
                logger.info(
                    table + " (%d tuples)" % len(FreeTable(self.connection, table))
                )
            do_drop = user_choice("Proceed?", default="no") == "yes"
        if do_drop:
            for table in reversed(tables):
                FreeTable(self.connection, table).drop_quick()
            logger.info("Tables dropped. Restart kernel.")

    @property
    def size_on_disk(self):
        """
        :return: size of data and indices in bytes on the storage device
        """
        ret = self.connection.query(
            'SHOW TABLE STATUS FROM `{database}` WHERE NAME="{table}"'.format(
                database=self.database, table=self.table_name
            ),
            as_dict=True,
        ).fetchone()
        return ret["Data_length"] + ret["Index_length"]

    def show_definition(self):
        raise AttributeError(
            "show_definition is deprecated. Use the describe method instead."
        )

    def describe(self, context=None, printout=False):
        """
        :return:  the definition string for the query using DataJoint DDL.
        """
        if context is None:
            frame = inspect.currentframe().f_back
            context = dict(frame.f_globals, **frame.f_locals)
            del frame
        if self.full_table_name not in self.connection.dependencies:
            self.connection.dependencies.load()
        parents = self.parents(foreign_key_info=True)
        in_key = True
        definition = (
            "# " + self.heading.table_status["comment"] + "\n"
            if self.heading.table_status["comment"]
            else ""
        )
        attributes_thus_far = set()
        attributes_declared = set()
        indexes = self.heading.indexes.copy()
        for attr in self.heading.attributes.values():
            if in_key and not attr.in_key:
                definition += "---\n"
                in_key = False
            attributes_thus_far.add(attr.name)
            do_include = True
            for parent_name, fk_props in parents:
                if attr.name in fk_props["attr_map"]:
                    do_include = False
                    if attributes_thus_far.issuperset(fk_props["attr_map"]):
                        # foreign key properties
                        try:
                            index_props = indexes.pop(tuple(fk_props["attr_map"]))
                        except KeyError:
                            index_props = ""
                        else:
                            index_props = [k for k, v in index_props.items() if v]
                            index_props = (
                                " [{}]".format(", ".join(index_props))
                                if index_props
                                else ""
                            )

                        if not fk_props["aliased"]:
                            # simple foreign key
                            definition += "->{props} {class_name}\n".format(
                                props=index_props,
                                class_name=lookup_class_name(parent_name, context)
                                or parent_name,
                            )
                        else:
                            # projected foreign key
                            definition += (
                                "->{props} {class_name}.proj({proj_list})\n".format(
                                    props=index_props,
                                    class_name=lookup_class_name(parent_name, context)
                                    or parent_name,
                                    proj_list=",".join(
                                        '{}="{}"'.format(attr, ref)
                                        for attr, ref in fk_props["attr_map"].items()
                                        if ref != attr
                                    ),
                                )
                            )
                            attributes_declared.update(fk_props["attr_map"])
            if do_include:
                attributes_declared.add(attr.name)
                definition += "%-20s : %-28s %s\n" % (
                    (
                        attr.name
                        if attr.default is None
                        else "%s=%s" % (attr.name, attr.default)
                    ),
                    "%s%s"
                    % (attr.type, " auto_increment" if attr.autoincrement else ""),
                    "# " + attr.comment if attr.comment else "",
                )
        # add remaining indexes
        for k, v in indexes.items():
            definition += "{unique}INDEX ({attrs})\n".format(
                unique="UNIQUE " if v["unique"] else "", attrs=", ".join(k)
            )
        if printout:
            logger.info("\n" + definition)
        return definition

    # --- private helper functions ----
    def __make_placeholder(self, name, value, ignore_extra_fields=False):
        """
        For a given attribute `name` with `value`, return its processed value or value placeholder
        as a string to be included in the query and the value, if any, to be submitted for
        processing by mysql API.

        :param name:  name of attribute to be inserted
        :param value: value of attribute to be inserted
        """
        if ignore_extra_fields and name not in self.heading:
            return None
        attr = self.heading[name]
        if attr.adapter:
            value = attr.adapter.put(value)
        if value is None or (attr.numeric and (value == "" or np.isnan(float(value)))):
            # set default value
            placeholder, value = "DEFAULT", None
        else:  # not NULL
            placeholder = "%s"
            if attr.uuid:
                if not isinstance(value, uuid.UUID):
                    try:
                        value = uuid.UUID(value)
                    except (AttributeError, ValueError):
                        raise DataJointError(
                            "badly formed UUID value {v} for attribute `{n}`".format(
                                v=value, n=name
                            )
                        )
                value = value.bytes
            elif attr.is_blob:
                value = blob.pack(value)
                value = (
                    self.external[attr.store].put(value).bytes
                    if attr.is_external
                    else value
                )
            elif attr.is_attachment:
                attachment_path = Path(value)
                if attr.is_external:
                    # value is hash of contents
                    value = (
                        self.external[attr.store]
                        .upload_attachment(attachment_path)
                        .bytes
                    )
                else:
                    # value is filename + contents
                    value = (
                        str.encode(attachment_path.name)
                        + b"\0"
                        + attachment_path.read_bytes()
                    )
            elif attr.is_filepath:
                value = self.external[attr.store].upload_filepath(value).bytes
            elif attr.numeric:
                value = str(int(value) if isinstance(value, bool) else value)
            elif attr.json:
                value = json.dumps(value)
        return name, placeholder, value

    def __make_row_to_insert(self, row, field_list, ignore_extra_fields):
        """
        Helper function for insert and update

        :param row:  A tuple to insert
        :return: a dict with fields 'names', 'placeholders', 'values'
        """

        def check_fields(fields):
            """
            Validates that all items in `fields` are valid attributes in the heading

            :param fields: field names of a tuple
            """
            if not field_list:
                if not ignore_extra_fields:
                    for field in fields:
                        if field not in self.heading:
                            raise KeyError(
                                "`{0:s}` is not in the table heading".format(field)
                            )
            elif set(field_list) != set(fields).intersection(self.heading.names):
                raise DataJointError("Attempt to insert rows with different fields.")

        if isinstance(row, np.void):  # np.array
            check_fields(row.dtype.fields)
            attributes = [
                self.__make_placeholder(name, row[name], ignore_extra_fields)
                for name in self.heading
                if name in row.dtype.fields
            ]
        elif isinstance(row, collections.abc.Mapping):  # dict-based
            check_fields(row)
            attributes = [
                self.__make_placeholder(name, row[name], ignore_extra_fields)
                for name in self.heading
                if name in row
            ]
        else:  # positional
            try:
                if len(row) != len(self.heading):
                    raise DataJointError(
                        "Invalid insert argument. Incorrect number of attributes: "
                        "{given} given; {expected} expected".format(
                            given=len(row), expected=len(self.heading)
                        )
                    )
            except TypeError:
                raise DataJointError("Datatype %s cannot be inserted" % type(row))
            else:
                attributes = [
                    self.__make_placeholder(name, value, ignore_extra_fields)
                    for name, value in zip(self.heading, row)
                ]
        if ignore_extra_fields:
            attributes = [a for a in attributes if a is not None]

        assert len(attributes), "Empty tuple"
        row_to_insert = dict(zip(("names", "placeholders", "values"), zip(*attributes)))
        if not field_list:
            # first row sets the composition of the field list
            field_list.extend(row_to_insert["names"])
        else:
            #  reorder attributes in row_to_insert to match field_list
            order = list(row_to_insert["names"].index(field) for field in field_list)
            row_to_insert["names"] = list(row_to_insert["names"][i] for i in order)
            row_to_insert["placeholders"] = list(
                row_to_insert["placeholders"][i] for i in order
            )
            row_to_insert["values"] = list(row_to_insert["values"][i] for i in order)
        return row_to_insert


def lookup_class_name(name, context, depth=3):
    """
    given a table name in the form `schema_name`.`table_name`, find its class in the context.

    :param name: `schema_name`.`table_name`
    :param context: dictionary representing the namespace
    :param depth: search depth into imported modules, helps avoid infinite recursion.
    :return: class name found in the context or None if not found
    """
    # breadth-first search
    nodes = [dict(context=context, context_name="", depth=depth)]
    while nodes:
        node = nodes.pop(0)
        for member_name, member in node["context"].items():
            if not member_name.startswith("_"):  # skip IPython's implicit variables
                if inspect.isclass(member) and issubclass(member, Table):
                    if member.full_table_name == name:  # found it!
                        return ".".join([node["context_name"], member_name]).lstrip(".")
                    try:  # look for part tables
                        parts = member.__dict__
                    except AttributeError:
                        pass  # not a UserTable -- cannot have part tables.
                    else:
                        for part in (
                            getattr(member, p)
                            for p in parts
                            if p[0].isupper() and hasattr(member, p)
                        ):
                            if (
                                inspect.isclass(part)
                                and issubclass(part, Table)
                                and part.full_table_name == name
                            ):
                                return ".".join(
                                    [node["context_name"], member_name, part.__name__]
                                ).lstrip(".")
                elif (
                    node["depth"] > 0
                    and inspect.ismodule(member)
                    and member.__name__ != "datajoint"
                ):
                    try:
                        nodes.append(
                            dict(
                                context=dict(inspect.getmembers(member)),
                                context_name=node["context_name"] + "." + member_name,
                                depth=node["depth"] - 1,
                            )
                        )
                    except ImportError:
                        pass  # could not import, so do not attempt
    return None


class FreeTable(Table):
    """
    A base table without a dedicated class. Each instance is associated with a table
    specified by full_table_name.

    :param conn:  a dj.Connection object
    :param full_table_name: in format `database`.`table_name`
    """

    def __init__(self, conn, full_table_name):
        self.database, self._table_name = (
            s.strip("`") for s in full_table_name.split(".")
        )
        self._connection = conn
        self._support = [full_table_name]
        self._heading = Heading(
            table_info=dict(
                conn=conn,
                database=self.database,
                table_name=self.table_name,
                context=None,
            )
        )

    def __repr__(self):
        return (
            "FreeTable(`%s`.`%s`)\n" % (self.database, self._table_name)
            + super().__repr__()
        )


class Log(Table):
    """
    The log table for each schema.
    Instances are callable.  Calls log the time and identifying information along with the event.

    :param skip_logging: if True, then log entry is skipped by default. See __call__
    """

    _table_name = "~log"

    def __init__(self, conn, database, skip_logging=False):
        self.database = database
        self.skip_logging = skip_logging
        self._connection = conn
        self._heading = Heading(
            table_info=dict(
                conn=conn, database=database, table_name=self.table_name, context=None
            )
        )
        self._support = [self.full_table_name]

        self._definition = """    # event logging table for `{database}`
        id       :int unsigned auto_increment     # event order id
        ---
        timestamp = CURRENT_TIMESTAMP : timestamp # event timestamp
        version  :varchar(12)                     # datajoint version
        user     :varchar(255)                    # user@host
        host=""  :varchar(255)                    # system hostname
        event="" :varchar(255)                    # event message
        """.format(
            database=database
        )

        super().__init__()

        if not self.is_declared:
            self.declare()
            self.connection.dependencies.clear()
        self._user = self.connection.get_user()

    @property
    def definition(self):
        return self._definition

    def __call__(self, event, skip_logging=None):
        """

        :param event: string to write into the log table
        :param skip_logging: If True then do not log. If None, then use self.skip_logging
        """
        skip_logging = self.skip_logging if skip_logging is None else skip_logging
        if not skip_logging:
            try:
                self.insert1(
                    dict(
                        user=self._user,
                        version=version + "py",
                        host=platform.uname().node,
                        event=event,
                    ),
                    skip_duplicates=True,
                    ignore_extra_fields=True,
                )
            except DataJointError:
                logger.info("could not log event in table ~log")

    def delete(self):
        """
        bypass interactive prompts and cascading dependencies

        :return: number of deleted items
        """
        return self.delete_quick(get_count=True)

    def drop(self):
        """bypass interactive prompts and cascading dependencies"""
        self.drop_quick()
