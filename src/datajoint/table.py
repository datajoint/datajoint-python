import collections
import csv
import inspect
import itertools
import json
import logging
import re
import uuid
import warnings
from dataclasses import dataclass, field
from pathlib import Path

import numpy as np
import pandas

from .condition import make_condition
from .declare import alter, declare
from .errors import (
    AccessError,
    DataJointError,
    DuplicateError,
    IntegrityError,
    UnknownAttributeError,
)
from .expression import QueryExpression
from .heading import Heading
from .settings import config
from .staged_insert import staged_insert1 as _staged_insert1
from .utils import get_master, is_camel_case, user_choice

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


@dataclass
class ValidationResult:
    """
    Result of table.validate() call.

    Attributes:
        is_valid: True if all rows passed validation
        errors: List of (row_index, field_name, error_message) tuples
        rows_checked: Number of rows that were validated
    """

    is_valid: bool
    errors: list = field(default_factory=list)  # list of (row_index, field_name | None, message)
    rows_checked: int = 0

    def __bool__(self) -> bool:
        """Allow using ValidationResult in boolean context."""
        return self.is_valid

    def raise_if_invalid(self):
        """Raise DataJointError if validation failed."""
        if not self.is_valid:
            raise DataJointError(self.summary())

    def summary(self) -> str:
        """Return formatted error summary."""
        if self.is_valid:
            return f"Validation passed: {self.rows_checked} rows checked"
        lines = [f"Validation failed: {len(self.errors)} error(s) in {self.rows_checked} rows"]
        for row_idx, field_name, message in self.errors[:10]:  # Show first 10 errors
            field_str = f" in field '{field_name}'" if field_name else ""
            lines.append(f"  Row {row_idx}{field_str}: {message}")
        if len(self.errors) > 10:
            lines.append(f"  ... and {len(self.errors) - 10} more errors")
        return "\n".join(lines)


class Table(QueryExpression):
    """
    Table is an abstract class that represents a table in the schema.
    It implements insert and delete methods and inherits query functionality.
    To make it a concrete class, override the abstract properties specifying the connection,
    table name, database, and definition.
    """

    _table_name = None  # must be defined in subclass

    # These properties must be set by the schema decorator (schemas.py) at class level
    # or by FreeTable at instance level
    database = None
    declaration_context = None

    @property
    def table_name(self):
        # For UserTable subclasses, table_name is computed by the metaclass.
        # Delegate to the class's table_name if _table_name is not set.
        if self._table_name is None:
            return type(self).table_name
        return self._table_name

    @property
    def class_name(self):
        return self.__class__.__name__

    # Base tier class names that should not raise errors when heading is None
    _base_tier_classes = frozenset({"Table", "UserTable", "Lookup", "Manual", "Imported", "Computed", "Part"})

    @property
    def heading(self):
        """
        Return the table's heading, or raise a helpful error if not configured.

        Overrides QueryExpression.heading to provide a clear error message
        when the table is not properly associated with an activated schema.
        For base tier classes (Lookup, Manual, etc.), returns None to support
        introspection (e.g., help()).
        """
        if self._heading is None:
            # Don't raise error for base tier classes - they're used for introspection
            if self.__class__.__name__ in self._base_tier_classes:
                return None
            raise DataJointError(
                f"Table `{self.__class__.__name__}` is not properly configured. "
                "Ensure the schema is activated before using the table. "
                "Example: schema.activate('database_name') or schema = dj.Schema('database_name')"
            )
        return self._heading

    @property
    def definition(self):
        raise NotImplementedError("Subclasses of Table must implement the `definition` property")

    def declare(self, context=None):
        """
        Declare the table in the schema based on self.definition.

        :param context: the context for foreign key resolution. If None, foreign keys are
            not allowed.
        """
        if self.connection.in_transaction:
            raise DataJointError("Cannot declare new tables inside a transaction, e.g. from inside a populate/make call")
        # Enforce strict CamelCase #1150
        if not is_camel_case(self.class_name):
            raise DataJointError(
                "Table class name `{name}` is invalid. Please use CamelCase. ".format(name=self.class_name)
                + "Classes defining tables should be formatted in strict CamelCase."
            )
        sql, _external_stores, primary_key, fk_attribute_map = declare(self.full_table_name, self.definition, context)

        # Call declaration hook for validation (subclasses like AutoPopulate can override)
        self._declare_check(primary_key, fk_attribute_map)

        sql = sql.format(database=self.database)
        try:
            self.connection.query(sql)
        except AccessError:
            # Only suppress if table already exists (idempotent declaration)
            # Otherwise raise - user needs to know about permission issues
            if self.is_declared:
                return
            raise AccessError(
                f"Cannot declare table {self.full_table_name}. "
                f"Check that you have CREATE privilege on schema `{self.database}` "
                f"and REFERENCES privilege on any referenced parent tables."
            ) from None

        # Populate lineage table for this table's attributes
        self._populate_lineage(primary_key, fk_attribute_map)

    def _declare_check(self, primary_key, fk_attribute_map):
        """
        Hook for declaration-time validation. Subclasses can override.

        Called before the table is created in the database. Override this method
        to add validation logic (e.g., AutoPopulate validates FK-only primary keys).

        :param primary_key: list of primary key attribute names
        :param fk_attribute_map: dict mapping child_attr -> (parent_table, parent_attr)
        """
        pass  # Default: no validation

    def _populate_lineage(self, primary_key, fk_attribute_map):
        """
        Populate the ~lineage table with lineage information for this table's attributes.

        Lineage is stored for:
        - All FK attributes (traced to their origin)
        - Native primary key attributes (lineage = self)

        :param primary_key: list of primary key attribute names
        :param fk_attribute_map: dict mapping child_attr -> (parent_table, parent_attr)
        """
        from .lineage import (
            ensure_lineage_table,
            get_lineage,
            delete_table_lineages,
            insert_lineages,
        )

        # Ensure the ~lineage table exists
        ensure_lineage_table(self.connection, self.database)

        # Delete any existing lineage entries for this table (for idempotent re-declaration)
        delete_table_lineages(self.connection, self.database, self.table_name)

        entries = []

        # FK attributes: copy lineage from parent (whether in PK or not)
        for attr, (parent_table, parent_attr) in fk_attribute_map.items():
            # Parse parent table name: `schema`.`table` -> (schema, table)
            parent_clean = parent_table.replace("`", "")
            if "." in parent_clean:
                parent_db, parent_tbl = parent_clean.split(".", 1)
            else:
                parent_db = self.database
                parent_tbl = parent_clean

            # Get parent's lineage for this attribute
            parent_lineage = get_lineage(self.connection, parent_db, parent_tbl, parent_attr)
            if parent_lineage:
                # Copy parent's lineage
                entries.append((self.table_name, attr, parent_lineage))
            else:
                # Parent doesn't have lineage entry - use parent as origin
                # This can happen for legacy/external schemas without lineage tracking
                lineage = f"{parent_db}.{parent_tbl}.{parent_attr}"
                entries.append((self.table_name, attr, lineage))
                logger.warning(
                    f"Lineage for `{parent_db}`.`{parent_tbl}`.`{parent_attr}` not found "
                    f"(parent schema's ~lineage table may be missing or incomplete). "
                    f"Using it as origin. Once the parent schema's lineage is rebuilt, "
                    f"run schema.rebuild_lineage() on this schema to correct the lineage."
                )

        # Native PK attributes (in PK but not FK): this table is the origin
        for attr in primary_key:
            if attr not in fk_attribute_map:
                lineage = f"{self.database}.{self.table_name}.{attr}"
                entries.append((self.table_name, attr, lineage))

        if entries:
            insert_lineages(self.connection, self.database, entries)

    def alter(self, prompt=True, context=None):
        """
        Alter the table definition from self.definition
        """
        if self.connection.in_transaction:
            raise DataJointError("Cannot update table declaration inside a transaction, e.g. from inside a populate/make call")
        if context is None:
            frame = inspect.currentframe().f_back
            context = dict(frame.f_globals, **frame.f_locals)
            del frame
        old_definition = self.describe(context=context)
        sql, _external_stores = alter(self.definition, old_definition, context)
        if not sql:
            if prompt:
                logger.warning("Nothing to alter.")
        else:
            sql = "ALTER TABLE {tab}\n\t".format(tab=self.full_table_name) + ",\n\t".join(sql)
            if not prompt or user_choice(sql + "\n\nExecute?") == "yes":
                try:
                    self.connection.query(sql)
                except AccessError:
                    # skip if no create privilege
                    pass
                else:
                    # reset heading
                    self.__class__._heading = Heading(table_info=self.heading.table_info)
                    if prompt:
                        logger.info("Table altered")

    def from_clause(self):
        """
        :return: the FROM clause of SQL SELECT statements.
        """
        return self.full_table_name

    def get_select_fields(self, select_fields=None):
        """
        :return: the selected attributes from the SQL SELECT statement.
        """
        return "*" if select_fields is None else self.heading.project(select_fields).as_sql

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
        self.connection.dependencies.load(force=False)
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
                'SHOW TABLES in `{database}` LIKE "{table_name}"'.format(database=self.database, table_name=self.table_name)
            ).rowcount
            > 0
        )

    @property
    def full_table_name(self):
        """
        :return: full table name in the schema
        """
        if self.database is None or self.table_name is None:
            raise DataJointError(
                f"Class {self.__class__.__name__} is not associated with a schema. "
                "Apply a schema decorator or use schema() to bind it."
            )
        return f"{self.adapter.quote_identifier(self.database)}.{self.adapter.quote_identifier(self.table_name)}"

    @property
    def adapter(self):
        """Database adapter for backend-agnostic SQL generation."""
        return self.connection.adapter

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
            raise DataJointError("The argument of update1 must supply all primary key values.")
        try:
            raise DataJointError("Attribute `%s` not found." % next(k for k in row if k not in self.heading.names))
        except StopIteration:
            pass  # ok
        if len(self.restriction):
            raise DataJointError("Update cannot be applied to a restricted table.")
        key = {k: row[k] for k in self.primary_key}
        if len(self & key) != 1:
            raise DataJointError("Update can only be applied to one existing entry.")
        # UPDATE query
        row = [self.__make_placeholder(k, v) for k, v in row.items() if k not in self.primary_key]
        assignments = ",".join(f"{self.adapter.quote_identifier(r[0])}={r[1]}" for r in row)
        query = "UPDATE {table} SET {assignments} WHERE {where}".format(
            table=self.full_table_name,
            assignments=assignments,
            where=make_condition(self, key, set()),
        )
        self.connection.query(query, args=list(r[2] for r in row if r[2] is not None))

    def validate(self, rows, *, ignore_extra_fields=False) -> ValidationResult:
        """
        Validate rows without inserting them.

        :param rows: Same format as insert() - iterable of dicts, tuples, numpy records,
            or a pandas DataFrame.
        :param ignore_extra_fields: If True, ignore fields not in the table heading.
        :return: ValidationResult with is_valid, errors list, and rows_checked count.

        Validates:
            - Field existence (all fields must be in table heading)
            - Row format (correct number of attributes for positional inserts)
            - Codec validation (type checking via codec.validate())
            - NULL constraints (non-nullable fields must have values)
            - Primary key completeness (all PK fields must be present)
            - UUID format and JSON serializability

        Cannot validate (database-enforced):
            - Foreign key constraints
            - Unique constraints (other than PK)
            - Custom MySQL constraints

        Example::

            result = table.validate(rows)
            if result:
                table.insert(rows)
            else:
                print(result.summary())
        """
        errors = []

        # Convert DataFrame to records
        if isinstance(rows, pandas.DataFrame):
            rows = rows.reset_index(drop=len(rows.index.names) == 1 and not rows.index.names[0]).to_records(index=False)

        # Convert Path (CSV) to list of dicts
        if isinstance(rows, Path):
            with open(rows, newline="") as data_file:
                rows = list(csv.DictReader(data_file, delimiter=","))

        rows = list(rows)  # Materialize iterator
        row_count = len(rows)

        for row_idx, row in enumerate(rows):
            # Validate row format and fields
            row_dict = None
            try:
                if isinstance(row, np.void):  # numpy record
                    fields = list(row.dtype.fields.keys())
                    row_dict = {name: row[name] for name in fields}
                elif isinstance(row, collections.abc.Mapping):
                    fields = list(row.keys())
                    row_dict = dict(row)
                else:  # positional tuple/list
                    if len(row) != len(self.heading):
                        errors.append(
                            (
                                row_idx,
                                None,
                                f"Incorrect number of attributes: {len(row)} given, {len(self.heading)} expected",
                            )
                        )
                        continue
                    fields = list(self.heading.names)
                    row_dict = dict(zip(fields, row))
            except TypeError:
                errors.append((row_idx, None, f"Invalid row type: {type(row).__name__}"))
                continue

            # Check for unknown fields
            if not ignore_extra_fields:
                for field_name in fields:
                    if field_name not in self.heading:
                        errors.append((row_idx, field_name, f"Field '{field_name}' not in table heading"))

            # Validate each field value
            for name in self.heading.names:
                if name not in row_dict:
                    # Check if field is required (non-nullable, no default, not autoincrement)
                    attr = self.heading[name]
                    if not attr.nullable and attr.default is None and not attr.autoincrement:
                        errors.append((row_idx, name, f"Required field '{name}' is missing"))
                    continue

                value = row_dict[name]
                attr = self.heading[name]

                # Skip validation for None values on nullable columns
                if value is None:
                    if not attr.nullable and attr.default is None:
                        errors.append((row_idx, name, f"NULL value not allowed for non-nullable field '{name}'"))
                    continue

                # Codec validation
                if attr.codec:
                    try:
                        attr.codec.validate(value)
                    except (TypeError, ValueError) as e:
                        errors.append((row_idx, name, f"Codec validation failed: {e}"))
                        continue

                # UUID validation
                if attr.uuid and not isinstance(value, uuid.UUID):
                    try:
                        uuid.UUID(value)
                    except (AttributeError, ValueError):
                        errors.append((row_idx, name, f"Invalid UUID format: {value}"))
                        continue

                # JSON serialization check
                if attr.json:
                    try:
                        json.dumps(value)
                    except (TypeError, ValueError) as e:
                        errors.append((row_idx, name, f"Value not JSON serializable: {e}"))
                        continue

                # Numeric NaN check
                if attr.numeric and value != "" and not isinstance(value, (bool, np.bool_)):
                    try:
                        if np.isnan(float(value)):
                            # NaN is allowed - will be converted to NULL
                            pass
                    except (TypeError, ValueError):
                        # Not a number that can be checked for NaN - let it pass
                        pass

            # Check primary key completeness
            for pk_field in self.primary_key:
                if pk_field not in row_dict or row_dict[pk_field] is None:
                    pk_attr = self.heading[pk_field]
                    if not pk_attr.autoincrement:
                        errors.append((row_idx, pk_field, f"Primary key field '{pk_field}' is missing or NULL"))

        return ValidationResult(is_valid=len(errors) == 0, errors=errors, rows_checked=row_count)

    def insert1(self, row, **kwargs):
        """
        Insert one data record into the table. For ``kwargs``, see ``insert()``.

        :param row: a numpy record, a dict-like object, or an ordered sequence to be inserted
            as one row.
        """
        self.insert((row,), **kwargs)

    @property
    def staged_insert1(self):
        """
        Context manager for staged insert with direct object storage writes.

        Use this for large objects like Zarr arrays where copying from local storage
        is inefficient. Allows writing directly to the destination storage before
        finalizing the database insert.

        Example:
            with table.staged_insert1 as staged:
                staged.rec['subject_id'] = 123
                staged.rec['session_id'] = 45

                # Create object storage directly
                z = zarr.open(staged.store('raw_data', '.zarr'), mode='w', shape=(1000, 1000))
                z[:] = data

                # Assign to record
                staged.rec['raw_data'] = z

            # On successful exit: metadata computed, record inserted
            # On exception: storage cleaned up, no record inserted

        Yields:
            StagedInsert: Context for setting record values and getting storage handles
        """
        return _staged_insert1(self)

    def insert(
        self,
        rows,
        replace=False,
        skip_duplicates=False,
        ignore_extra_fields=False,
        allow_direct_insert=None,
        chunk_size=None,
    ):
        """
        Insert a collection of rows.

        :param rows: Either (a) an iterable where an element is a numpy record, a
            dict-like object, a pandas.DataFrame, a polars.DataFrame, a pyarrow.Table,
            a sequence, or a query expression with the same heading as self, or
            (b) a pathlib.Path object specifying a path relative to the current
            directory with a CSV file, the contents of which will be inserted.
        :param replace: If True, replaces the existing tuple.
        :param skip_duplicates: If True, silently skip duplicate inserts.
        :param ignore_extra_fields: If False, fields that are not in the heading raise error.
        :param allow_direct_insert: Only applies in auto-populated tables. If False (default),
            insert may only be called from inside the make callback.
        :param chunk_size: If set, insert rows in batches of this size. Useful for very
            large inserts to avoid memory issues. Each chunk is a separate transaction.

        Example:

            >>> Table.insert([
            >>>     dict(subject_id=7, species="mouse", date_of_birth="2014-09-01"),
            >>>     dict(subject_id=8, species="mouse", date_of_birth="2014-09-02")])

            # Large insert with chunking
            >>> Table.insert(large_dataset, chunk_size=10000)
        """
        if isinstance(rows, pandas.DataFrame):
            # drop 'extra' synthetic index for 1-field index case -
            # frames with more advanced indices should be prepared by user.
            rows = rows.reset_index(drop=len(rows.index.names) == 1 and not rows.index.names[0]).to_records(index=False)

        # Polars DataFrame -> list of dicts (soft dependency, check by type name)
        if type(rows).__module__.startswith("polars") and type(rows).__name__ == "DataFrame":
            rows = rows.to_dicts()

        # PyArrow Table -> list of dicts (soft dependency, check by type name)
        if type(rows).__module__.startswith("pyarrow") and type(rows).__name__ == "Table":
            rows = rows.to_pylist()

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
            # insert from select - chunk_size not applicable
            if chunk_size is not None:
                raise DataJointError("chunk_size is not supported for QueryExpression inserts")
            if not ignore_extra_fields:
                try:
                    raise DataJointError(
                        "Attribute %s not found. To ignore extra attributes in insert, "
                        "set ignore_extra_fields=True." % next(name for name in rows.heading if name not in self.heading)
                    )
                except StopIteration:
                    pass
            fields = list(name for name in rows.heading if name in self.heading)
            quoted_fields = ",".join(self.adapter.quote_identifier(f) for f in fields)

            # Duplicate handling (MySQL-specific for Phase 5)
            if skip_duplicates:
                quoted_pk = self.adapter.quote_identifier(self.primary_key[0])
                duplicate = f" ON DUPLICATE KEY UPDATE {quoted_pk}={self.full_table_name}.{quoted_pk}"
            else:
                duplicate = ""

            command = "REPLACE" if replace else "INSERT"
            query = f"{command} INTO {self.full_table_name} ({quoted_fields}) {rows.make_sql(fields)}{duplicate}"
            self.connection.query(query)
            return

        # Chunked insert mode
        if chunk_size is not None:
            rows_iter = iter(rows)
            while True:
                chunk = list(itertools.islice(rows_iter, chunk_size))
                if not chunk:
                    break
                self._insert_rows(chunk, replace, skip_duplicates, ignore_extra_fields)
            return

        # Single batch insert (original behavior)
        self._insert_rows(rows, replace, skip_duplicates, ignore_extra_fields)

    def _insert_rows(self, rows, replace, skip_duplicates, ignore_extra_fields):
        """
        Internal helper to insert a batch of rows.

        :param rows: Iterable of rows to insert
        :param replace: If True, use REPLACE instead of INSERT
        :param skip_duplicates: If True, use ON DUPLICATE KEY UPDATE
        :param ignore_extra_fields: If True, ignore unknown fields
        """
        # collects the field list from first row (passed by reference)
        field_list = []
        rows = list(self.__make_row_to_insert(row, field_list, ignore_extra_fields) for row in rows)
        if rows:
            try:
                # Handle empty field_list (all-defaults insert)
                if field_list:
                    fields_clause = f"({','.join(self.adapter.quote_identifier(f) for f in field_list)})"
                else:
                    fields_clause = "()"

                # Build duplicate clause (MySQL-specific for Phase 5)
                if skip_duplicates:
                    quoted_pk = self.adapter.quote_identifier(self.primary_key[0])
                    duplicate = f" ON DUPLICATE KEY UPDATE {quoted_pk}=VALUES({quoted_pk})"
                else:
                    duplicate = ""

                command = "REPLACE" if replace else "INSERT"
                placeholders = ",".join("(" + ",".join(row["placeholders"]) + ")" for row in rows)
                query = f"{command} INTO {self.from_clause()}{fields_clause} VALUES {placeholders}{duplicate}"
                self.connection.query(
                    query,
                    args=list(itertools.chain.from_iterable((v for v in r["values"] if v is not None) for r in rows)),
                )
            except UnknownAttributeError as err:
                raise err.suggest("To ignore extra fields in insert, set ignore_extra_fields=True")
            except DuplicateError as err:
                raise err.suggest("To ignore duplicate entries in insert, set skip_duplicates=True")

    def insert_dataframe(self, df, index_as_pk=None, **insert_kwargs):
        """
        Insert DataFrame with explicit index handling.

        This method provides symmetry with to_pandas(): data fetched with to_pandas()
        (which sets primary key as index) can be modified and re-inserted using
        insert_dataframe() without manual index manipulation.

        :param df: pandas DataFrame to insert
        :param index_as_pk: How to handle DataFrame index:
            - None (default): Auto-detect. Use index as primary key if index names
              match primary_key columns. Drop if unnamed RangeIndex.
            - True: Treat index as primary key columns. Raises if index names don't
              match table primary key.
            - False: Ignore index entirely (drop it).
        :param **insert_kwargs: Passed to insert() - replace, skip_duplicates,
            ignore_extra_fields, allow_direct_insert, chunk_size

        Example::

            # Round-trip with to_pandas()
            df = table.to_pandas()           # PK becomes index
            df['value'] = df['value'] * 2    # Modify data
            table.insert_dataframe(df)       # Auto-detects index as PK

            # Explicit control
            table.insert_dataframe(df, index_as_pk=True)   # Use index
            table.insert_dataframe(df, index_as_pk=False)  # Ignore index
        """
        if not isinstance(df, pandas.DataFrame):
            raise DataJointError("insert_dataframe requires a pandas DataFrame")

        # Auto-detect if index should be used as PK
        if index_as_pk is None:
            index_as_pk = self._should_index_be_pk(df)

        # Validate index if using as PK
        if index_as_pk:
            self._validate_index_columns(df)

        # Prepare rows
        if index_as_pk:
            rows = df.reset_index(drop=False).to_records(index=False)
        else:
            rows = df.reset_index(drop=True).to_records(index=False)

        self.insert(rows, **insert_kwargs)

    def _should_index_be_pk(self, df) -> bool:
        """
        Auto-detect if DataFrame index should map to primary key.

        Returns True if:
        - Index has named columns that exactly match the table's primary key
        Returns False if:
        - Index is unnamed RangeIndex (synthetic index)
        - Index names don't match primary key
        """
        # RangeIndex with no name -> False (synthetic index)
        if df.index.names == [None]:
            return False
        # Check if index names match PK columns
        index_names = set(n for n in df.index.names if n is not None)
        return index_names == set(self.primary_key)

    def _validate_index_columns(self, df):
        """Validate that index columns match the table's primary key."""
        index_names = [n for n in df.index.names if n is not None]
        if set(index_names) != set(self.primary_key):
            raise DataJointError(
                f"DataFrame index columns {index_names} do not match "
                f"table primary key {list(self.primary_key)}. "
                f"Use index_as_pk=False to ignore index, or reset_index() first."
            )

    def delete_quick(self, get_count=False):
        """
        Deletes the table without cascading and without user prompt.
        If this table has populated dependent tables, this will fail.
        """
        query = "DELETE FROM " + self.full_table_name + self.where_clause()
        self.connection.query(query)
        count = self.connection.query("SELECT ROW_COUNT()").fetchone()[0] if get_count else None
        return count

    def delete(
        self,
        transaction: bool = True,
        prompt: bool | None = None,
        part_integrity: str = "enforce",
    ) -> int:
        """
        Deletes the contents of the table and its dependent tables, recursively.

        Args:
            transaction: If `True`, use of the entire delete becomes an atomic transaction.
                This is the default and recommended behavior. Set to `False` if this delete is
                nested within another transaction.
            prompt: If `True`, show what will be deleted and ask for confirmation.
                If `False`, delete without confirmation. Default is `dj.config['safemode']`.
            part_integrity: Policy for master-part integrity. One of:
                - ``"enforce"`` (default): Error if parts would be deleted without masters.
                - ``"ignore"``: Allow deleting parts without masters (breaks integrity).
                - ``"cascade"``: Also delete masters when parts are deleted (maintains integrity).

        Returns:
            Number of deleted rows (excluding those from dependent tables).

        Raises:
            DataJointError: Delete exceeds maximum number of delete attempts.
            DataJointError: When deleting within an existing transaction.
            DataJointError: Deleting a part table before its master (when part_integrity="enforce").
            ValueError: Invalid part_integrity value.
        """
        if part_integrity not in ("enforce", "ignore", "cascade"):
            raise ValueError(f"part_integrity must be 'enforce', 'ignore', or 'cascade', got {part_integrity!r}")
        deleted = set()
        visited_masters = set()

        def cascade(table):
            """service function to perform cascading deletes recursively."""
            max_attempts = 50
            for _ in range(max_attempts):
                try:
                    delete_count = table.delete_quick(get_count=True)
                except IntegrityError as error:
                    match = foreign_key_error_regexp.match(error.args[0])
                    if match is None:
                        raise DataJointError(
                            "Cascading deletes failed because the error message is missing foreign key information."
                            "Make sure you have REFERENCES privilege to all dependent tables."
                        ) from None
                    match = match.groupdict()
                    # if schema name missing, use table
                    if "`.`" not in match["child"]:
                        match["child"] = "{}.{}".format(table.full_table_name.split(".")[0], match["child"])
                    if match["pk_attrs"] is not None:  # fully matched, adjusting the keys
                        match["fk_attrs"] = [k.strip("`") for k in match["fk_attrs"].split(",")]
                        match["pk_attrs"] = [k.strip("`") for k in match["pk_attrs"].split(",")]
                    else:  # only partially matched, querying with constraint to determine keys
                        match["fk_attrs"], match["parent"], match["pk_attrs"] = list(
                            map(
                                list,
                                zip(
                                    *table.connection.query(
                                        constraint_info_query,
                                        args=(
                                            match["name"].strip("`"),
                                            *[_.strip("`") for _ in match["child"].split("`.`")],
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
                    if set(table.restriction_attributes) <= set(child.primary_key) and match["fk_attrs"] == match["pk_attrs"]:
                        child._restriction = table._restriction
                        child._restriction_attributes = table.restriction_attributes
                    elif match["fk_attrs"] != match["pk_attrs"]:
                        child &= table.proj(**dict(zip(match["fk_attrs"], match["pk_attrs"])))
                    else:
                        child &= table.proj()

                    master_name = get_master(child.full_table_name)
                    if (
                        part_integrity == "cascade"
                        and master_name
                        and master_name != table.full_table_name
                        and master_name not in visited_masters
                    ):
                        master = FreeTable(table.connection, master_name)
                        master._restriction_attributes = set()
                        master._restriction = [
                            make_condition(  # &= may cause in target tables in subquery
                                master,
                                (master.proj() & child.proj()).to_arrays(),
                                master._restriction_attributes,
                            )
                        ]
                        visited_masters.add(master_name)
                        cascade(master)
                    else:
                        cascade(child)
                else:
                    deleted.add(table.full_table_name)
                    logger.info("Deleting {count} rows from {table}".format(count=delete_count, table=table.full_table_name))
                    break
            else:
                raise DataJointError("Exceeded maximum number of delete attempts.")
            return delete_count

        prompt = config["safemode"] if prompt is None else prompt

        # Start transaction
        if transaction:
            if not self.connection.in_transaction:
                self.connection.start_transaction()
            else:
                if not prompt:
                    transaction = False
                else:
                    raise DataJointError(
                        "Delete cannot use a transaction within an ongoing transaction. Set transaction=False or prompt=False."
                    )

        # Cascading delete
        try:
            delete_count = cascade(self)
        except:
            if transaction:
                self.connection.cancel_transaction()
            raise

        if part_integrity == "enforce":
            # Avoid deleting from part before master (See issue #151)
            for part in deleted:
                master = get_master(part)
                if master and master not in deleted:
                    if transaction:
                        self.connection.cancel_transaction()
                    raise DataJointError(
                        "Attempt to delete part table {part} before deleting from its master {master} first. "
                        "Use part_integrity='ignore' to allow, or part_integrity='cascade' to also delete master.".format(
                            part=part, master=master
                        )
                    )

        # Confirm and commit
        if delete_count == 0:
            if prompt:
                logger.warning("Nothing to delete.")
            if transaction:
                self.connection.cancel_transaction()
        elif not transaction:
            logger.info("Delete completed")
        else:
            if not prompt or user_choice("Commit deletes?", default="no") == "yes":
                if transaction:
                    self.connection.commit_transaction()
                if prompt:
                    logger.info("Delete committed.")
            else:
                if transaction:
                    self.connection.cancel_transaction()
                if prompt:
                    logger.warning("Delete cancelled")
                delete_count = 0  # Reset count when delete is cancelled
        return delete_count

    def drop_quick(self):
        """
        Drops the table without cascading to dependent tables and without user prompt.
        """
        if self.is_declared:
            # Clean up lineage entries for this table
            from .lineage import delete_table_lineages

            delete_table_lineages(self.connection, self.database, self.table_name)

            query = "DROP TABLE %s" % self.full_table_name
            self.connection.query(query)
            logger.info("Dropped table %s" % self.full_table_name)
        else:
            logger.info("Nothing to drop: table %s is not declared" % self.full_table_name)

    def drop(self, prompt: bool | None = None):
        """
        Drop the table and all tables that reference it, recursively.

        Args:
            prompt: If `True`, show what will be dropped and ask for confirmation.
                If `False`, drop without confirmation. Default is `dj.config['safemode']`.
        """
        if self.restriction:
            raise DataJointError(
                "A table with an applied restriction cannot be dropped. Call drop() on the unrestricted Table."
            )
        prompt = config["safemode"] if prompt is None else prompt

        self.connection.dependencies.load()
        do_drop = True
        tables = [table for table in self.connection.dependencies.descendants(self.full_table_name) if not table.isdigit()]

        # avoid dropping part tables without their masters: See issue #374
        for part in tables:
            master = get_master(part)
            if master and master not in tables:
                raise DataJointError(
                    "Attempt to drop part table {part} before dropping its master. Drop {master} first.".format(
                        part=part, master=master
                    )
                )

        if prompt:
            for table in tables:
                logger.info(table + " (%d tuples)" % len(FreeTable(self.connection, table)))
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
            'SHOW TABLE STATUS FROM `{database}` WHERE NAME="{table}"'.format(database=self.database, table=self.table_name),
            as_dict=True,
        ).fetchone()
        return ret["Data_length"] + ret["Index_length"]

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
        definition = "# " + self.heading.table_status["comment"] + "\n" if self.heading.table_status["comment"] else ""
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
                            index_props = " [{}]".format(", ".join(index_props)) if index_props else ""

                        if not fk_props["aliased"]:
                            # simple foreign key
                            definition += "->{props} {class_name}\n".format(
                                props=index_props,
                                class_name=lookup_class_name(parent_name, context) or parent_name,
                            )
                        else:
                            # projected foreign key
                            definition += "->{props} {class_name}.proj({proj_list})\n".format(
                                props=index_props,
                                class_name=lookup_class_name(parent_name, context) or parent_name,
                                proj_list=",".join(
                                    '{}="{}"'.format(attr, ref) for attr, ref in fk_props["attr_map"].items() if ref != attr
                                ),
                            )
                            attributes_declared.update(fk_props["attr_map"])
            if do_include:
                attributes_declared.add(attr.name)
                # Use original_type (core type alias) if available, otherwise use type
                display_type = attr.original_type or attr.type
                definition += "%-20s : %-28s %s\n" % (
                    (attr.name if attr.default is None else "%s=%s" % (attr.name, attr.default)),
                    "%s%s" % (display_type, " auto_increment" if attr.autoincrement else ""),
                    "# " + attr.comment if attr.comment else "",
                )
        # add remaining indexes
        for k, v in indexes.items():
            definition += "{unique}INDEX ({attrs})\n".format(unique="UNIQUE " if v["unique"] else "", attrs=", ".join(k))
        if printout:
            logger.info("\n" + definition)
        return definition

    # --- private helper functions ----
    def __make_placeholder(self, name, value, ignore_extra_fields=False, row=None):
        """
        For a given attribute `name` with `value`, return its processed value or value placeholder
        as a string to be included in the query and the value, if any, to be submitted for
        processing by mysql API.

        In the simplified type system:
        - Codecs handle all custom encoding via type chains
        - UUID values are converted to bytes
        - JSON values are serialized
        - Blob values pass through as bytes
        - Numeric values are stringified

        :param name:  name of attribute to be inserted
        :param value: value of attribute to be inserted
        :param ignore_extra_fields: if True, return None for unknown fields
        :param row: the full row dict (unused in simplified model)
        """
        if ignore_extra_fields and name not in self.heading:
            return None
        attr = self.heading[name]

        # Apply adapter encoding with type chain support
        if attr.codec:
            from .codecs import resolve_dtype

            # Skip validation and encoding for None values (nullable columns)
            if value is None:
                return name, "DEFAULT", None

            attr.codec.validate(value)

            # Resolve full type chain
            _, type_chain, resolved_store = resolve_dtype(f"<{attr.codec.name}>", store_name=attr.store)

            # Build context dict for schema-addressed codecs
            # Include _schema, _table, _field, and primary key values
            context = {
                "_schema": self.database,
                "_table": self.table_name,
                "_field": name,
            }
            # Add primary key values from row if available
            if row is not None:
                for pk_name in self.primary_key:
                    if pk_name in row:
                        context[pk_name] = row[pk_name]

            # Apply encoders from outermost to innermost
            for attr_type in type_chain:
                # Pass store_name to encoders that support it (check via introspection)
                import inspect

                sig = inspect.signature(attr_type.encode)
                if "store_name" in sig.parameters:
                    value = attr_type.encode(value, key=context, store_name=resolved_store)
                else:
                    value = attr_type.encode(value, key=context)

        # Handle NULL values
        if value is None or (attr.numeric and (value == "" or np.isnan(float(value)))):
            placeholder, value = "DEFAULT", None
        else:
            placeholder = "%s"
            # UUID - convert to bytes
            if attr.uuid:
                if not isinstance(value, uuid.UUID):
                    try:
                        value = uuid.UUID(value)
                    except (AttributeError, ValueError):
                        raise DataJointError(f"badly formed UUID value {value} for attribute `{name}`")
                value = value.bytes
            # JSON - serialize to string
            elif attr.json:
                value = json.dumps(value)
            # Numeric - convert to string
            elif attr.numeric:
                value = str(int(value) if isinstance(value, (bool, np.bool_)) else value)
            # Blob - pass through as bytes (use <blob> for automatic serialization)

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
                            raise KeyError("`{0:s}` is not in the table heading".format(field))
            elif set(field_list) != set(fields).intersection(self.heading.names):
                raise DataJointError("Attempt to insert rows with different fields.")

        # Convert row to dict for object attribute processing
        row_dict = None
        if isinstance(row, np.void):  # np.array
            check_fields(row.dtype.fields)
            row_dict = {name: row[name] for name in row.dtype.fields}
            attributes = [
                self.__make_placeholder(name, row[name], ignore_extra_fields, row=row_dict)
                for name in self.heading
                if name in row.dtype.fields
            ]
        elif isinstance(row, collections.abc.Mapping):  # dict-based
            check_fields(row)
            row_dict = dict(row)
            attributes = [
                self.__make_placeholder(name, row[name], ignore_extra_fields, row=row_dict)
                for name in self.heading
                if name in row
            ]
        else:  # positional
            warnings.warn(
                "Positional inserts (tuples/lists) are deprecated and will be removed in a future version. "
                "Use dict with explicit field names instead: table.insert1({'field': value, ...})",
                DeprecationWarning,
                stacklevel=4,  # Point to user's insert()/insert1() call
            )
            try:
                if len(row) != len(self.heading):
                    raise DataJointError(
                        "Invalid insert argument. Incorrect number of attributes: {given} given; {expected} expected".format(
                            given=len(row), expected=len(self.heading)
                        )
                    )
            except TypeError:
                raise DataJointError("Datatype %s cannot be inserted" % type(row))
            else:
                row_dict = dict(zip(self.heading.names, row))
                attributes = [
                    self.__make_placeholder(name, value, ignore_extra_fields, row=row_dict)
                    for name, value in zip(self.heading, row)
                ]
        if ignore_extra_fields:
            attributes = [a for a in attributes if a is not None]

        if not attributes:
            # Check if empty insert is allowed (all attributes have defaults)
            required_attrs = [
                attr.name
                for attr in self.heading.attributes.values()
                if not (attr.autoincrement or attr.nullable or attr.default is not None)
            ]
            if required_attrs:
                raise DataJointError(f"Cannot insert empty row. The following attributes require values: {required_attrs}")
            # All attributes have defaults - allow empty insert
            row_to_insert = {"names": (), "placeholders": (), "values": ()}
        else:
            row_to_insert = dict(zip(("names", "placeholders", "values"), zip(*attributes)))
        if not field_list:
            # first row sets the composition of the field list
            field_list.extend(row_to_insert["names"])
        else:
            #  reorder attributes in row_to_insert to match field_list
            order = list(row_to_insert["names"].index(field) for field in field_list)
            row_to_insert["names"] = list(row_to_insert["names"][i] for i in order)
            row_to_insert["placeholders"] = list(row_to_insert["placeholders"][i] for i in order)
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
            # skip IPython's implicit variables
            if not member_name.startswith("_"):
                if inspect.isclass(member) and issubclass(member, Table):
                    if member.full_table_name == name:  # found it!
                        return ".".join([node["context_name"], member_name]).lstrip(".")
                    try:  # look for part tables
                        parts = member.__dict__
                    except AttributeError:
                        pass  # not a UserTable -- cannot have part tables.
                    else:
                        for part in (getattr(member, p) for p in parts if p[0].isupper() and hasattr(member, p)):
                            if inspect.isclass(part) and issubclass(part, Table) and part.full_table_name == name:
                                return ".".join([node["context_name"], member_name, part.__name__]).lstrip(".")
                elif node["depth"] > 0 and inspect.ismodule(member) and member.__name__ != "datajoint":
                    try:
                        nodes.append(
                            dict(
                                context=dict(inspect.getmembers(member)),
                                context_name=node["context_name"] + "." + member_name,
                                depth=node["depth"] - 1,
                            )
                        )
                    except (ImportError, TypeError):
                        pass  # could not inspect module members, skip
    return None


class FreeTable(Table):
    """
    A base table without a dedicated class. Each instance is associated with a table
    specified by full_table_name.

    :param conn:  a dj.Connection object
    :param full_table_name: in format `database`.`table_name`
    """

    def __init__(self, conn, full_table_name):
        self.database, self._table_name = (s.strip("`") for s in full_table_name.split("."))
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
        return "FreeTable(`%s`.`%s`)\n" % (self.database, self._table_name) + super().__repr__()
