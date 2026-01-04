"""
Schema management for DataJoint.

This module provides the Schema class for binding Python table classes to
database schemas, and utilities for schema introspection and management.
"""

from __future__ import annotations

import collections
import inspect
import itertools
import logging
import re
import types
import warnings
from typing import TYPE_CHECKING, Any

from .connection import conn
from .errors import AccessError, DataJointError

if TYPE_CHECKING:
    from .connection import Connection
from .heading import Heading
from .jobs import Job
from .settings import config
from .table import FreeTable, lookup_class_name
from .user_tables import Computed, Imported, Lookup, Manual, Part, _get_tier
from .utils import to_camel_case, user_choice

logger = logging.getLogger(__name__.split(".")[0])


def ordered_dir(class_: type) -> list[str]:
    """
    List class attributes respecting declaration order.

    Similar to the ``dir()`` built-in, but preserves attribute declaration
    order as much as possible.

    Parameters
    ----------
    class_ : type
        Class to list members for.

    Returns
    -------
    list[str]
        Attributes declared in class_ and its superclasses.
    """
    attr_list = list()
    for c in reversed(class_.mro()):
        attr_list.extend(e for e in c.__dict__ if e not in attr_list)
    return attr_list


class Schema:
    """
    Decorator that binds table classes to a database schema.

    Schema objects associate Python table classes with database schemas and
    provide the namespace context for foreign key resolution.

    Parameters
    ----------
    schema_name : str, optional
        Database schema name. If omitted, call ``activate()`` later.
    context : dict, optional
        Namespace for foreign key lookup. None uses caller's context.
    connection : Connection, optional
        Database connection. Defaults to ``dj.conn()``.
    create_schema : bool, optional
        If False, raise error if schema doesn't exist. Default True.
    create_tables : bool, optional
        If False, raise error when accessing missing tables. Default True.
    add_objects : dict, optional
        Additional objects for the declaration context.

    Examples
    --------
    >>> schema = dj.Schema('my_schema')
    >>> @schema
    ... class Session(dj.Manual):
    ...     definition = '''
    ...     session_id : int
    ...     '''
    """

    def __init__(
        self,
        schema_name: str | None = None,
        context: dict[str, Any] | None = None,
        *,
        connection: Connection | None = None,
        create_schema: bool = True,
        create_tables: bool = True,
        add_objects: dict[str, Any] | None = None,
    ) -> None:
        """
        Initialize the schema object.

        Parameters
        ----------
        schema_name : str, optional
            Database schema name. If omitted, call ``activate()`` later.
        context : dict, optional
            Namespace for foreign key lookup. None uses caller's context.
        connection : Connection, optional
            Database connection. Defaults to ``dj.conn()``.
        create_schema : bool, optional
            If False, raise error if schema doesn't exist. Default True.
        create_tables : bool, optional
            If False, raise error when accessing missing tables. Default True.
        add_objects : dict, optional
            Additional objects for the declaration context.
        """
        self.connection = connection
        self.database = None
        self.context = context
        self.create_schema = create_schema
        self.create_tables = create_tables
        self.add_objects = add_objects
        self.declare_list = []
        if schema_name:
            self.activate(schema_name)

    def is_activated(self) -> bool:
        """Check if the schema has been activated."""
        return self.database is not None

    def activate(
        self,
        schema_name: str | None = None,
        *,
        connection: Connection | None = None,
        create_schema: bool | None = None,
        create_tables: bool | None = None,
        add_objects: dict[str, Any] | None = None,
    ) -> None:
        """
        Associate with a database schema.

        If the schema does not exist, attempts to create it on the server.

        Parameters
        ----------
        schema_name : str, optional
            Database schema name. None asserts schema is already activated.
        connection : Connection, optional
            Database connection. Defaults to ``dj.conn()``.
        create_schema : bool, optional
            If False, raise error if schema doesn't exist.
        create_tables : bool, optional
            If False, raise error when accessing missing tables.
        add_objects : dict, optional
            Additional objects for the declaration context.

        Raises
        ------
        DataJointError
            If schema_name is None and schema not yet activated, or if
            schema already activated for a different database.
        """
        if schema_name is None:
            if self.exists:
                return
            raise DataJointError("Please provide a schema_name to activate the schema.")
        if self.database is not None and self.exists:
            if self.database == schema_name:  # already activated
                return
            raise DataJointError("The schema is already activated for schema {db}.".format(db=self.database))
        if connection is not None:
            self.connection = connection
        if self.connection is None:
            self.connection = conn()
        self.database = schema_name
        if create_schema is not None:
            self.create_schema = create_schema
        if create_tables is not None:
            self.create_tables = create_tables
        if add_objects:
            self.add_objects = add_objects
        if not self.exists:
            if not self.create_schema or not self.database:
                raise DataJointError(
                    "Database `{name}` has not yet been declared. Set argument create_schema=True to create it.".format(
                        name=schema_name
                    )
                )
            # create database
            logger.debug("Creating schema `{name}`.".format(name=schema_name))
            try:
                self.connection.query("CREATE DATABASE `{name}`".format(name=schema_name))
            except AccessError:
                raise DataJointError(
                    "Schema `{name}` does not exist and could not be created. Check permissions.".format(name=schema_name)
                )
        self.connection.register(self)

        # decorate all tables already decorated
        for cls, context in self.declare_list:
            if self.add_objects:
                context = dict(context, **self.add_objects)
            self._decorate_master(cls, context)

    def _assert_exists(self, message=None):
        if not self.exists:
            raise DataJointError(message or "Schema `{db}` has not been created.".format(db=self.database))

    def __call__(self, cls: type, *, context: dict[str, Any] | None = None) -> type:
        """
        Bind a table class to this schema. Used as a decorator.

        Parameters
        ----------
        cls : type
            Table class to decorate.
        context : dict, optional
            Declaration context. Supplied by spawn_missing_classes.

        Returns
        -------
        type
            The decorated class.

        Raises
        ------
        DataJointError
            If applied to a Part table (use on master only).
        """
        context = context or self.context or inspect.currentframe().f_back.f_locals
        if issubclass(cls, Part):
            raise DataJointError("The schema decorator should not be applied to Part tables.")
        if self.is_activated():
            self._decorate_master(cls, context)
        else:
            self.declare_list.append((cls, context))
        return cls

    def _decorate_master(self, cls: type, context: dict[str, Any]) -> None:
        """
        Process a master table class and its part tables.

        Parameters
        ----------
        cls : type
            Master table class to process.
        context : dict
            Declaration context for foreign key resolution.
        """
        self._decorate_table(cls, context=dict(context, self=cls, **{cls.__name__: cls}))
        # Process part tables
        for part in ordered_dir(cls):
            if part[0].isupper():
                part = getattr(cls, part)
                if inspect.isclass(part) and issubclass(part, Part):
                    part._master = cls
                    # allow addressing master by name or keyword 'master'
                    self._decorate_table(
                        part,
                        context=dict(context, master=cls, self=part, **{cls.__name__: cls}),
                    )

    def _decorate_table(self, table_class: type, context: dict[str, Any], assert_declared: bool = False) -> None:
        """
        Assign schema properties to the table class and declare the table.

        Parameters
        ----------
        table_class : type
            Table class to decorate.
        context : dict
            Declaration context for foreign key resolution.
        assert_declared : bool, optional
            If True, assert table is already declared. Default False.
        """
        table_class.database = self.database
        table_class._connection = self.connection
        table_class._heading = Heading(
            table_info=dict(
                conn=self.connection,
                database=self.database,
                table_name=table_class.table_name,
                context=context,
            )
        )
        table_class._support = [table_class.full_table_name]
        table_class.declaration_context = context

        # instantiate the class, declare the table if not already
        instance = table_class()
        is_declared = instance.is_declared
        if not is_declared and not assert_declared and self.create_tables:
            instance.declare(context)
            self.connection.dependencies.clear()
        is_declared = is_declared or instance.is_declared

        # add table definition to the doc string
        if isinstance(table_class.definition, str):
            table_class.__doc__ = (table_class.__doc__ or "") + "\nTable definition:\n\n" + table_class.definition

        # fill values in Lookup tables from their contents property
        if isinstance(instance, Lookup) and hasattr(instance, "contents") and is_declared:
            contents = list(instance.contents)
            if len(contents) > len(instance):
                if instance.heading.has_autoincrement:
                    warnings.warn(
                        ("Contents has changed but cannot be inserted because {table} has autoincrement.").format(
                            table=instance.__class__.__name__
                        )
                    )
                else:
                    instance.insert(contents, skip_duplicates=True)

    def __repr__(self):
        return "Schema `{name}`\n".format(name=self.database)

    @property
    def size_on_disk(self) -> int:
        """
        Return the total size of all tables in the schema.

        Returns
        -------
        int
            Size in bytes (data + indices).
        """
        self._assert_exists()
        return int(
            self.connection.query(
                """
            SELECT SUM(data_length + index_length)
            FROM information_schema.tables WHERE table_schema='{db}'
            """.format(db=self.database)
            ).fetchone()[0]
        )

    def spawn_missing_classes(self, context: dict[str, Any] | None = None) -> None:
        """
        Create Python table classes for tables without existing classes.

        Introspects the database schema and creates appropriate Python classes
        (Lookup, Manual, Imported, Computed, Part) for tables that don't have
        corresponding classes in the context.

        Parameters
        ----------
        context : dict, optional
            Namespace to place created classes into. Defaults to caller's
            local namespace.
        """
        self._assert_exists()
        if context is None:
            if self.context is not None:
                context = self.context
            else:
                # if context is missing, use the calling namespace
                frame = inspect.currentframe().f_back
                context = frame.f_locals
                del frame
        tables = [
            row[0]
            for row in self.connection.query("SHOW TABLES in `%s`" % self.database)
            if lookup_class_name("`{db}`.`{tab}`".format(db=self.database, tab=row[0]), context, 0) is None
        ]
        master_classes = (Lookup, Manual, Imported, Computed)
        part_tables = []
        for table_name in tables:
            class_name = to_camel_case(table_name)
            if class_name not in context:
                try:
                    cls = next(cls for cls in master_classes if re.fullmatch(cls.tier_regexp, table_name))
                except StopIteration:
                    if re.fullmatch(Part.tier_regexp, table_name):
                        part_tables.append(table_name)
                else:
                    # declare and decorate master table classes
                    context[class_name] = self(type(class_name, (cls,), dict()), context=context)

        # attach parts to masters
        for table_name in part_tables:
            groups = re.fullmatch(Part.tier_regexp, table_name).groupdict()
            class_name = to_camel_case(groups["part"])
            try:
                master_class = context[to_camel_case(groups["master"])]
            except KeyError:
                raise DataJointError("The table %s does not follow DataJoint naming conventions" % table_name)
            part_class = type(class_name, (Part,), dict(definition=...))
            part_class._master = master_class
            self._decorate_table(part_class, context=context, assert_declared=True)
            setattr(master_class, class_name, part_class)

    def drop(self, force: bool = False) -> None:
        """
        Drop the associated schema and all its tables.

        Parameters
        ----------
        force : bool, optional
            If True, skip confirmation prompt. Default False.

        Raises
        ------
        AccessError
            If insufficient permissions to drop the schema.
        """
        if not self.exists:
            logger.info("Schema named `{database}` does not exist. Doing nothing.".format(database=self.database))
        elif (
            not config["safemode"]
            or force
            or user_choice("Proceed to delete entire schema `%s`?" % self.database, default="no") == "yes"
        ):
            logger.debug("Dropping `{database}`.".format(database=self.database))
            try:
                self.connection.query("DROP DATABASE `{database}`".format(database=self.database))
                logger.debug("Schema `{database}` was dropped successfully.".format(database=self.database))
            except AccessError:
                raise AccessError(
                    "An attempt to drop schema `{database}` has failed. Check permissions.".format(database=self.database)
                )

    @property
    def exists(self) -> bool:
        """
        Check if the associated schema exists on the server.

        Returns
        -------
        bool
            True if the schema exists.

        Raises
        ------
        DataJointError
            If schema has not been activated.
        """
        if self.database is None:
            raise DataJointError("Schema must be activated first.")
        return bool(
            self.connection.query(
                "SELECT schema_name FROM information_schema.schemata WHERE schema_name = '{database}'".format(
                    database=self.database
                )
            ).rowcount
        )

    @property
    def lineage_table_exists(self) -> bool:
        """
        Check if the ~lineage table exists in this schema.

        Returns
        -------
        bool
            True if the lineage table exists.
        """
        from .lineage import lineage_table_exists

        self._assert_exists()
        return lineage_table_exists(self.connection, self.database)

    @property
    def lineage(self) -> dict[str, str]:
        """
        Get all lineages for tables in this schema.

        Returns
        -------
        dict[str, str]
            Mapping of ``'schema.table.attribute'`` to its lineage origin.
        """
        from .lineage import get_schema_lineages

        self._assert_exists()
        return get_schema_lineages(self.connection, self.database)

    def rebuild_lineage(self) -> None:
        """
        Rebuild the ~lineage table for all tables in this schema.

        Recomputes lineage for all attributes by querying FK relationships
        from the information_schema. Use to restore lineage for schemas that
        predate the lineage system or after corruption.

        Notes
        -----
        After rebuilding, restart the Python kernel and reimport to pick up
        the new lineage information.

        Upstream schemas (referenced via cross-schema foreign keys) must
        have their lineage rebuilt first.
        """
        from .lineage import rebuild_schema_lineage

        self._assert_exists()
        rebuild_schema_lineage(self.connection, self.database)

    @property
    def jobs(self) -> list[Job]:
        """
        Return Job objects for auto-populated tables with job tables.

        Only returns Job objects when both the target table and its
        ``~~table_name`` job table exist in the database. Job tables are
        created lazily on first access to ``table.jobs`` or
        ``populate(reserve_jobs=True)``.

        Returns
        -------
        list[Job]
            Job objects for existing job tables.
        """
        self._assert_exists()
        jobs_list = []

        # Get all existing job tables (~~prefix)
        # Note: %% escapes the % in pymysql
        result = self.connection.query(f"SHOW TABLES IN `{self.database}` LIKE '~~%%'").fetchall()
        existing_job_tables = {row[0] for row in result}

        # Iterate over auto-populated tables and check if their job table exists
        for table_name in self.list_tables():
            table = FreeTable(self.connection, f"`{self.database}`.`{table_name}`")
            tier = _get_tier(table.full_table_name)
            if tier in (Computed, Imported):
                # Compute expected job table name: ~~base_name
                base_name = table_name.lstrip("_")
                job_table_name = f"~~{base_name}"
                if job_table_name in existing_job_tables:
                    jobs_list.append(Job(table))

        return jobs_list

    @property
    def code(self):
        self._assert_exists()
        return self.save()

    def save(self, python_filename: str | None = None) -> str:
        """
        Generate Python code that recreates this schema.

        Parameters
        ----------
        python_filename : str, optional
            If provided, write the code to this file.

        Returns
        -------
        str
            Python module source code defining this schema.

        Notes
        -----
        This method is in preparation for a future release and is not
        officially supported.
        """
        self.connection.dependencies.load()
        self._assert_exists()
        module_count = itertools.count()
        # add virtual modules for referenced modules with names vmod0, vmod1, ...
        module_lookup = collections.defaultdict(lambda: "vmod" + str(next(module_count)))
        db = self.database

        def make_class_definition(table):
            tier = _get_tier(table).__name__
            class_name = table.split(".")[1].strip("`")
            indent = ""
            if tier == "Part":
                class_name = class_name.split("__")[-1]
                indent += "    "
            class_name = to_camel_case(class_name)

            def replace(s):
                d, tabs = s.group(1), s.group(2)
                return ("" if d == db else (module_lookup[d] + ".")) + ".".join(
                    to_camel_case(tab) for tab in tabs.lstrip("__").split("__")
                )

            return ("" if tier == "Part" else "\n@schema\n") + (
                '{indent}class {class_name}(dj.{tier}):\n{indent}    definition = """\n{indent}    {defi}"""'
            ).format(
                class_name=class_name,
                indent=indent,
                tier=tier,
                defi=re.sub(
                    r"`([^`]+)`.`([^`]+)`",
                    replace,
                    FreeTable(self.connection, table).describe(),
                ).replace("\n", "\n    " + indent),
            )

        tables = self.connection.dependencies.topo_sort()
        body = "\n\n".join(make_class_definition(table) for table in tables)
        python_code = "\n\n".join(
            (
                '"""This module was auto-generated by datajoint from an existing schema"""',
                "import datajoint as dj\n\nschema = dj.Schema('{db}')".format(db=db),
                "\n".join(
                    "{module} = dj.VirtualModule('{module}', '{schema_name}')".format(module=v, schema_name=k)
                    for k, v in module_lookup.items()
                ),
                body,
            )
        )
        if python_filename is None:
            return python_code
        with open(python_filename, "wt") as f:
            f.write(python_code)

    def list_tables(self) -> list[str]:
        """
        Return all user tables in the schema.

        Excludes hidden tables (starting with ``~``) such as ``~lineage``
        and job tables (``~~``).

        Returns
        -------
        list[str]
            Table names in topological order.
        """
        self.connection.dependencies.load()
        return [
            t
            for d, t in (table_name.replace("`", "").split(".") for table_name in self.connection.dependencies.topo_sort())
            if d == self.database
        ]


class VirtualModule(types.ModuleType):
    """
    A virtual module representing a DataJoint schema from database tables.

    Creates a Python module with table classes automatically generated from
    the database schema. Useful for accessing schemas without Python source.

    Parameters
    ----------
    module_name : str
        Display name for the module.
    schema_name : str
        Database schema name.
    create_schema : bool, optional
        If True, create the schema if it doesn't exist. Default False.
    create_tables : bool, optional
        If True, allow declaring new tables. Default False.
    connection : Connection, optional
        Database connection. Defaults to ``dj.conn()``.
    add_objects : dict, optional
        Additional objects to add to the module namespace.

    Examples
    --------
    >>> lab = dj.VirtualModule('lab', 'my_lab_schema')
    >>> lab.Subject.fetch()
    """

    def __init__(
        self,
        module_name: str,
        schema_name: str,
        *,
        create_schema: bool = False,
        create_tables: bool = False,
        connection: Connection | None = None,
        add_objects: dict[str, Any] | None = None,
    ) -> None:
        """
        Initialize the virtual module.

        Parameters
        ----------
        module_name : str
            Display name for the module.
        schema_name : str
            Database schema name.
        create_schema : bool, optional
            If True, create the schema if it doesn't exist. Default False.
        create_tables : bool, optional
            If True, allow declaring new tables. Default False.
        connection : Connection, optional
            Database connection. Defaults to ``dj.conn()``.
        add_objects : dict, optional
            Additional objects to add to the module namespace.
        """
        super(VirtualModule, self).__init__(name=module_name)
        _schema = Schema(
            schema_name,
            create_schema=create_schema,
            create_tables=create_tables,
            connection=connection,
        )
        if add_objects:
            self.__dict__.update(add_objects)
        self.__dict__["schema"] = _schema
        _schema.spawn_missing_classes(context=self.__dict__)


def list_schemas(connection: Connection | None = None) -> list[str]:
    """
    List all accessible schemas on the server.

    Parameters
    ----------
    connection : Connection, optional
        Database connection. Defaults to ``dj.conn()``.

    Returns
    -------
    list[str]
        Names of all accessible schemas.
    """
    return [
        r[0]
        for r in (connection or conn()).query(
            'SELECT schema_name FROM information_schema.schemata WHERE schema_name <> "information_schema"'
        )
    ]
