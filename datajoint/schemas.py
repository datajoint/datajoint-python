import warnings
import logging
import inspect
import re
import itertools
import collections
from .connection import conn
from .diagram import Diagram, _get_tier
from .settings import config
from .errors import DataJointError, AccessError
from .jobs import JobTable
from .external import ExternalMapping
from .heading import Heading
from .utils import user_choice, to_camel_case
from .user_tables import Part, Computed, Imported, Manual, Lookup
from .table import lookup_class_name, Log, FreeTable
import types

logger = logging.getLogger(__name__)


def ordered_dir(class_):
    """
    List (most) attributes of the class including inherited ones, similar to `dir` build-in function,
    but respects order of attribute declaration as much as possible.
    This becomes unnecessary in Python 3.6+ as dicts became ordered.
    :param class_: class to list members for
    :return: a list of attributes declared in class_ and its superclasses
    """
    attr_list = list()
    for c in reversed(class_.mro()):
        attr_list.extend(e for e in (
            c._ordered_class_members if hasattr(c, '_ordered_class_members') else c.__dict__)
            if e not in attr_list)
    return attr_list


class Schema:
    """
    A schema object is a decorator for UserTable classes that binds them to their database.
    It also specifies the namespace `context` in which other UserTable classes are defined.
    """

    def __init__(self, schema_name=None, context=None, *, connection=None, create_schema=True,
                 create_tables=True, add_objects=None):
        """
        Associate database schema `schema_name`. If the schema does not exist, attempt to
        create it on the server.

        If the schema_name is omitted, then schema.activate(..) must be called later
        to associate with the database.

        :param schema_name: the database schema to associate.
        :param context: dictionary for looking up foreign key references, leave None to use local context.
        :param connection: Connection object. Defaults to datajoint.conn().
        :param create_schema: When False, do not create the schema and raise an error if missing.
        :param create_tables: When False, do not create tables and raise errors when accessing missing tables.
        :param add_objects: a mapping with additional objects to make available to the context in which table classes
        are declared.
        """
        self._log = None
        self.connection = connection
        self.database = None
        self.context = context
        self.create_schema = create_schema
        self.create_tables = create_tables
        self._jobs = None
        self.external = ExternalMapping(self)
        self.add_objects = add_objects
        self.declare_list = []
        if schema_name:
            self.activate(schema_name)

    def is_activated(self):
        return self.database is not None

    def activate(self, schema_name=None, *, connection=None, create_schema=None,
                 create_tables=None, add_objects=None):
        """
        Associate database schema `schema_name`. If the schema does not exist, attempt to
        create it on the server.
        :param schema_name: the database schema to associate.
            schema_name=None is used to assert that the schema has already been activated.
        :param connection: Connection object. Defaults to datajoint.conn().
        :param create_schema: If False, do not create the schema and raise an error if missing.
        :param create_tables: If False, do not create tables and raise errors when attempting
            to access missing tables.
        :param add_objects: a mapping with additional objects to make available to the context
            in which table classes are declared.
        """
        if schema_name is None:
            if self.exists:
                return
            raise DataJointError("Please provide a schema_name to activate the schema.")
        if self.database is not None and self.exists:
            if self.database == schema_name:  # already activated
                return
            raise DataJointError(
                "The schema is already activated for schema {db}.".format(db=self.database))
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
                    "Database `{name}` has not yet been declared. "
                    "Set argument create_schema=True to create it.".format(name=schema_name))
            # create database
            logger.info("Creating schema `{name}`.".format(name=schema_name))
            try:
                self.connection.query("CREATE DATABASE `{name}`".format(name=schema_name))
            except AccessError:
                raise DataJointError(
                    "Schema `{name}` does not exist and could not be created. "
                    "Check permissions.".format(name=schema_name))
            else:
                self.log('created')
        self.connection.register(self)

        # decorate all tables already decorated
        for cls, context in self.declare_list:
            if self.add_objects:
                context = dict(context, **self.add_objects)
            self._decorate_master(cls, context)

    def _assert_exists(self, message=None):
        if not self.exists:
            raise DataJointError(
                message or "Schema `{db}` has not been created.".format(db=self.database))

    def __call__(self, cls, *, context=None):
        """
        Binds the supplied class to a schema. This is intended to be used as a decorator.
        :param cls: class to decorate.
        :param context: supplied when called from spawn_missing_classes
        """
        context = context or self.context or inspect.currentframe().f_back.f_locals
        if issubclass(cls, Part):
            raise DataJointError('The schema decorator should not be applied to Part relations')
        if self.is_activated():
            self._decorate_master(cls, context)
        else:
            self.declare_list.append((cls, context))
        return cls

    def _decorate_master(self, cls, context):
        """
        :param cls: the master class to process
        :param context: the class' declaration context
        """
        self._decorate_table(cls, context=dict(context, self=cls, **{cls.__name__: cls}))
        # Process part tables
        for part in ordered_dir(cls):
            if part[0].isupper():
                part = getattr(cls, part)
                if inspect.isclass(part) and issubclass(part, Part):
                    part._master = cls
                    # allow addressing master by name or keyword 'master'
                    self._decorate_table(part, context=dict(
                        context, master=cls, self=part, **{cls.__name__: cls}))

    def _decorate_table(self, table_class, context, assert_declared=False):
        """
        assign schema properties to the table class and declare the table
        """
        table_class.database = self.database
        table_class._connection = self.connection
        table_class._heading = Heading(table_info=dict(
            conn=self.connection,
            database=self.database,
            table_name=table_class.table_name,
            context=context))
        table_class._support = [table_class.full_table_name]
        table_class.declaration_context = context

        # instantiate the class, declare the table if not already
        instance = table_class()
        is_declared = instance.is_declared
        if not is_declared:
            if not self.create_tables or assert_declared:
                raise DataJointError('Table `%s` not declared' % instance.table_name)
            instance.declare(context)
        is_declared = is_declared or instance.is_declared

        # add table definition to the doc string
        if isinstance(table_class.definition, str):
            table_class.__doc__ = (table_class.__doc__ or "") + "\nTable definition:\n\n" + table_class.definition

        # fill values in Lookup tables from their contents property
        if isinstance(instance, Lookup) and hasattr(instance, 'contents') and is_declared:
            contents = list(instance.contents)
            if len(contents) > len(instance):
                if instance.heading.has_autoincrement:
                    warnings.warn(('Contents has changed but cannot be inserted because '
                                  '{table} has autoincrement.').format(
                        table=instance.__class__.__name__))
                else:
                    instance.insert(contents, skip_duplicates=True)

    @property
    def log(self):
        self._assert_exists()
        if self._log is None:
            self._log = Log(self.connection, self.database)
        return self._log

    def __repr__(self):
        return 'Schema `{name}`\n'.format(name=self.database)

    @property
    def size_on_disk(self):
        """
        :return: size of the entire schema in bytes
        """
        self._assert_exists()
        return int(self.connection.query(
            """
            SELECT SUM(data_length + index_length)
            FROM information_schema.tables WHERE table_schema='{db}'
            """.format(db=self.database)).fetchone()[0])

    def spawn_missing_classes(self, context=None):
        """
        Creates the appropriate python user relation classes from tables in the schema and places them
        in the context.
        :param context: alternative context to place the missing classes into, e.g. locals()
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
            row[0] for row in self.connection.query('SHOW TABLES in `%s`' % self.database)
            if lookup_class_name('`{db}`.`{tab}`'.format(db=self.database, tab=row[0]), context, 0) is None]
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
                    # declare and decorate master relation classes
                    context[class_name] = self(type(class_name, (cls,), dict()), context=context)

        # attach parts to masters
        for table_name in part_tables:
            groups = re.fullmatch(Part.tier_regexp, table_name).groupdict()
            class_name = to_camel_case(groups['part'])
            try:
                master_class = context[to_camel_case(groups['master'])]
            except KeyError:
                raise DataJointError('The table %s does not follow DataJoint naming conventions' % table_name)
            part_class = type(class_name, (Part,), dict(definition=...))
            part_class._master = master_class
            self._decorate_table(part_class, context=context, assert_declared=True)
            setattr(master_class, class_name, part_class)

    def drop(self, force=False):
        """
        Drop the associated schema if it exists
        """
        if not self.exists:
            logger.info("Schema named `{database}` does not exist. Doing nothing.".format(
                database=self.database))
        elif (not config['safemode'] or
              force or
              user_choice("Proceed to delete entire schema `%s`?" % self.database, default='no') == 'yes'):
            logger.info("Dropping `{database}`.".format(database=self.database))
            try:
                self.connection.query("DROP DATABASE `{database}`".format(database=self.database))
                logger.info("Schema `{database}` was dropped successfully.".format(database=self.database))
            except AccessError:
                raise AccessError(
                    "An attempt to drop schema `{database}` "
                    "has failed. Check permissions.".format(database=self.database))

    @property
    def exists(self):
        """
        :return: true if the associated schema exists on the server
        """
        if self.database is None:
            raise DataJointError("Schema must be activated first.")
        return self.database is not None and (
            self.connection.query(
                "SELECT schema_name "
                "FROM information_schema.schemata "
                "WHERE schema_name = '{database}'".format(
                    database=self.database)).rowcount > 0)

    @property
    def jobs(self):
        """
        schema.jobs provides a view of the job reservation table for the schema
        :return: jobs table
        """
        self._assert_exists()
        if self._jobs is None:
            self._jobs = JobTable(self.connection, self.database)
        return self._jobs

    @property
    def code(self):
        self._assert_exists()
        return self.save()

    def save(self, python_filename=None):
        """
        Generate the code for a module that recreates the schema.
        This method is in preparation for a future release and is not officially supported.
        :return: a string containing the body of a complete Python module defining this schema.
        """
        self._assert_exists()
        module_count = itertools.count()
        # add virtual modules for referenced modules with names vmod0, vmod1, ...
        module_lookup = collections.defaultdict(lambda: 'vmod' + str(next(module_count)))
        db = self.database

        def make_class_definition(table):
            tier = _get_tier(table).__name__
            class_name = table.split('.')[1].strip('`')
            indent = ''
            if tier == 'Part':
                class_name = class_name.split('__')[-1]
                indent += '    '
            class_name = to_camel_case(class_name)

            def replace(s):
                d, tabs = s.group(1), s.group(2)
                return ('' if d == db else (module_lookup[d]+'.')) + '.'.join(
                    to_camel_case(tab) for tab in tabs.lstrip('__').split('__'))

            return ('' if tier == 'Part' else '\n@schema\n') + (
                '{indent}class {class_name}(dj.{tier}):\n'
                '{indent}    definition = """\n'
                '{indent}    {defi}"""').format(
                    class_name=class_name,
                    indent=indent,
                    tier=tier,
                    defi=re.sub(r'`([^`]+)`.`([^`]+)`', replace,
                                FreeTable(self.connection, table).describe(printout=False)
                                ).replace('\n', '\n    ' + indent))

        diagram = Diagram(self)
        body = '\n\n'.join(make_class_definition(table) for table in diagram.topological_sort())
        python_code = '\n\n'.join((
            '"""This module was auto-generated by datajoint from an existing schema"""',
            "import datajoint as dj\n\nschema = dj.Schema('{db}')".format(db=db),
            '\n'.join("{module} = dj.VirtualModule('{module}', '{schema_name}')".format(module=v, schema_name=k)
                      for k, v in module_lookup.items()), body))
        if python_filename is None:
            return python_code
        with open(python_filename, 'wt') as f:
            f.write(python_code)

    def list_tables(self):
        """
        Return a list of all tables in the schema except tables with ~ in first character such
        as ~logs and ~job
        :return: A list of table names from the database schema.
        """
        return [table_name for (table_name,) in self.connection.query("""
            SELECT table_name FROM information_schema.tables
            WHERE table_schema = %s and table_name NOT LIKE '~%%'""", args=(self.database,))]


class VirtualModule(types.ModuleType):
    """
    A virtual module imitates a Python module representing a DataJoint schema from table definitions in the database.
    It declares the schema objects and a class for each table.
    """
    def __init__(self, module_name, schema_name, *, create_schema=False,
                 create_tables=False, connection=None, add_objects=None):
        """
        Creates a python module with the given name from the name of a schema on the server and
        automatically adds classes to it corresponding to the tables in the schema.
        :param module_name: displayed module name
        :param schema_name: name of the database in mysql
        :param create_schema: if True, create the schema on the database server
        :param create_tables: if True, module.schema can be used as the decorator for declaring new
        :param connection: a dj.Connection object to pass into the schema
        :param add_objects: additional objects to add to the module
        :return: the python module containing classes from the schema object and the table classes
        """
        super(VirtualModule, self).__init__(name=module_name)
        _schema = Schema(schema_name, create_schema=create_schema,
                         create_tables=create_tables, connection=connection)
        if add_objects:
            self.__dict__.update(add_objects)
        self.__dict__['schema'] = _schema
        _schema.spawn_missing_classes(context=self.__dict__)


def list_schemas(connection=None):
    """
    :param connection: a dj.Connection object
    :return: list of all accessible schemas on the server
    """
    return [r[0] for r in (connection or conn()).query(
        'SELECT schema_name '
        'FROM information_schema.schemata '
        'WHERE schema_name <> "information_schema"')]
