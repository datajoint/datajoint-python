import warnings
import pymysql
import logging
import inspect
import re
import itertools
import collections
from . import conn, config
from .errors import DataJointError
from .jobs import JobTable
from .external import ExternalTable
from .heading import Heading
from .erd import ERD, _get_tier
from .utils import user_choice, to_camel_case
from .user_tables import Part, Computed, Imported, Manual, Lookup
from .table import lookup_class_name, Log, FreeTable

logger = logging.getLogger(__name__)


def ordered_dir(klass):
    """
    List (most) attributes of the class including inherited ones, similar to `dir` build-in function,
    but respects order of attribute declaration as much as possible.
    This becomes unnecessary in Python 3.6+ as dicts became ordered.
    :param klass: class to list members for
    :return: a list of attributes declared in klass and its superclasses
    """
    attr_list = list()
    for c in reversed(klass.mro()):
        attr_list.extend(e for e in (
            c._ordered_class_members if hasattr(c, '_ordered_class_members') else c.__dict__)
            if e not in attr_list)
    return attr_list


class Schema:
    """
    A schema object is a decorator for UserTable classes that binds them to their database.
    It also specifies the namespace `context` in which other UserTable classes are defined.
    """

    def __init__(self, schema_name, context=None, connection=None, create_schema=True, create_tables=True):
        """
        Associate database schema `schema_name`. If the schema does not exist, attempt to create it on the server.

        :param schema_name: the database schema to associate.
        :param context: dictionary for looking up foreign key references, leave None to use local context.
        :param connection: Connection object. Defaults to datajoint.conn().
        :param create_schema: When False, do not create the schema and raise an error if missing.
        :param create_tables: When False, do not create tables and raise errors when accessing missing tables.
        """
        if connection is None:
            connection = conn()
        self._log = None

        self.database = schema_name
        self.connection = connection
        self.context = context
        self.create_tables = create_tables
        self._jobs = None
        self._external = None
        if not self.exists:
            if not create_schema:
                raise DataJointError(
                    "Database named `{name}` was not defined. "
                    "Set argument create_schema=True to create it.".format(name=schema_name))
            else:
                # create database
                logger.info("Creating schema `{name}`.".format(name=schema_name))
                try:
                    connection.query("CREATE DATABASE `{name}`".format(name=schema_name))
                    logger.info('Creating schema `{name}`.'.format(name=schema_name))
                except pymysql.OperationalError:
                    raise DataJointError(
                        "Schema `{name}` does not exist and could not be created. "
                        "Check permissions.".format(name=schema_name))
                else:
                    self.log('created')
        self.log('connect')
        connection.register(self)

    @property
    def log(self):
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
                    context[class_name] = self(type(class_name, (cls,), dict()))

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
            self.process_relation_class(part_class, context=context, assert_declared=True)
            setattr(master_class, class_name, part_class)

    def drop(self, force=False):
        """
        Drop the associated schema if it exists
        """
        if not self.exists:
            logger.info("Schema named `{database}` does not exist. Doing nothing.".format(database=self.database))
        elif (not config['safemode'] or
              force or
              user_choice("Proceed to delete entire schema `%s`?" % self.database, default='no') == 'yes'):
            logger.info("Dropping `{database}`.".format(database=self.database))
            try:
                self.connection.query("DROP DATABASE `{database}`".format(database=self.database))
                logger.info("Schema `{database}` was dropped successfully.".format(database=self.database))
            except pymysql.OperationalError:
                raise DataJointError("An attempt to drop schema `{database}` "
                                     "has failed. Check permissions.".format(database=self.database))

    @property
    def exists(self):
        """
        :return: true if the associated schema exists on the server
        """
        cur = self.connection.query("SHOW DATABASES LIKE '{database}'".format(database=self.database))
        return cur.rowcount > 0

    def process_relation_class(self, relation_class, context, assert_declared=False):
        """
        assign schema properties to the relation class and declare the table
        """
        relation_class.database = self.database
        relation_class._connection = self.connection
        relation_class._heading = Heading()
        # instantiate the class, declare the table if not already
        instance = relation_class()
        is_declared = instance.is_declared
        if not is_declared:
            if not self.create_tables or assert_declared:
                raise DataJointError('Table not declared %s' % instance.table_name)
            else:
                instance.declare(context)
        is_declared = is_declared or instance.is_declared

        # fill values in Lookup tables from their contents property
        if isinstance(instance, Lookup) and hasattr(instance, 'contents') and is_declared:
            contents = list(instance.contents)
            if len(contents) > len(instance):
                if instance.heading.has_autoincrement:
                    warnings.warn(
                        'Contents has changed but cannot be inserted because {table} has autoincrement.'.format(
                            table=instance.__class__.__name__))
                else:
                    instance.insert(contents, skip_duplicates=True)

    def __call__(self, cls):
        """
        Binds the supplied class to a schema. This is intended to be used as a decorator.
        :param cls: class to decorate.
        """
        context = self.context if self.context is not None else inspect.currentframe().f_back.f_locals
        if issubclass(cls, Part):
            raise DataJointError('The schema decorator should not be applied to Part relations')
        self.process_relation_class(cls, context=dict(context, self=cls, **{cls.__name__: cls}))

        # Process part relations
        for part in ordered_dir(cls):
            if part[0].isupper():
                part = getattr(cls, part)
                if inspect.isclass(part) and issubclass(part, Part):
                    part._master = cls
                    # allow addressing master by name or keyword 'master'
                    self.process_relation_class(part, context=dict(
                        context, master=cls, self=part, **{cls.__name__: cls}))
        return cls

    @property
    def jobs(self):
        """
        schema.jobs provides a view of the job reservation table for the schema
        :return: jobs table
        """
        if self._jobs is None:
            self._jobs = JobTable(self.connection, self.database)
        return self._jobs

    @property
    def external_table(self):
        """
        schema.external provides a view of the external hash table for the schema
        :return: external table
        """
        if self._external is None:
            self._external = ExternalTable(self.connection, self.database)
        return self._external
