import warnings
import pymysql
import logging
import inspect
import re
from . import conn, DataJointError, config
from .heading import Heading
from .utils import user_choice, to_camel_case
from .user_relations import UserRelation, Part, Computed, Imported, Manual, Lookup

logger = logging.getLogger(__name__)


class Schema:
    """
    A schema object is a decorator for UserRelation classes that binds them to their database.
    It also specifies the namespace `context` in which other UserRelation classes are defined.
    """

    def __init__(self, database, context, connection=None):
        """
        Associates the specified database with this schema object. If the target database does not exist
        already, will attempt on creating the database.

        :param database: name of the database to associate the decorated class with
        :param context: dictionary for looking up foreign keys references, usually set to locals()
        :param connection: Connection object. Defaults to datajoint.conn()
        """
        if connection is None:
            connection = conn()
        self.database = database
        self.connection = connection
        self.context = context
        if not self.exists:
            # create database
            logger.info("Database `{database}` could not be found. "
                        "Attempting to create the database.".format(database=database))
            try:
                connection.query("CREATE DATABASE `{database}`".format(database=database))
                logger.info('Created database `{database}`.'.format(database=database))
            except pymysql.OperationalError:
                raise DataJointError("Database named `{database}` was not defined, and"
                                     " an attempt to create has failed. Check"
                                     " permissions.".format(database=database))
        connection.register(self)

    def __repr__(self):
        return 'Schema database: `{database}` in module: {context}\n'.format(
            database=self.database,
            context=self.context['__name__'] if '__name__' in self.context else "__")

    def spawn_missing_classes(self):
        """
        Creates the appropriate python user relation classes from tables in the database and places them
        in the context.
        """

        tables = [row[0] for row in self.connection.query('SHOW TABLES in `%s`' % self.database)]

        # declare master relation classes
        master_classes = {}
        part_tables = []
        for table_name in tables:
            class_name = to_camel_case(table_name)
            if class_name not in self.context:
                try:
                    cls = next(cls for cls in (Lookup, Manual, Imported, Computed)
                               if re.fullmatch(cls.tier_regexp, table_name))
                except StopIteration:
                    if re.fullmatch(Part.tier_regexp, table_name):
                        part_tables.append(table_name)
                else:
                    master_classes[table_name] = type(class_name, (cls,), dict())

        # attach parts to masters
        for part_table in part_tables:
            groups = re.fullmatch(Part.tier_regexp, part_table).groupdict()
            class_name = to_camel_case(groups['part'])
            try:
                master_class = master_classes[groups['master']]
            except KeyError:
                # if master not found among the spawned classes, check in the context
                master_class = self.context[to_camel_case(groups['master'])]
                if not hasattr(master_class, class_name):
                    part_class = type(class_name, (Part,), dict(definition=...))
                    part_class._master = master_class
                    self.process_relation_class(part_class, context=self.context, assert_declared=True)
                    setattr(master_class, class_name, part_class)
            else:
                setattr(master_class, class_name, type(class_name, (Part,), dict()))

        # place classes in context upon decorating them with the schema
        for cls in master_classes.values():
            self.context[cls.__name__] = self(cls)

    def drop(self):
        """
        Drop the associated database if it exists
        """
        if not self.exists:
            logger.info("Database named `{database}` does not exist. Doing nothing.".format(database=self.database))
        elif (not config['safemode'] or
              user_choice("Proceed to delete entire schema `%s`?" % self.database, default='no') == 'yes'):
            logger.info("Dropping `{database}`.".format(database=self.database))
            try:
                self.connection.query("DROP DATABASE `{database}`".format(database=self.database))
                logger.info("Database `{database}` was dropped successfully.".format(database=self.database))
            except pymysql.OperationalError:
                raise DataJointError("An attempt to drop database named `{database}` "
                                     "has failed. Check permissions.".format(database=self.database))

    @property
    def exists(self):
        """
        :return: true if the associated database exists on the server
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
        relation_class._context = context
        # instantiate the class, declare the table if not already, and fill it with initial values.
        instance = relation_class()
        if not instance.is_declared:
            assert not assert_declared, 'incorrect table name generation'
            instance.declare()
        if hasattr(instance, 'contents'):
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
        Binds the passed in class object to a database. This is intended to be used as a decorator.
        :param cls: class to be decorated
        """

        if issubclass(cls, Part):
            raise DataJointError('The schema decorator should not be applied to Part relations')

        self.process_relation_class(cls, context=self.context)

        # Process part relations
        for part in cls._ordered_class_members:
            if part[0].isupper():
                part = getattr(cls, part)
                if inspect.isclass(part) and issubclass(part, Part):
                    part._master = cls
                    # allow addressing master
                    self.process_relation_class(part, context=dict(self.context, **{cls.__name__: cls}))
        return cls

    @property
    def jobs(self):
        """
        schema.jobs provides a view of the job reservation table for the schema
        :return: jobs relation
        """
        return self.connection.jobs[self.database]
