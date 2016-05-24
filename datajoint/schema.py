import pymysql
import logging
import re
from . import conn, DataJointError
from datajoint.utils import to_camel_case
from .heading import Heading
from .user_relations import Part, Computed, Imported, Manual, Lookup
import inspect

logger = logging.getLogger(__name__)


class Schema:
    """
    A schema object can be used  as a decorator that associates a Relation class to a database as
    well as a namespace for looking up foreign key references in table declaration.
    """

    def __init__(self, database, context, connection=None, prepare=True):
        """
        Associates the specified database with this schema object. If the target database does not exist
        already, will attempt on creating the database.

        :param database: name of the database to associate the decorated class with
        :param context: dictionary for looking up foreign keys references, usually set to locals()
        :param connection: Connection object. Defaults to datajoint.conn()
        :param prepare: if True, then all classes will execute cls().prepare() upon declaration
        """
        if connection is None:
            connection = conn()
        self.database = database
        self.connection = connection
        self.context = context
        self.prepare = prepare
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

    def spawn_missing_classes(self):
        """
        Creates the appropriate python user relation classes from tables in the database and places them
        in the context.
        """

        def _make_tuples_stub(unused_self, unused_key):
            raise NotImplementedError(
                "This is an automatically created user relation class. _make_tuples is not implemented.")

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
                    master_classes[table_name] = type(class_name, (cls,),
                                                      dict(definition=..., _make_tuples=_make_tuples_stub))
        # attach parts to masters
        for part_table in part_tables:
            groups = re.fullmatch(Part.tier_regexp, part_table).groupdict()
            try:
                master_class = master_classes[groups['master']]
            except KeyError:
                pass   # ignore part tables with no masters
            else:
                class_name = to_camel_case(groups['part'])
                setattr(master_class, class_name, type(class_name, (Part,), dict(definition=...)))

        # place classes in context upon decorating them with the schema
        for cls in master_classes.values():
            self.context[cls.__name__] = self(cls)

    def drop(self):
        """
        Drop the associated database if it exists
        """
        if self.exists:
            logger.info("Dropping `{database}`.".format(database=self.database))
            try:
                self.connection.query("DROP DATABASE `{database}`".format(database=self.database))
                logger.info("Database `{database}` was dropped successfully.".format(database=self.database))
            except pymysql.OperationalError:
                raise DataJointError("An attempt to drop database named `{database}` "
                                     "has failed. Check permissions.".format(database=self.database))
        else:
            logger.info("Database named `{database}` does not exist. Doing nothing.".format(database=self.database))

    @property
    def exists(self):
        """
        :return: true if the associated database exists on the server
        """
        cur = self.connection.query("SHOW DATABASES LIKE '{database}'".format(database=self.database))
        return cur.rowcount > 0

    def __call__(self, cls):
        """
        Binds the passed in class object to a database. This is intended to be used as a decorator.
        :param cls: class to be decorated
        """

        def process_relation_class(relation_class, context):
            """
            assign schema properties to the relation class and declare the table
            """
            relation_class.database = self.database
            relation_class._connection = self.connection
            relation_class._heading = Heading()
            relation_class._context = context
            # instantiate the class and declare the table in database if not already present
            relation_class().declare()

        if issubclass(cls, Part):
            raise DataJointError('The schema decorator should not be applied to Part relations')

        process_relation_class(cls, context=self.context)

        # Process part relations
        def is_part(x):
            return inspect.isclass(x) and issubclass(x, Part)

        parts = list()
        for part in dir(cls):
            if part[0].isupper():
                part = getattr(cls, part)
                if is_part(part):
                    parts.append(part)
                    part._master = cls
                    process_relation_class(part, context=dict(self.context, **{cls.__name__: cls}))

        # invoke Relation.prepare() on class and its part relations.
        if self.prepare:
            cls().prepare()
            for part in parts:
                part().prepare()

        return cls

    @property
    def jobs(self):
        """
        schema.jobs provides a view of the job reservation table for the schema
        :return: jobs relation
        """
        return self.connection.jobs[self.database]
