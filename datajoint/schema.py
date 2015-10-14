import pymysql
import logging

from . import conn, DataJointError
from .heading import Heading
from .base_relation import BaseRelation
from .user_relations import Part
import inspect
logger = logging.getLogger(__name__)


class Schema:
    """
    A schema object can be used  as a decorator that associates a Relation class to a database as
    well as a namespace for looking up foreign key references in table declaration.
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
            # create schema
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

        # Process subordinate relations
        parts = list()
        is_part = lambda x: inspect.isclass(x) and issubclass(x, Part)

        for var, part in inspect.getmembers(cls, is_part):
            parts.append(part)
            part._master = cls
            # TODO: look into local namespace for the subclasses
            process_relation_class(part, context=dict(self.context, **{cls.__name__: cls}))

        # invoke Relation._prepare() on class and its part relations.
        cls()._prepare()
        for part in parts:
            part()._prepare()

        return cls

    @property
    def jobs(self):
        """
        schema.jobs provides a view of the job reservation table for the schema
        :return: jobs relation
        """
        return self.connection.jobs[self.database]
