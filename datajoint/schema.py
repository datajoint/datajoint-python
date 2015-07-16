import pymysql
import logging

from . import DataJointError, conn
from .heading import Heading

logger = logging.getLogger(__name__)


class schema:
    """
    A schema object can be used  as a decorator that associates a Relation class to a database as
    well as a namespace for looking up foreign key references.
    """

    def __init__(self, database, context, connection=None):
        """
        :param database: name of the database to associate the decorated class with
        :param context: dictionary for looking up foreign keys references, usually set to locals()
        :param connection: Connection object. Defaults to datajoint.conn()
        """
        if connection is None:
            connection = conn()
        self.database = database
        self.connection = connection
        self.context = context

        # if the database does not exist, create it
        cur = connection.query("SHOW DATABASES LIKE '{database}'".format(database=database))
        if cur.rowcount == 0:
            logger.info("Database `{database}` could not be found. "
                        "Attempting to create the database.".format(database=database))
            try:
                connection.query("CREATE DATABASE `{database}`".format(database=database))
                logger.info('Created database `{database}`.'.format(database=database))
            except pymysql.OperationalError:
                raise DataJointError("Database named `{database}` was not defined, and"
                                     "an attempt to create has failed. Check"
                                     " permissions.".format(database=database))

    def __call__(self, cls):
        """
        The decorator binds its argument class object to a database
        :param cls: class to be decorated
        """
        # class-level attributes
        cls.database = self.database
        cls._connection = self.connection
        cls._heading = Heading()
        cls._context = self.context

        # trigger table declaration by requesting the heading from an instance
        instance = cls()
        instance.heading
        instance.prepare()
        return cls

    @property
    def jobs(self):
        return self.connection.jobs[self.database]
