import pymysql
import logging

from . import conn, DataJointError
from .heading import Heading
from .relation import Relation
from .user_relations import Part
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

        def process_relation_class(relation_class, context):
            """
            assign schema properties to the relation class and declare the table
            """
            relation_class.database = self.database
            relation_class._connection = self.connection
            relation_class._heading = Heading()
            relation_class._context = context
            instance = relation_class()
            instance.heading  # trigger table declaration
            instance._prepare()

        if issubclass(cls, Part):
            raise DataJointError('The schema decorator should not apply to part relations')

        process_relation_class(cls, context=self.context)

        #  Process subordinate relations
        for name in (name for name in dir(cls) if not name.startswith('_')):
            part = getattr(cls, name)
            try:
                is_sub = issubclass(part, Part)
            except TypeError:
                pass
            else:
                if is_sub:
                    part._master = cls
                    process_relation_class(part, context=dict(self.context, **{cls.__name__: cls}))
                elif issubclass(part, Relation):
                    raise DataJointError('Part relations must subclass from datajoint.Part')
        return cls

    @property
    def jobs(self):
        """
        schema.jobs provides a view of the job reservation table for the schema
        :return: jobs relation
        """
        return self.connection.jobs[self.database]
